use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};

use rocket::data::ToByteUnit;
use rocket::fairing::AdHoc;
use rocket::futures::stream::{self, StreamExt as _};
use rocket::http::Status;
use rocket::tokio::io::{AsyncReadExt, AsyncWriteExt};
use rocket::tokio::net::TcpStream;
use rocket::tokio::sync::{Mutex, Notify, oneshot};
use rocket::tokio::task::JoinHandle;
use rocket::tokio::time::timeout;
use rocket::{Config, Data, Ignite, Rocket, Shutdown, State};

const PREFIX: &[u8] = b"request-prefix";
const DEADLINE: Duration = Duration::from_secs(10);

type UploadResult = io::Result<Vec<u8>>;

#[derive(Default)]
struct Control {
    prefix_read: Notify,
    entered: Notify,
    release: Notify,
    result: Mutex<Option<oneshot::Sender<UploadResult>>>,
}

#[rocket::post("/early", data = "<_data>")]
fn early(_data: Data<'_>) -> (Status, &'static str) {
    (Status::UnprocessableEntity, "early rejection")
}

#[rocket::post("/upload", data = "<data>")]
async fn upload(data: Data<'_>, control: &State<Arc<Control>>) -> &'static str {
    let mut reader = data.open(1.mebibytes());
    let mut body = vec![0; PREFIX.len()];
    let result = match reader.read_exact(&mut body).await {
        Ok(_) => {
            control.prefix_read.notify_one();
            reader.read_to_end(&mut body).await.map(|_| body)
        }
        Err(e) => Err(e),
    };

    if let Some(sender) = control.result.lock().await.take() {
        let _ = sender.send(result);
    }

    "upload complete"
}

#[rocket::post("/gated-upload", data = "<data>")]
async fn gated_upload(data: Data<'_>, control: &State<Arc<Control>>) -> &'static str {
    control.entered.notify_one();
    control.release.notified().await;
    upload(data, control).await
}

struct Server {
    addr: SocketAddr,
    shutdown: Shutdown,
    task: JoinHandle<Result<Rocket<Ignite>, rocket::Error>>,
    control: Arc<Control>,
}

impl Server {
    async fn start() -> Self {
        let control = Arc::new(Control::default());
        let (tx, rx) = oneshot::channel();
        let mut config = Config { port: 0, ..Config::debug_default() };
        config.address = [127, 0, 0, 1].into();
        config.log_level = rocket::config::LogLevel::Off;
        config.shutdown.ctrlc = false;
        #[cfg(unix)]
        config.shutdown.signals.clear();
        config.shutdown.grace = 1;
        config.shutdown.mercy = 1;

        let rocket = rocket::custom(config)
            .manage(control.clone())
            .mount("/", rocket::routes![early, upload, gated_upload])
            .attach(AdHoc::on_liftoff("Bound Port", move |rocket| Box::pin(async move {
                tx.send(rocket.config().port).unwrap();
            })))
            .ignite().await.unwrap();

        let shutdown = rocket.shutdown();
        let task = rocket::tokio::spawn(rocket.launch());
        let port = timeout(DEADLINE, rx).await.unwrap().unwrap();
        Self { addr: ([127, 0, 0, 1], port).into(), shutdown, task, control }
    }

    async fn expect_upload(&self) -> oneshot::Receiver<UploadResult> {
        let (tx, rx) = oneshot::channel();
        *self.control.result.lock().await = Some(tx);
        rx
    }

    async fn stop(&mut self) {
        self.shutdown.clone().notify();
        timeout(DEADLINE, &mut self.task).await.unwrap().unwrap().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.clone().notify();
        self.task.abort();
    }
}

async fn upload_result(result: oneshot::Receiver<UploadResult>) -> UploadResult {
    timeout(DEADLINE, result).await.unwrap().unwrap()
}

fn protocols() -> Vec<bool> {
    if cfg!(feature = "http2") { vec![false, true] } else { vec![false] }
}

type RequestBody = BoxBody<Bytes, io::Error>;

enum Client {
    Http1(http1::SendRequest<RequestBody>),
    Http2(http2::SendRequest<RequestBody>),
}

impl Client {
    async fn connect(server: &Server, http2: bool) -> Self {
        let io = TokioIo::new(TcpStream::connect(server.addr).await.unwrap());
        if http2 {
            let (client, conn) = http2::handshake(TokioExecutor::new(), io).await.unwrap();
            rocket::tokio::spawn(conn);
            Client::Http2(client)
        } else {
            let (client, conn) = http1::handshake(io).await.unwrap();
            rocket::tokio::spawn(conn);
            Client::Http1(client)
        }
    }

    async fn post(
        &mut self,
        path: &str,
        length: Option<usize>,
        body: RequestBody,
    ) -> hyper::Response<Incoming> {
        let mut request = hyper::Request::post(format!("http://localhost{path}"));
        if let Some(length) = length {
            request = request.header("Content-Length", length.to_string());
        }

        let request = request.body(body).unwrap();
        let response = timeout(DEADLINE, async {
            match self {
                Client::Http1(client) => {
                    client.ready().await.unwrap();
                    client.send_request(request).await
                },
                Client::Http2(client) => {
                    client.ready().await.unwrap();
                    client.send_request(request).await
                },
            }
        });

        response.await.expect("response timed out").unwrap()
    }
}

async fn body_text(response: hyper::Response<Incoming>) -> Bytes {
    timeout(DEADLINE, response.collect()).await.unwrap().unwrap().to_bytes()
}

#[rocket::async_test]
async fn complete_uploads_end_cleanly_with_and_without_length() {
    for http2 in protocols() {
        for known_length in [false, true] {
            let mut server = Server::start().await;
            let result = server.expect_upload().await;
            let mut client = Client::connect(&server, http2).await;
            let (length, body) = if known_length {
                let body = Full::new(Bytes::from_static(PREFIX));
                (Some(PREFIX.len()), body.map_err(|e| match e {}).boxed())
            } else {
                let frames = stream::iter([Ok(Frame::data(Bytes::from_static(PREFIX)))]);
                (None, BodyExt::boxed(StreamBody::new(frames)))
            };

            let response = client.post("/upload", length, body).await;
            assert_eq!(response.status(), Status::Ok.code);
            assert_eq!(body_text(response).await, "upload complete");
            assert_eq!(upload_result(result).await.unwrap(), PREFIX);
            server.stop().await;
        }
    }
}

#[rocket::async_test]
async fn early_response_does_not_wait_for_upload_eof() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let mut client = Client::connect(&server, http2).await;
        let frames = stream::iter([Ok(Frame::data(Bytes::from_static(PREFIX)))])
            .chain(stream::pending());

        let body = BodyExt::boxed(StreamBody::new(frames));
        let response = client.post("/early", Some(1_000_000), body).await;

        assert_eq!(response.status(), Status::UnprocessableEntity.code);
        assert_eq!(body_text(response).await, "early rejection");
        server.stop().await;
    }
}

#[rocket::async_test]
async fn h1_disconnect_mid_upload_is_error_not_eof_with_and_without_length() {
    for chunked in [false, true] {
        let mut server = Server::start().await;
        let result = server.expect_upload().await;
        let mut socket = TcpStream::connect(server.addr).await.unwrap();
        let framing = if chunked { "Transfer-Encoding: chunked" } else { "Content-Length: 100" };
        let head = format!("POST /upload HTTP/1.1\r\nHost: localhost\r\n{framing}\r\n\r\n");
        socket.write_all(head.as_bytes()).await.unwrap();
        if chunked {
            socket.write_all(format!("{:x}\r\n", PREFIX.len()).as_bytes()).await.unwrap();
            socket.write_all(PREFIX).await.unwrap();
            socket.write_all(b"\r\n").await.unwrap();
        } else {
            socket.write_all(PREFIX).await.unwrap();
        }

        timeout(DEADLINE, server.control.prefix_read.notified()).await.unwrap();
        drop(socket);
        upload_result(result).await.expect_err("disconnected upload ended cleanly");
        server.stop().await;
    }
}

#[cfg(feature = "http2")]
mod h2_frames {
    use super::*;

    const DATA: u8 = 0;
    const HEADERS: u8 = 1;
    const RST_STREAM: u8 = 3;
    const SETTINGS: u8 = 4;
    const PING: u8 = 6;

    const END_STREAM: u8 = 0x1;
    const ACK: u8 = 0x1;
    const END_HEADERS: u8 = 0x4;

    pub const NO_ERROR: u32 = 0x0;
    const CANCEL: u32 = 0x8;

    pub async fn write_frame(socket: &mut TcpStream, kind: u8, flags: u8, payload: &[u8]) {
        let stream: u32 = if matches!(kind, SETTINGS | PING) { 0 } else { 1 };
        let len = (payload.len() as u32).to_be_bytes();
        let id = stream.to_be_bytes();
        let header = [len[1], len[2], len[3], kind, flags, id[0], id[1], id[2], id[3]];
        timeout(DEADLINE, socket.write_all(&header)).await.unwrap().unwrap();
        timeout(DEADLINE, socket.write_all(payload)).await.unwrap().unwrap();
    }

    async fn read_frame(socket: &mut TcpStream) -> (u8, u8, Vec<u8>) {
        let mut header = [0; 9];
        timeout(DEADLINE, socket.read_exact(&mut header)).await.unwrap().unwrap();
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let mut payload = vec![0; len];
        timeout(DEADLINE, socket.read_exact(&mut payload)).await.unwrap().unwrap();
        (header[3], header[4], payload)
    }

    /// Opens stream 1 as `POST {path}` without ending the stream.
    pub async fn open_upload(server: &Server, path: &str, length: Option<usize>) -> TcpStream {
        let mut socket = TcpStream::connect(server.addr).await.unwrap();
        socket.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.unwrap();
        write_frame(&mut socket, SETTINGS, 0, &[]).await;
        let (kind, _, _) = read_frame(&mut socket).await;
        assert_eq!(kind, SETTINGS);
        write_frame(&mut socket, SETTINGS, ACK, &[]).await;

        // HPACK: :method POST, :scheme http, :path {path}, :authority localhost,
        // and an optional literal content-length; no dynamic table or Huffman.
        let mut block = vec![0x83, 0x86, 0x04, path.len() as u8];
        block.extend_from_slice(path.as_bytes());
        block.extend_from_slice(b"\x01\x09localhost");
        if let Some(length) = length {
            let length = length.to_string();
            block.extend_from_slice(b"\x00\x0econtent-length");
            block.push(length.len() as u8);
            block.extend_from_slice(length.as_bytes());
        }

        write_frame(&mut socket, HEADERS, END_HEADERS, &block).await;
        socket
    }

    pub async fn data(socket: &mut TcpStream, bytes: &[u8], end_stream: bool) {
        write_frame(socket, DATA, if end_stream { END_STREAM } else { 0 }, bytes).await;
    }

    pub async fn reset(socket: &mut TcpStream, reason: u32) {
        write_frame(socket, RST_STREAM, 0, &reason.to_be_bytes()).await;
    }

    pub async fn cancel(socket: &mut TcpStream) {
        reset(socket, CANCEL).await;
    }

    /// Sends a PING and waits for its ACK. The server processes frames in
    /// order, so every frame written before the PING has been received.
    pub async fn barrier(socket: &mut TcpStream) {
        let payload = *b"barrier!";
        write_frame(socket, PING, 0, &payload).await;
        loop {
            let (kind, flags, body) = read_frame(socket).await;
            if kind == PING && flags & ACK != 0 && body == payload {
                return;
            }
        }
    }

    pub async fn trailers(socket: &mut TcpStream) {
        // HPACK: literal header `x-probe: ok` without indexing.
        let block = b"\x00\x07x-probe\x02ok";
        write_frame(socket, HEADERS, END_HEADERS | END_STREAM, block).await;
    }
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_cancel_mid_upload_is_error_not_eof_with_and_without_length() {
    for known_length in [false, true] {
        for cancel in [false, true] {
            let mut server = Server::start().await;
            let result = server.expect_upload().await;
            let length = known_length.then_some(if cancel { 100 } else { PREFIX.len() });
            let mut socket = h2_frames::open_upload(&server, "/upload", length).await;
            h2_frames::data(&mut socket, PREFIX, false).await;
            timeout(DEADLINE, server.control.prefix_read.notified()).await.unwrap();
            if cancel {
                h2_frames::cancel(&mut socket).await;
                let error = upload_result(result).await.expect_err("canceled upload ended cleanly");
                assert!(format!("{error:?}").contains("CANCEL"), "{error:?}");
            } else {
                h2_frames::data(&mut socket, &[], true).await;
                assert_eq!(upload_result(result).await.unwrap(), PREFIX);
            }

            drop(socket);
            server.stop().await;
        }
    }
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_no_error_reset_before_content_length_is_error_not_eof() {
    let mut server = Server::start().await;
    let result = server.expect_upload().await;
    let mut socket = h2_frames::open_upload(&server, "/upload", Some(100)).await;
    h2_frames::data(&mut socket, PREFIX, false).await;
    timeout(DEADLINE, server.control.prefix_read.notified()).await.unwrap();
    h2_frames::reset(&mut socket, h2_frames::NO_ERROR).await;
    let error = upload_result(result).await.expect_err("truncated upload ended cleanly");
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof, "{error:?}");
    drop(socket);
    server.stop().await;
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_upload_with_trailers_completes_cleanly() {
    for known_length in [false, true] {
        let mut server = Server::start().await;
        let result = server.expect_upload().await;
        let length = known_length.then_some(PREFIX.len());
        let mut socket = h2_frames::open_upload(&server, "/upload", length).await;
        h2_frames::data(&mut socket, PREFIX, false).await;
        timeout(DEADLINE, server.control.prefix_read.notified()).await.unwrap();
        h2_frames::trailers(&mut socket).await;
        assert_eq!(upload_result(result).await.unwrap(), PREFIX);
        drop(socket);
        server.stop().await;
    }
}

// h2 0.4.20 keeps a stream that received `END_STREAM` as cleanly closed even
// after a later `RST_STREAM(CANCEL)` (`Cause::ErrorAfterEndStream`), so the
// gated read returns the full body. Intentionally failing evidence only:
// stricter post-completion semantics are deferred until a need is shown.
#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_cancel_after_end_stream_while_read_gated_is_error_not_eof() {
    let mut outcomes = vec![];
    for known_length in [false, true] {
        let mut server = Server::start().await;
        let result = server.expect_upload().await;
        let length = known_length.then_some(PREFIX.len());
        let mut socket = h2_frames::open_upload(&server, "/gated-upload", length).await;
        h2_frames::data(&mut socket, PREFIX, true).await;
        h2_frames::barrier(&mut socket).await;
        timeout(DEADLINE, server.control.entered.notified()).await.unwrap();
        h2_frames::cancel(&mut socket).await;
        h2_frames::barrier(&mut socket).await;
        server.control.release.notify_one();
        outcomes.push((known_length, upload_result(result).await));
        drop(socket);
        server.stop().await;
    }

    let clean = outcomes.iter().filter(|(_, r)| r.is_ok()).collect::<Vec<_>>();
    assert!(clean.is_empty(), "canceled upload ended cleanly: {clean:?}");
}
