use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::{Bytes, Frame, Incoming};
use hyper::Request;
use http_body_util::{BodyExt, StreamBody, combinators::UnsyncBoxBody};
#[cfg(feature = "http2")]
use http_body_util::Empty;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
#[cfg(feature = "http2")]
use hyper_util::rt::TokioIo;

type Body = UnsyncBoxBody<Bytes, io::Error>;

#[cfg(feature = "http2")]
fn empty_body() -> Body {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed_unsync()
}

fn stream_body<S>(stream: S) -> Body
where S: rocket::futures::Stream<Item = io::Result<Bytes>> + Send + 'static {
    StreamBody::new(stream.map(|item| item.map(Frame::data))).boxed_unsync()
}

async fn to_bytes(response: hyper::Response<Incoming>) -> Result<Bytes, hyper::Error> {
    response.into_body().collect().await.map(|body| body.to_bytes())
}

async fn data(body: &mut Incoming) -> Option<Result<Bytes, hyper::Error>> {
    loop {
        match body.frame().await? {
            Ok(frame) => if let Ok(bytes) = frame.into_data() { return Some(Ok(bytes)); },
            Err(error) => return Some(Err(error)),
        }
    }
}
use rocket::fairing::AdHoc;
use rocket::futures::{StreamExt, stream};
use rocket::http::Status;
use rocket::tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
#[cfg(feature = "http2")]
use rocket::tokio::io::AsyncWriteExt;
use rocket::tokio::sync::{Notify, Mutex, oneshot};
use rocket::tokio::task::JoinHandle;
use rocket::tokio::time::timeout;
use rocket::{Config, Data, Ignite, Response, Rocket, Shutdown, State};

const BODY_LEN: usize = 64 * 1024;
const UPLOAD_PREFIX: &[u8] = b"fourteen-bytes!";
const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Control {
    release: Arc<Notify>,
    dropped: Arc<Notify>,
    entered: Arc<Notify>,
    upload_prefix: Arc<Notify>,
    upload_result: Arc<Mutex<Option<oneshot::Sender<io::Result<usize>>>>>,
    upload_entered: Arc<Notify>,
    upload_release: Arc<Notify>,
}

struct Reader {
    remaining: usize,
    fail: bool,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    dropped: Arc<Notify>,
    entered: Arc<Notify>,
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining > 0 {
            let n = self.remaining.min(buf.remaining()).min(4096);
            buf.put_slice(&[b'x'; 4096][..n]);
            self.remaining -= n;
            return Poll::Ready(Ok(()));
        }

        if let Some(wait) = self.wait.as_mut() {
            if wait.as_mut().poll(cx).is_pending() {
                self.entered.notify_one();
                return Poll::Pending;
            }
            self.wait = None;
        }

        if self.fail {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "stream failed")))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.dropped.notify_one();
    }
}

struct WireResponse(Response<'static>);

impl<'r> rocket::response::Responder<'r, 'static> for WireResponse {
    fn respond_to(self, _: &'r rocket::Request<'_>) -> rocket::response::Result<'static> {
        Ok(self.0)
    }
}

#[rocket::get("/stream/<mode>")]
fn response(mode: &str, control: &State<Control>) -> WireResponse {
    let fail = matches!(mode, "unknown" | "known" | "before");
    let wait = if fail || mode == "drain" {
        let release = control.release.clone();
        Some(Box::pin(async move { release.notified().await }) as _)
    } else {
        None
    };

    let reader = Reader {
        remaining: if matches!(mode, "empty" | "before") { 0 } else { BODY_LEN },
        fail,
        wait,
        dropped: control.dropped.clone(),
        entered: control.entered.clone(),
    };

    let mut response = Response::build();
    response.status(Status::Accepted).raw_header("X-Stream-Test", "preserved");
    if mode == "known" {
        response.raw_header("Content-Length", (BODY_LEN * 2).to_string());
    }

    WireResponse(response.streamed_body(reader).finalize())
}

#[rocket::post("/early", data = "<_data>")]
fn early(_data: Data<'_>) -> (Status, &'static str) {
    (Status::UnprocessableEntity, "early rejection")
}

#[rocket::post("/upload", data = "<data>")]
async fn upload(data: Data<'_>, control: &State<Control>) -> &'static str {
    use rocket::data::ToByteUnit;

    let mut reader = data.open(1.mebibytes());
    let mut prefix = [0; UPLOAD_PREFIX.len()];
    reader.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix[..], UPLOAD_PREFIX);
    control.upload_prefix.notify_one();
    let result = reader.read_to_end(&mut Vec::new()).await;
    control.upload_result.lock().await.take().unwrap().send(result).unwrap();
    "upload complete"
}

#[rocket::post("/gated-upload", data = "<data>")]
async fn gated_upload(data: Data<'_>, control: &State<Control>) -> &'static str {
    control.upload_entered.notify_one();
    control.upload_release.notified().await;
    upload(data, control).await
}

struct Server {
    address: String,
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
            .manage(Control {
                release: control.release.clone(),
                dropped: control.dropped.clone(),
                entered: control.entered.clone(),
                upload_prefix: control.upload_prefix.clone(),
                upload_result: control.upload_result.clone(),
                upload_entered: control.upload_entered.clone(),
                upload_release: control.upload_release.clone(),
            })
            .mount("/", rocket::routes![response, early, upload, gated_upload])
            .attach(AdHoc::on_liftoff("Bound Port", move |rocket| Box::pin(async move {
                tx.send(rocket.config().port).unwrap();
            })))
            .ignite().await.unwrap();

        let shutdown = rocket.shutdown();
        let task = rocket::tokio::spawn(rocket.launch());
        let port = timeout(DEADLINE, rx).await.unwrap().unwrap();
        Self { address: format!("http://127.0.0.1:{}", port), shutdown, task, control }
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

fn protocols() -> Vec<bool> {
    #[cfg(feature = "http2")]
    { vec![false, true] }
    #[cfg(not(feature = "http2"))]
    { vec![false] }
}

fn client(http2: bool) -> Client<HttpConnector, Body> {
    let mut builder = Client::builder(TokioExecutor::new());
    #[cfg(feature = "http2")]
    builder.http2_only(http2);
    #[cfg(not(feature = "http2"))]
    assert!(!http2);
    builder.pool_max_idle_per_host(0).build_http()
}

async fn get(server: &Server, client: &Client<HttpConnector, Body>, mode: &str) -> hyper::Response<Incoming> {
    let uri = format!("{}/stream/{}", server.address, mode).parse().unwrap();
    let response = timeout(DEADLINE, client.get(uri)).await.unwrap().unwrap();
    assert_eq!(response.status(), Status::Accepted.code);
    assert_eq!(response.headers()["X-Stream-Test"], "preserved");
    response
}

async fn failure(mode: &str) {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let client = client(http2);
        let mut response = get(&server, &client, mode).await;
        if mode != "before" {
            let first = timeout(DEADLINE, data(response.body_mut())).await;
            assert!(!first.unwrap().unwrap().unwrap().is_empty());
        }

        server.control.release.notify_one();
        let result = timeout(DEADLINE, to_bytes(response)).await.unwrap();
        assert!(result.is_err(), "{} on HTTP/2={} ended successfully", mode, http2);
        server.stop().await;
    }
}

#[rocket::async_test]
async fn unknown_length_failure_aborts_after_headers() {
    failure("unknown").await;
}

#[rocket::async_test]
async fn known_length_failure_remains_incomplete() {
    failure("known").await;
}

#[rocket::async_test]
async fn failure_before_first_body_chunk_aborts_after_headers() {
    failure("before").await;
}

#[rocket::async_test]
async fn ordinary_and_empty_streams_complete() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let client = client(http2);
        for mode in ["ordinary", "empty"] {
            let response = get(&server, &client, mode).await;
            let body = timeout(DEADLINE, to_bytes(response)).await.unwrap().unwrap();
            let expected = if mode == "empty" { 0 } else { BODY_LEN };
            assert_eq!(body.as_ref(), vec![b'x'; expected]);
        }

        server.stop().await;
    }
}

#[rocket::async_test]
async fn early_response_does_not_wait_for_upload_eof() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let body = stream::iter(vec![Ok::<_, io::Error>(Bytes::from_static(b"fourteen-bytes!"))])
            .chain(stream::pending());
        let request = Request::post(format!("{}/early", server.address))
            .header("Content-Length", "1000000")
            .body(stream_body(body)).unwrap();
        let response = timeout(DEADLINE, client(http2).request(request)).await.unwrap().unwrap();
        assert_eq!(response.status(), Status::UnprocessableEntity.code);
        let body = timeout(DEADLINE, to_bytes(response)).await.unwrap().unwrap();
        assert_eq!(body, "early rejection");
        server.stop().await;
    }
}

#[rocket::async_test]
async fn client_cancellation_releases_reader_without_stopping_server() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let client = client(http2);
        let mut response = get(&server, &client, "unknown").await;
        timeout(DEADLINE, data(response.body_mut())).await.unwrap().unwrap().unwrap();
        drop(response);
        server.control.release.notify_one();
        timeout(DEADLINE, server.control.dropped.notified()).await.unwrap();
        let response = get(&server, &client, "empty").await;
        assert!(to_bytes(response).await.unwrap().is_empty());
        server.stop().await;
    }
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_cancellation_drops_stalled_reader_without_releasing_it() {
    let mut server = Server::start().await;
    let socket = rocket::tokio::net::TcpStream::connect(
        server.address.trim_start_matches("http://")).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(socket)).await.unwrap();
    let driver = rocket::tokio::spawn(connection);
    let request = Request::get(format!("{}/stream/unknown", server.address))
        .body(empty_body()).unwrap();
    let mut response = timeout(DEADLINE, sender.send_request(request)).await.unwrap().unwrap();
    let mut observed = Vec::new();
    while observed.len() < BODY_LEN {
        let bytes = timeout(DEADLINE, data(response.body_mut()))
            .await.unwrap().unwrap().unwrap();
        observed.extend_from_slice(&bytes);
    }
    assert_eq!(observed, vec![b'x'; BODY_LEN]);
    timeout(DEADLINE, server.control.entered.notified()).await.unwrap();
    drop(response);
    timeout(DEADLINE, server.control.dropped.notified()).await.unwrap();

    let request = Request::get(format!("{}/stream/empty", server.address))
        .body(empty_body()).unwrap();
    let response = timeout(DEADLINE, sender.send_request(request)).await.unwrap().unwrap();
    assert_eq!(response.status(), Status::Accepted.code);
    assert!(timeout(DEADLINE, to_bytes(response)).await.unwrap().unwrap().is_empty());
    assert!(!driver.is_finished());
    drop(sender);
    server.stop().await;
    timeout(DEADLINE, driver).await.unwrap().unwrap().unwrap();
}

#[cfg(feature = "http2")]
async fn write_h2_frame(
    socket: &mut rocket::tokio::net::TcpStream,
    kind: u8,
    flags: u8,
    stream: u32,
    payload: &[u8],
) {
    let len = payload.len();
    let id = stream.to_be_bytes();
    let header = [
        (len >> 16) as u8, (len >> 8) as u8, len as u8, kind, flags,
        id[0], id[1], id[2], id[3],
    ];
    timeout(DEADLINE, socket.write_all(&header)).await.unwrap().unwrap();
    timeout(DEADLINE, socket.write_all(payload)).await.unwrap().unwrap();
}

#[cfg(feature = "http2")]
async fn open_h2_upload(server: &Server, path: &str, length: Option<usize>)
    -> rocket::tokio::net::TcpStream
{
    let mut socket = rocket::tokio::net::TcpStream::connect(
        server.address.trim_start_matches("http://")).await.unwrap();
    socket.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.unwrap();
    write_h2_frame(&mut socket, 4, 0, 0, &[]).await;
    let mut header = [0; 9];
    timeout(DEADLINE, socket.read_exact(&mut header)).await.unwrap().unwrap();
    assert_eq!(header[3], 4);
    let len = ((header[0] as usize) << 16) | ((header[1] as usize) << 8)
        | header[2] as usize;
    timeout(DEADLINE, socket.read_exact(&mut vec![0; len])).await.unwrap().unwrap();
    write_h2_frame(&mut socket, 4, 1, 0, &[]).await;

    // HPACK: POST, http, path, localhost; no dynamic table or Huffman coding.
    let mut headers = vec![0x83, 0x86, 0x04, path.len() as u8];
    headers.extend_from_slice(path.as_bytes());
    headers.extend_from_slice(&[0x01, 9]);
    headers.extend_from_slice(b"localhost");
    if let Some(length) = length {
        let length = length.to_string();
        headers.extend_from_slice(&[0, 14]);
        headers.extend_from_slice(b"content-length");
        headers.push(length.len() as u8);
        headers.extend_from_slice(length.as_bytes());
    }
    write_h2_frame(&mut socket, 1, 4, 1, &headers).await;
    socket
}

#[cfg(feature = "http2")]
async fn h2_ping_barrier(socket: &mut rocket::tokio::net::TcpStream, payload: &[u8; 8]) {
    write_h2_frame(socket, 6, 0, 0, payload).await;
    loop {
        let mut header = [0; 9];
        timeout(DEADLINE, socket.read_exact(&mut header)).await.unwrap().unwrap();
        let len = ((header[0] as usize) << 16) | ((header[1] as usize) << 8)
            | header[2] as usize;
        let mut body = vec![0; len];
        timeout(DEADLINE, socket.read_exact(&mut body)).await.unwrap().unwrap();
        if header[3] == 6 && header[4] == 1 && body == payload {
            return;
        }
    }
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_end_stream_then_cancel_preserves_completed_upload() {
    for known_length in [false, true] {
        let mut server = Server::start().await;
        let (completed, result) = oneshot::channel();
        *server.control.upload_result.lock().await = Some(completed);
        let length = known_length.then_some(UPLOAD_PREFIX.len());
        let mut socket = open_h2_upload(&server, "/gated-upload", length).await;
        write_h2_frame(&mut socket, 0, 1, 1, UPLOAD_PREFIX).await;
        h2_ping_barrier(&mut socket, b"data-ack").await;
        timeout(DEADLINE, server.control.upload_entered.notified()).await.unwrap();
        write_h2_frame(&mut socket, 3, 0, 1, &8u32.to_be_bytes()).await;
        h2_ping_barrier(&mut socket, b"rst--ack").await;
        server.control.upload_release.notify_one();
        // h2's recv_reset_preserves_received_end_stream preserves EOF after a reset.
        assert_eq!(timeout(DEADLINE, result).await.unwrap().unwrap().unwrap(), 0);
        let response = get(&server, &client(true), "empty").await;
        assert!(timeout(DEADLINE, to_bytes(response)).await.unwrap().unwrap().is_empty());
        drop(socket);
        server.stop().await;
    }
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_upload_with_trailers_completes_cleanly() {
    for known_length in [false, true] {
        let mut server = Server::start().await;
        let (completed, result) = oneshot::channel();
        *server.control.upload_result.lock().await = Some(completed);
        let length = known_length.then_some(UPLOAD_PREFIX.len());
        let mut socket = open_h2_upload(&server, "/upload", length).await;
        write_h2_frame(&mut socket, 0, 0, 1, UPLOAD_PREFIX).await;
        timeout(DEADLINE, server.control.upload_prefix.notified()).await.unwrap();
        let trailers = b"\x00\x07x-probe\x02ok";
        write_h2_frame(&mut socket, 1, 5, 1, trailers).await;
        assert_eq!(timeout(DEADLINE, result).await.unwrap().unwrap().unwrap(), 0);
        drop(socket);
        server.stop().await;
    }
}

#[cfg(feature = "http2")]
#[rocket::async_test]
async fn h2_upload_cancellation_is_error_not_eof_with_and_without_length() {
    for known_length in [false, true] {
        for cancel in [false, true] {
            let mut server = Server::start().await;
            let (completed, result) = oneshot::channel();
            *server.control.upload_result.lock().await = Some(completed);
            let mut socket = rocket::tokio::net::TcpStream::connect(
                server.address.trim_start_matches("http://")).await.unwrap();
            socket.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.unwrap();
            write_h2_frame(&mut socket, 4, 0, 0, &[]).await;
            let mut header = [0; 9];
            timeout(DEADLINE, socket.read_exact(&mut header)).await.unwrap().unwrap();
            assert_eq!(header[3], 4);
            let len = ((header[0] as usize) << 16) | ((header[1] as usize) << 8)
                | header[2] as usize;
            timeout(DEADLINE, socket.read_exact(&mut vec![0; len])).await.unwrap().unwrap();
            write_h2_frame(&mut socket, 4, 1, 0, &[]).await;

            // HPACK: POST, http, /upload, localhost; no dynamic table or Huffman coding.
            let mut headers = vec![0x83, 0x86, 0x04, 7];
            headers.extend_from_slice(b"/upload");
            headers.extend_from_slice(&[0x01, 9]);
            headers.extend_from_slice(b"localhost");
            if known_length {
                let length = if cancel { 100 } else { UPLOAD_PREFIX.len() };
                let length = length.to_string();
                headers.extend_from_slice(&[0, 14]);
                headers.extend_from_slice(b"content-length");
                headers.push(length.len() as u8);
                headers.extend_from_slice(length.as_bytes());
            }
            write_h2_frame(&mut socket, 1, 4, 1, &headers).await;
            write_h2_frame(&mut socket, 0, 0, 1, UPLOAD_PREFIX).await;
            timeout(DEADLINE, server.control.upload_prefix.notified()).await.unwrap();
            if cancel {
                write_h2_frame(&mut socket, 3, 0, 1, &8u32.to_be_bytes()).await;
            } else {
                write_h2_frame(&mut socket, 0, 1, 1, &[]).await;
            }
            let result = timeout(DEADLINE, result).await.unwrap().unwrap();
            if cancel {
                let error = result.expect_err("canceled upload ended cleanly");
                assert!(format!("{:?}", error).contains("CANCEL"), "{:?}", error);
            } else {
                assert_eq!(result.unwrap(), 0);
            }
            drop(socket);
            server.stop().await;
        }
    }
}

#[rocket::async_test]
async fn complete_uploads_still_end_cleanly() {
    for http2 in protocols() {
        for known_length in [false, true] {
            let mut server = Server::start().await;
            let (completed, result) = oneshot::channel();
            *server.control.upload_result.lock().await = Some(completed);
            let body = stream_body(stream::iter([
                Ok::<_, io::Error>(Bytes::from_static(UPLOAD_PREFIX))]));
            let mut request = Request::post(format!("{}/upload", server.address));
            if known_length {
                request = request.header("Content-Length", UPLOAD_PREFIX.len().to_string());
            }
            let response = timeout(DEADLINE, client(http2).request(request.body(body).unwrap()))
                .await.unwrap().unwrap();
            assert_eq!(to_bytes(response).await.unwrap(), "upload complete");
            assert_eq!(timeout(DEADLINE, result).await.unwrap().unwrap().unwrap(), 0);
            server.stop().await;
        }
    }
}

#[rocket::async_test]
async fn failure_during_shutdown_does_not_become_successful_eof() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let mut response = get(&server, &client(http2), "unknown").await;
        timeout(DEADLINE, data(response.body_mut())).await.unwrap().unwrap().unwrap();
        server.shutdown.clone().notify();
        server.control.release.notify_one();
        assert!(timeout(DEADLINE, to_bytes(response)).await.unwrap().is_err());
        server.stop().await;
    }
}

#[rocket::async_test]
async fn accepted_stream_can_complete_during_shutdown() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let response = get(&server, &client(http2), "drain").await;
        server.shutdown.clone().notify();
        server.control.release.notify_one();
        let body = timeout(DEADLINE, to_bytes(response)).await.unwrap().unwrap();
        assert_eq!(body.as_ref(), vec![b'x'; BODY_LEN]);
        server.stop().await;
    }
}
