#[macro_use] extern crate rocket;

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rocket::fairing::AdHoc;
use rocket::response::{Response, Responder};
use rocket::tokio::io::{AsyncRead, ReadBuf};
use rocket::tokio::net::TcpStream;
use rocket::tokio::sync::{mpsc, oneshot, Notify};
use rocket::tokio::task::JoinHandle;
use rocket::tokio::time::timeout;
use rocket::{Config, Ignite, Rocket, Shutdown, State};
use tokio_stream::wrappers::ReceiverStream;

const PREFIX_LEN: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(10);
type RequestBody = StreamBody<ReceiverStream<io::Result<Frame<Bytes>>>>;

#[derive(Default)]
struct Control {
    entered: Notify,
    dropped: Notify,
    release: Notify,
}

struct Reader {
    remaining: usize,
    fail: bool,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    control: Arc<Control>,
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
                self.control.entered.notify_one();
                return Poll::Pending;
            }
            self.wait = None;
        }

        if self.fail {
            Poll::Ready(Err(io::Error::other("response reader failed")))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.control.dropped.notify_one();
    }
}

struct WireResponse(Response<'static>);

impl<'r> Responder<'r, 'static> for WireResponse {
    fn respond_to(self, _: &'r rocket::Request<'_>) -> rocket::response::Result<'static> {
        Ok(self.0)
    }
}

#[get("/stream/<mode>/<known>")]
fn response(mode: &str, known: bool, control: &State<Arc<Control>>) -> WireResponse {
    let wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = match mode {
        "stall" => Some(Box::pin(std::future::pending())),
        "error" => {
            let control = control.inner().clone();
            Some(Box::pin(async move { control.release.notified().await }))
        },
        _ => None,
    };
    let reader = Reader {
        remaining: if mode == "empty" { 0 } else { PREFIX_LEN },
        fail: mode == "error",
        wait,
        control: control.inner().clone(),
    };

    let mut response = Response::build();
    response.streamed_body(reader);
    if known {
        let length = match mode {
            "empty" => 0,
            "finite" => PREFIX_LEN,
            _ => PREFIX_LEN * 2,
        };
        response.raw_header("Content-Length", length.to_string());
    }
    WireResponse(response.finalize())
}

#[post("/early")]
fn early() -> &'static str {
    "early response"
}

struct Server {
    port: u16,
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
        config.shutdown.ctrlc = false;
        #[cfg(unix)]
        config.shutdown.signals.clear();
        config.shutdown.grace = 1;
        config.shutdown.mercy = 1;
        let rocket = rocket::custom(config).manage(control.clone())
            .mount("/", routes![response, early])
            .attach(AdHoc::on_liftoff("Bound Port", |rocket| Box::pin(async move {
                tx.send(rocket.config().port).unwrap();
            })))
            .ignite().await.unwrap();
        let shutdown = rocket.shutdown();
        let task = rocket::tokio::spawn(rocket.launch());
        let port = timeout(DEADLINE, rx).await.unwrap().unwrap();
        Self { port, shutdown, task, control }
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

enum Client {
    Http1(hyper::client::conn::http1::SendRequest<RequestBody>),
    Http2(hyper::client::conn::http2::SendRequest<RequestBody>),
}

impl Client {
    async fn connect(port: u16, http2: bool)
        -> (Self, JoinHandle<Result<(), hyper::Error>>)
    {
        let io = TokioIo::new(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
        if http2 {
            let (client, connection) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
                .await.unwrap();
            (Self::Http2(client), rocket::tokio::spawn(connection))
        } else {
            let (client, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
            (Self::Http1(client), rocket::tokio::spawn(connection))
        }
    }

    async fn send(&mut self, method: &str, path: &str, body: RequestBody)
        -> hyper::Response<Incoming>
    {
        let request = hyper::Request::builder().method(method)
            .uri(format!("http://localhost{path}")).body(body).unwrap();
        let response = match self {
            Self::Http1(client) => client.send_request(request).await.unwrap(),
            Self::Http2(client) => client.send_request(request).await.unwrap(),
        };
        assert_eq!(response.status(), 200);
        response
    }

    async fn get(&mut self, mode: &str, known: bool) -> hyper::Response<Incoming> {
        let (sender, receiver) = mpsc::channel(1);
        drop(sender);
        self.send("GET", &format!("/stream/{mode}/{known}"),
            StreamBody::new(ReceiverStream::new(receiver))).await
    }
}

async fn read_prefix(response: &mut hyper::Response<Incoming>) {
    let mut prefix = Vec::new();
    while prefix.len() < PREFIX_LEN {
        let frame = response.frame().await.unwrap().unwrap();
        prefix.extend_from_slice(frame.data_ref().unwrap());
    }
    assert_eq!(prefix, vec![b'x'; PREFIX_LEN]);
}

fn protocols() -> Vec<bool> {
    #[cfg(feature = "http2")]
    { vec![false, true] }
    #[cfg(not(feature = "http2"))]
    { vec![false] }
}

#[rocket::async_test]
async fn cancellation_drops_stalled_reader_without_releasing_it() {
    for http2 in protocols() {
        for known in [false, true] {
            timeout(DEADLINE, async {
                let mut server = Server::start().await;
                let (mut client, connection) = Client::connect(server.port, http2).await;
                let mut response = client.get("stall", known).await;
                assert_eq!(response.headers().contains_key("Content-Length"), known);
                read_prefix(&mut response).await;
                server.control.entered.notified().await;
                drop(response);
                if !http2 {
                    connection.abort();
                }
                server.control.dropped.notified().await;

                if http2 {
                    let response = client.get("empty", false).await;
                    assert!(response.collect().await.unwrap().to_bytes().is_empty());
                    assert!(!connection.is_finished());
                }
                drop(client);
                connection.abort();
                server.stop().await;
            }).await.unwrap();
        }
    }
}

#[rocket::async_test]
async fn source_errors_remain_errors() {
    for http2 in protocols() {
        for known in [false, true] {
            timeout(DEADLINE, async {
                let mut server = Server::start().await;
                let (mut client, connection) = Client::connect(server.port, http2).await;
                let mut response = client.get("error", known).await;
                read_prefix(&mut response).await;
                server.control.entered.notified().await;
                server.control.release.notify_one();
                assert!(response.collect().await.is_err());
                server.control.dropped.notified().await;
                drop(client);
                connection.abort();
                server.stop().await;
            }).await.unwrap();
        }
    }
}

#[rocket::async_test]
async fn ordinary_empty_and_early_responses_complete() {
    for http2 in protocols() {
        timeout(DEADLINE, async {
            let mut server = Server::start().await;
            let (mut client, connection) = Client::connect(server.port, http2).await;
            for known in [false, true] {
                for mode in ["finite", "empty"] {
                    let response = client.get(mode, known).await;
                    let bytes = response.collect().await.unwrap().to_bytes();
                    let length = if mode == "empty" { 0 } else { PREFIX_LEN };
                    assert_eq!(bytes, vec![b'x'; length]);
                }
            }
            let (sender, receiver) = mpsc::channel(1);
            sender.try_send(Ok(Frame::data(Bytes::from_static(&[b'x'; 64])))).unwrap();
            let body = StreamBody::new(ReceiverStream::new(receiver));
            let response = client.send("POST", "/early", body).await;
            assert_eq!(response.collect().await.unwrap().to_bytes(), "early response");
            drop(sender);
            drop(client);
            connection.abort();
            server.stop().await;
        }).await.unwrap();
    }
}
