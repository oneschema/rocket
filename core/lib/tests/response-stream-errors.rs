use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use hyper::body::{Bytes, HttpBody};
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request};
use rocket::fairing::AdHoc;
use rocket::futures::{StreamExt, stream};
use rocket::http::Status;
use rocket::tokio::io::{AsyncRead, ReadBuf};
use rocket::tokio::sync::{Notify, oneshot};
use rocket::tokio::task::JoinHandle;
use rocket::tokio::time::timeout;
use rocket::{Config, Data, Ignite, Response, Rocket, Shutdown, State};

const BODY_LEN: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Control {
    release: Arc<Notify>,
    dropped: Arc<Notify>,
}

struct Reader {
    remaining: usize,
    fail: bool,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    dropped: Arc<Notify>,
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
            ready!(wait.as_mut().poll(cx));
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
            })
            .mount("/", rocket::routes![response, early])
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

fn client(http2: bool) -> Client<HttpConnector> {
    let mut builder = Client::builder();
    #[cfg(feature = "http2")]
    builder.http2_only(http2);
    #[cfg(not(feature = "http2"))]
    assert!(!http2);
    builder.pool_max_idle_per_host(0).build_http()
}

async fn get(server: &Server, client: &Client<HttpConnector>, mode: &str) -> hyper::Response<Body> {
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
            let first = timeout(DEADLINE, response.body_mut().data()).await;
            assert!(!first.unwrap().unwrap().unwrap().is_empty());
        }

        server.control.release.notify_one();
        let result = timeout(DEADLINE, hyper::body::to_bytes(response)).await.unwrap();
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
            let body = timeout(DEADLINE, hyper::body::to_bytes(response)).await.unwrap().unwrap();
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
            .body(Body::wrap_stream(body)).unwrap();
        let response = timeout(DEADLINE, client(http2).request(request)).await.unwrap().unwrap();
        assert_eq!(response.status(), Status::UnprocessableEntity.code);
        let body = timeout(DEADLINE, hyper::body::to_bytes(response)).await.unwrap().unwrap();
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
        timeout(DEADLINE, response.body_mut().data()).await.unwrap().unwrap().unwrap();
        drop(response);
        server.control.release.notify_one();
        timeout(DEADLINE, server.control.dropped.notified()).await.unwrap();
        let response = get(&server, &client, "empty").await;
        assert!(hyper::body::to_bytes(response).await.unwrap().is_empty());
        server.stop().await;
    }
}

#[rocket::async_test]
async fn failure_during_shutdown_does_not_become_successful_eof() {
    for http2 in protocols() {
        let mut server = Server::start().await;
        let mut response = get(&server, &client(http2), "unknown").await;
        timeout(DEADLINE, response.body_mut().data()).await.unwrap().unwrap().unwrap();
        server.shutdown.clone().notify();
        server.control.release.notify_one();
        assert!(timeout(DEADLINE, hyper::body::to_bytes(response)).await.unwrap().is_err());
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
        let body = timeout(DEADLINE, hyper::body::to_bytes(response)).await.unwrap().unwrap();
        assert_eq!(body.as_ref(), vec![b'x'; BODY_LEN]);
        server.stop().await;
    }
}
