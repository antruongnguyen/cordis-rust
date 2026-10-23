//! Authenticated loopback GitHub event journal with explicit owned shutdown.
#[path = "github_activity/common.rs"]
mod common;

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequest, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use common::{drain, invalid, validate_repository};
use cordis_core::{BoxError, Context, Disposer, FiberHandle, Plugin, Runtime, ServiceKey};
use hmac::{Hmac, Mac};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use serde_json::{Value, json};
use sha2::Sha256;
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    io,
    net::SocketAddr,
    path::PathBuf,
    rc::Rc,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
};

pub(crate) const BODY_LIMIT: usize = 256 * 1024;
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) repository: String,
    pub(crate) bind: SocketAddr,
    pub(crate) secret: Vec<u8>,
}

impl Config {
    fn validate(&self) -> Result<Self, BoxError> {
        validate_repository(&self.repository)?;
        if !self.bind.ip().is_loopback() {
            return Err(invalid("webhook must bind a loopback address").into());
        }
        if self.secret.len() < 32 {
            return Err(invalid("webhook secret must contain at least 32 bytes").into());
        }
        Ok(self.clone())
    }
}

#[derive(Clone)]
pub(crate) struct Journal {
    file: Rc<RefCell<Option<tokio::fs::File>>>,
    poisoned: Rc<Cell<bool>>,
}

impl Journal {
    async fn append(&self, record: &Value) -> Result<(), io::Error> {
        if self.poisoned.get() {
            return Err(io::Error::other(
                "journal failed earlier; reconfigure before accepting more events",
            ));
        }
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        let mut file = self
            .file
            .borrow_mut()
            .take()
            .ok_or_else(|| io::Error::other("journal is closed or busy"))?;
        let result = async {
            file.write_all(&bytes).await?;
            file.flush().await
        }
        .await;
        self.file.borrow_mut().replace(file);
        if result.is_err() {
            self.poisoned.set(true);
        }
        result
    }

    async fn close(&self) -> Result<(), io::Error> {
        let mut file = self
            .file
            .borrow_mut()
            .take()
            .ok_or_else(|| io::Error::other("journal already closed"))?;
        file.flush().await
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self) -> bool {
        self.file.borrow().is_some()
    }
}

struct Delivery {
    record: Value,
    reply: oneshot::Sender<Result<(), io::Error>>,
}

struct Endpoint {
    config: Config,
    sender: mpsc::Sender<Delivery>,
}

pub(crate) fn verified(secret: &[u8], signature: &str, body: &[u8]) -> bool {
    let Some(hex) = signature.strip_prefix("sha256=") else {
        return false;
    };
    if hex.len() != 64 || !hex.bytes().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let Ok(value) = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16) else {
            return false;
        };
        *byte = value;
    }
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&digest).is_ok()
}

fn header(headers: &HeaderMap, name: &str) -> Result<String, StatusCode> {
    if headers.get_all(name).iter().count() != 1 {
        return Err(StatusCode::BAD_REQUEST);
    }
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or(StatusCode::BAD_REQUEST)
}

async fn process(endpoint: Arc<Endpoint>, request: Request) -> Response {
    let headers = request.headers();
    let parsed = (|| {
        let signature = header(headers, "x-hub-signature-256")?;
        let event = header(headers, "x-github-event")?;
        let delivery = header(headers, "x-github-delivery")?;
        let content_type = header(headers, "content-type")?;
        if content_type.split(';').next().map(str::trim) != Some("application/json") {
            return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        if !["issues", "pull_request"].contains(&event.as_str()) {
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        if delivery.len() != 36
            || delivery.bytes().enumerate().any(|(i, c)| {
                if [8, 13, 18, 23].contains(&i) {
                    c != b'-'
                } else {
                    !c.is_ascii_hexdigit()
                }
            })
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok((signature, event, delivery))
    })();
    let (signature, event, delivery) = match parsed {
        Ok(values) => values,
        Err(status) => return (status, "invalid webhook headers").into_response(),
    };
    let body = match Bytes::from_request(request, &()).await {
        Ok(body) => body,
        Err(error) => {
            return (error.status(), "unreadable or oversized webhook body").into_response();
        }
    };
    if !verified(&endpoint.config.secret, &signature, &body) {
        return (StatusCode::UNAUTHORIZED, "invalid webhook signature").into_response();
    }
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid JSON payload").into_response(),
    };
    let target = if event == "issues" {
        "issue"
    } else {
        "pull_request"
    };
    let object = &payload[target];
    let number = object["number"].as_u64().filter(|number| *number > 0);
    let path = if event == "issues" { "issues" } else { "pull" };
    let valid = payload["repository"]["full_name"].as_str() == Some(&endpoint.config.repository)
        && payload["action"]
            .as_str()
            .is_some_and(|action| !action.trim().is_empty())
        && object["title"]
            .as_str()
            .is_some_and(|title| !title.trim().is_empty())
        && number.is_some_and(|number| {
            object["html_url"].as_str()
                == Some(&format!(
                    "https://github.com/{}/{path}/{number}",
                    endpoint.config.repository
                ))
        });
    if !valid {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            "wrong repository or invalid event payload",
        )
            .into_response();
    }
    let received_at = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_secs(),
        Err(error) => {
            eprintln!("webhook clock failed: {error}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let record = json!({ "delivery": delivery, "event": event, "received_at": received_at, "payload": payload });
    let (reply, result) = oneshot::channel();
    if let Err(error) = endpoint.sender.try_send(Delivery { record, reply }) {
        eprintln!("webhook journal unavailable: {error}");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match result.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(_)) | Err(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "journal append failed").into_response()
        }
    }
}

async fn receive(State(endpoint): State<Arc<Endpoint>>, request: Request) -> Response {
    match tokio::time::timeout(REQUEST_TIMEOUT, process(endpoint, request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            "webhook request deadline exceeded",
        )
            .into_response(),
    }
}

fn connection_result(
    result: Result<Result<(), hyper::Error>, tokio::task::JoinError>,
) -> Result<(), BoxError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            eprintln!("webhook HTTP connection failed: {error}");
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn serve(
    listener: TcpListener,
    router: Router,
    shutdown: watch::Sender<bool>,
) -> Result<(), BoxError> {
    let mut stopped = shutdown.subscribe();
    if *stopped.borrow_and_update() {
        return Ok(());
    }
    let mut connections = tokio::task::JoinSet::new();
    let outcome = loop {
        tokio::select! {
            biased;
            _ = stopped.changed() => break Ok(()),
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = result {
                    if let Err(error) = connection_result(result) {
                        break Err(error);
                    }
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error.into()),
                };
                if connections.len() >= 64 {
                    eprintln!("webhook connection limit reached");
                    drop(stream);
                    continue;
                }
                let router = router.clone();
                let mut stopped = stopped.clone();
                connections.spawn_local(async move {
                    let connection = hyper::server::conn::http1::Builder::new()
                        .timer(TokioTimer::new())
                        .header_read_timeout(REQUEST_TIMEOUT)
                        .max_buf_size(32 * 1024)
                        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(router));
                    tokio::pin!(connection);
                    tokio::select! {
                        result = &mut connection => result,
                        _ = stopped.changed() => {
                            connection.as_mut().graceful_shutdown();
                            connection.await
                        }
                    }
                });
            }
        }
    };
    drop(listener);
    drop(router);
    shutdown.send_replace(true);
    let mut failure = outcome.err();
    while let Some(result) = connections.join_next().await {
        if let Err(error) = connection_result(result) {
            eprintln!("webhook connection task failed: {error}");
            failure.get_or_insert(error);
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) struct Webhook {
    pub(crate) journal: FiberHandle<PathBuf>,
    pub(crate) receiver: FiberHandle<Config>,
    pub(crate) address: watch::Receiver<Option<SocketAddr>>,
}

pub(crate) fn start(root: &Context, config: Config, output: PathBuf) -> Result<Webhook, BoxError> {
    config.validate()?;
    let key = ServiceKey::<Journal>::new("githubWebhookJournal");
    let journal = root.plugin(
        &Plugin::initialized("WebhookJournal", {
            let key = key.clone();
            move |ctx, output: PathBuf| {
                let key = key.clone();
                async move {
                    let file = tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(output)
                        .await?;
                    let journal = Journal {
                        file: Rc::new(RefCell::new(Some(file))),
                        poisoned: Rc::new(Cell::new(false)),
                    };
                    ctx.provide(&key, journal.clone())?;
                    Ok::<_, BoxError>(Disposer::new(move || async move { journal.close().await }))
                }
            }
        }),
        output,
    )?;
    let (address, observation) = watch::channel(None);
    let dependency = key.required();
    let receiver = root.plugin(
        &Plugin::validated(
            "GitHubWebhook",
            Config::validate,
            move |ctx, config: Config| {
                let key = key.clone();
                let address = address.clone();
                async move {
                    let journal = ctx
                        .get(&key)?
                        .ok_or_else(|| invalid("webhook journal unavailable"))?;
                    let listener = TcpListener::bind(config.bind).await?;
                    let bound = listener.local_addr()?;
                    let logger = ctx.logger(None);
                    ctx.effect(move || {
                        let (sender, mut pending) = mpsc::channel::<Delivery>(32);
                        let endpoint = Arc::new(Endpoint { config, sender });
                        let router = Router::new()
                            .route("/github", post(receive))
                            .layer(DefaultBodyLimit::max(BODY_LIMIT))
                            .with_state(endpoint);
                        let writer = tokio::task::spawn_local(async move {
                            while let Some(delivery) = pending.recv().await {
                                let result = journal.append(&delivery.record).await;
                                if let Err(error) = &result {
                                    eprintln!("webhook journal append failed: {error}");
                                    logger
                                        .error(format!("webhook journal append failed: {error}"))?;
                                }
                                if delivery.reply.send(result).is_err() {
                                    logger.warn(
                                        "webhook request ended before append acknowledgement",
                                    )?;
                                }
                            }
                            Ok::<_, BoxError>(())
                        });
                        let (stop, _) = watch::channel(false);
                        let shutdown = stop.clone();
                        let server = tokio::task::spawn_local(async move {
                            let result = serve(listener, router, shutdown).await;
                            if let Err(error) = &result {
                                eprintln!("webhook server failed: {error}");
                            }
                            result
                        });
                        address.send_replace(Some(bound));
                        Ok::<_, Infallible>(Disposer::new(move || async move {
                            address.send_replace(None);
                            stop.send_replace(true);
                            let (server, writer) = tokio::join!(server, writer);
                            server??;
                            writer??;
                            Ok::<_, BoxError>(())
                        }))
                    })?;
                    Ok::<_, BoxError>(())
                }
            },
        )
        .injecting([dependency]),
        config,
    )?;
    Ok(Webhook {
        journal,
        receiver,
        address: observation,
    })
}

pub(crate) async fn run(config: Config, output: PathBuf) -> Result<(), BoxError> {
    let runtime = Runtime::new();
    runtime.run(async {
        let root = runtime.context();
        let result = async {
            let app = start(&root, config, output)?;
            tokio::select! {
                result = async {
                    app.journal.ready()?.await?;
                    app.receiver.ready()?.await?;
                    let address = app.address.borrow().ok_or_else(|| invalid("webhook address unavailable"))?;
                    println!("GitHub webhook listening on http://{address}/github");
                    std::future::pending::<Result<(), BoxError>>().await
                } => result,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    Ok(())
                }
            }
        }.await;
        let cleanup = drain(&root).await;
        result.and(cleanup)
    }).await?
}

#[cfg(not(test))]
fn main() -> Result<(), BoxError> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=3).contains(&args.len()) {
        return Err(invalid("usage: github_webhook OWNER/REPO JOURNAL.jsonl [PORT]; set CORDIS_GITHUB_WEBHOOK_SECRET (32+ bytes)").into());
    }
    let port = match args.get(2) {
        Some(port) => port.parse()?,
        None => 3000,
    };
    let secret = std::env::var("CORDIS_GITHUB_WEBHOOK_SECRET")?.into_bytes();
    let config = Config {
        repository: args[0].clone(),
        bind: ([127, 0, 0, 1], port).into(),
        secret,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(config, args[1].clone().into()))
}
