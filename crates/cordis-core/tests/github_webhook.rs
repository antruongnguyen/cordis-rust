//! Real loopback HTTP, raw-body authentication and joined journal ownership.
#[path = "support/github_activity.rs"]
mod support;
#[path = "../examples/github_webhook.rs"]
mod webhook;

use cordis_core::{FiberState, Runtime, ServiceKey};
use futures_util::FutureExt;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{net::SocketAddr, time::Duration};
use support::Directory;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use webhook::{Config, Journal};

const SECRET: &[u8] = b"synthetic-local-test-key-not-a-secret";
const DELIVERY: &str = "01234567-89ab-cdef-0123-456789abcdef";
const PAYLOAD: &[u8] = include_bytes!("fixtures/github_issue_event.json");

fn config() -> Config {
    Config {
        repository: "antruongnguyen/cordis-rust".into(),
        bind: ([127, 0, 0, 1], 0).into(),
        secret: SECRET.to_vec(),
    }
}

fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(body);
    let mut signature = "sha256=".to_owned();
    for byte in mac.finalize().into_bytes() {
        use std::fmt::Write;
        write!(signature, "{byte:02x}").unwrap();
    }
    signature
}

fn headers(body: &[u8], event: &str, signature: &str, extra: &str) -> String {
    format!(
        "POST /github HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         X-GitHub-Event: {event}\r\nX-GitHub-Delivery: {DELIVERY}\r\n\
         X-Hub-Signature-256: {signature}\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
        body.len()
    )
}

async fn response(mut stream: TcpStream) -> u16 {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    let response = String::from_utf8(bytes).unwrap();
    response.split_whitespace().nth(1).unwrap().parse().unwrap()
}

async fn post(address: SocketAddr, body: &[u8], event: &str, signature: &str, extra: &str) -> u16 {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(headers(body, event, signature, extra).as_bytes())
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
    response(stream).await
}

async fn held(address: SocketAddr, body: &[u8], secret: &[u8]) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            headers(
                body,
                "issues",
                &sign(secret, body),
                "Expect: 100-continue\r\n",
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut interim = [0; 25];
    stream.read_exact(&mut interim).await.unwrap();
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
    stream
}

async fn ready(app: &webhook::Webhook) -> SocketAddr {
    app.journal.ready().unwrap().await.unwrap();
    app.receiver.ready().unwrap().await.unwrap();
    app.address.borrow().unwrap()
}

#[test]
fn signature_verification_matches_rfc4231_and_rejects_modified_or_malformed_input() {
    let signature = "sha256=b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
    assert!(webhook::verified(&[0x0b; 20], signature, b"Hi There"));
    assert!(!webhook::verified(&[0x0b; 20], signature, b"Hi There!"));
    for invalid in [
        "",
        "sha1=abc",
        "sha256=ab",
        &format!("sha256={}", "z".repeat(64)),
    ] {
        assert!(!webhook::verified(SECRET, invalid, PAYLOAD));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn authenticated_events_append_before_ack_and_invalid_requests_do_not_write() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dir = Directory::new("webhook");
        let output = dir.path("events.jsonl");
        let runtime = Runtime::new();
        runtime.run(async {
            let root = runtime.context();
            let app = webhook::start(&root, config(), output.clone()).unwrap();
            let address = ready(&app).await;
            assert_eq!(post(address, PAYLOAD, "issues", &sign(SECRET, PAYLOAD), "").await, 204);
            let first = tokio::fs::read_to_string(&output).await.unwrap();
            assert_eq!(first.lines().count(), 1, "ack preceded journal append");
            let pull = serde_json::json!({
                "action": "closed", "repository": { "full_name": "antruongnguyen/cordis-rust" },
                "pull_request": { "number": 2, "title": "Synthetic PR", "html_url": "https://github.com/antruongnguyen/cordis-rust/pull/2" }
            }).to_string();
            assert_eq!(post(address, pull.as_bytes(), "pull_request", &sign(SECRET, pull.as_bytes()), "").await, 204);
            assert_eq!(post(address, PAYLOAD, "issues", &sign(SECRET, PAYLOAD), "").await, 204);
            let good = tokio::fs::read_to_string(&output).await.unwrap();
            let records: Vec<serde_json::Value> = good.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
            assert_eq!(records.len(), 3, "duplicate deliveries are journaled, not silently deduplicated");
            assert_eq!(records[0]["delivery"], DELIVERY);
            assert_eq!(records[0]["event"], "issues");
            assert_eq!(records[0]["payload"], serde_json::from_slice::<serde_json::Value>(PAYLOAD).unwrap());
            assert!(records[0]["received_at"].as_u64().unwrap() > 0);
            assert_eq!(post(address, PAYLOAD, "issues", &sign(b"wrong key", PAYLOAD), "").await, 401);
            let altered = [PAYLOAD, b" "].concat();
            assert_eq!(post(address, &altered, "issues", &sign(SECRET, PAYLOAD), "").await, 401);
            assert_eq!(post(address, PAYLOAD, "push", &sign(SECRET, PAYLOAD), "").await, 422);
            assert_eq!(post(address, b"{", "issues", &sign(SECRET, b"{"), "").await, 400);
            for invalid in [
                String::from_utf8(PAYLOAD.to_vec()).unwrap().replace("antruongnguyen/cordis-rust", "another/repository"),
                String::from_utf8(PAYLOAD.to_vec()).unwrap().replace("\"number\": 1", "\"number\": 0"),
                "{}".to_owned(),
            ] {
                assert_eq!(post(address, invalid.as_bytes(), "issues", &sign(SECRET, invalid.as_bytes()), "").await, 422);
            }
            assert_eq!(post(address, PAYLOAD, "issues", &sign(SECRET, PAYLOAD), "X-Hub-Signature-256: duplicate\r\n").await, 400);
            let huge = vec![b' '; webhook::BODY_LIMIT + 1];
            assert_eq!(post(address, &huge, "issues", &sign(SECRET, &huge), "").await, 413);
            assert_eq!(tokio::fs::read_to_string(&output).await.unwrap(), good);
            let report = root.drain().unwrap().await.unwrap();
            assert!(report.failures().is_empty());
            assert!(root.registry().is_empty().unwrap());
            assert!(runtime.take_failures().is_empty());
            assert!(TcpStream::connect(address).await.is_err());
        }).await.unwrap();
    }).await.expect("webhook acceptance stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn held_requests_retain_old_journal_through_reconfiguration_and_shutdown() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dir = Directory::new("webhook-ownership");
        let first = dir.path("first.jsonl");
        let second = dir.path("second.jsonl");
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let baseline = root.get_effects().unwrap();
                let mut app = webhook::start(&root, config(), first.clone()).unwrap();
                let address = ready(&app).await;
                app.address.borrow_and_update();
                let old = app
                    .journal
                    .context()
                    .get(&ServiceKey::<Journal>::new("githubWebhookJournal"))
                    .unwrap()
                    .unwrap();
                let mut request = held(address, PAYLOAD, SECRET).await;
                let mut updating = Box::pin(app.journal.update(second.clone()).unwrap());
                app.address.changed().await.unwrap();
                assert!(app.address.borrow_and_update().is_none());
                assert!(updating.as_mut().now_or_never().is_none());
                assert!(old.is_open());
                assert!(!second.exists());
                request.write_all(PAYLOAD).await.unwrap();
                assert_eq!(response(request).await, 204);
                updating.await.unwrap();
                app.receiver.ready().unwrap().await.unwrap();
                assert!(!old.is_open());
                assert_eq!(
                    tokio::fs::read_to_string(&first)
                        .await
                        .unwrap()
                        .lines()
                        .count(),
                    1
                );
                let mut next = config();
                next.secret = b"another-synthetic-local-key-not-secret".to_vec();
                app.receiver.update(next.clone()).unwrap().await.unwrap();
                let address = app.address.borrow_and_update().unwrap();
                assert_eq!(
                    post(address, PAYLOAD, "issues", &sign(SECRET, PAYLOAD), "").await,
                    401
                );
                assert_eq!(
                    post(address, PAYLOAD, "issues", &sign(&next.secret, PAYLOAD), "").await,
                    204
                );
                let mut invalid = next.clone();
                invalid.bind = ([0, 0, 0, 0], 0).into();
                assert!(app.receiver.update(invalid).is_err());
                assert_eq!(app.receiver.state(), FiberState::Active);
                let current = app
                    .journal
                    .context()
                    .get(&ServiceKey::<Journal>::new("githubWebhookJournal"))
                    .unwrap()
                    .unwrap();
                let mut request = held(address, PAYLOAD, &next.secret).await;
                let mut stopping = Box::pin(root.drain().unwrap());
                app.address.changed().await.unwrap();
                assert!(app.address.borrow().is_none());
                assert!(stopping.as_mut().now_or_never().is_none());
                assert!(current.is_open());
                request.write_all(PAYLOAD).await.unwrap();
                assert_eq!(response(request).await, 204);
                assert!(stopping.await.unwrap().failures().is_empty());
                assert!(
                    !current.is_open(),
                    "retained service value must not lease the file"
                );
                assert_eq!(
                    tokio::fs::read_to_string(&second)
                        .await
                        .unwrap()
                        .lines()
                        .count(),
                    2
                );
                assert!(root.registry().is_empty().unwrap());
                assert_eq!(root.get_effects().unwrap(), baseline);
                assert!(runtime.take_failures().is_empty());
            })
            .await
            .unwrap();
    })
    .await
    .expect("webhook ownership stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn failed_bind_requires_explicit_recovery_and_invalid_config_cannot_open_output() {
    let dir = Directory::new("webhook-recovery");
    let mut invalid = config();
    invalid.secret.clear();
    assert!(
        webhook::run(invalid, dir.path("invalid.jsonl"))
            .await
            .is_err()
    );
    assert!(!dir.path("invalid.jsonl").exists());
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config();
    config.bind = occupied.local_addr().unwrap();
    let runtime = Runtime::new();
    runtime
        .run(async {
            let root = runtime.context();
            let app = webhook::start(&root, config.clone(), dir.path("first.jsonl")).unwrap();
            app.journal.ready().unwrap().await.unwrap();
            assert!(app.receiver.ready().unwrap().await.is_err());
            assert_eq!(runtime.take_failures().len(), 1);
            drop(occupied);
            app.journal
                .update(dir.path("recovered.jsonl"))
                .unwrap()
                .await
                .unwrap();
            assert_eq!(app.receiver.state(), FiberState::Failed);
            app.receiver.update(config).unwrap().await.unwrap();
            let address = app.address.borrow().unwrap();
            assert_eq!(
                post(address, PAYLOAD, "issues", &sign(SECRET, PAYLOAD), "").await,
                204
            );
            assert_eq!(
                tokio::fs::read_to_string(dir.path("recovered.jsonl"))
                    .await
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert!(root.drain().unwrap().await.unwrap().failures().is_empty());
            assert!(runtime.take_failures().is_empty());
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn body_deadline_and_connection_limit_are_measured_without_sleeps() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let dir = Directory::new("webhook-limits");
        let output = dir.path("events.jsonl");
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let app = webhook::start(&root, config(), output.clone()).unwrap();
                let address = ready(&app).await;
                let request = held(address, PAYLOAD, SECRET).await;
                tokio::time::pause();
                tokio::time::advance(webhook::REQUEST_TIMEOUT + Duration::from_secs(1)).await;
                tokio::time::resume();
                assert_eq!(response(request).await, 504);
                assert!(tokio::fs::read(&output).await.unwrap().is_empty());
                let mut held_requests = Vec::new();
                for _ in 0..64 {
                    held_requests.push(held(address, PAYLOAD, SECRET).await);
                }
                let mut overflow = TcpStream::connect(address).await.unwrap();
                let mut byte = [0];
                match overflow.read(&mut byte).await {
                    Ok(0) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                    result => panic!("65th connection was not refused: {result:?}"),
                }
                drop(held_requests);
                assert!(root.drain().unwrap().await.unwrap().failures().is_empty());
                assert!(root.registry().is_empty().unwrap());
            })
            .await
            .unwrap();
    })
    .await
    .expect("webhook limits stalled");
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn journal_write_failure_returns_error_and_poisoned_sink_never_acknowledges_success() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = Directory::new("webhook-io-failure");
        let output = dir.path("broken-pipe");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&output)
                .status()
                .unwrap()
                .success()
        );
        let reading = tokio::spawn({
            let output = output.clone();
            async move { tokio::fs::File::open(output).await.unwrap() }
        });
        let runtime = Runtime::new();
        runtime
            .run(async {
                let root = runtime.context();
                let app = webhook::start(&root, config(), output).unwrap();
                app.journal.ready().unwrap().await.unwrap();
                drop(reading.await.unwrap());
                app.receiver.ready().unwrap().await.unwrap();
                let address = app.address.borrow().unwrap();
                for _ in 0..2 {
                    assert_eq!(
                        post(address, PAYLOAD, "issues", &sign(SECRET, PAYLOAD), "").await,
                        500
                    );
                }
                let messages: Vec<_> = root
                    .log_buffer()
                    .unwrap()
                    .into_iter()
                    .map(|record| record.message)
                    .collect();
                assert!(
                    messages
                        .iter()
                        .any(|message| message.contains("journal append failed"))
                );
                assert!(
                    messages
                        .iter()
                        .any(|message| message.contains("journal failed earlier"))
                );
                assert!(root.drain().unwrap().await.unwrap().failures().is_empty());
                assert!(root.registry().is_empty().unwrap());
            })
            .await
            .unwrap();
    })
    .await
    .expect("journal failure handling stalled");
}
