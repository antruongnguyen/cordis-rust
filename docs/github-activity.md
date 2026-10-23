# GitHub activity consumers

The approved first-consumer scope is three thin, application-level integrations:
a snapshot-to-Markdown digest, a `gh`-based collector, and an authenticated
loopback webhook journal. They are not satellite crates, a generic harness,
a public API redesign, or a new parity milestone. Production core code and its
ownership/cancellation restrictions remain unchanged.

## Snapshot digest

```sh
cargo run -p cordis-core --example github_digest --locked -- \
  crates/cordis-core/tests/fixtures/github_snapshot.json /tmp/cordis-report.md
```

The fixture is synthetic, not an assertion about real repository activity.
For a real snapshot, the JSON contract is:

```json
{
  "repository": "antruongnguyen/cordis-rust",
  "captured_at": 1792742400,
  "issues": [
    {
      "number": 1,
      "title": "Example issue",
      "state": "OPEN",
      "url": "https://github.com/antruongnguyen/cordis-rust/issues/1",
      "updatedAt": "2026-10-23T12:00:00Z"
    }
  ],
  "pull_requests": []
}
```

The digest accepts at most 4 MiB and 100 items of each kind. Repository names,
positive unique issue/PR numbers, canonical github.com links, states and valid
UTC calendar timestamps are checked. Issues cannot be Merged. Unknown JSON
fields are rejected. This deliberately supports GitHub.com, not Enterprise
hosts or arbitrary export schemas.

Markdown groups issues and PRs by state, orders each group by descending update
time then ascending number, and escapes untrusted titles. Empty sections are
explicit. The report states that the snapshot is bounded, not complete history.
`captured_at` is a Unix-second observation timestamp, not a collection cursor.

The output parent directory must already exist. Output replaces the named
report only after full input validation and a successful same-directory
temporary write/flush/rename. Invalid input and failed writes leave the previous
report intact. Input and output cannot resolve to the same file. No fsync/crash
durability, multi-process locking, durable queue, or file watching is promised.
Temporary-file cleanup is best effort on cancellation, with cleanup failures
reported to stderr; process termination may leave a uniquely named `.tmp` file.

### Cordis integration and reconfiguration

`github_digest::start` registers a destination provider and an injecting
processor. Missing dependencies remain Pending. Each admitted processor load
reads and writes one batch; its committed injection retains that destination
through completion. Destination updates trigger reprocessing at the new path.
Input updates explicitly restart/recover the processor. Failed processors do
not recover merely because the destination changes.

The CLI performs one batch and always awaits root cleanup on ordinary success
or failure. Runtime reconfiguration/recovery is currently programmatic through
the example's typed handles, exercised in its integration tests; there is no
interactive reload command. Runtime destruction remains cancellation, not
successful cleanup.

`tests/github_digest.rs` includes the executable implementation, checks exact
golden output, invalid-input preservation, size limits, rename failure, provider
reconfiguration, explicit recovery and restored Registry/effect baselines.

## Snapshot collector

```sh
# Uses your existing gh authentication; performs read-only GitHub requests.
cargo run -p cordis-core --example github_collect --locked -- \
  antruongnguyen/cordis-rust /tmp/cordis-snapshot.json
# Repeat after a 300-second delay following each completed poll.
cargo run -p cordis-core --example github_collect --locked -- \
  antruongnguyen/cordis-rust /tmp/cordis-snapshot.json 300
```

`gh` must already be installed and authenticated. The application does not log
in, create credentials or mutate GitHub. It invokes `gh issue list` and
`gh pr list` directly without a shell, selecting GitHub.com, all states, the
`sort:updated-desc` search, a 100-item limit, and only snapshot fields. Search
index freshness is GitHub's, not an immediate-consistency guarantee.

Each direct subprocess has a 30-second deadline, 4 MiB stdout bound and 64 KiB
stderr bound. Timeout/oversize errors kill and await that child. No cleanup of
arbitrary descendant processes is promised. The two sequential queries are not
a transaction against GitHub: they observe potentially different instants.
Only when both succeed, the combined data validates, and the serialized output
fits 4 MiB does atomic replacement publish the new snapshot. It is a timestamped
latest snapshot, not an accumulating history or complete repository inventory.
The previous good snapshot survives a failed query/parse/write.

Initial collection is part of the injecting poller's setup; failure leaves it
Failed with one core diagnostic, requiring explicit update for recovery.
Later periodic failures are printed to stderr and recorded in the core logger.
There is no immediate retry; the next attempt waits the configured interval.
Polling is sequential, with no overlapping requests or catch-up burst.

A configuration provider owns repository/destination/interval selection.
Programmatic provider updates wait for the consumer's in-flight poll and joined
worker teardown before replacement. Await the consumer's readiness separately.
As with the digest, CLI configuration is startup-only; live updates/recovery use
the typed example handles. Ctrl-C initiates awaited root teardown, including
during initial collection. Shutdown finishes an admitted poll (at most two query
deadlines plus filesystem work), rather than claiming cancellation is cleanup.

`tests/github_collect.rs` runs the executable's implementation with a local fake
`gh` subprocess. Tests verify exact arguments, complete publication, failure
preservation, timeout/reaping and output bounds. FIFO gates and a manually
advanced Tokio clock prove in-flight work joins before reconfiguration/shutdown
and explicit recovery follows ignored provider refresh. These subprocess
fixtures require Unix, `/bin/sh`, `mkfifo`, `/bin/kill`, and `head`; the commands
themselves use portable Tokio process APIs. No live GitHub request is exercised.

## Signed loopback webhook journal

Set `CORDIS_GITHUB_WEBHOOK_SECRET` from your local secret manager (at least
32 bytes; never commit a real key), then:

```sh
cargo run -p cordis-core --example github_webhook --locked -- \
  antruongnguyen/cordis-rust /tmp/cordis-events.jsonl 3000
```

Only loopback addresses are admitted; the CLI binds `127.0.0.1` and defaults
to port 3000. Port 0 selects an available port and prints it. There is no TLS,
public ingress, account/token creation, webhook configuration, or notification
delivery. Actual GitHub cannot deliver to this loopback listener. The executed
evidence is signed local replays, not live GitHub delivery.

`POST /github` requires exactly one each of Content-Type (`application/json`),
`X-GitHub-Event`, `X-GitHub-Delivery` (UUID-shaped), and
`X-Hub-Signature-256`. Only `issues` and `pull_request` events are accepted.
HMAC-SHA256 verification covers the exact raw body, using the library's
constant-time MAC comparison. It precedes JSON parsing. Payload validation checks
the exact repository full name, nonempty action/title, positive target number
and canonical target URL. This is a bounded envelope contract, not validation of
every field in GitHub's complete evolving schemas. Use canonical repository case.
Ping events are intentionally not supported in this local-only slice.

The authenticated payload is appended as one JSONL record with `delivery`,
`event`, `received_at` (Unix seconds), and `payload`. HTTP 204 means write and
flush completed before acknowledgement; it does not mean fsync/crash durability.
Delivery IDs are preserved but duplicates are not deduplicated. The journal
contains event metadata, potentially from private repositories: choose a suitably
protected local output directory and do not publish it accidentally.

### Limits, failures and resource ownership

- Body limit: 256 KiB, including accepted exact-boundary and rejected
  boundary-plus-one tests. The header buffer limit is 32 KiB.
- At most 64 active HTTP connections and 32 queued journal entries. Excess
  connections close; excess queued deliveries receive HTTP 503.
- Headers have a 10-second read deadline; handlers have a separate 10-second
  body/ack deadline. Invalid signatures return 401, invalid JSON/headers 400,
  unsupported events/repository/payload 422, oversize bodies 413, journal errors
  500, and exhausted handler deadlines 504.
- An event admitted to the journal queue may still append after its client
  disconnects or receives 504. It has not received a success acknowledgement;
  a retry can therefore duplicate it. Request cancellation never aborts an
  already-admitted file write.
- An append/flush failure is reported and poisons that journal instance.
  Later requests cannot receive a false success. A failed append may leave a
  partial final line; repair/reconciliation is manual before reusing that file.
  Reconfigure to a healthy destination rather than silently retry writes.

An initialized plugin owns the actual file; an injecting receiver owns the
bounded queue, serialized writer, listener, and all HTTP connection tasks.
The application uses Axum routing/extraction over a directly hosted Hyper
HTTP/1 loop. A local JoinSet owns every connection, including header-read
timeouts; no connection is detached into an unowned Tokio task.

Cleanup stops acceptance, gracefully joins in-flight HTTP work and queued writes,
then releases the exact provider and closes its file. Config/destination updates
use those same barriers, with consumer readiness awaited separately. Typed
handles provide programmatic reconfiguration and Failed-listener recovery;
the CLI is startup-configured and uses Ctrl-C for awaited teardown.
Malformed/disconnected client connections are logged without stopping unrelated
requests. Unexpected server/task failures are logged and retained for cleanup's
error result, not silently converted to successful teardown.

### Local replay

With the listener running and the same secret available in the calling shell:

```sh
payload=crates/cordis-core/tests/fixtures/github_issue_event.json
signature=$(python3 -c 'import hashlib,hmac,os,pathlib,sys; print("sha256="+hmac.new(os.environ["CORDIS_GITHUB_WEBHOOK_SECRET"].encode(),pathlib.Path(sys.argv[1]).read_bytes(),hashlib.sha256).hexdigest())' "$payload")
curl --fail-with-body http://127.0.0.1:3000/github \
  -H 'Content-Type: application/json' \
  -H 'X-GitHub-Event: issues' \
  -H 'X-GitHub-Delivery: 01234567-89ab-cdef-0123-456789abcdef' \
  -H "X-Hub-Signature-256: $signature" \
  --data-binary "@$payload"
```

This fixture and its delivery ID are synthetic. `tests/github_webhook.rs` checks
RFC 4231's independent HMAC vector, both event types, raw-body tampering,
malformed/duplicate headers and invalid payloads, limits, acknowledgement timing,
key rotation, bind recovery, exact old-file retention through reconfiguration,
and awaited shutdown. HTTP `100 Continue` gates hold in-flight bodies; a
controlled Tokio clock tests the deadline, and the 65th connection is rejected.
Unix FIFO fixtures also prove real write failure/poisoning without a fake writer.
No test relies on live GitHub or sleeps.

Public ingress, creating GitHub webhooks, live delivery, mutations, notifications
to other people and durable/exactly-once delivery remain excluded.

## Acceptance

Start with the relevant `github_*` integration tests and executable. Preserve
the explicit MSRV/stable selection and complete acceptance matrix in
[m6.md](m6.md#verification), and the six paired scenarios in
[lifecycle-parity.md](lifecycle-parity.md). Application acceptance uses synthetic
files and local peers, not authenticated live GitHub requests.
