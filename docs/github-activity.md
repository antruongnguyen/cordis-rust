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

## Remaining approved slice

The webhook receiver is approved but not yet implemented. It will verify
raw-body HMAC-SHA256 signatures, accept only selected issue/PR events for the
configured repository, and acknowledge after append.
Public ingress, creating GitHub webhooks, live delivery, mutations, notifications
to other people and durable/exactly-once delivery are excluded.

## Acceptance

Start with the relevant `github_*` integration tests and executable. Preserve
the explicit MSRV/stable selection and complete acceptance matrix in
[m6.md](m6.md#verification), and the six paired scenarios in
[lifecycle-parity.md](lifecycle-parity.md). Application acceptance uses synthetic
files and local peers, not authenticated live GitHub requests.
