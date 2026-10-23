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

## Remaining approved slices

The collector and webhook receiver are approved but not yet implemented.
The collector will use existing `gh` authentication, bounded issue/PR queries,
timeouts, one-shot/periodic collection and complete snapshot publication.
The receiver will verify raw-body HMAC-SHA256 signatures, accept only selected
issue/PR events for the configured repository, and acknowledge after append.
Public ingress, creating GitHub webhooks, live delivery, mutations, notifications
to other people and durable/exactly-once delivery are excluded.

## Acceptance

Start with the relevant `github_*` integration tests and executable. Preserve
the explicit MSRV/stable selection and complete acceptance matrix in
[m6.md](m6.md#verification), and the six paired scenarios in
[lifecycle-parity.md](lifecycle-parity.md). Application acceptance uses synthetic
files and local peers, not authenticated live GitHub requests.
