# Post-M6: executable lifecycle parity

These two approved, bounded hardening slices run six equivalent scenarios
against the production Rust core and TypeScript Cordis at
`35fdc969e4b6c48c48ed44717c7a018324e4dd88`. They add evidence, not a new public
API, satellite package, or general M7 definition. No production change or new
Rust dependency was needed.

## Scenarios and source evidence

| Scenario | Required behavior | Pinned TS source |
|---|---|---|
| `combined` | Provider and consumer config updates apply `(1, old)` then `(2, new)`, never `(1, new)`. | `packages/core/tests/fiber.spec.ts`: `update config while injected service reloads` |
| `coalesced` | Hold old consumer cleanup, queue further provider/config updates, retain exact old-provider access, skip provider 2 and middle config, then load `(3, new)`. Hold that final load to check readiness. | `update.spec.ts`: concurrent latest config and coalescing during unloading; `service-dispose.spec.ts`: retained resources |
| `removed` | Remove the consumer's Registry group while cleanup is held. Keep its old provider alive until final use, then dispose the consumer without loading queued config. | `service-dispose.spec.ts`: removed consumer; `update.spec.ts`: queued config after disposal |
| `superseded-failure` | Fail held load 1 after update 2 supersedes it. Hold rollback: both the original readiness waiter and update remain pending. Clean load 1 once, then activate load 2 without propagating the obsolete error. | `update.spec.ts:121-150`: cleans up a superseded failure without failing the latest update |
| `latest-failure` | Gate load 1, queue load 2 with multiple update waiters, and abandon another update waiter. Fail load 2 and gate its rollback. Both retained updates reject with its original error only after cleanup; diagnose once. | `update.spec.ts:152-187`: latest failure rejects every waiting update; `fiber.spec.ts:156-183`: reload failure and dropped failure |
| `explicit-recovery` | An injecting consumer fails and rolls back. Replace its provider: it stays Failed with one apply attempt. Explicit update loads against provider 2, stays pending through gated setup, then reaches Active. | `fiber.spec.ts:93-154`: no dependency-driven retry; explicit and asynchronous recovery |

All test paths in the last column are under `packages/core/tests/`.
Implementation evidence is `fiber.ts`'s `_setEpoch`, `_reload`, `_unload`,
`await`, and `update`, plus `reflect.ts`'s exact-publication withdrawal.

The Rust fixture is
[`tests/lifecycle_parity.rs`](../crates/cordis-core/tests/lifecycle_parity.rs).
Its TypeScript counterpart is
[`tests/parity/lifecycle.spec.ts`](../tests/parity/lifecycle.spec.ts).
Both assert six shared `.txt` traces under `tests/parity/`.
Channel/Promise gates, not sleeps, control load, rollback, cleanup, and recovery
windows. Each test has a five-second failure bound.

## Measured contract and normalization

The original three scenarios check the final read occurs against the exact old
publication while its provider is open, before provider close. Intermediate
targets never apply. Blocked cleanup prevents update/disposal completion. Final disposal
restores Registry, publication, and root effect baselines without diagnostics.
The retained direct provider value is used only to observe the open flag; the
committed injection, not that value, keeps provider resources alive.

Both implementations let consumer update waiters settle **Pending** when their
provider is unavailable. Provider arrival subsequently starts another lifecycle
pass. Await provider completion and then consumer readiness to join that pass;
neither an update nor a provider waiter is a runtime-wide convergence barrier.
In `coalesced`, all update waiters finish while the latest consumer load is
still gated Loading, but the newly acquired consumer readiness waiter remains
pending until the load gate opens. In `removed`, the consumer waiters instead
finish Disposed.

The three failure/recovery scenarios add 36 observations to the original 37,
for **73 total**. Superseded failure cannot poison the latest target, and latest
failure cannot settle retained update waiters until partial rollback finishes.
Abandoning another update's result neither cancels rollback nor hides diagnostics;
the TS fixture leaves that result unobserved, with Vitest also checking unhandled
rejections. Each scenario asserts exactly one load-failure diagnostic, before
and after final disposal, and restores Registry/root-effect baselines.
Recovery also checks exact injected values during cleanup and removes its
publication on final disposal. Provider withdrawal/republication alone does not
retry a Failed consumer; only explicit update enables its gated successful load.

Error-source identity is asserted separately from shared traces: TS waiter
rejections and logger arguments retain the same Error object. A test-only Rust
Error adapter retains an original `Rc<std::io::Error>` as its source, and every
callback error and diagnostic is checked against that exact allocation. No
string-only normalization stands in for these checks. Diagnostic totals are
summarized after waiter outcomes, not used to claim a cross-language ordering
of error logging callbacks.

The compared observations are causal resource events and controlled
checkpoints. `update-waiters` summarizes already-checked outcomes at a later
checkpoint; its position does not claim those promises resolved after the
latest apply. Rust and TS can schedule their completion callbacks differently.
No timestamps, allocation IDs, enum ordinals, stack text, or unrelated callback
ordering is compared. The tests do not sort away resource-order violations.

## Reproduce

The normal `cargo test --workspace --locked` includes the six Rust tests and
requires no Node/Python/reference checkout. For the paired comparison, install
Python 3, Node 22.12 or newer with Corepack, and rustup's stable toolchain.
Use only an isolated reference checkout under this worktree. If absent:

```sh
git clone --no-checkout https://github.com/cordiverse/cordis.git local/cordis
git -C local/cordis checkout --detach 35fdc969e4b6c48c48ed44717c7a018324e4dd88
```

Restore the reference's dependencies when absent, then run:

```sh
(cd local/cordis && corepack yarn install)
python3 tests/parity/run.py
```

The standard-library Python runner:

- Refuses a different source revision or tracked upstream changes.
- Selects stable Cargo/rustc/rustdoc explicitly, prepends their bin directory
  to PATH, and isolates artifacts in `target/stable`.
- Exclusively creates a temporary test in the upstream core tests directory,
  lints it with upstream ESLint (explicitly including the normally ignored test),
  type-checks it with upstream TypeScript and core compiler allowances, and
  runs `corepack yarn test core/cordis-rust-lifecycle-parity`. This uses the
  existing Yakumo/Vitest configuration, not a replacement JS runner.
- Removes the copied fixture in a `finally` block, including test failures.
  It never overwrites an existing fixture or edits upstream tracked files.
- Runs the Rust integration suite and compares all six emitted traces.
  Missing output, wrong expectations, runner failures, and mismatches fail
  explicitly. Temporary trace directories are removed on exit.

Each command has a timeout (180 seconds for test/type-check commands,
30 seconds for pin/tool-path probes). The runner does not clone, install
dependencies, or modify another checkout automatically. It is a separate
opt-in acceptance check, not a new CI Node dependency. Full cross-language
execution is not claimed by the existing Rust-only CI.

## Verification and limits

Verified on 2026-10-23:

| Check | Result |
|---|---|
| Paired fixtures | Six TS and six Rust tests; 73 matching observations, including three consecutive repeated comparison runs; the original 37 observations are unchanged |
| TypeScript environment | Node 22.23.3, Yarn 4.14.1, Vitest 4.1.12, cosmokit 1.8.1; targeted fixture type-check and warnings-denied ESLint pass |
| Original upstream regressions | `core/update core/service-dispose core/fiber`: 61 tests pass |
| Stable 1.97.1 / Clippy 0.1.97 | All-target workspace check, 152 integration tests, 17 Rustdoc checks, strict workspace/M0 Clippy, both formatting checks, warnings-denied public docs, executable lifecycle example, M0's 20 tests and one Rustdoc |
| Actual Rust/Cargo/rustdoc 1.85.0 / Clippy 0.1.85 | All-target workspace check, the same workspace/Rustdoc tests, strict workspace/M0 Clippy, warnings-denied public docs, executable example, and independent M0 suite |

Both Rust toolchains used explicit PATH/RUSTC/RUSTDOC selection and separate
`target/stable` / `target/msrv` directories, following [M6 precautions](m6.md#verification).
The 1.85.0 Clippy component was initially absent, allowing Cargo's external
subcommand lookup to reach Homebrew despite compiler selection. Installing the
missing component and verifying its binaries resolved this; the complete MSRV
matrix then passed with actual Clippy 0.1.85. No shared caches were deleted.
An intentionally corrupted trace expectation was rejected, and the temporary
upstream fixture was removed on that failure path before restoring and
rerunning the correct expectations.

The upstream pin does not track a Yarn lockfile. Its source is pinned; the
entire transitive JavaScript dependency graph is not frozen by this repository.
These checks execute only the six paired scenarios and the stated original
regressions, not every core spec or every parameter variant. All [M6 compatibility
restrictions](m6.md#remaining-restrictions-and-safety-deviations), local hosting,
weak ownership, cancellation and non-lease service-value contracts remain.

## Completion boundary

Both approved slices are complete. The failure/recovery extension reuses the
existing fixtures/runner and strengthens the Rust-only contracts in
`tests/updates.rs`, `tests/plugins.rs`, and `tests/services.rs`; no production
mismatch was demonstrated. This is not every failure/recovery, cancellation,
observer-error, or exporter-error protocol. It does not change the Pending
settlement boundary or make update/provider waits runtime-wide convergence
barriers. No satellite, public API redesign, new dependency, or broader M7 is
selected. Prefer concrete consuming application needs over indefinite parity
expansion, and change production only for evidenced mismatches.
