# Post-M6: executable lifecycle parity

This approved hardening slice runs three equivalent scenarios against the
production Rust core and TypeScript Cordis at
`35fdc969e4b6c48c48ed44717c7a018324e4dd88`. It adds evidence, not a new public
API, satellite package, or general M7 definition. No production change or new
Rust dependency was needed.

## Scenarios and source evidence

| Scenario | Required behavior | Pinned TS source |
|---|---|---|
| `combined` | Provider and consumer config updates apply `(1, old)` then `(2, new)`, never `(1, new)`. | `packages/core/tests/fiber.spec.ts`: `update config while injected service reloads` |
| `coalesced` | Hold old consumer cleanup, queue further provider/config updates, retain exact old-provider access, skip provider 2 and middle config, then load `(3, new)`. Hold that final load to check readiness. | `update.spec.ts`: concurrent latest config and coalescing during unloading; `service-dispose.spec.ts`: retained resources |
| `removed` | Remove the consumer's Registry group while cleanup is held. Keep its old provider alive until final use, then dispose the consumer without loading queued config. | `service-dispose.spec.ts`: removed consumer; `update.spec.ts`: queued config after disposal |

All test paths in the last column are under `packages/core/tests/`.
Implementation evidence is `fiber.ts`'s `_setEpoch`, `_reload`, `_unload`,
`await`, and `update`, plus `reflect.ts`'s exact-publication withdrawal.

The Rust fixture is
[`tests/lifecycle_parity.rs`](../crates/cordis-core/tests/lifecycle_parity.rs).
Its TypeScript counterpart is
[`tests/parity/lifecycle.spec.ts`](../tests/parity/lifecycle.spec.ts).
Both assert shared `combined.txt`, `coalesced.txt`, and `removed.txt` traces.
Channel/Promise gates, not sleeps, control cleanup and final-load windows.
Each test has a five-second failure bound.

## Measured contract and normalization

Every scenario checks the final read occurs against the exact old publication
while its provider is open, before provider close. Intermediate targets never
apply. Blocked cleanup prevents update/disposal completion. Final disposal
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

The 37 compared observations are causal resource events and controlled
checkpoints. `update-waiters` summarizes already-checked outcomes at a later
checkpoint; its position does not claim those promises resolved after the
latest apply. Rust and TS can schedule their completion callbacks differently.
No timestamps, allocation IDs, enum ordinals, stack text, or unrelated callback
ordering is compared. The tests do not sort away resource-order violations.

## Reproduce

The normal `cargo test --workspace --locked` includes the three Rust tests and
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
- Runs the Rust integration suite and compares all three emitted traces.
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
| Paired fixtures | Three TS and three Rust tests; 37 matching observations, including three consecutive repeated comparison runs |
| TypeScript environment | Node 22.23.3, Yarn 4.14.1, Vitest 4.1.12, cosmokit 1.8.1; targeted fixture type-check and warnings-denied ESLint pass |
| Original upstream regressions | `core/update core/service-dispose core/fiber`: 61 tests pass |
| Stable 1.97.1 | All-target workspace check, 149 integration tests, 17 Rustdoc checks, strict workspace/M0 Clippy, both formatting checks, warnings-denied public docs, executable lifecycle example, M0's 20 tests and one Rustdoc |
| Actual Rust/Cargo/rustdoc 1.85.0 | All-target workspace check, the same workspace/Rustdoc tests, executable example, and independent M0 suite |

Both Rust toolchains used explicit PATH/RUSTC/RUSTDOC selection and separate
`target/stable` / `target/msrv` directories, following [M6 precautions](m6.md#verification).
An intentionally corrupted trace expectation was rejected, and the temporary
upstream fixture was removed on that failure path before restoring and
rerunning the correct expectations.

The upstream pin does not track a Yarn lockfile. Its source is pinned; the
entire transitive JavaScript dependency graph is not frozen by this repository.
These checks execute only the three paired scenarios and the stated original
regressions, not every core spec or every parameter variant. All [M6 compatibility
restrictions](m6.md#remaining-restrictions-and-safety-deviations), local hosting,
weak ownership, cancellation and non-lease service-value contracts remain.
