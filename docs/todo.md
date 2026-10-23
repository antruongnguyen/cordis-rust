# Roadmap / TODO

A living plan for porting [Cordis](https://github.com/cordiverse/cordis) to
Rust. The source of truth is the TypeScript `cordis` core
(`local/cordis/packages/core/src/`). This list will be reordered and refined as
we learn; it is not a contract. Detailed source evidence, design gates,
milestone prerequisites, and exit criteria live in the
[implementation plan](implementation-plan.md).

## Planning and project setup

- [x] Initialize documentation (`AGENTS.md`, `README.md`, `docs/*`).
- [x] Review and pin both references, including independent read-only agent
  reviews; write and source-check the initial core implementation plan.
- [x] Select Rust edition 2024 as the initial baseline (no older edition).
- [x] M0: Inventory all pinned core test declarations and exercise lifecycle,
  typed access/tracing, and event/config API candidates in `spikes/m0`.
  Record local-first execution, cancellation/dispatch restrictions, and the
  production handoff in [m0.md](m0.md); parity cases remain pending M2-M6.
- [x] Choose MIT licensing (see `LICENSE`).
- [x] M1: Add only `cordis-core` to the root Cargo workspace (edition 2024,
  resolver 3, Rust 1.85 minimum), explicit local hosting, Context/metadata/root
  cleanup foundations, integrated ownership/error checks, and stable/MSRV CI.
  Supported APIs and scope are documented in [m1.md](m1.md).

## Core delivery (`cordis-core`)

M2-M6 depend on their predecessor; see the implementation plan for the
additional design gates and exact tests. Each slice lands with documentation
and behavioral tests, not just public type declarations.

- [x] M2 (root scope): Effect setup/rollback/composition, synchronous disposal,
  labels/tree introspection, five typed dispatch modes, listener snapshot
  filtering/cleanup, and core diagnostics. See [m2.md](m2.md) for contracts and
  explicit Rust equivalents. M3 now integrates plugin-owned disposal; the
  special internal-update listener protocol is implemented under M5. These are not
  claims of complete plugin/event/service suite parity.
- [x] M3 (base lifecycle): Registry identity/grouping, observable Fiber states,
  typed source/resolved validation, supervised restart/disposal, parent-owned
  child/listener/exporter cleanup, and reusable-root disposal; see [m3.md](m3.md).
  M4 adds dependency-driven Pending; M5 adds config updates/recovery and internal
  status/update hook protocols. Complete upstream suite parity is not claimed.
- [x] M4 (global slots): Reflect/Service typed publication and lookup, injection resolution,
  Pending/Active convergence, committed snapshots, and exact-provider retention
  through consumer cleanup (including consumers removed from the Registry).
  See [m4.md](m4.md). Isolation-dependent service cases and complete reflection/
  internal hook protocols remain assigned to M5/M6, not claimed as suite parity.
- [x] M5 (explicit Rust adapters): Isolation boundaries, intercepts, definition/use-site tracing,
  explicit accessor/mixin/associated/callable equivalents, internal hooks,
  update middleware, concurrent latest-config convergence, and the remaining
  isolation-dependent service-disposal cases. See [m5.md](m5.md) for contracts,
  safety deviations, and explicit replacements for JavaScript reflection.
  Remaining case-level evidence is reviewed under M6, not inferred from API names.
- [x] M6 (explicit Rust acceptance): Core Logger behavior, decorator-injection equivalents,
  compile-time API checks, an end-to-end example, and a parity/deviation review.
  See [m6.md](m6.md). Checked cases include documented Rust representations,
  not unrestricted JavaScript object/property/reflection compatibility.

## Satellite packages

Only after M6. Choose the next package from concrete application needs:

- [ ] `cordis-timer` — timer service.
- [ ] `cordis-logger` — console exporter; core logging is already part of M2-M6.
- [ ] `cordis-loader` — plugin loading/configuration, not assumed native hot reload.
- [ ] Others as they prove necessary (`group`, `hmr`, `include`, `create`).

## Post-M6 core hardening

- [x] Approved focused lifecycle-parity slice: execute three equivalent
  Rust/TypeScript scenarios for concurrent provider/config updates, replacement
  coalescing during gated cleanup, and Registry-removed consumer disposal.
  Shared traces and readiness assertions are documented in
  [lifecycle-parity.md](lifecycle-parity.md). No production fix was required.
  This does not select a satellite or define a broader M7.

- [x] Approved bounded failure/recovery evidence using the existing lifecycle-parity
  fixtures: superseded failure rolls back without poisoning the latest target;
  latest failure reaches all waiters while abandoned waits do not hide
  diagnostics or cancel rollback; provider refresh does not retry Failed consumers,
  but explicit update recovers through gated asynchronous setup.
  Three new paired scenarios add 36 observations (six scenarios / 73 total).
  No production fix, new dependency, or public API change was required.

These are bounded executed compositions, not complete upstream failure/recovery
protocol parity. Both slices are complete; no further parity slice, satellite,
or broader M7 is selected. Retain all acceptance checks and require evidence
before changing production behavior.

## Approved first consumers: GitHub activity

The user selected repository activity reporting, collection, and webhook
journaling. Keep them thin application examples, with no production core change
unless an approved requirement demonstrates a gap. See
[github-activity.md](github-activity.md) for the bounded scope and limitations.

- [x] Snapshot-to-Markdown digest: validated bounded input, deterministic output,
  failure-safe replacement, destination updates and explicit processor recovery.
- [x] `gh` collector: bounded complete snapshots, timeouts, one-shot/periodic
  operation, reconfiguration and joined shutdown.
- [ ] Loopback webhook journal: raw-body signature verification, repository/event
  validation, limits, append-before-ack and graceful in-flight shutdown.

Public ingress, real GitHub webhook configuration/delivery, mutations,
notifications to other people and durable/exactly-once delivery are excluded.
These consumers are not a generic harness or a new satellite/M7 commitment.

## Cross-cutting

- [x] Initial TS-to-Rust mapping and source-backed testing strategy documented.
- [x] Maintain case-level parity evidence and explicit limitations through M6.
- [x] Prove error reporting, dropped-waiter behavior, and resource retention
  without strong-reference leaks; do not equate Rust Drop with async cleanup.
- [x] Run targeted tests per slice, then full workspace/doc tests, lint, and
  declared-MSRV/current-stable checks before core acceptance.

Future work must preserve these checks. Arbitrary user-created Rc cycles,
executor destruction, and the explicit compatibility restrictions in M6 are
not promises of automatic cleanup or full upstream syntax parity.
