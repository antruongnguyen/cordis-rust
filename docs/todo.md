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

### Proposed next slice (requires scope approval)

- [ ] Paired failure/recovery evidence using the existing lifecycle-parity
  fixtures: superseded failure rolls back without poisoning the latest target;
  latest failure reaches all waiters while abandoned waits do not hide
  diagnostics; provider refresh does not retry Failed consumers, but explicit
  update can recover through gated asynchronous setup.

This is a recommendation, not approved implementation work or a completed
upstream protocol. Rust-only tests already cover these areas; the gap is paired
execution. Keep production changes conditional on an evidenced mismatch, avoid
new dependencies or a generic harness, and retain all acceptance checks.
After a bounded slice, prefer a concrete consuming application's requirements
over indefinite parity expansion or speculative satellite scaffolding.

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
