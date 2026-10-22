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
- [ ] M0: Turn pinned TS behaviors into a case-by-case checklist and resolve
  async/ownership/cancellation, typed access/tracing, and event/config design
  gates with runnable spikes. Record explicit deviations before implementation.
- [ ] Choose a license (upstream is MIT).
- [ ] M1: Scaffold only `cordis-core` in a Cargo workspace (edition 2024,
  resolver 3); declare and test the actual MSRV, add CI, and implement the
  minimal Context/root/effect foundations.

## Core delivery (`cordis-core`)

M2-M6 depend on their predecessor; see the implementation plan for the
additional design gates and exact tests. Each slice lands with documentation
and behavioral tests, not just public type declarations.

- [ ] M2: Effects, all five event modes, scoped listener cleanup, filtering,
  and diagnostic core logging. Preserve concurrent top-level versus sequential
  composed cleanup and continuation-based waterfall.
- [ ] M3: Registry identity/grouping, base Fiber states, config validation,
  restart/disposal, parent-owned child cleanup, and reusable-root disposal.
- [ ] M4: Reflect/Service typed publication and lookup, injection resolution,
  Pending/Active convergence, committed snapshots, and exact-provider retention
  through consumer cleanup (including consumers removed from the Registry).
- [ ] M5: Isolation boundaries, intercepts, definition/use-site tracing,
  explicit accessor/mixin/associated/callable equivalents, internal hooks,
  update middleware, and concurrent latest-config convergence.
- [ ] M6: Complete core Logger behavior, decorator-injection equivalents,
  compile-time API checks, an end-to-end example, and a parity/deviation review.

## Satellite packages

Only after M6. Choose the next package from concrete application needs:

- [ ] `cordis-timer` — timer service.
- [ ] `cordis-logger` — console exporter; core logging is already part of M2-M6.
- [ ] `cordis-loader` — plugin loading/configuration, not assumed native hot reload.
- [ ] Others as they prove necessary (`group`, `hmr`, `include`, `create`).

## Cross-cutting

- [x] Initial TS-to-Rust mapping and source-backed testing strategy documented.
- [ ] Maintain case-level parity evidence and explicit limitations as code lands.
- [ ] Prove error reporting, dropped-waiter behavior, and resource retention
  without strong-reference leaks; do not equate Rust Drop with async cleanup.
- [ ] Run targeted tests per slice, then full workspace/doc tests, lint, and
  declared-MSRV/current-stable checks before core acceptance.
