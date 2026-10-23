# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project does not yet follow semantic versioning — the API is unstable.

## [Unreleased]

### Added
- First approved consuming-application slice: a Cordis-hosted GitHub snapshot
  digest with deterministic escaped Markdown, bounded validated input,
  atomic report replacement, provider reconfiguration and explicit recovery.
  Three application integration tests reuse the executable. JSON dependencies
  and filesystem/I/O features are example/test-only; production core is unchanged.
- Approved bounded failure/recovery parity: three additional paired Rust/TS
  scenarios prove superseded-load rollback without poisoning the latest target,
  latest-failure delivery after gated rollback despite an abandoned update,
  and explicit gated recovery while Failed consumers ignore provider refresh.
- Six paired scenarios now compare 73 observations (the original 37 unchanged).
  Original per-language error sources and exactly-once diagnostics are checked;
  the Rust baseline is 152 integration tests and 17 Rustdoc checks. Production
  code, public APIs, dependencies, and Rust 1.85 remain unchanged.
- Integrated-checkpoint handoff documentation distinguishing completed M6 and
  lifecycle-parity evidence from the then-proposed failure/recovery slice.
- Post-M6 executable lifecycle parity: three paired Rust/pinned-TypeScript
  scenarios covering provider/config updates, latest-target convergence during
  gated cleanup, and Registry-removed consumer retention/terminal disposal.
- Shared normalized trace expectations, a bounded pin-checking comparison
  command using upstream Yarn/Vitest, and explicit Pending-versus-readiness
  evidence. Three new Rust tests bring the workspace to 149 integration tests;
  17 Rustdoc checks, production behavior, dependencies and Rust 1.85 stay unchanged.
- M6 logger intercept/name/verbosity precedence, nested service attribution,
  normalized names, inspectable weak Fiber metadata, explicit cause/aggregate
  error values, exact error identities, bounded per-line formatting, and ANSI palettes.
- Explicit initialized-plugin and method-injection adapters, registered-symbol/
  callback/source/accessor/invocation compile-time checks, and an end-to-end
  isolated lifecycle/configuration example exercised by integration tests.
- M6 parity/deviation audit and 18 new integration cases (146 total), plus
  seven additional Rustdoc checks (17 total). Rust 1.85 and existing dependencies
  remain unchanged; no proc macros, satellite crates, or schema dependencies.
- M5 isolation identities and boundary history, scoped typed publications,
  explicit definition/use-site bound calls, intercept merging, event filtering,
  typed accessor/mixin/associated/callable equivalents, and captured-holder tracing.
- Typed internal hook contracts and exact listener replacement, scoped update
  middleware with local/global ordering, source updates/noSave/recovery,
  structured validation issues, and latest-config convergence.
- Obsolete lifecycle targets stop effect-stream advancement while retaining the
  in-flight final acquisition and its cleanup. Automatic diagnostics correlate
  propagated lifecycle failures without duplicate reports.
- M5 contracts and 28 additional integration tests (128 total at this boundary),
  retaining the ten Rustdoc checks and Rust 1.85 minimum without new dependencies.
- M4 typed named service publication, owned set/withdrawal, checked/direct lookup,
  explicit duplicate-provider/type/access errors, and synchronous readiness checks.
- Per-Fiber injection declarations, dependency-driven Pending/Active convergence,
  exact committed publication snapshots, and replacement/restart coalescing.
- Provider-resource retention through removed-consumer, transitive, child, setup,
  rollback, and final-event cleanup, without blocking unrelated owners or effects.
- M4 contracts and source-case evidence; 28 new deterministic integration tests
  bring the production suite to 100 tests, with ten Rustdoc checks.

- M3 typed reusable plugins and Registry grouping, per-load synchronous validation
  with retained source/resolved types, and supervised non-root Fiber lifecycle.
- Fiber-owned effects/listeners/exporters, parent-owned child disposal, restart
  coalescing, exact Registry group removal/pruning, lifecycle completion handles,
  and inactive Context/self-or-ancestor wait refusal.
- Current Fiber names/diagnostic attribution, lifecycle/type-safety regressions,
  M3 contracts, and source-case coverage (46 pinned contracts/equivalents total).
- Synchronous disposer/registration APIs with immediate manual cleanup and
  all-sync setup rollback, preserving async barriers and concurrent root drains.
- Effect labels and value-only nested tree introspection, including async
  acquisition updates, scoped listener/exporter labels, and recursive-transfer
  refusal before ownership changes; root M2 contracts are now complete.
- Root effect factories, iterator/async/stream setup, partial rollback, readiness,
  and exact registration transfer into reverse sequential composition.
- Typed named/symbol events, five dispatch modes, explicit callback roles,
  snapshot filters/prepend/global/once, scoped cleanup, supervised dispatch
  completion, and owned one-use continuations.
- Structured diagnostic logger, default 1000-record buffer, scoped exporters
  with independent verbosity routing, and source-preserving automatic diagnostics.
- M2 API/compatibility notes and integrated evidence for 32 pinned root cases.
- Rust 2024/resolver-3 production workspace containing unpublished `cordis-core`,
  with Rust 1.85 minimum and MIT licensing.
- Explicit local runtime hosting, weak Context views and typed metadata ancestry,
  opaque runtime/root identities, root-owned cleanup, eager completion handles,
  and preserved error-source reporting with supervised task failures.
- Integrated foundation/type-safety checks and stable/MSRV CI, plus M1 contracts.
- Isolated Rust 2024 M0 design spikes for lifecycle ownership, exact service
  binding retention, typed traced calls, events/continuations, and config
  validation; executable on Rust 1.85.0 and current stable.
- M0 results, explicit API restrictions, and a complete pinned core case checklist.
- Source-backed core implementation plan in `docs/implementation-plan.md`,
  with pinned TypeScript/prior Rust references, independent agent reviews,
  design gates, staged delivery, and behavioral acceptance criteria; aligned
  roadmap and known limitations.
- Rust edition 2024 baseline for the planned Cargo workspace.
- Initial project documentation: `AGENTS.md`, `README.md`, and
  `docs/{todo,decision,limitation}.md`.

### Fixed
- Service availability hooks notify current publications exactly once per
  transition, without treating retired bindings or an already-inactive owner's
  journal pause as a new availability change.
- Lifecycle disposal joining an earlier journal drain also claims later-admitted
  effects before releasing committed providers; reusable manual drains remain snapshots.
- Lifecycle drains no longer retain the local executor across dependent cleanup
  waits, so host destruction cancels those waits rather than retaining the host.
