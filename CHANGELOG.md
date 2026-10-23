# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project does not yet follow semantic versioning — the API is unstable.

## [Unreleased]

### Added
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
