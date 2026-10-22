# cordis-rust

A Rust reimplementation of [Cordis](https://github.com/cordiverse/cordis) — a
meta-framework for spatiotemporal composability.

> **Status: greenfield.** This repository currently contains documentation
> only. There is no published crate, no build, and no install instructions yet.
> This README describes *intent and plan*, not shipped functionality.

## Why

Cordis is a TypeScript meta-framework for building long-lived, plugin-oriented
applications: components (plugins) can load and unload at runtime, services can
appear and disappear, configuration can change, and resources must still be
cleaned up deterministically.

This project reimplements that model in Rust, with the **TypeScript `cordis` as
the source of truth**. A separate, independent Rust port already exists
([dshbox/cordis-rs](https://github.com/dshbox/cordis-rs)); we treat it as a
reference point for comparison, not as a target to match. The aim here is to
understand the original framework deeply enough to re-express its semantics
faithfully in idiomatic Rust.

## Concept map

These are the core concepts of the original Cordis (`packages/core`) that this
port will mirror. Names and shapes may be adapted to Rust idioms; the semantics
are the contract.

| Concept      | What it does |
|--------------|--------------|
| **Context**  | A cheap, traceable view into one runtime. Exposes the core services and derives scoped child views via `extend` / `isolate` / `intercept`. |
| **Service**  | A typed, named capability provided into the runtime, with isolation and per-context configuration (intercept). |
| **Fiber**    | The lifecycle unit — one plugin instance: config validation, disposables, and a state machine governing load/unload. |
| **Registry** | Plugin registration and dependency (`inject`) resolution. |
| **Reflect**  | The reflection layer powering `get` / `set` / `provide` / `mixin` on a Context. (In TS this is a `Proxy`; in Rust it will be expressed differently — see limitations.) |
| **Events**   | A typed event system with multiple dispatch modes (`emit`, `parallel`, `serial`, `bail`, `waterfall`) and scoped hooks. |
| **Logger**   | Structured, context-aware logging. |

## Goals

- Faithfully mirror the **semantics** of TypeScript Cordis core.
- Idiomatic, safe Rust — no attempt to transliterate JS-only mechanisms.
- Rust edition **2024** as the initial baseline; no older edition.
- Deterministic lifecycle and resource cleanup.
- A clear mapping, documented, from each TS concept to its Rust expression.

## Non-goals (for now)

- Matching the exact API surface of `dshbox/cordis-rs`.
- Feature parity with every satellite Cordis package before the core is solid.
- A stable public API — this will change freely until the core is proven.

## Planned layout

A Cargo workspace mirroring the original `cordis` packages, built up in stages:

```
crates/
  cordis-core      # Context, Service, Fiber, Registry, Reflect, Events, Logger
  cordis-loader    # (later) plugin loading
  cordis-timer     # (later) timer service
  cordis-logger    # (later) console logger
  ...
```

Only `cordis-core` is in scope first. The rest are planned, not committed. See
the [implementation plan](docs/implementation-plan.md) for the source review,
Rust design gates, milestone dependencies, and acceptance criteria.

## Documentation

- [`docs/implementation-plan.md`](docs/implementation-plan.md) — source-backed
  core implementation plan, reference comparison, and validation strategy.
- [`docs/todo.md`](docs/todo.md) — the porting roadmap.
- [`docs/decision.md`](docs/decision.md) — decisions and their rationale.
- [`docs/limitation.md`](docs/limitation.md) — known gaps and open tensions.
- [`AGENTS.md`](AGENTS.md) — conventions for contributors and agents.
- [`CHANGELOG.md`](CHANGELOG.md) — notable changes.

## References

- [cordiverse/cordis](https://github.com/cordiverse/cordis) — the original
  TypeScript framework (the spec).
- [dshbox/cordis-rs](https://github.com/dshbox/cordis-rs) — an independent prior
  Rust port (reference only).

## License

Not yet chosen. The upstream Cordis is MIT-licensed.
