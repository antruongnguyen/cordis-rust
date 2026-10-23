# cordis-rust

A Rust reimplementation of [Cordis](https://github.com/cordiverse/cordis) — a
meta-framework for spatiotemporal composability.

> **Status: M5 explicit scoped reflection and config updates implemented.**
> The unpublished `cordis-core` has local
> hosting, typed plugins/Registry grouping, scoped effects and diagnostics,
> typed events, validation, restart, parent-owned disposal, typed service
> publication/lookup, and dependency-driven injection convergence.
> It also has isolation boundaries, explicit bound service calls/intercepts,
> typed reflection/hooks, config updates/recovery, and latest-target convergence.
> Complete logging and the final parity/deviation review remain M6;
> transparent JavaScript Proxy/decorator syntax is not a Rust API promise.
> The concept map below remains the framework target, not a shipped feature list.

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

## Workspace and planned layout

A Cargo workspace mirroring the original `cordis` packages, built up in stages:

```
crates/
  cordis-core      # Plugins/Fibers/Context/services/injection/effects/events/diagnostics
  cordis-loader    # (later) plugin loading
  cordis-timer     # (later) timer service
  cordis-logger    # (later) console logger
  ...
```

Only `cordis-core` is in scope first. The rest are planned, not committed. See
the [implementation plan](docs/implementation-plan.md) for the source review,
Rust design gates, milestone dependencies, and acceptance criteria.

## Build and develop

Requires Rust **1.85 or newer**, edition **2024**. The root workspace uses
Cargo resolver 3. From the repository root:

```sh
cargo test --workspace --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

See [M5 scope/update APIs](docs/m5.md), [M4 service APIs](docs/m4.md), [M3 lifecycle APIs](docs/m3.md),
[M2 APIs](docs/m2.md), and the examples in the
[`cordis-core` crate docs](crates/cordis-core/src/lib.rs). The runtime must be
explicitly driven from Tokio; Context values do not keep their host alive.
CI covers Rust 1.85.0 and stable, including the independent M0 experiments.

## M0 experiments

Run the unpublished design spikes independently of the production workspace:

```sh
cargo test --manifest-path spikes/m0/Cargo.toml --locked
```

See [M0 results](docs/m0.md) for evidence, selected design directions, and limits.

## Documentation

- [`docs/m5.md`](docs/m5.md) — isolation, bound calls, typed reflection/hooks, and configuration convergence.
- [`docs/m4.md`](docs/m4.md) — typed services, checked/direct lookup, injection convergence, and exact-provider retention.
- [`docs/m3.md`](docs/m3.md) — typed plugins, Registry, scoped resources, and base lifecycle.
- [`docs/m1.md`](docs/m1.md) — supported foundation APIs, ownership, and limits.
- [`docs/m2.md`](docs/m2.md) — effect/event/diagnostic APIs and source-case coverage.
- [`docs/m0.md`](docs/m0.md) — executable design-spike findings and M1 handoff.
- [`docs/m0-cases.md`](docs/m0-cases.md) — pinned case-by-case behavioral checklist.
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

MIT; see [LICENSE](LICENSE). The TypeScript upstream is also MIT-licensed.
