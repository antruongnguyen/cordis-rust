# Roadmap / TODO

A living plan for porting [Cordis](https://github.com/cordiverse/cordis) to
Rust. The source of truth is the TypeScript `cordis` core
(`local/cordis/packages/core/src/`). This list will be reordered and refined as
we learn; it is not a contract.

## Phase 0 — Project setup

- [x] Initialize documentation (`AGENTS.md`, `README.md`, `docs/*`).
- [ ] Choose a license (upstream is MIT).
- [ ] Scaffold the Cargo workspace with an empty `cordis-core` crate.

## Phase 1 — Core concepts (`cordis-core`)

Ordered roughly by dependency, following the TS core modules. Each item should
land with its semantics documented and tested against the behavior of the TS
original.

- [ ] **Context** — the traceable runtime view; `extend` to derive child views.
- [ ] **Reflect** — `get` / `set` / `provide` / `mixin` over a Context, and the
  isolation model. (The TS `Proxy` has no Rust equivalent; design required.)
- [ ] **Registry** — plugin registration and `inject` dependency resolution.
- [ ] **Service** — typed named capabilities, isolation, intercept config.
- [ ] **Fiber** — lifecycle state machine, config validation, disposables, and
  deterministic cleanup. (Largest, most central module — ~530 LOC in TS.)
- [ ] **Events** — dispatch modes (`emit`, `parallel`, `serial`, `bail`,
  `waterfall`), scoped hooks, bail semantics.
- [ ] **Logger** — context-aware structured logging.
- [ ] `isolate` / `intercept` scoping semantics proven across the above.

## Phase 2 — Satellite packages

Only after the core is solid. Mirror the original `cordis` packages:

- [ ] `cordis-loader` — plugin loading.
- [ ] `cordis-timer` — timer service.
- [ ] `cordis-logger` (console logger).
- [ ] Others as they prove necessary (`group`, `hmr`, `include`, `create`).

## Cross-cutting

- [ ] A documented mapping from each TS concept to its Rust expression.
- [ ] Testing strategy that validates behavior against the TS original.
- [ ] Decide async runtime story (if any) — the TS model is promise-based.
