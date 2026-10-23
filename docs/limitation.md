# Limitations & Open Tensions

Known gaps, unresolved questions, and places where a faithful port is hard.
This is where we are honest about what does *not* translate cleanly from the
TypeScript original to Rust.

## JavaScript-only mechanisms with no Rust equivalent

The original Cordis core derives much of its ergonomics from features Rust does
not have. These require real design, not translation:

- **Proxy-based reflection.** In TS, a `Context` is a `Proxy` (`ReflectService`)
  that intercepts property access to resolve services, apply isolation, and
  trace callers. Rust has no runtime property interception. The `get` / `set` /
  `provide` / `mixin` surface must be re-expressed through explicit APIs,
  traits, or a typed registry.
- **Prototype-chain inheritance.** `isolate` and `intercept` build scoped child
  contexts by creating objects whose prototype is the parent, so lookups walk
  the chain. Rust has no prototype chain; scope inheritance must be modeled
  explicitly (e.g. parent links, layered maps).
- **Callable service objects.** A TS `Service` can itself be callable (a
  function with properties). Rust closures/`Fn` traits can approximate this but
  not with the same transparency.
- **Dynamic, string-keyed, heterogeneously-typed services.** TS stores services
  in string-keyed dictionaries with types attached via declaration merging.
  Rust needs a typed strategy (type-erased storage, downcasting, or a
  trait-object registry) to recover both dynamism and type safety.

## M3 current boundary

The unpublished core includes [M3 base plugin lifecycle/Registry](m3.md) and
Fiber-scoped [effects, typed events, and diagnostics](m2.md) on the [M1 host](m1.md).
There is no service lookup/injection/convergence or isolation/intercept API,
configuration update/recovery, or complete internal lifecycle/update hook protocol.

- Plugin declarations are immutable and reusable with explicit allocation
  identity; JS function/object/constructor behavior is adapted to typed callbacks.
  Apply registers effects rather than implicitly interpreting arbitrary return
  shapes. Dynamic callback metadata mutation and automatic Service.init are
  not implemented.
- Pending is an initial vocabulary position; dependency-driven Pending/Active
  transitions need M4. State snapshots are not internal/status event evidence.
  Failed Fibers stay resident until disposal; config-update recovery needs M5.
- Restart/dispose self-waits and owning-ancestor waits are refused from directly
  attributed callbacks. User-spawned task graphs do not gain that attribution
  or general deadlock detection automatically.

- Synchronous cleanup is explicit through `Disposer::sync`/`on_dispose_sync`.
  All-sync manual chains/partial rollback complete before returning; an async
  child remains an awaited barrier. Async setup still uses readiness/cleanup
  futures. Labels and tree snapshots contain only metadata, not resources.
- Recursive self/ancestor composition transfers are refused before claiming
  their root registration, avoiding a self-wait or metadata reference cycle.
- Listener responder and around callback roles are explicit. Bail is sync-only;
  sync and async waterfall have separate owned continuation types. Wrong mode
  or live name/type contracts return errors rather than silently skipping hooks.
- Diagnostic logging has its bounded buffer/exporters, raw Fiber names, and
  owner IDs. Traced service names, plugin-name normalization, intercept defaults,
  JS printf/color formatting, and
  cause/aggregate flattening remain M5/M6.
- Context is non-owning: after Runtime drop, metadata/IDs are readable but
  operations return `RuntimeDropped`. Arbitrary user-created Rc cycles are
  not collected. Retained Fiber handles can retain source/metadata and pending
  cleanup captures until dropped; host destruction does not run async cleanup.
  Full definition-site ancestry is still M5 work.
- Only its explicit run scope admits work. Missing/wrong hosts, nested or
  concurrent driving, and hosting inside spawned tasks are refused. Pausing
  a run pauses local tasks; host destruction is cancellation, not cleanup.
- Root drain covers a snapshot, not an executor-wide shutdown barrier. Later
  registrations require another pass; already-claimed manual cleanup has
  its own completion handle that must be awaited separately.
- User callback failures preserve their sources and are reported/retained;
  diagnostic sinks do not imply full Logger parity. Panic-abort, panicking
  destructors, and framework invariant failures cannot promise completion.

## M0 boundaries selected for the initial implementation

The [M0 experiments](m0.md) select local-first Tokio/LocalSet execution,
non-Send handles/futures, explicit definition/use-bound calls, synchronous
validation, and owned one-use continuations. They do not constitute an
integrated framework or resolve every behavioral gap.

- Started operations outlive dropped waiters only while the host executor
  remains driven. Shutdown is not async cleanup; explicitly await teardown.
- An async emit listener needs an explicit eager-prefix/continuation adapter.
  Passing a native async closure does not automatically run its body at call time.
- Bail is synchronous-only; async responses use serial. Mixed bail snapshots
  are refused before invocation rather than mimicking JS promise-valued results.
- Duplicate next is a move error, not an upstream-style runtime exception.
- Name/type or cross-name shared-label type mismatches return explicit errors;
  unchecked JS heterogeneous aliasing is not a Rust compatibility promise.
- Same-owner lifecycle requests during cleanup are explicitly refused. Task-local
  attribution does not propagate through arbitrary user-spawned tasks. M3 covers
  setup/restart/dispose and owning-ancestor refusal; update is still M5 work.

## Unresolved design questions

- **Async model.** `parallel` and `serial` return promises; `emit` and `bail`
  dispatch synchronously, and continuation-based `waterfall` returns whatever
  its middleware chain returns (including promises). Rust futures are lazy,
  so returning or dropping an unpolled future does not reproduce JS start
  behavior. M3 integrates base non-root lifecycle and per-Fiber ownership;
  complete internal event/config behavior and service convergence remain future
  work. See design gates A and C in the
  [implementation plan](implementation-plan.md).
- **Ownership and teardown.** Rust Drop cannot await cleanup. M3 implements
  parent-owned children and reusable-root disposal with weak backreferences.
  M4 must keep exact provider bindings usable through dependent cleanup, even
  after consumers leave the Registry. Top-level
  cleanup is concurrent upstream; a universal sequential LIFO drain is not
  equivalent. These are requirements, not accepted omissions.
- **Definition site versus use site.** Typed shared service pointers alone
  cannot retain upstream shadow/caller tracing. Dependency authority,
  use-site intercept/effect behavior, nested service calls, and explicit
  accessor/mixin equivalents need a bound-call design. Re-selecting the same
  isolation label is still an injection-inheritance boundary; retaining only
  final realm values loses that information.
- **Config validation.** TS Cordis uses Standard Schema for plugin config
  validation (`ValidationError`) and rejects asynchronous validators. Source
  and resolved config differ, validation is deferred while Pending, and
  invalid updates to loaded Fibers preserve the running configuration. M3 has
  synchronous typed validation/source separation but no update or dependency
  deferral API yet. Schema integration remains undecided; serde is not an
  automatic core dependency.
- **Logging representation.** Default buffering, exporter lifetimes, caller
  naming, and error aggregation are core behavior. JS formatters, callable
  logger syntax, and weak Fiber metadata still need explicit Rust equivalents.
- **Error model.** How faithfully to reproduce Cordis's error enhancement and
  stack-trace handling in Rust is open.

## Scope limitations (current)

- Only `cordis-core` is in scope initially; satellite packages are deferred.
- No stability guarantees — the API will change freely until the core is proven.
- `spikes/m0` uses separate test-only models: one dependency per lifecycle
  Fiber, synchronous setup, and no integrated update/recovery/logger protocol.
  Its passing tests are design evidence, not production parity coverage.

This document should shrink as tensions are resolved (move resolved items into
`docs/decision.md`) and grow as new ones surface.
