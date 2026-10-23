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

## M6 current boundary

The unpublished core includes [M6 logging/injection/type/example acceptance](m6.md),
[M5 scoped reflection/config updates](m5.md),
[M4 typed services/injection](m4.md),
[M3 base plugin lifecycle/Registry](m3.md), and Fiber-scoped
[effects, typed events, and diagnostics](m2.md) on the [M1 host](m1.md).
The final audit accounts for all pinned declarations through integrated tests
or explicit Rust representations. This is not unrestricted TS compatibility;
the [M6 restriction/deviation list](m6.md#remaining-restrictions-and-safety-deviations)
is part of the supported contract.

The post-M6 [lifecycle-parity slices](lifecycle-parity.md) add cross-language
execution for six gated provider/config/cleanup/failure/recovery scenarios only.
They do not establish full upstream suite parity, transparent JavaScript compatibility,
or reproducibility of upstream's entire JavaScript dependency graph (the pin
does not track a Yarn lockfile). No production API/behavior was changed.
In both implementations, consumer update settlement may be Pending before
a later provider arrival. It is not a runtime-wide readiness guarantee.
Three failure/recovery compositions now have paired execution: obsolete/latest
load failures with gated rollback and exactly-once diagnostics, an abandoned
update waiter, and explicit gated recovery after ignored provider refresh.
This is not exhaustive failure/recovery, cancellation, observer-error, or
exporter-error protocol parity. No further slice or satellite is selected.

- Services use explicit `ServiceKey<T>` access, not JS properties. M5 supports
  isolated slots with retained boundary history and explicit `BoundService`
  adapters carrying separate definition/use Contexts. An ordinary `Rc<T>`
  cannot transparently rebind arbitrary Rust methods. The two realm-dependent
  service-disposal declarations now have integrated evidence.
- Reads return `Rc<T>` snapshots of a publication's current value. Holding a
  returned value is not a lease on the provider's effects/resources; only
  committed injection retains those through cleanup. Existing `Rc<T>` reads
  do not change when set replaces the stored value.
- Injection declares required names/types, typed config metadata and synchronous
  provider checks. Intercepts merge through an explicit service-specific callback.
  Automatic Service.init/constructor discovery is not implemented; explicit
  initialized factories provide the initialization barrier and method-injection
  adapters replace decorators. `set` preserves publication
  identity and does not notify dependencies or recheck readiness.

- Plugin declarations are immutable and reusable with explicit allocation
  identity; JS function/object/constructor behavior is adapted to typed callbacks.
  Apply registers effects rather than implicitly interpreting arbitrary return
  shapes. Dynamic callback metadata mutation is not implemented.
- Missing dependencies now produce Pending without validation; exact publication
  replacements drive unload/reload. Typed status-event sequences are now tested.
  Failed Fibers stay resident and recover only through config update.
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
- Diagnostic logging has its bounded buffer/exporters, traced normalized names,
  typed intercepts, owner IDs and expiring weak lifecycle metadata. Explicit
  ErrorValue trees represent cause/aggregate metadata. Rust formatting/exporter
  callbacks replace dynamic JS formatter dictionaries and Error properties.
  Per-line formatting preserves Unicode scalars rather than lone surrogates.
- Context is non-owning: after Runtime drop, metadata/IDs are readable but
  operations return `RuntimeDropped`. Arbitrary user-created Rc cycles are
  not collected. Retained Fiber handles can retain source/metadata and pending
  cleanup captures until dropped; host destruction does not run async cleanup.
  Tracing requires explicit bound adapters; native property/method interception
  and automatic tracing of arbitrary returned objects are not provided.
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
  M5 adds typed internal protocols and explicit bound service views, while native
  lazy futures still require eager-prefix adapters where immediate execution
  matters. See design gates A and C in the
  [implementation plan](implementation-plan.md).
- **Ownership and teardown.** Rust Drop cannot await cleanup. M3 implements
  parent-owned children and reusable-root disposal with weak backreferences.
  M4 keeps exact provider bindings usable through dependent cleanup, even
  after consumers leave the Registry, and preserves unrelated concurrent
  cleanup. M5 covers the isolation-dependent retention cases too; remaining
  syntax/tracing representation differences are explicit in its contract.
- **Definition site versus use site.** Typed shared service pointers alone
  cannot retain upstream shadow/caller tracing. Dependency authority,
  use-site intercept/effect behavior and nested service calls use M5's bound
  adapters, not transparent pointer reads. Re-selecting the same
  isolation label is still an injection-inheritance boundary; retaining only
  final realm values loses that information.
- **Config validation.** TS Cordis uses Standard Schema for plugin config
  validation (`ValidationError`) and rejects asynchronous validators. Source
  and resolved config differ, validation is deferred while Pending, and
  invalid updates to loaded Fibers preserve the running configuration. M3 has
  synchronous typed validation/source separation, and M4 defers it until
  dependencies resolve. M5 adds typed updates/recovery and structured issue paths.
  Automatic schema discovery remains unsupported; serde is not an
  automatic core dependency.
- **Logging representation.** Use Rust Display/Debug, ErrorValue metadata and
  exporter callbacks. Arbitrary Error::source graphs require explicit owned
  cause adaptation; `.error(Display)` does not inspect dynamic properties.
  Callable logger syntax and JS printf/JSON argument semantics are not promised.
- **Error model.** Original Rust error sources and typed validation paths are
  retained. JS stack-string rewriting and hostile destructor recovery are not
  implemented. Typed config middleware cannot replace the validator's Rust
  contract, and virtual get-only values have no publication identity for bind.

## Scope limitations (current)

- Approved [GitHub activity consumers](github-activity.md) are application
  examples, not production satellites. The digest supports bounded GitHub.com
  snapshots and programmatic updates/recovery; its CLI is one-shot. Atomic
  replacement is not crash durability or multi-process coordination.
  Collection and signed loopback webhook journaling remain the next approved
  slices. Public ingress, live webhook delivery, mutations, and durable/exactly-once
  delivery are excluded.
- Only `cordis-core` is in scope initially; satellite packages are deferred.
- No stability guarantees — the API will change freely until the core is proven.
- `spikes/m0` uses separate test-only models: one dependency per lifecycle
  Fiber, synchronous setup, and no integrated update/recovery/logger protocol.
  Its passing tests are design evidence, not production parity coverage.

This document should shrink as tensions are resolved (move resolved items into
`docs/decision.md`) and grow as new ones surface.

Core safety deviations: observer hook failures are diagnosed without interrupting
retained-provider cleanup; shared-slot aliases also receive dependency refresh.
These strengthen error/teardown safety rather than claiming identical upstream
observer-failure or name-only alias notification behavior.
