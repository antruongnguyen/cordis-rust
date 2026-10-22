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

## Unresolved design questions

- **Async model.** `parallel` and `serial` return promises; `emit` and `bail`
  dispatch synchronously, and continuation-based `waterfall` returns whatever
  its middleware chain returns (including promises). Rust futures are lazy,
  so returning or dropping an unpolled future does not reproduce JS start
  behavior. Executor, Send/Sync guarantees, started-operation cancellation,
  reentrancy, and panic policy remain open; see design gates A and C in the
  [implementation plan](implementation-plan.md).
- **Ownership and teardown.** Rust Drop cannot await cleanup. Parent-owned
  child Fibers and reusable-root disposal must survive the translation without
  strong-reference cycles. Exact provider bindings must remain usable through
  dependent cleanup, even after consumers leave the Registry. Top-level
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
  invalid updates to loaded Fibers preserve the running configuration. The
  typed validator/schema integration remains undecided; serde is not an
  automatic core dependency.
- **Logging representation.** Default buffering, exporter lifetimes, caller
  naming, and error aggregation are core behavior. JS formatters, callable
  logger syntax, and weak Fiber metadata still need explicit Rust equivalents.
- **Error model.** How faithfully to reproduce Cordis's error enhancement and
  stack-trace handling in Rust is open.

## Scope limitations (current)

- Only `cordis-core` is in scope initially; satellite packages are deferred.
- No stability guarantees — the API will change freely until the core is proven.

This document should shrink as tensions are resolved (move resolved items into
`docs/decision.md`) and grow as new ones surface.
