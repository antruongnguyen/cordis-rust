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

- **Async model.** The TS core is promise-based (`parallel`, `serial`,
  `waterfall` return promises). Whether/which async runtime the Rust port
  adopts is undecided. See `docs/todo.md`.
- **Config validation.** TS Cordis uses Standard Schema for plugin config
  validation (`ValidationError`). The Rust equivalent (serde? a schema crate?
  custom?) is undecided.
- **Error model.** How faithfully to reproduce Cordis's error enhancement and
  stack-trace handling in Rust is open.

## Scope limitations (current)

- Only `cordis-core` is in scope initially; satellite packages are deferred.
- No stability guarantees — the API will change freely until the core is proven.

This document should shrink as tensions are resolved (move resolved items into
`docs/decision.md`) and grow as new ones surface.
