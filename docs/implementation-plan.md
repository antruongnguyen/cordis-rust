# Cordis Rust implementation plan

Status: planning only; no framework code exists yet. Target the TypeScript
Cordis core first, using **Rust edition 2024**. A newer stable edition may be
adopted deliberately later; an older edition is out of scope.

## 1. Evidence and scope

Source review date: 2026-10-22. Pin acceptance criteria to these revisions so
later upstream changes cannot silently redefine the target.

| Reference | Reviewed revision | Role |
|---|---|---|
| [cordiverse/cordis](https://github.com/cordiverse/cordis/tree/35fdc969e4b6c48c48ed44717c7a018324e4dd88) | `35fdc969e4b6c48c48ed44717c7a018324e4dd88` (`cordis` package `4.0.0-rc.12`) | Behavioral specification |
| [dshbox/cordis-rs](https://github.com/dshbox/cordis-rs/tree/1754062cef67cfc119fc7a2084c0321a231b9092) | `1754062cef67cfc119fc7a2084c0321a231b9092` | Rust engineering reference, not a compatibility target |

`TS:` paths below are relative to `local/cordis/`; `RS:` paths are relative
to `local/cordis-rs/`. These ignored checkouts are research inputs, not future
build dependencies. Use the revision links above when they are unavailable.
The prior port's parity ledger targets an older TS revision,
`8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`; recheck its conclusions against
our newer pin rather than treating its parity classifications as current.

Two independent, read-only child agents reviewed the references separately.
Their recommendations were reconciled against the pinned implementations;
neither an agent report nor the prior port overrides the TS source.

Core scope is Context, reflection/service resolution, Registry, Fiber,
effects, events, and core logging together. Satellite crates, native dynamic
library loading, proc macros, and a stable public API are not prerequisites.
Do not scaffold them speculatively.

### Source map

| Concern | Implementation | Behavioral tests to port |
|---|---|---|
| Context construction and derivation | `TS:packages/core/src/context.ts` | `plugin.spec.ts`, `isolate.spec.ts`, `intercept.spec.ts` |
| Access, publication, dependency location | `TS:packages/core/src/reflect.ts` | `reflect.spec.ts`, `service.spec.ts`, `service-dispose.spec.ts` |
| Plugin identity, registration, injection | `TS:packages/core/src/registry.ts` | `plugin.spec.ts`, `decorator.spec.ts` |
| States, updates, setup, cleanup | `TS:packages/core/src/fiber.ts`, `TS:packages/core/src/utils.ts` | `fiber.spec.ts`, `update.spec.ts`, `dispose.spec.ts` |
| Dispatch and listener ownership | `TS:packages/core/src/events.ts` | `events.spec.ts`, `events.types.ts`, `isolate.spec.ts` |
| Caller tracing and service configuration | `TS:packages/core/src/service.ts`, `TS:packages/core/src/utils.ts` | `shadow.spec.ts`, `intercept.spec.ts`, `associate.spec.ts`, `invoke.spec.ts` |
| Core logging | `TS:packages/core/src/logger.ts` | `logger.spec.ts` |

All test filenames above live under `TS:packages/core/tests/`. When tests
leave a case unspecified, read the TS implementation rather than guessing
from the Rust port.

## 2. Findings that constrain implementation

### Context and services

- One root constructs the core services and an active root Fiber. Derived
  contexts share the runtime; `extend` inherits metadata, while `isolate`
  and `intercept` add shadowing layers.
- A service name selects an isolation identity; upstream stores publications
  by that identity, not by a Rust value type. Fresh isolation separates
  providers; an explicit shared label joins the selected slot without parent
  fallback. Check cross-name label reuse before choosing a typed storage key.
- Isolation also bounds access declarations. Even reselecting the same label
  blocks inheritance of an ancestor's injected view. A flat final realm map
  alone cannot represent this boundary.
- Checked property access walks the definition site's Fiber chain: matching
  own provisions first, then the nearest declaring Fiber's committed inject
  snapshot. Undeclared access and an inactive declaration are different errors.
  Direct `reflect.get`/`ctx.get` is a separate path with an active-provider
  check, not equivalent to checked property access.
  Root-defined access keeps its unchecked behavior; invoking a plugin-defined
  service method from root must not give that method the root bypass.
- Only the providing Fiber may set a publication. Duplicate live providers
  in a slot are errors. `set` changes the value, not publication identity,
  and does not itself trigger dependency notification.
- Definition and use sites remain distinct through nested service calls:
  dependency authority comes from the definition site; use-site isolation,
  intercepts, and effect attribution are traced through the call. A plain
  shared service pointer cannot express this alone.
- Intercepts merge the nearest admitting injection config first, then
  context layers outer-to-inner. Own provisions do not borrow injected config;
  services may override the merge operation.

### Fiber, Registry, and cleanup

- A plugin callback identifies a Registry runtime; repeated registrations
  create distinct Fibers. The first registration captures runtime name and
  validator, while inject declarations are per Fiber.
- Child plugin creation is an effect of its spawning Fiber. Parent unload
  cascades to children. Root disposal drains root-owned effects and children
  while leaving the root reusable; it is not permanent runtime shutdown.
  Upstream root has no plugin runtime, keeps identity `0`, and implements
  disposal through restart. Constructor-time core registrations are detached
  from its cleanup list without being run, so built-in hooks and the default
  logging buffer survive root disposal (`Context` constructor, `Fiber` constructor).
- Preserve Pending, Loading, Active, Failed, Unloading, and Disposed. Missing
  dependencies produce Pending, not failure. Registration admission checks
  live identity (`uid !== null` upstream), not simply Active state; setup
  must be able to register effects while Loading.
- Dependency targets include exact publication identities and a revision,
  not only names or value equality. Commit the dependency snapshot before
  apply and retain it through setup, rollback, and teardown.
- One lifecycle driver coalesces changes toward the latest target. Updates
  during load/unload may skip obsolete configurations. Explicit restart still
  matters when final config equals the old config.
  New targets do not interrupt an in-flight unload: finish its cleanup, then
  load only the latest target. `_setEpoch` updates intent while `inertia`
  remains the driver (`fiber.ts`; `update.spec.ts`, "coalesces updates during unloading").
- Failed Fibers do not retry on provider changes. `update` clears the stored
  error and enables recovery; do not import another retry policy.
- Withdrawal bars new consumers from the retired publication, then waits for
  its exact dependents, including those already removed from the Registry.
  Their cleanup may call the old provider. Provider resources must survive
  until dependent teardown completes.
- Top-level unload snapshots disposers in reverse registration order and
  invokes them concurrently, catching/logging individual failures. Composed
  effects instead clean up in reverse order and chain async disposers. A
  global sequential LIFO drain is not equivalent: unrelated providers and
  sibling effects are tested to progress concurrently.
- Setup can finish after its target changes. Collect its final acquired
  disposer, stop obsolete generator advancement, and clean up the old load
  without letting its failure poison the latest target.
- Validators are synchronous. Preserve source config separately from resolved
  config. Invalid loaded updates leave the old config running; Pending Fibers
  defer validation until reload. Update middleware sees resolved config and
  may suppress restart. `noSave` skips source-config commit notification,
  not the actual update.
  In `Fiber.update`, loaded-config validation runs before commit notification,
  source replacement, error reset, and update middleware. An invalid loaded
  candidate cannot reach that middleware or mutate the running source config
  (`fiber.spec.ts`, "keep a running plugin on update").

### Events and logging

| Mode | Required TS behavior |
|---|---|
| `emit` | Invoke a listener snapshot immediately in order; do not await returned promises; synchronous exceptions stop dispatch. |
| `parallel` | Invoke all listeners, await all outcomes, and aggregate failures without short-circuiting. |
| `serial` | Await listeners in order; stop at the first bailed result. |
| `bail` | Synchronous ordered short-circuit with the same bail predicate. |
| `waterfall` | Around-middleware with a terminal callback and one-use `next`, supporting sync or async chains; not a value-reduction loop. |

Only `null`, `undefined`, and `false` mean continue: zero and the empty string
still bail. Encode continue/break explicitly in Rust, for example with
`ControlFlow`, rather than approximating JavaScript truthiness.

Snapshot selection, prepend, once-removal before invocation, global listeners,
explicit dispatch filters, and scoped disposal all matter. Service-originated
filtering can depend on service isolation; do not replace it with automatic
ancestor-only routing. Per-Fiber `internal/update` middleware is special and
must not accumulate on reload. Other lifecycle/config/get/set/dispatch hooks
also need explicit Rust mappings.

`EventsService._resolve` admits each listener when it is global, no filter
was supplied, or the filter accepts its registration context. All five modes,
including waterfall, use the resulting callback snapshot. Disposing an
already-selected callback does not remove it from that in-flight dispatch;
do not import the prior port's per-invocation occurrence-claim behavior.

Logging belongs in core, not only a later console adapter. Preserve contextual
and service-derived names, intercept overrides, exporter lifetimes, per-exporter
levels, sequence/timestamp metadata, the default chronological bounded buffer
(capacity 1000), and cause/aggregate error handling. JS formatting, weak Fiber
metadata, and callable syntax need documented Rust representations.

At logger acquisition, name precedence is explicit argument, intercept name,
then the hyphenated nearest named Fiber (falling back to root). The returned
Logger retains that name; convenience methods acquire a logger on each call.
Keep weak Fiber attribution from retaining an otherwise released lifecycle.

### Lessons from the prior Rust port

Read `RS:crates/cordis-core/src/context.rs`, `service.rs`, `plugin.rs`,
`effect.rs`, `fiber/mod.rs`, `events/dispatch.rs`, and `logger.rs` under that
same `src/` directory. Relevant tests include
`RS:crates/cordis-core/tests/service_v3.rs`,
`RS:crates/cordis-core/tests/lifecycle_v3.rs`,
`RS:crates/cordis-core/tests/event_dispatch.rs`,
`RS:crates/cordis-core/tests/realms.rs`, and its `ui-*` directories.

Evaluate cheap Context handles, checked type erasure, exact registration tokens,
snapshot-before-callback dispatch, and avoiding user code under storage locks.
Its Tokio/futures/parking_lot/thiserror/trybuild/loom choices are not an
automatic dependency list for this project.

| Prior-port choice | Our disposition |
|---|---|
| Edition 2024, MSRV 1.88, resolver 3 (`RS:Cargo.toml`) | Adopt edition 2024/resolver 3; choose our own tested MSRV. |
| Orthogonal event Scope ancestry; no general Context ancestry | Do not inherit: TS access checks and filters need different relations. |
| Spawn does not own children; no root disposal | Preserve TS parent effects and reusable-root disposal. |
| Sequential generation cleanup (`RS:crates/cordis-core/src/effect.rs`) | Preserve concurrent top-level and sequential composed cleanup. |
| Prepared config before admission (`RS:crates/cordis-core/src/plugin.rs`) | Use typed validation without changing TS timing/failure visibility. |
| Async `emit`, `emit_parallel`/`query` surface | Retain five TS modes and distinguish sync/async behavior. |
| Reclaim listener occurrences before each invocation (`RS:crates/cordis-core/src/events/dispatch.rs`) | Retain TS callback-snapshot behavior, including waterfall. |
| Opt-in buffer exporter (`RS:crates/cordis-core/src/logger.rs`) | Keep the default TS core buffer. |
| Era replacement, observation protocols, completion runtime | No initial requirement; add only for a demonstrated problem. |

## 3. Proposed Rust shape and design gates

Directions below are not frozen public signatures. Record each gate's outcome
in [decision.md](decision.md) before implementation depends on it.

### Workspace and toolchain

Start with a virtual workspace containing only `crates/cordis-core`. Set
`[workspace.package] edition = "2024"`, inherit it in all members, and use
`[workspace] resolver = "3"`. Edition 2024 requires Rust 1.85 or later; that
is not yet an MSRV promise. Choose and declare `rust-version` from actual
features/dependencies, then test it and current stable in CI. Do not copy
the prior port's 1.88 minimum by default. See the
[edition guide](https://doc.rust-lang.org/edition-guide/rust-2024/index.html).

| Proposed module in `cordis-core/src/` | Responsibility |
|---|---|
| `context.rs` | Root/derived views, metadata, isolation/intercept layers |
| `reflect.rs`, `service.rs` | Typed keys, checked/direct lookup, publication ownership, bound-call context |
| `registry.rs`, `plugin.rs` | Reusable plugin identity, typed config, normalized injection |
| `fiber.rs`, `effect.rs` | Lifecycle driver, snapshots, parent ownership, setup/cleanup |
| `events.rs` | Typed events, registrations, filters, five dispatch modes |
| `logger.rs`, `error.rs` | Core logging and focused operation errors with source diagnostics |

Keep helpers private until a real second owner needs them. Expose handles
and operations, not mutable store internals.

### Gate A: execution, ownership, cancellation

Prototype a provider, waiting consumer, and parent/root disposal on a
single-thread executor first, matching TS. Evaluate Tokio initially and
`futures` only for needed combinators. Decide `Rc`/local futures versus
`Arc`/Send futures explicitly; do not advertise multi-thread safety or
executor independence without evidence. The prototype must establish:

- Progress after a public operation starts even if its waiter is dropped.
  Rust futures are lazy, unlike TS promises: document and test admission/start
  semantics, abandoned waits, and terminal cleanup completion.
- Explicit awaited cleanup. Drop cannot await it, and dropping a public
  control handle must not dispose a Registry-resident Fiber silently.
- Define separate creation boundaries: request, lifecycle admission,
  publication/cleanup ownership commit, and delivery of the control handle.
  Test cancellation at each boundary; a started but undelivered child must
  remain parent-owned or be fully rolled back, never become an orphan.
  Do not assume the first poll necessarily reaches the ownership commit.
- An ownership diagram for runtime residency, parent-owned child cleanup,
  retained providers, and weak backreferences. No internal strong cycles may
  trap an otherwise released runtime.
- No mutex guard or `RefCell` borrow across user callbacks or awaits,
  including validators, exporters, and destructors.
- Reentrant dispatch/lifecycle requests, executor shutdown, and panic policy.
  Self-wait rejection or timeout is an explicit safety deviation, not a TS
  guarantee to claim without explanation.
- Define error ownership and reporting at typed/erased callback boundaries.
  Preserve structured validation issues and useful error sources; correlate
  each failure once rather than repeatedly wrapping or logging it. The prior
  port's opaque string-only normalization is not a requirement. Keep user
  callback failures distinct from framework invariant panics; do not swallow
  invariants or turn failed work into successful results.

Keep production timer requirements out of core unless necessary. Do not add
a hidden secondary runtime or executor abstraction before a demonstrated need.

### Gate B: typed access, identity, and tracing

Prototype a typed named service key, publication token, isolation label, and
bound service call carrying definition and use contexts. `Any` with checked
downcasts is an internal option; name/type mismatches must return errors,
never panic or silently create a different slot.

Separate service slot, publication occurrence, plugin identity, and Fiber
identity. `TypeId` alone cannot model two names for one value type or two
independent reusable plugins of the same Rust type. Re-registering one plugin
handle should group its Fibers; define identity for different handles rather
than guessing from function pointers or closure types.

Prove nearest injection, matching own provisions, same-label isolation
boundaries, nested service calls, and use-site effect ownership. Make direct
lookup visibly distinct from checked plugin access. Prototype typed extension
metadata and explicit accessor/mixin/associated access equivalents; document
any unsupported dynamic shapes.

### Gate C: event and config ergonomics

Use typed event argument/result contracts and opaque symbol-like keys; test
same-name contract mismatches. Distinguish sync and async callbacks honestly:
returning an unpolled Rust future from `emit` does not start a JS-like async
listener. Settle a scheduled-start adapter or explicitly document the narrower
surface. Test immediate prefixes/failures, not only final call counts.

Prototype one-use waterfall continuations, async nested chains, and explicit
continue/break results for bail/serial. Do not replace around-middleware with
simple mappers to dodge lifetime problems.

Use typed source/resolved config and a synchronous validator with structured
issue paths. Serde and schema packages are not core prerequisites. Prove
validation timing, update middleware, and source-config retention before
selecting an integration or convenience macro.

## 4. Delivery sequence

Each milestone lands with source-backed tests and current documentation.
Temporary skeletons needed to break core dependency cycles stay private;
finishing a slice does not imply full core parity.

| Milestone | Prerequisites | Deliverable and exit criteria |
|---|---|---|
| M0: behavioral baseline and design spikes | None | Create a case checklist from the source map; resolve gates A-C with runnable prototypes and record deviations. Agree on expected failure/concurrency traces. |
| M1: workspace and foundations | M0 | Choose a license before publication; scaffold only core, edition 2024/resolver 3, MSRV and formatting/lint/test CI, focused errors, opaque identities, Context views, minimal root/effect skeleton. Prove separate roots and no internal runtime leaks. |
| M2: events, effects, diagnostic logging | M1 | Effect setup/manual disposal/composition, concurrent top-level drain, listener ownership, five dispatch modes/filtering, and logging sufficient for lifecycle failures. Port `dispose.spec.ts` and `events.spec.ts`, including sync/async waterfall and all-settled errors. |
| M3: Registry and base Fiber | M2 | Plugin grouping, per-instance handles, source/resolved config, initial load/failure, parent-owned children, restart/idempotent disposal, Registry removal, reusable-root disposal. Port relevant `plugin.spec.ts` and `fiber.spec.ts`; waiters observe errors and cleanup completion. |
| M4: publication and injection convergence | M3 and gate B | Strict/direct access, owned set/provide, provider checks, committed bindings, Pending/Active transitions, dependency-driven unload/reload. Port `reflect.spec.ts`, `service.spec.ts`, `service-dispose.spec.ts`; retired bindings wait for removed consumers without blocking unrelated cleanup. |
| M5: traced scoping and updates | M4 | Isolation boundaries, own/injected views, intercept precedence/custom merging, bound calls, accessor/mixin/associated/callable equivalents, internal hooks, update/noSave/validation/latest-target coalescing. Port `isolate.spec.ts`, `intercept.spec.ts`, `shadow.spec.ts`, `associate.spec.ts`, `invoke.spec.ts`, `update.spec.ts`, and remaining Fiber cases. |
| M6: core acceptance | M5 | Finish logger semantics/`logger.spec.ts`, decorator-injection equivalents, type checks corresponding to `events.types.ts`, end-to-end example, parity/deviation review, and API/error/cleanup docs. Every source-map area has evidence or an explicit limitation. |

After M6 choose satellites from application needs: likely timer, then console
logging or loader. Timer cleanup reuses effects; file formats/serialization
belong with loader. Do not promise native shared-library hot reload as a direct
equivalent of JS loading.

## 5. Acceptance and validation

Put integration tests under `crates/cordis-core/tests/`, named by concern
(for example `events.rs`, `service_dispose.rs`, `updates.rs`). Link cases to
the pinned TS filename and test name. Start small type-safety checks with
Rustdoc compile-fail examples; add trybuild only if insufficient. The prior
port's tests are comparison evidence, not our expected-output oracle.

Use gates/channels and traces, not sleeps. Assert partial order for concurrent
cleanup: the dependent's final read precedes provider close, while unrelated
and sibling cleanup progress with the dependent gated. Do not impose a total
order where TS allows concurrency. Repeated disposal claims each occurrence
once; repeated reloads restore listener/publication/Registry baseline counts.

Mandatory adversarial scenarios before M6:

1. A Pending consumer activates on provider arrival; withdrawal preserves
   old-binding cleanup access; replacement reloads even with an equal value.
2. A consumer removed from the Registry still delays provider teardown;
   transitive consumers and children retain providers until their final use.
3. Gated load `1` with updates `2` and `3` applies `[0, 1, 3]`, not `2`;
   all waiters settle on the latest outcome. Repeat during unload, unchanged
   restart, obsolete failure, and disposal.
4. Failed Fibers ignore provider refresh; update can recover them; latest-load
   failure reaches every waiter and still triggers cleanup and logging.
5. Invalid loaded updates preserve active config; Pending defers validation;
   accepted middleware can retain source config without restarting and does
   not accumulate on reload.
6. Same-label re-isolation does not inherit inject authority; nested calls
   preserve definition-site checks and use-site intercept/effect behavior.
7. Parallel waits for sync/async failures; zero/empty-string equivalents bail;
   once removes before recursive calls; waterfall cannot call one next twice.
   Test filtered versus unfiltered/global delivery and disposal of an
   already-selected listener in every mode, including waterfall.
8. Parent disposal removes children; root disposal drains but leaves root
   usable. Dropping control handles is not lifecycle disposal.
   Verify that built-in hooks and the default buffer survive repeated root drains.
9. Dropped waiters do not abandon committed cleanup; terminally disposed
   owners cannot gain stale registrations. Error/panic reporting does not
   skip independent cleanup.
10. Logger naming follows nested service calls and intercept overrides;
    buffer order/capacity, exporter removal, levels, and empty/nonempty
    aggregates match pinned expectations.

For ambiguous scheduling, run a small TS trace fixture and equivalent Rust
scenario, comparing normalized states, calls, publications, errors, and
required happens-before edges. Use upstream's existing Yarn/Vitest setup,
not a replacement runner. Exclude wall-clock timestamps, JS stack strings,
pointer identities, and enum numbers. No JS installation or fixture runner
is needed for this planning change; introduce it with the first ambiguity
that needs executable proof.

Once Cargo exists, use `cargo fmt --all --check`, targeted
`cargo test -p cordis-core --test <concern>`, and
`cargo clippy --workspace --all-targets -- -D warnings`. Core acceptance also
requires `cargo test --workspace` including doc tests, and declared-MSRV plus
current-stable checks. Add concurrency modeling/benchmarks only when the
chosen model and observed risks justify them.
Run `cargo +<MSRV> check --workspace --all-targets` explicitly and document
which language features or dependencies require that minimum.

## 6. Completion boundary and open decisions

Core acceptance requires runnable source-map evidence and an end-to-end
load/reconfigure/unload example for a provider-consumer tree, with all
representation differences and omissions documented. Passing tests copied
from the prior port alone does not establish TS parity.

Still open: executor and Send/Sync guarantees; callback/panic/cancellation
boundaries; typed extension and trace-bound call ergonomics; dynamic
accessor/callable/decorator equivalents; logger formatting; schema adapters;
exact MSRV; licensing. Gates/milestones own these decisions; they are not
permission to omit behavior silently. Keep [todo.md](todo.md),
[decision.md](decision.md), and [limitation.md](limitation.md) aligned as
each is resolved.
