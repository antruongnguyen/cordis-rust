# M0 pinned behavioral case checklist

Specification: TypeScript Cordis commit
`35fdc969e4b6c48c48ed44717c7a018324e4dd88`, `packages/core/tests/`.
This inventory records all **144 named test declarations in 15 spec files**,
not just the cases selected for the M0 experiments. Parameterized declarations
must be expanded to every upstream variant when ported; line numbers locate
the declaration, not an assertion of coverage.

Check an item only when an integrated production Rust test proves its behavior,
or link an explicit Rust representation restriction instead. M2-M6 mark
integrated root/lifecycle evidence; the [M2 mapping](m2.md#evidence-and-remaining-case-coverage)
and [M3 mapping](m3.md#evidence-and-remaining-source-coverage) plus
[M4 mapping](m4.md#evidence-and-remaining-cases), [M5 mapping](m5.md#evidence),
and [M6 audit](m6.md#source-map-audit) record API
restrictions and remaining gaps. A passing disposable M0
model alone is still not production parity. The [M0 results](m0.md) map its
selected experiments to source-backed expectations. File-level
milestone owners are defaults; validation/update cases in `fiber.spec.ts` and
internal-update listener cases in `events.spec.ts` extend into M5.

## associate.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/associate.spec.ts); production owner: **M5**.

- [x] `service injection` (line 5; typed associated keys, removal while base remains)
- [x] `property injection` (line 30; Rust fields, named slots and explicit receiver/accessor adapters)
- [x] `associated type - service injection` (line 58; associated_key on explicit mapped holders)
- [x] `associated type - accessor injection` (line 114; typed Session receiver and mixin authority)
- [x] `associated access follows the service fiber` (line 176)
- [x] `inspect` (line 235; ordinary Rust Debug/type arguments, no Proxy/class-toString promise)

## decorator.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/decorator.spec.ts); production owner: **M6**.

- [x] `@Inject on class method` (line 7; inject_method Rc/Context adapter, not native decorator syntax)

## dispose.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/dispose.spec.ts); production owner: **M2**.

- [x] `dispose by plugin` (line 7)
- [x] `dispose manually` (line 23)
- [x] `yield dispose` (line 37)
- [x] `async return 1` (line 76)
- [x] `async return 2` (line 90)
- [x] `async yield 1` (line 103)
- [x] `async yield 2 (aborted)` (line 123)
- [x] `async yield 3 (aborted)` (line 143)
- [x] `async yield 4 (await dispose)` (line 165)
- [x] `return with error` (line 185)
- [x] `yield with error` (line 197)
- [x] `async return with error` (line 210)
- [x] `async yield with error` (line 222)

## events.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/events.spec.ts); production owner: **M2**.

- [x] `supports symbol event names across dispatch modes` (line 18)
- [x] `supports symbol event names with ctx.once()` (line 35)
- [x] `treats prototype property names as ordinary events` (line 46)
- [x] `removes empty event buckets after disposal` (line 65)
- [x] `ctx.on()` (line 77)
- [x] `ctx.once()` (line 90)
- [x] `ctx.parallel()` (line 103)
- [x] `ctx.parallel() labels symbol events` (line 134)
- [x] `ctx.emit()` (line 145)
- [x] `ctx.serial()` (line 164)
- [x] `ctx.bail()` (line 183)
- [x] `ctx.waterfall()` (line 202)
- [x] `ctx.waterfall() rejects duplicate next()` (line 230)
- [x] `ctx.waterfall() rejects continuations from outer frames` (line 244)
- [x] `ctx.waterfall() rejects duplicate next() after awaiting` (line 265)
- [x] `ctx.waterfall() supports nested async calls` (line 278)
- [x] `does not accumulate across reloads` (line 296)
- [x] `can be disposed explicitly` (line 313)
- [x] `global listeners are not tied to a child fiber reload` (line 330)

## fiber.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/fiber.spec.ts); production owner: **M3/M5**.

- [x] `inertia lock 1` (line 7; channel-gated causal states, not wall-clock timer matching)
- [x] `inertia lock 2` (line 27; replacement before old setup completes)
- [x] `inertia lock 3` (line 49; Active provider disposal converges consumer to Pending)
- [x] `plugin error` (line 71)
- [x] `failed fiber does not re-enter on dependency refresh` (line 93)
- [x] `update recovers a failed fiber` (line 108)
- [x] `update waits for asynchronous recovery` (line 122)
- [x] `update surfaces a failed recovery to its caller` (line 144)
- [x] `update surfaces a failed reload to its caller` (line 156)
- [x] `update does not leak a dropped failure` (line 171)
- [x] `dispose error` (line 185)
- [x] `settles a pending fiber to DISPOSED on dispose` (line 204)
- [x] `settles a failed fiber to DISPOSED on dispose` (line 231)
- [x] `keeps the active dispose sequence stable` (line 264)
- [x] `update config on wrapped fiber` (line 292; shared typed FiberHandle clone representation)
- [x] `restart wrapped fiber` (line 312)
- [x] `update config while injected service reloads` (line 326)
- [x] `fail on first load` (line 384)
- [x] `keep a running plugin on update` (line 396)
- [x] `keep the issues and their paths` (line 413)
- [x] `pass the resolved config to listeners` (line 426)
- [x] `keep the source config when a listener handles the update` (line 448)

## intercept.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/intercept.spec.ts); production owner: **M5**.

- [x] `merge context intercepts from outer to inner` (line 34)
- [x] `merge injected config under context intercepts` (line 42)
- [x] `take injected config from the declaring fiber` (line 52)
- [x] `stop looking for injected config at an isolation boundary` (line 69)
- [x] `take no injected config for own provisions` (line 83)
- [x] `take injected config from the def site` (line 96)
- [x] `custom merge` (line 103)

## invoke.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/invoke.spec.ts); production owner: **M5**.

- [x] `functional service` (line 5; typed ServiceInvoke/map adapters)
- [x] `uses the service shadow for callable extensions` (line 62)

## isolate.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/isolate.spec.ts); production owner: **M5**.

- [x] `does not expose own provisions across isolation boundaries` (line 7)
- [x] `does not inherit the injected view across isolation boundaries` (line 24)
- [x] `isolated context` (line 55)
- [x] `shared label` (line 106)
- [x] `isolated event` (line 152)
- [x] `keeps own provisions separate from the injected view` (line 172)

## logger.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/logger.spec.ts); production owner: **M6**.

- [x] `keeps the bounded buffer in place and chronological` (line 18)
- [x] `disposes the exporter that registered the disposer` (line 37)
- [x] `uses fiber name when called from outside any service` (line 55)
- [x] `honours explicit name argument` (line 61)
- [x] `honours intercept name` (line 67)
- [x] `uses service name when called from inside a Service method (regression)` (line 73; explicit bound adapter)
- [x] `still lets outer caller intercept override the service-derived name` (line 91)
- [x] `uses the innermost service name and restores the outer service` (line 109)
- [x] `uses service name when called from inside [Service.init] (unchanged behaviour)` (line 140; initialized factory)
- [x] `logs an error whose errors array is empty` (line 156; explicit ErrorValue metadata)
- [x] `flattens an aggregate error into its members` (line 165; exact source identity retained)

## plugin.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/plugin.spec.ts); production owner: **M3**.

- [x] `apply functional plugin` (line 8)
- [x] `apply object plugin` (line 18)
- [x] `apply invalid plugin` (line 29)
- [x] `inactive context` (line 36)
- [x] `context inspect` (line 50)
- [x] `ctx.registry` (line 77)
- [x] `nested plugins` (line 86)
- [x] `compare snapshot` (line 123)
- [x] `root dispose` (line 145)
- [x] `Service.init` (line 165; Plugin::initialized, not constructor/method discovery)

## reflect.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/reflect.spec.ts); production owner: **M4**.

- [x] `Context.is()` (line 6; concrete Context/Any type checks; JS subclass branding explicitly unsupported)
- [x] `access check` (line 12; explicit typed access/static Context representation, see M4)
- [x] `service injection` (line 30; typed services/accessors/metadata and direct-slot distinction)
- [x] `service inject leak` (line 57)

## service-dispose.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/service-dispose.spec.ts); production owner: **M4**.

- [x] `releases an injected binding after providing the same name in another realm (%s)` (line 31; all parameter variants)
- [x] `waits for a disposing consumer that has already left the registry` (line 48)
- [x] `withdraws one binding without waiting for another binding from the same provider` (line 73)
- [x] `retains resources during consumer cleanup (%s)` (line 95; all parameter variants)
- [x] `retains transitive dependencies while a consumer providing another service drains` (line 131)
- [x] `retains dependencies through in-flight setup and rollback (failure: %s)` (line 156; all parameter variants)
- [x] `releases dependencies after a cleanup error` (line 184)
- [x] `keeps provider listeners alive through the final consumer event` (line 199)
- [x] `allows unrelated providers and sibling effects to clean up concurrently` (line 214)
- [x] `supports composed provider effects (owner shutdown: %s)` (line 238; all parameter variants)
- [x] `keeps an inherited service accessible to a child in a composed effect` (line 271)
- [x] `retains an ancestor injection until its child finishes cleanup` (line 287)
- [x] `waits for a child that explicitly injects its parent service` (line 307)
- [x] `tracks same-name services in separate isolation realms (root shutdown: %s)` (line 323; all parameter variants)
- [x] `withdraws only the old binding without waiting for the next load` (line 355)
- [x] `waits for current and withdrawn bindings during provider disposal (withdrawn first: %s)` (line 386; all parameter variants)
- [x] `replaces a binding from the same provider during consumer initialization` (line 418; also covers equal/same-Rc values)

## service.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/service.spec.ts); production owner: **M4**.

- [x] `pending inject` (line 7; explicit initialized factory gates availability)
- [x] `traceable effect (with inject)` (line 36; explicit bound Context)
- [x] `traceable effect (without inject)` (line 76; inherited root provision authority)
- [x] `compare snapshot` (line 109; repeated initialized provider/child/listener baselines)
- [x] `multiple injects` (line 128; explicit typed Plugin/inject equivalents)
- [x] `dispose dependents before provider` (line 191)
- [x] `dispose dependents before provider restarts` (line 203)
- [x] `dispose dependents before isolated provider` (line 217)

M6 adds explicit initialized-factory/method adapters and repeated lifecycle
baseline checks. These checkmarks account for the declared cases at the Rust
contract level; they do not claim automatic Service.init/decorator discovery
or transparent proxy behavior. See the M6 restriction/deviation audit.

## shadow.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/shadow.spec.ts); production owner: **M5**.

- [x] `keeps caller metadata separate from the service shadow` (line 5)
- [x] `applies the service inject when entered from root` (line 60)
- [x] `resolves dependencies through a captured shadow` (line 93; explicit map/rebind)
- [x] `resolves same-name own provisions at their definition sites` (line 157)
- [x] `keeps unchecked access for services provided on the root ctx` (line 195)
- [x] `exposes the caller without preserving shadow for noShadow services` (line 226; call_unshadowed)
- [x] `exposes the caller to callable services` (line 271; caller is explicit on BoundService)
- [x] `strips service shadow before creating plugins` (line 313)

## update.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/update.spec.ts); production owner: **M5**.

- [x] `settles concurrent updates on the latest config (inject: %s)` (line 7; all parameter variants)
- [x] `skips superseded loads before executing the plugin` (line 54)
- [x] `coalesces updates during unloading` (line 69)
- [x] `honors %s while loading even when config is unchanged` (line 96; all parameter variants)
- [x] `cleans up a superseded failure without failing the latest update` (line 121)
- [x] `rejects all waiting updates when the latest load fails` (line 152)
- [x] `stops advancing an outdated async generator and collects its final yield` (line 188; explicit owned effect_stream)
- [x] `waits for dependencies and loads only the latest config` (line 212)
- [x] `does not restart when a listener accepts an update during loading` (line 234)
- [x] `does not load a queued config after disposal` (line 259)

## events.types.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/events.types.ts);
production owner: **M6**. This is a type-checking suite, not a named runtime spec.

- [x] Event argument/listener/result/dispatch contracts have Rustdoc compile-pass/
  compile-fail evidence, including the registered-symbol cases. Dynamic symbols
  use explicitly typed heterogeneous Value lists rather than unchecked JS args.

## M6 interpretation

All 144 named declarations (plus the separate event-type inventory) have been
accounted for with integrated tests or explicit Rust representation restrictions.
Parameterized declarations are exercised through their corresponding gated/
disposal/config variants. See [M6](m6.md) for the per-area evidence and boundaries.
This inventory is not a claim that arbitrary JavaScript reflection, subclass
branding, implicit method tracing, or every possible internal-hook mutation is
supported. Restricted mechanisms remain restricted even when their named
source-case representation is checked.

## Post-M6 executable lifecycle evidence

[Lifecycle parity](lifecycle-parity.md) adds three Rust/TypeScript paired
scenarios without changing these checkmarks or treating further protocols as
complete. `tests/parity/lifecycle.spec.ts` executes against the pinned TS core;
`crates/cordis-core/tests/lifecycle_parity.rs` uses production Rust APIs.

| Paired scenario | Existing source cases strengthened |
|---|---|
| `combined` | `fiber.spec.ts`: update config while injected service reloads |
| `coalesced` | `update.spec.ts`: coalesces updates during unloading; settles concurrent updates on latest config; `service-dispose.spec.ts`: retains resources during consumer cleanup |
| `removed` | `service-dispose.spec.ts`: waits for a disposing consumer already out of the Registry; `update.spec.ts`: does not load a queued config after disposal |

These compositions add direct shared-trace evidence for selected cases, not
cross-language execution of every parameter variant in this inventory.
