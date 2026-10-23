# M0 pinned behavioral case checklist

Specification: TypeScript Cordis commit
`35fdc969e4b6c48c48ed44717c7a018324e4dd88`, `packages/core/tests/`.
This inventory records all **144 named test declarations in 15 spec files**,
not just the cases selected for the M0 experiments. Parameterized declarations
must be expanded to every upstream variant when ported; line numbers locate
the declaration, not an assertion of coverage.

Check an item only when an integrated production Rust test proves its behavior,
or link an explicit Rust representation restriction instead. M2/M3 mark
integrated root/lifecycle evidence; the [M2 mapping](m2.md#evidence-and-remaining-case-coverage)
and [M3 mapping](m3.md#evidence-and-remaining-source-coverage) record API
restrictions and remaining gaps. A passing disposable M0
model alone is still not production parity. The [M0 results](m0.md) map its
selected experiments to source-backed expectations. File-level
milestone owners are defaults; validation/update cases in `fiber.spec.ts` and
internal-update listener cases in `events.spec.ts` extend into M5.

## associate.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/associate.spec.ts); production owner: **M5**.

- [ ] `service injection` (line 5)
- [ ] `property injection` (line 30)
- [ ] `associated type - service injection` (line 58)
- [ ] `associated type - accessor injection` (line 114)
- [ ] `associated access follows the service fiber` (line 176)
- [ ] `inspect` (line 235)

## decorator.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/decorator.spec.ts); production owner: **M6**.

- [ ] `@Inject on class method` (line 7)

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
- [ ] `does not accumulate across reloads` (line 296)
- [ ] `can be disposed explicitly` (line 313)
- [ ] `global listeners are not tied to a child fiber reload` (line 330)

## fiber.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/fiber.spec.ts); production owner: **M3/M5**.

- [ ] `inertia lock 1` (line 7)
- [ ] `inertia lock 2` (line 27)
- [ ] `inertia lock 3` (line 49)
- [x] `plugin error` (line 71)
- [ ] `failed fiber does not re-enter on dependency refresh` (line 93)
- [ ] `update recovers a failed fiber` (line 108)
- [ ] `update waits for asynchronous recovery` (line 122)
- [ ] `update surfaces a failed recovery to its caller` (line 144)
- [ ] `update surfaces a failed reload to its caller` (line 156)
- [ ] `update does not leak a dropped failure` (line 171)
- [x] `dispose error` (line 185)
- [ ] `settles a pending fiber to DISPOSED on dispose` (line 204)
- [ ] `settles a failed fiber to DISPOSED on dispose` (line 231)
- [ ] `keeps the active dispose sequence stable` (line 264)
- [ ] `update config on wrapped fiber` (line 292)
- [x] `restart wrapped fiber` (line 312)
- [ ] `update config while injected service reloads` (line 326)
- [x] `fail on first load` (line 384)
- [ ] `keep a running plugin on update` (line 396)
- [ ] `keep the issues and their paths` (line 413)
- [ ] `pass the resolved config to listeners` (line 426)
- [ ] `keep the source config when a listener handles the update` (line 448)

## intercept.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/intercept.spec.ts); production owner: **M5**.

- [ ] `merge context intercepts from outer to inner` (line 34)
- [ ] `merge injected config under context intercepts` (line 42)
- [ ] `take injected config from the declaring fiber` (line 52)
- [ ] `stop looking for injected config at an isolation boundary` (line 69)
- [ ] `take no injected config for own provisions` (line 83)
- [ ] `take injected config from the def site` (line 96)
- [ ] `custom merge` (line 103)

## invoke.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/invoke.spec.ts); production owner: **M5**.

- [ ] `functional service` (line 5)
- [ ] `uses the service shadow for callable extensions` (line 62)

## isolate.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/isolate.spec.ts); production owner: **M5**.

- [ ] `does not expose own provisions across isolation boundaries` (line 7)
- [ ] `does not inherit the injected view across isolation boundaries` (line 24)
- [ ] `isolated context` (line 55)
- [ ] `shared label` (line 106)
- [ ] `isolated event` (line 152)
- [ ] `keeps own provisions separate from the injected view` (line 172)

## logger.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/logger.spec.ts); production owner: **M6**.

- [x] `keeps the bounded buffer in place and chronological` (line 18)
- [x] `disposes the exporter that registered the disposer` (line 37)
- [x] `uses fiber name when called from outside any service` (line 55)
- [x] `honours explicit name argument` (line 61)
- [ ] `honours intercept name` (line 67)
- [ ] `uses service name when called from inside a Service method (regression)` (line 73)
- [ ] `still lets outer caller intercept override the service-derived name` (line 91)
- [ ] `uses the innermost service name and restores the outer service` (line 109)
- [ ] `uses service name when called from inside [Service.init] (unchanged behaviour)` (line 140)
- [ ] `logs an error whose errors array is empty` (line 156)
- [ ] `flattens an aggregate error into its members` (line 165)

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
- [ ] `Service.init` (line 165)

## reflect.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/reflect.spec.ts); production owner: **M4**.

- [ ] `Context.is()` (line 6)
- [ ] `access check` (line 12)
- [ ] `service injection` (line 30)
- [ ] `service inject leak` (line 57)

## service-dispose.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/service-dispose.spec.ts); production owner: **M4**.

- [ ] `releases an injected binding after providing the same name in another realm (%s)` (line 31; all parameter variants)
- [ ] `waits for a disposing consumer that has already left the registry` (line 48)
- [ ] `withdraws one binding without waiting for another binding from the same provider` (line 73)
- [ ] `retains resources during consumer cleanup (%s)` (line 95; all parameter variants)
- [ ] `retains transitive dependencies while a consumer providing another service drains` (line 131)
- [ ] `retains dependencies through in-flight setup and rollback (failure: %s)` (line 156; all parameter variants)
- [ ] `releases dependencies after a cleanup error` (line 184)
- [ ] `keeps provider listeners alive through the final consumer event` (line 199)
- [ ] `allows unrelated providers and sibling effects to clean up concurrently` (line 214)
- [ ] `supports composed provider effects (owner shutdown: %s)` (line 238; all parameter variants)
- [ ] `keeps an inherited service accessible to a child in a composed effect` (line 271)
- [ ] `retains an ancestor injection until its child finishes cleanup` (line 287)
- [ ] `waits for a child that explicitly injects its parent service` (line 307)
- [ ] `tracks same-name services in separate isolation realms (root shutdown: %s)` (line 323; all parameter variants)
- [ ] `withdraws only the old binding without waiting for the next load` (line 355)
- [ ] `waits for current and withdrawn bindings during provider disposal (withdrawn first: %s)` (line 386; all parameter variants)
- [ ] `replaces a binding from the same provider during consumer initialization` (line 418)

## service.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/service.spec.ts); production owner: **M4**.

- [ ] `pending inject` (line 7)
- [ ] `traceable effect (with inject)` (line 36)
- [ ] `traceable effect (without inject)` (line 76)
- [ ] `compare snapshot` (line 109)
- [ ] `multiple injects` (line 128)
- [ ] `dispose dependents before provider` (line 191)
- [ ] `dispose dependents before provider restarts` (line 203)
- [ ] `dispose dependents before isolated provider` (line 217)

## shadow.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/shadow.spec.ts); production owner: **M5**.

- [ ] `keeps caller metadata separate from the service shadow` (line 5)
- [ ] `applies the service inject when entered from root` (line 60)
- [ ] `resolves dependencies through a captured shadow` (line 93)
- [ ] `resolves same-name own provisions at their definition sites` (line 157)
- [ ] `keeps unchecked access for services provided on the root ctx` (line 195)
- [ ] `exposes the caller without preserving shadow for noShadow services` (line 226)
- [ ] `exposes the caller to callable services` (line 271)
- [ ] `strips service shadow before creating plugins` (line 313)

## update.spec.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/update.spec.ts); production owner: **M5**.

- [ ] `settles concurrent updates on the latest config (inject: %s)` (line 7; all parameter variants)
- [ ] `skips superseded loads before executing the plugin` (line 54)
- [ ] `coalesces updates during unloading` (line 69)
- [ ] `honors %s while loading even when config is unchanged` (line 96; all parameter variants)
- [ ] `cleans up a superseded failure without failing the latest update` (line 121)
- [ ] `rejects all waiting updates when the latest load fails` (line 152)
- [ ] `stops advancing an outdated async generator and collects its final yield` (line 188)
- [ ] `waits for dependencies and loads only the latest config` (line 212)
- [ ] `does not restart when a listener accepts an update during loading` (line 234)
- [ ] `does not load a queued config after disposal` (line 259)

## events.types.ts

[Pinned source](https://github.com/cordiverse/cordis/blob/35fdc969e4b6c48c48ed44717c7a018324e4dd88/packages/core/tests/events.types.ts);
production owner: **M6**. This is a type-checking suite, not a named runtime spec.

- [ ] Port its event argument, listener, result, and dispatch type constraints
  into Rust compile-pass/compile-fail evidence after the public API is selected.
