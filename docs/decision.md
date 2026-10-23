# Decisions

A lightweight log of decisions made for this project and the reasoning behind
them. Newest first. This is intentionally not a heavy ADR process; it records
*what we chose and why* so the choices aren't re-litigated silently.

## 2026-10-23 — Select bounded GitHub activity consumers

After workflow selection and explicit scope/dependency approval, implement a
snapshot digest, `gh`-based collector and signed loopback webhook journal,
in that order. Keep them executable application examples, exercising the same
implementation from integration tests. Reuse typed plugins/injection, explicit
updates and scoped teardown; no production core change is currently required.
Only actually shared application contracts belong in a private example module.

JSON/network/cryptographic dependencies and expanded Tokio features belong to
dev-dependencies, not the production core. Use the installed `gh` transport and
its existing authentication rather than add an HTTP client. Do not create
credentials, GitHub webhooks or public ingress as a side effect of this work.

The first slice validates bounded snapshots and generates deterministic escaped
Markdown via same-directory temporary write/flush/rename. Destination replacement
uses existing dependency convergence; Failed processors need explicit update.
CLI execution is one-shot; live reconfiguration is a programmatic example API.
See [github-activity.md](github-activity.md).

The collector uses two bounded direct `gh` processes and publishes only a
complete validated snapshot. One injecting consumer owns and joins its periodic
worker; in-flight collection completes before provider replacement or shutdown.
Initial failure follows Failed/explicit-update recovery. Operational poll errors
are visible but leave the periodic worker running for its next scheduled attempt.
CLI configuration is startup-only; typed handles expose live updates in tests.
Unix subprocess fixtures use FIFO gates and a controlled clock, not sleeps or
live authenticated services.

**Why:** The user identified repository activity as useful, providing a concrete
consumer instead of indefinite parity expansion. The bounded applications can
demonstrate actual I/O while preserving local hosting and resource contracts.

## 2026-10-23 — Bound failure/recovery parity to three additional compositions

Following explicit scope approval, extend the existing lifecycle fixtures and
runner rather than introduce a new harness. Gate superseded failure rollback,
latest failure rollback with multiple retained waiters and an abandoned update,
and explicit asynchronous recovery after a Failed consumer ignores provider
replacement. Both implementations produce the same 36 new observations;
the original 37 remain unchanged. Each actual load failure has one diagnostic,
rollback completes before waiter settlement, and original per-language error
sources remain identifiable rather than being reduced to comparable strings.

Rust uses a test-only Error adapter retaining the original Rc error as its
source; TS checks the original Error object in waiters and logger arguments.
Cross-language traces normalize only causal events and checkpoint outcomes,
not error allocations, stack strings, transient scheduling, or enum ordinals.
The comparisons demonstrate no production mismatch, so core code, APIs and
dependencies stay unchanged.

**Why:** These paths already had Rust-only coverage; the missing evidence was
paired execution. This completes the approved bounded slice, not every upstream
failure/recovery protocol or a preassigned M7. Prefer concrete consuming
application needs before selecting more framework work. See
[lifecycle-parity.md](lifecycle-parity.md) for reproduction and limits.

## 2026-10-23 — Harden lifecycle evidence before choosing a satellite

The approved post-M6 scope is three executable Rust/TypeScript lifecycle
comparisons, not a preassigned M7 package. Exercise concurrent provider/config
updates, latest-target coalescing during gated cleanup, and Registry-removed
consumer retention with terminal disposal. Keep fixtures small, use the
existing upstream Yarn/Vitest setup at the exact source pin, and compare shared
plain-text observations without adding Rust dependencies or a generic harness.

The comparisons confirm existing behavior, so production code stays unchanged.
Both implementations allow consumer update waiters to settle Pending while
their provider is unavailable. Provider arrival starts a distinct lifecycle
pass; an explicit consumer readiness wait joins that pass. A gate holds the
latest consumer load to prove this distinction. Do not "fix" this into a
runtime-wide convergence barrier.

**Why:** No concrete application need selects timer, console logging or loader.
M6 explicitly had source-backed rather than differential evidence. These
three executable checks strengthen that evidence while preserving documented
ownership/cancellation and explicit-adapter contracts. See
[lifecycle-parity.md](lifecycle-parity.md) for the exact scope and reproduction.

## 2026-10-23 — M6 completes explicit Rust logging and acceptance

Logger acquisition caches explicit name, then use-site logger intercept, then
the normalized definition-Fiber name. Nested bound calls restore outer names.
Malformed typed intercepts remain an error in the handle and fail logging before
formatting/export, rather than falling back silently. Records retain stable
identity plus a weak current lifecycle snapshot, never a strong Fiber.

Use finite owned `ErrorValue` trees to represent JS cause/aggregate metadata.
Nonempty aggregates flatten recursively, empty aggregates log themselves, and
causes precede their outer error. Records retain the exact error allocation.
Rust Display/Debug and exporter closures replace JS printf/formatter dictionaries;
per-line UTF-16 budgets and the pinned ANSI palettes have measured tests.
Never split a Rust Unicode scalar merely to mimic a lone JS surrogate.
Default name tokenization follows cosmokit 1.8.1's source (not a guessed regex).

`Plugin::initialized` explicitly awaits an initializer's acquired disposer.
`Context::inject_method` explicitly binds an Rc target to a dependency-owned
child, replacing decorator/constructor discovery without proc macros.
Forwarding an already-normalized callback failure preserves its diagnostic
claim, so failed initializers report once.

**Why:** Native Rust types cannot transparently implement JS property/prototype
reflection or inspect arbitrary Error properties. Explicit typed adapters keep
the useful lifecycle/configuration contracts while making representation
restrictions testable and honest. The same production example runs standalone
and under integration tests. See [M6 audit](m6.md), not a blanket TS parity claim.

## 2026-10-23 — M5 uses explicit bound views, not transparent Rust pointers

Keep M4 `Rc<T>` reads unchanged. Add `BoundService<T>` with an explicit method
adapter receiving the service and traced Context. Checked authority follows
the publication's definition; realm/intercept/metadata/effect ownership follows
the use site. Nested calls retain separate caller and definition views, with
one flattened shadow. Associated access uses the original accessing code's
authority, not the service's own injections. Callable/holder extensions and
accessor/mixin receivers have explicit typed adapters.

Isolation slots have opaque allocation identity and retained per-name boundary
history. Reusing a label still stops inherited inject authority; slots have no
parent fallback. Publications remember their exact selected slot independently
of Context derivation. Cross-name shared slots use checked contracts.

**Why:** A transparent `Rc<T>` cannot rebind arbitrary Rust methods. Explicit
adapters preserve existing callers and avoid Proxy simulation or proc macros.
Notifications also refresh alias declarations selecting the changed slot:
this is a deliberate safety strengthening over upstream's name-only notify,
preventing aliased consumers from trapping a withdrawing publication.

## 2026-10-23 — M5 preserves source validation timing and owns update continuations

Retain source config separately from validated apply input. Loaded updates
validate before commit/source mutation or middleware; Pending updates defer
validation. Commit hooks see source; update hooks see resolved input. noSave
suppresses only commit notification. Local update hooks precede ordinary global
hooks, while prepended globals precede the local router; all remain scoped
effects. Calling the owned update continuation starts restart now. Veto retains
the new source without interrupting in-flight apply. Recovery clears failure
only through update, and every waiter follows the latest lifecycle pass.

Typed `hooks` values use checked heterogeneous payloads rather than unchecked
downcasts. Config/get/set and listener replacement preserve their explicit
contracts. Observer failures are diagnosed without stopping cleanup: unlike
upstream's throwing observer path, an observer cannot strand retained providers.
Duplicate propagation of one callback failure shares a diagnostic claim.

Owner-level weak acquisition stop flags prevent advancing an obsolete effect
stream while still collecting its in-flight final disposer. They do not abort
native apply futures or retain the host. This closes the update/generator race
using the existing setup and cleanup implementations.

See [M5 contracts and evidence](m5.md); M6 owns logger/type/example acceptance.

## 2026-10-23 — M4 separates service slots, publications, and committed loads

Use explicit `ServiceKey<T>` named slots with checked internal type erasure,
opaque `PublicationId` occurrences, and Fiber-owned publication effects.
`set` replaces only the value: it preserves the occurrence and does not notify
consumers. Withdrawal removes only the exact current occurrence before waiting;
republication always creates a new occurrence, even for the same `Rc` value.
Checked access walks own provisions and the nearest declaring ancestor's
committed snapshot. Root access and explicit direct lookups remain distinct.
Captured plugin Contexts do not acquire root authority when used from root.

Injection lists are captured per Fiber, independently of shared Plugin identity.
Resolve affected candidates on service notifications, not on set or arbitrary
restart. A lifecycle target combines the restart revision and exact candidate
identities. Commit and retain its dependencies before validation/apply, and
release only after setup, rollback, children, and resource cleanup finish.
Missing dependencies remain Pending without validation. Failed Fibers do not
retry on refresh; config-update recovery remains M5.

**Why:** Value equality cannot detect replacement or safely identify the load
whose teardown must finish. Per-name resolution preserves upstream readiness
check timing without adding config/intercept machinery prematurely.

## 2026-10-23 — Provider retention is independent of Registry residency

Each publication tracks exact dependent Fiber identities until their committed
load releases them. Provider owners retain current and already-withdrawn
publications and wait for their dependents before draining resources/listeners.
Individual withdrawal waits only for its own occurrence, not the provider's
other publications or a consumer's replacement load. Self-provisions do not add
a self-dependency.

Whole-journal drains temporarily stop the owner's services admitting consumers,
including root-owned publications, while remaining reusable snapshot drains.
Unrelated owner cleanup and independent sibling effects stay concurrent;
composition still uses the existing reverse sequential effect protocol.
Publication/Context links to owners/runtime are weak. Lifecycle drains release
their temporary executor handle before awaiting cleanup, so host destruction
is cancellation rather than a host/task reference cycle.

**Why:** Removed consumers still need their providers, and transitive teardown
must preserve final service calls/events. Scanning only the Registry, waiting for
a consumer's latest ready barrier, or serializing every root effect is incorrect.
See [M4 contracts and evidence](m4.md). Full isolation/use-site tracing and
internal service/status hook protocols remain M5/M6.

## 2026-10-23 — M3 separates resource ownership from runtime residency

Each Fiber owns its own cleanup journal and observable state. Registry groups
retain live/failed instances; Context and registration tokens retain weak
runtime/owner links. A child's disposal is a parent effect, removed immediately
by explicit child disposal. Child-to-parent links are weak and used for callback
attribution, not lifetime retention. Root remains permanent and reusable.

**Why:** Existing M2 resources must follow their registering Fiber without a
runtime-to-callback-to-Context cycle or a parent accumulating retired children.
Group retirement/pruning uses exact allocation checks so old teardown cannot
delete a new group of the same reusable declaration. Scoped cleanup still allows
final event dispatch/logging but refuses new terminal-owner registrations.

## 2026-10-23 — Typed plugin declarations and one supervised lifecycle driver

Use reusable `Plugin<C, R>` declarations with per-load synchronous validation
and explicit Context-based acquisitions. Clones share grouping identity; new
declarations do not collapse by Rust callback type or name. Retain typed source
config separately from resolved apply input. Invalid plugin/config shapes are
compile errors; runtime callback errors retain their original Error source.

One in-flight driver coalesces restart revisions; terminal disposal wins over
queued work and waits for in-flight apply/cleanup. Failed instances remain
resident until disposal; restart does not silently recover them. Config update
and recovery remain M5. Self/owning-ancestor lifecycle waits are refused from
attributed callbacks instead of deadlocking their subtree.

**Why:** This preserves the TS parent/resource ownership, validation timing,
and observable failure semantics without JavaScript callable-object discovery
or the prior port's parent-unowned spawn model. Callback definitions are
immutable, so the TS first-spawn metadata snapshot has no mutable counterpart.
Runtime host lifetime remains explicit; Drop is not successful async teardown.
See [M3 contracts and evidence](m3.md).

## 2026-10-23 — Complete root M2 timing with explicit synchronous cleanup

Use one cleanup representation with ready and pending outcomes. Sync manual
chains and all-sync setup rollback run before the request returns. Mixed
composition runs in reverse order up to the first async child, then awaits
that child before advancing; independent root cleanups remain concurrent and
supervised. Expose `Disposer::sync` and `Context::on_dispose_sync` rather than
force-polling arbitrary async futures or treating an async block as sync code.

Expose effect labels and `Context::get_effects()` value-only snapshots.
Transferred effects become nested metadata children in acquisition order;
raw disposers do not add synthetic effect nodes. Async acquisitions update
their existing parent node. Active setup cannot transfer itself or an ancestor
containing it: refuse before removing the root claim. Metadata never owns
callbacks or resources.

**Why:** This closes the remaining root M2 timing/introspection gaps against
`fiber.ts` and `dispose.spec.ts`, without reference cycles or a second cleanup
implementation. M3 still owns plugin disposal; M5 owns internal-update hooks.
See [M2 contracts and evidence](m2.md).

## 2026-10-23 — M2 uses explicit callback roles and owned resource composition

Admit root effect ownership before user setup. Collect acquired disposers from
iterators/streams, transfer existing root registrations into composition, and
await children sequentially in reverse order. Root drains remain concurrent.
Stop stream advancement on disposal, but retain the first/in-flight final
acquisition and await its cleanup. Ready observes async setup and rollback,
not disposal. The completed timing decision above distinguishes immediate
all-sync rollback from async rollback that must continue on the host.

Typed named/symbol events use checked live buckets and callback snapshots.
Global/filter/prepend/once behavior is explicit; removal never erases a callback
already selected. Responders use ControlFlow, while middleware owns one-use
Next/AsyncNext continuations. Bail is synchronous-only and waterfall has
explicit sync/async callback shapes. Duplicate next becomes a compile error.

**Why:** These contracts preserve the proven ownership and dispatch semantics
without JS Proxy/callable-object emulation or accidental occurrence rechecks.
Root introspection and synchronous cleanup timing are now covered by the
decision above; plugin-owned/internal-update cases need their lifecycle owners.
See [M2 APIs and evidence](m2.md), not a blanket upstream parity claim.

## 2026-10-23 — Automatic failures flow through core diagnostics

Keep original callback sources, record structured diagnostics in the default
1000-record chronological buffer, and scope exporters through root cleanup.
Each exporter selects name/default verbosity independently. Attempt all selected
exporters, including on diagnostic failures, without recursively logging broken
exporters through themselves. Preserve sources and stderr fallback reporting.

**Why:** Dropped waiters must not hide callback failures. Attempt-all exporters
are an explicit diagnostic safety strengthening over TS's unguarded export
loop. Full traced naming and JS formatting remain M5/M6 work.

## 2026-10-23 — M1 owns the host; Context is a non-owning view

The root workspace contains only the unpublished `cordis-core` crate. Use
Rust 2024, resolver 3, and an initially tested MSRV of Rust 1.85. Commit both
workspace lockfiles for stable/MSRV CI. Adopt MIT licensing for this project.

`Runtime` owns the local task host and root cleanup journal. `Context` and
manual effect tokens hold weak runtime links; metadata layers own only their
values and parent metadata layers. Dropping Context does not tear down work,
and surviving Context values cannot admit work after their host is destroyed.

**Why:** Capturing a derived Context in its own cleanup must not form an
internal strong-reference cycle. Integrated M1 tests prove release both after
drain and on host destruction. Separating typed ancestry from ownership is
the basis for later definition/use-site service views; those are not yet built.

## 2026-10-23 — Root cleanup is eager, explicitly hosted, and supervised

Admit work only inside the owning `Runtime::run` scope. Root drains/manual
claims commit synchronously and return wait-only completion handles. Repeated
drains share one current snapshot; later registrations belong to the next
pass. Root stays reusable. Host destruction cancels work rather than pretending
async Drop succeeded. Refuse nested/spawned/concurrent host driving and
self-drain requests before admission.

**Why:** This preserves the proven M0 local-first direction without unsafe
implicit LocalSet entry or cancellation-prone lazy cleanup. Report every user
failure to stderr and retain its typed error source/report, even if the waiter
is abandoned. Supervise unexpected driver task failures separately from user
callback unwinds; no blanket invariant recovery. Full effects, plugins, and
Logger integration remain M2 onward. See [M1 contracts](m1.md).

## 2026-10-23 — M0 experiments stay outside the production workspace

Keep the disposable, unpublished Rust 2024 experiments under `spikes/m0`,
with their own resolver-3 manifest and lockfile. Their behavioral models are
not `cordis-core`. Record the source case inventory and executable findings
in [m0-cases.md](m0-cases.md) and [m0.md](m0.md); production parity remains
unchecked until integrated tests exist.

**Why:** We can prove risky Rust mechanisms before freezing framework APIs
without confusing a deliberately incomplete model with M1's implementation.
The spike passes on Rust 1.85.0. The subsequent M1 decision above confirms the
production MSRV, root workspace, and CI; M0's models stay separate.

## 2026-10-23 — Local-first execution and explicit lifecycle completion

Start M1 with Tokio, an explicitly hosted LocalSet, Rc handles, and local
non-Send futures. Admission and ownership commit happen in synchronous
request methods, which return completion handles; abandoning a waiter does
not cancel started work. Parent Fibers own children, exact publication
bindings retain dependents through cleanup, and root disposal is reusable.

**Why:** The M0 lifecycle probes demonstrate these behaviors without a
multi-thread runtime, executor abstraction, or hidden completion runtime.
Destroying the executor cancels work; applications must await cleanup before
stopping it. Do not advertise executor-independent or Send/Sync guarantees.
M1 must make the host requirement explicit before admitting work and integrate
definition ancestry without strong ownership cycles. See [M0 ceilings](m0.md).

## 2026-10-23 — Explicit typed access and honest sync/async event APIs

Use named typed keys, checked internal downcasts, retained isolation-boundary
history, and bound service calls carrying definition and use views separately.
Prefer typed metadata and explicit methods/extension traits over property or
callable-object emulation. Preserve symbol-selected slots, including cross-name
shared-label aliasing; report incompatible Rust contracts explicitly.

Use ControlFlow for bail/serial and owned FnOnce continuations for waterfall.
An explicit split adapter runs an async listener's prefix synchronously and
schedules its continuation. Restrict bail to sync listeners, refusing mixed
snapshots rather than returning promise-like values. Keep validation synchronous,
with separate typed source/resolved config and structured validation issues.

**Why:** M0 proves these shapes while surfacing Rust's lazy-future and ownership
differences. Single-use-next violations become compile errors; async bail and
automatic async-function prefix execution are deliberately not promised.
These directions are not a frozen API or evidence of complete TS parity.

## 2026-10-22 — Rust edition 2024 baseline

Use Rust edition **2024** for the initial workspace, inherited by every crate,
with Cargo resolver **3**. A newer stable edition can be adopted explicitly;
an older edition does not satisfy the project requirement.

**Why:** This is a new implementation with no legacy-edition compatibility
obligation. Edition 2024 requires Rust 1.85 or later, but the exact supported
compiler minimum will be selected and tested when dependencies and language
features are chosen. The prior port's Rust 1.88 minimum is not ours by default.

## 2026-10-22 — Plan from pinned behavior, not the prior port's architecture

The initial [implementation plan](implementation-plan.md) reviews TypeScript
Cordis at `35fdc969e4b6c48c48ed44717c7a018324e4dd88` and the independent Rust
port at `1754062cef67cfc119fc7a2084c0321a231b9092`. Preserve the TS behavior
of parent-owned children, reusable-root disposal, exact binding retention,
concurrent top-level cleanup, and the five dispatch modes. Runtime ownership,
typed tracing, and callback/config API choices require design spikes before
their public contracts are fixed.

**Why:** The prior port intentionally diverges in these areas and its parity
ledger uses an older TS revision. Its implementation is useful evidence for
Rust engineering, not authority to silently narrow this project's semantics.

## 2026-10-22 — TypeScript `cordis` is the source of truth

The original [cordiverse/cordis](https://github.com/cordiverse/cordis)
(TypeScript) is the specification. When references disagree or behavior is
ambiguous, the TS implementation wins.

**Why:** The goal is a faithful reimplementation of the original framework, not
a variation on an existing Rust port.

## 2026-10-22 — `dshbox/cordis-rs` is reference-only

The existing Rust port [dshbox/cordis-rs](https://github.com/dshbox/cordis-rs)
is kept locally for comparison and inspiration but is not a target. Its API
shape and architectural decisions are not authoritative and may be diverged
from freely.

**Why:** It is an independent effort whose distance from the upstream semantics
is unknown; adopting its decisions wholesale would undermine the goal of
verifying the port against the original.

## 2026-10-22 — Mirror semantics, not mechanisms

We reimplement the *behavior* of Cordis, not its implementation techniques. The
original leans on JavaScript-only mechanisms that do not exist in Rust.

**Why:** A line-by-line transliteration is impossible and undesirable. See
`docs/limitation.md` for the specific mechanisms affected.

## 2026-10-22 — Cargo workspace mirroring the TS packages

The planned layout is a Cargo workspace whose crates mirror the original
`cordis` packages (`cordis-core` first, then `cordis-loader`, `cordis-timer`,
`cordis-logger`, …), rather than a single monolithic crate.

**Why:** It keeps the correspondence to the upstream package structure explicit
and lets satellite capabilities stay optional, as they are upstream.
