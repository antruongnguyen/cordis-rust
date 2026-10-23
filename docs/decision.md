# Decisions

A lightweight log of decisions made for this project and the reasoning behind
them. Newest first. This is intentionally not a heavy ADR process; it records
*what we chose and why* so the choices aren't re-litigated silently.

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
