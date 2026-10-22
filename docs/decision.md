# Decisions

A lightweight log of decisions made for this project and the reasoning behind
them. Newest first. This is intentionally not a heavy ADR process; it records
*what we chose and why* so the choices aren't re-litigated silently.

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
