# Decisions

A lightweight log of decisions made for this project and the reasoning behind
them. Newest first. This is intentionally not a heavy ADR process; it records
*what we chose and why* so the choices aren't re-litigated silently.

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
