# AGENTS.md

Instructions for agents (and humans) working in this repository.

## What this project is

`cordis-rust` is a from-scratch Rust reimplementation of the
[Cordis](https://github.com/cordiverse/cordis) meta-framework.

**Status: M4 global-slot services and injection convergence implemented.**
The root Rust 2024 workspace contains only the unpublished `crates/cordis-core`
(Rust 1.85 minimum). Use `cargo test --workspace`.
Publication/access/dependency-retention contracts are in `docs/m4.md`;
Plugin/Registry/scoped-resource contracts are in `docs/m3.md`, with effects,
events, and diagnostics in `docs/m2.md`. Isolation/intercepts/tracing, config
updates, and complete internal hook protocols remain M5/M6. The isolated `spikes/m0`
package remains research evidence; run it with its own `--manifest-path`.

## Source of truth

The **TypeScript `cordis`** project is the specification. When behavior is
ambiguous or two references disagree, the TS implementation wins.

Two reference checkouts live under `local/` (git-ignored, cloned locally):

- `local/cordis` — the original TypeScript Cordis. **This is the spec.**
  Core concepts live in `local/cordis/packages/core/src/`.
- `local/cordis-rs` — an independent prior Rust port
  ([dshbox/cordis-rs](https://github.com/dshbox/cordis-rs)). **Reference only.**
  Useful for seeing how others solved a problem in Rust; it is not a target and
  may diverge from upstream. Do not treat its decisions as authoritative.

We mirror the *semantics* of TS Cordis, not its mechanisms. The original relies
on JavaScript-only features (Proxy-based reflection, prototype-chain
inheritance, callable objects) that have no direct Rust equivalent. See
`docs/limitation.md`.

## Keep the docs current

This repository front-loads documentation. Whenever you make a meaningful
change, update the relevant doc in the same change:

- `README.md` — project front door: intent, concept map, planned layout.
- `CHANGELOG.md` — notable changes (Keep a Changelog style).
- `docs/todo.md` — the porting roadmap.
- `docs/decision.md` — decisions made and why.
- `docs/limitation.md` — known gaps and unresolved tensions.

## Tooling conventions

- After implementing and verifying each milestone, commit its code and related
  documentation on the current branch. Do not push unless explicitly requested.
- Use `rg` (ripgrep) instead of `grep` for searching.
- If a `.codegraph/` directory appears at the repo root, prefer CodeGraph
  (`codegraph explore ...`) over grep/find for understanding code. Indexing is
  the maintainer's decision — do not run it unprompted.

These conventions are intentionally lean and will grow as the project does.
