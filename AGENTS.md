# adaptif-gatotkaca-runtime

Runtime abstraction crate for the adaptif-gatotkaca engine, developed by PT Adaptif Sinergi Dinamis. It is mostly traits: a stable
interface that hides which runtime executes the work (tokio, a thread pool, or another executor). Public crate, dual licensed
MIT OR Apache-2.0.

## Layout

- `src/lib.rs`: the crate entry point. Declares modules and re-exports; keep it thin.
- `src/runtime.rs`: the runtime traits.

## Rules

- **No runtime dependency.** tokio is a dev-dependency only. Never add it, or any executor, to `[dependencies]`. Implementations of
  the traits for tokio or thread pools live in the crates that use them, or behind an optional feature.
- **Runtime-agnostic signatures.** Public traits use `std` types (`Future`, `Pin`, `Poll`, `Waker`). No tokio types in a public signature.
- **Public API is documented.** `missing_docs` is a warning; every public item needs a doc comment.
- **Stable surface.** This is a published crate. A change to a public trait is a breaking change: note it in `CHANGELOG.md`.
- Type names carry the `Gk` prefix, matching the other gatotkaca crates.
- MSRV is 1.89 (`rust-version` in `Cargo.toml`).

## Commands

```sh
cargo fmt --all                                  # rustfmt.toml: 140 columns
cargo clippy --all-targets -- -D warnings
cargo test
```

Run all three before finishing a change.

## Style

- Comments and docs: plain, short sentences. Explain why, not what.
- Tests that need a runtime use `#[tokio::test]` (dev-dependency).
