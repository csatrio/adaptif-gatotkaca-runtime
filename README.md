# adaptif-gatotkaca-runtime

[![crates.io](https://img.shields.io/crates/v/adaptif-gatotkaca-runtime.svg)](https://crates.io/crates/adaptif-gatotkaca-runtime)
[![docs.rs](https://docs.rs/adaptif-gatotkaca-runtime/badge.svg)](https://docs.rs/adaptif-gatotkaca-runtime)

Part of the adaptif-gatotkaca engine: the runtime abstraction layer,
developed by [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).

A set of traits that give gatotkaca a stable interface over the runtime underneath it. gatotkaca leans heavily on tokio, but an
implementation may run on a thread pool instead. Code written against this crate does not change when the runtime does.

## Install

```toml
[dependencies]
adaptif-gatotkaca-runtime = "0.1"
```

## Status

Early development. The API is not stable yet.

## Development

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

Formatting is set by `rustfmt.toml` (140 columns). tokio is a dev-dependency only, used by tests and examples.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate, as defined
in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
