# Rust Graph Client (`fraud-graph-client`)

Standalone Rust library for talking to the **Fraud Graph Engine** over gRPC. This crate vendors the `.proto` files under `proto/`, so you can copy or publish this project by itself—consumers only need Rust, `protoc` (for build), and a running engine.

The engine implementation lives in the sibling [`Graph`](../Graph) repository.

## Requirements

- Rust 1.83+
- `protoc` on `PATH` (used by `tonic-build` during `cargo build`)

The crate pins `tempfile = 3.10.1` for build dependencies so `prost-build` does not pull `getrandom 0.4` (which requires a newer Cargo than 1.83).

> **Note:** The client project directory is **`RustGraphClient`**. If you created an empty `RustGraphClent` folder by mistake, you can remove it and use this tree instead.

## Use in another application

```toml
[dependencies]
fraud-graph-client = { path = "../RustGraphClient" }
tokio = { version = "1", features = ["full"] }
```

Or depend via git when this folder lives inside a larger repo (replace URL/branch):

```toml
[dependencies]
fraud-graph-client = { git = "https://github.com/your-org/your-monorepo.git", branch = "main", path = "RustGraphClient" }
```

Or publish this crate to a private registry / crates.io and depend by version.

## Build & example

```bash
cd RustGraphClient
cargo build
cargo run --example quickstart
```

`quickstart` expects an engine at `http://localhost:50051` with schema already finalized.

## API overview

See the crate-level docs (`src/lib.rs`) and the former integration notes in the engine repo’s `INTEGRATION_MANUAL.md` (Section 16 — update paths to point at this project).

### Keeping protos in sync

When the engine’s `proto/*.proto` changes, copy the updated files into this project’s `proto/` directory and bump the crate version if you publish.
