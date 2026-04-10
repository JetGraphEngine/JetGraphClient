# Rust Graph Client (`rust-graph-client`)

Standalone Rust library for talking to **JetGraph** over gRPC. This crate vendors the `.proto` files under `proto/`, so you can copy or publish this project by itself—consumers only need Rust, `protoc` (for build), and a running engine.

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

### gRPC load test (upsert + query)

Run the dedicated load-test example:

```bash
cargo run --example grpc_load_test -- \
  --endpoint http://localhost:50051 \
  --mode both \
  --edge-type TRANSACTS_AT \
  --src-type card \
  --dst-type merchant \
  --pair-count 20000 \
  --upsert-requests 200000 \
  --query-requests 200000 \
  --concurrency 64
```

If your engine uses a different schema, set `--src-type`, `--dst-type`, and `--edge-type` to match it.
On a fresh/non-finalized schema, you can auto-register the test types with `--bootstrap-schema`.

What it does:
- creates/ensures node pairs for the configured source/destination types
- seeds edge pairs so query tests hit existing relationships
- runs warmup + measured phases for upsert and/or query over gRPC
- prints throughput and latency stats (avg, p50, p95, p99, max)

For lowest latency numbers, run in release mode and keep the default `node_id` fast path
(`--use-external-refs` disables it and is slower because the engine must resolve external IDs).
Add `--require-sub-ms` to make the run fail unless `lat_avg_ms < 1.000`.

### One-call transaction ingest (best-effort)

Use one RPC to ensure multiple nodes and upsert multiple edges in one request:

```bash
cargo run --example ingest_transaction
```

The request carries:
- `nodes[]`: `(node_type, external_id, optional request key, optional properties)`
- `edges[]`: `(edge_type, src ref, dst ref, optional numeric/event/bool values)`

Edges can reference nodes by `request_node_key` declared in the same request, or by normal
`NodeRef` (`node_id` / external). The response is best-effort and includes per-node/per-edge
results plus aggregate counters for created/updated/errors.

## API overview

See the crate-level docs (`src/lib.rs`) and the former integration notes in the engine repo’s `INTEGRATION_MANUAL.md` (Section 16 — update paths to point at this project).

### Keeping protos in sync

The **canonical** definitions live in the engine repo: **`Graph/proto/*.proto`**.  
When those files change, copy them here so this crate stays on the latest wire format:

```bash
cp ../Graph/proto/*.proto proto/
cargo build
```

Verify there is no drift:

```bash
diff -rq ../Graph/proto proto
```
