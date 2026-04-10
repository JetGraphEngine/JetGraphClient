# JetGraph Rust Client

A standalone Rust library for connecting to **[JetGraph](https://jetgraph.io)** over gRPC. This crate vendors the `.proto` definitions under `proto/`, so it can be used independently — consumers only need Rust, `protoc` (for the build step), and a running JetGraph engine.

JetGraph is a purpose-built, in-memory graph engine for real-time decisions across any domain: fraud detection, recommendation systems, network security, supply chain analytics, knowledge graphs, and more. The Rust client is the highest-performance way to connect to it, using a persistent gRPC connection with a typed, ergonomic API.

The engine implementation lives in the sibling [`Graph`](../Graph) repository.

---

## Requirements

- Rust 1.83+
- `protoc` on `PATH` (used by `tonic-build` during `cargo build`)

> The crate pins `tempfile = 3.10.1` for build dependencies so `prost-build` does not pull `getrandom 0.4` (which requires a newer Cargo than 1.83).

---

## Adding to Your Project

**Path dependency** (if this folder is part of your workspace):

```toml
[dependencies]
fraud-graph-client = { path = "../RustGraphClient" }
tokio = { version = "1", features = ["full"] }
```

**Git dependency** (if this lives inside a larger monorepo):

```toml
[dependencies]
fraud-graph-client = { git = "https://github.com/your-org/your-repo.git", branch = "main", path = "RustGraphClient" }
```

Or publish to a private registry / crates.io and depend by version.

---

## Quick Start

```bash
cd RustGraphClient
cargo build
cargo run --example quickstart
```

The `quickstart` example expects a running JetGraph engine at `http://localhost:50051` with the schema already finalized. To spin one up locally:

```bash
docker compose up -d
curl http://localhost:8080/health
# → {"status":"READY","ready":true}
```

---

## Usage Pattern

Every event-processing loop in JetGraph follows a three-phase pattern regardless of domain:

```
Query → Score/Process → Insert
```

1. **Query** — collect all graph signals before making a decision (velocity counts, relationship novelty, risk context from neighbours)
2. **Process** — apply your business logic using those signals
3. **Insert** — always write the event edge, even if the outcome is negative (the graph needs full history for accurate future signals)

```rust
use fraud_graph_client::{
    GraphClient, CreateEdgeRequest, VelocityQuery, FraudContextQuery, FlagRequest, prop,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the engine
    let graph = GraphClient::connect("http://localhost:50051").await?;

    // ── Phase 1: Query graph signals ─────────────────────────────────────────
    let entity_id  = graph.lookup_node("USER",    "user-001").await?;
    let target_id  = graph.lookup_node("PRODUCT", "prod-42").await?;

    // Velocity: how many VIEWED edges in the last 1 hour? (O(1) pre-computed)
    let views_1h = graph
        .get_velocity_count(VelocityQuery {
            node:        entity_id,
            edge_type:   "VIEWED".into(),
            window_secs: 3600,
        })
        .await?
        .count;

    // Novelty: has this user interacted with this product before?
    let is_new_relationship = !graph
        .edge_exists(entity_id, target_id, "VIEWED")
        .await?;

    // Context: aggregated signal from 1-hop neighbours
    let ctx = graph
        .get_fraud_context(FraudContextQuery { node: entity_id })
        .await?;

    // ── Phase 2: Apply your logic ─────────────────────────────────────────────
    let score = compute_score(views_1h, is_new_relationship, ctx.max_neighbor_fraud_score);

    // ── Phase 3: Insert — always record the event ─────────────────────────────
    graph.create_edge(CreateEdgeRequest {
        edge_type_name: "VIEWED".into(),
        src: entity_id,
        dst: target_id,
        properties: vec![prop("score", score)],
    }).await?;

    println!("Signal score: {:.2}", score);
    Ok(())
}
```

---

## Use Cases

JetGraph and this client are domain-agnostic. The same API works across any problem where relationships between entities carry signal:

| Domain | Node Types | Edge Types | Key Signal |
|---|---|---|---|
| **Fraud Detection** | `CARD`, `MERCHANT`, `DEVICE`, `IP` | `TRANSACTS_AT`, `USES_DEVICE` | Velocity, novelty, risk contagion |
| **Recommendations** | `USER`, `PRODUCT`, `CATEGORY` | `VIEWED`, `PURCHASED`, `SIMILAR_TO` | Collaborative filtering via shared edges |
| **Network Security** | `HOST`, `IP`, `USER`, `SERVICE` | `CONNECTS_TO`, `AUTHENTICATES` | Lateral movement, anomalous path detection |
| **Supply Chain** | `SUPPLIER`, `PRODUCT`, `WAREHOUSE` | `SUPPLIES`, `SHIPS_TO`, `STORES` | Dependency tracing, disruption propagation |
| **Knowledge Graph** | `ENTITY`, `CONCEPT`, `SOURCE` | `RELATED_TO`, `CITED_BY` | Entity resolution, cluster detection |

---

## Examples

### `quickstart`

The minimal end-to-end example: connect, register a schema, create nodes, create an edge, and query it back.

```bash
cargo run --example quickstart
```

### `complete_guide`

A comprehensive walkthrough of the full client API — schema registration, node/edge creation, velocity queries, novelty detection, fraud context, and risk flagging. Good starting point for any new integration.

```bash
cargo run --example complete_guide
```

### `ingest_transaction`

Demonstrates the **one-call bulk ingest** RPC: ensure multiple nodes and upsert multiple edges in a single request. Useful for high-throughput pipelines where you want to minimize round trips.

```bash
cargo run --example ingest_transaction
```

The request carries:
- `nodes[]` — `(node_type, external_id, optional request key, optional properties)`
- `edges[]` — `(edge_type, src ref, dst ref, optional numeric/event/bool values)`

Edges can reference nodes by `request_node_key` declared in the same request, or by a normal `NodeRef` (`node_id` / external). The response includes per-node/per-edge results plus aggregate counters for created/updated/errors.

### `grpc_load_test`

Benchmarks the gRPC connection with configurable upsert and query phases. Prints throughput and latency stats (avg, p50, p95, p99, max).

```bash
cargo run --example grpc_load_test -- \
  --endpoint http://localhost:50051 \
  --mode both \
  --edge-type TRANSACTS_AT \
  --src-type CARD \
  --dst-type MERCHANT \
  --pair-count 20000 \
  --upsert-requests 200000 \
  --query-requests 200000 \
  --concurrency 64
```

Adapt `--src-type`, `--dst-type`, and `--edge-type` to match your schema. Use `--bootstrap-schema` on a fresh engine to auto-register the test types before running. Add `--require-sub-ms` to fail the run unless average latency stays below 1 ms.

For lowest latency numbers, run in release mode:

```bash
cargo run --release --example grpc_load_test -- [flags]
```

> **Note:** `--use-external-refs` resolves nodes by external ID on every call (slower). The default fast path uses internal `node_id` references directly.

### `stress_test`

High-concurrency sustained load test for stability and memory profiling under real-world throughput.

```bash
cargo run --example stress_test
```

---

## API Overview

See the crate-level docs in `src/lib.rs` for the full method list. The main client methods:

| Method | Description |
|---|---|
| `lookup_node(type, external_id)` | Resolve an external ID to an internal node ID |
| `create_node(type, external_id, props)` | Create a new node (idempotent by external ID) |
| `edge_exists(src, dst, edge_type)` | Check if a directed relationship exists |
| `create_edge(request)` | Create a directed edge with optional properties |
| `get_velocity_count(query)` | O(1) time-windowed edge count for a node |
| `get_fraud_context(query)` | Aggregated risk signal from 1-hop neighbours |
| `flag_node(request)` | Set a fraud/risk score on a node (propagates automatically) |
| `ingest(request)` | Bulk: ensure N nodes and upsert M edges in one RPC call |

---

## Keeping Protos in Sync

The **canonical** `.proto` definitions live in the engine repository at `Graph/proto/*.proto`. When those files change, copy them into this crate to stay on the latest wire format:

```bash
cp ../Graph/proto/*.proto proto/
cargo build
```

Verify there is no drift between the two copies:

```bash
diff -rq ../Graph/proto proto
```
