# JetGraph Rust Client

A standalone Rust library for connecting to **[JetGraph](https://jetgraph.io)** over gRPC. This crate vendors the `.proto` definitions under `proto/`, so it can be used independently — consumers only need Rust, `protoc` (for the build step), and a running JetGraph engine.

JetGraph is a purpose-built, in-memory graph engine for real-time decisions across any domain: fraud detection, recommendation systems, network security, supply chain analytics, knowledge graphs, and more. The Rust client is the highest-performance way to connect to it, using a persistent gRPC connection with a typed, ergonomic API.

The engine implementation lives in the sibling [`Graph`](../Graph) repository.

---

## Requirements

- Rust 1.85+
- `protoc` on `PATH` (used by `tonic-build` during `cargo build`)

> The crate pins `tempfile = 3.10.1` for build dependencies so `prost-build` does not pull `getrandom 0.4` (which requires a newer Cargo than 1.83).

---

## Adding to Your Project

**Path dependency** (if this folder is part of your workspace):

```toml
[dependencies]
jetgraph-client = { path = "../RustGraphClient" }
tokio = { version = "1", features = ["full"] }
```

**Git dependency** (if this lives inside a larger monorepo):

```toml
[dependencies]
jetgraph-client = { git = "https://github.com/your-org/your-repo.git", branch = "main", path = "RustGraphClient" }
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
use jetgraph_client::{Client, NodeRef, TransactionNode, TransactionEdge, TransactionNodeRef};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Connect ───────────────────────────────────────────────────────────────
    let client = Client::connect("http://localhost:50051").await?;

    // ── Phase 1: Query graph signals ──────────────────────────────────────────

    // Point lookup: has this card ever been seen at this merchant?
    let edge_state = client.get_edge_state(
        "TRANSACTS_AT",
        NodeRef::external("card", "card-001"),
        NodeRef::external("merchant", "merch-42"),
        None,
        Some(&[3600, 86400]),   // counts for last 1 h and 24 h activity windows
    ).await?;

    let is_new_relationship = edge_state.is_none();
    let views_1h = edge_state.as_ref()
        .and_then(|s| s.activity_counts.first().copied())
        .unwrap_or(0);

    // Aggregated risk context from 1-hop neighbours
    let ctx = client.features().get_fraud_context(
        NodeRef::external("card", "card-001"),
    ).await?;

    // ── Phase 2: Apply your logic ─────────────────────────────────────────────
    let is_risky = is_new_relationship && views_1h > 10
        || ctx.max_neighbor_fraud_score > 0.8;

    // ── Phase 3: Insert — always record the event ─────────────────────────────
    client.ingest_transaction(
        Some("txn-unique-id"),
        &[
            TransactionNode::new("card",     "card-001"),
            TransactionNode::new("merchant", "merch-42"),
        ],
        &[
            TransactionEdge::new(
                "TRANSACTS_AT",
                TransactionNodeRef::request_node_key("card-001"),
                TransactionNodeRef::request_node_key("merch-42"),
            ),
        ],
    ).await?;

    println!("Is risky: {is_risky}");
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

### `scale_growth_test`

Progressive scale test that loads up to **30 million nodes** across three node types (`user`, `product`, `device`) and up to **10 million edges** (`VIEWS`, `PURCHASES`, `USES_DEVICE`), pausing at regular checkpoints to measure how query latency and write throughput change as the graph grows. Identifies the breaking point where performance degrades.

```bash
# Quick run (1 M nodes, 1 M edges)
cargo run --release --example scale_growth_test -- --bootstrap-schema

# Full 30 M node / 10 M edge run
cargo run --release --example scale_growth_test -- \
  --bootstrap-schema \
  --user-count    15000000 \
  --product-count 10000000 \
  --device-count   5000000 \
  --edge-count    10000000 \
  --checkpoint-every 1000000 \
  --concurrency 64 \
  --probe-iters 200
```

At each checkpoint the test prints a latency table across five query types:

| Query type | What it measures |
|---|---|
| `edge_state` | Point lookup — does this user→product edge exist? |
| `edge_state+windows` | Same + activity counts for last 1 h and 24 h windows |
| `neighbors_out_50` | Outbound VIEWS neighbours of a user (top 50) |
| `neighbors_in_50` | Inbound VIEWS to a product (top 50 users who viewed it) |
| `two_hop` | User → products → other users who viewed those products |

A ⚠ warning is printed when `p99 > 10 ms` or `error_rate > 1 %`. The final summary table shows all checkpoints side-by-side to reveal exactly where performance degraded and at what node/edge count.

---

## API Overview

See the crate-level docs in `src/lib.rs` for the full method list. The unified `Client` is the recommended entry point; `GraphClient`, `SchemaClient`, `FeatureClient`, and `SegmentClient` are available for fine-grained access.

### Graph operations (`client.graph()` or `Client` convenience methods)

| Method | Description |
|---|---|
| `create_node(type, external_id, props)` | Create or retrieve a node (idempotent by external ID) |
| `upsert_edge(edge_type, src, dst, amount, ts, bool_flag)` | Write a compact edge; updates activity window, bins, and sum |
| `get_edge_state(edge_type, src, dst, …)` | Point lookup with optional activity-window counts |
| `get_neighbors(node, edge_type, …)` | Paginated neighbour list with optional property filters |
| `ingest_transaction(txn_id, nodes, edges)` | Bulk: ensure N nodes + upsert M edges in one RPC call |
| `ingest_stream()` | High-throughput streaming ingest (persistent connection, micro-batched) |

### Schema operations (`client.schema()`)

| Method | Description |
|---|---|
| `register_node_type(name)` | Declare a new node type |
| `register_compact_edge_type(name, from, to, …)` | Declare an edge type with activity bitmap and value bins |
| `register_static_edge_type(name, from, to, ttl, symmetric)` | Declare a minimal-payload edge type (e.g. SIMILAR_TO) |
| `register_property(node_type, name, value_type)` | Add a typed property to a node type |
| `finalize()` | Lock the schema and start accepting ingest traffic |

### Feature operations (`client.features()`)

| Method | Description |
|---|---|
| `query_node_histogram(node, edge_type, window_hours, window_days)` | Time-windowed event counts from the node histogram |
| `get_node_feature_vector(node, edge_types, windows)` | Pre-computed multi-signal feature vector for a node |
| `get_fraud_context(node)` | Aggregated risk signal from 1-hop neighbours |
| `flag_node(node, score)` | Set a fraud/risk score (propagates to neighbours automatically) |
| `find_similar_nodes(node, weights, …)` | Real-time weighted Jaccard k-NN |
| `build_similarity_graph(node_type, weights, …)` | Batch-build SIMILAR_TO edges for all nodes of a type |

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
