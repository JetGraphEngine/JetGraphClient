//! # JetGraph — Complete Developer Guide
//!
//! This file demonstrates every capability of the `jet_graph_client` library.
//! Work through it top-to-bottom; each section builds on the previous ones.
//!
//! ## Prerequisites
//!
//! 1. JetGraph must be running:
//!    ```bash
//!    cargo run --release --bin jetgraph
//!    # or the pre-built binary:
//!    ./target/release/jetgraph
//!    ```
//!
//! 2. Run this example:
//!    ```bash
//!    cargo run --example complete_guide
//!    ```
//!
//! ## Schema used throughout
//!
//! ```text
//! Node types:   card, merchant, device, ip
//!
//! Edge types:
//!   TRANSACTS_AT  card → merchant  (90d TTL, amount bins, 1h activity bitmap)
//!   USES_DEVICE   card → device    (30d TTL, no bins, 5min activity bitmap)
//!   SIMILAR_TO    card → card      (24h TTL, static/minimal 8-byte payload)
//! ```
//!
//! ## How the engine stores edges (read this first)
//!
//! Every edge type stores a compact payload per `(src, dst)` pair:
//!
//! - `tx_count`          – total number of times this edge was upserted (event count)
//! - `last_seen`         – Unix timestamp (seconds) of the most recent upsert
//! - `activity_bitmap`   – 64-bit sliding-window bitmap, one bit per `tick_size_secs`
//!                         e.g. 1h ticks → 64 hours of activity history in 8 bytes
//! - `bins[0..7]`        – per-amount-bucket event counts (only on edge types with
//!                         `bin_boundaries` defined, e.g. TRANSACTS_AT)
//! - `approx_sum`        – accumulated numeric_value (e.g. total USD spend)
//! - `bool_flag`         – optional single boolean property stored in bit 63 of the
//!                         activity flags field (e.g. "is_international")
//!
//! Static (minimal-payload) edge types such as SIMILAR_TO only store `approx_sum`
//! (the similarity score) and `last_seen`. All other fields are zero/unused.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_stream::StreamExt;

use fraud_graph_client::{
    // Core client
    Client,
    GraphClient,

    // Node / edge reference types
    NodeRef,
    PropertyEntry,
    NodePropertyFilter,

    // Transaction ingest types
    TransactionNode,
    TransactionEdge,
    TransactionNodeRef,

    // Similarity & segment helpers
    EdgeTypeWeight,
    BoolPropertyWeight,

    // Schema enum
    schema::schema_proto::ValueType,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current Unix timestamp in seconds.
fn now_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

fn secs_ago(s: u32) -> u32 { now_secs().saturating_sub(s) }
fn hours_ago(h: u32) -> u32 { secs_ago(h * 3_600) }
fn days_ago(d: u32)  -> u32 { secs_ago(d * 86_400) }

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // =========================================================================
    // SECTION 1 — Connect & Health Check
    // =========================================================================
    //
    // `Client::connect` opens a single multiplexed HTTP/2 (gRPC) connection.
    // All sub-clients (graph, schema, features, segment) share this channel, so
    // you only need one `Client` per process.
    //
    // Clone the client freely — it is cheap (an Arc around the channel).

    println!("\n=== 1. Connect ===");

    let client = Client::connect("http://localhost:50051").await?;

    // Check the engine is ready before doing any work.
    let ready = client.health().check().await?;
    assert!(ready, "Engine is not READY — start the engine first");
    println!("Engine status: READY");

    // =========================================================================
    // SECTION 2 — Schema Registration
    // =========================================================================
    //
    // The schema defines what node types, edge types, and properties exist.
    // You must register the schema ONCE before writing any data.
    //
    // Schema is persistent: the engine remembers it across restarts.
    // Calling register_node_type/register_compact_edge_type a second time
    // for an already-existing type is a no-op (idempotent).
    //
    // IMPORTANT: call `finalize()` after all registrations. Until finalized,
    // no ingest calls will be accepted. After finalization the schema is locked
    // — add new types, then finalize again to bump the schema version.

    println!("\n=== 2. Schema Registration ===");

    let mut schema = client.schema();

    // --- Node types ----------------------------------------------------------
    //
    // `numeric_ids = false` → external IDs are strings (e.g. "card-00001").
    //                         Use for most real-world IDs that contain dashes etc.
    // `numeric_ids = true`  → external IDs are pure integers stored as u64.
    //                         Saves ~28 bytes per node. Use when all IDs are numbers.
    //
    // WARNING: the kind is frozen at registration. Changing it after data is written
    // corrupts NodeIds and requires a full data migration.

    schema.register_node_type("card",     false).await?;
    schema.register_node_type("merchant", false).await?;
    schema.register_node_type("device",   false).await?;
    schema.register_node_type("ip",       false).await?;
    println!("  Node types registered: card, merchant, device, ip");

    // --- Compact edge type: TRANSACTS_AT (card → merchant) -------------------
    //
    // Parameters:
    //   name              – edge type name (SCREAMING_SNAKE_CASE by convention)
    //   from_node_type    – source node type name (must be registered above)
    //   to_node_type      – destination node type name
    //   state_ttl_secs    – after this many seconds without an upsert the engine
    //                       may expire the edge. 0 = permanent (never expires).
    //   bin_boundaries    – amount thresholds (N thresholds → N+1 bins).
    //                       e.g. [5, 25, 50, 100] → bins:
    //                         bin0 < $5 | bin1 $5-$25 | bin2 $25-$50 |
    //                         bin3 $50-$100 | bin4 ≥ $100
    //                       Pass `vec![]` for no amount tracking.
    //   tracked_property  – human-readable name for the numeric value being binned
    //                       (informational only, e.g. "amount_usd")
    //   activity_tick_size_secs – granularity of the 64-bit activity bitmap.
    //                       3600 = 1-hour ticks → 64 hours of history.
    //                       300  = 5-min ticks  → ~5.3 hours of history.
    //   bool_property_name – optional name for a boolean flag stored in bit 63
    //                        of the activity flags field (e.g. "is_international").
    //                        Pass `None` for no boolean property.
    //   symmetric         – when true the edge is undirected: upserts normalize
    //                       to (min(src,dst), max(src,dst)). Only valid when
    //                       from_node_type == to_node_type.

    schema.register_compact_edge_type(
        "TRANSACTS_AT",
        "card",
        "merchant",
        90 * 86_400,                                        // 90-day TTL
        vec![5.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0], // 7 thresholds → 8 bins
        "amount_usd",
        3_600,                                              // 1-hour activity ticks
        Some("is_international"),                           // bool property on bit 63
        false,                                              // directed
    ).await?;

    // --- Compact edge type: USES_DEVICE (card → device) ----------------------
    //
    // No amount bins (vec![]) and no bool property — only activity tracking.
    // 5-minute ticks let us detect multiple logins from different devices
    // within a short window (card-sharing / device-farm signals).

    schema.register_compact_edge_type(
        "USES_DEVICE",
        "card",
        "device",
        30 * 86_400,    // 30-day TTL
        vec![],         // no amount bins
        "",
        300,            // 5-minute activity ticks
        None,           // no bool property
        false,
    ).await?;

    // --- Static edge type: SIMILAR_TO (card → card) --------------------------
    //
    // `register_static_edge_type` creates a minimal 8-byte payload edge:
    //   - `approx_sum` stores the similarity score (f32)
    //   - `last_seen`  stores the timestamp of the last score update
    //   - No activity bitmap, no bins, no tx_count
    //
    // Use for computed/derived relationships where only the latest value matters.
    // `symmetric = true` because similarity is undirected.

    schema.register_static_edge_type(
        "SIMILAR_TO",
        "card",
        "card",
        86_400,     // 24-hour TTL — recomputed daily
        true,       // symmetric (undirected)
    ).await?;
    println!("  Edge types registered: TRANSACTS_AT, USES_DEVICE, SIMILAR_TO");

    // --- Node properties -----------------------------------------------------
    //
    // Properties are stored per-node and can be read back via `get_node`.
    // They are also available for server-side filtering in `get_neighbors`
    // (see Section 7).
    //
    // Supported types: Int (i64), Float (f64), String, Bool, Timestamp (i64 unix secs).

    schema.register_property("card_type",    "card",     true, ValueType::String).await?;
    schema.register_property("credit_limit", "card",     true, ValueType::Float).await?;
    schema.register_property("is_virtual",   "card",     true, ValueType::Bool).await?;
    schema.register_property("name",         "merchant", true, ValueType::String).await?;
    schema.register_property("mcc",          "merchant", true, ValueType::Int).await?;
    schema.register_property("country",      "merchant", true, ValueType::String).await?;
    println!("  Properties registered");

    // --- Finalize ------------------------------------------------------------
    //
    // Finalizing locks the schema and increments the schema version counter.
    // Call this every time you add new types or properties.

    let version = schema.finalize().await?;
    println!("  Schema finalized — version {version}");

    // --- Inspect the schema --------------------------------------------------
    //
    // `get_schema` returns the full current schema. Useful for debugging
    // or verifying that all expected types are registered.

    let s = schema.get_schema().await?;
    println!("  Schema v{}: {} node types, {} edge types",
        s.schema_version, s.node_types.len(), s.edge_types.len());
    for et in &s.edge_types {
        println!("    edge '{}': {} → {} | ttl={}s | tick={}s | symmetric={}",
            et.name, et.from_node_type, et.to_node_type,
            et.state_ttl_secs, et.tick_size_secs, et.is_symmetric);
    }

    // =========================================================================
    // SECTION 3 — Node Operations
    // =========================================================================
    //
    // Nodes are identified by a (node_type, external_id) pair.
    // `create_node` is content-addressable: calling it twice with the same
    // (node_type, external_id) returns the same NodeId both times — the second
    // call is a no-op for the node itself (created=false).

    println!("\n=== 3. Node Operations ===");

    // --- Create a single node with properties --------------------------------

    let card_result = client.create_node(
        "card",
        Some("card-demo-001"),
        &[
            PropertyEntry::string("card_type",    "CREDIT"),
            PropertyEntry::float("credit_limit",  5_000.0),
            PropertyEntry::bool("is_virtual",     false),
        ],
    ).await?;
    println!("  card-demo-001 → node_id={} created={}",
        card_result.node_id, card_result.created);

    // Create the merchant we'll transact at throughout these examples.
    let merchant_result = client.create_node(
        "merchant",
        Some("merchant-demo-001"),
        &[
            PropertyEntry::string("name",    "Demo Coffee Shop"),
            PropertyEntry::int("mcc",        5812),         // restaurant MCC
            PropertyEntry::string("country", "US"),
        ],
    ).await?;
    println!("  merchant-demo-001 → node_id={} created={}",
        merchant_result.node_id, merchant_result.created);

    // Create some devices and IPs for later examples.
    client.create_node("device", Some("device-demo-001"), &[]).await?;
    client.create_node("device", Some("device-demo-002"), &[]).await?;
    client.create_node("ip",     Some("ip-us-001"),       &[]).await?;
    client.create_node("ip",     Some("ip-uk-001"),       &[]).await?;
    println!("  Created device-demo-001/002, ip-us-001, ip-uk-001");

    // --- Read a node back ----------------------------------------------------
    //
    // `get_node` returns node_id, node_type, external_id, and all properties.

    let node = client.graph().get_node(
        NodeRef::external("card", "card-demo-001")
    ).await?;
    println!("  get_node: id={} type={} ext={:?}",
        node.node_id, node.node_type, node.external_id);
    for prop in &node.properties {
        println!("    prop '{}' = {:?}", prop.name, prop.value);
    }

    // --- Reference nodes by NodeId vs external (type, id) --------------------
    //
    // Every operation that accepts a `NodeRef` supports two forms:
    //
    //   NodeRef::external("card", "card-demo-001")  → resolved by external ID lookup
    //   NodeRef::node_id(card_result.node_id)       → direct numeric lookup (faster)
    //
    // If you already have the node_id from a previous call, prefer NodeRef::node_id.

    let _same_node_by_id = client.graph().get_node(
        NodeRef::node_id(card_result.node_id)
    ).await?;

    // --- List nodes ----------------------------------------------------------

    let (nodes, total) = client.graph().list_nodes("card", "", 10).await?;
    println!("  list_nodes(card): found {} (total {})", nodes.len(), total);

    // =========================================================================
    // SECTION 4 — Single Edge Upsert
    // =========================================================================
    //
    // `upsert_edge` creates or updates a (src, dst) edge under the given edge type.
    //
    // Each call:
    //   - Increments `tx_count` by 1
    //   - Updates `last_seen` to `event_ts_secs` (or server clock if None)
    //   - Updates the activity bitmap based on elapsed ticks since `last_seen`
    //   - Adds `numeric_value` to `approx_sum` and increments the matching bin
    //   - Optionally sets `bool_property_value` (bit 63 of the activity flags)
    //
    // Returns the full edge payload after the update.

    println!("\n=== 4. Single Edge Upsert ===");

    // First transaction: domestic purchase 3 days ago.
    let r = client.upsert_edge(
        "TRANSACTS_AT",
        NodeRef::external("card",     "card-demo-001"),
        NodeRef::external("merchant", "merchant-demo-001"),
        Some(42.50),                // amount → goes into the $25-$50 bin
        Some(days_ago(3)),          // historical timestamp (3 days ago)
        Some(false),                // is_international = false
    ).await?;
    println!("  upsert #1: created_new={} tx_count={} approx_sum={:.2}",
        r.created_new, r.tx_count, r.approx_sum);
    println!("  bins: {:?}", r.bins);

    // Second transaction: larger domestic purchase today.
    let r = client.upsert_edge(
        "TRANSACTS_AT",
        NodeRef::external("card",     "card-demo-001"),
        NodeRef::external("merchant", "merchant-demo-001"),
        Some(120.00),               // amount → goes into the $100-$250 bin
        Some(hours_ago(2)),
        Some(false),
    ).await?;
    println!("  upsert #2: created_new={} tx_count={} approx_sum={:.2}",
        r.created_new, r.tx_count, r.approx_sum);

    // Third transaction: international purchase today (bool_flag = true).
    let r = client.upsert_edge(
        "TRANSACTS_AT",
        NodeRef::external("card",     "card-demo-001"),
        NodeRef::external("merchant", "merchant-demo-001"),
        Some(890.00),               // amount → goes into the $500-$1k bin
        Some(now_secs()),
        Some(true),                 // is_international = true
    ).await?;
    println!("  upsert #3: tx_count={} approx_sum={:.2} bool_flag={:?}",
        r.tx_count, r.approx_sum, r.bool_flag);

    // =========================================================================
    // SECTION 5 — Read Edge State
    // =========================================================================
    //
    // `get_edge_state` reads the current payload for a specific (src, dst) pair.
    //
    // Optional parameters:
    //   min_value / max_value – filter the read to edges whose `approx_sum` is
    //                           within the given range (returns `filtered_count`
    //                           and `filtered_approx_sum` for the matching window).
    //   query_time_secs       – evaluate the activity bitmap as if it were this
    //                           time (useful for back-testing).
    //   activity_windows_secs – list of window sizes in seconds; for each window
    //                           the engine counts how many activity bitmap ticks
    //                           are set within that window. Returned in
    //                           `activity_counts` in the same order.

    println!("\n=== 5. Read Edge State ===");

    let state = client.get_edge_state(
        "TRANSACTS_AT",
        NodeRef::external("card",     "card-demo-001"),
        NodeRef::external("merchant", "merchant-demo-001"),
        None,                       // no specific query_time
        Some(&[3_600, 86_400]),     // how many activity ticks in the last 1h / 24h?
    ).await?;

    if let Some(s) = state {
        println!("  tx_count={}  approx_sum=${:.2}  last_seen={}s ago",
            s.tx_count, s.approx_sum, now_secs().saturating_sub(s.last_seen));
        println!("  bins: {:?}", s.bins);
        println!("  activity_bitmap=0x{:016x}", s.activity_bitmap_raw);
        println!("  activity_counts(1h, 24h): {:?}", s.activity_counts);
        println!("  bool_flag (is_international): {:?}", s.bool_flag);
    }

    // =========================================================================
    // SECTION 6 — Transaction Ingest (best-effort batch)
    // =========================================================================
    //
    // `ingest_transaction` atomically ensures nodes exist and upserts edges in
    // a single gRPC call. It is the preferred write path when you want to:
    //   - Create nodes and edges together without two separate calls
    //   - Reference nodes by a request-scoped key rather than an external ID
    //
    // The engine processes all `nodes` first (idempotent create), then resolves
    // all `edges`. Errors are per-item — one failed edge does not abort the others.
    //
    // Throughput: ~26,000 transactions/second per connection.
    // For higher throughput see Section 7 (ingest_stream).

    println!("\n=== 6. Transaction Ingest ===");

    // --- request_node_key linkage --------------------------------------------
    //
    // When you list a node in `nodes[]` and give it a `request_node_key`, you
    // can reference that node in `edges[]` by key instead of by external ID.
    // This avoids a separate node-ID lookup and is the most efficient pattern
    // when writing new nodes and edges together in one request.

    let nodes = vec![
        TransactionNode::new("card",     "card-demo-002").with_key("card"),
        TransactionNode::new("merchant", "merchant-demo-002")
            .with_key("merch")
            .with_properties(vec![
                PropertyEntry::string("name",    "Demo Bookstore"),
                PropertyEntry::int("mcc",        5942),
                PropertyEntry::string("country", "US"),
            ]),
    ];

    // Build an edge that links nodes declared in this same request by key.
    // The edge types TransactionNodeRef::request_node_key and
    // TransactionNodeRef::node(NodeRef::external(...)) can be freely mixed.
    let mut edge_by_key = TransactionEdge::new(
        "TRANSACTS_AT",
        TransactionNodeRef::request_node_key("card"),  // resolved from nodes[] above
        TransactionNodeRef::request_node_key("merch"),
    ).with_key("purchase-001");
    edge_by_key.numeric_value = Some(29.99);
    edge_by_key.event_ts_secs = Some(hours_ago(1));

    // Mix: src by request key, dst by direct external ref.
    let mut edge_mixed = TransactionEdge::new(
        "USES_DEVICE",
        TransactionNodeRef::request_node_key("card"),
        TransactionNodeRef::node(NodeRef::external("device", "device-demo-001")),
    ).with_key("device-link");
    edge_mixed.event_ts_secs = Some(hours_ago(1));

    let result = client
        .ingest_transaction(
            Some("txn-guide-001"),
            &nodes,
            &[edge_by_key, edge_mixed],
        )
        .await?;

    println!("  transaction_id={}", result.transaction_id);
    println!("  nodes: created={} existing={} errors={}",
        result.nodes_created, result.nodes_existing, result.node_errors);
    println!("  edges: created={} updated={} errors={}",
        result.edges_created, result.edges_updated, result.edge_errors);

    // Inspect per-item results — useful when you need the allocated NodeId or
    // want to confirm which edges were newly created vs updated.
    for n in &result.node_results {
        println!("  node[{}] key={:?} node_id={:?} created={} error={:?}",
            n.index, n.request_node_key, n.node_id, n.created, n.error);
    }
    for e in &result.edge_results {
        println!("  edge[{}] key={:?} created_new={} error={:?}",
            e.index, e.request_edge_key, e.created_new, e.error);
    }

    // =========================================================================
    // SECTION 7 — High-Throughput Streaming Ingest (IngestStream)
    // =========================================================================
    //
    // `ingest_stream` opens a persistent bidirectional gRPC stream.
    //
    // The server accumulates incoming requests into micro-batches (up to 256
    // transactions or 2 ms, whichever comes first), merges all edges from all
    // active streams by (edge_type, src), and applies each group with a single
    // sorted-merge RCU pass. This amortises lock acquisition and memory cloning
    // across the entire batch, giving ~175,000 edge writes/second.
    //
    // When to use ingest_stream vs ingest_transaction:
    //   - ingest_transaction: up to ~26k ops/sec; simpler, one call per batch
    //   - ingest_stream:      up to ~175k ops/sec; persistent connection, fire-and-forget
    //
    // The stream is opened ONCE and stays open until you drop the sender.
    // Use multiple concurrent streams (one per worker task) to saturate the engine.
    //
    // IMPORTANT: always spawn a separate task to drain responses. If the response
    // buffer fills up, the server's send path blocks and throughput drops.

    println!("\n=== 7. High-Throughput Streaming Ingest ===");

    // --- Basic streaming pattern ---------------------------------------------

    let (tx, rx) = client.ingest_stream().await?;

    // Spawn a response reader task so sends never stall on a full response buffer.
    let reader = tokio::spawn(async move {
        let mut rx = rx;
        let mut edges_created = 0u32;
        let mut edges_updated = 0u32;
        while let Some(Ok(resp)) = rx.next().await {
            edges_created += resp.edges_created;
            edges_updated += resp.edges_updated;
        }
        (edges_created, edges_updated)
    });

    // Send 200 card→merchant transactions.
    // `GraphClient::build_ingest_request` converts high-level types to the proto
    // format expected by the sender — it does NOT make any network call.
    for i in 0u32..200 {
        let card_id     = format!("card-stream-{i:04}");
        let merchant_id = format!("merchant-demo-{:03}", (i % 3) + 1);
        let amount      = 10.0 + (i % 50) as f32 * 5.0;

        // Ensure both nodes exist as part of the same request.
        let req = GraphClient::build_ingest_request(
            Some(&format!("stream-tx-{i}")),
            &[
                TransactionNode::new("card",     &card_id),
                TransactionNode::new("merchant", &merchant_id),
            ],
            &[{
                let mut edge = TransactionEdge::new(
                    "TRANSACTS_AT",
                    TransactionNodeRef::node(NodeRef::external("card",     &card_id)),
                    TransactionNodeRef::node(NodeRef::external("merchant", &merchant_id)),
                );
                edge.numeric_value = Some(amount);
                edge.event_ts_secs = Some(secs_ago(i * 60)); // spread over last 3 hours
                edge
            }],
        );

        // `.send()` is non-blocking as long as the internal buffer (1024 slots) is
        // not full. If it fills up, `.send()` blocks until space is available —
        // this is natural backpressure from the server.
        tx.send(req).await?;
    }

    // Dropping the sender signals end-of-stream to the server. The server will
    // flush the final micro-batch and close the response stream.
    drop(tx);

    // Wait for all responses to come back.
    let (created, updated) = reader.await?;
    println!("  Streaming ingest done: created={} updated={}", created, updated);

    // --- Multiple concurrent streams for maximum throughput ------------------
    //
    // Open one stream per worker. The server micro-batches across all of them
    // simultaneously, so edges from different workers can be merged together.

    println!("  Launching 4 concurrent streams...");
    let client_arc = Arc::new(client.clone());
    let mut handles = Vec::new();

    for worker in 0u32..4 {
        let c = Arc::clone(&client_arc);
        handles.push(tokio::spawn(async move {
            let (tx, rx) = c.ingest_stream().await?;
            let reader = tokio::spawn(async move {
                let mut rx = rx;
                let mut total = 0u32;
                while let Some(Ok(r)) = rx.next().await { total += r.edges_created; }
                total
            });
            for i in 0u32..500 {
                let card = format!("card-w{worker}-{i:04}");
                let merch = format!("merchant-demo-{:03}", (i % 3) + 1);
                let mut edge = TransactionEdge::new(
                    "TRANSACTS_AT",
                    TransactionNodeRef::node(NodeRef::external("card",     &card)),
                    TransactionNodeRef::node(NodeRef::external("merchant", &merch)),
                );
                edge.numeric_value = Some(50.0 + i as f32);
                let req = GraphClient::build_ingest_request(
                    None,
                    &[TransactionNode::new("card", &card)],
                    &[edge],
                );
                tx.send(req).await?;
            }
            drop(tx);
            let created = reader.await.unwrap_or(0);
            Ok::<u32, Box<dyn std::error::Error + Send + Sync>>(created)
        }));
    }
    let mut total_stream = 0u32;
    for h in handles {
        total_stream += h.await?.unwrap_or(0);
    }
    println!("  4-worker streaming ingest: total created={}", total_stream);

    // =========================================================================
    // SECTION 8 — Neighbor Queries
    // =========================================================================
    //
    // Neighbor queries traverse edges from a given node.
    //
    // `out_neighbors = true`  → src→dst (e.g. which merchants did this card visit?)
    // `out_neighbors = false` → dst←src (e.g. which cards visited this merchant?)
    //
    // Symmetric edge types return the union of both directions automatically.

    println!("\n=== 8. Neighbor Queries ===");

    // Create a known card with several merchants for predictable demo output.
    client.create_node("card", Some("card-demo-nav"), &[]).await?;
    for m in 1u32..=5 {
        let merch = format!("merchant-demo-{m:03}");
        client.create_node("merchant", Some(&merch), &[]).await?;
        client.upsert_edge(
            "TRANSACTS_AT",
            NodeRef::external("card",     "card-demo-nav"),
            NodeRef::external("merchant", &merch),
            Some(m as f32 * 30.0),
            Some(secs_ago(m * 3_600)),
            None,
        ).await?;
    }

    // --- Paginated neighbor list ---------------------------------------------
    //
    // `limit` controls page size; 0 means unlimited.
    // `cursor` is the `neighbor_node_id` of the last item on the previous page —
    // pass 0 to start from the beginning.
    // `include_props` when true populates `NeighborEdge::neighbor_props` with
    // all node properties for each neighbor. Use this to enrich results without
    // an extra `get_node` call per neighbor.

    let (neighbors, has_more) = client.get_neighbors(
        NodeRef::external("card", "card-demo-nav"),
        "TRANSACTS_AT",
        true,    // out-neighbors: merchants this card transacted at
        3,       // page size = 3
        0,       // cursor = start
        &[],     // no property filters
        false,   // don't include neighbor properties (faster)
    ).await?;
    println!("  get_neighbors (page 1, size 3): {} results, has_more={}",
        neighbors.len(), has_more);
    for n in &neighbors {
        println!("    neighbor_id={} edge_id={} created={}",
            n.neighbor_node_id, n.edge_id, n.created_at_us);
    }

    // --- Server-side property filtering --------------------------------------
    //
    // `NodePropertyFilter` lets the engine filter neighbors by their node
    // properties without returning all of them to the client first.
    // The filter evaluates on the server; only matching neighbors are sent.
    //
    // Available predicates:
    //   NodePropertyFilter::int_gt(property, val)
    //   NodePropertyFilter::int_lt(property, val)
    //   NodePropertyFilter::int_eq(property, val)
    //   NodePropertyFilter::float_gt(property, val)
    //   NodePropertyFilter::float_lt(property, val)
    //   NodePropertyFilter::string_eq(property, val)
    //   NodePropertyFilter::bool_eq(property, val)
    //   NodePropertyFilter::ts_after(property, unix_secs)
    //   NodePropertyFilter::ts_before(property, unix_secs)

    let filters = vec![
        NodePropertyFilter::int_eq("mcc", 5812),   // only restaurants (MCC 5812)
    ];
    let (filtered, _) = client.get_neighbors(
        NodeRef::external("card", "card-demo-001"),
        "TRANSACTS_AT",
        true,
        50,
        0,
        &filters,
        true,   // include properties so we can print them
    ).await?;
    println!("  filtered neighbors (mcc=5812): {} found", filtered.len());
    for n in &filtered {
        print!("    neighbor_id={}", n.neighbor_node_id);
        if let Some(ext) = &n.neighbor_external_id {
            print!(" ({})", ext);
        }
        for p in &n.neighbor_props {
            print!("  {}={:?}", p.name, p.value);
        }
        println!();
    }

    // --- Neighbor count ------------------------------------------------------
    //
    // Exact count of unique neighbors without fetching the list.
    // Useful for velocity checks: "how many unique merchants has this card hit?"

    let (count, _approx) = client.graph().get_neighbor_count(
        NodeRef::external("card", "card-demo-nav"),
        "TRANSACTS_AT",
    ).await?;
    println!("  neighbor_count(card-demo-nav, TRANSACTS_AT) = {}", count);

    // --- Last neighbor (impossible-travel detection) --------------------------
    //
    // `get_last_neighbor` returns the neighbor with the highest `last_seen`
    // timestamp, optionally excluding a specific node (e.g. the current IP).
    //
    // Use case: "card just transacted at ip-uk-001. What was the previous IP?"
    // If the previous IP is ip-us-001 and the gap is 8 minutes, that is
    // physically impossible → flag as impossible travel.

    // Simulate a US transaction 8 minutes ago and a UK transaction now.
    client.upsert_edge(
        "USES_DEVICE",
        NodeRef::external("card", "card-demo-001"),
        NodeRef::external("device", "device-demo-001"),
        None,
        Some(secs_ago(8 * 60)),     // 8 minutes ago
        None,
    ).await?;
    client.upsert_edge(
        "USES_DEVICE",
        NodeRef::external("card", "card-demo-001"),
        NodeRef::external("device", "device-demo-002"),
        None,
        Some(now_secs()),           // right now
        None,
    ).await?;

    let prev = client.graph().get_last_neighbor(
        NodeRef::external("card", "card-demo-001"),
        "USES_DEVICE",
        Some(NodeRef::external("device", "device-demo-002")), // exclude current device
    ).await?;
    if let Some((prev_node_id, prev_ts)) = prev {
        let gap_mins = now_secs().saturating_sub(prev_ts) / 60;
        println!("  Previous device: node_id={} last_seen={}min ago", prev_node_id, gap_mins);
        if gap_mins < 10 {
            println!("  ⚠ Rapid device switch detected ({} min gap)", gap_mins);
        }
    }

    // =========================================================================
    // SECTION 9 — Node Histogram
    // =========================================================================
    //
    // Node histograms are a pre-aggregated time-series view of activity for a
    // specific node+edge_type combination.
    //
    // They are automatically populated whenever an edge is upserted (both the
    // src-side and dst-side histograms are updated). You can query them to get:
    //   - Total event count over a window
    //   - Per-amount-bin totals (showing the spending distribution)
    //   - Hourly or daily breakdowns
    //
    // Node histograms are only available for edge types that were registered
    // with `bin_boundaries` (e.g. TRANSACTS_AT). They are not available for
    // USES_DEVICE (no bins) or SIMILAR_TO (static/minimal payload).

    println!("\n=== 9. Node Histogram ===");

    let hist = client.features().query_node_histogram(
        NodeRef::external("card", "card-demo-001"),
        "TRANSACTS_AT",
        48,     // look back 48 hours
        0,      // 0 days (hourly window only)
    ).await?;
    println!("  total_events={} window_covered={}s",
        hist.total_events, hist.window_covered_secs);
    let bin_labels = ["<$5","$5-25","$25-50","$50-100","$100-250","$250-500","$500-1k","≥$1k"];
    print!("  bins: ");
    for (label, count) in bin_labels.iter().zip(hist.total_counts.iter()) {
        print!("[{label}:{count}] ");
    }
    println!();

    // =========================================================================
    // SECTION 10 — Feature Vector
    // =========================================================================
    //
    // `get_node_feature_vector` is a single RPC that aggregates multiple signals
    // into one response. It is designed for real-time ML scoring at authorization
    // time — replacing 6+ separate queries with one call.
    //
    // For each edge type in `edge_types` it returns:
    //   - neighbor_count           – unique neighbor count
    //   - total_tx_count           – accumulated tx_count across all neighbors
    //   - total_approx_sum         – accumulated amount across all neighbors
    //   - activity_bitmap_union    – bitwise OR of all neighbor activity bitmaps
    //
    // `nodes_to_check_fraud`: any nodes in the current transaction context
    //   (merchant, device, IP). The engine checks each against the fraud flag
    //   store and returns:
    //   - direct_fraud_score       – fraud score of the node itself
    //   - fraudulent_neighbor_count
    //   - max_neighbor_fraud_score

    println!("\n=== 10. Feature Vector ===");

    let fv = client.features().get_node_feature_vector(
        NodeRef::external("card", "card-demo-001"),
        &["TRANSACTS_AT", "USES_DEVICE"],    // edge types to aggregate
        24,     // histogram look-back: 24 hours
        7,      // histogram look-back: 7 days
        &[
            NodeRef::external("merchant", "merchant-demo-001"),
            NodeRef::external("device",   "device-demo-001"),
        ],
    ).await?;
    println!("  node_id={}", fv.node_id);
    for ef in &fv.edge_features {
        println!("  [{}] neighbors={} tx_count={} approx_sum=${:.2} bitmap=0x{:016x}",
            ef.edge_type_name, ef.neighbor_count, ef.total_tx_count,
            ef.total_approx_sum, ef.activity_bitmap_union);
    }
    println!("  fraud: direct_score={:.2} flagged_neighbors={} max_neighbor_score={:.2}",
        fv.direct_fraud_score, fv.fraudulent_neighbor_count, fv.max_neighbor_fraud_score);

    // =========================================================================
    // SECTION 11 — Fraud Flagging
    // =========================================================================
    //
    // Any node can be flagged as fraudulent with a score (0.0–1.0) and a reason.
    // Flags are stored as edges from the node to a special FRAUD sentinel node.
    //
    // `get_fraud_context` checks a list of nodes in one call and returns all
    // that are flagged. Use this at authorization time to check the card,
    // merchant, device, and IP in a single RPC.

    println!("\n=== 11. Fraud Flagging ===");

    // Flag a known compromised device.
    client.features().flag_node(
        NodeRef::external("device", "device-demo-002"),
        0.95,
        "Device seen on 500+ unrelated cards in 24h — suspected device farm",
    ).await?;
    println!("  Flagged device-demo-002");

    // Batch-check all parties in the current transaction.
    let ctx = client.features().get_fraud_context(&[
        NodeRef::external("card",     "card-demo-001"),
        NodeRef::external("merchant", "merchant-demo-001"),
        NodeRef::external("device",   "device-demo-002"),
    ]).await?;

    if ctx.flagged_nodes.is_empty() {
        println!("  No flagged nodes — transaction parties are clean");
    } else {
        println!("  FRAUD HITS ({}):", ctx.flagged_nodes.len());
        for n in &ctx.flagged_nodes {
            println!("    node_id={} score={:.2} reason=\"{}\"",
                n.node_id, n.fraud_score, n.reason);
        }
    }

    // Remove a flag (e.g. after investigation concludes it was a false positive).
    client.features().unflag_node(
        NodeRef::external("device", "device-demo-002"),
    ).await?;
    println!("  Unflagged device-demo-002");

    // =========================================================================
    // SECTION 12 — Similarity Graph
    // =========================================================================
    //
    // The engine computes per-type Jaccard similarity between nodes based on
    // shared neighbors. This is used for:
    //   - Finding cards with similar merchant exposure (mule ring detection)
    //   - Clustering cards by device/IP usage (account sharing)
    //
    // Two APIs:
    //
    //   `find_similar_nodes`    – real-time query for ONE node, top-k results
    //   `build_similarity_graph` – batch sweep over ALL nodes of a type;
    //                              writes SIMILAR_TO edges for the top-k matches
    //
    // Both APIs accept:
    //   weighted_edge_types     – which edge types to compare, with weights
    //   required_edge_types     – candidate must share ≥1 neighbor on these types
    //   bool_property_weights   – boolean node properties as virtual shared neighbors
    //   required_bool_properties – candidate must match on these bool properties
    //
    // SIMILAR_TO must be registered as a static edge type (see Section 2).

    println!("\n=== 12. Similarity ===");

    // Real-time similarity query: which cards look most like card-demo-001?
    //
    // `bool_property_weights` treats a boolean node property as a virtual shared
    // neighbor. Two cards both having `is_virtual = true` score Jaccard 1.0 on that
    // dimension. This lets you boost similarity when cards share a boolean attribute
    // (e.g. both are virtual, both are flagged international, etc.).
    let bool_weights = vec![
        BoolPropertyWeight::new("is_virtual", 0.2),  // 20% weight for shared "virtual card" flag
    ];
    let similar = client.find_similar_nodes(
        NodeRef::external("card", "card-demo-001"),
        &[
            EdgeTypeWeight::new("TRANSACTS_AT", 0.5),  // merchant overlap (primary)
            EdgeTypeWeight::new("USES_DEVICE",  0.3),  // device overlap (secondary)
        ],
        &[],                            // no required edge types
        &bool_weights,                  // boost score when both cards are virtual
        &[],                            // no required bool properties
        5,                              // return top-5 most similar cards
        0.1,                            // minimum similarity threshold (0.0–1.0)
        false,                          // don't write SIMILAR_TO edges (query only)
        "SIMILAR_TO",
    ).await?;
    println!("  find_similar_nodes for card-demo-001: {} results", similar.similar_nodes.len());
    for s in &similar.similar_nodes {
        println!("    node_id={} similarity={:.3} shared_neighbors={}",
            s.node_id, s.similarity, s.shared_neighbors);
    }

    // Batch build: compute similarity for ALL cards and write SIMILAR_TO edges.
    let build_result = client.build_similarity_graph(
        "card",                         // sweep all nodes of this type
        &[
            EdgeTypeWeight::new("TRANSACTS_AT", 0.6),
            EdgeTypeWeight::new("USES_DEVICE",  0.4),
        ],
        &[],                            // no required types
        &[],                            // no bool property weights
        &[],                            // no required bool properties
        10,                             // top-10 SIMILAR_TO edges per card
        0.05,                           // min similarity
        "SIMILAR_TO",                   // write edges to this type
    ).await?;
    println!("  build_similarity_graph: processed={} created={} updated={} elapsed={}ms",
        build_result.nodes_processed, build_result.edges_created,
        build_result.edges_updated, build_result.elapsed_ms);

    // Read a SIMILAR_TO edge to get the stored similarity score.
    if let Some(sim_state) = client.get_edge_state(
        "SIMILAR_TO",
        NodeRef::external("card", "card-demo-001"),
        NodeRef::external("card", "card-demo-002"),
        None, None,
    ).await? {
        // On static edges approx_sum holds the float value (the Jaccard score).
        println!("  SIMILAR_TO card-demo-001→card-demo-002: score={:.3}", sim_state.approx_sum);
    }

    // =========================================================================
    // SECTION 13 — Segment Evaluation
    // =========================================================================
    //
    // The segment evaluator uses a two-step pattern:
    //
    //   1. `prefetch_eval_context` — fires ONE GetNodeFeatureVector RPC and
    //      caches the result in `EvalContextData`. ~70% of segment signals can
    //      be answered from this cache at zero additional RPC cost.
    //
    //   2. Lazy signal helpers — called only when a loaded rule actually needs
    //      a signal not covered by the feature vector:
    //        - `days_since_last_neighbor`   → last_neighbor RPC
    //        - `neighbor_count`             → get_neighbor_count RPC
    //        - `histogram_field`            → query_node_histogram RPC
    //        - `node_property_f64`          → get_node RPC
    //        - `edge_state_field`           → get_edge_state RPC
    //
    // Segment membership is stored as MEMBER_OF static edges from a customer
    // node to a segment node. The segment node is identified by a well-known
    // external ID (the segment name).

    println!("\n=== 13. Segment Evaluation ===");

    let mut seg = client.segment();

    // Step 1 — prefetch feature vector once for this customer.
    let ctx = seg.prefetch_eval_context(
        NodeRef::external("card", "card-demo-001"),
        &["TRANSACTS_AT", "USES_DEVICE"],
        24,     // histogram hours
        7,      // histogram days
    ).await?;
    println!("  Eval context for node_id={}", ctx.node_id);
    println!("  tx_count(TRANSACTS_AT)={}", ctx.total_tx_count("TRANSACTS_AT"));
    println!("  neighbor_count(TRANSACTS_AT)={}", ctx.neighbor_count("TRANSACTS_AT"));

    // Step 2 — lazy signals (only called if rules need them).
    let days_since = seg.days_since_last_neighbor(
        NodeRef::external("card", "card-demo-001"),
        "TRANSACTS_AT",
    ).await?;
    println!("  days_since_last_transact={:.2}", days_since);

    let unique_merchants = seg.neighbor_count(
        NodeRef::external("card", "card-demo-001"),
        "TRANSACTS_AT",
    ).await?;
    println!("  unique_merchants={}", unique_merchants);

    let spend_30d = seg.histogram_field(
        NodeRef::external("card", "card-demo-001"),
        "TRANSACTS_AT",
        0,      // hours
        30,     // days
        fraud_graph_client::HistogramField::TotalEvents,
    ).await?;
    println!("  events_30d={}", spend_30d);

    // Segment membership — upsert a MEMBER_OF edge (card → segment node).
    // First ensure the segment node exists.
    client.create_node("card", Some("seg-high-velocity"), &[]).await?; // segment node
    seg.upsert_segment_membership(
        NodeRef::external("card", "card-demo-001"),     // customer
        NodeRef::external("card", "seg-high-velocity"), // segment node
        0.87,                                           // confidence
    ).await?;
    println!("  Upserted MEMBER_OF edge with confidence=0.87");

    // Query which segments a customer belongs to.
    let memberships = seg.get_customer_segments(
        NodeRef::external("card", "card-demo-001"),
    ).await?;
    println!("  Customer segments: {} memberships", memberships.len());
    for m in &memberships {
        println!("    segment='{}' confidence={:.2} last_seen={}",
            m.segment_name, m.confidence, m.last_seen_secs);
    }

    // =========================================================================
    // SECTION 14 — Utility: Clear Edge Type Data
    // =========================================================================
    //
    // Remove all stored (src, dst) pairs for an edge type.
    // The type definition (schema) is preserved; only the data is removed.
    // Useful when recomputing similarity graphs daily: clear old scores first,
    // then rebuild fresh.

    println!("\n=== 14. Clear Edge Type Data ===");

    let removed = client.clear_edge_type_data("SIMILAR_TO").await?;
    println!("  Cleared SIMILAR_TO: {} pairs removed", removed);

    // =========================================================================
    // Done
    // =========================================================================

    println!("\n=== Complete Guide finished ===");
    println!("All engine features demonstrated successfully.");
    Ok(())
}
