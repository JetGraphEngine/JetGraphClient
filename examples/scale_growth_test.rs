//! # JetGraph — Scale Growth Test
//!
//! Progressively loads up to 30 million nodes across three node types and up to
//! 10 million edges, pausing at regular intervals to run a suite of five query
//! types and record how latency and throughput change as the graph grows.
//!
//! ## What it measures
//!
//! At every checkpoint (configurable, default every 1 M records written) the
//! test freezes ingest, runs N iterations of each query type, and prints a
//! latency table.  The full history is printed as a summary table at the end so
//! you can see exactly where — and how fast — performance degrades.
//!
//! ## Schema (created automatically with --bootstrap-schema)
//!
//! ```text
//! Node types
//!   user     (numeric IDs)
//!   product  (numeric IDs)
//!   device   (numeric IDs)
//!
//! Edge types
//!   VIEWS        user → product   (compact, amount bins, 1 h activity bitmap)
//!   PURCHASES    user → product   (compact, amount bins, 1 h activity bitmap)
//!   USES_DEVICE  user → device    (compact, no bins,     5 min bitmap)
//! ```
//!
//! ## Usage
//!
//! ```bash
//! # Defaults: 1 M users · 500 K products · 200 K devices · 1 M edges
//! cargo run --release --example scale_growth_test -- --bootstrap-schema
//!
//! # Full 30 M nodes / 10 M edges run
//! cargo run --release --example scale_growth_test -- \
//!   --bootstrap-schema \
//!   --user-count    15000000 \
//!   --product-count 10000000 \
//!   --device-count   5000000 \
//!   --edge-count    10000000 \
//!   --checkpoint-every 1000000 \
//!   --concurrency 64 \
//!   --probe-iters 200
//! ```
//!
//! ## Output example
//!
//! ```text
//! ═══════════════════════════════════════════════════════════════════════════
//!  CHECKPOINT #3 │ nodes=3,000,000 │ edges=2,100,000 │ elapsed=47.3 s
//!  Write phase   │ 63,450 ops/s  (last interval: 47,340 ops/s)
//! ───────────────────────────────────────────────────────────────────────────
//!  Query type         iters  avg_ms   p50_ms   p95_ms   p99_ms   max_ms  err
//!  edge_state           200   0.124    0.113    0.245    0.467    1.234    0
//!  edge_state+windows   200   0.147    0.135    0.278    0.512    1.478    0
//!  neighbors_out        200   0.231    0.208    0.456    0.823    2.345    0
//!  neighbors_in         200   0.253    0.229    0.489    0.867    2.567    0
//!  two_hop              200   0.561    0.502    1.234    2.345    5.678    0
//! ═══════════════════════════════════════════════════════════════════════════
//! ```

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};


use jetgraph_client::{Client, NodeRef, TransactionNode, TransactionEdge, TransactionNodeRef};

// ─────────────────────────────────────────────────────────────────────────────
// CLI config
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Config {
    endpoint:         String,
    user_count:       usize,
    product_count:    usize,
    device_count:     usize,
    edge_count:       usize,
    checkpoint_every: usize,
    concurrency:      usize,
    probe_iters:      usize,
    bootstrap_schema: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint:         "http://localhost:50051".into(),
            user_count:       1_000_000,
            product_count:      500_000,
            device_count:       200_000,
            edge_count:       1_000_000,
            checkpoint_every: 1_000_000,
            concurrency:             32,
            probe_iters:            100,
            bootstrap_schema:     false,
        }
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut cfg = Self::default();
        let args: Vec<String> = env::args().skip(1).collect();
        let mut i = 0usize;
        while i < args.len() {
            match args[i].as_str() {
                "--help" | "-h" => { print_help(); std::process::exit(0); }
                "--endpoint"        => { i += 1; cfg.endpoint = val(&args, i, "--endpoint")?; }
                "--user-count"      => { i += 1; cfg.user_count = pu(&args, i, "--user-count")?; }
                "--product-count"   => { i += 1; cfg.product_count = pu(&args, i, "--product-count")?; }
                "--device-count"    => { i += 1; cfg.device_count = pu(&args, i, "--device-count")?; }
                "--edge-count"      => { i += 1; cfg.edge_count = pu(&args, i, "--edge-count")?; }
                "--checkpoint-every"=> { i += 1; cfg.checkpoint_every = pu(&args, i, "--checkpoint-every")?; }
                "--concurrency"     => { i += 1; cfg.concurrency = pu(&args, i, "--concurrency")?; }
                "--probe-iters"     => { i += 1; cfg.probe_iters = pu(&args, i, "--probe-iters")?; }
                "--bootstrap-schema"=> { cfg.bootstrap_schema = true; }
                f => return Err(format!("unknown flag '{f}' — run --help")),
            }
            i += 1;
        }
        Ok(cfg)
    }

    fn total_nodes(&self) -> usize {
        self.user_count + self.product_count + self.device_count
    }
}

fn val(args: &[String], i: usize, f: &str) -> Result<String, String> {
    args.get(i).cloned().ok_or_else(|| format!("missing value for {f}"))
}
fn pu(args: &[String], i: usize, f: &str) -> Result<usize, String> {
    val(args, i, f)?.parse::<usize>().map_err(|_| format!("invalid usize for {f}"))
}

fn print_help() {
    println!("Usage: cargo run --release --example scale_growth_test -- [options]

Options:
  --endpoint <url>           gRPC endpoint          (default: http://localhost:50051)
  --user-count <n>           USER nodes to create   (default: 1_000_000)
  --product-count <n>        PRODUCT nodes          (default: 500_000)
  --device-count <n>         DEVICE nodes           (default: 200_000)
  --edge-count <n>           Target total edges     (default: 1_000_000)
  --checkpoint-every <n>     Record interval        (default: 1_000_000)
  --concurrency <n>          Parallel streams       (default: 32)
  --probe-iters <n>          Query iterations/type  (default: 100)
  --bootstrap-schema         Register schema before running
");
}

// ─────────────────────────────────────────────────────────────────────────────
// RNG (no external crate)
// ─────────────────────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed ^ 0x9e3779b97f4a7c15) }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn usize_below(&mut self, n: usize) -> usize { (self.next() as usize) % n }
    fn bool_pct(&mut self, pct: u64) -> bool { self.next() % 100 < pct }
}

// ─────────────────────────────────────────────────────────────────────────────
// Timing helpers
// ─────────────────────────────────────────────────────────────────────────────

fn now_secs() -> u32 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as u32
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let idx = ((p / 100.0 * (sorted.len() - 1) as f64).round() as usize)
        .min(sorted.len() - 1);
    sorted[idx] as f64
}

fn avg_micros(v: &[u64]) -> f64 {
    if v.is_empty() { return 0.0; }
    v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64
}

fn fmt_n(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(ch);
    }
    out.chars().rev().collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Checkpoint record
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct QueryProbeResult {
    label:           &'static str,
    iters:           usize,
    latencies_us:    Vec<u64>,
    errors:          usize,
}

impl QueryProbeResult {
    fn avg_ms(&self)    -> f64 { avg_micros(&self.latencies_us) / 1000.0 }
    fn p50_ms(&self)    -> f64 { let mut s = self.latencies_us.clone(); s.sort_unstable(); percentile(&s, 50.0) / 1000.0 }
    fn p95_ms(&self)    -> f64 { let mut s = self.latencies_us.clone(); s.sort_unstable(); percentile(&s, 95.0) / 1000.0 }
    fn p99_ms(&self)    -> f64 { let mut s = self.latencies_us.clone(); s.sort_unstable(); percentile(&s, 99.0) / 1000.0 }
    fn max_ms(&self)    -> f64 { self.latencies_us.iter().copied().max().unwrap_or(0) as f64 / 1000.0 }
    fn is_degraded(&self) -> bool { self.p99_ms() > 10.0 || (self.errors as f64 / self.iters.max(1) as f64) > 0.01 }
}

#[derive(Debug)]
struct Checkpoint {
    index:            usize,
    total_nodes:      usize,
    total_edges:      usize,
    elapsed:          Duration,
    interval_ops:     usize,
    interval_secs:    f64,
    overall_ops_s:    f64,
    queries:          Vec<QueryProbeResult>,
}

impl Checkpoint {
    fn interval_ops_s(&self) -> f64 {
        if self.interval_secs < 0.001 { 0.0 } else { self.interval_ops as f64 / self.interval_secs }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema
// ─────────────────────────────────────────────────────────────────────────────

async fn bootstrap_schema(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let mut s = client.schema();
    let state = s.get_schema().await?;

    let node_names: Vec<String> = state.node_types.iter().map(|n| n.name.clone()).collect();
    let edge_names: Vec<String> = state.edge_types.iter().map(|e| e.name.clone()).collect();

    let needs_user    = !node_names.iter().any(|n| n == "user");
    let needs_product = !node_names.iter().any(|n| n == "product");
    let needs_device  = !node_names.iter().any(|n| n == "device");
    let needs_views   = !edge_names.iter().any(|e| e == "VIEWS");
    let needs_purch   = !edge_names.iter().any(|e| e == "PURCHASES");
    let needs_device_e= !edge_names.iter().any(|e| e == "USES_DEVICE");

    if !needs_user && !needs_product && !needs_device && !needs_views && !needs_purch && !needs_device_e {
        println!("  Schema already finalized — skipping registration.");
        return Ok(());
    }

    println!("  Registering schema...");

    // Node types — numeric IDs save ~28 B/node vs string at this scale
    if needs_user    { s.register_node_type("user",    true).await?; println!("    + node type: user"); }
    if needs_product { s.register_node_type("product", true).await?; println!("    + node type: product"); }
    if needs_device  { s.register_node_type("device",  true).await?; println!("    + node type: device"); }

    // VIEWS: user → product, 90 d TTL, amount bins, 1 h bitmap ticks
    if needs_views {
        s.register_compact_edge_type(
            "VIEWS", "user", "product",
            90 * 86_400,
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0],
            "amount",
            3_600,
            None,
            false,
        ).await?;
        println!("    + edge type: VIEWS (user → product)");
    }

    // PURCHASES: user → product, 180 d TTL, same amount bins, 1 h bitmap
    if needs_purch {
        s.register_compact_edge_type(
            "PURCHASES", "user", "product",
            180 * 86_400,
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0],
            "amount",
            3_600,
            None,
            false,
        ).await?;
        println!("    + edge type: PURCHASES (user → product)");
    }

    // USES_DEVICE: user → device, 30 d TTL, no bins, 5 min bitmap ticks
    if needs_device_e {
        s.register_compact_edge_type(
            "USES_DEVICE", "user", "device",
            30 * 86_400,
            vec![],
            "",
            300,
            None,
            false,
        ).await?;
        println!("    + edge type: USES_DEVICE (user → device)");
    }

    let ver = s.finalize().await?;
    println!("  Schema finalized (version {ver}).");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk node pre-load (products + devices)
// ─────────────────────────────────────────────────────────────────────────────

/// Load `count` nodes of `node_type` with numeric external IDs 0..count.
/// Uses high-throughput streaming ingest with `concurrency` parallel streams.
/// Returns the total number of records created and the elapsed time.
async fn preload_nodes(
    client: &Client,
    node_type: &'static str,
    count: usize,
    concurrency: usize,
) -> Result<(usize, Duration), Box<dyn std::error::Error>> {
    if count == 0 {
        return Ok((0, Duration::ZERO));
    }
    let counter  = Arc::new(AtomicUsize::new(0));
    let created  = Arc::new(AtomicUsize::new(0));
    let start    = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let c       = client.clone();
        let counter = counter.clone();
        let created = created.clone();

        handles.push(tokio::spawn(async move {
            let (tx, rx) = c.ingest_stream().await?;

            // Drain responses in the background so back-pressure never stalls sends.
            let created2 = created.clone();
            let reader = tokio::spawn(async move {
                let mut responses = rx;
                let mut n = 0usize;
                while let Some(Ok(resp)) = responses.next().await {
                    n += resp.nodes_created as usize;
                }
                created2.fetch_add(n, Ordering::Relaxed);
            });

            loop {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                if idx >= count { break; }
                let ext = idx.to_string();
                if tx.send(None, &[TransactionNode::new(node_type, &ext)], &[]).await.is_err() { break; }
            }

            drop(tx);
            reader.await.ok();
            Ok::<(), jetgraph_client::ClientError>(())
        }));
    }

    for h in handles { h.await??; }
    Ok((created.load(Ordering::Relaxed), start.elapsed()))
}

// ─────────────────────────────────────────────────────────────────────────────
// User + edge ingestion (main growth loop)
// ─────────────────────────────────────────────────────────────────────────────

/// Shared state updated by all workers during a chunk of ingest.
struct ChunkStats {
    nodes_written: AtomicUsize,
    edges_written:  AtomicUsize,
    errors:         AtomicUsize,
}

/// Load `chunk_users` USER nodes starting at `user_start`, each with edges:
///  - VIEWS → random product  (always)
///  - PURCHASES → random product (50 % probability)
///  - USES_DEVICE → random device  (30 % probability)
///
/// Processes ALL users in [user_start, user_start+chunk_users). The outer loop's
/// `edges_remaining > 0` condition controls when to stop starting new chunks; we
/// never cut a chunk short, so user_cursor always accurately reflects the set of
/// users for which ingest requests were sent and user IDs are safe to probe.
async fn ingest_user_chunk(
    client:       &Client,
    user_start:   usize,
    chunk_users:  usize,
    product_count: usize,
    device_count:  usize,
    concurrency:  usize,
    worker_seed:  u64,
) -> Result<ChunkStats, Box<dyn std::error::Error>> {
    let stats   = Arc::new(ChunkStats {
        nodes_written: AtomicUsize::new(0),
        edges_written:  AtomicUsize::new(0),
        errors:         AtomicUsize::new(0),
    });
    let counter  = Arc::new(AtomicUsize::new(user_start));
    let user_end = user_start + chunk_users;

    let mut handles = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let c            = client.clone();
        let counter      = counter.clone();
        let stats        = stats.clone();
        let product_count = product_count;
        let device_count  = device_count;

        handles.push(tokio::spawn(async move {
            let mut rng = Rng::new(worker_seed.wrapping_add(worker as u64));
            let (tx, rx) = c.ingest_stream().await?;

            let stats2 = stats.clone();
            let reader = tokio::spawn(async move {
                let mut responses = rx;
                while let Some(Ok(resp)) = responses.next().await {
                    stats2.nodes_written.fetch_add(resp.nodes_created as usize, Ordering::Relaxed);
                    let edges = (resp.edges_created + resp.edges_updated) as usize;
                    stats2.edges_written.fetch_add(edges, Ordering::Relaxed);
                }
            });

            loop {
                let uid = counter.fetch_add(1, Ordering::Relaxed);
                if uid >= user_end { break; }

                let user_ext    = uid.to_string();
                let product_idx = rng.usize_below(product_count.max(1));
                let product_ext = product_idx.to_string();

                let ts_offset = (rng.next() % 2_592_000) as u32; // within 30 days
                let ts        = now_secs().saturating_sub(ts_offset);
                let amount    = 1.0 + (rng.next() % 10_000) as f32 / 100.0;

                let user_ref    = TransactionNodeRef::request_node_key("U");
                let product_ref = TransactionNodeRef::request_node_key("P");

                let mut edges: Vec<TransactionEdge> = Vec::with_capacity(3);

                // VIEWS always
                let mut e = TransactionEdge::new("VIEWS", user_ref.clone(), product_ref.clone());
                e.numeric_value  = Some(amount);
                e.event_ts_secs  = Some(ts);
                edges.push(e);

                // PURCHASES 50 %
                if rng.bool_pct(50) {
                    let mut e = TransactionEdge::new("PURCHASES", user_ref.clone(), product_ref.clone());
                    e.numeric_value = Some(amount * 1.2);
                    e.event_ts_secs = Some(ts);
                    edges.push(e);
                }

                // USES_DEVICE 30 %
                let device_ref = if rng.bool_pct(30) && device_count > 0 {
                    let device_idx = rng.usize_below(device_count);
                    let device_ext = device_idx.to_string();
                    let dr = TransactionNodeRef::request_node_key("D");
                    let mut e = TransactionEdge::new("USES_DEVICE", user_ref.clone(), dr.clone());
                    e.event_ts_secs = Some(ts);
                    edges.push(e);
                    Some((device_ext, dr))
                } else {
                    None
                };

                // Build node list
                let mut nodes = vec![
                    TransactionNode::new("user",    &user_ext).with_key("U"),
                    TransactionNode::new("product", &product_ext).with_key("P"),
                ];
                if let Some((device_ext, _)) = device_ref {
                    nodes.push(TransactionNode::new("device", &device_ext).with_key("D"));
                }

                if tx.send(None, &nodes, &edges).await.is_err() {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }

            drop(tx);
            reader.await.ok();
            Ok::<(), jetgraph_client::ClientError>(())
        }));
    }

    for h in handles {
        if let Err(e) = h.await? {
            eprintln!("  worker error: {e}");
        }
    }

    Ok(Arc::try_unwrap(stats).unwrap_or_else(|a| {
        ChunkStats {
            nodes_written: AtomicUsize::new(a.nodes_written.load(Ordering::Relaxed)),
            edges_written: AtomicUsize::new(a.edges_written.load(Ordering::Relaxed)),
            errors:        AtomicUsize::new(a.errors.load(Ordering::Relaxed)),
        }
    }))
}

// ─────────────────────────────────────────────────────────────────────────────
// Query probe suite
// ─────────────────────────────────────────────────────────────────────────────

/// A single probe: call the lambda `iters` times, record per-call latency in µs.
/// The future must return `Ok(())` on success or `Err(String)` with the error
/// description on failure. Errors are counted and the first few distinct messages
/// are printed so failures are diagnosable without a debugger.
async fn probe<F, Fut>(label: &'static str, iters: usize, mut f: F) -> QueryProbeResult
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut latencies     = Vec::with_capacity(iters);
    let mut errors        = 0usize;
    let mut seen_messages: Vec<String> = Vec::new();

    for i in 0..iters {
        let t0 = Instant::now();
        let result = f(i).await;
        let us = t0.elapsed().as_micros() as u64;
        latencies.push(us);
        if let Err(msg) = result {
            errors += 1;
            // Collect up to 3 distinct error messages for the summary.
            if seen_messages.len() < 3 && !seen_messages.contains(&msg) {
                seen_messages.push(msg);
            }
        }
    }

    if !seen_messages.is_empty() {
        println!("    [{}] {} error(s) — sample messages:", label, errors);
        for msg in &seen_messages {
            println!("      • {}", msg);
        }
    }

    QueryProbeResult { label, iters, latencies_us: latencies, errors }
}

async fn run_query_probes(
    client:        &Client,
    iters:         usize,
    users_so_far:  usize,
    products_so_far: usize,
) -> Vec<QueryProbeResult> {
    let mut rng = Rng::new(now_secs() as u64);
    let u_max = users_so_far.max(1);
    let p_max = products_so_far.max(1);

    // ── 1. edge_state: raw presence + count for a VIEWS edge ─────────────────
    let c1 = client.clone();
    let r1 = probe("edge_state", iters, |i| {
        let c  = c1.clone();
        let uid = (i * 97 + rng.usize_below(u_max)) % u_max;
        let pid = (i * 53 + rng.usize_below(p_max)) % p_max;
        async move {
            c.get_edge_state(
                "VIEWS",
                NodeRef::external("user",    &uid.to_string()),
                NodeRef::external("product", &pid.to_string()),
                None, None,
            ).await.map(|_| ()).map_err(|e| format!("user={uid} product={pid}: {e}"))
        }
    }).await;

    // ── 2. edge_state + activity windows (last 1 h, 24 h) ────────────────────
    let c2 = client.clone();
    let r2 = probe("edge_state+windows", iters, |i| {
        let c  = c2.clone();
        let uid = (i * 113 + 3) % u_max;
        let pid = (i * 71  + 7) % p_max;
        async move {
            c.get_edge_state(
                "VIEWS",
                NodeRef::external("user",    &uid.to_string()),
                NodeRef::external("product", &pid.to_string()),
                None, Some(&[3_600, 86_400]),
            ).await.map(|_| ()).map_err(|e| format!("user={uid} product={pid}: {e}"))
        }
    }).await;

    // ── 3. neighbors_out (outbound VIEWS from a user, limit 50) ──────────────
    let c3 = client.clone();
    let r3 = probe("neighbors_out_50", iters, |i| {
        let c   = c3.clone();
        let uid = (i * 131 + 11) % u_max;
        async move {
            c.get_neighbors(
                NodeRef::external("user", &uid.to_string()),
                "VIEWS",
                true,  // outbound
                50, 0,
                &[], false,
            ).await.map(|_| ()).map_err(|e| format!("user={uid}: {e}"))
        }
    }).await;

    // ── 4. neighbors_in (inbound VIEWS to a product, limit 50) ───────────────
    let c4 = client.clone();
    let r4 = probe("neighbors_in_50", iters, |i| {
        let c   = c4.clone();
        let pid = (i * 79 + 13) % p_max;
        async move {
            c.get_neighbors(
                NodeRef::external("product", &pid.to_string()),
                "VIEWS",
                false, // inbound
                50, 0,
                &[], false,
            ).await.map(|_| ()).map_err(|e| format!("product={pid}: {e}"))
        }
    }).await;

    // ── 5. two_hop: user → products → OTHER users who viewed those products ───
    //     (simulates "users similar to me" via shared product views)
    let c5 = client.clone();
    let r5 = probe("two_hop", iters, |i| {
        let c   = c5.clone();
        let uid = (i * 157 + 17) % u_max;
        async move {
            // Hop 1: outbound VIEWS of user
            let hop1 = match c.get_neighbors(
                NodeRef::external("user", &uid.to_string()),
                "VIEWS", true, 10, 0, &[], false,
            ).await {
                Ok((n, _)) => n,
                Err(e)     => return Err(format!("hop1 user={uid}: {e}")),
            };

            if hop1.is_empty() { return Ok(()); } // no edges yet — not an error

            // Hop 2: inbound VIEWS to the first product neighbor
            let first_product_id = hop1[0].neighbor_node_id;
            c.get_neighbors(
                NodeRef::node_id(first_product_id),
                "VIEWS", false, 10, 0, &[], false,
            ).await.map(|_| ()).map_err(|e| format!("hop2 product_node={first_product_id}: {e}"))
        }
    }).await;

    vec![r1, r2, r3, r4, r5]
}

// ─────────────────────────────────────────────────────────────────────────────
// Output formatting
// ─────────────────────────────────────────────────────────────────────────────

fn print_checkpoint(cp: &Checkpoint) {
    println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("  CHECKPOINT #{:<3} │ nodes={:<12} │ edges={:<12} │ elapsed={:.1}s",
        cp.index, fmt_n(cp.total_nodes), fmt_n(cp.total_edges), cp.elapsed.as_secs_f64());
    println!(
        "  Write phase   │ overall {:.0} ops/s  │  last interval {:.0} ops/s  ({} ops in {:.2}s)",
        cp.overall_ops_s, cp.interval_ops_s(), fmt_n(cp.interval_ops), cp.interval_secs
    );
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("  {:<22} {:>5}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>5}",
        "Query type", "iters", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "err");
    println!("  {}", "─".repeat(78));
    for q in &cp.queries {
        let warn = if q.is_degraded() { " ⚠" } else { "" };
        println!(
            "  {:<22} {:>5}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>7.3}  {:>5}{}",
            q.label, q.iters,
            q.avg_ms(), q.p50_ms(), q.p95_ms(), q.p99_ms(), q.max_ms(),
            q.errors, warn,
        );
    }
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}

fn print_summary(checkpoints: &[Checkpoint]) {
    if checkpoints.is_empty() { return; }
    println!("\n\n╔══════════════════════════════════════ FINAL SUMMARY ══════════════════════════════════════╗");
    println!(
        "  {:<4}  {:<14} {:<12} {:<10}  {:>10}  {}",
        "#", "nodes", "edges", "ingest/s",
        "edge_state p99",
        "two_hop p99"
    );
    println!("  {}", "─".repeat(90));

    for cp in checkpoints {
        let edge_state_p99 = cp.queries.iter()
            .find(|q| q.label == "edge_state")
            .map(|q| q.p99_ms()).unwrap_or(0.0);
        let two_hop_p99 = cp.queries.iter()
            .find(|q| q.label == "two_hop")
            .map(|q| q.p99_ms()).unwrap_or(0.0);

        let edge_warn  = if edge_state_p99 > 10.0 { " ⚠" } else { "" };
        let hop_warn   = if two_hop_p99 > 50.0     { " ⚠" } else { "" };

        println!(
            "  #{:<3}  {:<14} {:<12} {:>10.0}  {:>12.3}{}  {:>10.3}{}",
            cp.index,
            fmt_n(cp.total_nodes),
            fmt_n(cp.total_edges),
            cp.interval_ops_s(),
            edge_state_p99, edge_warn,
            two_hop_p99, hop_warn,
        );
    }
    println!("╚══════════════════════════════════════════════════════════════════════════════════════════╝");

    // Report every checkpoint where any probe exceeded thresholds.
    // The old code stopped at the first one, hiding later regressions.
    let degraded: Vec<&Checkpoint> = checkpoints
        .iter()
        .filter(|cp| cp.queries.iter().any(|q| q.is_degraded()))
        .collect();

    if degraded.is_empty() {
        println!("\n✅  No breaking point detected across all checkpoints.");
        println!("   All p99 latencies stayed below 10 ms and error rates below 1 %.");
    } else {
        println!("\n⚠  Degraded checkpoints ({} of {}):", degraded.len(), checkpoints.len());
        for bp in degraded {
            println!(
                "   checkpoint #{} — nodes={} edges={}",
                bp.index, fmt_n(bp.total_nodes), fmt_n(bp.total_edges)
            );
            for q in &bp.queries {
                if q.is_degraded() {
                    println!(
                        "     → {} : p99={:.3} ms, err_rate={:.1}% ({} errors)",
                        q.label, q.p99_ms(),
                        q.errors as f64 / q.iters.max(1) as f64 * 100.0,
                        q.errors
                    );
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::parse().map_err(|e| format!("argument error: {e}"))?;

    println!("╔════════════════════════════════════════════════════════╗");
    println!("║       JetGraph Scale Growth Test                      ║");
    println!("╚════════════════════════════════════════════════════════╝");
    println!("  endpoint        : {}", cfg.endpoint);
    println!("  user_count      : {}", fmt_n(cfg.user_count));
    println!("  product_count   : {}", fmt_n(cfg.product_count));
    println!("  device_count    : {}", fmt_n(cfg.device_count));
    println!("  total_nodes     : {}", fmt_n(cfg.total_nodes()));
    println!("  edge_count      : {}", fmt_n(cfg.edge_count));
    println!("  checkpoint_every: {}", fmt_n(cfg.checkpoint_every));
    println!("  concurrency     : {}", cfg.concurrency);
    println!("  probe_iters     : {}", cfg.probe_iters);
    println!();

    // ── Connect ───────────────────────────────────────────────────────────────
    println!("Connecting to engine...");
    let client = Client::connect(&cfg.endpoint).await
        .map_err(|e| format!("failed to connect to {}: {e}", cfg.endpoint))?;

    let ready = client.health().check().await?;
    if !ready {
        return Err("Engine is not READY — start the engine and try again.".into());
    }
    println!("  Engine status: READY\n");

    // ── Schema ────────────────────────────────────────────────────────────────
    if cfg.bootstrap_schema {
        println!("Schema bootstrap:");
        bootstrap_schema(&client).await?;
        println!();
    } else {
        println!("Skipping schema bootstrap (use --bootstrap-schema to auto-register).\n");
    }

    // ── Phase 1: Pre-load PRODUCT nodes ───────────────────────────────────────
    println!("Phase 1 — Loading {} PRODUCT nodes...", fmt_n(cfg.product_count));
    let (prod_created, prod_elapsed) = preload_nodes(&client, "product", cfg.product_count, cfg.concurrency).await?;
    println!(
        "  Done: {} created in {:.2}s ({:.0} nodes/s)\n",
        fmt_n(prod_created), prod_elapsed.as_secs_f64(),
        prod_created as f64 / prod_elapsed.as_secs_f64().max(0.001)
    );

    // ── Phase 2: Pre-load DEVICE nodes ────────────────────────────────────────
    println!("Phase 2 — Loading {} DEVICE nodes...", fmt_n(cfg.device_count));
    let (dev_created, dev_elapsed) = preload_nodes(&client, "device", cfg.device_count, cfg.concurrency).await?;
    println!(
        "  Done: {} created in {:.2}s ({:.0} nodes/s)\n",
        fmt_n(dev_created), dev_elapsed.as_secs_f64(),
        dev_created as f64 / dev_elapsed.as_secs_f64().max(0.001)
    );

    // ── Phase 3: USER nodes + edges, with periodic checkpoints ───────────────
    println!("Phase 3 — Loading {} USER nodes with edges...", fmt_n(cfg.user_count));
    println!("  Will checkpoint every {} total records.\n", fmt_n(cfg.checkpoint_every));

    let mut checkpoints:    Vec<Checkpoint> = Vec::new();
    let mut user_cursor:    usize = 0;
    let mut total_nodes:    usize = prod_created + dev_created;
    let mut total_edges:    usize = 0;
    let mut edges_remaining:usize = cfg.edge_count;
    let     test_start            = Instant::now();
    let mut interval_start        = Instant::now();
    let mut interval_records:usize= 0;
    let mut checkpoint_idx:  usize= 0;
    let mut cumulative_records:usize = 0;
    let mut next_checkpoint:  usize = cfg.checkpoint_every;

    // How many users per chunk to load before checking the checkpoint threshold?
    // Aim for ~100K per chunk so we check frequently but don't call too many
    // short-running ingest bursts.
    let chunk_size = (cfg.checkpoint_every / 4).max(50_000).min(500_000);

    while user_cursor < cfg.user_count && edges_remaining > 0 {
        let users_this_chunk = chunk_size.min(cfg.user_count - user_cursor);

        // No per-chunk edge budget — process every user in this chunk fully.
        // The outer `edges_remaining > 0` condition stops new chunks once the
        // total edge target is met, so user_cursor always accurately reflects
        // which users have been ingested and are safe to probe.
        let stats = ingest_user_chunk(
            &client,
            user_cursor,
            users_this_chunk,
            cfg.product_count,
            cfg.device_count,
            cfg.concurrency,
            test_start.elapsed().as_nanos() as u64,
        ).await?;

        let nodes_w = stats.nodes_written.load(Ordering::Relaxed);
        let edges_w  = stats.edges_written.load(Ordering::Relaxed);

        total_nodes    += nodes_w;
        total_edges    += edges_w;
        edges_remaining = edges_remaining.saturating_sub(edges_w);
        user_cursor    += users_this_chunk;
        interval_records += nodes_w + edges_w;
        cumulative_records += nodes_w + edges_w;

        // Print progress every chunk
        let elapsed = test_start.elapsed();
        println!(
            "  [+{:.0}s]  users={:<10}  nodes={:<12}  edges={:<12}  err={}",
            elapsed.as_secs_f64(),
            fmt_n(user_cursor),
            fmt_n(total_nodes),
            fmt_n(total_edges),
            stats.errors.load(Ordering::Relaxed),
        );

        // ── checkpoint? ───────────────────────────────────────────────────────
        if cumulative_records >= next_checkpoint || user_cursor >= cfg.user_count || edges_remaining == 0 {
            checkpoint_idx += 1;
            let interval_secs = interval_start.elapsed().as_secs_f64();
            let overall_ops_s = cumulative_records as f64 / elapsed.as_secs_f64().max(0.001);

            println!("\n  Running query probe suite (checkpoint #{checkpoint_idx})...");
            // All products are guaranteed loaded after Phase 1 completes, regardless
            // of whether they were newly created (prod_created) or pre-existed (0 created).
            // total_nodes only counts *new* nodes in this run, so using it to cap
            // products_known would incorrectly exclude pre-existing products.
            let products_known = cfg.product_count;
            let users_known    = user_cursor;
            let queries = run_query_probes(
                &client,
                cfg.probe_iters,
                users_known,
                products_known,
            ).await;

            let cp = Checkpoint {
                index:          checkpoint_idx,
                total_nodes,
                total_edges,
                elapsed,
                interval_ops:   interval_records,
                interval_secs,
                overall_ops_s,
                queries,
            };
            print_checkpoint(&cp);
            checkpoints.push(cp);

            // Reset interval tracking
            interval_start   = Instant::now();
            interval_records = 0;
            next_checkpoint  = cumulative_records + cfg.checkpoint_every;
        }
    }

    // ── Final summary ─────────────────────────────────────────────────────────
    let total_elapsed = test_start.elapsed();
    println!("\n\nLoad complete:");
    println!("  total_nodes    : {}", fmt_n(total_nodes));
    println!("  total_edges    : {}", fmt_n(total_edges));
    println!("  total_elapsed  : {:.2}s", total_elapsed.as_secs_f64());
    println!("  overall_rate   : {:.0} records/s",
        (total_nodes + total_edges) as f64 / total_elapsed.as_secs_f64().max(0.001));

    print_summary(&checkpoints);

    Ok(())
}
