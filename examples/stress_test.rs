//! JetGraph Stress Test — surface system limits, failure points, and bottlenecks.
//!
//! Connects to a running JetGraph engine and executes eight progressively more
//! aggressive test phases designed to identify exactly where the system breaks
//! and quantify the gains available from production client-side patterns.
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │  Phase 1 — Write Throughput Sweep                                        │
//! │    upsert_edge at concurrency 1 → max_concurrency                        │
//! │    Optional: --compare-connections shows shared vs per-worker delta       │
//! │  Phase 2 — Read Throughput Sweep                                         │
//! │    get_edge_state at same concurrency ladder                              │
//! │  Phase 3 — Streaming Ingest Stress                                       │
//! │    IngestStream bidirectional RPC, pipeline depth 4 → 256                │
//! │    Production reconnection loop: auto-reconnects on stream drop           │
//! │  Phase 4 — Mixed Read/Write Contention                                   │
//! │    Three ratio mixes (80/20, 50/50, 20/80) at fixed concurrency          │
//! │  Phase 5 — Hot-Node Fan-In                                               │
//! │    All writes target a single sink node                                   │
//! │  Phase 6 — Multi-Hop Traversal                                           │
//! │    Runs hop-2 BOTH sequentially and in parallel (tokio::spawn)           │
//! │    → directly shows the speedup from the parallel production pattern      │
//! │  Phase 7 — Failure Boundary                                              │
//! │    Timeout injection at shrinking deadlines                               │
//! │  Phase 8 — Streaming vs. Individual RPC (write path comparison)          │
//! │    Same concurrency, same duration, direct throughput comparison          │
//! │    → quantifies the streaming advantage at identical client concurrency   │
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! Usage:
//!   cargo run --example stress_test --release -- [OPTIONS]
//!
//! Options:
//!   --endpoint <url>          gRPC endpoint (default: http://localhost:50051)
//!   --bootstrap-schema        Register node/edge types if missing
//!   --phase <1-8>             Run only a specific phase (default: all)
//!   --step-secs <n>           Seconds per concurrency step (default: 5)
//!   --max-concurrency <n>     Maximum concurrency to test (default: 256)
//!   --pair-count <n>          src/dst pairs for workload data (default: 10000)
//!   --per-worker-connections  Open one TCP connection per worker
//!   --compare-connections     Phase 1: run shared vs per-worker comparison step

use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fraud_graph_client::{
    Client, GraphClient, NodeRef,
    TransactionEdge, TransactionNode, TransactionNodeRef,
};

// ─── CLI Config ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Config {
    endpoint: String,
    bootstrap_schema: bool,
    only_phase: Option<u8>,
    step_secs: u64,
    max_concurrency: usize,
    pair_count: usize,
    per_worker_connections: bool,
    /// Phase 1: after the main sweep, run one extra step at max_concurrency with
    /// per-worker connections to directly quantify the h2 connection bottleneck.
    compare_connections: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:50051".to_string(),
            bootstrap_schema: false,
            only_phase: None,
            step_secs: 5,
            max_concurrency: 256,
            pair_count: 10_000,
            per_worker_connections: false,
            compare_connections: false,
        }
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut cfg = Self::default();
        let args: Vec<String> = env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--help" | "-h" => {
                    eprintln!("See module doc at the top of stress_test.rs for options.");
                    std::process::exit(0);
                }
                "--endpoint" => {
                    i += 1;
                    cfg.endpoint = args.get(i).cloned().ok_or("missing value for --endpoint")?;
                }
                "--bootstrap-schema"      => cfg.bootstrap_schema      = true,
                "--per-worker-connections"=> cfg.per_worker_connections = true,
                "--compare-connections"   => cfg.compare_connections    = true,
                "--phase" => {
                    i += 1;
                    let v = args.get(i).ok_or("missing value for --phase")?;
                    cfg.only_phase = Some(v.parse::<u8>().map_err(|_| "invalid --phase")?);
                }
                "--step-secs" => {
                    i += 1;
                    let v = args.get(i).ok_or("missing value for --step-secs")?;
                    cfg.step_secs = v.parse::<u64>().map_err(|_| "invalid --step-secs")?;
                }
                "--max-concurrency" => {
                    i += 1;
                    let v = args.get(i).ok_or("missing value for --max-concurrency")?;
                    cfg.max_concurrency = v.parse::<usize>().map_err(|_| "invalid --max-concurrency")?;
                }
                "--pair-count" => {
                    i += 1;
                    let v = args.get(i).ok_or("missing value for --pair-count")?;
                    cfg.pair_count = v.parse::<usize>().map_err(|_| "invalid --pair-count")?;
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
            i += 1;
        }
        Ok(cfg)
    }

    fn concurrency_ladder(&self) -> Vec<usize> {
        let mut steps = vec![1, 2, 4, 8, 16, 32, 64, 128, 256];
        steps.retain(|&c| c <= self.max_concurrency);
        if steps.last().copied() != Some(self.max_concurrency) {
            steps.push(self.max_concurrency);
        }
        steps
    }
}

// ─── Latency Histogram ────────────────────────────────────────────────────────

#[derive(Default)]
struct Histogram {
    samples_us: Vec<u64>,
    errors: u64,
    timeouts: u64,
}

impl Histogram {
    fn record_ok(&mut self, us: u64) {
        self.samples_us.push(us);
    }
    fn record_err(&mut self) {
        self.errors += 1;
    }
    fn record_timeout(&mut self) {
        self.timeouts += 1;
        self.errors += 1;
    }
    fn ok_count(&self) -> u64 {
        self.samples_us.len() as u64
    }
    fn total(&self) -> u64 {
        self.ok_count() + self.errors
    }
    fn error_rate_pct(&self) -> f64 {
        if self.total() == 0 { return 0.0; }
        self.errors as f64 / self.total() as f64 * 100.0
    }
    fn timeout_rate_pct(&self) -> f64 {
        if self.total() == 0 { return 0.0; }
        self.timeouts as f64 / self.total() as f64 * 100.0
    }
    fn percentile_ms(&mut self, pct: f64) -> f64 {
        if self.samples_us.is_empty() { return 0.0; }
        self.samples_us.sort_unstable();
        let idx = ((pct / 100.0) * (self.samples_us.len() - 1) as f64).round() as usize;
        self.samples_us[idx.min(self.samples_us.len() - 1)] as f64 / 1_000.0
    }
    fn avg_ms(&self) -> f64 {
        if self.samples_us.is_empty() { return 0.0; }
        self.samples_us.iter().sum::<u64>() as f64 / self.samples_us.len() as f64 / 1_000.0
    }
    fn max_ms(&self) -> f64 {
        self.samples_us.iter().max().copied().unwrap_or(0) as f64 / 1_000.0
    }
    fn throughput(&self, elapsed: Duration) -> f64 {
        self.ok_count() as f64 / elapsed.as_secs_f64().max(1e-9)
    }
    fn merge(&mut self, other: Histogram) {
        self.samples_us.extend(other.samples_us);
        self.errors  += other.errors;
        self.timeouts += other.timeouts;
    }
}

// ─── Fast RNG ─────────────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self { Self(seed ^ 0x9e37_79b9_7f4a_7c15) }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn now_secs() -> u32 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as u32
}

async fn make_clients(endpoint: &str, count: usize, per_worker: bool) -> Result<Vec<Client>, Box<dyn Error>> {
    if per_worker {
        let mut v = Vec::with_capacity(count);
        for _ in 0..count { v.push(Client::connect(endpoint).await?); }
        Ok(v)
    } else {
        let c = Client::connect(endpoint).await?;
        Ok(vec![c; count])
    }
}

fn print_separator(title: &str) {
    let line = "─".repeat(72);
    println!("\n{line}");
    println!("  {title}");
    println!("{line}");
}

fn print_sweep_header() {
    println!("{:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}  {:>8}",
        "concurrency", "ops/sec", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "err%");
}

fn print_sweep_row(concurrency: usize, elapsed: Duration, mut hist: Histogram) {
    let tput = hist.throughput(elapsed);
    let avg  = hist.avg_ms();
    let p50  = hist.percentile_ms(50.0);
    let p95  = hist.percentile_ms(95.0);
    let p99  = hist.percentile_ms(99.0);
    let max  = hist.max_ms();
    let err  = hist.error_rate_pct();
    println!("{concurrency:>12}  {tput:>10.0}  {avg:>10.3}  {p50:>10.3}  {p95:>10.3}  {p99:>10.3}  {max:>8.3}  {err:>7.2}%");
}

fn percentile_us(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ─── Schema Bootstrap ─────────────────────────────────────────────────────────

async fn ensure_schema(client: &Client, bootstrap: bool) -> Result<(), Box<dyn Error>> {
    let mut schema = client.schema();
    let state = schema.get_schema().await?;

    let has_card     = state.node_types.iter().any(|n| n.name == "card");
    let has_merchant = state.node_types.iter().any(|n| n.name == "merchant");
    let has_edge     = state.edge_types.iter().any(|e| e.name == "TRANSACTS_AT");

    if has_card && has_merchant && has_edge {
        println!("Schema OK (card, merchant, TRANSACTS_AT already registered).");
        return Ok(());
    }

    if !bootstrap {
        return Err(format!(
            "Schema preflight failed: card={has_card}, merchant={has_merchant}, TRANSACTS_AT={has_edge}.\n\
             Rerun with --bootstrap-schema to register missing types."
        ).into());
    }

    println!("Bootstrapping schema...");
    let mut s = client.schema();
    if !has_card     { s.register_node_type("card",     false).await?; }
    if !has_merchant { s.register_node_type("merchant", false).await?; }
    if !has_edge {
        s.register_compact_edge_type(
            "TRANSACTS_AT", "card", "merchant",
            90 * 86_400,
            vec![10.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_000.0],
            "amount", 3_600, None, false,
        ).await?;
    }
    s.finalize().await?;
    println!("Schema bootstrap complete.");
    Ok(())
}

// ─── Workload Data ────────────────────────────────────────────────────────────

async fn build_workload_pairs(
    client: &Client,
    pair_count: usize,
) -> Result<Arc<Vec<(NodeRef, NodeRef)>>, Box<dyn Error>> {
    println!("Provisioning {pair_count} card/merchant node pairs...");

    let sem     = Arc::new(tokio::sync::Semaphore::new(64));
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let results: Arc<std::sync::Mutex<Vec<Option<(u64, u64)>>>> =
        Arc::new(std::sync::Mutex::new(vec![None; pair_count]));

    let mut handles = Vec::new();
    for _ in 0..64 {
        let c       = client.clone();
        let sem     = sem.clone();
        let counter = counter.clone();
        let results = results.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                if idx >= pair_count { break; }
                let _permit = sem.acquire().await.unwrap();
                let src = c.create_node("card",     Some(&format!("stress-card-{idx:08}")),  &[]).await?;
                let dst = c.create_node("merchant", Some(&format!("stress-merch-{idx:08}")), &[]).await?;
                results.lock().unwrap()[idx] = Some((src.node_id, dst.node_id));
            }
            Ok::<(), fraud_graph_client::ClientError>(())
        }));
    }
    for h in handles { h.await??; }

    let guard = results.lock().unwrap();
    let pairs: Vec<(NodeRef, NodeRef)> = guard
        .iter()
        .map(|opt| {
            let (src_id, dst_id) = opt.expect("missing pair");
            (NodeRef::node_id(src_id), NodeRef::node_id(dst_id))
        })
        .collect();

    println!("  {} pairs ready (using fast node_id refs).", pairs.len());
    println!("  Seeding edges for read phases...");
    let seed_count = pair_count.min(5_000);
    for i in 0..seed_count {
        let (src, dst) = &pairs[i];
        let ts = now_secs().saturating_sub((i as u32) % 86_400);
        client.upsert_edge("TRANSACTS_AT", src.clone(), dst.clone(),
            Some((i % 500) as f32 + 1.0), Some(ts), None).await?;
    }
    println!("  {seed_count} seed edges written.");
    Ok(Arc::new(pairs))
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 1: Write Throughput Sweep
//  CLIENT CHANGE: --compare-connections runs a shared vs. per-worker comparison
//  at the end to directly quantify the h2 connection bottleneck.
// ─────────────────────────────────────────────────────────────────────────────

async fn phase1_write_sweep(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 1 — Write Throughput Sweep (upsert_edge)");
    println!("Gradually increase concurrent writers. Find saturation point.\n");
    print_sweep_header();

    let mut prev_tput = 0.0f64;
    let mut saturation_concurrency: Option<usize> = None;

    // Track the final sweep step's metrics for the connection comparison.
    let mut last_tput = 0.0f64;
    let mut last_p99  = 0.0f64;
    let mut last_err  = 0.0f64;

    for &concurrency in &cfg.concurrency_ladder() {
        let clients  = make_clients(&cfg.endpoint, concurrency, cfg.per_worker_connections).await?;
        let duration = Duration::from_secs(cfg.step_secs);
        let deadline = Instant::now() + duration;
        let lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let mut handles = Vec::new();

        for (wid, client) in clients.into_iter().enumerate() {
            let pairs     = pairs.clone();
            let lats      = lats.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng   = Rng::new(wid as u64 + 0xDEAD);
                let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si  = rng.next() as usize % pairs.len();
                    let di  = rng.next() as usize % pairs.len();
                    let val = 1.0 + (rng.next() % 999) as f32;
                    let ts  = now_secs().saturating_sub((rng.next() as u32) % 86_400);
                    let t0  = Instant::now();
                    match client.upsert_edge(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), Some(val), Some(ts), None).await {
                        Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                        Err(_) => local.record_err(),
                    }
                }
                lats.lock().unwrap().merge(local);
            }));
        }
        for h in handles { let _ = h.await; }

        let elapsed = duration;
        let mut guard = lats.lock().unwrap();
        let tput = guard.throughput(elapsed);

        // Save metrics before hist is consumed by print_sweep_row.
        last_tput = tput;
        last_p99  = guard.percentile_ms(99.0);
        last_err  = guard.error_rate_pct();

        if concurrency > 1 && prev_tput > 0.0 && (tput - prev_tput) / prev_tput < 0.05 {
            if saturation_concurrency.is_none() {
                saturation_concurrency = Some(concurrency);
            }
        }

        let hist = std::mem::take(&mut *guard);
        drop(guard);
        print_sweep_row(concurrency, elapsed, hist);
        prev_tput = tput;
    }

    if let Some(sc) = saturation_concurrency {
        println!("\n  ► Write saturation detected at concurrency={sc} (throughput gain <5% per doubling).");
    } else {
        println!("\n  ► No clear write saturation within tested range.");
    }

    // ── CLIENT CHANGE: Connection architecture comparison ─────────────────────
    // Answers: "Is the h2 connection the bottleneck or is it the engine itself?"
    // Run one more step at max_concurrency with per-worker connections and compare.
    if cfg.compare_connections && !cfg.per_worker_connections {
        let concurrency = cfg.max_concurrency;
        println!("\n  Connection architecture comparison (concurrency={concurrency}):");
        println!("  One extra step with per-worker connections to isolate the h2 bottleneck.\n");

        let clients_pw = make_clients(&cfg.endpoint, concurrency, true).await?;
        let duration   = Duration::from_secs(cfg.step_secs);
        let deadline   = Instant::now() + duration;
        let lats_pw: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let mut handles = Vec::new();

        for (wid, client) in clients_pw.into_iter().enumerate() {
            let pairs     = pairs.clone();
            let lats      = lats_pw.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng   = Rng::new(wid as u64 + 0xC0DE);
                let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si  = rng.next() as usize % pairs.len();
                    let di  = rng.next() as usize % pairs.len();
                    let val = 1.0 + (rng.next() % 999) as f32;
                    let ts  = now_secs().saturating_sub((rng.next() as u32) % 86_400);
                    let t0  = Instant::now();
                    match client.upsert_edge(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), Some(val), Some(ts), None).await {
                        Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                        Err(_) => local.record_err(),
                    }
                }
                lats.lock().unwrap().merge(local);
            }));
        }
        for h in handles { let _ = h.await; }

        let mut pw_guard = lats_pw.lock().unwrap();
        let pw_tput = pw_guard.throughput(duration);
        let pw_p99  = pw_guard.percentile_ms(99.0);
        let pw_err  = pw_guard.error_rate_pct();
        drop(pw_guard);

        println!("  {:>28}  {:>12}  {:>10}  {:>8}", "connection_mode", "ops/sec", "p99_ms", "err%");
        println!("  {:>28}  {:>12.0}  {:>10.3}  {:>7.2}%", "shared  (1 TCP conn)", last_tput, last_p99, last_err);
        println!("  {:>28}  {:>12.0}  {:>10.3}  {:>7.2}%", format!("per-worker ({concurrency} conns)"), pw_tput, pw_p99, pw_err);

        let delta_pct = if last_tput > 0.0 { (pw_tput - last_tput) / last_tput * 100.0 } else { 0.0 };
        let verdict = if delta_pct > 5.0 {
            "h2 connection IS the bottleneck → use per-worker connections in production"
        } else {
            "h2 connection is NOT the bottleneck → engine compute or semaphore is the limit"
        };
        println!("\n  Throughput delta: {:+.1}%", delta_pct);
        println!("  Verdict: {verdict}");
    } else if !cfg.compare_connections {
        println!("  (Add --compare-connections to run shared vs. per-worker connection comparison)");
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 2: Read Throughput Sweep
// ─────────────────────────────────────────────────────────────────────────────

async fn phase2_read_sweep(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 2 — Read Throughput Sweep (get_edge_state)");
    println!("Gradually increase concurrent readers. Find saturation point.\n");
    print_sweep_header();

    let mut prev_tput = 0.0f64;
    let mut saturation_concurrency: Option<usize> = None;

    for &concurrency in &cfg.concurrency_ladder() {
        let clients  = make_clients(&cfg.endpoint, concurrency, cfg.per_worker_connections).await?;
        let duration = Duration::from_secs(cfg.step_secs);
        let deadline = Instant::now() + duration;
        let lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let mut handles = Vec::new();

        for (wid, client) in clients.into_iter().enumerate() {
            let pairs     = pairs.clone();
            let lats      = lats.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng   = Rng::new(wid as u64 + 0xBEEF);
                let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si = rng.next() as usize % pairs.len();
                    let di = rng.next() as usize % pairs.len();
                    let t0 = Instant::now();
                    match client.get_edge_state(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), None, None).await {
                        Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                        Err(_) => local.record_err(),
                    }
                }
                lats.lock().unwrap().merge(local);
            }));
        }
        for h in handles { let _ = h.await; }

        let elapsed = duration;
        let mut guard = lats.lock().unwrap();
        let tput = guard.throughput(elapsed);

        if concurrency > 1 && prev_tput > 0.0 && (tput - prev_tput) / prev_tput < 0.05 {
            if saturation_concurrency.is_none() {
                saturation_concurrency = Some(concurrency);
            }
        }

        let hist = std::mem::take(&mut *guard);
        drop(guard);
        print_sweep_row(concurrency, elapsed, hist);
        prev_tput = tput;
    }

    if let Some(sc) = saturation_concurrency {
        println!("\n  ► Read saturation detected at concurrency={sc}.");
    } else {
        println!("\n  ► No clear read saturation within tested range.");
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 3: Streaming Ingest Stress
//  CLIENT CHANGE: Each worker uses a production-quality reconnection loop.
//  If the stream drops (server restart, network blip, stream reset), the worker
//  reconnects with exponential backoff and continues from where it left off.
//  The sequence counter persists across reconnections — no duplicate tx_ids.
// ─────────────────────────────────────────────────────────────────────────────

async fn phase3_streaming_stress(cfg: &Config) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 3 — Streaming Ingest Stress (IngestStream RPC)");
    println!("Workers use production reconnection loop. Pipeline depth 4 → 256.\n");

    const WORKERS:    usize = 8;
    const DURATION_S: u64   = 10;

    println!("{:>16}  {:>12}  {:>12}  {:>12}  {:>12}  {:>10}  {:>12}",
        "pipeline_depth", "edges/sec", "tx/sec", "p50_us", "p99_us", "fallback%", "reconnects");

    for &pipeline in &[4usize, 8, 16, 32, 64, 128, 256] {
        let total_edges     = Arc::new(AtomicU64::new(0));
        let total_txns      = Arc::new(AtomicU64::new(0));
        let total_fallback  = Arc::new(AtomicU64::new(0));
        let total_reconnects = Arc::new(AtomicU64::new(0));
        let lat_data: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

        let deadline = Instant::now() + Duration::from_secs(DURATION_S);
        let mut handles = Vec::new();

        for wid in 0..WORKERS {
            let ep          = cfg.endpoint.clone();
            let te          = total_edges.clone();
            let tt          = total_txns.clone();
            let tf          = total_fallback.clone();
            let tr          = total_reconnects.clone();
            let ld          = lat_data.clone();

            handles.push(tokio::spawn(async move {
                // CLIENT CHANGE: sequence persists across reconnections so
                // tx_ids remain unique after a stream drop and reconnect.
                let mut total_seq = 0u64;
                let mut backoff   = Duration::from_millis(100);

                'reconnect: loop {
                    if Instant::now() >= deadline { break; }

                    // Connect with exponential backoff on failure.
                    let client = match Client::connect(&ep).await {
                        Ok(c)  => { backoff = Duration::from_millis(100); c }
                        Err(e) => {
                            eprintln!("  [w{wid}] connect: {e}, retry in {backoff:?}");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                    };

                    // Open bidirectional stream with backoff on failure.
                    let (tx, mut resp) = match client.ingest_stream().await {
                        Ok(p)  => p,
                        Err(e) => {
                            eprintln!("  [w{wid}] ingest_stream: {e}, retry");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                    };

                    // Fresh pipeline semaphore for this stream session.
                    let sem  = Arc::new(tokio::sync::Semaphore::new(pipeline));
                    let sem2 = sem.clone();
                    let te2  = te.clone();
                    let tf2  = tf.clone();
                    let tt2  = tt.clone();
                    let ld2  = ld.clone();

                    // Drain responses in a separate task, returning permits.
                    let drain = tokio::spawn(async move {
                        let mut t_prev = Instant::now();
                        while let Some(r) = resp.message().await.ok().flatten() {
                            sem2.add_permits(1);
                            te2.fetch_add((r.edges_created + r.edges_updated) as u64, Ordering::Relaxed);
                            tt2.fetch_add(1, Ordering::Relaxed);
                            if r.edge_errors > 0 { tf2.fetch_add(1, Ordering::Relaxed); }
                            let us = t_prev.elapsed().as_micros() as u64;
                            t_prev = Instant::now();
                            if us < 1_000_000 { ld2.lock().unwrap().push(us); }
                        }
                    });

                    let card_base     = (wid as u64) * 100_000;
                    let merchant_base = (wid as u64) * 200_000;

                    // Send until deadline or until the stream drops.
                    // Returns true if stream broke (needs reconnect), false if deadline was reached cleanly.
                    let stream_broken = loop {
                        if Instant::now() >= deadline { break false; }

                        let permit = match sem.acquire().await {
                            Ok(p)  => p,
                            Err(_) => break false,
                        };

                        let seq         = total_seq;
                        let card_id     = card_base + (seq % 10_000);
                        let merchant_id = merchant_base + (seq % 50_000);
                        let tx_id       = format!("s{wid}-{seq}");
                        let val         = 1.0 + (seq % 500) as f32;
                        let ts          = now_secs().saturating_sub((seq as u32) % 86_400);

                        let mut req = GraphClient::build_ingest_request(
                            Some(&tx_id),
                            &[
                                TransactionNode::new("card",     card_id.to_string()).with_key("c"),
                                TransactionNode::new("merchant", merchant_id.to_string()).with_key("m"),
                            ],
                            &[
                                TransactionEdge::new(
                                    "TRANSACTS_AT",
                                    TransactionNodeRef::request_node_key("c"),
                                    TransactionNodeRef::request_node_key("m"),
                                ).with_key("e0"),
                            ],
                        );
                        if let Some(edge) = req.edges.first_mut() {
                            edge.numeric_value = Some(val);
                            edge.event_ts_secs = Some(ts);
                        }

                        if tx.send(req).await.is_err() {
                            // Server closed the stream — reconnect.
                            break true;
                        }
                        // Permit is released by the drain task when the response arrives.
                        std::mem::forget(permit);
                        total_seq += 1;
                    };

                    drop(tx);
                    let _ = drain.await;

                    if stream_broken {
                        tr.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        // Continue outer reconnect loop.
                    } else {
                        // Deadline reached — clean exit.
                        break 'reconnect;
                    }
                }
            }));
        }

        for h in handles { let _ = h.await; }

        let edges      = total_edges.load(Ordering::Relaxed);
        let txns       = total_txns.load(Ordering::Relaxed);
        let fallback   = total_fallback.load(Ordering::Relaxed);
        let reconnects = total_reconnects.load(Ordering::Relaxed);
        let edge_tput  = edges as f64 / DURATION_S as f64;
        let tx_tput    = txns  as f64 / DURATION_S as f64;
        let fb_pct     = if txns > 0 { fallback as f64 / txns as f64 * 100.0 } else { 0.0 };

        let mut samples = lat_data.lock().unwrap();
        samples.sort_unstable();
        let p50 = percentile_us(&samples, 50.0);
        let p99 = percentile_us(&samples, 99.0);

        println!("{pipeline:>16}  {edge_tput:>12.0}  {tx_tput:>12.0}  {p50:>12}  {p99:>12}  {fb_pct:>9.2}%  {reconnects:>12}");
    }

    println!("\n  ► Fallback ratio > 0% = server fell back to sequential processing (batch overflow).");
    println!("    Reconnects > 0 during a stable test = server-side stream resets under pressure.");
    println!("    Production workers auto-reconnect with exponential backoff (see 'reconnect' loop above).");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 4: Mixed Read/Write Contention
// ─────────────────────────────────────────────────────────────────────────────

async fn phase4_mixed_contention(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 4 — Mixed Read/Write Contention");
    println!("Fixed total concurrency={}, three R/W ratios. Exposes RCU contention.\n",
        cfg.max_concurrency.min(64));

    let total_concurrency = cfg.max_concurrency.min(64);
    let duration = Duration::from_secs(cfg.step_secs * 2);

    println!("{:>10}  {:>12}  {:>12}  {:>12}  {:>12}  {:>12}  {:>12}",
        "rw_ratio", "write_ops/s", "w_p99_ms", "read_ops/s", "r_p99_ms", "w_err%", "r_err%");

    for (write_frac, read_frac, label) in [(0.8, 0.2, "80w/20r"), (0.5, 0.5, "50w/50r"), (0.2, 0.8, "20w/80r")] {
        let write_workers = ((total_concurrency as f64 * write_frac) as usize).max(1);
        let read_workers  = ((total_concurrency as f64 * read_frac)  as usize).max(1);

        let deadline = Instant::now() + duration;
        let w_lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let r_lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let mut handles = Vec::new();

        let write_clients = make_clients(&cfg.endpoint, write_workers, cfg.per_worker_connections).await?;
        for (wid, client) in write_clients.into_iter().enumerate() {
            let pairs = pairs.clone(); let w_lats = w_lats.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng = Rng::new(wid as u64 + 0x1234); let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si  = rng.next() as usize % pairs.len();
                    let di  = rng.next() as usize % pairs.len();
                    let val = 1.0 + (rng.next() % 499) as f32;
                    let t0  = Instant::now();
                    match client.upsert_edge(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), Some(val), None, None).await {
                        Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                        Err(_) => local.record_err(),
                    }
                }
                w_lats.lock().unwrap().merge(local);
            }));
        }

        let read_clients = make_clients(&cfg.endpoint, read_workers, cfg.per_worker_connections).await?;
        for (rid, client) in read_clients.into_iter().enumerate() {
            let pairs = pairs.clone(); let r_lats = r_lats.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng = Rng::new(rid as u64 + 0xABCD); let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si = rng.next() as usize % pairs.len();
                    let di = rng.next() as usize % pairs.len();
                    let t0 = Instant::now();
                    match client.get_edge_state(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), None, None).await {
                        Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                        Err(_) => local.record_err(),
                    }
                }
                r_lats.lock().unwrap().merge(local);
            }));
        }

        for h in handles { let _ = h.await; }

        let mut wg = w_lats.lock().unwrap();
        let mut rg = r_lats.lock().unwrap();
        let w_tput = wg.throughput(duration); let w_p99 = wg.percentile_ms(99.0); let w_err = wg.error_rate_pct();
        let r_tput = rg.throughput(duration); let r_p99 = rg.percentile_ms(99.0); let r_err = rg.error_rate_pct();
        println!("{label:>10}  {w_tput:>12.0}  {w_p99:>12.3}  {r_tput:>12.0}  {r_p99:>12.3}  {w_err:>11.2}%  {r_err:>11.2}%");
    }

    println!("\n  ► Read latency increasing under heavy write load = RCU reader contention.");
    println!("    Write latency spike under heavy reads = inverse-lock waits.");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 5: Hot-Node Fan-In Contention
// ─────────────────────────────────────────────────────────────────────────────

async fn phase5_hot_node(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 5 — Hot-Node Fan-In Contention");
    println!("All workers write to a single destination node. Surfaces per-node lock pressure.\n");

    let hot_dst = pairs[0].1.clone();
    println!("{:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}",
        "concurrency", "ops/sec", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "err%");

    for &concurrency in &cfg.concurrency_ladder() {
        let clients  = make_clients(&cfg.endpoint, concurrency, cfg.per_worker_connections).await?;
        let duration = Duration::from_secs(cfg.step_secs);
        let deadline = Instant::now() + duration;
        let lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let mut handles = Vec::new();

        for (wid, client) in clients.into_iter().enumerate() {
            let pairs     = pairs.clone();
            let hot_dst   = hot_dst.clone();
            let lats      = lats.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng   = Rng::new(wid as u64 + 0xF00D);
                let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si  = rng.next() as usize % pairs.len();
                    let val = 1.0 + (rng.next() % 999) as f32;
                    let t0  = Instant::now();
                    match client.upsert_edge(&edge_type, pairs[si].0.clone(), hot_dst.clone(), Some(val), None, None).await {
                        Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                        Err(_) => local.record_err(),
                    }
                }
                lats.lock().unwrap().merge(local);
            }));
        }
        for h in handles { let _ = h.await; }

        let hist = std::mem::take(&mut *lats.lock().unwrap());
        print_sweep_row(concurrency, duration, hist);
    }

    println!("\n  ► Throughput plateau + p99 spike = lock stripe saturation.");
    println!("    Compare to Phase 1 (uniform) to quantify hot-node penalty.");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 6: Multi-Hop Traversal
//  CLIENT CHANGE: runs hop-2 BOTH sequentially and in parallel using
//  tokio::spawn per merchant, then shows the speedup directly in the output.
//  This proves the 3.1× latency amplification is purely sequential overhead —
//  eliminated for free with concurrent spawns in production fraud-ring code.
// ─────────────────────────────────────────────────────────────────────────────

async fn phase6_multi_hop(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 6 — Multi-Hop Traversal (fraud-ring detection pattern)");
    println!("Hop-2 runs sequentially AND in parallel in each iteration.");
    println!("Directly measures the speedup from the parallel production pattern.\n");

    const WORKERS: usize = 16;
    const LIMIT:   u32   = 50;

    let duration = Duration::from_secs(cfg.step_secs * 2);
    let deadline = Instant::now() + duration;

    let hop1_lats:    Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
    let hop2_seq_lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
    let hop2_par_lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
    let hop1_neighbors = Arc::new(AtomicU64::new(0));
    let hop2_neighbors = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    let clients = make_clients(&cfg.endpoint, WORKERS, cfg.per_worker_connections).await?;

    for (wid, client) in clients.into_iter().enumerate() {
        let pairs      = pairs.clone();
        let h1l        = hop1_lats.clone();
        let h2sl       = hop2_seq_lats.clone();
        let h2pl       = hop2_par_lats.clone();
        let h1n        = hop1_neighbors.clone();
        let h2n        = hop2_neighbors.clone();

        handles.push(tokio::spawn(async move {
            let mut rng   = Rng::new(wid as u64 + 0xCAFE);
            let mut l1    = Histogram::default();
            let mut l2_seq = Histogram::default();
            let mut l2_par = Histogram::default();

            while Instant::now() < deadline {
                let si   = rng.next() as usize % pairs.len();
                let card = pairs[si].0.clone();

                // ── Hop 1: card → out-neighbors (merchants) ──────────────────
                let t0 = Instant::now();
                let merchants = match client.get_neighbors(card.clone(), "TRANSACTS_AT", true, LIMIT, 0, &[], false).await {
                    Ok((edges, _)) => edges,
                    Err(_) => { l1.record_err(); continue; }
                };
                l1.record_ok(t0.elapsed().as_micros() as u64);
                h1n.fetch_add(merchants.len() as u64, Ordering::Relaxed);

                if merchants.is_empty() { continue; }

                let sample_count = merchants.len().min(3);

                // ── Hop 2a: Sequential (baseline — the costly pattern) ────────
                let t_seq = Instant::now();
                for merch in &merchants[..sample_count] {
                    match client.get_neighbors(
                        NodeRef::node_id(merch.neighbor_node_id),
                        "TRANSACTS_AT", false, LIMIT, 0, &[], false,
                    ).await {
                        Ok(_)  => {}
                        Err(_) => { l2_seq.record_err(); }
                    }
                }
                l2_seq.record_ok(t_seq.elapsed().as_micros() as u64);

                // ── Hop 2b: Parallel (production pattern — tokio::spawn) ──────
                // CLIENT CHANGE: spawn one task per merchant so all queries fire
                // concurrently. Wall-clock time = max(individual latencies)
                // instead of sum — eliminating the N× amplification for free.
                let tasks: Vec<_> = merchants[..sample_count].iter().map(|m| {
                    let c  = client.clone();
                    let id = m.neighbor_node_id;
                    tokio::spawn(async move {
                        c.get_neighbors(
                            NodeRef::node_id(id),
                            "TRANSACTS_AT", false, LIMIT, 0, &[], false,
                        ).await
                    })
                }).collect();

                let t_par = Instant::now();
                let mut hop2_par_count = 0usize;
                for task in tasks {
                    match task.await {
                        Ok(Ok((edges, _))) => hop2_par_count += edges.len(),
                        _                  => { l2_par.record_err(); }
                    }
                }
                l2_par.record_ok(t_par.elapsed().as_micros() as u64);
                h2n.fetch_add(hop2_par_count as u64, Ordering::Relaxed);
            }

            h1l.lock().unwrap().merge(l1);
            h2sl.lock().unwrap().merge(l2_seq);
            h2pl.lock().unwrap().merge(l2_par);
        }));
    }
    for h in handles { let _ = h.await; }

    let mut g1   = hop1_lats.lock().unwrap();
    let mut g2s  = hop2_seq_lats.lock().unwrap();
    let mut g2p  = hop2_par_lats.lock().unwrap();

    let hop1_avg = g1.avg_ms();
    let hop2s_avg = g2s.avg_ms();
    let hop2p_avg = g2p.avg_ms();
    let speedup   = if hop2p_avg > 0.0 { hop2s_avg / hop2p_avg } else { 0.0 };

    println!("  Hop 1 (card → merchants):");
    println!("    ops/sec:  {:.0}", g1.throughput(duration));
    println!("    avg_ms:   {:.3}  p50: {:.3}  p95: {:.3}  p99: {:.3}  max: {:.3}",
        hop1_avg, g1.percentile_ms(50.0), g1.percentile_ms(95.0), g1.percentile_ms(99.0), g1.max_ms());
    println!("    err%:     {:.2}", g1.error_rate_pct());
    println!("    avg neighbors/query: {:.1}",
        if g1.ok_count() > 0 { hop1_neighbors.load(Ordering::Relaxed) as f64 / g1.ok_count() as f64 } else { 0.0 });

    println!("\n  Hop 2 — Sequential (up to 3 merchants, one-by-one):          ← naive pattern");
    println!("    avg_ms:   {:.3}  ({:.1}× hop-1)  p50: {:.3}  p99: {:.3}  max: {:.3}",
        hop2s_avg, if hop1_avg > 0.0 { hop2s_avg / hop1_avg } else { 0.0 },
        g2s.percentile_ms(50.0), g2s.percentile_ms(99.0), g2s.max_ms());
    println!("    err%:     {:.2}", g2s.error_rate_pct());

    println!("\n  Hop 2 — Parallel (tokio::spawn per merchant, concurrent):    ← production pattern");
    println!("    avg_ms:   {:.3}  ({:.1}× hop-1)  p50: {:.3}  p99: {:.3}  max: {:.3}",
        hop2p_avg, if hop1_avg > 0.0 { hop2p_avg / hop1_avg } else { 0.0 },
        g2p.percentile_ms(50.0), g2p.percentile_ms(99.0), g2p.max_ms());
    println!("    err%:     {:.2}", g2p.error_rate_pct());
    println!("    total 2nd-hop neighbors found: {}", hop2_neighbors.load(Ordering::Relaxed));

    println!("\n  ┌─────────────────────────────────────────────────────────────┐");
    println!("  │  Parallel speedup: {speedup:.2}×  ({:.1}% faster per query)       │",
        (speedup - 1.0) * 100.0);
    println!("  │  In production fraud-ring queries, spawn one task per        │");
    println!("  │  neighbor and join_all — latency becomes max, not sum.       │");
    println!("  └─────────────────────────────────────────────────────────────┘");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 7: Failure Boundary (Timeout Injection)
// ─────────────────────────────────────────────────────────────────────────────

async fn phase7_failure_boundary(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 7 — Failure Boundary (timeout injection at max concurrency)");
    println!("Wrap every request with a shrinking timeout. Find the threshold where error% > 1%.\n");

    let concurrency = cfg.max_concurrency;
    let duration    = Duration::from_secs(cfg.step_secs);

    println!("{:>12}  {:>10}  {:>10}  {:>12}  {:>12}  {:>12}  {:>12}",
        "timeout_ms", "ops/sec", "err%", "timeout%", "avg_ms", "p95_ms", "p99_ms");

    for &timeout_ms in &[500u64, 200, 100, 50, 20, 10] {
        let clients  = make_clients(&cfg.endpoint, concurrency, cfg.per_worker_connections).await?;
        let deadline = Instant::now() + duration;
        let lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
        let mut handles = Vec::new();

        for (wid, client) in clients.into_iter().enumerate() {
            let pairs     = pairs.clone();
            let lats      = lats.clone();
            let edge_type = "TRANSACTS_AT".to_string();
            handles.push(tokio::spawn(async move {
                let mut rng   = Rng::new(wid as u64 + 0x5EED);
                let mut local = Histogram::default();
                while Instant::now() < deadline {
                    let si  = rng.next() as usize % pairs.len();
                    let di  = rng.next() as usize % pairs.len();
                    let val = 1.0 + (rng.next() % 499) as f32;
                    let t0  = Instant::now();
                    let fut = client.upsert_edge(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), Some(val), None, None);
                    match tokio::time::timeout(Duration::from_millis(timeout_ms), fut).await {
                        Err(_elapsed) => local.record_timeout(),
                        Ok(Err(_))    => local.record_err(),
                        Ok(Ok(_))     => local.record_ok(t0.elapsed().as_micros() as u64),
                    }
                }
                lats.lock().unwrap().merge(local);
            }));
        }
        for h in handles { let _ = h.await; }

        let mut guard = lats.lock().unwrap();
        let tput    = guard.throughput(duration);
        let err_pct = guard.error_rate_pct();
        let to_pct  = guard.timeout_rate_pct();
        let avg     = guard.avg_ms();
        let p95     = guard.percentile_ms(95.0);
        let p99     = guard.percentile_ms(99.0);
        let marker  = if err_pct > 1.0 { " ◄ SLO BREACH" } else { "" };
        println!("{timeout_ms:>12}  {tput:>10.0}  {err_pct:>9.2}%  {to_pct:>11.2}%  {avg:>12.3}  {p95:>12.3}  {p99:>12.3}{marker}");
    }

    println!("\n  ► First row where err% > 1% = practical minimum client-side timeout.");
    println!("    Timeouts before transport errors = server is slow (backpressure).");
    println!("    Transport errors before timeouts = connection/queue exhaustion.");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  PHASE 8: Streaming vs. Individual RPC Head-to-Head
//  CLIENT CHANGE: directly compares the two write paths at identical concurrency
//  to quantify the streaming advantage in the user's specific environment.
//  Root cause of the gap: each individual RPC acquires one slot from the server's
//  max_ingest_concurrency semaphore per transaction; each streaming micro-batch
//  acquires one slot for up to 256 transactions — same semaphore, more work.
// ─────────────────────────────────────────────────────────────────────────────

async fn phase8_streaming_vs_rpc(cfg: &Config, pairs: Arc<Vec<(NodeRef, NodeRef)>>) -> Result<(), Box<dyn Error>> {
    print_separator("PHASE 8 — Streaming vs. Individual RPC (write path comparison)");
    println!("Same workers, same duration. Streaming uses pipeline=64 with reconnection loop.");
    println!("This directly quantifies why streaming is the production write path.\n");

    let concurrency = cfg.max_concurrency.min(64);
    let duration    = Duration::from_secs(cfg.step_secs * 2);

    // ── Arm A: Individual upsert_edge RPC ─────────────────────────────────────
    println!("  [A] Individual upsert_edge (workers={concurrency}, shared_conn={})...",
        !cfg.per_worker_connections);

    let rpc_clients  = make_clients(&cfg.endpoint, concurrency, cfg.per_worker_connections).await?;
    let rpc_deadline = Instant::now() + duration;
    let rpc_lats: Arc<std::sync::Mutex<Histogram>> = Arc::new(std::sync::Mutex::new(Histogram::default()));
    let mut handles = Vec::new();

    for (wid, client) in rpc_clients.into_iter().enumerate() {
        let pairs     = pairs.clone();
        let lats      = rpc_lats.clone();
        let edge_type = "TRANSACTS_AT".to_string();
        handles.push(tokio::spawn(async move {
            let mut rng   = Rng::new(wid as u64 + 0x8888);
            let mut local = Histogram::default();
            while Instant::now() < rpc_deadline {
                let si  = rng.next() as usize % pairs.len();
                let di  = rng.next() as usize % pairs.len();
                let val = 1.0 + (rng.next() % 999) as f32;
                let ts  = now_secs().saturating_sub((rng.next() as u32) % 86_400);
                let t0  = Instant::now();
                match client.upsert_edge(&edge_type, pairs[si].0.clone(), pairs[di].1.clone(), Some(val), Some(ts), None).await {
                    Ok(_)  => local.record_ok(t0.elapsed().as_micros() as u64),
                    Err(_) => local.record_err(),
                }
            }
            lats.lock().unwrap().merge(local);
        }));
    }
    for h in handles { let _ = h.await; }

    let mut rpc_guard = rpc_lats.lock().unwrap();
    let rpc_tput = rpc_guard.throughput(duration);
    let rpc_p99  = rpc_guard.percentile_ms(99.0);
    let rpc_err  = rpc_guard.error_rate_pct();
    drop(rpc_guard);

    // ── Arm B: IngestStream with reconnection loop ────────────────────────────
    const PIPELINE: usize = 64;
    println!("  [B] IngestStream (workers={concurrency}, pipeline_depth={PIPELINE}, auto-reconnect)...");

    let stream_edges    = Arc::new(AtomicU64::new(0));
    let stream_txns     = Arc::new(AtomicU64::new(0));
    let stream_reconnects = Arc::new(AtomicU64::new(0));
    let stream_deadline = Instant::now() + duration;
    let mut stream_handles = Vec::new();

    for wid in 0..concurrency {
        let ep          = cfg.endpoint.clone();
        let se          = stream_edges.clone();
        let st          = stream_txns.clone();
        let sr          = stream_reconnects.clone();

        stream_handles.push(tokio::spawn(async move {
            let mut total_seq = 0u64;
            let mut backoff   = Duration::from_millis(50);

            'reconnect: loop {
                if Instant::now() >= stream_deadline { break; }

                let client = match Client::connect(&ep).await {
                    Ok(c)  => { backoff = Duration::from_millis(50); c }
                    Err(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(2));
                        continue;
                    }
                };
                let (tx, mut resp) = match client.ingest_stream().await {
                    Ok(p)  => p,
                    Err(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(2));
                        continue;
                    }
                };

                let sem  = Arc::new(tokio::sync::Semaphore::new(PIPELINE));
                let sem2 = sem.clone();
                let se2  = se.clone();
                let st2  = st.clone();

                let drain = tokio::spawn(async move {
                    while let Some(r) = resp.message().await.ok().flatten() {
                        sem2.add_permits(1);
                        se2.fetch_add((r.edges_created + r.edges_updated) as u64, Ordering::Relaxed);
                        st2.fetch_add(1, Ordering::Relaxed);
                    }
                });

                let card_base     = (wid as u64) * 50_000;
                let merchant_base = (wid as u64) * 100_000;

                let stream_broken = loop {
                    if Instant::now() >= stream_deadline { break false; }
                    let permit = match sem.acquire().await { Ok(p) => p, Err(_) => break false };

                    let seq         = total_seq;
                    let card_id     = card_base + (seq % 10_000);
                    let merchant_id = merchant_base + (seq % 50_000);
                    let tx_id       = format!("p8w{wid}-{seq}");
                    let val         = 1.0 + (seq % 500) as f32;
                    let ts          = now_secs().saturating_sub((seq as u32) % 86_400);

                    let mut req = GraphClient::build_ingest_request(
                        Some(&tx_id),
                        &[
                            TransactionNode::new("card",     card_id.to_string()).with_key("c"),
                            TransactionNode::new("merchant", merchant_id.to_string()).with_key("m"),
                        ],
                        &[
                            TransactionEdge::new(
                                "TRANSACTS_AT",
                                TransactionNodeRef::request_node_key("c"),
                                TransactionNodeRef::request_node_key("m"),
                            ).with_key("e0"),
                        ],
                    );
                    if let Some(edge) = req.edges.first_mut() {
                        edge.numeric_value = Some(val);
                        edge.event_ts_secs = Some(ts);
                    }

                    if tx.send(req).await.is_err() { break true; }
                    std::mem::forget(permit);
                    total_seq += 1;
                };

                drop(tx);
                let _ = drain.await;

                if stream_broken {
                    sr.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    break 'reconnect;
                }
            }
        }));
    }
    for h in stream_handles { let _ = h.await; }

    let s_edges      = stream_edges.load(Ordering::Relaxed);
    let s_tput       = s_edges as f64 / duration.as_secs_f64();
    let s_reconnects = stream_reconnects.load(Ordering::Relaxed);
    let speedup      = if rpc_tput > 0.0 { s_tput / rpc_tput } else { 0.0 };

    // ── Results ───────────────────────────────────────────────────────────────
    println!();
    println!("  {:>30}  {:>14}  {:>10}  {:>8}", "write_path", "ops/sec", "p99_ms", "note");
    println!("  {:>30}  {:>14.0}  {:>10.3}  err={:.2}%",
        "individual upsert_edge", rpc_tput, rpc_p99, rpc_err);
    println!("  {:>30}  {:>14.0}  {:>10}  reconnects={}",
        format!("ingest_stream (pl={PIPELINE})"), s_tput, "pipelined", s_reconnects);

    println!();
    println!("  ┌────────────────────────────────────────────────────────────────┐");
    println!("  │  Streaming advantage: {speedup:.2}×  ({:+.1}% more throughput)          │",
        (speedup - 1.0) * 100.0);
    println!("  │                                                                │");
    println!("  │  Why: individual RPCs = 1 server semaphore slot / transaction  │");
    println!("  │        streaming     = 1 server semaphore slot / 256 tx batch  │");
    println!("  │                                                                │");
    println!("  │  Production recommendation:                                    │");
    println!("  │    Use ingest_stream for ALL bulk writes.                      │");
    println!("  │    Reserve upsert_edge for single low-volume control-plane ops │");
    println!("  └────────────────────────────────────────────────────────────────┘");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let cfg = Config::parse().map_err(|e| format!("argument error: {e}"))?;

    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║            JetGraph Stress Test — System Limit Discovery                ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  endpoint:            {}", cfg.endpoint);
    println!("  max_concurrency:     {}", cfg.max_concurrency);
    println!("  step_secs:           {}", cfg.step_secs);
    println!("  pair_count:          {}", cfg.pair_count);
    println!("  per_worker_conn:     {}", cfg.per_worker_connections);
    println!("  compare_connections: {}", cfg.compare_connections);
    if let Some(p) = cfg.only_phase {
        println!("  only_phase:          {p}");
    }

    println!("\nConnecting to engine...");
    let client = Client::connect(&cfg.endpoint).await
        .map_err(|e| format!("cannot connect to {}: {e}", cfg.endpoint))?;

    let healthy = client.health().check().await?;
    if !healthy { return Err("engine health check returned NOT_SERVING".into()); }
    println!("Engine health: SERVING ✓");

    ensure_schema(&client, cfg.bootstrap_schema).await?;

    println!("\nProvisioning workload data...");
    let pairs = build_workload_pairs(&client, cfg.pair_count).await?;

    let run_phase = |n: u8| cfg.only_phase.map_or(true, |p| p == n);

    if run_phase(1) { phase1_write_sweep(&cfg,       pairs.clone()).await?; }
    if run_phase(2) { phase2_read_sweep(&cfg,         pairs.clone()).await?; }
    if run_phase(3) { phase3_streaming_stress(&cfg).await?; }
    if run_phase(4) { phase4_mixed_contention(&cfg,   pairs.clone()).await?; }
    if run_phase(5) { phase5_hot_node(&cfg,           pairs.clone()).await?; }
    if run_phase(6) { phase6_multi_hop(&cfg,          pairs.clone()).await?; }
    if run_phase(7) { phase7_failure_boundary(&cfg,   pairs.clone()).await?; }
    if run_phase(8) { phase8_streaming_vs_rpc(&cfg,   pairs.clone()).await?; }

    print_separator("STRESS TEST COMPLETE");
    println!("\n  Interpretation guide:");
    println!("  • Ph1 saturation point      → write ceiling for individual RPCs");
    println!("  • Ph1 connection comparison → h2 bottleneck (run with --compare-connections)");
    println!("  • Ph2 saturation point      → read ceiling");
    println!("  • Ph3 fallback ratio        → streaming batch efficiency (target <0.1%)");
    println!("  • Ph3 reconnect column      → stream stability under sustained load");
    println!("  • Ph4 read/write p99 delta  → RCU contention severity");
    println!("  • Ph5 vs Ph1 p99            → per-node lock stripe contention cost");
    println!("  • Ph6 parallel speedup      → traversal latency gain from tokio::spawn");
    println!("  • Ph7 first SLO breach row  → minimum safe client-side timeout");
    println!("  • Ph8 streaming advantage   → throughput multiplier vs individual RPCs");
    println!();

    Ok(())
}
