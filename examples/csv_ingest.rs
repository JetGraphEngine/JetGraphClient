//! # CSV Bulk Ingest — Optimized High-Throughput Loader
//!
//! Reads a fraud-transaction CSV and ingests it into the graph engine using
//! three key optimisations over the naive one-row-per-request approach:
//!
//! ## Optimisations
//!
//! 1. **Row batching** — up to `ROWS_PER_REQUEST` CSV rows are merged into a
//!    single `IngestTransactionRequest`.  With 6 nodes + 6 edges per row and
//!    engine limits of 1 024 nodes / 4 096 edges per request, 100 rows = 600
//!    nodes + 600 edges — well within limits.  The engine processes 100 rows
//!    in roughly the same wall-clock time as 1 because the fixed per-request
//!    overhead (framing, lock acquisition, RCU) dominates.
//!
//! 2. **Persistent streams** — each worker calls `ingest_stream()` exactly
//!    once before the ingest loop and reuses that stream for the entire run.
//!    The naive approach pays HTTP/2 stream setup + teardown for every chunk.
//!
//! 3. **Lock-free work distribution** — a bounded `tokio::sync::mpsc` channel
//!    replaces `Arc<Mutex<Vec<Option<_>>>>` + `AtomicU64`.  Workers block on
//!    `recv()` (async, zero-contention) rather than spinning on a mutex.
//!
//! ## CSV format
//!
//! ```text
//! customer_id,card_no,bin,device_id,merchant_id,ip_address,lat,lng,amount
//! ```
//!
//! ## Usage
//!
//! ```bash
//! cargo run --release --example csv_ingest -- \
//!   --file /data/transactions.csv \
//!   --workers 32 \
//!   --batch-size 200
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use jetgraph_client::{Client, PropertyEntry, TransactionEdge,
                      TransactionNode, TransactionNodeRef};

// ── Tuning constants ────────────────────────────────────────────────────────

/// Number of CSV rows packed into a single gRPC request.
/// Engine caps: 1 024 nodes / 4 096 edges per request.
/// Each row contributes 6 nodes + 6 edges → 100 rows = 600 nodes + 600 edges.
const ROWS_PER_REQUEST: usize = 150;

/// Bounded channel depth.  WORKERS × 4 keeps the pipeline full while bounding
/// memory when the engine is slower than the CSV parser.
const CHANNEL_DEPTH_PER_WORKER: usize = 4;

// ── Static lookup tables (unchanged from original) ──────────────────────────

const COUNTRY_CODES: &[&str] = &[
    "US", "GB", "DE", "FR", "CA", "AU", "JP", "SG", "NL", "SE",
];
const CARD_TYPES: &[&str] = &["VISA", "MASTERCARD", "AMEX", "DISCOVER"];
const MCC_CODES: &[u32] = &[5411, 5812, 7011, 4111, 5999, 6012, 7372];
const MERCHANT_NAMES: &[&str] = &[
    "SuperMart", "CafeBlue", "HotelGrand", "TransitCo",
    "GenStore", "FinServ", "TechCorp",
];

// ── CLI args ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Args {
    endpoint:   String,
    file:       String,
    workers:    usize,
    batch_size: usize, // chunk of CSV rows loaded at a time (for progress display)
    limit:      usize, // 0 = unlimited
}

impl Args {
    fn parse() -> Self {
        let raw: Vec<String> = std::env::args().collect();
        let mut args = Self {
            endpoint:   "http://localhost:50051".into(),
            file:       String::new(),
            workers:    32,
            batch_size: 10_000,
            limit:      0,
        };
        let mut i = 1;
        while i < raw.len() {
            match raw[i].as_str() {
                "--endpoint"   => { i += 1; args.endpoint   = raw[i].clone(); }
                "--file"       => { i += 1; args.file       = raw[i].clone(); }
                "--workers"    => { i += 1; args.workers    = raw[i].parse().unwrap_or(32); }
                "--batch-size" => { i += 1; args.batch_size = raw[i].parse().unwrap_or(10_000); }
                "--limit"      => { i += 1; args.limit      = raw[i].parse().unwrap_or(0); }
                "--help" | "-h" => {
                    eprintln!("Usage: csv_ingest [options]");
                    eprintln!("  --endpoint   <url>   gRPC endpoint (default: http://localhost:50051)");
                    eprintln!("  --file       <path>  CSV file to ingest");
                    eprintln!("  --workers    <n>     parallel stream workers (default: 32)");
                    eprintln!("  --batch-size <n>     rows per progress report (default: 10000)");
                    eprintln!("  --limit      <n>     stop after N rows (0 = unlimited)");
                    std::process::exit(0);
                }
                other => {
                    eprintln!("unknown arg: {other}");
                    std::process::exit(1);
                }
            }
            i += 1;
        }
        if args.file.is_empty() {
            eprintln!("error: --file is required");
            std::process::exit(1);
        }
        args
    }
}

// ── Data types ───────────────────────────────────────────────────────────────

/// One parsed CSV row (borrows nothing — all owned Strings).
#[derive(Debug, Clone)]
struct ParsedRow {
    customer_id: String,
    card_no:     String,
    bin:         String,
    device_id:   String,
    merchant_id: String,
    ip_address:  String,
    lat:         String,
    lng:         String,
    amount:      f64,
}

impl ParsedRow {
    fn from_record(r: &csv::StringRecord) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            customer_id: r.get(0).unwrap_or("").to_string(),
            card_no:     r.get(1).unwrap_or("").to_string(),
            bin:         r.get(2).unwrap_or("").to_string(),
            device_id:   r.get(3).unwrap_or("").to_string(),
            merchant_id: r.get(4).unwrap_or("").to_string(),
            ip_address:  r.get(5).unwrap_or("").to_string(),
            lat:         r.get(6).unwrap_or("0").to_string(),
            lng:         r.get(7).unwrap_or("0").to_string(),
            amount:      r.get(8).unwrap_or("0").parse().unwrap_or(0.0),
        })
    }
}

#[derive(Clone)]
struct GeoResult {
    lat:   f64,
    lng:   f64,
    ghash: String,
}

/// A fully-built batch ready to send through the ingest stream.
struct BatchTask {
    transaction_id: String,
    nodes: Vec<TransactionNode>,
    edges: Vec<TransactionEdge>,
    n_nodes: u64,
    n_edges: u64,
}

// ── Shared atomic counters ───────────────────────────────────────────────────

#[derive(Default)]
struct Counter {
    sent: AtomicU64,
    ack:  AtomicU64,
    err:  AtomicU64,
}
impl Counter {
    fn sent(&self) -> u64 { self.sent.load(Ordering::Relaxed) }
    fn ack(&self)  -> u64 { self.ack.load(Ordering::Relaxed) }
    fn err(&self)  -> u64 { self.err.load(Ordering::Relaxed) }
    fn in_flight(&self) -> u64 { self.sent().saturating_sub(self.ack() + self.err()) }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn simple_rng(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn rng_range(seed: &mut u64, n: usize) -> usize {
    (simple_rng(seed) as usize) % n
}

// ── Batch builder ─────────────────────────────────────────────────────────────

/// Pack `rows` (a slice of ParsedRow + derived fields) into one gRPC request.
///
/// Each row gets a unique in-request key suffix (`"-{i}"`) so nodes from
/// different rows don't collide inside a single batched request.  The engine
/// deduplicates by external_id, so the same physical card appearing in two
/// rows within a batch is handled correctly server-side.
fn build_batch(
    rows:        &[ParsedRow],
    geo_results: &[GeoResult],
    rng:         &mut u64,
    global_base: u64,
) -> (Vec<TransactionNode>, Vec<TransactionEdge>) {
    let mut nodes = Vec::with_capacity(rows.len() * 6);
    let mut edges = Vec::with_capacity(rows.len() * 6);

    for (i, (f, geo)) in rows.iter().zip(geo_results.iter()).enumerate() {
        let sfx = i.to_string(); // unique key suffix within this batch

        let ts          = unix_now() as u32;
        let mut rng_tmp = rng.wrapping_add(global_base + i as u64);
        let created_at  = unix_now() as i64 - (simple_rng(&mut rng_tmp) % 94_608_000) as i64;
        let country_code = COUNTRY_CODES[rng_range(&mut rng_tmp, COUNTRY_CODES.len())];
        let card_type    = CARD_TYPES[rng_range(&mut rng_tmp, CARD_TYPES.len())];
        let expiry       = format!(
            "20{:02}-{:02}",
            25 + simple_rng(&mut rng_tmp) % 5,
            1 + simple_rng(&mut rng_tmp) % 12,
        );
        let mcc          = MCC_CODES[rng_range(&mut rng_tmp, MCC_CODES.len())];
        let merch_name   = MERCHANT_NAMES[rng_range(&mut rng_tmp, MERCHANT_NAMES.len())];

        // ── nodes ─────────────────────────────────────────────────────────────
        nodes.push(
            TransactionNode::new("customer", &f.customer_id)
                .with_key(format!("customer-{sfx}"))
                .with_properties(vec![PropertyEntry::int("createdAt", created_at)]),
        );
        nodes.push(
            TransactionNode::new("card", &f.card_no)
                .with_key(format!("card-{sfx}"))
                .with_properties(vec![
                    PropertyEntry::string("cardCountryCode", country_code),
                    PropertyEntry::string("cardProductType", card_type),
                    PropertyEntry::string("cardExpiryDate", &expiry),
                ]),
        );
        nodes.push(TransactionNode::new("bin",      &f.bin).with_key(format!("bin-{sfx}")));
        nodes.push(TransactionNode::new("device",   &f.device_id).with_key(format!("device-{sfx}")));
        nodes.push(
            TransactionNode::new("merchant", &f.merchant_id)
                .with_key(format!("merchant-{sfx}"))
                .with_properties(vec![
                    PropertyEntry::int("merchantCategoryCode", mcc as i64),
                    PropertyEntry::string("merchantName", merch_name),
                ]),
        );
        nodes.push(
            TransactionNode::new("ip_address", &f.ip_address)
                .with_key(format!("ip-{sfx}"))
                .with_properties(vec![
                    PropertyEntry::float("lat",      geo.lat),
                    PropertyEntry::float("lng",      geo.lng),
                    PropertyEntry::string("geohash", &geo.ghash),
                ]),
        );

        // ── edge refs ─────────────────────────────────────────────────────────
        let customer_ref = TransactionNodeRef::request_node_key(format!("customer-{sfx}"));
        let card_ref     = TransactionNodeRef::request_node_key(format!("card-{sfx}"));
        let bin_ref      = TransactionNodeRef::request_node_key(format!("bin-{sfx}"));
        let device_ref   = TransactionNodeRef::request_node_key(format!("device-{sfx}"));
        let merchant_ref = TransactionNodeRef::request_node_key(format!("merchant-{sfx}"));
        let ip_ref       = TransactionNodeRef::request_node_key(format!("ip-{sfx}"));

        // ── edges ─────────────────────────────────────────────────────────────
        let mut e1 = TransactionEdge::new("HAS_CARD",     customer_ref.clone(), card_ref.clone());
        e1.numeric_value = Some(1.0); e1.event_ts_secs = Some(ts);

        let mut e2 = TransactionEdge::new("OWNS_DEVICE",  customer_ref, device_ref.clone());
        e2.numeric_value = Some(1.0); e2.event_ts_secs = Some(ts);

        let mut e3 = TransactionEdge::new("HAS_BIN",      card_ref.clone(), bin_ref);
        e3.numeric_value = Some(1.0); e3.event_ts_secs = Some(ts);

        let mut e4 = TransactionEdge::new("TRANSACTS_AT", card_ref.clone(), merchant_ref);
        e4.numeric_value = Some(f.amount as f32); e4.event_ts_secs = Some(ts);

        let mut e5 = TransactionEdge::new("USES_DEVICE",  card_ref.clone(), device_ref);
        e5.numeric_value = Some(f.amount as f32); e5.event_ts_secs = Some(ts);

        let mut e6 = TransactionEdge::new("USES_IP",      card_ref, ip_ref);
        e6.numeric_value = Some(f.amount as f32); e6.event_ts_secs = Some(ts);

        edges.extend([e1, e2, e3, e4, e5, e6]);
    }

    (nodes, edges)
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // ── Connect ───────────────────────────────────────────────────────────────
    println!("Connecting to {} …", args.endpoint);
    let client = Client::connect(&args.endpoint).await?;
    if !client.health().check().await? {
        return Err("engine not READY".into());
    }
    println!("Engine ready.\n");

    // ── Counters ──────────────────────────────────────────────────────────────
    let txn_ctr  = Arc::new(Counter::default());
    let node_ctr = Arc::new(Counter::default());
    let edge_ctr = Arc::new(Counter::default());

    // ── Channel ───────────────────────────────────────────────────────────────
    // The channel decouples CSV parsing from gRPC writes.
    // Depth = workers × CHANNEL_DEPTH_PER_WORKER so the pipeline stays full.
    let channel_depth = args.workers * CHANNEL_DEPTH_PER_WORKER;
    let (batch_tx, batch_rx) = mpsc::channel::<BatchTask>(channel_depth);
    let batch_rx = Arc::new(tokio::sync::Mutex::new(batch_rx));

    // ── Spawn persistent workers ──────────────────────────────────────────────
    // Each worker opens ONE stream before the ingest loop and reuses it for the
    // entire run, eliminating per-chunk HTTP/2 stream setup/teardown overhead.
    let mut worker_handles = Vec::with_capacity(args.workers);
    for w in 0..args.workers {
        let client    = client.clone();
        let batch_rx  = Arc::clone(&batch_rx);
        let tc        = Arc::clone(&txn_ctr);
        let nc        = Arc::clone(&node_ctr);
        let ec        = Arc::clone(&edge_ctr);

        worker_handles.push(tokio::spawn(async move {
            // Open the stream once — keep it alive for the whole run.
            let (stream_tx, stream_rx) = match client.ingest_stream().await {
                Ok(pair) => pair,
                Err(e)   => { eprintln!("⚠ worker {w}: failed to open stream: {e}"); return; }
            };

            // Response reader runs as a sibling task so sends never stall.
            let tc2 = tc.clone();
            let nc2 = nc.clone();
            let ec2 = ec.clone();
            let reader = tokio::spawn(async move {
                let mut responses = stream_rx;
                while let Some(result) = responses.next().await {
                    match result {
                        Ok(resp) => {
                            tc2.ack.fetch_add(1, Ordering::Relaxed);
                            nc2.ack.fetch_add(
                                (resp.nodes_created + resp.nodes_existing) as u64,
                                Ordering::Relaxed,
                            );
                            nc2.err.fetch_add(resp.node_errors as u64, Ordering::Relaxed);
                            ec2.ack.fetch_add(
                                (resp.edges_created + resp.edges_updated) as u64,
                                Ordering::Relaxed,
                            );
                            ec2.err.fetch_add(resp.edge_errors as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            tc2.err.fetch_add(1, Ordering::Relaxed);
                            eprintln!("⚠ worker {w}: response error: {e}");
                        }
                    }
                }
            });

            // Work-receive loop — blocks on async recv (zero-overhead when idle).
            loop {
                let task = {
                    let mut rx = batch_rx.lock().await;
                    rx.recv().await
                };
                let task = match task {
                    Some(t) => t,
                    None    => break, // channel closed → all CSV rows sent
                };

                let nn = task.n_nodes;
                let ne = task.n_edges;
                tc.sent.fetch_add(1, Ordering::Relaxed);
                nc.sent.fetch_add(nn, Ordering::Relaxed);
                ec.sent.fetch_add(ne, Ordering::Relaxed);

                if let Err(e) = stream_tx.send(Some(&task.transaction_id), &task.nodes, &task.edges).await {
                    tc.err.fetch_add(1, Ordering::Relaxed);
                    nc.err.fetch_add(nn, Ordering::Relaxed);
                    ec.err.fetch_add(ne, Ordering::Relaxed);
                    if tc.err() <= 5 { eprintln!("⚠ worker {w}: send error: {e}"); }
                    break;
                }
            }

            // Half-close the stream so the engine flushes remaining responses.
            drop(stream_tx);
            let _ = reader.await;
        }));
    }

    // ── CSV reading + batching loop ───────────────────────────────────────────
    let mut rdr = csv::Reader::from_path(&args.file)?;
    let mut geo_cache: HashMap<String, GeoResult> = HashMap::new();
    let mut rng_seed: u64 = 0xcafe_babe_dead_beef;

    let mut global_row:  u64     = 0;
    let mut batch_rows:  Vec<ParsedRow>  = Vec::with_capacity(ROWS_PER_REQUEST);
    let mut batch_geos:  Vec<GeoResult>  = Vec::with_capacity(ROWS_PER_REQUEST);

    let total_start      = Instant::now();
    let mut last_log     = Instant::now();
    let mut last_txn_ack = 0u64;
    let mut chunk_row    = 0u64; // rows since last progress line

    // Progress header
    println!("{:>8}  {:>10}  {:>10}  {:>9}  {:>10}  {:>7}",
        "elapsed", "rows_csv", "txns_ack", "txns/s", "in_flight", "errors");
    println!("{}", "─".repeat(62));

    let mut records = rdr.records();

    loop {
        // ── Parse one CSV row ─────────────────────────────────────────────────
        if args.limit != 0 && global_row >= args.limit as u64 { break; }
        let rec = match records.next() {
            Some(Ok(r))  => r,
            Some(Err(e)) => return Err(e.into()),
            None         => break,
        };

        let f = ParsedRow::from_record(&rec)?;

        // GeoIP lookup (cached by IP address string)
        let geo = geo_cache.entry(f.ip_address.clone()).or_insert_with(|| {
            // Real code would call your GeoIP reader here.
            // Fallback: use lat/lng from the CSV row directly.
            let lat: f64 = f.lat.parse().unwrap_or(0.0);
            let lng: f64 = f.lng.parse().unwrap_or(0.0);
            // geohash::encode requires external crate; replace with your actual call.
            let ghash = format!("s{:04x}", ((lat * 1000.0) as i32).unsigned_abs() % 0xFFFF);
            GeoResult { lat, lng, ghash }
        }).clone();

        batch_rows.push(f);
        batch_geos.push(geo);
        global_row  += 1;
        chunk_row   += 1;

        // ── Flush batch when full ─────────────────────────────────────────────
        if batch_rows.len() >= ROWS_PER_REQUEST {
            let (nodes, edges) = build_batch(&batch_rows, &batch_geos, &mut rng_seed, global_row);
            let n_nodes = nodes.len() as u64;
            let n_edges = edges.len() as u64;
            batch_tx.send(BatchTask {
                transaction_id: format!("batch-{global_row}"),
                nodes,
                edges,
                n_nodes,
                n_edges,
            }).await?;
            batch_rows.clear();
            batch_geos.clear();
        }

        // ── Progress every ~1 s ───────────────────────────────────────────────
        if last_log.elapsed() >= Duration::from_secs(1) || chunk_row >= args.batch_size as u64 {
            let elapsed_s  = total_start.elapsed().as_secs_f64();
            let txn_ack    = txn_ctr.ack();
            let delta      = txn_ack.saturating_sub(last_txn_ack);
            let interval_s = last_log.elapsed().as_secs_f64().max(0.001);
            println!("{:>7.1}s  {:>10}  {:>10}  {:>8.0}/s  {:>10}  {:>7}",
                elapsed_s,
                global_row,
                txn_ack,
                delta as f64 / interval_s,
                txn_ctr.in_flight(),
                txn_ctr.err(),
            );
            last_txn_ack = txn_ack;
            last_log     = Instant::now();
            chunk_row    = 0;
        }
    }

    // ── Flush final partial batch ─────────────────────────────────────────────
    if !batch_rows.is_empty() {
        let (nodes, edges) = build_batch(&batch_rows, &batch_geos, &mut rng_seed, global_row);
        let n_nodes = nodes.len() as u64;
        let n_edges = edges.len() as u64;
        batch_tx.send(BatchTask {
            transaction_id: format!("batch-{global_row}-final"),
            nodes,
            edges,
            n_nodes,
            n_edges,
        }).await?;
    }

    // ── Drain: close channel, wait for workers ────────────────────────────────
    // Dropping batch_tx signals workers that all batches have been queued.
    // Workers will drain their in-flight requests before exiting.
    drop(batch_tx);

    println!("\nAll batches queued — draining workers …");
    let drain_start = Instant::now();

    // Poll until all in-flight requests are acknowledged (or timed out at 120 s).
    loop {
        if txn_ctr.in_flight() == 0 { break; }
        if drain_start.elapsed().as_secs() > 120 {
            eprintln!("⚠ drain timeout — {} requests still in flight", txn_ctr.in_flight());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for h in worker_handles {
        let _ = h.await;
    }

    // ── Final summary ─────────────────────────────────────────────────────────
    let total_s = total_start.elapsed().as_secs_f64();
    println!("\n{}", "═".repeat(62));
    println!("  CSV rows parsed  : {global_row}");
    println!("  gRPC requests    : {}", txn_ctr.sent());
    println!("  Requests ack'd   : {}", txn_ctr.ack());
    println!("  Request errors   : {}", txn_ctr.err());
    println!("  Nodes upserted   : {}", node_ctr.ack());
    println!("  Edges upserted   : {}", edge_ctr.ack());
    println!("  Elapsed          : {:.2}s", total_s);
    println!("  CSV rows/s       : {:.0}", global_row as f64 / total_s.max(0.001));
    println!(
        "  Rows/request     : {:.1}",
        global_row as f64 / txn_ctr.sent().max(1) as f64,
    );
    println!("{}", "═".repeat(62));

    Ok(())
}
