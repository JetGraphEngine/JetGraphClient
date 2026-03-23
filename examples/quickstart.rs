//! # Fraud Graph Demo — Credit Card Fraud Detection at Bank Scale
//!
//! A realistic fraud-detection graph that a senior fraud analyst at a major
//! bank would use to catch, investigate, and prevent credit-card fraud in
//! real-time. Run this to populate the engine and see the demo queries.
//!
//! ## Schema (7 node types · 7 edge types · ~100,000 nodes · ~90,000+ edges)
//!
//! ### Node Types
//! | Type     | Count  | Key Properties                                         |
//! |----------|--------|--------------------------------------------------------|
//! | customer |  10000 | tenure_days, country, risk_tier                        |
//! | account  |  15000 | account_type, credit_limit, open_date_ts               |
//! | card     |  20000 | card_type, credit_limit, exp_year, is_virtual          |
//! | device   |  15000 | device_type, os, is_rooted                             |
//! | ip       |  25000 | lat, lon, country, is_tor, is_datacenter, asn          |
//! | bin      |    500 | network, issuer_country, card_level                    |
//! | merchant |  14500 | name, mcc, country, lat, lon, risk_tier                |
//!
//! ### Edge Types
//! | Edge         | From     | To       | Features                                   |
//! |--------------|----------|----------|--------------------------------------------|
//! | OWNS_ACCOUNT | customer | account  | Permanent · structural                     |
//! | HAS_CARD     | account  | card     | Permanent · structural                     |
//! | CARD_TO_BIN  | card     | bin      | Permanent · structural                     |
//! | TRANSACTS_AT | card     | merchant | 90d TTL · amount bins ($5–$1k) · 1h activity bitmap |
//! | USES_DEVICE  | card     | device   | 30d TTL · 5-min activity bitmap                     |
//! | USES_IP      | card     | ip       | 30d TTL · 5-min activity bitmap                     |
//! | LINKED_ACCT  | account  | account  | Permanent · 1h activity bitmap                      |
//!
//! ### Fraud Scenarios Injected for Demo
//! 1. **Card Testing**      — card-tester-001: 150 micro-transactions, many merchants
//! 2. **Account Takeover**  — card-ato-001: normal history → sudden $800–$3k intl spike
//! 3. **Device Farm**       — device-farm-001: 500 unrelated cards share one device
//! 4. **Impossible Travel** — card-itv-001: New York → London in 8 minutes
//! 5. **Money Mule Chain**  — 3 linked accounts flagged for investigation
//!
//! ### Demo Queries
//! Q1  Edge State      — TX stats & activity bitmap for a card↔merchant pair
//! Q2  Neighbors       — Paginated merchant list for a card-testing card (velocity)
//! Q3  Neighbor Count  — Unique merchant velocity alert
//! Q4  Last Neighbor   — Previous IP for impossible-travel detection
//! Q5  Histogram       — Hourly spend distribution revealing ATO spike
//! Q6  Feature Vector  — Full ML feature set for real-time transaction scoring
//! Q7  Fraud Context   — Batch party check: which nodes are already flagged?
//!
//! Run with: `cargo run --example quickstart`
//! Requires the engine at http://localhost:50051

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Semaphore;

use fraud_graph_client::{Client, NodeRef, PropertyEntry, PropertyValue, ValueType};

/// Max concurrent in-flight gRPC calls during bulk data load.
const CONCURRENCY: usize = 64;

// ---------------------------------------------------------------------------
// Deterministic PRNG (xorshift64) — no external crate required
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0xcafe_babe_dead_beef)
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn u32_in(&mut self, lo: u32, hi: u32) -> u32 {
        lo + (self.next_u64() % (hi - lo) as u64) as u32
    }

    fn i64_in(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next_u64() % (hi - lo) as u64) as i64
    }

    fn f64_in(&mut self, lo: f64, hi: f64) -> f64 {
        let r = (self.next_u64() % 100_000) as f64 / 100_000.0;
        lo + r * (hi - lo)
    }

    fn pick<'a, T>(&mut self, s: &'a [T]) -> &'a T {
        &s[(self.next_u64() % s.len() as u64) as usize]
    }

    fn pct(&mut self, p: u64) -> bool {
        self.next_u64() % 100 < p
    }
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

fn now_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

fn secs_ago(s: u32) -> u32 {
    now_secs().saturating_sub(s)
}
fn mins_ago(m: u32) -> u32 {
    secs_ago(m * 60)
}
fn hours_ago(h: u32) -> u32 {
    secs_ago(h * 3600)
}
fn days_ago(d: u32) -> u32 {
    secs_ago(d * 86_400)
}

// ---------------------------------------------------------------------------
// Bulk node creation helper (parallel, semaphore-bounded)
// ---------------------------------------------------------------------------

async fn bulk_create_nodes(
    client: &Client,
    node_type: &str,
    items: Vec<(String, Vec<PropertyEntry>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sem = Arc::new(Semaphore::new(CONCURRENCY));
    let mut handles = Vec::with_capacity(items.len());
    let nt = node_type.to_string();

    for (ext_id, props) in items {
        let c = client.clone();
        let nt2 = nt.clone();
        let permit = sem.clone().acquire_owned().await.unwrap();
        handles.push(tokio::spawn(async move {
            let r = c.create_node(&nt2, Some(&ext_id), &props).await;
            drop(permit);
            r
        }));
    }

    for h in handles {
        h.await??;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Bulk edge upsert helper
// ---------------------------------------------------------------------------

async fn bulk_upsert_edges(
    client: &Client,
    edge_type: &str,
    items: Vec<(NodeRef, NodeRef, Option<f32>, Option<u32>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sem = Arc::new(Semaphore::new(CONCURRENCY));
    let mut handles = Vec::with_capacity(items.len());
    let et = edge_type.to_string();

    for (src, dst, val, ts) in items {
        let c = client.clone();
        let et2 = et.clone();
        let permit = sem.clone().acquire_owned().await.unwrap();
        handles.push(tokio::spawn(async move {
            let r = c.upsert_edge(&et2, src, dst, val, ts).await;
            drop(permit);
            r
        }));
    }

    for h in handles {
        h.await??;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("http://localhost:50051").await?;

    let ready = client.health().check().await?;
    println!("Engine ready: {ready}");
    assert!(ready, "Engine is not READY — start the Fraud Graph Engine first");

    println!("\n[1/6] Schema registration");
    setup_schema(&client).await?;

    println!("\n[2/6] Creating ~100,000 nodes");
    create_nodes(&client).await?;

    println!("\n[3/6] Creating structural ownership edges (~55,000)");
    create_static_edges(&client).await?;

    println!("\n[4/6] Creating transaction & activity edges (~90,000)");
    create_transaction_edges(&client).await?;

    println!("\n[5/6] Injecting named fraud scenarios");
    inject_fraud_scenarios(&client).await?;

    println!("\n[6/6] Demo Queries — thinking like a fraud analyst");
    demo_queries(&client).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 1. Schema
// ---------------------------------------------------------------------------

async fn setup_schema(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let mut s = client.schema();

    // Node types
    for nt in &[
        "customer", "account", "card", "device", "ip", "bin", "merchant",
    ] {
        s.register_node_type(nt).await?;
        println!("  node type: {nt}");
    }

    // ── Structural edges (permanent) ────────────────────────────────────────
    for (name, from, to) in &[
        ("OWNS_ACCOUNT", "customer", "account"),
        ("HAS_CARD", "account", "card"),
        ("CARD_TO_BIN", "card", "bin"),
    ] {
        s.register_compact_edge_type(name, from, to, 0, vec![], "", 3_600)
            .await?;
        println!("  edge type: {name}");
    }

    // ── TRANSACTS_AT: Card → Merchant ───────────────────────────────────────
    // Tracks every purchase event with:
    //   - Amount histogram (8 bins from <$5 to ≥$1k) for spending pattern analysis
    //   - 1-hour activity bitmap ticks for velocity analysis
    //   - 48-slot hourly + 30-slot daily node histograms for velocity
    //   - 90-day TTL matching typical card fraud investigation windows
    s.register_compact_edge_type(
        "TRANSACTS_AT",
        "card",
        "merchant",
        90 * 86_400, // 90-day TTL
        // 7 thresholds → 8 amount bins (USD)
        // bin0:<$5 | bin1:$5-$25 | bin2:$25-$50 | bin3:$50-$100
        // bin4:$100-$250 | bin5:$250-$500 | bin6:$500-$1k | bin7:≥$1k
        vec![5.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0],
        "amount",
        3_600, // 1-hour tick granularity
    )
    .await?;
    println!("  edge type: TRANSACTS_AT");

    // ── USES_DEVICE: Card → Device ───────────────────────────────────────────
    // Activity bitmap (5-min ticks) reveals temporal device sharing:
    //   - A card using 2+ devices in the same session = ATO signal
    //   - A device used by 100+ unrelated cards = device farm / mass compromise
    s.register_compact_edge_type(
        "USES_DEVICE",
        "card",
        "device",
        30 * 86_400,
        vec![],
        "",
        300, // 5-minute tick granularity
    )
    .await?;
    println!("  edge type: USES_DEVICE  [activity-bitmap 5m]");

    // ── USES_IP: Card → IP ───────────────────────────────────────────────────
    // Activity bitmap (5-min ticks) enables impossible-travel detection:
    //   - Card seen at US IP then UK IP within 10 minutes = alert
    //   - High velocity across many IPs in short window = account-sharing / bot
    s.register_compact_edge_type(
        "USES_IP",
        "card",
        "ip",
        30 * 86_400,
        vec![],
        "",
        300, // 5-minute tick granularity
    )
    .await?;
    println!("  edge type: USES_IP      [activity-bitmap 5m]");

    // ── LINKED_ACCT: Account → Account ──────────────────────────────────────
    // Money-mule network links. Permanent TTL — mule rings are investigated long
    // after the fraud window.
    s.register_compact_edge_type(
        "LINKED_ACCT",
        "account",
        "account",
        0, // permanent
        vec![],
        "",
        3_600, // 1-hour ticks
    )
    .await?;
    println!("  edge type: LINKED_ACCT");

    // ── Properties ──────────────────────────────────────────────────────────

    // Customer: demographic & risk attributes
    s.register_property("tenure_days", "customer", true, ValueType::Int)
        .await?;
    s.register_property("country", "customer", true, ValueType::String)
        .await?;
    // LOW / MEDIUM / HIGH — pre-computed offline risk segment
    s.register_property("risk_tier", "customer", true, ValueType::String)
        .await?;

    // Account: financial attributes
    s.register_property("account_type", "account", true, ValueType::String)
        .await?;
    s.register_property("credit_limit", "account", true, ValueType::Float)
        .await?;
    s.register_property("open_date_ts", "account", true, ValueType::Timestamp)
        .await?;

    // Card: issuance attributes used in scoring
    s.register_property("card_type", "card", true, ValueType::String)
        .await?;
    s.register_property("credit_limit", "card", true, ValueType::Float)
        .await?;
    s.register_property("exp_year", "card", true, ValueType::Int)
        .await?;
    // Virtual cards have higher CNP fraud rates
    s.register_property("is_virtual", "card", true, ValueType::Bool)
        .await?;

    // Device: fingerprint attributes for shared-device detection
    s.register_property("device_type", "device", true, ValueType::String)
        .await?;
    s.register_property("os", "device", true, ValueType::String)
        .await?;
    s.register_property("is_rooted", "device", true, ValueType::Bool)
        .await?;

    // IP: geo attributes for impossible-travel & proxy detection
    s.register_property("lat", "ip", true, ValueType::Float)
        .await?;
    s.register_property("lon", "ip", true, ValueType::Float)
        .await?;
    s.register_property("country", "ip", true, ValueType::String)
        .await?;
    s.register_property("is_tor", "ip", true, ValueType::Bool)
        .await?;
    s.register_property("is_datacenter", "ip", true, ValueType::Bool)
        .await?;
    // ASN → can flag known bad ASNs or residential vs datacenter traffic
    s.register_property("asn", "ip", true, ValueType::Int)
        .await?;

    // BIN: card network meta (first 6 digits of PAN)
    s.register_property("network", "bin", true, ValueType::String)
        .await?;
    s.register_property("issuer_country", "bin", true, ValueType::String)
        .await?;
    s.register_property("card_level", "bin", true, ValueType::String)
        .await?;

    // Merchant: acceptance attributes + risk classification
    s.register_property("name", "merchant", true, ValueType::String)
        .await?;
    // Merchant Category Code — key for high-risk classification
    s.register_property("mcc", "merchant", true, ValueType::Int)
        .await?;
    s.register_property("country", "merchant", true, ValueType::String)
        .await?;
    s.register_property("lat", "merchant", true, ValueType::Float)
        .await?;
    s.register_property("lon", "merchant", true, ValueType::Float)
        .await?;
    s.register_property("risk_tier", "merchant", true, ValueType::String)
        .await?;

    let v = s.finalize().await?;
    println!("  Schema finalized (version {v})");
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Node generation (~100,000 total)
// ---------------------------------------------------------------------------

async fn create_nodes(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // Geographic clusters for realistic lat/lon generation
    // (US domestic, UK, Europe, APAC, LatAm)
    let geo_clusters: &[(f64, f64, &str)] = &[
        (40.71, -74.00, "US"), // New York
        (34.05, -118.24, "US"), // Los Angeles
        (41.88, -87.63, "US"), // Chicago
        (29.76, -95.37, "US"), // Houston
        (33.45, -112.07, "US"), // Phoenix
        (51.51, -0.13, "GB"),  // London
        (50.11, 8.68, "DE"),   // Frankfurt
        (48.86, 2.35, "FR"),   // Paris
        (35.69, 139.69, "JP"), // Tokyo
        (1.35, 103.82, "SG"),  // Singapore
        (-23.55, -46.63, "BR"), // São Paulo
    ];

    // ── BINs (500) ──────────────────────────────────────────────────────────
    // Realistic card networks and issuer countries
    let networks = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"];
    let issuer_countries = ["US", "US", "US", "GB", "DE", "CA", "AU", "FR", "JP", "SG"];
    let card_levels = ["CLASSIC", "CLASSIC", "GOLD", "PLATINUM", "WORLD", "SIGNATURE"];

    let bins: Vec<_> = (1u32..=500)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 10_000);
            (
                format!("bin-{i:03}"),
                vec![
                    PropertyEntry::string("network", *rng.pick(&networks)),
                    PropertyEntry::string("issuer_country", *rng.pick(&issuer_countries)),
                    PropertyEntry::string("card_level", *rng.pick(&card_levels)),
                ],
            )
        })
        .collect();
    println!("  Creating 500 BINs...");
    bulk_create_nodes(client, "bin", bins).await?;

    // ── Merchants (14,500) ──────────────────────────────────────────────────
    // Mix of domestic and international merchants, with realistic MCCs.
    // High-risk MCCs: gambling (7995), crypto (6051), wire (4829), adult (5967)
    let merchant_configs: &[(i64, &str, &str)] = &[
        (5411, "Supermarket", "LOW"),   // Grocery
        (5812, "Restaurant", "LOW"),    // Dining
        (5912, "Pharmacy", "LOW"),      // Drug store
        (5999, "Retail Store", "LOW"),  // General retail
        (5732, "Electronics", "LOW"),   // Electronics
        (4121, "Taxi / Rideshare", "LOW"), // Transport
        (5944, "Jewelry Store", "MEDIUM"), // Jewelry
        (5045, "Computer Parts", "MEDIUM"), // IT wholesale
        (7011, "Hotel", "MEDIUM"),      // Lodging
        (4722, "Travel Agency", "MEDIUM"), // Travel
        (7995, "Gambling Platform", "HIGH"),  // HIGH RISK
        (6051, "Crypto Exchange", "HIGH"),    // HIGH RISK
        (4829, "Wire Transfer", "HIGH"),      // HIGH RISK
        (5967, "Adult Content", "HIGH"),      // HIGH RISK
    ];

    let merchant_countries = [
        "US", "US", "US", "US", "US", "US", "US",
        "GB", "GB", "DE", "FR", "SG", "NL", "CY",
    ];

    let merchants: Vec<_> = (1u32..=14_500)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 20_000);
            let cfg_idx = (i as usize - 1) % merchant_configs.len();
            let (mcc, category, risk_tier) = merchant_configs[cfg_idx];
            let country = *rng.pick(&merchant_countries);
            let &(base_lat, base_lon, _) = rng.pick(geo_clusters);
            let lat = base_lat + rng.f64_in(-2.0, 2.0);
            let lon = base_lon + rng.f64_in(-2.0, 2.0);
            (
                format!("merchant-{i:05}"),
                vec![
                    PropertyEntry::string("name", format!("{category} #{i}")),
                    PropertyEntry::int("mcc", mcc),
                    PropertyEntry::string("country", country),
                    PropertyEntry::float("lat", lat),
                    PropertyEntry::float("lon", lon),
                    PropertyEntry::string("risk_tier", risk_tier),
                ],
            )
        })
        .collect();
    println!("  Creating 14,500 merchants...");
    bulk_create_nodes(client, "merchant", merchants).await?;

    // ── Customers (10,000) ──────────────────────────────────────────────────
    let cust_countries = [
        "US", "US", "US", "US", "US", "US", "US",
        "CA", "GB", "DE", "AU", "MX",
    ];
    let risk_tiers = [
        "LOW", "LOW", "LOW", "LOW", "LOW", "LOW", "LOW",
        "MEDIUM", "MEDIUM", "HIGH",
    ];

    let customers: Vec<_> = (1u32..=10_000)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 30_000);
            (
                format!("customer-{i:05}"),
                vec![
                    // Tenure in days: 30 days (new) to 20 years (loyal)
                    PropertyEntry::int("tenure_days", rng.i64_in(30, 7_300)),
                    PropertyEntry::string("country", *rng.pick(&cust_countries)),
                    PropertyEntry::string("risk_tier", *rng.pick(&risk_tiers)),
                ],
            )
        })
        .collect();
    println!("  Creating 10,000 customers...");
    bulk_create_nodes(client, "customer", customers).await?;

    // ── Accounts (15,000) ──────────────────────────────────────────────────
    let account_types = ["CHECKING", "SAVINGS", "CREDIT", "CREDIT", "CREDIT"];

    let accounts: Vec<_> = (1u32..=15_000)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 40_000);
            let acct_type = *rng.pick(&account_types);
            let credit_limit = if acct_type == "CREDIT" {
                rng.f64_in(500.0, 50_000.0)
            } else {
                0.0
            };
            let open_date =
                days_ago(rng.u32_in(30, 3_650)) as i64;
            (
                format!("account-{i:05}"),
                vec![
                    PropertyEntry::string("account_type", acct_type),
                    PropertyEntry::float("credit_limit", credit_limit),
                    PropertyEntry { name: "open_date_ts".into(), value: PropertyValue::Timestamp(open_date) },
                ],
            )
        })
        .collect();
    println!("  Creating 15,000 accounts...");
    bulk_create_nodes(client, "account", accounts).await?;

    // ── Cards (20,000) ──────────────────────────────────────────────────────
    let card_types = ["CREDIT", "CREDIT", "CREDIT", "DEBIT", "DEBIT", "PREPAID"];

    let cards: Vec<_> = (1u32..=20_000)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 50_000);
            let card_type = *rng.pick(&card_types);
            let credit_limit = if card_type == "CREDIT" {
                rng.f64_in(500.0, 30_000.0)
            } else {
                0.0
            };
            let is_virtual = rng.pct(15); // 15% virtual cards
            (
                format!("card-{i:05}"),
                vec![
                    PropertyEntry::string("card_type", card_type),
                    PropertyEntry::float("credit_limit", credit_limit),
                    PropertyEntry::int("exp_year", rng.i64_in(2025, 2031)),
                    PropertyEntry::bool("is_virtual", is_virtual),
                ],
            )
        })
        .collect();
    println!("  Creating 20,000 cards...");
    bulk_create_nodes(client, "card", cards).await?;

    // ── Devices (15,000) ────────────────────────────────────────────────────
    let device_types = ["MOBILE", "MOBILE", "MOBILE", "BROWSER", "TABLET"];
    let os_options = ["iOS", "iOS", "Android", "Android", "Windows", "macOS", "Linux"];

    let devices: Vec<_> = (1u32..=15_000)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 60_000);
            let is_rooted = rng.pct(3); // 3% rooted/jailbroken
            (
                format!("device-{i:05}"),
                vec![
                    PropertyEntry::string("device_type", *rng.pick(&device_types)),
                    PropertyEntry::string("os", *rng.pick(&os_options)),
                    PropertyEntry::bool("is_rooted", is_rooted),
                ],
            )
        })
        .collect();
    println!("  Creating 15,000 devices...");
    bulk_create_nodes(client, "device", devices).await?;

    // ── IPs (25,000) ────────────────────────────────────────────────────────
    // Clustered around major cities; 2% TOR, 8% datacenter IPs
    let ips: Vec<_> = (1u32..=25_000)
        .map(|i| {
            let mut rng = Rng::new(i as u64 + 70_000);
            let &(base_lat, base_lon, country) = rng.pick(geo_clusters);
            let lat = base_lat + rng.f64_in(-3.0, 3.0);
            let lon = base_lon + rng.f64_in(-3.0, 3.0);
            let is_tor = rng.pct(2);
            let is_datacenter = !is_tor && rng.pct(8);
            let asn = rng.i64_in(1000, 65_000);
            (
                format!("ip-{i:05}"),
                vec![
                    PropertyEntry::float("lat", lat),
                    PropertyEntry::float("lon", lon),
                    PropertyEntry::string("country", country),
                    PropertyEntry::bool("is_tor", is_tor),
                    PropertyEntry::bool("is_datacenter", is_datacenter),
                    PropertyEntry::int("asn", asn),
                ],
            )
        })
        .collect();
    println!("  Creating 25,000 IPs...");
    bulk_create_nodes(client, "ip", ips).await?;

    // ── Named fraud scenario nodes (created/overwritten with specific properties) ─
    // These are referenced by the demo queries in step 6.

    // New York IP — used by card-itv-001 before impossible travel event
    client
        .create_node(
            "ip",
            Some("ip-ny-001"),
            &[
                PropertyEntry::float("lat", 40.71),
                PropertyEntry::float("lon", -74.00),
                PropertyEntry::string("country", "US"),
                PropertyEntry::bool("is_tor", false),
                PropertyEntry::bool("is_datacenter", false),
                PropertyEntry::int("asn", 7922),
            ],
        )
        .await?;

    // London IP — used by card-itv-001 AFTER impossible travel event
    client
        .create_node(
            "ip",
            Some("ip-uk-001"),
            &[
                PropertyEntry::float("lat", 51.51),
                PropertyEntry::float("lon", -0.13),
                PropertyEntry::string("country", "GB"),
                PropertyEntry::bool("is_tor", false),
                PropertyEntry::bool("is_datacenter", false),
                PropertyEntry::int("asn", 5089),
            ],
        )
        .await?;

    // Device Farm — rooted Android shared by 500+ unrelated cards
    client
        .create_node(
            "device",
            Some("device-farm-001"),
            &[
                PropertyEntry::string("device_type", "MOBILE"),
                PropertyEntry::string("os", "Android"),
                PropertyEntry::bool("is_rooted", true),
            ],
        )
        .await?;

    // Fraud scenario cards
    for (ext_id, limit, is_virtual) in &[
        ("card-tester-001", 500.0_f64, true),   // card-testing fraud
        ("card-ato-001", 15_000.0_f64, false),  // account takeover victim
        ("card-itv-001", 8_000.0_f64, false),   // impossible travel victim
    ] {
        client
            .create_node(
                "card",
                Some(ext_id),
                &[
                    PropertyEntry::string("card_type", "CREDIT"),
                    PropertyEntry::float("credit_limit", *limit),
                    PropertyEntry::int("exp_year", 2027),
                    PropertyEntry::bool("is_virtual", *is_virtual),
                ],
            )
            .await?;
    }

    // Fraud scenario accounts (money mule chain)
    for ext_id in &["account-mule-001", "account-mule-002", "account-mule-003"] {
        client
            .create_node(
                "account",
                Some(ext_id),
                &[
                    PropertyEntry::string("account_type", "CHECKING"),
                    PropertyEntry::float("credit_limit", 0.0),
                    PropertyEntry { name: "open_date_ts".into(), value: PropertyValue::Timestamp(days_ago(90) as i64) },
                ],
            )
            .await?;
    }

    // Crypto merchant used in money-mule scenario
    client
        .create_node(
            "merchant",
            Some("merchant-crypto-001"),
            &[
                PropertyEntry::string("name", "CryptoFX Exchange"),
                PropertyEntry::int("mcc", 6051),
                PropertyEntry::string("country", "CY"), // Cyprus — common offshore
                PropertyEntry::float("lat", 35.17),
                PropertyEntry::float("lon", 33.36),
                PropertyEntry::string("risk_tier", "HIGH"),
            ],
        )
        .await?;

    println!("  Nodes created: ~100,000 total");
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Structural ownership edges (~55,000)
// ---------------------------------------------------------------------------

async fn create_static_edges(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // OWNS_ACCOUNT: customer-{i} → account-{j}
    // ~15,000 accounts distributed across 10,000 customers (some have 2 accounts)
    let owns_account: Vec<_> = (1u32..=15_000)
        .map(|acct_i| {
            // Map each account to a customer; last 5,000 accounts share customers 1–5,000
            let cust_i = if acct_i <= 10_000 { acct_i } else { acct_i - 10_000 };
            (
                NodeRef::external("customer", &format!("customer-{cust_i:05}")),
                NodeRef::external("account", &format!("account-{acct_i:05}")),
                None::<f32>,
                None::<u32>,
            )
        })
        .collect();
    println!("  OWNS_ACCOUNT: 15,000 edges...");
    bulk_upsert_edges(client, "OWNS_ACCOUNT", owns_account).await?;

    // HAS_CARD: account-{j} → card-{k}
    // ~20,000 cards, 1–2 per account
    let has_card: Vec<_> = (1u32..=20_000)
        .map(|card_i| {
            let acct_i = if card_i <= 15_000 { card_i } else { card_i - 15_000 };
            (
                NodeRef::external("account", &format!("account-{acct_i:05}")),
                NodeRef::external("card", &format!("card-{card_i:05}")),
                None::<f32>,
                None::<u32>,
            )
        })
        .collect();
    println!("  HAS_CARD: 20,000 edges...");
    bulk_upsert_edges(client, "HAS_CARD", has_card).await?;

    // CARD_TO_BIN: card-{k} → bin-{hash}
    // Deterministically assign each card to one of 500 BINs
    let card_to_bin: Vec<_> = (1u32..=20_000)
        .map(|card_i| {
            let bin_i = (card_i % 500) + 1;
            (
                NodeRef::external("card", &format!("card-{card_i:05}")),
                NodeRef::external("bin", &format!("bin-{bin_i:03}")),
                None::<f32>,
                None::<u32>,
            )
        })
        .collect();
    println!("  CARD_TO_BIN: 20,000 edges...");
    bulk_upsert_edges(client, "CARD_TO_BIN", card_to_bin).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Transaction & activity edges (~90,000)
// ---------------------------------------------------------------------------

async fn create_transaction_edges(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // ── TRANSACTS_AT: Card → Merchant ───────────────────────────────────────
    // 15,000 active cards, each with 2–4 merchant relationships.
    // ~30,000 edges total; amounts vary by merchant risk tier.
    let mut tx_edges: Vec<(NodeRef, NodeRef, Option<f32>, Option<u32>)> = Vec::with_capacity(30_000);

    for card_i in 1u32..=15_000 {
        let mut rng = Rng::new(card_i as u64 + 100_000);
        let num_merchants = rng.u32_in(2, 5); // 2–4 unique merchants per card
        for _ in 0..num_merchants {
            let merch_i = rng.u32_in(1, 14_501);
            let merchant_ref =
                NodeRef::external("merchant", &format!("merchant-{merch_i:05}"));
            let card_ref = NodeRef::external("card", &format!("card-{card_i:05}"));

            let is_high_risk = merch_i % 14 >= 10; // ~29% of merchants are high-risk MCCs
            let amount: f32 = if is_high_risk {
                rng.f64_in(10.0, 500.0) as f32
            } else {
                rng.f64_in(5.0, 300.0) as f32
            };

            // Spread transactions across last 30 days
            let ts = secs_ago(rng.u32_in(0, 30 * 86_400));

            tx_edges.push((card_ref, merchant_ref, Some(amount), Some(ts)));
        }
    }
    println!("  TRANSACTS_AT: {} edges...", tx_edges.len());
    bulk_upsert_edges(client, "TRANSACTS_AT", tx_edges).await?;

    // ── USES_DEVICE: Card → Device ───────────────────────────────────────────
    // 15,000 cards each linked to 1–2 devices (occasionally 3 for high-risk cards)
    let mut dev_edges: Vec<(NodeRef, NodeRef, Option<f32>, Option<u32>)> =
        Vec::with_capacity(20_000);
    for card_i in 1u32..=15_000 {
        let mut rng = Rng::new(card_i as u64 + 200_000);
        let num_devices = if rng.pct(10) { 2 } else { 1 };
        for _ in 0..num_devices {
            let dev_i = rng.u32_in(1, 15_001);
            dev_edges.push((
                NodeRef::external("card", &format!("card-{card_i:05}")),
                NodeRef::external("device", &format!("device-{dev_i:05}")),
                None,
                Some(secs_ago(rng.u32_in(0, 7 * 86_400))),
            ));
        }
    }
    println!("  USES_DEVICE: {} edges...", dev_edges.len());
    bulk_upsert_edges(client, "USES_DEVICE", dev_edges).await?;

    // ── USES_IP: Card → IP ───────────────────────────────────────────────────
    // 15,000 cards each linked to 2–4 IPs (some IP reuse, some unique)
    let mut ip_edges: Vec<(NodeRef, NodeRef, Option<f32>, Option<u32>)> =
        Vec::with_capacity(40_000);
    for card_i in 1u32..=15_000 {
        let mut rng = Rng::new(card_i as u64 + 300_000);
        let num_ips = rng.u32_in(2, 5);
        for _ in 0..num_ips {
            let ip_i = rng.u32_in(1, 25_001);
            ip_edges.push((
                NodeRef::external("card", &format!("card-{card_i:05}")),
                NodeRef::external("ip", &format!("ip-{ip_i:05}")),
                None,
                Some(secs_ago(rng.u32_in(0, 7 * 86_400))),
            ));
        }
    }
    println!("  USES_IP: {} edges...", ip_edges.len());
    bulk_upsert_edges(client, "USES_IP", ip_edges).await?;

    // ── LINKED_ACCT: Account → Account ──────────────────────────────────────
    // 5,000 account-to-account suspicious links.
    // Most link low-index accounts to nearby accounts (simulating referral rings).
    let mut link_edges: Vec<(NodeRef, NodeRef, Option<f32>, Option<u32>)> =
        Vec::with_capacity(5_000);
    for i in 0u32..5_000 {
        let mut rng = Rng::new(i as u64 + 400_000);
        let src_i = rng.u32_in(1, 10_001);
        let dst_i = rng.u32_in(1, 10_001);
        if src_i == dst_i {
            continue;
        }
        link_edges.push((
            NodeRef::external("account", &format!("account-{src_i:05}")),
            NodeRef::external("account", &format!("account-{dst_i:05}")),
            None,
            None,
        ));
    }
    println!("  LINKED_ACCT: {} edges...", link_edges.len());
    bulk_upsert_edges(client, "LINKED_ACCT", link_edges).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Named fraud scenarios
// ---------------------------------------------------------------------------

async fn inject_fraud_scenarios(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // ── Scenario 1: Card Testing ─────────────────────────────────────────────
    // Fraudster uses a stolen card number to verify it's live by making many
    // tiny transactions ($0.50–$4.99) at different merchants.
    // Signal: extreme velocity + micro-amounts + many unique merchants.
    println!("  [1/5] Card Testing — card-tester-001");
    {
        let card = NodeRef::external("card", "card-tester-001");
        let mut rng = Rng::new(111_111);
        let mut edges = Vec::with_capacity(150);
        for i in 1u32..=150 {
            // Each transaction at a different merchant
            let merch_i = i % 14_500 + 1;
            let amount = rng.f64_in(0.50, 4.99) as f32;
            // All within the last 6 hours, in rapid succession
            let ts = hours_ago(6) + (i * 140); // ~2.3 min apart
            edges.push((
                card.clone(),
                NodeRef::external("merchant", &format!("merchant-{merch_i:05}")),
                Some(amount),
                Some(ts),
            ));
        }
        bulk_upsert_edges(client, "TRANSACTS_AT", edges).await?;
        println!("    150 micro-transactions across 150 merchants in 6 hours");
    }

    // ── Scenario 2: Account Takeover (ATO) ──────────────────────────────────
    // Cardholder's credentials were stolen. Fraudster logs in from a new device
    // and UK IP, then immediately makes high-value cross-border purchases.
    // Signal: new device + new country IP + sudden high-value + CROSS_BORDER.
    println!("  [2/5] Account Takeover — card-ato-001");
    {
        let card = NodeRef::external("card", "card-ato-001");

        // ── Normal history (past 30 days): low-value domestic purchases
        let normal_merchants = [
            ("merchant-00001", 42.50_f32),  // grocery
            ("merchant-00002", 18.75_f32),  // coffee / dining
            ("merchant-00003", 67.00_f32),  // pharmacy
            ("merchant-00004", 89.50_f32),  // gas station
            ("merchant-00005", 31.20_f32),  // retail
        ];
        let mut normal_edges = Vec::new();
        for (days_back, (merch, amount)) in
            (1u32..=30).zip(normal_merchants.iter().cycle()).take(30)
        {
            normal_edges.push((
                card.clone(),
                NodeRef::external("merchant", merch),
                Some(*amount),
                Some(days_ago(days_back)),
            ));
        }
        bulk_upsert_edges(client, "TRANSACTS_AT", normal_edges).await?;

        // Normal device & IP usage (domestic)
        for h in [72u32, 48, 24, 12] {
            client
                .upsert_edge(
                    "USES_DEVICE",
                    card.clone(),
                    NodeRef::external("device", "device-00001"),
                    None,
                    Some(hours_ago(h)),
                )
                .await?;
            client
                .upsert_edge(
                    "USES_IP",
                    card.clone(),
                    NodeRef::external("ip", "ip-00001"), // US IP
                    None,
                    Some(hours_ago(h)),
                )
                .await?;
        }

        // ── ATO pattern (last 3 hours): new device, UK IP, high-value international
        client
            .upsert_edge(
                "USES_DEVICE",
                card.clone(),
                NodeRef::external("device", "device-farm-001"), // NEW rooted device
                None,
                Some(hours_ago(3)),
            )
            .await?;
        client
            .upsert_edge(
                "USES_IP",
                card.clone(),
                NodeRef::external("ip", "ip-uk-001"), // NEW UK IP
                None,
                Some(hours_ago(3)),
            )
            .await?;

        let ato_purchases = [
            ("merchant-crypto-001", 1_850.00_f32),
            ("merchant-09001",      2_400.00_f32),
            ("merchant-09002",      3_200.00_f32),
            ("merchant-09003",      1_100.00_f32),
            ("merchant-09004",        950.00_f32),
            ("merchant-09005",      2_750.00_f32),
            ("merchant-09006",      1_450.00_f32),
            ("merchant-09007",        800.00_f32),
        ];
        let mut ato_edges = Vec::new();
        for (i, (merch, amount)) in ato_purchases.iter().enumerate() {
            ato_edges.push((
                card.clone(),
                NodeRef::external("merchant", merch),
                Some(*amount),
                Some(mins_ago(180 - i as u32 * 20)), // one every 20 mins
            ));
        }
        bulk_upsert_edges(client, "TRANSACTS_AT", ato_edges).await?;
        println!("    30 normal txns + 8 ATO txns ($800–$3,200) from UK IP + rooted device");
    }

    // ── Scenario 3: Device Farm ──────────────────────────────────────────────
    // A single compromised rooted Android device (device-farm-001) is being used
    // to perform transactions from 500 completely unrelated cards.
    // Signal: neighbor_count(device-farm-001, USES_DEVICE, in) >> normal device.
    println!("  [3/5] Device Farm — device-farm-001 (500 cards)");
    {
        let device = NodeRef::external("device", "device-farm-001");
        let mut edges = Vec::with_capacity(500);
        for card_i in 1u32..=500 {
            edges.push((
                NodeRef::external("card", &format!("card-{card_i:05}")),
                device.clone(),
                None,
                Some(secs_ago(card_i * 300)), // staggered over 2 days
            ));
        }
        bulk_upsert_edges(client, "USES_DEVICE", edges).await?;
        println!("    500 cards linked to device-farm-001");
    }

    // ── Scenario 4: Impossible Travel ────────────────────────────────────────
    // card-itv-001 was used in New York (ip-ny-001) then 8 minutes later in London
    // (ip-uk-001). Distance: 5,570 km. Max commercial flight: ~900 km/h = 6.2 hours.
    // Signal: get_last_neighbor(card, USES_IP, exclude=current_ip) → time_gap < threshold.
    println!("  [4/5] Impossible Travel — card-itv-001");
    {
        let card = NodeRef::external("card", "card-itv-001");

        // Historical US usage (normal — spread over past week)
        for h in [168u32, 120, 72, 48, 24] {
            client
                .upsert_edge(
                    "USES_IP",
                    card.clone(),
                    NodeRef::external("ip", "ip-ny-001"),
                    None,
                    Some(hours_ago(h)),
                )
                .await?;
        }
        // Most recent New York activity — 8 MINUTES ago (this is what makes travel impossible)
        client
            .upsert_edge(
                "USES_IP",
                card.clone(),
                NodeRef::external("ip", "ip-ny-001"),
                None,
                Some(mins_ago(8)),
            )
            .await?;

        // Normal domestic transaction 8 minutes ago (still in New York)
        client
            .upsert_edge(
                "TRANSACTS_AT",
                card.clone(),
                NodeRef::external("merchant", "merchant-00100"),
                Some(55.00_f32),
                Some(mins_ago(8)), // New York, 8 minutes ago
            )
            .await?;

        // Suspicious transaction NOW from London — the impossible travel event
        client
            .upsert_edge(
                "USES_IP",
                card.clone(),
                NodeRef::external("ip", "ip-uk-001"),
                None,
                Some(now_secs()),
            )
            .await?;

        client
            .upsert_edge(
                "TRANSACTS_AT",
                card.clone(),
                NodeRef::external("merchant", "merchant-06001"), // London merchant
                Some(320.00_f32),
                Some(now_secs()),
            )
            .await?;

        println!("    ip-ny-001 (New York) at T-8min + ip-uk-001 (London) at T+0 → 5,570 km in 8 min");
    }

    // ── Scenario 5: Money Mule Chain ─────────────────────────────────────────
    // Stolen funds flow: account-mule-001 → account-mule-002 → account-mule-003.
    // All three accounts were opened from the same device; the receiving end
    // cashes out via the crypto merchant.
    // Signal: graph traversal via LINKED_ACCT + MULE_SUSPECTED flag.
    println!("  [5/5] Money Mule Chain — account-mule-001 → 002 → 003");
    {
        // Link: 001 → 002 (suspected mule chain)
        client
            .upsert_edge(
                "LINKED_ACCT",
                NodeRef::external("account", "account-mule-001"),
                NodeRef::external("account", "account-mule-002"),
                None,
                Some(days_ago(10)),
            )
            .await?;

        // Link: 002 → 003 (suspected mule chain)
        client
            .upsert_edge(
                "LINKED_ACCT",
                NodeRef::external("account", "account-mule-002"),
                NodeRef::external("account", "account-mule-003"),
                None,
                Some(days_ago(8)),
            )
            .await?;

        // Flag all three mule accounts so fraud context queries surface them
        client
            .features()
            .flag_node(
                NodeRef::external("account", "account-mule-001"),
                0.91,
                "Money mule — funds received from 12 compromised cards, forwarded to crypto exchange",
            )
            .await?;
        client
            .features()
            .flag_node(
                NodeRef::external("account", "account-mule-002"),
                0.88,
                "Money mule — intermediary layer, opened from same device as account-mule-001",
            )
            .await?;
        client
            .features()
            .flag_node(
                NodeRef::external("account", "account-mule-003"),
                0.85,
                "Money mule — cash-out account, linked to CryptoFX Exchange withdrawals",
            )
            .await?;

        // Flag the device farm and the crypto merchant
        client
            .features()
            .flag_node(
                NodeRef::external("device", "device-farm-001"),
                0.97,
                "Device farm — rooted Android shared by 500+ unrelated cards, mass card compromise",
            )
            .await?;
        client
            .features()
            .flag_node(
                NodeRef::external("merchant", "merchant-crypto-001"),
                0.82,
                "High-risk merchant — offshore crypto exchange receiving funds from 47 flagged cards",
            )
            .await?;

        println!("    3-hop mule chain created + 5 nodes flagged for fraud context queries");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// 6. Demo queries
// ---------------------------------------------------------------------------

async fn demo_queries(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // ─────────────────────────────────────────────────────────────────────────
    // Q1: Edge State — transaction stats for a specific card↔merchant pair
    //
    // Use case: "How many times has this card transacted here, what's the
    // cumulative spend, and what does the activity bitmap say?"
    // Answered in O(1) — no table scan, no join.
    // ─────────────────────────────────────────────────────────────────────────
    // card-tester-001 visits merchants via: merch_i = i % 14_500 + 1 (i=1..150)
    // → i=1 → merchant-00002, i=2 → merchant-00003, … merchant-00002 is always hit.
    println!("\n─── Q1: Edge State (card-tester-001 at merchant-00002) ───");
    println!("    Use case: TX stats & activity bitmap for a specific card↔merchant pair");
    let state = client
        .get_edge_state(
            "TRANSACTS_AT",
            NodeRef::external("card", "card-tester-001"),
            NodeRef::external("merchant", "merchant-00002"),
            None,
            None,
        )
        .await?;
    if let Some(s) = state {
        println!(
            "    tx_count={} | approx_sum=${:.2} | last_seen={}s ago",
            s.tx_count,
            s.approx_sum,
            now_secs().saturating_sub(s.last_seen)
        );
        println!("    activity_bitmap=0x{:016x}", s.activity_bitmap_raw);
        println!(
            "    amount_bins: <$5={} $5-25={} $25-50={} $50-100={} $100-250={} $250-500={} $500k-1k={} ≥$1k={}",
            s.bins[0], s.bins[1], s.bins[2], s.bins[3],
            s.bins[4], s.bins[5], s.bins[6], s.bins[7]
        );
    } else {
        println!("    Edge not found");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Q2: Neighbors — all merchants visited by the card-testing card (paginated)
    //
    // Use case: "Show me the full merchant exposure graph for this suspicious
    // card — how many unique merchants has it touched, and which ones?"
    // In card-testing fraud, this count is 10-100× higher than normal.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Q2: Neighbors — Merchant exposure for card-tester-001 ───");
    println!("    Use case: Paginated merchant list for velocity investigation");
    let (neighbors, has_more) = client
        .get_neighbors(
            NodeRef::external("card", "card-tester-001"),
            "TRANSACTS_AT",
            true,  // out-neighbors (merchants this card transacted at)
            10,    // page size — show first 10
            0,     // cursor: start from the beginning
        )
        .await?;
    println!("    First page ({} merchants shown):", neighbors.len());
    for n in &neighbors {
        println!(
            "      neighbor_node_id={} edge_id={} created_at={}µs",
            n.neighbor_node_id, n.edge_id, n.created_at_us
        );
    }
    println!(
        "    has_more={} — use cursor to paginate through all merchants",
        has_more
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Q3: Neighbor Count — unique merchant velocity check
    //
    // Use case: "How many unique merchants has this card visited? Compare to
    // normal baseline to decide if this is a velocity alert."
    // Normal card: 5–15 unique merchants/month.  Card-testing: 100–500+ in hours.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Q3: Neighbor Count — Unique merchant velocity ───");
    println!("    Use case: Card-testing and bust-out velocity alerts");

    for card_id in &["card-tester-001", "card-ato-001", "card-00100"] {
        let (count, approx) = client
            .graph()
            .get_neighbor_count(
                NodeRef::external("card", card_id),
                "TRANSACTS_AT",
            )
            .await?;
        let label = match *card_id {
            "card-tester-001" => "⚠ VELOCITY ALERT (card-testing)",
            "card-ato-001"    => "⚠ ATO spike — 8 new merchants in 3h",
            _                 => "✓ Normal card",
        };
        println!(
            "    {card_id}: {count} unique merchants (approx={approx}) → {label}"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Q4: Last Neighbor — Impossible Travel Detection
    //
    // Use case: "card-itv-001 just transacted in London (ip-uk-001).
    // What was the PREVIOUS IP, and how long ago was it used?
    // If the time gap is < 6 hours, flag as impossible travel."
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Q4: Last Neighbor — Impossible Travel Detection ───");
    println!("    Use case: card-itv-001 just transacted in London — what was the previous IP?");

    let prev = client
        .graph()
        .get_last_neighbor(
            NodeRef::external("card", "card-itv-001"),
            "USES_IP",
            // Exclude the current IP so we get the PREVIOUS one
            Some(NodeRef::external("ip", "ip-uk-001")),
        )
        .await?;

    if let Some((prev_node_id, prev_ts)) = prev {
        let gap_secs = now_secs().saturating_sub(prev_ts);
        let gap_mins = gap_secs / 60;
        println!(
            "    Previous IP node_id={prev_node_id} last_seen={gap_mins} min ago"
        );

        // Retrieve geo for the previous IP
        let prev_node = client
            .graph()
            .get_node(NodeRef::NodeId(prev_node_id))
            .await?;
        let prev_country = prev_node
            .properties
            .iter()
            .find(|p| p.name == "country")
            .and_then(|p| {
                if let PropertyValue::String(s) = &p.value {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "?".into());
        println!("    Previous country: {prev_country} | Current country: GB (London)");

        if prev_country != "GB" {
            let distance_km = 5_570.0_f64; // NYC → London
            let speed_kmh = distance_km / (gap_mins.max(1) as f64 / 60.0);
            let verdict = if speed_kmh > 900.0 {
                "PHYSICALLY IMPOSSIBLE — fastest commercial aircraft ~900 km/h"
            } else if speed_kmh > 600.0 {
                "IMPOSSIBLE without direct flight — no connecting time"
            } else {
                "Suspicious — very tight travel window"
            };
            println!("    ⚠ IMPOSSIBLE TRAVEL DETECTED");
            println!("      {prev_country} → GB | {gap_mins} min gap | {distance_km:.0} km");
            println!("      Required speed: {speed_kmh:.0} km/h → {verdict}");
        }
    } else {
        println!("    No previous IP found");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Q5: Histogram — Spending pattern reveals ATO spike
    //
    // Use case: "Plot card-ato-001's hourly spending distribution for the last
    // 48 hours. A sudden shift from low-value domestic to high-value international
    // bins is the behavioural signature of an account takeover."
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Q5: Histogram — Spending pattern for card-ato-001 ───");
    println!("    Use case: Detect behavioural shift from ATO in hourly spend distribution");

    let hist = client
        .features()
        .query_node_histogram(
            NodeRef::external("card", "card-ato-001"),
            "TRANSACTS_AT",
            48, // last 48 hours
            0,
        )
        .await?;

    println!(
        "    Total events in last 48h: {} | Window covered: {}s",
        hist.total_events, hist.window_covered_secs
    );
    let bin_labels = [
        "<$5", "$5–$25", "$25–$50", "$50–$100",
        "$100–$250", "$250–$500", "$500–$1k", "≥$1k",
    ];
    print!("    Amount bins: ");
    for (label, count) in bin_labels.iter().zip(hist.total_counts.iter()) {
        print!("[{label}:{count}] ");
    }
    println!();

    let high_value_count: u32 = hist.total_counts.iter().skip(6).sum();
    if hist.total_events > 0 {
        let high_value_pct = high_value_count * 100 / hist.total_events;
        if high_value_pct >= 50 {
            println!(
                "    ⚠ ATO SPIKE: {high_value_pct}% of transactions are ≥$500 (expected <5% for this card)"
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Q6: Feature Vector — Full ML input for real-time transaction scoring
    //
    // Use case: At transaction time, pull a single combined feature vector for
    // the scoring model: neighbor counts, flag unions, amount histograms across
    // all edge types + contamination scores from flagged neighbors.
    // One call replaces 6+ separate queries.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Q6: Feature Vector — Real-time scoring for card-ato-001 ───");
    println!("    Use case: Single gRPC call to produce all ML features for the scoring model");
    println!("    Transaction context: merchant=merchant-crypto-001, device=device-farm-001, ip=ip-uk-001");

    let fv = client
        .features()
        .get_node_feature_vector(
            NodeRef::external("card", "card-ato-001"),
            &["TRANSACTS_AT", "USES_DEVICE", "USES_IP"],
            24, // histogram: last 24 hours
            7,  // histogram: last 7 days
            // Nodes in current transaction — checked for fraud flags
            &[
                NodeRef::external("merchant", "merchant-crypto-001"),
                NodeRef::external("device", "device-farm-001"),
                NodeRef::external("ip", "ip-uk-001"),
            ],
        )
        .await?;

    println!("    card node_id={}", fv.node_id);
    for ef in &fv.edge_features {
        println!(
            "    edge={:<15} unique_neighbors={:<6} total_tx={:<5} total_spend=${:.2}  bitmap=0x{:016x}",
            ef.edge_type_name, ef.neighbor_count, ef.total_tx_count,
            ef.total_approx_sum, ef.activity_bitmap_union
        );
    }
    println!("    direct_fraud_score={:.2}  fraudulent_neighbor_count={}  max_neighbor_fraud_score={:.2}",
        fv.direct_fraud_score, fv.fraudulent_neighbor_count, fv.max_neighbor_fraud_score);

    if fv.fraudulent_neighbor_count > 0 || fv.max_neighbor_fraud_score > 0.5 {
        println!("    ⚠ FRAUD SIGNAL: transaction parties include flagged nodes — DECLINE RECOMMENDED");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Q7: Fraud Context — Batch party check before authorising a transaction
    //
    // Use case: Before authorising ANY transaction, check every party involved —
    // the card, merchant, device, and IP — against the fraud flag store.
    // Any hit is a hard block or forces a step-up challenge.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Q7: Fraud Context — Batch party check ───");
    println!("    Use case: Is any node in this transaction already flagged?");
    println!("    Checking: card-ato-001, device-farm-001, ip-uk-001, merchant-crypto-001, account-mule-002");

    let ctx = client
        .features()
        .get_fraud_context(&[
            NodeRef::external("card", "card-ato-001"),
            NodeRef::external("device", "device-farm-001"),
            NodeRef::external("ip", "ip-uk-001"),
            NodeRef::external("merchant", "merchant-crypto-001"),
            NodeRef::external("account", "account-mule-002"),
        ])
        .await?;

    if ctx.flagged_nodes.is_empty() {
        println!("    ✓ No flagged nodes — proceed normally");
    } else {
        println!("    ⚠ FRAUD HITS ({} flagged nodes):", ctx.flagged_nodes.len());
        for n in &ctx.flagged_nodes {
            println!(
                "      node_id={} score={:.2} reason=\"{}\"",
                n.node_id, n.fraud_score, n.reason
            );
        }
        println!("    → TRANSACTION SHOULD BE DECLINED / STEP-UP AUTHENTICATION REQUIRED");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Bonus: USES_DEVICE edge state with activity windows
    //
    // Use case: "How many times did card-ato-001 use device-farm-001 in the last
    // 5 minutes, 1 hour, and 24 hours?" — reveals the ATO's device usage timeline.
    // Activity bitmaps make these window queries O(1) without any table scan.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n─── Bonus: Activity Windows — device usage timeline for card-ato-001 ───");
    println!("    Use case: How active is this card on device-farm-001 across time windows?");

    let dev_state = client
        .graph()
        .get_edge_state(
            "USES_DEVICE",
            NodeRef::external("card", "card-ato-001"),
            NodeRef::external("device", "device-farm-001"),
            None,
            None,
            None,
            Some(&[300_u64, 3_600, 86_400]), // 5 min · 1 hour · 24 hours
        )
        .await?;

    if let Some(s) = dev_state {
        println!(
            "    tx_count={} last_seen={}s ago",
            s.tx_count,
            now_secs().saturating_sub(s.last_seen)
        );
        let windows = [(300, "5min"), (3_600, "1h"), (86_400, "24h")];
        for ((_, label), count) in windows.iter().zip(s.activity_counts.iter()) {
            println!("    [{label}] {count} device interactions");
        }
        println!("    activity_bitmap=0x{:016x}", s.activity_bitmap_raw);
    } else {
        println!("    Edge not found");
    }

    println!("\n=== Demo complete ===");
    println!("Nodes: ~100,000 | Edges: ~90,000+ | Scenarios: 5 | Query types demonstrated: 7");
    Ok(())
}
