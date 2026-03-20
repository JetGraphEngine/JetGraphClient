//! Example: connect, create nodes, upsert edges, query.
//!
//! Run with: cargo run --example quickstart
//! Requires the engine to be running at localhost:50051 (e.g. run quickstart in Python shell first).

use fraud_graph_client::{Client, NodeRef};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("http://localhost:50051").await?;

    // Health check
    let mut health = client.health();
    let ready = health.check().await?;
    println!("Engine ready: {}", ready);

    // Create nodes (schema must already be set up). With external_id, idempotent.
    let card = client.create_node("card", Some("card-demo"), &[]).await?;
    println!("Card node: {} (created={})", card.node_id, card.created);

    let merchant = client.create_node("merchant", Some("merch-demo"), &[]).await?;
    println!("Merchant node: {} (created={})", merchant.node_id, merchant.created);

    // Upsert edge
    let result = client.upsert_edge(
        "TRANSACTS_AT",
        NodeRef::external("card", "card-demo"),
        NodeRef::external("merchant", "merch-demo"),
        Some(142.50),
        None,
    ).await?;
    println!("Upserted edge: created_new={} tx_count={} approx_sum=${:.2}",
        result.created_new, result.tx_count, result.approx_sum);

    // Get edge state
    let state = client.get_edge_state(
        "TRANSACTS_AT",
        NodeRef::external("card", "card-demo"),
        NodeRef::external("merchant", "merch-demo"),
        None,
        None,
    ).await?;
    if let Some(s) = state {
        println!("Edge state: tx_count={} last_seen={} flags={:?}",
            s.tx_count, s.last_seen, s.active_flag_names);
    } else {
        println!("Edge not found");
    }

    Ok(())
}
