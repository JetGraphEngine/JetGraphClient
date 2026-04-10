//! One-call best-effort transaction ingest example.
//!
//! Requires an engine at http://localhost:50051 and a finalized schema containing:
//! - node types: card, merchant
//! - edge type: TRANSACTS_AT (card -> merchant)

use jetgraph_client::{
    Client, NodeRef, TransactionNode, TransactionNodeRef, TransactionEdge,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("http://localhost:50051").await?;
    let ready = client.health().check().await?;
    if !ready {
        return Err("engine is not ready".into());
    }

    let nodes = vec![
        TransactionNode::new("card", "card-1001").with_key("cardA"),
        TransactionNode::new("merchant", "merchant-9001").with_key("merchantA"),
    ];

    let mut edge = TransactionEdge::new(
        "TRANSACTS_AT",
        TransactionNodeRef::request_node_key("cardA"),
        TransactionNodeRef::request_node_key("merchantA"),
    )
    .with_key("edge-primary");
    edge.numeric_value = Some(125.75);
    edge.event_ts_secs = Some(1_700_000_000);

    // Demonstrate mixed references: request key + standard external NodeRef.
    let mut edge_external = TransactionEdge::new(
        "TRANSACTS_AT",
        TransactionNodeRef::request_node_key("cardA"),
        TransactionNodeRef::node(NodeRef::external("merchant", "merchant-9001")),
    )
    .with_key("edge-external-ref");
    edge_external.numeric_value = Some(49.00);
    edge_external.event_ts_secs = Some(1_700_000_100);

    let result = client
        .ingest_transaction(Some("txn-demo-001"), &nodes, &[edge, edge_external])
        .await?;

    println!("transaction_id={}", result.transaction_id);
    println!(
        "nodes: created={} existing={} errors={}",
        result.nodes_created, result.nodes_existing, result.node_errors
    );
    println!(
        "edges: created={} updated={} errors={}",
        result.edges_created, result.edges_updated, result.edge_errors
    );

    for n in &result.node_results {
        println!(
            "node_result index={} key={:?} node_id={:?} created={} error={:?}",
            n.index, n.request_node_key, n.node_id, n.created, n.error
        );
    }
    for e in &result.edge_results {
        println!(
            "edge_result index={} key={:?} created_new={} error={:?}",
            e.index, e.request_edge_key, e.created_new, e.error
        );
    }

    Ok(())
}
