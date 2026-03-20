//! # Fraud Graph Client
//!
//! Rust client library for the Fraud Graph Engine. Connect to the engine via gRPC
//! and perform schema registration, node/edge operations, and feature queries.
//!
//! ## Quick start
//!
//! ```no_run
//! use fraud_graph_client::{Client, NodeRef};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::connect("http://localhost:50051").await?;
//!
//!     // Create a node
//!     let node_id = client.create_node("card", Some("card-01"), &[]).await?;
//!
//!     // Upsert a compact edge
//!     client.upsert_edge(
//!         "TRANSACTS_AT",
//!         NodeRef::external("card", "card-01"),
//!         NodeRef::external("merchant", "merch-01"),
//!         Some(99.99),
//!         None,
//!     ).await?;
//!
//!     // Get edge state with activity windows
//!     let state = client.get_edge_state(
//!         "USES_DEVICE",
//!         NodeRef::external("card", "card-01"),
//!         NodeRef::external("device", "dev-01"),
//!         None,
//!         Some(&[300, 3600]),
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod graph;
pub mod schema;
pub mod features;
pub mod health;
pub mod types;
mod error;

pub use error::ClientError;
pub use graph::GraphClient;
pub use schema::SchemaClient;
pub use features::FeatureClient;
pub use health::HealthClient;
pub use types::*;

/// Re-export for schema property registration.
pub use schema::schema_proto::ValueType;

use tonic::transport::Channel;
use std::time::Duration;

/// Unified client for all Fraud Graph Engine services.
///
/// Holds a shared gRPC channel and provides access to graph, schema, feature,
/// and health operations.
#[derive(Clone)]
pub struct Client {
    channel: Channel,
}

impl Client {
    /// Connect to the engine at the given endpoint.
    ///
    /// # Example
    /// ```no_run
    /// # use fraud_graph_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("http://localhost:50051").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .tcp_nodelay(true)
            .connect()
            .await?;
        Ok(Self { channel })
    }

    /// Create from an existing channel (e.g. for connection pooling).
    pub fn from_channel(channel: Channel) -> Self {
        Self { channel }
    }

    /// Graph operations: nodes, edges, neighbors, etc.
    pub fn graph(&self) -> GraphClient {
        GraphClient::new(self.channel.clone())
    }

    /// Schema operations: register types, finalize.
    pub fn schema(&self) -> SchemaClient {
        SchemaClient::new(self.channel.clone())
    }

    /// Feature operations: histograms, fraud context, flagging.
    pub fn features(&self) -> FeatureClient {
        FeatureClient::new(self.channel.clone())
    }

    /// Health check.
    pub fn health(&self) -> HealthClient {
        HealthClient::new(self.channel.clone())
    }

    // -------------------------------------------------------------------------
    // Convenience shortcuts (delegate to graph client)
    // -------------------------------------------------------------------------

    /// Create a node. With external_id, idempotent (content-addressable).
    pub async fn create_node(
        &self,
        node_type: &str,
        external_id: Option<&str>,
        properties: &[PropertyEntry],
    ) -> Result<CreateNodeResult, ClientError> {
        self.graph().create_node(node_type, external_id, properties).await
    }

    /// Upsert a compact edge.
    pub async fn upsert_edge(
        &self,
        edge_type: &str,
        src: NodeRef,
        dst: NodeRef,
        numeric_value: Option<f32>,
        event_ts_secs: Option<u32>,
    ) -> Result<UpsertEdgeResult, ClientError> {
        self.graph().upsert_edge(edge_type, src, dst, 0, 0, numeric_value, event_ts_secs).await
    }

    /// Get edge state, optionally with activity windows for activity-bitmap edge types.
    pub async fn get_edge_state(
        &self,
        edge_type: &str,
        src: NodeRef,
        dst: NodeRef,
        query_time_secs: Option<u32>,
        activity_windows_secs: Option<&[u64]>,
    ) -> Result<Option<EdgeState>, ClientError> {
        self.graph().get_edge_state(edge_type, src, dst, None, None, query_time_secs, activity_windows_secs).await
    }

    /// Get neighbors of a node.
    /// `limit` 0 = unlimited. `cursor` = neighbor_node_id after which to resume (0 = start).
    /// Returns (edges, has_more).
    pub async fn get_neighbors(
        &self,
        node: NodeRef,
        edge_type: &str,
        out_neighbors: bool,
        limit: u32,
        cursor: u64,
    ) -> Result<(Vec<NeighborEdge>, bool), ClientError> {
        self.graph().get_neighbors(node, edge_type, out_neighbors, limit, cursor).await
    }
}
