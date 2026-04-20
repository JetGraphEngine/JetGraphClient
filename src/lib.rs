//! # Fraud Graph Client
//!
//! Rust client library for JetGraph. Connect to the engine via gRPC
//! and perform schema registration, node/edge operations, and feature queries.
//!
//! ## Quick start
//!
//! ```no_run
//! use jetgraph_client::{Client, NodeRef};
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
//!
//! ## Similarity (k-NN SIMILAR_TO edges)
//!
//! Register a static edge type (8 B/pair — no activity bitmap, only score + timestamp),
//! then build or query similarity in real time:
//!
//! ```no_run
//! use jetgraph_client::{Client, EdgeTypeWeight, NodeRef};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = Client::connect("http://localhost:50051").await?;
//!
//!     // Register SIMILAR_TO with 24h TTL and minimal 8-byte payload.
//!     client.register_static_edge_type("SIMILAR_TO", "card", "card", 86400, true).await?;
//!     client.schema().finalize().await?;
//!
//!     // Batch-build with per-type weights.
//!     // LOCATED_IN is required: candidates with zero location similarity are excluded
//!     // even if their overall weighted score is high.
//!     let result = client.build_similarity_graph(
//!         "card",
//!         &[
//!             EdgeTypeWeight::new("TRANSACTS_AT", 0.5),
//!             EdgeTypeWeight::new("USES_DEVICE",  0.3),
//!             EdgeTypeWeight::new("USES_IP",       0.2),
//!         ],
//!         &[],   // no required types for this example
//!         10,    // top-k
//!         0.1,   // min weighted similarity
//!         "SIMILAR_TO",
//!     ).await?;
//!     println!("{} edges created in {} ms", result.edges_created, result.elapsed_ms);
//!
//!     // Per-node real-time query with a required location constraint.
//!     let similar = client.find_similar_nodes(
//!         NodeRef::external("card", "card-01"),
//!         &[
//!             EdgeTypeWeight::new("LOCATED_IN",   1.0),
//!             EdgeTypeWeight::new("HAS_PRICE_RANGE", 0.5),
//!         ],
//!         &["LOCATED_IN"],  // must share at least one location neighbor
//!         5, 0.1, false, "",
//!     ).await?;
//!     for s in &similar.similar_nodes {
//!         println!("  node {} — similarity {:.3}", s.node_id, s.similarity);
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod graph;
pub mod schema;
pub mod features;
pub mod health;
pub mod types;
pub mod segment;
mod error;

pub use error::ClientError;
pub use graph::{GraphClient, IngestSender, IngestResponseStream};
pub use graph::graph_proto::EdgeEvent;
pub use schema::{SchemaClient, GetSchemaResult, NodeTypeInfo, EdgeTypeInfo, RemoveEdgeTypeResult, MemoryUsage};
pub use features::{
    FeatureClient,
    SimilarNodeInfo,
    FindSimilarNodesResult,
    BuildSimilarityGraphResult,
};
pub use types::EdgeTypeWeight;
pub use health::HealthClient;
pub use types::*;
pub use segment::{SegmentClient, EvalContextData, segment_name_from_edge};

/// Property value type for schema registration. Use with [`SchemaClient::register_property`].
pub use schema::schema_proto::ValueType;

use tonic::transport::Channel;
use std::time::Duration;

/// Unified client for all JetGraph services.
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
    /// # use jetgraph_client::Client;
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

    /// Segment operations: prefetch eval context, signal helpers, MEMBER_OF membership.
    pub fn segment(&self) -> SegmentClient {
        SegmentClient::new(self.channel.clone())
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
        bool_property_value: Option<bool>,
    ) -> Result<UpsertEdgeResult, ClientError> {
        self.graph().upsert_edge(edge_type, src, dst, numeric_value, event_ts_secs, bool_property_value).await
    }

    /// Subscribe to the engine's real-time CDC edge-upsert stream.
    /// Pass an empty Vec to receive every edge type.
    pub async fn watch_edge_upserts(
        &self,
        watch_edge_types: Vec<String>,
    ) -> Result<tonic::Streaming<EdgeEvent>, ClientError> {
        self.graph().watch_edge_upserts(watch_edge_types).await
    }

    /// Best-effort transaction ingest: ensure nodes and upsert edges in one call.
    pub async fn ingest_transaction(
        &self,
        transaction_id: Option<&str>,
        nodes: &[TransactionNode],
        edges: &[TransactionEdge],
    ) -> Result<IngestTransactionResult, ClientError> {
        self.graph().ingest_transaction(transaction_id, nodes, edges).await
    }

    /// Open a high-throughput bidirectional streaming ingest session.
    ///
    /// Returns an [`IngestSender`] / [`IngestResponseStream`] pair. The engine
    /// accumulates messages into micro-batches, merges all edges by
    /// `(edge_type, src)` across the batch, and applies each group in a single
    /// pass — yielding significantly higher throughput than calling
    /// `ingest_transaction` repeatedly.
    ///
    /// Send individual transactions with [`IngestSender::send`]; poll responses
    /// with [`IngestResponseStream::next`]. Drop the sender when finished to
    /// signal end-of-input; always drain the response stream so the engine can
    /// flush remaining acknowledgements.
    ///
    /// # Example
    /// ```no_run
    /// # use jetgraph_client::{Client, TransactionNode, TransactionEdge, TransactionNodeRef, NodeRef};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("http://localhost:50051").await?;
    /// let (sender, mut responses) = client.ingest_stream().await?;
    ///
    /// sender.send(Some("txn-001"), &[], &[]).await?;
    /// drop(sender);
    ///
    /// while let Some(result) = responses.next().await {
    ///     let ack = result?;
    ///     println!("nodes_created={} edges_created={}", ack.nodes_created, ack.edges_created);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_stream(&self) -> Result<(IngestSender, IngestResponseStream), ClientError> {
        self.graph().ingest_stream().await
    }

    /// Ingest a transaction and return the raw result.
    ///
    /// After the write completes, extracts the `node_id` of every successfully
    /// ingested node and returns them alongside the transaction result so the
    /// segment evaluator can immediately re-evaluate those nodes without a
    /// separate list call.
    pub async fn ingest_and_get_node_ids(
        &self,
        transaction_id: Option<&str>,
        nodes: &[TransactionNode],
        edges: &[TransactionEdge],
    ) -> Result<(IngestTransactionResult, Vec<u64>), ClientError> {
        let result = self.graph().ingest_transaction(transaction_id, nodes, edges).await?;
        let node_ids: Vec<u64> = result.node_results
            .iter()
            .filter_map(|n| n.node_id)
            .collect();
        Ok((result, node_ids))
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
    ///
    /// - `limit` 0 = unlimited. `cursor` = neighbor_node_id after which to resume (0 = start).
    /// - `neighbor_filters`: server-side predicates on neighbour node properties; pass `&[]` for unfiltered.
    /// - `include_props`: when true, populate `NeighborEdge::neighbor_props` with all node properties.
    ///
    /// Returns `(edges, has_more)`.
    pub async fn get_neighbors(
        &self,
        node: NodeRef,
        edge_type: &str,
        out_neighbors: bool,
        limit: u32,
        cursor: u64,
        neighbor_filters: &[NodePropertyFilter],
        include_props: bool,
    ) -> Result<(Vec<NeighborEdge>, bool), ClientError> {
        self.graph().get_neighbors(
            node, edge_type, out_neighbors, limit, cursor, neighbor_filters, include_props,
        ).await
    }

    // -------------------------------------------------------------------------
    // Similarity shortcuts
    // -------------------------------------------------------------------------

    /// Register a static (minimal-payload) edge type for computed edges such as SIMILAR_TO.
    ///
    /// Uses an 8-byte payload (f32 score + u32 timestamp per pair). No activity bitmap,
    /// no bins, no tx_count. `state_ttl_secs` controls automatic TTL expiry (0 = permanent).
    /// Must be called before `schema().finalize()`.
    /// `symmetric`: when true edges are undirected — requires `from_node_type == to_node_type`.
    pub async fn register_static_edge_type(
        &self,
        name:           &str,
        from_node_type: &str,
        to_node_type:   &str,
        state_ttl_secs: u64,
        symmetric:      bool,
    ) -> Result<u32, ClientError> {
        self.schema().register_static_edge_type(name, from_node_type, to_node_type, state_ttl_secs, symmetric).await
    }

    /// Find the top-k most similar nodes to `node` using weighted per-type Jaccard.
    ///
    /// Each `EdgeTypeWeight` specifies an edge type and its relative importance.
    /// Weights need not sum to 1.0 — the engine normalises internally.
    ///
    /// `required_edge_types`: names of edge types where a non-zero per-type Jaccard
    /// score is mandatory. Candidates scoring 0 on any required type are excluded
    /// regardless of their overall weighted score. Pass `&[]` for no hard constraints.
    ///
    /// `bool_property_weights`: boolean node properties treated as virtual edges.
    /// A node with the property `true` scores Jaccard 1.0 when the candidate also has
    /// it `true`. Pass `&[]` to disable. See [`BoolPropertyWeight`].
    ///
    /// `required_bool_properties`: boolean property names that must score 1.0 for a
    /// candidate to be returned. Pass `&[]` for no constraints.
    ///
    /// If `upsert_edges` is true, the engine writes SIMILAR_TO edges
    /// (must exist with `minimal_payload=true`).
    pub async fn find_similar_nodes(
        &self,
        node:                    NodeRef,
        weighted_edge_types:     &[EdgeTypeWeight],
        required_edge_types:     &[&str],
        bool_property_weights:   &[BoolPropertyWeight],
        required_bool_properties: &[&str],
        k:                       u32,
        min_similarity:          f32,
        upsert_edges:            bool,
        similar_to_edge_type:    &str,
    ) -> Result<FindSimilarNodesResult, ClientError> {
        let mut features = self.features();
        features.find_similar_nodes(
            node, weighted_edge_types, required_edge_types,
            bool_property_weights, required_bool_properties,
            k, min_similarity, upsert_edges, similar_to_edge_type,
        ).await
    }

    /// Batch-build SIMILAR_TO edges for all nodes of `node_type` in parallel.
    ///
    /// Iterates every node, computes weighted per-type Jaccard similarity via
    /// shared neighbors, and upserts the top-k results into `similar_to_edge_type`
    /// (must be registered with `minimal_payload=true`).
    ///
    /// `required_edge_types`: names of edge types where a non-zero per-type Jaccard
    /// score is mandatory. Pass `&[]` for no hard constraints.
    ///
    /// `bool_property_weights`: boolean node properties treated as virtual edges.
    /// See [`BoolPropertyWeight`] and [`Client::find_similar_nodes`] for details.
    ///
    /// `required_bool_properties`: boolean property names that must score 1.0
    /// for a candidate to be linked. Pass `&[]` for no constraints.
    pub async fn build_similarity_graph(
        &self,
        node_type:               &str,
        weighted_edge_types:     &[EdgeTypeWeight],
        required_edge_types:     &[&str],
        bool_property_weights:   &[BoolPropertyWeight],
        required_bool_properties: &[&str],
        k:                       u32,
        min_similarity:          f32,
        similar_to_edge_type:    &str,
    ) -> Result<BuildSimilarityGraphResult, ClientError> {
        let mut features = self.features();
        features.build_similarity_graph(
            node_type, weighted_edge_types, required_edge_types,
            bool_property_weights, required_bool_properties,
            k, min_similarity, similar_to_edge_type,
        ).await
    }

    /// Delete all stored edges of the given edge type while keeping the type definition intact.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example(client: jetgraph_client::Client) -> anyhow::Result<()> {
    /// let removed = client.clear_edge_type_data("SIMILAR_TO").await?;
    /// println!("{removed} pairs removed");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_edge_type_data(&self, edge_type_name: &str) -> Result<u64, ClientError> {
        let mut features = self.features();
        features.clear_edge_type_data(edge_type_name).await
    }

    /// Remove an edge type from the engine schema **and** drop all its stored edge data.
    ///
    /// Unlike [`clear_edge_type_data`](Self::clear_edge_type_data), this also removes
    /// the type definition from the schema so that the name can be re-registered later.
    /// The operation is destructive and irreversible.
    ///
    /// Returns a [`RemoveEdgeTypeResult`] containing the numeric ID and how many pairs
    /// were dropped. Returns an error if the edge type name is not found in the schema.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example(client: jetgraph_client::Client) -> anyhow::Result<()> {
    /// let result = client.remove_edge_type("NEXT_EFT").await?;
    /// println!("removed id={} pairs={}", result.edge_type_id, result.pairs_dropped);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_edge_type(&self, name: &str) -> Result<RemoveEdgeTypeResult, ClientError> {
        self.schema().remove_edge_type(name).await
    }

    /// Query current memory usage across nodes, edges, histograms, and runtime overhead.
    ///
    /// Returns a [`MemoryUsage`] with per-category byte counts and a human-readable
    /// breakdown string. Useful for capacity planning and debugging memory growth.
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example(client: jetgraph_client::Client) -> anyhow::Result<()> {
    /// let mem = client.memory_usage().await?;
    /// println!("total={} MB", mem.total_bytes / 1_048_576);
    /// println!("{}", mem.breakdown_text);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn memory_usage(&self) -> Result<MemoryUsage, ClientError> {
        self.schema().get_memory_usage().await
    }
}
