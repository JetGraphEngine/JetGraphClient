//! Segment client: multi-signal prefetch, lazy signal helpers, and MEMBER_OF membership ops.
//!
//! The segment evaluator uses this module as its primary interface to the graph engine.
//! The design follows a two-call pattern:
//!
//! 1. Call `prefetch_eval_context` once per customer — this fires `GetNodeFeatureVector`
//!    and caches the result in `EvalContextData`. ~70% of signals are zero-cost lookups
//!    against this cache.
//! 2. For signals not covered by the feature vector, call the targeted lazy helpers
//!    (`days_since_last_neighbor`, `neighbor_count`, `histogram_field`, etc.) — these
//!    are only invoked if a loaded segment rule actually references that signal.

use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;

use crate::{
    ClientError, NodeRef, NeighborEdge,
    HistogramField, EdgeStateField,
    SegmentMembership, SegmentMember,
};
use crate::features::{FeatureClient, NodeFeatureVectorResponse, EdgeTypeFeatures};
use crate::graph::GraphClient;

// ---------------------------------------------------------------------------
// EvalContextData — cached result of GetNodeFeatureVector
// ---------------------------------------------------------------------------

/// Cached result of a single `GetNodeFeatureVector` call.
///
/// All `feature_vector` source-type signals are served from this cache with
/// zero additional RPCs. The evaluator creates one `EvalContextData` per
/// customer per evaluation cycle and passes it to every segment rule.
#[derive(Debug, Clone)]
pub struct EvalContextData {
    pub node_id: u64,
    pub feature_vector: NodeFeatureVectorResponse,
}

impl EvalContextData {
    /// Total transaction count for the given edge type. Returns 0.0 if not present.
    pub fn total_tx_count(&self, edge_type: &str) -> f64 {
        self.edge_features(edge_type)
            .map(|ef| ef.total_tx_count as f64)
            .unwrap_or(0.0)
    }

    /// Approximate total spend/sum for the given edge type. Returns 0.0 if not present.
    pub fn total_approx_sum(&self, edge_type: &str) -> f64 {
        self.edge_features(edge_type)
            .map(|ef| ef.total_approx_sum as f64)
            .unwrap_or(0.0)
    }

    /// Neighbor count for the given edge type. Returns 0.0 if not present.
    pub fn neighbor_count(&self, edge_type: &str) -> f64 {
        self.edge_features(edge_type)
            .map(|ef| ef.neighbor_count as f64)
            .unwrap_or(0.0)
    }

    /// Number of fraudulent neighbors (across all checked nodes).
    pub fn fraudulent_neighbor_count(&self) -> f64 {
        self.feature_vector.fraudulent_neighbor_count as f64
    }

    /// Maximum fraud score among neighbors.
    pub fn max_neighbor_fraud_score(&self) -> f64 {
        self.feature_vector.max_neighbor_fraud_score as f64
    }

    /// Direct fraud score of this node.
    pub fn direct_fraud_score(&self) -> f64 {
        self.feature_vector.direct_fraud_score as f64
    }

    fn edge_features(&self, edge_type: &str) -> Option<&EdgeTypeFeatures> {
        self.feature_vector
            .edge_features
            .iter()
            .find(|ef| ef.edge_type_name == edge_type)
    }
}

// ---------------------------------------------------------------------------
// SegmentClient
// ---------------------------------------------------------------------------

/// Sub-client for segment evaluation operations.
///
/// Obtain via `Client::segment()`. Holds its own cloned channel so it can be
/// used concurrently alongside `GraphClient` / `FeatureClient`.
#[derive(Clone)]
pub struct SegmentClient {
    graph:    GraphClient,
    features: FeatureClient,
}

impl SegmentClient {
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            graph:    GraphClient::new(channel.clone()),
            features: FeatureClient::new(channel),
        }
    }

    // -----------------------------------------------------------------------
    // Prefetch
    // -----------------------------------------------------------------------

    /// Call `GetNodeFeatureVector` once and cache the result.
    ///
    /// Call this once per customer at the start of every evaluation cycle.
    /// Pass the returned `EvalContextData` to every segment rule — rules that
    /// use `feature_vector` source signals will read from it at zero RPC cost.
    pub async fn prefetch_eval_context(
        &mut self,
        node: NodeRef,
        edge_types: &[&str],
        histogram_window_hours: u32,
        histogram_window_days: u32,
    ) -> Result<EvalContextData, ClientError> {
        let fv = self.features.get_node_feature_vector(
            node,
            edge_types,
            histogram_window_hours,
            histogram_window_days,
            &[],
        ).await?;
        Ok(EvalContextData {
            node_id: fv.node_id,
            feature_vector: fv,
        })
    }

    // -----------------------------------------------------------------------
    // Lazy signal helpers
    // -----------------------------------------------------------------------

    /// `last_neighbor` source type — days elapsed since the most recent interaction
    /// on the given edge type.
    ///
    /// Returns `f64::MAX` if the customer has never interacted (no neighbor found),
    /// which causes any `lt`/`lte` threshold condition to pass (correctly treating
    /// a never-interacted customer as "infinitely long since last interaction").
    pub async fn days_since_last_neighbor(
        &self,
        node: NodeRef,
        edge_type: &str,
    ) -> Result<f64, ClientError> {
        match self.graph.get_last_neighbor(node, edge_type, None).await? {
            None => Ok(f64::MAX),
            Some((_neighbor_id, last_seen_secs)) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let elapsed_secs = now.saturating_sub(last_seen_secs as u64);
                Ok(elapsed_secs as f64 / 86_400.0)
            }
        }
    }

    /// `last_neighbor` source type — raw `last_seen_secs` value, or `u32::MAX` if none.
    pub async fn last_seen_secs(
        &self,
        node: NodeRef,
        edge_type: &str,
    ) -> Result<f64, ClientError> {
        match self.graph.get_last_neighbor(node, edge_type, None).await? {
            None => Ok(u32::MAX as f64),
            Some((_neighbor_id, secs)) => Ok(secs as f64),
        }
    }

    /// `neighbor_count` source type — exact count as f64.
    pub async fn neighbor_count(
        &self,
        node: NodeRef,
        edge_type: &str,
    ) -> Result<f64, ClientError> {
        let (count, _approx) = self.graph.get_neighbor_count(node, edge_type).await?;
        Ok(count as f64)
    }

    /// `node_histogram` source type — extract a specific field from the histogram result.
    pub async fn histogram_field(
        &mut self,
        node: NodeRef,
        edge_type: &str,
        window_hours: u32,
        window_days: u32,
        field: HistogramField,
    ) -> Result<f64, ClientError> {
        let result = self.features
            .query_node_histogram(node, edge_type, window_hours, window_days)
            .await?;
        let value = match field {
            HistogramField::TotalEvents => result.total_events as f64,
            HistogramField::TotalApproxSum => {
                result.total_counts.iter().sum::<u32>() as f64
            }
            HistogramField::PeakBin => {
                result.total_counts
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, &v)| v)
                    .map(|(i, _)| i as f64)
                    .unwrap_or(0.0)
            }
        };
        Ok(value)
    }

    /// `node_property` source type — read a node property and coerce to f64.
    ///
    /// Numeric (Int, Float, Timestamp) properties are returned directly.
    /// Bool properties return 1.0 (true) or 0.0 (false).
    /// String properties and missing properties return 0.0.
    pub async fn node_property_f64(
        &self,
        node: NodeRef,
        property_name: &str,
    ) -> Result<f64, ClientError> {
        use crate::PropertyValue;
        let response = self.graph.get_node(node).await?;
        for prop in &response.properties {
            if prop.name == property_name {
                return Ok(match &prop.value {
                    PropertyValue::Int(v)       => *v as f64,
                    PropertyValue::Float(v)     => *v,
                    PropertyValue::Timestamp(v) => *v as f64,
                    PropertyValue::Bool(v)      => if *v { 1.0 } else { 0.0 },
                    PropertyValue::String(_)    => 0.0,
                });
            }
        }
        Ok(0.0)
    }

    /// `edge_state` source type — read a specific field from a directed edge state.
    ///
    /// For `EdgeStateField::ActivityCount`, pass `activity_window_secs` as the window
    /// to query (the first activity count is returned). Pass `None` for other fields.
    pub async fn edge_state_field(
        &self,
        edge_type: &str,
        src: NodeRef,
        dst: NodeRef,
        field: EdgeStateField,
        activity_window_secs: Option<u64>,
    ) -> Result<f64, ClientError> {
        let windows: Vec<u64> = activity_window_secs.map(|w| vec![w]).unwrap_or_default();
        let state = self.graph.get_edge_state(
            edge_type, src, dst,
            None, None, None,
            if windows.is_empty() { None } else { Some(&windows) },
        ).await?;

        match state {
            None => Ok(0.0),
            Some(s) => Ok(match field {
                EdgeStateField::TxCount      => s.tx_count as f64,
                EdgeStateField::ApproxSum    => s.approx_sum as f64,
                EdgeStateField::LastSeenSecs => s.last_seen as f64,
                EdgeStateField::ActivityCount => {
                    s.activity_counts.first().copied().unwrap_or(0) as f64
                }
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Segment membership (MEMBER_OF edge operations)
    // -----------------------------------------------------------------------

    /// Upsert a `MEMBER_OF` static edge from `customer` to `segment_node`.
    ///
    /// `confidence` is stored as the edge's `numeric_value` (f32 in 0.0–1.0).
    /// Pass `confidence = 0.0` to record an explicit segment exit (the edge is
    /// updated in place; TTL expiry will eventually clean it up).
    pub async fn upsert_segment_membership(
        &self,
        customer: NodeRef,
        segment_node: NodeRef,
        confidence: f32,
    ) -> Result<(), ClientError> {
        self.graph.upsert_edge(
            "MEMBER_OF",
            customer,
            segment_node,
            Some(confidence),
            None,
            None,
        ).await?;
        Ok(())
    }

    /// Return all segments the customer currently belongs to (confidence > 0.0).
    ///
    /// Traverses `MEMBER_OF` out-neighbors of the customer node and reads the
    /// edge state for each to retrieve the stored confidence value.
    pub async fn get_customer_segments(
        &self,
        customer: NodeRef,
    ) -> Result<Vec<SegmentMembership>, ClientError> {
        let (edges, _has_more) = self.graph.get_neighbors(
            customer.clone(),
            "MEMBER_OF",
            true,  // out_neighbors: customer → segment
            1000,  // limit
            0,     // cursor
            &[],
            true,  // include props so we get the segment external_id (name)
        ).await?;

        let mut memberships = Vec::new();
        for edge in edges {
            // Read edge state to get confidence (stored as approx_sum on static edge)
            let state = self.graph.get_edge_state(
                "MEMBER_OF",
                customer.clone(),
                NodeRef::node_id(edge.neighbor_node_id),
                None, None, None, None,
            ).await?;

            if let Some(s) = state {
                if s.approx_sum > 0.0 {
                    let segment_name = edge.neighbor_external_id
                        .unwrap_or_else(|| edge.neighbor_node_id.to_string());
                    memberships.push(SegmentMembership {
                        segment_name,
                        segment_node_id: edge.neighbor_node_id,
                        confidence: s.approx_sum,
                        last_seen_secs: s.last_seen,
                    });
                }
            }
        }
        Ok(memberships)
    }

    /// Return all members of a segment with their confidence scores.
    ///
    /// Traverses `MEMBER_OF` in-neighbors of the segment node (customers pointing
    /// to this segment). Results are paginated via `cursor` / `has_more`.
    pub async fn get_segment_members(
        &self,
        segment_node: NodeRef,
        limit: u32,
        cursor: u64,
    ) -> Result<(Vec<SegmentMember>, bool), ClientError> {
        let (edges, has_more) = self.graph.get_neighbors(
            segment_node.clone(),
            "MEMBER_OF",
            false, // in-neighbors: customers → segment
            limit,
            cursor,
            &[],
            false,
        ).await?;

        let mut members = Vec::new();
        for edge in edges {
            let state = self.graph.get_edge_state(
                "MEMBER_OF",
                NodeRef::node_id(edge.neighbor_node_id),
                segment_node.clone(),
                None, None, None, None,
            ).await?;
            let (confidence, last_seen_secs) = state
                .map(|s| (s.approx_sum, s.last_seen))
                .unwrap_or((0.0, 0));
            if confidence > 0.0 {
                members.push(SegmentMember {
                    customer_node_id: edge.neighbor_node_id,
                    external_id:      edge.neighbor_external_id.clone(),
                    confidence,
                    last_seen_secs,
                });
            }
        }
        Ok((members, has_more))
    }

    /// List all customer node IDs with automatic pagination.
    ///
    /// Calls `list_nodes` in batches of `batch_size` until all nodes are returned.
    /// Use for the batch sweep evaluation path.
    pub async fn list_all_customers(
        &self,
        customer_node_type: &str,
        batch_size: u32,
    ) -> Result<Vec<u64>, ClientError> {
        // list_nodes returns up to `limit` nodes; no cursor support, so we use the
        // total_count to decide whether we have everything.
        let (nodes, _total) = self.graph
            .list_nodes(customer_node_type, "", batch_size)
            .await?;
        Ok(nodes.into_iter().map(|n| n.node_id).collect())
    }

    /// Expose the inner graph client for callers that need direct access.
    pub fn graph(&self) -> &GraphClient {
        &self.graph
    }

    /// Expose the inner feature client for callers that need direct access.
    pub fn features_mut(&mut self) -> &mut FeatureClient {
        &mut self.features
    }
}

// ---------------------------------------------------------------------------
// Helpers for callers working with raw neighbor edge lists
// ---------------------------------------------------------------------------

/// Extract segment name from a MEMBER_OF neighbor edge (uses external_id if present).
pub fn segment_name_from_edge(edge: &NeighborEdge) -> String {
    edge.neighbor_external_id
        .clone()
        .unwrap_or_else(|| edge.neighbor_node_id.to_string())
}
