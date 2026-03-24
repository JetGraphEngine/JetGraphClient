//! Feature service client: histograms, fraud context, flagging.

use tonic::transport::Channel;
use crate::{ClientError, NodeRef};

pub mod features_proto {
    tonic::include_proto!("features");
}

use features_proto::feature_service_client::FeatureServiceClient;

fn node_ref_to_proto(r: &NodeRef) -> features_proto::NodeRef {
    match r {
        NodeRef::NodeId(id) => features_proto::NodeRef {
            identifier: Some(features_proto::node_ref::Identifier::NodeId(*id)),
        },
        NodeRef::External { node_type, external_id } => features_proto::NodeRef {
            identifier: Some(features_proto::node_ref::Identifier::External(features_proto::ExternalRef {
                node_type_name: node_type.clone(),
                external_id: external_id.clone(),
            })),
        },
    }
}

#[derive(Clone)]
pub struct FeatureClient {
    client: FeatureServiceClient<Channel>,
}

impl FeatureClient {
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            client: FeatureServiceClient::new(channel),
        }
    }

    /// Query node histogram (compact edge type).
    pub async fn query_node_histogram(
        &mut self,
        node: NodeRef,
        edge_type_name: &str,
        window_hours: u32,
        window_days: u32,
    ) -> Result<NodeHistogramResult, ClientError> {
        let req = features_proto::NodeHistogramQuery {
            node: Some(node_ref_to_proto(&node)),
            edge_type_name: edge_type_name.to_string(),
            window_hours,
            window_days,
            include_buckets: false,
        };
        let r = self.client.query_node_histogram(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(NodeHistogramResult {
            total_events: inner.total_events,
            total_counts: inner.totals.map(|t| t.counts).unwrap_or_default(),
            window_covered_secs: inner.window_covered_secs,
        })
    }

    /// Get combined feature vector for a node.
    /// `nodes_to_check_fraud`: transaction context (e.g. merchant, device, IP) to check for fraud.
    pub async fn get_node_feature_vector(
        &mut self,
        node: NodeRef,
        edge_types: &[&str],
        histogram_window_hours: u32,
        histogram_window_days: u32,
        nodes_to_check_fraud: &[NodeRef],
    ) -> Result<NodeFeatureVectorResponse, ClientError> {
        let req = features_proto::NodeFeatureVectorRequest {
            node: Some(node_ref_to_proto(&node)),
            edge_types: edge_types.iter().map(|s| (*s).to_string()).collect(),
            histogram_window_hours,
            histogram_window_days,
            nodes_to_check_fraud: nodes_to_check_fraud
                .iter()
                .map(|n| node_ref_to_proto(n))
                .collect(),
        };
        let r = self.client.get_node_feature_vector(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(NodeFeatureVectorResponse {
            node_id: inner.node_id,
            edge_features: inner.edge_features.into_iter().map(|ef| EdgeTypeFeatures {
                edge_type_name: ef.edge_type_name,
                neighbor_count: ef.neighbor_count,
                activity_bitmap_union: ef.activity_bitmap_union,
                total_approx_sum: ef.total_approx_sum,
                total_tx_count: ef.total_tx_count,
            }).collect(),
            direct_fraud_score: inner.direct_fraud_score,
            fraudulent_neighbor_count: inner.fraudulent_neighbor_count,
            max_neighbor_fraud_score: inner.max_neighbor_fraud_score,
        })
    }

    /// Check which of the given nodes have an edge to FRAUD.
    /// Returns `(node_id, fraud_score, reason)` for each flagged node.
    pub async fn get_fraud_context(
        &mut self,
        nodes: &[NodeRef],
    ) -> Result<FraudContext, ClientError> {
        let req = features_proto::FraudContextQuery {
            nodes: nodes.iter().map(|n| node_ref_to_proto(n)).collect(),
        };
        let r = self.client.get_fraud_context(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(FraudContext {
            flagged_nodes: inner
                .flagged_nodes
                .into_iter()
                .map(|n| NodeFraudInfo {
                    node_id: n.node_id,
                    fraud_score: n.fraud_score,
                    reason: n.reason,
                })
                .collect(),
        })
    }

    /// Alias for `get_fraud_context`. Check which of the given nodes have an edge to FRAUD.
    pub async fn nodes_with_fraud_edge(
        &mut self,
        nodes: &[NodeRef],
    ) -> Result<FraudContext, ClientError> {
        self.get_fraud_context(nodes).await
    }

    /// Flag a node as fraudulent.
    pub async fn flag_node(
        &mut self,
        node: NodeRef,
        fraud_score: f32,
        reason: &str,
    ) -> Result<(), ClientError> {
        let req = features_proto::FlagRequest {
            node: Some(node_ref_to_proto(&node)),
            fraud_score,
            reason: reason.to_string(),
        };
        self.client.flag_node(req).await.map_err(ClientError::from)?;
        Ok(())
    }

    /// Remove fraud flag from a node.
    pub async fn unflag_node(&mut self, node: NodeRef) -> Result<(), ClientError> {
        let req = features_proto::UnflagRequest {
            node: Some(node_ref_to_proto(&node)),
        };
        self.client.unflag_node(req).await.map_err(ClientError::from)?;
        Ok(())
    }

    /// Find the top-k most similar nodes to `node` using shared-neighbor Jaccard.
    ///
    /// `edge_types` determines which edge types are used to discover co-neighbors.
    /// If `upsert_edges` is true, the engine will write SIMILAR_TO edges with the
    /// Jaccard score into `similar_to_edge_type` (must exist with minimal_payload=true).
    pub async fn find_similar_nodes(
        &mut self,
        node:                 NodeRef,
        edge_types:           &[&str],
        k:                    u32,
        min_similarity:       f32,
        upsert_edges:         bool,
        similar_to_edge_type: &str,
    ) -> Result<FindSimilarNodesResult, ClientError> {
        let req = features_proto::FindSimilarNodesRequest {
            node: Some(node_ref_to_proto(&node)),
            edge_types: edge_types.iter().map(|s| (*s).to_string()).collect(),
            k,
            min_similarity,
            upsert_edges,
            similar_to_edge_type: similar_to_edge_type.to_string(),
        };
        let r = self.client.find_similar_nodes(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(FindSimilarNodesResult {
            query_node_id: inner.query_node_id,
            similar_nodes: inner.similar_nodes.into_iter().map(|sn| SimilarNodeInfo {
                node_id:          sn.node_id,
                similarity:       sn.similarity,
                shared_neighbors: sn.shared_neighbors,
            }).collect(),
        })
    }

    /// Batch-build SIMILAR_TO edges for all nodes of `node_type`.
    ///
    /// Iterates every node of the given type, computes Jaccard similarity against
    /// co-neighbors, and upserts the top-k results as edges of `similar_to_edge_type`
    /// (must be registered with minimal_payload=true).
    pub async fn build_similarity_graph(
        &mut self,
        node_type:            &str,
        edge_types:           &[&str],
        k:                    u32,
        min_similarity:       f32,
        similar_to_edge_type: &str,
    ) -> Result<BuildSimilarityGraphResult, ClientError> {
        let req = features_proto::BuildSimilarityGraphRequest {
            node_type:            node_type.to_string(),
            edge_types:           edge_types.iter().map(|s| (*s).to_string()).collect(),
            k,
            min_similarity,
            similar_to_edge_type: similar_to_edge_type.to_string(),
        };
        let r = self.client.build_similarity_graph(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(BuildSimilarityGraphResult {
            nodes_processed: inner.nodes_processed,
            edges_created:   inner.edges_created,
            edges_updated:   inner.edges_updated,
            elapsed_ms:      inner.elapsed_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NodeHistogramResult {
    pub total_events: u32,
    pub total_counts: Vec<u32>,
    pub window_covered_secs: u64,
}

#[derive(Debug, Clone)]
pub struct NodeFeatureVectorResponse {
    pub node_id: u64,
    pub edge_features: Vec<EdgeTypeFeatures>,
    pub direct_fraud_score: f32,
    pub fraudulent_neighbor_count: u32,
    pub max_neighbor_fraud_score: f32,
}

#[derive(Debug, Clone)]
pub struct EdgeTypeFeatures {
    pub edge_type_name: String,
    pub neighbor_count: u64,
    /// Union of raw activity bitmap values across all neighbors of this type.
    pub activity_bitmap_union: u64,
    pub total_approx_sum: f32,
    pub total_tx_count: u32,
}

#[derive(Debug, Clone)]
pub struct NodeFraudInfo {
    pub node_id: u64,
    pub fraud_score: f32,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct FraudContext {
    pub flagged_nodes: Vec<NodeFraudInfo>,
}

#[derive(Debug, Clone)]
pub struct SimilarNodeInfo {
    pub node_id:          u64,
    pub similarity:       f32,
    pub shared_neighbors: u32,
}

#[derive(Debug, Clone)]
pub struct FindSimilarNodesResult {
    pub query_node_id: u64,
    pub similar_nodes: Vec<SimilarNodeInfo>,
}

#[derive(Debug, Clone)]
pub struct BuildSimilarityGraphResult {
    pub nodes_processed: u64,
    pub edges_created:   u64,
    pub edges_updated:   u64,
    pub elapsed_ms:      u64,
}
