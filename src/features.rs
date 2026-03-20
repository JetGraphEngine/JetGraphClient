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
                active_flags: ef.active_flags,
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
    pub active_flags: Vec<String>,
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
