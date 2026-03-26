//! Graph service client: nodes, edges, neighbors.

use tonic::transport::Channel;
use crate::{ClientError, NodeRef, PropertyEntry, PropertyValue, UpsertEdgeResult, EdgeState, NeighborEdge, CreateNodeResult, NodePropertyFilter, NodePropPredicate};

pub mod graph_proto {
    tonic::include_proto!("graph");
}

use graph_proto::{
    graph_service_client::GraphServiceClient,
    node_ref::Identifier,
};

/// Decode 16 bytes (8 × little-endian u16) from the proto `bins` field into `[u16; 8]`.
/// Gracefully returns all-zeros if the server sent fewer bytes (e.g. a slim/non-numeric edge).
fn decode_bins(bytes: &[u8]) -> [u16; 8] {
    let mut out = [0u16; 8];
    for (i, chunk) in bytes.chunks(2).enumerate().take(8) {
        out[i] = match chunk {
            [lo, hi] => u16::from_le_bytes([*lo, *hi]),
            [lo]     => *lo as u16,
            _        => 0,
        };
    }
    out
}

#[derive(Clone)]
pub struct GraphClient {
    client: GraphServiceClient<Channel>,
}

impl GraphClient {
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            client: GraphServiceClient::new(channel)
                .max_decoding_message_size(32 * 1024 * 1024)
                .max_encoding_message_size(32 * 1024 * 1024),
        }
    }

    fn node_ref_to_proto(r: &NodeRef) -> graph_proto::NodeRef {
        match r {
            NodeRef::NodeId(id) => graph_proto::NodeRef {
                identifier: Some(Identifier::NodeId(*id)),
            },
            NodeRef::External { node_type, external_id } => graph_proto::NodeRef {
                identifier: Some(Identifier::External(graph_proto::ExternalRef {
                    node_type_name: node_type.clone(),
                    external_id: external_id.clone(),
                })),
            },
        }
    }

    fn property_to_proto(p: &PropertyEntry) -> graph_proto::PropertyEntry {
        let value = match &p.value {
            PropertyValue::Int(v) => graph_proto::PropertyValue { value: Some(graph_proto::property_value::Value::IntVal(*v)) },
            PropertyValue::Float(v) => graph_proto::PropertyValue { value: Some(graph_proto::property_value::Value::FloatVal(*v)) },
            PropertyValue::String(v) => graph_proto::PropertyValue { value: Some(graph_proto::property_value::Value::StringVal(v.clone())) },
            PropertyValue::Bool(v) => graph_proto::PropertyValue { value: Some(graph_proto::property_value::Value::BoolVal(*v)) },
            PropertyValue::Timestamp(v) => graph_proto::PropertyValue { value: Some(graph_proto::property_value::Value::TimestampVal(*v)) },
        };
        graph_proto::PropertyEntry {
            name: p.name.clone(),
            value: Some(value),
        }
    }

    /// Create a node. With external_id, idempotent (no lookup, content-addressable).
    /// Returns (node_id, created).
    pub async fn create_node(
        &self,
        node_type: &str,
        external_id: Option<&str>,
        properties: &[PropertyEntry],
    ) -> Result<CreateNodeResult, ClientError> {
        let req = graph_proto::CreateNodeRequest {
            node_type_name: node_type.to_string(),
            external_id: external_id.map(String::from),
            properties: properties.iter().map(Self::property_to_proto).collect(),
        };
        let r = self.client.clone().create_node(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(CreateNodeResult {
            node_id: inner.node_id,
            created: inner.created,
        })
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
        let req = graph_proto::UpsertEdgeRequest {
            edge_type_name: edge_type.to_string(),
            src: Some(Self::node_ref_to_proto(&src)),
            dst: Some(Self::node_ref_to_proto(&dst)),
            numeric_value,
            event_ts_secs,
        };
        let r = self.client.clone().upsert_edge(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        let payload = inner.payload.ok_or_else(|| ClientError::Internal("missing payload".into()))?;
        let bins = decode_bins(&payload.bins);
        Ok(UpsertEdgeResult {
            created_new: inner.created_new,
            tx_count: payload.tx_count,
            approx_sum: payload.approx_sum,
            last_seen: payload.last_seen,
            activity_bitmap_raw: payload.flags,
            bins,
        })
    }

    /// Get edge state. Returns None if not found.
    pub async fn get_edge_state(
        &self,
        edge_type: &str,
        src: NodeRef,
        dst: NodeRef,
        min_value: Option<f32>,
        max_value: Option<f32>,
        query_time_secs: Option<u32>,
        activity_windows_secs: Option<&[u64]>,
    ) -> Result<Option<EdgeState>, ClientError> {
        let req = graph_proto::GetEdgeStateRequest {
            edge_type_name: edge_type.to_string(),
            src: Some(Self::node_ref_to_proto(&src)),
            dst: Some(Self::node_ref_to_proto(&dst)),
            min_value,
            max_value,
            query_time_secs,
            activity_windows_secs: activity_windows_secs.map(|s| s.to_vec()).unwrap_or_default(),
        };
        let r = self.client.clone().get_edge_state(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        if !inner.found {
            return Ok(None);
        }
        let payload = inner.payload.ok_or_else(|| ClientError::Internal("missing payload".into()))?;
        let bins = decode_bins(&payload.bins);
        Ok(Some(EdgeState {
            found: true,
            tx_count: payload.tx_count,
            approx_sum: payload.approx_sum,
            last_seen: payload.last_seen,
            activity_bitmap_raw: payload.flags,
            bins,
            filtered_count: inner.filtered_count,
            filtered_approx_sum: inner.filtered_approx_sum,
            activity_counts: inner.activity_counts,
        }))
    }

    /// Get neighbors of a node.
    ///
    /// - `limit` 0 = unlimited. `cursor` = neighbor_node_id after which to resume (0 = start).
    /// - `neighbor_filters`: server-side predicates on neighbour node properties; pass `&[]` for unfiltered.
    /// - `include_props`: when true, populate `NeighborEdge::neighbor_props` with all node properties.
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
        let proto_filters = neighbor_filters.iter().map(Self::filter_to_proto).collect();
        let req = graph_proto::NeighborQuery {
            node: Some(Self::node_ref_to_proto(&node)),
            edge_type: edge_type.to_string(),
            out_neighbors,
            filter: None,
            limit,
            cursor,
            neighbor_filters: proto_filters,
            include_neighbor_props: include_props,
        };
        let r = self.client.clone().get_neighbors(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        let edges: Vec<NeighborEdge> = inner.edges
            .into_iter()
            .map(|e| {
                let neighbor_props = e.neighbor_props.into_iter().map(|p| {
                    let value = match p.value.and_then(|v| v.value) {
                        Some(graph_proto::property_value::Value::IntVal(x))       => PropertyValue::Int(x),
                        Some(graph_proto::property_value::Value::FloatVal(x))     => PropertyValue::Float(x),
                        Some(graph_proto::property_value::Value::StringVal(x))    => PropertyValue::String(x),
                        Some(graph_proto::property_value::Value::BoolVal(x))      => PropertyValue::Bool(x),
                        Some(graph_proto::property_value::Value::TimestampVal(x)) => PropertyValue::Timestamp(x),
                        _ => PropertyValue::Int(0),
                    };
                    PropertyEntry { name: p.name, value }
                }).collect();
                NeighborEdge {
                    neighbor_node_id:     e.neighbor_node_id,
                    edge_id:              e.edge_id,
                    created_at_us:        e.created_at_us,
                    neighbor_node_type:   e.neighbor_node_type,
                    neighbor_external_id: e.neighbor_external_id,
                    neighbor_props,
                }
            })
            .collect();
        Ok((edges, inner.has_more))
    }

    fn filter_to_proto(f: &NodePropertyFilter) -> graph_proto::PropertyPredicate {
        use graph_proto::property_predicate::Predicate;
        let predicate = Some(match &f.predicate {
            NodePropPredicate::IntGt(v)    => Predicate::IntGt(*v),
            NodePropPredicate::IntLt(v)    => Predicate::IntLt(*v),
            NodePropPredicate::IntEq(v)    => Predicate::IntEq(*v),
            NodePropPredicate::FloatGt(v)  => Predicate::FloatGt(*v),
            NodePropPredicate::FloatLt(v)  => Predicate::FloatLt(*v),
            NodePropPredicate::TsAfter(v)  => Predicate::TsAfter(*v),
            NodePropPredicate::TsBefore(v) => Predicate::TsBefore(*v),
            NodePropPredicate::StringEq(v) => Predicate::StringEq(v.clone()),
            NodePropPredicate::BoolEq(v)   => Predicate::BoolEq(*v),
        });
        graph_proto::PropertyPredicate {
            property_name: f.property.clone(),
            predicate,
        }
    }

    /// Get exact neighbor count.
    pub async fn get_neighbor_count(
        &self,
        node: NodeRef,
        edge_type: &str,
    ) -> Result<(u64, bool), ClientError> {
        let req = graph_proto::CountQuery {
            node: Some(Self::node_ref_to_proto(&node)),
            edge_type: edge_type.to_string(),
        };
        let r = self.client.clone().get_neighbor_count(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok((inner.count, inner.approximate))
    }

    /// Get neighbor with max last_seen (e.g. for impossible-travel).
    pub async fn get_last_neighbor(
        &self,
        node: NodeRef,
        edge_type: &str,
        exclude_neighbor: Option<NodeRef>,
    ) -> Result<Option<(u64, u32)>, ClientError> {
        let req = graph_proto::LastNeighborQuery {
            node: Some(Self::node_ref_to_proto(&node)),
            edge_type: edge_type.to_string(),
            exclude_neighbor: exclude_neighbor.map(|n| Self::node_ref_to_proto(&n)),
        };
        let r = self.client.clone().get_last_neighbor(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        if !inner.found {
            return Ok(None);
        }
        Ok(Some((inner.neighbor_node_id, inner.last_seen_secs)))
    }

    /// Get node by ref.
    pub async fn get_node(&self, node: NodeRef) -> Result<NodeResponse, ClientError> {
        let req = graph_proto::GetNodeRequest {
            node: Some(Self::node_ref_to_proto(&node)),
        };
        let r = self.client.clone().get_node(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(NodeResponse {
            node_id: inner.node_id,
            node_type: inner.node_type,
            external_id: inner.external_id,
            properties: inner.properties.into_iter().map(|p| {
                let value = match p.value.and_then(|v| v.value) {
                    Some(graph_proto::property_value::Value::IntVal(x)) => PropertyValue::Int(x),
                    Some(graph_proto::property_value::Value::FloatVal(x)) => PropertyValue::Float(x),
                    Some(graph_proto::property_value::Value::StringVal(x)) => PropertyValue::String(x),
                    Some(graph_proto::property_value::Value::BoolVal(x)) => PropertyValue::Bool(x),
                    Some(graph_proto::property_value::Value::TimestampVal(x)) => PropertyValue::Timestamp(x),
                    _ => PropertyValue::Int(0),
                };
                PropertyEntry { name: p.name, value }
            }).collect(),
        })
    }

    /// List nodes with optional filters.
    pub async fn list_nodes(
        &self,
        node_type_filter: &str,
        external_id_filter: &str,
        limit: u32,
    ) -> Result<(Vec<NodeSummary>, u32), ClientError> {
        let req = graph_proto::ListNodesRequest {
            node_type_filter: node_type_filter.to_string(),
            external_id_filter: external_id_filter.to_string(),
            limit,
        };
        let r = self.client.clone().list_nodes(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        let nodes = inner.nodes.into_iter().map(|n| NodeSummary {
            node_id: n.node_id,
            node_type: n.node_type,
            external_id: n.external_id,
        }).collect();
        Ok((nodes, inner.total_count))
    }
}

#[derive(Debug, Clone)]
pub struct NodeResponse {
    pub node_id: u64,
    pub node_type: String,
    pub external_id: Option<String>,
    pub properties: Vec<PropertyEntry>,
}

#[derive(Debug, Clone)]
pub struct NodeSummary {
    pub node_id: u64,
    pub node_type: String,
    pub external_id: String,
}
