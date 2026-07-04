//! Graph service client: nodes, edges, neighbors.

use tonic::transport::Channel;
use tonic::Streaming;
use crate::{
    ClientError, NodeRef, PropertyEntry, PropertyValue, UpsertEdgeResult, EdgeState, NeighborEdge,
    CreateNodeResult, EdgeFilter, NodePropertyFilter, NodePropPredicate,
    TransactionNode, TransactionNodeRef, TransactionEdge,
    IngestTransactionResult, NodeIngestOutcome, EdgeIngestOutcome,
};

pub(crate) mod graph_proto {
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

    pub(crate) fn node_ref_to_proto(r: &NodeRef) -> graph_proto::NodeRef {
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

    pub(crate) fn property_to_proto(p: &PropertyEntry) -> graph_proto::PropertyEntry {
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

    pub(crate) fn transaction_node_ref_to_proto(r: &TransactionNodeRef) -> graph_proto::TransactionNodeRef {
        use graph_proto::transaction_node_ref::Reference;
        let reference = match r {
            TransactionNodeRef::Node(node_ref) => {
                Reference::Node(Self::node_ref_to_proto(node_ref))
            }
            TransactionNodeRef::RequestNodeKey(key) => {
                Reference::RequestNodeKey(key.clone())
            }
        };
        graph_proto::TransactionNodeRef {
            reference: Some(reference),
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

    /// Update (merge) properties on an existing node.
    ///
    /// The supplied properties are applied on top of the node's current
    /// properties; properties not listed are left unchanged. Errors if the node
    /// does not exist.
    pub async fn update_node(
        &self,
        node: NodeRef,
        properties: &[PropertyEntry],
    ) -> Result<(), ClientError> {
        let req = graph_proto::UpdateNodeRequest {
            node: Some(Self::node_ref_to_proto(&node)),
            props: properties.iter().map(Self::property_to_proto).collect(),
        };
        self.client.clone().update_node(req).await.map_err(ClientError::from)?;
        Ok(())
    }

    /// Delete a node by reference.
    ///
    /// Removes the node (and tombstones it so its external id no longer
    /// resolves). Edge cleanup follows the engine's deletion semantics.
    pub async fn delete_node(&self, node: NodeRef) -> Result<(), ClientError> {
        let req = graph_proto::DeleteNodeRequest {
            node: Some(Self::node_ref_to_proto(&node)),
        };
        self.client.clone().delete_node(req).await.map_err(ClientError::from)?;
        Ok(())
    }

    /// Upsert a compact edge.
    ///
    /// `bool_property_value`: when the edge type has a boolean property registered,
    /// pass `Some(true/false)` to set it. Pass `None` to leave it unchanged.
    pub async fn upsert_edge(
        &self,
        edge_type: &str,
        src: NodeRef,
        dst: NodeRef,
        numeric_value: Option<f32>,
        event_ts_secs: Option<u32>,
        bool_property_value: Option<bool>,
    ) -> Result<UpsertEdgeResult, ClientError> {
        let req = graph_proto::UpsertEdgeRequest {
            edge_type_name: edge_type.to_string(),
            src: Some(Self::node_ref_to_proto(&src)),
            dst: Some(Self::node_ref_to_proto(&dst)),
            numeric_value,
            event_ts_secs,
            bool_property_value,
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
            activity_bitmap: payload.flags,
            bins,
            bool_flag: payload.bool_flag,
        })
    }

    /// Best-effort transaction ingest: ensure nodes and upsert edges in one call.
    pub async fn ingest_transaction(
        &self,
        transaction_id: Option<&str>,
        nodes: &[TransactionNode],
        edges: &[TransactionEdge],
    ) -> Result<IngestTransactionResult, ClientError> {
        let req = graph_proto::IngestTransactionRequest {
            transaction_id: transaction_id.unwrap_or("").to_string(),
            nodes: nodes
                .iter()
                .map(|n| graph_proto::TransactionNode {
                    request_node_key: n.request_node_key.clone().unwrap_or_default(),
                    node_type_name: n.node_type.clone(),
                    external_id: n.external_id.clone(),
                    properties: n.properties.iter().map(Self::property_to_proto).collect(),
                })
                .collect(),
            edges: edges
                .iter()
                .map(|e| graph_proto::TransactionEdge {
                    request_edge_key: e.request_edge_key.clone().unwrap_or_default(),
                    edge_type_name: e.edge_type.clone(),
                    src: Some(Self::transaction_node_ref_to_proto(&e.src)),
                    dst: Some(Self::transaction_node_ref_to_proto(&e.dst)),
                    numeric_value: e.numeric_value,
                    event_ts_secs: e.event_ts_secs,
                    bool_property_value: e.bool_property_value,
                })
                .collect(),
        };

        let r = self.client.clone().ingest_transaction(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();

        let node_results = inner.node_results.into_iter().map(|n| NodeIngestOutcome {
            index: n.index,
            request_node_key: if n.request_node_key.is_empty() { None } else { Some(n.request_node_key) },
            node_id: if n.error.is_some() { None } else { Some(n.node_id) },
            created: n.created,
            error: n.error,
        }).collect();

        let edge_results = inner.edge_results.into_iter().map(|e| {
            let graph_proto::EdgeIngestResult {
                index,
                request_edge_key,
                created_new,
                payload,
                error,
            } = e;
            let payload = payload.map(|p| {
                let bins = decode_bins(&p.bins);
                UpsertEdgeResult {
                    created_new,
                    tx_count: p.tx_count,
                    approx_sum: p.approx_sum,
                    last_seen: p.last_seen,
                    activity_bitmap: p.flags,
                    bins,
                    bool_flag: p.bool_flag,
                }
            });
            EdgeIngestOutcome {
                index,
                request_edge_key: if request_edge_key.is_empty() { None } else { Some(request_edge_key) },
                created_new,
                payload,
                error,
            }
        }).collect();

        Ok(IngestTransactionResult {
            transaction_id: inner.transaction_id,
            nodes_created: inner.nodes_created,
            nodes_existing: inner.nodes_existing,
            node_errors: inner.node_errors,
            edges_created: inner.edges_created,
            edges_updated: inner.edges_updated,
            edge_errors: inner.edge_errors,
            node_results,
            edge_results,
        })
    }

    /// Open a high-throughput bidirectional streaming ingest session.
    ///
    /// Returns an [`IngestSender`] / [`IngestResponseStream`] pair backed by
    /// the `IngestStream` gRPC RPC. The server accumulates messages into
    /// micro-batches, merges all edges by `(edge_type, src)` across the batch,
    /// and applies each group in a single sorted-merge RCU pass — yielding
    /// significantly higher throughput than calling `ingest_transaction`
    /// repeatedly.
    ///
    /// Send individual transactions with [`IngestSender::send`]; poll responses
    /// with [`IngestResponseStream::next`]. Drop the sender when finished to
    /// half-close the stream; drain the response stream to receive remaining
    /// acknowledgements.
    pub async fn ingest_stream(&self) -> Result<(IngestSender, IngestResponseStream), ClientError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<graph_proto::IngestTransactionRequest>(1024);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let resp = self.client.clone()
            .ingest_stream(stream)
            .await
            .map_err(ClientError::from)?
            .into_inner();
        Ok((IngestSender { tx }, IngestResponseStream { inner: resp }))
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
            activity_bitmap: payload.flags,
            bins,
            filtered_count: inner.filtered_count,
            filtered_approx_sum: inner.filtered_approx_sum,
            activity_counts: inner.activity_counts,
            bool_flag: payload.bool_flag,
        }))
    }

    /// Get neighbors of a node.
    ///
    /// - `limit` 0 = unlimited. `cursor` = neighbor_node_id after which to resume (0 = start).
    /// - `neighbor_filters`: server-side predicates on neighbour node properties; pass `&[]` for unfiltered.
    /// - `include_props`: when true, populate `NeighborEdge::neighbor_props` with all node properties.
    ///
    /// To additionally filter on the *edge* itself (created-at range or edge
    /// properties), use [`GraphClient::get_neighbors_filtered`].
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
        self.get_neighbors_filtered(
            node, edge_type, out_neighbors, limit, cursor, neighbor_filters, include_props, None,
        )
        .await
    }

    /// Get neighbors of a node, additionally filtering on the connecting edge.
    ///
    /// Same as [`GraphClient::get_neighbors`], plus an optional [`EdgeFilter`]
    /// applied server-side to each edge (created-at range and/or predicates on
    /// the edge's own properties). Pass `None` for no edge-level filtering.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_neighbors_filtered(
        &self,
        node: NodeRef,
        edge_type: &str,
        out_neighbors: bool,
        limit: u32,
        cursor: u64,
        neighbor_filters: &[NodePropertyFilter],
        include_props: bool,
        edge_filter: Option<&EdgeFilter>,
    ) -> Result<(Vec<NeighborEdge>, bool), ClientError> {
        let proto_filters = neighbor_filters.iter().map(Self::filter_to_proto).collect();
        let req = graph_proto::NeighborQuery {
            node: Some(Self::node_ref_to_proto(&node)),
            edge_type: edge_type.to_string(),
            out_neighbors,
            filter: edge_filter.map(Self::edge_filter_to_proto),
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

    fn edge_filter_to_proto(f: &EdgeFilter) -> graph_proto::EdgeFilter {
        graph_proto::EdgeFilter {
            min_created_at_us: f.min_created_at_us,
            max_created_at_us: f.max_created_at_us,
            property_filters: f.property_filters.iter().map(Self::filter_to_proto).collect(),
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

    /// Subscribe to the engine's real-time CDC edge-upsert stream.
    /// Pass an empty Vec to receive every edge type.
    /// Returns a `tonic::Streaming<EdgeEvent>` that yields one event per upsert.
    pub async fn watch_edge_upserts(
        &self,
        watch_edge_types: Vec<String>,
    ) -> Result<Streaming<graph_proto::EdgeEvent>, ClientError> {
        let req = graph_proto::EdgeEventRequest { watch_edge_types };
        let stream = self.client.clone()
            .watch_edge_upserts(req)
            .await
            .map_err(ClientError::from)?;
        Ok(stream.into_inner())
    }
}

// ---------------------------------------------------------------------------
// High-level streaming ingest types (hide gRPC wire types from callers)
// ---------------------------------------------------------------------------

/// Sender half of a high-throughput streaming ingest session.
///
/// Created by [`GraphClient::ingest_stream`] or [`crate::Client::ingest_stream`].
/// Drop this value to half-close the stream so the engine can flush remaining
/// responses.
pub struct IngestSender {
    tx: tokio::sync::mpsc::Sender<graph_proto::IngestTransactionRequest>,
}

impl IngestSender {
    /// Send one transaction (a set of nodes + edges) on the stream.
    ///
    /// The call is async but cheap — it places the serialised message into the
    /// in-process channel without waiting for server acknowledgement.
    /// Back-pressure is applied when the channel is full (capacity 1 024).
    pub async fn send(
        &self,
        transaction_id: Option<&str>,
        nodes: &[TransactionNode],
        edges: &[TransactionEdge],
    ) -> Result<(), ClientError> {
        let req = build_ingest_proto(transaction_id, nodes, edges);
        self.tx.send(req).await
            .map_err(|_| ClientError::Internal("ingest stream closed".into()))
    }

    /// Returns `true` if the engine-side stream has already been closed.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// Response stream from a high-throughput streaming ingest session.
///
/// Created alongside an [`IngestSender`] by [`GraphClient::ingest_stream`].
/// Each item corresponds to one [`IngestSender::send`] call in order.
pub struct IngestResponseStream {
    inner: Streaming<graph_proto::IngestTransactionResponse>,
}

impl IngestResponseStream {
    /// Receive the next acknowledgement from the engine.
    ///
    /// Returns `None` when all sent transactions have been processed and the
    /// stream is closed. Always drain this stream after dropping the sender so
    /// the engine can flush any buffered responses.
    pub async fn next(&mut self) -> Option<Result<IngestTransactionResult, ClientError>> {
        match self.inner.message().await {
            Ok(Some(r)) => Some(Ok(IngestTransactionResult {
                transaction_id:  r.transaction_id,
                nodes_created:   r.nodes_created,
                nodes_existing:  r.nodes_existing,
                node_errors:     r.node_errors,
                edges_created:   r.edges_created,
                edges_updated:   r.edges_updated,
                edge_errors:     r.edge_errors,
                node_results:    vec![],
                edge_results:    vec![],
            })),
            Ok(None)   => None,
            Err(e)     => Some(Err(ClientError::from(e))),
        }
    }
}

/// Assemble a proto ingest request from high-level client types.
fn build_ingest_proto(
    transaction_id: Option<&str>,
    nodes: &[TransactionNode],
    edges: &[TransactionEdge],
) -> graph_proto::IngestTransactionRequest {
    graph_proto::IngestTransactionRequest {
        transaction_id: transaction_id.unwrap_or("").to_string(),
        nodes: nodes.iter().map(|n| graph_proto::TransactionNode {
            request_node_key: n.request_node_key.clone().unwrap_or_default(),
            node_type_name:   n.node_type.clone(),
            external_id:      n.external_id.clone(),
            properties:       n.properties.iter().map(GraphClient::property_to_proto).collect(),
        }).collect(),
        edges: edges.iter().map(|e| graph_proto::TransactionEdge {
            request_edge_key:    e.request_edge_key.clone().unwrap_or_default(),
            edge_type_name:      e.edge_type.clone(),
            src:                 Some(GraphClient::transaction_node_ref_to_proto(&e.src)),
            dst:                 Some(GraphClient::transaction_node_ref_to_proto(&e.dst)),
            numeric_value:       e.numeric_value,
            event_ts_secs:       e.event_ts_secs,
            bool_property_value: e.bool_property_value,
        }).collect(),
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
