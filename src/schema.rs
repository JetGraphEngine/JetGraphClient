//! Schema service client: register types, finalize.

use tonic::transport::Channel;
use crate::ClientError;

pub(crate) mod schema_proto {
    tonic::include_proto!("schema");
}

use schema_proto::schema_service_client::SchemaServiceClient;

#[derive(Clone)]
pub struct SchemaClient {
    client: SchemaServiceClient<Channel>,
}

impl SchemaClient {
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            client: SchemaServiceClient::new(channel),
        }
    }

    /// Register a node type with the given name and external ID kind.
    ///
    /// `numeric` (default): external IDs are stored as `u64`. Saves ~28 B/node vs `string`.
    /// Use `numeric` when all external IDs for this type are pure integers.
    ///
    /// `string`: external IDs are stored as `Arc<str>`. Use when IDs contain non-numeric chars.
    ///
    /// The kind is **frozen at registration** — changing it after nodes are written corrupts NodeIds.
    pub async fn register_node_type(
        &mut self,
        name: &str,
        numeric_ids: bool,
    ) -> Result<u32, ClientError> {
        let ext_id_kind = if numeric_ids {
            schema_proto::ExternalIdKind::Numeric as i32
        } else {
            schema_proto::ExternalIdKind::String as i32
        };
        let req = schema_proto::NodeTypeSpec {
            name: name.to_string(),
            ext_id_kind,
        };
        let r = self.client.register_node_type(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().node_type_id)
    }

    /// Register a compact edge type. Activity bitmap is mandatory; pass tick_size_secs (e.g. 3600).
    ///
    /// `bool_property_name`: optional name for the single boolean property stored in bit 63 of
    /// the activity flags field. Pass `None` (or `Some("")`) for no boolean property. Cannot be
    /// combined with static edge types (`minimal_payload = true`).
    ///
    /// `symmetric`: when true, edges are undirected — upserts normalize to (min,max) and
    /// neighbor queries return out ∪ in. Requires `from_node_type == to_node_type`.
    pub async fn register_compact_edge_type(
        &mut self,
        name: &str,
        from_node_type: &str,
        to_node_type: &str,
        state_ttl_secs: u64,
        bin_boundaries: Vec<f32>,
        tracked_property: &str,
        activity_tick_size_secs: u64,
        bool_property_name: Option<&str>,
        symmetric: bool,
    ) -> Result<u32, ClientError> {
        let field_schema = if !bin_boundaries.is_empty() {
            Some(schema_proto::CompactFieldSchema {
                bin_boundaries,
                tracked_property: tracked_property.to_string(),
            })
        } else {
            None
        };
        let node_histogram = if field_schema.is_some() {
            Some(schema_proto::NodeHistogramConfig {
                enabled_for_src: true,
                enabled_for_dst: true,
                hourly_slots: 48,
                daily_slots: 30,
            })
        } else {
            None
        };
        let req = schema_proto::CompactEdgeTypeSpec {
            name: name.to_string(),
            from_node_type: from_node_type.to_string(),
            to_node_type: to_node_type.to_string(),
            state_ttl_secs,
            field_schema,
            node_histogram,
            activity_bitmap: Some(schema_proto::ActivityBitmapConfig {
                tick_size_secs: activity_tick_size_secs,
            }),
            minimal_payload: false,
            bool_property: bool_property_name.unwrap_or("").to_string(),
            symmetric,
        };
        let r = self.client.register_compact_edge_type(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().edge_type_id)
    }

    /// Register a property.
    pub async fn register_property(
        &mut self,
        name: &str,
        owner_type_name: &str,
        owner_is_node: bool,
        value_type: schema_proto::ValueType,
    ) -> Result<u32, ClientError> {
        let req = schema_proto::PropertySpec {
            name: name.to_string(),
            owner_type_name: owner_type_name.to_string(),
            owner_is_node,
            value_type: value_type.into(),
        };
        let r = self.client.register_property(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().property_id)
    }

    /// Register a static (minimal) edge type for computed edges (e.g. SIMILAR_TO).
    ///
    /// Uses the 8-byte `StaticEdgePayload` (value + last_seen only). No activity bitmap,
    /// no bins, no tx_count. Ideal for similarity scores or any float-tagged relationship.
    /// Register with `state_ttl_secs > 0` for automatic TTL-based expiry.
    ///
    /// `symmetric`: when true, edges are undirected — upserts normalize to (min,max) and
    /// neighbor queries return out ∪ in. Requires `from_node_type == to_node_type`.
    pub async fn register_static_edge_type(
        &mut self,
        name:            &str,
        from_node_type:  &str,
        to_node_type:    &str,
        state_ttl_secs:  u64,
        symmetric:       bool,
    ) -> Result<u32, ClientError> {
        let req = schema_proto::CompactEdgeTypeSpec {
            name: name.to_string(),
            from_node_type: from_node_type.to_string(),
            to_node_type: to_node_type.to_string(),
            state_ttl_secs,
            field_schema: None,
            node_histogram: None,
            activity_bitmap: Some(schema_proto::ActivityBitmapConfig {
                tick_size_secs: 3600,
            }),
            minimal_payload: true,
            bool_property: String::new(), // static edge types cannot have a bool property
            symmetric,
        };
        let r = self.client.register_compact_edge_type(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().edge_type_id)
    }

    /// Remove an edge type from the engine schema and drop **all** stored edges of that type.
    ///
    /// This is a destructive, irreversible operation. Use it to clean up transition edge
    /// types created by the pattern miner when deleting a rule, or to reclaim memory from
    /// an edge type that is no longer needed.
    ///
    /// Returns `(edge_type_id, pairs_dropped)` on success.
    /// Returns an error if `name` is not found in the live schema.
    pub async fn remove_edge_type(
        &mut self,
        name: &str,
    ) -> Result<RemoveEdgeTypeResult, ClientError> {
        let req = schema_proto::RemoveEdgeTypeRequest { name: name.to_string() };
        let r = self.client.remove_edge_type(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(RemoveEdgeTypeResult {
            edge_type_id:  inner.edge_type_id,
            pairs_dropped: inner.pairs_dropped,
        })
    }

    /// Query current memory usage across nodes, edges, histograms, and runtime overhead.
    pub async fn get_memory_usage(&mut self) -> Result<MemoryUsage, ClientError> {
        let req = schema_proto::GetMemoryUsageRequest {};
        let r = self.client.get_memory_usage(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        let est = inner.estimate.unwrap_or_default();
        Ok(MemoryUsage {
            total_bytes:         est.total_bytes,
            nodes_bytes:         est.nodes_bytes,
            compact_store_bytes: est.compact_store_bytes,
            histogram_bytes:     est.histogram_bytes,
            runtime_bytes:       est.runtime_bytes,
            breakdown_text:      est.breakdown_text,
            node_count:          inner.node_count,
            compact_pair_count:  inner.compact_pair_count,
        })
    }

    /// Finalize the schema. Call after all types are registered.
    pub async fn finalize(&mut self) -> Result<u32, ClientError> {
        let req = schema_proto::FinalizeRequest {};
        let r = self.client.finalize_schema(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().schema_version)
    }

    /// Get the current schema.
    pub async fn get_schema(&mut self) -> Result<GetSchemaResult, ClientError> {
        let req = schema_proto::GetSchemaRequest {};
        let r = self.client.get_schema(req).await.map_err(ClientError::from)?;
        let inner = r.into_inner();
        Ok(GetSchemaResult {
            schema_version: inner.schema_version,
            node_types: inner.node_types.into_iter().map(|n| NodeTypeInfo {
                id: n.id,
                name: n.name,
                numeric_ids: n.ext_id_kind != schema_proto::ExternalIdKind::String as i32,
            }).collect(),
            edge_types: inner.edge_types.into_iter().map(|e| EdgeTypeInfo {
                id: e.id,
                name: e.name,
                from_node_type: e.from_node_type,
                to_node_type: e.to_node_type,
                state_ttl_secs: e.state_ttl_secs,
                tick_size_secs: e.tick_size_secs,
                bool_property: if e.bool_property.is_empty() { None } else { Some(e.bool_property) },
                is_symmetric: e.is_symmetric,
            }).collect(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GetSchemaResult {
    pub schema_version: u32,
    pub node_types: Vec<NodeTypeInfo>,
    pub edge_types: Vec<EdgeTypeInfo>,
}

#[derive(Debug, Clone)]
pub struct NodeTypeInfo {
    pub id: u32,
    pub name: String,
    /// True when external IDs for this node type are stored as `u64` (numeric).
    /// False when stored as `Arc<str>` (string).
    pub numeric_ids: bool,
}

#[derive(Debug, Clone)]
pub struct EdgeTypeInfo {
    pub id: u32,
    pub name: String,
    pub from_node_type: String,
    pub to_node_type: String,
    pub state_ttl_secs: u64,
    /// Seconds per tick for the mandatory activity bitmap on this edge type.
    pub tick_size_secs: u64,
    /// Name of the single boolean property stored in bit 63 of the flags field.
    /// `None` when this edge type has no boolean property defined.
    pub bool_property: Option<String>,
    /// True when the edge type was registered as symmetric (undirected).
    /// Upserts normalize to (min(src,dst), max(src,dst)) and queries return out ∪ in.
    pub is_symmetric: bool,
}

/// Result returned by [`SchemaClient::remove_edge_type`].
#[derive(Debug, Clone)]
pub struct RemoveEdgeTypeResult {
    /// Numeric ID of the edge type that was removed.
    pub edge_type_id: u32,
    /// Number of edge pairs that were dropped from the in-memory store.
    pub pairs_dropped: u64,
}

/// Memory usage breakdown returned by [`SchemaClient::get_memory_usage`].
#[derive(Debug, Clone, Default)]
pub struct MemoryUsage {
    pub total_bytes:         u64,
    pub nodes_bytes:         u64,
    pub compact_store_bytes: u64,
    pub histogram_bytes:     u64,
    pub runtime_bytes:       u64,
    /// Human-readable per-type breakdown string produced by the engine.
    pub breakdown_text:      String,
    pub node_count:          u64,
    pub compact_pair_count:  u64,
}
