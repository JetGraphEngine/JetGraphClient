//! Schema service client: register types, finalize.

use tonic::transport::Channel;
use crate::ClientError;

pub mod schema_proto {
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

    /// Register a node type.
    pub async fn register_node_type(&mut self, name: &str) -> Result<u32, ClientError> {
        let req = schema_proto::NodeTypeSpec { name: name.to_string() };
        let r = self.client.register_node_type(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().node_type_id)
    }

    /// Register a compact edge type. Activity bitmap is mandatory; pass tick_size_secs (e.g. 3600).
    pub async fn register_compact_edge_type(
        &mut self,
        name: &str,
        from_node_type: &str,
        to_node_type: &str,
        state_ttl_secs: u64,
        bin_boundaries: Vec<f32>,
        tracked_property: &str,
        activity_tick_size_secs: u64,
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
    pub async fn register_static_edge_type(
        &mut self,
        name:            &str,
        from_node_type:  &str,
        to_node_type:    &str,
        state_ttl_secs:  u64,
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
        };
        let r = self.client.register_compact_edge_type(req).await.map_err(ClientError::from)?;
        Ok(r.into_inner().edge_type_id)
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
            }).collect(),
            edge_types: inner.edge_types.into_iter().map(|e| EdgeTypeInfo {
                id: e.id,
                name: e.name,
                from_node_type: e.from_node_type,
                to_node_type: e.to_node_type,
                state_ttl_secs: e.state_ttl_secs,
                tick_size_secs: e.tick_size_secs,
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
}
