//! Convenience types for the client API.

/// Reference to a node: either by NodeId or by external (type, id).
#[derive(Debug, Clone)]
pub enum NodeRef {
    NodeId(u64),
    External { node_type: String, external_id: String },
}

impl NodeRef {
    /// Create a reference by NodeId.
    pub fn node_id(id: u64) -> Self {
        Self::NodeId(id)
    }

    /// Create a reference by external (type, id).
    pub fn external(node_type: &str, external_id: &str) -> Self {
        Self::External {
            node_type: node_type.to_string(),
            external_id: external_id.to_string(),
        }
    }
}

/// A property entry (name, value).
#[derive(Debug, Clone)]
pub struct PropertyEntry {
    pub name: String,
    pub value: PropertyValue,
}

impl PropertyEntry {
    pub fn int(name: impl Into<String>, v: i64) -> Self {
        Self { name: name.into(), value: PropertyValue::Int(v) }
    }
    pub fn float(name: impl Into<String>, v: f64) -> Self {
        Self { name: name.into(), value: PropertyValue::Float(v) }
    }
    pub fn string(name: impl Into<String>, v: impl Into<String>) -> Self {
        Self { name: name.into(), value: PropertyValue::String(v.into()) }
    }
    pub fn bool(name: impl Into<String>, v: bool) -> Self {
        Self { name: name.into(), value: PropertyValue::Bool(v) }
    }
}

#[derive(Debug, Clone)]
pub enum PropertyValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Timestamp(i64),
}

/// Result of a create node call. With external_id, create is idempotent (created=false if existed).
#[derive(Debug, Clone)]
pub struct CreateNodeResult {
    pub node_id: u64,
    pub created: bool,
}

/// Result of an upsert edge call.
///
/// For static (minimal-payload) edges such as SIMILAR_TO:
/// - `approx_sum` carries the stored float value (e.g. the Jaccard score).
/// - `tx_count` is always 1 (no accumulation).
/// - `activity_bitmap_raw` and `bins` are always 0.
#[derive(Debug, Clone)]
pub struct UpsertEdgeResult {
    pub created_new: bool,
    pub tx_count: u32,
    /// For static edges this holds the stored float value (e.g. Jaccard score).
    pub approx_sum: f32,
    pub last_seen: u32,
    /// Raw 64-bit activity bitmap value. Always 0 for static edge types.
    pub activity_bitmap_raw: u64,
    /// Per-bin transaction counts (8 bins). Always [0; 8] for static edge types.
    pub bins: [u16; 8],
}

/// Edge state from GetEdgeState.
///
/// For static (minimal-payload) edges such as SIMILAR_TO:
/// - `approx_sum` holds the stored float value (e.g. the Jaccard similarity score).
/// - `tx_count` is always 1.
/// - `activity_bitmap_raw` and `bins` are always 0.
#[derive(Debug, Clone)]
pub struct EdgeState {
    pub found: bool,
    pub tx_count: u32,
    /// For static edges this holds the stored float value (e.g. Jaccard score).
    pub approx_sum: f32,
    pub last_seen: u32,
    /// Raw 64-bit activity bitmap value. Always 0 for static edge types.
    pub activity_bitmap_raw: u64,
    /// Per-bin transaction counts (8 bins). Always [0; 8] for static edge types.
    pub bins: [u16; 8],
    pub filtered_count: u32,
    pub filtered_approx_sum: f32,
    pub activity_counts: Vec<u32>,
}

/// A neighbor edge from GetNeighbors.
#[derive(Debug, Clone)]
pub struct NeighborEdge {
    pub neighbor_node_id: u64,
    pub edge_id: u64,
    pub created_at_us: i64,
}
