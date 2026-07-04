//! Convenience types for the client API.

/// An edge type with a caller-supplied weight for weighted similarity scoring.
///
/// The engine normalises internally — weights do not need to sum to 1.0.
/// Matching on more (or more heavily weighted) edge types always increases the score.
///
/// # Example
/// ```
/// use jetgraph_client::EdgeTypeWeight;
/// let weights = vec![
///     EdgeTypeWeight::new("TRANSACTS_AT", 0.5),
///     EdgeTypeWeight::new("USES_DEVICE",  0.3),
///     EdgeTypeWeight::new("USES_IP",      0.2),
/// ];
/// ```
#[derive(Debug, Clone)]
pub struct EdgeTypeWeight {
    pub edge_type: String,
    pub weight:    f32,
}

impl EdgeTypeWeight {
    /// Convenience constructor.
    pub fn new(edge_type: impl Into<String>, weight: f32) -> Self {
        Self { edge_type: edge_type.into(), weight }
    }
}

/// A boolean node property treated as a virtual edge for weighted similarity scoring.
///
/// When a node has this property set to `true`, it is treated as though connected to
/// a virtual shared neighbor. Two nodes both having the property `true` score Jaccard
/// 1.0 on this dimension; one or both `false`/null scores 0.0.
///
/// The engine normalises internally; weights do not need to sum to 1.0.
#[derive(Debug, Clone)]
pub struct BoolPropertyWeight {
    pub property_name: String,
    pub weight:        f32,
}

impl BoolPropertyWeight {
    /// Convenience constructor.
    pub fn new(property_name: impl Into<String>, weight: f32) -> Self {
        Self { property_name: property_name.into(), weight }
    }
}

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

/// Node declaration in a best-effort transaction ingest request.
#[derive(Debug, Clone)]
pub struct TransactionNode {
    pub request_node_key: Option<String>,
    pub node_type: String,
    pub external_id: String,
    pub properties: Vec<PropertyEntry>,
}

impl TransactionNode {
    pub fn new(node_type: impl Into<String>, external_id: impl Into<String>) -> Self {
        Self {
            request_node_key: None,
            node_type: node_type.into(),
            external_id: external_id.into(),
            properties: Vec::new(),
        }
    }

    pub fn with_key(mut self, request_node_key: impl Into<String>) -> Self {
        self.request_node_key = Some(request_node_key.into());
        self
    }

    pub fn with_properties(mut self, properties: Vec<PropertyEntry>) -> Self {
        self.properties = properties;
        self
    }
}

/// Source/destination reference for transaction ingest edges.
#[derive(Debug, Clone)]
pub enum TransactionNodeRef {
    Node(NodeRef),
    RequestNodeKey(String),
}

impl TransactionNodeRef {
    pub fn node(node: NodeRef) -> Self {
        Self::Node(node)
    }

    pub fn request_node_key(key: impl Into<String>) -> Self {
        Self::RequestNodeKey(key.into())
    }
}

/// Edge declaration in a best-effort transaction ingest request.
#[derive(Debug, Clone)]
pub struct TransactionEdge {
    pub request_edge_key: Option<String>,
    pub edge_type: String,
    pub src: TransactionNodeRef,
    pub dst: TransactionNodeRef,
    pub numeric_value: Option<f32>,
    pub event_ts_secs: Option<u32>,
    pub bool_property_value: Option<bool>,
}

impl TransactionEdge {
    pub fn new(
        edge_type: impl Into<String>,
        src: TransactionNodeRef,
        dst: TransactionNodeRef,
    ) -> Self {
        Self {
            request_edge_key: None,
            edge_type: edge_type.into(),
            src,
            dst,
            numeric_value: None,
            event_ts_secs: None,
            bool_property_value: None,
        }
    }

    pub fn with_key(mut self, request_edge_key: impl Into<String>) -> Self {
        self.request_edge_key = Some(request_edge_key.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct NodeIngestOutcome {
    pub index: u32,
    pub request_node_key: Option<String>,
    pub node_id: Option<u64>,
    pub created: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EdgeIngestOutcome {
    pub index: u32,
    pub request_edge_key: Option<String>,
    pub created_new: bool,
    pub payload: Option<UpsertEdgeResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IngestTransactionResult {
    pub transaction_id: String,
    pub nodes_created: u32,
    pub nodes_existing: u32,
    pub node_errors: u32,
    pub edges_created: u32,
    pub edges_updated: u32,
    pub edge_errors: u32,
    pub node_results: Vec<NodeIngestOutcome>,
    pub edge_results: Vec<EdgeIngestOutcome>,
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
/// - `activity_bitmap` and `bins` are always 0 / `[0; 8]`.
#[derive(Debug, Clone)]
pub struct UpsertEdgeResult {
    pub created_new: bool,
    pub tx_count: u32,
    /// Accumulated numeric value (or, for static edges, the stored float score).
    pub approx_sum: f32,
    /// Unix timestamp (seconds) of the last event on this edge.
    pub last_seen: u32,
    /// Sliding activity window bitmap. Non-zero only for compact edge types
    /// that have activity tracking enabled; always 0 for static edges.
    pub activity_bitmap: u64,
    /// Per-range transaction counts across the numeric value histogram bins.
    /// Always `[0; 8]` for static edge types.
    pub bins: [u16; 8],
    /// Value of the edge type's boolean property, if one is registered.
    /// `None` when the edge type has no boolean property in its schema.
    pub bool_flag: Option<bool>,
}

/// State of a single edge returned by `get_edge_state`.
///
/// For static (minimal-payload) edges such as SIMILAR_TO:
/// - `approx_sum` holds the stored float value (e.g. the Jaccard similarity score).
/// - `tx_count` is always 1.
/// - `activity_bitmap` and `bins` are always 0 / `[0; 8]`.
#[derive(Debug, Clone)]
pub struct EdgeState {
    pub found: bool,
    pub tx_count: u32,
    /// Accumulated numeric value (or, for static edges, the stored float score).
    pub approx_sum: f32,
    /// Unix timestamp (seconds) of the last event on this edge.
    pub last_seen: u32,
    /// Sliding activity window bitmap. Non-zero only for compact edge types
    /// that have activity tracking enabled; always 0 for static edges.
    pub activity_bitmap: u64,
    /// Per-range transaction counts across the numeric value histogram bins.
    /// Always `[0; 8]` for static edge types.
    pub bins: [u16; 8],
    /// Event count matching the optional min/max value filter supplied to `get_edge_state`.
    pub filtered_count: u32,
    /// Accumulated value for events matching the optional value filter.
    pub filtered_approx_sum: f32,
    /// Event counts for each activity window (in the same order as the windows
    /// supplied to `get_edge_state`). Empty when no windows were requested.
    pub activity_counts: Vec<u32>,
    /// Value of the edge type's boolean property, if one is registered.
    /// `None` when the edge type has no boolean property in its schema.
    pub bool_flag: Option<bool>,
}

/// A neighbor edge from GetNeighbors.
#[derive(Debug, Clone)]
pub struct NeighborEdge {
    pub neighbor_node_id:    u64,
    pub edge_id:             u64,
    pub created_at_us:       i64,
    /// The type name of the neighbour node (e.g. `"Card"`).
    /// Populated when `include_neighbor_props = true` or filters are applied.
    pub neighbor_node_type:  String,
    /// The external ID of the neighbour, if it has one.
    pub neighbor_external_id: Option<String>,
    /// All registered properties of the neighbour node.
    /// Empty when `include_neighbor_props = false`.
    pub neighbor_props:      Vec<PropertyEntry>,
}

// ---------------------------------------------------------------------------
// NodePropertyFilter — ergonomic API for neighbour property predicates
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Segment types
// ---------------------------------------------------------------------------

/// Which field to extract from a NodeHistogramResult.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramField {
    /// Total event count over the window.
    TotalEvents,
    /// Weighted approximate sum (sum of bin_count * bin_midpoint).
    TotalApproxSum,
    /// Index of the highest-count bin (0-based).
    PeakBin,
}

/// Which field to extract from an EdgeState.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeStateField {
    /// Transaction count.
    TxCount,
    /// Approximate sum (for full edges, the accumulated numeric value).
    ApproxSum,
    /// Activity count over a window (requires `activity_window_secs` to be set).
    ActivityCount,
    /// Seconds since the edge was last seen/updated.
    LastSeenSecs,
}

/// A segment a customer belongs to, returned by `get_customer_segments`.
#[derive(Debug, Clone)]
pub struct SegmentMembership {
    pub segment_name:    String,
    pub segment_node_id: u64,
    /// Confidence score stored as the edge's float value (0.0–1.0).
    pub confidence:      f32,
    /// Unix timestamp (seconds) of the last evaluation that set this membership.
    pub last_seen_secs:  u32,
}

/// A member of a segment, returned by `get_segment_members`.
#[derive(Debug, Clone)]
pub struct SegmentMember {
    pub customer_node_id: u64,
    /// Human-readable external ID of the member node (e.g. "card-velocity").
    /// `None` if the node was created without an external ID.
    pub external_id:      Option<String>,
    /// Confidence score stored as the edge's float value (0.0–1.0).
    pub confidence:       f32,
    /// Unix timestamp (seconds) of the last evaluation that set this membership.
    pub last_seen_secs:   u32,
}

/// A predicate on a neighbour node's property for use with `get_neighbors`.
#[derive(Debug, Clone)]
pub struct NodePropertyFilter {
    pub property:  String,
    pub predicate: NodePropPredicate,
}

#[derive(Debug, Clone)]
pub enum NodePropPredicate {
    IntGt(i64),
    IntLt(i64),
    IntEq(i64),
    FloatGt(f64),
    FloatLt(f64),
    TsAfter(i64),
    TsBefore(i64),
    StringEq(String),
    BoolEq(bool),
}

impl NodePropertyFilter {
    pub fn int_gt(property: &str, val: i64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::IntGt(val) }
    }
    pub fn int_lt(property: &str, val: i64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::IntLt(val) }
    }
    pub fn int_eq(property: &str, val: i64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::IntEq(val) }
    }
    pub fn float_gt(property: &str, val: f64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::FloatGt(val) }
    }
    pub fn float_lt(property: &str, val: f64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::FloatLt(val) }
    }
    pub fn ts_after(property: &str, val: i64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::TsAfter(val) }
    }
    pub fn ts_before(property: &str, val: i64) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::TsBefore(val) }
    }
    pub fn string_eq(property: &str, val: &str) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::StringEq(val.to_string()) }
    }
    pub fn bool_eq(property: &str, val: bool) -> Self {
        Self { property: property.to_string(), predicate: NodePropPredicate::BoolEq(val) }
    }
}

/// Server-side filter applied to the *edges* returned by `get_neighbors_filtered`.
///
/// All conditions are ANDed together. Leave a field unset (or the vec empty) to
/// skip that constraint.
///
/// - `min_created_at_us` / `max_created_at_us`: keep only edges whose
///   `created_at_us` falls within `[min, max]` (microseconds since epoch).
/// - `property_filters`: predicates evaluated against the *edge*'s own
///   properties (distinct from `neighbor_filters`, which target the neighbour
///   node's properties).
#[derive(Debug, Clone, Default)]
pub struct EdgeFilter {
    pub min_created_at_us: Option<i64>,
    pub max_created_at_us: Option<i64>,
    pub property_filters:  Vec<NodePropertyFilter>,
}

impl EdgeFilter {
    /// An empty filter (matches every edge). Equivalent to `EdgeFilter::default()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep only edges created at or after `us` (microseconds since epoch).
    pub fn min_created_at_us(mut self, us: i64) -> Self {
        self.min_created_at_us = Some(us);
        self
    }

    /// Keep only edges created at or before `us` (microseconds since epoch).
    pub fn max_created_at_us(mut self, us: i64) -> Self {
        self.max_created_at_us = Some(us);
        self
    }

    /// Add a predicate on one of the edge's own properties.
    pub fn with_property_filter(mut self, filter: NodePropertyFilter) -> Self {
        self.property_filters.push(filter);
        self
    }

    /// True when this filter imposes no constraints at all.
    pub fn is_empty(&self) -> bool {
        self.min_created_at_us.is_none()
            && self.max_created_at_us.is_none()
            && self.property_filters.is_empty()
    }
}
