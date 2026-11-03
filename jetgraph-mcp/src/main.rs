//! JetGraph investigation MCP server.
//!
//! Cursor, or any other MCP host, is the agent. This process only runs tools
//! against the engine and the segment evaluator.

mod guard;

use std::time::Duration;

use jetgraph_client::{
    Client, ConditionOp, EdgeFeatureMetric, EdgeTypeWeight, FeatureDirection, FraudCaseMetric,
    NodeFeatureSourceKind, NodeRef, SegmentCondition, SegmentConfigClient, SegmentDefinition,
    SegmentLogic, SignalDefinition, SignalValueKind,
};
use rmcp::{
    handler::server::{
        router::{prompt::PromptRouter, tool::ToolRouter},
        wrapper::Parameters,
    },
    model::{CallToolResult, ContentBlock, PromptMessage, Role},
    prompt, prompt_handler, prompt_router, schemars, tool, tool_handler, tool_router,
    ServerHandler, ServiceExt,
};
use serde::Deserialize;
use serde_json::{json, Value};

const INSTRUCTIONS: &str = "\
Read get_schema before naming a node type or an edge type. Map the words in the question through each type's description and business_meaning.\n\
\n\
Write the hypothesis as measurements: get_neighbors, get_neighbor_count, get_last_neighbor, get_edge_state, get_histogram, get_feature_vector, find_similar, or a capped read-only cypher_read. Use the same edge type and window on every node you compare.\n\
\n\
Try those measurements on a sample before create_signal. create_signal and create_segment change evaluator configuration only. list_segment_members returns customers the evaluator has already linked. Include the definition you used — types, window, and score — in the answer.\n\
\n\
find_similar is shared-neighbor overlap and does not write edges. This server does not ingest data, delete types, or build a similarity graph.";

const DEFAULT_PAGE: u32 = 25;
const MAX_PAGE: u32 = 100;
const MAX_SIMILAR: u32 = 25;
const MAX_WINDOWS: usize = 4;

#[derive(Clone)]
struct Jetgraph {
    graph: Client,
    segments: SegmentConfigClient,
    http: reqwest::Client,
    cypher_url: String,
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
}

impl Jetgraph {
    fn from_env() -> anyhow::Result<Self> {
        let grpc =
            std::env::var("JETGRAPH_GRPC_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".into());
        let http =
            std::env::var("JETGRAPH_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
        let segments = std::env::var("JETGRAPH_SEGMENTS_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8081".into());
        let graph = Client::connect_lazy(&grpc)?;
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()?;
        Ok(Self {
            graph,
            segments: SegmentConfigClient::connect(segments),
            http: http_client,
            cypher_url: format!("{}/cypher", http.trim_end_matches('/')),
            tool_router: Self::tool_router(),
            prompt_router: Self::prompt_router(),
        })
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct NodeArgs {
    /// Node type name from get_schema.
    node_type: String,
    /// External id of the node.
    external_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct NeighborsArgs {
    node_type: String,
    external_id: String,
    /// Edge type name from get_schema.
    edge_type: String,
    /// `out` follows edges leaving the node. `in` follows edges arriving at it.
    #[serde(default = "default_direction")]
    direction: String,
    /// Page size. Default 25, maximum 100. Zero is rejected.
    #[serde(default)]
    limit: Option<u32>,
    /// Pass the previous page's next_cursor to continue. Zero starts at the beginning.
    #[serde(default)]
    cursor: u64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CountArgs {
    node_type: String,
    external_id: String,
    edge_type: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EdgeStateArgs {
    edge_type: String,
    src_type: String,
    src_id: String,
    dst_type: String,
    dst_id: String,
    /// Activity windows in seconds, at most four. Counts come back in the same order.
    #[serde(default)]
    activity_windows_secs: Vec<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct HistogramArgs {
    node_type: String,
    external_id: String,
    edge_type: String,
    /// Trailing hours. Set this or window_days, not both.
    #[serde(default)]
    window_hours: u32,
    /// Trailing days. Set this or window_hours, not both.
    #[serde(default)]
    window_days: u32,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EdgeWeightArg {
    edge_type: String,
    /// Relative weight. The engine normalizes the set.
    weight: f32,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SimilarArgs {
    node_type: String,
    external_id: String,
    /// Edge types whose shared neighbors define behavior.
    edge_weights: Vec<EdgeWeightArg>,
    /// A candidate must overlap on every listed edge type.
    #[serde(default)]
    required_edge_types: Vec<String>,
    /// Maximum neighbors to return. Default 10, maximum 25.
    #[serde(default)]
    k: Option<u32>,
    /// Drop candidates below this weighted overlap. Default 0.
    #[serde(default)]
    min_similarity: f32,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CypherArgs {
    /// One read-only statement. LIMIT is required above 100 and is added when missing.
    query: String,
    /// Cypher parameters. Omit when the query has none.
    #[serde(default)]
    parameters: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CreateSignalArgs {
    name: String,
    /// Read-only Cypher. Bind $node and return one column named value.
    query: String,
    /// `number` or `epoch_secs`. epoch_secs means value is a unix timestamp.
    #[serde(default = "default_value_kind")]
    value_kind: String,
    /// True when the value changes as time passes, even without a new event.
    #[serde(default)]
    time_sensitive: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ConditionArg {
    signal: String,
    /// `gt`, `lt`, `gte`, `lte`, `eq`, or `between`.
    op: String,
    threshold: f64,
    #[serde(default)]
    threshold_max: Option<f64>,
    #[serde(default)]
    weight: Option<f32>,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CreateSegmentArgs {
    name: String,
    #[serde(default)]
    label: String,
    /// Node type whose instances this segment describes.
    entity_type: String,
    /// `all`, `any`, or `weighted`. Default `all`.
    #[serde(default = "default_logic")]
    logic: String,
    #[serde(default)]
    min_confidence: Option<f32>,
    conditions: Vec<ConditionArg>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MembersArgs {
    /// Segment name, which is also the segment node's external id.
    name: String,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: u64,
}

fn default_direction() -> String {
    "out".into()
}
fn default_value_kind() -> String {
    "number".into()
}
fn default_logic() -> String {
    "all".into()
}

#[tool_router]
impl Jetgraph {
    #[tool(
        description = "Live schema: node types, edge types, descriptions, business meanings, and stored feature-vector slots. Call this before using a type name."
    )]
    async fn get_schema(&self) -> CallToolResult {
        let mut schema_client = self.graph.schema();
        let schema = match schema_client.get_schema().await {
            Ok(schema) => schema,
            Err(error) => return tool_err(error.to_string()),
        };
        let node_types: Vec<Value> = schema
            .node_types
            .iter()
            .map(|node| {
                json!({
                    "name": node.name,
                    "description": node.description,
                    "business_meaning": node.business_meaning,
                    "external_id": if node.numeric_ids { "numeric" } else { "string" },
                })
            })
            .collect();
        let edge_types: Vec<Value> = schema
            .edge_types
            .iter()
            .map(|edge| {
                json!({
                    "name": edge.name,
                    "from": edge.from_node_type,
                    "to": edge.to_node_type,
                    "description": edge.description,
                    "business_meaning": edge.business_meaning,
                    "symmetric": edge.is_symmetric,
                    "state_ttl_secs": edge.state_ttl_secs,
                    "activity_tick_secs": edge.tick_size_secs,
                    "bool_property": edge.bool_property,
                })
            })
            .collect();
        let feature_schemas: Vec<Value> = schema
            .node_feature_schemas
            .iter()
            .map(|spec| {
                let slots: Vec<Value> = spec
                    .slots
                    .iter()
                    .filter(|slot| {
                        slot.source_kind != NodeFeatureSourceKind::NodeFeatureSourceUnused as i32
                    })
                    .map(feature_slot)
                    .collect();
                json!({
                    "node_type": spec.node_type,
                    "version": spec.version,
                    "enabled": spec.enabled,
                    "slots": slots,
                })
            })
            .collect();
        let profiles: Vec<Value> = schema
            .transaction_feature_profiles
            .iter()
            .map(|profile| json!({ "name": profile.name, "version": profile.version }))
            .collect();
        tool_ok(json!({
            "schema_version": schema.schema_version,
            "node_types": node_types,
            "edge_types": edge_types,
            "feature_schemas": feature_schemas,
            "transaction_profiles": profiles,
        }))
    }

    #[tool(
        description = "One page of neighbors on one edge type. Returns external ids. Use next_cursor to continue. Maximum 100."
    )]
    async fn get_neighbors(&self, Parameters(args): Parameters<NeighborsArgs>) -> CallToolResult {
        let limit = match guard::clamp_page(args.limit, DEFAULT_PAGE, MAX_PAGE) {
            Ok(limit) => limit,
            Err(error) => return tool_err(error),
        };
        let out = match direction(&args.direction) {
            Ok(out) => out,
            Err(error) => return tool_err(error),
        };
        let node = match node_ref(&args.node_type, &args.external_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        match self
            .graph
            .get_neighbors(node, &args.edge_type, out, limit, args.cursor, &[], true)
            .await
        {
            Ok((edges, has_more)) => {
                let next_cursor = edges.last().map(|edge| edge.neighbor_node_id);
                let neighbors: Vec<Value> = edges
                    .into_iter()
                    .map(|edge| {
                        json!({
                            "node_type": edge.neighbor_node_type,
                            "external_id": edge.neighbor_external_id,
                            "node_id": edge.neighbor_node_id,
                            "created_at_us": edge.created_at_us,
                        })
                    })
                    .collect();
                tool_ok(json!({
                    "edge_type": args.edge_type,
                    "direction": args.direction,
                    "neighbors": neighbors,
                    "has_more": has_more,
                    "next_cursor": next_cursor,
                }))
            }
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(description = "How many neighbors a node has on one edge type.")]
    async fn get_neighbor_count(&self, Parameters(args): Parameters<CountArgs>) -> CallToolResult {
        let node = match node_ref(&args.node_type, &args.external_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        match self
            .graph
            .graph()
            .get_neighbor_count(node, &args.edge_type)
            .await
        {
            Ok((count, approximate)) => tool_ok(json!({
                "edge_type": args.edge_type,
                "count": count,
                "approximate": approximate,
            })),
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "The neighbor with the latest last_seen on one edge type, plus that timestamp."
    )]
    async fn get_last_neighbor(&self, Parameters(args): Parameters<CountArgs>) -> CallToolResult {
        let node = match node_ref(&args.node_type, &args.external_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        match self
            .graph
            .graph()
            .get_last_neighbor(node, &args.edge_type, None)
            .await
        {
            Ok(None) => tool_ok(json!({ "found": false, "edge_type": args.edge_type })),
            Ok(Some((node_id, last_seen_secs))) => {
                let identity = self.identity(node_id).await;
                tool_ok(json!({
                    "found": true,
                    "edge_type": args.edge_type,
                    "node_id": node_id,
                    "node_type": identity.as_ref().map(|item| item.0.clone()),
                    "external_id": identity.and_then(|item| item.1),
                    "last_seen_secs": last_seen_secs,
                }))
            }
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "State of one pair: tx_count, approx_sum, last_seen, and optional activity-window counts. A missing pair means the relationship has not been seen."
    )]
    async fn get_edge_state(&self, Parameters(args): Parameters<EdgeStateArgs>) -> CallToolResult {
        if args.activity_windows_secs.len() > MAX_WINDOWS {
            return tool_err(format!("at most {MAX_WINDOWS} activity windows"));
        }
        if args.activity_windows_secs.iter().any(|window| *window == 0) {
            return tool_err("activity windows must be greater than zero");
        }
        let src = match node_ref(&args.src_type, &args.src_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        let dst = match node_ref(&args.dst_type, &args.dst_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        let windows = if args.activity_windows_secs.is_empty() {
            None
        } else {
            Some(args.activity_windows_secs.as_slice())
        };
        match self
            .graph
            .get_edge_state(&args.edge_type, src, dst, None, windows)
            .await
        {
            Ok(None) => tool_ok(json!({ "found": false, "edge_type": args.edge_type })),
            Ok(Some(state)) => tool_ok(json!({
                "found": state.found,
                "edge_type": args.edge_type,
                "tx_count": state.tx_count,
                "approx_sum": state.approx_sum,
                "last_seen_secs": state.last_seen,
                "activity_windows_secs": args.activity_windows_secs,
                "activity_counts": state.activity_counts,
                "bool_flag": state.bool_flag,
            })),
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "Histogram totals for one node on one edge type. Pass window_hours or window_days, and the same window for every node in a comparison."
    )]
    async fn get_histogram(&self, Parameters(args): Parameters<HistogramArgs>) -> CallToolResult {
        if (args.window_hours == 0 && args.window_days == 0)
            || (args.window_hours > 0 && args.window_days > 0)
        {
            return tool_err("set exactly one of window_hours or window_days");
        }
        if args.window_hours > 24 * 60 || args.window_days > 365 {
            return tool_err("window_hours must be at most 1440 and window_days at most 365");
        }
        let node = match node_ref(&args.node_type, &args.external_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        let mut features = self.graph.features();
        match features
            .query_node_histogram(node, &args.edge_type, args.window_hours, args.window_days)
            .await
        {
            Ok(result) => tool_ok(json!({
                "edge_type": args.edge_type,
                "window_hours": args.window_hours,
                "window_days": args.window_days,
                "total_events": result.total_events,
                "bin_counts": result.total_counts,
                "window_covered_secs": result.window_covered_secs,
            })),
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "Stored 32-slot feature vector for one node. Slots are defined by the feature schema from get_schema. effective is the value after transform, weight, and fallback."
    )]
    async fn get_feature_vector(&self, Parameters(args): Parameters<NodeArgs>) -> CallToolResult {
        let node = match node_ref(&args.node_type, &args.external_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        let mut features = self.graph.features();
        match features.get_stored_node_feature_vector(node).await {
            Ok(vector) => {
                let slots: Vec<Value> = vector
                    .slots
                    .into_iter()
                    .filter(|slot| !slot.name.is_empty() && !slot.name.starts_with("unused_"))
                    .map(|slot| {
                        json!({
                            "index": slot.index,
                            "name": slot.name,
                            "raw": slot.raw,
                            "effective": slot.effective,
                            "support": slot.support,
                            "used_fallback": slot.used_fallback,
                        })
                    })
                    .collect();
                tool_ok(json!({
                    "node_type": args.node_type,
                    "external_id": args.external_id,
                    "schema_version": vector.schema_version,
                    "confidence": vector.confidence,
                    "age_secs": vector.age_secs,
                    "quality_flags": vector.quality_flags,
                    "slots": slots,
                }))
            }
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "Customers or other nodes that share neighbors with this node on the given edge types. The score is weighted overlap. This call does not write SIMILAR_TO edges. k is at most 25."
    )]
    async fn find_similar(&self, Parameters(args): Parameters<SimilarArgs>) -> CallToolResult {
        let k = match guard::clamp_page(args.k, 10, MAX_SIMILAR) {
            Ok(k) => k,
            Err(error) => return tool_err(error.replace("limit", "k")),
        };
        if args.edge_weights.is_empty() {
            return tool_err("edge_weights needs at least one edge type");
        }
        if !args.min_similarity.is_finite() || args.min_similarity < 0.0 {
            return tool_err("min_similarity must be zero or greater");
        }
        let mut weights = Vec::with_capacity(args.edge_weights.len());
        for weight in &args.edge_weights {
            if weight.edge_type.is_empty() || !weight.weight.is_finite() || weight.weight < 0.0 {
                return tool_err("each edge weight needs an edge type and a non-negative weight");
            }
            weights.push(EdgeTypeWeight::new(weight.edge_type.clone(), weight.weight));
        }
        let node = match node_ref(&args.node_type, &args.external_id) {
            Ok(node) => node,
            Err(error) => return tool_err(error),
        };
        let required: Vec<&str> = args
            .required_edge_types
            .iter()
            .map(String::as_str)
            .collect();
        let found = match self
            .graph
            .find_similar_nodes(
                node,
                &weights,
                &required,
                &[],
                &[],
                k,
                args.min_similarity,
                false,
                "",
            )
            .await
        {
            Ok(found) => found,
            Err(error) => return tool_err(error.to_string()),
        };
        let mut similar = Vec::with_capacity(found.similar_nodes.len());
        for item in found.similar_nodes {
            let identity = self.identity(item.node_id).await;
            similar.push(json!({
                "node_id": item.node_id,
                "node_type": identity.as_ref().map(|value| value.0.clone()),
                "external_id": identity.and_then(|value| value.1),
                "similarity": item.similarity,
                "shared_neighbors": item.shared_neighbors,
            }));
        }
        tool_ok(json!({
            "edge_weights": args.edge_weights.iter().map(|weight| json!({
                "edge_type": weight.edge_type,
                "weight": weight.weight,
            })).collect::<Vec<_>>(),
            "required_edge_types": args.required_edge_types,
            "min_similarity": args.min_similarity,
            "k": k,
            "similar": similar,
        }))
    }

    #[tool(
        description = "One read-only Cypher statement for a pattern the other tools do not express. CREATE, MERGE, DELETE, SET, REMOVE, and DROP are rejected. LIMIT above 100 is rejected; a missing LIMIT becomes 100."
    )]
    async fn cypher_read(&self, Parameters(args): Parameters<CypherArgs>) -> CallToolResult {
        let query = match guard::prepare_cypher(&args.query) {
            Ok(query) => query,
            Err(error) => return tool_err(error),
        };
        let parameters = if args.parameters.is_null() {
            json!({})
        } else if args.parameters.is_object() {
            args.parameters
        } else {
            return tool_err("parameters must be an object");
        };
        match self.run_cypher(&query, &parameters).await {
            Ok(body) => tool_ok(json!({
                "query": query,
                "columns": body.get("columns").cloned().unwrap_or(Value::Null),
                "rows": body.get("rows").cloned().unwrap_or(Value::Null),
                "elapsed_us": body.get("elapsed_us").cloned().unwrap_or(Value::Null),
                "truncated": body.get("truncated").cloned().unwrap_or(Value::Bool(false)),
            })),
            Err(error) => tool_err(error),
        }
    }

    #[tool(description = "Signal definitions already stored by the segment evaluator.")]
    async fn list_signals(&self) -> CallToolResult {
        match self.segments.list_signals().await {
            Ok(signals) => match serde_json::to_value(&signals) {
                Ok(signals) => tool_ok(json!({ "signals": signals })),
                Err(error) => tool_err(error.to_string()),
            },
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(description = "Segment definitions already stored by the segment evaluator.")]
    async fn list_segments(&self) -> CallToolResult {
        match self.segments.list_segments().await {
            Ok(segments) => match serde_json::to_value(&segments) {
                Ok(segments) => tool_ok(json!({ "segments": segments })),
                Err(error) => tool_err(error.to_string()),
            },
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "Store a signal. The query is read-only, binds $node, and returns one column named value. A signal with the same name is replaced. This changes evaluator configuration, not the graph."
    )]
    async fn create_signal(
        &self,
        Parameters(args): Parameters<CreateSignalArgs>,
    ) -> CallToolResult {
        if let Err(error) = guard::require_name(&args.name) {
            return tool_err(error);
        }
        if let Err(error) = guard::validate_signal_query(&args.query) {
            return tool_err(error);
        }
        let value_kind = match args.value_kind.as_str() {
            "number" => SignalValueKind::Number,
            "epoch_secs" => SignalValueKind::EpochSecs,
            other => {
                return tool_err(format!(
                    "value_kind must be number or epoch_secs, got {other}"
                ))
            }
        };
        let definition =
            SignalDefinition::new(args.name, args.query, value_kind, args.time_sensitive);
        match self.segments.create_signal(&definition).await {
            Ok(()) => match serde_json::to_value(&definition) {
                Ok(definition) => tool_ok(json!({
                    "stored": true,
                    "definition": definition,
                })),
                Err(error) => tool_err(error.to_string()),
            },
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "Store a segment over existing signals. A segment with the same name is replaced. This changes evaluator configuration, not the graph. Membership appears in list_segment_members only after the evaluator has processed those nodes."
    )]
    async fn create_segment(
        &self,
        Parameters(args): Parameters<CreateSegmentArgs>,
    ) -> CallToolResult {
        if let Err(error) = guard::require_name(&args.name) {
            return tool_err(error);
        }
        if args.entity_type.trim().is_empty() {
            return tool_err("entity_type is required");
        }
        if args.conditions.is_empty() {
            return tool_err("a segment needs at least one condition");
        }
        let logic = match args.logic.as_str() {
            "all" => SegmentLogic::All,
            "any" => SegmentLogic::Any,
            "weighted" => SegmentLogic::Weighted,
            other => return tool_err(format!("logic must be all, any, or weighted, got {other}")),
        };
        let mut conditions = Vec::with_capacity(args.conditions.len());
        for condition in args.conditions {
            if let Err(error) = guard::require_name(&condition.signal) {
                return tool_err(error);
            }
            let op = match condition.op.as_str() {
                "gt" => ConditionOp::Gt,
                "lt" => ConditionOp::Lt,
                "gte" => ConditionOp::Gte,
                "lte" => ConditionOp::Lte,
                "eq" => ConditionOp::Eq,
                "between" => ConditionOp::Between,
                other => {
                    return tool_err(format!(
                        "op must be gt, lt, gte, lte, eq, or between, got {other}"
                    ))
                }
            };
            if op == ConditionOp::Between && condition.threshold_max.is_none() {
                return tool_err("between requires threshold_max");
            }
            let mut built =
                SegmentCondition::new(condition.signal, op, condition.threshold, condition.reason);
            built.threshold_max = condition.threshold_max;
            if let Some(weight) = condition.weight {
                built.weight = weight;
            }
            conditions.push(built);
        }
        let definition = SegmentDefinition {
            name: args.name,
            label: args.label,
            entity_type: args.entity_type,
            logic,
            min_confidence: args.min_confidence.unwrap_or(0.5),
            modes: vec![
                jetgraph_client::EvalMode::Realtime,
                jetgraph_client::EvalMode::Batch,
            ],
            conditions,
        };
        match self.segments.create_segment(&definition).await {
            Ok(()) => match serde_json::to_value(&definition) {
                Ok(definition) => tool_ok(json!({
                    "stored": true,
                    "definition": definition,
                })),
                Err(error) => tool_err(error.to_string()),
            },
            Err(error) => tool_err(error.to_string()),
        }
    }

    #[tool(
        description = "Customers already linked to a segment by the evaluator. A segment created moments ago can be empty here until the evaluator processes nodes. Maximum 100."
    )]
    async fn list_segment_members(
        &self,
        Parameters(args): Parameters<MembersArgs>,
    ) -> CallToolResult {
        if let Err(error) = guard::require_name(&args.name) {
            return tool_err(error);
        }
        let limit = match guard::clamp_page(args.limit, DEFAULT_PAGE, MAX_PAGE) {
            Ok(limit) => limit,
            Err(error) => return tool_err(error),
        };
        match self
            .graph
            .segment()
            .get_segment_members(NodeRef::external("segment", &args.name), limit, args.cursor)
            .await
        {
            Ok((members, has_more)) => {
                let next_cursor = members.last().map(|member| member.customer_node_id);
                let members: Vec<Value> = members
                    .into_iter()
                    .map(|member| {
                        json!({
                            "external_id": member.external_id,
                            "node_id": member.customer_node_id,
                            "confidence": member.confidence,
                            "last_seen_secs": member.last_seen_secs,
                        })
                    })
                    .collect();
                tool_ok(json!({
                    "segment": args.name,
                    "members": members,
                    "has_more": has_more,
                    "next_cursor": next_cursor,
                    "note": "These are customers the evaluator has already linked. A new segment stays empty until that processing happens.",
                }))
            }
            Err(error) => tool_err(error.to_string()),
        }
    }

    async fn identity(&self, node_id: u64) -> Option<(String, Option<String>)> {
        self.graph
            .graph()
            .get_node(NodeRef::node_id(node_id))
            .await
            .ok()
            .map(|node| (node.node_type, node.external_id))
    }

    async fn run_cypher(&self, query: &str, parameters: &Value) -> Result<Value, String> {
        let response = self
            .http
            .post(&self.cypher_url)
            .json(&json!({ "query": query, "parameters": parameters }))
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let body: Value = response.json().await.map_err(|error| error.to_string())?;
        if !status.is_success() {
            let message = body
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("cypher query failed");
            return Err(message.to_string());
        }
        Ok(body)
    }
}

#[prompt_router]
impl Jetgraph {
    /// Order of work for an investigation. The question itself is not part of this prompt.
    #[prompt(
        name = "investigate",
        description = "How to investigate a JetGraph question with these tools."
    )]
    fn investigate(&self) -> Vec<PromptMessage> {
        vec![PromptMessage::new_text(Role::User, INSTRUCTIONS)]
    }
}

#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for Jetgraph {
    fn get_info(&self) -> rmcp::model::ServerConfig {
        rmcp::model::ServerConfig::new(
            rmcp::model::ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .build(),
        )
        .with_server_info(rmcp::model::Implementation::new("jetgraph", "0.1.0"))
        .with_instructions(INSTRUCTIONS)
    }
}

fn tool_ok(value: Value) -> CallToolResult {
    CallToolResult::success(vec![ContentBlock::text(value.to_string())])
}

fn tool_err(message: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message.into())])
}

fn node_ref(node_type: &str, external_id: &str) -> Result<NodeRef, String> {
    if node_type.trim().is_empty() || external_id.trim().is_empty() {
        return Err("node_type and external_id are required".into());
    }
    Ok(NodeRef::external(node_type, external_id))
}

fn direction(value: &str) -> Result<bool, String> {
    match value {
        "out" => Ok(true),
        "in" => Ok(false),
        other => Err(format!("direction must be out or in, got {other}")),
    }
}

fn feature_slot(slot: &jetgraph_client::NodeFeatureSlotSpec) -> Value {
    let source = NodeFeatureSourceKind::try_from(slot.source_kind).ok();
    json!({
        "index": slot.index,
        "name": slot.name,
        "source": source.map(source_label).unwrap_or("unknown"),
        "edge_type": empty_as_none(&slot.edge_type),
        "direction": FeatureDirection::try_from(slot.direction).ok().map(direction_label),
        "metric": EdgeFeatureMetric::try_from(slot.edge_metric).ok().map(metric_label),
        "window_secs": slot.window_secs,
        "node_property": empty_as_none(&slot.node_property),
        "fraud_case": FraudCaseMetric::try_from(slot.fraud_case_metric).ok().map(fraud_label),
    })
}

fn source_label(kind: NodeFeatureSourceKind) -> &'static str {
    match kind {
        NodeFeatureSourceKind::NodeFeatureSourceUnused => "unused",
        NodeFeatureSourceKind::NodeFeatureSourceConstant => "constant",
        NodeFeatureSourceKind::NodeFeatureSourceNodeProperty => "node_property",
        NodeFeatureSourceKind::NodeFeatureSourceEdgeAggregate => "edge_aggregate",
        NodeFeatureSourceKind::NodeFeatureSourceFraudCase => "fraud_case",
        NodeFeatureSourceKind::NodeFeatureSourceNodeAge => "node_age",
        NodeFeatureSourceKind::NodeFeatureSourceConfidence => "confidence",
    }
}

fn direction_label(direction: FeatureDirection) -> &'static str {
    match direction {
        FeatureDirection::Out => "out",
        FeatureDirection::In => "in",
        FeatureDirection::Both => "both",
    }
}

fn metric_label(metric: EdgeFeatureMetric) -> &'static str {
    match metric {
        EdgeFeatureMetric::EdgeFeatureEventCount => "event_count",
        EdgeFeatureMetric::EdgeFeatureValueSum => "value_sum",
        EdgeFeatureMetric::EdgeFeatureValueMean => "value_mean",
        EdgeFeatureMetric::EdgeFeatureActiveDegree => "active_degree",
        EdgeFeatureMetric::EdgeFeatureLastSeen => "last_seen",
        EdgeFeatureMetric::EdgeFeatureHistogramCount => "histogram_count",
        EdgeFeatureMetric::EdgeFeatureNeighborEntropy => "neighbor_entropy",
        EdgeFeatureMetric::EdgeFeatureActivityTickCount => "activity_tick_count",
    }
}

fn fraud_label(metric: FraudCaseMetric) -> &'static str {
    match metric {
        FraudCaseMetric::Count => "count",
        FraudCaseMetric::MaxScore => "max_score",
        FraudCaseMetric::LastSeen => "last_seen",
    }
}

fn empty_as_none(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Jetgraph::from_env().map_err(|error| {
        anyhow::anyhow!("jetgraph-mcp could not read its configuration: {error}")
    })?;
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod handshake {
    use rmcp::model::{GetPromptRequestParams, PaginatedRequestParams};
    use rmcp::{ClientHandler, ServerHandler, ServiceExt};

    use super::Jetgraph;

    #[derive(Default)]
    struct NopClient;

    impl ClientHandler for NopClient {}

    #[tokio::test]
    async fn lists_investigation_tools_and_the_prompt() -> anyhow::Result<()> {
        let (server_transport, client_transport) = tokio::io::duplex(4096);
        let server = Jetgraph::from_env()?;
        let instructions = server.get_info().instructions.unwrap_or_default();
        assert!(instructions.contains("business_meaning"));
        assert!(server.get_info().capabilities.prompts.is_some());
        let server_task = tokio::spawn(async move {
            server.serve(server_transport).await?.waiting().await?;
            anyhow::Ok(())
        });

        let client = NopClient.serve(client_transport).await?;
        let tools = client.list_tools(None::<PaginatedRequestParams>).await?;
        let names: Vec<&str> = tools.tools.iter().map(|tool| tool.name.as_ref()).collect();
        for required in [
            "get_schema",
            "get_neighbors",
            "get_neighbor_count",
            "get_last_neighbor",
            "get_edge_state",
            "get_histogram",
            "get_feature_vector",
            "find_similar",
            "cypher_read",
            "list_signals",
            "list_segments",
            "create_signal",
            "create_segment",
            "list_segment_members",
        ] {
            assert!(names.contains(&required), "missing tool {required}");
        }

        let prompts = client.list_prompts(None).await?;
        assert!(prompts
            .prompts
            .iter()
            .any(|prompt| prompt.name == "investigate"));
        let prompt = client
            .get_prompt(GetPromptRequestParams::new("investigate"))
            .await?;
        let text = prompt
            .messages
            .first()
            .and_then(|message| message.content.as_text())
            .map(|text| text.text.as_str())
            .unwrap_or("");
        assert!(text.contains("get_schema"));
        assert!(text.contains("business_meaning"));

        client.cancel().await?;
        server_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn reads_the_live_schema_when_the_engine_is_up() -> anyhow::Result<()> {
        if std::env::var("JETGRAPH_LIVE").ok().as_deref() != Some("1") {
            return Ok(());
        }
        let server = Jetgraph::from_env()?;
        let result = server.get_schema().await;
        let text = result
            .content
            .first()
            .and_then(|block| block.as_text())
            .map(|text| text.text.clone())
            .unwrap_or_default();
        assert!(result.is_error != Some(true), "get_schema failed: {text}");
        let body: serde_json::Value = serde_json::from_str(&text)?;
        assert!(body.get("node_types").is_some());
        assert!(body.get("edge_types").is_some());
        Ok(())
    }
}
