//! Create, list, update, and delete signal and segment definitions.
//!
//! Definitions are stored by the segment evaluator (`http://localhost:8081` by
//! default), not by the graph engine. Create and update both send the full
//! definition; the evaluator replaces whatever is stored under that name.

use serde::{Deserialize, Serialize};

use crate::ClientError;

const DEFAULT_EVALUATOR: &str = "http://localhost:8081";

/// How a signal query result is turned into the number a condition compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalValueKind {
    /// A missing value compares as 0.
    #[default]
    Number,
    /// `value` is a unix timestamp. The evaluator compares days since that time.
    EpochSecs,
}

/// A signal definition. `query` is read-only Cypher that binds `$node` and
/// returns one column named `value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalDefinition {
    pub name: String,
    pub query: String,
    #[serde(default)]
    pub value_kind: SignalValueKind,
    #[serde(default)]
    pub time_sensitive: bool,
}

impl SignalDefinition {
    pub fn new(
        name: impl Into<String>,
        query: impl Into<String>,
        value_kind: SignalValueKind,
        time_sensitive: bool,
    ) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            value_kind,
            time_sensitive,
        }
    }
}

/// How a segment combines its conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentLogic {
    /// Every condition must be true.
    #[default]
    All,
    /// One true condition is enough.
    Any,
    /// Membership starts when the weighted score reaches `min_confidence`.
    Weighted,
}

/// Which sweeps include this segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalMode {
    Realtime,
    Batch,
}

/// Comparison used by one condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOp {
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
    Between,
}

/// One condition inside a segment definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentCondition {
    pub signal: String,
    pub op: ConditionOp,
    pub threshold: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold_max: Option<f64>,
    #[serde(default = "default_weight")]
    pub weight: f32,
    #[serde(default)]
    pub reason: String,
}

fn default_weight() -> f32 {
    1.0
}

impl SegmentCondition {
    pub fn new(
        signal: impl Into<String>,
        op: ConditionOp,
        threshold: f64,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            signal: signal.into(),
            op,
            threshold,
            threshold_max: None,
            weight: 1.0,
            reason: reason.into(),
        }
    }
}

/// A segment definition stored by the evaluator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentDefinition {
    pub name: String,
    #[serde(default)]
    pub label: String,
    #[serde(default = "default_entity_type")]
    pub entity_type: String,
    #[serde(default)]
    pub logic: SegmentLogic,
    #[serde(default = "default_min_confidence")]
    pub min_confidence: f32,
    #[serde(default = "default_modes")]
    pub modes: Vec<EvalMode>,
    #[serde(default)]
    pub conditions: Vec<SegmentCondition>,
}

fn default_entity_type() -> String {
    "CUSTOMER".into()
}

fn default_min_confidence() -> f32 {
    0.5
}

fn default_modes() -> Vec<EvalMode> {
    vec![EvalMode::Realtime, EvalMode::Batch]
}

impl Default for SegmentDefinition {
    fn default() -> Self {
        Self {
            name: String::new(),
            label: String::new(),
            entity_type: default_entity_type(),
            logic: SegmentLogic::All,
            min_confidence: default_min_confidence(),
            modes: default_modes(),
            conditions: Vec::new(),
        }
    }
}

/// HTTP client for the segment evaluator config API.
#[derive(Clone, Debug)]
pub struct SegmentConfigClient {
    base_url: String,
    http: reqwest::Client,
}

impl SegmentConfigClient {
    /// Connect to a segment evaluator. `base_url` is the evaluator origin,
    /// for example `http://localhost:8081`.
    pub fn connect(base_url: impl Into<String>) -> Self {
        let mut base_url = base_url.into();
        while base_url.ends_with('/') {
            base_url.pop();
        }
        Self {
            base_url,
            http: reqwest::Client::new(),
        }
    }

    /// Connect to the evaluator on `http://localhost:8081`.
    pub fn connect_default() -> Self {
        Self::connect(DEFAULT_EVALUATOR)
    }

    pub async fn list_signals(&self) -> Result<Vec<SignalDefinition>, ClientError> {
        let body: ListSignals = self.get("/config/signals").await?;
        Ok(body.signals)
    }

    /// Store a new signal. A signal with the same name is replaced.
    pub async fn create_signal(&self, definition: &SignalDefinition) -> Result<(), ClientError> {
        self.upsert_signal(definition).await
    }

    /// Replace the signal stored under `definition.name`.
    pub async fn update_signal(&self, definition: &SignalDefinition) -> Result<(), ClientError> {
        self.upsert_signal(definition).await
    }

    pub async fn delete_signal(&self, name: &str) -> Result<(), ClientError> {
        self.require_name(name)?;
        self.delete(&format!("/config/signals/{}", encode_path(name)))
            .await
    }

    pub async fn list_segments(&self) -> Result<Vec<SegmentDefinition>, ClientError> {
        let body: ListSegments = self.get("/config/segments").await?;
        Ok(body.segments)
    }

    /// Store a new segment. A segment with the same name is replaced.
    pub async fn create_segment(&self, definition: &SegmentDefinition) -> Result<(), ClientError> {
        self.upsert_segment(definition).await
    }

    /// Replace the segment stored under `definition.name`.
    pub async fn update_segment(&self, definition: &SegmentDefinition) -> Result<(), ClientError> {
        self.upsert_segment(definition).await
    }

    pub async fn delete_segment(&self, name: &str) -> Result<(), ClientError> {
        self.require_name(name)?;
        self.delete(&format!("/config/segments/{}", encode_path(name)))
            .await
    }

    async fn upsert_signal(&self, definition: &SignalDefinition) -> Result<(), ClientError> {
        self.require_name(&definition.name)?;
        if definition.query.trim().is_empty() {
            return Err(ClientError::SegmentConfig {
                status: 400,
                message: "signal query is required".into(),
            });
        }
        self.post("/config/signals", definition).await
    }

    async fn upsert_segment(&self, definition: &SegmentDefinition) -> Result<(), ClientError> {
        self.require_name(&definition.name)?;
        if definition.conditions.is_empty() {
            return Err(ClientError::SegmentConfig {
                status: 400,
                message: "a segment needs at least one condition".into(),
            });
        }
        self.post("/config/segments", definition).await
    }

    fn require_name(&self, name: &str) -> Result<(), ClientError> {
        if name.trim().is_empty() {
            Err(ClientError::SegmentConfig {
                status: 400,
                message: "name is required".into(),
            })
        } else {
            Ok(())
        }
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let response = self
            .http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .map_err(|error| ClientError::Internal(error.to_string()))?;
        decode(response).await
    }

    async fn post<B: Serialize>(&self, path: &str, body: &B) -> Result<(), ClientError> {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .json(body)
            .send()
            .await
            .map_err(|error| ClientError::Internal(error.to_string()))?;
        let _: serde_json::Value = decode(response).await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), ClientError> {
        let response = self
            .http
            .delete(format!("{}{path}", self.base_url))
            .send()
            .await
            .map_err(|error| ClientError::Internal(error.to_string()))?;
        let _: serde_json::Value = decode(response).await?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct ListSignals {
    signals: Vec<SignalDefinition>,
}

#[derive(Deserialize)]
struct ListSegments {
    segments: Vec<SegmentDefinition>,
}

#[derive(Deserialize)]
struct ErrorBody {
    error: String,
}

async fn decode<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, ClientError> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ClientError::Internal(error.to_string()))?;
    if !status.is_success() {
        let message = serde_json::from_str::<ErrorBody>(&text)
            .map(|body| body.error)
            .unwrap_or(text);
        return Err(ClientError::SegmentConfig {
            status: status.as_u16(),
            message,
        });
    }
    serde_json::from_str(&text).map_err(|error| ClientError::Internal(error.to_string()))
}

fn encode_path(name: &str) -> String {
    let mut encoded = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_and_segment_json_matches_the_evaluator() {
        let signal = SignalDefinition::new(
            "churn_score",
            "MATCH (n) WHERE id(n) = $node RETURN n.churn_score AS value",
            SignalValueKind::Number,
            false,
        );
        let json = serde_json::to_value(&signal).unwrap();
        assert_eq!(json["value_kind"], "number");
        assert_eq!(json["time_sensitive"], false);

        let segment = SegmentDefinition {
            name: "churn_risk".into(),
            label: "Churn risk".into(),
            entity_type: "SUBSCRIBER".into(),
            logic: SegmentLogic::Any,
            min_confidence: 0.7,
            modes: vec![EvalMode::Realtime],
            conditions: vec![SegmentCondition {
                threshold_max: Some(1.0),
                ..SegmentCondition::new("churn_score", ConditionOp::Between, 0.75, "Churn score {value}")
            }],
        };
        let json = serde_json::to_value(&segment).unwrap();
        assert_eq!(json["logic"], "any");
        assert_eq!(json["modes"][0], "realtime");
        assert_eq!(json["conditions"][0]["op"], "between");
        assert_eq!(json["conditions"][0]["threshold_max"], 1.0);

        let parsed: SegmentDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, segment);
    }

    #[test]
    fn path_encoding_keeps_names_in_one_segment() {
        assert_eq!(encode_path("churn_risk"), "churn_risk");
        assert_eq!(encode_path("a/b"), "a%2Fb");
    }

    #[tokio::test]
    async fn live_config_round_trip() {
        let Ok(url) = std::env::var("SEGMENT_EVALUATOR_URL") else {
            return;
        };
        let client = SegmentConfigClient::connect(url);
        let signal_name = "client_probe_signal";
        let segment_name = "client_probe_segment";
        let _ = client.delete_signal(signal_name).await;
        let _ = client.delete_segment(segment_name).await;

        let signal = SignalDefinition::new(
            signal_name,
            "MATCH (n) WHERE id(n) = $node RETURN 1 AS value",
            SignalValueKind::Number,
            false,
        );
        client.create_signal(&signal).await.unwrap();
        let mut updated = signal.clone();
        updated.time_sensitive = true;
        client.update_signal(&updated).await.unwrap();
        let signals = client.list_signals().await.unwrap();
        let stored = signals.iter().find(|item| item.name == signal_name).unwrap();
        assert!(stored.time_sensitive);

        let segment = SegmentDefinition {
            name: segment_name.into(),
            label: "Client probe".into(),
            entity_type: "SUBSCRIBER".into(),
            conditions: vec![SegmentCondition::new(signal_name, ConditionOp::Eq, 1.0, "probe")],
            ..SegmentDefinition::default()
        };
        client.create_segment(&segment).await.unwrap();
        let mut renamed_label = segment.clone();
        renamed_label.label = "Client probe updated".into();
        client.update_segment(&renamed_label).await.unwrap();
        let segments = client.list_segments().await.unwrap();
        let stored = segments.iter().find(|item| item.name == segment_name).unwrap();
        assert_eq!(stored.label, "Client probe updated");

        client.delete_segment(segment_name).await.unwrap();
        client.delete_signal(signal_name).await.unwrap();
        let segments = client.list_segments().await.unwrap();
        assert!(segments.iter().all(|item| item.name != segment_name));
        let signals = client.list_signals().await.unwrap();
        assert!(signals.iter().all(|item| item.name != signal_name));
    }
}
