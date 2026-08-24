use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeKind {
    User,
    Project,
    Thread,
    Task,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scope {
    pub id: String,
    pub kind: ScopeKind,
    pub parent_id: Option<String>,
    pub name: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EventKind {
    UserMessage,
    AssistantMessage,
    ToolCall,
    ToolResult,
    FileReference,
    SystemEvent,
    MemoryCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Sensitivity {
    Normal,
    Secret,
    DoNotStore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEvent {
    pub id: String,
    pub stream_id: String,
    pub sequence: i64,
    pub scope_id: String,
    pub kind: EventKind,
    pub actor_id: Option<String>,
    pub occurred_at: String,
    pub recorded_at: String,
    pub content: Value,
    pub content_hash: String,
    pub token_count: i64,
    pub sensitivity: Sensitivity,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewEvent {
    pub scope_id: String,
    pub stream_id: String,
    pub kind: EventKind,
    pub actor_id: Option<String>,
    pub occurred_at: Option<String>,
    pub content: Value,
    pub token_count: Option<i64>,
    pub sensitivity: Sensitivity,
    pub metadata: Value,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ObservationKind {
    Event,
    Decision,
    Outcome,
    Failure,
    Constraint,
    Preference,
    OpenLoop,
    Relationship,
    Continuation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub id: String,
    pub run_id: String,
    pub scope_id: String,
    pub kind: ObservationKind,
    pub content: String,
    pub importance: f64,
    pub confidence: f64,
    pub event_time_from: Option<String>,
    pub event_time_to: Option<String>,
    pub source_start_sequence: i64,
    pub source_end_sequence: i64,
    pub observer_model: String,
    pub prompt_version: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimKind {
    Fact,
    Preference,
    Decision,
    Goal,
    Commitment,
    Constraint,
    OpenLoop,
    EntityAlias,
    Relationship,
    Hypothesis,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimCardinality {
    #[default]
    Single,
    Set,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimModality {
    ExplicitAssertion,
    AcceptedDecision,
    Proposal,
    Inference,
    Observation,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimStatus {
    Pending,
    Active,
    Disputed,
    Superseded,
    Rejected,
    Expired,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimAuthority {
    ExplicitUser,
    TrustedSource,
    ModelInference,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    pub id: String,
    pub origin_run_id: Option<String>,
    pub scope_id: String,
    pub kind: ClaimKind,
    pub subject: String,
    pub predicate: String,
    pub cardinality: ClaimCardinality,
    pub value: Value,
    pub value_hash: String,
    pub modality: ClaimModality,
    pub status: ClaimStatus,
    pub authority: ClaimAuthority,
    pub confidence: f64,
    pub supersedes_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ViewKind {
    Continuity,
    Continuation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryView {
    pub id: String,
    pub scope_id: String,
    pub stream_id: String,
    pub kind: ViewKind,
    pub generation: i64,
    pub content: String,
    pub source_from_sequence: i64,
    pub source_through_sequence: i64,
    pub previous_view_id: Option<String>,
    pub model: Option<String>,
    pub prompt_version: Option<String>,
    pub token_count: i64,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationPlan {
    pub run_id: String,
    pub scope: Scope,
    pub stream_id: String,
    pub from_sequence: i64,
    pub to_sequence: i64,
    pub events: Vec<MemoryEvent>,
    pub active_claims: Vec<Claim>,
    pub previous_continuation: Option<MemoryView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum ObservationPlanOutcome {
    Ready {
        #[serde(flatten)]
        plan: Box<ObservationPlan>,
        next_action: String,
    },
    CaughtUp {
        scope_id: String,
        stream_id: String,
        observed_through_sequence: i64,
        next_action: String,
    },
}

impl ObservationPlanOutcome {
    pub fn ready(plan: ObservationPlan) -> Self {
        Self::Ready {
            next_action: format!(
                "produce a strict ObserverResult for run {} and commit it",
                plan.run_id
            ),
            plan: Box::new(plan),
        }
    }

    pub fn caught_up(scope_id: &str, stream_id: &str, cursor: i64) -> Self {
        Self::CaughtUp {
            scope_id: scope_id.to_owned(),
            stream_id: stream_id.to_owned(),
            observed_through_sequence: cursor,
            next_action:
                "append new evidence or wait for new events; use a new idempotency key for the next plan"
                    .to_owned(),
        }
    }

    pub fn into_plan(self) -> Option<ObservationPlan> {
        match self {
            Self::Ready { plan, .. } => Some(*plan),
            Self::CaughtUp { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ObservationDraft {
    pub kind: ObservationKind,
    pub content: String,
    pub importance: f64,
    pub confidence: f64,
    pub source_event_ids: Vec<String>,
    pub event_time_from: Option<String>,
    pub event_time_to: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ClaimDraft {
    pub kind: ClaimKind,
    pub subject: String,
    pub predicate: String,
    #[serde(default)]
    pub cardinality: ClaimCardinality,
    pub value: Value,
    pub modality: ClaimModality,
    pub confidence: f64,
    pub source_event_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ContinuationDraft {
    pub current_task: Option<String>,
    pub completed: Vec<String>,
    pub blockers: Vec<String>,
    pub next_actions: Vec<String>,
    pub unresolved_questions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct AmbiguityDraft {
    pub description: String,
    pub source_event_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ObserverResult {
    pub observations: Vec<ObservationDraft>,
    pub claims: Vec<ClaimDraft>,
    pub continuation: ContinuationDraft,
    pub ambiguities: Vec<AmbiguityDraft>,
    pub empty_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationCommit {
    pub run_id: String,
    pub observations: Vec<Observation>,
    pub claims: Vec<Claim>,
    pub continuation_view: MemoryView,
    pub continuation_action: ContinuationAction,
    pub ambiguities: Vec<AmbiguityDraft>,
    pub next_required_action: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ContinuationAction {
    Created,
    Preserved,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationSummary {
    pub activated: Vec<String>,
    pub disputed: Vec<String>,
    pub duplicates_rejected: Vec<String>,
    pub left_pending: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextDiagnostics {
    pub estimated_tokens: i64,
    pub omitted_items: Vec<OmittedItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmittedItem {
    pub id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextBundle {
    pub claims: Vec<Claim>,
    pub pending_claims: Vec<Claim>,
    pub continuation: Option<ContinuationDraft>,
    pub continuity_views: Vec<MemoryView>,
    pub observations: Vec<Observation>,
    pub recent_events: Vec<MemoryEvent>,
    pub recalled_evidence: Vec<MemoryEvent>,
    pub diagnostics: ContextDiagnostics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    pub record_type: String,
    pub id: String,
    pub scope_id: String,
    pub text: String,
    pub rank: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadAccess {
    pub anchor_scope_id: String,
    pub reveal_secrets: bool,
}

impl ReadAccess {
    pub fn agent(anchor_scope_id: impl Into<String>) -> Self {
        Self {
            anchor_scope_id: anchor_scope_id.into(),
            reveal_secrets: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimExplanation {
    pub claim: Claim,
    pub source_events: Vec<MemoryEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationExplanation {
    pub observation: Observation,
    pub source_events: Vec<MemoryEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationRunInfo {
    pub id: String,
    pub scope_id: String,
    pub stream_id: String,
    pub cursor_at_plan: i64,
    pub from_sequence: i64,
    pub to_sequence: i64,
    pub status: String,
    pub source_integrity: SourceIntegrity,
    pub observer_model: String,
    pub prompt_version: String,
    pub ambiguities: Vec<AmbiguityDraft>,
    pub error: Option<String>,
    pub next_action: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SourceIntegrity {
    Intact,
    PrivacyPurged,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamStatus {
    pub id: String,
    pub scope_id: String,
    pub observed_through_sequence: i64,
    pub next_sequence: i64,
    pub last_sequence: Option<i64>,
    pub runs: Vec<ObservationRunInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationMetadata {
    pub replayed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationResult<T> {
    pub data: T,
    pub operation: OperationMetadata,
}

impl<T> MutationResult<T> {
    pub fn created(data: T) -> Self {
        Self {
            data,
            operation: OperationMetadata { replayed: false },
        }
    }

    pub fn replayed(data: T) -> Self {
        Self {
            data,
            operation: OperationMetadata { replayed: true },
        }
    }
}

impl<T> std::ops::Deref for MutationResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub fn enum_text<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .expect("serializing an enum cannot fail")
        .as_str()
        .expect("enum serialization must be a string")
        .to_owned()
}

pub fn parse_enum<T: for<'de> Deserialize<'de>>(value: &str) -> rusqlite::Result<T> {
    serde_json::from_value(Value::String(value.to_owned())).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}
