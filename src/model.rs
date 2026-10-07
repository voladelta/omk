use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

impl MemoryEvent {
    pub(crate) fn compact_model_record(&self) -> Value {
        let mut record = json!({
            "id": self.id,
            "streamId": self.stream_id,
            "sequence": self.sequence,
            "scopeId": self.scope_id,
            "kind": self.kind,
            "occurredAt": self.occurred_at,
            "content": self.content,
            "sensitivity": self.sensitivity,
        });

        if let Some(actor) = &self.actor_id {
            record["actorId"] = json!(actor);
        }
        if self
            .metadata
            .as_object()
            .is_none_or(|metadata| !metadata.is_empty())
        {
            record["metadata"] = self.metadata.clone();
        }

        record
    }
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

impl Observation {
    pub(crate) fn compact_model_record(&self) -> Value {
        let mut record = json!({
            "id": self.id,
            "scopeId": self.scope_id,
            "kind": self.kind,
            "content": self.content,
            "confidence": self.confidence,
        });

        if let Some(start) = &self.event_time_from {
            record["eventTimeFrom"] = json!(start);
        }
        if let Some(end) = &self.event_time_to {
            record["eventTimeTo"] = json!(end);
        }

        record
    }
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

impl Claim {
    pub(crate) fn compact_model_record(&self) -> Value {
        let mut record = json!({
            "id": self.id,
            "scopeId": self.scope_id,
            "kind": self.kind,
            "subject": self.subject,
            "predicate": self.predicate,
            "cardinality": self.cardinality,
            "value": self.value,
            "modality": self.modality,
            "status": self.status,
            "authority": self.authority,
            "confidence": self.confidence,
        });

        if let Some(supersedes) = &self.supersedes_id {
            record["supersedesId"] = json!(supersedes);
        }

        record
    }
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

impl MemoryView {
    pub(crate) fn compact_model_record(&self) -> Value {
        json!({
            "id": self.id,
            "scopeId": self.scope_id,
            "kind": self.kind,
            "content": self.content,
            "sourceFromSequence": self.source_from_sequence,
            "sourceThroughSequence": self.source_through_sequence,
        })
    }
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

impl ObservationPlan {
    /// Model input only. Run identifiers and command instructions are envelopes.
    pub fn model_payload(&self) -> Value {
        serde_json::json!({"scope": self.scope, "events": self.events,
            "activeClaims": self.active_claims, "previousContinuation": self.previous_continuation})
    }
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
    #[serde(default)]
    pub truncated: bool,
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
    /// BM25 multiplied by the record boost; lower sorts first.
    pub rank: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_status: Option<ClaimStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
}

/// One bounded search answer. `matched` counts every hit the filters allow.
/// On an empty page, `searchable` counts the records the filters allow before
/// the query, separating "no match" from "nothing here to match".
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    pub shown: usize,
    pub matched: usize,
    /// Counted only when `matched` is 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub searchable: Option<usize>,
    pub next_action: Option<String>,
}

impl SearchPage {
    pub fn new(
        hits: Vec<SearchHit>,
        matched: usize,
        searchable: Option<usize>,
        limit: usize,
    ) -> Self {
        let shown = hits.len();
        let next_action = if searchable == Some(0) {
            Some("no searchable records in this scope with these filters; check --scope (siblings are not searched), the type filters, and --field (subject, predicate and value hold claims only)".to_owned())
        } else if let (0, Some(searchable)) = (matched, searchable) {
            Some(format!(
                "no match among {searchable} searchable records; try --terms or fewer words before treating the fact as unknown"
            ))
        } else if matched > shown {
            Some(format!(
                "{} more matches not shown; raise --limit (now {limit}) or narrow the query",
                matched - shown
            ))
        } else {
            None
        };
        Self {
            hits,
            shown,
            matched,
            searchable,
            next_action,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchMode {
    #[default]
    Phrase,
    Terms,
    Advanced,
}

/// Record types a search may return. All false means all types.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchTypes {
    pub claims: bool,
    pub observations: bool,
    pub events: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum SearchField {
    /// Full record text: claim subject, predicate and value, or event and observation content.
    #[default]
    Text,
    /// Claim subject only.
    Subject,
    /// Claim predicate only.
    Predicate,
    /// Claim value only.
    Value,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SearchOptions {
    pub mode: SearchMode,
    /// Filter claims to active status; events and observations remain searchable.
    pub current_only: bool,
    pub types: SearchTypes,
    pub field: SearchField,
    /// Also return `memory-command` events, which repeat each direct claim write.
    pub include_commands: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResolveStatus {
    /// One subject matched exactly or by normalized name.
    Resolved,
    /// One subject matched only by containment or spelling distance.
    Probable,
    /// More than one subject matched at the best tier.
    Ambiguous,
    /// No subject matched.
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResolveTier {
    Exact,
    Name,
    Contains,
    /// Words match in any order through initials, prefixes or nicknames.
    Tokens,
    Fuzzy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedName {
    pub name: String,
    /// `subject` or `alias`.
    pub via: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    pub scope_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveCandidate {
    pub subject: String,
    pub matched: Vec<ResolvedName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resolution {
    pub query: String,
    pub status: ResolveStatus,
    pub tier: Option<ResolveTier>,
    /// Candidates returned, at most 20.
    pub shown: usize,
    /// Subjects that matched at the deciding tier; more than `shown` when
    /// the candidate list was cut.
    pub matched: usize,
    pub candidates: Vec<ResolveCandidate>,
    /// Distinct active subjects and alias names compared against the query.
    pub considered_subjects: usize,
    pub considered_aliases: usize,
    pub next_action: String,
}

/// Evidence query used during context composition.
#[derive(Clone, Copy, Debug)]
pub struct ContextQuery<'a> {
    pub text: &'a str,
    pub options: SearchOptions,
}

impl ContextBundle {
    pub fn model_payload(&self) -> Value {
        serde_json::json!({"claims": self.claims, "pendingClaims": self.pending_claims,
            "continuation": self.continuation, "continuityViews": self.continuity_views,
            "observations": self.observations, "recentEvents": self.recent_events,
            "recalledEvidence": self.recalled_evidence})
    }

    /// Compact model input. Record IDs can be used with native exact recall.
    pub fn compact_model_payload(&self) -> Value {
        json!({
            "claims": self.claims.iter().map(Claim::compact_model_record).collect::<Vec<_>>(),
            "pendingClaims": self.pending_claims.iter().map(Claim::compact_model_record).collect::<Vec<_>>(),
            "continuation": self.continuation,
            "continuityViews": self.continuity_views.iter().map(MemoryView::compact_model_record).collect::<Vec<_>>(),
            "observations": self.observations.iter().map(Observation::compact_model_record).collect::<Vec<_>>(),
            "recentEvents": self.recent_events.iter().map(MemoryEvent::compact_model_record).collect::<Vec<_>>(),
            "recalledEvidence": self.recalled_evidence.iter().map(MemoryEvent::compact_model_record).collect::<Vec<_>>(),
        })
    }
}

pub const MAX_OBSERVER_BYTES: usize = 1_048_576;
pub const MAX_OBSERVER_ITEMS: usize = 256;
pub const MAX_SOURCE_IDS: usize = 256;

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
