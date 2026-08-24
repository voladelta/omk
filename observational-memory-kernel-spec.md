# Observational Memory Kernel

## Design and Implementation Contract for Codex

Reference implementation: OMK 0.5.0, Rust 2024 edition, SQLite schema v4.

### Objective

Build a framework-neutral, local-first memory kernel for a personal agent system. It must preserve long-running conversational continuity without treating summaries as truth and keep model execution and scheduling outside the kernel.

The system is complete when it can:

1. Store an immutable, source-backed history of messages and tool activity.
2. Convert older history into compact observations in the background.
3. Maintain structured current state without allowing an LLM to mutate it directly.
4. Preserve every observation and reflection source so exact evidence remains recoverable.
5. Compose bounded context from current state, episodic memory, recent raw events, and targeted recall.
6. Support user, project, thread, and task scopes without cross-scope leakage.
7. Replace external observer and reflector models or prompts without changing persisted memory invariants.

Target the reference implementation at Rust 2024 with SQLite. Keep the core independent of any agent framework or model provider. Data-model snippets below use TypeScript-shaped pseudocode only as compact, language-neutral schema notation; the Rust types and serialized camelCase JSON are the executable interfaces.

---

## 1. Design principle

Use four first-class records only:

```text
Event       = what happened; immutable evidence
Observation = what the observer concluded happened; source-linked interpretation
Claim       = a structured statement that may become current state
View        = a disposable, versioned rendering of observations or claims for context
```

The active memory presented to an agent is derived from these records. No summary, embedding, or model output is authoritative by itself.

### Required invariants

- Events are immutable under normal operation.
- Every observation references one or more source events.
- Every claim references source events or observations.
- An LLM may propose claims but may not directly activate, supersede, or delete them.
- Proposals and inferences never silently become accepted decisions.
- Reflections create new views; they never overwrite or delete observations.
- Scope inheritance is explicit and policy-controlled.
- Exact source evidence remains recallable after any amount of compaction.
- Failed observation or reflection work leaves the previously active memory unchanged.
- All write operations are idempotent.

---

## 2. Non-goals for the first implementation

Do not build these initially:

- A graph database.
- Mandatory vector search.
- One resource-wide summary of the user’s entire life.
- Direct LLM writes to canonical state.
- Destructive summarization.
- Autonomous forgetting based only on age.
- Framework-specific agent lifecycle logic in the core.
- A distributed worker system before the SQLite implementation is correct.
- A multi-crate workspace with one crate per interface.

Implement one clean crate with internal ports. Extract crates only after a second implementation genuinely requires it.

---

## 3. Scope model

Represent scopes as a generic tree rather than hard-coding inheritance into storage.

```ts
export type ScopeKind = 'user' | 'project' | 'thread' | 'task';

export interface Scope {
  id: string;
  kind: ScopeKind;
  parentId: string | null;
  name?: string;
  createdAt: string;
}
```

Default hierarchy:

```text
user
  └── project
        └── thread
              └── task
```

Default visibility rules:

| Scope | Default visibility |
|---|---|
| User | All descendants, filtered by context policy |
| Project | Threads and tasks in that project |
| Thread | That thread and its tasks |
| Task | That task only |

Do not flatten parent memories into child records. Resolve inheritance at read time.

Examples:

- “Prefer complete implementation plans” may be a user-scoped preference.
- “Settlement remains ETH-only” is project-scoped.
- “Fix test `feeDecay_03` next” is thread- or task-scoped.
- “Maybe add USDG later” remains a project-scoped proposal, not current state.

---

## 4. Core records

### 4.1 Event

```ts
export type EventKind =
  | 'user-message'
  | 'assistant-message'
  | 'tool-call'
  | 'tool-result'
  | 'file-reference'
  | 'system-event'
  | 'memory-command';

export interface MemoryEvent {
  id: string;
  streamId: string;
  sequence: number;
  scopeId: string;
  kind: EventKind;
  actorId?: string;
  occurredAt: string;
  recordedAt: string;
  content: unknown;
  contentHash: string;
  tokenCount: number;
  sensitivity: 'normal' | 'secret' | 'do-not-store';
  metadata: Record<string, unknown>;
}
```

Requirements:

- Sequence numbers are monotonic per stream.
- `contentHash` is used for idempotency and corruption checks.
- `do-not-store` content must not be persisted; store only a safe tombstone event when necessary for sequence continuity.
- Secrets must be rejected or redacted before observer and reflector model calls.

### 4.2 Observation

```ts
export type ObservationKind =
  | 'event'
  | 'decision'
  | 'outcome'
  | 'failure'
  | 'constraint'
  | 'preference'
  | 'open-loop'
  | 'relationship'
  | 'continuation';

export interface Observation {
  id: string;
  runId: string;
  scopeId: string;
  kind: ObservationKind;
  content: string;
  importance: number;        // 0..1
  confidence: number;        // 0..1
  eventTimeFrom?: string;
  eventTimeTo?: string;
  sourceStartSequence: number;
  sourceEndSequence: number;
  observerModel: string;
  promptVersion: string;
  createdAt: string;
}
```

Use a join table for exact event provenance even when a contiguous range is also stored.

### 4.3 Claim

A claim begins as a model proposal or explicit memory command. Active claims form canonical current state.

```ts
export type ClaimKind =
  | 'fact'
  | 'preference'
  | 'decision'
  | 'goal'
  | 'commitment'
  | 'constraint'
  | 'open-loop'
  | 'entity-alias'
  | 'relationship'
  | 'hypothesis';

export type ClaimModality =
  | 'explicit-assertion'
  | 'accepted-decision'
  | 'proposal'
  | 'inference'
  | 'observation';

export type ClaimStatus =
  | 'pending'
  | 'active'
  | 'disputed'
  | 'superseded'
  | 'rejected'
  | 'expired';

export interface Claim {
  id: string;
  scopeId: string;
  kind: ClaimKind;
  subject: string;
  predicate: string;
  value: unknown;
  modality: ClaimModality;
  status: ClaimStatus;
  authority:
    | 'explicit-user'
    | 'accepted-record'
    | 'trusted-source'
    | 'model-inference';
  confidence: number;
  supersedesId?: string;
  createdAt: string;
  updatedAt: string;
}
```

The logical key for reconciliation is:

```text
scope + kind + subject + predicate
```

Do not assume a newer claim supersedes an older one. Check scope, modality, and explicit replacement language first.

### 4.4 View

Views are derived context artifacts. They are append-only and replaceable.

```ts
export type ViewKind =
  | 'continuity'
  | 'continuation';

export interface MemoryView {
  id: string;
  scopeId: string;
  kind: ViewKind;
  generation: number;
  content: string;
  sourceFromSequence: number;
  sourceThroughSequence: number;
  previousViewId?: string;
  model?: string;
  promptVersion?: string;
  tokenCount: number;
  createdAt: string;
}
```

Support only `continuity` and `continuation` views.

---

## 5. Observer contract

The observer receives a contiguous event range and returns strict structured output.

```ts
export interface ObserverInput {
  scope: Scope;
  events: MemoryEvent[];
  activeClaims: Claim[];
  previousContinuation?: MemoryView;
}

export interface ObserverResult {
  observations: Array<{
    kind: ObservationKind;
    content: string;
    importance: number;
    confidence: number;
    sourceEventIds: string[];
    eventTimeFrom?: string;
    eventTimeTo?: string;
  }>;

  claims: Array<{
    kind: ClaimKind;
    subject: string;
    predicate: string;
    value: unknown;
    modality: ClaimModality;
    confidence: number;
    sourceEventIds: string[];
  }>;

  continuation: {
    currentTask?: string;
    completed?: string[];
    blockers?: string[];
    nextActions?: string[];
    unresolvedQuestions?: string[];
  };

  ambiguities: Array<{
    description: string;
    sourceEventIds: string[];
  }>;
}
```

Observer prompt rules:

1. Record concrete outcomes, decisions, failures, constraints, preferences, and unresolved work.
2. Preserve modality. “Maybe” is a proposal; it is not a decision.
3. Preserve uncertainty rather than inventing resolution.
4. Separate event time from observation time.
5. Include source event IDs for every output item.
6. Do not restate greetings, acknowledgments, or low-value procedural noise.
7. Do not infer stable preferences from a single weak example.
8. Do not include secrets or redacted content.
9. Never instruct the system to mutate memory; output proposals only.
10. Produce schema-valid JSON and no free-form wrapper text.

Version every observer prompt. Store the prompt version and model on each observation run.
Repeat the exact input shape and allowed enum values in `omk observe commit --help`; an agent must not need repository access to construct valid input.

---

## 6. Observation lifecycle

Use immediate, non-destructive observation rather than delayed replacement of raw history.

```text
append events
    ↓
select next contiguous unobserved range
    ↓
run observer outside the database transaction
    ↓
validate result and provenance
    ↓
commit observations, pending claims, continuation view, and cursor atomically
    ↓
reconcile pending claims
```

Raw events are never removed by observation. Context composition decides whether to show raw events, observations, or a reflected view.

### Observation run state

```text
pending → committed
       ├→ failed
       └→ stale
```

### Atomic commit

Inside one transaction:

1. Verify the stream’s `observedThroughSequence` still equals `run.fromSequence - 1`.
2. Insert observations.
3. Insert observation-to-event provenance rows.
4. Insert claims with `pending` status.
5. Insert a new `continuation` view.
6. Advance `observedThroughSequence` to `run.toSequence`.
7. Mark the run `committed`.
8. Commit.

If the cursor check fails, mark the run stale and retry from the new cursor. Do not partially commit.

### Default scheduling policy

All thresholds are configurable. Recommended defaults:

```yaml
observe_chunk_tokens: 6000
observe_after_idle_ms: 120000
hard_unobserved_limit_tokens: 32000
recent_raw_retention_tokens: 6000
reflect_after_observation_tokens: 18000
reflection_target_tokens: 6000
```

Trigger observation when either the chunk threshold or idle threshold is reached. If the hard limit is reached, run observation synchronously before continuing.

---

## 7. Claim reconciliation

Use deterministic code for common cases and an optional model-assisted classifier only for unresolved cases.

### Default rules

```text
exact active match
  → add evidence; do not create a duplicate active claim

explicit user correction
  → activate new claim and supersede the old claim

accepted decision with explicit replacement
  → activate and supersede prior active decision

proposal
  → keep pending with proposal modality; never supersede active state

model inference
  → keep pending unless reinforced or explicitly confirmed

same key, different scope
  → store separately

apparent contradiction
  → compare time, scope, entity, environment, and modality

unresolved conflict
  → mark disputed; leave previous active state unchanged
```

Examples:

```text
“Use ETH as the launch asset.”
→ active decision: ETH

“Maybe users could choose ETH or USDG.”
→ pending proposal: ETH-or-USDG

“We have decided to support both ETH and USDG instead.”
→ new active decision; prior ETH-only decision becomes superseded
```

Expose explicit user commands:

- remember globally
- remember for this project
- mark as proposal
- confirm decision
- correct memory
- forget or purge
- show source
- show conflicts

Explicit privacy deletion may physically remove records and cascade through provenance. This is the controlled exception to append-only storage.

---

## 8. Reflection

Reflection reduces prompt cost; it does not replace memory.

Build a `continuity` view from:

- The previous continuity view, if any.
- New observations not yet represented in that view.
- Active claims relevant to the scope.

Reflect only history older than the configured recent raw retention window. Store the new view as another generation and keep prior generations.

Reflector rules:

1. Preserve accepted decisions, outcomes, failures, reasons, and unresolved work.
2. Remove duplicated procedural detail.
3. Keep proposals distinguishable from accepted decisions.
4. Preserve dates and changes in state.
5. Do not invent causal explanations.
6. Include source observation IDs in structured metadata.
7. Target the configured token budget.
8. On failure, continue using the prior view.

The first implementation needs only one continuity view per thread and, optionally, one project continuity view.

---

## 9. Context composition

Context construction is separate from memory creation.

Default order:

```text
1. Relevant active user-scoped claims
2. Relevant active project-scoped claims
3. Relevant active thread/task claims
4. Latest continuity view for stable older history
5. Unreflected observations after the view and before the raw tail
6. Recent raw events
7. Query-specific recalled evidence
8. Current user input
```

Do not include observations whose entire source range is already present in the recent raw tail.

Return a structured context bundle rather than one opaque string:

```ts
export interface ContextBundle {
  claims: Claim[];
  continuityViews: MemoryView[];
  observations: Observation[];
  recentEvents: MemoryEvent[];
  recalledEvidence: MemoryEvent[];
  diagnostics: {
    estimatedTokens: number;
    omittedItems: Array<{ id: string; reason: string }>;
  };
}
```

A renderer converts the bundle into the target model format. Provide a default Markdown/XML renderer, but keep rendering outside the kernel.

Recommended budget priorities:

1. System and safety instructions.
2. Current task and active decisions.
3. Recent raw user/tool interaction.
4. Continuity view.
5. Supporting evidence.
6. Low-importance historical observations.

Never trim current task, explicit user corrections, active constraints, or unresolved blockers before lower-priority history.

---

## 10. Recall and retrieval

Implement retrieval in this order:

1. Exact event or observation source recall.
2. Exact identifier lookup.
3. SQLite FTS full-text search.
4. Optional semantic search adapter.
5. Optional relationship traversal.

The first complete version must support:

```ts
recallByObservation(observationId)
recallByEventRange(streamId, fromSequence, toSequence)
searchFullText(scopeIds, query, limit)
explainClaim(claimId)
```

`explainClaim` must return the claim, source observations, and raw source events.

Semantic similarity is a discovery signal only. It cannot establish truth, identity, or supersession.

---

## 11. Public kernel API

The Rust crate exports `MemoryStore`, the model types, and `SCHEMA_VERSION`. `MemoryStore` owns SQLite state and provides operations for:

```text
scope create/list/get/visibility
event append/get/range recall/purge
observation plan/commit/fail/get/list/status
claim remember/propose/confirm/correct/rescope/reject/forget/purge/reconcile
view create/list
context composition
literal and explicit-FTS search
claim and observation explanation
```

Every mutating method accepts an idempotency key and returns a mutation result that distinguishes a newly created result from an identical replay. Model execution and scheduling remain outside `MemoryStore`; callers plan work, run the model, and commit validated output.

---

## 12. Agent CLI contract

The `omk` binary is an agent-first, non-interactive interface over the kernel.

### Output and discovery

- Every successful data command emits one compact JSON value. Help and version output remain plain text.
- Successful output goes to stdout. Command runtime errors use structured JSON on stderr with a nonzero exit status; command-line syntax errors use clap's concise usage diagnostics on stderr.
- Errors contain stable `code`, `message`, `retryable`, and `sameKeyReusable` fields and include `nextAction` when a concrete recovery action exists.
- Running `omk` with no subcommand prints top-level help to stdout and exits successfully.
- Support `-h`, `--help`, `help <command>`, and `--version` without opening the database.
- Top-level help and agent-critical command help include copyable examples. Help must document privacy-sensitive input restrictions where they apply.
- The database path resolves from `--db`, then `OMK_DB`, then `.omk/memory.db`.

### Input and retry behavior

- Every write requires a non-empty, request-stable, globally unique idempotency key.
- An identical retry returns the original result with `operation.replayed: true`.
- Reusing a recorded key with changed input returns `idempotency_conflict` and `sameKeyReusable: false`.
- Validation that occurs before recording an operation returns `sameKeyReusable: true`.
- Event content may come from `--content`, `--content-file`, or stdin when neither option is supplied. `--content-file -` explicitly selects stdin.
- Event metadata may come from `--metadata` or `--metadata-file`; `--metadata-file -` selects stdin. Omitted metadata is `{}`.
- One append command cannot consume both content and metadata from stdin.
- If required stdin is attached to a terminal or is empty, fail immediately with `missing_input`; never wait indefinitely for an agent to type input.

### Secret boundary

- For `sensitivity=secret`, reject inline `--content` and inline `--metadata` before recording the operation. Secret content must use stdin or `--content-file`; secret metadata must use `--metadata-file`.
- Do not echo secret values in help, errors, logs, append results, or idempotent replay results.
- Secret append and replay results contain a redaction tombstone and empty metadata.
- Exact stored secret evidence is available only through the explicit local event-get and recall commands. Observer plans, context, and full-text search must never expose it.
- `do-not-store` persists only a sequence-preserving tombstone and discards metadata.

---

## 13. SQLite schema

While the kernel is pre-1.0, initialize one current schema and reject incompatible nonzero schema versions before writes. Do not carry forward migrations without a real compatibility requirement. Use WAL mode and avoid coupling the core to a particular ORM.

Required tables:

1. `memory_scopes`
2. `memory_streams`
3. `memory_events`
4. `observation_runs`
5. `observations`
6. `observation_sources`
7. `claims`
8. `claim_sources`
9. `memory_views`
10. `view_sources`
11. `memory_fts`
12. `memory_operations`

Important constraints:

- Unique `(stream_id, sequence)` on events.
- Unique idempotency key per mutating operation.
- Observation runs store `from_sequence` and `to_sequence`.
- Only one committed observation run may cover a specific starting cursor.
- Provenance foreign keys use cascading deletion for explicit privacy purge.
- Claim provenance links directly to source events; there is no observation-to-claim source variant.
- Claims are append-only except for lifecycle status fields.
- Views are append-only.
- A `NULL` operation result is a privacy-purged idempotency tombstone; a non-`NULL` result is replayable.

Use FTS5 over event text, observation content, and claim text. Keep embeddings outside the initial schema or behind a separate adapter table.

---

## 14. Reference source layout

```text
Cargo.toml
src/
├── lib.rs       # public crate surface
├── model.rs     # serialized records and command inputs
├── store.rs     # SQLite schema, invariants, and kernel operations
└── main.rs      # clap-based agent CLI and JSON error boundary
prompts/
├── observer.v1.md
└── reflector.v1.md
tests/
├── kernel.rs    # storage, lifecycle, privacy, and recovery integration tests
└── cli.rs       # process-level CLI contract tests
```

Keep this compact layout while ownership remains clear. Split modules only when file-level boundaries become materially easier to test or reason about.

---

## 15. Required tests

### Correctness

- Appending the same idempotency key twice creates one event set.
- Observation failure does not advance the cursor.
- Retrying a committed observation run creates no duplicates.
- Concurrent runs from the same cursor allow only one commit.
- Every observation has valid source events.
- A proposal cannot supersede an active decision.
- An explicit correction supersedes prior state and preserves history.
- Scope-specific claims do not leak into unrelated projects.
- Reflection failure leaves the previous view usable.
- Raw evidence remains recallable after multiple reflections.
- Context composition does not duplicate observations already represented by raw events.
- Secret content never enters observer or reflector input.
- Inline secret content and metadata are rejected without being echoed.
- Secret append and replay envelopes contain only redacted content and empty metadata.
- Empty or interactive required stdin fails immediately with `missing_input`.
- No-argument invocation, `--help`, and `help <command>` expose the agent-critical contract without opening a database.

### Semantic fixtures

Include fixtures covering:

- A decision later reversed.
- A proposal that is never accepted.
- A user preference that applies only to implementation plans.
- Two similarly named projects that must not be merged.
- A tool failure followed by a successful repair.
- An explicit user correction of a model inference.
- A long thread where the user says only “continue” and the agent still resumes correctly.

### Recovery tests

Simulate interruption:

- Before the observer call.
- After observer output but before commit.
- During commit.
- After commit but before acknowledgement.
- During reconciliation.
- During reflection.
- During FTS indexing.

Every interruption must have an explicit idempotent recovery path.

---

## 16. Definition of done

The implementation is done when all of the following are true:

1. The kernel has no dependency on Mastra, LangGraph, OpenAI Agents, or another agent framework.
2. The observer and reflector can be replaced with deterministic test doubles.
3. SQLite is the only required runtime service.
4. A user can ask what the system remembers, why it believes it, and where the evidence came from.
5. A user can correct, confirm, rescope, reject, forget, or purge a memory.
6. Long sessions remain within a configurable context budget.
7. Current decisions and open work survive compaction.
8. Exact raw history remains available.
9. No model output becomes authoritative without reconciliation.
10. All invariants and recovery tests pass.
11. An agent can discover, invoke, retry, and recover CLI operations without parsing prose command output or waiting on interactive input.

---

## 17. Final implementation rule

Keep this distinction visible in code, storage, prompts, and user-facing inspection:

```text
Events are evidence.
Observations are interpretations.
Claims are state proposals or state.
Views are disposable context.
```

Do not collapse these into a single memory text field. That separation is the core of the design.
