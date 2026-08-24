# Observational memory kernel

This document defines the design and implementation contract for Codex.

Reference implementation: OMK 0.5.0, Rust 2024 edition, SQLite schema v4.

## What OMK must do

Build a local-first memory kernel that does not depend on an agent framework. Keep model execution and scheduling outside the kernel.

OMK must preserve long conversations without treating summaries as fact.

OMK must:

- store an immutable, source-backed history of messages and tool activity
- convert older history into compact observations
- keep structured current state without letting a large language model (LLM) change it directly
- preserve every source used for observations and reflections
- recover exact evidence
- build bounded context from current state, observations, recent events and targeted recall
- support user, project, thread and task scopes without leaking data between scopes
- allow replacement observer and reflector models or prompts without changing stored memory rules

Use Rust 2024 and SQLite for the reference implementation. Keep the core independent of agent frameworks and model providers.

The data model examples use TypeScript-shaped pseudocode. The Rust types and camelCase JSON are the executable interfaces.

---

## 1. Design principle

Use only 4 core records:

```text
Event       = what happened; immutable evidence
Observation = what the observer concluded happened; source-linked interpretation
Claim       = a structured statement that may become current state
View        = a disposable, versioned rendering of observations or claims for context
```

OMK derives active memory from these records. A summary, embedding or model output is never authoritative by itself.

### Required invariants

- events are immutable during normal operation
- every observation references one or more source events
- every claim references source events
- an LLM may propose claims but cannot activate, supersede or delete them
- proposals and inferences never become accepted decisions without an explicit state change
- reflections create views and never overwrite or delete observations
- scope inheritance follows an explicit policy
- exact source evidence remains available after compaction
- failed observation or reflection work leaves active memory unchanged
- every write is idempotent

---

## 2. What OMK 0.5 does not include

OMK 0.5 does not include:

- a graph database
- required vector search
- one resource-wide summary of a user’s life
- direct LLM writes to canonical state
- destructive summarisation
- automatic forgetting based only on age
- framework-specific agent lifecycle logic in the core
- a distributed worker system
- a multi-crate workspace with one crate for each interface

Use one crate with internal boundaries. Create more crates only when another implementation needs them.

---

## 3. Scope model

Store scopes as a general tree. Do not hard-code inheritance into storage.

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

Use this default hierarchy:

```text
user
  └── project
        └── thread
              └── task
```

Use these default visibility rules:

| Scope | Default visibility |
|---|---|
| User | All descendants, filtered by context policy |
| Project | Threads and tasks in that project |
| Thread | That thread and its tasks |
| Task | That task only |

Do not copy parent memories into child records. Resolve inheritance when you read.

Use these scope examples:

- ‘Prefer complete implementation plans’ can be a user preference
- ‘Settlement remains ETH-only’ belongs to a project
- ‘Fix test `feeDecay_03` next’ belongs to a thread or task
- ‘Maybe add USDG later’ is a project proposal, not current state

---

## 4. Core records

### Event record

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

An event must meet these requirements:

- sequence numbers increase within each stream
- `contentHash` supports idempotency and corruption checks
- `do-not-store` content is never stored
- a safe event marker may preserve sequence continuity for `do-not-store` content
- secrets are rejected or redacted before observer and reflector calls

### Observation record

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

Use a join table for exact event provenance. Keep this table even when you also store a continuous sequence range.

### Claim record

A model proposal or an explicit memory command creates a claim. Active claims form canonical current state.

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
    | 'trusted-source'
    | 'model-inference';
  confidence: number;
  supersedesId?: string;
  createdAt: string;
  updatedAt: string;
}
```

Use this logical key for reconciliation:

```text
scope + kind + subject + predicate
```

Do not assume that a newer claim replaces an older claim. Check its scope, modality and explicit replacement language.

### View record

Views provide derived context. They are append-only and replaceable.

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

The observer receives an unbroken event sequence. It returns strict, structured output.

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
    currentTask: string | null;
    completed: string[];
    blockers: string[];
    nextActions: string[];
    unresolvedQuestions: string[];
  };

  ambiguities: Array<{
    description: string;
    sourceEventIds: string[];
  }>;

  emptyReason: string | null;
}
```

Apply these observer prompt rules:

- record concrete outcomes, decisions, failures, constraints, preferences and unresolved work
- preserve modality, so ‘maybe’ remains a proposal rather than a decision
- preserve uncertainty rather than inventing a resolution
- separate event time from observation time
- include source event IDs for every output item
- omit greetings, acknowledgements and low-value procedural detail
- do not infer a stable preference from one weak example
- do not include secrets or redacted content
- output proposals only and never instruct OMK to change memory
- produce valid JSON without free-form wrapper text

Give every observer prompt a version. Store the prompt version and model on each observation run.

Repeat the exact input shape and allowed values in `omk observe commit --help`. Agents must not need repository access to make valid input.

---

## 6. Observation lifecycle

Observe events without replacing raw history.

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

Observation never removes raw events. Context composition chooses whether to show events, observations or a reflected view.

### Observation run state

```text
pending → committed
       ├→ failed
       └→ stale
```

### Atomic commit

Commit a run in one transaction.

1. Verify the stream’s `observedThroughSequence` still equals `run.fromSequence - 1`.
2. Insert observations.
3. Insert observation-to-event provenance rows.
4. Insert claims with `pending` status.
5. Insert a new `continuation` view.
6. Advance `observedThroughSequence` to `run.toSequence`.
7. Mark the run `committed`.
8. Commit.

If the cursor check fails, mark the run stale. Retry from the new cursor. Never commit part of a run.

### Default scheduling policy

Make every threshold configurable. Use these recommended defaults:

```yaml
observe_chunk_tokens: 6000
observe_after_idle_ms: 120000
hard_unobserved_limit_tokens: 32000
recent_raw_retention_tokens: 6000
reflect_after_observation_tokens: 18000
reflection_target_tokens: 6000
```

Trigger observation when the chunk or idle threshold is reached. At the hard limit, complete observation before continuing.

---

## 7. Claim reconciliation

Use deterministic code for common cases. Use an optional model classifier only when rules cannot resolve a case.

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

Apply the rules as shown in these examples:

```text
“Use ETH as the launch asset.”
→ active decision: ETH

“Maybe users could choose ETH or USDG.”
→ pending proposal: ETH-or-USDG

“We have decided to support both ETH and USDG instead.”
→ new active decision; prior ETH-only decision becomes superseded
```

Provide commands that let users:

- remember something globally
- remember something for one project
- mark a claim as a proposal
- confirm a decision
- correct memory
- forget or purge memory
- show a source
- show conflicts

An explicit privacy deletion can remove records and their provenance. This is the controlled exception to append-only storage.

---

## 8. Reflection

Reflection reduces prompt cost. It does not replace memory.

Build a `continuity` view from these sources:

- the previous continuity view, when one exists
- new observations that the view does not contain
- active claims for the scope

Reflect only history older than the recent raw retention window. Store a new generation and keep earlier generations.

Apply these reflector rules:

- preserve accepted decisions, outcomes, failures, reasons and unresolved work
- remove repeated procedural detail
- distinguish proposals from accepted decisions
- preserve dates and changes in state
- do not invent causes
- include source observation IDs in structured metadata
- meet the configured token budget
- continue using the previous view after a failure

OMK 0.5 needs one continuity view for each thread. It can also use one project continuity view.

---

## 9. Context composition

Build context separately from memory creation.

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

Do not include an observation if the recent event tail contains its whole source range.

Return a structured context bundle, not one opaque string:

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

Callers convert the bundle into their target model format. Keep rendering outside the kernel.

Recommended budget priorities:

1. System and safety instructions.
2. Current task and active decisions.
3. Recent raw user/tool interaction.
4. Continuity view.
5. Supporting evidence.
6. Low-importance historical observations.

Keep the current task, user corrections, active constraints and unresolved blockers before lower-priority history.

---

## 10. Recall and retrieval

Use this retrieval order:

1. Recall exact events or observation sources.
2. Look up exact identifiers.
3. Search text with SQLite FTS.
4. Use an optional semantic search adapter.
5. Use optional relationship traversal.

OMK 0.5 must support:

```ts
recallByObservation(observationId)
recallByEventRange(streamId, fromSequence, toSequence)
searchFullText(scopeIds, query, limit)
explainClaim(claimId)
```

`explainClaim` must return the claim and its raw source events.

Semantic similarity can help discovery only. It cannot establish truth, identity or replacement.

---

## 11. Public kernel API

The Rust crate exports `MemoryStore`, the model types and `SCHEMA_VERSION`.

`MemoryStore` owns SQLite state and provides these operations:

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

Every write method accepts an idempotency key. Its result distinguishes a new write from an identical replay.

Model execution and scheduling stay outside `MemoryStore`. Callers plan work, run the model and commit validated output.

---

## 12. Agent CLI contract

The `omk` binary provides a non-interactive agent interface to the kernel.

### Output and discovery

- every successful data command writes one compact JSON value
- help and version commands write plain text
- successful output goes to standard output
- runtime errors use structured JSON on standard error and return a nonzero status
- command syntax errors use concise clap diagnostics on standard error
- errors include stable `code`, `message`, `retryable` and `sameKeyReusable` fields
- errors include `nextAction` when OMK can provide a specific recovery action
- `omk` without a command prints top-level help and exits successfully
- `-h`, `--help`, `help <command>` and `--version` work without opening the database
- top-level and agent-critical help include examples that agents can copy
- help explains restrictions for secret input where they apply
- the database path resolves from `--db`, then `OMK_DB`, then `.omk/memory.db`

### Input and retry behavior

- every write needs a non-empty, stable and globally unique idempotency key
- an identical retry returns the original result with `operation.replayed: true`
- a recorded key with changed input returns `idempotency_conflict` and `sameKeyReusable: false`
- validation before recording returns `sameKeyReusable: true`
- event content can come from `--content`, `--content-file` or standard input
- `--content-file -` explicitly selects standard input
- metadata can come from `--metadata` or `--metadata-file`
- `--metadata-file -` selects standard input and omitted metadata becomes `{}`
- one append command cannot read both content and metadata from standard input
- empty or interactive required input fails at once with `missing_input`
- OMK never waits for an agent to type required input

### Secret boundary

- reject inline `--content` and `--metadata` when `sensitivity=secret`
- require standard input or `--content-file` for secret content
- require `--metadata-file` for secret metadata
- never echo secret values in help, errors, logs, append results or replay results
- return a redaction marker and empty metadata for secret appends and replays
- return exact stored secrets only through explicit local event and recall commands
- never expose secrets in observer plans, context or full-text search
- store only a sequence marker for `do-not-store` events and discard their metadata

---

## 13. SQLite schema

While OMK is pre-1.0, initialise only the current schema. Reject incompatible nonzero versions before any schema write.

Do not add migrations without a real compatibility need. Use WAL mode and do not tie the core to one object-relational mapper (ORM).

Schema v4 has 12 required tables:

- `memory_scopes`
- `memory_streams`
- `memory_events`
- `observation_runs`
- `observations`
- `observation_sources`
- `claims`
- `claim_sources`
- `memory_views`
- `view_sources`
- `memory_fts`
- `memory_operations`

Apply these constraints:

- events have a unique `(stream_id, sequence)` pair
- every write operation has a unique idempotency key
- observation runs store `from_sequence` and `to_sequence`
- only one committed run can cover a starting cursor
- provenance foreign keys cascade on an explicit privacy purge
- claim provenance links directly to source events
- observation-to-claim provenance does not exist
- claims are append-only except for lifecycle status
- views are append-only
- a `NULL` operation result is an idempotency marker for purged data
- a non-`NULL` operation result can be replayed

Use FTS5 for event text, observation content and claim text. Keep embeddings outside schema v4 or in a separate adapter table.

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

Keep this layout while each file has clear ownership. Split a module only when the new boundary makes testing or reasoning easier.

---

## 15. Required tests

### Correctness

Tests must prove that:

- the same append key creates one event set
- a failed observation does not move the cursor
- retrying a committed observation run creates no duplicates
- only one concurrent run from the same cursor can commit
- every observation has valid source events
- a proposal cannot replace an active decision
- an explicit correction replaces earlier state and keeps its history
- claims do not leak into unrelated projects
- a failed reflection leaves the previous view usable
- raw evidence remains available after several reflections
- context does not repeat observations already represented by raw events
- observer and reflector input never contains secret content
- OMK rejects inline secret content and metadata without echoing it
- secret append and replay results contain only redacted content and empty metadata
- empty or interactive required standard input fails at once with `missing_input`
- no-command and help invocations explain the agent contract without opening a database

### Semantic fixtures

Include these fixtures:

- a decision that is later reversed
- a proposal that is never accepted
- a user preference that applies only to implementation plans
- 2 similarly named projects that must remain separate
- a tool failure followed by a successful repair
- an explicit user correction of a model inference
- a long thread where the user says only ‘continue’ and the agent resumes correctly

### Recovery tests

Simulate an interruption at these points:

- before the observer call
- after observer output but before commit
- during commit
- after commit but before acknowledgement
- during reconciliation
- during reflection
- during FTS indexing

Provide an explicit idempotent recovery path for every interruption.

---

## 16. Definition of done

The implementation is complete when:

- the kernel does not depend on Mastra, LangGraph, OpenAI Agents or another agent framework
- deterministic test doubles can replace the observer and reflector
- SQLite is the only required runtime service
- users can inspect what OMK remembers, why it believes it and where the evidence came from
- users can correct, confirm, rescope, reject, forget or purge memory
- long sessions stay within a configurable context budget
- current decisions and open work survive compaction
- exact raw history remains available
- model output becomes authoritative only after reconciliation
- all invariant and recovery tests pass
- agents can discover, run, retry and recover CLI operations from structured output
- agents never need to wait for interactive input

---

## 17. Final implementation rule

Keep this distinction visible in code, storage, prompts and user-facing inspection:

```text
Events are evidence.
Observations are interpretations.
Claims are state proposals or state.
Views are disposable context.
```

Do not combine these records into one memory text field. Their separation defines the design.
