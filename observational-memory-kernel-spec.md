# Observational Memory Kernel

## Design and Implementation Contract for Codex

### Objective

Build a framework-neutral, local-first memory kernel for a personal agent system. It must preserve long-running conversational continuity without treating summaries as truth, and it must remain easy to customize at every boundary: storage, models, prompts, scope rules, reconciliation, retrieval, privacy, and context rendering.

The system is complete when it can:

1. Store an immutable, source-backed history of messages and tool activity.
2. Convert older history into compact observations in the background.
3. Maintain structured current state without allowing an LLM to mutate it directly.
4. Preserve every observation and reflection source so exact evidence remains recoverable.
5. Compose bounded context from current state, episodic memory, recent raw events, and targeted recall.
6. Support user, project, thread, and task scopes without cross-scope leakage.
7. Replace any model, prompt, storage adapter, or policy without changing the kernel.

Target the reference implementation at TypeScript with strict mode and SQLite. Keep the core independent of any agent framework or model provider.

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
- A multi-package monorepo with one package per interface.

Implement one clean package with internal ports. Extract packages only after a second implementation genuinely requires it.

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
  sensitivity: 'normal' | 'private' | 'secret' | 'do-not-store';
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
  validFrom?: string;
  validTo?: string;
  expiresAt?: string;
  supersedesId?: string;
  createdAt: string;
  updatedAt: string;
}
```

The logical key for reconciliation is:

```text
scope + kind + subject + predicate
```

Do not assume a newer claim supersedes an older one. Check scope, modality, validity period, and explicit replacement language first.

### 4.4 View

Views are derived context artifacts. They are append-only and replaceable.

```ts
export type ViewKind =
  | 'continuity'
  | 'project-digest'
  | 'continuation'
  | 'decision-rationale'
  | 'open-loops';

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

Implement only `continuity` and `continuation` in the first complete version. Keep the view-builder interface generic so other views can be added without schema changes.

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
    validFrom?: string;
    validTo?: string;
    expiresAt?: string;
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
pending → running → committed
              └────→ failed
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

## 11. Extension ports

Keep the public extension surface small.

```ts
export interface MemoryStore { /* events, observations, claims, views, runs */ }
export interface Observer { observe(input: ObserverInput): Promise<ObserverResult> }
export interface Reflector { reflect(input: ReflectorInput): Promise<ViewDraft> }
export interface ReconciliationPolicy { decide(input: ReconcileInput): Promise<ReconcileAction> }
export interface ScopePolicy { resolveVisibleScopes(input: ScopeQuery): Promise<string[]> }
export interface RetrievalStrategy { search(input: RetrievalQuery): Promise<RetrievalHit[]> }
export interface ContextPolicy { compose(input: ContextInput): Promise<ContextBundle> }
export interface RedactionPolicy { sanitize(events: MemoryEvent[]): Promise<MemoryEvent[]> }
```

Model routing belongs inside observer and reflector implementations. Scheduling belongs outside the kernel and calls kernel methods. Agent frameworks receive thin adapters that translate messages and tool events into `MemoryEvent` objects.

---

## 12. Public kernel API

```ts
export interface MemoryKernel {
  append(scopeId: string, events: NewMemoryEvent[]): Promise<MemoryEvent[]>;

  planObservation(scopeId: string): Promise<ObservationPlan | null>;
  runObservation(plan: ObservationPlan): Promise<ObservationRunResult>;
  reconcile(scopeId: string): Promise<ReconciliationSummary>;
  reflect(scopeId: string, kind?: ViewKind): Promise<MemoryView | null>;

  composeContext(request: ContextRequest): Promise<ContextBundle>;
  recall(request: RecallRequest): Promise<RecallResult>;

  applyMemoryCommand(command: MemoryCommand): Promise<MemoryCommandResult>;
}
```

Every mutating method accepts an idempotency key.

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

Important constraints:

- Unique `(stream_id, sequence)` on events.
- Unique idempotency key per mutating operation.
- Observation runs store `from_sequence` and `to_sequence`.
- Only one committed observation run may cover a specific starting cursor.
- Provenance foreign keys use cascading deletion for explicit privacy purge.
- Claims are append-only except for lifecycle status fields.
- Views are append-only.

Use FTS5 over event text, observation content, and claim text. Keep embeddings outside the initial schema or behind a separate adapter table.

---

## 14. Reference source layout

```text
src/
├── core/
│   ├── types.ts
│   ├── errors.ts
│   ├── kernel.ts
│   └── ports.ts
├── events/
│   ├── append.ts
│   └── normalization.ts
├── observation/
│   ├── planner.ts
│   ├── runner.ts
│   ├── validator.ts
│   └── commit.ts
├── claims/
│   ├── reconcile.ts
│   ├── rules.ts
│   └── commands.ts
├── reflection/
│   ├── planner.ts
│   ├── runner.ts
│   └── validator.ts
├── context/
│   ├── composer.ts
│   ├── budget.ts
│   └── renderer.ts
├── retrieval/
│   ├── recall.ts
│   ├── full-text.ts
│   └── semantic-port.ts
├── privacy/
│   ├── redaction.ts
│   └── purge.ts
├── storage/sqlite/
│   ├── sqlite-store.ts
│   └── repositories/
├── adapters/
│   └── generic-agent-adapter.ts
├── prompts/
│   ├── observer.v1.md
│   └── reflector.v1.md
├── inspector/
│   └── cli.ts
└── evals/
    ├── fixtures/
    └── memory-evals.test.ts
```

---

## 15. Implementation sequence

### Milestone 1 — Ledger and exact recall

Implement:

- Scope tree.
- SQLite schema initialization with fail-closed version checks.
- Event append with idempotency.
- Exact event-range recall.
- FTS indexing.
- Unit tests for ordering, duplicates, and privacy flags.

Exit criterion: a long thread can be stored and exactly replayed without an observer.

### Milestone 2 — Observation pipeline

Implement:

- Observation planner.
- Observer port and deterministic fake observer.
- Strict result validation.
- Atomic observation commit and cursor advancement.
- Background/idle scheduler adapter.

Exit criterion: events are observed exactly once; failures and retries create no gaps or duplicates.

### Milestone 3 — Claims and current state

Implement:

- Claim persistence.
- Default reconciliation rules.
- Explicit memory commands.
- Claim explanation with source evidence.
- Conflict and supersession tests.

Exit criterion: proposals, inferences, accepted decisions, and corrections behave differently and predictably.

### Milestone 4 — Context composer

Implement:

- Scope inheritance.
- Token budgeting.
- Recent raw tail selection.
- Observation de-duplication against raw events.
- Structured context bundle and default renderer.

Exit criterion: the composer produces bounded context without losing active constraints, current task, or exact recent interaction.

### Milestone 5 — Reflection

Implement:

- Continuity-view planner.
- Reflector port and fake reflector.
- Append-only generations.
- Stable cutoff before recent raw history.
- Failure fallback to prior view.

Exit criterion: older history can be compacted repeatedly while all observations and raw evidence remain recoverable.

### Milestone 6 — Production hardening

Implement:

- Real observer and reflector model adapters.
- Secret redaction.
- Cancellation and timeout handling.
- Inspector CLI.
- Metrics and structured logs.
- End-to-end evaluation fixtures.

Exit criterion: the system survives crashes, retries, contradictory updates, scope changes, and long-running sessions.

---

## 16. Required tests

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

### Semantic fixtures

Include fixtures covering:

- A decision later reversed.
- A proposal that is never accepted.
- A user preference that applies only to implementation plans.
- Two similarly named projects that must not be merged.
- A tool failure followed by a successful repair.
- A stale decision valid only during an earlier time period.
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

## 17. Definition of done

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

---

## 18. Final implementation rule

Keep this distinction visible in code, storage, prompts, and user-facing inspection:

```text
Events are evidence.
Observations are interpretations.
Claims are state proposals or state.
Views are disposable context.
```

Do not collapse these into a single memory text field. That separation is the core of the design.
