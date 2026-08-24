use std::fs;
use std::io::{self, IsTerminal, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use clap::{Args, Parser, Subcommand, error::ErrorKind};
use omk::{
    ClaimCardinality, ClaimKind, ClaimStatus, EventKind, MemoryStore, MutationResult, NewEvent,
    ObserverResult, ReadAccess, SCHEMA_VERSION, ScopeKind, Sensitivity, ViewKind,
    store::CreateView,
};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Parser)]
#[command(
    name = "omk",
    version,
    arg_required_else_help = true,
    about = "Local-first observational memory for agents",
    long_about = "Store immutable agent history, derive source-backed observations and claims, compose bounded context, and recover exact evidence. All command output is compact JSON.",
    after_help = "Examples:\n  omk init\n  omk event append --scope thread:build --stream codex-1 --kind user-message --content 'Continue the implementation' --idempotency-key codex-1-event-42\n  omk observe plan --scope thread:build --stream codex-1 --model codex --idempotency-key codex-1-plan-1"
)]
struct Cli {
    /// SQLite database path.
    #[arg(long, env = "OMK_DB", default_value = ".omk/memory.db", global = true)]
    db: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize the database.
    Init,
    /// Manage user/project/thread/task scopes.
    Scope {
        #[command(subcommand)]
        command: ScopeCommand,
    },
    /// Append, replay, inspect, or privacy-purge raw events.
    Event {
        #[command(subcommand)]
        command: EventCommand,
    },
    /// Plan and atomically commit model-produced observations.
    Observe {
        #[command(subcommand)]
        command: ObserveCommand,
    },
    /// Manage canonical and proposed claims.
    Claim {
        #[command(subcommand)]
        command: ClaimCommand,
    },
    /// Create and inspect append-only context views.
    View {
        #[command(subcommand)]
        command: ViewCommand,
    },
    /// Recall exact sources or search full text.
    Recall {
        #[command(subcommand)]
        command: RecallCommand,
    },
    /// Compose bounded agent context.
    Context(ContextArgs),
}

#[derive(Debug, Subcommand)]
enum ScopeCommand {
    /// Create one scope. Parent scopes must already exist.
    Add {
        /// Stable scope ID, such as user:me or thread:build.
        #[arg(long)]
        id: String,
        /// Scope kind: user, project, thread, or task.
        #[arg(long)]
        kind: ScopeKind,
        /// Existing parent scope ID; omit only for a root scope.
        #[arg(long)]
        parent: Option<String>,
        /// Optional human-readable label.
        #[arg(long)]
        name: Option<String>,
        /// Globally unique key; reuse only for an identical request.
        #[arg(long)]
        idempotency_key: String,
    },
    /// List all scopes in creation order.
    List,
}

#[derive(Debug, Subcommand)]
enum EventCommand {
    #[command(
        after_help = "Examples:\n  omk event append --scope thread:build --stream codex-1 --kind user-message --content 'Continue the implementation' --idempotency-key codex-1-event-42\n  printf '%s' 'credential material' | omk event append --scope thread:build --stream codex-1 --kind tool-result --sensitivity secret --idempotency-key codex-1-secret-1\n\nSecret content must come from stdin or --content-file. Secret metadata must come from --metadata-file."
    )]
    Append {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        stream: String,
        #[arg(long)]
        kind: EventKind,
        /// JSON value or plain text. Reads stdin when omitted.
        #[arg(long, conflicts_with = "content_file")]
        content: Option<String>,
        /// File containing a JSON value or plain text; use - for stdin.
        #[arg(long)]
        content_file: Option<PathBuf>,
        #[arg(long)]
        actor: Option<String>,
        #[arg(long)]
        occurred_at: Option<String>,
        /// Conservative token-count hint; OMK never accepts less than its own estimate.
        #[arg(long)]
        token_count: Option<i64>,
        #[arg(long, default_value = "normal")]
        /// Storage/privacy mode: normal, secret, or do-not-store.
        sensitivity: Sensitivity,
        /// JSON object metadata. Do not use this flag for secret metadata.
        #[arg(long, conflicts_with = "metadata_file")]
        metadata: Option<String>,
        /// File containing JSON object metadata; use - for stdin.
        #[arg(long)]
        metadata_file: Option<PathBuf>,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Return one scope-visible event by UUID; secrets are redacted by default.
    Get {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        id: String,
        #[arg(long)]
        reveal_secret: bool,
    },
    /// Privacy-purge an event and report every affected derived record type.
    Purge {
        /// Event UUID returned by append/get, not a stream sequence number.
        #[arg(long)]
        id: String,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
enum ObserveCommand {
    /// Reserve the next contiguous unobserved range and return redacted model input.
    #[command(
        after_help = "Ready output uses .data.status=\"ready\", .data.runId, and .data.events[].id. A caught-up stream returns .data.status=\"caught-up\" with no runId."
    )]
    Plan {
        /// Leaf scope that owns the stream.
        #[arg(long)]
        scope: String,
        /// Stream ID to observe.
        #[arg(long)]
        stream: String,
        /// Hard maximum for the redacted model-visible event batch.
        #[arg(long, default_value_t = 6_000)]
        max_tokens: i64,
        /// Observer model label recorded for provenance.
        #[arg(long)]
        model: String,
        #[arg(long, default_value = "observer.v1")]
        prompt_version: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Validate and atomically commit a strict ObserverResult JSON object.
    #[command(
        after_help = "Read the run UUID from observe plan output at .data.runId. The input is this exact JSON shape; do not include runId in the object:\n\n  {\n    \"observations\": [{\n      \"kind\": \"decision\",\n      \"content\": \"Concrete source-backed statement\",\n      \"importance\": 0.9,\n      \"confidence\": 1.0,\n      \"sourceEventIds\": [\"event-uuid-from-plan\"],\n      \"eventTimeFrom\": null,\n      \"eventTimeTo\": null\n    }],\n    \"claims\": [{\n      \"kind\": \"decision\",\n      \"subject\": \"subject\",\n      \"predicate\": \"predicate\",\n      \"cardinality\": \"single\",\n      \"value\": \"any JSON value\",\n      \"modality\": \"explicit-assertion\",\n      \"confidence\": 1.0,\n      \"sourceEventIds\": [\"event-uuid-from-plan\"]\n    }],\n    \"continuation\": {\n      \"currentTask\": null,\n      \"completed\": [],\n      \"blockers\": [],\n      \"nextActions\": [],\n      \"unresolvedQuestions\": []\n    },\n    \"ambiguities\": [],\n    \"emptyReason\": null\n  }\n\nAllowed observation kinds: event, decision, outcome, failure, constraint, preference, open-loop, relationship, continuation.\nAllowed claim kinds: fact, preference, decision, goal, commitment, constraint, open-loop, entity-alias, relationship, hypothesis.\nAllowed claim cardinalities: single, set.\nAllowed modalities: explicit-assertion, accepted-decision, proposal, inference, observation.\n\nEvery observation, claim, and ambiguity must cite one or more event UUIDs from the plan. Observer claims always remain pending until an explicit claim command changes them. For a non-empty result, continuation completely replaces the previous snapshot. If observations, claims, and ambiguities are all empty, send the empty continuation shown above and a concrete non-empty emptyReason; the kernel then preserves the previous continuation."
    )]
    Commit {
        #[arg(long)]
        run: String,
        /// ObserverResult JSON file; use - or omit for stdin.
        #[arg(long)]
        input: Option<PathBuf>,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Record an external observer failure without advancing the stream cursor.
    Fail {
        #[arg(long)]
        run: String,
        /// Short, non-sensitive failure classification.
        #[arg(long)]
        reason: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Inspect one observation run, including failure and ambiguity details.
    Get {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        run: String,
    },
    /// List observation runs by scope, stream, or lifecycle status.
    List {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        stream: Option<String>,
        #[arg(long)]
        status: Option<String>,
    },
    /// Inspect a stream cursor, sequence allocator, and its observation runs.
    Status {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        stream: String,
    },
}

#[derive(Debug, Args)]
struct ClaimValueArgs {
    /// Scope that owns the claim.
    #[arg(long)]
    scope: String,
    #[arg(long)]
    kind: ClaimKind,
    /// Whether this logical slot has one active value or a set of active values.
    #[arg(long, default_value = "single")]
    cardinality: ClaimCardinality,
    /// Stable logical subject.
    #[arg(long)]
    subject: String,
    /// Stable logical predicate.
    #[arg(long)]
    predicate: String,
    /// JSON value or plain text.
    #[arg(long)]
    value: String,
    /// Optional source event UUID; repeat for multiple sources.
    #[arg(long = "source-event")]
    source_events: Vec<String>,
    #[arg(long)]
    idempotency_key: String,
}

#[derive(Debug, Subcommand)]
enum ClaimCommand {
    /// Store an explicit claim; conflicts become disputed rather than silently replacing state.
    #[command(
        after_help = "--source-event accepts an event UUID. When omitted, OMK creates a source memory-command event automatically."
    )]
    Remember(ClaimValueArgs),
    /// Store a proposal that cannot supersede active state.
    Propose(ClaimValueArgs),
    /// Accept a pending/disputed claim and explicitly replace same-key active state.
    Confirm {
        #[arg(long)]
        id: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Create an explicit correction while preserving the old claim as superseded.
    Correct {
        #[arg(long)]
        id: String,
        #[arg(long)]
        value: String,
        #[arg(long = "source-event")]
        source_events: Vec<String>,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Move a claim while preserving only evidence visible from the destination.
    Rescope {
        #[arg(long)]
        id: String,
        #[arg(long)]
        scope: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Reject a pending or disputed claim while retaining history.
    Reject {
        #[arg(long)]
        id: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Make a claim inactive without deleting its history.
    Forget {
        #[arg(long)]
        id: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Physically delete one claim and its provenance links.
    Purge {
        #[arg(long)]
        id: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Deterministically activate safe source-backed claims and surface conflicts.
    Reconcile {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// List claims at one scope, optionally including inherited ancestors.
    List {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        ancestors: bool,
        #[arg(long)]
        status: Option<ClaimStatus>,
    },
}

#[derive(Debug, Subcommand)]
enum ViewCommand {
    /// Create the next append-only generation of a supported view kind.
    Create(Box<ViewCreateArgs>),
    /// List all view generations owned by a scope.
    List {
        #[arg(long)]
        scope: String,
    },
}

#[derive(Debug, Args)]
struct ViewCreateArgs {
    #[arg(long)]
    scope: String,
    #[arg(long)]
    stream: String,
    #[arg(long)]
    kind: ViewKind,
    /// View text. Reads stdin when omitted.
    #[arg(long, conflicts_with = "content_file")]
    content: Option<String>,
    #[arg(long)]
    content_file: Option<PathBuf>,
    #[arg(long)]
    from: i64,
    #[arg(long)]
    through: i64,
    #[arg(long = "source-observation")]
    source_observations: Vec<String>,
    /// Latest continuity view used to produce this result; omit only for generation one.
    #[arg(long)]
    expected_previous_view: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    prompt_version: Option<String>,
    /// Conservative token-count hint; OMK never accepts less than its own estimate.
    #[arg(long)]
    token_count: Option<i64>,
    #[arg(long)]
    idempotency_key: String,
}

#[derive(Debug, Subcommand)]
enum RecallCommand {
    /// Return an observation together with its exact source events.
    Observation {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        id: String,
        #[arg(long)]
        reveal_secret: bool,
    },
    /// Return exact local events for an inclusive stream sequence range.
    EventRange {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        stream: String,
        #[arg(long)]
        from: i64,
        #[arg(long)]
        to: i64,
        #[arg(long)]
        reveal_secret: bool,
    },
    #[command(
        after_help = "Queries are literal phrases by default. Use --fts-query only for intentional SQLite FTS5 syntax."
    )]
    Search {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        query: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Interpret --query as raw SQLite FTS5 syntax instead of a literal phrase.
        #[arg(long)]
        fts_query: bool,
    },
    /// Return a claim with its exact source events.
    ExplainClaim {
        #[arg(long)]
        scope: String,
        #[arg(long)]
        id: String,
        #[arg(long)]
        reveal_secret: bool,
    },
}

#[derive(Debug, Args)]
struct ContextArgs {
    #[arg(long)]
    scope: String,
    #[arg(long)]
    stream: String,
    #[arg(long, default_value_t = 16_000)]
    max_tokens: i64,
    #[arg(long, default_value_t = 6_000)]
    recent_raw_tokens: i64,
    #[arg(long)]
    query: Option<String>,
}

#[derive(Clone, Copy)]
enum RecoveryContext {
    NoReusableKey,
    IdempotentMutation,
}

impl RecoveryContext {
    fn for_command(command: &Command) -> Self {
        let is_idempotent_mutation = match command {
            Command::Init | Command::Recall { .. } | Command::Context(_) => false,
            Command::Scope { command } => matches!(command, ScopeCommand::Add { .. }),
            Command::Event { command } => {
                matches!(
                    command,
                    EventCommand::Append { .. } | EventCommand::Purge { .. }
                )
            }
            Command::Observe { command } => matches!(
                command,
                ObserveCommand::Plan { .. }
                    | ObserveCommand::Commit { .. }
                    | ObserveCommand::Fail { .. }
            ),
            Command::Claim { command } => matches!(
                command,
                ClaimCommand::Remember(_)
                    | ClaimCommand::Propose(_)
                    | ClaimCommand::Confirm { .. }
                    | ClaimCommand::Correct { .. }
                    | ClaimCommand::Rescope { .. }
                    | ClaimCommand::Reject { .. }
                    | ClaimCommand::Forget { .. }
                    | ClaimCommand::Purge { .. }
                    | ClaimCommand::Reconcile { .. }
            ),
            Command::View { command } => matches!(command, ViewCommand::Create(_)),
        };
        if is_idempotent_mutation {
            Self::IdempotentMutation
        } else {
            Self::NoReusableKey
        }
    }

    fn same_key_reusable(self) -> bool {
        matches!(self, Self::IdempotentMutation)
    }
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp
                    | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
                    | ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return;
        }
        Err(error) => {
            print_error(
                "cli_usage",
                &error.to_string(),
                false,
                false,
                Some("inspect --help"),
            );
            std::process::exit(2);
        }
    };
    let recovery_context = RecoveryContext::for_command(&cli.command);
    if let Err(error) = run(cli) {
        let message = format!("{error:#}");
        let (code, retryable, same_key_reusable, next_action) =
            classify_error(&message, recovery_context);
        print_error(code, &message, retryable, same_key_reusable, next_action);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let mut store = MemoryStore::open(&cli.db)?;
    match cli.command {
        Command::Init => print_json(&MutationResult::created(
            json!({"database": cli.db, "schemaVersion": SCHEMA_VERSION, "ready": true}),
        ))?,
        Command::Scope { command } => match command {
            ScopeCommand::Add {
                id,
                kind,
                parent,
                name,
                idempotency_key,
            } => print_json(&store.create_scope(
                &id,
                kind,
                parent.as_deref(),
                name.as_deref(),
                &idempotency_key,
            )?)?,
            ScopeCommand::List => print_json(&store.list_scopes()?)?,
        },
        Command::Event { command } => match command {
            EventCommand::Append {
                scope,
                stream,
                kind,
                content,
                content_file,
                actor,
                occurred_at,
                token_count,
                sensitivity,
                metadata,
                metadata_file,
                idempotency_key,
            } => {
                if sensitivity == Sensitivity::Secret {
                    ensure!(
                        content.is_none(),
                        "secret content must be read from stdin or --content-file"
                    );
                    ensure!(
                        metadata.is_none(),
                        "secret metadata must be read from --metadata-file"
                    );
                }
                let content_uses_stdin = content.is_none()
                    && content_file
                        .as_deref()
                        .is_none_or(|path| path == Path::new("-"));
                let metadata_uses_stdin = metadata_file.as_deref() == Some(Path::new("-"));
                ensure!(
                    !(content_uses_stdin && metadata_uses_stdin),
                    "content and metadata cannot both be read from stdin"
                );
                let raw = read_inline_or_file(content, content_file.as_deref())?;
                let content = parse_json_or_string(&raw);
                let raw_metadata = if let Some(metadata) = metadata {
                    metadata
                } else if let Some(path) = metadata_file.as_deref() {
                    read_path_or_stdin(Some(path))?
                } else {
                    "{}".to_owned()
                };
                let metadata: Value = serde_json::from_str(&raw_metadata)
                    .context("event metadata must be valid JSON")?;
                print_json(&store.append_event(NewEvent {
                    scope_id: scope,
                    stream_id: stream,
                    kind,
                    actor_id: actor,
                    occurred_at,
                    content,
                    token_count,
                    sensitivity,
                    metadata,
                    idempotency_key,
                })?)?;
            }
            EventCommand::Get {
                scope,
                id,
                reveal_secret,
            } => print_json(&store.get_event(
                &ReadAccess {
                    anchor_scope_id: scope,
                    reveal_secrets: reveal_secret,
                },
                &id,
            )?)?,
            EventCommand::Purge {
                id,
                idempotency_key,
            } => print_json(&store.purge_event(&id, &idempotency_key)?)?,
        },
        Command::Observe { command } => match command {
            ObserveCommand::Plan {
                scope,
                stream,
                max_tokens,
                model,
                prompt_version,
                idempotency_key,
            } => print_json(&store.plan_observation(
                &scope,
                &stream,
                max_tokens,
                &model,
                &prompt_version,
                &idempotency_key,
            )?)?,
            ObserveCommand::Commit {
                run,
                input,
                idempotency_key,
            } => {
                let raw = read_path_or_stdin(input.as_deref())?;
                let result: ObserverResult =
                    serde_json::from_str(&raw).context("parsing strict ObserverResult JSON")?;
                print_json(&store.commit_observation(&run, result, &idempotency_key)?)?;
            }
            ObserveCommand::Fail {
                run,
                reason,
                idempotency_key,
            } => print_json(&store.fail_observation(&run, &reason, &idempotency_key)?)?,
            ObserveCommand::Get { scope, run } => {
                print_json(&store.get_observation_run(&ReadAccess::agent(scope), &run)?)?;
            }
            ObserveCommand::List {
                scope,
                stream,
                status,
            } => print_json(&store.list_observation_runs(
                &ReadAccess::agent(scope),
                stream.as_deref(),
                status.as_deref(),
            )?)?,
            ObserveCommand::Status { scope, stream } => {
                print_json(&store.stream_status(&ReadAccess::agent(scope), &stream)?)?;
            }
        },
        Command::Claim { command } => match command {
            ClaimCommand::Remember(args) => print_json(&store.remember_claim_with_cardinality(
                &args.scope,
                args.kind,
                &args.subject,
                &args.predicate,
                args.cardinality,
                parse_json_or_string(&args.value),
                &args.source_events,
                &args.idempotency_key,
            )?)?,
            ClaimCommand::Propose(args) => print_json(&store.propose_claim_with_cardinality(
                &args.scope,
                args.kind,
                &args.subject,
                &args.predicate,
                args.cardinality,
                parse_json_or_string(&args.value),
                &args.source_events,
                &args.idempotency_key,
            )?)?,
            ClaimCommand::Confirm {
                id,
                idempotency_key,
            } => print_json(&store.confirm_claim(&id, &idempotency_key)?)?,
            ClaimCommand::Correct {
                id,
                value,
                source_events,
                idempotency_key,
            } => print_json(&store.correct_claim(
                &id,
                parse_json_or_string(&value),
                &source_events,
                &idempotency_key,
            )?)?,
            ClaimCommand::Rescope {
                id,
                scope,
                idempotency_key,
            } => print_json(&store.rescope_claim(&id, &scope, &idempotency_key)?)?,
            ClaimCommand::Reject {
                id,
                idempotency_key,
            } => print_json(&store.reject_claim(&id, &idempotency_key)?)?,
            ClaimCommand::Forget {
                id,
                idempotency_key,
            } => print_json(&store.forget_claim(&id, &idempotency_key)?)?,
            ClaimCommand::Purge {
                id,
                idempotency_key,
            } => print_json(&store.purge_claim(&id, &idempotency_key)?)?,
            ClaimCommand::Reconcile {
                scope,
                idempotency_key,
            } => print_json(&store.reconcile(&scope, &idempotency_key)?)?,
            ClaimCommand::List {
                scope,
                ancestors,
                status,
            } => print_json(&store.list_claims(&scope, ancestors, status)?)?,
        },
        Command::View { command } => match command {
            ViewCommand::Create(args) => {
                let ViewCreateArgs {
                    scope,
                    stream,
                    kind,
                    content,
                    content_file,
                    from,
                    through,
                    source_observations,
                    expected_previous_view,
                    model,
                    prompt_version,
                    token_count,
                    idempotency_key,
                } = *args;
                let content = read_inline_or_file(content, content_file.as_deref())?;
                print_json(&store.create_view(CreateView {
                    scope_id: scope,
                    stream_id: stream,
                    kind,
                    content,
                    source_from_sequence: from,
                    source_through_sequence: through,
                    source_observation_ids: source_observations,
                    expected_previous_view_id: expected_previous_view,
                    model,
                    prompt_version,
                    token_count,
                    idempotency_key,
                })?)?;
            }
            ViewCommand::List { scope } => {
                print_json(&store.list_views(&scope)?)?;
            }
        },
        Command::Recall { command } => match command {
            RecallCommand::Observation {
                scope,
                id,
                reveal_secret,
            } => {
                print_json(&store.explain_observation(
                    &ReadAccess {
                        anchor_scope_id: scope,
                        reveal_secrets: reveal_secret,
                    },
                    &id,
                )?)?;
            }
            RecallCommand::EventRange {
                scope,
                stream,
                from,
                to,
                reveal_secret,
            } => print_json(&store.recall_event_range(
                &ReadAccess {
                    anchor_scope_id: scope,
                    reveal_secrets: reveal_secret,
                },
                &stream,
                from,
                to,
            )?)?,
            RecallCommand::Search {
                scope,
                query,
                limit,
                fts_query,
            } => {
                let hits = if fts_query {
                    store.search_full_text_advanced(&scope, &query, limit)?
                } else {
                    store.search_full_text(&scope, &query, limit)?
                };
                print_json(&hits)?;
            }
            RecallCommand::ExplainClaim {
                scope,
                id,
                reveal_secret,
            } => {
                print_json(&store.explain_claim(
                    &ReadAccess {
                        anchor_scope_id: scope,
                        reveal_secrets: reveal_secret,
                    },
                    &id,
                )?)?;
            }
        },
        Command::Context(args) => {
            let bundle = store.compose_context(
                &args.scope,
                &args.stream,
                args.max_tokens,
                args.recent_raw_tokens,
                args.query.as_deref(),
            )?;
            print_json(&bundle)?;
        }
    }
    Ok(())
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn read_inline_or_file(inline: Option<String>, path: Option<&Path>) -> Result<String> {
    if let Some(inline) = inline {
        return Ok(inline);
    }
    read_path_or_stdin(path)
}

fn read_path_or_stdin(path: Option<&Path>) -> Result<String> {
    match path {
        Some(path) if path != Path::new("-") => fs::read_to_string(path)
            .with_context(|| format!("reading input file {}", path.display())),
        _ => {
            ensure!(
                !io::stdin().is_terminal(),
                "stdin is interactive; pipe input or pass a file option"
            );
            let mut input = String::new();
            io::stdin().read_to_string(&mut input)?;
            ensure!(!input.is_empty(), "stdin was empty");
            Ok(input)
        }
    }
}

fn parse_json_or_string(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn classify_error(
    message: &str,
    recovery_context: RecoveryContext,
) -> (&'static str, bool, bool, Option<&'static str>) {
    let same_key_reusable = recovery_context.same_key_reusable();
    if message.contains("idempotency conflict") || message.contains("already used for") {
        (
            "idempotency_conflict",
            false,
            false,
            Some("use a new key or retry the identical request"),
        )
    } else if message.contains("budget too small") {
        let next_action = if same_key_reusable {
            "increase the token budget and retry with the same key"
        } else {
            "increase the token budget and retry"
        };
        (
            "budget_exceeded",
            false,
            same_key_reusable,
            Some(next_action),
        )
    } else if message.contains("view is stale") {
        (
            "stale_view",
            false,
            same_key_reusable,
            Some(
                "read the latest continuity view, rerun reflection from it, and retry with the same key",
            ),
        )
    } else if message.contains(" is stale") {
        (
            "stale_observation_run",
            false,
            false,
            Some("request a new observation plan"),
        )
    } else if message.contains("privacy-purged") {
        (
            "privacy_purged",
            false,
            false,
            Some("use a new idempotency key without restoring purged data"),
        )
    } else if message.contains("does not exist") {
        if same_key_reusable {
            (
                "not_found",
                false,
                true,
                Some("create or target an existing resource and retry with the same key"),
            )
        } else {
            (
                "not_found",
                false,
                false,
                Some("inspect the identifier and scope"),
            )
        }
    } else if message.contains("not visible") || message.contains("does not belong to scope") {
        (
            "scope_violation",
            false,
            same_key_reusable,
            Some("inspect the scope tree and target scope"),
        )
    } else if message.contains("FTS") || message.contains("SQL logic error") {
        (
            "invalid_search_query",
            false,
            same_key_reusable,
            Some("use literal search or correct --fts-query syntax"),
        )
    } else if message.contains("stdin is interactive") || message.contains("stdin was empty") {
        (
            "missing_input",
            false,
            same_key_reusable,
            Some("pipe input on stdin or pass the command's file option"),
        )
    } else if message.contains("ObserverResult")
        || message.contains("source event")
        || message.contains("must be")
        || message.contains("cannot be empty")
        || message.contains("parsing")
        || message.contains("reading input file")
        || message.contains("claim slot already uses")
    {
        let next_action = if same_key_reusable {
            "correct the input and retry with the same key"
        } else {
            "correct the input and retry"
        };
        ("invalid_input", false, same_key_reusable, Some(next_action))
    } else {
        ("kernel_error", false, false, None)
    }
}

fn print_error(
    code: &str,
    message: &str,
    retryable: bool,
    same_key_reusable: bool,
    next_action: Option<&str>,
) {
    let error = json!({
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
            "sameKeyReusable": same_key_reusable,
            "nextAction": next_action
        }
    });
    eprintln!(
        "{}",
        serde_json::to_string(&error).expect("serializing an error envelope cannot fail")
    );
}

#[cfg(test)]
mod tests {
    use super::{RecoveryContext, classify_error};

    #[test]
    fn stale_view_errors_have_view_specific_recovery() {
        let classified = classify_error(
            "view is stale: expected previous view None, found Some(\"view-id\")",
            RecoveryContext::IdempotentMutation,
        );
        assert_eq!(classified.0, "stale_view");
        assert!(!classified.1);
        assert!(classified.2);
        assert!(
            classified
                .3
                .is_some_and(|action| action.contains("continuity view"))
        );
    }

    #[test]
    fn not_found_recovery_only_reuses_keys_for_idempotent_mutations() {
        let mutation = classify_error(
            "scope project:later does not exist",
            RecoveryContext::IdempotentMutation,
        );
        assert_eq!(mutation.0, "not_found");
        assert!(mutation.2);
        assert!(mutation.3.is_some_and(|action| action.contains("same key")));

        let read = classify_error(
            "event event-id does not exist",
            RecoveryContext::NoReusableKey,
        );
        assert_eq!(read.0, "not_found");
        assert!(!read.2);
        assert!(read.3.is_some_and(|action| !action.contains("key")));
    }

    #[test]
    fn input_file_errors_are_correctable_with_the_same_mutation_key() {
        let classified = classify_error(
            "reading input file /tmp/missing: No such file or directory",
            RecoveryContext::IdempotentMutation,
        );
        assert_eq!(classified.0, "invalid_input");
        assert!(!classified.1);
        assert!(classified.2);
    }
}
