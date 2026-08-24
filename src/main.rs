use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};
use omk::{
    ClaimKind, ClaimStatus, ContextBundle, EventKind, MemoryStore, MutationResult, NewEvent,
    ObserverResult, SCHEMA_VERSION, ScopeKind, Sensitivity, ViewKind, store::CreateView,
};
use serde::Serialize;
use serde_json::{Value, json};

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(
    name = "omk",
    version,
    about = "Local-first observational memory for agents",
    long_about = "Store immutable agent history, derive source-backed observations and claims, compose bounded context, and recover exact evidence. All command output is JSON unless --format markdown is selected."
)]
struct Cli {
    /// SQLite database path.
    #[arg(long, env = "OMK_DB", default_value = ".omk/memory.db", global = true)]
    db: PathBuf,

    /// Emit compact JSON instead of indented JSON.
    #[arg(long, global = true)]
    compact: bool,

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
        after_help = "Example:\n  omk event append --scope thread:build --stream codex-1 --kind user-message --content 'Continue the implementation' --idempotency-key codex-1-event-42"
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
        /// Storage/privacy mode: normal, private, secret, or do-not-store.
        sensitivity: Sensitivity,
        /// JSON object metadata. Secret output redacts it; do-not-store discards it.
        #[arg(long, default_value = "{}")]
        metadata: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Return exact local evidence for an inclusive stream sequence range.
    Range {
        #[arg(long)]
        stream: String,
        #[arg(long)]
        from: i64,
        #[arg(long)]
        to: i64,
    },
    /// Return one exact local event by UUID, including stored secret content.
    Get {
        #[arg(long)]
        id: String,
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
        after_help = "Input must match prompts/observer.v1.md. Read runId from observe plan output at .data.runId."
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
        run: String,
    },
    /// List observation runs by scope, stream, or lifecycle status.
    List {
        #[arg(long)]
        scope: Option<String>,
        #[arg(long)]
        stream: Option<String>,
        #[arg(long)]
        status: Option<String>,
    },
    /// Inspect a stream cursor, sequence allocator, and its observation runs.
    Status {
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
    Create {
        #[arg(long)]
        scope: String,
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
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        prompt_version: Option<String>,
        /// Conservative token-count hint; OMK never accepts less than its own estimate.
        #[arg(long)]
        token_count: Option<i64>,
        #[arg(long)]
        idempotency_key: String,
    },
    /// List all view generations owned by a scope.
    List {
        #[arg(long)]
        scope: String,
    },
}

#[derive(Debug, Subcommand)]
enum RecallCommand {
    /// Return an observation together with its exact source events.
    Observation {
        #[arg(long)]
        id: String,
    },
    /// Return exact local events for an inclusive stream sequence range.
    EventRange {
        #[arg(long)]
        stream: String,
        #[arg(long)]
        from: i64,
        #[arg(long)]
        to: i64,
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
    /// Return a claim with all source observations and exact source events.
    ExplainClaim {
        #[arg(long)]
        id: String,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormat {
    Json,
    Markdown,
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
    #[arg(long, default_value = "json")]
    format: OutputFormat,
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
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
    if let Err(error) = run(cli) {
        let message = format!("{error:#}");
        let (code, retryable, same_key_reusable, next_action) = classify_error(&message);
        print_error(code, &message, retryable, same_key_reusable, next_action);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let mut store = MemoryStore::open(&cli.db)?;
    match cli.command {
        Command::Init => print_json(
            &MutationResult::created(
                json!({"database": cli.db, "schemaVersion": SCHEMA_VERSION, "ready": true}),
            ),
            cli.compact,
        )?,
        Command::Scope { command } => match command {
            ScopeCommand::Add {
                id,
                kind,
                parent,
                name,
                idempotency_key,
            } => print_json(
                &store.create_scope(
                    &id,
                    kind,
                    parent.as_deref(),
                    name.as_deref(),
                    &idempotency_key,
                )?,
                cli.compact,
            )?,
            ScopeCommand::List => print_json(&store.list_scopes()?, cli.compact)?,
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
                idempotency_key,
            } => {
                let raw = read_inline_or_file(content, content_file.as_deref())?;
                let content = parse_json_or_string(&raw);
                let metadata: Value =
                    serde_json::from_str(&metadata).context("--metadata must be valid JSON")?;
                print_json(
                    &store.append_event(NewEvent {
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
                    })?,
                    cli.compact,
                )?;
            }
            EventCommand::Range { stream, from, to } => {
                print_json(&store.recall_event_range(&stream, from, to)?, cli.compact)?
            }
            EventCommand::Get { id } => print_json(&store.get_event(&id)?, cli.compact)?,
            EventCommand::Purge {
                id,
                idempotency_key,
            } => print_json(&store.purge_event(&id, &idempotency_key)?, cli.compact)?,
        },
        Command::Observe { command } => match command {
            ObserveCommand::Plan {
                scope,
                stream,
                max_tokens,
                model,
                prompt_version,
                idempotency_key,
            } => print_json(
                &store.plan_observation(
                    &scope,
                    &stream,
                    max_tokens,
                    &model,
                    &prompt_version,
                    &idempotency_key,
                )?,
                cli.compact,
            )?,
            ObserveCommand::Commit {
                run,
                input,
                idempotency_key,
            } => {
                let raw = read_path_or_stdin(input.as_deref())?;
                let result: ObserverResult =
                    serde_json::from_str(&raw).context("parsing strict ObserverResult JSON")?;
                print_json(
                    &store.commit_observation(&run, result, &idempotency_key)?,
                    cli.compact,
                )?;
            }
            ObserveCommand::Fail {
                run,
                reason,
                idempotency_key,
            } => print_json(
                &store.fail_observation(&run, &reason, &idempotency_key)?,
                cli.compact,
            )?,
            ObserveCommand::Get { run } => {
                print_json(&store.get_observation_run(&run)?, cli.compact)?;
            }
            ObserveCommand::List {
                scope,
                stream,
                status,
            } => print_json(
                &store.list_observation_runs(
                    scope.as_deref(),
                    stream.as_deref(),
                    status.as_deref(),
                )?,
                cli.compact,
            )?,
            ObserveCommand::Status { stream } => {
                print_json(&store.stream_status(&stream)?, cli.compact)?;
            }
        },
        Command::Claim { command } => match command {
            ClaimCommand::Remember(args) => print_json(
                &store.remember_claim(
                    &args.scope,
                    args.kind,
                    &args.subject,
                    &args.predicate,
                    parse_json_or_string(&args.value),
                    &args.source_events,
                    &args.idempotency_key,
                )?,
                cli.compact,
            )?,
            ClaimCommand::Propose(args) => print_json(
                &store.propose_claim(
                    &args.scope,
                    args.kind,
                    &args.subject,
                    &args.predicate,
                    parse_json_or_string(&args.value),
                    &args.source_events,
                    &args.idempotency_key,
                )?,
                cli.compact,
            )?,
            ClaimCommand::Confirm {
                id,
                idempotency_key,
            } => print_json(&store.confirm_claim(&id, &idempotency_key)?, cli.compact)?,
            ClaimCommand::Correct {
                id,
                value,
                source_events,
                idempotency_key,
            } => print_json(
                &store.correct_claim(
                    &id,
                    parse_json_or_string(&value),
                    &source_events,
                    &idempotency_key,
                )?,
                cli.compact,
            )?,
            ClaimCommand::Rescope {
                id,
                scope,
                idempotency_key,
            } => print_json(
                &store.rescope_claim(&id, &scope, &idempotency_key)?,
                cli.compact,
            )?,
            ClaimCommand::Reject {
                id,
                idempotency_key,
            } => print_json(&store.reject_claim(&id, &idempotency_key)?, cli.compact)?,
            ClaimCommand::Forget {
                id,
                idempotency_key,
            } => print_json(&store.forget_claim(&id, &idempotency_key)?, cli.compact)?,
            ClaimCommand::Purge {
                id,
                idempotency_key,
            } => print_json(&store.purge_claim(&id, &idempotency_key)?, cli.compact)?,
            ClaimCommand::Reconcile {
                scope,
                idempotency_key,
            } => print_json(&store.reconcile(&scope, &idempotency_key)?, cli.compact)?,
            ClaimCommand::List {
                scope,
                ancestors,
                status,
            } => print_json(&store.list_claims(&scope, ancestors, status)?, cli.compact)?,
        },
        Command::View { command } => match command {
            ViewCommand::Create {
                scope,
                kind,
                content,
                content_file,
                from,
                through,
                source_observations,
                model,
                prompt_version,
                token_count,
                idempotency_key,
            } => {
                let content = read_inline_or_file(content, content_file.as_deref())?;
                print_json(
                    &store.create_view(CreateView {
                        scope_id: scope,
                        kind,
                        content,
                        source_from_sequence: from,
                        source_through_sequence: through,
                        source_observation_ids: source_observations,
                        model,
                        prompt_version,
                        token_count,
                        idempotency_key,
                    })?,
                    cli.compact,
                )?;
            }
            ViewCommand::List { scope } => {
                print_json(&store.list_views(&scope)?, cli.compact)?;
            }
        },
        Command::Recall { command } => match command {
            RecallCommand::Observation { id } => {
                print_json(&store.explain_observation(&id)?, cli.compact)?;
            }
            RecallCommand::EventRange { stream, from, to } => {
                print_json(&store.recall_event_range(&stream, from, to)?, cli.compact)?
            }
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
                print_json(&hits, cli.compact)?;
            }
            RecallCommand::ExplainClaim { id } => {
                print_json(&store.explain_claim(&id)?, cli.compact)?;
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
            match args.format {
                OutputFormat::Json => print_json(&bundle, cli.compact)?,
                OutputFormat::Markdown => print!("{}", render_markdown(&bundle)),
            }
        }
    }
    Ok(())
}

fn print_json(value: &impl Serialize, compact: bool) -> Result<()> {
    if compact {
        println!("{}", serde_json::to_string(value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
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

fn enum_label(value: &impl Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn render_markdown(bundle: &ContextBundle) -> String {
    let mut output = String::new();
    if !bundle.claims.is_empty() {
        output.push_str("<active-claims>\n");
        for claim in &bundle.claims {
            output.push_str(&format!(
                "- [{}] {} {} {} (claim: {})\n",
                enum_label(&claim.kind),
                claim.subject,
                claim.predicate,
                claim.value,
                claim.id
            ));
        }
        output.push_str("</active-claims>\n\n");
    }
    if !bundle.pending_claims.is_empty() {
        output.push_str("<pending-claims>\n");
        for claim in &bundle.pending_claims {
            output.push_str(&format!(
                "- [{}; {}] {} {} {} (claim: {})\n",
                enum_label(&claim.kind),
                enum_label(&claim.status),
                claim.subject,
                claim.predicate,
                claim.value,
                claim.id
            ));
        }
        output.push_str("</pending-claims>\n\n");
    }
    for view in &bundle.continuity_views {
        output.push_str(&format!(
            "<memory-view kind=\"{}\" id=\"{}\">\n{}\n</memory-view>\n\n",
            enum_label(&view.kind),
            view.id,
            view.content
        ));
    }
    if !bundle.observations.is_empty() {
        output.push_str("<observations>\n");
        for observation in &bundle.observations {
            output.push_str(&format!(
                "- {} (observation: {})\n",
                observation.content, observation.id
            ));
        }
        output.push_str("</observations>\n\n");
    }
    if !bundle.recent_events.is_empty() {
        output.push_str("<recent-events>\n");
        for event in &bundle.recent_events {
            output.push_str(&format!(
                "- #{} [{}] {}\n",
                event.sequence,
                enum_label(&event.kind),
                event.content
            ));
        }
        output.push_str("</recent-events>\n\n");
    }
    if !bundle.recalled_evidence.is_empty() {
        output.push_str("<recalled-evidence>\n");
        for event in &bundle.recalled_evidence {
            output.push_str(&format!(
                "- {} #{}: {}\n",
                event.stream_id, event.sequence, event.content
            ));
        }
        output.push_str("</recalled-evidence>\n\n");
    }
    output.push_str(&format!(
        "<!-- estimated-memory-tokens: {} -->\n",
        bundle.diagnostics.estimated_tokens
    ));
    output
}

fn classify_error(message: &str) -> (&'static str, bool, bool, Option<&'static str>) {
    if message.contains("idempotency conflict") || message.contains("already used for") {
        (
            "idempotency_conflict",
            false,
            false,
            Some("use a new key or retry the identical request"),
        )
    } else if message.contains("budget too small") {
        (
            "budget_exceeded",
            false,
            true,
            Some("increase the token budget and retry with the same key"),
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
        ("not_found", false, false, None)
    } else if message.contains("not visible") || message.contains("does not belong to scope") {
        (
            "scope_violation",
            false,
            true,
            Some("inspect the scope tree and target scope"),
        )
    } else if message.contains("FTS") || message.contains("SQL logic error") {
        (
            "invalid_search_query",
            false,
            true,
            Some("use literal search or correct --fts-query syntax"),
        )
    } else if message.contains("ObserverResult")
        || message.contains("source event")
        || message.contains("must be")
        || message.contains("cannot be empty")
        || message.contains("parsing")
    {
        (
            "invalid_input",
            false,
            true,
            Some("correct the input and retry with the same key"),
        )
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
