#[macro_use]
mod error;
mod capability;
mod cli;
mod crypto;
mod doctor;
mod identity;
mod storage;
mod task;
mod types;
mod ui;
mod ui_html;
mod util;
mod wakeup;

use crate::cli::*;
use crate::doctor::cmd_doctor;
use crate::error::{RaftError, Result};
use crate::storage::*;
use crate::types::*;
use crate::ui::cmd_ui;
use crate::util::*;
use crate::wakeup::Waker;
use chrono::{DateTime, TimeDelta, Utc};
use clap::Parser;
use serde::Serialize;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::flag as signal_flag;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_RATE_WINDOW_SECONDS: u64 = 60;
pub(crate) const DEFAULT_RATE_MAX_MESSAGES: u64 = 10;
pub(crate) const DEFAULT_MAX_MESSAGE_BYTES: usize = 32_768;
pub(crate) const MAX_SUMMARY_BYTES: usize = 2_000;
pub(crate) const DEFAULT_AGENT_TTL_SECONDS: u64 = 120;
pub(crate) const LOCK_TTL_SECONDS: u64 = 30;
pub(crate) const LOCK_TIMEOUT_SECONDS: u64 = 5;
const SERVE_LOCK_TTL_SECONDS: u64 = 30;
pub(crate) const SCHEMA_VERSION: u16 = 1;

fn main() {
    let cli = Cli::parse();
    let json = command_wants_json(&cli.command);
    let root = match root_path(cli.root) {
        Ok(path) => path,
        Err(err) => fail(err, json),
    };

    if let Err(err) = run(root, cli.command) {
        fail(err, json);
    }
}

/// Report a fatal error and exit. In `--json` mode the error is emitted as a
/// stable, machine-parseable envelope on stderr; otherwise a human-readable
/// `raft: <message>` line. The process exit code is derived from the error's
/// category (see `RaftError::exit_code`).
fn fail(err: RaftError, json: bool) -> ! {
    if json {
        let mut error = serde_json::json!({ "code": err.code, "message": err.message });
        if let Some(details) = &err.details
            && let Some(extra) = details.as_object()
            && let Some(error_obj) = error.as_object_mut()
        {
            for (key, value) in extra {
                error_obj.insert(key.clone(), value.clone());
            }
        }
        let envelope = serde_json::json!({ "ok": false, "error": error });
        eprintln!("{envelope}");
    } else {
        eprintln!("raft: {err}");
    }
    process::exit(err.exit_code());
}

/// Whether the invoked command was asked to produce machine-readable JSON.
/// Mirrors the set of commands that carry a `--json` flag so the top-level
/// error path can match the success path's output format.
fn command_wants_json(command: &Commands) -> bool {
    match command {
        Commands::Init(args) => args.json,
        Commands::Claim(args) => args.json,
        Commands::Register(args) => args.json,
        Commands::Heartbeat(args) => args.json,
        Commands::State { command } => match command {
            StateCommand::Set(args) => args.json,
            StateCommand::Get(args) => args.json,
        },
        Commands::Channel { command } => match command {
            ChannelCommand::Create(args) => args.json,
            ChannelCommand::Join(args) => args.json,
            ChannelCommand::Leave(args) => args.json,
            ChannelCommand::List(args) => args.json,
        },
        Commands::Conversation { command } => match command {
            ConversationCommand::Create(args) => args.json,
            ConversationCommand::Open(args) => args.json,
            ConversationCommand::Add(args) => args.json,
            ConversationCommand::Remove(args) => args.json,
        },
        Commands::Send(args) => args.json,
        Commands::Reply(args) => args.json,
        Commands::Withdraw(args) => args.json,
        Commands::Me(args) => args.json,
        Commands::Awaiting(args) => args.json,
        Commands::Roster(args) => args.json,
        Commands::Inbox(args) => args.json,
        Commands::Wait(args) => args.json,
        Commands::Watch(args) => args.json,
        Commands::Show(args) => args.json,
        Commands::Search(args) => args.json,
        Commands::Thread(args) => args.json,
        Commands::Read(args) => args.json,
        Commands::Ack(args) => args.json,
        Commands::Receipts(args) => args.json,
        Commands::Journal(args) => args.json,
        Commands::Status(args) => args.json,
        Commands::Doctor(args) => args.json,
        Commands::Id { command } => match command {
            IdCommand::New(args) => args.json,
            IdCommand::Show(args) => args.json,
            IdCommand::Verify(args) => args.json,
            IdCommand::Fingerprint(args) => args.json,
        },
        Commands::Grant { command } => match command {
            GrantCommand::New(args) => args.json,
            GrantCommand::Attenuate(args) => args.json,
            GrantCommand::Verify(args) => args.json,
            GrantCommand::Inspect(args) => args.json,
        },
        Commands::Task { command } => match command {
            TaskCommand::Dispatch(args) => args.json,
            TaskCommand::Status(args) => args.json,
            TaskCommand::Cancel(args) => args.json,
        },
        Commands::Swarm { command } => match command {
            SwarmCommand::Candidates(args) => args.json,
            SwarmCommand::Assign(args) => args.json,
            SwarmCommand::Dispatch(args) => args.json,
        },
        Commands::Run(args) => args.json,
        _ => false,
    }
}

fn run(root: PathBuf, command: Commands) -> Result<()> {
    match command {
        Commands::Init(args) => cmd_init(&root, args),
        Commands::Claim(args) => cmd_claim(&root, args),
        Commands::Register(args) => cmd_register(&root, args),
        Commands::Heartbeat(args) => cmd_heartbeat(&root, args),
        Commands::State { command } => match command {
            StateCommand::Set(args) => cmd_state_set(&root, args),
            StateCommand::Get(args) => cmd_state_get(&root, args),
        },
        Commands::Channel { command } => match command {
            ChannelCommand::Create(args) => cmd_channel_create(&root, args),
            ChannelCommand::Join(args) => cmd_channel_join(&root, args),
            ChannelCommand::Leave(args) => cmd_channel_leave(&root, args),
            ChannelCommand::List(args) => cmd_channel_list(&root, args),
        },
        Commands::Conversation { command } => match command {
            ConversationCommand::Create(args) => cmd_conversation_create(&root, args),
            ConversationCommand::Open(args) => cmd_conversation_open(&root, args),
            ConversationCommand::Add(args) => cmd_conversation_add(&root, args),
            ConversationCommand::Remove(args) => cmd_conversation_remove(&root, args),
        },
        Commands::Send(args) => cmd_send(&root, args),
        Commands::Reply(args) => cmd_reply(&root, args),
        Commands::Withdraw(args) => cmd_withdraw(&root, args),
        Commands::Me(args) => cmd_me(&root, args),
        Commands::Awaiting(args) => cmd_awaiting(&root, args),
        Commands::Roster(args) => cmd_roster(&root, args),
        Commands::Inbox(args) => cmd_inbox(&root, args),
        Commands::Wait(args) => cmd_wait(&root, args),
        Commands::Watch(args) => cmd_watch(&root, args),
        Commands::Show(args) => cmd_show(&root, args),
        Commands::Search(args) => cmd_search(&root, args),
        Commands::Thread(args) => cmd_thread(&root, args),
        Commands::Read(args) => cmd_read(&root, args),
        Commands::Ack(args) => cmd_ack(&root, args),
        Commands::Receipts(args) => cmd_receipts(&root, args),
        Commands::Journal(args) => cmd_journal(&root, args),
        Commands::Status(args) => cmd_status(&root, args),
        Commands::Doctor(args) => cmd_doctor(&root, args),
        Commands::Gc(args) => cmd_gc(&root, args),
        Commands::Serve(args) => cmd_serve(&root, args),
        Commands::Ui(args) => cmd_ui(&root, args),
        Commands::Id { command } => match command {
            IdCommand::New(args) => cmd_id_new(&root, args),
            IdCommand::Show(args) => cmd_id_show(&root, args),
            IdCommand::Verify(args) => cmd_id_verify(&root, args),
            IdCommand::Fingerprint(args) => cmd_id_fingerprint(&root, args),
        },
        Commands::Grant { command } => match command {
            GrantCommand::New(args) => cmd_grant_new(&root, args),
            GrantCommand::Attenuate(args) => cmd_grant_attenuate(&root, args),
            GrantCommand::Verify(args) => cmd_grant_verify(&root, args),
            GrantCommand::Inspect(args) => cmd_grant_inspect(&root, args),
        },
        Commands::Task { command } => match command {
            TaskCommand::Dispatch(args) => cmd_task_dispatch(&root, args),
            TaskCommand::Status(args) => cmd_task_status(&root, args),
            TaskCommand::Cancel(args) => cmd_task_cancel(&root, args),
        },
        Commands::Swarm { command } => match command {
            SwarmCommand::Candidates(args) => cmd_swarm_candidates(&root, args),
            SwarmCommand::Assign(args) => cmd_swarm_assign(&root, args),
            SwarmCommand::Dispatch(args) => cmd_swarm_dispatch(&root, args),
        },
        Commands::Run(args) => cmd_run(&root, args),
    }
}

/// Resolve a holder/issuer/root reference to an `ed25519:<hex>` public key: a
/// literal key is taken as-is (after a validity check); otherwise it is an agent
/// id whose passport supplies the key.
fn resolve_pubkey(root: &Path, value: &str) -> Result<String> {
    if value.starts_with(crypto::PUBKEY_PREFIX) {
        crypto::parse_pubkey(value)?;
        return Ok(value.to_string());
    }
    let agent_id = validate_id(value, "agent id")?;
    let passport = identity::load_passport(root, &agent_id)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no identity for {agent_id:?}; run `raft id new {agent_id}`"),
        )
    })?;
    Ok(passport.pubkey)
}

/// Build capability caveats from CLI flags, turning `--ttl` into an absolute
/// `expires_at`.
fn build_caveats(args: &CaveatArgs) -> Result<capability::Caveats> {
    let parse_set = |value: &Option<String>| -> Result<Vec<String>> {
        match value {
            Some(raw) => split_csv(raw),
            None => Ok(Vec::new()),
        }
    };
    let expires_at = match &args.ttl {
        Some(ttl) => Some(iso_after(parse_duration_seconds(ttl)?)),
        None => None,
    };
    Ok(capability::Caveats {
        action: parse_set(&args.action)?,
        tool: parse_set(&args.tool)?,
        conversation: args.conversation.clone(),
        env: parse_set(&args.env)?,
        max_runtime_s: args.max_runtime_s,
        max_output_bytes: args.max_output_bytes,
        expires_at,
    })
}

/// Parse a duration like `30m`, `2h`, `7d` (or bare seconds) into seconds.
fn parse_duration_seconds(value: &str) -> Result<u64> {
    let (number, unit) = match value.char_indices().last() {
        Some((idx, last)) if last.is_ascii_alphabetic() => (&value[..idx], &value[idx..]),
        _ => (value, "s"),
    };
    let amount: u64 = number
        .parse()
        .map_err(|_| RaftError::new(format!("invalid duration {value:?}; try 30m, 2h, 7d")))?;
    let seconds = match unit {
        "s" => amount,
        "m" => amount * 60,
        "h" => amount * 3600,
        "d" => amount * 86_400,
        _ => bail!("invalid duration {value:?}; use s, m, h, or d"),
    };
    Ok(seconds)
}

/// Emit a freshly-minted/attenuated token, either to `--out` or to stdout.
fn emit_token(token: &capability::Token, out: &Option<PathBuf>, json: bool) -> Result<()> {
    match out {
        Some(path) => {
            atomic_write_json(path, token)?;
            if json {
                emit_ok(serde_json::json!({
                    "out": path.display().to_string(),
                    "root_issuer": token.root_issuer,
                    "blocks": token.blocks.len(),
                }))?;
            } else {
                println!("wrote capability to {}", path.display());
            }
        }
        None => println!("{}", serde_json::to_string_pretty(token)?),
    }
    Ok(())
}

fn cmd_grant_new(root: &Path, args: GrantNewArgs) -> Result<()> {
    let issuer_id = validate_id(&args.issuer, "agent id")?;
    ensure_root(root)?;
    let keypair = identity::load_keypair(root, &issuer_id)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no keypair for {issuer_id:?}; run `raft id new {issuer_id}`"),
        )
    })?;
    let holder = resolve_pubkey(root, &args.to)?;
    let caveats = build_caveats(&args.caveats)?;
    let token = capability::issue_root(&keypair, &holder, caveats)?;
    emit_token(&token, &args.out, args.json)
}

fn cmd_grant_attenuate(root: &Path, args: GrantAttenuateArgs) -> Result<()> {
    let holder_id = validate_id(&args.holder, "agent id")?;
    ensure_root(root)?;
    let keypair = identity::load_keypair(root, &holder_id)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no keypair for {holder_id:?}; run `raft id new {holder_id}`"),
        )
    })?;
    let token: capability::Token = read_json(&args.token_file)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no such token file: {}", args.token_file.display()),
        )
    })?;
    let new_holder = resolve_pubkey(root, &args.to)?;
    let caveats = build_caveats(&args.caveats)?;
    let attenuated = capability::attenuate(&token, &keypair, &new_holder, caveats)?;
    emit_token(&attenuated, &args.out, args.json)
}

fn cmd_grant_verify(root: &Path, args: GrantVerifyArgs) -> Result<()> {
    let token: capability::Token = read_json(&args.token_file)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no such token file: {}", args.token_file.display()),
        )
    })?;
    let expected_root = match &args.root {
        Some(value) => {
            ensure_root(root)?;
            Some(resolve_pubkey(root, value)?)
        }
        None => None,
    };
    let request = capability::AuthRequest {
        action: &args.action,
        conversation: args.conversation.as_deref(),
        tool: args.tool.as_deref(),
        env: args.env.as_deref(),
        now: Utc::now(),
        requested_runtime_s: None,
        requested_output_bytes: None,
    };
    capability::authorize(&token, expected_root.as_deref(), &request)?;
    if args.json {
        emit_ok(serde_json::json!({
            "authorized": true,
            "action": args.action,
            "root_issuer": token.root_issuer,
        }))?;
    } else {
        println!("authorized: {} (root {})", args.action, token.root_issuer);
    }
    Ok(())
}

fn build_task_body(
    tool: &str,
    raw_args: &str,
    cap: Option<&Path>,
    max_runtime_s: Option<u64>,
    max_output_bytes: Option<u64>,
) -> Result<task::TaskBody> {
    let arguments: serde_json::Value = serde_json::from_str(raw_args)
        .map_err(|err| RaftError::coded("parse", format!("--args must be JSON: {err}")))?;
    if !arguments.is_object() {
        bail_code!("parse", "--args must be a JSON object");
    }
    // If a capability is attached, fail fast on a malformed/forged token; the
    // worker's executor performs the full contextual authorization.
    let capability = match cap {
        Some(path) => {
            let token: capability::Token = read_json(path)?.ok_or_else(|| {
                RaftError::coded(
                    "not_found",
                    format!("no such capability file: {}", path.display()),
                )
            })?;
            capability::verify_chain(&token, None)?;
            Some(token)
        }
        None => None,
    };
    Ok(task::TaskBody {
        tool_call: task::ToolCall {
            name: tool.to_string(),
            arguments,
        },
        capability,
        limits: task::TaskLimits {
            max_runtime_s,
            max_output_bytes,
        },
    })
}

fn send_task_message(
    root: &Path,
    conversation_id: String,
    from: String,
    worker: String,
    tool: &str,
    body: task::TaskBody,
) -> Result<Message> {
    send_message(
        root,
        SendMessageInput {
            conversation_id,
            sender: from,
            to: worker.clone(),
            subject: format!("task: {tool}"),
            body: body.to_body_string()?,
            kind: "task".to_string(),
            after: None,
            subject_id: None,
            requires_ack: false,
            needs_response_from: worker,
        },
    )
}

/// `raft task dispatch` — delegate a Hermes tool call to a worker as a
/// capability-gated task ask. The obligation engine tracks it; the worker's
/// executor (`raft run`) authorizes and runs it.
fn cmd_task_dispatch(root: &Path, args: TaskDispatchArgs) -> Result<()> {
    let from = validate_id(&args.from, "agent id")?;
    let worker = validate_id(&args.to, "agent id")?;
    let conversation_id = target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    ensure_root(root)?;
    load_claimed_agent(root, &worker)?;
    let body = build_task_body(
        &args.tool,
        &args.args,
        args.cap.as_deref(),
        args.max_runtime_s,
        args.max_output_bytes,
    )?;
    let message = send_task_message(
        root,
        conversation_id,
        from,
        worker.clone(),
        &args.tool,
        body,
    )?;
    if args.json {
        emit_ok(serde_json::json!({
            "task_id": message.id,
            "conversation_id": message.conversation_id,
            "to": worker,
            "tool": args.tool,
        }))?;
    } else {
        println!(
            "dispatched task {} to @{worker} (tool {})",
            message.id, args.tool
        );
    }
    Ok(())
}

/// `raft task status` — the task's worker receipt lifecycle plus its result.
fn cmd_task_status(root: &Path, args: TaskStatusArgs) -> Result<()> {
    ensure_root(root)?;
    let task_id = validate_id(&args.task, "message id")?;
    let (_, message) = find_message(root, &task_id)?;
    if message.kind != "task" {
        bail_code!("not_found", "message {task_id:?} is not a task");
    }
    let receipts = load_message_receipts(root, &message)?;
    let workers: Vec<serde_json::Value> = message
        .needs_response_from
        .iter()
        .map(|worker| {
            let status = receipts
                .get(worker)
                .map(|receipt| receipt.status.clone())
                .unwrap_or_else(|| "pending".to_string());
            serde_json::json!({ "agent": worker, "status": status })
        })
        .collect();
    let tool = task::TaskBody::parse(&message.body)
        .ok()
        .map(|body| body.tool_call.name);
    let result = find_task_result(root, &message)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "task_id": task_id,
                "tool": tool,
                "from": message.from,
                "workers": workers,
                "result": result,
            }))?
        );
    } else {
        println!(
            "task {task_id} (tool {}) from @{}",
            tool.as_deref().unwrap_or("?"),
            message.from
        );
        for worker in &workers {
            println!(
                "  @{} -> {}",
                worker["agent"].as_str().unwrap_or("?"),
                worker["status"]
            );
        }
        match result {
            Some(result) => println!("  result: {}", serde_json::to_string(&result)?),
            None => println!("  result: (pending)"),
        }
    }
    Ok(())
}

/// `raft task cancel` — withdraw a dispatched task (releases the obligation and
/// notifies the worker to stop).
fn cmd_task_cancel(root: &Path, args: TaskCancelArgs) -> Result<()> {
    cmd_withdraw(
        root,
        WithdrawArgs {
            from: args.from,
            message_id: args.task,
            reason: args.reason,
            json: args.json,
        },
    )
}

/// Find the result reply for a task: a message in the same conversation that
/// follows the task and was authored by one of its workers.
fn find_task_result(root: &Path, task: &Message) -> Result<Option<task::TaskResult>> {
    let conv = conversation_path(root, &task.conversation_id)?;
    for entry in sorted_read_dir(&conv.join("messages"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(message): Option<Message> = read_json(&entry.path())? else {
            continue;
        };
        if message.after.as_deref() == Some(task.id.as_str())
            && task.needs_response_from.contains(&message.from)
            && let Ok(result) = serde_json::from_str::<task::TaskResult>(&message.body)
        {
            return Ok(Some(result));
        }
    }
    Ok(None)
}

/// `raft run` — the executor loop. Claims this worker's authorized task asks and
/// runs their tools in the sandbox, returning a result reply and closing the
/// obligation with a terminal receipt.
fn cmd_run(root: &Path, args: RunArgs) -> Result<()> {
    let worker = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    load_claimed_agent(root, &worker)?;
    let mut tools: BTreeMap<String, PathBuf> = BTreeMap::new();
    for spec in &args.tool {
        let (name, path) = spec.split_once('=').ok_or_else(|| {
            RaftError::new(format!("invalid --tool {spec:?}; use name=/path/to/exe"))
        })?;
        tools.insert(name.to_string(), PathBuf::from(path));
    }
    let trusted_root = match &args.trust {
        Some(value) => Some(resolve_pubkey(root, value)?),
        None => None,
    };
    let worker_pubkey = identity::load_passport(root, &worker)?.map(|passport| passport.pubkey);

    let interval = Duration::from_secs_f64(args.interval.max(0.05));
    let mut total = 0usize;
    loop {
        total += run_pending_tasks(
            root,
            &worker,
            &tools,
            trusted_root.as_deref(),
            worker_pubkey.as_deref(),
        )?;
        if args.once {
            break;
        }
        thread::sleep(interval);
    }
    if args.json {
        emit_ok(serde_json::json!({ "agent": worker, "processed": total }))?;
    } else {
        println!("executor processed {total} task(s)");
    }
    Ok(())
}

/// Process every task currently owed by `worker`. Returns how many were handled.
fn run_pending_tasks(
    root: &Path,
    worker: &str,
    tools: &BTreeMap<String, PathBuf>,
    trusted_root: Option<&str>,
    worker_pubkey: Option<&str>,
) -> Result<usize> {
    let asks = gather_open_asks(root, None, Some(worker))?;
    let mut processed = 0;
    for ask in asks {
        if ask.awaited != worker {
            continue;
        }
        let (_, message) = find_message(root, &ask.message_id)?;
        if message.kind != "task" {
            continue;
        }
        let body = match task::TaskBody::parse(&message.body) {
            Ok(body) => body,
            Err(err) => {
                record_task_outcome(
                    root,
                    worker,
                    &message,
                    false,
                    None,
                    Some(err.message),
                    None,
                    false,
                    Vec::new(),
                    None,
                )?;
                processed += 1;
                continue;
            }
        };
        // Authorization. A capability-bearing task is fully authorized against
        // the worker's runtime context; the worker must be the token's holder.
        if let Some(token) = &body.capability {
            if let Some(expected) = worker_pubkey
                && token.blocks.last().map(|block| block.holder.as_str()) != Some(expected)
            {
                record_task_outcome(
                    root,
                    worker,
                    &message,
                    false,
                    None,
                    Some("capability holder is not this worker".to_string()),
                    None,
                    false,
                    Vec::new(),
                    None,
                )?;
                processed += 1;
                continue;
            }
            let request = capability::AuthRequest {
                action: "tool.run",
                conversation: Some(&message.conversation_id),
                tool: Some(&body.tool_call.name),
                env: None,
                now: Utc::now(),
                requested_runtime_s: body.limits.max_runtime_s,
                requested_output_bytes: body.limits.max_output_bytes,
            };
            if let Err(err) = capability::authorize(token, trusted_root, &request) {
                record_task_outcome(
                    root,
                    worker,
                    &message,
                    false,
                    None,
                    Some(format!("unauthorized: {}", err.message)),
                    None,
                    false,
                    Vec::new(),
                    None,
                )?;
                processed += 1;
                continue;
            }
        } else if trusted_root.is_some() {
            // A pinned trust root means an unsigned task is not runnable.
            record_task_outcome(
                root,
                worker,
                &message,
                false,
                None,
                Some("task carries no capability but a trusted root is required".to_string()),
                None,
                false,
                Vec::new(),
                None,
            )?;
            processed += 1;
            continue;
        }
        let Some(executable) = tools.get(&body.tool_call.name) else {
            record_task_outcome(
                root,
                worker,
                &message,
                false,
                None,
                Some(format!("unknown tool {:?}", body.tool_call.name)),
                None,
                false,
                Vec::new(),
                None,
            )?;
            processed += 1;
            continue;
        };
        let timeout = Duration::from_secs(body.limits.max_runtime_s.unwrap_or(30));
        let max_output = body.limits.max_output_bytes.unwrap_or(1_048_576) as usize;
        let scratch = root.join("run").join("sandbox").join(&message.id);
        let outcome = match task::run_tool(
            executable,
            &body.tool_call.arguments,
            &scratch,
            timeout,
            max_output,
        ) {
            Ok(outcome) => outcome,
            Err(err) => {
                let _ = fs::remove_dir_all(&scratch);
                record_task_outcome(
                    root,
                    worker,
                    &message,
                    false,
                    None,
                    Some(err.message),
                    None,
                    false,
                    Vec::new(),
                    None,
                )?;
                processed += 1;
                continue;
            }
        };
        let _ = fs::remove_dir_all(&scratch);

        let (ok, error) = if outcome.timed_out {
            (
                false,
                Some(format!("tool timed out after {}s", timeout.as_secs())),
            )
        } else if outcome.exit_code == Some(0) {
            (true, None)
        } else {
            (
                false,
                Some(format!(
                    "tool exited with code {}: {}",
                    outcome
                        .exit_code
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "killed".into()),
                    outcome.stderr.trim()
                )),
            )
        };
        let result_value = if ok {
            let trimmed = outcome.stdout.trim();
            Some(
                serde_json::from_str::<serde_json::Value>(trimmed)
                    .unwrap_or_else(|_| serde_json::Value::String(outcome.stdout.clone())),
            )
        } else {
            None
        };
        let mut artifacts = Vec::new();
        if let Some(artifact) = persist_task_artifact(root, "stdout", outcome.stdout.as_bytes())? {
            artifacts.push(artifact);
        }
        if let Some(artifact) = persist_task_artifact(root, "stderr", outcome.stderr.as_bytes())? {
            artifacts.push(artifact);
        }
        let log = Some(persist_task_log(root, &message, &outcome)?);
        record_task_outcome(
            root,
            worker,
            &message,
            ok,
            result_value,
            error,
            outcome.exit_code,
            outcome.truncated,
            artifacts,
            log,
        )?;
        processed += 1;
    }
    Ok(processed)
}

fn persist_task_artifact(
    root: &Path,
    name: &str,
    bytes: &[u8],
) -> Result<Option<task::TaskArtifact>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let hash = crypto::sha256_hex(bytes);
    let hash_hex = hash.strip_prefix("sha256:").unwrap_or(&hash);
    let relative = format!("artifacts/sha256-{hash_hex}");
    let path = root.join(&relative);
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
            set_dir_private(parent)?;
        }
        fs::write(&path, bytes)?;
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    Ok(Some(task::TaskArtifact {
        name: name.to_string(),
        hash,
        path: relative,
        bytes: bytes.len() as u64,
    }))
}

fn persist_task_log(
    root: &Path,
    task_message: &Message,
    outcome: &task::ToolOutcome,
) -> Result<String> {
    let relative = format!(
        "conversations/{}/streams/{}.log",
        task_message.conversation_id, task_message.id
    );
    let path = root.join(&relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        set_dir_private(parent)?;
    }
    let mut log = String::new();
    if !outcome.stdout.is_empty() {
        log.push_str("[stdout] ");
        log.push_str(&outcome.stdout);
        if !outcome.stdout.ends_with('\n') {
            log.push('\n');
        }
    }
    if !outcome.stderr.is_empty() {
        log.push_str("[stderr] ");
        log.push_str(&outcome.stderr);
        if !outcome.stderr.ends_with('\n') {
            log.push('\n');
        }
    }
    if outcome.timed_out {
        log.push_str("[timeout]\n");
    }
    log.push_str(&format!(
        "[exit] {}\n",
        outcome
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "killed".to_string())
    ));
    fs::write(&path, log)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    Ok(relative)
}

/// Deliver a task's result as a reply and close the obligation with a terminal
/// receipt (`done` on success, `rejected` otherwise).
#[allow(clippy::too_many_arguments)]
fn record_task_outcome(
    root: &Path,
    worker: &str,
    task: &Message,
    ok: bool,
    result: Option<serde_json::Value>,
    error: Option<String>,
    exit_code: Option<i32>,
    truncated: bool,
    artifacts: Vec<task::TaskArtifact>,
    log: Option<String>,
) -> Result<()> {
    let status = if ok { "done" } else { "rejected" };
    let result_body = task::TaskResult {
        ok,
        result,
        error: error.clone(),
        exit_code,
        output_truncated: truncated,
        artifacts,
        log,
    }
    .to_body_string()?;
    send_message(
        root,
        SendMessageInput {
            conversation_id: task.conversation_id.clone(),
            sender: worker.to_string(),
            to: task.from.clone(),
            subject: format!("result: {}", task.subject),
            body: result_body,
            kind: "message".to_string(),
            after: Some(task.id.clone()),
            subject_id: None,
            requires_ack: false,
            needs_response_from: String::new(),
        },
    )?;
    let note = error.or_else(|| Some("ok".to_string()));
    write_receipt(root, worker, task, status, note)?;
    Ok(())
}

fn cmd_grant_inspect(_root: &Path, args: GrantInspectArgs) -> Result<()> {
    let token: capability::Token = read_json(&args.token_file)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no such token file: {}", args.token_file.display()),
        )
    })?;
    let effective = capability::verify_chain(&token, None)?;
    let effective_json = capability::effective_to_json(&effective);
    if args.json {
        let blocks: Vec<serde_json::Value> = token
            .blocks
            .iter()
            .map(|block| {
                serde_json::json!({
                    "issuer": block.issuer,
                    "holder": block.holder,
                    "caveats": block.caveats,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "root_issuer": token.root_issuer,
                "blocks": blocks,
                "effective": effective_json,
                "signatures_valid": true,
            }))?
        );
    } else {
        println!("capability rooted at {}", token.root_issuer);
        for (index, block) in token.blocks.iter().enumerate() {
            println!("  block {index}: {} -> {}", block.issuer, block.holder);
        }
        println!("  signatures: valid");
        println!("  effective scope: {effective_json}");
    }
    Ok(())
}

/// `raft id new` — mint an Ed25519 keypair + self-signed passport for an agent.
fn cmd_id_new(root: &Path, args: IdNewArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let capabilities = match &args.capabilities {
        Some(raw) => split_csv(raw)?,
        None => Vec::new(),
    };
    let passport = identity::create_identity(root, &agent_id, &capabilities)?;
    if args.json {
        emit_ok(serde_json::json!({
            "agent": agent_id,
            "pubkey": passport.pubkey.clone(),
            "fingerprint": fingerprint(&passport.pubkey),
            "capabilities": passport.capabilities,
            "issued_at": passport.issued_at,
        }))?;
    } else {
        println!(
            "minted identity for @{agent_id}\n  pubkey      {}\n  fingerprint {}",
            passport.pubkey,
            fingerprint(&passport.pubkey)
        );
    }
    Ok(())
}

/// `raft id show` — print an agent's passport (shareable public identity).
fn cmd_id_show(root: &Path, args: IdShowArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let passport = identity::load_passport(root, &agent_id)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no identity for {agent_id:?}; run `raft id new`"),
        )
    })?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&passport)?);
    } else {
        println!(
            "@{} {}\n  fingerprint {}\n  capabilities {}\n  issued_at {}",
            passport.id,
            passport.pubkey,
            fingerprint(&passport.pubkey),
            if passport.capabilities.is_empty() {
                "(none)".to_string()
            } else {
                passport.capabilities.join(", ")
            },
            passport.issued_at,
        );
    }
    Ok(())
}

/// `raft id verify` — verify a passport's self-signature, by agent id or file.
fn cmd_id_verify(root: &Path, args: IdVerifyArgs) -> Result<()> {
    let passport = match (&args.agent, &args.file) {
        (Some(_), Some(_)) => {
            bail!("pass either an agent id or --file, not both");
        }
        (None, None) => {
            bail!("pass an agent id or --file to verify");
        }
        (Some(agent), None) => {
            let agent_id = validate_id(agent, "agent id")?;
            ensure_root(root)?;
            identity::load_passport(root, &agent_id)?.ok_or_else(|| {
                RaftError::coded(
                    "not_found",
                    format!("no identity for {agent_id:?}; run `raft id new`"),
                )
            })?
        }
        (None, Some(path)) => read_json::<identity::Passport>(path)?.ok_or_else(|| {
            RaftError::coded("not_found", format!("no such file: {}", path.display()))
        })?,
    };
    let result = passport.verify();
    if args.json {
        match &result {
            Ok(()) => emit_ok(serde_json::json!({
                "id": passport.id,
                "pubkey": passport.pubkey,
                "fingerprint": fingerprint(&passport.pubkey),
                "verified": true,
            }))?,
            Err(err) => {
                return Err(RaftError::coded("error", err.message.clone())
                    .with_details(serde_json::json!({ "id": passport.id, "verified": false })));
            }
        }
    } else {
        match &result {
            Ok(()) => println!(
                "ok: @{} passport verifies ({})",
                passport.id, passport.pubkey
            ),
            Err(err) => return Err(RaftError::coded("error", err.message.clone())),
        }
    }
    Ok(())
}

/// `raft id fingerprint` — print the short public-key fingerprint for an agent.
fn cmd_id_fingerprint(root: &Path, args: IdFingerprintArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let passport = identity::load_passport(root, &agent_id)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no identity for {agent_id:?}; run `raft id new`"),
        )
    })?;
    let fingerprint = fingerprint(&passport.pubkey);
    if args.json {
        emit_ok(serde_json::json!({
            "agent": agent_id,
            "pubkey": passport.pubkey,
            "fingerprint": fingerprint,
        }))?;
    } else {
        println!("{fingerprint}");
    }
    Ok(())
}

/// A short, human-comparable fingerprint of a public key: the first 16 hex chars
/// of `sha256(pubkey-string)`, grouped for readability. Enough to eyeball that
/// two agents mean the same key; the full pubkey remains the authority.
fn fingerprint(pubkey: &str) -> String {
    let digest = crypto::sha256_hex(pubkey.as_bytes());
    let hex = digest.strip_prefix("sha256:").unwrap_or(&digest);
    let short = &hex[..hex.len().min(16)];
    short
        .as_bytes()
        .chunks(4)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("-")
}

fn agent_signing_key(root: &Path, agent_id: &str) -> Result<(String, crypto::Keypair)> {
    let agent: Agent =
        read_json(&agent_path(root, agent_id))?.ok_or_else(|| not_claimed(root, agent_id))?;
    let pubkey = agent.pubkey.ok_or_else(|| {
        RaftError::coded(
            "auth_failed",
            format!(
                "agent @{agent_id} is not bound to a passport key; run `raft register {agent_id}`"
            ),
        )
    })?;
    let keypair = identity::load_bound_keypair(root, agent_id, &pubkey)?;
    Ok((pubkey, keypair))
}

fn signed_record_hash<T: Serialize>(record: &T) -> Result<String> {
    let value = serde_json::to_value(record)?;
    Ok(crypto::sha256_hex(&crypto::canonical_omitting(
        &value,
        &["hash", "sig"],
    )?))
}

fn signed_record_signature<T: Serialize>(keypair: &crypto::Keypair, record: &T) -> Result<String> {
    let value = serde_json::to_value(record)?;
    Ok(keypair.sign(&crypto::canonical_omitting(&value, &["sig"])?))
}

fn sign_message(root: &Path, sender: &str, message: &mut Message) -> Result<()> {
    let (pubkey, keypair) = agent_signing_key(root, sender)?;
    message.signer_key = Some(pubkey);
    message.hash = None;
    message.sig = None;
    message.hash = Some(signed_record_hash(message)?);
    message.sig = Some(signed_record_signature(&keypair, message)?);
    Ok(())
}

fn sign_receipt(root: &Path, agent_id: &str, receipt: &mut Receipt) -> Result<()> {
    let (pubkey, keypair) = agent_signing_key(root, agent_id)?;
    receipt.signer_key = Some(pubkey);
    receipt.hash = None;
    receipt.sig = None;
    receipt.hash = Some(signed_record_hash(receipt)?);
    receipt.sig = Some(signed_record_signature(&keypair, receipt)?);
    Ok(())
}

fn root_path(root: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = root {
        return Ok(path);
    }
    if let Ok(value) = env::var("RAFT_ROOT") {
        return Ok(PathBuf::from(value));
    }
    Ok(env::current_dir()?.join("run").join("bus"))
}

fn cmd_init(root: &Path, args: InitArgs) -> Result<()> {
    ensure_root(root)?;
    if args.json {
        emit_ok(serde_json::json!({ "root": root.display().to_string() }))?;
    } else {
        println!("initialized raft bus at {}", root.display());
    }
    Ok(())
}

/// Print a success envelope `{"ok":true, ...fields}` as pretty JSON. Mirrors the
/// failure envelope emitted by `fail` so agents see a consistent `ok` discriminant
/// on both paths.
fn emit_ok(fields: serde_json::Value) -> Result<()> {
    let mut map = serde_json::Map::new();
    map.insert("ok".to_string(), serde_json::Value::Bool(true));
    if let serde_json::Value::Object(extra) = fields {
        map.extend(extra);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Object(map))?
    );
    Ok(())
}

fn migrate_conversation_records(conv: &Path) -> Result<()> {
    if let Some(meta) = read_json::<Meta>(&conv.join("meta.json"))? {
        atomic_write_json(&conv.join("meta.json"), &meta)?;
    }
    if let Some(rate) = read_json::<RateState>(&conv.join("rate.json"))? {
        atomic_write_json(&conv.join("rate.json"), &rate)?;
    }
    for entry in sorted_read_dir(&conv.join("messages"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        if let Some(message) = read_json::<Message>(&entry.path())? {
            atomic_write_json(&entry.path(), &message)?;
        }
    }
    for message_receipts in sorted_read_dir(&conv.join("receipts"))? {
        if !message_receipts.path().is_dir() {
            continue;
        }
        for entry in sorted_read_dir(&message_receipts.path())? {
            if entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            if let Some(receipt) = read_json::<Receipt>(&entry.path())? {
                atomic_write_json(&entry.path(), &receipt)?;
            }
        }
    }
    Ok(())
}

fn cmd_claim(root: &Path, args: ClaimArgs) -> Result<()> {
    let agent_id = validate_claim_name(&args.agent)?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let path = agent_path(root, &agent_id);
    if path.exists() {
        bail_code!("conflict", "agent name @{agent_id} is already claimed");
    }
    let workspace = args.workspace.map(|path| resolve_path(&path)).transpose()?;
    let capabilities = split_csv(&args.capabilities)?;
    let passport = identity::ensure_identity(root, &agent_id, &capabilities)?;
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.clone(),
        mention: format!("@{agent_id}"),
        workspace,
        capabilities,
        pubkey: Some(passport.pubkey.clone()),
        pid: process::id(),
        host: hostname(),
        last_seen_at: iso_now(),
        ttl_seconds: args.ttl,
        expires_at: iso_after(args.ttl),
        current_state: default_agent_state(),
        state_note: None,
        state_updated_at: iso_now(),
    };
    atomic_write_json(&path, &payload)?;
    if args.json {
        emit_ok(serde_json::json!({
            "agent": agent_id,
            "mention": payload.mention,
            "pubkey": passport.pubkey.clone(),
            "fingerprint": fingerprint(&passport.pubkey),
            "expires_at": payload.expires_at,
            "root": root.display().to_string(),
        }))?;
    } else {
        println!("claimed @{agent_id} at {}", root.display());
    }
    Ok(())
}

fn cmd_register(root: &Path, args: RegisterArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let previous: Agent =
        read_json(&agent_path(root, &agent_id))?.ok_or_else(|| not_claimed(root, &agent_id))?;
    let workspace = args
        .workspace
        .map(|path| resolve_path(&path))
        .transpose()?
        .or_else(|| previous.workspace.clone());
    let capabilities = {
        let parsed = split_csv(&args.capabilities)?;
        if parsed.is_empty() {
            previous.capabilities.clone()
        } else {
            parsed
        }
    };
    let passport = identity::ensure_identity(root, &agent_id, &capabilities)?;
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.clone(),
        mention: format!("@{agent_id}"),
        workspace,
        capabilities,
        pubkey: Some(passport.pubkey.clone()),
        pid: process::id(),
        host: hostname(),
        last_seen_at: iso_now(),
        ttl_seconds: args.ttl,
        expires_at: iso_after(args.ttl),
        current_state: previous.current_state,
        state_note: previous.state_note,
        state_updated_at: previous.state_updated_at,
    };
    atomic_write_json(&agent_path(root, &agent_id), &payload)?;
    if args.json {
        emit_ok(serde_json::json!({
            "agent": agent_id,
            "mention": payload.mention,
            "pubkey": passport.pubkey.clone(),
            "fingerprint": fingerprint(&passport.pubkey),
            "expires_at": payload.expires_at,
            "root": root.display().to_string(),
        }))?;
    } else {
        println!("registered {agent_id} at {}", root.display());
    }
    Ok(())
}

fn cmd_heartbeat(root: &Path, args: HeartbeatArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    if args.watch {
        return cmd_heartbeat_watch(root, &agent_id, args.ttl, args.interval);
    }
    let agent = heartbeat_once(root, &agent_id, args.ttl, !args.json)?;
    if args.json {
        emit_ok(serde_json::json!({
            "agent": agent.id,
            "last_seen_at": agent.last_seen_at,
            "expires_at": agent.expires_at,
        }))?;
    }
    Ok(())
}

fn heartbeat_once(
    root: &Path,
    agent_id: &str,
    ttl_override: Option<u64>,
    print: bool,
) -> Result<Agent> {
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let previous: Agent =
        read_json(&agent_path(root, agent_id))?.ok_or_else(|| not_claimed(root, agent_id))?;
    let ttl = ttl_override.unwrap_or(previous.ttl_seconds);
    let pubkey = match previous.pubkey {
        Some(pubkey) => Some(pubkey),
        None => Some(identity::ensure_identity(root, agent_id, &previous.capabilities)?.pubkey),
    };
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.to_string(),
        mention: format!("@{agent_id}"),
        workspace: previous.workspace,
        capabilities: previous.capabilities,
        pubkey,
        pid: process::id(),
        host: hostname(),
        last_seen_at: iso_now(),
        ttl_seconds: ttl,
        expires_at: iso_after(ttl),
        current_state: previous.current_state,
        state_note: previous.state_note,
        state_updated_at: previous.state_updated_at,
    };
    atomic_write_json(&agent_path(root, agent_id), &payload)?;
    if print {
        println!("heartbeat {agent_id}");
    }
    Ok(payload)
}

fn cmd_heartbeat_watch(
    root: &Path,
    agent_id: &str,
    ttl_override: Option<u64>,
    interval_override: Option<f64>,
) -> Result<()> {
    let previous: Agent =
        read_json(&agent_path(root, agent_id))?.ok_or_else(|| not_claimed(root, agent_id))?;
    let ttl = ttl_override.unwrap_or(previous.ttl_seconds);
    let interval = interval_override.unwrap_or_else(|| (ttl as f64 / 2.0).max(1.0));
    if !interval.is_finite() || interval <= 0.0 {
        bail!("--interval must be a positive finite number");
    }
    let state_path = heartbeat_state_path(root, agent_id);
    let _lock = DirLock::acquire(
        root,
        &format!("heartbeat-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if let Some(existing) = read_json::<HeartbeatState>(&state_path)?
        && existing.shutdown_at.is_none()
        && existing.pid != process::id()
        && process_is_alive(existing.pid)
    {
        bail_code!(
            "conflict",
            "heartbeat watcher for @{agent_id} already appears active with pid {}",
            existing.pid
        );
    }
    // Install signal handlers before publishing our pid so a SIGTERM that lands
    // during startup is caught and turned into a graceful shutdown instead of
    // hitting the default disposition and killing us mid-init.
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_flag::register(SIGTERM, Arc::clone(&shutdown))?;
    signal_flag::register(SIGINT, Arc::clone(&shutdown))?;

    let started_at = iso_now();
    let mut state = HeartbeatState {
        v: SCHEMA_VERSION,
        agent: agent_id.to_string(),
        pid: process::id(),
        host: hostname(),
        started_at: started_at.clone(),
        updated_at: started_at.clone(),
        last_heartbeat_at: started_at,
        interval_seconds: interval,
        ttl_seconds: ttl,
        shutdown_at: None,
    };
    atomic_write_json(&state_path, &state)?;
    drop(_lock);

    loop {
        let agent = heartbeat_once(root, agent_id, Some(ttl), false)?;
        state.last_heartbeat_at = agent.last_seen_at;
        state.updated_at = iso_now();
        atomic_write_json(&state_path, &state)?;
        sleep_interruptibly(Duration::from_secs_f64(interval), &shutdown);
        if shutdown.load(Ordering::Relaxed) {
            state.shutdown_at = Some(iso_now());
            state.updated_at = iso_now();
            atomic_write_json(&state_path, &state)?;
            return Ok(());
        }
    }
}

fn cmd_state_set(root: &Path, args: StateSetArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let state = validate_agent_state(&args.state)?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let previous: Agent =
        read_json(&agent_path(root, &agent_id))?.ok_or_else(|| not_claimed(root, &agent_id))?;
    let changed = previous.current_state != state || previous.state_note != args.note;
    let now = iso_now();
    let pubkey = match previous.pubkey {
        Some(pubkey) => Some(pubkey),
        None => Some(identity::ensure_identity(root, &agent_id, &previous.capabilities)?.pubkey),
    };
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.clone(),
        mention: format!("@{agent_id}"),
        workspace: previous.workspace,
        capabilities: previous.capabilities,
        pubkey,
        pid: process::id(),
        host: hostname(),
        last_seen_at: now.clone(),
        ttl_seconds: previous.ttl_seconds,
        expires_at: iso_after(previous.ttl_seconds),
        current_state: state.clone(),
        state_note: args.note.clone(),
        state_updated_at: now,
    };
    atomic_write_json(&agent_path(root, &agent_id), &payload)?;
    if changed {
        write_state_change_messages(root, &agent_id, &state, args.note.as_deref())?;
    }
    if args.json {
        emit_ok(serde_json::json!({
            "agent": agent_id,
            "state": state,
            "note": args.note,
            "changed": changed,
        }))?;
    } else {
        println!("@{agent_id} {state}");
    }
    Ok(())
}

fn cmd_state_get(root: &Path, args: StateGetArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let agent: Agent =
        read_json(&agent_path(root, &agent_id))?.ok_or_else(|| not_claimed(root, &agent_id))?;
    // A crashed or exited agent leaves its last `current_state` on disk
    // unchanged, so a bare `state get` would report e.g. `working` as if it
    // were authoritative. Join the same liveness signal `roster`/`me` use
    // (heartbeat TTL not yet expired) so a reader can tell a current state from
    // a stale one before acting on it.
    let live = parse_time(&agent.expires_at)
        .map(|expires_at| expires_at >= Utc::now())
        .unwrap_or(false);
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": agent.id,
                "state": agent.current_state,
                "note": agent.state_note,
                "updated_at": agent.state_updated_at,
                "live": live,
                "last_seen_at": agent.last_seen_at,
                "expires_at": agent.expires_at,
            }))?
        );
    } else {
        let staleness = if live { "" } else { " (stale)" };
        match agent.state_note {
            Some(note) => println!(
                "@{} {}{} since {} — {}",
                agent.id, agent.current_state, staleness, agent.state_updated_at, note
            ),
            None => println!(
                "@{} {}{} since {}",
                agent.id, agent.current_state, staleness, agent.state_updated_at
            ),
        }
    }
    Ok(())
}

fn cmd_channel_create(root: &Path, args: ChannelCreateArgs) -> Result<()> {
    let channel_id = validate_id(&args.channel, "channel id")?;
    let creator = validate_id(&args.creator, "creator")?;
    let mut participants = vec![creator.clone()];
    participants.extend(split_csv(&args.members)?);
    let participants = unique(participants);
    ensure_root(root)?;
    ensure_claimed_agents(root, &participants)?;
    let conv = conversation_path(root, &channel_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{channel_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if conv.join("meta.json").exists() {
        if args.if_missing {
            migrate_conversation_records(&conv)?;
            let meta: Meta = read_json(&conv.join("meta.json"))?
                .ok_or_else(|| RaftError::new(format!("channel {channel_id:?} has no metadata")))?;
            if !meta.channel {
                bail_code!(
                    "conflict",
                    "{channel_id:?} already exists but is not a channel"
                );
            }
            if args.json {
                emit_ok(serde_json::json!({
                    "channel": channel_id,
                    "created": false,
                    "participants": meta.participants,
                    "root": root.display().to_string(),
                }))?;
            } else {
                println!("channel {channel_id} ready; root={}", root.display());
            }
            return Ok(());
        }
        bail_code!("conflict", "channel {channel_id:?} already exists");
    }
    fs::create_dir_all(conv.join("messages"))?;
    fs::create_dir_all(conv.join("receipts"))?;
    set_dir_private(&conv)?;
    set_dir_private(&conv.join("messages"))?;
    set_dir_private(&conv.join("receipts"))?;

    let now = iso_now();
    let joined_at = participants
        .iter()
        .map(|id| (id.clone(), now.clone()))
        .collect();
    let meta = Meta {
        v: SCHEMA_VERSION,
        id: channel_id.clone(),
        participants,
        channel: true,
        private: false,
        state: "open".to_string(),
        created_at: now.clone(),
        updated_at: now,
        retention_days: args.retention_days,
        rate: Rate {
            window_seconds: args.rate_window,
            max_messages_per_sender: args.rate_max,
            max_message_bytes: args.max_message_bytes,
        },
        joined_at,
    };
    atomic_write_json(&conv.join("meta.json"), &meta)?;
    write_system_message(
        &conv,
        &channel_id,
        vec![creator],
        format!(
            "Channel opened. Subscribers: {}.",
            meta.participants.join(",")
        ),
        "channel opened",
    )?;
    if args.json {
        emit_ok(serde_json::json!({
            "channel": channel_id,
            "created": true,
            "participants": meta.participants,
            "root": root.display().to_string(),
        }))?;
    } else {
        println!("channel {channel_id} ready; root={}", root.display());
    }
    Ok(())
}

fn cmd_channel_join(root: &Path, args: ChannelJoinArgs) -> Result<()> {
    let channel_id = validate_id(&args.channel, "channel id")?;
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    load_claimed_agent(root, &agent_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{channel_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let conv = conversation_path(root, &channel_id)?;
    let mut meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| conversation_not_found(root, &channel_id, "channel"))?;
    if !meta.channel {
        bail!("{channel_id:?} is not a channel");
    }
    let already_member = meta
        .participants
        .iter()
        .any(|participant| participant == &agent_id);
    if !already_member {
        let now = iso_now();
        let prior_meta = meta.clone();
        meta.participants.push(agent_id.clone());
        // Preserve the original join time across a leave/rejoin. Overwriting it
        // with `now` would push the membership baseline past any ask created
        // during the prior membership, so `predates_membership` would hide an
        // undischarged obligation — silently resolving owed work on reconnect.
        meta.joined_at
            .entry(agent_id.clone())
            .or_insert_with(|| now.clone());
        meta.updated_at = now;
        atomic_write_json(&conv.join("meta.json"), &meta)?;
        write_system_message(
            &conv,
            &channel_id,
            vec!["*".to_string()],
            format!("@{agent_id} joined channel {channel_id}."),
            "channel joined",
        )?;
        write_onboarding_summary_message(&conv, &channel_id, &agent_id, &prior_meta)?;
    }
    if args.json {
        emit_ok(serde_json::json!({
            "channel": channel_id,
            "agent": agent_id,
            "joined": !already_member,
            "participants": meta.participants,
        }))?;
    } else {
        println!("@{agent_id} subscribed to channel {channel_id}");
    }
    Ok(())
}

fn cmd_channel_list(root: &Path, args: ChannelListArgs) -> Result<()> {
    ensure_root(root)?;
    let agent = match &args.agent {
        Some(name) => Some(validate_id(name, "agent id")?),
        None => None,
    };
    let mut channels = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta.channel {
            continue;
        }
        let mut total = 0usize;
        let mut unread = 0usize;
        for message_entry in sorted_read_dir(&conv.join("messages"))? {
            if message_entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            let Some(message): Option<Message> = read_json(&message_entry.path())? else {
                continue;
            };
            total += 1;
            if let Some(agent) = &agent
                && message_visible_to(&message, agent)
                && message_is_unread(root, &message, agent)
            {
                unread += 1;
            }
        }
        let mut record = serde_json::json!({
            "id": meta.id,
            "members": meta.participants,
            "member_count": meta.participants.len(),
            "messages": total,
        });
        if let Some(agent) = &agent {
            let joined = meta.participants.iter().any(|item| item == agent);
            record["joined"] = serde_json::json!(joined);
            record["unread"] = serde_json::json!(if joined { unread } else { 0 });
        }
        channels.push(record);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&channels)?);
        return Ok(());
    }
    println!("channels ({} found):", channels.len());
    if channels.is_empty() {
        println!("  none");
    }
    for channel in &channels {
        let id = channel["id"].as_str().unwrap_or("unknown");
        let members = channel["member_count"].as_u64().unwrap_or(0);
        let messages = channel["messages"].as_u64().unwrap_or(0);
        if agent.is_some() {
            let membership = if channel["joined"].as_bool().unwrap_or(false) {
                "joined"
            } else {
                "not joined"
            };
            println!(
                "  {id} [{membership}] {members} members, {messages} messages, {} unread",
                channel["unread"].as_u64().unwrap_or(0)
            );
        } else {
            println!("  {id} — {members} members, {messages} messages");
        }
    }
    Ok(())
}

fn cmd_conversation_create(root: &Path, args: ConversationCreateArgs) -> Result<()> {
    let conversation_id = validate_id(&args.conversation, "conversation id")?;
    let participants = unique(split_csv(&args.participants)?);
    if participants.len() < 2 {
        bail_code!("parse", "a conversation needs at least two participants");
    }
    for participant in &participants {
        validate_id(participant, "participant")?;
    }
    let starter = match args.starter {
        Some(starter) => validate_id(&starter, "starter")?,
        None => participants[0].clone(),
    };
    if !participants.contains(&starter) {
        bail_code!("parse", "starter must be one of the participants");
    }

    ensure_root(root)?;
    ensure_claimed_agents(root, &participants)?;
    let conv = conversation_path(root, &conversation_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if conv.join("meta.json").exists() {
        if args.if_missing {
            migrate_conversation_records(&conv)?;
            if args.json {
                let meta: Meta = read_json(&conv.join("meta.json"))?.ok_or_else(|| {
                    RaftError::coded(
                        "not_found",
                        format!("conversation {conversation_id:?} has no metadata"),
                    )
                })?;
                emit_ok(serde_json::json!({
                    "conversation_id": conversation_id,
                    "created": false,
                    "participants": meta.participants,
                    "root": root.display().to_string(),
                }))?;
            } else {
                println!(
                    "conversation {conversation_id} ready; root={}",
                    root.display()
                );
            }
            return Ok(());
        }
        bail_code!(
            "conflict",
            "conversation {conversation_id:?} already exists"
        );
    }
    fs::create_dir_all(conv.join("messages"))?;
    fs::create_dir_all(conv.join("receipts"))?;
    set_dir_private(&conv)?;
    set_dir_private(&conv.join("messages"))?;
    set_dir_private(&conv.join("receipts"))?;

    let now = iso_now();
    let joined_at = participants
        .iter()
        .map(|id| (id.clone(), now.clone()))
        .collect();
    let meta = Meta {
        v: SCHEMA_VERSION,
        id: conversation_id.clone(),
        participants,
        channel: false,
        private: args.private,
        state: "open".to_string(),
        created_at: now.clone(),
        updated_at: now,
        retention_days: args.retention_days,
        rate: Rate {
            window_seconds: args.rate_window,
            max_messages_per_sender: args.rate_max,
            max_message_bytes: args.max_message_bytes,
        },
        joined_at,
    };
    atomic_write_json(&conv.join("meta.json"), &meta)?;
    write_system_message(
        &conv,
        &conversation_id,
        vec![starter.clone()],
        format!(
            "Conversation opened by {starter}. Participants: {}.",
            meta.participants.join(",")
        ),
        "conversation opened",
    )?;
    if args.json {
        emit_ok(serde_json::json!({
            "conversation_id": conversation_id,
            "created": true,
            "participants": meta.participants,
            "root": root.display().to_string(),
        }))?;
    } else {
        println!(
            "conversation {conversation_id} ready; root={}",
            root.display()
        );
    }
    Ok(())
}

fn cmd_conversation_open(root: &Path, args: ConversationOpenArgs) -> Result<()> {
    let opener = validate_id(&args.opener, "opener")?;
    let mut participants = vec![opener.clone()];
    participants.extend(split_csv(&args.to)?);
    let participants = unique(participants);
    if participants.len() < 2 {
        bail_code!(
            "parse",
            "a private chat needs at least two unique participants"
        );
    }
    let conversation_id = match args.conversation {
        Some(id) => validate_id(&id, "conversation id")?,
        None => generated_private_conversation_id(&participants, &args.topic),
    };

    cmd_conversation_create(
        root,
        ConversationCreateArgs {
            conversation: conversation_id,
            participants: participants.join(","),
            starter: Some(opener),
            private: true,
            if_missing: args.if_missing,
            retention_days: args.retention_days,
            rate_window: args.rate_window,
            rate_max: args.rate_max,
            max_message_bytes: args.max_message_bytes,
            json: args.json,
        },
    )
}

fn cmd_conversation_add(root: &Path, args: ConversationAddArgs) -> Result<()> {
    let conversation_id = validate_id(&args.conversation, "conversation id")?;
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    load_claimed_agent(root, &agent_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let conv = conversation_path(root, &conversation_id)?;
    let mut meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| conversation_not_found(root, &conversation_id, "conversation"))?;
    if meta.channel {
        bail!("{conversation_id:?} is a channel; use `raft channel join` instead");
    }
    let already_participant = meta
        .participants
        .iter()
        .any(|participant| participant == &agent_id);
    if !already_participant {
        let now = iso_now();
        let prior_meta = meta.clone();
        meta.participants.push(agent_id.clone());
        // Preserve the original join time across a remove/re-add. Overwriting it
        // with `now` would push the membership baseline past any ask created
        // during the prior membership, so `predates_membership` would hide an
        // undischarged obligation — silently resolving owed work on re-add.
        meta.joined_at
            .entry(agent_id.clone())
            .or_insert_with(|| now.clone());
        meta.updated_at = now;
        atomic_write_json(&conv.join("meta.json"), &meta)?;
        write_system_message(
            &conv,
            &conversation_id,
            meta.participants.clone(),
            format!("@{agent_id} added to conversation {conversation_id}."),
            "participant added",
        )?;
        write_onboarding_summary_message(&conv, &conversation_id, &agent_id, &prior_meta)?;
    }
    if args.json {
        emit_ok(serde_json::json!({
            "conversation_id": conversation_id,
            "agent": agent_id,
            "added": !already_participant,
            "participants": meta.participants,
        }))?;
    } else if already_participant {
        println!("@{agent_id} is already a participant in {conversation_id}");
    } else {
        println!("@{agent_id} added to conversation {conversation_id}");
    }
    Ok(())
}

struct ParticipantRemoval {
    conversation_id: String,
    agent_id: String,
    removed: bool,
    participants: Vec<String>,
}

/// Shared body for `conversation remove` and `channel leave`: drop a participant
/// from an existing room. Idempotent — removing an agent that is not a member
/// reports `removed: false` and leaves the member set untouched. Refuses to
/// remove the last remaining participant so a room is never orphaned.
fn remove_participant(
    root: &Path,
    id: &str,
    agent: &str,
    want_channel: bool,
) -> Result<ParticipantRemoval> {
    let noun = if want_channel {
        "channel"
    } else {
        "conversation"
    };
    let conversation_id = validate_id(id, "conversation id")?;
    let agent_id = validate_id(agent, "agent id")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let conv = conversation_path(root, &conversation_id)?;
    let mut meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| conversation_not_found(root, &conversation_id, noun))?;
    if want_channel && !meta.channel {
        bail!("{conversation_id:?} is not a channel; use `raft conversation remove` instead");
    }
    if !want_channel && meta.channel {
        bail!("{conversation_id:?} is a channel; use `raft channel leave` instead");
    }
    let position = meta
        .participants
        .iter()
        .position(|participant| participant == &agent_id);
    let removed = if let Some(index) = position {
        if meta.participants.len() <= 1 {
            bail!("cannot remove the last participant from {conversation_id:?}");
        }
        meta.participants.remove(index);
        meta.updated_at = iso_now();
        atomic_write_json(&conv.join("meta.json"), &meta)?;
        let (verb, subject) = if want_channel {
            (
                format!("@{agent_id} left channel {conversation_id}."),
                "channel left",
            )
        } else {
            (
                format!("@{agent_id} removed from conversation {conversation_id}."),
                "participant removed",
            )
        };
        write_system_message(
            &conv,
            &conversation_id,
            meta.participants.clone(),
            verb,
            subject,
        )?;
        true
    } else {
        false
    };
    Ok(ParticipantRemoval {
        conversation_id,
        agent_id,
        removed,
        participants: meta.participants,
    })
}

fn cmd_conversation_remove(root: &Path, args: ConversationRemoveArgs) -> Result<()> {
    let result = remove_participant(root, &args.conversation, &args.agent, false)?;
    if args.json {
        emit_ok(serde_json::json!({
            "conversation_id": result.conversation_id,
            "agent": result.agent_id,
            "removed": result.removed,
            "participants": result.participants,
        }))?;
    } else if result.removed {
        println!(
            "@{} removed from conversation {}",
            result.agent_id, result.conversation_id
        );
    } else {
        println!(
            "@{} is not a participant in {}",
            result.agent_id, result.conversation_id
        );
    }
    Ok(())
}

fn cmd_channel_leave(root: &Path, args: ChannelLeaveArgs) -> Result<()> {
    let result = remove_participant(root, &args.channel, &args.agent, true)?;
    if args.json {
        emit_ok(serde_json::json!({
            "channel": result.conversation_id,
            "agent": result.agent_id,
            "left": result.removed,
            "members": result.participants,
        }))?;
    } else if result.removed {
        println!(
            "@{} left channel {}",
            result.agent_id, result.conversation_id
        );
    } else {
        println!(
            "@{} is not subscribed to channel {}",
            result.agent_id, result.conversation_id
        );
    }
    Ok(())
}

fn cmd_send(root: &Path, args: SendArgs) -> Result<()> {
    let conversation_id = target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let json = args.json;
    let message = send_message(
        root,
        SendMessageInput {
            conversation_id,
            sender: args.sender,
            to: args.to,
            subject: args.subject,
            body: args.body,
            kind: args.kind,
            after: args.after,
            subject_id: args.subject_id,
            requires_ack: args.requires_ack,
            needs_response_from: args.needs_response_from,
        },
    )?;
    let offline = offline_recipients(root, &message)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "message_id": message.id,
                "conversation_id": message.conversation_id,
                "to": message.to,
                "mentions": message.mentions,
                "needs_response_from": message.needs_response_from,
                "offline_recipients": offline,
            }))?
        );
    } else {
        println!("{}", message.id);
        warn_offline_recipients(&offline);
    }
    Ok(())
}

fn cmd_reply(root: &Path, args: ReplyArgs) -> Result<()> {
    ensure_root(root)?;
    let (_, parent) = find_message(root, &args.message)?;
    let json = args.json;
    let sender = validate_id(&args.sender, "sender")?;
    // Validate the optional ack status up front so a bad status fails before we
    // send the reply, rather than leaving a sent reply with no receipt.
    let ack_status = args.ack.as_deref().map(validate_ack_status).transpose()?;
    // A bare reply (no --to) answers only the parent's sender. In a group or
    // channel thread that silently drops everyone else who was on the parent, so
    // we surface the omitted participants below — but only when the audience was
    // defaulted, since an explicit --to is a deliberate choice we don't second-guess.
    let explicit_to = args.to.is_some();
    let to = args.to.unwrap_or_else(|| parent.from.clone());
    let subject = args.subject.unwrap_or_else(|| parent.subject.clone());
    // Hold the conversation lock across both the reply send and the optional
    // ack receipt so `reply --ack` is atomic: a lock failure aborts before
    // anything is written (no half-sent reply that a retry would duplicate),
    // and no other writer can interleave between the two writes.
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", parent.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let message = send_message_locked(
        root,
        SendMessageInput {
            conversation_id: parent.conversation_id.clone(),
            sender: sender.clone(),
            to,
            subject,
            body: args.body,
            kind: args.kind,
            after: Some(parent.id.clone()),
            subject_id: None,
            requires_ack: args.requires_ack,
            needs_response_from: args.needs_response_from,
        },
    )?;
    // Report the effective stored status: the downgrade guard may preserve a
    // stronger existing receipt (e.g. `reply --ack working` after `done`).
    let effective_ack = match &ack_status {
        Some(status) => Some(write_receipt(
            root,
            &sender,
            &parent,
            status,
            args.ack_note,
        )?),
        None => None,
    };
    let offline = offline_recipients(root, &message)?;
    let omitted = if explicit_to {
        Vec::new()
    } else {
        omitted_thread_participants(root, &parent, &message)?
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": true,
                "message_id": message.id,
                "conversation_id": message.conversation_id,
                "to": message.to,
                "mentions": message.mentions,
                "needs_response_from": message.needs_response_from,
                "after": message.after,
                "ack": effective_ack,
                "offline_recipients": offline,
                "omitted_recipients": omitted,
            }))?
        );
    } else {
        println!("{}", message.id);
        warn_offline_recipients(&offline);
        warn_omitted_recipients(&omitted);
    }
    Ok(())
}

/// Conversation participants who were in the loop on the parent message but are
/// NOT reached by this reply. A bare `reply` defaults its audience to the
/// parent's sender, so in a group or channel thread the parent's other
/// recipients silently fall out; this names them so the agent can re-address
/// with `--to` if it meant to answer the whole thread. Both `*` audiences expand
/// to the conversation's participants before the diff.
fn omitted_thread_participants(
    root: &Path,
    parent: &Message,
    reply: &Message,
) -> Result<Vec<String>> {
    let (_, meta) = load_conversation(root, &parent.conversation_id)?;
    let expand = |recipients: &[String]| -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        for recipient in recipients {
            if recipient == "*" {
                set.extend(meta.participants.iter().cloned());
            } else {
                set.insert(recipient.clone());
            }
        }
        set
    };
    // The parent's audience is everyone it was addressed to, plus its sender.
    let mut parent_audience = expand(&parent.to);
    parent_audience.insert(parent.from.clone());
    let reached = expand(&reply.to);
    Ok(parent_audience
        .into_iter()
        .filter(|id| {
            id != &reply.from
                && id != "*"
                && !reached.contains(id)
                && meta.participants.contains(id)
        })
        .collect())
}

/// Text-mode courtesy: warn on stderr (so it never pollutes the stdout id) when
/// a message went to a recipient whose heartbeat has expired.
fn warn_offline_recipients(offline: &[String]) {
    if offline.is_empty() {
        return;
    }
    let names = offline
        .iter()
        .map(|id| format!("@{id}"))
        .collect::<Vec<_>>()
        .join(", ");
    eprintln!("warning: offline recipient(s): {names}");
}

/// Text-mode courtesy: warn on stderr when a bare `reply` reached fewer agents
/// than the parent thread, so the sender notices it answered only part of a
/// group/channel conversation and can re-address with `--to`.
fn warn_omitted_recipients(omitted: &[String]) {
    if omitted.is_empty() {
        return;
    }
    let names = omitted
        .iter()
        .map(|id| format!("@{id}"))
        .collect::<Vec<_>>()
        .join(", ");
    eprintln!(
        "warning: reply did not reach other thread participant(s): {names}; pass --to to include them"
    );
}

/// Retract an open ask the sender no longer needs answered. Marks the message
/// with a `withdrawn` stamp so it drops out of every `awaited` computation
/// (owed_to_you, roster owes, wait --owed) without deleting the message or
/// fabricating per-recipient receipts. Only the original sender may withdraw.
fn cmd_withdraw(root: &Path, args: WithdrawArgs) -> Result<()> {
    ensure_root(root)?;
    let asker = validate_id(&args.from, "from")?;
    let json = args.json;
    let (path, mut message) = find_message(root, &args.message_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", message.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    // Only the sender owns the ask. Mirror `wait --resolved`: a non-sender is
    // told the ask is not theirs via not_found rather than a distinct code.
    if message.from != asker {
        bail_code!(
            "not_found",
            "message {:?} is not an ask you sent",
            message.id
        );
    }
    // Idempotent: withdrawing an already-withdrawn ask is a no-op success.
    if let Some(existing) = &message.withdrawn {
        if json {
            emit_ok(serde_json::json!({
                "message_id": message.id,
                "withdrawn": true,
                "already_withdrawn": true,
                "released": Vec::<String>::new(),
                "at": existing.at,
            }))?;
        } else {
            println!("already withdrawn");
        }
        return Ok(());
    }
    let (conv, meta) = load_conversation(root, &message.conversation_id)?;
    // `released` must be the genuinely-open obligations, not every awaited agent:
    // an agent that already recorded a terminal receipt owes nothing, so it must
    // not appear in `released[]` nor receive an "ask withdrawn, stop work" notice
    // for work it already finished. Mirror `gather_open_asks`'s terminal-receipt
    // filter so withdraw agrees with every other open-ask view.
    let receipts = load_message_receipts(root, &message)?;
    let released: Vec<String> = message_awaited(&message, &meta)
        .into_iter()
        .filter(|who| {
            !receipts
                .get(who)
                .map(|receipt| ask_is_terminal(&receipt.status))
                .unwrap_or(false)
        })
        .collect();
    if released.is_empty() {
        bail_code!(
            "not_found",
            "message {:?} is not an open ask (nothing to withdraw)",
            message.id
        );
    }
    let at = iso_now();
    message.withdrawn = Some(Withdrawal {
        by: asker.clone(),
        at: at.clone(),
        reason: args.reason.clone(),
    });
    sign_message(root, &asker, &mut message)?;
    atomic_write_json(&path, &message)?;
    // Drop a discoverable lifecycle notice for the released workers, mirroring
    // the `participant removed`/`channel left`/`state changed` notices. Without
    // it the asymmetry is stark: the sender gets `released[]` back, but a worker
    // who already acked `working` only sees the item silently vanish from
    // `you_owe`, with no way to tell withdrawn from done-by-someone-else from a
    // bug. The notice names the ask and carries the reason so the worker can
    // correlate and stop in-flight work. Like the other notices it is `system`
    // kind, so it surfaces through `inbox`/`show`/`thread` rather than as a new
    // unread item or open ask.
    let reason_clause = match &args.reason {
        Some(reason) if !reason.trim().is_empty() => format!(" reason: {reason}"),
        _ => String::new(),
    };
    let notice = format!(
        "@{asker} withdrew their ask {:?} (message {}).{reason_clause}",
        message.subject, message.id
    );
    write_system_message(
        &conv,
        &message.conversation_id,
        released.clone(),
        notice,
        "ask withdrawn",
    )?;
    if json {
        emit_ok(serde_json::json!({
            "message_id": message.id,
            "withdrawn": true,
            "already_withdrawn": false,
            "released": released,
            "at": at,
        }))?;
    } else {
        let names = released
            .iter()
            .map(|id| format!("@{id}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("withdrew ask {}; released: {names}", message.id);
    }
    Ok(())
}

pub(crate) fn send_message(root: &Path, input: SendMessageInput) -> Result<Message> {
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", input.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    send_message_locked(root, input)
}

/// Send a message assuming the caller already holds the conversation lock. This
/// lets `reply --ack` perform its reply-send and its ack-receipt under a single
/// lock: a lock-acquisition failure aborts before anything is written (no
/// half-sent reply), and no other writer can interleave between the two writes.
pub(crate) fn send_message_locked(root: &Path, input: SendMessageInput) -> Result<Message> {
    let conversation_id = input.conversation_id;
    let sender = validate_id(&input.sender, "sender")?;
    let mut recipients = unique(split_recipients(&input.to)?);
    if recipients.is_empty() {
        bail_code!("parse", "--to needs at least one recipient");
    }
    for recipient in &recipients {
        if recipient != "*" {
            validate_id(recipient, "recipient")?;
        }
    }
    let (conv, meta) = load_conversation(root, &conversation_id)?;
    ensure_participant(&meta, &sender)?;
    let mentions = mentioned_participants(&meta, &input.subject, &input.body);
    recipients.extend(mentions.iter().cloned());
    recipients = unique(recipients);
    ensure_recipients(&meta, &recipients)?;
    let needs_response_from = unique(split_recipients(&input.needs_response_from)?);
    for awaited in &needs_response_from {
        ensure_participant(&meta, awaited)?;
        if !recipients
            .iter()
            .any(|recipient| recipient == awaited || recipient == "*")
        {
            recipients.push(awaited.clone());
        }
    }
    recipients = unique(recipients);
    ensure_claimed_recipients(root, &meta, &recipients)?;
    let kind = normalize_send_kind(&input.kind)?;
    if kind == "summary" && input.body.len() > MAX_SUMMARY_BYTES {
        return Err(RaftError::coded(
            "too_large",
            format!(
                "summary is {} bytes; limit is {}",
                input.body.len(),
                MAX_SUMMARY_BYTES
            ),
        )
        .with_details(serde_json::json!({
            "size": input.body.len(),
            "limit": MAX_SUMMARY_BYTES,
        })));
    }
    // Obligation flags belong only to obligation-bearing kinds (`message`,
    // `task`). An `event` (e.g. an IM bridge relaying a human) is not asking a
    // peer for a reply, and a bridge agent rarely runs `ack`, so honoring these
    // on a non-obligation kind would fabricate a permanently-open ask that
    // misreports who owes/awaits work. Reject loudly rather than silently strip,
    // so a misusing caller learns its mistake.
    if !is_obligation_kind(&kind) && (input.requires_ack || !needs_response_from.is_empty()) {
        bail!(
            "--requires-ack and --needs-response-from are only valid on kind \"message\" or \"task\", not {kind:?}"
        );
    }
    let subject_id = input
        .subject_id
        .as_deref()
        .map(validate_subject_id)
        .transpose()?;
    let after = input
        .after
        .as_deref()
        .map(|value| validate_id(value, "after message id"))
        .transpose()?;
    enforce_rate_limit(&conv, &meta, &sender, subject_id.as_deref(), &input.body)?;

    let message_id = new_message_id();
    let mut message = Message {
        v: SCHEMA_VERSION,
        id: message_id.clone(),
        conversation_id: meta.id.clone(),
        kind,
        from: sender.clone(),
        to: recipients,
        mentions,
        subject: input.subject,
        body: input.body,
        created_at: iso_now(),
        requires_ack: input.requires_ack,
        needs_response_from,
        subject_id,
        after,
        hash: None,
        signer_key: None,
        sig: None,
        withdrawn: None,
    };
    sign_message(root, &sender, &mut message)?;
    atomic_write_json(
        &conv.join("messages").join(format!("{message_id}.json")),
        &message,
    )?;
    Ok(message)
}

fn ask_is_terminal(status: &str) -> bool {
    TERMINAL_ACK_STATUSES.contains(&status)
}

/// Why a specific agent is on a message's awaited set. A `--needs-response-from`
/// name owes a substantive reply; a bare `--requires-ack` recipient owes only an
/// acknowledgement. The two are independent flags that may both be set on one
/// message (see `message_awaited`), so the label is per-agent, not per-message:
/// `needs_response` wins for an agent named in both, since a reply is the
/// stronger obligation and subsumes an ack.
fn await_kind_for(message: &Message, agent: &str) -> &'static str {
    if message.needs_response_from.iter().any(|who| who == agent) {
        "needs_response"
    } else {
        "requires_ack"
    }
}

/// True when `message` was created before `agent_id` joined `meta`'s room, so
/// the agent is owed no obligation for activity predating its membership. A
/// member with no recorded `joined_at` (legacy rooms, or founders of rooms
/// created before this field existed) is treated as present from the start.
/// Unparseable timestamps fall back to false — never hide a message we cannot
/// place in time.
fn predates_membership(meta: &Meta, message: &Message, agent_id: &str) -> bool {
    let Some(joined) = meta.joined_at.get(agent_id) else {
        return false;
    };
    matches!(
        (parse_time(&message.created_at), parse_time(joined)),
        (Ok(created), Ok(joined)) if created < joined
    )
}

fn message_awaited(message: &Message, meta: &Meta) -> Vec<String> {
    // Only obligation-bearing kinds (`message`, `task`) carry obligation
    // semantics (protocol.md: "Only this kind may use --needs-response-from").
    // `event`/`receipt`/`system` never open an ask, so neutralize them here — the
    // single chokepoint every ask-accounting path (gather_open_asks, build_view,
    // cmd_read, watch, gc archive) flows through — which also disarms any legacy
    // non-obligation rows already on disk written with these flags before
    // send-time rejected them. A `task` is a delegated unit of work whose
    // receipt lifecycle (working → done/rejected) is exactly an ask's lifecycle.
    if !is_obligation_kind(&message.kind) || message.withdrawn.is_some() {
        return Vec::new();
    }
    // The two obligation sources are independent and may both be set: every
    // recipient owes an ack when `requires_ack`, and each `needs_response_from`
    // name additionally owes a substantive reply. Union them so a "@b please
    // reply, everyone ack" message strands neither requirement (a non-empty
    // `needs_response_from` previously suppressed `requires_ack` entirely).
    let mut awaited: BTreeSet<String> = message.needs_response_from.iter().cloned().collect();
    if message.requires_ack {
        awaited.extend(receipt_recipients(message, meta));
    }
    awaited
        .into_iter()
        .filter(|agent| {
            agent != &message.from
                && agent != "*"
                && !predates_membership(meta, message, agent)
                // An agent removed from the room (`conversation remove` /
                // `channel leave`) can no longer ack or reply — `write_receipt`
                // and `send` reject non-participants. Keeping them awaited left
                // the ask permanently open: the asker's `owed_to_you` showed a
                // false `awaited_live`, `wait --owed` blocked forever, and the
                // removed agent (skipped by `gather_open_asks`) couldn't even
                // see it. Drop awaited agents who are no longer participants so
                // the obligation resolves; the removal already posts a
                // `participant removed` system notice to the room.
                && meta.participants.iter().any(|participant| participant == agent)
        })
        .collect()
}

/// Ids of agents whose heartbeat has not yet expired, scanned once so callers
/// can join liveness onto other records without re-reading each agent file.
pub(crate) fn live_agent_ids(root: &Path) -> Result<BTreeSet<String>> {
    let now = Utc::now();
    let mut live = BTreeSet::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if !is_agent_record_file(&entry.path()) {
            continue;
        }
        let Some(agent): Option<Agent> = read_json(&entry.path())? else {
            continue;
        };
        if parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= now)
            .unwrap_or(false)
        {
            live.insert(agent.id);
        }
    }
    Ok(live)
}

/// Resolved recipients of a just-sent message whose heartbeat has expired. A
/// `*` recipient expands to the conversation's participants so a broadcast
/// reports each offline member; the sender is never counted. Lets a sender that
/// just delegated work learn immediately that a peer is down — before it blocks
/// on `wait` for a reply that will never come.
fn offline_recipients(root: &Path, message: &Message) -> Result<Vec<String>> {
    let live = live_agent_ids(root)?;
    let (_, meta) = load_conversation(root, &message.conversation_id)?;
    let mut targets = Vec::new();
    for recipient in &message.to {
        if recipient == "*" {
            targets.extend(meta.participants.iter().cloned());
        } else {
            targets.push(recipient.clone());
        }
    }
    Ok(unique(targets)
        .into_iter()
        .filter(|target| target != &message.from && !live.contains(target))
        .collect())
}

pub(crate) fn gather_open_asks(
    root: &Path,
    only_conversation: Option<&str>,
    participant: Option<&str>,
) -> Result<Vec<OpenAsk>> {
    let live = live_agent_ids(root)?;
    let mut asks = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if let Some(id) = only_conversation
            && meta.id != id
        {
            continue;
        }
        if let Some(agent) = participant
            && !meta.participants.iter().any(|item| item == agent)
        {
            continue;
        }
        for message_entry in sorted_read_dir(&conv.join("messages"))? {
            if message_entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            let Some(message): Option<Message> = read_json(&message_entry.path())? else {
                continue;
            };
            if message.kind == "system" || message.kind == "receipt" {
                continue;
            }
            let awaited = message_awaited(&message, &meta);
            if awaited.is_empty() {
                continue;
            }
            let receipts = load_message_receipts(root, &message)?;
            for who in awaited {
                let status = receipts.get(&who).map(|receipt| receipt.status.clone());
                if status.as_deref().map(ask_is_terminal).unwrap_or(false) {
                    continue;
                }
                let awaited_live = live.contains(&who);
                let await_kind = await_kind_for(&message, &who);
                asks.push(OpenAsk {
                    conversation_id: meta.id.clone(),
                    message_id: message.id.clone(),
                    from: message.from.clone(),
                    awaited: who,
                    subject: message.subject.clone(),
                    created_at: message.created_at.clone(),
                    status: status.unwrap_or_else(|| "none".to_string()),
                    awaited_live,
                    await_kind,
                });
            }
        }
    }
    asks.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
    Ok(asks)
}

fn cmd_awaiting(root: &Path, args: AwaitingArgs) -> Result<()> {
    ensure_root(root)?;
    let agent = validate_id(&args.agent, "agent id")?;
    let only = optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let asks = gather_open_asks(root, only.as_deref(), Some(&agent))?;
    let incoming: Vec<&OpenAsk> = asks.iter().filter(|ask| ask.awaited == agent).collect();
    let outgoing: Vec<&OpenAsk> = asks
        .iter()
        .filter(|ask| ask.from == agent && ask.awaited != agent)
        .collect();
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": agent,
                "you_owe": incoming,
                "owed_to_you": outgoing
            }))?
        );
        return Ok(());
    }
    println!("awaiting for @{agent}");
    println!("you owe a response to:");
    if incoming.is_empty() {
        println!("  nothing");
    }
    for ask in &incoming {
        let wants = if ask.await_kind == "needs_response" {
            "reply"
        } else {
            "ack"
        };
        println!(
            "  {} in {} from @{} [{}, wants {}]: {}",
            ask.message_id, ask.conversation_id, ask.from, ask.status, wants, ask.subject
        );
    }
    println!("waiting on a response from others:");
    if outgoing.is_empty() {
        println!("  nothing");
    }
    for ask in &outgoing {
        let presence = if ask.awaited_live { "" } else { " (offline)" };
        println!(
            "  {} in {} -> @{}{} [{}]: {}",
            ask.message_id, ask.conversation_id, ask.awaited, presence, ask.status, ask.subject
        );
    }
    Ok(())
}

fn cmd_me(root: &Path, args: MeArgs) -> Result<()> {
    ensure_root(root)?;
    let agent = validate_id(&args.agent, "agent id")?;
    // Read the agent's own record (not just `.exists()`): liveness in raft is
    // computed only for *peers* (offline_recipients, roster `active`, this
    // command's `live_peers`), so a stale agent orienting via `me` would
    // otherwise see nothing wrong while every peer's `wait --owed` deadlocks on
    // a reply it doesn't know it looks too dead to be expected to send.
    let Some(me): Option<Agent> = read_json(&agent_path(root, &agent))? else {
        return Err(not_claimed(root, &agent));
    };

    // Unread totals and per-conversation counts, from a single visibility scan.
    let messages = visible_messages(root, &agent, None)?;
    let mut unread = 0usize;
    let mut per_conversation: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for message in &messages {
        let counts = per_conversation
            .entry(message.conversation_id.clone())
            .or_default();
        counts.0 += 1;
        if message_is_unread(root, message, &agent) {
            counts.1 += 1;
            unread += 1;
        }
    }

    // Open asks split into the two directions, reusing the awaiting logic.
    let asks = gather_open_asks(root, None, Some(&agent))?;
    let you_owe: Vec<&OpenAsk> = asks.iter().filter(|ask| ask.awaited == agent).collect();
    let owed_to_you: Vec<&OpenAsk> = asks
        .iter()
        .filter(|ask| ask.from == agent && ask.awaited != agent)
        .collect();

    // Live peers (other agents whose heartbeat has not expired).
    let now = Utc::now();
    let self_live = parse_time(&me.expires_at)
        .map(|expires_at| expires_at >= now)
        .unwrap_or(false);
    let mut live_peers = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if !is_agent_record_file(&entry.path()) {
            continue;
        }
        let Some(peer): Option<Agent> = read_json(&entry.path())? else {
            continue;
        };
        if peer.id == agent {
            continue;
        }
        let live = parse_time(&peer.expires_at)
            .map(|expires_at| expires_at >= now)
            .unwrap_or(false);
        if !live {
            continue;
        }
        live_peers.push(serde_json::json!({
            "id": peer.id,
            "current_state": peer.current_state,
            "state_note": peer.state_note,
        }));
    }

    // Conversations the agent participates in, annotated with unread/message counts.
    let mut conversations = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta.participants.iter().any(|item| item == &agent) {
            continue;
        }
        let (total, unread_here) = per_conversation.get(&meta.id).copied().unwrap_or((0, 0));
        conversations.push(serde_json::json!({
            "id": meta.id,
            "channel": meta.channel,
            "private": meta.private,
            "messages": total,
            "unread": unread_here,
        }));
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": agent,
                "live": self_live,
                "expires_at": me.expires_at,
                "unread": unread,
                "you_owe": you_owe,
                "owed_to_you": owed_to_you,
                "live_peers": live_peers,
                "conversations": conversations,
            }))?
        );
        return Ok(());
    }

    if self_live {
        println!("@{agent} — {unread} unread");
    } else {
        println!(
            "@{agent} — STALE: heartbeat expired {}; run 'raft heartbeat {agent}' — {unread} unread",
            me.expires_at
        );
    }
    println!("you owe ({}):", you_owe.len());
    if you_owe.is_empty() {
        println!("  nothing");
    }
    for ask in &you_owe {
        println!(
            "  {} in {} from @{} [{}]: {}",
            ask.message_id, ask.conversation_id, ask.from, ask.status, ask.subject
        );
    }
    println!("owed to you ({}):", owed_to_you.len());
    if owed_to_you.is_empty() {
        println!("  nothing");
    }
    for ask in &owed_to_you {
        let presence = if ask.awaited_live { "" } else { " (offline)" };
        println!(
            "  {} in {} -> @{}{} [{}]: {}",
            ask.message_id, ask.conversation_id, ask.awaited, presence, ask.status, ask.subject
        );
    }
    println!("live peers ({}):", live_peers.len());
    if live_peers.is_empty() {
        println!("  none");
    }
    for peer in &live_peers {
        let note = peer["state_note"].as_str().unwrap_or("");
        let note_suffix = if note.is_empty() {
            String::new()
        } else {
            format!(" — {note}")
        };
        println!(
            "  @{} [{}]{}",
            peer["id"].as_str().unwrap_or("unknown"),
            peer["current_state"].as_str().unwrap_or("idle"),
            note_suffix
        );
    }
    println!("conversations ({}):", conversations.len());
    if conversations.is_empty() {
        println!("  none");
    }
    for conv in &conversations {
        let kind = if conv["channel"].as_bool().unwrap_or(false) {
            "channel"
        } else if conv["private"].as_bool().unwrap_or(false) {
            "private"
        } else {
            "group"
        };
        println!(
            "  {} [{}] {} unread / {} total",
            conv["id"].as_str().unwrap_or("unknown"),
            kind,
            conv["unread"].as_u64().unwrap_or(0),
            conv["messages"].as_u64().unwrap_or(0)
        );
    }
    Ok(())
}

#[derive(Serialize, Clone)]
struct SwarmCandidate {
    id: String,
    mention: String,
    active: bool,
    current_state: String,
    state_note: Option<String>,
    capabilities: Vec<String>,
    matching_capabilities: Vec<String>,
    missing_capabilities: Vec<String>,
    owes: usize,
    waiting_on: usize,
    score: i64,
    reasons: Vec<String>,
}

fn parse_repeated_csv(values: &[String], label: &str) -> Result<Vec<String>> {
    let mut parsed = Vec::new();
    for value in values {
        for item in value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            parsed.push(validate_id(item.trim_start_matches('@'), label)?);
        }
    }
    Ok(unique(parsed))
}

fn swarm_state_score(state: &str) -> i64 {
    match state {
        "idle" => 30,
        "working" => 5,
        "away" => -20,
        "blocked" => -50,
        _ => -10,
    }
}

fn rank_swarm_candidates(
    root: &Path,
    required_capabilities: &[String],
    excluded: &BTreeSet<String>,
    allow_partial: bool,
    include_stale: bool,
    only_participants: Option<&BTreeSet<String>>,
    limit: usize,
) -> Result<Vec<SwarmCandidate>> {
    let asks = gather_open_asks(root, None, None)?;
    let mut owes: BTreeMap<String, usize> = BTreeMap::new();
    let mut waiting_on: BTreeMap<String, usize> = BTreeMap::new();
    for ask in &asks {
        *owes.entry(ask.awaited.clone()).or_default() += 1;
        *waiting_on.entry(ask.from.clone()).or_default() += 1;
    }

    let now = Utc::now();
    let mut candidates = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if !is_agent_record_file(&entry.path()) {
            continue;
        }
        let Some(agent): Option<Agent> = read_json(&entry.path())? else {
            continue;
        };
        if excluded.contains(&agent.id) {
            continue;
        }
        if let Some(participants) = only_participants
            && !participants.contains(&agent.id)
        {
            continue;
        }
        let active = parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= now)
            .unwrap_or(false);
        if !active && !include_stale {
            continue;
        }

        let capability_set: BTreeSet<&str> =
            agent.capabilities.iter().map(String::as_str).collect();
        let matching_capabilities: Vec<String> = required_capabilities
            .iter()
            .filter(|capability| capability_set.contains(capability.as_str()))
            .cloned()
            .collect();
        let missing_capabilities: Vec<String> = required_capabilities
            .iter()
            .filter(|capability| !capability_set.contains(capability.as_str()))
            .cloned()
            .collect();
        if !allow_partial && !missing_capabilities.is_empty() {
            continue;
        }
        if allow_partial && !required_capabilities.is_empty() && matching_capabilities.is_empty() {
            continue;
        }

        let owes_count = owes.get(&agent.id).copied().unwrap_or(0);
        let waiting_count = waiting_on.get(&agent.id).copied().unwrap_or(0);
        let mut score = 0i64;
        score += if active { 100 } else { -100 };
        score += (matching_capabilities.len() as i64) * 50;
        score -= (missing_capabilities.len() as i64) * 40;
        score += swarm_state_score(&agent.current_state);
        score -= (owes_count as i64) * 10;
        score -= (waiting_count as i64) * 3;

        let mut reasons = Vec::new();
        reasons.push(if active { "live" } else { "stale" }.to_string());
        reasons.push(format!("state={}", agent.current_state));
        if required_capabilities.is_empty() {
            reasons.push("no capability filter".to_string());
        } else {
            reasons.push(format!("matched={}", matching_capabilities.join(",")));
            if !missing_capabilities.is_empty() {
                reasons.push(format!("missing={}", missing_capabilities.join(",")));
            }
        }
        if owes_count > 0 || waiting_count > 0 {
            reasons.push(format!("load=owes:{owes_count},waiting:{waiting_count}"));
        }

        let mention = if agent.mention.is_empty() {
            format!("@{}", agent.id)
        } else {
            agent.mention.clone()
        };
        candidates.push(SwarmCandidate {
            id: agent.id,
            mention,
            active,
            current_state: agent.current_state,
            state_note: agent.state_note,
            capabilities: agent.capabilities,
            matching_capabilities,
            missing_capabilities,
            owes: owes_count,
            waiting_on: waiting_count,
            score,
            reasons,
        });
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.owes.cmp(&right.owes))
            .then_with(|| left.waiting_on.cmp(&right.waiting_on))
            .then_with(|| left.id.cmp(&right.id))
    });
    if candidates.len() > limit {
        candidates.truncate(limit);
    }
    Ok(candidates)
}

fn cmd_swarm_candidates(root: &Path, args: SwarmCandidatesArgs) -> Result<()> {
    ensure_root(root)?;
    if args.limit == 0 {
        bail_code!("parse", "swarm candidates --limit must be at least 1");
    }
    let required = parse_repeated_csv(&args.capabilities, "capability")?;
    let excluded: BTreeSet<String> = parse_repeated_csv(&args.exclude, "agent id")?
        .into_iter()
        .collect();
    let candidates = rank_swarm_candidates(
        root,
        &required,
        &excluded,
        args.allow_partial,
        args.all,
        None,
        args.limit,
    )?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "root": root,
                "required_capabilities": required,
                "allow_partial": args.allow_partial,
                "include_stale": args.all,
                "candidates": candidates,
            }))?
        );
        return Ok(());
    }
    println!("swarm candidates ({} shown):", candidates.len());
    if candidates.is_empty() {
        println!("  none");
    }
    for candidate in candidates {
        let caps = if candidate.capabilities.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", candidate.capabilities.join(","))
        };
        println!(
            "  {} score={} state={} owes={} waiting={}{}",
            candidate.id,
            candidate.score,
            candidate.current_state,
            candidate.owes,
            candidate.waiting_on,
            caps
        );
    }
    Ok(())
}

fn cmd_swarm_assign(root: &Path, args: SwarmAssignArgs) -> Result<()> {
    ensure_root(root)?;
    let sender = validate_id(&args.sender, "sender")?;
    let channel_id = validate_id(&args.channel, "channel id")?;
    if args.count == 0 {
        bail_code!("parse", "swarm assign --count must be at least 1");
    }
    let required = parse_repeated_csv(&args.capabilities, "capability")?;
    if required.is_empty() {
        bail_code!("parse", "swarm assign requires at least one --capability");
    }
    let mut excluded = parse_repeated_csv(&args.exclude, "agent id")?;
    excluded.push(sender.clone());
    let excluded: BTreeSet<String> = unique(excluded).into_iter().collect();
    let (_, meta) = load_conversation(root, &channel_id)?;
    if !meta.channel {
        bail_code!("not_found", "{channel_id:?} is not a channel");
    }
    ensure_participant(&meta, &sender)?;
    let participants: BTreeSet<String> = meta.participants.iter().cloned().collect();
    let candidates = rank_swarm_candidates(
        root,
        &required,
        &excluded,
        false,
        false,
        Some(&participants),
        args.count,
    )?;
    if candidates.len() < args.count {
        return Err(RaftError::coded(
            "not_found",
            format!(
                "requested {} swarm assignee(s), but only {} live channel member(s) matched",
                args.count,
                candidates.len()
            ),
        )
        .with_details(serde_json::json!({
            "requested": args.count,
            "available": candidates.len(),
            "required_capabilities": required.clone(),
        })));
    }
    let selected_agents: Vec<String> = candidates
        .iter()
        .map(|candidate| candidate.id.clone())
        .collect();
    if args.dry_run {
        if args.json {
            emit_ok(serde_json::json!({
                "dry_run": true,
                "channel": channel_id,
                "selected_agents": selected_agents,
                "required_capabilities": required,
                "candidates": candidates,
            }))?;
        } else {
            println!("would assign to: {}", selected_agents.join(","));
        }
        return Ok(());
    }

    let selected_csv = selected_agents.join(",");
    let message = send_message(
        root,
        SendMessageInput {
            conversation_id: channel_id.clone(),
            sender,
            to: selected_csv.clone(),
            subject: args.subject,
            body: args.body,
            kind: "message".to_string(),
            after: None,
            subject_id: None,
            requires_ack: false,
            needs_response_from: selected_csv,
        },
    )?;
    let offline = offline_recipients(root, &message)?;
    if args.json {
        emit_ok(serde_json::json!({
            "channel": channel_id,
            "message_id": message.id,
            "selected_agents": selected_agents,
            "needs_response_from": message.needs_response_from,
            "offline_recipients": offline,
            "required_capabilities": required,
            "candidates": candidates,
        }))?;
    } else {
        println!("{} -> {}", message.id, selected_agents.join(","));
        warn_offline_recipients(&offline);
    }
    Ok(())
}

fn cmd_swarm_dispatch(root: &Path, args: SwarmDispatchArgs) -> Result<()> {
    ensure_root(root)?;
    let sender = validate_id(&args.sender, "sender")?;
    let channel_id = validate_id(&args.channel, "channel id")?;
    let required = parse_repeated_csv(&args.capabilities, "capability")?;
    if required.is_empty() {
        bail_code!("parse", "swarm dispatch requires at least one --capability");
    }
    let mut excluded = parse_repeated_csv(&args.exclude, "agent id")?;
    excluded.push(sender.clone());
    let excluded: BTreeSet<String> = unique(excluded).into_iter().collect();

    let (_, meta) = load_conversation(root, &channel_id)?;
    if !meta.channel {
        bail_code!("not_found", "{channel_id:?} is not a channel");
    }
    ensure_participant(&meta, &sender)?;
    let participants: BTreeSet<String> = meta.participants.iter().cloned().collect();
    let mut candidates = rank_swarm_candidates(
        root,
        &required,
        &excluded,
        false,
        false,
        Some(&participants),
        1,
    )?;
    if candidates.is_empty() {
        return Err(RaftError::coded(
            "not_found",
            "no live channel member matched the requested swarm capabilities",
        )
        .with_details(serde_json::json!({
            "available": candidates.len(),
            "required_capabilities": required.clone(),
        })));
    }
    let candidate = candidates.pop().expect("checked non-empty candidates");
    let worker = candidate.id.clone();
    let body = build_task_body(
        &args.tool,
        &args.args,
        args.cap.as_deref(),
        args.max_runtime_s,
        args.max_output_bytes,
    )?;
    if let Some(token) = &body.capability {
        let selected_pubkey = resolve_pubkey(root, &worker)?;
        let holder = token
            .blocks
            .last()
            .map(|block| block.holder.as_str())
            .ok_or_else(|| RaftError::coded("error", "capability token has no blocks"))?;
        if holder != selected_pubkey {
            bail_code!(
                "not_authorized",
                "capability token holder does not match the selected swarm worker"
            );
        }
    }
    let message = send_task_message(
        root,
        channel_id.clone(),
        sender,
        worker.clone(),
        &args.tool,
        body,
    )?;
    if args.json {
        emit_ok(serde_json::json!({
            "channel": channel_id,
            "selected_agent": worker,
            "required_capabilities": required,
            "candidate": candidate,
            "task": {
                "task_id": message.id,
                "conversation_id": message.conversation_id,
                "to": worker,
                "tool": args.tool,
            },
        }))?;
    } else {
        println!(
            "dispatched swarm task {} to @{worker} (tool {})",
            message.id, args.tool
        );
    }
    Ok(())
}

fn state_priority(state: &str) -> u8 {
    match state {
        "blocked" => 0,
        "working" => 1,
        "idle" => 2,
        "away" => 3,
        _ => 4,
    }
}

fn cmd_roster(root: &Path, args: RosterArgs) -> Result<()> {
    ensure_root(root)?;
    let asks = gather_open_asks(root, None, None)?;
    let mut owes: BTreeMap<String, usize> = BTreeMap::new();
    let mut waiting_on: BTreeMap<String, usize> = BTreeMap::new();
    for ask in &asks {
        *owes.entry(ask.awaited.clone()).or_default() += 1;
        *waiting_on.entry(ask.from.clone()).or_default() += 1;
    }
    let mut entries = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if !is_agent_record_file(&entry.path()) {
            continue;
        }
        let Some(agent): Option<Agent> = read_json(&entry.path())? else {
            continue;
        };
        let active = parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= Utc::now())
            .unwrap_or(false);
        if !active && !args.all {
            continue;
        }
        if let Some(capability) = &args.capability
            && !agent.capabilities.iter().any(|tag| tag == capability)
        {
            continue;
        }
        let mention = if agent.mention.is_empty() {
            format!("@{}", agent.id)
        } else {
            agent.mention.clone()
        };
        entries.push(serde_json::json!({
            "id": agent.id,
            "mention": mention,
            "active": active,
            "current_state": agent.current_state,
            "state_note": agent.state_note,
            "capabilities": agent.capabilities,
            "last_seen_at": agent.last_seen_at,
            "expires_at": agent.expires_at,
            "owes": owes.get(&agent.id).copied().unwrap_or(0),
            "waiting_on": waiting_on.get(&agent.id).copied().unwrap_or(0)
        }));
    }
    entries.sort_by(|left, right| {
        let lp = state_priority(left["current_state"].as_str().unwrap_or(""));
        let rp = state_priority(right["current_state"].as_str().unwrap_or(""));
        lp.cmp(&rp).then_with(|| {
            left["id"]
                .as_str()
                .unwrap_or("")
                .cmp(right["id"].as_str().unwrap_or(""))
        })
    });
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "root": root,
                "agents": entries
            }))?
        );
        return Ok(());
    }
    println!("roster ({} shown):", entries.len());
    if entries.is_empty() {
        println!("  none");
    }
    for entry in &entries {
        let liveness = if entry["active"].as_bool().unwrap_or(false) {
            "live"
        } else {
            "stale"
        };
        let note = entry["state_note"].as_str().unwrap_or("");
        let note_suffix = if note.is_empty() {
            String::new()
        } else {
            format!(" — {note}")
        };
        let caps: Vec<&str> = entry["capabilities"]
            .as_array()
            .map(|tags| tags.iter().filter_map(|tag| tag.as_str()).collect())
            .unwrap_or_default();
        let caps_suffix = if caps.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", caps.join(","))
        };
        println!(
            "  {} [{}/{}] owes={} waiting={}{}{}",
            entry["id"].as_str().unwrap_or("unknown"),
            liveness,
            entry["current_state"].as_str().unwrap_or("idle"),
            entry["owes"].as_u64().unwrap_or(0),
            entry["waiting_on"].as_u64().unwrap_or(0),
            caps_suffix,
            note_suffix
        );
    }
    Ok(())
}

fn cmd_inbox(root: &Path, args: InboxArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    ensure_root(root)?;
    let rows = visible_messages(root, &agent_id, conversation_id.as_deref())?;
    let mut views = build_views(root, rows, &agent_id)?;
    // `visible_messages` concatenates each conversation's messages in
    // conversation-id order, so the merged list is sorted only *within* a room.
    // Sort globally by message id (ids are time-ordered) before `--limit` keeps
    // the tail, or `inbox --limit` would drop the newest messages overall and
    // retain stale ones from a later-sorting room — `show`/`search`/`thread`
    // all sort first for exactly this reason.
    views.sort_by(|left, right| left.message.id.cmp(&right.message.id));
    if args.unread {
        views.retain(|view| view.unread);
    }
    if args.needs_action {
        views.retain(|view| view.unread || view.awaiting_me);
    }
    if views.len() > args.limit {
        views = views.split_off(views.len() - args.limit);
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&views)?);
        return Ok(());
    }
    if views.is_empty() {
        println!("no messages");
        return Ok(());
    }
    for view in views {
        let unread = if view.unread { "*" } else { " " };
        let message = &view.message;
        let body = truncated_body(&message.body, args.width);
        println!(
            "{unread} {} {} {} -> {} [{}] {} {}",
            message.id,
            message.conversation_id,
            message.from,
            message.to.join(","),
            message.kind,
            message.subject,
            body
        );
    }
    Ok(())
}

fn cmd_wait(root: &Path, args: WaitArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    // Fail fast if the waiting agent isn't claimed. Otherwise a typo'd or
    // unclaimed id blocks for the full `--timeout` — `visible_messages` and
    // `gather_open_asks` just return nothing for a non-participant — and then
    // exits 2 (`timeout`), masking what is really a `not_claimed` mistake. The
    // error carries nearest-id suggestions so the caller recovers immediately.
    if !agent_path(root, &agent_id).exists() {
        return Err(not_claimed(root, &agent_id));
    }
    if args.owed || args.resolved.is_some() {
        return cmd_wait_resolution(root, &agent_id, &args);
    }
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let deadline = Instant::now() + Duration::from_secs(args.timeout);
    let waker = Waker::new(&[&root.join("conversations")]);
    let interval = Duration::from_secs_f64(args.interval);
    loop {
        let rows = visible_messages(root, &agent_id, conversation_id.as_deref())?;
        if let Some(message) = rows
            .into_iter()
            .find(|message| message_is_unread(root, message, &agent_id))
        {
            if args.json {
                let view = build_view(root, message, &agent_id)?;
                println!("{}", serde_json::to_string_pretty(&view)?);
            } else {
                println!("{}", message.id);
            }
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail_code!(
                "timeout",
                "no unread message arrived within {}s",
                args.timeout
            );
        }
        waker.wait(interval.min(remaining));
    }
}

/// Read the terminal ack status (and note) for one awaited agent on a message,
/// or `None` if the awaited agent has not recorded a terminal `done`/`rejected`
/// receipt yet.
fn read_terminal_status(
    root: &Path,
    conversation_id: &str,
    message_id: &str,
    awaited: &str,
) -> Result<Option<(String, Option<String>)>> {
    let path = root
        .join("conversations")
        .join(conversation_id)
        .join("receipts")
        .join(message_id)
        .join(format!("{awaited}.json"));
    let Some(receipt): Option<Receipt> = read_json(&path)? else {
        return Ok(None);
    };
    if ask_is_terminal(&receipt.status) {
        Ok(Some((receipt.status, receipt.note)))
    } else {
        Ok(None)
    }
}

fn emit_resolution(args: &WaitArgs, resolved: Option<(OpenAsk, Option<String>)>) -> Result<()> {
    if args.json {
        let value = resolved.as_ref().map(|(ask, note)| {
            serde_json::json!({
                "message_id": ask.message_id,
                "conversation_id": ask.conversation_id,
                "awaited": ask.awaited,
                "awaited_live": ask.awaited_live,
                "status": ask.status,
                "note": note,
                "subject": ask.subject,
            })
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"ok": true, "resolved": value}))?
        );
    } else {
        match resolved {
            Some((ask, _)) => println!(
                "{} in {} -> @{} [{}]: {}",
                ask.message_id, ask.conversation_id, ask.awaited, ask.status, ask.subject
            ),
            None => println!("nothing owed"),
        }
    }
    Ok(())
}

/// Block until an ask the agent is owed reaches a terminal receipt. With
/// `--owed`, watch every ask the agent currently owns; with `--resolved <id>`,
/// watch one specific ask (and report it immediately if already resolved).
fn cmd_wait_resolution(root: &Path, agent_id: &str, args: &WaitArgs) -> Result<()> {
    ensure_root(root)?;
    let target = args
        .resolved
        .as_deref()
        .map(|id| validate_id(id, "message id"))
        .transpose()?;

    let mut pending: Vec<OpenAsk> = gather_open_asks(root, None, Some(agent_id))?
        .into_iter()
        .filter(|ask| ask.from == agent_id)
        .collect();
    if let Some(id) = target.as_deref() {
        pending.retain(|ask| ask.message_id == id);
        if pending.is_empty() {
            // Either the id is already resolved, or it was never the agent's ask.
            return resolved_ask_already_closed(root, agent_id, id, args);
        }
    } else if pending.is_empty() {
        // `--owed` with nothing open: there is nothing to block on.
        return emit_resolution(args, None);
    }

    let deadline = Instant::now() + Duration::from_secs(args.timeout);
    let waker = Waker::new(&[&root.join("conversations")]);
    let interval = Duration::from_secs_f64(args.interval);
    loop {
        if target.is_some() {
            // `--resolved <id>` targets one ask, which may owe several agents. It
            // is only resolved once every awaited agent has a terminal receipt;
            // returning on the first would falsely report the ask done while
            // others still owe a reply.
            let mut terminal: Vec<(OpenAsk, Option<String>)> = Vec::with_capacity(pending.len());
            for ask in &pending {
                match read_terminal_status(
                    root,
                    &ask.conversation_id,
                    &ask.message_id,
                    &ask.awaited,
                )? {
                    Some((status, note)) => {
                        let mut resolved = ask.clone();
                        resolved.status = status;
                        terminal.push((resolved, note));
                    }
                    None => break,
                }
            }
            if terminal.len() == pending.len() {
                // Aggregate across recipients: a single rejection rejects the
                // ask; otherwise report the last agent to finish.
                let chosen = terminal
                    .iter()
                    .find(|(ask, _)| ask.status == "rejected")
                    .cloned()
                    .or_else(|| terminal.last().cloned());
                return emit_resolution(args, chosen);
            }
        } else {
            // `--owed` blocks on the whole owed set; the first ask to reach a
            // terminal receipt unblocks the caller.
            for ask in &pending {
                if let Some((status, note)) =
                    read_terminal_status(root, &ask.conversation_id, &ask.message_id, &ask.awaited)?
                {
                    let mut resolved = ask.clone();
                    resolved.status = status;
                    return emit_resolution(args, Some((resolved, note)));
                }
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail_code!("timeout", "no owed ask resolved within {}s", args.timeout);
        }
        waker.wait(interval.min(remaining));
    }
}

/// `--resolved <id>` where the id is not in the open-ask set: report it if it is
/// the agent's already-closed ask, otherwise reject it as not the agent's ask.
fn resolved_ask_already_closed(
    root: &Path,
    agent_id: &str,
    id: &str,
    args: &WaitArgs,
) -> Result<()> {
    let (_, message) = find_message(root, id)?;
    let meta: Option<Meta> = read_json(
        &root
            .join("conversations")
            .join(&message.conversation_id)
            .join("meta.json"),
    )?;
    let awaited = meta
        .as_ref()
        .map(|meta| message_awaited(&message, meta))
        .unwrap_or_default();
    if message.from != agent_id || awaited.is_empty() {
        bail_code!("not_found", "message {id:?} is not an ask you are owed");
    }
    // Aggregate across every recipient with the SAME rule as the live blocking
    // loop (a single rejection rejects the whole ask, otherwise report the last
    // to finish). Returning on the first awaited agent here would pick one by
    // `awaited`'s alphabetical `BTreeSet` order — silently hiding a `rejected`
    // behind an earlier-sorting `done`, so the resolution an asker sees would
    // depend on timing (this already-closed path vs. the live loop).
    let live = live_agent_ids(root)?;
    let mut terminal: Vec<(OpenAsk, Option<String>)> = Vec::new();
    for who in &awaited {
        if let Some((status, note)) =
            read_terminal_status(root, &message.conversation_id, &message.id, who)?
        {
            terminal.push((
                OpenAsk {
                    conversation_id: message.conversation_id.clone(),
                    message_id: message.id.clone(),
                    from: message.from.clone(),
                    awaited: who.clone(),
                    subject: message.subject.clone(),
                    created_at: message.created_at.clone(),
                    status,
                    awaited_live: live.contains(who),
                    await_kind: await_kind_for(&message, who),
                },
                note,
            ));
        }
    }
    let chosen = terminal
        .iter()
        .find(|(ask, _)| ask.status == "rejected")
        .cloned()
        .or_else(|| terminal.last().cloned());
    match chosen {
        Some(resolved) => emit_resolution(args, Some(resolved)),
        None => bail_code!("not_found", "message {id:?} is not an ask you are owed"),
    }
}

fn cmd_watch(root: &Path, args: WatchArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let since = args
        .since
        .as_deref()
        .map(|id| validate_id(id, "message id"))
        .transpose()?;
    ensure_root(root)?;
    // An explicit `--since` is a hard floor: the caller has declared everything
    // up to that id handled, so suppress it for every message kind. The resume
    // cursor persisted by a prior run is only a soft floor (see below).
    let since_floor = since.clone();
    let (mut state, resume_floor) = start_watch_state(root, &agent_id, since)?;
    // Exact in-session dedup, independent of id ordering. Message ids are not
    // monotonic across processes within a millisecond, so a scalar high-water
    // cursor alone could skip a low-id message that only becomes visible after
    // the cursor advanced; the set guarantees we emit each message at most once
    // per run regardless of arrival order.
    let mut seen: HashSet<String> = HashSet::new();
    let deadline = if args.timeout == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(args.timeout))
    };
    let conversations_dir = root.join("conversations");
    let agents_dir = root.join("agents");
    let mut watched: Vec<&Path> = vec![&conversations_dir];
    if args.state_changes {
        watched.push(&agents_dir);
    }
    let waker = Waker::new(&watched);
    let interval = Duration::from_secs_f64(args.interval);

    loop {
        let mut rows = visible_messages(root, &agent_id, conversation_id.as_deref())?;
        rows.sort_by(|left, right| left.id.cmp(&right.id));
        let mut emitted = false;
        for message in rows {
            if seen.contains(&message.id) {
                continue;
            }
            // Explicit `--since`: a hard floor for every message kind.
            if let Some(floor) = since_floor.as_deref()
                && message.id.as_str() <= floor
            {
                continue;
            }
            let is_state_change = is_state_change_message(&message);
            let should_emit = message_is_unread(root, &message, &agent_id)
                || args.state_changes && is_state_change;
            if !should_emit {
                continue;
            }
            // The persisted resume cursor is only a *soft* floor. Applying it to
            // receipt-deduped normal messages would let a non-monotonic id
            // silently drop a still-unread message, so honor it only where no
            // read receipt provides dedup: state-change notices (never
            // receipted) and `--no-auto-read` (receipts deliberately skipped).
            // Under the default auto-read, `message_is_unread` already prevents
            // re-emitting anything previously read, so a low id is safe to emit.
            if let Some(floor) = resume_floor.as_deref()
                && message.id.as_str() <= floor
                && (is_state_change || args.no_auto_read)
            {
                continue;
            }
            emit_watch_message(root, &message, &agent_id, args.json)?;
            if !args.no_auto_read && !is_state_change {
                let _lock = DirLock::acquire(
                    root,
                    &format!("conversation-{}", message.conversation_id),
                    LOCK_TTL_SECONDS,
                    LOCK_TIMEOUT_SECONDS,
                )?;
                write_receipt(root, &agent_id, &message, "read", None)?;
            }
            seen.insert(message.id.clone());
            // Persist a high-water mark for cross-restart resume. It only ever
            // advances, and on the next run it returns as the soft `resume_floor`.
            if state
                .last_event_id
                .as_deref()
                .is_none_or(|high| message.id.as_str() > high)
            {
                state.last_event_id = Some(message.id.clone());
            }
            state.updated_at = iso_now();
            atomic_write_json(&watch_state_path(root, &agent_id), &state)?;
            emitted = true;
        }
        if args.once {
            state.shutdown_at = Some(iso_now());
            state.updated_at = iso_now();
            atomic_write_json(&watch_state_path(root, &agent_id), &state)?;
            return Ok(());
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            state.shutdown_at = Some(iso_now());
            state.updated_at = iso_now();
            atomic_write_json(&watch_state_path(root, &agent_id), &state)?;
            return Ok(());
        }
        if !emitted {
            let wait = match deadline {
                Some(deadline) => interval.min(deadline.saturating_duration_since(Instant::now())),
                None => interval,
            };
            waker.wait(wait);
        }
    }
}

fn cmd_show(root: &Path, args: ShowArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id = target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    ensure_root(root)?;
    let mut rows = visible_messages(root, &agent_id, Some(&conversation_id))?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    if rows.len() > args.limit {
        rows = rows.split_off(rows.len() - args.limit);
    }
    if args.json {
        let views = build_views(root, rows, &agent_id)?;
        println!("{}", serde_json::to_string_pretty(&views)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("no visible messages");
        return Ok(());
    }
    for (index, message) in rows.iter().enumerate() {
        if index > 0 {
            println!();
        }
        println!(
            "{} {} {} -> {} [{}]",
            message.id,
            message.created_at,
            message.from,
            message.to.join(","),
            message.kind
        );
        if !message.subject.is_empty() {
            println!("Subject: {}", message.subject);
        }
        if !message.mentions.is_empty() {
            println!("Mentions: {}", message.mentions.join(","));
        }
        if let Some(after) = message.after.as_deref() {
            println!("After: {after}");
        }
        println!("{}", message.body);
    }
    Ok(())
}

fn cmd_search(root: &Path, args: SearchArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let pattern = match args.pattern.as_deref().map(str::trim) {
        Some("") => bail!("search pattern cannot be empty"),
        Some(pattern) => Some(pattern.to_lowercase()),
        None => None,
    };
    let from = args
        .from
        .as_deref()
        .map(|v| validate_id(v, "from"))
        .transpose()?;
    let kind = args.kind.as_deref().map(str::to_string);
    let mentions = args
        .mentions
        .as_deref()
        .map(|v| validate_id(v, "mentions"))
        .transpose()?;
    if pattern.is_none() && from.is_none() && kind.is_none() && mentions.is_none() {
        bail!("search needs a pattern or at least one of --from/--kind/--mentions");
    }
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let cutoff = args.since.as_deref().map(parse_since_cutoff).transpose()?;
    ensure_root(root)?;
    let candidates = visible_messages(root, &agent_id, conversation_id.as_deref())?;
    // A `*` broadcast is addressed to every participant, so `--mentions <id>`
    // should surface it for any member of that room — matching how
    // `message_visible_to` treats `*` as reaching all participants. Without
    // this, an agent searching `--mentions me` silently missed every broadcast
    // it had actually received. Resolve membership per conversation once.
    let mention_wildcard_convs: BTreeSet<String> = match mentions.as_deref() {
        Some(who) => {
            let mut convs = BTreeSet::new();
            let mut seen = BTreeSet::new();
            for message in &candidates {
                if message.to.iter().any(|recipient| recipient == "*")
                    && seen.insert(message.conversation_id.clone())
                {
                    let meta: Option<Meta> = read_json(
                        &conversation_path(root, &message.conversation_id)?.join("meta.json"),
                    )?;
                    if meta
                        .map(|meta| meta.participants.iter().any(|item| item == who))
                        .unwrap_or(false)
                    {
                        convs.insert(message.conversation_id.clone());
                    }
                }
            }
            convs
        }
        None => BTreeSet::new(),
    };
    let mut rows = candidates
        .into_iter()
        .filter(|message| {
            cutoff
                .map(|cutoff| {
                    parse_time(&message.created_at)
                        .map(|created_at| created_at >= cutoff)
                        .unwrap_or(false)
                })
                .unwrap_or(true)
        })
        .filter(|message| from.as_deref().map(|f| message.from == f).unwrap_or(true))
        .filter(|message| kind.as_deref().map(|k| message.kind == k).unwrap_or(true))
        .filter(|message| {
            mentions
                .as_deref()
                .map(|who| {
                    message.mentions.iter().any(|m| m == who)
                        || message.to.iter().any(|t| t == who)
                        || (message.to.iter().any(|recipient| recipient == "*")
                            && mention_wildcard_convs.contains(&message.conversation_id))
                })
                .unwrap_or(true)
        })
        .filter(|message| {
            pattern
                .as_deref()
                .map(|p| message_matches_pattern(message, p))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    if rows.len() > args.limit {
        rows = rows.split_off(rows.len() - args.limit);
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("no matches");
        return Ok(());
    }
    for message in rows {
        println!(
            "{} {} {} -> {} [{}] {} {}",
            message.id,
            message.conversation_id,
            message.from,
            message.to.join(","),
            message.kind,
            message.subject,
            truncated_body(&message.body, 120)
        );
    }
    Ok(())
}

fn cmd_thread(root: &Path, args: ThreadArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let (_, root_message) = find_message(root, &args.message_id)?;
    if !message_visible_to(&root_message, &agent_id) {
        bail_code!(
            "not_participant",
            "message {:?} is not visible to {agent_id:?}",
            root_message.id
        );
    }
    let mut rows = visible_messages(root, &agent_id, Some(&root_message.conversation_id))?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    let (tree, omitted) = build_thread_view(&root_message.id, &rows, args.limit)?;
    if args.json {
        let view = ThreadView {
            root: tree,
            truncated: omitted > 0,
            omitted,
        };
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        print_thread_node(&tree, 0);
        if omitted > 0 {
            println!("\n... {omitted} earlier message(s) omitted; raise --limit to see them");
        }
    }
    Ok(())
}

fn cmd_read(root: &Path, args: ReadArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let (path, message) = find_message(root, &args.message_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", message.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    write_receipt(root, &agent_id, &message, "read", None)?;
    // Emit the viewer-relative view (a superset of the raw message), matching
    // `inbox`/`show`. A non-terminal `read` receipt does not satisfy an ask, so
    // `awaiting_me` still flags an open ask the reader owes — without this the
    // agent had to make a separate `awaiting` call to learn that reading the
    // message did not discharge its obligation.
    let view = build_view(root, message, &agent_id)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        let message = &view.message;
        println!(
            "{} {} {} -> {:?}",
            message.id, message.conversation_id, message.from, message.to
        );
        if !message.subject.is_empty() {
            println!("Subject: {}", message.subject);
        }
        println!("\n{}", message.body);
        if view.awaiting_me {
            let wants = if await_kind_for(message, &agent_id) == "needs_response" {
                "reply"
            } else {
                "ack"
            };
            println!("\nawaiting: you still owe a {wants} on this");
        }
        println!("\nsource={}", path.display());
    }
    Ok(())
}

fn cmd_ack(root: &Path, args: AckArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let status = validate_ack_status(&args.status)?;
    ensure_root(root)?;
    let (_, message) = find_message(root, &args.message_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", message.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;

    // Decide, under the lock, whether this ack actually closes an open ask: the
    // status must be terminal, the agent must be in the message's awaited set,
    // and the agent must not have already recorded a terminal receipt.
    let meta: Option<Meta> = read_json(
        &root
            .join("conversations")
            .join(&message.conversation_id)
            .join("meta.json"),
    )?;
    let awaited = meta
        .as_ref()
        .map(|meta| message_awaited(&message, meta))
        .unwrap_or_default();
    let was_awaited = awaited.iter().any(|who| who == &agent_id);
    let already_terminal = read_json::<Receipt>(&receipt_path_for(root, &message, &agent_id))?
        .map(|receipt| ask_is_terminal(&receipt.status))
        .unwrap_or(false);
    let closed_ask = ask_is_terminal(&status) && was_awaited && !already_terminal;
    // A withdrawn ask collapses `awaited` to empty, so `was_awaited` reads false
    // — indistinguishable from "you were never asked". Surface the withdrawal so
    // a worker that raced the sender's withdrawal can tell "too late, it was
    // withdrawn" (and why) from "this was never my obligation".
    let withdrawn = message.withdrawn.clone();

    if args.require_open && !closed_ask {
        return Err(RaftError::coded(
            "not_awaited",
            format!(
                "ack does not close an open ask awaiting @{agent_id}: message {:?}",
                message.id
            ),
        )
        .with_details(serde_json::json!({
            "message_id": message.id,
            "awaited": awaited,
            "was_awaited": was_awaited,
            "withdrawn": withdrawn,
        })));
    }

    let effective = write_receipt(root, &agent_id, &message, &status, args.note)?;
    // The guard may have preserved a stronger stored status (e.g. `ack working`
    // after `done`). Report the status that actually stuck, plus a flag, so the
    // caller never believes a downgrade took effect.
    let downgrade_ignored = effective != status;
    if args.json {
        emit_ok(serde_json::json!({
            "message_id": args.message_id,
            "agent": agent_id,
            "status": effective,
            "requested_status": status,
            "downgrade_ignored": downgrade_ignored,
            "was_awaited": was_awaited,
            "closed_ask": closed_ask,
            "withdrawn": withdrawn,
        }))?;
    } else {
        let suffix = if downgrade_ignored {
            format!(" (kept {effective}; {status} would downgrade)")
        } else if withdrawn.is_some() {
            " (ask withdrawn)".to_string()
        } else if closed_ask {
            " (closed ask)".to_string()
        } else {
            String::new()
        };
        println!("{} {}{}", effective, args.message_id, suffix);
    }
    Ok(())
}

fn cmd_receipts(root: &Path, args: ReceiptsArgs) -> Result<()> {
    ensure_root(root)?;
    let (_, message) = find_message(root, &args.message_id)?;
    let conv = conversation_path(root, &message.conversation_id)?;
    let meta: Meta = read_json(&conv.join("meta.json"))?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("conversation {:?} does not exist", message.conversation_id),
        )
    })?;
    let recipients = receipt_recipients(&message, &meta);
    let receipts = load_message_receipts(root, &message)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "message": {
                    "id": message.id,
                    "conversation_id": message.conversation_id,
                    "from": message.from,
                    "to": message.to,
                    "created_at": message.created_at,
                    "requires_ack": message.requires_ack
                },
                "recipients": recipients,
                "receipts": receipts
            }))?
        );
        return Ok(());
    }
    println!(
        "{} in {} from={} created={}",
        message.id, message.conversation_id, message.from, message.created_at
    );
    println!("Recipients: {}", recipients.join(","));
    println!("Receipts:");
    for recipient in recipients {
        if let Some(receipt) = receipts.get(&recipient) {
            println!(
                "  {}: read={} last={} status={} note={}",
                recipient,
                receipt.read_at.as_deref().unwrap_or("null"),
                receipt.updated_at,
                receipt.status,
                receipt
                    .note
                    .as_ref()
                    .map(|note| format!("{note:?}"))
                    .unwrap_or_else(|| "null".to_string())
            );
        } else {
            println!("  {recipient}: <none>");
        }
    }
    Ok(())
}

fn cmd_journal(root: &Path, args: JournalArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    validate_id(&args.kind, "journal kind")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("journal-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let entry = JournalEntry {
        v: SCHEMA_VERSION,
        id: format!("j-{}", unique_token()),
        agent: agent_id.clone(),
        kind: args.kind,
        subject: args.subject,
        body: args.body,
        created_at: iso_now(),
    };
    append_jsonl(
        &root.join("journal").join(format!("{agent_id}.jsonl")),
        &entry,
    )?;
    if args.json {
        emit_ok(serde_json::json!({
            "entry_id": entry.id,
            "agent": agent_id,
            "kind": entry.kind,
        }))?;
    } else {
        println!("{}", entry.id);
    }
    Ok(())
}

fn cmd_status(root: &Path, args: StatusArgs) -> Result<()> {
    ensure_root(root)?;
    let scoped_agent = args
        .agent
        .as_deref()
        .map(|agent| validate_id(agent, "agent id"))
        .transpose()?;
    let mut agents = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if !is_agent_record_file(&entry.path()) {
            continue;
        }
        let agent: Agent = read_json(&entry.path())?.ok_or_else(|| {
            RaftError::new(format!(
                "agent file disappeared: {}",
                entry.path().display()
            ))
        })?;
        let active = parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= Utc::now())
            .unwrap_or(false);
        let mention = if agent.mention.is_empty() {
            format!("@{}", agent.id)
        } else {
            agent.mention.clone()
        };
        agents.push(serde_json::json!({
            "id": agent.id,
            "mention": mention,
            "workspace": agent.workspace,
            "capabilities": agent.capabilities,
            "last_seen_at": agent.last_seen_at,
            "expires_at": agent.expires_at,
            "current_state": agent.current_state,
            "state_note": agent.state_note,
            "state_updated_at": agent.state_updated_at,
            "active": active
        }));
    }

    let asks = gather_open_asks(root, None, scoped_agent.as_deref())?;
    let mut open_asks_by_conv: BTreeMap<String, usize> = BTreeMap::new();
    for ask in &asks {
        *open_asks_by_conv
            .entry(ask.conversation_id.clone())
            .or_default() += 1;
    }

    let mut conversations = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if let Some(agent_id) = scoped_agent.as_deref()
            && meta.private
            && !meta
                .participants
                .iter()
                .any(|participant| participant == agent_id)
        {
            continue;
        }
        // Count only `*.json` message files. `atomic_write_json` leaves a `.tmp`
        // sibling if a crash interrupts the create→rename, and every other count
        // path (gather_open_asks, visible_messages, channel list, gc archive, ui)
        // filters by extension — counting raw entries here would inflate `status`
        // past `me`/`ui`/`channel list` for the same room.
        let messages = sorted_read_dir(&conv.join("messages"))
            .map(|items| {
                items
                    .iter()
                    .filter(|entry| entry.path().extension() == Some(OsStr::new("json")))
                    .count()
            })
            .unwrap_or(0);
        conversations.push(serde_json::json!({
            "id": meta.id,
            "participants": meta.participants,
            "channel": meta.channel,
            "private": meta.private,
            "messages": messages,
            "open_asks": open_asks_by_conv.get(&meta.id).copied().unwrap_or(0)
        }));
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "root": root,
                "agents": agents,
                "conversations": conversations
            }))?
        );
        return Ok(());
    }

    println!("root: {}", root.display());
    println!("agents:");
    if agents.is_empty() {
        println!("  none");
    }
    for agent in agents {
        let liveness = if agent["active"].as_bool().unwrap_or(false) {
            "live"
        } else {
            "stale"
        };
        let note = agent["state_note"].as_str().unwrap_or("");
        let note_suffix = if note.is_empty() {
            String::new()
        } else {
            format!(" — {note}")
        };
        println!(
            "  {} ({}): {liveness}/{}{}",
            agent["id"].as_str().unwrap_or("unknown"),
            agent["mention"].as_str().unwrap_or(""),
            agent["current_state"].as_str().unwrap_or("idle"),
            note_suffix
        );
    }
    println!("conversations:");
    if conversations.is_empty() {
        println!("  none");
    }
    for conversation in conversations {
        let room_kind = if conversation["channel"].as_bool().unwrap_or(false) {
            "channel"
        } else if conversation["private"].as_bool().unwrap_or(false) {
            "private"
        } else {
            "chat"
        };
        println!(
            "  {} [{}]: messages={} open_asks={}",
            conversation["id"].as_str().unwrap_or("unknown"),
            room_kind,
            conversation["messages"].as_u64().unwrap_or(0),
            conversation["open_asks"].as_u64().unwrap_or(0)
        );
    }
    Ok(())
}

fn cmd_gc(root: &Path, args: GcArgs) -> Result<()> {
    ensure_root(root)?;
    let mut stale_locks = 0;
    let mut archived_messages = 0;
    let mut orphan_temp_files = 0;

    for entry in sorted_read_dir(&root.join("locks"))? {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("lock")) {
            continue;
        }
        if reap_stale_lock(root, &path)? {
            stale_locks += 1;
        }
    }

    for path in collect_orphan_temp_files(root)? {
        if fs::remove_file(&path).is_ok() {
            orphan_temp_files += 1;
        }
    }

    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if args.archive {
            let _lock = DirLock::acquire(
                root,
                &format!("conversation-{}", meta.id),
                LOCK_TTL_SECONDS,
                LOCK_TIMEOUT_SECONDS,
            )?;
            archived_messages += archive_old_messages(root, &conv, &meta)?;
        }
    }

    println!(
        "gc complete: stale_locks={stale_locks} archived_messages={archived_messages} orphan_temp_files={orphan_temp_files}"
    );
    Ok(())
}

fn cmd_serve(root: &Path, args: ServeArgs) -> Result<()> {
    ensure_root(root)?;
    let serve_lock = DirLock::acquire(root, "serve", SERVE_LOCK_TTL_SECONDS, LOCK_TIMEOUT_SECONDS)?;
    println!(
        "raft monitor serving {}; interval={}s",
        root.display(),
        args.interval
    );
    loop {
        serve_lock.refresh(SERVE_LOCK_TTL_SECONDS)?;
        cmd_gc(
            root,
            GcArgs {
                archive: args.archive,
            },
        )?;
        serve_lock.refresh(SERVE_LOCK_TTL_SECONDS)?;
        thread::sleep(Duration::from_secs_f64(args.interval));
    }
}

/// Ids of every conversation/channel on the bus (those with a `meta.json`).
fn conversation_ids(root: &Path) -> Vec<String> {
    let Ok(entries) = sorted_read_dir(&root.join("conversations")) else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter_map(|entry| {
            let path = entry.path();
            if path.is_dir() && path.join("meta.json").exists() {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_string)
            } else {
                None
            }
        })
        .collect()
}

/// A `not_found` error for a missing conversation/channel, enriched with
/// nearest-match `suggestions` so an agent can recover from a typo'd id in one
/// shot instead of falling back to `channel list` / `me`.
fn conversation_not_found(root: &Path, id: &str, noun: &str) -> RaftError {
    let err = RaftError::coded("not_found", format!("{noun} {id:?} does not exist"));
    let suggestions = nearest_ids(id, &conversation_ids(root), 3);
    if suggestions.is_empty() {
        err
    } else {
        err.with_details(serde_json::json!({ "suggestions": suggestions }))
    }
}

/// Ids of every claimed agent (live or stale), used for typo suggestions.
fn agent_ids(root: &Path) -> Vec<String> {
    let Ok(entries) = sorted_read_dir(&root.join("agents")) else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter_map(|entry| {
            let path = entry.path();
            if is_agent_record_file(&path) {
                path.file_stem()
                    .and_then(|name| name.to_str())
                    .map(str::to_string)
            } else {
                None
            }
        })
        .collect()
}

fn load_claimed_agent(root: &Path, agent_id: &str) -> Result<Agent> {
    read_json(&agent_path(root, agent_id))?.ok_or_else(|| not_claimed(root, agent_id))
}

fn ensure_claimed_agents(root: &Path, agent_ids: &[String]) -> Result<()> {
    for agent_id in agent_ids {
        load_claimed_agent(root, agent_id)?;
    }
    Ok(())
}

fn ensure_claimed_recipients(root: &Path, meta: &Meta, recipients: &[String]) -> Result<()> {
    let mut resolved = BTreeSet::new();
    for recipient in recipients {
        if recipient == "*" {
            resolved.extend(meta.participants.iter().cloned());
        } else {
            resolved.insert(recipient.clone());
        }
    }
    for agent_id in resolved {
        load_claimed_agent(root, &agent_id)?;
    }
    Ok(())
}

/// A `not_claimed` error enriched with nearest-match agent-id `suggestions`, so
/// an agent that mistypes its own id (or a peer's) gets a one-shot recovery
/// path instead of a bare string. Mirrors [`conversation_not_found`].
fn not_claimed(root: &Path, agent_id: &str) -> RaftError {
    let err = RaftError::coded(
        "not_claimed",
        format!("agent @{agent_id} is not claimed; use raft claim"),
    );
    let suggestions = nearest_ids(agent_id, &agent_ids(root), 3);
    if suggestions.is_empty() {
        err
    } else {
        err.with_details(serde_json::json!({ "suggestions": suggestions }))
    }
}

fn load_conversation(root: &Path, conversation_id: &str) -> Result<(PathBuf, Meta)> {
    let conv = conversation_path(root, conversation_id)?;
    let meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| conversation_not_found(root, conversation_id, "conversation"))?;
    Ok((conv, meta))
}

fn enforce_rate_limit(
    conv: &Path,
    meta: &Meta,
    sender: &str,
    subject_id: Option<&str>,
    body: &str,
) -> Result<()> {
    let size = body.len();
    if size > meta.rate.max_message_bytes {
        return Err(RaftError::coded(
            "too_large",
            format!(
                "message is {size} bytes; limit is {}",
                meta.rate.max_message_bytes
            ),
        )
        .with_details(serde_json::json!({
            "size": size,
            "limit": meta.rate.max_message_bytes,
        })));
    }
    let path = conv.join("rate.json");
    let mut state: RateState = read_json(&path)?.unwrap_or_default();
    let now = Utc::now();
    let rate_key = rate_key(sender, subject_id);
    let entry = state
        .senders
        .entry(rate_key.clone())
        .or_insert_with(|| SenderRate {
            window_start: iso_now(),
            count: 0,
            last_sent_at: None,
        });
    let window_start = parse_time(&entry.window_start).unwrap_or(now);
    if (now - window_start).num_seconds() >= meta.rate.window_seconds as i64 {
        entry.window_start = iso_now();
        entry.count = 0;
    }
    if entry.count >= meta.rate.max_messages_per_sender {
        let elapsed = (now - parse_time(&entry.window_start).unwrap_or(now)).num_seconds();
        let retry_after_seconds = (meta.rate.window_seconds as i64 - elapsed).max(0);
        return Err(RaftError::coded(
            "rate_limited",
            format!(
                "rate limited: {rate_key:?} already sent {} messages in {}s for {:?}",
                meta.rate.max_messages_per_sender, meta.rate.window_seconds, meta.id
            ),
        )
        .with_details(serde_json::json!({
            "window_seconds": meta.rate.window_seconds,
            "max_messages_per_sender": meta.rate.max_messages_per_sender,
            "count": entry.count,
            "retry_after_seconds": retry_after_seconds,
        })));
    }
    entry.count += 1;
    entry.last_sent_at = Some(iso_now());
    atomic_write_json(&path, &state)?;
    Ok(())
}

fn visible_messages(
    root: &Path,
    agent_id: &str,
    conversation_id: Option<&str>,
) -> Result<Vec<Message>> {
    let mut messages = Vec::new();
    let conversation_dirs = if let Some(conversation_id) = conversation_id {
        vec![conversation_path(root, conversation_id)?]
    } else {
        sorted_read_dir(&root.join("conversations"))?
            .into_iter()
            .map(|entry| entry.path())
            .collect()
    };
    for conv in conversation_dirs {
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta.participants.iter().any(|item| item == agent_id) {
            continue;
        }
        for entry in sorted_read_dir(&conv.join("messages"))? {
            if entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            let Some(message): Option<Message> = read_json(&entry.path())? else {
                continue;
            };
            if message_visible_to(&message, agent_id) {
                messages.push(message);
            }
        }
    }
    Ok(messages)
}

pub(crate) fn message_visible_to(message: &Message, agent_id: &str) -> bool {
    message.from == agent_id
        || message
            .to
            .iter()
            .any(|item| item == "*" || item == agent_id)
}

/// Decorate a message with viewer-relative fields (`unread`, `awaiting_me`,
/// `my_status`) so a `--json` reader gets the signals the CLI already computes
/// instead of re-deriving them with extra `awaiting`/`receipts` calls.
fn build_view(root: &Path, message: Message, agent_id: &str) -> Result<ViewMessage> {
    let unread = message_is_unread(root, &message, agent_id);
    let my_status =
        read_json::<Receipt>(&receipt_path_for(root, &message, agent_id))?.map(|r| r.status);
    let meta: Option<Meta> = read_json(
        &root
            .join("conversations")
            .join(&message.conversation_id)
            .join("meta.json"),
    )?;
    let awaiting_me = meta
        .map(|meta| {
            message_awaited(&message, &meta)
                .iter()
                .any(|a| a == agent_id)
                && !my_status.as_deref().map(ask_is_terminal).unwrap_or(false)
        })
        .unwrap_or(false);
    Ok(ViewMessage {
        message,
        unread,
        awaiting_me,
        my_status,
    })
}

fn build_views(root: &Path, rows: Vec<Message>, agent_id: &str) -> Result<Vec<ViewMessage>> {
    rows.into_iter()
        .map(|message| build_view(root, message, agent_id))
        .collect()
}

pub(crate) fn message_is_unread(root: &Path, message: &Message, agent_id: &str) -> bool {
    if message.kind == "receipt"
        || (message.kind == "system" && !is_onboarding_summary_message(message))
    {
        return false;
    }
    if message.from == agent_id {
        return false;
    }
    if receipt_path_for(root, message, agent_id).exists() {
        return false;
    }
    // No receipt yet: a message predating the viewer's membership is backlog,
    // not unread. Load meta only on this cold path to keep the common
    // already-read case a single stat.
    let meta: Option<Meta> = read_json(
        &root
            .join("conversations")
            .join(&message.conversation_id)
            .join("meta.json"),
    )
    .ok()
    .flatten();
    if let Some(meta) = meta
        && predates_membership(&meta, message, agent_id)
    {
        return false;
    }
    true
}

fn is_state_change_message(message: &Message) -> bool {
    message.kind == "system" && message.subject == "state changed"
}

/// Initialize the watch state for this run and return it alongside the prior
/// run's resume cursor. The resume cursor is reported separately from `--since`
/// because they are floors of different strength: an explicit `--since` is a
/// hard floor the caller chose, while the persisted resume cursor is only a soft
/// floor (see `cmd_watch`) that must never silently drop a still-unread message.
fn start_watch_state(
    root: &Path,
    agent_id: &str,
    since: Option<String>,
) -> Result<(WatchState, Option<String>)> {
    let previous: Option<WatchState> = read_json(&watch_state_path(root, agent_id))?;
    let resume_floor = previous.and_then(|state| state.last_event_id);
    let now = iso_now();
    let last_event_id = since.or_else(|| resume_floor.clone());
    let state = WatchState {
        v: SCHEMA_VERSION,
        agent: agent_id.to_string(),
        pid: process::id(),
        host: hostname(),
        started_at: now.clone(),
        updated_at: now,
        last_event_id,
        shutdown_at: None,
    };
    atomic_write_json(&watch_state_path(root, agent_id), &state)?;
    Ok((state, resume_floor))
}

fn emit_watch_message(root: &Path, message: &Message, agent_id: &str, json: bool) -> Result<()> {
    if json {
        let view = build_view(root, message.clone(), agent_id)?;
        println!("{}", serde_json::to_string(&view)?);
    } else {
        println!(
            "{} {} {} -> {} [{}] {} {}",
            message.id,
            message.conversation_id,
            message.from,
            message.to.join(","),
            message.kind,
            message.subject,
            truncated_body(&message.body, 120)
        );
    }
    io::stdout().flush()?;
    Ok(())
}

fn truncated_body(body: &str, width: usize) -> String {
    let mut body = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if body.len() > width {
        if width <= 3 {
            let mut end = width.min(body.len());
            while !body.is_char_boundary(end) {
                end -= 1;
            }
            body.truncate(end);
            return body;
        }
        let mut end = (width - 3).min(body.len());
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
        body.push_str("...");
    }
    body
}

/// Build the thread tree rooted at `root_id`, keeping at most `limit` messages.
/// When the reachable subtree is larger than the budget the *newest* messages
/// are kept (root is always kept), matching the windowing of `show`, `inbox`,
/// and `search`. Returns the assembled tree plus the count of omitted messages.
/// Dropped messages are spliced out: a survivor re-parents onto its nearest
/// surviving ancestor so the tree stays connected rather than losing branches.
fn build_thread_view(
    root_id: &str,
    messages: &[Message],
    limit: usize,
) -> Result<(ThreadNode, usize)> {
    let by_id: BTreeMap<&str, &Message> = messages
        .iter()
        .map(|message| (message.id.as_str(), message))
        .collect();
    if !by_id.contains_key(root_id) {
        return Err(RaftError::new(format!(
            "message {root_id:?} was not found in visible thread"
        )));
    }

    // Walk `after` pointers downward from the root to collect the reachable
    // subtree, guarding against cycles.
    let mut children_of: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for message in messages {
        if let Some(parent) = message.after.as_deref() {
            children_of
                .entry(parent)
                .or_default()
                .push(message.id.as_str());
        }
    }
    let mut reachable: BTreeSet<&str> = BTreeSet::new();
    let mut stack = vec![root_id];
    while let Some(id) = stack.pop() {
        if !reachable.insert(id) {
            continue;
        }
        if let Some(kids) = children_of.get(id) {
            stack.extend(kids.iter().copied());
        }
    }

    let total = reachable.len();
    let limit = limit.max(1);
    let kept: BTreeSet<&str> = if total <= limit {
        reachable.clone()
    } else {
        // Keep the root plus the newest `limit - 1` of the remaining reachable
        // ids. Ids are monotonically increasing, so highest id == most recent.
        let mut others: Vec<&str> = reachable
            .iter()
            .copied()
            .filter(|id| *id != root_id)
            .collect();
        others.sort();
        let tail = &others[others.len() - (limit - 1)..];
        let mut kept: BTreeSet<&str> = tail.iter().copied().collect();
        kept.insert(root_id);
        kept
    };
    let omitted = total - kept.len();

    // Re-parent each surviving non-root message onto its nearest surviving
    // ancestor (walk the `after` chain until a kept id is found; the root is
    // always kept, so this terminates).
    let mut display_children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for &id in &kept {
        if id == root_id {
            continue;
        }
        let mut ancestor = by_id[id].after.as_deref();
        while let Some(parent) = ancestor {
            if kept.contains(parent) {
                display_children.entry(parent).or_default().push(id);
                break;
            }
            ancestor = by_id
                .get(parent)
                .and_then(|message| message.after.as_deref());
        }
    }
    for kids in display_children.values_mut() {
        kids.sort();
    }

    let mut visited: BTreeSet<&str> = BTreeSet::new();
    let tree = assemble_thread_node(root_id, &by_id, &display_children, &mut visited);
    Ok((tree, omitted))
}

fn assemble_thread_node<'a>(
    id: &'a str,
    by_id: &BTreeMap<&'a str, &'a Message>,
    display_children: &BTreeMap<&'a str, Vec<&'a str>>,
    visited: &mut BTreeSet<&'a str>,
) -> ThreadNode {
    let message = (*by_id.get(id).expect("kept id must exist")).clone();
    if !visited.insert(id) {
        return ThreadNode {
            message,
            children: Vec::new(),
        };
    }
    let children = display_children
        .get(id)
        .map(|kids| {
            kids.iter()
                .map(|child| assemble_thread_node(child, by_id, display_children, visited))
                .collect()
        })
        .unwrap_or_default();
    ThreadNode { message, children }
}

fn print_thread_node(node: &ThreadNode, depth: usize) {
    let indent = "  ".repeat(depth);
    println!(
        "{}{} {} -> {} [{}] {}",
        indent,
        node.message.id,
        node.message.from,
        node.message.to.join(","),
        node.message.kind,
        node.message.subject
    );
    println!("{}  {}", indent, truncated_body(&node.message.body, 160));
    for child in &node.children {
        print_thread_node(child, depth + 1);
    }
}

fn message_matches_pattern(message: &Message, pattern: &str) -> bool {
    let haystack = format!(
        "{}\n{}\n{}\n{}\n{}",
        message.id, message.conversation_id, message.from, message.subject, message.body
    )
    .to_lowercase();
    haystack.contains(pattern)
}

fn parse_since_cutoff(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(time) = parse_time(value) {
        return Ok(time);
    }
    let Some((number, unit)) = value.split_at_checked(value.len().saturating_sub(1)) else {
        bail!("invalid --since {value:?}; use RFC3339 or a duration like 30m, 2h, 7d");
    };
    let amount: i64 = number.parse().map_err(|_| {
        RaftError::new(format!(
            "invalid --since {value:?}; duration must be numeric"
        ))
    })?;
    if amount < 0 {
        bail!("invalid --since {value:?}; duration must be non-negative");
    }
    let delta = match unit {
        "s" => TimeDelta::seconds(amount),
        "m" => TimeDelta::minutes(amount),
        "h" => TimeDelta::hours(amount),
        "d" => TimeDelta::days(amount),
        _ => bail!("invalid --since {value:?}; use s, m, h, or d duration suffix"),
    };
    Ok(Utc::now() - delta)
}

fn find_message(root: &Path, message_id: &str) -> Result<(PathBuf, Message)> {
    let message_id = validate_id(message_id, "message id")?;
    for conv_entry in sorted_read_dir(&root.join("conversations"))? {
        let path = conv_entry
            .path()
            .join("messages")
            .join(format!("{message_id}.json"));
        if path.exists() {
            let message: Message = read_json(&path)?.ok_or_else(|| {
                RaftError::new(format!("message file disappeared: {}", path.display()))
            })?;
            return Ok((path, message));
        }
    }
    bail_code!("not_found", "message {message_id:?} was not found");
}

pub(crate) fn receipt_recipients(message: &Message, meta: &Meta) -> Vec<String> {
    if message.to.iter().any(|recipient| recipient == "*") {
        return meta
            .participants
            .iter()
            .filter(|participant| *participant != &message.from)
            .cloned()
            .collect();
    }
    message
        .to
        .iter()
        .filter(|recipient| *recipient != &message.from)
        .cloned()
        .collect()
}

fn load_message_receipts(root: &Path, message: &Message) -> Result<BTreeMap<String, Receipt>> {
    let mut receipts = BTreeMap::new();
    let dir = root
        .join("conversations")
        .join(&message.conversation_id)
        .join("receipts")
        .join(&message.id);
    for entry in sorted_read_dir(&dir)? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(receipt): Option<Receipt> = read_json(&entry.path())? else {
            continue;
        };
        receipts.insert(receipt.agent.clone(), receipt);
    }
    Ok(receipts)
}

/// Record a receipt, returning the *effective* stored status after applying the
/// downgrade guard — which may differ from the requested `status` when the write
/// was preserved (callers report the effective status so an agent is never told
/// its weaker status stuck when it did not).
fn write_receipt(
    root: &Path,
    agent_id: &str,
    message: &Message,
    status: &str,
    note: Option<String>,
) -> Result<String> {
    let conv = conversation_path(root, &message.conversation_id)?;
    let meta: Meta = read_json(&conv.join("meta.json"))?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("conversation {:?} does not exist", message.conversation_id),
        )
    })?;
    ensure_participant(&meta, agent_id)?;
    if !message_visible_to(message, agent_id) {
        bail_code!(
            "not_participant",
            "message {:?} is not visible to {agent_id:?}",
            message.id
        );
    }
    let path = receipt_path_for(root, message, agent_id);
    let existing: Option<Receipt> = read_json(&path)?;
    let mut receipt = existing.unwrap_or_else(|| Receipt {
        v: SCHEMA_VERSION,
        message_id: message.id.clone(),
        conversation_id: message.conversation_id.clone(),
        agent: agent_id.to_string(),
        status: status.to_string(),
        updated_at: iso_now(),
        note: note.clone(),
        read_at: None,
        history: Vec::new(),
        hash: None,
        signer_key: None,
        sig: None,
    });
    receipt.history.push(ReceiptEvent {
        status: status.to_string(),
        at: iso_now(),
        note: note.clone(),
    });
    // A receipt status must never be silently downgraded: a weaker status that
    // arrives later must not overwrite a stronger stored one, which would reopen
    // a closed ask and un-resolve the asker's `wait --owed`. Two downgrades are
    // guarded:
    //   * a bare `read` marker (from re-reading or `watch` auto-read) must not
    //     revert any explicit ack back to `read`; and
    //   * a non-terminal ack (`received`/`accepted`/`working`/`blocked`) must not
    //     revert a terminal `done`/`rejected` — so `ack working` after `done`
    //     (directly or via `reply --ack`) is a no-op on the stored status.
    // A deliberate terminal→terminal change (`done`→`rejected`) and any upgrade
    // still apply. The `history`/`read_at` audit trail is stamped regardless.
    let downgrade = (status == "read" && receipt.status != "read")
        || (ask_is_terminal(&receipt.status) && !ask_is_terminal(status));
    if !downgrade {
        receipt.status = status.to_string();
        receipt.note = note;
    }
    receipt.updated_at = iso_now();
    if status == "read" && receipt.read_at.is_none() {
        receipt.read_at = Some(iso_now());
    }
    sign_receipt(root, agent_id, &mut receipt)?;
    atomic_write_json(&path, &receipt)?;
    Ok(receipt.status)
}

/// True when `message` is an open ask not yet discharged by every awaited agent
/// — at least one awaited agent lacks a terminal (`done`/`rejected`) receipt.
/// Every obligation view (`awaiting`/`me`/`roster`/`wait`) and the `ack`/
/// `withdraw` mutators scan only the live `messages/` dir, so archiving such a
/// message would silently vanish the obligation with no terminal receipt and no
/// withdrawal — stranding the worker's queue and falsely clearing the asker.
/// A withdrawn ask reads as resolved (`message_awaited` returns empty), so it
/// archives normally.
fn message_is_unresolved_ask(conv: &Path, message: &Message, meta: &Meta) -> Result<bool> {
    let awaited = message_awaited(message, meta);
    if awaited.is_empty() {
        return Ok(false);
    }
    let receipts = conv.join("receipts").join(&message.id);
    for who in awaited {
        let terminal = read_json::<Receipt>(&receipts.join(format!("{who}.json")))?
            .map(|receipt| ask_is_terminal(&receipt.status))
            .unwrap_or(false);
        if !terminal {
            return Ok(true);
        }
    }
    Ok(false)
}

fn archive_old_messages(_root: &Path, conv: &Path, meta: &Meta) -> Result<usize> {
    let cutoff = Utc::now() - TimeDelta::days(meta.retention_days as i64);
    let archive_dir = conv
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| RaftError::new("invalid conversation path".to_string()))?
        .join("archive")
        .join(&meta.id);
    let mut archived = 0;
    for entry in sorted_read_dir(&conv.join("messages"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(message): Option<Message> = read_json(&entry.path())? else {
            continue;
        };
        let created_at = parse_time(&message.created_at).unwrap_or_else(|_| Utc::now());
        if created_at >= cutoff {
            continue;
        }
        // Retain an unresolved obligation past its retention window rather than
        // archive it into invisibility; `withdraw` releases it so it can age out.
        if message_is_unresolved_ask(conv, &message, meta)? {
            continue;
        }
        fs::create_dir_all(&archive_dir)?;
        set_dir_private(&archive_dir)?;
        let mut target = archive_dir.join(entry.file_name());
        if target.exists() {
            target = archive_dir.join(format!("{}-{}.json", message.id, unique_token()));
        }
        fs::rename(entry.path(), &target)?;
        archive_message_receipts(conv, &archive_dir, &message, &target)?;
        archived += 1;
    }
    Ok(archived)
}

fn archive_message_receipts(
    conv: &Path,
    archive_dir: &Path,
    message: &Message,
    archived_message_path: &Path,
) -> Result<()> {
    let source = conv.join("receipts").join(&message.id);
    if !source.exists() {
        return Ok(());
    }
    let receipts_dir = archive_dir.join("receipts");
    fs::create_dir_all(&receipts_dir)?;
    set_dir_private(&receipts_dir)?;
    let stem = archived_message_path
        .file_stem()
        .and_then(OsStr::to_str)
        .ok_or_else(|| RaftError::new("invalid archived message path".to_string()))?;
    let mut target = receipts_dir.join(stem);
    if target.exists() {
        target = receipts_dir.join(format!("{stem}-{}", unique_token()));
    }
    fs::rename(source, target)?;
    Ok(())
}

fn write_state_change_messages(
    root: &Path,
    agent_id: &str,
    state: &str,
    note: Option<&str>,
) -> Result<()> {
    let body = match note {
        Some(note) if !note.is_empty() => format!("@{agent_id} is now {state}: {note}"),
        _ => format!("@{agent_id} is now {state}"),
    };
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta
            .participants
            .iter()
            .any(|participant| participant == agent_id)
        {
            continue;
        }
        let _lock = DirLock::acquire(
            root,
            &format!("conversation-{}", meta.id),
            LOCK_TTL_SECONDS,
            LOCK_TIMEOUT_SECONDS,
        )?;
        write_system_message(
            &conv,
            &meta.id,
            vec!["*".to_string()],
            body.clone(),
            "state changed",
        )?;
    }
    Ok(())
}

fn summary_reaches_all_participants(message: &Message, meta: &Meta) -> bool {
    meta.participants.iter().all(|participant| {
        message.from == *participant
            || message
                .to
                .iter()
                .any(|recipient| recipient == "*" || recipient == participant)
    })
}

fn latest_shared_summary_body(conv: &Path, meta: &Meta) -> Result<Option<String>> {
    let mut latest: Option<Message> = None;
    for entry in sorted_read_dir(&conv.join("messages"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(message): Option<Message> = read_json(&entry.path())? else {
            continue;
        };
        if message.kind != "summary" || !summary_reaches_all_participants(&message, meta) {
            continue;
        }
        let replace = latest
            .as_ref()
            .map(|current| {
                (message.created_at.as_str(), message.id.as_str())
                    > (current.created_at.as_str(), current.id.as_str())
            })
            .unwrap_or(true);
        if replace {
            latest = Some(message);
        }
    }
    Ok(latest.map(|message| message.body))
}

fn write_onboarding_summary_message(
    conv: &Path,
    conversation_id: &str,
    agent_id: &str,
    prior_meta: &Meta,
) -> Result<String> {
    let body = latest_shared_summary_body(conv, prior_meta)?
        .unwrap_or_else(|| "No rolling summary yet.".to_string());
    write_system_message(
        conv,
        conversation_id,
        vec![agent_id.to_string()],
        body,
        "conversation summary",
    )
}

fn is_onboarding_summary_message(message: &Message) -> bool {
    message.kind == "system" && message.subject == "conversation summary"
}

pub(crate) fn write_system_message(
    conv: &Path,
    conversation_id: &str,
    to: Vec<String>,
    body: String,
    subject: &str,
) -> Result<String> {
    let message_id = new_message_id();
    let message = Message {
        v: SCHEMA_VERSION,
        id: message_id.clone(),
        conversation_id: conversation_id.to_string(),
        kind: "system".to_string(),
        from: "raft".to_string(),
        to,
        mentions: Vec::new(),
        subject: subject.to_string(),
        body,
        created_at: iso_now(),
        requires_ack: false,
        needs_response_from: Vec::new(),
        subject_id: None,
        after: None,
        hash: None,
        signer_key: None,
        sig: None,
        withdrawn: None,
    };
    atomic_write_json(
        &conv.join("messages").join(format!("{message_id}.json")),
        &message,
    )?;
    Ok(message_id)
}

fn ensure_participant(meta: &Meta, agent_id: &str) -> Result<()> {
    if !meta.participants.iter().any(|item| item == agent_id) {
        return Err(RaftError::coded(
            "not_participant",
            format!("agent {agent_id:?} is not a participant in {:?}", meta.id),
        )
        .with_details(serde_json::json!({ "participants": meta.participants })));
    }
    Ok(())
}

fn ensure_recipients(meta: &Meta, recipients: &[String]) -> Result<()> {
    for recipient in recipients {
        if recipient != "*" && !meta.participants.iter().any(|item| item == recipient) {
            return Err(RaftError::coded(
                "not_participant",
                format!(
                    "recipient {recipient:?} is not a participant in {:?}",
                    meta.id
                ),
            )
            .with_details(serde_json::json!({ "participants": meta.participants })));
        }
    }
    Ok(())
}

fn mentioned_participants(meta: &Meta, subject: &str, body: &str) -> Vec<String> {
    let mut mentions = BTreeSet::new();
    for mention in extract_mentions(subject)
        .into_iter()
        .chain(extract_mentions(body))
    {
        if meta
            .participants
            .iter()
            .any(|participant| participant == &mention)
        {
            mentions.insert(mention);
        }
    }
    mentions.into_iter().collect()
}
