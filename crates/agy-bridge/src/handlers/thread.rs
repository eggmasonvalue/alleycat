//! `thread/*` request handlers for Google Antigravity CLI (`agy`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use alleycat_codex_proto as p;
use thiserror::Error;

use crate::handlers::model::{DEFAULT_MODEL, normalize_agy_model, normalize_agy_model_id};
use crate::index::{AgySessionRef, IndexEntry, entry_to_thread};
use crate::pool::PoolError;
use crate::state::ConnectionState;

#[derive(Debug, Error)]
pub enum ThreadError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("thread `{0}` not found in index")]
    NotFound(String),
    #[error("pool error: {0}")]
    Pool(String),
    #[error("method `{0}` is not supported")]
    Unsupported(String),
    #[error(transparent)]
    Index(#[from] anyhow::Error),
}

impl ThreadError {
    pub fn rpc_code(&self) -> i64 {
        match self {
            ThreadError::InvalidParams(_) | ThreadError::NotFound(_) => {
                p::error_codes::INVALID_PARAMS
            }
            ThreadError::Unsupported(_) => p::error_codes::METHOD_NOT_FOUND,
            ThreadError::Pool(_) | ThreadError::Index(_) => p::error_codes::INTERNAL_ERROR,
        }
    }

    fn pool(err: PoolError) -> Self {
        Self::Pool(format!("{err:#}"))
    }
}

pub async fn handle_thread_start(
    state: &Arc<ConnectionState>,
    params: p::ThreadStartParams,
) -> Result<p::ThreadStartResponse, ThreadError> {
    let cwd = resolve_cwd(params.cwd.as_deref())?;
    let defaults = state.defaults();

    let model = normalize_agy_model(params.model.clone().or_else(|| defaults.model.clone()));
    let reasoning_effort = effort_from_params(&params.additional).or(defaults.reasoning_effort);
    let effort = reasoning_effort.map(|e| format!("{e:?}").to_lowercase());

    let (thread_id, _handle) = state
        .agy_pool()
        .acquire_for_new_thread(&cwd, model.clone(), effort)
        .await
        .map_err(ThreadError::pool)?;

    let now_ms = now_unix_millis();
    let model_provider = params
        .model_provider
        .clone()
        .or_else(|| defaults.model_provider.clone())
        .unwrap_or_else(|| "google".to_string());

    let entry = IndexEntry {
        thread_id: thread_id.clone(),
        cwd: cwd.to_string_lossy().into_owned(),
        name: params.service_name.clone(),
        preview: String::new(),
        created_at: now_ms,
        updated_at: now_ms,
        archived: false,
        forked_from_id: None,
        model_provider: model_provider.clone(),
        source: p::ThreadSourceKind::AppServer,
        metadata: AgySessionRef {
            agy_session_id: thread_id.clone(),
            parent_conversation_id: None,
            nesting_depth: 0,
            status: "ACTIVE".to_string(),
            step_count: 0,
        },
    };

    state
        .thread_index()
        .insert(entry.clone())
        .await
        .map_err(ThreadError::from)?;

    let response_model = model
        .map(|m| normalize_agy_model_id(&m))
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());

    let approval_policy = params
        .approval_policy
        .or(defaults.approval_policy)
        .unwrap_or(p::AskForApproval::Never);
    let approvals_reviewer = params
        .approvals_reviewer
        .or(defaults.approvals_reviewer)
        .unwrap_or(p::ApprovalsReviewer::User);
    let sandbox = sandbox_value(params.sandbox.or(defaults.sandbox));

    Ok(p::ThreadStartResponse {
        thread: entry_to_thread(&entry),
        model: response_model,
        model_provider,
        service_tier: Some(default_service_tier()),
        cwd: entry.cwd.clone(),
        instruction_sources: Vec::new(),
        approval_policy,
        approvals_reviewer,
        sandbox,
        permission_profile: params.permission_profile.or_else(|| Some(default_permission_profile())),
        active_permission_profile: None,
        reasoning_effort: reasoning_effort.or(Some(p::ReasoningEffort::High)),
    })
}

pub async fn handle_thread_resume(
    state: &Arc<ConnectionState>,
    params: p::ThreadResumeParams,
) -> Result<p::ThreadResumeResponse, ThreadError> {
    let entry = match state.thread_index().lookup(&params.thread_id).await {
        Some(e) => e,
        None => {
            let cwd_str = resolve_cwd(params.cwd.as_deref())
                .unwrap_or_else(|_| PathBuf::from("/tmp"))
                .to_string_lossy()
                .into_owned();
            let now_ms = now_unix_millis();
            let entry = IndexEntry {
                thread_id: params.thread_id.clone(),
                cwd: cwd_str,
                name: None,
                preview: String::new(),
                created_at: now_ms,
                updated_at: now_ms,
                archived: false,
                forked_from_id: None,
                model_provider: "google".to_string(),
                source: p::ThreadSourceKind::AppServer,
                metadata: AgySessionRef {
                    agy_session_id: params.thread_id.clone(),
                    parent_conversation_id: None,
                    nesting_depth: 0,
                    status: "ACTIVE".to_string(),
                    step_count: 0,
                },
            };
            let _ = state.thread_index().insert(entry.clone()).await;
            entry
        }
    };

    let cwd = PathBuf::from(&entry.cwd);
    let defaults = state.defaults();
    let model = normalize_agy_model(params.model.clone().or_else(|| defaults.model.clone()));
    let effort = defaults.reasoning_effort.map(|e| format!("{e:?}").to_lowercase());

    let _handle = state
        .agy_pool()
        .acquire_for_resume(&params.thread_id, &cwd, model.clone(), effort)
        .await
        .map_err(ThreadError::pool)?;

    let mut thread = entry_to_thread(&entry);
    let recorded = state.recorded_turns(&params.thread_id);
    if !recorded.is_empty() {
        thread.turns = recorded
            .into_iter()
            .map(|r| p::Turn {
                id: r.turn_id,
                items: r.items,
                items_view: p::default_items_view(),
                status: r.status,
                error: r.error,
                started_at: Some(r.started_at),
                completed_at: r.completed_at,
                duration_ms: r.completed_at.map(|c| c.saturating_sub(r.started_at)),
            })
            .collect();
    }

    let response_model = model
        .map(|m| normalize_agy_model_id(&m))
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());

    let model_provider = entry.model_provider.clone();
    let approval_policy = params
        .approval_policy
        .or(defaults.approval_policy)
        .unwrap_or(p::AskForApproval::Never);
    let approvals_reviewer = params
        .approvals_reviewer
        .or(defaults.approvals_reviewer)
        .unwrap_or(p::ApprovalsReviewer::User);
    let sandbox = sandbox_value(params.sandbox.or(defaults.sandbox));

    Ok(p::ThreadResumeResponse {
        thread,
        model: response_model,
        model_provider,
        service_tier: Some(default_service_tier()),
        cwd: entry.cwd.clone(),
        instruction_sources: Vec::new(),
        approval_policy,
        approvals_reviewer,
        sandbox,
        permission_profile: params.permission_profile.or_else(|| Some(default_permission_profile())),
        active_permission_profile: None,
        reasoning_effort: defaults.reasoning_effort.or(Some(p::ReasoningEffort::High)),
    })
}

pub async fn handle_thread_list(
    state: &Arc<ConnectionState>,
    params: p::ThreadListParams,
) -> Result<p::ThreadListResponse, ThreadError> {
    let archived = Some(params.archived.unwrap_or(false));
    let filter = crate::state::ListFilter {
        archived,
        cwds: parse_cwd_filter(&params.cwd),
        search_term: params.search_term.clone(),
        model_providers: params.model_providers.clone(),
        source_kinds: params.source_kinds.clone(),
    };
    let sort = crate::state::ListSort {
        key: params.sort_key.unwrap_or(p::ThreadSortKey::UpdatedAt),
        direction: params.sort_direction.unwrap_or(p::SortDirection::Desc),
    };
    let limit = alleycat_bridge_core::resolve_list_limit(params.limit);

    let page = state
        .thread_index()
        .list(&filter, sort, params.cursor.as_deref(), Some(limit))
        .await
        .map_err(ThreadError::from)?;

    let data = page
        .data
        .into_iter()
        .map(|entry| entry_to_thread(&entry))
        .collect();

    Ok(p::ThreadListResponse {
        data,
        next_cursor: page.next_cursor,
        backwards_cursor: None,
    })
}

pub async fn handle_thread_loaded_list(
    state: &Arc<ConnectionState>,
    _params: p::ThreadLoadedListParams,
) -> p::ThreadLoadedListResponse {
    let data = state.agy_pool().loaded_thread_ids().await;
    p::ThreadLoadedListResponse {
        data,
        next_cursor: None,
    }
}

pub async fn handle_thread_read(
    state: &Arc<ConnectionState>,
    params: p::ThreadReadParams,
) -> Result<p::ThreadReadResponse, ThreadError> {
    let entry = match state.thread_index().lookup(&params.thread_id).await {
        Some(e) => e,
        None => {
            let now_ms = now_unix_millis();
            IndexEntry {
                thread_id: params.thread_id.clone(),
                cwd: "/tmp".to_string(),
                name: None,
                preview: String::new(),
                created_at: now_ms,
                updated_at: now_ms,
                archived: false,
                forked_from_id: None,
                model_provider: "google".to_string(),
                source: p::ThreadSourceKind::AppServer,
                metadata: AgySessionRef {
                    agy_session_id: params.thread_id.clone(),
                    parent_conversation_id: None,
                    nesting_depth: 0,
                    status: "ACTIVE".to_string(),
                    step_count: 0,
                },
            }
        }
    };

    let mut thread = entry_to_thread(&entry);
    if params.include_turns {
        let recorded = state.recorded_turns(&params.thread_id);
        if !recorded.is_empty() {
            thread.turns = recorded
                .into_iter()
                .map(|r| p::Turn {
                    id: r.turn_id,
                    items: r.items,
                    items_view: p::default_items_view(),
                    status: r.status,
                    error: r.error,
                    started_at: Some(r.started_at),
                    completed_at: r.completed_at,
                    duration_ms: r.completed_at.map(|c| c.saturating_sub(r.started_at)),
                })
                .collect();
        }
    }
    Ok(p::ThreadReadResponse { thread })
}

pub async fn handle_thread_turns_list(
    state: &Arc<ConnectionState>,
    params: p::ThreadTurnsListParams,
) -> Result<p::ThreadTurnsListResponse, ThreadError> {
    let recorded = state.recorded_turns(&params.thread_id);
    let data = recorded
        .into_iter()
        .map(|r| p::Turn {
            id: r.turn_id,
            items: r.items,
            items_view: p::default_items_view(),
            status: r.status,
            error: r.error,
            started_at: Some(r.started_at),
            completed_at: r.completed_at,
            duration_ms: r.completed_at.map(|c| c.saturating_sub(r.started_at)),
        })
        .collect();
    Ok(p::ThreadTurnsListResponse {
        data,
        next_cursor: None,
        backwards_cursor: None,
    })
}

pub async fn handle_thread_archive(
    state: &Arc<ConnectionState>,
    params: p::ThreadArchiveParams,
) -> Result<p::ThreadArchiveResponse, ThreadError> {
    let changed = state
        .thread_index()
        .set_archived(&params.thread_id, true)
        .await
        .map_err(ThreadError::from)?;
    if !changed {
        return Err(ThreadError::NotFound(params.thread_id));
    }
    Ok(p::ThreadArchiveResponse::default())
}

pub async fn handle_thread_set_name(
    state: &Arc<ConnectionState>,
    params: p::ThreadSetNameParams,
) -> Result<p::ThreadSetNameResponse, ThreadError> {
    let trimmed = params.name.trim().to_string();
    let stored = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    };
    let changed = state
        .thread_index()
        .set_name(&params.thread_id, stored)
        .await
        .map_err(ThreadError::from)?;
    if !changed {
        return Err(ThreadError::NotFound(params.thread_id));
    }
    Ok(p::ThreadSetNameResponse::default())
}

fn parse_cwd_filter(value: &Option<serde_json::Value>) -> Option<Vec<String>> {
    let v = value.as_ref()?;
    match v {
        serde_json::Value::String(s) => Some(vec![s.clone()]),
        serde_json::Value::Array(arr) => Some(
            arr.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

fn resolve_cwd(user_cwd: Option<&str>) -> Result<PathBuf, ThreadError> {
    match user_cwd {
        Some(raw) if !raw.is_empty() => {
            let path = PathBuf::from(raw);
            if path.is_absolute() {
                Ok(path)
            } else {
                std::env::current_dir()
                    .map(|d| d.join(path))
                    .map_err(|e| ThreadError::InvalidParams(e.to_string()))
            }
        }
        _ => std::env::current_dir().map_err(|e| ThreadError::InvalidParams(e.to_string())),
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn default_service_tier() -> p::ServiceTier {
    serde_json::Value::Null
}

fn default_permission_profile() -> p::PermissionProfile {
    serde_json::json!({
        "type": "dangerFullAccess"
    })
}

fn sandbox_value(mode: Option<p::SandboxMode>) -> p::SandboxPolicy {
    match mode {
        Some(p::SandboxMode::ReadOnly) => serde_json::json!({ "type": "readOnly" }),
        Some(p::SandboxMode::WorkspaceWrite) => serde_json::json!({ "type": "workspaceWrite" }),
        Some(p::SandboxMode::DangerFullAccess) => serde_json::json!({ "type": "dangerFullAccess" }),
        None => serde_json::json!({ "type": "dangerFullAccess" }),
    }
}

fn effort_from_params(
    additional: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<p::ReasoningEffort> {
    additional
        .get("reasoningEffort")
        .or_else(|| additional.get("effort"))
        .and_then(parse_effort)
}

fn parse_effort(value: &serde_json::Value) -> Option<p::ReasoningEffort> {
    match value.as_str()? {
        "minimal" => Some(p::ReasoningEffort::Minimal),
        "low" => Some(p::ReasoningEffort::Low),
        "medium" => Some(p::ReasoningEffort::Medium),
        "high" => Some(p::ReasoningEffort::High),
        _ => None,
    }
}
