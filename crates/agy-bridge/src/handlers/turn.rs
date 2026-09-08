//! `turn/*` request handlers and background event pump for Google Antigravity CLI (`agy`).

use std::sync::Arc;
use std::time::SystemTime;

use alleycat_codex_proto as p;
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::pool::AgyProcessHandle;
use crate::pool::agy_protocol::AgyOutbound;
use crate::state::{ConnectionState, RecordedTurn};
use crate::translate::events::EventTranslatorState;
use crate::translate::input::translate_user_input;

#[derive(Debug, Error)]
pub enum TurnError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("thread `{0}` is not loaded; call thread/start or thread/resume first")]
    ThreadNotLoaded(String),
    #[error("input translation failed: {0}")]
    InputTranslation(String),
    #[error("agy process error: {0}")]
    AgyProcess(String),
}

impl TurnError {
    pub fn rpc_code(&self) -> i64 {
        match self {
            TurnError::InvalidParams(_) | TurnError::ThreadNotLoaded(_) | TurnError::InputTranslation(_) => {
                p::error_codes::INVALID_PARAMS
            }
            TurnError::AgyProcess(_) => p::error_codes::INTERNAL_ERROR,
        }
    }
}

pub async fn handle_turn_start(
    state: &Arc<ConnectionState>,
    params: p::TurnStartParams,
) -> Result<p::TurnStartResponse, TurnError> {
    let handle = state
        .agy_pool()
        .get(&params.thread_id)
        .await
        .ok_or_else(|| TurnError::ThreadNotLoaded(params.thread_id.clone()))?;

    let prompt = translate_user_input(&params.input)
        .map_err(|e| TurnError::InputTranslation(e.to_string()))?;

    let turn_id = Uuid::now_v7().to_string();
    let started_at = now_unix_millis();

    let _ = state.agy_pool().mark_active(&params.thread_id).await;
    let events_rx = handle.subscribe();

    handle
        .send_prompt(&prompt)
        .map_err(|e| TurnError::AgyProcess(e.to_string()))?;

    let turn = p::Turn {
        id: turn_id.clone(),
        items: Vec::new(),
        items_view: p::default_items_view(),
        status: p::TurnStatus::InProgress,
        error: None,
        started_at: Some(started_at),
        completed_at: None,
        duration_ms: None,
    };

    if state.should_emit("turn/started") {
        let frame = notification_frame(p::ServerNotification::TurnStarted(
            p::TurnStartedNotification {
                thread_id: params.thread_id.clone(),
                turn: turn.clone(),
            },
        ));
        state.send(frame);
    }

    let cwd = handle.cwd().to_string_lossy().to_string();

    tokio::spawn(run_event_pump(
        Arc::clone(state),
        params.thread_id,
        turn_id,
        cwd,
        handle,
        events_rx,
        started_at,
        params.input,
    ));

    Ok(p::TurnStartResponse { turn })
}

pub async fn handle_turn_steer(
    state: &Arc<ConnectionState>,
    params: p::TurnSteerParams,
) -> Result<p::TurnSteerResponse, TurnError> {
    let handle = state
        .agy_pool()
        .get(&params.thread_id)
        .await
        .ok_or_else(|| TurnError::ThreadNotLoaded(params.thread_id.clone()))?;

    let prompt = translate_user_input(&params.input)
        .map_err(|e| TurnError::InputTranslation(e.to_string()))?;

    handle
        .send_prompt(&prompt)
        .map_err(|e| TurnError::AgyProcess(e.to_string()))?;

    Ok(p::TurnSteerResponse {
        turn_id: params.expected_turn_id,
    })
}

pub async fn handle_turn_interrupt(
    state: &Arc<ConnectionState>,
    params: p::TurnInterruptParams,
) -> Result<p::TurnInterruptResponse, TurnError> {
    if let Some(handle) = state.agy_pool().get(&params.thread_id).await {
        handle.interrupt().await;
    }
    state.agy_pool().release(&params.thread_id).await;
    Ok(p::TurnInterruptResponse::default())
}

async fn run_event_pump(
    state: Arc<ConnectionState>,
    thread_id: String,
    turn_id: String,
    cwd: String,
    _handle: Arc<AgyProcessHandle>,
    mut events_rx: broadcast::Receiver<AgyOutbound>,
    started_at: i64,
    user_input: Vec<p::UserInput>,
) {
    let mut translator = EventTranslatorState::new(thread_id.clone(), turn_id.clone(), cwd);
    let mut recorded_items = vec![p::ThreadItem::UserMessage {
        id: Uuid::now_v7().to_string(),
        content: user_input,
    }];
    let mut terminal_seen = false;
    let mut final_turn_status = p::TurnStatus::Completed;
    let mut final_error = None;

    loop {
        let event = match events_rx.recv().await {
            Ok(ev) => ev,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(thread_id = %thread_id, turn_id = %turn_id, "agy event pump lagged by {n}");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::debug!(thread_id = %thread_id, "agy stdout closed");
                break;
            }
        };

        if matches!(event, AgyOutbound::Result { .. }) {
            terminal_seen = true;
        }

        let notifications = translator.translate(event);
        for notif in notifications {
            if let p::ServerNotification::ItemCompleted(ref n) = notif {
                recorded_items.push(n.item.clone());
            }
            if let p::ServerNotification::TurnCompleted(ref n) = notif {
                final_turn_status = n.turn.status;
                final_error = n.turn.error.clone();
            }

            let method = notif_method(&notif);
            if state.should_emit(method) {
                state.send(notification_frame(notif));
            }
        }

        if terminal_seen {
            break;
        }
    }

    let completed_at = now_unix_millis();
    let duration_ms = completed_at.saturating_sub(started_at);

    state.record_turn(
        &thread_id,
        RecordedTurn {
            turn_id: turn_id.clone(),
            started_at,
            completed_at: Some(completed_at),
            status: final_turn_status,
            error: final_error,
            items: recorded_items,
        },
    );

    state.agy_pool().mark_idle(&thread_id).await;

    if !terminal_seen && state.should_emit("turn/completed") {
        let turn = p::Turn {
            id: turn_id,
            items: Vec::new(),
            items_view: p::default_items_view(),
            status: p::TurnStatus::Completed,
            error: None,
            started_at: Some(started_at),
            completed_at: Some(completed_at),
            duration_ms: Some(duration_ms),
        };
        state.send(notification_frame(p::ServerNotification::TurnCompleted(
            p::TurnCompletedNotification {
                thread_id,
                turn,
            },
        )));
    }
}

fn notification_frame(notif: p::ServerNotification) -> p::JsonRpcMessage {
    let value = serde_json::to_value(&notif).expect("ServerNotification serializes");
    let method = value
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let params = value.get("params").cloned();
    p::JsonRpcMessage::Notification(p::JsonRpcNotification {
        jsonrpc: p::JsonRpcVersion,
        method,
        params,
    })
}

fn notif_method(notif: &p::ServerNotification) -> &'static str {
    match notif {
        p::ServerNotification::ItemStarted(_) => "item/started",
        p::ServerNotification::ItemCompleted(_) => "item/completed",
        p::ServerNotification::AgentMessageDelta(_) => "item/agentMessage/delta",
        p::ServerNotification::CommandExecutionOutputDelta(_) => "item/commandExecution/outputDelta",
        p::ServerNotification::ThreadTokenUsageUpdated(_) => "thread/tokenUsage/updated",
        p::ServerNotification::TurnStarted(_) => "turn/started",
        p::ServerNotification::TurnCompleted(_) => "turn/completed",
        _ => "notification",
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
