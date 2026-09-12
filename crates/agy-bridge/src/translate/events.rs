//! Translates `agy` NDJSON stream events into standard Codex JSON-RPC v2 notifications.

use std::collections::HashMap;

use alleycat_codex_proto::{
    AgentMessageDeltaNotification, CollabAgentState, CollabAgentStatus, CollabAgentTool,
    CollabAgentToolCallStatus, CommandExecutionOutputDeltaNotification, CommandExecutionSource,
    CommandExecutionStatus, FileUpdateChange, ItemCompletedNotification, ItemStartedNotification,
    PatchApplyStatus, PatchChangeKind, ServerNotification, ThreadItem, ThreadTokenUsage,
    ThreadTokenUsageUpdatedNotification, TokenUsageBreakdown, Turn, TurnCompletedNotification,
    TurnError, TurnStatus,
};
use uuid::Uuid;

use crate::pool::agy_protocol::{AgyOutbound, AgyResult, AgyStepUpdate};

#[derive(Debug, Clone)]
struct OpenTool {
    item_id: String,
    tool_name: String,
    command_or_path: String,
}

#[derive(Debug, Clone)]
struct OpenSubagent {
    item_id: String,
    initial_prompt: Option<String>,
}

pub struct EventTranslatorState {
    thread_id: String,
    turn_id: String,
    cwd: String,

    open_agent_message: Option<(String, String)>,
    open_tools: HashMap<u32, OpenTool>,
    open_subagents: HashMap<u32, OpenSubagent>,

    last_token_usage: Option<TokenUsageBreakdown>,
    cumulative_token_usage: TokenUsageBreakdown,
}

impl EventTranslatorState {
    pub fn new(thread_id: String, turn_id: String, cwd: String) -> Self {
        Self {
            thread_id,
            turn_id,
            cwd,
            open_agent_message: None,
            open_tools: HashMap::new(),
            open_subagents: HashMap::new(),
            last_token_usage: None,
            cumulative_token_usage: TokenUsageBreakdown::default(),
        }
    }

    pub fn translate(&mut self, event: AgyOutbound) -> Vec<ServerNotification> {
        match event {
            AgyOutbound::Init { .. } => Vec::new(),
            AgyOutbound::StepUpdate { step_update } => self.translate_step_update(step_update),
            AgyOutbound::Result { result } => self.translate_result(result),
            AgyOutbound::Unknown => Vec::new(),
        }
    }

    fn translate_step_update(&mut self, update: AgyStepUpdate) -> Vec<ServerNotification> {
        let mut out = Vec::new();

        match update.step_type.as_str() {
            "agent_response" => {
                // If this is the start of an assistant message block, emit item/started
                if self.open_agent_message.is_none() {
                    let item_id = Uuid::now_v7().to_string();
                    self.open_agent_message = Some((item_id.clone(), String::new()));
                    out.push(ServerNotification::ItemStarted(ItemStartedNotification {
                        item: ThreadItem::AgentMessage {
                            id: item_id,
                            text: String::new(),
                            phase: None,
                            memory_citation: None,
                        },
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        parent_item_id: None,
                    }));
                }

                if let Some((ref item_id, ref mut acc)) = self.open_agent_message {
                    if let Some(ref delta) = update.text_delta {
                        if !delta.is_empty() {
                            acc.push_str(delta);
                            out.push(ServerNotification::AgentMessageDelta(
                                AgentMessageDeltaNotification {
                                    thread_id: self.thread_id.clone(),
                                    turn_id: self.turn_id.clone(),
                                    item_id: item_id.clone(),
                                    delta: delta.clone(),
                                    parent_item_id: None,
                                },
                            ));
                        }
                    }
                }

                if update.is_done() {
                    if let Some((item_id, acc)) = self.open_agent_message.take() {
                        out.push(ServerNotification::ItemCompleted(
                            ItemCompletedNotification {
                                item: ThreadItem::AgentMessage {
                                    id: item_id,
                                    text: acc,
                                    phase: None,
                                    memory_citation: None,
                                },
                                thread_id: self.thread_id.clone(),
                                turn_id: self.turn_id.clone(),
                                parent_item_id: None,
                            },
                        ));
                    }

                    if let Some(ref usage) = update.usage {
                        let breakdown = usage.to_token_breakdown();
                        self.last_token_usage = Some(breakdown.clone());
                        self.cumulative_token_usage = breakdown.clone();
                        out.push(ServerNotification::ThreadTokenUsageUpdated(
                            ThreadTokenUsageUpdatedNotification {
                                thread_id: self.thread_id.clone(),
                                turn_id: self.turn_id.clone(),
                                token_usage: ThreadTokenUsage {
                                    total: breakdown.clone(),
                                    last: breakdown,
                                    model_context_window: None,
                                },
                            },
                        ));
                    }
                }
            }

            "tool" => {
                let tool_name = update.tool_name.clone().unwrap_or_else(|| "tool".to_string());
                let idx = update.step_index;

                if update.is_active() {
                    let item_id = Uuid::now_v7().to_string();
                    let (item, cmd_or_path) = match tool_name.as_str() {
                        "run_command" => {
                            let cmd = update
                                .tool_info
                                .as_ref()
                                .and_then(|info| info.get("parameters"))
                                .and_then(|p| p.get("CommandLine"))
                                .and_then(|c| c.as_str())
                                .unwrap_or("")
                                .to_string();
                            let item = ThreadItem::CommandExecution {
                                id: item_id.clone(),
                                command: cmd.clone(),
                                cwd: self.cwd.clone(),
                                process_id: None,
                                source: CommandExecutionSource::Agent,
                                status: CommandExecutionStatus::InProgress,
                                command_actions: Vec::new(),
                                aggregated_output: None,
                                exit_code: None,
                                duration_ms: None,
                            };
                            (item, cmd)
                        }
                        "write_to_file" | "replace_file_content" | "multi_replace_file_content" => {
                            let path = update
                                .tool_info
                                .as_ref()
                                .and_then(|info| info.get("parameters"))
                                .and_then(|p| p.get("TargetFile"))
                                .and_then(|c| c.as_str())
                                .unwrap_or("")
                                .to_string();
                            let item = ThreadItem::FileChange {
                                id: item_id.clone(),
                                changes: vec![FileUpdateChange {
                                    path: path.clone(),
                                    kind: PatchChangeKind::Update { move_path: None },
                                    diff: String::new(),
                                }],
                                status: PatchApplyStatus::InProgress,
                            };
                            (item, path)
                        }
                        "search_web" => {
                            let query = update
                                .tool_info
                                .as_ref()
                                .and_then(|info| info.get("parameters"))
                                .and_then(|p| p.get("Query"))
                                .and_then(|c| c.as_str())
                                .unwrap_or("")
                                .to_string();
                            let item = ThreadItem::WebSearch {
                                id: item_id.clone(),
                                query: query.clone(),
                                action: None,
                            };
                            (item, query)
                        }
                        _ => {
                            let item = ThreadItem::CommandExecution {
                                id: item_id.clone(),
                                command: tool_name.clone(),
                                cwd: self.cwd.clone(),
                                process_id: None,
                                source: CommandExecutionSource::Agent,
                                status: CommandExecutionStatus::InProgress,
                                command_actions: Vec::new(),
                                aggregated_output: None,
                                exit_code: None,
                                duration_ms: None,
                            };
                            (item, tool_name.clone())
                        }
                    };

                    self.open_tools.insert(
                        idx,
                        OpenTool {
                            item_id: item_id.clone(),
                            tool_name,
                            command_or_path: cmd_or_path,
                        },
                    );

                    out.push(ServerNotification::ItemStarted(ItemStartedNotification {
                        item,
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        parent_item_id: None,
                    }));
                } else if update.is_done() {
                    let open_tool = self.open_tools.remove(&idx);
                    let item_id = open_tool
                        .as_ref()
                        .map(|t| t.item_id.clone())
                        .unwrap_or_else(|| Uuid::now_v7().to_string());
                    let tool_name = open_tool
                        .as_ref()
                        .map(|t| t.tool_name.clone())
                        .unwrap_or(tool_name);
                    let cmd_or_path = open_tool
                        .map(|t| t.command_or_path)
                        .unwrap_or_default();

                    let duration_ms = update
                        .duration_seconds
                        .map(|s| (s * 1000.0) as i64);

                    let item = match tool_name.as_str() {
                        "run_command" => {
                            let output = update
                                .tool_info
                                .as_ref()
                                .and_then(|info| info.get("output"))
                                .and_then(|o| o.as_str())
                                .map(str::to_string);

                            if let Some(ref out_text) = output {
                                out.push(ServerNotification::CommandExecutionOutputDelta(
                                    CommandExecutionOutputDeltaNotification {
                                        thread_id: self.thread_id.clone(),
                                        turn_id: self.turn_id.clone(),
                                        item_id: item_id.clone(),
                                        delta: out_text.clone(),
                                        parent_item_id: None,
                                    },
                                ));
                            }

                            ThreadItem::CommandExecution {
                                id: item_id.clone(),
                                command: cmd_or_path,
                                cwd: self.cwd.clone(),
                                process_id: None,
                                source: CommandExecutionSource::Agent,
                                status: CommandExecutionStatus::Completed,
                                command_actions: Vec::new(),
                                aggregated_output: output,
                                exit_code: Some(0),
                                duration_ms,
                            }
                        }
                        "write_to_file" | "replace_file_content" | "multi_replace_file_content" => {
                            ThreadItem::FileChange {
                                id: item_id.clone(),
                                changes: vec![FileUpdateChange {
                                    path: cmd_or_path,
                                    kind: PatchChangeKind::Update { move_path: None },
                                    diff: String::new(),
                                }],
                                status: PatchApplyStatus::Completed,
                            }
                        }
                        "search_web" => ThreadItem::WebSearch {
                            id: item_id.clone(),
                            query: cmd_or_path,
                            action: None,
                        },
                        _ => ThreadItem::CommandExecution {
                            id: item_id.clone(),
                            command: cmd_or_path,
                            cwd: self.cwd.clone(),
                            process_id: None,
                            source: CommandExecutionSource::Agent,
                            status: CommandExecutionStatus::Completed,
                            command_actions: Vec::new(),
                            aggregated_output: None,
                            exit_code: Some(0),
                            duration_ms,
                        },
                    };

                    out.push(ServerNotification::ItemCompleted(
                        ItemCompletedNotification {
                            item,
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: None,
                        },
                    ));
                }
            }

            "subagent" => {
                let idx = update.step_index;
                let subagents = update
                    .subagent_info
                    .as_ref()
                    .map(|w| w.subagents.clone())
                    .unwrap_or_default();

                if update.is_active() {
                    let item_id = Uuid::now_v7().to_string();
                    let prompt = subagents
                        .first()
                        .and_then(|s| s.initial_prompt.clone().or_else(|| s.role.clone()));

                    self.open_subagents.insert(
                        idx,
                        OpenSubagent {
                            item_id: item_id.clone(),
                            initial_prompt: prompt.clone(),
                        },
                    );

                    out.push(ServerNotification::ItemStarted(ItemStartedNotification {
                        item: ThreadItem::CollabAgentToolCall {
                            id: item_id,
                            tool: CollabAgentTool::SpawnAgent,
                            status: CollabAgentToolCallStatus::InProgress,
                            sender_thread_id: self.thread_id.clone(),
                            receiver_thread_ids: Vec::new(),
                            prompt,
                            model: None,
                            reasoning_effort: None,
                            agents_states: HashMap::new(),
                        },
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        parent_item_id: None,
                    }));
                } else if update.is_done() {
                    let open_sub = self.open_subagents.remove(&idx);
                    let item_id = open_sub
                        .as_ref()
                        .map(|s| s.item_id.clone())
                        .unwrap_or_else(|| Uuid::now_v7().to_string());
                    let prompt = open_sub.and_then(|s| s.initial_prompt);

                    let mut receiver_thread_ids = Vec::new();
                    let mut agents_states = HashMap::new();

                    for sub in subagents {
                        if let Some(conv_id) = sub.conversation_id {
                            receiver_thread_ids.push(conv_id.clone());
                            agents_states.insert(
                                conv_id,
                                CollabAgentState {
                                    status: CollabAgentStatus::Completed,
                                    message: None,
                                },
                            );
                        }
                    }

                    out.push(ServerNotification::ItemCompleted(
                        ItemCompletedNotification {
                            item: ThreadItem::CollabAgentToolCall {
                                id: item_id,
                                tool: CollabAgentTool::SpawnAgent,
                                status: CollabAgentToolCallStatus::Completed,
                                sender_thread_id: self.thread_id.clone(),
                                receiver_thread_ids,
                                prompt,
                                model: None,
                                reasoning_effort: None,
                                agents_states,
                            },
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: None,
                        },
                    ));
                }
            }

            _ => {}
        }

        out
    }

    fn translate_result(&mut self, result: AgyResult) -> Vec<ServerNotification> {
        let mut out = Vec::new();

        // If an agent message was still open, complete it
        if let Some((item_id, acc)) = self.open_agent_message.take() {
            out.push(ServerNotification::ItemCompleted(
                ItemCompletedNotification {
                    item: ThreadItem::AgentMessage {
                        id: item_id,
                        text: acc,
                        phase: None,
                        memory_citation: None,
                    },
                    thread_id: self.thread_id.clone(),
                    turn_id: self.turn_id.clone(),
                    parent_item_id: None,
                },
            ));
        }

        if let Some(ref usage) = result.usage {
            let breakdown = usage.to_token_breakdown();
            self.last_token_usage = Some(breakdown.clone());
            self.cumulative_token_usage = breakdown.clone();
            out.push(ServerNotification::ThreadTokenUsageUpdated(
                ThreadTokenUsageUpdatedNotification {
                    thread_id: self.thread_id.clone(),
                    turn_id: self.turn_id.clone(),
                    token_usage: ThreadTokenUsage {
                        total: breakdown.clone(),
                        last: breakdown,
                        model_context_window: None,
                    },
                },
            ));
        }

        let is_success = result.is_success();
        let is_interrupted = result.error.as_ref().map_or(false, |e| {
            let s = match e {
                serde_json::Value::String(s) => s.as_str(),
                _ => "",
            };
            let lower = s.to_lowercase();
            lower.contains("canceled") || lower.contains("cancelled") || lower.contains("interrupt")
        });

        let status = if is_interrupted {
            TurnStatus::Interrupted
        } else if is_success {
            TurnStatus::Completed
        } else {
            TurnStatus::Failed
        };

        let turn = Turn {
            id: self.turn_id.clone(),
            items: Vec::new(),
            items_view: alleycat_codex_proto::default_items_view(),
            status,
            error: if is_success {
                None
            } else {
                let error_message = result
                    .error
                    .as_ref()
                    .and_then(|v| match v {
                        serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
                        other if !other.is_null() => Some(other.to_string()),
                        _ => None,
                    })
                    .or_else(|| result.response.filter(|r| !r.is_empty()))
                    .unwrap_or_else(|| {
                        if is_interrupted {
                            "turn interrupted".to_string()
                        } else {
                            "agy turn failed".to_string()
                        }
                    });

                Some(TurnError {
                    message: error_message,
                    codex_error_info: None,
                    additional_details: None,
                })
            },
            started_at: None,
            completed_at: None,
            duration_ms: result.duration_seconds.map(|s| (s * 1000.0) as i64),
        };

        out.push(ServerNotification::TurnCompleted(
            TurnCompletedNotification {
                thread_id: self.thread_id.clone(),
                turn,
            },
        ));

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::agy_protocol::{AgySubagentEntry, AgySubagentInfoWrapper, AgyUsage};

    #[test]
    fn translates_agent_response_stream() {
        let mut translator = EventTranslatorState::new(
            "th-1".to_string(),
            "turn-1".to_string(),
            "/cwd".to_string(),
        );

        let active = AgyOutbound::StepUpdate {
            step_update: AgyStepUpdate {
                conversation_id: "th-1".to_string(),
                step_index: 1,
                state: "ACTIVE".to_string(),
                step_type: "agent_response".to_string(),
                text_delta: Some("Hello ".to_string()),
                tool_name: None,
                tool_info: None,
                subagent_info: None,
                duration_seconds: None,
                usage: None,
            },
        };

        let notifs = translator.translate(active);
        assert_eq!(notifs.len(), 2);
        assert!(matches!(notifs[0], ServerNotification::ItemStarted(_)));
        assert!(matches!(notifs[1], ServerNotification::AgentMessageDelta(_)));

        let done = AgyOutbound::StepUpdate {
            step_update: AgyStepUpdate {
                conversation_id: "th-1".to_string(),
                step_index: 1,
                state: "DONE".to_string(),
                step_type: "agent_response".to_string(),
                text_delta: Some("world!\n".to_string()),
                tool_name: None,
                tool_info: None,
                subagent_info: None,
                duration_seconds: Some(1.2),
                usage: Some(AgyUsage {
                    input_tokens: 100,
                    output_tokens: 10,
                    thinking_tokens: 5,
                    cache_read_tokens: 0,
                    total_tokens: 110,
                }),
            },
        };

        let notifs = translator.translate(done);
        assert_eq!(notifs.len(), 3);
        assert!(matches!(notifs[0], ServerNotification::AgentMessageDelta(_)));
        assert!(matches!(notifs[1], ServerNotification::ItemCompleted(_)));
        assert!(matches!(notifs[2], ServerNotification::ThreadTokenUsageUpdated(_)));
    }

    #[test]
    fn translates_subagent_tool_lifecycle() {
        let mut translator = EventTranslatorState::new(
            "th-1".to_string(),
            "turn-1".to_string(),
            "/cwd".to_string(),
        );

        let active = AgyOutbound::StepUpdate {
            step_update: AgyStepUpdate {
                conversation_id: "th-1".to_string(),
                step_index: 2,
                state: "ACTIVE".to_string(),
                step_type: "subagent".to_string(),
                text_delta: None,
                tool_name: Some("invoke_subagent".to_string()),
                tool_info: None,
                subagent_info: Some(AgySubagentInfoWrapper {
                    subagents: vec![AgySubagentEntry {
                        type_name: Some("self".to_string()),
                        role: Some("Worker".to_string()),
                        initial_prompt: Some("Do math".to_string()),
                        conversation_id: None,
                        log_uri: None,
                    }],
                }),
                duration_seconds: None,
                usage: None,
            },
        };

        let notifs = translator.translate(active);
        assert_eq!(notifs.len(), 1);
        match &notifs[0] {
            ServerNotification::ItemStarted(s) => match &s.item {
                ThreadItem::CollabAgentToolCall { prompt, .. } => {
                    assert_eq!(prompt.as_deref(), Some("Do math"));
                }
                other => panic!("expected CollabAgentToolCall, got {other:?}"),
            },
            other => panic!("unexpected notification: {other:?}"),
        }

        let done = AgyOutbound::StepUpdate {
            step_update: AgyStepUpdate {
                conversation_id: "th-1".to_string(),
                step_index: 2,
                state: "DONE".to_string(),
                step_type: "subagent".to_string(),
                text_delta: None,
                tool_name: Some("invoke_subagent".to_string()),
                tool_info: None,
                subagent_info: Some(AgySubagentInfoWrapper {
                    subagents: vec![AgySubagentEntry {
                        type_name: Some("self".to_string()),
                        role: Some("Worker".to_string()),
                        initial_prompt: Some("Do math".to_string()),
                        conversation_id: Some("child-uuid-999".to_string()),
                        log_uri: None,
                    }],
                }),
                duration_seconds: Some(0.5),
                usage: None,
            },
        };

        let notifs = translator.translate(done);
        assert_eq!(notifs.len(), 1);
        match &notifs[0] {
            ServerNotification::ItemCompleted(c) => match &c.item {
                ThreadItem::CollabAgentToolCall {
                    receiver_thread_ids,
                    status,
                    ..
                } => {
                    assert_eq!(status, &CollabAgentToolCallStatus::Completed);
                    assert_eq!(receiver_thread_ids, &vec!["child-uuid-999".to_string()]);
                }
                other => panic!("expected CollabAgentToolCall, got {other:?}"),
            },
            other => panic!("unexpected notification: {other:?}"),
        }
    }
}
