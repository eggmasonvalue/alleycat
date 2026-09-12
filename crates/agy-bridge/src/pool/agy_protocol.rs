//! Serde mirror of Google Antigravity CLI (`agy`) wire protocol.
//!
//! Invocation:
//! ```bash
//! agy --input-format stream-json --output-format stream-json --dangerously-skip-permissions
//! ```
//!
//! Inbound (stdin):
//! One NDJSON object per turn: `{"event": "user", "message": {"content": "..."}}`.
//!
//! Outbound (stdout):
//! NDJSON objects tagged by `"event"`:
//! - `"init"`: Handshake readiness payload containing `conversation_id`, tools, cwd.
//! - `"step_update"`: State transitions & delta streaming (user input, agent response,
//!   tool execution, subagent calls).
//! - `"result"`: Turn completion containing final response, duration, and token usage.

use alleycat_codex_proto::TokenUsageBreakdown;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Inbound: bridge → agy on stdin
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgyInbound {
    pub event: String,
    pub message: AgyUserMessage,
}

impl AgyInbound {
    pub fn user_prompt(content: impl Into<String>) -> Self {
        Self {
            event: "user".to_string(),
            message: AgyUserMessage {
                content: content.into(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgyUserMessage {
    pub content: String,
}

// ============================================================================
// Outbound: agy → bridge on stdout
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event")]
pub enum AgyOutbound {
    #[serde(rename = "init")]
    Init {
        conversation_id: String,
        init: AgyInitData,
    },
    #[serde(rename = "step_update")]
    StepUpdate {
        step_update: AgyStepUpdate,
    },
    #[serde(rename = "result")]
    Result {
        result: AgyResult,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgyInitData {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgyStepUpdate {
    pub conversation_id: String,
    pub step_index: u32,
    pub state: String, // "ACTIVE" | "DONE"
    pub step_type: String, // "user_input" | "agent_response" | "tool" | "subagent" | "system_message"
    #[serde(default)]
    pub text_delta: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_info: Option<Value>,
    #[serde(default)]
    pub subagent_info: Option<AgySubagentInfoWrapper>,
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    #[serde(default)]
    pub usage: Option<AgyUsage>,
}

impl AgyStepUpdate {
    pub fn is_active(&self) -> bool {
        self.state.eq_ignore_ascii_case("ACTIVE")
    }

    pub fn is_done(&self) -> bool {
        self.state.eq_ignore_ascii_case("DONE")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgySubagentInfoWrapper {
    #[serde(default)]
    pub subagents: Vec<AgySubagentEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgySubagentEntry {
    #[serde(default)]
    pub type_name: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub initial_prompt: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub log_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AgyUsage {
    #[serde(default)]
    pub input_tokens: i64,
    #[serde(default)]
    pub output_tokens: i64,
    #[serde(default)]
    pub thinking_tokens: i64,
    #[serde(default)]
    pub cache_read_tokens: i64,
    #[serde(default)]
    pub total_tokens: i64,
}

impl AgyUsage {
    pub fn to_token_breakdown(&self) -> TokenUsageBreakdown {
        TokenUsageBreakdown {
            total_tokens: if self.total_tokens > 0 {
                self.total_tokens
            } else {
                self.input_tokens + self.output_tokens
            },
            input_tokens: self.input_tokens,
            cached_input_tokens: self.cache_read_tokens,
            output_tokens: self.output_tokens,
            reasoning_output_tokens: self.thinking_tokens,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgyResult {
    pub conversation_id: String,
    pub status: String, // "SUCCESS" | "ERROR"
    #[serde(default)]
    pub response: Option<String>,
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    #[serde(default)]
    pub num_turns: Option<u32>,
    #[serde(default)]
    pub usage: Option<AgyUsage>,
    #[serde(default)]
    pub error: Option<Value>,
}

impl AgyResult {
    pub fn is_success(&self) -> bool {
        self.status.eq_ignore_ascii_case("SUCCESS")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_init_event() {
        let json = r#"{"event":"init","conversation_id":"fa9c6509-0bb2-4f8b-8351-b","init":{"cwd":"/home/user/src","tools":["run_command","search_web"],"permission_mode":"always-proceed"}}"#;
        let event: AgyOutbound = serde_json::from_str(json).expect("deserialize init");
        match event {
            AgyOutbound::Init { conversation_id, init } => {
                assert_eq!(conversation_id, "fa9c6509-0bb2-4f8b-8351-b");
                assert_eq!(init.cwd.as_deref(), Some("/home/user/src"));
                assert_eq!(init.tools.len(), 2);
                assert_eq!(init.permission_mode.as_deref(), Some("always-proceed"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_step_update_agent_response() {
        let json = r#"{"event":"step_update","step_update":{"conversation_id":"fa9","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"HELLO\n","duration_seconds":1.888,"usage":{"input_tokens":5762,"output_tokens":22,"thinking_tokens":21,"cache_read_tokens":8131,"total_tokens":5784}}}"#;
        let event: AgyOutbound = serde_json::from_str(json).expect("deserialize step_update");
        match event {
            AgyOutbound::StepUpdate { step_update } => {
                assert_eq!(step_update.conversation_id, "fa9");
                assert_eq!(step_update.step_index, 1);
                assert!(step_update.is_done());
                assert_eq!(step_update.step_type, "agent_response");
                assert_eq!(step_update.text_delta.as_deref(), Some("HELLO\n"));
                let usage = step_update.usage.expect("usage present");
                assert_eq!(usage.thinking_tokens, 21);
                let breakdown = usage.to_token_breakdown();
                assert_eq!(breakdown.reasoning_output_tokens, 21);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_subagent_step_update() {
        let json = r#"{"event":"step_update","step_update":{"conversation_id":"fa9","step_index":2,"state":"DONE","step_type":"subagent","tool_name":"invoke_subagent","subagent_info":{"subagents":[{"type_name":"self","role":"Math Calculator","initial_prompt":"calc 42*42","conversation_id":"child-123","log_uri":"file:///path"}]}}}"#;
        let event: AgyOutbound = serde_json::from_str(json).expect("deserialize subagent");
        match event {
            AgyOutbound::StepUpdate { step_update } => {
                assert_eq!(step_update.step_type, "subagent");
                let subagents = step_update.subagent_info.expect("subagents").subagents;
                assert_eq!(subagents.len(), 1);
                assert_eq!(subagents[0].role.as_deref(), Some("Math Calculator"));
                assert_eq!(subagents[0].conversation_id.as_deref(), Some("child-123"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_result_event() {
        let json = r#"{"event":"result","result":{"conversation_id":"fa9","status":"SUCCESS","response":"HELLO\n","duration_seconds":1.97,"num_turns":1,"usage":{"input_tokens":5762,"output_tokens":22,"thinking_tokens":21,"cache_read_tokens":8131,"total_tokens":5784}}}"#;
        let event: AgyOutbound = serde_json::from_str(json).expect("deserialize result");
        match event {
            AgyOutbound::Result { result } => {
                assert!(result.is_success());
                assert_eq!(result.response.as_deref(), Some("HELLO\n"));
                assert_eq!(result.num_turns, Some(1));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
