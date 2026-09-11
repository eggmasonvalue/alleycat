//! Per-connection state for `agy-bridge`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use alleycat_bridge_core::ProcessLauncher;
use alleycat_bridge_core::session::Session;

use alleycat_codex_proto::{
    ApprovalsReviewer, AskForApproval, InitializeCapabilities, JsonRpcMessage, ReasoningEffort,
    SandboxMode, ThreadItem, TurnError, TurnStatus,
};
use serde::{Deserialize, Serialize};

use crate::handlers::model::DiscoveredModel;
use crate::index::AgySessionRef;
use crate::pool::AgyPool;

pub trait ThreadIndexHandle: alleycat_bridge_core::ThreadIndexHandle<AgySessionRef> {}

impl<T> ThreadIndexHandle for T where
    T: alleycat_bridge_core::ThreadIndexHandle<AgySessionRef> + ?Sized
{
}

pub use crate::index::{IndexEntry, ListFilter, ListPage, ListSort};

pub struct ConnectionState {
    defaults: Mutex<ThreadDefaults>,
    session: Arc<Session>,
    agy_pool: Arc<AgyPool>,
    thread_index: Arc<dyn ThreadIndexHandle>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    trust_persisted_cwd: bool,
    codex_home: PathBuf,
    cached_models: Mutex<Option<Vec<DiscoveredModel>>>,
    cached_agents: Mutex<Option<Vec<String>>>,
    thread_logs: Mutex<HashMap<String, Vec<RecordedTurn>>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedTurn {
    pub turn_id: String,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub status: TurnStatus,
    pub error: Option<TurnError>,
    pub items: Vec<ThreadItem>,
}

#[derive(Debug, Clone, Default)]
pub struct ThreadDefaults {
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub approval_policy: Option<AskForApproval>,
    pub approvals_reviewer: Option<ApprovalsReviewer>,
    pub sandbox: Option<SandboxMode>,
    pub service_name: Option<String>,
    pub system_prompt: Option<String>,
}

impl ConnectionState {
    pub fn new(
        session: Arc<Session>,
        agy_pool: Arc<AgyPool>,
        thread_index: Arc<dyn ThreadIndexHandle>,
        defaults: ThreadDefaults,
        codex_home: PathBuf,
    ) -> Self {
        Self {
            defaults: Mutex::new(defaults),
            session,
            agy_pool,
            thread_index,
            launcher: None,
            trust_persisted_cwd: false,
            codex_home,
            cached_models: Mutex::new(None),
            cached_agents: Mutex::new(None),
            thread_logs: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_launcher(
        session: Arc<Session>,
        agy_pool: Arc<AgyPool>,
        thread_index: Arc<dyn ThreadIndexHandle>,
        defaults: ThreadDefaults,
        launcher: Option<Arc<dyn ProcessLauncher>>,
        trust_persisted_cwd: bool,
        codex_home: PathBuf,
        seed_models: Option<Vec<DiscoveredModel>>,
        seed_agents: Option<Vec<String>>,
    ) -> Self {
        Self {
            defaults: Mutex::new(defaults),
            session,
            agy_pool,
            thread_index,
            launcher,
            trust_persisted_cwd,
            codex_home,
            cached_models: Mutex::new(seed_models),
            cached_agents: Mutex::new(seed_agents),
            thread_logs: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_capabilities(
        &self,
        client_name: Option<String>,
        client_title: Option<String>,
        client_version: Option<String>,
        caps: Option<&InitializeCapabilities>,
    ) {
        let opt_out = caps
            .and_then(|c| c.opt_out_notification_methods.as_ref())
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();
        self.session.set_capabilities(alleycat_bridge_core::state::Capabilities {
            experimental_api: caps.is_some_and(|c| c.experimental_api),
            opt_out_notification_methods: opt_out,
            client_name,
            client_title,
            client_version,
        });
    }

    pub fn capabilities(&self) -> alleycat_bridge_core::state::Capabilities {
        self.session.capabilities()
    }

    pub fn should_emit(&self, method: &str) -> bool {
        self.session.should_emit(method)
    }

    pub fn send(&self, msg: JsonRpcMessage) {
        if let Ok(value) = serde_json::to_value(&msg) {
            self.session.enqueue(value);
        }
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub fn agy_pool(&self) -> &Arc<AgyPool> {
        &self.agy_pool
    }

    pub fn thread_index(&self) -> &Arc<dyn ThreadIndexHandle> {
        &self.thread_index
    }

    pub fn launcher(&self) -> Option<&Arc<dyn ProcessLauncher>> {
        self.launcher.as_ref()
    }

    pub fn trust_persisted_cwd(&self) -> bool {
        self.trust_persisted_cwd
    }

    pub fn codex_home(&self) -> &Path {
        &self.codex_home
    }

    pub fn get_cached_models(&self) -> Option<Vec<DiscoveredModel>> {
        self.cached_models.lock().unwrap().clone()
    }

    pub fn set_cached_models(&self, models: Vec<DiscoveredModel>) {
        *self.cached_models.lock().unwrap() = Some(models);
    }

    pub fn get_cached_agents(&self) -> Option<Vec<String>> {
        self.cached_agents.lock().unwrap().clone()
    }

    pub fn set_cached_agents(&self, agents: Vec<String>) {
        *self.cached_agents.lock().unwrap() = Some(agents);
    }

    pub fn defaults(&self) -> ThreadDefaults {
        self.defaults.lock().unwrap().clone()
    }

    pub fn update_defaults<F: FnOnce(&mut ThreadDefaults)>(&self, f: F) {
        let mut guard = self.defaults.lock().unwrap();
        f(&mut guard);
    }

    pub fn record_or_update_turn(&self, thread_id: &str, turn: RecordedTurn) {
        let mut logs = self.thread_logs.lock().unwrap();
        let turns = logs.entry(thread_id.to_string()).or_insert_with(|| {
            // Load existing persisted turns from disk if present so past turns
            // are preserved across server restarts and reconnections!
            let file_path = self.codex_home.join("agy_turns").join(format!("{thread_id}.json"));
            if file_path.is_file() {
                if let Ok(content) = std::fs::read_to_string(&file_path) {
                    if let Ok(existing) = serde_json::from_str::<Vec<RecordedTurn>>(&content) {
                        return existing;
                    }
                }
            }
            Vec::new()
        });
        if let Some(pos) = turns.iter().position(|t| t.turn_id == turn.turn_id) {
            turns[pos] = turn;
        } else {
            turns.push(turn);
        }
        let turns_clone = turns.clone();
        drop(logs);

        // Persist to disk: <codex_home>/agy_turns/<thread_id>.json
        let turns_dir = self.codex_home.join("agy_turns");
        if let Err(err) = std::fs::create_dir_all(&turns_dir) {
            tracing::warn!(?err, path = %turns_dir.display(), "failed to create agy_turns dir");
            return;
        }
        let file_path = turns_dir.join(format!("{thread_id}.json"));
        if let Ok(json) = serde_json::to_string_pretty(&turns_clone) {
            if let Err(err) = std::fs::write(&file_path, json) {
                tracing::warn!(?err, path = %file_path.display(), "failed to persist agy turns to disk");
            }
        }
    }

    pub fn record_turn(&self, thread_id: &str, turn: RecordedTurn) {
        self.record_or_update_turn(thread_id, turn);
    }

    pub fn recorded_turns(&self, thread_id: &str) -> Vec<RecordedTurn> {
        let mut logs = self.thread_logs.lock().unwrap();
        if let Some(turns) = logs.get(thread_id) {
            if !turns.is_empty() {
                return turns.clone();
            }
        }

        // Try reading persisted turns from disk: <codex_home>/agy_turns/<thread_id>.json
        let file_path = self.codex_home.join("agy_turns").join(format!("{thread_id}.json"));
        if file_path.is_file() {
            if let Ok(content) = std::fs::read_to_string(&file_path) {
                if let Ok(turns) = serde_json::from_str::<Vec<RecordedTurn>>(&content) {
                    logs.insert(thread_id.to_string(), turns.clone());
                    return turns;
                }
            }
        }

        logs.get(thread_id).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn persists_and_restores_turns_from_disk() {
        let dir = tempdir().unwrap();
        let codex_home = dir.path().to_path_buf();
        let session = Arc::new(Session::new("agy", "test".into(), 64, 1 << 20));
        let pool = Arc::new(AgyPool::new("agy"));
        let index = alleycat_bridge_core::ThreadIndex::<AgySessionRef>::open_at(
            codex_home.join("threads.json"),
        )
        .await
        .unwrap();

        let state = ConnectionState::new(
            session,
            pool,
            index,
            ThreadDefaults::default(),
            codex_home.clone(),
        );

        let turn = RecordedTurn {
            turn_id: "turn-test-1".to_string(),
            started_at: 1000,
            completed_at: Some(2000),
            status: TurnStatus::Completed,
            error: None,
            items: vec![ThreadItem::AgentMessage {
                id: "item-1".to_string(),
                text: "Hello from agy!".to_string(),
                phase: None,
                memory_citation: None,
            }],
        };

        state.record_turn("th-test", turn.clone());

        // Check memory
        let loaded_mem = state.recorded_turns("th-test");
        assert_eq!(loaded_mem, vec![turn.clone()]);

        // Check disk file exists
        let disk_file = codex_home.join("agy_turns").join("th-test.json");
        assert!(disk_file.is_file());

        // Clear in-memory log to simulate reconnection/restart
        state.thread_logs.lock().unwrap().clear();
        assert!(state.thread_logs.lock().unwrap().is_empty());

        // Call recorded_turns: should restore from disk!
        let loaded_disk = state.recorded_turns("th-test");
        assert_eq!(loaded_disk, vec![turn.clone()]);

        // Add a second turn after memory was cleared (simulating another session turn)
        let turn2 = RecordedTurn {
            turn_id: "turn-test-2".to_string(),
            started_at: 3000,
            completed_at: Some(4000),
            status: TurnStatus::Completed,
            error: None,
            items: vec![ThreadItem::AgentMessage {
                id: "item-2".to_string(),
                text: "Second turn reply".to_string(),
                phase: None,
                memory_citation: None,
            }],
        };
        // Clear memory again before recording turn 2
        state.thread_logs.lock().unwrap().clear();
        state.record_turn("th-test", turn2.clone());

        // Both turns must be present on disk!
        state.thread_logs.lock().unwrap().clear();
        let both_turns = state.recorded_turns("th-test");
        assert_eq!(both_turns.len(), 2);
        assert_eq!(both_turns[0], turn);
        assert_eq!(both_turns[1], turn2);
    }
}
