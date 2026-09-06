//! Per-connection state for `agy-bridge`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use alleycat_bridge_core::ProcessLauncher;
use alleycat_bridge_core::session::Session;

use alleycat_codex_proto::{
    ApprovalsReviewer, AskForApproval, InitializeCapabilities, JsonRpcMessage, ReasoningEffort,
    SandboxMode, ThreadItem, TurnError, TurnStatus,
};

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
    thread_logs: Mutex<HashMap<String, Vec<RecordedTurn>>>,
}

#[derive(Debug, Clone)]
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
    ) -> Self {
        Self {
            defaults: Mutex::new(defaults),
            session,
            agy_pool,
            thread_index,
            launcher: None,
            trust_persisted_cwd: false,
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
    ) -> Self {
        Self {
            defaults: Mutex::new(defaults),
            session,
            agy_pool,
            thread_index,
            launcher,
            trust_persisted_cwd,
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

    pub fn defaults(&self) -> ThreadDefaults {
        self.defaults.lock().unwrap().clone()
    }

    pub fn update_defaults<F: FnOnce(&mut ThreadDefaults)>(&self, f: F) {
        let mut guard = self.defaults.lock().unwrap();
        f(&mut guard);
    }

    pub fn record_turn(&self, thread_id: &str, turn: RecordedTurn) {
        let mut logs = self.thread_logs.lock().unwrap();
        logs.entry(thread_id.to_string()).or_default().push(turn);
    }

    pub fn recorded_turns(&self, thread_id: &str) -> Vec<RecordedTurn> {
        let logs = self.thread_logs.lock().unwrap();
        logs.get(thread_id).cloned().unwrap_or_default()
    }
}
