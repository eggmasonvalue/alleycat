//! `AgyBridge` — the unified `Bridge` implementation for Google Antigravity CLI (`agy`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::server::{Bridge, Conn};
use alleycat_bridge_core::{
    JsonRpcError, LocalLauncher, ProcessLauncher, ThreadIndex as CoreThreadIndex, error_codes,
};
use alleycat_codex_proto as p;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;

use crate::handlers;
use crate::index::{AgyHydrator, AgySessionRef};
use crate::pool::{AgyPool, PoolPolicy};
use crate::state::{ConnectionState, ThreadDefaults};

pub type ThreadIndexHandle = Arc<dyn crate::state::ThreadIndexHandle>;

fn default_codex_home() -> PathBuf {
    handlers::lifecycle::default_codex_home()
}

pub struct AgyBridge {
    pool: Arc<AgyPool>,
    thread_index: ThreadIndexHandle,
    codex_home: PathBuf,
    launcher: Arc<dyn ProcessLauncher>,
    per_conn: DashMap<String, Arc<ConnectionState>>,
    trust_persisted_cwd: bool,
    seed_models: Option<Vec<crate::handlers::model::DiscoveredModel>>,
    seed_agents: Option<Vec<String>>,
}

impl std::fmt::Debug for AgyBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgyBridge")
            .field("codex_home", &self.codex_home)
            .field("connections", &self.per_conn.len())
            .field("trust_persisted_cwd", &self.trust_persisted_cwd)
            .finish_non_exhaustive()
    }
}

impl AgyBridge {
    pub fn builder() -> AgyBridgeBuilder {
        AgyBridgeBuilder::default()
    }

    pub fn pool(&self) -> &Arc<AgyPool> {
        &self.pool
    }

    pub fn thread_index(&self) -> &ThreadIndexHandle {
        &self.thread_index
    }

    pub fn codex_home(&self) -> &std::path::Path {
        &self.codex_home
    }

    pub fn launcher(&self) -> &Arc<dyn ProcessLauncher> {
        &self.launcher
    }

    pub fn per_conn(&self, ctx: &Conn) -> Arc<ConnectionState> {
        let session = ctx.session();
        let key = format!("{}:{}", session.agent, session.node_id);
        if let Some(existing) = self.per_conn.get(&key) {
            return Arc::clone(existing.value());
        }
        let state = Arc::new(ConnectionState::with_launcher(
            Arc::clone(ctx.session()),
            Arc::clone(&self.pool),
            Arc::clone(&self.thread_index),
            ThreadDefaults::default(),
            Some(Arc::clone(&self.launcher)),
            self.trust_persisted_cwd,
            self.codex_home.clone(),
            self.seed_models.clone(),
            self.seed_agents.clone(),
        ));
        let entry = self
            .per_conn
            .entry(key)
            .or_insert_with(|| Arc::clone(&state));
        Arc::clone(entry.value())
    }

    pub fn drop_session(&self, session_id: &str) {
        self.per_conn.remove(session_id);
    }
}

pub struct AgyBridgeBuilder {
    agent_bin: Option<PathBuf>,
    agent: Option<String>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    codex_home: Option<PathBuf>,
    pool_capacity: Option<usize>,
    idle_ttl: Option<Duration>,
    bypass_permissions: bool,
    trust_persisted_cwd: bool,
    summaries_db_override: Option<PathBuf>,
    seed_models: Option<Vec<crate::handlers::model::DiscoveredModel>>,
    seed_agents: Option<Vec<String>>,
}

impl Default for AgyBridgeBuilder {
    fn default() -> Self {
        Self {
            agent_bin: None,
            agent: None,
            launcher: None,
            codex_home: None,
            pool_capacity: None,
            idle_ttl: None,
            bypass_permissions: true,
            trust_persisted_cwd: false,
            summaries_db_override: None,
            seed_models: None,
            seed_agents: None,
        }
    }
}

impl AgyBridgeBuilder {
    pub fn agent_bin(mut self, bin: impl Into<PathBuf>) -> Self {
        self.agent_bin = Some(bin.into());
        self
    }

    pub fn agent(mut self, agent: impl Into<String>) -> Self {
        self.agent = Some(agent.into());
        self
    }

    pub fn launcher(mut self, launcher: Arc<dyn ProcessLauncher>) -> Self {
        self.launcher = Some(launcher);
        self
    }

    pub fn codex_home(mut self, home: impl Into<PathBuf>) -> Self {
        self.codex_home = Some(home.into());
        self
    }

    pub fn pool_capacity(mut self, n: usize) -> Self {
        self.pool_capacity = Some(n);
        self
    }

    pub fn idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = Some(ttl);
        self
    }

    pub fn bypass_permissions(mut self, b: bool) -> Self {
        self.bypass_permissions = b;
        self
    }

    pub fn trust_persisted_cwd(mut self, trust: bool) -> Self {
        self.trust_persisted_cwd = trust;
        self
    }

    pub fn summaries_db_override(mut self, db: PathBuf) -> Self {
        self.summaries_db_override = Some(db);
        self
    }

    pub fn seed_models(mut self, models: Vec<crate::handlers::model::DiscoveredModel>) -> Self {
        self.seed_models = Some(models);
        self
    }

    pub fn seed_agents(mut self, agents: Vec<String>) -> Self {
        self.seed_agents = Some(agents);
        self
    }

    pub fn from_env(mut self) -> Self {
        if self.agent_bin.is_none() && let Some(bin) = std::env::var_os("AGY_BRIDGE_BIN") {
            self.agent_bin = Some(PathBuf::from(bin));
        }
        if self.agent.is_none() && let Ok(agent) = std::env::var("AGY_BRIDGE_AGENT") {
            if !agent.is_empty() {
                self.agent = Some(agent);
            }
        }
        if self.codex_home.is_none()
            && let Some(home) = std::env::var_os("CODEX_HOME").filter(|v| !v.is_empty())
        {
            self.codex_home = Some(PathBuf::from(home));
        }
        if let Ok(value) = std::env::var("AGY_BRIDGE_BYPASS_PERMISSIONS") {
            self.bypass_permissions = matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }
        self
    }

    pub async fn build(self) -> Result<Arc<AgyBridge>> {
        let agy_bin = self.agent_bin.unwrap_or_else(|| PathBuf::from("agy"));
        let launcher = self
            .launcher
            .unwrap_or_else(|| Arc::new(LocalLauncher) as Arc<dyn ProcessLauncher>);
        let codex_home = self.codex_home.unwrap_or_else(default_codex_home);
        if let Err(err) = std::fs::create_dir_all(&codex_home) {
            tracing::warn!(?err, path = %codex_home.display(), "failed to create agy codex_home");
        }

        let policy = PoolPolicy {
            bypass_permissions: self.bypass_permissions,
            agent: self.agent,
        };

        let pool = match (self.pool_capacity, self.idle_ttl) {
            (Some(cap), Some(ttl)) => Arc::new(AgyPool::with_launcher_and_limits(
                agy_bin,
                Arc::clone(&launcher),
                policy,
                cap,
                ttl,
            )),
            _ => Arc::new(AgyPool::with_launcher(
                agy_bin,
                Arc::clone(&launcher),
                policy,
            )),
        };

        let hydrator = match self.summaries_db_override {
            Some(db) => AgyHydrator::with_override_db(db),
            None => AgyHydrator::new(),
        };

        let index = CoreThreadIndex::<AgySessionRef>::open_and_hydrate(
            codex_home.join("threads.json"),
            &hydrator,
        )
        .await?;

        let thread_index: ThreadIndexHandle = index;

        Ok(Arc::new(AgyBridge {
            pool,
            thread_index,
            codex_home,
            launcher,
            per_conn: DashMap::new(),
            trust_persisted_cwd: self.trust_persisted_cwd,
            seed_models: self.seed_models,
            seed_agents: self.seed_agents,
        }))
    }
}

#[async_trait]
impl Bridge for AgyBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let typed: p::InitializeParams =
            serde_json::from_value(params).map_err(|err| invalid_params(err.to_string()))?;
        let state = self.per_conn(ctx);
        let response = handlers::lifecycle::handle_initialize(&state, typed, &self.codex_home);
        serde_json::to_value(response).map_err(|err| internal(err.to_string()))
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let state = self.per_conn(ctx);
        dispatch_request(&state, &self.codex_home, method, params).await
    }

    async fn notification(&self, ctx: &Conn, method: &str, _params: Value) {
        if method == "initialized" {
            let state = self.per_conn(ctx);
            handlers::lifecycle::handle_initialized(&state);
            return;
        }
        tracing::debug!(method, "ignoring unknown agy client notification");
    }
}

fn invalid_params(msg: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: msg.into(),
        data: None,
    }
}

fn internal(msg: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: msg.into(),
        data: None,
    }
}

fn method_not_found(method: &str) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: format!("method `{method}` is not implemented"),
        data: None,
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, JsonRpcError> {
    serde_json::from_value(value).map_err(|err| invalid_params(err.to_string()))
}

fn to_value<T: serde::Serialize>(v: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(v).map_err(|err| internal(err.to_string()))
}

fn thread_to_rpc(err: handlers::thread::ThreadError) -> JsonRpcError {
    JsonRpcError {
        code: err.rpc_code(),
        message: err.to_string(),
        data: None,
    }
}

fn turn_to_rpc(err: handlers::turn::TurnError) -> JsonRpcError {
    JsonRpcError {
        code: err.rpc_code(),
        message: err.to_string(),
        data: None,
    }
}

async fn dispatch_request(
    state: &Arc<ConnectionState>,
    codex_home: &std::path::Path,
    method: &str,
    params: Value,
) -> Result<Value, JsonRpcError> {
    match method {
        "account/read" => {
            let typed: p::GetAccountParams = if params.is_null() {
                Default::default()
            } else {
                decode(params)?
            };
            to_value(handlers::lifecycle::handle_account_read(state, typed))
        }
        "account/rateLimits/read" => {
            to_value(handlers::lifecycle::handle_account_rate_limits_read(state))
        }
        "account/login/start" => {
            let typed: p::LoginAccountParams = decode(params)?;
            let resp = handlers::lifecycle::handle_account_login_start(state, typed)
                .map_err(|err| internal(err.to_string()))?;
            to_value(resp)
        }
        "account/login/cancel" => {
            let typed: p::CancelLoginAccountParams = decode(params)?;
            to_value(handlers::lifecycle::handle_account_login_cancel(state, typed))
        }
        "account/logout" => to_value(handlers::lifecycle::handle_account_logout(state)),
        "feedback/upload" => {
            let typed: p::FeedbackUploadParams = decode(params)?;
            to_value(handlers::lifecycle::handle_feedback_upload(state, typed))
        }
        "config/read" => {
            let typed: p::ConfigReadParams = if params.is_null() {
                Default::default()
            } else {
                decode(params)?
            };
            let resp = handlers::config::handle_config_read(state, codex_home, typed)
                .map_err(|err| internal(err.to_string()))?;
            to_value(resp)
        }
        "config/value/write" => {
            let typed: p::ConfigValueWriteParams = decode(params)?;
            let resp = handlers::config::handle_config_value_write(state, codex_home, typed)
                .map_err(|err| internal(err.to_string()))?;
            to_value(resp)
        }
        "config/batchWrite" => {
            let typed: p::ConfigBatchWriteParams = decode(params)?;
            let resp = handlers::config::handle_config_batch_write(state, codex_home, typed)
                .map_err(|err| internal(err.to_string()))?;
            to_value(resp)
        }
        "configRequirements/read" => {
            to_value(handlers::config::handle_config_requirements_read(state))
        }
        "model/list" => {
            let typed: p::ModelListParams = if params.is_null() {
                Default::default()
            } else {
                decode(params)?
            };
            to_value(handlers::model::handle_model_list(state, typed).await)
        }
        "experimentalFeature/list" => to_value(p::ExperimentalFeatureListResponse {
            data: Vec::new(),
            next_cursor: None,
        }),
        "collaborationMode/list" => to_value(p::CollaborationModeListResponse { data: Vec::new() }),
        "thread/start" => {
            let typed: p::ThreadStartParams = decode(params)?;
            let resp = handlers::thread::handle_thread_start(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "thread/resume" => {
            let typed: p::ThreadResumeParams = decode(params)?;
            let resp = handlers::thread::handle_thread_resume(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "thread/list" => {
            let typed: p::ThreadListParams = if params.is_null() {
                Default::default()
            } else {
                decode(params)?
            };
            let resp = handlers::thread::handle_thread_list(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "thread/loaded/list" => {
            let typed: p::ThreadLoadedListParams = if params.is_null() {
                Default::default()
            } else {
                decode(params)?
            };
            to_value(handlers::thread::handle_thread_loaded_list(state, typed).await)
        }
        "thread/read" => {
            let typed: p::ThreadReadParams = decode(params)?;
            let resp = handlers::thread::handle_thread_read(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "thread/turns/list" => {
            let typed: p::ThreadTurnsListParams = decode(params)?;
            let resp = handlers::thread::handle_thread_turns_list(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "thread/archive" => {
            let typed: p::ThreadArchiveParams = decode(params)?;
            let resp = handlers::thread::handle_thread_archive(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "thread/name/set" => {
            let typed: p::ThreadSetNameParams = decode(params)?;
            let resp = handlers::thread::handle_thread_set_name(state, typed)
                .await
                .map_err(thread_to_rpc)?;
            to_value(resp)
        }
        "turn/start" => {
            let typed: p::TurnStartParams = decode(params)?;
            let resp = handlers::turn::handle_turn_start(state, typed)
                .await
                .map_err(turn_to_rpc)?;
            to_value(resp)
        }
        "turn/steer" => {
            let typed: p::TurnSteerParams = decode(params)?;
            let resp = handlers::turn::handle_turn_steer(state, typed)
                .await
                .map_err(turn_to_rpc)?;
            to_value(resp)
        }
        "turn/interrupt" => {
            let typed: p::TurnInterruptParams = decode(params)?;
            let resp = handlers::turn::handle_turn_interrupt(state, typed)
                .await
                .map_err(turn_to_rpc)?;
            to_value(resp)
        }
        other => Err(method_not_found(other)),
    }
}
