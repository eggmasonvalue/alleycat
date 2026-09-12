//! `AgyPool` — owns the set of live `agy` subprocesses and routes
//! codex thread ids to the right process.

pub mod agy_protocol;
pub mod process;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::pool::ProcessPool;
pub use alleycat_bridge_core::pool::{
    DEFAULT_IDLE_TTL, DEFAULT_MAX_PROCESSES, PoolError, ThreadId,
};
use alleycat_bridge_core::{LocalLauncher, ProcessLauncher};
use uuid::Uuid;

pub use agy_protocol::*;
pub use process::{
    AgyProcessError, AgyProcessHandle, AgySpawnConfig, DEFAULT_INIT_TIMEOUT,
};

#[derive(Debug, Clone)]
pub struct PoolPolicy {
    pub bypass_permissions: bool,
    pub agent: Option<String>,
}

impl Default for PoolPolicy {
    fn default() -> Self {
        Self {
            bypass_permissions: true,
            agent: None,
        }
    }
}

#[derive(Clone)]
pub struct AgyPool {
    inner: ProcessPool<AgyProcessHandle>,
    agy_bin: PathBuf,
    policy: PoolPolicy,
    launcher: Arc<dyn ProcessLauncher>,
}

impl std::fmt::Debug for AgyPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgyPool")
            .field("agy_bin", &self.agy_bin)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl AgyPool {
    pub fn new(agy_bin: impl Into<PathBuf>) -> Self {
        Self::with_launcher(
            agy_bin,
            Arc::new(LocalLauncher) as Arc<dyn ProcessLauncher>,
            PoolPolicy::default(),
        )
    }

    pub fn with_launcher(
        agy_bin: impl Into<PathBuf>,
        launcher: Arc<dyn ProcessLauncher>,
        policy: PoolPolicy,
    ) -> Self {
        Self {
            inner: ProcessPool::new(DEFAULT_MAX_PROCESSES, DEFAULT_IDLE_TTL),
            agy_bin: agy_bin.into(),
            policy,
            launcher,
        }
    }

    pub fn with_launcher_and_limits(
        agy_bin: impl Into<PathBuf>,
        launcher: Arc<dyn ProcessLauncher>,
        policy: PoolPolicy,
        max_processes: usize,
        idle_ttl: Duration,
    ) -> Self {
        Self {
            inner: ProcessPool::new(max_processes, idle_ttl),
            agy_bin: agy_bin.into(),
            policy,
            launcher,
        }
    }

    pub fn agy_bin(&self) -> &Path {
        &self.agy_bin
    }

    pub fn policy(&self) -> &PoolPolicy {
        &self.policy
    }

    pub fn launcher(&self) -> &Arc<dyn ProcessLauncher> {
        &self.launcher
    }

    pub async fn acquire_for_new_thread(
        &self,
        cwd: &Path,
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<(String, Arc<AgyProcessHandle>), PoolError> {
        let thread_id = Uuid::now_v7().to_string();
        self.inner.ensure_capacity_for(&thread_id).await?;
        let handle = self
            .spawn_handle(&thread_id, None, cwd, model, effort, false)
            .await
            .map_err(PoolError::Spawn)?;
        let arc = Arc::new(handle);
        self.inner
            .track_new(thread_id.clone(), cwd.to_path_buf(), Arc::clone(&arc))
            .await?;
        Ok((thread_id, arc))
    }

    pub async fn acquire_for_resume(
        &self,
        thread_id: &str,
        agy_session_id: Option<&str>,
        cwd: &Path,
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<Arc<AgyProcessHandle>, PoolError> {
        if let Some(existing) = self.inner.get(thread_id).await {
            return Ok(existing);
        }
        self.inner.ensure_capacity_for(thread_id).await?;
        let handle = self
            .spawn_handle(thread_id, agy_session_id, cwd, model, effort, true)
            .await
            .map_err(PoolError::Spawn)?;
        let arc = Arc::new(handle);
        self.inner
            .track_new(thread_id.to_string(), cwd.to_path_buf(), Arc::clone(&arc))
            .await?;
        Ok(arc)
    }

    pub async fn get(&self, thread_id: &str) -> Option<Arc<AgyProcessHandle>> {
        self.inner.get(thread_id).await
    }

    pub async fn mark_active(&self, thread_id: &str) {
        self.inner.mark_active(thread_id).await;
    }

    pub async fn mark_idle(&self, thread_id: &str) {
        self.inner.mark_idle(thread_id).await;
    }

    pub async fn release(&self, thread_id: &str) {
        self.inner.release(thread_id).await;
    }

    pub async fn loaded_thread_ids(&self) -> Vec<ThreadId> {
        self.inner.loaded_thread_ids().await
    }

    async fn spawn_handle(
        &self,
        thread_id: &str,
        agy_session_id: Option<&str>,
        cwd: &Path,
        model: Option<String>,
        effort: Option<String>,
        resume: bool,
    ) -> anyhow::Result<AgyProcessHandle> {
        let config = AgySpawnConfig {
            thread_id: thread_id.to_string(),
            agy_session_id: agy_session_id.map(str::to_string),
            cwd: cwd.to_path_buf(),
            agy_bin: self.agy_bin.clone(),
            model,
            effort,
            agent: self.policy.agent.clone(),
            resume,
            bypass_permissions: self.policy.bypass_permissions,
        };
        AgyProcessHandle::spawn(&self.launcher, config).await
    }
}
