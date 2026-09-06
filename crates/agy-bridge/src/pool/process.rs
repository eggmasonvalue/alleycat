//! `AgyProcessHandle` — spawns one `agy` subprocess in headless NDJSON mode
//! (`--input-format stream-json --output-format stream-json`) and manages stdio.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::{
    ChildProcess, ChildStderr, ChildStdin, ChildStdout, ProcessLauncher, ProcessRole, ProcessSpec,
    StdioMode,
};
use anyhow::{Context, Result, anyhow};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::agy_protocol::{AgyInitData, AgyInbound, AgyOutbound};

const EVENT_CHANNEL_CAPACITY: usize = 1024;
pub const DEFAULT_INIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct AgySpawnConfig {
    pub thread_id: String,
    pub cwd: PathBuf,
    pub agy_bin: PathBuf,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub agent: Option<String>,
    pub resume: bool,
    pub bypass_permissions: bool,
}

#[derive(Debug, Error)]
pub enum AgyProcessError {
    #[error("agy process exited before publishing init")]
    InitTimeout,
    #[error("agy process died unexpectedly with exit code {0:?}")]
    ChildDied(Option<i32>),
    #[error("writer task failed: {0}")]
    WriterFailed(String),
}

#[derive(Debug, Default)]
struct InitSlot {
    data: Mutex<Option<AgyInitData>>,
    notify: Notify,
}

impl InitSlot {
    async fn publish(&self, init: AgyInitData) {
        let mut guard = self.data.lock().await;
        if guard.is_none() {
            *guard = Some(init);
            self.notify.notify_waiters();
        }
    }

    async fn get(&self) -> Option<AgyInitData> {
        self.data.lock().await.clone()
    }

    async fn wait(&self, duration: Duration) -> Result<AgyInitData, AgyProcessError> {
        if let Some(data) = self.get().await {
            return Ok(data);
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        match timeout(duration, notified).await {
            Ok(_) => self.get().await.ok_or(AgyProcessError::InitTimeout),
            Err(_) => Err(AgyProcessError::InitTimeout),
        }
    }
}

pub struct AgyProcessHandle {
    cwd: PathBuf,
    agy_bin: PathBuf,
    thread_id: String,
    pid: Option<u32>,
    writer_tx: mpsc::UnboundedSender<String>,
    events_tx: broadcast::Sender<AgyOutbound>,
    init_slot: Arc<InitSlot>,
    tasks: Arc<TaskSet>,
}

struct TaskSet {
    writer: Mutex<Option<JoinHandle<()>>>,
    reader: Mutex<Option<JoinHandle<()>>>,
    stderr: Mutex<Option<JoinHandle<()>>>,
    child: Mutex<Option<Box<dyn ChildProcess>>>,
}

impl AgyProcessHandle {
    pub async fn spawn(
        launcher: &Arc<dyn ProcessLauncher>,
        config: AgySpawnConfig,
    ) -> Result<Self> {
        let mut args: Vec<String> = vec![
            "--input-format".to_string(),
            "stream-json".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
        ];

        if config.bypass_permissions {
            args.push("--dangerously-skip-permissions".to_string());
        }

        // Section 4 Model & Agent handling:
        // Namespaced model slug format "<agent>/<model>"
        let mut resolved_agent = config.agent.clone();
        let mut resolved_model = config.model.clone();

        if let Some(ref m) = config.model {
            if let Some((agent_part, model_part)) = m.split_once('/') {
                resolved_agent = Some(agent_part.to_string());
                resolved_model = Some(model_part.to_string());
            }
        }

        if let Some(agent) = resolved_agent {
            args.push("--agent".to_string());
            args.push(agent);
        }

        if let Some(model) = resolved_model {
            args.push("--model".to_string());
            args.push(model);
        }

        if let Some(ref effort) = config.effort {
            args.push("--effort".to_string());
            args.push(effort.clone());
        }

        if config.resume {
            args.push("--conversation".to_string());
            args.push(config.thread_id.clone());
        }

        args.push("--add-dir".to_string());
        args.push(config.cwd.to_string_lossy().to_string());

        let spec_args: Vec<OsString> = args.into_iter().map(OsString::from).collect();

        let spec = ProcessSpec {
            role: ProcessRole::Agent,
            program: config.agy_bin.clone(),
            args: spec_args,
            cwd: Some(config.cwd.clone()),
            env: Vec::new(),
            env_clear: false,
            stdin: StdioMode::Piped,
            stdout: StdioMode::Piped,
            stderr: StdioMode::Piped,
        };

        let mut child = launcher.launch(spec).await.with_context(|| {
            format!(
                "spawning {} in {}",
                config.agy_bin.display(),
                config.cwd.display()
            )
        })?;

        let pid = child.id();
        let stdin = child
            .take_stdin()
            .ok_or_else(|| anyhow!("agy child has no stdin pipe"))?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| anyhow!("agy child has no stdout pipe"))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| anyhow!("agy child has no stderr pipe"))?;

        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<String>();
        let (events_tx, _events_rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let init_slot = Arc::new(InitSlot::default());

        let writer = tokio::spawn(writer_task(stdin, writer_rx));
        let reader = tokio::spawn(reader_task(stdout, Arc::clone(&init_slot), events_tx.clone()));
        let stderr_handle = tokio::spawn(stderr_task(stderr, pid));

        let tasks = Arc::new(TaskSet {
            writer: Mutex::new(Some(writer)),
            reader: Mutex::new(Some(reader)),
            stderr: Mutex::new(Some(stderr_handle)),
            child: Mutex::new(Some(child)),
        });

        Ok(Self {
            cwd: config.cwd,
            agy_bin: config.agy_bin,
            thread_id: config.thread_id,
            pid,
            writer_tx,
            events_tx,
            init_slot,
            tasks,
        })
    }

    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub fn agy_bin(&self) -> &Path {
        &self.agy_bin
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AgyOutbound> {
        self.events_tx.subscribe()
    }

    pub async fn wait_for_init(&self, duration: Duration) -> Result<AgyInitData, AgyProcessError> {
        self.init_slot.wait(duration).await
    }

    pub fn send_prompt(&self, content: &str) -> Result<(), AgyProcessError> {
        let envelope = AgyInbound::user_prompt(content);
        let line = serde_json::to_string(&envelope)
            .map_err(|e| AgyProcessError::WriterFailed(e.to_string()))?;
        self.writer_tx
            .send(line)
            .map_err(|e| AgyProcessError::WriterFailed(e.to_string()))
    }

    pub async fn shutdown(&self) {
        if let Some(handle) = self.tasks.writer.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.tasks.stderr.lock().await.take() {
            handle.abort();
        }
        if let Some(mut child) = self.tasks.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        if let Some(handle) = self.tasks.reader.lock().await.take() {
            handle.abort();
        }
    }
}

impl alleycat_bridge_core::pool::PoolMember for AgyProcessHandle {
    async fn shutdown(&self) {
        AgyProcessHandle::shutdown(self).await;
    }
}

async fn writer_task(mut stdin: ChildStdin, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(mut line) = rx.recv().await {
        line.push('\n');
        if let Err(err) = stdin.write_all(line.as_bytes()).await {
            tracing::warn!(?err, "agy writer task: stdin write failed; exiting");
            break;
        }
        if let Err(err) = stdin.flush().await {
            tracing::warn!(?err, "agy writer task: stdin flush failed; exiting");
            break;
        }
    }
}

async fn reader_task(
    stdout: ChildStdout,
    init_slot: Arc<InitSlot>,
    events_tx: broadcast::Sender<AgyOutbound>,
) {
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => {
                tracing::debug!("agy reader task: stdout closed");
                break;
            }
            Err(err) => {
                tracing::warn!(?err, "agy reader task: read error; exiting");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match serde_json::from_str::<AgyOutbound>(trimmed) {
            Ok(event) => {
                if let AgyOutbound::Init { ref init, .. } = event {
                    init_slot.publish(init.clone()).await;
                }
                let _ = events_tx.send(event);
            }
            Err(err) => {
                tracing::debug!(?err, line = %trimmed, "agy reader: ignoring unparsed stdout line");
            }
        }
    }
}

async fn stderr_task(stderr: ChildStderr, pid: Option<u32>) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            tracing::debug!(pid = ?pid, "agy stderr: {}", trimmed);
        }
    }
}
