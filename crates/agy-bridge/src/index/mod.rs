//! Bridge-side thread index for Google Antigravity CLI (`agy`).

pub mod agy_session_scan;

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use agy_session_scan::{AgySessionInfo, default_agy_summaries_db, list_all};

use alleycat_bridge_core::Hydrator;
pub use alleycat_bridge_core::{
    IndexEntry as CoreIndexEntry, ListFilter, ListPage, ListSort, ThreadIndex as CoreThreadIndex,
};
use alleycat_codex_proto::{SessionSource, Thread, ThreadSourceKind, ThreadStatus};

pub const CLI_VERSION: &str = concat!("alleycat-agy-bridge/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgySessionRef {
    pub agy_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_conversation_id: Option<String>,
    #[serde(default)]
    pub nesting_depth: i32,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub step_count: i64,
}

pub type IndexEntry = CoreIndexEntry<AgySessionRef>;

pub fn entry_from_agy(info: &AgySessionInfo) -> IndexEntry {
    IndexEntry {
        thread_id: info.conversation_id.clone(),
        cwd: info.cwd.clone(),
        created_at: info.last_modified_ms,
        updated_at: info.last_modified_ms,
        archived: false,
        name: if info.title.is_empty() {
            None
        } else {
            Some(info.title.clone())
        },
        preview: if info.preview.is_empty() {
            info.title.clone()
        } else {
            info.preview.clone()
        },
        forked_from_id: info.parent_conversation_id.clone(),
        model_provider: "google".to_string(),
        source: ThreadSourceKind::AppServer,
        metadata: AgySessionRef {
            agy_session_id: info.conversation_id.clone(),
            parent_conversation_id: info.parent_conversation_id.clone(),
            nesting_depth: info.nesting_depth,
            status: info.status.clone(),
            step_count: info.step_count,
        },
    }
}

pub fn entry_to_thread(entry: &IndexEntry) -> Thread {
    Thread {
        id: entry.thread_id.clone(),
        session_id: entry.metadata.agy_session_id.clone(),
        forked_from_id: entry.forked_from_id.clone(),
        preview: entry.preview.clone(),
        ephemeral: false,
        model_provider: entry.model_provider.clone(),
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        status: ThreadStatus::NotLoaded,
        path: None,
        cwd: entry.cwd.clone(),
        cli_version: CLI_VERSION.to_string(),
        source: SessionSource::AppServer,
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        git_info: alleycat_bridge_core::git_info_for_cwd(&entry.cwd),
        name: entry.name.clone(),
        turns: Vec::new(),
    }
}

#[derive(Default)]
pub struct AgyHydrator {
    pub override_db: Option<PathBuf>,
}

impl AgyHydrator {
    pub fn new() -> Self {
        Self { override_db: None }
    }

    pub fn with_override_db(db: PathBuf) -> Self {
        Self {
            override_db: Some(db),
        }
    }
}

#[async_trait]
impl Hydrator<AgySessionRef> for AgyHydrator {
    async fn scan(&self) -> Result<Vec<IndexEntry>> {
        let scanned = match self.override_db.as_ref() {
            Some(path) => {
                let p = path.clone();
                tokio::task::spawn_blocking(move || agy_session_scan::read_sessions(&p).unwrap_or_default())
                    .await
                    .unwrap_or_default()
            }
            None => list_all().await,
        };
        Ok(scanned.iter().map(entry_from_agy).collect())
    }
}
