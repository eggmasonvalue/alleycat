//! Reads `~/.gemini/antigravity-cli/conversation_summaries.db` directly
//! via SQLite to provide instant, zero-JSON-deserialization session enumeration.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::DateTime;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgySessionInfo {
    pub conversation_id: String,
    pub title: String,
    pub preview: String,
    pub step_count: i64,
    pub last_modified_ms: i64,
    pub cwd: String,
    pub parent_conversation_id: Option<String>,
    pub nesting_depth: i32,
    pub status: String,
}

pub fn default_agy_summaries_db() -> Option<PathBuf> {
    if let Ok(env_path) = std::env::var("AGY_SUMMARIES_DB") {
        return Some(PathBuf::from(env_path));
    }
    let home = std::env::var_os("HOME")?;
    Some(PathBuf::from(home).join(".gemini/antigravity-cli/conversation_summaries.db"))
}

pub async fn list_all() -> Vec<AgySessionInfo> {
    let Some(db_path) = default_agy_summaries_db() else {
        return Vec::new();
    };
    if !db_path.exists() {
        return Vec::new();
    }
    tokio::task::spawn_blocking(move || read_sessions(&db_path).unwrap_or_default())
        .await
        .unwrap_or_default()
}

pub fn read_sessions(path: &Path) -> Result<Vec<AgySessionInfo>> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening agy summaries db at {}", path.display()))?;

    let mut stmt = conn.prepare(
        "SELECT conversation_id, title, preview, step_count, last_modified_time, \
                workspace_uris, parent_conversation_id, nesting_depth, status \
         FROM conversation_summaries \
         ORDER BY last_modified_time DESC",
    )?;

    let rows = stmt.query_map([], |row| {
        let conversation_id: String = row.get(0)?;
        let title: String = row.get(1)?;
        let preview: String = row.get(2)?;
        let step_count: i64 = row.get(3)?;
        let raw_time: String = row.get(4)?;
        let raw_uris: String = row.get(5)?;
        let raw_parent: String = row.get(6)?;
        let nesting_depth: i32 = row.get(7)?;
        let status: String = row.get(8)?;

        let last_modified_ms = parse_timestamp_ms(&raw_time);
        let cwd = parse_workspace_cwd(&raw_uris);
        let parent_conversation_id = if raw_parent.trim().is_empty() {
            None
        } else {
            Some(raw_parent.trim().to_string())
        };

        Ok(AgySessionInfo {
            conversation_id,
            title,
            preview,
            step_count,
            last_modified_ms,
            cwd,
            parent_conversation_id,
            nesting_depth,
            status,
        })
    })?;

    let mut out = Vec::new();
    for row in rows {
        if let Ok(info) = row {
            out.push(info);
        }
    }
    Ok(out)
}

fn parse_timestamp_ms(raw: &str) -> i64 {
    let cleaned = raw.trim().replace(' ', "T");
    if let Ok(dt) = DateTime::parse_from_rfc3339(&cleaned) {
        let ms = dt.timestamp_millis();
        if ms > 0 {
            return ms;
        }
    }
    chrono::Utc::now().timestamp_millis()
}

fn parse_workspace_cwd(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(uris) = serde_json::from_str::<Vec<String>>(trimmed) {
        if let Some(first) = uris.into_iter().next() {
            if let Some(path) = first.strip_prefix("file://") {
                return path.to_string();
            }
            return first;
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn parses_sqlite_session_row() {
        let file = NamedTempFile::new().expect("temp file");
        let conn = Connection::open(file.path()).expect("open db");
        conn.execute(
            "CREATE TABLE conversation_summaries (
                conversation_id text PRIMARY KEY,
                title text NOT NULL DEFAULT '',
                preview text NOT NULL DEFAULT '',
                step_count integer NOT NULL DEFAULT 0,
                last_modified_time datetime NOT NULL,
                workspace_uris text NOT NULL,
                parent_conversation_id text NOT NULL DEFAULT '',
                nesting_depth integer NOT NULL DEFAULT 0,
                status text NOT NULL DEFAULT ''
            )",
            [],
        )
        .expect("create table");

        conn.execute(
            "INSERT INTO conversation_summaries VALUES (
                'test-conv-1', 'Test Title', 'Test Preview', 12,
                '2026-08-03 13:53:49.287144374+00:00',
                '[\"file:///workspace/project\"]',
                'parent-uuid', 1, 'ACTIVE'
            )",
            [],
        )
        .expect("insert row");

        let sessions = read_sessions(file.path()).expect("read sessions");
        assert_eq!(sessions.len(), 1);
        let s = &sessions[0];
        assert_eq!(s.conversation_id, "test-conv-1");
        assert_eq!(s.title, "Test Title");
        assert_eq!(s.preview, "Test Preview");
        assert_eq!(s.step_count, 12);
        assert_eq!(s.cwd, "/workspace/project");
        assert_eq!(s.parent_conversation_id.as_deref(), Some("parent-uuid"));
        assert_eq!(s.nesting_depth, 1);
        assert_eq!(s.status, "ACTIVE");
        assert!(s.last_modified_ms > 0);
    }
}
