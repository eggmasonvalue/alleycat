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
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
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

    let models_map: std::collections::HashMap<String, String> = path
        .parent()
        .map(|p| p.join("conversation_models.json"))
        .filter(|p| p.is_file())
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    let mut stmt = conn.prepare(
        "SELECT conversation_id, title, preview, step_count, last_modified_time, \
                workspace_uris, parent_conversation_id, nesting_depth, status \
         FROM conversation_summaries \
         ORDER BY last_modified_time DESC",
    )?;

    let base_dir = path.parent();
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

        let raw_model = models_map.get(&conversation_id).cloned().or_else(|| {
            read_conversation_model_from_disk(base_dir, &conversation_id)
        });
        let (model, effort) = raw_model
            .as_deref()
            .map(parse_model_and_effort)
            .unwrap_or((None, None));

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
            model,
            effort,
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

pub fn parse_model_and_effort(raw: &str) -> (Option<String>, Option<String>) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    if let Some(base) = trimmed.strip_suffix("-high") {
        (Some(base.to_string()), Some("high".to_string()))
    } else if let Some(base) = trimmed.strip_suffix("-medium") {
        (Some(base.to_string()), Some("medium".to_string()))
    } else if let Some(base) = trimmed.strip_suffix("-low") {
        (Some(base.to_string()), Some("low".to_string()))
    } else {
        (Some(trimmed.to_string()), None)
    }
}

pub fn read_conversation_model_from_disk(base: Option<&Path>, conv_id: &str) -> Option<String> {
    let conv_db = base?.join("conversations").join(format!("{conv_id}.db"));
    if !conv_db.is_file() {
        return None;
    }
    let conn = Connection::open_with_flags(
        &conv_db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    let mut stmt = conn
        .prepare("SELECT data FROM executor_metadata ORDER BY idx DESC LIMIT 1")
        .ok()?;
    let blob: Vec<u8> = stmt.query_row([], |row| row.get(0)).ok()?;
    extract_model_from_blob(&blob)
}

fn extract_model_from_blob(blob: &[u8]) -> Option<String> {
    for prefix in [&b"gemini-"[..], &b"claude-"[..], &b"gpt-oss-"[..]] {
        if let Some(pos) = blob.windows(prefix.len()).position(|w| w == prefix) {
            let slice = &blob[pos..];
            let len = slice
                .iter()
                .take_while(|&&b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
                .count();
            if len > prefix.len() {
                if let Ok(s) = std::str::from_utf8(&slice[..len]) {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
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

    #[test]
    fn parses_model_and_effort_correctly() {
        assert_eq!(
            parse_model_and_effort("gemini-3.8-flash-high"),
            (Some("gemini-3.8-flash".to_string()), Some("high".to_string()))
        );
        assert_eq!(
            parse_model_and_effort("gemini-3.7-flash-medium"),
            (Some("gemini-3.7-flash".to_string()), Some("medium".to_string()))
        );
        assert_eq!(
            parse_model_and_effort("gemini-3.1-pro-low"),
            (Some("gemini-3.1-pro".to_string()), Some("low".to_string()))
        );
        assert_eq!(
            parse_model_and_effort("claude-sonnet-4-6"),
            (Some("claude-sonnet-4-6".to_string()), None)
        );
        assert_eq!(
            parse_model_and_effort("gpt-oss-120b-medium"),
            (Some("gpt-oss-120b".to_string()), Some("medium".to_string()))
        );
        assert_eq!(parse_model_and_effort(""), (None, None));
    }

    #[test]
    fn extracts_model_from_raw_binary_blob() {
        let blob = b"\x08\x04\x12\x15gemini-3.8-flash-high\x18\x01";
        assert_eq!(
            extract_model_from_blob(blob),
            Some("gemini-3.8-flash-high".to_string())
        );

        let claude_blob = b"\x00\x00claude-sonnet-4-6\x10\x02";
        assert_eq!(
            extract_model_from_blob(claude_blob),
            Some("claude-sonnet-4-6".to_string())
        );

        let empty = b"\x00\x01\x02";
        assert_eq!(extract_model_from_blob(empty), None);
    }
}
