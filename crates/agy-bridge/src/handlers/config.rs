//! `config/*` request handlers for `agy-bridge`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_codex_proto as p;
use anyhow::Result;
use serde_json::Value;

use crate::state::ConnectionState;

pub fn agy_settings_path() -> Option<PathBuf> {
    let home = directories::UserDirs::new()?.home_dir().to_path_buf();
    Some(home.join(".gemini").join("antigravity-cli").join("settings.json"))
}

pub fn handle_config_read(
    _state: &Arc<ConnectionState>,
    _codex_home: &Path,
    _params: p::ConfigReadParams,
) -> Result<p::ConfigReadResponse> {
    let config = read_json_or_default(agy_settings_path().as_deref());
    Ok(p::ConfigReadResponse {
        config,
        origins: Default::default(),
        layers: None,
    })
}

pub fn handle_config_value_write(
    _state: &Arc<ConnectionState>,
    _codex_home: &Path,
    _params: p::ConfigValueWriteParams,
) -> Result<p::ConfigWriteResponse> {
    Ok(stub_write_response())
}

pub fn handle_config_batch_write(
    _state: &Arc<ConnectionState>,
    _codex_home: &Path,
    _params: p::ConfigBatchWriteParams,
) -> Result<p::ConfigWriteResponse> {
    Ok(stub_write_response())
}

pub fn handle_config_requirements_read(
    _state: &Arc<ConnectionState>,
) -> p::ConfigRequirementsReadResponse {
    p::ConfigRequirementsReadResponse { requirements: None }
}

fn stub_write_response() -> p::ConfigWriteResponse {
    p::ConfigWriteResponse {
        status: p::WriteStatus::Ok,
        version: "1".to_string(),
        file_path: String::new(),
        overridden_metadata: None,
    }
}

fn read_json_or_default(path: Option<&Path>) -> Value {
    let Some(path) = path else {
        return serde_json::json!({});
    };
    match std::fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    }
}
