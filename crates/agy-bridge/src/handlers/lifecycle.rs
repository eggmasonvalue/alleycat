//! `initialize` / `initialized` plus `account/*` handlers for `agy-bridge`.

use std::path::PathBuf;
use std::sync::Arc;

use alleycat_codex_proto as p;
use anyhow::Result;

use crate::state::ConnectionState;

pub const USER_AGENT: &str = concat!("alleycat-agy-bridge/", env!("CARGO_PKG_VERSION"));

pub fn default_codex_home() -> PathBuf {
    if let Some(dirs) = directories::ProjectDirs::from("", "", "codex") {
        dirs.config_dir().join("agy-bridge")
    } else {
        PathBuf::from(".codex/agy-bridge")
    }
}

pub fn handle_initialize(
    state: &Arc<ConnectionState>,
    params: p::InitializeParams,
    codex_home: &std::path::Path,
) -> p::InitializeResponse {
    state.set_capabilities(
        Some(params.client_info.name.clone()),
        params.client_info.title.clone(),
        Some(params.client_info.version.clone()),
        params.capabilities.as_ref(),
    );

    p::InitializeResponse {
        user_agent: USER_AGENT.to_string(),
        codex_home: codex_home.to_string_lossy().into_owned(),
        platform_family: platform_family().to_string(),
        platform_os: platform_os().to_string(),
    }
}

pub fn handle_initialized(_state: &Arc<ConnectionState>) {
    tracing::debug!("agy client sent initialized; connection ready");
}

pub fn handle_account_read(
    _state: &Arc<ConnectionState>,
    _params: p::GetAccountParams,
) -> p::GetAccountResponse {
    p::GetAccountResponse {
        account: Some(p::Account::ApiKey {}),
        requires_openai_auth: false,
    }
}

pub fn handle_account_rate_limits_read(
    _state: &Arc<ConnectionState>,
) -> p::GetAccountRateLimitsResponse {
    p::GetAccountRateLimitsResponse {
        rate_limits: p::RateLimitSnapshot::default(),
        rate_limits_by_limit_id: None,
    }
}

pub fn handle_account_login_start(
    _state: &Arc<ConnectionState>,
    _params: p::LoginAccountParams,
) -> Result<p::LoginAccountResponse> {
    anyhow::bail!("authentication is managed by the `agy` CLI on the host");
}

pub fn handle_account_login_cancel(
    _state: &Arc<ConnectionState>,
    _params: p::CancelLoginAccountParams,
) -> p::CancelLoginAccountResponse {
    p::CancelLoginAccountResponse {
        status: p::CancelLoginAccountStatus::Canceled,
    }
}

pub fn handle_account_logout(_state: &Arc<ConnectionState>) -> p::LogoutAccountResponse {
    p::LogoutAccountResponse {}
}

pub fn handle_feedback_upload(
    _state: &Arc<ConnectionState>,
    _params: p::FeedbackUploadParams,
) -> p::FeedbackUploadResponse {
    p::FeedbackUploadResponse {}
}

fn platform_family() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        "windows"
    }
    #[cfg(target_os = "macos")]
    {
        "darwin"
    }
    #[cfg(target_os = "linux")]
    {
        "unix"
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        "unknown"
    }
}

fn platform_os() -> &'static str {
    std::env::consts::OS
}
