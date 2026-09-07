//! `model/list` handler for Google Antigravity CLI (`agy`).
//!
//! Provides default Antigravity models and dynamic namespacing for custom
//! agent profiles (`<agent>/<model>`) to enable multi-agent support without
//! frontend changes.

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use alleycat_codex_proto as p;
use serde_json::json;

use crate::state::ConnectionState;

pub const MODEL_PROVIDER: &str = "google";

pub const DEFAULT_MODEL: &str = "gemini-3.7-flash-high";

static CACHED_MODELS: LazyLock<Mutex<Option<Vec<DiscoveredModel>>>> =
    LazyLock::new(|| Mutex::new(None));
static CACHED_AGENTS: LazyLock<Mutex<Option<Vec<String>>>> =
    LazyLock::new(|| Mutex::new(None));

#[derive(Clone)]
pub struct DiscoveredModel {
    pub id: String,
    pub name: String,
    pub is_default: bool,
    pub default_effort: p::ReasoningEffort,
}

pub fn normalize_agy_model_id(model: &str) -> String {
    let model = model.trim();
    model
        .strip_prefix(&format!("{MODEL_PROVIDER}/"))
        .unwrap_or(model)
        .to_string()
}

pub fn normalize_agy_model(model: Option<String>) -> Option<String> {
    model.map(|value| normalize_agy_model_id(&value))
}

pub async fn handle_model_list(
    state: &Arc<ConnectionState>,
    _params: p::ModelListParams,
) -> p::ModelListResponse {
    let agy_bin = state.agy_pool().agy_bin().to_path_buf();
    let stock_models = get_or_refresh_models(&agy_bin).await;
    let custom_agents = get_or_refresh_agents(&agy_bin).await;

    let mut data = Vec::new();

    // 1. Stock models (vanilla agy)
    for m in &stock_models {
        data.push(build_model(
            &m.id,
            &m.name,
            &format!("Google Antigravity model: {}", m.name),
            m.is_default,
            m.default_effort,
        ));
    }

    // 2. Custom agents exposed as namespaced model slugs (<agent>/<model>)
    for agent in &custom_agents {
        for m in &stock_models {
            let namespaced_id = format!("{}/{}", agent, m.id);
            let display_name = format!("{} · {}", agent, m.name);
            data.push(build_model(
                &namespaced_id,
                &display_name,
                &format!("Run {} persona with {}", agent, m.name),
                false,
                m.default_effort,
            ));
        }
    }

    p::ModelListResponse {
        data,
        next_cursor: None,
    }
}

async fn get_or_refresh_models(agy_bin: &Path) -> Vec<DiscoveredModel> {
    if let Some(cached) = CACHED_MODELS.lock().unwrap().clone() {
        return cached;
    }
    let models = discover_models(agy_bin).await;
    *CACHED_MODELS.lock().unwrap() = Some(models.clone());
    models
}

async fn get_or_refresh_agents(agy_bin: &Path) -> Vec<String> {
    if let Some(cached) = CACHED_AGENTS.lock().unwrap().clone() {
        return cached;
    }
    let agents = discover_agents(agy_bin).await;
    *CACHED_AGENTS.lock().unwrap() = Some(agents.clone());
    agents
}

async fn discover_models(agy_bin: &Path) -> Vec<DiscoveredModel> {
    if let Ok(output) = tokio::process::Command::new(agy_bin)
        .arg("models")
        .output()
        .await
    {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            let mut list = Vec::new();
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with("Fetching") {
                    continue;
                }
                let parts: Vec<&str> = trimmed.split('\t').collect();
                if let Some(id) = parts.first() {
                    let id_str = id.trim().to_string();
                    let name_str = parts.get(1).map(|n| n.trim().to_string()).unwrap_or_else(|| id_str.clone());
                    let is_default = id_str == DEFAULT_MODEL;
                    let effort = if id_str.contains("high") || id_str.contains("thinking") {
                        p::ReasoningEffort::High
                    } else if id_str.contains("medium") {
                        p::ReasoningEffort::Medium
                    } else {
                        p::ReasoningEffort::Low
                    };
                    list.push(DiscoveredModel {
                        id: id_str,
                        name: name_str,
                        is_default,
                        default_effort: effort,
                    });
                }
            }
            if !list.is_empty() {
                return list;
            }
        }
    }

    // Static fallback if CLI models command fails
    default_stock_models()
}

async fn discover_agents(agy_bin: &Path) -> Vec<String> {
    if let Ok(output) = tokio::process::Command::new(agy_bin)
        .arg("agents")
        .output()
        .await
    {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            let mut list = Vec::new();
            for line in text.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() && !trimmed.starts_with("Fetching") {
                    list.push(trimmed.to_string());
                }
            }
            return list;
        }
    }
    vec!["better-agy".to_string(), "lean-agy".to_string()]
}

fn default_stock_models() -> Vec<DiscoveredModel> {
    vec![
        DiscoveredModel {
            id: "gemini-3.7-flash-high".to_string(),
            name: "Gemini 3.7 Flash (High)".to_string(),
            is_default: true,
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "gemini-3.7-flash-medium".to_string(),
            name: "Gemini 3.7 Flash (Medium)".to_string(),
            is_default: false,
            default_effort: p::ReasoningEffort::Medium,
        },
        DiscoveredModel {
            id: "gemini-3.8-flash-high".to_string(),
            name: "Gemini 3.8 Flash (High)".to_string(),
            is_default: false,
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "claude-sonnet-4-6".to_string(),
            name: "Claude Sonnet 4.6 (Thinking)".to_string(),
            is_default: false,
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "gemini-3.1-pro-high".to_string(),
            name: "Gemini 3.1 Pro (High)".to_string(),
            is_default: false,
            default_effort: p::ReasoningEffort::High,
        },
    ]
}

fn build_model(
    model_id: &str,
    display_name: &str,
    description: &str,
    is_default: bool,
    default_effort: p::ReasoningEffort,
) -> p::Model {
    p::Model {
        id: model_id.to_string(),
        model: model_id.to_string(),
        upgrade: None,
        upgrade_info: None,
        availability_nux: None,
        display_name: display_name.to_string(),
        description: description.to_string(),
        hidden: false,
        supported_reasoning_efforts: reasoning_options(),
        default_reasoning_effort: default_effort,
        input_modalities: vec![json!("text"), json!("image")],
        supports_personality: false,
        additional_speed_tiers: Vec::new(),
        service_tiers: standard_service_tiers(),
        is_default,
    }
}

fn standard_service_tiers() -> Vec<p::ModelServiceTier> {
    vec![p::ModelServiceTier {
        id: "standard".to_string(),
        name: "Standard".to_string(),
        description: "Default service tier".to_string(),
    }]
}

fn reasoning_options() -> Vec<p::ReasoningEffortOption> {
    vec![
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Low,
            description: "Low reasoning effort".to_string(),
        },
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Medium,
            description: "Medium reasoning effort".to_string(),
        },
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::High,
            description: "High reasoning effort".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_model_ids() {
        assert_eq!(
            normalize_agy_model_id("google/gemini-3.7-flash-high"),
            "gemini-3.7-flash-high"
        );
        assert_eq!(
            normalize_agy_model_id("gemini-3.7-flash-high"),
            "gemini-3.7-flash-high"
        );
    }
}
