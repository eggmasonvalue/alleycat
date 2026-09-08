//! `model/list` handler for Google Antigravity CLI (`agy`).
//!
//! Provides default Antigravity models and dynamic namespacing for custom
//! agent profiles (`<agent>/<model>`) to enable multi-agent support without
//! frontend changes.

use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::{ProcessLauncher, ProcessRole, ProcessSpec, StdioMode};
use alleycat_codex_proto as p;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::state::ConnectionState;

pub const MODEL_PROVIDER: &str = "google";

pub const DEFAULT_MODEL: &str = "gemini-3.7-flash";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredModel {
    pub id: String,
    pub name: String,
    pub is_default: bool,
    pub supported_efforts: Vec<p::ReasoningEffort>,
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
    let stock_models = get_or_refresh_models(state).await;
    let custom_agents = get_or_refresh_agents(state).await;

    let mut data = Vec::new();

    // 1. Stock models (vanilla agy)
    for m in &stock_models {
        data.push(build_model(
            &m.id,
            &m.name,
            &format!("Google Antigravity model: {}", m.name),
            m.is_default,
            &m.supported_efforts,
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
                &m.supported_efforts,
                m.default_effort,
            ));
        }
    }

    p::ModelListResponse {
        data,
        next_cursor: None,
    }
}

async fn get_or_refresh_models(state: &Arc<ConnectionState>) -> Vec<DiscoveredModel> {
    if let Some(cached) = state.get_cached_models() {
        return cached;
    }
    let agy_bin = state.agy_pool().agy_bin().to_path_buf();
    let launcher = state
        .launcher()
        .cloned()
        .unwrap_or_else(|| Arc::clone(state.agy_pool().launcher()));
    let models = discover_models(&launcher, &agy_bin).await;
    state.set_cached_models(models.clone());
    models
}

async fn get_or_refresh_agents(state: &Arc<ConnectionState>) -> Vec<String> {
    if let Some(cached) = state.get_cached_agents() {
        return cached;
    }
    let agy_bin = state.agy_pool().agy_bin().to_path_buf();
    let launcher = state
        .launcher()
        .cloned()
        .unwrap_or_else(|| Arc::clone(state.agy_pool().launcher()));
    let agents = discover_agents(&launcher, &agy_bin).await;
    state.set_cached_agents(agents.clone());
    agents
}

async fn run_launcher_command(
    launcher: &Arc<dyn ProcessLauncher>,
    program: &Path,
    args: &[&str],
) -> anyhow::Result<String> {
    use tokio::io::AsyncReadExt;

    let spec = ProcessSpec {
        role: ProcessRole::ToolCommand,
        program: program.to_path_buf(),
        args: args.iter().map(OsString::from).collect(),
        cwd: None,
        env: Vec::new(),
        env_clear: false,
        stdin: StdioMode::Null,
        stdout: StdioMode::Piped,
        stderr: StdioMode::Piped,
    };

    let mut child = launcher.launch(spec).await?;
    let mut stdout = child
        .take_stdout()
        .ok_or_else(|| anyhow::anyhow!("no stdout pipe from launched process"))?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stdout.read_to_end(&mut buf))
        .await
        .map_err(|_| anyhow::anyhow!("timed out reading agy command output"))??;
    let _ = child.wait().await;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

async fn discover_models(
    launcher: &Arc<dyn ProcessLauncher>,
    agy_bin: &Path,
) -> Vec<DiscoveredModel> {
    match run_launcher_command(launcher, agy_bin, &["models"]).await {
        Ok(text) => parse_agy_models(&text),
        Err(err) => {
            tracing::warn!(?err, "failed to discover agy models via launcher, using stock models");
            default_stock_models()
        }
    }
}

async fn discover_agents(
    launcher: &Arc<dyn ProcessLauncher>,
    agy_bin: &Path,
) -> Vec<String> {
    if let Ok(text) = run_launcher_command(launcher, agy_bin, &["agents"]).await {
        let mut list = Vec::new();
        for line in text.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("Fetching") {
                list.push(trimmed.to_string());
            }
        }
        if !list.is_empty() {
            return list;
        }
    }
    vec!["better-agy".to_string(), "lean-agy".to_string()]
}

pub fn parse_agy_models(text: &str) -> Vec<DiscoveredModel> {
    struct ModelEntry {
        base_id: String,
        base_name: String,
        efforts: Vec<p::ReasoningEffort>,
    }

    let mut entries: Vec<ModelEntry> = Vec::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("Fetching") {
            continue;
        }
        let parts: Vec<&str> = trimmed.split('\t').collect();
        let id_str = match parts.first() {
            Some(id) => id.trim(),
            None => continue,
        };
        let raw_name = parts.get(1).map(|n| n.trim()).unwrap_or(id_str);

        let (base_id, effort) = if let Some(base) = id_str.strip_suffix("-high") {
            (base, Some(p::ReasoningEffort::High))
        } else if let Some(base) = id_str.strip_suffix("-medium") {
            (base, Some(p::ReasoningEffort::Medium))
        } else if let Some(base) = id_str.strip_suffix("-low") {
            (base, Some(p::ReasoningEffort::Low))
        } else {
            (id_str, None)
        };

        let base_name = if let Some(stripped) = raw_name.strip_suffix(" (High)") {
            stripped
        } else if let Some(stripped) = raw_name.strip_suffix(" (Medium)") {
            stripped
        } else if let Some(stripped) = raw_name.strip_suffix(" (Low)") {
            stripped
        } else {
            raw_name
        };

        if let Some(existing) = entries.iter_mut().find(|e| e.base_id == base_id) {
            if let Some(eff) = effort {
                if !existing.efforts.contains(&eff) {
                    existing.efforts.push(eff);
                }
            }
        } else {
            let mut efforts = Vec::new();
            if let Some(eff) = effort {
                efforts.push(eff);
            }
            entries.push(ModelEntry {
                base_id: base_id.to_string(),
                base_name: base_name.to_string(),
                efforts,
            });
        }
    }

    if entries.is_empty() {
        return default_stock_models();
    }

    entries
        .into_iter()
        .map(|entry| {
            let mut sorted_efforts = Vec::new();
            if entry.efforts.contains(&p::ReasoningEffort::Low) {
                sorted_efforts.push(p::ReasoningEffort::Low);
            }
            if entry.efforts.contains(&p::ReasoningEffort::Medium) {
                sorted_efforts.push(p::ReasoningEffort::Medium);
            }
            if entry.efforts.contains(&p::ReasoningEffort::High) {
                sorted_efforts.push(p::ReasoningEffort::High);
            }

            let default_effort = if sorted_efforts.contains(&p::ReasoningEffort::High) {
                p::ReasoningEffort::High
            } else if sorted_efforts.contains(&p::ReasoningEffort::Medium) {
                p::ReasoningEffort::Medium
            } else if sorted_efforts.contains(&p::ReasoningEffort::Low) {
                p::ReasoningEffort::Low
            } else {
                p::ReasoningEffort::High
            };

            let is_default = entry.base_id == DEFAULT_MODEL;

            DiscoveredModel {
                id: entry.base_id,
                name: entry.base_name,
                is_default,
                supported_efforts: sorted_efforts,
                default_effort,
            }
        })
        .collect()
}

pub fn default_stock_models() -> Vec<DiscoveredModel> {
    vec![
        DiscoveredModel {
            id: "gemini-3.7-flash".to_string(),
            name: "Gemini 3.7 Flash".to_string(),
            is_default: true,
            supported_efforts: vec![
                p::ReasoningEffort::Low,
                p::ReasoningEffort::Medium,
                p::ReasoningEffort::High,
            ],
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "gemini-3.8-flash".to_string(),
            name: "Gemini 3.8 Flash".to_string(),
            is_default: false,
            supported_efforts: vec![
                p::ReasoningEffort::Low,
                p::ReasoningEffort::Medium,
                p::ReasoningEffort::High,
            ],
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "gemini-3.6-flash".to_string(),
            name: "Gemini 3.6 Flash".to_string(),
            is_default: false,
            supported_efforts: vec![
                p::ReasoningEffort::Low,
                p::ReasoningEffort::Medium,
                p::ReasoningEffort::High,
            ],
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "gemini-3.1-pro".to_string(),
            name: "Gemini 3.1 Pro".to_string(),
            is_default: false,
            supported_efforts: vec![
                p::ReasoningEffort::Low,
                p::ReasoningEffort::High,
            ],
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "claude-sonnet-4-6".to_string(),
            name: "Claude Sonnet 4.6 (Thinking)".to_string(),
            is_default: false,
            supported_efforts: Vec::new(),
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "claude-opus-4-6-thinking".to_string(),
            name: "Claude Opus 4.6 (Thinking)".to_string(),
            is_default: false,
            supported_efforts: Vec::new(),
            default_effort: p::ReasoningEffort::High,
        },
        DiscoveredModel {
            id: "gpt-oss-120b".to_string(),
            name: "GPT-OSS 120B".to_string(),
            is_default: false,
            supported_efforts: vec![
                p::ReasoningEffort::Medium,
            ],
            default_effort: p::ReasoningEffort::Medium,
        },
    ]
}

fn build_model(
    model_id: &str,
    display_name: &str,
    description: &str,
    is_default: bool,
    supported_efforts: &[p::ReasoningEffort],
    default_effort: p::ReasoningEffort,
) -> p::Model {
    let reasoning_options = supported_efforts
        .iter()
        .map(|effort| p::ReasoningEffortOption {
            reasoning_effort: *effort,
            description: format!("{effort:?} reasoning effort"),
        })
        .collect();

    p::Model {
        id: model_id.to_string(),
        model: model_id.to_string(),
        upgrade: None,
        upgrade_info: None,
        availability_nux: None,
        display_name: display_name.to_string(),
        description: description.to_string(),
        hidden: false,
        supported_reasoning_efforts: reasoning_options,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_model_ids() {
        assert_eq!(
            normalize_agy_model_id("google/gemini-3.7-flash"),
            "gemini-3.7-flash"
        );
        assert_eq!(
            normalize_agy_model_id("gemini-3.7-flash"),
            "gemini-3.7-flash"
        );
    }

    #[test]
    fn parses_canonical_models_from_cli_output() {
        let sample = "\
Fetching available models...
gemini-3.8-flash-high\tGemini 3.8 Flash (High)
gemini-3.8-flash-medium\tGemini 3.8 Flash (Medium)
gemini-3.8-flash-low\tGemini 3.8 Flash (Low)
gemini-3.7-flash-high\tGemini 3.7 Flash (High)
gemini-3.7-flash-medium\tGemini 3.7 Flash (Medium)
gemini-3.7-flash-low\tGemini 3.7 Flash (Low)
gemini-3.1-pro-high\tGemini 3.1 Pro (High)
gemini-3.1-pro-low\tGemini 3.1 Pro (Low)
claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)
claude-opus-4-6-thinking\tClaude Opus 4.6 (Thinking)
gpt-oss-120b-medium\tGPT-OSS 120B (Medium)
";
        let models = parse_agy_models(sample);
        assert_eq!(models.len(), 6);

        // gemini-3.8-flash
        let g38 = models.iter().find(|m| m.id == "gemini-3.8-flash").unwrap();
        assert_eq!(g38.name, "Gemini 3.8 Flash");
        assert_eq!(
            g38.supported_efforts,
            vec![
                p::ReasoningEffort::Low,
                p::ReasoningEffort::Medium,
                p::ReasoningEffort::High
            ]
        );
        assert_eq!(g38.default_effort, p::ReasoningEffort::High);
        assert!(!g38.is_default);

        // gemini-3.7-flash is default
        let g37 = models.iter().find(|m| m.id == "gemini-3.7-flash").unwrap();
        assert_eq!(g37.name, "Gemini 3.7 Flash");
        assert!(g37.is_default);

        // claude-sonnet-4-6 has no efforts (fixed thinking)
        let claude = models.iter().find(|m| m.id == "claude-sonnet-4-6").unwrap();
        assert_eq!(claude.name, "Claude Sonnet 4.6 (Thinking)");
        assert!(claude.supported_efforts.is_empty());

        // gpt-oss-120b has only medium
        let gpt = models.iter().find(|m| m.id == "gpt-oss-120b").unwrap();
        assert_eq!(gpt.name, "GPT-OSS 120B");
        assert_eq!(gpt.supported_efforts, vec![p::ReasoningEffort::Medium]);
        assert_eq!(gpt.default_effort, p::ReasoningEffort::Medium);
    }
}
