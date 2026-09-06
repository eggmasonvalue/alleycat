//! Translates Codex `Vec<UserInput>` into user prompt content for `agy`.

use alleycat_codex_proto::UserInput;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InputTranslationError {
    #[error("input vector was empty (codex requires at least one item)")]
    EmptyInput,
}

pub fn translate_user_input(inputs: &[UserInput]) -> Result<String, InputTranslationError> {
    if inputs.is_empty() {
        return Err(InputTranslationError::EmptyInput);
    }

    let mut buf = String::new();
    for input in inputs {
        match input {
            UserInput::Text { text, .. } => {
                if !buf.is_empty() && !buf.ends_with('\n') {
                    buf.push('\n');
                }
                buf.push_str(text);
            }
            UserInput::Skill { name, .. } => {
                if !buf.is_empty() && !buf.ends_with('\n') {
                    buf.push('\n');
                }
                buf.push('/');
                buf.push_str(name);
            }
            UserInput::Mention { name, .. } => {
                if !buf.is_empty() && !buf.ends_with(' ') && !buf.ends_with('\n') {
                    buf.push(' ');
                }
                buf.push('@');
                buf.push_str(name);
            }
            UserInput::Image { url } => {
                if !buf.is_empty() && !buf.ends_with('\n') {
                    buf.push('\n');
                }
                buf.push_str(&format!("[Attached Image: {url}]"));
            }
            UserInput::LocalImage { path } => {
                if !buf.is_empty() && !buf.ends_with('\n') {
                    buf.push('\n');
                }
                buf.push_str(&format!("[Local Image: {}]", path.display()));
            }
        }
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_text_and_mention() {
        let inputs = vec![
            UserInput::Text {
                text: "Hello".to_string(),
                text_elements: Vec::new(),
            },
            UserInput::Mention {
                name: "agent".to_string(),
                path: "test".to_string(),
            },
        ];
        let result = translate_user_input(&inputs).expect("translated");
        assert_eq!(result, "Hello @agent");
    }
}
