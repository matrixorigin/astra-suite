//! Built-in tool catalog. Currently one tool: [`bash`]. Reserved extension
//! slot: future additions (Read / Grep / ...) would register here.

use serde::Serialize;

pub mod bash;

pub const BASH_TOOL_NAME: &str = "bash";

/// Anthropic tool schema sent in the `tools` field of a Messages request.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Schema for the built-in bash tool. Matches what Claude Code CLI
/// advertises so skill prompts written for the CLI keep working.
pub fn bash_spec() -> ToolSpec {
    ToolSpec {
        name: BASH_TOOL_NAME.to_string(),
        description: "Execute a bash command on the gateway host. \
            Runs under `/bin/bash -c`. Times out after the configured \
            limit; output is truncated past the configured byte cap. \
            Use for curl / jq / simple shell pipelines; avoid long-running \
            interactive commands."
            .to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to run."
                }
            },
            "required": ["command"]
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bash_spec_has_expected_shape() {
        let spec = bash_spec();
        assert_eq!(spec.name, "bash");
        assert!(!spec.description.is_empty());
        assert_eq!(
            spec.input_schema["properties"]["command"]["type"],
            serde_json::Value::String("string".into())
        );
        assert_eq!(
            spec.input_schema["required"],
            serde_json::json!(["command"])
        );
    }
}
