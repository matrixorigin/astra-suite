#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisionCapability {
    Supported,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
struct ModelVisionRule {
    pattern: &'static str,
    capability: VisionCapability,
}

const MODEL_VISION_RULES: &[ModelVisionRule] = &[
    // OpenAI multimodal families.
    supported("gpt-4o"),
    supported("gpt-4.1"),
    supported("gpt-5"),
    supported("o3"),
    supported("o4"),
    // Anthropic Claude 3+ families.
    supported("claude-3"),
    supported("claude-opus-4"),
    supported("claude-sonnet-4"),
    // Chinese multimodal model families.
    supported("qwen2.5-vl"),
    supported("qwen2-vl"),
    supported("qwen-vl"),
    supported("qvq"),
    supported("qwen-omni"),
    supported("glm-4v"),
    supported("glm-v"),
    supported("glm-vision"),
    supported("kimi-vl"),
    // Known text-only families commonly exposed through Anthropic-compatible CLIs.
    unsupported("qwen3"),
    unsupported("qwen-max"),
    unsupported("deepseek"),
    unsupported("glm-5"),
    unsupported("glm-4.5"),
    unsupported("kimi-k2"),
    unsupported("minimax-m"),
];

const fn supported(pattern: &'static str) -> ModelVisionRule {
    ModelVisionRule {
        pattern,
        capability: VisionCapability::Supported,
    }
}

const fn unsupported(pattern: &'static str) -> ModelVisionRule {
    ModelVisionRule {
        pattern,
        capability: VisionCapability::Unsupported,
    }
}

pub fn vision_capability(model: Option<&str>) -> VisionCapability {
    vision_capability_with_supported_models(model, &[])
}

pub fn vision_capability_with_supported_models(
    model: Option<&str>,
    supported_models: &[String],
) -> VisionCapability {
    let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) else {
        return VisionCapability::Unknown;
    };
    let normalized = model.to_ascii_lowercase();
    if supported_models.iter().any(|pattern| {
        let pattern = pattern.trim().to_ascii_lowercase();
        !pattern.is_empty() && normalized.contains(&pattern)
    }) {
        return VisionCapability::Supported;
    }
    MODEL_VISION_RULES
        .iter()
        .find(|rule| normalized.contains(rule.pattern))
        .map(|rule| rule.capability)
        .unwrap_or(VisionCapability::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vision_models_are_supported() {
        assert_eq!(
            vision_capability(Some("claude-sonnet-4-20250514")),
            VisionCapability::Supported
        );
        assert_eq!(
            vision_capability(Some("qwen2.5-vl-72b-instruct")),
            VisionCapability::Supported
        );
        assert_eq!(
            vision_capability(Some("gpt-5.5")),
            VisionCapability::Supported
        );
    }

    #[test]
    fn known_text_only_models_are_unsupported() {
        assert_eq!(
            vision_capability(Some("qwen3.7-max")),
            VisionCapability::Unsupported
        );
        assert_eq!(
            vision_capability(Some("deepseek-v4-pro")),
            VisionCapability::Unsupported
        );
        assert_eq!(
            vision_capability(Some("glm-5.1")),
            VisionCapability::Unsupported
        );
    }

    #[test]
    fn unknown_or_default_models_are_unknown() {
        assert_eq!(vision_capability(None), VisionCapability::Unknown);
        assert_eq!(
            vision_capability(Some("vendor-model")),
            VisionCapability::Unknown
        );
    }

    #[test]
    fn configured_supported_models_override_builtin_rules() {
        let models = vec!["qwen3.7".into()];
        assert_eq!(
            vision_capability_with_supported_models(Some("qwen3.7-max"), &models),
            VisionCapability::Supported
        );
    }
}
