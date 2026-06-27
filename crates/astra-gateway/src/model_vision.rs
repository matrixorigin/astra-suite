#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisionCapability {
    Supported,
    Unsupported,
    Unknown,
}

const BUILTIN_SUPPORTED_MODELS: &[&str] = &[
    "gpt-4o",
    "gpt-4.1",
    "gpt-5",
    "o3",
    "o4",
    "claude-3",
    "claude-fable-5",
    "claude-opus-4",
    "claude-sonnet-4",
    "qwen2.5-vl",
    "qwen2-vl",
    "qwen-vl",
    "qvq",
    "qwen-omni",
    "glm-4v",
    "glm-v",
    "glm-vision",
    "kimi-vl",
];
const BUILTIN_UNSUPPORTED_MODELS: &[&str] = &[
    "qwen3",
    "qwen-max",
    "deepseek",
    "glm-5",
    "glm-4.5",
    "kimi-k2",
    "minimax-m",
];

pub fn vision_capability_with_supported_models(
    model: Option<&str>,
    supported_models: &[String],
) -> VisionCapability {
    let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) else {
        return VisionCapability::Unknown;
    };
    let normalized = model.to_ascii_lowercase();
    if supported_models
        .iter()
        .map(|pattern| pattern.trim().to_ascii_lowercase())
        .any(|pattern| !pattern.is_empty() && normalized.contains(&pattern))
        || BUILTIN_SUPPORTED_MODELS
            .iter()
            .any(|pattern| normalized.contains(pattern))
    {
        return VisionCapability::Supported;
    }
    if BUILTIN_UNSUPPORTED_MODELS
        .iter()
        .any(|pattern| normalized.contains(pattern))
    {
        VisionCapability::Unsupported
    } else {
        VisionCapability::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vision_models_are_supported() {
        for model in [
            "claude-sonnet-4-20250514",
            "claude-fable-5-20260609",
            "qwen2.5-vl-72b-instruct",
            "gpt-5.5",
        ] {
            assert_eq!(
                vision_capability_with_supported_models(Some(model), &[]),
                VisionCapability::Supported
            );
        }
    }

    #[test]
    fn known_text_only_models_are_unsupported() {
        for model in ["qwen3.7-max", "deepseek-v4-pro", "glm-5.1"] {
            assert_eq!(
                vision_capability_with_supported_models(Some(model), &[]),
                VisionCapability::Unsupported
            );
        }
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
