use std::collections::BTreeMap;

use crate::config::GitHubTokenConfig;

pub(crate) fn resolve_github_token_for_user(
    user_id: &str,
    tokens: &BTreeMap<String, GitHubTokenConfig>,
) -> Option<String> {
    token_from_entry(tokens.get(user_id)).or_else(|| token_from_entry(tokens.get("default")))
}

fn token_from_entry(entry: Option<&GitHubTokenConfig>) -> Option<String> {
    entry
        .map(|entry| entry.token.trim())
        .filter(|token| !token.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_token_by_wecom_user_mapping() {
        let tokens = BTreeMap::from([(
            "wecom-1".to_string(),
            GitHubTokenConfig {
                token: " ghp_secret ".into(),
                remark: Some("aptend".into()),
            },
        )]);

        let token = resolve_github_token_for_user("wecom-1", &tokens);
        assert_eq!(token.as_deref(), Some("ghp_secret"));
    }

    #[test]
    fn missing_mapping_returns_none() {
        let tokens = BTreeMap::new();

        let token = resolve_github_token_for_user("wecom-1", &tokens);
        assert_eq!(token, None);
    }

    #[test]
    fn missing_mapping_uses_default_token() {
        let tokens = BTreeMap::from([(
            "default".to_string(),
            GitHubTokenConfig {
                token: " ghp_default ".into(),
                remark: Some("matrix-meow".into()),
            },
        )]);

        let token = resolve_github_token_for_user("wecom-1", &tokens);
        assert_eq!(token.as_deref(), Some("ghp_default"));
    }

    #[test]
    fn user_mapping_wins_over_default_token() {
        let tokens = BTreeMap::from([
            (
                "default".to_string(),
                GitHubTokenConfig {
                    token: "ghp_default".into(),
                    remark: Some("matrix-meow".into()),
                },
            ),
            (
                "wecom-1".to_string(),
                GitHubTokenConfig {
                    token: "ghp_user".into(),
                    remark: Some("aptend".into()),
                },
            ),
        ]);

        let token = resolve_github_token_for_user("wecom-1", &tokens);
        assert_eq!(token.as_deref(), Some("ghp_user"));
    }
}
