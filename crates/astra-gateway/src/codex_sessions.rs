//! Read-only access to Codex's local thread catalog.
//!
//! Unlike the gateway session store, this catalog is intentionally not scoped
//! to a platform, chat, or user. It mirrors the machine-wide sessions that
//! Codex itself can resume.

use sqlx::{
    Row,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::collections::HashMap;
use std::io::{BufRead, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub const PAGE_SIZE: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexSessionSummary {
    pub id: String,
    pub name: Option<String>,
    pub title: String,
    pub last_user_message: String,
    pub cwd: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub git_branch: Option<String>,
    pub source: String,
    pub thread_source: Option<String>,
    pub updated_at_ms: i64,
    pub archived: bool,
}

#[derive(Debug, Clone)]
pub struct CodexSessionPage {
    pub items: Vec<CodexSessionSummary>,
    pub page: u32,
    pub total: u64,
    pub total_pages: u32,
}

pub fn state_db_path() -> Result<PathBuf, String> {
    let codex_home = std::env::var_os("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".codex"))
        })
        .ok_or_else(|| "CODEX_HOME 和 HOME 均未设置".to_string())?;

    newest_state_db(&codex_home)
        .ok_or_else(|| format!("{} 下没有 state_*.sqlite", codex_home.display()))
}

fn newest_state_db(codex_home: &Path) -> Option<PathBuf> {
    let mut candidates = std::fs::read_dir(codex_home)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let version = name
                .strip_prefix("state_")?
                .strip_suffix(".sqlite")?
                .parse::<u32>()
                .ok()?;
            Some((version, entry.path()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(version, _)| *version);
    candidates.pop().map(|(_, path)| path)
}

pub async fn list_sessions(page: u32) -> Result<CodexSessionPage, String> {
    let path = state_db_path()?;
    list_sessions_from(&path, page).await
}

async fn open_read_only(path: &Path) -> Result<sqlx::SqlitePool, String> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| format!("无法读取 {}: {error}", path.display()))
}

pub async fn list_sessions_from(
    path: &Path,
    requested_page: u32,
) -> Result<CodexSessionPage, String> {
    let pool = open_read_only(path).await?;
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM threads WHERE COALESCE(thread_source, '') <> 'subagent'",
    )
    .fetch_one(&pool)
    .await
    .map_err(|error| format!("读取 Codex 会话数量失败: {error}"))?;
    let total = total.max(0) as u64;
    let total_pages = if total == 0 {
        1
    } else {
        total.div_ceil(PAGE_SIZE as u64) as u32
    };
    let page = requested_page.max(1).min(total_pages);
    let offset = (page - 1) as i64 * PAGE_SIZE as i64;

    let rows = sqlx::query(
        r#"
        SELECT
            id,
            NULLIF(rollout_path, '') AS rollout_path,
            NULLIF(name, '') AS name,
            COALESCE(
                NULLIF(title, ''),
                NULLIF(name, ''),
                NULLIF(first_user_message, ''),
                '(无标题)'
            ) AS display_title,
            COALESCE(cwd, '') AS cwd,
            NULLIF(model, '') AS model,
            NULLIF(reasoning_effort, '') AS reasoning_effort,
            NULLIF(git_branch, '') AS git_branch,
            COALESCE(source, '') AS source,
            NULLIF(thread_source, '') AS thread_source,
            COALESCE(
                NULLIF(recency_at_ms, 0),
                NULLIF(updated_at_ms, 0),
                updated_at * 1000,
                0
            ) AS effective_updated_at_ms,
            COALESCE(archived, 0) AS archived
        FROM threads
        WHERE COALESCE(thread_source, '') <> 'subagent'
        ORDER BY effective_updated_at_ms DESC, id DESC
        LIMIT ? OFFSET ?
        "#,
    )
    .bind(PAGE_SIZE as i64)
    .bind(offset)
    .fetch_all(&pool)
    .await
    .map_err(|error| format!("读取 Codex 会话失败: {error}"))?;
    let indexed_names = load_session_index(path);

    let mut items = rows
        .into_iter()
        .map(|row| {
            let rollout_path: Option<String> = row.get("rollout_path");
            let id: String = row.get("id");
            let database_name: Option<String> = row.get("name");
            let name = indexed_names.get(&id).cloned().or(database_name);
            (
                CodexSessionSummary {
                    id,
                    name,
                    title: row.get("display_title"),
                    last_user_message: String::new(),
                    cwd: row.get("cwd"),
                    model: row.get("model"),
                    reasoning_effort: row.get("reasoning_effort"),
                    git_branch: row.get("git_branch"),
                    source: row.get("source"),
                    thread_source: row.get("thread_source"),
                    updated_at_ms: row.get("effective_updated_at_ms"),
                    archived: row.get::<i64, _>("archived") != 0,
                },
                rollout_path,
            )
        })
        .collect::<Vec<_>>();

    tokio::task::spawn_blocking(move || {
        for (session, rollout_path) in &mut items {
            session.last_user_message = rollout_path
                .as_deref()
                .and_then(read_last_user_message)
                .unwrap_or_default();
        }
        items
    })
    .await
    .map_err(|error| format!("读取 Codex 会话内容失败: {error}"))
    .map(|items| CodexSessionPage {
        items: items.into_iter().map(|(session, _)| session).collect(),
        page,
        total,
        total_pages,
    })
}

fn load_session_index(state_db_path: &Path) -> HashMap<String, String> {
    let Some(parent) = state_db_path.parent() else {
        return HashMap::new();
    };
    let Ok(file) = std::fs::File::open(parent.join("session_index.jsonl")) else {
        return HashMap::new();
    };
    let mut names = HashMap::new();
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if let (Some(id), Some(name)) = (
            value.get("id").and_then(|value| value.as_str()),
            value.get("thread_name").and_then(|value| value.as_str()),
        ) && !name.trim().is_empty()
        {
            names.insert(id.to_string(), name.to_string());
        }
    }
    names
}

fn read_last_user_message(path: &str) -> Option<String> {
    const CHUNK_SIZE: u64 = 64 * 1024;

    let mut file = std::fs::File::open(path).ok()?;
    let mut position = file.seek(SeekFrom::End(0)).ok()?;
    let mut pending = Vec::new();

    while position > 0 {
        let size = position.min(CHUNK_SIZE);
        position -= size;
        file.seek(SeekFrom::Start(position)).ok()?;
        let mut data = vec![0; size as usize];
        file.read_exact(&mut data).ok()?;
        data.extend_from_slice(&pending);

        if let Some(first_newline) = data.iter().position(|byte| *byte == b'\n') {
            for line in data[first_newline + 1..].rsplit(|byte| *byte == b'\n') {
                if let Some(message) = user_message_from_rollout_line(line) {
                    return Some(message);
                }
            }
            pending = data[..first_newline].to_vec();
        } else {
            pending = data;
        }
    }

    user_message_from_rollout_line(&pending)
}

fn user_message_from_rollout_line(line: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    let payload = value.get("payload")?;
    if value.get("type").and_then(|value| value.as_str()) == Some("event_msg")
        && payload.get("type").and_then(|value| value.as_str()) == Some("user_message")
    {
        payload
            .get("message")
            .and_then(|value| value.as_str())
            .map(str::to_string)
    } else {
        None
    }
}

pub async fn find_sessions_by_prefix(prefix: &str) -> Result<Vec<String>, String> {
    let path = state_db_path()?;
    find_sessions_by_prefix_from(&path, prefix).await
}

pub async fn session_cwd(session_id: &str) -> Result<Option<String>, String> {
    let path = state_db_path()?;
    session_cwd_from(&path, session_id).await
}

pub async fn session_cwd_from(path: &Path, session_id: &str) -> Result<Option<String>, String> {
    let pool = open_read_only(path).await?;
    sqlx::query_scalar("SELECT NULLIF(cwd, '') FROM threads WHERE id = ?")
        .bind(session_id)
        .fetch_optional(&pool)
        .await
        .map_err(|error| format!("读取 Codex 会话目录失败: {error}"))
        .map(Option::flatten)
}

pub async fn find_sessions_by_prefix_from(
    path: &Path,
    prefix: &str,
) -> Result<Vec<String>, String> {
    let pool = open_read_only(path).await?;
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    sqlx::query_scalar(
        r#"
        SELECT id
        FROM threads
        WHERE id LIKE ? ESCAPE '\'
        ORDER BY COALESCE(NULLIF(recency_at_ms, 0), NULLIF(updated_at_ms, 0), updated_at * 1000, 0)
            DESC
        LIMIT 2
        "#,
    )
    .bind(format!("{escaped}%"))
    .fetch_all(&pool)
    .await
    .map_err(|error| format!("查找 Codex 会话失败: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Executor;

    async fn test_db() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state_5.sqlite");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        pool.execute(
            r#"
            CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                rollout_path TEXT,
                updated_at INTEGER NOT NULL,
                source TEXT NOT NULL,
                cwd TEXT NOT NULL,
                title TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0,
                first_user_message TEXT,
                git_branch TEXT,
                model TEXT,
                reasoning_effort TEXT,
                updated_at_ms INTEGER NOT NULL DEFAULT 0,
                thread_source TEXT,
                preview TEXT,
                recency_at_ms INTEGER NOT NULL DEFAULT 0,
                name TEXT
            )
            "#,
        )
        .await
        .unwrap();
        (dir, path)
    }

    #[tokio::test]
    async fn lists_top_level_threads_in_recency_order() {
        let (_dir, path) = test_db().await;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(false),
            )
            .await
            .unwrap();
        for index in 0..12 {
            sqlx::query(
                r#"
                INSERT INTO threads (
                    id, updated_at, source, cwd, title, archived, first_user_message,
                    model, reasoning_effort, updated_at_ms, thread_source, preview,
                    recency_at_ms, name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(format!("session-{index:02}"))
            .bind(index)
            .bind(if index == 11 { "exec" } else { "cli" })
            .bind("/workspace")
            .bind(format!("title {index}"))
            .bind(if index == 10 { 1 } else { 0 })
            .bind(format!("first {index}"))
            .bind("gpt-test")
            .bind("medium")
            .bind(index * 1000)
            .bind(if index == 11 { Some("subagent") } else { None })
            .bind(format!("preview {index}"))
            .bind(index * 1000)
            .bind(Option::<String>::None)
            .execute(&pool)
            .await
            .unwrap();
        }
        std::fs::write(
            path.parent().unwrap().join("session_index.jsonl"),
            "{\"id\":\"session-10\",\"thread_name\":\"hashbuild\"}\n",
        )
        .unwrap();

        let first = list_sessions_from(&path, 1).await.unwrap();
        assert_eq!(first.total, 11);
        assert_eq!(first.total_pages, 2);
        assert_eq!(first.items.len(), 10);
        assert_eq!(first.items[0].id, "session-10");
        assert_eq!(first.items[0].name.as_deref(), Some("hashbuild"));
        assert!(first.items[0].archived);
        assert!(
            first
                .items
                .iter()
                .all(|item| item.thread_source.as_deref() != Some("subagent"))
        );

        let second = list_sessions_from(&path, 2).await.unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].id, "session-00");
    }

    #[tokio::test]
    async fn prefix_lookup_reports_ambiguity_without_scanning_gateway_sessions() {
        let (_dir, path) = test_db().await;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(false),
            )
            .await
            .unwrap();
        for id in ["abcdefgh-one", "abcdefgh-two", "unique12-three"] {
            sqlx::query(
                "INSERT INTO threads (id, updated_at, source, cwd, title) VALUES (?, 1, 'cli', '/', 't')",
            )
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        }

        assert_eq!(
            find_sessions_by_prefix_from(&path, "abcdefgh")
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            find_sessions_by_prefix_from(&path, "unique12")
                .await
                .unwrap(),
            vec!["unique12-three"]
        );
    }

    #[tokio::test]
    async fn reads_session_working_directory() {
        let (_dir, path) = test_db().await;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(false),
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO threads (id, updated_at, source, cwd, title) VALUES ('session-cwd', 1, 'cli', '/original/worktree', 't')",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            session_cwd_from(&path, "session-cwd").await.unwrap(),
            Some("/original/worktree".into())
        );
        assert_eq!(session_cwd_from(&path, "missing").await.unwrap(), None);
    }

    #[test]
    fn selects_highest_state_database_version() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::File::create(dir.path().join("state_4.sqlite")).unwrap();
        std::fs::File::create(dir.path().join("state_12.sqlite")).unwrap();
        std::fs::File::create(dir.path().join("state_99.sqlite.bak")).unwrap();
        assert_eq!(
            newest_state_db(dir.path()).unwrap(),
            dir.path().join("state_12.sqlite")
        );
    }

    #[test]
    fn reads_the_last_user_message_from_rollout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"first\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"reply\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"last\"}}\n"
            ),
        )
        .unwrap();
        assert_eq!(
            read_last_user_message(path.to_str().unwrap()).as_deref(),
            Some("last")
        );
    }
}
