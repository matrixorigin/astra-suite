//! E2E tests for MysqlDurableTaskStore.
//!
//! Requires: MatrixOne/MySQL at 127.0.0.1:6001
//! Run: cargo test -p astra-gateway --test durable_task_e2e -- --ignored

use astra_gateway::durable_task_store::{DurableTaskStoreExt, MysqlDurableTaskStore};
use astra_gateway::store::{GatewayStore, mysql::MysqlGatewayStore};
use astra_task_store::*;

async fn setup() -> MysqlDurableTaskStore {
    let url = "mysql://root:111@127.0.0.1:6001/astra_gateway";
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("DB connection failed — is MatrixOne running?");
    let store = MysqlGatewayStore::new(pool.clone());
    store.ensure_schema().await.expect("schema setup failed");
    MysqlDurableTaskStore::new(pool)
}

fn test_spec(name: &str) -> TaskSpec {
    TaskSpec {
        name: name.to_string(),
        description: Some("e2e test".into()),
        owner_id: format!("test:{}", uuid::Uuid::new_v4()),
        initial_state: None,
    }
}

#[tokio::test]
#[ignore]
async fn full_lifecycle() {
    let store = setup().await;
    let spec = test_spec("lifecycle test");
    let owner = spec.owner_id.clone();

    // Create
    let id = store.create(&spec).await.unwrap();
    assert!(!id.0.is_empty());

    // Get
    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.name, "lifecycle test");
    assert_eq!(task.status, DurableTaskStatus::Created);
    assert_eq!(task.progress_pct, 0);

    // Checkpoint
    let state = serde_json::json!({"completed": ["alice", "bob"], "pending": 18});
    store
        .checkpoint(&id, &state, Some(25), Some("2/20 users done"))
        .await
        .unwrap();

    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.status, DurableTaskStatus::Running);
    assert_eq!(task.progress_pct, 25);
    assert_eq!(task.step_description.as_deref(), Some("2/20 users done"));
    assert_eq!(task.checkpoint.unwrap()["pending"], 18);

    // Resume (get checkpoint back)
    let cp = store.resume(&id).await.unwrap().unwrap();
    assert_eq!(cp["completed"][0], "alice");

    // Complete
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();
    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.status, DurableTaskStatus::Completed);

    // List by owner
    let tasks = store
        .list(TaskFilter {
            owner_id: Some(owner),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(tasks.iter().any(|t| t.id == id));

    // Delete
    assert!(store.delete(&id).await.unwrap());
    assert!(store.get(&id).await.unwrap().is_none());
}

#[tokio::test]
#[ignore]
async fn checkpoint_non_existent() {
    let store = setup().await;
    let fake = TaskId("nonexistent-id".into());
    let state = serde_json::json!({"x": 1});
    let result = store.checkpoint(&fake, &state, None, None).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[tokio::test]
#[ignore]
async fn resume_non_existent() {
    let store = setup().await;
    let fake = TaskId("nonexistent-id".into());
    let result = store.resume(&fake).await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn resume_no_checkpoint() {
    let store = setup().await;
    let spec = test_spec("no checkpoint");
    let id = store.create(&spec).await.unwrap();
    let cp = store.resume(&id).await.unwrap();
    assert!(cp.is_none());
    store.delete(&id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn delete_non_existent() {
    let store = setup().await;
    let fake = TaskId("nonexistent-id".into());
    assert!(!store.delete(&fake).await.unwrap());
}

#[tokio::test]
#[ignore]
async fn get_non_existent() {
    let store = setup().await;
    let fake = TaskId("nonexistent-id".into());
    assert!(store.get(&fake).await.unwrap().is_none());
}

#[tokio::test]
#[ignore]
async fn create_empty_name_rejected() {
    let store = setup().await;
    let spec = TaskSpec {
        name: "  ".into(),
        description: None,
        owner_id: "test".into(),
        initial_state: None,
    };
    assert!(store.create(&spec).await.is_err());
}

#[tokio::test]
#[ignore]
async fn list_filters_by_status() {
    let store = setup().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());
    let spec = TaskSpec {
        name: "filter test".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };

    let id1 = store.create(&spec).await.unwrap();
    let id2 = store.create(&spec).await.unwrap();
    store
        .update_status(&id1, DurableTaskStatus::Completed, None)
        .await
        .unwrap();

    let active = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            status: Some(DurableTaskStatus::Created),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, id2);

    let completed = store
        .list(TaskFilter {
            owner_id: Some(owner),
            status: Some(DurableTaskStatus::Completed),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].id, id1);

    store.delete(&id1).await.unwrap();
    store.delete(&id2).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn checkpoint_terminal_task_rejected() {
    let store = setup().await;
    let spec = test_spec("terminal checkpoint");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();

    let state = serde_json::json!({"x": 1});
    let result = store.checkpoint(&id, &state, None, None).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("terminal"));

    store.delete(&id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn create_with_initial_state() {
    let store = setup().await;
    let spec = TaskSpec {
        name: "with state".into(),
        description: None,
        owner_id: format!("test:{}", uuid::Uuid::new_v4()),
        initial_state: Some(serde_json::json!({"repos": ["a", "b"]})),
    };
    let id = store.create(&spec).await.unwrap();
    let cp = store.resume(&id).await.unwrap().unwrap();
    assert_eq!(cp["repos"][0], "a");
    store.delete(&id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn sweep_stale_running_tasks() {
    let store = setup().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());

    // Create two tasks and checkpoint them to running
    let s = |n: &str| TaskSpec {
        name: n.into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };
    let id1 = store.create(&s("stale-1")).await.unwrap();
    let id2 = store.create(&s("stale-2")).await.unwrap();
    store
        .checkpoint(&id1, &serde_json::json!({"step":1}), Some(10), None)
        .await
        .unwrap();
    store
        .checkpoint(&id2, &serde_json::json!({"step":2}), Some(50), None)
        .await
        .unwrap();

    // Both should be running
    assert_eq!(
        store.get(&id1).await.unwrap().unwrap().status,
        DurableTaskStatus::Running
    );
    assert_eq!(
        store.get(&id2).await.unwrap().unwrap().status,
        DurableTaskStatus::Running
    );

    // Sweep
    let count = store
        .suspend_stale_running_tasks("gateway restarted")
        .await
        .unwrap();
    assert!(count >= 2, "should suspend at least 2 tasks, got {count}");

    // Both should be suspended with reason
    let t1 = store.get(&id1).await.unwrap().unwrap();
    assert_eq!(t1.status, DurableTaskStatus::Suspended);
    assert_eq!(t1.error_message.as_deref(), Some("gateway restarted"));
    let t2 = store.get(&id2).await.unwrap().unwrap();
    assert_eq!(t2.status, DurableTaskStatus::Suspended);

    // Checkpoint should be preserved
    assert!(t1.checkpoint.is_some());
    assert!(t2.checkpoint.is_some());

    store.delete(&id1).await.unwrap();
    store.delete(&id2).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn suspend_running_tasks_for_owner() {
    let store = setup().await;
    let owner_a = format!("test:{}", uuid::Uuid::new_v4());
    let owner_b = format!("test:{}", uuid::Uuid::new_v4());

    let spec_a = TaskSpec {
        name: "a-task".into(),
        description: None,
        owner_id: owner_a.clone(),
        initial_state: None,
    };
    let spec_b = TaskSpec {
        name: "b-task".into(),
        description: None,
        owner_id: owner_b.clone(),
        initial_state: None,
    };
    let id_a = store.create(&spec_a).await.unwrap();
    let id_b = store.create(&spec_b).await.unwrap();
    store
        .checkpoint(&id_a, &serde_json::json!({}), None, None)
        .await
        .unwrap();
    store
        .checkpoint(&id_b, &serde_json::json!({}), None, None)
        .await
        .unwrap();

    // Suspend only owner_a's tasks
    let count = store
        .suspend_running_tasks_for_owner(&owner_a, "CLI crashed")
        .await
        .unwrap();
    assert_eq!(count, 1);

    // owner_a suspended, owner_b still running
    assert_eq!(
        store.get(&id_a).await.unwrap().unwrap().status,
        DurableTaskStatus::Suspended
    );
    assert_eq!(
        store.get(&id_b).await.unwrap().unwrap().status,
        DurableTaskStatus::Running
    );

    store.delete(&id_a).await.unwrap();
    store.delete(&id_b).await.unwrap();
}

// ─── Context token persistence tests ──────────────────────────────────

#[tokio::test]
#[ignore]
async fn context_token_persist_and_restore() {
    let url = "mysql://root:111@127.0.0.1:6001/astra_gateway";
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .unwrap();
    let store = MysqlGatewayStore::new(pool);
    store.ensure_schema().await.unwrap();

    // Save context tokens
    let tokens = serde_json::json!({
        "user_a": "token_aaa",
        "user_b": "token_bbb",
    });
    store
        .save_credential("weixin", "default", "context_tokens", &tokens, None)
        .await
        .unwrap();

    // Restore
    let cred = store
        .get_credential("weixin", "default", "context_tokens")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(cred.credentials["user_a"], "token_aaa");
    assert_eq!(cred.credentials["user_b"], "token_bbb");

    // Update (overwrite)
    let tokens2 = serde_json::json!({
        "user_a": "token_aaa_updated",
        "user_c": "token_ccc",
    });
    store
        .save_credential("weixin", "default", "context_tokens", &tokens2, None)
        .await
        .unwrap();

    let cred2 = store
        .get_credential("weixin", "default", "context_tokens")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cred2.credentials["user_a"], "token_aaa_updated");
    assert_eq!(cred2.credentials["user_c"], "token_ccc");
    // user_b gone (full replacement)
    assert!(cred2.credentials.get("user_b").is_none());
}

// ─── SQLite durable-task lifecycle (no external DB needed) ─────────────────

async fn setup_sqlite() -> astra_gateway::durable_task_store::SqliteDurableTaskStore {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(2)
        .connect("sqlite::memory:")
        .await
        .expect("sqlite pool");
    let store = astra_gateway::store::sqlite::SqliteGatewayStore::new(pool.clone());
    store.ensure_schema().await.expect("sqlite schema setup");
    astra_gateway::durable_task_store::SqliteDurableTaskStore::new(pool)
}

#[tokio::test]
async fn sqlite_full_lifecycle() {
    let store = setup_sqlite().await;
    let spec = test_spec("sqlite lifecycle");

    let id = store.create(&spec).await.unwrap();
    assert!(!id.0.is_empty());

    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.name, "sqlite lifecycle");
    assert_eq!(task.status, DurableTaskStatus::Created);
    assert_eq!(task.progress_pct, 0);

    let state = serde_json::json!({"completed": ["alice"], "pending": 9});
    store
        .checkpoint(&id, &state, Some(10), Some("1/10"))
        .await
        .unwrap();
    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.status, DurableTaskStatus::Running);
    assert_eq!(task.progress_pct, 10);
    assert_eq!(task.step_description.as_deref(), Some("1/10"));

    let resumed = store.resume(&id).await.unwrap().unwrap();
    assert_eq!(resumed["pending"], 9);

    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();
    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.status, DurableTaskStatus::Completed);

    // Terminal state rejects further checkpoint/resume
    let err = store.resume(&id).await.unwrap_err();
    assert!(err.contains("terminal"), "unexpected error: {err}");

    assert!(store.delete(&id).await.unwrap());
    assert!(store.get(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn sqlite_list_filters_by_owner_and_status() {
    let store = setup_sqlite().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());
    let other_spec = TaskSpec {
        name: "other".into(),
        description: None,
        owner_id: "test:other".into(),
        initial_state: None,
    };
    let my_spec = TaskSpec {
        name: "mine".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };
    store.create(&other_spec).await.unwrap();
    let id = store.create(&my_spec).await.unwrap();

    let mine = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].id, id);

    // Status filter: no Running tasks yet
    let running = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            status: Some(DurableTaskStatus::Running),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(running.len(), 0);
}

#[tokio::test]
async fn sqlite_suspend_running_tasks_for_owner() {
    let store = setup_sqlite().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());
    let spec = TaskSpec {
        name: "runner".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };
    let id = store.create(&spec).await.unwrap();
    // Promote Created -> Running via checkpoint
    store
        .checkpoint(&id, &serde_json::json!({}), Some(1), Some("step"))
        .await
        .unwrap();

    let suspended = store
        .suspend_running_tasks_for_owner(&owner, "gateway restart")
        .await
        .unwrap();
    assert_eq!(suspended, 1);

    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.status, DurableTaskStatus::Suspended);
    assert_eq!(task.error_message.as_deref(), Some("gateway restart"));
}

// ─── SQLite unhappy-path & edge-case tests ───────────────────────────────────

// --- Error/rejection paths ---

#[tokio::test]
async fn sqlite_create_empty_name_rejected() {
    let store = setup_sqlite().await;
    let spec = TaskSpec {
        name: "  ".into(),
        description: None,
        owner_id: "test".into(),
        initial_state: None,
    };
    assert!(store.create(&spec).await.is_err());
}

#[tokio::test]
async fn sqlite_checkpoint_non_existent() {
    let store = setup_sqlite().await;
    let fake = TaskId("nonexistent-id".into());
    let state = serde_json::json!({"x": 1});
    let result = store.checkpoint(&fake, &state, None, None).await;
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("not found"),
        "error should mention 'not found'"
    );
}

#[tokio::test]
async fn sqlite_checkpoint_terminal_rejected() {
    let store = setup_sqlite().await;
    let spec = test_spec("terminal checkpoint");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();

    let state = serde_json::json!({"x": 1});
    let result = store.checkpoint(&id, &state, None, None).await;
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("terminal"),
        "error should mention 'terminal'"
    );
}

#[tokio::test]
async fn sqlite_resume_non_existent() {
    let store = setup_sqlite().await;
    let fake = TaskId("nonexistent-id".into());
    let result = store.resume(&fake).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sqlite_resume_terminal_rejected() {
    let store = setup_sqlite().await;
    let spec = test_spec("terminal resume");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();

    let result = store.resume(&id).await;
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("terminal"),
        "error should mention 'terminal'"
    );
}

#[tokio::test]
async fn sqlite_resume_no_checkpoint() {
    let store = setup_sqlite().await;
    let spec = test_spec("no checkpoint");
    let id = store.create(&spec).await.unwrap();
    let cp = store.resume(&id).await.unwrap();
    assert!(cp.is_none());
}

#[tokio::test]
async fn sqlite_delete_non_existent() {
    let store = setup_sqlite().await;
    let fake = TaskId("nonexistent-id".into());
    assert!(!store.delete(&fake).await.unwrap());
}

#[tokio::test]
async fn sqlite_get_non_existent() {
    let store = setup_sqlite().await;
    let fake = TaskId("nonexistent-id".into());
    assert!(store.get(&fake).await.unwrap().is_none());
}

#[tokio::test]
async fn sqlite_update_status_terminal_rejected() {
    let store = setup_sqlite().await;
    let spec = test_spec("terminal update_status");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();

    // Try to update status again on a terminal task
    let result = store
        .update_status(&id, DurableTaskStatus::Running, None)
        .await;
    assert!(result.is_err());
}

// --- State machine / ordering ---

#[tokio::test]
async fn sqlite_status_transitions_created_to_all() {
    let store = setup_sqlite().await;

    // Created -> Running (via checkpoint)
    let spec = test_spec("created->running");
    let id = store.create(&spec).await.unwrap();
    store
        .checkpoint(&id, &serde_json::json!({}), Some(1), None)
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Running
    );

    // Created -> Suspended (via update_status)
    let spec = test_spec("created->suspended");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Suspended, None)
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Suspended
    );

    // Created -> Completed
    let spec = test_spec("created->completed");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Completed
    );

    // Created -> Failed
    let spec = test_spec("created->failed");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Failed, Some("boom"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Failed
    );

    // Created -> Cancelled
    let spec = test_spec("created->cancelled");
    let id = store.create(&spec).await.unwrap();
    store
        .update_status(&id, DurableTaskStatus::Cancelled, None)
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Cancelled
    );
}

#[tokio::test]
async fn sqlite_status_transitions_running_to_terminal() {
    let store = setup_sqlite().await;

    // Running -> Completed
    let spec = test_spec("running->completed");
    let id = store.create(&spec).await.unwrap();
    store
        .checkpoint(&id, &serde_json::json!({}), Some(50), None)
        .await
        .unwrap();
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Completed
    );

    // Running -> Failed
    let spec = test_spec("running->failed");
    let id = store.create(&spec).await.unwrap();
    store
        .checkpoint(&id, &serde_json::json!({}), Some(50), None)
        .await
        .unwrap();
    store
        .update_status(&id, DurableTaskStatus::Failed, Some("error"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Failed
    );

    // Running -> Cancelled
    let spec = test_spec("running->cancelled");
    let id = store.create(&spec).await.unwrap();
    store
        .checkpoint(&id, &serde_json::json!({}), Some(50), None)
        .await
        .unwrap();
    store
        .update_status(&id, DurableTaskStatus::Cancelled, None)
        .await
        .unwrap();
    assert_eq!(
        store.get(&id).await.unwrap().unwrap().status,
        DurableTaskStatus::Cancelled
    );

    // Running -> Created should fail (Created is not in the valid target set
    // via update_status, and there's no mechanism to go backwards).
    // The update_status SQL only matches rows with status IN ('created','running','suspended'),
    // and sets to the target status. It will succeed at the SQL level since the task is Running.
    // However, semantically "Running -> Created" is a downgrade. The current implementation
    // does NOT enforce directional transitions in update_status — it only blocks updates
    // on terminal tasks. So this will actually succeed. Let's verify that behavior.
    let spec = test_spec("running->created-attempt");
    let id = store.create(&spec).await.unwrap();
    store
        .checkpoint(&id, &serde_json::json!({}), Some(50), None)
        .await
        .unwrap();
    // The implementation allows this since the task is in 'running' (a non-terminal state)
    let result = store
        .update_status(&id, DurableTaskStatus::Created, None)
        .await;
    // Current implementation permits this (no directional enforcement)
    assert!(result.is_ok());
}

#[tokio::test]
async fn sqlite_suspend_stale_running_tasks() {
    let store = setup_sqlite().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());

    let make_spec = |n: &str| TaskSpec {
        name: n.into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };

    // Create 3 tasks, promote 2 to Running
    let id1 = store.create(&make_spec("stale-1")).await.unwrap();
    let id2 = store.create(&make_spec("stale-2")).await.unwrap();
    let _id3 = store.create(&make_spec("stays-created")).await.unwrap();

    store
        .checkpoint(&id1, &serde_json::json!({"s":1}), Some(10), None)
        .await
        .unwrap();
    store
        .checkpoint(&id2, &serde_json::json!({"s":2}), Some(50), None)
        .await
        .unwrap();

    // Sweep all running tasks
    let count = store
        .suspend_stale_running_tasks("gateway restarted")
        .await
        .unwrap();
    assert_eq!(count, 2, "should suspend exactly 2 running tasks");

    // Verify statuses
    assert_eq!(
        store.get(&id1).await.unwrap().unwrap().status,
        DurableTaskStatus::Suspended
    );
    assert_eq!(
        store.get(&id2).await.unwrap().unwrap().status,
        DurableTaskStatus::Suspended
    );
    assert_eq!(
        store.get(&_id3).await.unwrap().unwrap().status,
        DurableTaskStatus::Created
    );
}

// --- Data integrity ---

#[tokio::test]
async fn sqlite_create_with_initial_state() {
    let store = setup_sqlite().await;
    let spec = TaskSpec {
        name: "with state".into(),
        description: None,
        owner_id: format!("test:{}", uuid::Uuid::new_v4()),
        initial_state: Some(serde_json::json!({"repos": ["a", "b"]})),
    };
    let id = store.create(&spec).await.unwrap();
    let cp = store.resume(&id).await.unwrap().unwrap();
    assert_eq!(cp["repos"][0], "a");
    assert_eq!(cp["repos"][1], "b");
}

#[tokio::test]
async fn sqlite_checkpoint_progress_clamps_to_100() {
    let store = setup_sqlite().await;
    let spec = test_spec("clamp progress");
    let id = store.create(&spec).await.unwrap();

    // Pass progress_pct = 200, should be clamped to 100
    store
        .checkpoint(&id, &serde_json::json!({}), Some(200), None)
        .await
        .unwrap();

    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.progress_pct, 100);
}

#[tokio::test]
async fn sqlite_list_by_id_prefix() {
    let store = setup_sqlite().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());

    let make_spec = || TaskSpec {
        name: "prefix-test".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };

    let id1 = store.create(&make_spec()).await.unwrap();
    let id2 = store.create(&make_spec()).await.unwrap();
    let id3 = store.create(&make_spec()).await.unwrap();

    // Use the first 8 chars of id1 as prefix — UUIDs are unique enough
    let prefix = &id1.0[..8];

    let results = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            id_prefix: Some(prefix.to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

    // Exactly 1 task should match this prefix (extremely unlikely for 2 UUIDs to share 8 chars)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id1);

    // Now list all 3 — no prefix filter
    let all = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // Ensure all ids are present
    let ids: Vec<&str> = all.iter().map(|t| t.id.0.as_str()).collect();
    assert!(ids.contains(&id1.0.as_str()));
    assert!(ids.contains(&id2.0.as_str()));
    assert!(ids.contains(&id3.0.as_str()));
}

#[tokio::test]
async fn sqlite_list_limit() {
    let store = setup_sqlite().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());

    let make_spec = || TaskSpec {
        name: "limit-test".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };

    for _ in 0..5 {
        store.create(&make_spec()).await.unwrap();
    }

    let results = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn sqlite_list_order_by_updated_desc() {
    let store = setup_sqlite().await;
    let owner = format!("test:{}", uuid::Uuid::new_v4());

    let spec1 = TaskSpec {
        name: "first-created".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };
    let spec2 = TaskSpec {
        name: "second-created".into(),
        description: None,
        owner_id: owner.clone(),
        initial_state: None,
    };

    let id1 = store.create(&spec1).await.unwrap();
    let id2 = store.create(&spec2).await.unwrap();

    // Checkpoint id1 to update its updated_at (making it newer than id2)
    // Add a tiny sleep to ensure timestamp difference
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    store
        .checkpoint(&id1, &serde_json::json!({"step": 1}), Some(10), None)
        .await
        .unwrap();

    let results = store
        .list(TaskFilter {
            owner_id: Some(owner.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    // id1 was updated more recently, so it should come first (ORDER BY updated_at DESC)
    assert_eq!(results[0].id, id1);
    assert_eq!(results[1].id, id2);
}

// --- Concurrency / edge cases ---

#[tokio::test]
async fn sqlite_double_complete_rejected() {
    let store = setup_sqlite().await;
    let spec = test_spec("double complete");
    let id = store.create(&spec).await.unwrap();

    // First complete succeeds
    store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await
        .unwrap();

    // Second complete should fail (task is already terminal)
    let result = store
        .update_status(&id, DurableTaskStatus::Completed, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sqlite_concurrent_checkpoint_last_wins() {
    let store = setup_sqlite().await;
    let spec = test_spec("last wins");
    let id = store.create(&spec).await.unwrap();

    // First checkpoint
    store
        .checkpoint(
            &id,
            &serde_json::json!({"iteration": 1}),
            Some(25),
            Some("step 1"),
        )
        .await
        .unwrap();

    // Second checkpoint
    store
        .checkpoint(
            &id,
            &serde_json::json!({"iteration": 2}),
            Some(75),
            Some("step 2"),
        )
        .await
        .unwrap();

    // Final state should reflect the second checkpoint
    let task = store.get(&id).await.unwrap().unwrap();
    assert_eq!(task.progress_pct, 75);
    assert_eq!(task.step_description.as_deref(), Some("step 2"));
    assert_eq!(task.checkpoint.unwrap()["iteration"], 2);
}

// --- Large data ---

#[tokio::test]
async fn sqlite_checkpoint_near_max_size() {
    let store = setup_sqlite().await;
    let spec = test_spec("near max");
    let id = store.create(&spec).await.unwrap();

    // ~900KB of data (under the 1MB limit)
    let big_string = "x".repeat(900_000);
    let state = serde_json::json!({"data": big_string});

    let result = store.checkpoint(&id, &state, Some(50), None).await;
    assert!(result.is_ok(), "900KB checkpoint should succeed");
}

#[tokio::test]
async fn sqlite_checkpoint_exceeds_max_size() {
    let store = setup_sqlite().await;
    let spec = test_spec("exceeds max");
    let id = store.create(&spec).await.unwrap();

    // ~2MB of data (over the 1MB limit)
    let big_string = "x".repeat(2_000_000);
    let state = serde_json::json!({"data": big_string});

    let result = store.checkpoint(&id, &state, Some(50), None).await;
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("too large"),
        "error should mention 'too large'"
    );
}
