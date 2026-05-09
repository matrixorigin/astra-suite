//! Bash tool: `/bin/bash -c <command>` with workspace sandbox, timeout,
//! and bounded output capture.
//!
//! Contract (what `loop_runner` sees):
//! - Input: `BashInput { command: String }`
//! - Output: `BashOutput { stdout, stderr, exit_code, stdout_truncated,
//!   stderr_truncated, timed_out, cancelled }`
//! - `is_error` (for the Anthropic `tool_result` block) is derived from
//!   `exit_code != 0 || timed_out || cancelled`.
//!
//! Safety posture:
//! - No command allowlist/denylist. Aligned with Claude CLI which runs
//!   under `--dangerously-skip-permissions` — skills already call
//!   arbitrary `curl`/`jq`/`python3`. A denylist was considered and
//!   rejected: too easy to bypass, too easy to false-positive on skill
//!   usage.
//! - Workspace-rooted `current_dir` (caller passes the gateway's resolved
//!   workspace path; None = inherit gateway cwd, matching existing Claude
//!   CLI behavior).
//! - Per-invocation timeout (kill on deadline).
//! - `CancellationToken` drops the child.
//! - stdout/stderr captured up to `max_bytes` each; excess is silently
//!   dropped after truncation (the model gets a visible `...<truncated>`
//!   suffix so it knows).
//! - `kill_on_drop(true)` ensures a stray panic still reaps the child.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Input schema matches the Anthropic tool `input_schema` we advertise.
#[derive(Debug, Deserialize)]
pub struct BashInput {
    pub command: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct BashOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub timed_out: bool,
    pub cancelled: bool,
}

impl BashOutput {
    /// Whether this result should flip the `tool_result.is_error` flag.
    pub fn is_error(&self) -> bool {
        self.cancelled || self.timed_out || !matches!(self.exit_code, Some(0))
    }

    /// Render a single string suitable for `tool_result.content`. Combines
    /// stdout / stderr / status into something the model can reason about.
    pub fn to_tool_result_content(&self) -> String {
        let mut out = String::new();
        if !self.stdout.is_empty() {
            out.push_str(&self.stdout);
            if self.stdout_truncated {
                out.push_str(&format!(
                    "\n...<stdout truncated at {} bytes>",
                    self.stdout.len()
                ));
            }
        }
        if !self.stderr.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("[stderr]\n");
            out.push_str(&self.stderr);
            if self.stderr_truncated {
                out.push_str(&format!(
                    "\n...<stderr truncated at {} bytes>",
                    self.stderr.len()
                ));
            }
        }
        if self.cancelled {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("[cancelled by user]");
        } else if self.timed_out {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("[timed out]");
        } else if let Some(code) = self.exit_code
            && code != 0
        {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("[exit code: {code}]"));
        }
        if out.is_empty() {
            out.push_str("(no output)");
        }
        out
    }
}

/// Run a bash command with the given constraints.
///
/// - `working_dir`: if `Some`, `cd` there before exec; if `None`, inherit.
/// - `timeout`: hard deadline; after this, child is SIGKILL'd and output so
///   far is returned with `timed_out=true`.
/// - `max_bytes`: per-stream cap. Each of stdout and stderr captures up to
///   this many bytes; the rest is discarded silently (flagged via
///   `*_truncated`).
/// - `cancel`: if triggered, child is killed and `cancelled=true`.
pub async fn run_bash(
    input: &BashInput,
    working_dir: Option<&Path>,
    timeout: Duration,
    max_bytes: usize,
    cancel: &CancellationToken,
) -> Result<BashOutput, String> {
    let mut cmd = Command::new("/bin/bash");
    cmd.arg("-c")
        .arg(&input.command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(dir) = working_dir {
        cmd.current_dir(dir);
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn bash: {e}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "bash: stdout unavailable".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "bash: stderr unavailable".to_string())?;

    let stdout_task = tokio::spawn(read_bounded(stdout, max_bytes));
    let stderr_task = tokio::spawn(read_bounded(stderr, max_bytes));

    let cancel_fut = cancel.cancelled();
    let timeout_fut = tokio::time::sleep(timeout);

    let outcome = tokio::select! {
        wait = child.wait() => Outcome::Exited(wait.map_err(|e| format!("wait: {e}"))?),
        _ = timeout_fut => {
            let _ = child.kill().await;
            Outcome::TimedOut
        }
        _ = cancel_fut => {
            let _ = child.kill().await;
            Outcome::Cancelled
        }
    };

    let (stdout_bytes, stdout_truncated) =
        stdout_task.await.unwrap_or_else(|_| (Vec::new(), false));
    let (stderr_bytes, stderr_truncated) =
        stderr_task.await.unwrap_or_else(|_| (Vec::new(), false));

    let (exit_code, timed_out, cancelled) = match outcome {
        Outcome::Exited(status) => (status.code(), false, false),
        Outcome::TimedOut => (None, true, false),
        Outcome::Cancelled => (None, false, true),
    };

    Ok(BashOutput {
        stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
        stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
        exit_code,
        stdout_truncated,
        stderr_truncated,
        timed_out,
        cancelled,
    })
}

enum Outcome {
    Exited(std::process::ExitStatus),
    TimedOut,
    Cancelled,
}

/// Read from `reader` until EOF or `max_bytes` is hit. After the cap, keep
/// draining so the child's pipe doesn't block — but throw away the excess.
async fn read_bounded<R: AsyncReadExt + Unpin + Send + 'static>(
    mut reader: R,
    max_bytes: usize,
) -> (Vec<u8>, bool) {
    let mut buf = Vec::with_capacity(max_bytes.min(4096));
    let mut truncated = false;
    let mut chunk = [0u8; 4096];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                let remaining = max_bytes.saturating_sub(buf.len());
                if remaining == 0 {
                    truncated = true;
                    continue; // drain without storing
                }
                let take = n.min(remaining);
                buf.extend_from_slice(&chunk[..take]);
                if take < n {
                    truncated = true;
                }
            }
            Err(_) => break,
        }
    }
    (buf, truncated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_cancel() -> CancellationToken {
        CancellationToken::new()
    }

    #[tokio::test]
    async fn echo_succeeds() {
        let input = BashInput {
            command: "echo hello".into(),
        };
        let out = run_bash(
            &input,
            None,
            Duration::from_secs(5),
            32 * 1024,
            &test_cancel(),
        )
        .await
        .unwrap();
        assert_eq!(out.exit_code, Some(0));
        assert_eq!(out.stdout.trim(), "hello");
        assert!(out.stderr.is_empty());
        assert!(!out.timed_out);
        assert!(!out.cancelled);
        assert!(!out.is_error());
    }

    #[tokio::test]
    async fn nonzero_exit_flags_error() {
        let input = BashInput {
            command: "exit 7".into(),
        };
        let out = run_bash(
            &input,
            None,
            Duration::from_secs(5),
            32 * 1024,
            &test_cancel(),
        )
        .await
        .unwrap();
        assert_eq!(out.exit_code, Some(7));
        assert!(out.is_error());
        assert!(out.to_tool_result_content().contains("exit code: 7"));
    }

    #[tokio::test]
    async fn timeout_kills_long_running_command() {
        let input = BashInput {
            command: "sleep 10".into(),
        };
        let started = std::time::Instant::now();
        let out = run_bash(
            &input,
            None,
            Duration::from_millis(150),
            32 * 1024,
            &test_cancel(),
        )
        .await
        .unwrap();
        // Should return well before the 10s sleep would have finished.
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(out.timed_out);
        assert!(out.cancelled.eq(&false));
        assert!(out.exit_code.is_none());
        assert!(out.is_error());
        assert!(out.to_tool_result_content().contains("timed out"));
    }

    #[tokio::test]
    async fn output_truncates_past_max_bytes() {
        // 200 KB of 'a' bytes, cap at 4 KB.
        let input = BashInput {
            command: "head -c 204800 /dev/zero | tr '\\0' 'a'".into(),
        };
        let out = run_bash(
            &input,
            None,
            Duration::from_secs(5),
            4 * 1024,
            &test_cancel(),
        )
        .await
        .unwrap();
        assert!(out.stdout.len() <= 4 * 1024);
        assert!(out.stdout_truncated, "expected stdout to be flagged");
    }

    #[tokio::test]
    async fn cancel_token_aborts() {
        let cancel = CancellationToken::new();
        let cancel_trigger = cancel.clone();
        let input = BashInput {
            command: "sleep 10".into(),
        };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel_trigger.cancel();
        });
        let started = std::time::Instant::now();
        let out = run_bash(&input, None, Duration::from_secs(30), 32 * 1024, &cancel)
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(out.cancelled);
        assert!(!out.timed_out);
        assert!(out.is_error());
        assert!(out.to_tool_result_content().contains("cancelled"));
    }

    #[tokio::test]
    async fn stderr_is_captured_separately() {
        let input = BashInput {
            command: "echo stdout_line; echo stderr_line 1>&2".into(),
        };
        let out = run_bash(
            &input,
            None,
            Duration::from_secs(5),
            32 * 1024,
            &test_cancel(),
        )
        .await
        .unwrap();
        assert_eq!(out.exit_code, Some(0));
        assert!(out.stdout.contains("stdout_line"));
        assert!(out.stderr.contains("stderr_line"));
        assert!(!out.is_error());
    }

    #[tokio::test]
    async fn working_dir_is_applied() {
        let tempdir = tempfile::tempdir().unwrap();
        let input = BashInput {
            command: "pwd".into(),
        };
        let out = run_bash(
            &input,
            Some(tempdir.path()),
            Duration::from_secs(5),
            32 * 1024,
            &test_cancel(),
        )
        .await
        .unwrap();
        assert_eq!(out.exit_code, Some(0));
        // tempdir.path() may be canonicalized by pwd (e.g. /private/tmp on mac),
        // so match by final component instead of full path.
        let last = tempdir.path().file_name().unwrap().to_str().unwrap();
        assert!(
            out.stdout.contains(last),
            "pwd output {:?} did not contain tempdir last component {}",
            out.stdout,
            last
        );
    }
}
