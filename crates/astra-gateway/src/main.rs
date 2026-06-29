use clap::{Parser, Subcommand};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(
    name = "astra-gateway",
    version,
    about = "Chat platform gateway for AI agent CLIs"
)]
struct Cli {
    /// Path to gateway config (default: ~/.astra-gateway/config.yaml)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override database URL (also: GATEWAY_DATABASE_URL env var)
    #[arg(long)]
    database_url: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

impl Cli {
    fn effective_config(&self) -> PathBuf {
        self.config.clone().unwrap_or_else(default_config_path)
    }
}

#[derive(Subcommand)]
enum Command {
    /// Generate a starter config at ~/.astra-gateway/config.yaml
    Init {
        /// Overwrite an existing config file
        #[arg(long)]
        force: bool,
    },
    /// WeChat personal account helpers.
    Weixin {
        #[command(subcommand)]
        command: WeixinCommand,
    },
    /// WhatsApp Web sidecar helpers.
    Whatsapp {
        #[command(subcommand)]
        command: WhatsappCommand,
    },
    /// Start the gateway as a background daemon
    Start,
    /// Stop the running gateway daemon (graceful SIGTERM, then SIGKILL after 15s)
    Stop,
    /// Restart the gateway daemon (stop if running, then start)
    Restart,
    /// Show whether the gateway daemon is running
    Status,
    /// Update astra-gateway in place to the latest release.
    /// Downloads the prebuilt binary for the current platform and atomically
    /// replaces the running executable.
    Update {
        /// Install a specific gateway tag or version (e.g. 0.4.0, v0.4.0).
        /// Default: latest gateway release.
        #[arg(long)]
        version: Option<String>,
        /// Mirror prefix for github.com (default: $ASTRA_GHPROXY or https://ghfast.top)
        #[arg(long)]
        mirror: Option<String>,
    },
    /// Run as MCP stdio server (spawned by Claude CLI via --mcp-config)
    #[command(name = "mcp-serve")]
    McpServe {
        /// Database URL for storage access (MySQL/MatrixOne)
        #[arg(long, env = "GATEWAY_DATABASE_URL")]
        database_url: Option<String>,
        /// SQLite database file path
        #[arg(long, env = "GW_MCP_SQLITE_PATH")]
        sqlite_path: Option<String>,
        /// Platform identifier for scoping queries
        #[arg(long, env = "GW_MCP_PLATFORM")]
        platform: Option<String>,
        /// Chat ID for scoping queries
        #[arg(long, env = "GW_MCP_CHAT_ID")]
        chat_id: Option<String>,
        /// User ID for scoping queries
        #[arg(long, env = "GW_MCP_USER_ID")]
        user_id: Option<String>,
        /// Colon-separated project directories
        #[arg(long, env = "GW_MCP_PROJECT_DIRS")]
        project_dirs: Option<String>,
        /// Local gateway runtime API URL
        #[arg(long, env = "GW_MCP_RUNTIME_API_URL")]
        runtime_api_url: Option<String>,
        /// Bearer token for local gateway runtime API
        #[arg(long, env = "GW_MCP_RUNTIME_API_TOKEN")]
        runtime_api_token: Option<String>,
    },
}

#[derive(Subcommand)]
enum WhatsappCommand {
    /// Start Baileys QR login and persist WhatsApp Web auth state.
    Login {
        /// Optional proxy for WhatsApp Web traffic.
        #[arg(long)]
        proxy: Option<String>,
        /// Remove existing WhatsApp auth state before login.
        #[arg(long)]
        force: bool,
        /// Skip dependency installation even if node_modules is missing.
        #[arg(long)]
        no_install: bool,
    },
}

#[derive(Subcommand)]
enum WeixinCommand {
    /// QR code login for WeChat (iLink Bot API).
    Login,
}

fn data_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".astra-gateway")
}

fn default_config_path() -> PathBuf {
    default_run_dir().join("config.yaml")
}

fn absolutize_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn default_run_dir() -> PathBuf {
    let path = std::env::var_os("GATEWAY_RUN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(data_dir);
    absolutize_path(path)
}

fn pid_path() -> PathBuf {
    default_run_dir().join("gateway.pid")
}

fn lock_path() -> PathBuf {
    default_run_dir().join("gateway.lock")
}

fn log_path() -> PathBuf {
    default_run_dir().join("gateway.log")
}

fn runtime_api_token_path() -> PathBuf {
    default_run_dir().join("runtime-api-token")
}

const REPO: &str = "matrixorigin/astra-suite";
const BIN_NAME: &str = "astra-gateway";

fn current_target() -> Result<&'static str, String> {
    use std::env::consts::{ARCH, OS};
    Ok(match (OS, ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-musl",
        ("linux", "aarch64") => "aarch64-unknown-linux-musl",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        _ => return Err(format!("unsupported platform: {OS}/{ARCH}")),
    })
}

// Curl with 10s direct timeout, then mirror fallback for both lookup and download.
fn curl_with_fallback(args_direct: &[&str], target_url: &str, proxy: &str) -> Result<(), String> {
    let mut direct = std::process::Command::new("curl");
    direct.args(args_direct).arg(target_url);
    let ok = direct.status().map(|s| s.success()).unwrap_or(false);
    if ok {
        return Ok(());
    }
    eprintln!("Direct request failed, retrying via mirror: {proxy}");
    let mirrored = format!("{}/{}", proxy.trim_end_matches('/'), target_url);
    let mut mirror = std::process::Command::new("curl");
    let mut filtered: Vec<&str> = Vec::with_capacity(args_direct.len());
    let mut skip_next = false;
    for a in args_direct {
        if skip_next {
            skip_next = false;
            continue;
        }
        if *a == "--max-time" {
            skip_next = true;
            continue;
        }
        filtered.push(a);
    }
    mirror.args(&filtered).arg(&mirrored);
    let s = mirror
        .status()
        .map_err(|e| format!("curl spawn failed: {e}"))?;
    if !s.success() {
        return Err("curl request failed (direct and mirror)".into());
    }
    Ok(())
}

fn curl_capture_with_fallback(
    args_direct: &[&str],
    target_url: &str,
    proxy: &str,
) -> Result<String, String> {
    let mut direct = std::process::Command::new("curl");
    direct.args(args_direct).arg(target_url);
    if let Ok(out) = direct.output()
        && out.status.success()
    {
        return String::from_utf8(out.stdout).map_err(|e| format!("curl output utf8: {e}"));
    }

    eprintln!("Direct request failed, retrying via mirror: {proxy}");
    let mirrored = format!("{}/{}", proxy.trim_end_matches('/'), target_url);
    let mut filtered: Vec<&str> = Vec::with_capacity(args_direct.len());
    let mut skip_next = false;
    for a in args_direct {
        if skip_next {
            skip_next = false;
            continue;
        }
        if *a == "--max-time" {
            skip_next = true;
            continue;
        }
        filtered.push(a);
    }
    let out = std::process::Command::new("curl")
        .args(&filtered)
        .arg(&mirrored)
        .output()
        .map_err(|e| format!("curl spawn failed: {e}"))?;
    if !out.status.success() {
        return Err("curl request failed (direct and mirror)".into());
    }
    String::from_utf8(out.stdout).map_err(|e| format!("curl output utf8: {e}"))
}

fn asset_exists(target_url: &str, proxy: &str) -> bool {
    let direct = std::process::Command::new("curl")
        .args(["-fsIL", "--max-time", "10", "-o", "/dev/null"])
        .arg(target_url)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if direct {
        return true;
    }

    let mirrored = format!("{}/{}", proxy.trim_end_matches('/'), target_url);
    std::process::Command::new("curl")
        .args(["-fsIL", "-o", "/dev/null"])
        .arg(mirrored)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn gateway_version_from_tag(tag: &str) -> Option<&str> {
    tag.strip_prefix('v')
}

fn stable_semver_key(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn gateway_archive_url(tag: &str, target: &str) -> String {
    let archive = format!("{BIN_NAME}-{target}.tar.gz");
    format!("https://github.com/{REPO}/releases/download/{tag}/{archive}")
}

fn gateway_tag_candidates(version: &str) -> Vec<String> {
    let bare = version.strip_prefix('v').unwrap_or(version);
    vec![format!("v{bare}")]
}

#[derive(serde::Deserialize)]
struct GithubRelease {
    tag_name: String,
    draft: bool,
    prerelease: bool,
}

fn resolve_latest_tag(proxy: &str, target: &str) -> Result<String, String> {
    let api_url = format!("https://api.github.com/repos/{REPO}/releases?per_page=50");
    let body = curl_capture_with_fallback(&["-fsSL", "--max-time", "10"], &api_url, proxy)?;
    let releases: Vec<GithubRelease> =
        serde_json::from_str(&body).map_err(|e| format!("parse GitHub releases: {e}"))?;

    let mut best: Option<((u64, u64, u64), String)> = None;
    for release in releases {
        if release.draft || release.prerelease {
            continue;
        }
        let Some(version) = gateway_version_from_tag(&release.tag_name) else {
            continue;
        };
        let Some(key) = stable_semver_key(version) else {
            continue;
        };
        let archive_url = gateway_archive_url(&release.tag_name, target);
        if asset_exists(&archive_url, proxy) {
            let replace = best
                .as_ref()
                .map(|(current_key, _)| key > *current_key)
                .unwrap_or(true);
            if replace {
                best = Some((key, release.tag_name));
            }
        }
    }

    best.map(|(_, tag)| tag)
        .ok_or_else(|| format!("no astra-gateway release found for {target}"))
}

fn run_self_update(version: Option<String>, mirror: Option<String>) -> Result<(), String> {
    let proxy = mirror
        .or_else(|| std::env::var("ASTRA_GHPROXY").ok())
        .unwrap_or_else(|| "https://ghfast.top".into());

    let target = current_target()?;
    let tag_candidates = match version {
        Some(v) => gateway_tag_candidates(&v),
        None => vec![resolve_latest_tag(&proxy, target)?],
    };

    let current = env!("CARGO_PKG_VERSION");
    if tag_candidates
        .iter()
        .any(|tag| gateway_version_from_tag(tag).is_some_and(|version| version == current))
    {
        println!("astra-gateway is already at v{current}.");
        return Ok(());
    }

    let archive = format!("{BIN_NAME}-{target}.tar.gz");

    // Random suffix avoids collisions between concurrent updates; Drop guard
    // ensures cleanup even if download / extract / self-replace fails.
    let suffix: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_else(|_| std::process::id() as u64);
    let tmp = std::env::temp_dir().join(format!(
        "astra-gateway-update-{}-{}",
        std::process::id(),
        suffix
    ));
    std::fs::create_dir_all(&tmp).map_err(|e| format!("tempdir: {e}"))?;
    struct CleanupGuard(PathBuf);
    impl Drop for CleanupGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _guard = CleanupGuard(tmp.clone());

    let archive_path = tmp.join(&archive);
    let archive_path_str = archive_path.to_string_lossy().into_owned();

    let mut selected_tag = None;
    let mut last_error = None;
    for tag in &tag_candidates {
        println!("Updating astra-gateway: v{current} → {tag} ({target})");
        let gh_url = format!("https://github.com/{REPO}/releases/download/{tag}/{archive}");
        match curl_with_fallback(
            &["-fL#", "--max-time", "10", "-o", &archive_path_str],
            &gh_url,
            &proxy,
        ) {
            Ok(()) => {
                selected_tag = Some(tag.clone());
                break;
            }
            Err(err) => {
                last_error = Some(format!("{tag}: {err}"));
            }
        }
    }
    let tag = selected_tag.ok_or_else(|| {
        last_error.unwrap_or_else(|| "failed to download gateway release".to_string())
    })?;

    let status = std::process::Command::new("tar")
        .args(["xzf"])
        .arg(&archive_path)
        .arg("-C")
        .arg(&tmp)
        .status()
        .map_err(|e| format!("tar spawn failed: {e}"))?;
    if !status.success() {
        return Err("tar extract failed".into());
    }

    let new_bin = tmp.join(BIN_NAME);
    if !new_bin.exists() {
        return Err(format!(
            "expected binary not found in archive: {}",
            new_bin.display()
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&new_bin)
            .map_err(|e| format!("stat new binary: {e}"))?
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&new_bin, perms).map_err(|e| format!("chmod new binary: {e}"))?;
    }

    self_replace::self_replace(&new_bin).map_err(|e| {
        let exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.display().to_string()))
            .unwrap_or_else(|| "<unknown>".into());
        format!(
            "self-replace failed: {e}\n  hint: need write permission on {exe}; \
             try `sudo astra-gateway update` if installed system-wide"
        )
    })?;

    println!("✓ astra-gateway updated to {tag}");
    Ok(())
}

struct GatewayInstanceGuard {
    _lock_file: File,
    pid_file: PathBuf,
}

impl Drop for GatewayInstanceGuard {
    fn drop(&mut self) {
        let current_pid = std::process::id().to_string();
        if std::fs::read_to_string(&self.pid_file)
            .map(|pid| pid.trim() == current_pid)
            .unwrap_or(false)
        {
            let _ = std::fs::remove_file(&self.pid_file);
        }
    }
}

fn acquire_gateway_instance_guard() -> Result<GatewayInstanceGuard, String> {
    let run_dir = default_run_dir();
    std::fs::create_dir_all(&run_dir)
        .map_err(|e| format!("failed to create run dir {}: {e}", run_dir.display()))?;
    let lock_path = lock_path();
    let pid_file = pid_path();
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| format!("failed to open {}: {e}", lock_path.display()))?;

    if let Err(e) = lock_file.try_lock_exclusive() {
        let pid = std::fs::read_to_string(&pid_file)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        return Err(format!(
            "astra-gateway is already running (pid: {pid}); use `astra-gateway stop` to stop it: {e}"
        ));
    }

    std::fs::write(&pid_file, std::process::id().to_string())
        .map_err(|e| format!("failed to write {}: {e}", pid_file.display()))?;

    Ok(GatewayInstanceGuard {
        _lock_file: lock_file,
        pid_file,
    })
}

// ── Daemon control: start / stop / status ───────────────────────────

fn current_running_pid() -> Option<i32> {
    let pid_str = std::fs::read_to_string(pid_path()).ok()?;
    let pid: i32 = pid_str.trim().parse().ok()?;
    // kill(pid, 0) probes for existence without sending a signal.
    // errno == EPERM means the process exists but we lack permission to
    // signal it — still "alive" from our perspective.
    let alive = if unsafe { libc::kill(pid, 0) } == 0 {
        true
    } else {
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    };
    if alive { Some(pid) } else { None }
}

fn cmd_start(config: &Path) -> Result<(), String> {
    if !config.exists() {
        return Err(format!(
            "config not found: {}\n  hint: run `astra-gateway init` to create one",
            config.display()
        ));
    }
    if let Some(pid) = current_running_pid() {
        return Err(format!("astra-gateway is already running (pid: {pid})"));
    }

    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    std::fs::create_dir_all(default_run_dir())
        .map_err(|e| format!("create {}: {e}", default_run_dir().display()))?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path())
        .map_err(|e| format!("open log {}: {e}", log_path().display()))?;
    let log2 = log.try_clone().map_err(|e| format!("clone log fd: {e}"))?;

    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("--config")
        .arg(config)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(log2));
    unsafe {
        cmd.pre_exec(|| {
            // Detach from controlling terminal: new session + new process group.
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd.spawn().map_err(|e| format!("spawn daemon: {e}"))?;
    let spawned_pid = child.id();

    // Wait briefly for the child to register its pidfile, or exit.
    for _ in 0..20 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!(
                "daemon (pid {spawned_pid}) exited immediately with {}; check {}",
                status
                    .code()
                    .map(|c| format!("exit code {c}"))
                    .unwrap_or_else(|| "signal".into()),
                log_path().display()
            ));
        }
        if let Some(pid) = current_running_pid() {
            println!("✓ astra-gateway started (pid: {pid})");
            println!("  log:    {}", log_path().display());
            println!("  config: {}", config.display());
            return Ok(());
        }
    }
    Err(format!(
        "daemon (pid {spawned_pid}) did not register pidfile within 2s; check {}",
        log_path().display()
    ))
}

fn cmd_stop() -> Result<(), String> {
    let pid = match current_running_pid() {
        Some(p) => p,
        None => {
            println!("astra-gateway is not running");
            return Ok(());
        }
    };

    let _ = unsafe { libc::kill(pid, libc::SIGTERM) };
    use std::io::Write;
    print!("Stopping astra-gateway (pid: {pid})");
    let _ = std::io::stdout().flush();

    for _ in 0..30 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        print!(".");
        let _ = std::io::stdout().flush();
        if current_running_pid().is_none() {
            println!(" stopped");
            return Ok(());
        }
    }
    println!();
    eprintln!("did not exit in 15s; sending SIGKILL");
    unsafe { libc::kill(pid, libc::SIGKILL) };
    Ok(())
}

fn cmd_restart(config: &Path) -> Result<(), String> {
    cmd_stop()?;
    cmd_start(config)
}

fn cmd_status(config: &Path) {
    match current_running_pid() {
        Some(pid) => {
            println!("● astra-gateway: running (pid: {pid})");
            println!("  config: {}", config.display());
            println!("  log:    {}", log_path().display());
            println!("  pid:    {}", pid_path().display());
        }
        None => {
            println!("○ astra-gateway: stopped");
            if pid_path().exists() {
                println!("  (stale pidfile at {})", pid_path().display());
            }
        }
    }
}

fn run_whatsapp_login(proxy: Option<String>, force: bool, no_install: bool) -> Result<(), String> {
    let run_dir = astra_gateway::whatsapp_bridge::run_dir();
    let runtime_dir = astra_gateway::whatsapp_bridge::prepare_runtime(no_install)?;
    let auth_dir = astra_gateway::whatsapp_bridge::auth_dir();
    let qr_dir = astra_gateway::whatsapp_bridge::qr_dir();

    if force {
        astra_gateway::whatsapp_bridge::remove_path_if_exists(&auth_dir)?;
        astra_gateway::whatsapp_bridge::remove_path_if_exists(&qr_dir)?;
        println!("Removed existing WhatsApp auth state.");
    }

    println!("Starting WhatsApp login sidecar.");
    println!("  bridge: {}", runtime_dir.display());
    println!("  auth:   {}", auth_dir.display());
    println!("  QR:     {}", qr_dir.display());
    println!();
    println!("Scan the QR from the terminal, or open:");
    println!("  {}", qr_dir.join("qr.png").display());
    println!();
    println!("The command exits automatically after WhatsApp connects.");

    let mut cmd = std::process::Command::new("node");
    cmd.arg("index.js")
        .arg("login")
        .current_dir(&runtime_dir)
        .env("GATEWAY_RUN_DIR", &run_dir)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
    if let Some(proxy) = proxy {
        cmd.env("WHATSAPP_PROXY", proxy);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to start node sidecar: {e}"))?;

    let status = child
        .wait()
        .map_err(|e| format!("failed waiting for node sidecar: {e}"))?;
    if !status.success() {
        return Err(format!("WhatsApp login sidecar exited with {status}"));
    }
    Ok(())
}

async fn run_weixin_login(config_path: PathBuf) -> Result<(), String> {
    match astra_gateway::platforms::weixin::qr_login().await {
        Ok((token, account_id)) => {
            // Save to store if config is loadable.
            let db_saved = if config_path.exists() {
                if let Ok(cfg) = astra_gateway::config::GatewayConfig::load(&config_path) {
                    let storage_config = cfg.resolve_storage();
                    match astra_gateway::store::open_store_bundle(&storage_config).await {
                        Ok(Some(bundle)) => {
                            let creds = serde_json::json!({
                                "token": token,
                                "account_id": account_id,
                            });
                            match bundle
                                .store
                                .save_credential("weixin", "default", "bot_token", &creds, None)
                                .await
                            {
                                Ok(()) => {
                                    println!("✅ 凭证已保存到存储 (换机器无需重新扫码)");
                                    true
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "store save failed, falling back to config file");
                                    false
                                }
                            }
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !db_saved {
                // Fallback: write to yaml.
                if config_path.exists() {
                    if let Ok(content) = std::fs::read_to_string(&config_path) {
                        use regex::Regex;
                        let token_re = Regex::new(r#"(?m)(    token: )"[^"]*""#).unwrap();
                        let account_re = Regex::new(r#"(?m)(    account_id: )"[^"]*""#).unwrap();
                        let patched = token_re
                            .replace(&content, &format!("${{1}}\"{token}\""))
                            .to_string();
                        let patched = account_re
                            .replace(&patched, &format!("${{1}}\"{account_id}\""))
                            .to_string();
                        if patched != content {
                            std::fs::write(&config_path, &patched).ok();
                            println!("✅ 已自动写入 {}", config_path.display());
                        }
                    }
                } else {
                    println!("将以下内容写入 {}:", config_path.display());
                    println!();
                    println!("platforms:");
                    println!("  weixin:");
                    println!("    enabled: true");
                    println!("    token: \"{token}\"");
                    println!("    account_id: \"{account_id}\"");
                }
            }
            println!();
            println!("现在可以运行: astra-gateway start");
            Ok(())
        }
        Err(e) => Err(format!("WeChat login failed: {e}")),
    }
}

#[tokio::main]
async fn main() {
    // Load .env file if present (before logging init so RUST_LOG works)
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();
    let is_mcp_stdio = matches!(&cli.command, Some(Command::McpServe { .. }));
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,astra_gateway=debug".parse().unwrap());

    if is_mcp_stdio {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    if let Some(Command::McpServe {
        database_url,
        sqlite_path,
        platform,
        chat_id,
        user_id,
        project_dirs,
        runtime_api_url,
        runtime_api_token,
    }) = cli.command
    {
        let dirs: Vec<String> = project_dirs
            .map(|s| s.split(':').map(String::from).collect())
            .unwrap_or_default();
        if let Err(e) = astra_gateway::mcp::server::run_stdio_server(
            database_url,
            sqlite_path,
            platform,
            chat_id,
            user_id,
            dirs,
            runtime_api_url,
            runtime_api_token,
        )
        .await
        {
            eprintln!("mcp-serve error: {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Command::Update { version, mirror }) = cli.command {
        match run_self_update(version, mirror) {
            Ok(()) => return,
            Err(e) => {
                eprintln!("❌ update failed: {e}");
                std::process::exit(1);
            }
        }
    }

    if let Some(Command::Whatsapp { command }) = cli.command {
        let result = match command {
            WhatsappCommand::Login {
                proxy,
                force,
                no_install,
            } => run_whatsapp_login(proxy, force, no_install),
        };
        if let Err(e) = result {
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Command::Weixin {
        command: WeixinCommand::Login,
    }) = cli.command
    {
        let config_path = cli.effective_config();
        if let Err(e) = run_weixin_login(config_path).await {
            tracing::error!(error = %e, "WeChat login failed");
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Command::Status) = cli.command {
        cmd_status(&cli.effective_config());
        return;
    }

    if let Some(Command::Stop) = cli.command {
        if let Err(e) = cmd_stop() {
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Command::Restart) = cli.command {
        let config = cli.effective_config();
        if let Err(e) = cmd_restart(&config) {
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Command::Start) = cli.command {
        let config = cli.effective_config();
        if let Err(e) = cmd_start(&config) {
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
        return;
    }

    if let Some(Command::Init { force }) = cli.command {
        let dest = cli.effective_config();
        if dest.exists() && !force {
            eprintln!(
                "{} already exists — pass --force to overwrite, or edit it manually.",
                dest.display()
            );
            std::process::exit(1);
        }
        if let Some(parent) = dest.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            eprintln!("failed to create {}: {e}", parent.display());
            std::process::exit(1);
        }
        let template = include_str!("../gateway-wecom-claude.yaml");
        std::fs::write(&dest, template).expect("failed to write config");
        // Best-effort tighten perms — the file holds Bedrock + WeCom secrets.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600));
        }
        println!(
            "Created {} (WeCom + Claude/Bedrock + SQLite)",
            dest.display()
        );
        println!();
        println!("Next steps:");
        println!("  1. Edit {} and fill in:", dest.display());
        println!("       - AWS_BEARER_TOKEN_BEDROCK   (cli.env)");
        println!("       - platforms.wecom.bot_id / secret");
        println!("  2. astra-gateway start          # run as background daemon");
        println!("     astra-gateway status         # check it's up");
        println!("     astra-gateway stop           # stop it");
        return;
    }

    let _instance_guard = match acquire_gateway_instance_guard() {
        Ok(guard) => guard,
        Err(e) => {
            tracing::error!(error = %e, "gateway already running");
            eprintln!("❌ {e}");
            std::process::exit(1);
        }
    };

    let config_path = cli.effective_config();
    let mut config = match astra_gateway::config::GatewayConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(path = %config_path.display(), error = %e, "config load failed");
            std::process::exit(1);
        }
    };

    // CLI flag overrides config file
    if let Some(ref db_url) = cli.database_url {
        config.storage = astra_gateway::store::StorageConfig::Mysql {
            url: db_url.clone(),
        };
    }

    // Apply timezone offset for cron scheduling
    if let Some(ref tz) = config.timezone {
        let offset = match parse_timezone_offset(tz) {
            Some(o) => o,
            None => {
                tracing::error!(timezone = %tz, "unsupported timezone; use a known zone (Asia/Shanghai, Asia/Tokyo, UTC) or numeric offset (\"+8\", \"-5\")");
                std::process::exit(1);
            }
        };
        astra_gateway::store::set_cron_timezone_offset(offset);
        tracing::info!(timezone = %tz, offset_hours = offset, "cron timezone configured");
    }

    let mut runner = match astra_gateway::runner::GatewayRunner::new(config.clone()).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "runner init failed");
            std::process::exit(1);
        }
    };
    let scheduler_config = config.clone();

    let mut adapters: Vec<Box<dyn astra_gateway::platforms::PlatformAdapter>> = Vec::new();

    if let Some(wecom_cfg) = config.platforms.wecom
        && wecom_cfg.enabled
    {
        adapters.push(Box::new(
            astra_gateway::platforms::wecom::WeComAdapter::new(wecom_cfg),
        ));
    }

    if let Some(weixin_cfg) = config.platforms.weixin
        && weixin_cfg.enabled
    {
        let mut adapter = astra_gateway::platforms::weixin::WeixinAdapter::new(weixin_cfg);
        if let Some(store) = runner.store() {
            adapter = adapter.with_store(store);
        }
        adapters.push(Box::new(adapter));
    }

    if let Some(whatsapp_cfg) = config.platforms.whatsapp
        && whatsapp_cfg.enabled
    {
        adapters.push(Box::new(
            astra_gateway::platforms::whatsapp::WhatsAppAdapter::new(whatsapp_cfg),
        ));
    }

    if let Some(whatsapp_web_cfg) = config.platforms.whatsapp_web
        && whatsapp_web_cfg.enabled
    {
        adapters.push(Box::new(
            astra_gateway::platforms::whatsapp_web::WhatsAppWebAdapter::new(whatsapp_web_cfg),
        ));
    }

    if adapters.is_empty() {
        tracing::error!("no platforms enabled");
        std::process::exit(1);
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (cron_tx, cron_rx) = tokio::sync::mpsc::channel(64);
    let (inject_tx, inject_rx) = tokio::sync::mpsc::channel(64);
    let (runtime_cmd_tx, runtime_cmd_rx) = tokio::sync::mpsc::channel(64);
    runner.set_outbound_tx(cron_tx.clone());

    // Start local runtime API server if configured
    if let Some(port) = config.api_port {
        let tx = inject_tx.clone();
        let command_tx = runtime_cmd_tx.clone();
        let token = uuid::Uuid::new_v4().to_string();
        let token_path = runtime_api_token_path();
        if let Err(e) = std::fs::write(&token_path, &token) {
            tracing::warn!(path = %token_path.display(), error = %e, "failed to write runtime API token file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600));
        }
        runner.set_runtime_api(format!("http://127.0.0.1:{port}"), token.clone());
        tokio::spawn(astra_gateway::runtime_api::run(
            port,
            tx,
            command_tx,
            Some(token),
        ));
    }

    // Start cron scheduler (only if store + trace_repo available)
    if let (Some(store), Some(trace_repo)) = (runner.store(), runner.trace_repo()) {
        let scheduler = astra_gateway::scheduler::CronScheduler::new(
            store,
            scheduler_config,
            trace_repo,
            cron_tx,
        );
        let _scheduler_handle = scheduler.spawn(shutdown_tx.subscribe());
    } else {
        tracing::info!("cron scheduler disabled (no store or trace_repo)");
    }

    // Ctrl+C / SIGTERM (so `astra-gateway stop` triggers graceful shutdown)
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut term = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGTERM handler; relying on Ctrl+C only");
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("shutting down (SIGINT)");
                let _ = shutdown_tx_clone.send(());
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("shutting down (SIGINT)"),
            _ = term.recv()             => tracing::info!("shutting down (SIGTERM)"),
        }
        let _ = shutdown_tx_clone.send(());
    });

    let runner = std::sync::Arc::new(runner);
    runner
        .run(adapters, cron_rx, inject_rx, runtime_cmd_rx, shutdown_rx)
        .await;
}

// Only fixed-offset timezones are supported (no DST handling).
// For DST-affected zones, use explicit numeric offset (e.g. "+8", "-5").
fn parse_timezone_offset(tz: &str) -> Option<i32> {
    match tz {
        "Asia/Shanghai" | "Asia/Chongqing" => Some(8),
        "Asia/Tokyo" | "JST" => Some(9),
        "Europe/London" | "GMT" | "UTC" => Some(0),
        s if s.starts_with('+') || s.starts_with('-') => Some(s.parse().unwrap_or(0)),
        _ => None,
    }
}
