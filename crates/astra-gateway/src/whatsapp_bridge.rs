use std::path::{Path, PathBuf};

const WHATSAPP_BRIDGE_FILES: &[(&str, &str)] = &[
    (
        "index.js",
        include_str!("../../../bridges/whatsapp-baileys/index.js"),
    ),
    (
        "package.json",
        include_str!("../../../bridges/whatsapp-baileys/package.json"),
    ),
    (
        "package-lock.json",
        include_str!("../../../bridges/whatsapp-baileys/package-lock.json"),
    ),
];

pub fn run_dir() -> PathBuf {
    let path = std::env::var_os("GATEWAY_RUN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".astra-gateway")
        });
    absolutize_path(path)
}

pub fn auth_dir() -> PathBuf {
    run_dir().join("whatsapp-auth")
}

pub fn qr_dir() -> PathBuf {
    run_dir().join("whatsapp-qr")
}

pub fn socket_path() -> PathBuf {
    run_dir().join("whatsapp-baileys.sock")
}

pub fn runtime_dir() -> PathBuf {
    run_dir().join("whatsapp-bridge")
}

pub fn remove_path_if_exists(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path)
            .map_err(|e| format!("failed to remove {}: {e}", path.display())),
        Ok(_) => std::fs::remove_file(path)
            .map_err(|e| format!("failed to remove {}: {e}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to inspect {}: {err}", path.display())),
    }
}

pub fn prepare_runtime(no_install: bool) -> Result<PathBuf, String> {
    let runtime_dir = runtime_dir();
    std::fs::create_dir_all(&runtime_dir).map_err(|e| {
        format!(
            "failed to create bridge runtime dir {}: {e}",
            runtime_dir.display()
        )
    })?;

    let mut manifest_changed = false;
    for (file, content) in WHATSAPP_BRIDGE_FILES {
        let path = runtime_dir.join(file);
        let changed = std::fs::read_to_string(&path)
            .map(|existing| existing != *content)
            .unwrap_or(true);
        if changed {
            std::fs::write(&path, content).map_err(|e| {
                format!(
                    "failed to write embedded WhatsApp bridge {file} to {}: {e}",
                    runtime_dir.display()
                )
            })?;
        }
        if changed && (*file == "package.json" || *file == "package-lock.json") {
            manifest_changed = true;
        }
    }

    let has_baileys = runtime_dir
        .join("node_modules/@whiskeysockets/baileys")
        .exists();
    if has_baileys && !manifest_changed {
        return Ok(runtime_dir);
    }
    if no_install {
        return Err(format!(
            "WhatsApp bridge dependencies missing or stale under {}; omit --no-install to install them",
            runtime_dir.display()
        ));
    }
    println!(
        "Installing WhatsApp bridge dependencies in {}...",
        runtime_dir.display()
    );
    let status = std::process::Command::new("npm")
        .arg("ci")
        .current_dir(&runtime_dir)
        .status()
        .map_err(|e| format!("failed to run npm ci: {e}"))?;
    if !status.success() {
        return Err("npm ci failed".into());
    }
    Ok(runtime_dir)
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
