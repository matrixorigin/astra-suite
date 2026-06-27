use crate::platforms::{AttachmentKind, InboundAttachment};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use tokio::io::AsyncWriteExt;

pub(crate) const MAX_INBOUND_ATTACHMENT_BYTES: u64 = 128 * 1024 * 1024;

pub(crate) struct PrepareDirGuard(Option<PathBuf>);

impl PrepareDirGuard {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self(Some(dir))
    }

    pub(crate) fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for PrepareDirGuard {
    fn drop(&mut self) {
        let Some(dir) = self.0.as_ref() else {
            return;
        };
        if let Err(e) = std::fs::remove_dir_all(dir)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::debug!(dir = %dir.display(), error = %e, "attachment prepare dir cleanup failed");
        }
    }
}

pub(crate) fn prepare_response(
    dropped: usize,
    all_dropped: bool,
    failures: &[String],
) -> Option<String> {
    if dropped == 0 {
        return None;
    }
    let detail = failure_detail(failures);
    Some(if all_dropped {
        format!("收到附件，但当前无法读取该附件内容。{detail}请重新发送可下载的图片或文件。")
    } else {
        format!(
            "收到 {dropped} 个附件，但无法读取其中部分内容。{detail}请重新发送可下载的图片或文件。"
        )
    })
}

fn failure_detail(failures: &[String]) -> String {
    let mut unique = Vec::new();
    for failure in failures {
        if !unique.contains(failure) {
            unique.push(failure.clone());
        }
        if unique.len() >= 3 {
            break;
        }
    }
    if unique.is_empty() {
        String::new()
    } else {
        format!("原因：{}。", unique.join("；"))
    }
}

pub(crate) fn missing_url_failure(attachment: &InboundAttachment, idx: usize) -> String {
    format!("{} 缺少可下载链接", label(attachment, idx))
}

pub(crate) fn too_large_failure(attachment: &InboundAttachment, idx: usize) -> String {
    format!(
        "{} 超过大小上限 {}MB",
        label(attachment, idx),
        MAX_INBOUND_ATTACHMENT_BYTES / 1024 / 1024
    )
}

pub(crate) fn label(attachment: &InboundAttachment, idx: usize) -> String {
    attachment
        .name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .map(|name| format!("附件 `{}`", name.trim()))
        .unwrap_or_else(|| format!("第 {} 个附件", idx + 1))
}

pub(crate) fn dir(msg_id: &str) -> PathBuf {
    run_dir()
        .join(".attachments")
        .join(sanitize_path_part(msg_id))
}

fn run_dir() -> PathBuf {
    std::env::var_os("GATEWAY_RUN_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".astra-gateway")))
        .unwrap_or_else(|| PathBuf::from(".astra-gateway"))
}

pub(crate) fn filename(attachment: &InboundAttachment, idx: usize, bytes: &[u8]) -> String {
    let inferred_name = infer_name_from_url(attachment.url.as_deref());
    let base = attachment
        .name
        .as_deref()
        .or(inferred_name.as_deref())
        .map(sanitize_path_part)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| format!("attachment-{idx}"));
    let filename = ensure_extension(
        &base,
        attachment.mime_type.as_deref(),
        attachment.kind,
        bytes,
    );
    format!("{}-{}", idx + 1, filename)
}

pub(crate) fn ensure_extension(
    filename: &str,
    mime_type: Option<&str>,
    kind: AttachmentKind,
    bytes: &[u8],
) -> String {
    let Some(ext) = extension(mime_type, kind, bytes) else {
        return filename.to_string();
    };
    let path = Path::new(filename);
    let current_ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    if current_ext
        .as_deref()
        .is_some_and(|current| current == ext && is_trusted_extension(current))
    {
        return filename.to_string();
    }
    if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        && current_ext
            .as_deref()
            .is_some_and(|current| !is_trusted_extension(current))
    {
        return format!("{stem}.{ext}");
    }
    format!("{filename}.{ext}")
}

fn is_trusted_extension(ext: &str) -> bool {
    matches!(
        ext,
        "png"
            | "jpg"
            | "jpeg"
            | "gif"
            | "webp"
            | "pdf"
            | "html"
            | "htm"
            | "txt"
            | "mp4"
            | "mov"
            | "mp3"
            | "wav"
            | "m4a"
            | "ogg"
    )
}

fn extension(mime_type: Option<&str>, kind: AttachmentKind, bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"%PDF-") {
        return Some("pdf");
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some("jpg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("gif");
    }
    if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        return Some("webp");
    }
    let sample_len = bytes.len().min(512);
    let sample = String::from_utf8_lossy(&bytes[..sample_len]).to_ascii_lowercase();
    if sample.contains("<!doctype html") || sample.contains("<html") {
        return Some("html");
    }
    match mime_type.unwrap_or("").trim().to_ascii_lowercase().as_str() {
        "text/html" | "application/xhtml+xml" => Some("html"),
        "application/pdf" => Some("pdf"),
        "image/png" => Some("png"),
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "text/plain" => Some("txt"),
        _ => match kind {
            AttachmentKind::Video => Some("mp4"),
            AttachmentKind::Audio => Some("audio"),
            AttachmentKind::Image | AttachmentKind::File | AttachmentKind::Unknown => None,
        },
    }
}

pub(crate) fn detect_mime_from_bytes<'a>(
    bytes: &[u8],
    fallback: Option<&'a str>,
) -> Option<&'a str> {
    if bytes.starts_with(b"%PDF-") {
        return Some("application/pdf");
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some("image/jpeg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        return Some("image/webp");
    }
    let sample_len = bytes.len().min(512);
    let sample = String::from_utf8_lossy(&bytes[..sample_len]).to_ascii_lowercase();
    if sample.contains("<!doctype html") || sample.contains("<html") {
        return Some("text/html");
    }
    fallback
}

pub(crate) async fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dir).await?;
    #[cfg(unix)]
    if let Err(e) = tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).await {
        tracing::debug!(dir = %dir.display(), error = %e, "attachment dir chmod failed");
    }
    Ok(())
}

pub(crate) async fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .await?;
    #[cfg(unix)]
    if let Err(e) = file
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .await
    {
        tracing::debug!(path = %path.display(), error = %e, "attachment file chmod failed");
    }
    file.write_all(bytes).await
}

pub(crate) fn sanitize_path_part(input: &str) -> String {
    input
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => ch,
            _ => '_',
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

pub(crate) fn infer_name_from_url(url: Option<&str>) -> Option<String> {
    let url = url?;
    let parsed = reqwest::Url::parse(url).ok()?;
    parsed
        .path_segments()?
        .next_back()
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
}
