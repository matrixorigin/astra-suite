#!/usr/bin/env sh
# Install astra, astra-server, or astra-edge binary from GitHub releases.
# Usage:
#   curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
#   curl -sSL ... | sh -s -- -b astra-server -y
#   curl -sSL ... | sh -s -- -v v0.1.0 -d ~/.local/bin
#   curl -sSL ... | sh -s -- -n              # dry-run (print URLs, don't install)
#
# Network: tries GitHub directly (10s timeout); on failure falls back to a
# mirror. Override with ASTRA_GHPROXY=https://your-mirror (default ghfast.top).

set -eu

BOLD="$(tput bold 2>/dev/null || printf '')"
GREEN="$(tput setaf 2 2>/dev/null || printf '')"
YELLOW="$(tput setaf 3 2>/dev/null || printf '')"
RED="$(tput setaf 1 2>/dev/null || printf '')"
NC="$(tput sgr0 2>/dev/null || printf '')"

info()  { printf '%s\n' "${BOLD}>${NC} $*"; }
warn()  { printf '%s\n' "${YELLOW}! $*${NC}"; }
error() { printf '%s\n' "${RED}x $*${NC}" >&2; }
ok()    { printf '%s\n' "${GREEN}✓${NC} $*"; }

REPO="matrixorigin/astra-suite"
BINARY="${ASTRA_BINARY:-astra}"
VERSION=""
INSTALL_DIR=""
FORCE=false
DRY_RUN=false
GHPROXY="${ASTRA_GHPROXY:-https://ghfast.top}"

# Try direct GitHub URL first (10s timeout); on failure, fall back to a
# GitHub mirror (default https://ghfast.top, override via ASTRA_GHPROXY).
# Args: <url> <output-path>
download() {
  _url="$1"; _out="$2"
  info "Downloading $_url"
  if curl -fL# --max-time 10 -o "$_out" "$_url"; then
    return 0
  fi
  warn "Direct download failed, retrying via mirror: $GHPROXY"
  curl -fL# -o "$_out" "${GHPROXY}/${_url}"
}

# Resolve "latest" via the redirect of /releases/latest (avoids api.github.com).
# Falls back through the mirror on timeout.
resolve_latest() {
  _repo_url="https://github.com/$REPO/releases/latest"
  _redir=$(curl -sIL --max-time 10 -o /dev/null -w '%{url_effective}' "$_repo_url" 2>/dev/null || true)
  case "$_redir" in
    *"/releases/tag/"*) ;;
    *)
      warn "Direct version lookup failed, retrying via mirror: $GHPROXY"
      _redir=$(curl -sIL -o /dev/null -w '%{url_effective}' "${GHPROXY}/${_repo_url}" 2>/dev/null || true)
      ;;
  esac
  printf '%s' "${_redir##*/tag/}"
}

# ── Parse args ──────────────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    -b|--binary)  BINARY="$2"; shift 2 ;;
    -v|--version) VERSION="$2"; shift 2 ;;
    -d|--dir)     INSTALL_DIR="$2"; shift 2 ;;
    -y|--yes)     FORCE=true; shift ;;
    -n|--dry-run) DRY_RUN=true; shift ;;
    -h|--help)
      cat <<EOF
Usage: install.sh [OPTIONS]

Options:
  -b, --binary NAME   Binary to install (default: astra)
                       Choices: astra, astra-server, astra-edge
  -v, --version TAG   Install a specific version (default: latest)
  -d, --dir PATH      Install directory (default: /usr/local/bin or ~/.local/bin)
  -y, --yes           Skip confirmation and PATH prompts
  -n, --dry-run       Print download URLs and exit (no install)
  -h, --help          Show this help

Environment:
  ASTRA_BINARY         Same as -b
  ASTRA_GHPROXY        GitHub mirror base URL (default: https://ghfast.top)
EOF
      exit 0
      ;;
    *) shift ;;
  esac
done

# Validate binary name
case "$BINARY" in
  astra|astra-server|astra-edge) ;;
  *) error "Unknown binary: $BINARY (choose astra, astra-server, or astra-edge)"; exit 1 ;;
esac

# ── Platform detection ──────────────────────────────────────────────

detect_target() {
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64) arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *) error "Unsupported architecture: $arch"; exit 1 ;;
  esac
  case "$os" in
    linux)
      [ "$arch" = "x86_64" ] && printf "x86_64-unknown-linux-musl" && return
      [ "$arch" = "aarch64" ] && printf "aarch64-unknown-linux-musl" && return
      ;;
    darwin)
      [ "$arch" = "x86_64" ] && printf "x86_64-apple-darwin" && return
      [ "$arch" = "aarch64" ] && printf "aarch64-apple-darwin" && return
      ;;
  esac
  error "Unsupported platform: $os/$arch"
  exit 1
}

# ── Resolve version ─────────────────────────────────────────────────

if [ -z "$VERSION" ]; then
  info "Resolving latest version..."
  VERSION=$(resolve_latest)
  if [ -z "$VERSION" ]; then
    error "Failed to fetch latest version"
    exit 1
  fi
  ok "Latest version: $VERSION"
fi

# ── Resolve install dir ─────────────────────────────────────────────

if [ -z "$INSTALL_DIR" ]; then
  if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
  fi
fi

# ── Download & install ──────────────────────────────────────────────

TARGET=$(detect_target)
ARCHIVE="${BINARY}-${TARGET}.tar.gz"
CHECKSUM="${BINARY}-${TARGET}.tar.gz.sha256"
BASE_URL="https://github.com/$REPO/releases/download/$VERSION"
URL="${BASE_URL}/${ARCHIVE}"
CHECKSUM_URL="${BASE_URL}/${CHECKSUM}"

info "Installing $BINARY $VERSION ($TARGET)"
info "From: $URL"
info "To:   $INSTALL_DIR/$BINARY"

if [ "$DRY_RUN" = true ]; then
  info "Dry-run mode: skipping download and install"
  exit 0
fi

# Prompt the user, even when the script is piped via `curl | sh`.
ask() {
  _prompt="$1"
  if [ -t 0 ]; then
    printf '%s' "$_prompt" >&2
    IFS= read -r _ans || _ans=""
    printf '%s' "$_ans"
    return 0
  fi
  printf '%s' "$_prompt" >&2
  _result=$(exec 2>/dev/null; IFS= read -r x </dev/tty && printf 'TTY:%s' "$x") || _result=""
  case "$_result" in
    TTY:*) printf '%s' "${_result#TTY:}"; return 0 ;;
    *)     printf '\n' >&2; return 1 ;;
  esac
}

# Check for already-installed version
if [ -x "$INSTALL_DIR/$BINARY" ]; then
  INSTALLED_VER=$("$INSTALL_DIR/$BINARY" --version 2>/dev/null | head -1 || true)
  if [ -n "$INSTALLED_VER" ] && [ "$INSTALLED_VER" = "$BINARY $VERSION" ]; then
    ok "$BINARY $VERSION is already installed"
    exit 0
  fi
  if [ -n "$INSTALLED_VER" ]; then
    warn "Upgrading $INSTALLED_VER → $VERSION"
  fi
fi

if [ "$FORCE" != "true" ]; then
  if ! _ans=$(ask "Install $BINARY $VERSION to $INSTALL_DIR? [y/N] "); then
    info "Skipping (no tty). Use -y for non-interactive install."
    exit 0
  fi
  case "$_ans" in y|Y|yes|YES) ;; *) info "Aborted."; exit 0 ;; esac
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

download "$URL" "$TMPDIR/$ARCHIVE"       || { error "Download failed"; exit 1; }
download "$CHECKSUM_URL" "$TMPDIR/$CHECKSUM" 2>/dev/null || warn "Checksum not found, skipping verification"

# Sha256 verification
if [ -f "$TMPDIR/$CHECKSUM" ]; then
  EXPECTED=$(awk '{print $1}' "$TMPDIR/$CHECKSUM")
  ACTUAL=$(sha256sum "$TMPDIR/$ARCHIVE" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$TMPDIR/$ARCHIVE" | awk '{print $1}')
  if [ "$EXPECTED" = "$ACTUAL" ]; then
    ok "Checksum verified"
  else
    error "Checksum mismatch!"
    error "  Expected: $EXPECTED"
    error "  Got:      $ACTUAL"
    exit 1
  fi
fi

info "Extracting..."
tar xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"

# The archive may contain the binary directly or in a subdirectory.
# Find the binary by name.
BIN_PATH=$(find "$TMPDIR" -maxdepth 2 -type f -name "$BINARY" | head -1)
if [ -z "$BIN_PATH" ]; then
  # Fallback: the binary might be named differently. Try 'astra'.
  BIN_PATH=$(find "$TMPDIR" -maxdepth 2 -type f -name "$BINARY" -o -name "astra" -type f | head -1)
fi
if [ -z "$BIN_PATH" ]; then
  error "Binary '$BINARY' not found in archive"
  exit 1
fi

if [ -w "$INSTALL_DIR" ]; then
  install -m 755 "$BIN_PATH" "$INSTALL_DIR/$BINARY"
else
  info "Need sudo to install to $INSTALL_DIR"
  sudo install -m 755 "$BIN_PATH" "$INSTALL_DIR/$BINARY"
fi

ok "$BINARY $VERSION installed to $INSTALL_DIR/$BINARY"

# ── PATH setup ──────────────────────────────────────────────────────

case ":$PATH:" in
  *":$INSTALL_DIR:"*) exit 0 ;;
esac

warn "$INSTALL_DIR is not in your PATH."

if [ "$FORCE" = "true" ]; then
  answer=y
elif ! answer=$(ask "Append it to your shell rc file? [y/N] "); then
  answer=n
fi

case "$answer" in
  y|Y|yes|YES) ;;
  *)
    warn "Skipped. Add it manually with:"
    warn "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.bashrc   # or ~/.zshrc"
    warn "  export PATH=\"$INSTALL_DIR:\$PATH\"                      # current session"
    exit 0
    ;;
esac

LINE="export PATH=\"$INSTALL_DIR:\$PATH\""
case "${SHELL##*/}" in
  zsh)  RC="$HOME/.zshrc" ;;
  bash) [ -f "$HOME/.bashrc" ] && RC="$HOME/.bashrc" || RC="$HOME/.bash_profile" ;;
  fish)
    mkdir -p "$HOME/.config/fish"
    RC="$HOME/.config/fish/config.fish"
    LINE="set -gx PATH $INSTALL_DIR \$PATH"
    ;;
  *)    RC="$HOME/.profile" ;;
esac

touch "$RC"
if grep -Fqs "$INSTALL_DIR" "$RC"; then
  ok "$INSTALL_DIR already referenced in $RC"
else
  printf '\n# Added by astra installer\n%s\n' "$LINE" >> "$RC"
  ok "Appended PATH entry to $RC"
fi

ok "Restart your shell, or run: source $RC"
