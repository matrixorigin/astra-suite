#!/usr/bin/env sh
# Install astra-gateway binary from GitHub releases.
# Usage:
#   curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
#   curl -sSL ... | sh -s -- -v v0.1.0
#   curl -sSL ... | sh -s -- -y              # skip confirmation
#   curl -sSL ... | sh -s -- -d ~/.local/bin  # custom directory
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
VERSION=""
INSTALL_DIR=""
FORCE=false
GHPROXY="${ASTRA_GHPROXY:-https://ghfast.top}"

# Try direct GitHub URL first (10s timeout); on failure, fall back to a
# GitHub mirror (default https://ghfast.top, override via ASTRA_GHPROXY).
# Args: <url> <output-path>
download() {
  _url="$1"; _out="$2"
  if curl -fL# --max-time 10 -o "$_out" "$_url" 2>/dev/null; then
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
    -v|--version) VERSION="$2"; shift 2 ;;
    -d|--dir)     INSTALL_DIR="$2"; shift 2 ;;
    -y|--yes)     FORCE=true; shift ;;
    *) shift ;;
  esac
done

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
  VERSION=$(resolve_latest)
  if [ -z "$VERSION" ]; then
    error "Failed to fetch latest version"
    exit 1
  fi
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
ARCHIVE="astra-gateway-${TARGET}.tar.gz"
URL="https://github.com/$REPO/releases/download/$VERSION/$ARCHIVE"

info "Installing astra-gateway $VERSION ($TARGET)"
info "From: $URL"
info "To:   $INSTALL_DIR/astra-gateway"

if [ "$FORCE" != "true" ]; then
  printf "Continue? [y/N] "
  read -r answer
  case "$answer" in
    y|Y|yes|YES) ;;
    *) info "Aborted."; exit 0 ;;
  esac
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

download "$URL" "$TMPDIR/$ARCHIVE" || { error "Download failed"; exit 1; }
tar xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"

if [ -w "$INSTALL_DIR" ]; then
  mv "$TMPDIR/astra-gateway" "$INSTALL_DIR/astra-gateway"
else
  info "Need sudo to install to $INSTALL_DIR"
  sudo mv "$TMPDIR/astra-gateway" "$INSTALL_DIR/astra-gateway"
fi

chmod +x "$INSTALL_DIR/astra-gateway"

ok "astra-gateway $VERSION installed to $INSTALL_DIR/astra-gateway"

# ── PATH setup ──────────────────────────────────────────────────────

case ":$PATH:" in
  *":$INSTALL_DIR:"*) exit 0 ;;
esac

warn "$INSTALL_DIR is not in your PATH."

if [ "$FORCE" = "true" ]; then
  answer=y
elif [ -t 0 ]; then
  printf "Append it to your shell rc file? [y/N] "
  read -r answer || answer=n
else
  answer=auto-no
fi

case "$answer" in
  y|Y|yes|YES) ;;
  auto-no)
    warn "Non-interactive shell detected; skipping rc update."
    warn "Re-run with -y to auto-append, or add manually:"
    warn "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.bashrc   # or ~/.zshrc"
    warn "  export PATH=\"$INSTALL_DIR:\$PATH\"                      # current session"
    exit 0
    ;;
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
  printf '\n# Added by astra-gateway installer\n%s\n' "$LINE" >> "$RC"
  ok "Appended PATH entry to $RC"
fi

ok "Restart your shell, or run: source $RC"
