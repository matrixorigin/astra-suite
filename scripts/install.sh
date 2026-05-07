#!/usr/bin/env sh
# Install astra-gateway binary from GitHub releases.
# Usage:
#   curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
#   curl -sSL ... | sh -s -- -v v0.1.0
#   curl -sSL ... | sh -s -- -y              # skip confirmation
#   curl -sSL ... | sh -s -- -d ~/.local/bin  # custom directory

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
  VERSION=$(curl -sSf "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name"' | sed 's/.*"tag_name": *"//;s/".*//')
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

curl -sSfL "$URL" -o "$TMPDIR/$ARCHIVE" || { error "Download failed"; exit 1; }
tar xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"

if [ -w "$INSTALL_DIR" ]; then
  mv "$TMPDIR/astra-gateway" "$INSTALL_DIR/astra-gateway"
else
  info "Need sudo to install to $INSTALL_DIR"
  sudo mv "$TMPDIR/astra-gateway" "$INSTALL_DIR/astra-gateway"
fi

chmod +x "$INSTALL_DIR/astra-gateway"

ok "astra-gateway $VERSION installed to $INSTALL_DIR/astra-gateway"

# Check PATH
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) warn "$INSTALL_DIR is not in your PATH. Add it with:"
     warn "  export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
