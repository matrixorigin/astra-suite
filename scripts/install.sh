#!/usr/bin/env sh
# Install astra-gateway binary from GitHub releases.
# Usage:
#   curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
#   curl -sSL ... | sh -s -- -v v0.4.0
#   curl -sSL ... | sh -s -- -y
#   curl -sSL ... | sh -s -- -d ~/.local/bin
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
BINARY="astra-gateway"
VERSION=""
INSTALL_DIR=""
FORCE=false
DRY_RUN=false
GHPROXY="${ASTRA_GHPROXY:-https://ghfast.top}"

download() {
  _url="$1"; _out="$2"
  info "Downloading $_url"
  if curl -fL# --max-time 10 -o "$_out" "$_url"; then
    return 0
  fi
  warn "Direct download failed, retrying via mirror: $GHPROXY"
  curl -fL# -o "$_out" "${GHPROXY}/${_url}"
}

asset_exists() {
  _url="$1"
  if curl -fsIL --max-time 10 -o /dev/null "$_url" 2>/dev/null; then
    return 0
  fi
  curl -fsIL -o /dev/null "${GHPROXY}/${_url}" 2>/dev/null
}

fetch_releases() {
  _url="https://api.github.com/repos/$REPO/releases?per_page=50"
  if curl -fsSL --max-time 10 "$_url" 2>/dev/null; then
    return 0
  fi
  warn "Direct release lookup failed, retrying via mirror: $GHPROXY"
  curl -fsSL "${GHPROXY}/${_url}"
}

gateway_version_from_tag() {
  case "$1" in
    v*) printf '%s' "${1#v}" ;;
    *) return 1 ;;
  esac
}

release_asset_url() {
  _tag="$1"
  _target="$2"
  gateway_version_from_tag "$_tag" >/dev/null || return 1
  printf 'https://github.com/%s/releases/download/%s/%s-%s.tar.gz' \
    "$REPO" "$_tag" "$BINARY" "$_target"
}

stable_semver_key() {
  printf '%s\n' "$1" | awk -F. '
    NF == 3 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {
      printf "%010d.%010d.%010d", $1, $2, $3
    }
  '
}

semver_key_gt() {
  [ "$1" != "$2" ] || return 1
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort | tail -n 1)" = "$1" ]
}

# Resolve the latest gateway release by asset presence rather than the
# repository-wide /releases/latest pointer. astra-suite also hosts astra CLI
# releases, so repo-level latest can point at the wrong component.
resolve_latest() {
  _target="$1"
  _json=$(fetch_releases) || return 1
  _best_tag=""
  _best_key=""
  for _tag in $(printf '%s\n' "$_json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'); do
    case "$_tag" in
      *-rc*|*-alpha*|*-beta*) continue ;;
    esac
    _version=$(gateway_version_from_tag "$_tag" 2>/dev/null || true)
    [ -n "$_version" ] || continue
    _key=$(stable_semver_key "$_version")
    [ -n "$_key" ] || continue
    if _asset=$(release_asset_url "$_tag" "$_target") && asset_exists "$_asset"; then
      if [ -z "$_best_key" ] || semver_key_gt "$_key" "$_best_key"; then
        _best_key="$_key"
        _best_tag="$_tag"
      fi
    fi
  done

  [ -n "$_best_tag" ] || return 1
  printf '%s' "$_best_tag"
}

usage() {
  cat <<EOF
Usage: install.sh [OPTIONS]

Options:
  -v, --version TAG   Install a specific gateway version or tag (default: latest)
  -d, --dir PATH      Install directory (default: /usr/local/bin or ~/.local/bin)
  -y, --yes           Skip confirmation and PATH prompts
  -n, --dry-run       Print download URL and exit
  -h, --help          Show this help

Environment:
  ASTRA_GHPROXY        GitHub mirror base URL (default: https://ghfast.top)

Examples:
  sh scripts/install.sh
  sh scripts/install.sh -v v0.4.0
EOF
}

# ── Parse args ──────────────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    -v|--version) VERSION="$2"; shift 2 ;;
    -d|--dir)     INSTALL_DIR="$2"; shift 2 ;;
    -y|--yes)     FORCE=true; shift ;;
    -n|--dry-run) DRY_RUN=true; shift ;;
    -h|--help)    usage; exit 0 ;;
    *) error "Unknown option: $1"; exit 1 ;;
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

TARGET=$(detect_target)

if [ -z "$VERSION" ]; then
  info "Resolving latest gateway version..."
  TAG=$(resolve_latest "$TARGET" || true)
  if [ -z "$TAG" ]; then
    error "Failed to find an astra-gateway release for $TARGET"
    error "Expected a release tagged v<version>"
    exit 1
  fi
  ok "Latest gateway version: $TAG"
else
  case "$VERSION" in
    v*) TAG="$VERSION" ;;
    *) TAG="v${VERSION#v}" ;;
  esac
fi

gateway_version_from_tag "$TAG" >/dev/null || {
  error "Invalid gateway version tag: $TAG"
  exit 1
}

# ── Resolve install dir ─────────────────────────────────────────────

if [ -z "$INSTALL_DIR" ]; then
  if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
  fi
fi

set_release_urls() {
  TAG="$1"
  gateway_version_from_tag "$TAG" >/dev/null || return 1
  ARCHIVE="${BINARY}-${TARGET}.tar.gz"
  BASE_URL="https://github.com/$REPO/releases/download/$TAG"
  URL="${BASE_URL}/${ARCHIVE}"
}

set_release_urls "$TAG" || {
  error "Invalid gateway version tag: $TAG"
  exit 1
}

# ── Download & install ──────────────────────────────────────────────

info "Installing $BINARY $TAG ($TARGET)"
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

if [ "$FORCE" != "true" ]; then
  if ! _ans=$(ask "Install $BINARY $TAG to $INSTALL_DIR? [y/N] "); then
    info "Skipping (no tty). Use -y for non-interactive install."
    exit 0
  fi
  case "$_ans" in y|Y|yes|YES) ;; *) info "Aborted."; exit 0 ;; esac
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

if ! download "$URL" "$TMPDIR/$ARCHIVE"; then
  error "Download failed"
  exit 1
fi

info "Extracting..."
tar xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"

BIN_PATH=$(find "$TMPDIR" -maxdepth 2 -type f -name "$BINARY" | head -1)
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

ok "$BINARY $TAG installed to $INSTALL_DIR/$BINARY"

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
  printf '\n# Added by astra-gateway installer\n%s\n' "$LINE" >> "$RC"
  ok "Appended PATH entry to $RC"
fi

ok "Restart your shell, or run: source $RC"
