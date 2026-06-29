#!/usr/bin/env sh
# Install the astra CLI binary from GitHub releases.
# Usage:
#   curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install-astra.sh | sh
#   curl -sSL ... | sh -s -- -v v0.1.0 -d ~/.local/bin
#   curl -sSL ... | sh -s -- -n              # dry-run (print URLs, don't install)
#   curl -sSL ... | sh -s -- --init-models  # also write ./.models.yaml template
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
BINARY="astra"
CLI_TAG_PREFIX="astra-cli-v"
VERSION=""
INSTALL_DIR=""
FORCE=false
DRY_RUN=false
INIT_MODELS=false
MODELS_PATH="${ASTRA_MODELS_PATH:-.models.yaml}"
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

cli_version_from_tag() {
  case "$1" in
    ${CLI_TAG_PREFIX}*) printf '%s' "${1#${CLI_TAG_PREFIX}}" ;;
    v*) printf '%s' "${1#v}" ;;
    *) return 1 ;;
  esac
}

release_asset_url() {
  _tag="$1"
  _target="$2"
  _version=$(cli_version_from_tag "$_tag") || return 1
  printf 'https://github.com/%s/releases/download/%s/%s-v%s-%s.tar.gz' \
    "$REPO" "$_tag" "$BINARY" "$_version" "$_target"
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

# Resolve the latest astra CLI release by asset presence rather than the
# repository-wide /releases/latest pointer. astra-suite also publishes
# astra-gateway, so repo-level latest can point at the wrong component.
resolve_latest() {
  _target="$1"
  _json=$(fetch_releases) || return 1
  _best_tag=""
  _best_key=""
  for _tag in $(printf '%s\n' "$_json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'); do
    case "$_tag" in
      *-rc*|*-alpha*|*-beta*) continue ;;
    esac
    _version=$(cli_version_from_tag "$_tag" 2>/dev/null || true)
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

write_models_template() {
  _path="$1"
  if [ -e "$_path" ]; then
    warn "$_path already exists; not overwriting"
    return 0
  fi
  _dir=$(dirname "$_path")
  if [ "$_dir" != "." ]; then
    mkdir -p "$_dir"
  fi
  cat > "$_path" <<'EOF'
# Commented model registry template for `astra admin model load`.
#
# Copy this file, uncomment one or more model entries, fill real credentials,
# then load it:
#   astra admin --api-url http://127.0.0.1:17001 model load .models.yaml --update-existing
#
# Do not commit .models.yaml with real API keys.

# --- OpenAI-compatible example ----------------------------------------------
#
# - name: qwen-plus
#   provider: openai
#   api_key: sk-your-model-api-key
#   base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
#   description: "Qwen Plus via OpenAI-compatible API"
#   tags: [chat, code]
#   context_window: 1000000
#   max_completion_tokens: 32768
#   supported_parameters: [tools]

# --- Anthropic-compatible example -------------------------------------------
#
# - name: deepseek-v4-flash-anthropic
#   provider: anthropic
#   wire_model_name: deepseek-v4-flash
#   api_key: sk-your-model-api-key
#   base_url: https://api.deepseek.com/anthropic
#   description: "DeepSeek v4 flash via Anthropic-compatible API"
#   tags: [chat, code, reasoning]
#   context_window: 1000000
#   max_completion_tokens: 384000
#   supported_parameters: [tools]
#   prompt_cache_capability:
#     protocol: marker_explicit
#     volatile_placement: marker_isolated
#     reuse_scope: conversation_turns

# --- Common fields -----------------------------------------------------------
#
# name: Local model name used by `astra chat --model <name>`.
# provider: openai | anthropic | bedrock | moonshot | glm, depending on server support.
# wire_model_name: Optional upstream model name when it differs from `name`.
# api_key: Upstream provider key.
# base_url: Upstream API base URL.
# tags: Free-form labels such as chat, code, reasoning, selector.
# supported_parameters: Set to [tools] when the upstream model supports tool use.
EOF
  ok "Wrote commented model template to $_path"
}

print_models_hint() {
  info "Models template: rerun with --init-models to write ./.models.yaml"
}

# ── Parse args ──────────────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    -b|--binary)
      if [ "${2:-}" != "astra" ]; then
        error "Only the astra CLI is published by this installer; use the Docker image for astra-server"
        exit 1
      fi
      shift 2
      ;;
    -v|--version) VERSION="$2"; shift 2 ;;
    -d|--dir)     INSTALL_DIR="$2"; shift 2 ;;
    -y|--yes)     FORCE=true; shift ;;
    -n|--dry-run) DRY_RUN=true; shift ;;
    --init-models) INIT_MODELS=true; shift ;;
    --models-path) MODELS_PATH="$2"; shift 2 ;;
    -h|--help)
      cat <<EOF
Usage: install-astra.sh [OPTIONS]

Options:
  -v, --version TAG   Install a specific version (default: latest)
  -d, --dir PATH      Install directory (default: /usr/local/bin or ~/.local/bin)
  -y, --yes           Skip confirmation and PATH prompts
  -n, --dry-run       Print download URLs and exit (no install)
      --init-models   Also write a commented .models.yaml template
      --models-path   Path for --init-models (default: ./.models.yaml)
  -h, --help          Show this help

Environment:
  ASTRA_GHPROXY        GitHub mirror base URL (default: https://ghfast.top)
  ASTRA_MODELS_PATH    Default path for --init-models
EOF
      exit 0
      ;;
    *) error "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Platform detection ──────────────────────────────────────────────

detect_target() {
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) error "Unsupported architecture: $arch"; exit 1 ;;
  esac
  case "$os" in
    linux|darwin) printf '%s-%s' "$os" "$arch"; return ;;
  esac
  error "Unsupported platform: $os/$arch"
  exit 1
}

# ── Resolve version ─────────────────────────────────────────────────

TARGET=$(detect_target)
FALLBACK_TAG=""

if [ -z "$VERSION" ]; then
  info "Resolving latest version..."
  TAG=$(resolve_latest "$TARGET" || true)
  if [ -z "$TAG" ]; then
    error "Failed to find an astra CLI release for $TARGET"
    error "Expected a release tagged astra-cli-v<version> with astra-v<version>-$TARGET.tar.gz"
    exit 1
  fi
  ok "Latest version: $TAG"
else
  case "$VERSION" in
    ${CLI_TAG_PREFIX}*|v*) TAG="$VERSION" ;;
    *)
      TAG="${CLI_TAG_PREFIX}${VERSION#v}"
      FALLBACK_TAG="v${VERSION#v}"
      ;;
  esac
fi

VERSION_STR=$(cli_version_from_tag "$TAG") || {
  error "Invalid astra CLI version tag: $TAG"
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
  VERSION_STR=$(cli_version_from_tag "$TAG") || return 1
  ARCHIVE="${BINARY}-v${VERSION_STR}-${TARGET}.tar.gz"
  CHECKSUM="${ARCHIVE}.sha256"
  BASE_URL="https://github.com/$REPO/releases/download/$TAG"
  URL="${BASE_URL}/${ARCHIVE}"
  CHECKSUM_URL="${BASE_URL}/${CHECKSUM}"
}

set_release_urls "$TAG" || {
  error "Invalid astra CLI version tag: $TAG"
  exit 1
}

# ── Download & install ──────────────────────────────────────────────

info "Installing $BINARY $TAG ($TARGET)"
info "From: $URL"
info "To:   $INSTALL_DIR/$BINARY"

if [ "$DRY_RUN" = true ]; then
  info "Dry-run mode: skipping download and install"
  if [ "$INIT_MODELS" = true ]; then
    info "Would write commented model template to $MODELS_PATH"
  fi
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
  if [ -n "$INSTALLED_VER" ] && [ "$INSTALLED_VER" = "$BINARY $VERSION_STR" ]; then
    ok "$BINARY $TAG is already installed"
    if [ "$INIT_MODELS" = true ]; then
      write_models_template "$MODELS_PATH"
    else
      print_models_hint
    fi
    exit 0
  fi
  if [ -n "$INSTALLED_VER" ]; then
    warn "Upgrading $INSTALLED_VER → $VERSION"
  fi
fi

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
  if [ -n "$FALLBACK_TAG" ]; then
    warn "Retrying legacy release tag: $FALLBACK_TAG"
    set_release_urls "$FALLBACK_TAG" || { error "Invalid legacy release tag: $FALLBACK_TAG"; exit 1; }
    download "$URL" "$TMPDIR/$ARCHIVE" || { error "Download failed"; exit 1; }
  else
    error "Download failed"
    exit 1
  fi
fi
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

ok "$BINARY $TAG installed to $INSTALL_DIR/$BINARY"

if [ "$INIT_MODELS" = true ]; then
  write_models_template "$MODELS_PATH"
else
  print_models_hint
fi

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
