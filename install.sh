#!/usr/bin/env bash
# migkit - Universal Enterprise Database Migration Toolkit Installer
# Usage:
#   ./install.sh                # Install standalone CLI to ~/.local/bin/migkit
#   ./install.sh --dev          # Set up local development environment (.venv)
#   curl -fsSL <url> | bash     # Remote one-line installer
set -euo pipefail

MIGKIT_VERSION="0.2.0"
INSTALL_DEV=0
USE_UV=1
PREFIX="${MIGKIT_PREFIX:-$HOME/.local}"
BIN_DIR="$PREFIX/bin"
SHARE_DIR="$PREFIX/share/migkit"
VENV_DIR="$SHARE_DIR/venv"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      INSTALL_DEV=1
      shift
      ;;
    --no-uv)
      USE_UV=0
      shift
      ;;
    --prefix)
      PREFIX="$2"
      BIN_DIR="$PREFIX/bin"
      SHARE_DIR="$PREFIX/share/migkit"
      VENV_DIR="$SHARE_DIR/venv"
      shift 2
      ;;
    --help|-h)
      echo "migkit installer v${MIGKIT_VERSION}"
      echo "Usage: $0 [OPTIONS]"
      echo "  --dev        Install development checkout into .venv with dev dependencies"
      echo "  --no-uv      Do not use uv even if available (use standard python -m venv)"
      echo "  --prefix DIR Custom installation directory (default: ~/.local)"
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || echo "")"
if [[ -z "$SCRIPT_DIR" || ! -f "$SCRIPT_DIR/pyproject.toml" ]]; then
  SRC_TARGET="git+https://github.com/arkashira/migkit.git"
  IS_LOCAL=0
else
  SRC_TARGET="$SCRIPT_DIR"
  IS_LOCAL=1
fi

echo "================================================================================"
echo "migkit installer v${MIGKIT_VERSION}"
echo "================================================================================"

# 1. Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"
echo "[1/5] Platform detected: ${OS} (${ARCH})"

# 2. Check or obtain compatible Python (3.12 or 3.13)
echo "[2/5] Checking Python runtime (requires >=3.12, <3.15)..."
PYTHON_BIN=""

# Check if uv is available
HAS_UV=0
if [[ "$USE_UV" -eq 1 ]] && command -v uv >/dev/null 2>&1; then
  HAS_UV=1
  echo "  found uv: $(uv --version)"
fi

if [[ "$HAS_UV" -eq 1 ]]; then
  # uv can manage python 3.12 automatically
  PYTHON_BIN="uv"
else
  # Find system python >= 3.12 and < 3.15
  for cand in python3.12 python3.13 python3; do
    if command -v "$cand" >/dev/null 2>&1; then
      if "$cand" -c 'import sys; raise SystemExit(0 if (3, 12) <= sys.version_info < (3, 15) else 1)' 2>/dev/null; then
        PYTHON_BIN="$(command -v "$cand")"
        echo "  found compatible Python: $PYTHON_BIN ($("$cand" -V 2>&1))"
        break
      fi
    fi
  done
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "ERROR: migkit requires Python 3.12 or 3.13." >&2
  if [[ "$OS" == "Darwin" ]]; then
    echo "To install on macOS via Homebrew or uv:" >&2
    echo "  brew install python@3.12" >&2
    echo "  OR curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
  else
    echo "To install on Linux:" >&2
    echo "  sudo apt-get install -y python3.12 python3.12-venv python3-pip" >&2
    echo "  OR curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
  fi
  exit 1
fi

# 3. Create Virtual Environment
echo "[3/5] Setting up isolated virtual environment..."
mkdir -p "$BIN_DIR"

if [[ "$INSTALL_DEV" -eq 1 ]]; then
  DEV_VENV="$SRC_TARGET/.venv"
  echo "  creating local development venv at: $DEV_VENV"
  if [[ "$HAS_UV" -eq 1 ]]; then
    uv venv --python 3.12 "$DEV_VENV"
    echo "  installing migkit in editable mode with dev dependencies..."
    VIRTUAL_ENV="$DEV_VENV" uv pip install -e "$SRC_TARGET[dev]"
  else
    "$PYTHON_BIN" -m venv "$DEV_VENV"
    "$DEV_VENV/bin/pip" install --upgrade pip
    "$DEV_VENV/bin/pip" install -e "$SRC_TARGET[dev]"
  fi
  TARGET_CLI="$DEV_VENV/bin/migkit"
else
  mkdir -p "$SHARE_DIR"
  echo "  standalone target venv: $VENV_DIR"
  if [[ "$HAS_UV" -eq 1 ]]; then
    uv tool install --python 3.12 "$SRC_TARGET" --reinstall
    TARGET_CLI="$(command -v migkit 2>/dev/null || echo "$HOME/.local/bin/migkit")"
  else
    rm -rf "$VENV_DIR"
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip
    "$VENV_DIR/bin/pip" install "$SRC_TARGET"
    TARGET_CLI="$VENV_DIR/bin/migkit"
    ln -sf "$TARGET_CLI" "$BIN_DIR/migkit"
  fi
fi

# 4. Check PATH accessibility
echo "[4/5] Verifying binary accessibility on PATH..."
if [[ -f "$TARGET_CLI" ]]; then
  echo "  binary executable at: $TARGET_CLI"
fi

IN_PATH=0
if [[ ":$PATH:" == *":$BIN_DIR:"* ]]; then
  IN_PATH=1
fi

if [[ "$IN_PATH" -eq 0 && "$INSTALL_DEV" -eq 0 ]]; then
  echo ""
  echo "NOTE: $BIN_DIR is not in your PATH."
  echo "Add the following line to your ~/.zshrc or ~/.bashrc:"
  echo "  export PATH=\"$BIN_DIR:\$PATH\""
  echo ""
fi

# 5. Run diagnostic doctor
echo "[5/5] Verifying installation with migkit doctor..."
if command -v migkit >/dev/null 2>&1; then
  MIGKIT_EXEC="migkit"
elif [[ -x "$BIN_DIR/migkit" ]]; then
  MIGKIT_EXEC="$BIN_DIR/migkit"
elif [[ -x "$TARGET_CLI" ]]; then
  MIGKIT_EXEC="$TARGET_CLI"
else
  echo "ERROR: migkit binary could not be located." >&2
  exit 1
fi

echo "migkit version: $("$MIGKIT_EXEC" --version)"
echo "Running migkit doctor (platform capabilities inspection)..."
MIGKIT_CONF=/tmp/migkit-install-check.yaml "$MIGKIT_EXEC" doctor || true

echo "================================================================================"
echo "Installation complete! You can now run:"
echo "  migkit --help"
echo "  migkit init"
echo "  migkit check <hop> --only counts"
echo "  migkit watch <hop> --verify --delta --count 1"
echo "================================================================================"
