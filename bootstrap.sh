#!/usr/bin/env bash
# Contributor setup. Installing migkit does not need this script:
#
#   uv tool install migkit      # or pipx install migkit
#
# This creates a development virtualenv in the checkout and installs the test
# extra, so `pytest` works. Nothing here is best-effort: a partial install
# that "mostly works" is how a migration finds a missing driver at cutover
# instead of on the laptop.
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"
"$PY" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)' || {
  echo "migkit needs Python 3.10 or newer; $($PY -V 2>&1) found." >&2
  echo "Set PYTHON=/path/to/python3.12 ./bootstrap.sh to pick another." >&2
  exit 1
}

[ -d .venv ] || "$PY" -m venv .venv
. .venv/bin/activate
python -m pip -q install --upgrade pip
pip -q install -e ".[dev]"
echo "python packages installed (every engine, every comparison library)"

# Platform programs for the capabilities that need one. migkit reports what is
# still missing and exits non-zero only if something required is absent.
migkit doctor --install
