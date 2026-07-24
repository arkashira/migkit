#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -x /opt/homebrew/opt/libpq/bin/psql ]; then
  brew install libpq
fi

python3 -m venv .venv 2>/dev/null || true
. .venv/bin/activate
pip -q install -e . && echo "core installed"

for extra in mysql mongo redis kafka; do
  pip -q install -e ".[$extra]" 2>/dev/null && echo "$extra ok" \
    || echo "$extra skipped (install later if needed)"
done
if command -v uv > /dev/null && [ ! -x .venv-tools/bin/reladiff ]; then
  uv venv --python 3.12 .venv-tools 2>/dev/null \
    && uv pip install --python .venv-tools/bin/python -q reladiff psycopg2-binary \
    && echo "reladiff ok (py3.12 tools venv)" \
    || echo "reladiff skipped (data diff falls back to builtin checksum)"
fi
pip -q install migra psycopg2-binary 2>/dev/null && echo "migra ok" \
  || echo "migra skipped (schema diff falls back to pg_dump diff)"

command -v atlas > /dev/null || echo "optional: brew install ariga/tap/atlas (schema diff + fix DDL for pg/mysql)"
pip -q install datacompy 2>/dev/null && echo "datacompy ok" || echo "datacompy skipped"
pip -q install mysql-replication 2>/dev/null && echo "mysql-replication ok (delta verify)" \
  || echo "mysql-replication skipped"
# best-tool movers: migkit move --via auto picks whichever of these exist
command -v mydumper > /dev/null || echo "optional: brew install mydumper (parallel mysql move)"
command -v pgloader > /dev/null || echo "optional: brew install pgloader (mysql->pg move)"
command -v mongodump > /dev/null || echo "optional: brew install mongodb-database-tools (mongo move)"

if [ ! -f conf/hops.yaml ]; then
  cp conf/hops.example.yaml conf/hops.yaml
  echo "created conf/hops.yaml, fill in your endpoints"
fi
chmod 600 conf/hops.yaml 2>/dev/null || true

echo
echo "activate with: source $(pwd)/.venv/bin/activate"
.venv/bin/migkit doctor || true
