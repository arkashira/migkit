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

pip -q install datacompy 2>/dev/null && echo "datacompy ok" || echo "datacompy skipped"
pip -q install mysql-replication 2>/dev/null && echo "mysql-replication ok (delta verify)" \
  || echo "mysql-replication skipped"

# every external tool migkit drives, installed for you (separate programs -
# never bundled into migkit, so GPL tools like pt-table-sync/mydumper stay at
# arm's length). formula:cmd pairs; each is attempted, missing ones warned.
BREW="brew"
command -v brew > /dev/null || { BREW=":"; echo "no brew found - install these tools manually:"; }
ensure() {  # ensure <cmd> <brew-formula> <purpose>
  command -v "$1" > /dev/null && { echo "$1 ok"; return; }
  $BREW install "$2" > /dev/null 2>&1 && echo "$1 ok ($2)" \
    || echo "MISSING $1 - needed for $3 - install: brew install $2"
}
ensure mysql        mysql-client              "mysql schema dump / cli"
ensure mysqldump    mysql-client              "mysql schema dump"
ensure mongodump    mongodb-database-tools    "mongo move"
ensure mongorestore mongodb-database-tools    "mongo move"
ensure mydumper     mydumper                  "parallel mysql move"
ensure pgloader     pgloader                  "mysql->pg move"
ensure pt-table-sync percona-toolkit          "mysql row repair"
ensure atlas        ariga/tap/atlas           "authoritative schema diff + repair DDL"
ensure liquibase    liquibase                 "schema diff (4th opinion)"
ensure sqlcmd       sqlcmd                     "mssql (needs microsoft tap)"

if [ ! -f conf/hops.yaml ]; then
  cp conf/hops.example.yaml conf/hops.yaml
  echo "created conf/hops.yaml, fill in your endpoints"
fi
chmod 600 conf/hops.yaml 2>/dev/null || true

echo
echo "activate with: source $(pwd)/.venv/bin/activate"
.venv/bin/migkit doctor || true
