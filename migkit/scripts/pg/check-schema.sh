#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: check-schema.sh <hop> [db ...]}"
shift
dbs="${*:-$(list_dbs)}"

rc=0
for db in $dbs; do
  d="$RPT/$db"
  mkdir -p "$d"
  dump_schema src "$db" > "$d/schema-src.sql"
  dump_schema dst "$db" > "$d/schema-dst.sql"
  if diff -u "$d/schema-src.sql" "$d/schema-dst.sql" > "$d/schema.diff"; then
    rm -f "$d/schema.diff"
    echo "$db: schema OK"
  else
    rc=1
    echo "$db: schema DIFF, $(grep -c '^[+-]' "$d/schema.diff") lines -> $d/schema.diff"
  fi
done
exit $rc
