#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: check-counts.sh <hop> [db ...]}"
shift
dbs="${*:-$(list_dbs)}"

rc=0
for db in $dbs; do
  d="$RPT/$db"
  mkdir -p "$d"
  list_tables src "$db" > "$d/tables-src"
  list_tables dst "$db" > "$d/tables-dst"

  comm -23 "$d/tables-src" "$d/tables-dst" | tr '|' '.' > "$d/tables.missing"
  comm -13 "$d/tables-src" "$d/tables-dst" | tr '|' '.' > "$d/tables.extra"
  if [ -s "$d/tables.missing" ]; then
    rc=1
    echo "$db: $(wc -l < "$d/tables.missing" | tr -d ' ') tables missing on target -> $d/tables.missing"
  fi
  if [ -s "$d/tables.extra" ]; then
    rc=1
    echo "$db: $(wc -l < "$d/tables.extra" | tr -d ' ') extra tables on target -> $d/tables.extra"
  fi

  comm -12 "$d/tables-src" "$d/tables-dst" | while IFS='|' read -r sch tbl; do
    printf 'select %s, count(*) from "%s"."%s";\n' "'$sch.$tbl'" "$sch" "$tbl"
  done > "$d/counts.sql"

  if [ ! -s "$d/counts.sql" ]; then
    echo "$db: no common tables"
    continue
  fi

  pg src "$db" -At -F'|' -f "$d/counts.sql" > "$d/counts-src" &
  p1=$!
  pg dst "$db" -At -F'|' -f "$d/counts.sql" > "$d/counts-dst" &
  p2=$!
  wait $p1
  wait $p2

  if paste -d'|' "$d/counts-src" "$d/counts-dst" \
      | awk -F'|' '$2 != $4 {print $1" src="$2" dst="$4; bad=1} END {exit bad}' \
      > "$d/counts.diff"; then
    rm -f "$d/counts.diff"
    total=$(awk -F'|' '{s += $2} END {print s+0}' "$d/counts-src")
    echo "$db: counts OK ($(wc -l < "$d/counts.sql" | tr -d ' ') tables, $total rows both sides)"
  else
    rc=1
    echo "$db: counts DIFF -> $d/counts.diff"
    cat "$d/counts.diff"
  fi
done
exit $rc
