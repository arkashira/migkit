#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: check-data-fast.sh <hop> [db] [schema.table]}"
only_db="${2:-}"
only_tbl="${3:-}"
WORKERS="${WORKERS:-8}"

fast_check() {
  local db="$1" sch="$2" tbl="$3"
  local t="$sch.$tbl" qt="\"$sch\".\"$tbl\""
  local t0=$SECONDS
  local q="select count(*)||'|'||coalesce(sum(
      ('x'||substr(md5(t::text),1,16))::bit(64)::bigint::numeric), 0)
    from $qt t"
  local ok=1
  PGX="-c max_parallel_workers_per_gather=$WORKERS"
  pg src "$db" -At -c "$q" > "$d/.f-src" &
  local p1=$!
  pg dst "$db" -At -c "$q" > "$d/.f-dst" &
  local p2=$!
  wait $p1 || ok=0
  wait $p2 || ok=0
  PGX=""
  if [ $ok = 0 ]; then
    echo "$t: ERROR (see above)"
    echo "$t error" >> "$d/data-summary-fast"
    return
  fi
  local a b
  a=$(cat "$d/.f-src")
  b=$(cat "$d/.f-dst")
  rm -f "$d/.f-src" "$d/.f-dst"
  if [ "$a" = "$b" ]; then
    echo "$t: OK rows=${a%%|*} checksum=${a##*|} ($((SECONDS - t0))s)"
    echo "$t ok" >> "$d/data-summary-fast"
  else
    echo "$t: DIFF src=$a dst=$b ($((SECONDS - t0))s)"
    echo "$t: drill down with ./check-data.sh $HOP $db $t"
    echo "$t diff" >> "$d/data-summary-fast"
  fi
}

rc=0
for db in $(list_dbs); do
  [ -n "$only_db" ] && [ "$db" != "$only_db" ] && continue
  d="$RPT/$db"
  mkdir -p "$d"
  : > "$d/data-summary-fast"
  while IFS='|' read -r sch tbl; do
    [ -n "$only_tbl" ] && [ "$sch.$tbl" != "$only_tbl" ] && continue
    fast_check "$db" "$sch" "$tbl"
  done < <(list_tables src "$db")
  grep -qv ' ok$' "$d/data-summary-fast" && rc=1 || true
done
exit $rc
