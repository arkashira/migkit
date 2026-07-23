#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: check-sequences.sh <hop> [db ...]}"
shift
dbs="${*:-$(list_dbs)}"

q="select schemaname||'.'||sequencename||'|'||coalesce(last_value,0)
   from pg_sequences
   where schemaname not like '\\_\\_%'
     and sequencename not like 'migkit\\_%' order by 1"

rc=0
for db in $dbs; do
  d="$RPT/$db"
  mkdir -p "$d"
  pg src "$db" -At -c "$q" > "$d/seq-src"
  pg dst "$db" -At -c "$q" > "$d/seq-dst"
  rm -f "$d/sequences.diff"

  awk -F'|' -v out="$d/sequences.diff" '
    FILENAME == ARGV[1] { s[$1] = $2; next }
    { d[$1] = $2 }
    END {
      for (k in s) {
        if (!(k in d)) print k" src="s[k]" dst=MISSING" > out
        else if (s[k] != d[k]) print k" src="s[k]" dst="d[k] > out
      }
      for (k in d) if (!(k in s)) print k" src=MISSING dst="d[k] > out
    }' "$d/seq-src" "$d/seq-dst"

  n=$(wc -l < "$d/seq-src" | tr -d ' ')
  if [ -s "$d/sequences.diff" ]; then
    rc=1
    echo "$db: sequences DIFF -> $d/sequences.diff"
    sort "$d/sequences.diff" | head -20
  else
    rm -f "$d/sequences.diff"
    echo "$db: sequences OK ($n)"
  fi
done
exit $rc
