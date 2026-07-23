#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: fix-sequences.sh <hop> [db ...]}"
shift
dbs="${*:-$(list_dbs)}"

for db in $dbs; do
  d="$RPT/$db"
  mkdir -p "$d"
  pg src "$db" -At -c "select format('select setval(%L, %s, true);',
      schemaname||'.'||sequencename, last_value)
    from pg_sequences
    where last_value is not null
      and schemaname not like '\\_\\_%'
      and sequencename not like 'migkit\\_%'" > "$d/setval.sql"
  if [ ! -s "$d/setval.sql" ]; then
    echo "$db: no sequences to sync"
    continue
  fi
  pg dst "$db" -f "$d/setval.sql" > /dev/null
  echo "$db: $(wc -l < "$d/setval.sql" | tr -d ' ') sequences synced"
done
echo "re-run check-sequences.sh to confirm"
