#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: fix-data.sh <hop> <db> <schema.table> [--full]}"
db="${2:?db required}"
t="${3:?schema.table required}"
mode="${4:-}"
sch="${t%%.*}"
tbl="${t#*.}"
qt="\"$sch\".\"$tbl\""
d="$RPT/$db"

if [ "$mode" = "--full" ]; then
  echo "full resync $t: truncate target then copy all rows from source"
  pg src "$db" -c "\\copy (select * from $qt) to stdout" \
    | pg dst "$db" -1 -c "truncate $qt" -c "\\copy $qt from stdin"
  echo "done, run fix-sequences.sh then re-run check-data.sh $HOP $db $t"
  exit 0
fi

mis="$d/data-$t.missing"
ext="$d/data-$t.extra"
chg="$d/data-$t.changed"
[ -s "$mis" ] || [ -s "$ext" ] || [ -s "$chg" ] \
  || { echo "no diff files for $t, run check-data.sh first"; exit 1; }

pks=$(pk_cols "$db" "$qt")
cols=""
conds=""
i=0
while IFS= read -r c; do
  i=$((i + 1))
  cols="${cols:+$cols, }c$i text"
  conds="${conds:+$conds and }t.\"$c\"::text = p.c$i"
done <<< "$pks"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cat "$mis" "$chg" 2> /dev/null > "$work/copy.pks" || true
cat "$ext" "$chg" 2> /dev/null > "$work/del.pks" || true

if [ -s "$work/copy.pks" ]; then
  pg src "$db" > /dev/null <<EOF
create temp table _pk ($cols);
\\copy _pk from '$work/copy.pks'
\\copy (select t.* from $qt t join _pk p on $conds) to '$work/rows.out'
EOF
fi

UNDO="$d/undo"
mkdir -p "$UNDO"
STAMP=$(date +%Y%m%d-%H%M%S)
cp "$work/del.pks" "$UNDO/$STAMP-$t.del.pks" 2> /dev/null || true
cp "$work/copy.pks" "$UNDO/$STAMP-$t.copy.pks" 2> /dev/null || true

cat > "$work/apply.sql" <<EOF
set session_replication_role = replica;
create temp table _pk ($cols);
\\copy _pk from '$work/del.pks'
\\copy (select t.* from $qt t join _pk p on $conds) to '$UNDO/$STAMP-$t.rows'
delete from $qt t using _pk p where $conds;
EOF
cat >> "$UNDO/manifest.txt" <<EOF
$STAMP $t rollback: delete rows with pks in $STAMP-$t.del.pks and $STAMP-$t.copy.pks, then \\copy $qt from '$UNDO/$STAMP-$t.rows'
EOF
[ -s "$work/rows.out" ] && echo "\\copy $qt from '$work/rows.out'" >> "$work/apply.sql"

if ! pg dst "$db" -1 -f "$work/apply.sql" > /dev/null 2> "$work/err"; then
  if grep -q session_replication_role "$work/err"; then
    echo "target refused session_replication_role, retrying with triggers active"
    sed '/session_replication_role/d' "$work/apply.sql" > "$work/apply2.sql"
    pg dst "$db" -1 -f "$work/apply2.sql" > /dev/null
  else
    cat "$work/err" >&2
    exit 1
  fi
fi

nc=$(wc -l < "$work/copy.pks" | tr -d ' ')
nd=$(wc -l < "$work/del.pks" | tr -d ' ')
echo "$t: deleted $nd, copied $nc rows"
echo "re-run check-data.sh $HOP $db $t to confirm"
