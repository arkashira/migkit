#!/usr/bin/env bash
. "$(dirname "$0")/lib/common.sh"
load_hop "${1:?usage: check-data.sh <hop> [db] [schema.table]}"
only_db="${2:-}"
only_tbl="${3:-}"

BIG_ROWS="${BIG_ROWS:-5000000}"
SLICE="${SLICE:-1000000}"

summary() {
  echo "$*" >> "$d/data-summary"
}

tuple() {
  printf '(%s)' "$(printf '%s' "$1" | tr '|' ',')"
}

classify() {
  awk -v mis="$d/data-$t.missing" -v ext="$d/data-$t.extra" -v chg="$d/data-$t.changed" '
    FILENAME == ARGV[1] { m[substr($0, 34)] = substr($0, 1, 32); next }
    {
      pk = substr($0, 34); h = substr($0, 1, 32)
      if (pk in m) { if (m[pk] != h) print pk >> chg; delete m[pk] }
      else print pk >> ext
    }
    END { for (pk in m) print pk >> mis }
  ' "$1" "$2"
}

tally() {
  local nm=0 nx=0 nc=0
  [ -f "$d/data-$t.missing" ] && nm=$(wc -l < "$d/data-$t.missing" | tr -d ' ')
  [ -f "$d/data-$t.extra" ] && nx=$(wc -l < "$d/data-$t.extra" | tr -d ' ')
  [ -f "$d/data-$t.changed" ] && nc=$(wc -l < "$d/data-$t.changed" | tr -d ' ')
  if [ "$nm" = 0 ] && [ "$nx" = 0 ] && [ "$nc" = 0 ]; then
    echo "$t: OK checksum flicker settled, 0 rows differ ($((SECONDS - t0))s)"
    summary "$t ok"
    return
  fi
  echo "$t: DIFF missing=$nm extra=$nx changed=$nc ($((SECONDS - t0))s) -> $d/data-$t.*"
  summary "$t diff missing=$nm extra=$nx changed=$nc"
}

big_check_table() {
  local db="$1" sch="$2" tbl="$3"
  local t="$sch.$tbl" qt="\"$sch\".\"$tbl\""
  local t0=$SECONDS

  local cols="" ctext="" c
  while IFS= read -r c; do
    cols="${cols:+$cols, }\"$c\""
    ctext="${ctext:+$ctext, }\"$c\"::text"
  done <<< "$pks"

  PGX="-c enable_seqscan=off"
  pg src "$db" -At -c "select concat_ws('|', $ctext) from
      (select $cols, row_number() over (order by $cols) as rn from $qt) s
    where rn % $SLICE = 0" > "$d/.bounds"
  PGX=""
  echo END >> "$d/.bounds"

  local total
  total=$(wc -l < "$d/.bounds" | tr -d ' ')
  rm -f "$d/data-$t.missing" "$d/data-$t.extra" "$d/data-$t.changed"
  local i=0 bad=0 a="" b pred
  while IFS= read -r b; do
    i=$((i + 1))
    if [ "$b" = END ] && [ -z "$a" ]; then
      pred="true"
    elif [ "$b" = END ]; then
      pred="($cols) > $(tuple "$a")"
    elif [ -z "$a" ]; then
      pred="($cols) <= $(tuple "$b")"
    else
      pred="($cols) > $(tuple "$a") and ($cols) <= $(tuple "$b")"
    fi

    local q="select count(*)||'|'||coalesce(md5(string_agg(h, '' order by $cols)), 'empty')
      from (select md5(to_jsonb(t)::text) as h, $cols from $qt t where $pred) s"
    pg src "$db" -At -c "$q" > "$d/.sl-src" &
    local p1=$!
    pg dst "$db" -At -c "$q" > "$d/.sl-dst" &
    local p2=$!
    wait $p1
    wait $p2
    if ! cmp -s "$d/.sl-src" "$d/.sl-dst"; then
      bad=$((bad + 1))
      local q2="select md5(to_jsonb(t)::text)||e'\\t'||concat_ws(e'\\t', $ctext)
        from $qt t where $pred"
      pg src "$db" -At -c "$q2" > "$d/.rows-src" &
      p1=$!
      pg dst "$db" -At -c "$q2" > "$d/.rows-dst" &
      p2=$!
      wait $p1
      wait $p2
      classify "$d/.rows-src" "$d/.rows-dst"
    fi
    [ $((i % 25)) = 0 ] && echo "  $t: slice $i/$total, $bad bad, $((SECONDS - t0))s"
    a="$b"
  done < "$d/.bounds"

  rm -f "$d/.bounds" "$d/.sl-src" "$d/.sl-dst" "$d/.rows-src" "$d/.rows-dst"
  if [ $bad = 0 ]; then
    echo "$t: OK big ($total slices, $((SECONDS - t0))s)"
    summary "$t ok"
  else
    tally
  fi
}

check_table() {
  local db="$1" sch="$2" tbl="$3"
  local t="$sch.$tbl" qt="\"$sch\".\"$tbl\""
  local t0=$SECONDS

  local pks
  pks=$(pk_cols "$db" "$qt")
  local rows
  rows=$(pg src "$db" -At -c "select greatest(coalesce(reltuples,0),0)::bigint
    from pg_class where oid = '$qt'::regclass")

  if [ -z "$pks" ]; then
    local q="select count(*)||'|'||coalesce(sum(abs(((('x'||substr(h,1,8))::bit(32)::int))::bigint)),0)
      from (select md5(to_jsonb(t)::text) as h from $qt t) s"
    local a b
    a=$(pg src "$db" -At -c "$q")
    b=$(pg dst "$db" -At -c "$q" || echo ERR)
    if [ "$a" = "$b" ]; then
      echo "$t: OK no-pk ($rows rows, $((SECONDS - t0))s)"
      summary "$t ok"
    else
      echo "$t: DIFF no-pk src=$a dst=$b (manual fix or fix-data --full)"
      summary "$t diff-nopk"
    fi
    return
  fi

  if [ "$rows" -gt "$BIG_ROWS" ]; then
    local nonint
    nonint=$(pg src "$db" -At -c "select count(*) from pg_attribute a
      join pg_index i on i.indrelid = a.attrelid
        and a.attnum = any(i.indkey) and i.indisprimary
      where a.attrelid = '$qt'::regclass
        and a.atttypid::regtype::text not in ('smallint','integer','bigint')")
    if [ "$nonint" = 0 ]; then
      big_check_table "$db" "$sch" "$tbl"
      return
    fi
    echo "$t: $rows rows with non-integer pk, whole-table checksum (heavy)"
  fi

  local nb=$(( rows / CHUNK + 1 ))
  local pkexpr="" c
  while IFS= read -r c; do
    pkexpr="${pkexpr:+$pkexpr, }\"$c\"::text"
  done <<< "$pks"
  pkexpr="concat_ws(e'\\t', $pkexpr)"
  local bexpr="abs(((('x'||substr(md5(pk),1,8))::bit(32)::int))::bigint) % $nb"

  local q="with r as (select $pkexpr as pk, md5(to_jsonb(t)::text) as h from $qt t)
    select $bexpr as b, count(*), md5(string_agg(h, '' order by pk collate \"C\"))
    from r group by 1 order by 1"
  local ok=1
  pg src "$db" -At -F'|' -c "$q" > "$d/.bk-src" &
  local p1=$!
  pg dst "$db" -At -F'|' -c "$q" > "$d/.bk-dst" &
  local p2=$!
  wait $p1 || ok=0
  wait $p2 || ok=0
  if [ $ok = 0 ]; then
    echo "$t: ERROR running checksum (see above)"
    summary "$t error"
    return
  fi

  if cmp -s "$d/.bk-src" "$d/.bk-dst"; then
    echo "$t: OK ($rows rows, $((SECONDS - t0))s)"
    summary "$t ok"
    rm -f "$d/.bk-src" "$d/.bk-dst"
    return
  fi

  awk -F'|' '
    FILENAME == ARGV[1] { s[$1] = $2"|"$3; n[$1] = $2; next }
    { d[$1] = $2"|"$3 }
    END {
      for (b in s) if (!(b in d) || d[b] != s[b]) { print b; c += n[b] }
      for (b in d) if (!(b in s)) print b
      print "ROWS "c > "/dev/stderr"
    }' "$d/.bk-src" "$d/.bk-dst" > "$d/.badbk" 2> "$d/.badrows"

  local badrows
  badrows=$(awk '{print $2+0}' "$d/.badrows")
  if [ "$badrows" -gt "$DRILL_MAX_ROWS" ]; then
    echo "$t: DIFF heavy, $badrows+ rows in mismatched buckets, skip drilldown (use fix-data --full)"
    summary "$t diff-heavy"
    return
  fi

  local blist
  blist=$(paste -sd, - < "$d/.badbk")
  local q2="with r as (select $pkexpr as pk, md5(to_jsonb(t)::text) as h from $qt t)
    select h||e'\\t'||pk from r where $bexpr in ($blist)"
  pg src "$db" -At -c "$q2" > "$d/.rows-src" &
  p1=$!
  pg dst "$db" -At -c "$q2" > "$d/.rows-dst" &
  p2=$!
  wait $p1
  wait $p2

  rm -f "$d/data-$t.missing" "$d/data-$t.extra" "$d/data-$t.changed"
  classify "$d/.rows-src" "$d/.rows-dst"
  tally
  rm -f "$d/.bk-src" "$d/.bk-dst" "$d/.rows-src" "$d/.rows-dst" "$d/.badbk" "$d/.badrows"
}

rc=0
for db in $(list_dbs); do
  [ -n "$only_db" ] && [ "$db" != "$only_db" ] && continue
  d="$RPT/$db"
  mkdir -p "$d"
  : > "$d/data-summary"
  while IFS='|' read -r sch tbl; do
    [ -n "$only_tbl" ] && [ "$sch.$tbl" != "$only_tbl" ] && continue
    check_table "$db" "$sch" "$tbl" || rc=1
  done < <(list_tables src "$db")
  grep -qv ' ok$' "$d/data-summary" && rc=1 || true
done
exit $rc
