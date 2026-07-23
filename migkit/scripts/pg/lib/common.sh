set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="/opt/homebrew/opt/libpq/bin:$PATH"

PGOPTS="-c TimeZone=UTC -c DateStyle=ISO -c statement_timeout=0 -c extra_float_digits=3"

load_hop() {
  HOP="${1:?hop name required, see conf/}"
  local conf_dir="${PGDC_CONF_DIR:-$BASE/conf}"
  local f="$conf_dir/$HOP.conf"
  [ -f "$f" ] || { echo "missing $f" >&2; exit 1; }
  . "$f"
  : "${SRC_HOST:?SRC_HOST empty in $f}" "${DST_HOST:?DST_HOST empty in $f}"
  SRC_PORT="${SRC_PORT:-5432}"
  DST_PORT="${DST_PORT:-5432}"
  CHUNK="${CHUNK:-50000}"
  DRILL_MAX_ROWS="${DRILL_MAX_ROWS:-2000000}"
  RPT="${PGDC_REPORT_DIR:-$BASE/reports}/$HOP"
  mkdir -p "$RPT"
}

pg() {
  local side="$1" db="$2"; shift 2
  local h p u w
  case "$side" in
    src) h="$SRC_HOST" p="$SRC_PORT" u="$SRC_USER" w="$SRC_PASS" ;;
    dst) h="$DST_HOST" p="$DST_PORT" u="$DST_USER" w="$DST_PASS" ;;
  esac
  PGPASSWORD="$w" PGCONNECT_TIMEOUT=15 PGOPTIONS="$PGOPTS ${PGX:-}" PGCLIENTENCODING=UTF8 \
    psql -h "$h" -p "$p" -U "$u" -d "$db" -X -q -v ON_ERROR_STOP=1 "$@"
}

dump_schema() {
  local side="$1" db="$2"
  local h p u w
  case "$side" in
    src) h="$SRC_HOST" p="$SRC_PORT" u="$SRC_USER" w="$SRC_PASS" ;;
    dst) h="$DST_HOST" p="$DST_PORT" u="$DST_USER" w="$DST_PASS" ;;
  esac
  local noise="${PGDC_NOISE_PREFIX:-zz_no_noise_zz}"
  PGPASSWORD="$w" PGCONNECT_TIMEOUT=15 \
    pg_dump -h "$h" -p "$p" -U "$u" -d "$db" --schema-only --no-owner \
      --no-privileges --no-security-labels --no-tablespaces \
      --exclude-schema="${PGDC_EXCLUDE_SCHEMA:-__*}" \
      --exclude-table='*.migkit_changelog*' \
    | sed -e '/^--/d' -e '/^$/d' -e '/^SET /d' \
          -e '/^SELECT pg_catalog.set_config/d' \
          -e '/^\\restrict/d' -e '/^\\unrestrict/d' \
          -e "/^CREATE EVENT TRIGGER $noise/,/;\$/d" \
          -e "/^ALTER EVENT TRIGGER $noise/d" \
          -e "/^CREATE PUBLICATION $noise/d" -e "/^ALTER PUBLICATION $noise/d" \
    | { if [ -f "$BASE/conf/$HOP.schema-ignore" ]; then
          grep -v -E -f "$BASE/conf/$HOP.schema-ignore"
        else
          cat
        fi; }
}

list_dbs() {
  if [ -n "${DBS:-}" ]; then
    printf '%s\n' $DBS | tr ',' '\n' | sed '/^$/d'
  else
    pg src postgres -At -c "select datname from pg_database
      where not datistemplate and datname not in ('postgres','rdsadmin')
      order by 1"
  fi
}

list_tables() {
  pg "$1" "$2" -At -c "select n.nspname||'|'||c.relname
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.relkind = 'r'
      and n.nspname not in ('pg_catalog','information_schema')
      and n.nspname not like 'pg\\_%'
      and n.nspname not like '\\_\\_%'
      and c.relname not like 'migkit\\_%'
    order by 1"
}

pk_cols() {
  pg src "$1" -At -c "select a.attname
    from pg_index i
    join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
    where i.indrelid = '$2'::regclass and i.indisprimary
    order by array_position(i.indkey, a.attnum)"
}
