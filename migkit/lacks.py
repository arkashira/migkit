"""What the source uses that the target does not have, asked of both
servers (backlog P0-SCENARIOS: a version down, a fork, an edition).

Whatever brought the two servers apart - a target a version older, another
brand behind the same protocol, a build without a library - the question
the move stops on half way is one: does the target have every type,
collation, extension, module and feature the source's databases use? It is
asked here before anything is written, and each answer is the target's
own wherever the target can give it:

* PostgreSQL - the built-in types the source's columns hold, looked up in
  the target's `pg_type`; the collations they sort by, in its
  `pg_collation`; each extension offered at the source's version or a
  later one (`upgrade._pg_extensions`, the one reading of it);
* MySQL and its forks - each collation in scope used by the target in an
  expression (`_my_collations`): a collation the target takes only as an
  alias - MariaDB 11.4.5 on takes `utf8mb4_0900_ai_ci` - is not in its
  list, and is still one it takes;
* SQL Server - the system types the columns hold, in the target's
  `sys.types`; the collations, in its `sys.fn_helpcollations()`;
* Oracle - text over 4000 bytes against the target's own
  `max_string_size`;
* MongoDB - each view's pipeline and each collection's validator run on
  the target against a collection that is not there: a stage or an
  operator it does not know is refused by its parser, and nothing is
  read or written;
* Redis and its forks - every command migkit's copy and check send, in
  the target's `COMMAND INFO`; every module the source loads, in its
  `MODULE LIST`, and whether the source's keys hold its types.

Only what no catalogue answers is held to a table (`PG`, `MY`, `ORA`,
`MONGO`, `SEARCH`): the feature, a statement that finds what uses it on
the source, and the version of each brand from which it is had - from the
release notes of each (PostgreSQL's "E.x" appendix, the MySQL 8.0 and 9.0
reference manuals' "What is new", MariaDB's release notes, Oracle's
Database New Features guides for 12.1, 12.2, 21c and 23ai, the MongoDB
manual's release notes for 3.4 to 7.0, the Elasticsearch and OpenSearch
field type references). A brand the table does not name, or whose
version is numbered otherwise than its family's own software's
(`case.FAMILY_NUMBERED` - CockroachDB's v23, a compatibility claim), is
"unknown, not clean" where the source uses the feature - never a pass.
A finder runs only where the source's own brand and version can hold the
feature at all.
"""
import re
from dataclasses import dataclass, field

SCOPE = "the target"


def _row(level, item, detail, fix=""):
    return {"level": level, "scope": SCOPE, "item": item,
            "detail": detail + (f"  [fix] {fix}" if fix and level != "pass"
                                else "")}


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def numbers(text, n=3):
    """The first `n` numbers of a version text as a tuple, padded with
    zeros; None where it holds none."""
    got = [int(x) for x in re.findall(r"\d+", str(text or ""))[:n]]
    return tuple(got + [0] * (n - len(got))) if got else None


def _pad(v, n=3):
    return tuple(list(v)[:n] + [0] * (n - len(v))) if v is not None else None


def _said(names, most=6):
    names = list(names)
    return ", ".join(names[:most]) + (f" and {len(names) - most} more"
                                      if len(names) > most else "")


@dataclass(frozen=True)
class Side:
    """One server as this module reads it: its brand's name (the engine's
    own name where the engine has no brands), its version as numbers,
    whether that version is numbered as its family's own software's
    (`real`), and how it is said."""
    brand: str
    version: tuple = None
    real: bool = True
    label: str = ""


@dataclass(frozen=True)
class Feature:
    """One thing a server may lack: what it is, how the source's uses of it
    are found (`finds(eng, db)` -> [names]), and the version of each brand
    from which it is had - None for a brand that never has it, "*" for
    every brand whose version is its own. `level` is what a target without
    it means: fail - what uses it cannot be made there, or behaves
    otherwise; warn - it is made, and works a little otherwise."""
    key: str
    said: str
    finds: object
    since: dict = field(default_factory=dict)
    fix: str = ""
    level: str = "fail"


_MISSING = object()


def had(feature, side):
    """Whether `side` has `feature`: True, False, or None where it is not
    known - a brand the table does not name, a version not read, or a
    version numbered otherwise than the family's, read against "*"."""
    at = feature.since.get(side.brand, _MISSING)
    named = at is not _MISSING
    if not named:
        at = feature.since.get("*", _MISSING)
    if at is _MISSING:
        return None
    if at is None:
        return False
    if side.version is None or (not side.real and not named):
        return None
    return _pad(side.version) >= _pad(at)


def _features(table, eng, db, src, dst):
    """Rows for the features of `table` the source uses and the target
    has not, or may not have."""
    out, looked = [], 0
    for f in table:
        there = had(f, dst)
        if there is True:
            continue
        if had(f, src) is False:
            # the source cannot hold it at all: nothing to find
            continue
        looked += 1
        try:
            found = list(f.finds(eng, db) or [])
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", f.said, f"could not be read on the"
                                            f" source: {_last(e)} - unknown,"
                                            " not clean"))
            continue
        if not found:
            continue
        where = f"{len(found)} on the source: {_said(found)}"
        if there is None:
            out.append(_row("warn", f.said,
                            f"{where}; whether {dst.label} has them is not"
                            " known - unknown, not clean", f.fix))
            continue
        since = f.since.get(dst.brand, f.since.get("*"))
        has_it = (f" (it has them from {'.'.join(map(str, since))})"
                  if since else "")
        out.append(_row(f.level, f.said, f"{where}; {dst.label} has"
                                         f" none{has_it}", f.fix))
    return out, looked


# --- PostgreSQL ------------------------------------------------------------------

_PG_USER = ("n.nspname not in ('pg_catalog', 'information_schema')"
            " and n.nspname not like 'pg\\_%'")
_PG_COLS = ("from pg_attribute a join pg_class c on c.oid = a.attrelid"
            " join pg_namespace n on n.oid = c.relnamespace"
            " where a.attnum > 0 and not a.attisdropped"
            " and c.relkind in ('r', 'p', 'm') and " + _PG_USER)
_PG_COL = "select n.nspname || '.' || c.relname || '.' || a.attname "


def _pg_names(sql):
    def finds(eng, db):
        from .upgrade import _pg
        return [r[0] for r in _pg(eng, "src", db, sql) if r and r[0]]
    return finds


#: what a PostgreSQL target a version older has not
PG = (
    Feature("identity", "identity columns",
            _pg_names(_PG_COL + _PG_COLS + " and a.attidentity <> ''"),
            {"*": (10,)}, "make each a column with a sequence default on the"
                          " source, or a target of version 10 or later"),
    Feature("partitioned", "partitioned tables", _pg_names(
        "select n.nspname || '.' || c.relname from pg_class c join"
        " pg_namespace n on n.oid = c.relnamespace where c.relkind = 'p'"
        " and " + _PG_USER), {"*": (10,)},
        "a target of version 10 or later"),
    Feature("hash partitions", "tables partitioned by hash", _pg_names(
        "select n.nspname || '.' || c.relname from pg_partitioned_table p"
        " join pg_class c on c.oid = p.partrelid join pg_namespace n on"
        " n.oid = c.relnamespace where p.partstrat = 'h' and " + _PG_USER),
        {"*": (11,)}, "a target of version 11 or later"),
    Feature("default partitions", "default partitions", _pg_names(
        "select n.nspname || '.' || c.relname from pg_partitioned_table p"
        " join pg_class c on c.oid = p.partrelid join pg_namespace n on"
        " n.oid = c.relnamespace where p.partdefid <> 0 and " + _PG_USER),
        {"*": (11,)}, "a target of version 11 or later"),
    Feature("procedures", "procedures", _pg_names(
        "select n.nspname || '.' || p.proname from pg_proc p join"
        " pg_namespace n on n.oid = p.pronamespace where p.prokind = 'p'"
        " and " + _PG_USER), {"*": (11,)},
        "a target of version 11 or later, or each made a function"),
    Feature("stored generated", "stored generated columns",
            _pg_names(_PG_COL + _PG_COLS + " and a.attgenerated = 's'"),
            {"*": (12,)}, "a target of version 12 or later, or each"
                          " column filled by a trigger instead"),
    Feature("sql bodies", "functions with SQL-standard bodies", _pg_names(
        "select n.nspname || '.' || p.proname from pg_proc p join"
        " pg_namespace n on n.oid = p.pronamespace where p.prosqlbody is"
        " not null and " + _PG_USER), {"*": (14,)},
        "a target of version 14 or later, or each written with a quoted"
        " body (AS $$ ... $$)"),
    Feature("lz4", "columns compressed with lz4",
            _pg_names(_PG_COL + _PG_COLS + " and a.attcompression = 'l'"),
            {"*": (14,)}, "ALTER TABLE ... ALTER COLUMN ... SET COMPRESSION"
                          " pglz on the source, or a target of version 14"
                          " or later"),
    Feature("nulls not distinct", "unique indexes with NULLS NOT DISTINCT",
            _pg_names(
                "select n.nspname || '.' || i.relname from pg_index x join"
                " pg_class i on i.oid = x.indexrelid join pg_namespace n on"
                " n.oid = i.relnamespace where x.indnullsnotdistinct and "
                + _PG_USER), {"*": (15,)},
            "a target of version 15 or later: without it the target takes"
            " two rows the source refuses"),
    Feature("security invoker", "views run with the caller's rights"
                                " (security_invoker)", _pg_names(
        "select n.nspname || '.' || c.relname from pg_class c join"
        " pg_namespace n on n.oid = c.relnamespace where c.relkind = 'v'"
        " and exists (select 1 from unnest(c.reloptions) o where o in"
        " ('security_invoker=true', 'security_invoker=on',"
        " 'security_invoker=1')) and " + _PG_USER), {"*": (15,)},
        "a target of version 15 or later: without it each view reads with"
        " its owner's rights"),
    Feature("virtual generated", "virtual generated columns",
            _pg_names(_PG_COL + _PG_COLS + " and a.attgenerated = 'v'"),
            {"*": (18,)}, "a target of version 18 or later, or each made a"
                          " stored generated column"),
    Feature("without overlaps", "keys WITHOUT OVERLAPS", _pg_names(
        "select conrelid::regclass::text || '.' || conname from"
        " pg_constraint where conperiod"), {"*": (18,)},
        "a target of version 18 or later"),
)


def _pg_types(eng, db):
    """Built-in types the source's columns hold that the target's
    `pg_type` does not have."""
    from .upgrade import _pg
    rows = _pg(eng, "src", db, _PG_COL + "|| chr(31) || t.typname "
               + _PG_COLS.replace(
                   "where", "join pg_type t on t.oid = a.atttypid where", 1)
               + " and t.typnamespace = 'pg_catalog'::regnamespace")
    used = {}
    for col, typ in (r for r in rows if len(r) == 2):
        used.setdefault(typ, []).append(col)
    if not used:
        return []
    names = ", ".join("'" + t.replace("'", "''") + "'" for t in used)
    have = {r[0] for r in _pg(eng, "dst", "postgres",
                              "select typname from pg_type where"
                              " typnamespace = 'pg_catalog'::regnamespace"
                              f" and typname in ({names})") if r}
    gone = sorted(set(used) - have)
    if not gone:
        return [_row("pass", "types", f"the {len(used)} built-in types the"
                                      " source's columns hold are all the"
                                      " target's")]
    return [_row("fail", "types",
                 "the target has no type " + ", ".join(gone) + ": "
                 + _said(c for t in gone for c in used[t]),
                 "change each column's type on the source, or a target"
                 " that has the type")]


def _pg_collations(eng, db):
    """Collations the source's columns sort by that the target's
    `pg_collation` does not have: a table made with one is refused."""
    from .upgrade import _pg
    rows = _pg(eng, "src", db, _PG_COL + "|| chr(31) || k.collname "
               + _PG_COLS.replace(
                   "where", "join pg_collation k on k.oid = a.attcollation"
                            " where", 1)
               + " and k.collnamespace = 'pg_catalog'::regnamespace"
                 " and k.collname not in ('default', 'C', 'POSIX')")
    used = {}
    for col, name in (r for r in rows if len(r) == 2):
        used.setdefault(name, []).append(col)
    if not used:
        return []
    names = ", ".join("'" + c.replace("'", "''") + "'" for c in used)
    have = {r[0] for r in _pg(eng, "dst", "postgres",
                              "select collname from pg_collation where"
                              f" collname in ({names})") if r}
    gone = sorted(set(used) - have)
    if not gone:
        return [_row("pass", "collations", f"the {len(used)} collations the"
                                           " source's columns sort by are"
                                           " all the target's")]
    return [_row("fail", "collations",
                 "the target has no collation " + ", ".join(gone)
                 + " (a server built without the library that makes it, or"
                   " a version before it): " + _said(
                       c for n in gone for c in used[n]),
                 "a target built with it, or each column given a collation"
                 " the target has on the source first")]


def _pg(hop, eng, db, src, dst, extensions=True):
    from . import upgrade
    out, looked = _features(PG, eng, db, src, dst)
    for part in (_pg_types, _pg_collations):
        try:
            out += part(eng, db)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", "types and collations",
                            f"could not be read: {_last(e)} - unknown, not"
                            " clean"))
    if extensions:
        try:
            out += upgrade._pg_extensions(eng, db, False, "postgres")
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", "extensions", f"could not be read:"
                                                  f" {_last(e)} - unknown,"
                                                  " not clean"))
    return out, looked


# --- MySQL and its forks -----------------------------------------------------------

def _my_rows(sql, per_db=True):
    def finds(eng, db):
        from .upgrade import _my
        e = getattr(eng, "src_engine", None) or eng
        args = (e._d("src", db),) if per_db else None
        return [str(r[0]) for r in _my(eng, "src", sql, args) if r and r[0]]
    return finds


_MY_COLS = ("select concat(table_name, '.', column_name) from"
            " information_schema.columns where table_schema = %s and ")


def _my_code(pattern):
    """The routines, views and triggers whose text matches `pattern`."""
    test = re.compile(pattern, re.I | re.S)

    def finds(eng, db):
        from .upgrade import _my
        e = getattr(eng, "src_engine", None) or eng
        name = e._d("src", db)
        rows = _my(eng, "src", """
            select concat('routine ', routine_name), routine_definition
              from information_schema.routines where routine_schema = %s
            union all
            select concat('view ', table_name), view_definition
              from information_schema.views where table_schema = %s
            union all
            select concat('trigger ', trigger_name), action_statement
              from information_schema.triggers where trigger_schema = %s""",
                   (name, name, name))
        return [str(n) for n, body in rows if test.search(str(body or ""))]
    return finds


_MY8 = {"mysql": (8, 0, 0), "percona": (8, 0, 0)}

#: what a MySQL-protocol target of another version or brand has not
MY = (
    Feature("invisible columns", "invisible columns",
            _my_rows(_MY_COLS + "extra like '%%INVISIBLE%%'"),
            {"mysql": (8, 0, 23), "percona": (8, 0, 23),
             "mariadb": (10, 3, 3)},
            "make them visible on the source, or a target that has them"),
    Feature("functional key parts", "indexes on expressions", _my_rows(
        "select distinct concat(table_name, '.', index_name) from"
        " information_schema.statistics where table_schema = %s and"
        " expression is not null and expression not like '%%array%%'"),
        {"mysql": (8, 0, 13), "percona": (8, 0, 13), "mariadb": None},
        "index a generated column holding the expression instead"),
    Feature("multi-valued indexes", "multi-valued indexes", _my_rows(
        "select distinct concat(table_name, '.', index_name) from"
        " information_schema.statistics where table_schema = %s and"
        " expression like '%%array%%'"),
        {"mysql": (8, 0, 17), "percona": (8, 0, 17), "mariadb": None},
        "drop them on the source, or a target that has them"),
    Feature("check constraints", "check constraints", _my_rows(
        "select concat(table_name, '.', constraint_name) from"
        " information_schema.table_constraints where table_schema = %s and"
        " constraint_type = 'CHECK'"),
        {"mysql": (8, 0, 16), "percona": (8, 0, 16), "mariadb": (10, 2, 1)},
        "a target that enforces them: an older one reads CHECK and ignores"
        " it, and takes the rows the source refuses"),
    Feature("expression defaults", "defaults that are expressions",
            _my_rows(_MY_COLS + "extra like '%%DEFAULT_GENERATED%%' and"
                                " upper(column_default) not like"
                                " 'CURRENT_TIMESTAMP%%'"),
            {"mysql": (8, 0, 13), "percona": (8, 0, 13),
             "mariadb": (10, 2, 1)},
            "fill each by a trigger instead, or a target that has them"),
    Feature("descending indexes", "descending indexes", _my_rows(
        "select distinct concat(table_name, '.', index_name) from"
        " information_schema.statistics where table_schema = %s and"
        " collation = 'D'"),
        {**_MY8, "mariadb": (10, 8, 1)},
        "nothing to do: the target makes each ascending, and reads it"
        " backwards", level="warn"),
    Feature("srid", "spatial columns restricted to one SRID",
            _my_rows(_MY_COLS + "srs_id is not null"),
            {**_MY8, "mariadb": None},
            "drop the SRID attribute on the source, or a MySQL 8 target"),
    Feature("vector", "vector columns", _my_rows(_MY_COLS
                                                 + "data_type = 'vector'"),
            {"mysql": (9, 0, 0)},
            "a target that has the type, or the column carried as binary"),
    Feature("generated columns", "generated columns",
            _my_rows(_MY_COLS + "(extra like '%%VIRTUAL GENERATED%%' or"
                                " extra like '%%STORED GENERATED%%')"),
            {"mysql": (5, 7, 6), "percona": (5, 7, 6),
             "mariadb": (5, 2, 0)}, "a target of 5.7 or later"),
    Feature("json", "JSON columns",
            _my_rows(_MY_COLS + "data_type = 'json'"),
            {"mysql": (5, 7, 8), "percona": (5, 7, 8),
             "mariadb": (10, 2, 7)}, "a target of 5.7.8 or later"),
    Feature("roles", "roles", _my_rows(
        "select concat(from_user, '@', from_host, ' to ', to_user, '@',"
        " to_host) from mysql.role_edges", per_db=False), _MY8,
        "grant each role's privileges to its users directly on the target",
        level="warn"),
    Feature("caching sha2", "users signing in with caching_sha2_password",
            _my_rows("select concat(user, '@', host) from mysql.user where"
                     " plugin = 'caching_sha2_password'", per_db=False),
            {"mysql": (8, 0, 4), "percona": (8, 0, 4), "mariadb": None},
            "ALTER USER ... IDENTIFIED WITH a plugin the target has, for"
            " each (their clients must speak it)", level="warn"),
    Feature("window functions", "code calling window functions",
            _my_code(r"\bover\s*\("),
            {"mysql": (8, 0, 2), "percona": (8, 0, 2),
             "mariadb": (10, 2, 0)},
            "rewrite each before the move; the target refuses to make it"),
    Feature("common table expressions", "code with WITH clauses",
            _my_code(r"\bwith\s+(recursive\s+)?`?\w+`?\s*(\([^)]*\))?\s*as"
                     r"\s*\("),
            {"mysql": (8, 0, 1), "percona": (8, 0, 1),
             "mariadb": (10, 2, 1)},
            "rewrite each before the move; the target refuses to make it"),
    Feature("json_table", "code calling JSON_TABLE",
            _my_code(r"\bjson_table\s*\("),
            {"mysql": (8, 0, 4), "percona": (8, 0, 4),
             "mariadb": (10, 6, 0)},
            "rewrite each before the move; the target refuses to make it"),
    Feature("lateral", "code with LATERAL derived tables",
            _my_code(r"\blateral\s*\("),
            {"mysql": (8, 0, 14), "percona": (8, 0, 14), "mariadb": None},
            "rewrite each before the move; the target refuses to make it"),
)


def _my_collations(eng, dbs):
    """Collations the source's schemas, tables and columns use that the
    target will not take. Its own list first; one it does not list is
    asked of it in an expression - an alias (MariaDB 11.4.5 on takes
    MySQL 8's `utf8mb4_0900_ai_ci`) is not listed, and is taken."""
    from .upgrade import _my
    e = getattr(eng, "src_engine", None) or eng
    names = [e._d("src", d) for d in dbs]
    if not names:
        return []
    marks = ", ".join(["%s"] * len(names))
    used = {}
    for coll, charset in _my(eng, "src", f"""
            select c.collation_name, c.character_set_name
              from information_schema.columns c
             where c.table_schema in ({marks})
               and c.collation_name is not null
            union select t.table_collation, k.character_set_name
              from information_schema.tables t
              join information_schema.collations k
                on k.collation_name = t.table_collation
             where t.table_schema in ({marks})
            union select s.default_collation_name, s.default_character_set_name
              from information_schema.schemata s
             where s.schema_name in ({marks})""", tuple(names) * 3):
        if coll:
            used[str(coll)] = str(charset or "")
    if not used:
        return []
    listed = {str(r[0]) for r in _my(
        eng, "dst", "select collation_name from"
                    " information_schema.collations")}
    gone = []
    for coll, charset in sorted(used.items()):
        if coll in listed:
            continue
        if not re.fullmatch(r"\w+", coll) or not re.fullmatch(r"\w+",
                                                                charset):
            gone.append(coll)
            continue
        try:
            _my(eng, "dst", f"select convert('' using {charset}) collate"
                            f" {coll}")
        except Exception:  # noqa: BLE001 - the target's own refusal
            gone.append(coll)
    if not gone:
        return [_row("pass", "collations", f"all {len(used)} collations in"
                                           " scope are taken by the"
                                           " target")]
    return [_row("fail", "collations",
                 f"{len(gone)} used in scope and unknown to the target,"
                 " where making a table with one stops the move: "
                 + _said(gone),
                 "choose, for each, the collation the target has that"
                 " sorts and compares as the application expects, and"
                 " change the source's columns to it before the move -"
                 " migkit does not choose how text sorts")]


def _my(hop, eng, db, src, dst):
    return _features(MY, eng, db, src, dst)


# --- SQL Server --------------------------------------------------------------------

def _ms(hop, eng, db, src, dst):
    e = getattr(eng, "src_engine", None) or eng
    rows = e._rows("src", db, "select s.name + '.' + t.name + '.' + c.name,"
                              " ty.name from sys.columns c join sys.tables t"
                              " on t.object_id = c.object_id join sys.schemas"
                              " s on s.schema_id = t.schema_id join sys.types"
                              " ty on ty.user_type_id = c.user_type_id where"
                              " t.is_ms_shipped = 0 and ty.is_user_defined ="
                              " 0")
    used = {}
    for col, typ in rows:
        used.setdefault(str(typ), []).append(str(col))
    out = []
    if used:
        d = getattr(eng, "dst_engine", None) or eng
        have = {str(r[0]) for r in d._rows(
            "dst", "master", "select name from sys.types where is_user_defined"
                       " = 0")}
        gone = sorted(set(used) - have)
        out.append(_row(
            "fail" if gone else "pass", "types",
            ("the target has no type " + ", ".join(gone) + ": " + _said(
                c for t in gone for c in used[t])) if gone else
            f"the {len(used)} system types the columns hold are all the"
            " target's", "change each column's type on the source, or a"
                         " target of a version that has the type"))
    colls = {}
    for col, coll in e._rows(
            "src", db, "select s.name + '.' + t.name + '.' + c.name,"
                       " c.collation_name from sys.columns c join sys.tables"
                       " t on t.object_id = c.object_id join sys.schemas s"
                       " on s.schema_id = t.schema_id where t.is_ms_shipped"
                       " = 0 and c.collation_name is not null union all"
                       " select '(the database)', cast(databasepropertyex("
                       "db_name(), 'Collation') as nvarchar(128))"):
        colls.setdefault(str(coll), []).append(str(col))
    if colls:
        d = getattr(eng, "dst_engine", None) or eng
        have = {str(r[0]) for r in d._rows(
            "dst", "master", "select name from sys.fn_helpcollations()")}
        gone = sorted(set(colls) - have)
        out.append(_row(
            "fail" if gone else "pass", "collations",
            (f"the target has no collation {', '.join(gone)} (UTF-8"
             " collations come with 2019, the _140 ones with 2017): "
             + _said(c for n in gone for c in colls[n])) if gone else
            f"the {len(colls)} collations in use are all the target's",
            "change each column's collation on the source, or a target"
            " that has it"))
    return out, 0


# --- Oracle ------------------------------------------------------------------------

def _ora_rows(sql):
    def finds(eng, db):
        e = getattr(eng, "src_engine", None) or eng
        from .engines.dbapi import MARK
        owner = e._owner("src", db)
        return [str(r[0]) for r in e._rows(
            "src", db, sql.replace(":owner", MARK),
            (owner,) * sql.count(":owner")) if r and r[0]]
    return finds


def _ora_type(name):
    return _ora_rows("select table_name || '.' || column_name from"
                     " all_tab_columns where owner = :owner and data_type"
                     f" = '{name}'")


#: what an Oracle target a version older has not
ORA = (
    Feature("boolean", "BOOLEAN columns", _ora_type("BOOLEAN"),
            {"oracle": (23,)}, "make each a NUMBER(1) on the source, or a"
                               " target of 23ai"),
    Feature("vector", "VECTOR columns", _ora_type("VECTOR"),
            {"oracle": (23,)}, "a target of 23ai"),
    Feature("json type", "JSON columns of the native type",
            _ora_type("JSON"), {"oracle": (21,)},
            "make each a BLOB or CLOB with an IS JSON check, or a target of"
            " 21c or later"),
    Feature("long names", "names longer than 30 bytes", _ora_rows(
        "select table_name from all_tables where owner = :owner and"
        " lengthb(table_name) > 30 union all select table_name || '.' ||"
        " column_name from all_tab_columns where owner = :owner and"
        " lengthb(column_name) > 30"), {"oracle": (12, 2)},
        "rename each to 30 bytes or less, or a target of 12.2 or later"),
    Feature("identity", "identity columns", _ora_rows(
        "select table_name || '.' || column_name from all_tab_identity_cols"
        " where owner = :owner"), {"oracle": (12, 1)},
        "a sequence and a trigger for each, or a target of 12c or later"),
)


def _ora_long_text(eng, db):
    """Text and raw columns over 4000 bytes, against the target's own
    `max_string_size`: a target that keeps it STANDARD refuses them."""
    e = getattr(eng, "src_engine", None) or eng
    from .engines.dbapi import MARK
    rows = e._rows("src", db, "select table_name || '.' || column_name from"
                              f" all_tab_columns where owner = {MARK} and"
                              " data_type in ('VARCHAR2', 'NVARCHAR2', 'RAW')"
                              " and data_length > 4000",
                   (e._owner("src", db),))
    long = [str(r[0]) for r in rows if r and r[0]]
    if not long:
        return []
    d = getattr(eng, "dst_engine", None) or eng
    got = d._rows("dst", db, "select value from v$parameter where name ="
                             " 'max_string_size'")
    size = str(got[0][0]).upper() if got else ""
    if size == "EXTENDED":
        return [_row("pass", "long text", f"{len(long)} columns over 4000"
                                          " bytes, and the target's strings"
                                          " are extended")]
    return [_row("fail", "long text",
                 f"{len(long)} columns over 4000 bytes on the source, and"
                 f" the target keeps max_string_size {size or 'STANDARD'}: "
                 + _said(long), "set max_string_size = EXTENDED on the"
                                " target (a DBA's, with its own upgrade"
                                " script), or make each a CLOB")]


def _ora(hop, eng, db, src, dst):
    out, looked = _features(ORA, eng, db, src, dst)
    try:
        out += _ora_long_text(eng, db)
    except Exception as e:  # noqa: BLE001 - unknown, said so
        out.append(_row("warn", "long text", f"could not be read:"
                                             f" {_last(e)} - unknown, not"
                                             " clean"))
    return out, looked


# --- MongoDB -----------------------------------------------------------------------

def _mongo_collections(eng, db):
    e = getattr(eng, "src_engine", None) or eng
    return list(e._client("src")[e._d("src", db)].list_collections())


def _mongo_options(test):
    def finds(eng, db):
        return [str(c.get("name")) for c in _mongo_collections(eng, db)
                if test(c.get("options") or {}, c)]
    return finds


def _mongo_indexes(test):
    def finds(eng, db):
        e = getattr(eng, "src_engine", None) or eng
        d = e._client("src")[e._d("src", db)]
        out = []
        for c in _mongo_collections(eng, db):
            if c.get("type") == "view" or str(c.get("name", "")).startswith(
                    "system."):
                continue
            for ix in d[c["name"]].list_indexes():
                if test(dict(ix)):
                    out.append(f"{c['name']}.{ix.get('name')}")
        return out
    return finds


def _hashed_compound(ix):
    key = dict(ix.get("key") or {})
    return len(key) > 1 and "hashed" in [str(v) for v in key.values()]


_MG = ("mongodb", "percona-server-mongodb")

#: what a MongoDB-protocol target of another version or brand has not
MONGO = (
    Feature("timeseries", "time-series collections",
            _mongo_options(lambda o, c: bool(o.get("timeseries"))),
            dict.fromkeys(_MG, (5, 0)), "a target that has them"),
    Feature("clustered", "clustered collections",
            _mongo_options(lambda o, c: bool(o.get("clusteredIndex"))),
            dict.fromkeys(_MG, (5, 3)),
            "a target that has them, or each made an ordinary collection on"
            " the source"),
    Feature("pre and post images", "collections keeping pre- and"
                                   " post-images for change streams",
            _mongo_options(lambda o, c: bool((o.get(
                "changeStreamPreAndPostImages") or {}).get("enabled"))),
            dict.fromkeys(_MG, (6, 0)),
            "nothing to do for the move: the target's change streams carry"
            " no before-image of a change", level="warn"),
    Feature("encrypted fields", "collections with queryable encryption",
            _mongo_options(lambda o, c: bool(o.get("encryptedFields"))),
            dict.fromkeys(_MG, (6, 0)), "a target that has it"),
    Feature("collation", "collations on collections or indexes",
            lambda eng, db: _mongo_options(
                lambda o, c: bool(o.get("collation")))(eng, db)
            + _mongo_indexes(lambda ix: bool(ix.get("collation")))(eng, db),
            dict.fromkeys(_MG, (3, 4)),
            "drop the collation on the source, or a target that has them"),
    Feature("views", "views",
            _mongo_options(lambda o, c: c.get("type") == "view"),
            dict.fromkeys(_MG, (3, 4)), "a target that has views"),
    Feature("json schema", "validators with $jsonSchema",
            _mongo_options(lambda o, c: "$jsonSchema" in str(o.get(
                "validator") or "")),
            dict.fromkeys(_MG, (3, 6)), "a target that has them"),
    Feature("wildcard", "wildcard indexes",
            _mongo_indexes(lambda ix: any("$**" in str(k) for k in dict(
                ix.get("key") or {}))),
            dict.fromkeys(_MG, (4, 2)), "a target that has them"),
    Feature("hashed compound", "compound hashed indexes",
            _mongo_indexes(_hashed_compound),
            dict.fromkeys(_MG, (4, 4)), "a target that has them"),
)

#: the collection the target is asked about: never there, so a pipeline or a
#: filter run against it reads nothing, and only its parser answers
NOWHERE = "migkit_not_a_collection"


def _parser_said_no(ex):
    """Whether `ex` is the target's own refusal of what it was given - an
    operation the server answered - and not a right it lacks (13) or a
    connection lost, which say nothing of the pipeline and are raised."""
    from pymongo.errors import OperationFailure
    if not isinstance(ex, OperationFailure):
        return False
    return getattr(ex, "code", None) != 13 and "not authorized" not in \
        str(ex).lower()


def _mongo_asked(eng, db):
    """Each view's pipeline and each validator the source holds, run on the
    target against a collection that is not there: a stage or an operator
    the target does not have is refused by its parser, before any read."""
    d = getattr(eng, "dst_engine", None) or eng
    target = d._client("dst")[d._d("dst", db)][NOWHERE]
    refused, asked = [], 0
    for c in _mongo_collections(eng, db):
        opts = c.get("options") or {}
        if c.get("type") == "view" and opts.get("pipeline") is not None:
            asked += 1
            try:
                list(target.aggregate(list(opts["pipeline"])))
            except Exception as ex:  # noqa: BLE001 - the target's answer
                if not _parser_said_no(ex):
                    raise
                refused.append(f"view {c['name']} ({_last(ex)[:80]})")
        if opts.get("validator"):
            asked += 1
            try:
                list(target.find(dict(opts["validator"])).limit(1))
            except Exception as ex:  # noqa: BLE001 - the target's answer
                if not _parser_said_no(ex):
                    raise
                refused.append(f"the validator of {c['name']}"
                               f" ({_last(ex)[:80]})")
    if not asked:
        return []
    if not refused:
        return [_row("pass", "pipelines and validators",
                     f"{asked} asked of the target: it takes each")]
    return [_row("fail", "pipelines and validators",
                 f"{len(refused)} of {asked} the target does not take: "
                 + _said(refused),
                 "rewrite each with what the target has, on the source"
                 " before the move")]


def _mongo(hop, eng, db, src, dst):
    out, looked = _features(MONGO, eng, db, src, dst)
    try:
        out += _mongo_asked(eng, db)
    except Exception as e:  # noqa: BLE001 - unknown, said so
        out.append(_row("warn", "pipelines and validators",
                        f"could not be asked: {_last(e)} - unknown, not"
                        " clean"))
    return out, looked


# --- Redis and its forks -------------------------------------------------------------

#: the commands migkit's copy and check send to both sides
REDIS_COMMANDS = ("scan", "type", "pttl", "dump", "restore")

#: the key types a module registers, as TYPE answers for a key of it
MODULE_TYPES = {"rejson": ("ReJSON-RL",), "json": ("ReJSON-RL",),
                "timeseries": ("TSDB-TYPE",),
                "bf": ("MBbloom--", "MBbloomCF", "TopK-TYPE", "CMSk-TYPE",
                       "TDIS-TYPE"),
                "graph": ("graphdata",)}

#: keys a scan goes through to prove a module's types are not used; past
#: it, a module loaded is taken as used
SCAN_MOST = 2_000_000


def _modules(client):
    out = {}
    for m in client.execute_command("MODULE LIST") or []:
        if isinstance(m, dict):
            got = {str(k).lower(): v for k, v in m.items()}
        else:
            it = iter(m)
            got = {str(k).lower(): v for k, v in zip(it, it)}
        name = str(got.get("name") or "")
        if name:
            out[name.lower()] = str(got.get("ver") or "")
    return out


def _holds_types(client, types):
    """True where a key of one of `types` is found, False where the whole
    keyspace was scanned and none was, None where it is too large to."""
    if int(client.dbsize() or 0) > SCAN_MOST:
        return None
    for typ in types:
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor=cursor, count=10000,
                                       _type=typ)
            if keys:
                return True
            if int(cursor) == 0:
                break
    return False


def _redis_modules(eng, db):
    e = getattr(eng, "src_engine", None) or eng
    d = getattr(eng, "dst_engine", None) or eng
    src = e._client("src", db)
    mine, theirs = _modules(src), _modules(d._client("dst", db))
    out = []
    for name, ver in sorted(mine.items()):
        if name in theirs:
            continue
        types = MODULE_TYPES.get(name, ())
        used = None
        if name == "search":
            try:
                used = bool(src.execute_command("FT._LIST"))
            except Exception:  # noqa: BLE001 - not asked, taken as used
                used = None
        elif types:
            used = _holds_types(src, types)
        if used is False:
            out.append(_row("pass", f"module {name}",
                            "loaded on the source and not on the target,"
                            " and no key of the source holds its types"))
            continue
        how = ("keys of its types are refused by the target" if used else
               "loaded on the source, and not proved unused there")
        out.append(_row("fail", f"module {name}",
                        f"{name} {ver} is loaded on the source and not on"
                        f" the target: {how}"
                        + (f" (the target loads {', '.join(theirs)})"
                           if theirs else ""),
                        f"load {name} on the target, or take it off the"
                        " source if nothing there uses it"))
    return out


def _redis_commands(eng, db):
    d = getattr(eng, "dst_engine", None) or eng
    # one word with its subcommand: the client sends them apart, and reads
    # the answer back as it comes - nil for a command the server lacks
    got = d._client("dst", db).execute_command("COMMAND INFO",
                                               *REDIS_COMMANDS)
    missing = [c for c, info in zip(REDIS_COMMANDS, got or [])
               if not info]
    if not missing:
        return [_row("pass", "commands", "the target has every command"
                                         " migkit's copy and check send")]
    return [_row("fail", "commands",
                 "the target has no " + ", ".join(c.upper() for c in missing)
                 + ", which migkit's copy and check send",
                 "a target that has them")]


def _redis(hop, eng, db, src, dst):
    out = []
    for part, what in ((_redis_commands, "commands"),
                       (_redis_modules, "modules")):
        try:
            out += part(eng, db)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", what, f"could not be asked:"
                                          f" {_last(e)} - unknown, not"
                                          " clean"))
    return out, 0


# --- Elasticsearch and OpenSearch -----------------------------------------------------

#: field types one of the two has and the other has not, from each one's
#: field type reference: {type: {brand: the version it came, None: never}}
SEARCH = {
    "flattened": {"elasticsearch": (7, 3), "opensearch": None},
    "dense_vector": {"elasticsearch": (7, 0), "opensearch": None},
    "sparse_vector": {"elasticsearch": (8, 11), "opensearch": None},
    "semantic_text": {"elasticsearch": (8, 15), "opensearch": None},
    "aggregate_metric_double": {"elasticsearch": (7, 11),
                                "opensearch": None},
    "histogram": {"elasticsearch": (7, 6), "opensearch": None},
    "version": {"elasticsearch": (7, 10), "opensearch": None},
    "counted_keyword": {"elasticsearch": (8, 12), "opensearch": None},
    "shape": {"elasticsearch": (7, 4), "opensearch": None},
    "point": {"elasticsearch": (7, 16), "opensearch": None},
    "wildcard": {"elasticsearch": (7, 9), "opensearch": (2, 15)},
    "match_only_text": {"elasticsearch": (7, 14), "opensearch": (2, 12)},
    "unsigned_long": {"elasticsearch": (7, 10), "opensearch": (2, 8)},
    "knn_vector": {"elasticsearch": None, "opensearch": (1, 0)},
    "flat_object": {"elasticsearch": None, "opensearch": (2, 7)},
    "xy_point": {"elasticsearch": None, "opensearch": (2, 4)},
    "xy_shape": {"elasticsearch": None, "opensearch": (2, 4)},
}


def _field_types(mapping, prefix=""):
    """{field path: type} of an index mapping's properties, nested ones
    and multi-fields included."""
    out = {}
    for name, spec in ((mapping or {}).get("properties") or {}).items():
        path = f"{prefix}{name}"
        if isinstance(spec, dict):
            if spec.get("type"):
                out[path] = str(spec["type"])
            out.update(_field_types(spec, path + "."))
            for sub, sspec in (spec.get("fields") or {}).items():
                if isinstance(sspec, dict) and sspec.get("type"):
                    out[f"{path}.{sub}"] = str(sspec["type"])
    return out


def _search(hop, eng, db, src, dst):
    e = getattr(eng, "src_engine", None) or eng
    tables = e.neutral_tables("src", db)
    used = {}
    for t in tables:
        index = e._index("src", db, t) if hasattr(e, "_index") else t
        got = e._call("src", "GET", f"/{index}/_mapping", missing_ok=True)
        for body in (got or {}).values():
            for path, typ in _field_types(
                    (body or {}).get("mappings") or {}).items():
                if typ in SEARCH:
                    used.setdefault(typ, []).append(f"{t}.{path}")
    feats = tuple(Feature(typ, f"{typ} fields",
                          lambda eng, db, typ=typ: used.get(typ, []),
                          SEARCH[typ], "map each to a type the target has,"
                                       " on a copy of the index first")
                  for typ in sorted(used))
    return _features(feats, eng, db, src, dst)


#: engine -> the reading of what its target lacks
READERS = {"postgres": _pg, "mysql": _my, "mssql": _ms, "oracle": _ora,
           "mongodb": _mongo, "redis": _redis, "opensearch": _search}


def checks(hop, eng, db, src, dst, extensions=True):
    """Rows of what the source's database `db` uses and the target (`dst`,
    a `Side`) has not - fail where it is known and would stop the move or
    change what the target keeps, warn where it is not known or only
    works a little otherwise - and a pass where nothing is missing."""
    engine = getattr(getattr(eng, "src_engine", None) or eng,
                     "CANON_ENGINE", "")
    read = READERS.get(engine)
    if read is None:
        return []
    kw = {"extensions": extensions} if read is _pg else {}
    out, looked = read(hop, eng, db, src, dst, **kw)
    if looked and not any(r["level"] != "pass" for r in out):
        out.append(_row("pass", "what the source uses",
                        f"{looked} features the target could lack looked"
                        f" for on the source: none it lacks"))
    return out


def gate_only(hop, eng, dbs):
    """What the engine's own `assess` already says for a MySQL-protocol
    pair and a gate must hold to: the collations in scope the target will
    not take, which stop the move at the first table made with one
    (measured, MariaDB 11 into MySQL 8.4: `ERROR 1273`). Asked here only
    for the gate - `assess` lists them itself. What only a MariaDB source
    keeps is not held to here: the move carries a versioned table as a
    plain one and says so, and leaves a sequence behind, named."""
    e = getattr(eng, "src_engine", None) or eng
    if getattr(e, "CANON_ENGINE", "") != "mysql":
        return []
    try:
        return _my_collations(eng, dbs)
    except Exception as ex:  # noqa: BLE001 - unknown, said so
        return [_row("warn", "collations", f"could not be read: {_last(ex)}"
                                           " - unknown, not clean")]
