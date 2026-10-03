"""A change of storage engine, made in the schema step and held to
(backlog P0-SCENARIOS): MyISAM, Aria, MEMORY, ARCHIVE, CSV and MariaDB's
own engines into InnoDB - or any of them into a server that keeps every
table one way, PostgreSQL's, SQL Server's.

A MySQL table's engine is part of its definition, and the definition was
carried as the source wrote it. Where the target lacked the source's
engine - MariaDB's Aria into MySQL - the server put its default in its
place with a warning nobody read, and what changed with it went unsaid:
statements that used to leave half their rows now leave none, foreign keys
MyISAM parsed and threw away are now refused, full-text search ranks and
drops words by other lists, an AUTO_INCREMENT numbered per group now
numbers the whole table. Here:

* **What changes** (`plan`): each table's engine on both sides - the
  target's own where it has the table, the hop's `table_engine: {to}`
  where it asks for one, and where the target lacks the source's engine,
  the target's default - read from each server's catalogue.
* **Made in the schema step** (`schema`): the source's definition as the
  target's engine takes it - its ENGINE, the table options only the old
  engine reads left out, a key where the new engine needs its
  AUTO_INCREMENT column to lead one, and the foreign keys the hop names
  (`foreign_keys`) - chosen as a way (`WAYS`): in the definition of a
  table migkit makes, or converted in place on a target that has the
  table already.
* **What changes, said, and checked before the move** (`checks`):
  transactions, foreign key enforcement - with every row on the source a
  key would now refuse found and refused by name, before anything is
  written - full-text search (each side's own settings, read), per-group
  numbering (refused unless the hop accepts it), locking, and what an
  engine held that another does not: a MEMORY table's rows kept across a
  restart, a MERGE table's rows that are its tables' (refused: carried
  they would be twice), a remote engine's rows that live elsewhere.
* **Proved after it** (`prove`): each table's engine on the target the
  one planned, and no row there that a foreign key refuses.
"""
import re
from dataclasses import dataclass, field

OPTION = "table_engine"
SCOPE = "storage engine"
#: what the hop may accept of a change, by name
ACCEPT = {"group numbering": "an AUTO_INCREMENT numbered per group becomes"
                             " one numbered across the table",
          "remote rows": "a table whose rows live on another server"
                         " carried as a copy of them, frozen at the move"}
EXAMPLES = 3


@dataclass(frozen=True)
class Traits:
    """What an engine does that a move into another may change."""
    said: str
    transactions: bool
    foreign_keys: bool
    locks: str                  #: "table", "page" or "row"
    fulltext: str = ""          #: whose full-text search, "" for none
    group_increment: bool = False
    #: "rows", "memory" (gone at a restart), "nothing" (BLACKHOLE),
    #: "union" (MERGE), "remote", "virtual" or "read-only"
    holds: str = "rows"
    compressed: bool = False
    #: table options only this engine reads, as `show create table` writes
    #: them: left out of a definition made for another
    options: tuple = ()


MYISAM_OPTIONS = ("PACK_KEYS", "DELAY_KEY_WRITE", "CHECKSUM")
TRAITS = {
    "innodb": Traits("InnoDB", True, True, "row", "innodb"),
    "myisam": Traits("MyISAM", False, False, "table", "myisam", True,
                     options=MYISAM_OPTIONS),
    "aria": Traits("Aria", False, False, "page", "myisam", True,
                   options=MYISAM_OPTIONS + ("PAGE_CHECKSUM",
                                             "TRANSACTIONAL")),
    "memory": Traits("MEMORY", False, False, "table", holds="memory"),
    "archive": Traits("ARCHIVE", False, False, "row", compressed=True),
    "csv": Traits("CSV", False, False, "table"),
    "blackhole": Traits("BLACKHOLE", False, False, "table",
                        holds="nothing"),
    "mrg_myisam": Traits("MERGE", False, False, "table", holds="union",
                         options=("INSERT_METHOD", "UNION")),
    "federated": Traits("FEDERATED", False, False, "row", holds="remote",
                        options=("CONNECTION",)),
    "federatedx": Traits("FederatedX", False, False, "row", holds="remote",
                         options=("CONNECTION",)),
    "connect": Traits("CONNECT", False, False, "table", holds="remote",
                      options=("TABLE_TYPE", "FILE_NAME", "CONNECTION",
                               "OPTION_LIST", "SRCDEF", "TABNAME",
                               "DBNAME")),
    "spider": Traits("Spider", True, False, "row", holds="remote",
                     options=("CONNECTION",)),
    "rocksdb": Traits("MyRocks", True, False, "row"),
    "tokudb": Traits("TokuDB", True, False, "row"),
    "columnstore": Traits("ColumnStore", True, False, "table"),
    "s3": Traits("S3", False, False, "table", holds="read-only",
                 options=("S3_BLOCK_SIZE", "COMPRESSION_ALGORITHM")),
    "sequence": Traits("SEQUENCE", False, False, "table", holds="virtual"),
    "oqgraph": Traits("OQGRAPH", False, False, "table", holds="virtual",
                      options=("DATA_TABLE", "ORIGID", "DESTID",
                               "WEIGHT")),
    "mroonga": Traits("Mroonga", False, False, "row", "mroonga"),
}
TRAITS["merge"] = TRAITS["mrg_myisam"]
TRAITS["heap"] = TRAITS["memory"]
#: a server that keeps every table one way, by its engine family
ONE_WAY = {"postgres": Traits("PostgreSQL's tables", True, True, "row",
                              "postgres"),
           "mssql": Traits("SQL Server's tables", True, True, "row",
                           "mssql"),
           "oracle": Traits("Oracle's tables", True, True, "row", "oracle"),
           "db2": Traits("Db2's tables", True, True, "row", "db2")}
#: row formats one engine takes and another refuses
ROW_FORMATS = {"innodb": ("DEFAULT", "DYNAMIC", "COMPACT", "REDUNDANT",
                          "COMPRESSED")}


def traits(name):
    return TRAITS.get(str(name or "").lower()) or Traits(
        str(name or "unknown"), False, False, "table", holds="unknown")


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def _row(level, item, detail, fix=""):
    return {"level": level, "scope": SCOPE, "item": item,
            "detail": detail + (f"  [fix] {fix}" if fix and level != "pass"
                                else "")}


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


def _family(engine):
    from .engines import ALIASES
    return ALIASES.get(engine, engine)


# --- the hop's word --------------------------------------------------------------

def settings(hop):
    """{"to", "accept", "foreign_keys"} of the hop's `table_engine:`, or
    None where it says none. `table_engine: InnoDB` is the engine alone."""
    raw = (hop.options or {}).get("table_engine")
    if raw in (None, False, ""):
        return None
    if isinstance(raw, str):
        raw = {"to": raw}
    if not isinstance(raw, dict):
        raise SystemExit(f"table_engine: {raw!r} - an engine, or {{to,"
                         " accept, foreign_keys}}")
    to = str(raw.get("to") or "").strip()
    if to and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", to):
        raise SystemExit(f"table_engine: to: {to!r} is not an engine name")
    accept = [str(a).strip().lower() for a in raw.get("accept") or []]
    wrong = [a for a in accept if a not in ACCEPT]
    if wrong:
        raise SystemExit(f"table_engine: accept: {', '.join(wrong)} - one"
                         f" of {', '.join(ACCEPT)}")
    keys = []
    for child, parent in (raw.get("foreign_keys") or {}).items():
        keys.append(_fk(str(child), str(parent)))
    return {"to": to, "accept": accept, "foreign_keys": keys}


def _side_of(text):
    """(table, [columns]) of `table.column` or `table(a, b)`."""
    text = text.strip()
    m = re.fullmatch(r"([^()]+)\(([^()]+)\)", text)
    if m:
        return m.group(1).strip(), [c.strip() for c in m.group(2).split(",")
                                    if c.strip()]
    table, _, column = text.rpartition(".")
    if not table or not column:
        raise SystemExit(f"table_engine: foreign_keys: {text!r} - write"
                         " table.column, or table(a, b)")
    return table, [column]


def _fk(child, parent):
    ct, cc = _side_of(child)
    pt, pc = _side_of(parent)
    if len(cc) != len(pc):
        raise SystemExit(f"table_engine: foreign_keys: {child} names"
                         f" {len(cc)} columns and {parent} {len(pc)}")
    return (ct, cc, pt, pc)


# --- the plan ----------------------------------------------------------------------

@dataclass
class Change:
    """One table whose engine changes, and why."""
    table: str
    src: str
    dst: str
    why: str
    #: the target has the table already, in `there`'s engine
    there: str = ""

    @property
    def a(self):
        return traits(self.src)

    @property
    def b(self):
        return (ONE_WAY.get(self.dst) if self.dst in ONE_WAY
                else traits(self.dst))


@dataclass
class Plan:
    db: str
    changes: list = field(default_factory=list)
    #: foreign keys held on the target that the source's engine never
    #: enforced: (child, [cols], parent, [cols], where it is declared)
    keys: list = field(default_factory=list)
    mysql_target: bool = False
    #: the source's tables in scope
    tables: set = field(default_factory=set)

    def of(self, table):
        return next((c for c in self.changes if c.table == table), None)

    def words(self):
        pairs = {}
        for c in self.changes:
            pairs.setdefault(f"{c.a.said} to {c.b.said}", []).append(c.table)
        return "; ".join(f"{k} ({len(v)} table{'s' if len(v) != 1 else ''})"
                         for k, v in sorted(pairs.items()))


def applies(hop, eng):
    """Whether tables may change engine on this hop: a MySQL-family source
    - the family whose tables each have an engine - on a hop that
    writes."""
    from .hopkeys import GOALS
    goal = getattr(hop, "goal", "") or "copy"
    if goal in GOALS and not GOALS[goal]["writes"]:
        return False
    try:
        pair = _pair(eng)
    except Exception:  # noqa: BLE001 - a pair that cannot be made
        return False
    return _family(pair.src_engine.CANON_ENGINE) == "mysql" and getattr(
        pair.src_engine, "_q", None) is not None


def _engines(e, side, db):
    """{table: engine, lowercase} of one side's base tables."""
    return {str(t): str(n or "").lower() for t, n in e._q(
        side, "select table_name, engine from information_schema.tables"
              " where table_schema = %s and table_type = 'BASE TABLE'",
        (e._d(side, db),))}


def _supported(e, side):
    """({engine the server takes}, its default), lowercase."""
    rows = e._q(side, "select lower(engine), support from"
                      " information_schema.engines")
    have = {str(n) for n, s in rows if str(s).upper() in ("YES",
                                                          "DEFAULT")}
    default = next((str(n) for n, s in rows
                    if str(s).upper() == "DEFAULT"), "innodb")
    return have, default


def plan(hop, eng, db):
    """The `Plan` of `db`, read once per run."""
    known = eng.__dict__.setdefault("_engine_plans", {})
    if db in known:
        return known[db]
    how = settings(hop)
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    p = Plan(db, mysql_target=_family(dst.CANON_ENGINE) == "mysql")
    mine = {t: n for t, n in _engines(src, "src", db).items()
            if not hop.excluded(db, t)}
    p.tables = set(mine)
    if p.mysql_target:
        try:
            there = _engines(dst, "dst", db)
        except Exception:  # noqa: BLE001 - a database not made yet
            there = {}
        have, default = _supported(dst, "dst")
        to = (how or {}).get("to", "").lower()
        for t, n in sorted(mine.items()):
            if t in there:
                if there[t] != n:
                    p.changes.append(Change(t, n, there[t], "the target's"
                                            " table is one", there[t]))
                elif to and to != n:
                    p.changes.append(Change(t, n, to, "the hop asks for it",
                                            there[t]))
            elif to and to != n:
                p.changes.append(Change(t, n, to, "the hop asks for it"))
            elif n not in have:
                p.changes.append(Change(
                    t, n, default, f"the target has no {traits(n).said}"
                                   " engine, and makes its tables"
                                   f" {traits(default).said}"))
    else:
        family = _family(dst.CANON_ENGINE)
        for t, n in sorted(mine.items()):
            b = ONE_WAY.get(family)
            if b is not None and (traits(n).said != "InnoDB"):
                p.changes.append(Change(t, n, family,
                                        "the target keeps every table one"
                                        " way"))
    p.keys = _held_keys(hop, pair, db, p, mine, how)
    known[db] = p
    return p


def _held_keys(hop, pair, db, p, mine, how):
    """The foreign keys the target holds the rows to where the source's
    engine never did: the hop's (`foreign_keys`), and the target's own on
    a table it has."""
    out = []
    for ct, cc, pt, pc in (how or {}).get("foreign_keys") or []:
        out.append((ct, cc, pt, pc, "the hop's foreign_keys"))
    try:
        theirs = pair.dst_engine.foreign_keys("dst", db)
    except Exception:  # noqa: BLE001 - a target not made yet has none
        theirs = []
    for ct, cc, pt, pc in theirs:
        leaf = str(ct).split(".")[-1]
        if not traits(mine.get(leaf)).foreign_keys and leaf in mine:
            got = (leaf, list(cc), str(pt).split(".")[-1], list(pc),
                   "the target's own")
            if not any(k[:4] == got[:4] for k in out):
                out.append(got)
    return out


# --- what changes, said and checked -----------------------------------------------

def _q(name):
    return "`" + str(name).replace("`", "``") + "`"


def _group_numbered(src, db, table):
    """The AUTO_INCREMENT column numbered per group - leading no index,
    so MyISAM and Aria number it per value of the columns before it -
    or None."""
    col = src._q("src", "select column_name from information_schema.columns"
                        " where table_schema = %s and table_name = %s and"
                        " extra like %s", (db, table, "%auto_increment%"))
    if not col:
        return None
    name = str(col[0][0])
    first = src._q("src", "select 1 from information_schema.statistics where"
                          " table_schema = %s and table_name = %s and"
                          " column_name = %s and seq_in_index = 1",
                   (db, table, name))
    return None if first else name


def _fulltext(src, db):
    """{table: [index]} of the source's full-text indexes."""
    out = {}
    for t, i in src._q("src", "select distinct table_name, index_name from"
                              " information_schema.statistics where"
                              " table_schema = %s and index_type ="
                              " 'FULLTEXT'", (db,)):
        out.setdefault(str(t), []).append(str(i))
    return out


FT_SETTINGS = {"myisam": ("ft_min_word_len", "ft_max_word_len",
                          "ft_stopword_file"),
               "innodb": ("innodb_ft_min_token_size",
                          "innodb_ft_max_token_size",
                          "innodb_ft_enable_stopword",
                          "innodb_ft_server_stopword_table"),
               "mroonga": ("mroonga_default_tokenizer",)}


def _settings(e, side, names):
    """{setting: value} of one side's own, as MySQL and MariaDB both
    answer it."""
    if not names:
        return {}
    marks = ", ".join(["%s"] * len(names))
    return {str(k).lower(): str(v) for k, v in e._q(
        side, f"show global variables where variable_name in ({marks})",
        tuple(names))}


def _leads(src, db, table, cols):
    """Whether an index of `table` on the source leads with `cols` - what
    a foreign key's parent needs on InnoDB."""
    got = {}
    for index, col, seq in src._q("src", "select index_name, column_name,"
                                         " seq_in_index from"
                                         " information_schema.statistics"
                                         " where table_schema = %s and"
                                         " table_name = %s", (db, table)):
        got.setdefault(str(index), {})[int(seq)] = str(col)
    return any([cs.get(i + 1) for i in range(len(cols))] == list(cols)
               for cs in got.values())


def _orphans(hop, src, db, key):
    """(rows the key would refuse, [their key values]) on the source,
    through each table's row filter."""
    ct, cc, pt, pc, _ = key
    cf = src.row_filter(db, ct)
    pf = src.row_filter(db, pt)
    child = f"(select * from {_q(db)}.{_q(ct)}" + (f" where {cf}" if cf
                                                   else "") + ")"
    parent = f"(select * from {_q(db)}.{_q(pt)}" + (f" where {pf}" if pf
                                                    else "") + ")"
    on = " and ".join(f"p.{_q(b)} = c.{_q(a)}" for a, b in zip(cc, pc))
    filled = " and ".join(f"c.{_q(a)} is not null" for a in cc)
    cols = ", ".join(f"c.{_q(a)}" for a in cc)
    where = f"{filled} and not exists (select 1 from {parent} p where {on})"
    n = int(src._q("src", f"select count(*) from {child} c where"
                          f" {where}")[0][0])
    shown = [tuple(r) for r in src._q(
        "src", f"select distinct {cols} from {child} c where {where}"
               f" limit {EXAMPLES}")] if n else []
    return n, shown


def checks(hop, eng, dbs, quick=False):
    """assess rows of the change of engine: what changes, what each
    change does, and each row a foreign key would now refuse - refused by
    name before anything is written."""
    if not applies(hop, eng):
        return []
    try:
        how = settings(hop)
    except SystemExit as e:
        return [_row("fail", "table_engine", str(e))]
    out = []
    pair = _pair(eng)
    src = pair.src_engine
    accept = set((how or {}).get("accept") or [])
    for db in dbs:
        try:
            p = plan(hop, eng, db)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", f"{db}: the tables' engines",
                            f"could not be read: {_last(e)} - unknown, not"
                            " clean"))
            continue
        if p.changes:
            out.append(_row("pass", f"{db}: a storage engine change",
                            p.words() + " - "
                            + ("made in the schema step" if p.mysql_target
                               else "the tables built as the target keeps"
                                    " them")))
        try:
            out += _behaviours(hop, eng, db, p, accept, quick)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", f"{db}: what the change does",
                            f"could not be read: {_last(e)} - unknown, not"
                            " clean"))
        for key in p.keys:
            ct, cc, pt, pc, where = key
            gone = [t for t in (ct, pt) if t not in p.tables]
            if gone:
                out.append(_row("fail", f"{db}: {ct} -> {pt}",
                                f"the key ({where}) names"
                                f" {', '.join(gone)}, which the hop does"
                                " not carry from the source",
                                "name tables the hop carries, or leave the"
                                " key out"))
                continue
            if where == "the hop's foreign_keys" and p.mysql_target:
                try:
                    led = _leads(src, db, pt, pc)
                except Exception:  # noqa: BLE001 - the make says it
                    led = True
                if not led:
                    out.append(_row(
                        "fail", f"{db}: {ct} -> {pt}",
                        f"no index of {pt} leads with ({', '.join(pc)}),"
                        " and the target's engine makes a foreign key only"
                        " to one that does: the table would not be made",
                        f"key {pt} by ({', '.join(pc)}) on the source, or"
                        " name its key"))
                    continue
            try:
                n, shown = _orphans(hop, src, db, key)
            except Exception as e:  # noqa: BLE001 - unknown, said so
                out.append(_row("warn", f"{db}: {ct} -> {pt}",
                                f"the rows it would refuse could not be"
                                f" counted: {_last(e)} - unknown, not"
                                " clean"))
                continue
            said = f"{ct} ({', '.join(cc)}) -> {pt} ({', '.join(pc)})"
            if n:
                out.append(_row(
                    "fail", f"{db}: {said}",
                    f"{n:,} rows on the source name no row of {pt}"
                    f" (e.g. {', '.join(map(str, shown))}): the source's"
                    f" engine never held them to this key ({where}), the"
                    " target's does - and a load with the keys' checks off"
                    " would leave them there unrefused",
                    f"remove or fix those rows of {ct} on the source, or"
                    " leave the key out"))
            else:
                out.append(_row("pass", f"{db}: {said}",
                                f"every row of {ct} names a row of {pt}:"
                                f" the key ({where}) holds from the first"
                                " row"))
    return out


def _behaviours(hop, eng, db, p, accept, quick):
    """The rows of what each change does, one for each kind of change."""
    out = []
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    by = {}
    for c in p.changes:
        by.setdefault((c.a.said, c.b.said), []).append(c)
    fulltext = _fulltext(src, db) if p.changes else {}
    for (a_said, b_said), cs in sorted(by.items()):
        a, b = cs[0].a, cs[0].b
        names = ", ".join(c.table for c in cs[:6]) + (
            " ..." if len(cs) > 6 else "")
        if a.transactions != b.transactions:
            out.append(_row(
                "warn", f"{db}: transactions ({names})",
                f"{a_said} " + ("keeps no transactions: a statement that"
                                " failed half way left its first rows, and"
                                f" {b_said} leaves none - the application's"
                                " error handling now meets whole"
                                " statements" if b.transactions else
                                "rolled a failed statement back whole, and"
                                f" {b_said} keeps the rows written before"
                                " the failure"),
                "run the application's writes that can fail against the"
                " target before the cutover"))
        if a.foreign_keys and not b.foreign_keys:
            out.append(_row("warn", f"{db}: foreign keys ({names})",
                            f"{b_said} enforces no foreign key: a row that"
                            " names nothing is taken from now on",
                            "keep the checks in the application, or keep"
                            " the engine"))
        if a.locks != b.locks:
            out.append(_row("pass", f"{db}: locking ({names})",
                            f"{a_said} locks by {a.locks}, {b_said} by"
                            f" {b.locks}: LOCK TABLES and long reads block"
                            " writers " + ("less" if b.locks == "row"
                                           else "more") + " than they did"))
        ft = [f"{c.table}.{i}" for c in cs for i in fulltext.get(c.table,
                                                                  [])]
        if ft and a.fulltext != b.fulltext:
            mine = _settings(src, "src", FT_SETTINGS.get(a.fulltext, ()))
            theirs = (_settings(dst, "dst", FT_SETTINGS.get(b.fulltext, ()))
                      if p.mysql_target else {})
            said = "; ".join(f"{k} {v}" for k, v in sorted(mine.items()))
            said2 = "; ".join(f"{k} {v}" for k, v in sorted(theirs.items()))
            out.append(_row(
                "warn", f"{db}: full-text search",
                f"{len(ft)} full-text indexes ({', '.join(ft[:4])}) are"
                f" {b_said}'s after the change: its own words dropped, its"
                " own shortest word, no 50% rule in natural language mode"
                " and its own boolean syntax - the same MATCH answers"
                " otherwise"
                + (f" (source: {said})" if said else "")
                + (f" (target: {said2})" if said2 else ""),
                "run the application's searches against the target before"
                " the cutover, and set the target's stopword table and"
                " token size where they must answer alike"))
        if a.group_increment and not b.group_increment:
            numbered = [(c.table, _group_numbered(src, db, c.table))
                        for c in cs]
            numbered = [(t, n) for t, n in numbered if n]
            if numbered:
                shown = ", ".join(f"{t}.{n}" for t, n in numbered[:4])
                if "group numbering" in accept:
                    out.append(_row(
                        "warn", f"{db}: AUTO_INCREMENT per group",
                        f"{shown}: numbered per group of the key's first"
                        f" columns on {a_said}, across the table on"
                        f" {b_said} - the rows carried keep their numbers,"
                        " new ones are numbered otherwise (accepted; a key"
                        " on the column is added so the target takes it)"))
                else:
                    out.append(_row(
                        "fail", f"{db}: AUTO_INCREMENT per group",
                        f"{shown}: numbered per group of the key's first"
                        f" columns on {a_said}, which {b_said} cannot do -"
                        " new rows would be numbered across the table",
                        "number them in the application, or accept it:"
                        " table_engine: {accept: [group numbering]}"))
        for c in cs:
            out += _holds(hop, src, db, c, accept)
    return out


def _holds(hop, src, db, c, accept):
    a, b = c.a, c.b
    if a.holds == "memory":
        return [_row("warn", f"{db}: {c.table}",
                     f"a {a.said} table loses its rows at a restart; on"
                     f" {b.said} it keeps them",
                     "if the application fills it at start, it now finds"
                     " last run's rows there: empty it at start, or exclude"
                     " the table")]
    if a.holds == "nothing":
        return [_row("warn", f"{db}: {c.table}",
                     f"a {a.said} table keeps no row it is given; on"
                     f" {b.said} every row written from now on is kept",
                     "exclude the table if it is only a replication"
                     " relay")]
    if a.holds == "union":
        ddl = src._q("src", f"show create table {_q(db)}.{_q(c.table)}")
        m = re.search(r"UNION=\(([^)]*)\)", ddl[0][1] if ddl else "")
        parts = [x.strip().strip("`") for x in (m.group(1) if m else ""
                                                ).split(",") if x.strip()]
        view = (f"create view {_q(c.table)} as "
                + " union all ".join(f"select * from {_q(t)}"
                                     for t in parts)) if parts else ""
        return [_row("fail", f"{db}: {c.table}",
                     f"a {a.said} table's rows are the rows of"
                     f" {', '.join(parts) or 'its tables'}: carried as a"
                     " table of its own they would be there twice",
                     f"exclude {c.table}"
                     + (f" and make it a view on the target: {view}"
                        if view else ""))]
    if a.holds == "remote":
        if "remote rows" in accept:
            return [_row("warn", f"{db}: {c.table}",
                         f"a {a.said} table's rows live on another server;"
                         " carried as a copy frozen at the move (accepted)")]
        return [_row("fail", f"{db}: {c.table}",
                     f"a {a.said} table's rows live on another server:"
                     " carried here they are a copy that stops following it",
                     "move that server's table itself, exclude this one, or"
                     " accept it: table_engine: {accept: [remote rows]}")]
    if a.holds == "virtual":
        return [_row("fail", f"{db}: {c.table}",
                     f"a {a.said} table computes its rows as it is read:"
                     " there is nothing of its own to carry",
                     f"exclude {c.table}")]
    if a.holds == "read-only":
        return [_row("warn", f"{db}: {c.table}",
                     f"a {a.said} table takes no write; on {b.said} it"
                     " takes them")]
    if a.compressed:
        try:
            size = int(src._q("src", "select data_length from"
                                     " information_schema.tables where"
                                     " table_schema = %s and table_name ="
                                     " %s", (db, c.table))[0][0] or 0)
        except Exception:  # noqa: BLE001 - said without the size
            size = 0
        from .wording import human_bytes
        return [_row("warn", f"{db}: {c.table}",
                     f"an {a.said} table is compressed"
                     + (f" ({human_bytes(size)} on the source)" if size
                        else "") + f": on {b.said} it takes several times"
                     " the room",
                     "room on the target for the table uncompressed")]
    if a.holds == "unknown":
        return [_row("warn", f"{db}: {c.table}",
                     f"{a.said} is an engine migkit does not know: what it"
                     " does that the target's does not is unknown, not"
                     " clean")]
    return []


# --- the schema step ------------------------------------------------------------

def _way_facts(hop, db, change):
    from .decide import Facts
    return Facts({"there": bool(change.there), "hop": hop, "db": db})


def ways():
    """The ways a table's engine is changed: in the definition of a table
    migkit makes (nothing to rebuild - the rows land in it once), or in
    place, on a target that has the table in the old engine already (the
    table rebuilt by the server: its rows are written a second time)."""
    from .decide import Need, Rung
    return (
        Rung("definition", "the definition migkit makes the table from",
             frozenset({"engine changed"}),
             needs=(Need("a table migkit makes",
                         lambda f: not f["there"],
                         "the target has the table already"),),
             because="the rows land in the new engine once"),
        Rung("in place", "the table converted in place on the target",
             frozenset({"engine changed"}),
             needs=(Need("the table on the target", lambda f: f["there"],
                         "the target has no table to convert"),),
             footprint=1,
             because="the target has the table in the old engine"))


def way(hop, db, change):
    """The `decide.Choice` of how `change` is made."""
    from . import decide
    return decide.climb(f"{db}.{change.table}: its engine", ways(),
                        _way_facts(hop, db, change), ("engine changed",))


#: table options `show create table` writes and no InnoDB table reads
_OPTION = r"\s+{name}\s*=\s*(\([^)]*\)|'[^']*'|\S+)"


def rewrite(ddl, change, key_for=None, foreign=()):
    """The source's `show create table` as the target's engine takes it:
    its ENGINE, the options only the old engine reads left out, a row
    format the new one refuses left out, a key leading with `key_for`
    (the per-group AUTO_INCREMENT column), and `foreign`, the hop's
    foreign keys of this table ([(columns, parent, parent's columns)])."""
    b = str(change.dst)
    out = re.sub(r"\bENGINE\s*=\s*\w+", f"ENGINE={b}", ddl, count=1,
                 flags=re.I)
    for name in change.a.options:
        out = re.sub(_OPTION.format(name=re.escape(name)), "", out,
                     flags=re.I)
    fmts = ROW_FORMATS.get(b.lower())
    m = re.search(r"\s+ROW_FORMAT\s*=\s*(\w+)", out, re.I)
    if fmts and m and m.group(1).upper() not in fmts:
        out = out[:m.start()] + out[m.end():]
    if b.lower() == "innodb":
        # MEMORY's hash indexes: InnoDB builds a B-tree whatever is said
        out = re.sub(r"\s+USING\s+HASH\b", "", out, flags=re.I)
    extra = []
    if key_for:
        extra.append(f"KEY {_q(('migkit_' + key_for)[:64])} ({_q(key_for)})")
    for cols, parent, pcols in foreign:
        extra.append(f"FOREIGN KEY ({', '.join(_q(c) for c in cols)})"
                     f" REFERENCES {_q(parent)}"
                     f" ({', '.join(_q(c) for c in pcols)})")
    if extra:
        close = _body_end(out)
        head, tail = out[:close].rstrip(), out[close:]
        out = head + ",\n  " + ",\n  ".join(extra) + "\n" + tail
    return out


def _body_end(ddl):
    """Where the parenthesis closing a `create table`'s column list is:
    the one matching the first opened, past quoted names, strings and
    comments - a partition clause or a comment after it has its own."""
    depth, quote, i = 0, None, 0
    while i < len(ddl):
        ch = ddl[i]
        if quote:
            if ch == "\\" and quote == "'":
                i += 2
                continue
            if ch == quote:
                if i + 1 < len(ddl) and ddl[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError("a create table statement with no column list")


def schema(hop, eng, db, table, ddl):
    """`ddl` (the source's definition of `table`) as migkit makes it on a
    MySQL target: rewritten for the new engine where the plan changes the
    table's (`rewrite`), as it is everywhere else - the schema step's one
    call (`movers._my_create_missing`)."""
    if not applies(hop, eng):
        return ddl
    how = settings(hop) or {}
    try:
        p = plan(hop, eng, db)
    except Exception:
        if how:
            raise
        # a hop that asks for no engine makes its tables as it always did
        return ddl
    change = p.of(table)
    foreign = [(cc, pt, pc) for ct, cc, pt, pc, where in p.keys
               if ct == table and where == "the hop's foreign_keys"]
    if change is None:
        if not foreign:
            return ddl
        return rewrite(ddl, Change(table, "innodb", "innodb", ""),
                       foreign=foreign)
    if way(hop, db, change).path != "definition":
        return ddl
    key_for = None
    if change.a.group_increment and "group numbering" in (
            how.get("accept") or []):
        key_for = _group_numbered(_pair(eng).src_engine, db, table)
    return rewrite(ddl, change, key_for, foreign)


def in_place(hop, eng, db, log):
    """A table the target has in the old engine, converted there once its
    rows are in (`ALTER TABLE ... ENGINE`) - the other way of `ways`;
    [lines said]. A hop that asks for no engine converts nothing."""
    if settings(hop) is None or not applies(hop, eng):
        return []
    p = plan(hop, eng, db)
    if not p.mysql_target:
        return []
    dst = _pair(eng).dst_engine
    done = []
    for c in p.changes:
        if not c.there or c.there == c.dst:
            continue
        if way(hop, db, c).path != "in place":
            continue
        dst.execute_ddl("dst", db, f"alter table {_q(dst._d('dst', db))}"
                                   f".{_q(c.table)} engine={c.dst}")
        done.append(c.table)
        log(f"{c.table}: converted in place to {c.b.said}")
    if done:
        (eng.__dict__.get("_engine_plans") or {}).pop(db, None)
    return [f"{len(done)} tables converted in place"] if done else []


# --- the proof --------------------------------------------------------------------

def prove(hop, eng, db):
    """[Result]: each changed table's engine on the target the planned one,
    and every foreign key the change holds the rows to with no row on the
    target it refuses."""
    from .engines.base import Result
    if not applies(hop, eng):
        return []
    (eng.__dict__.get("_engine_plans") or {}).pop(db, None)
    p = plan(hop, eng, db)
    out = []
    pair = _pair(eng)
    dst = pair.dst_engine
    if p.mysql_target and p.changes:
        there = _engines(dst, "dst", db)
        wrong = [f"{c.table} is {there.get(c.table) or 'missing'}, planned"
                 f" {c.dst}" for c in p.changes
                 if there.get(c.table, "") != str(c.dst).lower()]
        out.append(Result("engine", f"{db} engines", "diff" if wrong
                          else "ok", "; ".join(wrong[:6]) if wrong else
                          f"{len(p.changes)} tables in their new engine"))
    for ct, cc, pt, pc, where in p.keys:
        if not p.mysql_target:
            continue
        ddb = dst._d("dst", db)
        on = " and ".join(f"p.{_q(b)} = c.{_q(a)}" for a, b in zip(cc, pc))
        filled = " and ".join(f"c.{_q(a)} is not null" for a in cc)
        try:
            n = int(dst._q("dst", f"select count(*) from {_q(ddb)}.{_q(ct)}"
                                  f" c where {filled} and not exists (select"
                                  f" 1 from {_q(ddb)}.{_q(pt)} p where"
                                  f" {on})")[0][0])
        except Exception as e:  # noqa: BLE001 - said as the error
            out.append(Result("engine", f"{db} {ct} -> {pt}", "error",
                              f"could not be counted: {_last(e)}"))
            continue
        out.append(Result("engine", f"{db} {ct} -> {pt}",
                          "diff" if n else "ok",
                          f"{n:,} rows of {ct} name no row of {pt} on the"
                          " target" if n else
                          f"every row of {ct} names a row of {pt} ({where})"))
    return out


#: what names an engine, or an option only one engine reads, in a MySQL
#: schema dump
_ENGINE_WORDS = re.compile(
    r"\s+ENGINE\s*=\s*\w+|\s+ROW_FORMAT\s*=\s*\w+|\s+USING\s+(?:HASH|BTREE)"
    r"|\s+(?:" + "|".join(sorted({re.escape(o) for t in TRAITS.values()
                                  for o in t.options}, key=len,
                                 reverse=True))
    + r")\s*=\s*(?:\([^)]*\)|'[^']*'|\S+)", re.I)


def _schema_as_planned(hop, db, p):
    """Whether the two schema dumps the check wrote differ by nothing but
    what the plan changes: the engines, the options only the old engine
    reads, and on a table the plan made the key it added for per-group
    numbering and the foreign keys the hop names."""
    import difflib
    d = hop.report_dir(db)
    try:
        a = (d / "schema-src.sql").read_text().splitlines()
        b = (d / "schema-dst.sql").read_text().splitlines()
    except OSError:
        return False
    added = bool(p.keys)
    from . import charsets
    # a character set the hop converts to as well: its words are the
    # conversion's, whose own proof holds every column to them
    also = charsets._MY_WORDS if charsets.settings(hop) else None

    def norm(lines, target):
        out = []
        for line in lines:
            if target and "KEY `migkit_" in line:
                continue
            if target and added and "FOREIGN KEY" in line.upper():
                continue
            line = _ENGINE_WORDS.sub("", line)
            if also is not None:
                line = also.sub("", line)
            out.append(line.rstrip().rstrip(","))
        return out
    return not any(line[:1] in "+-" and not line.startswith(("+++", "---"))
                   for line in difflib.unified_diff(norm(a, False),
                                                    norm(b, True),
                                                    lineterm=""))


def check(hop, eng, db, results):
    """`check`'s results with the change of engine proved (`prove`), and a
    schema check that differs only by what the plan changes said as the
    plan it is - a table in its new engine is not a schema drift."""
    if not applies(hop, eng):
        return results
    proved = prove(hop, eng, db)
    p = plan(hop, eng, db)
    fine = (p.mysql_target and p.changes and proved
            and all(r.status == "ok" for r in proved))
    kept = []
    from .charsets import _dumps_differ
    for r in results:
        if fine and _dumps_differ(r, db) and _schema_as_planned(hop, db, p):
            kept.append(dict(r, status="ok", detail=(
                "differs from the source's only by the storage engines the"
                " plan changes, and what it adds for them - proved below")))
            continue
        kept.append(r)
    return kept + [r.__dict__ for r in proved]


def plan_lines(hop, eng, db):
    """The plan's line of the change of engine, or []."""
    if not db or not applies(hop, eng):
        return []
    try:
        p = plan(hop, eng, db)
    except Exception as e:  # noqa: BLE001 - said, the plan goes on
        return [f"  a storage engine change could not be read: {_last(e)}"]
    if not p.changes:
        return []
    made = sum(1 for c in p.changes if not c.there)
    return [f"  a storage engine change: {p.words()} -"
            + (f" {made} made in the new engine in the schema step" if made
               else "")
            + (f"; {len(p.changes) - made} converted in place after the"
               " copy" if len(p.changes) - made and p.mysql_target else "")]
