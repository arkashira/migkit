"""migkit's own reader of Oracle's redo, through LogMiner (backlog F5).

No licence beyond the database's, no access to the database host, and it
runs on a managed database that allows LogMiner as it runs here. Each read
mines a window of SCNs with the online catalogue as the dictionary and
committed data only: the database hands back each transaction whole, in
commit order, once it has committed, and nothing of one that rolled back.

The position (`token`) is SCN-positioned and says four things:
* `scn` - where the next window starts mining. A transaction that
  commits after the last one handed back may have begun long before it,
  and a window that starts after its first change hands back only its
  tail; so this is the oldest start of any transaction that was open,
  read from `v$transaction`, never simply the last commit.
* `commit`, `xids` - the last commit SCN handed back whole, and the
  transactions at that SCN already handed back: a window mined again from
  `scn` returns transactions already applied, and these say which to skip.
* `part` - a transaction larger than one read, handed back in part: its
  id and how many of its changes went.
* `through` - every commit up to this SCN has been handed back. The fence
  waits on it.

A commit written without waiting for the redo (`commit write nowait`, or
`commit_write` set so) can have an SCN below the window's end and not be
in the redo files yet; the log writer writes the buffer at least every
three seconds. So `through` and `scn` are only moved to what was true
`FLUSH_SECONDS` and `RESTART_SECONDS` before the window's end was read -
a fence waits those seconds longer; a window mines a little more again.
"""
import datetime
import re
import time
from decimal import Decimal

#: LogMiner's operation codes this reader acts on
INSERT, DELETE, UPDATE, DDL = 1, 2, 3, 5
LOB_OPS = (9, 10, 11, 28, 29)
UNSUPPORTED = 255

#: seconds after which a commit is in the redo files whether or not its
#: session waited for it (the log writer's longest sleep is three)
FLUSH_SECONDS = 4.0
#: seconds after which the oldest open transaction a sample saw may be
#: taken as where to mine from next
RESTART_SECONDS = 10.0

_SAMPLES = {}

#: a table not readable as of an SCN: its definition changed since - or in
#: the seconds before, which the server tells apart only to its clock's
#: granularity (measured: ORA-01466 on a table made a second before) - or
#: the undo no longer reaches it
AS_OF_GONE = ("ORA-01466", "ORA-01555", "ORA-08180", "ORA-08181")


def current_scn(eng, side, db):
    got = eng._rows(side, db, "select dbms_flashback."
                              "get_system_change_number from dual")
    return int(got[0][0])


def _sample(eng, side, db, conn=None):
    """(monotonic time, SCN now, oldest SCN an open transaction began at),
    read in the order that makes the third safe: a transaction open when
    the window's end is read was either open when `v$transaction` was read
    or began after the first SCN was."""
    rows = (lambda sql: [tuple(r) for r in conn.cursor().execute(
        sql).fetchall()]) if conn is not None else \
        (lambda sql: eng._rows(side, db, sql))
    first = int(rows("select dbms_flashback.get_system_change_number"
                     " from dual")[0][0])
    try:
        oldest = rows("select min(start_scn) from v$transaction")[0][0]
    except Exception as e:  # noqa: BLE001 - said, not guessed
        raise SystemExit(
            "the change reader cannot see the open transactions"
            " (select on v$transaction), and without them it cannot tell"
            " where a transaction still open began - so it cannot mine"
            " without losing that transaction's first changes:"
            f" {str(e).splitlines()[0][:80]}. Grant the account"
            " SELECT_CATALOG_ROLE, or select on v_$transaction")
    now = int(rows("select dbms_flashback.get_system_change_number"
                   " from dual")[0][0])
    low = min(first, int(oldest)) if oldest is not None else first
    return (time.monotonic(), now, low)


def change_point(eng, side, db):
    """A position for now: mining starts at the oldest open transaction's
    first change, and nothing committed before now is handed back. Taken
    before a copy, which a tail then starts from.

    Refused, naming what is missing, where the redo cannot be read back:
    the database not archiving it, supplemental logging off, a table the
    hop moves with no key or without its key in the redo."""
    readiness(eng, side, db, strict=True)
    _, now, low = _sample(eng, side, db)
    return {"scn": low, "commit": now, "xids": [], "through": now}


def reached(have, want):
    if have is None or want is None:
        return None
    try:
        return int(have["through"]) >= int(want)
    except (KeyError, TypeError, ValueError):
        return None


def identity(eng, side, db):
    """The database and its incarnation: an SCN means something in one
    incarnation only - a flashback of the database or an open with
    resetlogs starts counting over again."""
    got = eng._rows(side, db, "select dbid, resetlogs_change#, name from"
                              " v$database")
    con = eng._rows(side, db, "select sys_context('userenv', 'con_name')"
                              " from dual")
    if not got:
        return None
    return {"database": str(got[0][0]), "incarnation": str(got[0][1]),
            "name": str(got[0][2]), "container": str(con[0][0])}


def oldest_kept(eng, side, db):
    """The oldest SCN the redo still holds: the archived logs not deleted,
    and the online ones."""
    got = eng._rows(side, db, "select min(scn) from (select first_change#"
                              " scn from v$archived_log where deleted ="
                              " 'NO' and status = 'A' and name is not null"
                              " union all select first_change# from"
                              " v$log)")
    return int(got[0][0]) if got and got[0][0] is not None else None


def lost(eng, side, db, token):
    if token is None:
        return None
    try:
        start = int(token["scn"])
    except (KeyError, TypeError, ValueError):
        return None
    low = oldest_kept(eng, side, db)
    if low is None or start >= low:
        return None
    return (f"the redo no longer reaches SCN {start}, where this tail has"
            f" to mine from: the oldest log kept begins at SCN {low}. The"
            " changes in between are gone from the redo; move again with"
            " --mode full+cdc")


# ---- what the database must give a reader ---------------------------------

def _mode(eng, side, db):
    got = eng._rows(side, db, "select log_mode, supplemental_log_data_min,"
                              " supplemental_log_data_pk,"
                              " supplemental_log_data_all, force_logging"
                              " from v$database")
    return dict(zip(("log_mode", "min", "pk", "all", "force"), got[0]))


def logged_keys(eng, side, db):
    """{table: "all" | "key" | None}: what the redo carries of each table
    beyond the columns a change touched."""
    mode = _mode(eng, side, db)
    owner = eng._owner(side, db)
    groups = {}
    for t, kind in eng._rows(side, db, "select table_name, log_group_type"
                                       " from all_log_groups where owner ="
                                       " :1", (owner,)):
        groups.setdefault(t, set()).add(kind)
    out = {}
    for t in eng.neutral_tables(side, db):
        name = eng._q(t)[1:-1]
        kinds = groups.get(name, set())
        if mode["all"] == "YES" or "ALL COLUMN LOGGING" in kinds:
            out[t] = "all"
        elif mode["pk"] == "YES" or "PRIMARY KEY LOGGING" in kinds:
            out[t] = "key"
        else:
            out[t] = None
    return out


def granted(eng, side, db):
    """The system privileges and roles the account holds: its session's,
    and those granted to it since the session began - a role granted to
    a session already open is not in its `session_roles` until the next
    one, and a pooled session is older than a grant just made."""
    out = set()
    for sql in ("select privilege from session_privs",
                "select privilege from user_sys_privs",
                "select role from session_roles",
                "select granted_role from user_role_privs"):
        out |= {p for (p,) in eng._rows(side, db, sql)}
    return out


def readiness(eng, side, db, strict=False):
    """[(level, item, detail)] for whether the redo can be read back; with
    `strict`, the first failure stops the caller, in words."""
    rows = []
    mode = _mode(eng, side, db)
    if mode["log_mode"] != "ARCHIVELOG":
        rows.append(("fail", "redo archived",
                     "the database does not archive its redo, so a log is"
                     " overwritten once it is full and a tail behind it"
                     " loses what it held: alter database archivelog"
                     " (a restart of the database, the DBA's)"))
    else:
        rows.append(("pass", "redo archived", "ARCHIVELOG"))
    if mode["min"] not in ("YES", "IMPLICIT"):
        rows.append(("fail", "supplemental logging",
                     "minimal supplemental logging is off, and without it"
                     " the redo cannot be read back as rows: alter"
                     " database add supplemental log data"))
    else:
        rows.append(("pass", "supplemental logging", f"minimal {mode['min']}"
                     + (", all columns" if mode["all"] == "YES" else
                        ", primary keys" if mode["pk"] == "YES" else "")))
    have = granted(eng, side, db)
    short = [p for p in ("LOGMINING", "SELECT ANY TRANSACTION")
             if p not in have]
    if short and _elsewhere(eng._endpoint(side)):
        rows.append(("pass", "the reader's grants",
                     f"the account lacks {', '.join(short)}, and the redo"
                     " is mined elsewhere than the source, which needs"
                     " none of them"))
    elif short:
        rows.append(("fail", "the reader's grants",
                     f"the account lacks {', '.join(short)}, which mining"
                     f" the redo needs: grant {', '.join(short).lower()} to"
                     " it (and execute on dbms_logmnr)"))
    elif "FLASHBACK ANY TABLE" not in have:
        rows.append(("warn", "the reader's grants",
                     "the account lacks FLASHBACK ANY TABLE: a large value"
                     " or a time with a zone is read from its table as of"
                     " the commit, which needs flashback on each table"))
    else:
        rows.append(("pass", "the reader's grants",
                     "LOGMINING, SELECT ANY TRANSACTION, FLASHBACK ANY"
                     " TABLE"))
    keys = logged_keys(eng, side, db)
    no_key = [t for t in keys if not eng.neutral_key(side, db, t)]
    unlogged = [t for t, k in keys.items() if k is None and t not in no_key]
    if no_key:
        rows.append(("fail", "tables with no key",
                     f"{', '.join(no_key[:6])}"
                     + (f" and {len(no_key) - 6} more" if len(no_key) > 6
                        else "") + " have no primary key, so a change to"
                     " one cannot be placed on the target: give each a key,"
                     " or leave it out of the hop"))
    if unlogged:
        rows.append(("fail", "keys in the redo",
                     f"{', '.join(unlogged[:6])}"
                     + (f" and {len(unlogged) - 6} more" if len(unlogged) > 6
                        else "") + " do not log their key with each change,"
                     " so an update names its row by address only: alter"
                     " table ... add supplemental log data (primary key)"
                     " columns, for each"))
    elif keys and not no_key:
        full = sum(1 for k in keys.values() if k == "all")
        rows.append(("pass", "keys in the redo",
                     f"{len(keys)} tables log their key"
                     + (f", {full} every column (a whole row before each"
                        " change)" if full else "")))
    if mode["force"] != "YES":
        loose = [eng._said(t) for (t,) in eng._rows(
            side, db, "select table_name from all_tables where owner = :1"
                      " and logging = 'NO' and temporary = 'N'",
            (eng._owner(side, db),))]
        if loose:
            rows.append(("fail", "unlogged tables",
                         f"{', '.join(loose[:6])} are NOLOGGING and the"
                         " database does not force logging: a direct path"
                         " load into one writes no redo, and its rows would"
                         " never reach the target. alter database force"
                         " logging, or alter table ... logging"))
    if strict:
        failed = [r for r in rows if r[0] == "fail"]
        if failed:
            raise SystemExit(f"{db}: the redo cannot be read back as the"
                             f" changes it holds - {failed[0][1]}:"
                             f" {failed[0][2]}")
    return rows


# ---- reading SQL_REDO -------------------------------------------------------
#
# LogMiner writes each change as the statement that would make it again.
# The values in it are literals in the session's own spelling - which the
# reader's session fixes (`oracle._session`) - so they read back exactly.
# A column whose literal this does not know how to read, and a large
# value (written by separate LOB operations), is read as of the commit's
# SCN from the table itself instead.

_TOKEN = re.compile(r"""\s*(?:
      (?P<str>'(?:[^']|'')*')
    | (?P<ident>"(?:[^"]|"")*")
    | (?P<op>IS\s+NULL\b|<>|!=|=|,|\)|\(|;|\.)
    | (?P<word>[A-Za-z_][A-Za-z0-9_$#]*)
    | (?P<num>[-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?)
    )""", re.X | re.I)


class _Lexer:
    def __init__(self, text):
        self.text, self.at = text, 0

    def peek(self):
        m = _TOKEN.match(self.text, self.at)
        return m

    def next(self):
        m = self.peek()
        if m is None:
            raise ValueError(f"cannot read the redo statement at"
                             f" {self.text[self.at:self.at + 40]!r}")
        self.at = m.end()
        return m

    def expect(self, *words):
        m = self.next()
        got = m.group(0).strip().upper()
        if got not in [w.upper() for w in words]:
            raise ValueError(f"expected {' or '.join(words)} in the redo"
                             f" statement, found {got!r}")
        return m

    def ident(self):
        m = self.next()
        if not m.group("ident"):
            raise ValueError(f"expected a name, found {m.group(0)!r}")
        return m.group("ident")[1:-1].replace('""', '"')

    def value(self):
        """('str', text) | ('num', text) | ('null',) | ('fn', NAME, [args])"""
        m = self.next()
        if m.group("str"):
            return ("str", m.group("str")[1:-1].replace("''", "'"))
        if m.group("num"):
            return ("num", m.group("num"))
        nxt = self.peek()
        if m.group("word") and nxt is not None and \
                nxt.group(0).strip() == "(":
            self.next()
            name, args = m.group("word").upper(), []
            while True:
                nxt = self.peek()
                if nxt is not None and nxt.group(0).strip() == ")":
                    self.next()
                    break
                args.append(self.value())
                sep = self.next().group(0).strip()
                if sep == ")":
                    break
                if sep != ",":
                    raise ValueError(f"expected , in {name}(...)")
            return ("fn", name, args)
        if m.group("word") and m.group("word").upper() == "NULL":
            return ("null",)
        raise ValueError(f"not a value in the redo statement:"
                         f" {m.group(0)!r}")


def _pairs(lex, stop):
    """`"C" = v, ...` or `"C" = v and "D" IS NULL and ...` up to `stop`
    (a word or `;`); ROWID is left out."""
    out = {}
    while True:
        m = lex.peek()
        if m is None:
            return out
        word = m.group(0).strip().upper()
        if word in stop:
            return out
        if word in (",", "AND"):
            lex.next()
            continue
        if m.group("word") and word == "ROWID":
            lex.next()
            lex.expect("=")
            lex.value()
            continue
        name = lex.ident()
        op = lex.next().group(0).strip().upper()
        if op.startswith("IS"):
            out[name] = ("null",)
        elif op == "=":
            out[name] = lex.value()
        else:
            raise ValueError(f"unexpected {op!r} after {name}")


def parse_redo(sql):
    """(op, owner, table, after, before) from one SQL_REDO statement:
    `after` the values it writes, `before` what its WHERE held."""
    lex = _Lexer(sql)
    verb = lex.next().group(0).strip().lower()
    if verb == "insert":
        lex.expect("into")
        owner = lex.ident()
        lex.expect(".")
        table = lex.ident()
        lex.expect("(")
        names = []
        while True:
            names.append(lex.ident())
            if lex.next().group(0).strip() == ")":
                break
        lex.expect("values")
        lex.expect("(")
        values = []
        while True:
            values.append(lex.value())
            if lex.next().group(0).strip() == ")":
                break
        return "insert", owner, table, dict(zip(names, values)), {}
    if verb == "update":
        owner = lex.ident()
        lex.expect(".")
        table = lex.ident()
        lex.expect("set")
        after = _pairs(lex, ("WHERE", ";"))
        before = {}
        if lex.peek() is not None and \
                lex.peek().group(0).strip().upper() == "WHERE":
            lex.next()
            before = _pairs(lex, (";",))
        return "update", owner, table, after, before
    if verb == "delete":
        lex.expect("from")
        owner = lex.ident()
        lex.expect(".")
        table = lex.ident()
        lex.expect("where")
        return "delete", owner, table, {}, _pairs(lex, (";",))
    raise ValueError(f"not a row change: {sql[:40]!r}")


class NotRead(Exception):
    """A literal this reader does not turn into a value: the column is
    read from the table as of the commit instead."""


_UNISTR = re.compile(r"\\([0-9A-Fa-f]{4})")


def _text(v):
    if v[0] == "str":
        return v[1]
    if v[0] == "fn" and v[1] == "UNISTR" and v[2] and v[2][0][0] == "str":
        raw = _UNISTR.sub(lambda m: chr(int(m.group(1), 16)), v[2][0][1])
        # a character outside the first plane is written as its two halves
        return raw.encode("utf-16", "surrogatepass").decode("utf-16")
    raise NotRead(v)


def value_of(kind, v):
    """The Python value of one literal, for a column of `kind` - as the
    driver would have handed the column back."""
    if v[0] == "null":
        return None
    if kind in ("NUMBER", "FLOAT"):
        if v[0] in ("str", "num"):
            return Decimal(v[1])
        raise NotRead(v)
    if kind in ("BINARY_FLOAT", "BINARY_DOUBLE"):
        text = v[1] if v[0] in ("str", "num") else (
            v[2][0][1] if v[0] == "fn" and v[2] and v[2][0][0] == "str"
            else None)
        if text is None:
            raise NotRead(v)
        low = text.strip().lower()
        return {"nan": float("nan"), "inf": float("inf"),
                "-inf": float("-inf")}.get(low, None) if low in (
            "nan", "inf", "-inf") else float(text)
    if kind in ("VARCHAR2", "NVARCHAR2"):
        return _text(v)
    if kind in ("CHAR", "NCHAR"):
        return _text(v).rstrip(" ")
    if kind == "RAW":
        if v[0] == "fn" and v[1] == "HEXTORAW" and v[2][0][0] == "str":
            return bytes.fromhex(v[2][0][1])
        raise NotRead(v)
    if kind == "DATE":
        if v[0] == "fn" and v[1] == "TO_DATE" and v[2][0][0] == "str":
            return datetime.datetime.strptime(v[2][0][1],
                                              "%Y-%m-%d %H:%M:%S")
        raise NotRead(v)
    if kind == "TIMESTAMP":
        if v[0] == "fn" and v[1] == "TO_TIMESTAMP" and v[2][0][0] == "str":
            # every digit after the second, as a read keeps them
            from .. import nanotime
            return nanotime.parse(v[2][0][1])
        raise NotRead(v)
    raise NotRead(v)


def row_change(db, table, verb, kinds, key, values, missing, old,
               old_missing, scn):
    """(change record, columns to read from the table as of the commit)
    for one row change either reader read out of the redo.

    An update that moves the key arrives at its new address whole: its
    statement names only what it changed, and a row made at the new key
    from that alone would lose every other column - from the row as it was
    where the redo holds all of it, else from the table as of the
    commit."""
    from .. import canon
    missing = list(missing)
    if verb == "insert":
        if any(k in missing or k not in values for k in key):
            raise SystemExit(f"{db}.{table}: a row's key could not be read"
                             f" from the redo at SCN {scn}")
        missing += [n for n in kinds if n not in values and n not in missing]
        return canon.change("insert", table, {k: values[k] for k in key},
                            values), missing
    if any(k not in old for k in key):
        raise SystemExit(
            f"{db}.{table}: a change at SCN {scn} does not carry its row's"
            " key in the redo: alter table ... add supplemental log data"
            " (primary key) columns")
    ident = {k: old[k] for k in key}
    whole = not old_missing and set(old) >= set(kinds)
    before = dict(old) if whole else None
    if verb == "delete":
        return canon.change("delete", table, ident, before=before), []
    if any(k in values and values[k] != ident[k] for k in key):
        if whole:
            values = {**{n: v for n, v in old.items() if n not in missing},
                      **values}
        else:
            missing += [n for n in kinds
                        if n not in values and n not in missing]
    return canon.change("update", table, ident, values,
                        before=before), missing


def standby_gate(eng, side, db, conn):
    """Stops, in words, unless `conn` is a physical standby of this very
    source, open read-only while it applies (Active Data Guard): LogMiner
    reads the dictionary through an open database, and a standby of
    another database, or one that is not applying, has other redo.
    Returns the SCN the standby has applied - the furthest a window mined
    there may reach."""
    role, mode, dbid, applied = conn.cursor().execute(
        "select database_role, open_mode, dbid, current_scn from"
        " v$database").fetchone()
    mine = eng._rows(side, db, "select dbid from v$database")[0][0]
    if role != "PHYSICAL STANDBY":
        raise SystemExit(f"mine_standby names a database that is not a"
                         f" physical standby (it is {role}): mine on the"
                         " source instead, by leaving mine_standby out")
    if not str(mode).startswith("READ ONLY"):
        raise SystemExit(f"the standby mine_standby names is not open"
                         f" read-only ({mode}): LogMiner reads the"
                         " dictionary through an open database, which on a"
                         " standby is Active Data Guard")
    if str(dbid) != str(mine):
        raise SystemExit("the standby mine_standby names is the standby of"
                         f" another database (DBID {dbid}, the source's is"
                         f" {mine})")
    return int(applied)


# ---- where the redo is mined ---------------------------------------------
#
# Mining on the source asks it for grants, supplemental logging it already
# has, and the CPU of the mining; where it refuses that (the grants are not
# given, or the hop says the source mines nothing), the redo is mined
# elsewhere: on a physical standby that applies it, or on a database
# migkit runs itself, to which the source's archived logs are shipped and
# mined there with the dictionary the source wrote into its redo. The
# positions are the source's SCNs end to end: a shipped log is checked to
# read on the mining database at the SCNs the source wrote it at.

SOURCE, STANDBY, INSTANCE = "source", "standby", "own instance"
#: what mining on the source needs its account to hold
MINING_GRANTS = ("LOGMINING", "SELECT ANY TRANSACTION")
#: {(hop, side, db): {place: [seconds a window's change lagged, ...]}}
_LAGS = {}


class ShipGap(SystemExit):
    """A run of the source's archived logs with one missing (a sequence
    skipped, deleted, or an SCN range no log covers): the changes in it
    would be lost without a word, so the read stops instead."""


def _elsewhere(ep):
    """Where the hop names another database to mine on."""
    o = ep.options
    return bool(o.get("mine_standby")) or bool(
        o.get("mine_instance") and o.get("archive_from")
        and o.get("archive_to"))


def places():
    """The ways the redo is mined, by where: the first that holds taken,
    the measured lag ranking them where each was measured."""
    from ..decide import Need, Rung
    return (
        Rung(STANDBY, "mining on a physical standby of the source",
             needs=(Need("a standby named", lambda f: f["standby"],
                         "{rung} is not used: the hop names no"
                         " mine_standby"),),
             because="the source does no mining at all, and the standby"
                     " has the redo as far as it has applied it",
             leaves="nothing"),
        Rung(SOURCE, "mining on the source",
             needs=(Need("the source mining", lambda f: not f["off source"],
                         "{rung} is not used: the hop says the source mines"
                         " nothing (mine_off_source)"),
                    Need("the mining grants", lambda f: not f["short"],
                         lambda f: "mining on the source needs"
                         f" {', '.join(f['short'])}, which its account"
                         " lacks")),
             because="the redo is read where it is written: the least lag",
             leaves="nothing"),
        Rung(INSTANCE, "mining on migkit's own mining database, the"
                       " archived redo shipped to it",
             needs=(Need("a mining database", lambda f: f["instance"],
                         "{rung} is not used: the hop names no"
                         " mine_instance"),
                    Need("the archive reachable", lambda f: f["archive"],
                         "{rung} needs where the source's archived logs are"
                         " read (archive_from) and where the mining"
                         " database reads them (archive_to)"),
                    Need("a dictionary in the redo",
                         lambda f: f["dictionary"] is not None,
                         "the source has written no dictionary into its"
                         " archived redo, which mining it elsewhere reads"
                         " the tables by: run once on the source"
                         " begin dbms_logmnr_d.build(options =>"
                         " dbms_logmnr_d.store_in_redo_logs); end; and"
                         " alter system archive log current")),
             because="the source only archives its redo, as it does"
                     " anyway; the lag is an archive switch, the ship and"
                     " the mining",
             footprint=1,
             leaves="the shipped archived logs, where the mining database"
                    " reads them"))


def place(eng, side, db):
    """The climb of `places` for this source: what its account holds, what
    the hop names, the dictionary its redo has - and the lag each place
    was measured at here."""
    from ..decide import Facts, climb
    ep = eng._endpoint(side)
    o = ep.options
    lags = {k: sum(v) / len(v) for k, v in
            _LAGS.get((id(eng.hop), side, db), {}).items() if v}
    got = climb(db, places(), Facts(
        {"standby": bool(o.get("mine_standby")),
         "off source": str(o.get("mine_off_source", "")).lower()
         in ("1", "true", "yes"),
         "instance": bool(o.get("mine_instance")),
         "archive": bool(o.get("archive_from") and o.get("archive_to"))},
        short=lambda: [p for p in MINING_GRANTS
                       if p not in granted(eng, side, db)],
        dictionary=lambda: dictionary_scn(eng, side, db)),
        costs=lags or None, prove=False)
    if got.rung is None:
        raise SystemExit(f"{db}: the source's redo can be mined nowhere -"
                         f" {got.reason}")
    return got


def lagged(eng, side, db, where, seconds):
    """A window's lag measured: the place's from now on ranks by it."""
    got = _LAGS.setdefault((id(eng.hop), side, db), {}).setdefault(where,
                                                                   [])
    got.append(float(seconds))
    del got[:-20]


def dictionary_scn(eng, side, db, before=None):
    """The first SCN of the newest dictionary the source wrote into its
    archived redo (at or before `before`), or None."""
    sql = ("select max(first_change#) from v$archived_log where"
           " dictionary_begin = 'YES' and deleted = 'NO' and status = 'A'")
    args = ()
    if before is not None:
        sql += " and first_change# <= :1"
        args = (int(before),)
    got = eng._rows(side, db, sql, args)
    return int(got[0][0]) if got and got[0][0] is not None else None


#: the ways the logs a window adds are kept from growing with the time
#: since the dictionary was written
KEEP, REBUILD, CARRY = "keep", "rebuild", "carry on"
#: {(hop, side, db): [(bytes, seconds) one window's logs took to ship and
#: be mined]}
_SCANS = {}
#: {(hop, side, db): the last dictionary decision, as `assess` says it}
_DICTIONARIES = {}


def _can_build(eng, side, db):
    got = eng._rows(side, db, "select count(*) from all_tab_privs where"
                              " table_name = 'DBMS_LOGMNR_D' and privilege"
                              " = 'EXECUTE'")
    return bool(got and int(got[0][0]))


def _dictionary_change():
    from ..decide import BY_DBA, POSSIBLE, Change, Stand
    return Change(
        "dictionary", "a newer dictionary written into its redo",
        lambda f: Stand(POSSIBLE) if f["can build"] else Stand(
            BY_DBA, "the account may not run dbms_logmnr_d"),
        make=("begin dbms_logmnr_d.build(options =>"
              " dbms_logmnr_d.store_in_redo_logs); end;",
              "alter system archive log current"),
        after="at once: it only writes the dictionary into the redo",
        speeds=False)


def dictionary_ways():
    """The ways the logs a window adds - every one since the dictionary the
    read stands on - are kept in what the reader holds: keep them while
    they fit, write a newer dictionary (the read starts on it once it
    passes it) where the hop allows that change, or carry on over the
    bound and say it."""
    from ..decide import Need, Rung
    return (
        Rung(KEEP, "the logs since the dictionary kept as they are",
             needs=(Need("the logs within what the reader holds",
                         lambda f: f["fits"] or f["pending"],
                         lambda f: f"the logs a window adds are"
                         f" {f['size'] / 2 ** 20:.0f} MB, over the"
                         f" {f['bound'] / 2 ** 20:.0f} MB the reader holds"
                         f" ({f['limit']})"),),
             because="the logs fit what the reader holds, or a newer"
                     " dictionary is already written and the read will"
                     " pass it",
             leaves="nothing"),
        Rung(REBUILD, "a newer dictionary written into the source's redo",
             needs=(Need("no newer dictionary waiting",
                         lambda f: not f["pending"],
                         "a newer dictionary is written already"),),
             changes=(_dictionary_change(),),
             because="the read starts on the newer dictionary once it"
                     " passes it, and the set of logs starts there",
             leaves="one dictionary in the source's archived redo"),
        Rung(CARRY, "the logs kept over what the reader holds",
             because="no newer dictionary is allowed to be written, so the"
                     " window's lag grows with the logs",
             leaves="nothing"))


def dictionary_bound(eng, side, db, logs):
    """(bytes, why) the logs of a window may come to before the reader
    falls behind them: the least of half this machine's free memory and
    what the reader shipped and mined, a second, over half the source's
    measured archive switch (the logs are mined again at each window's
    start, so their mining has to fit well within a switch). (None, why)
    where neither is measured."""
    from .. import sizing
    parts = {}
    memory = (sizing.host() or {}).get("memory")
    if memory:
        parts["half this machine's free memory"] = int(memory) // 2
    scans = _SCANS.get((id(eng.hop), side, db))
    switch = switch_seconds(logs)
    if scans and switch:
        speed = sum(b for b, _ in scans) / max(sum(t for _, t in scans),
                                               1e-3)
        parts["what the reader mines in half an archive switch"] = \
            int(speed * switch / 2)
    if not parts:
        return None, "neither the memory nor the reader's pace is measured"
    why = min(parts, key=parts.get)
    return parts[why], why


def dictionary_choice(eng, side, db, logs, start):
    """(Choice, facts): which way keeps the logs of the window that starts
    at `start` within the reader's bound - decided, not run."""
    from .. import sourcechange
    from ..decide import Facts, climb
    size = sum(int(g.get("bytes") or 0) for g in logs)
    bound, limit = dictionary_bound(eng, side, db, logs)
    newest = dictionary_scn(eng, side, db)
    ways = dictionary_ways()
    facts = Facts({"size": size, "bound": bound or 0, "limit": limit,
                   "fits": bound is None or size <= bound,
                   "pending": newest is not None and newest > int(start)},
                  **{"can build": lambda: _can_build(eng, side, db)})
    sourcechange.put_answers(eng.hop, facts, ways)
    return climb(db, ways, facts, prove=False), facts


def build_dictionary(eng, side, db):
    """Writes a newer dictionary into the source's redo and archives it."""
    change = _dictionary_change()
    conn = eng._dedicated(side)
    try:
        cur = conn.cursor()
        for stmt in change.statements({}):
            cur.execute(stmt)
    finally:
        conn.close()


def dictionary_way(eng, side, db, logs, start):
    """The decision of `dictionary_choice`, carried out and kept for
    `assess`: a newer dictionary written where that is the way. A build that
    fails is said and the read goes on over the bound - never stopped."""
    got, facts = dictionary_choice(eng, side, db, logs, start)
    note = f"{got.rung.said}: {got.reason}"
    if got.rung.name == REBUILD:
        try:
            build_dictionary(eng, side, db)
        except Exception as e:  # noqa: BLE001 - said, the read goes on
            note += f" - not built: {str(e).strip().splitlines()[0][:120]}"
    _DICTIONARIES[(id(eng.hop), side, db)] = {
        "way": got.rung.name, "note": note, "size": facts["size"],
        "bound": facts["bound"], "limit": facts["limit"]}
    return got


def archived(eng, side, db, start):
    """The source's archived logs from `start` on, one destination, each
    {thread, seq, first, next, name, bytes, dict, at}, in order - held to
    `contiguous`."""
    rows = eng._rows(side, db, (
        "select thread#, sequence#, first_change#, next_change#, name,"
        " blocks * block_size, dictionary_begin, first_time from"
        " v$archived_log where status = 'A' and deleted = 'NO' and"
        " name is not null and next_change# > :1 and dest_id = (select"
        " min(dest_id) from v$archived_log where status = 'A') order by"
        " thread#, sequence#"), (int(start),))
    logs = [dict(zip(("thread", "seq", "first", "next", "name", "bytes",
                      "dict", "at"), r)) for r in rows]
    contiguous(logs, start)
    return logs


def contiguous(logs, start):
    """Stops with `ShipGap` where the logs leave a gap: per thread, the
    first must reach back to `start`, and each must begin where the one
    before it ended, the next sequence."""
    by = {}
    for g in logs:
        by.setdefault(int(g["thread"]), []).append(g)
    for thread, run in by.items():
        if int(run[0]["first"]) > int(start):
            raise ShipGap(
                f"the source's archived redo of thread {thread} begins at"
                f" SCN {run[0]['first']}, after {start} where the read"
                " stands: the logs between were deleted or never archived,"
                " and the changes in them cannot be read. Restore them to"
                " the archive, or copy the tables again")
        for a, b in zip(run, run[1:]):
            if int(b["seq"]) != int(a["seq"]) + 1 \
                    or int(b["first"]) != int(a["next"]):
                raise ShipGap(
                    f"the source's archived redo of thread {thread} has a"
                    f" gap after sequence {a['seq']} (SCN {a['next']}; the"
                    f" next log is sequence {b['seq']} from SCN"
                    f" {b['first']}): restore the missing log to the"
                    " archive - the changes in it are read nowhere else")


def reach(logs):
    """The SCN every thread's archived redo has reached: a window mined on
    shipped logs ends below it."""
    by = {}
    for g in logs:
        by[int(g["thread"])] = max(by.get(int(g["thread"]), 0),
                                   int(g["next"]))
    return min(by.values()) - 1 if by else None


def switch_seconds(logs):
    """The source's measured time between archive switches, from the logs'
    first times: what a change waits before it can be shipped."""
    times = sorted(g["at"] for g in logs if g.get("at") is not None)
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])
            if hasattr(b - a, "total_seconds")]
    return sum(gaps) / len(gaps) if gaps else None


class Ship:
    """The source's archived logs moved to where migkit's mining database
    reads them, each once, its digest kept, the move timed and its
    positions checked once the mining database has it."""

    def __init__(self, eng, side, db, clock=time.monotonic):
        from .. import carriers
        ep = eng._endpoint(side)
        o = ep.options
        self.eng, self.side, self.db, self.clock = eng, side, db, clock
        self.src = carriers.at(o["archive_from"],
                               said="the source's archived redo")
        self.dst = carriers.at(o["archive_to"],
                               said="where the mining database reads it")
        self.seen_as = str(o.get("archive_seen_as") or o["archive_to"])
        self.link = None
        self.shipped = {}
        self.seconds = []

    def path(self, g):
        return self.seen_as.rstrip("/") + "/" + _base(g["name"])

    def ship(self, logs):
        """[path the mining database reads, ...] for `logs`, each moved
        where it is not there whole already."""
        from .. import carriers
        out = []
        for g in logs:
            key = (int(g["thread"]), int(g["seq"]))
            name = _base(g["name"])
            if key not in self.shipped:
                have = self.dst.size(name)
                if have is None or have != self.src.size(name):
                    t = self.clock()
                    moved = carriers.move_file(self.src, name, self.dst,
                                               link=self.link)
                    self.seconds.append(self.clock() - t)
                    del self.seconds[:-20]
                    self.link = getattr(self.dst, "link", None) or self.link
                    digest = moved.digest
                else:
                    digest = None
                self.shipped[key] = (self.path(g), digest, int(g["first"]),
                                     int(g["next"]))
            out.append(self.shipped[key][0])
        return out



def _base(name):
    return str(name).replace("\\", "/").rsplit("/", 1)[-1]


def aligned(conn, logs, ship):
    """Stops where a shipped log reads on the mining database at other SCNs
    than the source wrote it at: the wrong file, or one cut short."""
    got = {(int(t), int(s)): (int(lo), int(hi)) for t, s, lo, hi in
           conn.cursor().execute(
               "select thread_id, log_id, low_scn, next_scn from"
               " v$logmnr_logs").fetchall()}
    for g in logs:
        key = (int(g["thread"]), int(g["seq"]))
        want = (int(g["first"]), int(g["next"]))
        if got.get(key) != want:
            ship.shipped.pop(key, None)
            raise ShipGap(
                f"the shipped log of thread {key[0]} sequence {key[1]}"
                f" reads on the mining database at SCNs {got.get(key)},"
                f" and the source wrote it at {want}: it is shipped again"
                " at the next read; where it stays so, the archive holds"
                " another file under its name")


# ---- the reader ----------------------------------------------------------

class Miner:
    """One LogMiner reader for one side and schema. The mining session and
    its open cursor are held between reads while the caller comes back
    with the position it was handed, as the binlog reader holds its
    stream: a window is mined once, not again for each batch of it."""

    RUNG = "logminer"

    def __init__(self, eng, side, db):
        self.eng, self.side, self.db = eng, side, db
        self.conn = self.cur = None
        self.held = None
        self.ddl = None
        self.where, self.ship, self.shipping = None, None, []
        self.samples = _SAMPLES.setdefault(
            (id(eng.hop), side, db), [])
        self.shapes = {}

    # -- the session

    def _open(self):
        """The mining session: on the source itself; from its container's
        root where `mine_service` names it (a database before 21c mines
        only there); or on a physical standby where `mine_standby` names
        one, so the source does no mining at all - gated (`standby_gate`)
        and not run here: this machine's sandbox has no Data Guard; or on
        migkit's own mining database (`mine_instance`), the source's
        archived logs shipped to it (`Ship`). Where is decided (`place`)."""
        if self.conn is not None:
            return self.conn
        ep = self.eng._endpoint(self.side)
        self.where = place(self.eng, self.side, self.db).path
        self.ship = None
        if self.where == INSTANCE:
            self.ship = Ship(self.eng, self.side, self.db)
            self.conn = self.eng._dedicated(
                self.side, dsn=ep.options["mine_instance"],
                user=ep.options.get("mine_instance_user"),
                password=ep.option_secret("mine_instance_password"))
            self.standby = False
            # the shipped logs are the whole container's redo: a source in
            # a pluggable database is told apart by its name in it
            con = self.eng._rows(self.side, self.db, "select sys_context("
                                 "'userenv', 'con_name') from dual")[0][0]
            self.pdb = None if con == "CDB$ROOT" else con
            self.root = True
            return self.conn
        standby = ep.options.get("mine_standby") \
            if self.where == STANDBY else None
        service = ep.options.get("mine_service")
        self.conn = self.eng._dedicated(
            self.side, dsn=standby or (self.eng._dsn(ep, service)
                                       if service else None),
            user=ep.options.get("mine_user"),
            password=ep.option_secret("mine_password"))
        self.standby = bool(standby)
        if self.standby:
            standby_gate(self.eng, self.side, self.db, self.conn)
        self.root = self.conn.cursor().execute(
            "select sys_context('userenv', 'con_name') from"
            " dual").fetchone()[0] == "CDB$ROOT"
        return self.conn

    def close(self):
        self._end()
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001 - already gone
                pass
        self.conn = None

    def _end(self):
        if self.cur is not None:
            try:
                self.cur.close()
            except Exception:  # noqa: BLE001
                pass
        self.cur = None
        self.held = None
        if self.conn is not None:
            try:
                self.conn.cursor().execute("begin dbms_logmnr.end_logmnr;"
                                           " end;")
            except Exception:  # noqa: BLE001 - no session was open
                pass

    def _add_logs(self, start, end):
        """Mining from the root of a container database: the logs covering
        the window are named to LogMiner, the archived ones first. On
        migkit's own mining database they are the shipped ones, checked
        to read at the SCNs the source wrote them at (`aligned`)."""
        cur = self.conn.cursor()
        if self.ship is not None:
            logs = [g for g in self.shipping if int(g["first"]) <= end]
            first = True
            for n in self.ship.ship(logs):
                cur.execute("begin dbms_logmnr.add_logfile(:1, :2); end;",
                            [n, 1 if first else 3])
                first = False
            aligned(self.conn, logs, self.ship)
            return
        names = [n for (n,) in cur.execute(
            "select name from v$archived_log where deleted = 'NO' and"
            " status = 'A' and name is not null and next_change# > :1 and"
            " first_change# <= :2 and dest_id = (select min(dest_id) from"
            " v$archived_log where status = 'A') order by sequence#",
            [start, end]).fetchall()]
        names += [m for (m,) in cur.execute(
            "select min(f.member) from v$log l join v$logfile f on"
            " f.group# = l.group# where l.next_change# > :1 or l.status ="
            " 'CURRENT' group by l.group#", [start]).fetchall()]
        first = True
        for n in dict.fromkeys(names):
            cur.execute("begin dbms_logmnr.add_logfile(:1, :2); end;",
                        [n, 1 if first else 3])
            first = False

    def _start(self, start, end):
        """A LogMiner session over [start, end], committed transactions
        only, and the cursor over the schema's rows. In a pluggable
        database the logs are found by LogMiner itself (measured on 23.26:
        naming one there is refused, ORA-65040); from the root they are
        named, and the rows are the pluggable database's."""
        self._open()
        if self.root:
            self._add_logs(start, end)
        # elsewhere than the source the online catalogue is another
        # database's: the dictionary is the one the source wrote into its
        # redo, kept current through the DDL mined after it
        dictionary = ("dbms_logmnr.dict_from_redo_logs +"
                      " dbms_logmnr.ddl_dict_tracking"
                      if self.ship is not None
                      else "dbms_logmnr.dict_from_online_catalog")
        self.conn.cursor().execute(
            "begin dbms_logmnr.start_logmnr(startscn => :1, endscn => :2,"
            f" options => {dictionary} +"
            " dbms_logmnr.committed_data_only); end;", [start, end])
        cur = self.conn.cursor()
        cur.arraysize = 2000
        cur.prefetchrows = 2001
        codes = ", ".join(str(c) for c in (INSERT, DELETE, UPDATE, DDL,
                                           UNSUPPORTED) + LOB_OPS)
        sql = ("select scn, commit_scn, xidusn || '.' || xidslt || '.' ||"
               " xidsqn, operation_code, table_name, csf, sql_redo, rs_id,"
               " ssn from v$logmnr_contents where seg_owner = :1 and"
               f" operation_code in ({codes})")
        owner = self.eng._owner(self.side, self.db)
        if self.ship is not None:
            if self.pdb:
                cur.execute(sql + " and src_con_name = :2", [owner,
                                                             self.pdb])
            else:
                cur.execute(sql, [owner])
        elif self.root:
            pdb = self.eng._rows(self.side, self.db, "select sys_context("
                                                     "'userenv', 'con_name')"
                                                     " from dual")[0][0]
            cur.execute(sql + " and src_con_name = :2", [owner, pdb])
        else:
            cur.execute(sql, [owner])
        self.cur = cur

    # -- the table's shape

    def _shape(self, table):
        """(kind by column, key) for a table, as the catalogue says now."""
        got = self.shapes.get(table)
        if got is None:
            facts = self.eng._facts(self.side, self.db, table)
            # a virtual column is not in the redo, and not written: the
            # target computes its own
            kinds = {f["name"]: f["kind"] for f in facts
                     if not f["virtual"]}
            key = self.eng.neutral_key(self.side, self.db, table)
            if not key:
                raise SystemExit(
                    f"{table}: a change to it came through, and it has no"
                    " primary key, so the change cannot be placed on the"
                    " target. Give it a key, or leave it out of the hop")
            got = self.shapes[table] = (kinds, key)
        return got

    # -- a window

    def _window(self, token):
        at, now, low = _sample(self.eng, self.side, self.db)
        self.samples.append((at, now, low))
        # samples older than the longest wait are kept only as the newest
        # of them
        old = [s for s in self.samples if at - s[0] > RESTART_SECONDS]
        if len(old) > 1:
            for s in old[:-1]:
                self.samples.remove(s)
        start = int(token["scn"])
        end = now
        if self._open() is not None and getattr(self, "standby", False):
            # the standby has the redo only as far as it has applied it:
            # the window ends there, and what `through` says is held to it
            end = min(end, standby_gate(self.eng, self.side, self.db,
                                        self.conn))
            self.samples[-1] = (at, end, low)
        switch = 0.0
        if self.ship is not None:
            # only an archived log can be shipped: the window ends where
            # every thread's archived redo has reached, and is mined from
            # the dictionary the source wrote at or before it
            since = dictionary_scn(self.eng, self.side, self.db,
                                   before=start)
            if since is None:
                raise SystemExit(
                    f"{self.db}: no dictionary the source wrote into its"
                    f" archived redo is at or before SCN {start}, where the"
                    " read stands, so the mining database cannot read its"
                    " tables there: run once on the source begin"
                    " dbms_logmnr_d.build(options =>"
                    " dbms_logmnr_d.store_in_redo_logs); end; - then copy"
                    " the tables again from a point after it")
            self.shipping = archived(self.eng, self.side, self.db, since)
            dictionary_way(self.eng, self.side, self.db, self.shipping,
                           start)
            got = reach(self.shipping)
            end = min(end, got if got is not None else start)
            self.samples[-1] = (at, end, low)
            switch = switch_seconds(self.shipping) or 0.0
        began = time.monotonic()
        self._start(start, max(end, start))
        if self.ship is not None:
            scans = _SCANS.setdefault((id(self.eng.hop), self.side,
                                       self.db), [])
            scans.append((sum(int(g["bytes"] or 0) for g in self.shipping
                              if int(g["first"]) <= end),
                          time.monotonic() - began))
            del scans[:-20]
        # the lag a change of this window had: the switch that archived it,
        # then its ship and the mining, which `_start` does
        lagged(self.eng, self.side, self.db, self.where,
               switch + time.monotonic() - began)
        return at

    def _settled(self, at, token):
        """(through, restart) for a window whose end was read at `at` and
        wholly handed back."""
        through, restart = int(token.get("through") or 0), int(token["scn"])
        for t, now, low in self.samples:
            if at - t >= FLUSH_SECONDS:
                through = max(through, now)
            if at - t >= RESTART_SECONDS:
                restart = max(restart, low)
        return through, restart

    #: rows of one transaction held before it is handed back in parts
    LARGEST_HELD = 20000

    def read(self, token, limit=1000, ddl=None):
        """(changes, token): whole transactions until about `limit`
        changes, in commit order; a transaction larger than the reader
        holds is handed back in parts, the part said in the token so a
        read that starts over leaves out what went. `ddl(db, text,
        commit)` turns a DDL statement of the redo into the changes that
        carry it."""
        self.ddl = ddl
        lost_why = lost(self.eng, self.side, self.db, token)
        if lost_why:
            raise SystemExit(lost_why)
        token = dict(token)
        token.setdefault("xids", [])
        part = dict(token.get("part") or {})
        held = self.held if (self.held is not None
                             and self.held["token"] == token) else None
        self.held = None
        if held is None:
            self._end()
            at = self._window(token)
            carry = []
            # mined again from the start: the part already handed back of
            # a transaction is read again, and left out
            skip_xid, skip = part.get("xid"), int(part.get("rows") or 0)
        else:
            at, carry = held["at"], held["carry"]
            skip_xid, skip = None, 0
        done_commit = int(token.get("commit") or 0)
        done = set(token.get("xids") or [])
        out, txn = [], list(carry)
        biggest = self.LARGEST_HELD

        def changes_of(rows):
            nonlocal skip
            got = self._changes(rows, int(rows[0][1]))
            if skip and rows[0][2] == skip_xid:
                gone = min(skip, len(got))
                got, skip = got[gone:], skip - gone
            return got

        def finished(xid, commit):
            nonlocal done_commit, done, part
            if commit != done_commit:
                done_commit, done = commit, set()
            done.add(xid)
            part = {}

        def close():
            xid, commit = txn[0][2], int(txn[0][1])
            out.extend(changes_of(txn))
            txn.clear()
            finished(xid, commit)

        exhausted = False
        while True:
            row = self._next()
            if row is None:
                exhausted = True
                break
            commit, xid = int(row[1]), row[2]
            if commit < done_commit or (commit == done_commit
                                        and xid in done):
                continue
            if txn and xid != txn[0][2]:
                close()
                if len(out) >= limit:
                    txn.append(row)
                    break
            elif not txn and part.get("xid") and xid != part["xid"]:
                # the transaction handed back in parts ended with its last
                # part
                finished(part["xid"], int(part["commit"]))
            txn.append(row)
            if len(txn) >= biggest:
                if out:
                    # what is whole goes first; this one is carried
                    break
                xid = txn[0][2]
                got = changes_of(txn)
                out.extend(got)
                base = int(part.get("rows") or 0) \
                    if part.get("xid") == xid else 0
                part = {"xid": xid, "rows": base + len(got),
                        "commit": int(txn[0][1])}
                txn.clear()
                break
        if exhausted:
            if txn:
                close()
            elif part.get("xid"):
                finished(part["xid"], int(part["commit"]))
        token.update(commit=done_commit, xids=sorted(done))
        if part:
            token["part"] = part
        else:
            token.pop("part", None)
        if exhausted:
            self._end()
            through, restart = self._settled(at, token)
            token.update(through=through, scn=restart)
            return self._in_scope(out), token
        self.held = {"token": dict(token), "at": at, "carry": list(txn)}
        return self._in_scope(out), dict(token)

    def _next(self):
        """The next change, its continued statement joined."""
        row = self.cur.fetchone()
        if row is None:
            return None
        row = list(row)
        while row[5]:
            more = self.cur.fetchone()
            if more is None:
                break
            row[6] = (row[6] or "") + (more[6] or "")
            row[5] = more[5]
        return tuple(row)

    def _in_scope(self, changes):
        return [c for c in changes if not self.eng.hop.excluded(
            self.db, c["table"])]

    def _changes(self, rows, commit):
        """One transaction's rows as change records, in order; the columns
        the statement did not give read from the table as of its commit."""
        from .. import canon
        out, fetch = [], {}
        for scn, _, xid, op, table, _, redo, _, _ in rows:
            table = self.eng._said(table) if table else table
            if op == UNSUPPORTED:
                raise SystemExit(
                    f"{self.db}.{table}: the redo holds a change LogMiner"
                    " cannot read back as a row (a type or a storage it"
                    " does not support), at SCN"
                    f" {scn}. Nothing after it was handed on; leave the"
                    " table out of the hop, or move it again after")
            if op == DDL:
                # a statement the redo carries as text, at its place among
                # the rows; the shape of what it touched is read again
                made = self.ddl(self.db, redo or "", commit) \
                    if self.ddl else []
                for one in made:
                    for name in (one["table"], one.get("to")):
                        self.shapes.pop(name, None)
                out.extend(made)
                continue
            if self.eng.hop.excluded(self.db, table):
                continue
            kinds, key = self._shape(table)
            lob_cols = [n for n, k in kinds.items() if k in
                        ("CLOB", "NCLOB", "BLOB")]
            if op in LOB_OPS:
                # a large value is written by operations of its own, each
                # naming its row; the value is read as of the commit
                where = re.search(r"\bwhere\b(.*?)(?:\bfor update\b|;|$)",
                                  redo or "", re.I | re.S)
                if not where:
                    continue
                got = {self.eng._said(n): v for n, v in _pairs(
                    _Lexer(where.group(1) + " ;"), (";",)).items()}
                if any(k not in got for k in key):
                    raise SystemExit(
                        f"{self.db}.{table}: a large value written at SCN"
                        f" {scn} does not name its row by key in the redo:"
                        " alter table ... add supplemental log data"
                        " (primary key) columns")
                ident = {k: value_of(kinds[k], got[k]) for k in key}
                change = canon.change("update", table, ident, dict(ident))
                out.append(change)
                fetch.setdefault(table, []).append((change, lob_cols))
                continue
            try:
                verb, _, _, after, before = parse_redo(redo)
            except ValueError as e:
                raise SystemExit(
                    f"{self.db}.{table}: a change at SCN {scn} could not"
                    f" be read back ({e}). Nothing after it was handed on")
            by_name = {self.eng._said(n): v for n, v in after.items()}
            was = {self.eng._said(n): v for n, v in before.items()}
            values, missing = {}, []
            for n, v in by_name.items():
                try:
                    values[n] = value_of(kinds.get(n, ""), v)
                except NotRead:
                    missing.append(n)
            old, old_missing = {}, []
            for n, v in was.items():
                try:
                    old[n] = value_of(kinds.get(n, ""), v)
                except NotRead:
                    old_missing.append(n)
            change, missing = row_change(self.db, table, verb, kinds, key,
                                         values, missing, old, old_missing,
                                         scn)
            out.append(change)
            if missing:
                fetch.setdefault(table, []).append((change, missing))
        for table, wanted in fetch.items():
            self._as_of(table, commit, wanted)
        # the transaction each committed in, as the SCN it did: what the
        # tail holds to the table copier's marks (`Engine.mark_covers`)
        for change in out:
            change.setdefault("txn", int(commit))
        return out

    def _as_of(self, table, commit, wanted):
        """The columns the redo did not give, read from the table as it was
        at the transaction's commit. A row gone by then was deleted in the
        same transaction, whose delete follows it."""
        from .. import canon
        kinds, key = self._shape(table)
        names = sorted({n for _, cols in wanted for n in cols} | set(key))
        columns = [(n, None) for n in names]
        select, finish = self.eng._read_plan(self.side, self.db, table,
                                             columns)
        # where the row is at the commit: an update that moved its key has
        # the new one among its values
        keys = [tuple(c["values"].get(k, c["key"].get(k)) for k in key)
                for c, _ in wanted]
        wanted_keys = list(dict.fromkeys(keys))
        table_name = self.eng._qualified(self.side, self.db, table)
        cur = self._open().cursor()
        found = {}
        for at in range(0, len(wanted_keys), 500):
            part = wanted_keys[at:at + 500]
            conds, args = [], []
            for k in part:
                conds.append("(" + " and ".join(
                    f"{self.eng._q(n)} = :{len(args) + i + 1}"
                    for i, n in enumerate(key)) + ")")
                args += [canon.sql_value(v) for v in k]
            where = " or ".join(conds)
            try:
                got = cur.execute(f"select {select} from {table_name} as of"
                                  f" scn {int(commit)} where {where}",
                                  args).fetchall()
            except Exception as e:  # noqa: BLE001 - classified here
                if not any(code in str(e) for code in AS_OF_GONE):
                    raise
                # the table as it is now: a value newer than the commit's
                # is the one a later change in the stream writes anyway,
                # and a row gone since is removed by its own delete
                got = cur.execute(f"select {select} from {table_name}"
                                  f" where {where}", args).fetchall()
            rows = self.eng._unpad(self.side, self.db, table, columns,
                                   finish(got))
            for r in rows:
                found[tuple(r[names.index(k)] for k in key)] = dict(
                    zip(names, r))
        for (change, cols), k in zip(wanted, keys):
            row = found.get(k)
            if row is None:
                continue
            for n in cols:
                change["values"][n] = row[n]


# ---- before a hop is accepted -------------------------------------------

def redo_rate(eng, side, db):
    """(peak bytes a second over an hour, average) of the redo archived in
    the last day, or None where none was."""
    got = eng._rows(side, db, "select max(b), avg(b) from (select"
                              " trunc(completion_time, 'HH') h,"
                              " sum(blocks * block_size) / 3600 b from"
                              " v$archived_log where completion_time >"
                              " sysdate - 1 and dest_id = (select"
                              " min(dest_id) from v$archived_log) group by"
                              " trunc(completion_time, 'HH'))")
    if not got or got[0][0] is None:
        return None
    return float(got[0][0]), float(got[0][1])


def reader_rate(eng, side, db):
    """(bytes of redo a second the reader mines, the log it was timed on),
    measured on the most recent archived log, or None where there is none
    yet. Measured on Oracle Free 23.26 in the sandbox (2 CPUs): 31 and
    95 MB a second in two runs, on a log of a few MB - so a small log
    says little, and `assess` says the log it timed."""
    got = eng._rows(side, db, "select first_change#, next_change#, blocks *"
                              " block_size, name from v$archived_log where"
                              " deleted = 'NO' and status = 'A' and name is"
                              " not null order by next_change# desc fetch"
                              " first 1 rows only")
    if not got:
        return None
    first, nxt, size, name = got[0]
    miner = Miner(eng, side, db)
    try:
        miner._open()
        began = time.perf_counter()
        miner._start(int(first), int(nxt) - 1)
        while miner.cur.fetchmany(5000):
            pass
        took = max(time.perf_counter() - began, 1e-3)
    finally:
        miner.close()
    return float(size) / took, name


#: {(hop, side, db): (the source's busiest hour of redo, the rate the
#: reader mined at)} in bytes a second, as `reader_check` last measured
#: them: what the choice of reader ranks LogMiner by (`oracle_redo.choice`)
_RATES = {}


def measured(eng, side, db):
    """(peak redo a second, mined a second) as last measured here, or
    None where they were not: unmeasured is not a reason to refuse."""
    return _RATES.get((id(eng.hop), side, db))


def reader_check(eng, side, db):
    """(level, detail, measured) of the LogMiner reader against the redo
    the source writes: the one place this is measured and judged. `fail`
    where the reader mines slower than the source's busiest hour writes
    (it would fall behind), `warn` where it is under twice that or where
    it could not be measured (`measured` False: unknown, never a pass),
    `pass` otherwise. The detail carries the figures. `assess` lists it;
    the gate of a move or a tail (`ready`) stops on its `fail`."""
    try:
        rate = redo_rate(eng, side, db)
        mined = reader_rate(eng, side, db)
    except SystemExit as e:
        return "warn", f"not measured: {e}", False
    except Exception as e:  # noqa: BLE001 - said
        return "warn", ("not measured:"
                        f" {str(e).splitlines()[0][:90]}"), False
    if mined is None or rate is None:
        return "warn", ("not measured: the source has archived no redo in"
                        " the last day, so neither its rate nor the"
                        " reader's could be read"), False
    peak, avg = rate
    speed, _ = mined
    _RATES[(id(eng.hop), side, db)] = (peak, speed)
    words = (f"the source archived {peak / 2 ** 20:.2f} MB of redo a second"
             f" at its busiest hour ({avg / 2 ** 20:.2f} on average); the"
             f" reader mined {speed / 2 ** 20:.2f} MB a second")
    if speed < peak:
        return "fail", (words + " - it would fall behind at the busiest"
                        " hour. The redo-file reader, run next to the redo,"
                        " reads without the database's help"), True
    if speed < 2 * peak:
        return "warn", (words + " - under twice the peak, so a busier hour"
                        " than the last day's would put it behind"), True
    return "pass", words, True


def _dictionary_assess(eng, side, db, logs, add):
    """The logs a window adds since the dictionary, against what the
    reader holds, and the way chosen to keep them there."""
    try:
        start = min(int(g["first"]) for g in logs)
        got, facts = dictionary_choice(eng, side, db, logs, start)
    except Exception as e:  # noqa: BLE001 - said, as unknown
        add("warn", "the dictionary's logs",
            f"could not be judged: {str(e).splitlines()[0][:90]}")
        return
    size = f"{facts['size'] / 2 ** 20:.0f} MB in {len(logs)} logs"
    bound = (f"{facts['bound'] / 2 ** 20:.0f} MB ({facts['limit']})"
             if facts["bound"] else f"not bounded ({facts['limit']})")
    over = bool(facts["bound"]) and facts["size"] > facts["bound"]
    add("warn" if over and got.rung.name == CARRY else "pass",
        "the logs since the dictionary",
        f"{size} against {bound}: {got.rung.said} ({got.reason})")


def assess(eng, side, db):
    """What `assess` says about following this source: whether the redo
    can be read back, and whether the reader keeps up with it - the redo
    the source writes at its busiest hour against the redo the reader
    mines a second, measured on the source's own latest log."""
    items = []

    def add(level, item, detail):
        # "the source": a pair's assess asks each side alone, and leaves
        # this out of the target's
        items.append({"level": level, "scope": db,
                      "item": f"change feed from the source: {item}",
                      "detail": detail})
    try:
        for level, item, detail in readiness(eng, side, db):
            add(level, item, detail)
    except Exception as e:  # noqa: BLE001 - said, as unknown
        add("warn", "readiness", "could not be read:"
            f" {str(e).splitlines()[0][:90]} - unknown, not clean")
        return items
    try:
        big = eng._rows(side, db, "select max(t.used_ublk) *"
                                  " (select value from v$parameter where"
                                  " name = 'db_block_size'), (select value"
                                  " from v$parameter where name ="
                                  " 'pga_aggregate_limit') from"
                                  " v$transaction t")
        used, cap = big[0] if big else (None, None)
        if used and cap and int(cap) and int(used) > int(cap) // 2:
            add("warn", "largest open transaction",
                f"{int(used) // 2 ** 20} MB of undo, more than half the"
                f" PGA limit ({int(cap) // 2 ** 20} MB) the database buffers"
                " a mined transaction in")
    except Exception:  # noqa: BLE001 - optional
        pass
    try:
        got = place(eng, side, db)
        lags = _LAGS.get((id(eng.hop), side, db), {}).get(got.path)
        detail = f"{got.rung.said}: {got.reason}"
        if got.path == INSTANCE:
            logs = archived(eng, side, db, dictionary_scn(eng, side, db)
                            or 0)
            switch = switch_seconds(logs)
            detail += ("; a change waits for the archive switch, measured"
                       f" every {switch:.0f} s here" if switch is not None
                       else "; the archive switch is not measured yet")
        detail += (f"; a window's lag measured {sum(lags) / len(lags):.1f} s"
                   if lags else "")
        add("pass", "where the redo is mined", detail)
        if got.path == INSTANCE and logs:
            _dictionary_assess(eng, side, db, logs, add)
    except SystemExit as e:
        add("fail", "where the redo is mined", str(e))
    level, detail, _ = reader_check(eng, side, db)
    add(level, "reader rate", detail)
    return items
