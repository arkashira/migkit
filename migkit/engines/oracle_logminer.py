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
    if short:
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
        self.samples = _SAMPLES.setdefault(
            (id(eng.hop), side, db), [])
        self.shapes = {}

    # -- the session

    def _open(self):
        """The mining session: on the source itself; from its container's
        root where `mine_service` names it (a database before 21c mines
        only there); or on a physical standby where `mine_standby` names
        one, so the source does no mining at all - gated (`standby_gate`)
        and not run here: this machine's sandbox has no Data Guard."""
        if self.conn is not None:
            return self.conn
        ep = self.eng._endpoint(self.side)
        standby = ep.options.get("mine_standby")
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
        the window are named to LogMiner, the archived ones first."""
        cur = self.conn.cursor()
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
        self.conn.cursor().execute(
            "begin dbms_logmnr.start_logmnr(startscn => :1, endscn => :2,"
            " options => dbms_logmnr.dict_from_online_catalog +"
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
        if self.root:
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
        self._start(start, max(end, start))
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

    def read(self, token, limit=1000):
        """(changes, token): whole transactions until about `limit`
        changes, in commit order; a transaction larger than the reader
        holds is handed back in parts, the part said in the token so a
        read that starts over leaves out what went."""
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
            if op == DDL or self.eng.hop.excluded(self.db, table):
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
        rate = redo_rate(eng, side, db)
        mined = reader_rate(eng, side, db)
    except SystemExit as e:
        add("warn", "reader rate", f"not measured: {e}")
        return items
    except Exception as e:  # noqa: BLE001 - said
        add("warn", "reader rate", "not measured:"
            f" {str(e).splitlines()[0][:90]}")
        return items
    if mined is None or rate is None:
        add("warn", "reader rate", "not measured: the source has archived"
            " no redo in the last day, so neither its rate nor the"
            " reader's could be read")
        return items
    peak, avg = rate
    speed, _ = mined
    words = (f"the source archived {peak / 2 ** 20:.2f} MB of redo a second"
             f" at its busiest hour ({avg / 2 ** 20:.2f} on average); the"
             f" reader mined {speed / 2 ** 20:.2f} MB a second")
    if speed < peak:
        add("fail", "reader rate", words + " - it would fall behind at the"
            " busiest hour. The redo-file reader, run next to the redo,"
            " reads without the database's help")
    elif speed < 2 * peak:
        add("warn", "reader rate", words + " - under twice the peak, so a"
            " busier hour than the last day's would put it behind")
    else:
        add("pass", "reader rate", words)
    return items
