"""Every table's digest as a check took it, kept so anyone can take it
again (backlog R23.1).

A verdict says `same`. What stands behind it is, per table and side, the
rows counted and the digest summed - and exactly how each was taken: the
statement the server ran where the server summed it, the salt drawn for
the run (`checkpoint.new_salt`), the rendering of every value (`canon`)
where the two sides are two engines. A check records each here as it
takes it (`record`), a line a table and side stamped with the run
(`session`); the evidence export (`export`) writes them beside the
verdict with the way to take them again: the statement run on each
server gives the same two numbers, and `migkit check` run again draws a
new salt and gives the same answer where nothing changed.

Only numbers are kept - counts, sums of hashes - and the statements,
which name columns and never hold a value; a row filter is in them as
the hop wrote it.

The two sides of a table are paired within one run (`read_in`): the salt
is drawn per run, so a side from one run beside the other side from
another always reads "different". Measured before: a check killed
mid-table, a side whose read failed (only the side that worked is
recorded), a `--table` run - each left the latest line of each side from
two runs, `migkit_table_same` said 0 and the evidence contradicted its
own verdict. A side its run has no partner for is one side only. A
digest that could not be written is counted (`unwritten`), and the
evidence and the run log say how many.
"""
import json
import threading
import time

FILE = "digests.jsonl"
_LOCK = threading.Lock()
#: {run: digests that could not be written}
_UNWRITTEN = {}

#: the ways a digest is taken, as an engine names the one it used
OWN = "own"
NEUTRAL = "neutral"
READ = "read"


def methods():
    """{method: what it is, in words}, the numbers in them read from the
    code that takes the digests."""
    from . import canon, rowtext
    return {
        OWN: ("the same engine on both sides: each server counts the rows"
              " and sums, as an exact number, the first 64 bits of"
              " md5(the run's salt followed by the row's own text), the"
              " row's columns named in name order; the statement each"
              " server ran is kept beside its numbers, the salt in it"),
        NEUTRAL: ("one rendering on both engines: every value rendered as"
                  " its class says (below), the row's values joined each"
                  f" with its length before it and `{rowtext.SEP}` between"
                  f" them, a NULL marked by a length of"
                  f" `{rowtext.NULL_LEN}`; md5 of the row's UTF-8 bytes,"
                  f" its first {canon.DIGEST_HEX} hex digits read as a"
                  " number and summed exactly over the rows, beside the"
                  " row count"),
        READ: ("the rows read in key order and each row's text hashed as"
               " it came, in migkit's own process; `migkit check` takes"
               " it again"),
    }


def _plain(v):
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    return str(v)


def record(hop, db, table, side, rows, digest, *, method, keys=None,
           sql=None, salt=None, engine=None, columns=None, where=None):
    """One side's digest of one table, as the check took it: `rows`
    counted and `digest` summed by `method`, with the key digest where
    one rode along, the statement run where the server summed, the
    run's salt, the engine, the columns as (name, class) where values
    were rendered one way on both sides, and the row filter or range the
    digest was taken over. Never raises: a record that cannot be written
    costs the export this line, not the check its answer."""
    from . import session
    line = {"session": session.current(), "at": time.strftime("%F %T"),
            "db": str(db), "table": str(table), "side": side,
            "rows": _count(rows), "digest": str(digest), "method": method}
    for k, v in (("keys", keys), ("sql", sql), ("salt", salt),
                 ("engine", engine), ("columns", columns),
                 ("where", where)):
        if v not in (None, "", [], ()):
            line[k] = _plain(v)
    try:
        from . import durable
        path = hop.report_dir() / FILE
        with _LOCK:
            durable.append_line(path, json.dumps(line), sync=False)
    except Exception as e:  # noqa: BLE001 - the check's answer stands
        from . import runlog
        with _LOCK:
            _UNWRITTEN[line["session"]] = \
                _UNWRITTEN.get(line["session"], 0) + 1
        runlog.event("digest-unwritten", f"the digest of {side} could not"
                     f" be kept ({type(e).__name__}); the evidence lists"
                     " this table without it", level="warn", db=str(db),
                     table=str(table))


def unwritten(session):
    """How many digests the run `session` took and could not write."""
    with _LOCK:
        return _UNWRITTEN.get(session, 0)


def _count(rows):
    try:
        return int(rows)
    except (TypeError, ValueError):
        return None


def read(hop, session=None, left=None):
    """{(db, table, where): {"src": line, "dst": line}}: each table's
    sides as one run took them - the run `session`, or the last run that
    took the table - the latest line of each side in it. A side with no
    partner in its run is there alone. Only the tables the hop takes now
    (`taken`): a table it has since left out was listed with its old
    answer beside the verdict on what it takes; `left`, where given, gets
    each (db, table) left out."""
    if session:
        from . import runlog
        got = runlog.appended(hop, session, FILE)
        if got is not None:
            return _within(hop, _paired(got, session), left)
    return read_in(hop.report_dir(), session, hop, left)


def taken(hop, db, table, engine=None, side="src"):
    """Whether the hop takes, now, the table a digest's line names - as
    `engine` names it (`util.split_table`), the source's where none is
    said. On the target's side, a name the hop renames a table to is
    that table's, and is kept."""
    from .util import split_table
    names = getattr(hop, "_names_engine", None)
    eng = engine or (names() if callable(names) else hop.engine)
    schema, name = split_table(eng, str(table))
    parts = [schema] if schema else []
    if side == "dst" and hasattr(hop, "source_table_in"):
        # the target's name read back to the source's: a table renamed on
        # the way is asked as the table it was, in its own database
        back = hop.source_table_in(db, *parts, name)
        if back is None:
            return False
        return not hop.excluded(db, *back)
    return not hop.excluded(db, *parts, name)


def _within(hop, paired, left=None):
    """`paired` with only the tables the hop takes now (`taken`), each
    asked by its source's line where there is one."""
    if hop is None or not hasattr(hop, "excluded"):
        return paired
    out = {}
    for key, sides in paired.items():
        db, table, _ = key
        side = "src" if sides.get("src") else "dst"
        line = sides.get(side) or {}
        if taken(hop, db, table, line.get("engine"), side):
            out[key] = sides
        elif left is not None and (db, table) not in left:
            left.append((db, table))
    return out


def read_in(where, session=None, hop=None, left=None):
    """`read` of the hop whose reports are at `where`, every part of the
    file read (`runlog.parts`); of the tables `hop` takes now, where it
    is given."""
    from . import runlog
    lines = []
    for part in runlog.parts(where / FILE):
        try:
            text = part.read_text()
        except OSError:
            continue
        for raw in text.splitlines():
            try:
                lines.append(json.loads(raw))
            except ValueError:
                continue
    return _within(hop, _paired(lines, session), left)


def _paired(lines, session=None):
    last, out = {}, {}
    for line in lines:
        if not isinstance(line, dict):
            continue
        if session and line.get("session") != session:
            continue
        key = (line.get("db", ""), line.get("table", ""),
               line.get("where", ""))
        last[key] = line.get("session")
        out.setdefault(key, []).append(line)
    paired = {}
    for key, got in out.items():
        sides = {}
        for line in got:
            if line.get("session") == last[key]:
                sides[line.get("side")] = line
        paired[key] = sides
    return paired


def statement(line):
    """The statement that takes this side's digest again, or None where
    no server sums it (the values are folded in migkit's process, or the
    engine has no rendering of its own in SQL). A line that kept the
    statement it ran is that statement; a neutral one is built from its
    columns by the same functions the check built it with."""
    if line.get("sql"):
        return line["sql"]
    if line.get("method") != NEUTRAL or not line.get("columns"):
        return None
    from . import canon
    engine = line.get("engine")
    cols = [tuple(c) for c in line["columns"]]
    # an engine summed in this process has no statement a server runs
    # (SQLite's aggregate is migkit's own)
    if engine in canon.IN_PROCESS or any(canon.folded_here(c)
                                         for _, c in cols):
        return None
    try:
        digest = canon.digest_expr(engine, canon.row_expr(engine, cols))
    except ValueError:
        return None
    where = line.get("where")
    return (f"select count(*), {digest} from"
            f" {_quoted(engine, line['table'])}"
            + (f" where {where}" if where else ""))


def _quoted(engine, table):
    q = "`" if engine == "mysql" else '"'
    return ".".join(f"{q}{part.replace(q, q + q)}{q}"
                    for part in str(table).split("."))


def recompute(line):
    """How to take this side's digest again, in words."""
    sql = statement(line)
    if sql:
        return ("run the statement on this side; it answers the rows and"
                " the digest above")
    if line.get("method") == NEUTRAL:
        return ("read the rows, render each value as its class says, join"
                " and hash each row as the method says, and sum; or run"
                " `migkit check` again")
    return "run `migkit check` again"
