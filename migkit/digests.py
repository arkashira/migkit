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
"""
import json
import threading
import time

FILE = "digests.jsonl"
_LOCK = threading.Lock()

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
        path = hop.report_dir() / FILE
        with _LOCK, open(path, "a") as f:
            f.write(json.dumps(line) + "\n")
    except Exception:  # noqa: BLE001 - the check's answer stands
        pass


def _count(rows):
    try:
        return int(rows)
    except (TypeError, ValueError):
        return None


def read(hop, session=None):
    """{(db, table, where): {"src": line, "dst": line}}: the latest line
    of each side, of the run `session` or of the hop's whole history."""
    return read_in(hop.report_dir(), session)


def read_in(where, session=None):
    """`read` of the hop whose reports are at `where`."""
    out = {}
    try:
        text = (where / FILE).read_text()
    except OSError:
        return out
    for raw in text.splitlines():
        try:
            line = json.loads(raw)
        except ValueError:
            continue
        if session and line.get("session") != session:
            continue
        key = (line.get("db", ""), line.get("table", ""),
               line.get("where", ""))
        out.setdefault(key, {})[line.get("side")] = line
    return out


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
