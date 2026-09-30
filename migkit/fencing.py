"""A mover's token, carried into every transaction it writes, so that a
mover which lost its hold is refused by the target itself (backlog W1).

A lease says who may write; it cannot stop one that no longer holds it
from writing anyway - a machine that froze for longer than its term and
woke with a transaction open, a batch read and a heartbeat that has not
yet noticed. So each lease a writer holds carries a number, one higher
every time it is taken (`lease.Lease.fence`, a range's claim in
`claims`), and the target keeps the highest number any holder raised it
to: a row of `migkit_origin` - the table the tail's batch marks already
keep - for the hop, for a table leased whole, and for each range of a
table machines share.

    raise   before a new holder reads what the last one saved, or writes
            anything: the row set to its number where it is lower - an
            update, which waits for every transaction holding the row, so
            a batch the last holder had in flight is committed before it
            or refused after it
    hold    in every transaction a holder writes, last before its commit:
            the row read under a shared lock and held to its number - a
            transaction under a lower number is refused there and rolled
            back, and until it commits no raise can pass it

The check and the commit are one transaction under the target's own
lock, so no newer holder can raise the row between them. Held last, so a
transaction a writer rolls back part of and sends again (a load taken
with warnings) is held as it commits, not as it began.

The row keeps who raised it and through which store their lease is kept
(`shared`): a machine whose hop file names another store - whose lease
was never in the same place - is refused rather than taken for the same
hop's.

Proved on each target before it is trusted (`prove`): a transaction
holding a probe's row under a shared lock must keep a raise out until it
ends, and a hold under the lower number must then read the higher one. A
target where either does not happen - a shared lock that is a no-op, as
some servers speaking another's wire keep it - is said, and no second
machine is run over it: a standby needs a target that can refuse the one
that lost.
"""
import dataclasses
import json
import threading
import time


class Fenced(SystemExit):
    """The target refused a write: a newer holder raised its number. What
    the transaction held was rolled back."""


#: the statements, by the dialect an engine speaks (`Engine.OWN_DIALECT`);
#: `{t}` is `migkit_origin`'s name there (`Engine.own_name`)
SQL = {
    "postgres": {
        "share": "select n, seen from {t} where origin = %s for share",
        "raise": "insert into {t} as o (origin, n, seen) values (%s, %s, %s)"
                 " on conflict (origin) do update set n = excluded.n,"
                 " seen = excluded.seen where o.n < excluded.n",
        "read": "select n, seen from {t} where origin = %s",
        "wait": "set lock_timeout = '{ms}ms'",
    },
    "mysql": {
        "share": "select n, seen from {t} where origin = %s"
                 " lock in share mode",
        # `seen` first: an assignment reads the ones before it as set
        "raise": "insert into {t} (origin, n, seen) values (%s, %s, %s)"
                 " on duplicate key update seen = if(n < values(n),"
                 " values(seen), seen), n = greatest(n, values(n))",
        "read": "select n, seen from {t} where origin = %s",
        "wait": "set session innodb_lock_wait_timeout = {s}",
    },
}

#: seconds a raise waits for a transaction holding the row before it says
#: so and waits again
RAISE_WAIT = 30


@dataclasses.dataclass
class Token:
    """What one holding writes under: the row it is held to, its number,
    who holds it and through which store. `lease`, where set, is checked
    before every hold (`lease.Lease.check`); `lazy` raises the row at the
    first hold in each database instead of before (a table or a range,
    whose new holder empties or rewrites what the last one could have
    written first); `before(db)` runs ahead of each hold - the leader's
    saved position put where the standby reads it (`ha`)."""
    row: str
    number: int
    by: str
    store: str
    lease: object = None
    lazy: bool = False
    before: object = None
    raised: set = dataclasses.field(default_factory=set)

    def seen(self):
        return json.dumps({"by": self.by, "store": self.store,
                           "at": time.time()})

    def plain(self):
        """What a process copying one range is handed (`claims`)."""
        return {"row": self.row, "number": self.number, "by": self.by,
                "store": self.store, "lazy": self.lazy}


def token_of(plain):
    return Token(**plain) if plain else None


_ACTIVE = {}
_LOCK = threading.Lock()


def arm(hop, token):
    """Every write this process makes on the hop's target from now held to
    `token` - through every engine made for the hop, a pair's included,
    which is why it is kept by the hop's name."""
    with _LOCK:
        _ACTIVE[hop.name] = token
    return token


def disarm(hop, token=None):
    with _LOCK:
        if token is None or _ACTIVE.get(hop.name) is token:
            _ACTIVE.pop(hop.name, None)


def armed(hop):
    with _LOCK:
        return _ACTIVE.get(getattr(hop, "name", None))


def marked(hop):
    """What a batch's mark on the target says of its writer (`twoway`'s
    `_batch_seen`): the number it was written under, where one is armed -
    so the target's own mark names the holding that committed its last
    batch (`ha.one_writer`)."""
    token = armed(hop)
    return {"fence": token.number} if token is not None else {}


def _seen(text):
    try:
        got = json.loads(text) if text else {}
    except (TypeError, ValueError):
        got = {}
    return got if isinstance(got, dict) else {}


def _first(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[0][:200]


def _lock_wait(e):
    """Whether `e` is a lock not granted in time: PostgreSQL's
    lock_not_available, MySQL's 1205."""
    code = getattr(e, "pgcode", None) or getattr(
        getattr(e, "diag", None), "sqlstate", None)
    if code == "55P03":
        return True
    args = getattr(e, "args", ())
    if args and args[0] == 1205:
        return True
    text = str(e).lower()
    return "lock timeout" in text or "lock wait timeout" in text


def _dialect(engine):
    d = getattr(engine, "OWN_DIALECT", None)
    if d not in SQL or not callable(getattr(engine, "own_table", None)):
        return None
    return SQL[d]


def read(engine, side, db, row):
    """(number, {"by", "store", "at"}) the target holds for `row`, or
    (None, {}) where it holds none."""
    d = _dialect(engine)
    if d is None:
        return None, {}
    conn, t = engine.own_table(side, db)
    try:
        cur = conn.cursor()
        try:
            cur.execute(d["read"].format(t=t), (row,))
            got = cur.fetchone()
        finally:
            cur.close()
        conn.rollback()
    finally:
        conn.close()
    return (int(got[0]), _seen(got[1])) if got else (None, {})


def raise_to(engine, side, db, token, log=None, wait=RAISE_WAIT):
    """The row raised to `token`'s number where the target holds a lower
    one, in a transaction of its own; (the number the target holds after,
    who raised it). Waits for every transaction holding the row - a batch
    the last holder has in flight commits or is refused first - and says
    so where it waits long."""
    d = _dialect(engine)
    if d is None:
        raise Fenced(f"{type(engine).__name__} keeps no number to hold a"
                     " writer to")
    said = False
    while True:
        conn, t = engine.own_table(side, db)
        try:
            cur = conn.cursor()
            try:
                cur.execute(d["wait"].format(s=int(wait),
                                             ms=int(wait * 1000)))
                cur.execute(d["raise"].format(t=t),
                            (token.row, token.number, token.seen()))
                conn.commit()
                cur.execute(d["read"].format(t=t), (token.row,))
                got = cur.fetchone()
                conn.commit()
            finally:
                cur.close()
        except Exception as e:  # noqa: BLE001 - a lock waited out, again
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - it is closed below
                pass
            if not _lock_wait(e):
                raise
            if log and not said:
                log("waiting for a transaction the last holder left open"
                    " on the target: it commits or is refused before this"
                    " machine goes on")
                said = True
            continue
        finally:
            conn.close()
        return int(got[0]), _seen(got[1])


def _refused(token, n, seen):
    who = seen.get("by") or "another machine"
    if n is None:
        return (f"the target holds no number for {token.row}: it was never"
                " raised for this writer")
    if n > token.number:
        return (f"a newer holder ({who}) raised the target's number to {n},"
                f" past this writer's {token.number}")
    if n < token.number:
        return (f"the target holds {n}, below this writer's"
                f" {token.number}: it was never raised for it")
    if seen.get("store") and seen.get("store") != token.store:
        return (f"{who} holds the same number through another store"
                f" ({seen.get('store')}, this machine's is {token.store}):"
                " every machine of a hop must name the same one")
    return f"{who} holds the same number ({n})"


def raise_once(engine, side, db, token, log=None):
    """`raise_to` for a holder that must hold the row after it: `Fenced`
    where a newer one, or another, holds it."""
    n, seen = raise_to(engine, side, db, token, log)
    if n != token.number or seen.get("by") != token.by:
        raise Fenced(f"this writer lost {token.row}: "
                     + _refused(token, n, seen)
                     + "; nothing more of it is written")
    token.raised.add((side, db))
    return n


def hold(engine, side, db, cur):
    """In the transaction `cur` is writing, last before its commit: the
    armed token's row read under a shared lock and held to its number
    (the module's words). Nothing where no token is armed for the hop, or
    the write is not to the target."""
    if side != "dst":
        return
    token = armed(engine.hop)
    if token is None:
        return
    if token.lease is not None:
        token.lease.check()
    if token.before is not None:
        token.before(db)
    d = _dialect(engine)
    if d is None:
        raise Fenced(f"{type(engine).__name__} keeps no number to hold this"
                     " write to, and it carries a writer's token: nothing"
                     " of it is committed")
    name = engine.own_name(side, db)
    if token.lazy and (side, db) not in token.raised:
        raise_once(engine, side, db, token)
    cur.execute(d["share"].format(t=name), (token.row,))
    got = cur.fetchone()
    n, seen = (int(got[0]), _seen(got[1])) if got else (None, {})
    if n != token.number or seen.get("by") != token.by:
        raise Fenced("the target refused a write: " + _refused(token, n, seen)
                     + "; this transaction is rolled back and this writer"
                       " stops")


def prove(engine, side, db):
    """None where the target holds a writer to its number - a transaction
    holding the row keeps a raise out, and a hold after the raise reads
    the new number - else why not. The probe's row is taken away."""
    import secrets
    d = _dialect(engine)
    if d is None:
        return (f"{type(engine).__name__.removesuffix('Engine')} keeps no"
                " table of migkit's to hold a writer to")
    row = f"{engine.hop.name}#probe:{secrets.token_hex(6)}"
    probe = Token(row, 1, "probe", "probe")
    conns = []

    def opened():
        conn, t = engine.own_table(side, db)
        conns.append(conn)
        return conn, conn.cursor(), t
    try:
        n, _ = raise_to(engine, side, db, probe)
        if n != 1:
            return "a number raised was not the number read back"
        a, ca, t = opened()
        ca.execute(d["share"].format(t=t), (row,))
        got = ca.fetchone()
        if not got or int(got[0]) != 1:
            return "a number was not read back under a shared lock"
        b, cb, _ = opened()
        cb.execute(d["wait"].format(s=1, ms=300))
        try:
            cb.execute(d["raise"].format(t=t), (row, 2, "{}"))
            b.commit()
            return ("a transaction holding its number under a shared lock"
                    " did not keep a newer number out: the target lets a"
                    " raise pass a writer that has not committed")
        except Exception as e:  # noqa: BLE001 - only a lock waited out proves
            if not _lock_wait(e):
                return f"the probe could not run: {_first(e)}"
            b.rollback()
        a.rollback()
        cb.execute(d["wait"].format(s=RAISE_WAIT, ms=RAISE_WAIT * 1000))
        cb.execute(d["raise"].format(t=t), (row, 2, "{}"))
        b.commit()
        ca.execute(d["share"].format(t=t), (row,))
        got = ca.fetchone()
        a.rollback()
        if not got or int(got[0]) != 2:
            return ("the number raised was not what the next writer read:"
                    " one under the old number would have written on")
        return None
    except Exception as e:  # noqa: BLE001 - said as why it is not trusted
        return f"the probe could not run: {_first(e)}"
    finally:
        for conn in conns:
            try:
                conn.rollback()
                conn.close()
            except Exception:  # noqa: BLE001 - it is going anyway
                pass
        try:
            conn, t = engine.own_table(side, db)
            try:
                cur = conn.cursor()
                cur.execute(f"delete from {t} where origin = %s", (row,))
                cur.close()
                conn.commit()
            finally:
                conn.close()
        except Exception:  # noqa: BLE001 - a probe's row left is harmless
            pass
