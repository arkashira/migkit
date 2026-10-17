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

A target without SQL holds the number the strongest way it has, chosen by
`decide.fence_way` from what the target is and what its account may make
(`Engine.fence_facts`), in a record of migkit's own placed in the target's
database or beside it (`decide.fence_place`): a document changed only
where its number still matches (MongoDB), a script that compares and sets
in one step (Redis), a lightweight transaction (Cassandra), the broker's
own producer epoch (Kafka: a producer under a lower epoch is fenced by the
broker), or - ClickHouse, which has no atomic compare - a replacing row
read whole, where the check is this process reading the target's highest
number, not the target refusing (`Way.target_refuses` False, said where it
is chosen). On those the hold is the target's atomic compare made at the
start of every batch the engine's own bulk path writes (`hold` with no
cursor), not inside the batch's own unit: a batch begun before a taker's
raise can still land, and only that one.
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
    # no shared row lock: the row taken for update, waited for {s} seconds
    # in the statement itself, which is where Oracle bounds a lock wait
    "oracle": {
        "share": "select n, seen from {t} where origin = :1"
                 " for update wait {s}",
        "raise": "declare c number; begin"
                 " select n into c from {t} where origin = :o1"
                 " for update wait {s};"
                 " if c < :n1 then update {t} set n = :n1, seen = :s1"
                 " where origin = :o1; end if;"
                 " exception when no_data_found then"
                 " insert into {t} (origin, n, seen) values (:o1, :n1, :s1);"
                 " end;",
        "read": "select n, seen from {t} where origin = :1",
        "wait": "begin null; end;",
        "delete": "delete from {t} where origin = :1",
    },
    "sqlserver": {
        "share": "select n, seen from {t} with (updlock, holdlock)"
                 " where origin = %s",
        "raise": "merge {t} with (holdlock) as o using (select %s as origin,"
                 " %s as n, %s as seen) as v on o.origin = v.origin"
                 " when matched and o.n < v.n then update set n = v.n,"
                 " seen = v.seen when not matched then insert (origin, n,"
                 " seen) values (v.origin, v.n, v.seen);",
        "read": "select n, seen from {t} where origin = %s",
        "wait": "set lock_timeout {ms}",
    },
}


def _sql(d, key, t, wait=None):
    """The dialect's statement `key` for table `t`, a lock waited `wait`
    seconds where the statement says its own wait."""
    wait = RAISE_WAIT if wait is None else wait
    return d.get(key, "delete from {t} where origin = %s").format(
        t=t, s=max(1, int(wait)), ms=int(wait * 1000))

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
    if args and args[0] in (1205, 1222):
        return True
    if args and getattr(args[0], "code", None) in (54, 30006):
        return True
    text = str(e).lower()
    return "lock timeout" in text or "lock wait timeout" in text


def own_table_sqlserver(engine, side, db):
    conn = engine._connect(side, db)
    t = (engine.own_name(side, db)
         if callable(getattr(engine, "own_name", None))
         else f"[{db}].[dbo].[migkit_origin]" if db else "dbo.migkit_origin")
    made = getattr(engine, "_own_made", None)
    if made is None:
        made = set()
        setattr(engine, "_own_made", made)
    if t not in made:
        cur = conn.cursor()
        try:
            cur.execute(
                f"if object_id('{t}', 'U') is null "
                f"create table {t} (origin nvarchar(400) not null primary key, "
                f"n bigint not null, seen nvarchar(max))")
            conn.commit()
            made.add(t)
        finally:
            cur.close()
    return conn, t


OWN_TABLE_HOOKS = {
    "sqlserver": own_table_sqlserver,
}


def _own_table(engine, side, db):
    hook = getattr(engine, "own_table", None)
    if callable(hook):
        return hook(side, db)
    d = getattr(engine, "OWN_DIALECT", None)
    if d in OWN_TABLE_HOOKS:
        return OWN_TABLE_HOOKS[d](engine, side, db)
    raise AttributeError(f"{type(engine).__name__} has no own_table")


def _dialect(engine):
    d = getattr(engine, "OWN_DIALECT", None)
    if d not in SQL:
        return None
    if not callable(getattr(engine, "own_table", None)) and d not in OWN_TABLE_HOOKS:
        return None
    return SQL[d]


def _no_way(engine, side, db):
    """Why the target holds no number, or None where it holds one (SQL, or
    a way of its own `way` opens)."""
    if _dialect(engine) is not None:
        return None
    got = way(engine, side, db)
    if isinstance(got, Way):
        return None
    return got or (f"{type(engine).__name__.removesuffix('Engine')} keeps"
                   " no table of migkit's to hold a writer to")


def read(engine, side, db, row):
    """(number, {"by", "store", "at"}) the target holds for `row`, or
    (None, {}) where it holds none."""
    d = _dialect(engine)
    if d is None:
        w = way(engine, side, db)
        return w.read(row) if isinstance(w, Way) else (None, {})
    conn, t = _own_table(engine, side, db)
    try:
        cur = conn.cursor()
        try:
            cur.execute(_sql(d, "read", t), (row,))
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
        w = way(engine, side, db)
        if not isinstance(w, Way):
            raise Fenced(w or f"{type(engine).__name__} keeps no number to"
                         " hold a writer to")
        return w.raise_(token.row, token.number, token.by, token.seen())
    said = False
    while True:
        conn, t = _own_table(engine, side, db)
        try:
            cur = conn.cursor()
            try:
                cur.execute(_sql(d, "wait", t, wait))
                cur.execute(_sql(d, "raise", t, wait),
                            (token.row, token.number, token.seen()))
                conn.commit()
                cur.execute(_sql(d, "read", t), (token.row,))
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
    if d is None or cur is None:
        _hold_own(engine, side, db, token, d, cur=cur)
        return
    name = (engine.own_name(side, db)
            if callable(getattr(engine, "own_name", None))
            else (f"[{db}].[dbo].[migkit_origin]" if d == "sqlserver" and db
                  else "dbo.migkit_origin" if d == "sqlserver"
                  else f"{db}.migkit_origin" if db else "migkit_origin"))
    if token.lazy and (side, db) not in token.raised:
        raise_once(engine, side, db, token)
    cur.execute(_sql(d, "share", name), (token.row,))
    got = cur.fetchone()
    n, seen = (int(got[0]), _seen(got[1])) if got else (None, {})
    if n != token.number or seen.get("by") != token.by:
        conn = getattr(cur, "connection", None)
        if conn is not None and callable(getattr(conn, "rollback", None)):
            try:
                conn.rollback()
            except Exception:
                pass
        raise Fenced("the target refused a write: " + _refused(token, n, seen)
                     + "; this transaction is rolled back and this writer"
                       " stops")


def _hold_own(engine, side, db, token, d, cur=None):
    """`hold` for a write with no transaction of migkit's to hold the row
    in - an engine's own bulk path, before each batch: the target's way
    compares, or the SQL row is held in a transaction of its own."""
    if token.lazy and (side, db) not in token.raised:
        raise_once(engine, side, db, token)
    if d is None:
        w = way(engine, side, db)
        if not isinstance(w, Way):
            raise Fenced((w or f"{type(engine).__name__} keeps no number to"
                          " hold this write to")
                         + ", and it carries a writer's token: nothing of"
                           " it is written")
        why = w.held(token.row, token.number, token.by, cur=cur)
    else:
        conn, t = _own_table(engine, side, db)
        try:
            cur = conn.cursor()
            cur.execute(_sql(d, "wait", t))
            cur.execute(_sql(d, "share", t), (token.row,))
            got = cur.fetchone()
            cur.close()
            conn.rollback()
        finally:
            conn.close()
        n, seen = (int(got[0]), _seen(got[1])) if got else (None, {})
        why = (None if n == token.number and seen.get("by") == token.by
               else _refused(token, n, seen))
    if why:
        raise Fenced("the target refused a write: " + why
                     + "; nothing of this batch is written and this writer"
                       " stops")


def prove(engine, side, db):
    """None where the target holds a writer to its number - a transaction
    holding the row keeps a raise out, and a hold after the raise reads
    the new number - else why not. The probe's row is taken away."""
    import secrets
    d = _dialect(engine)
    if d is None:
        why = _no_way(engine, side, db)
        return why if why else way(engine, side, db).prove(
            f"{engine.hop.name}#probe:{secrets.token_hex(6)}")
    row = f"{engine.hop.name}#probe:{secrets.token_hex(6)}"
    probe = Token(row, 1, "probe", "probe")
    conns = []

    def opened():
        conn, t = _own_table(engine, side, db)
        conns.append(conn)
        return conn, conn.cursor(), t
    try:
        n, _ = raise_to(engine, side, db, probe)
        if n != 1:
            return "a number raised was not the number read back"
        a, ca, t = opened()
        ca.execute(_sql(d, "share", t), (row,))
        got = ca.fetchone()
        if not got or int(got[0]) != 1:
            return "a number was not read back under a shared lock"
        b, cb, _ = opened()
        cb.execute(_sql(d, "wait", t, 0.3))
        try:
            cb.execute(_sql(d, "raise", t, 0.3), (row, 2, "{}"))
            b.commit()
            return ("a transaction holding its number under a shared lock"
                    " did not keep a newer number out: the target lets a"
                    " raise pass a writer that has not committed")
        except Exception as e:  # noqa: BLE001 - only a lock waited out proves
            if not _lock_wait(e):
                return f"the probe could not run: {_first(e)}"
            b.rollback()
        a.rollback()
        cb.execute(_sql(d, "wait", t))
        cb.execute(_sql(d, "raise", t), (row, 2, "{}"))
        b.commit()
        ca.execute(_sql(d, "share", t), (row,))
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
            conn, t = _own_table(engine, side, db)
            try:
                cur = conn.cursor()
                cur.execute(_sql(d, "delete", t), (row,))
                cur.close()
                conn.commit()
            finally:
                conn.close()
        except Exception:  # noqa: BLE001 - a probe's row left is harmless
            pass


# ---- a target without SQL: its own atomic way ------------------------------

#: the name migkit's own fence record goes under on a target without SQL
#: (a collection, a key prefix, a table, a topic)
OWN = "migkit_fence"
#: where it goes when the target's own database may not hold it
BESIDE = "migkit"

_WAYS = {}
_WAYS_LOCK = threading.Lock()


def way(engine, side, db):
    """The `Way` this target holds the number by, opened once per target
    database, or why it holds none: chosen by `decide.fence_way` and
    placed by `decide.fence_place` on the facts the engine reads of itself
    (`fence_facts`) - never a fixed default."""
    facts_of = getattr(engine, "fence_facts", None)
    if not callable(facts_of):
        return None
    key = (id(engine), side, str(db))
    with _WAYS_LOCK:
        if key in _WAYS:
            return _WAYS[key]
    from . import decide
    try:
        facts = dict(facts_of(side, db) or {})
    except Exception as e:  # noqa: BLE001 - a fact not read holds no rung
        got = f"its facts could not be read: {_first(e)}"
    else:
        place = decide.fence_place(facts)
        chosen = decide.fence_way(facts)
        if place.rung is None:
            got = f"no place for migkit's own record: {place.reason}"
        elif chosen.rung is None:
            got = f"no way to hold a number: {chosen.reason}"
        else:
            cls = WAY_CLASSES[chosen.path]
            got = cls(engine, side, db, place.path, chosen)
    with _WAYS_LOCK:
        return _WAYS.setdefault(key, got)


def forget(engine):
    """The ways opened for `engine` closed (its engine is going)."""
    with _WAYS_LOCK:
        for key in [k for k in _WAYS if k[0] == id(engine)]:
            got = _WAYS.pop(key)
            if isinstance(got, Way):
                got.close()


class Way:
    """How a target without SQL holds the number: `raise_` sets the record
    to a number where it holds a lower one and reads it back, `read` reads
    it, `held` is the target's compare of a writer's number and holder
    before a write - None where it holds, else why it refused - and
    `delete` takes a probe's record away. `target_refuses`: the refusal is
    the target's own atomic step, not this process reading and judging."""
    target_refuses = True

    def __init__(self, engine, side, db, place, choice):
        self.engine, self.side, self.db = engine, side, db
        self.place, self.choice = place, choice
        self.home = (engine.fence_home(side, db, place)
                     if callable(getattr(engine, "fence_home", None))
                     else (BESIDE if place == "beside" else db))

    def close(self):
        pass

    def _judge(self, row, n, by):
        got, seen = self.read(row)
        if got == n and seen.get("by") == by:
            return None
        return _refused(Token(row, n, by, ""), got, seen)

    def prove(self, row):
        """None where a holder's write under a lower number is refused by
        the target once a taker raised it, else why not."""
        try:
            first = self.raise_(row, 1, "probe-a", json.dumps({"by": "probe-a"}))
            if first[0] != 1:
                return "a number raised was not the number read back"
            if self.held(row, 1, "probe-a"):
                return "the holder of the number was refused its own write"
            got = self.raise_(row, 2, "probe-b", json.dumps({"by": "probe-b"}))
            if got[0] != 2:
                return "a taker's higher number was not taken"
            if self.raise_(row, 1, "probe-a",
                           json.dumps({"by": "probe-a"}))[0] != 2:
                return "a lower number was raised over a higher one"
            if not self.held(row, 1, "probe-a"):
                return ("a write under the lower number was let through"
                        " after a taker raised it")
            if self.held(row, 2, "probe-b"):
                return "the taker was refused its own write"
            return None
        except Exception as e:  # noqa: BLE001 - said as why it is not trusted
            return f"the probe could not run: {_first(e)}"
        finally:
            try:
                self.delete(row)
            except Exception:  # noqa: BLE001 - a probe's record left is harmless
                pass


class GuardedDocument(Way):
    """MongoDB: one document per row in migkit's own collection, raised by
    an update that matches only a lower number, and held by a find-and-
    modify that matches only the writer's own number and holder - the
    server's single-document atomicity is the compare."""

    def _coll(self):
        if not hasattr(self, "_c"):
            self._client = self.engine._client(self.side)
            self._c = self._client[self.home][OWN]
        return self._c

    def close(self):
        if hasattr(self, "_client"):
            self._client.close()

    def raise_(self, row, n, by, seen):
        from pymongo.errors import DuplicateKeyError
        try:
            self._coll().update_one(
                {"_id": row, "n": {"$lt": n}},
                {"$set": {"n": n, "by": by, "seen": seen}}, upsert=True)
        except DuplicateKeyError:
            pass        # the record holds this number or a higher one
        return self.read(row)

    def read(self, row):
        got = self._coll().find_one({"_id": row})
        return ((int(got["n"]), _seen(got.get("seen"))) if got
                else (None, {}))

    def held(self, row, n, by, cur=None):
        got = self._coll().find_one_and_update(
            {"_id": row, "n": n, "by": by}, {"$set": {"held": time.time()}})
        return None if got is not None else self._judge(row, n, by)

    def delete(self, row):
        self._coll().delete_one({"_id": row})


class ComparedKey(Way):
    """Redis: a hash per row under migkit's own key prefix, raised and held
    by scripts the server runs whole - nothing runs between their compare
    and their set."""
    RAISE = ("local c = redis.call('HGET', KEYS[1], 'n')"
             " if (not c) or tonumber(c) < tonumber(ARGV[1]) then"
             " redis.call('HSET', KEYS[1], 'n', ARGV[1], 'seen', ARGV[2],"
             " 'by', ARGV[3]) end"
             " return redis.call('HMGET', KEYS[1], 'n', 'seen')")
    HOLD = ("local v = redis.call('HMGET', KEYS[1], 'n', 'by')"
            " if v[1] ~= ARGV[1] or v[2] ~= ARGV[2] then"
            " return redis.error_reply('FENCED ' .. tostring(v[1])) end"
            " redis.call('HSET', KEYS[1], 'held', ARGV[3]) return 1")

    def _r(self):
        if not hasattr(self, "_c"):
            at = str(self.home)
            self._c = self.engine._client(self.side,
                                          int(at) if at.isdigit() else 0)
        return self._c

    def _key(self, row):
        return f"{OWN}:{row}"

    def raise_(self, row, n, by, seen):
        got = self._r().eval(self.RAISE, 1, self._key(row), n, seen, by)
        return int(got[0]), _seen(got[1])

    def read(self, row):
        got = self._r().hmget(self._key(row), "n", "seen")
        return (int(got[0]), _seen(got[1])) if got[0] is not None \
            else (None, {})

    def held(self, row, n, by, cur=None):
        if cur is not None and hasattr(cur, "watch"):
            try:
                cur.watch(self._key(row))
            except Exception:
                pass
        try:
            self._r().eval(self.HOLD, 1, self._key(row), n, by, time.time())
        except Exception as e:  # noqa: BLE001 - the script's refusal, or not
            if "FENCED" not in str(e):
                raise
            return self._judge(row, n, by)
        return None

    def delete(self, row):
        self._r().delete(self._key(row))


class LightweightTransaction(Way):
    """Cassandra: a row per fence row in migkit's own table, raised by a
    conditional update (`IF n < ?`) and held by one that applies only under
    the writer's own number and holder - Paxos at the replicas is the
    compare."""

    def _s(self):
        if not hasattr(self, "_t"):
            self._sess = self.engine._session(self.side)
            self._t = f'"{self.home}".{OWN}'
            self._sess.execute(
                f"create table if not exists {self._t} (origin text primary"
                " key, n bigint, seen text, holder text, held double)")
        return self._sess

    def raise_(self, row, n, by, seen):
        s = self._s()
        got = s.execute(f"insert into {self._t} (origin, n, seen, holder)"
                        " values (%s, %s, %s, %s) if not exists",
                        (row, n, seen, by))
        if not got.was_applied:
            s.execute(f"update {self._t} set n = %s, seen = %s, holder = %s"
                      " where origin = %s if n < %s", (n, seen, by, row, n))
        return self.read(row)

    def read(self, row):
        from cassandra import ConsistencyLevel
        from cassandra.query import SimpleStatement
        got = self._s().execute(SimpleStatement(
            f"select n, seen from {self._t} where origin = %s",
            consistency_level=ConsistencyLevel.SERIAL), (row,)).one()
        return (int(got[0]), _seen(got[1])) if got else (None, {})

    def held(self, row, n, by, cur=None):
        got = self._s().execute(
            f"update {self._t} set held = %s where origin = %s"
            " if n = %s and holder = %s", (time.time(), row, n, by))
        return None if got.was_applied else self._judge(row, n, by)

    def delete(self, row):
        self._s().execute(f"delete from {self._t} where origin = %s",
                          (row,))


class ProducerEpoch(Way):
    """Kafka: a transactional producer per fence row, its id the row's, so
    a taker's start bumps the broker's epoch for the id and the broker
    fences every producer under the lower one; the number and who raised
    it ride in a committed record of migkit's own topic, read back by a
    committed-only read. A raise first reads the record and starts no
    producer over a higher number - the broker would fence its holder."""

    def __init__(self, *a):
        super().__init__(*a)
        self._held = {}

    def _tid(self, row):
        return f"{OWN}:{row}"

    def close(self):
        for p in self._held.values():
            try:
                p.close()
            except Exception:  # noqa: BLE001 - it is going anyway
                pass
        self._held.clear()

    def _write(self, p, row, n, by, seen):
        p.begin_transaction()
        p.send(OWN, key=row.encode(), value=json.dumps(
            {"n": n, "by": by, "seen": seen}).encode())
        p.commit_transaction()

    def raise_(self, row, n, by, seen):
        got, _ = self.read(row)
        if got is not None and got >= n:
            return self.read(row)
        p = self.engine._producer(transactional_id=self._tid(row))
        p.init_transactions()
        self._write(p, row, n, by, seen)
        old = self._held.pop((row, n, by), None)
        if old is not None:
            old.close()
        self._held[(row, n, by)] = p
        return self.read(row)

    def read(self, row):
        from kafka import TopicPartition
        c = self.engine._consumer(self.side, isolation_level="read_committed",
                                  consumer_timeout_ms=3000)
        try:
            if OWN not in (c.topics() or ()):
                return None, {}
            tp = TopicPartition(OWN, 0)
            c.assign([tp])
            c.seek_to_beginning(tp)
            end = c.end_offsets([tp])[tp]
            last, empty = None, 0
            # the partition read to its end; polls that bring nothing are
            # bounded, so a broker that stops answering is an error, not a
            # read taken as whole
            while c.position(tp) < end:
                got = c.poll(timeout_ms=2000)
                empty = 0 if got else empty + 1
                if empty > 5:
                    raise Fenced(f"{OWN} could not be read to its end")
                for batch in got.values():
                    for m in batch:
                        if m.key == row.encode():
                            last = json.loads(m.value)
        finally:
            c.close()
        if last is None:
            return None, {}
        return int(last["n"]), _seen(last.get("seen"))

    def held(self, row, n, by, cur=None):
        p = self._held.get((row, n, by))
        if p is None:
            return self._judge(row, n, by) or (
                "this process started no producer under the number")
        try:
            self._write(p, row, n, by, json.dumps({"by": by}))
        except Exception as e:  # noqa: BLE001 - the broker's fence, or not
            text = str(e).lower()
            if "fenc" not in text and "epoch" not in text:
                raise
            return ("the broker fenced this producer: a newer holder"
                    " started one under the same id")
        return None

    def delete(self, row):
        for k in [k for k in self._held if k[0] == row]:
            self._held.pop(k).close()


class ReplacingRow(Way):
    """ClickHouse: no atomic compare - a row per raise in migkit's own
    ReplacingMergeTree, versioned by the number, read whole (`final`) so
    the highest number is what a read sees. The hold is this process
    reading that number before each batch: a weaker guarantee than the
    others, said where it is chosen."""
    target_refuses = False

    def _c(self):
        if not hasattr(self, "_cl"):
            self._cl = self.engine._client(self.side)
            self._t = f"`{self.home}`.{OWN}"
            self._cl.command(
                f"create table if not exists {self._t} (origin String,"
                " n UInt64, seen String, holder String)"
                " engine = ReplacingMergeTree(n) order by origin")
        return self._cl

    def close(self):
        if hasattr(self, "_cl"):
            self._cl.close()

    def raise_(self, row, n, by, seen):
        got, _ = self.read(row)
        if got is None or got < n:
            self._c().insert(self._t.replace("`", ""), [[row, n, seen, by]],
                             column_names=["origin", "n", "seen", "holder"])
        return self.read(row)

    def read(self, row):
        got = self._c().query(
            f"select n, seen from {self._t} final where origin ="
            " {o:String} order by n desc limit 1",
            parameters={"o": row}).result_rows
        return (int(got[0][0]), _seen(got[0][1])) if got else (None, {})

    def held(self, row, n, by, cur=None):
        return self._judge(row, n, by)

    def delete(self, row):
        self._c().command(f"delete from {self._t} where origin ="
                          " {o:String}", parameters={"o": row})


#: `decide.fence_way`'s rungs, by name, to the way that holds by them
WAY_CLASSES = {"guarded-document": GuardedDocument,
               "compared-key": ComparedKey,
               "lightweight-transaction": LightweightTransaction,
               "producer-epoch": ProducerEpoch,
               "replacing-row": ReplacingRow}
