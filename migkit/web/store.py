"""What the view keeps between requests and across a restart: sessions,
API tokens, the sign-ins under way, the local account, the throttle on
wrong passwords, and the people and groups an identity provider
provisions (`scim`).

One SQLite file in the view's private directory (0700, the file 0600),
written in full-sync WAL mode - a session cut off or a person
deprovisioned is on the disk when the answer goes out. SQLite rather than
the JSON files `durable` writes, because the provisioned directory is
searched by the filters a provider sends (`userName eq "..."`), turned
into SQL by the filter library, and a group's members change inside one
transaction with the group.

Nothing that opens a session is kept as it is given: a session's cookie,
an API token and a sign-in's browser value are kept as their SHA-256, so
the file read by someone else opens nothing.
"""
import contextlib
import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
create table if not exists sessions (
  id_hash text primary key, principal text not null, csrf text not null,
  created real not null, seen real not null, expires real not null,
  prefs text not null default '{}');
create table if not exists tokens (
  id_hash text primary key, id text unique not null, name text not null,
  principal text not null, created real not null, expires real not null,
  used real);
create table if not exists pending (
  key_hash text primary key, kind text not null, data text not null,
  expires real not null);
create table if not exists seen (
  kind text not null, id text not null, expires real not null,
  primary key (kind, id));
create table if not exists local (
  name text primary key, hash text not null, created real not null,
  changed real not null);
create table if not exists throttle (
  key text primary key, failures integer not null, first real not null,
  until real not null);
create table if not exists users (
  id text primary key, user_name text unique not null collate nocase,
  external_id text, display_name text collate nocase,
  email text collate nocase, active integer not null default 1,
  created text not null, modified text not null,
  version integer not null default 1, raw text not null default '{}');
create table if not exists groups (
  id text primary key, display_name text unique not null collate nocase,
  external_id text, created text not null, modified text not null,
  version integer not null default 1);
create table if not exists members (
  group_id text not null references groups(id) on delete cascade,
  user_id text not null references users(id) on delete cascade,
  primary key (group_id, user_id));
"""

#: wrong passwords in a row before sign-in waits, and the longest wait
FAILURES_FREE = 5
WAIT_MOST = 900.0


def digest(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def _now():
    return time.time()


class Store:
    """The view's file, opened once and shared by the server's threads
    under one lock."""

    def __init__(self, path):
        from .. import durable
        path = Path(path)
        durable.private_dir(path.parent)
        if not path.exists():
            os.close(os.open(str(path), os.O_WRONLY | os.O_CREAT, 0o600))
        self.path = path
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False,
                                  isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("pragma journal_mode=wal")
        self.db.execute("pragma synchronous=full")
        self.db.execute("pragma foreign_keys=on")
        self.db.executescript(SCHEMA)

    @contextlib.contextmanager
    def tx(self):
        """One transaction, whole or not at all."""
        with self.lock:
            self.db.execute("begin immediate")
            try:
                yield self.db
            except BaseException:
                self.db.execute("rollback")
                raise
            self.db.execute("commit")

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.db.execute(sql, args).fetchall()]

    def close(self):
        with self.lock:
            self.db.close()

    # ---- sessions -------------------------------------------------------

    def open_session(self, principal, hours, until=None):
        """A new session for `principal` (a dict); (cookie value, CSRF
        token). `until` caps it where the provider said the sign-in ends
        sooner."""
        value, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        now = _now()
        ends = now + hours * 3600
        if until:
            ends = min(ends, float(until))
        with self.tx() as db:
            db.execute("delete from sessions where expires < ?", (now,))
            db.execute("insert into sessions values (?,?,?,?,?,?,?)",
                       (digest(value), json.dumps(principal), csrf, now, now,
                        ends, "{}"))
        return value, csrf

    def session(self, value, idle_minutes):
        """(principal, csrf, prefs) of a live session, touched; None for
        one that ended, idled out or never was."""
        if not value:
            return None
        now = _now()
        key = digest(value)
        with self.tx() as db:
            got = db.execute("select * from sessions where id_hash = ?",
                             (key,)).fetchone()
            if got is None:
                return None
            if got["expires"] < now or now - got["seen"] > idle_minutes * 60:
                db.execute("delete from sessions where id_hash = ?", (key,))
                return None
            db.execute("update sessions set seen = ? where id_hash = ?",
                       (now, key))
        return (json.loads(got["principal"]), got["csrf"],
                json.loads(got["prefs"] or "{}"))

    def set_prefs(self, value, prefs):
        with self.tx() as db:
            db.execute("update sessions set prefs = ? where id_hash = ?",
                       (json.dumps(prefs), digest(value)))

    def close_session(self, value):
        with self.tx() as db:
            db.execute("delete from sessions where id_hash = ?",
                       (digest(value),))

    def close_sessions_of(self, principal_id):
        """Every session and API token of a person, ended: deprovisioned,
        they open nothing from the next request on. How many."""
        pid = str(principal_id).lower()
        n = 0
        with self.tx() as db:
            for table in ("sessions", "tokens"):
                for r in db.execute(f"select id_hash, principal from {table}"
                                    ).fetchall():
                    if str(json.loads(r["principal"]).get("id", "")
                           ).lower() == pid:
                        db.execute(f"delete from {table} where id_hash = ?",
                                   (r["id_hash"],))
                        n += 1
        return n

    def sessions(self):
        now = _now()
        return [{"principal": json.loads(r["principal"]).get("id"),
                 "via": json.loads(r["principal"]).get("via"),
                 "created": r["created"], "seen": r["seen"],
                 "expires": r["expires"]}
                for r in self.rows("select * from sessions where expires > ?"
                                   " order by seen desc", (now,))]

    # ---- API tokens -----------------------------------------------------

    def new_token(self, principal, name, days):
        """A token for the API, standing for `principal` until `days` are
        up; (its id, the token - shown once, kept only as its digest)."""
        value = "mkt_" + secrets.token_urlsafe(32)
        tid = secrets.token_hex(6)
        now = _now()
        with self.tx() as db:
            db.execute("insert into tokens values (?,?,?,?,?,?,?)",
                       (digest(value), tid, str(name)[:80],
                        json.dumps(principal), now, now + days * 86400,
                        None))
        return tid, value

    def token(self, value):
        """The principal a live token stands for, or None."""
        if not value or not str(value).startswith("mkt_"):
            return None
        now = _now()
        key = digest(value)
        with self.tx() as db:
            got = db.execute("select * from tokens where id_hash = ?",
                             (key,)).fetchone()
            if got is None:
                return None
            if got["expires"] < now:
                db.execute("delete from tokens where id_hash = ?", (key,))
                return None
            db.execute("update tokens set used = ? where id_hash = ?",
                       (now, key))
        return json.loads(got["principal"])

    def tokens_of(self, principal_id):
        pid = str(principal_id).lower()
        return [{"id": r["id"], "name": r["name"], "created": r["created"],
                 "expires": r["expires"], "used": r["used"]}
                for r in self.rows("select * from tokens order by created")
                if str(json.loads(r["principal"]).get("id", "")).lower()
                == pid]

    def revoke_token(self, principal_id, tid):
        """The token `tid` of this person ended; whether it was theirs."""
        pid = str(principal_id).lower()
        with self.tx() as db:
            got = db.execute("select principal from tokens where id = ?",
                             (str(tid),)).fetchone()
            if got is None or str(json.loads(got["principal"]).get(
                    "id", "")).lower() != pid:
                return False
            db.execute("delete from tokens where id = ?", (str(tid),))
        return True

    # ---- sign-ins under way ---------------------------------------------

    def put_pending(self, kind, key, data, seconds):
        now = _now()
        with self.tx() as db:
            db.execute("delete from pending where expires < ?", (now,))
            db.execute("insert or replace into pending values (?,?,?,?)",
                       (digest(key), kind, json.dumps(data), now + seconds))

    def take_pending(self, kind, key):
        """What was kept under `key`, once: taken, it is gone - a code, a
        state or an assertion's request replayed finds nothing."""
        if not key:
            return None
        with self.tx() as db:
            got = db.execute("select * from pending where key_hash = ? and"
                             " kind = ?", (digest(key), kind)).fetchone()
            if got is None:
                return None
            db.execute("delete from pending where key_hash = ?",
                       (digest(key),))
        if got["expires"] < _now():
            return None
        return json.loads(got["data"])

    def first_time(self, kind, ident, until):
        """True the first time `ident` is seen before `until` - an
        assertion presented twice is refused the second time."""
        now = _now()
        with self.tx() as db:
            db.execute("delete from seen where expires < ?", (now,))
            try:
                db.execute("insert into seen values (?,?,?)",
                           (kind, str(ident), max(float(until), now + 60)))
            except sqlite3.IntegrityError:
                return False
        return True

    # ---- the local account ----------------------------------------------

    def local_account(self):
        got = self.rows("select name, hash from local limit 1")
        return (got[0]["name"], got[0]["hash"]) if got else None

    def set_local(self, name, hashed):
        """The single account, set or its password changed: an install
        without a provider has one operator. Every session it had ends."""
        now = _now()
        with self.tx() as db:
            db.execute("delete from local")
            db.execute("insert into local values (?,?,?,?)",
                       (name, hashed, now, now))
        self.close_sessions_of(name)

    # ---- the throttle ---------------------------------------------------

    def wait(self, key):
        """Seconds to wait before this key may try a password again."""
        got = self.rows("select until from throttle where key = ?", (key,))
        return max(0.0, got[0]["until"] - _now()) if got else 0.0

    def failed(self, key):
        """A wrong password: after `FAILURES_FREE` in a row, each one
        doubles the wait, up to `WAIT_MOST`."""
        now = _now()
        with self.tx() as db:
            got = db.execute("select * from throttle where key = ?",
                             (key,)).fetchone()
            n = (got["failures"] if got else 0) + 1
            wait = 0.0 if n <= FAILURES_FREE else min(
                WAIT_MOST, 2.0 ** (n - FAILURES_FREE))
            db.execute("insert or replace into throttle values (?,?,?,?)",
                       (key, n, got["first"] if got else now, now + wait))
        return wait

    def succeeded(self, key):
        with self.tx() as db:
            db.execute("delete from throttle where key = ?", (key,))

    # ---- the directory a provider provisions ----------------------------

    def person(self, name):
        """{active, groups} of a provisioned person by user name or
        e-mail, or None where the provider provisioned nobody by it."""
        got = self.rows("select id, active from users where user_name = ?"
                        " or email = ?", (str(name), str(name)))
        if not got:
            return None
        groups = [r["display_name"] for r in self.rows(
            "select g.display_name from groups g join members m on"
            " m.group_id = g.id where m.user_id = ? order by 1",
            (got[0]["id"],))]
        return {"active": bool(got[0]["active"]), "groups": groups}
