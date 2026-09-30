"""A two-way tail's batch on PostgreSQL is taken under a replication origin
of the hop's on the batch's own connection - which is the driver that
writes the rows, not the one that reads. An origin not made yet is made,
and one another session holds is passed for the next, told apart by the
server's code for the error, whichever driver raised it: matched by the
reading driver's classes, a missing origin raised on the writing driver's
connection stopped the tail instead of being made.
"""
import psycopg
import pytest

from migkit.config import Endpoint, Hop

SETUP = "pg_replication_origin_session_setup"


def _engine(workers=2):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="h", port=1, user="u", password="p")
    return PostgresEngine(Hop(name="two", engine="postgres", source=ep,
                              target=ep, databases=["d"], workers=workers))


class _Conn:
    """The batch's connection: each attempt to take an origin answered by
    the next of `answers` (an error to raise, or None to take it)."""

    def __init__(self, answers):
        self.answers, self.sent = list(answers), []

    def cursor(self):
        conn = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, args=None):
                conn.sent.append((sql, args))
                if SETUP in sql:
                    got = conn.answers.pop(0)
                    if got is not None:
                        raise got
        return Cursor()


def test_an_origin_held_elsewhere_is_passed_and_one_missing_is_made(
        monkeypatch):
    eng = _engine()
    made = []
    monkeypatch.setattr(type(eng), "_origin_named",
                        lambda self, side, db, name: made.append(name))
    first = eng.twoway_origin()
    conn = _Conn([psycopg.errors.ObjectInUse("held"),
                  psycopg.errors.UndefinedObject("not there"), None])
    got = eng._origin_session(conn, "dst", "d", None)
    assert got == f"{first}_1"
    assert made == [f"{first}_1"]
    setups = [a[0] for s, a in conn.sent if SETUP in s]
    assert setups == [first, f"{first}_1", f"{first}_1"]
    # each failed attempt undone to its savepoint, the taken one released
    steps = [s for s, _ in conn.sent if "savepoint" in s]
    assert steps == ["savepoint migkit_origin",
                     "rollback to savepoint migkit_origin",
                     "savepoint migkit_origin",
                     "rollback to savepoint migkit_origin",
                     "release savepoint migkit_origin"]


def test_the_reading_drivers_errors_are_told_apart_the_same_way(
        monkeypatch):
    class Held(Exception):
        pgcode = "55006"
    eng = _engine()
    monkeypatch.setattr(type(eng), "_origin_named",
                        lambda self, side, db, name: None)
    conn = _Conn([Held(), None])
    assert eng._origin_session(conn, "dst", "d", None) == \
        f"{eng.twoway_origin()}_1"


def test_any_other_error_stops_the_batch():
    eng = _engine()
    conn = _Conn([psycopg.errors.InsufficientPrivilege("no")])
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        eng._origin_session(conn, "dst", "d", None)
    assert not [s for s, _ in conn.sent if "rollback" in s]
