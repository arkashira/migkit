"""The change tail at high rates: the next batch read while one is
applied, batches that grow while it is behind, independent rows written in
the fewest statements, and lanes applied side by side.

Measured, MySQL to PostgreSQL, 320,000 changes queued (inserts, an update
of every other row, a delete of every tenth): 21.8s before; reading ahead
and larger batches 7.7s; the column mapping skipped where there is none
5.5s. With the target 10 ms away: 59.6s in one lane - the batch's order had
broken it into a statement every nine rows - 17.1s in four; the rows no
order binds written as one run of deletes and one of upserts a table,
10.0s in one lane and 6.0s in four.
"""
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _up(table, i, **v):
    return {"op": "insert", "table": table, "key": {"id": i},
            "values": {"id": i, **v}}


def _gone(table, i):
    return {"op": "delete", "table": table, "key": {"id": i}}


class _Eng:
    """The base applier with its statements recorded, not sent."""

    def __init__(self, ordered):
        from migkit.engines.base import Engine
        self.base = Engine
        self.ordered, self.sent = ordered, []

    def run(self, rows):
        from migkit.engines.base import Engine

        class E(Engine):
            def __init__(me):
                me.hop = Hop(name="t", engine="x",
                             source=Endpoint(host="10.0.0.1", port=1,
                                             user="u", password="CHANGE_ME"),
                             target=Endpoint(host="10.0.0.2", port=1,
                                             user="u", password="CHANGE_ME"))

            def _ordered_tables(me, side, db):
                return self.ordered

            def _apply_run(me, side, db, shape, run):
                self.sent.append((shape[0], shape[1], [k["id"]
                                                       for k, _ in run]))
        e = E()
        e._apply_net("dst", "app", e._net_rows("dst", "app", rows))
        return self.sent


def test_rows_no_order_binds_go_as_one_delete_and_one_upsert_a_table():
    batch = []
    for i in range(1, 31):
        batch.append(_up("t", i, v=i))
        if i % 10 == 0:
            batch.append(_gone("t", i - 5))
    sent = _Eng({}).run(batch)
    assert [(t, k) for t, k, _ in sent] == [("t", "delete"),
                                            ("t", "upsert")], sent
    assert sent[0][2] == [5, 15, 25]


def test_rows_of_tables_joined_by_keys_keep_the_batchs_order():
    batch = [_up("parent", 1), _up("child", 1, parent=1), _gone("child", 1),
             _gone("parent", 1), _up("free", 1), _gone("free", 2),
             _up("free", 3)]
    sent = _Eng({"parent": "parent", "child": "parent"}).run(batch)
    assert sent == [("parent", "upsert", [1]), ("child", "upsert", [1]),
                    ("child", "delete", [1]), ("parent", "delete", [1]),
                    ("free", "delete", [2]), ("free", "upsert", [1, 3])], sent
    # an engine that cannot say which tables those are: every row
    # collapsed, in the order it was first touched, as before
    sent = _Eng(None).run(batch)
    assert [t for t, _, _ in sent] == ["parent", "child", "free", "free",
                                       "free"], sent


@needs_docker
def test_a_parent_and_child_changed_and_removed_in_one_batch_replay(
        pg_pair):
    """Collapsed, the parent's delete went where its update had been -
    before the child's delete - and the key refused it on every replay."""
    _schema(pg_pair)
    psql(pg_pair["dst"], "insert into public.parent values (1, 0);"
                         " insert into public.child values (1, 1, 0)")
    batch = [{"op": "update", "table": "parent", "key": {"id": 1},
              "values": {"id": 1, "v": 5}},
             {"op": "update", "table": "child", "key": {"id": 1},
              "values": {"id": 1, "parent": 1, "v": 5}},
             _gone("child", 1), _gone("parent", 1)]
    for workers in (1, 4):
        _pg(pg_pair, workers=workers).neutral_apply("dst", "postgres", batch)
        assert psql(pg_pair["dst"], "select (select count(*) from"
                                    " public.parent) || ',' || (select"
                                    " count(*) from public.child)"
                    ).stdout.strip() == "0,0"


def test_a_read_ahead_is_used_only_where_the_batch_asked_for_starts():
    from migkit.engines.hetero import _ReadAhead
    asked = []

    class Src:
        def neutral_changes(self, side, db, token, limit):
            asked.append((token, limit))
            n = int(token)
            return [{"n": n}] * limit, str(n + limit)
    ahead = _ReadAhead(Src(), "app")
    try:
        got, after = ahead.next("0", 10)
        assert after == "10"
        got, after = ahead.next("10", 20)
        assert after == "30" and len(got) == 20
        # back to a saved position, as after a lost connection: what was
        # read ahead from 30 is not what is asked for
        got, after = ahead.next("10", 20)
        assert after == "30"
    finally:
        ahead.close()
    assert ("10", 20) in asked and asked.count(("10", 20)) == 2, asked


def test_a_slot_is_never_read_ahead_of_what_was_applied():
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    assert PostgresEngine.READS_AHEAD is False
    assert MySQLEngine.READS_AHEAD is True


def _pg(pg_pair, workers=4):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=pg_pair["dst"], user="postgres",
                  password="test")
    return PostgresEngine(Hop(name="lanes", engine="postgres", source=ep,
                              target=ep, databases=["postgres"],
                              workers=workers))


def _schema(pg_pair):
    got = psql(pg_pair["dst"], """
        create table public.parent (id int primary key, v int);
        create table public.child (id int primary key,
            parent int references public.parent (id), v int);
        create table public.free (id int primary key, v int);
        create table public.mail (id int primary key, email text unique)""")
    assert got.returncode == 0, got.stderr


def _batch(n=6000):
    out = []
    for i in range(1, n + 1):
        out.append(_up("parent", i, v=i))
        out.append(_up("child", i, parent=i, v=i))
        out.append(_up("free", i, v=i))
    for i in range(1, n + 1, 7):
        out.append(_gone("child", i))
        out.append(_gone("parent", i))
        out.append(_gone("free", i + 1))
    return out


def _state(pg_pair):
    return psql(pg_pair["dst"], "select (select count(*) || ':' ||"
                                " sum(v) from public.parent), (select"
                                " count(*) || ':' || sum(v) from"
                                " public.child), (select count(*) || ':' ||"
                                " sum(v) from public.free)").stdout


@needs_docker
def test_lanes_land_what_one_lane_lands(pg_pair, monkeypatch):
    _schema(pg_pair)
    eng = _pg(pg_pair, workers=4)
    lanes = eng._lanes("dst", "postgres", _batch())
    assert lanes and len(lanes) >= 2
    # the tables a key joins are in one lane, whole
    holding = [i for i, lane in enumerate(lanes)
               if any(t in ("parent", "child") for t, _ in lane)]
    assert len(holding) == 1, holding
    eng.neutral_apply("dst", "postgres", _batch())
    four = _state(pg_pair)
    psql(pg_pair["dst"], "truncate public.child, public.parent, public.free")
    _pg(pg_pair, workers=1).neutral_apply("dst", "postgres", _batch())
    assert _state(pg_pair) == four
    assert "lane_retries" not in eng.__dict__


@needs_docker
def test_a_lane_that_fails_has_the_batch_applied_again_whole(pg_pair,
                                                             monkeypatch):
    _schema(pg_pair)
    eng = _pg(pg_pair, workers=4)
    real, calls = eng._apply_net, []

    def one_lane_breaks(side, db, rows):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("deadlock detected")
        return real(side, db, rows)
    monkeypatch.setattr(eng, "_apply_net", one_lane_breaks)
    eng.neutral_apply("dst", "postgres", _batch(3000))
    assert eng.__dict__.get("lane_retries") == 1
    monkeypatch.setattr(eng, "_apply_net", real)
    once = _state(pg_pair)
    psql(pg_pair["dst"], "truncate public.child, public.parent, public.free")
    _pg(pg_pair, workers=1).neutral_apply("dst", "postgres", _batch(3000))
    assert _state(pg_pair) == once


@needs_docker
def test_a_table_with_a_second_unique_index_keeps_its_order(pg_pair):
    _schema(pg_pair)
    psql(pg_pair["dst"], "insert into public.mail values (1, 'a')")
    eng = _pg(pg_pair, workers=4)
    assert eng._ordered_tables("dst", "postgres").get("mail") == "mail"
    batch = [{"op": "update", "table": "mail", "key": {"id": 1},
              "values": {"id": 1, "email": "b"}},
             {"op": "insert", "table": "mail", "key": {"id": 2},
              "values": {"id": 2, "email": "a"}}]
    batch += [_up("free", i, v=i) for i in range(1, 2500)]
    eng.neutral_apply("dst", "postgres", batch)
    assert psql(pg_pair["dst"], "select string_agg(id || email, ',' order"
                                " by id) from public.mail").stdout.strip() \
        == "1b,2a"
