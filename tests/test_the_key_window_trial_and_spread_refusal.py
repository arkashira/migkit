"""Three move-area gaps closed (backlog 6.fkwin, movers.choice, reshard):
the streaming copy loads inside the foreign-key window instead of handing
its work to the dump and load, and a copy that dies there leaves the keys
off and recorded for the one that goes on; a whole database's mover never
timed on a hop is owed one trial, and one that failed it is not tried
again; a cluster target that cannot say how it spreads a table refuses
the cluster's own way with the reason, and the climb goes on.

Without a server: the scripted target of the foreign-key tests, stand-in
hops and stand-in cluster sides.
"""
import pytest

from migkit import decide, indexes, movers, reshard
from migkit.indexes import ForeignKeyWindow
from tests.test_a_cluster_target_is_loaded_as_it_spreads_its_rows import \
    _group
from tests.test_foreign_keys_go_back_or_the_move_stops import (
    KEYS, Hop, _records, _target)


@pytest.fixture(autouse=True)
def _no_held():
    ForeignKeyWindow._held.clear()
    yield
    ForeignKeyWindow._held.clear()


# --- 6.fkwin ---------------------------------------------------------------

def test_inherited_keys_are_never_taken_off_alone():
    assert "k.coninhcount = 0" in indexes.PG_FK_SQL


def test_a_copy_that_dies_in_the_window_leaves_the_keys_off_for_the_next(
        tmp_path):
    t = _target()
    said = []
    with pytest.raises(RuntimeError, match="copy died"):
        with ForeignKeyWindow(Hop(tmp_path), "app", said.append,
                              tables=[("public", "orders")], run=t,
                              hold=True):
            raise RuntimeError("copy died")
    # the keys into and on the table stay off; nothing was put back over
    # half a copy, and the record stays for the cutover to see
    assert ("public", "orders", "orders_cust_fk") not in t.keys
    assert ("public", "items", "items_order_fk") not in t.keys
    assert not [s for s in t.sent if " add constraint " in s]
    assert len(_records(tmp_path)) == 1
    assert any("stay off the target" in s for s in said), said
    # the copy that goes on - this same process - takes them over: back,
    # validated, and the record gone
    with ForeignKeyWindow(Hop(tmp_path), "app", said.append,
                          tables=[("public", "orders")], run=t, hold=True):
        pass
    assert t.keys == {k: list(v) for k, v in KEYS.items()}
    assert not _records(tmp_path)
    assert not ForeignKeyWindow._held


def test_without_hold_a_failed_load_still_puts_every_key_back(tmp_path):
    t = _target()
    with pytest.raises(RuntimeError):
        with ForeignKeyWindow(Hop(tmp_path), "app", tables=["public.orders"],
                              run=t):
            raise RuntimeError("load died")
    assert t.keys == {k: list(v) for k, v in KEYS.items()}
    assert not _records(tmp_path)


def _streaming(monkeypatch, tmp_path, blocked):
    from tests.test_the_wrapped_programs_run_one_way import _pg_hop
    hop = _pg_hop(tmp_path)
    monkeypatch.setattr(movers, "pgcopydb_runner", lambda: "local")
    monkeypatch.setattr(movers, "_pgcopydb_options", lambda *sub: frozenset())
    monkeypatch.setattr(movers, "_SIZES", {"public.t": 1})
    monkeypatch.setattr(movers, "_source_sizes", lambda h, db: {})
    monkeypatch.setattr(movers, "_pg_large_objects", lambda h, db: 0)
    monkeypatch.setattr(movers, "_pg_references_into",
                        lambda h, db, routed=(): blocked)
    monkeypatch.setattr(movers, "_pg_missing_tables",
                        lambda h, db: ([], False))
    events = []
    for name in ("_pg_create_missing", "_pg_finish_created",
                 "_pg_carry_sequences", "_record_copy"):
        monkeypatch.setattr(movers, name, lambda *a, **k: None)
    monkeypatch.setattr(movers, "_pg_truncate_target",
                        lambda *a, **k: events.append("truncate"))
    monkeypatch.setattr(movers, "pgdump_move",
                        lambda *a: events.append("dump") or ["# dump"])

    class NoIndexes:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False
    monkeypatch.setattr(movers, "_IndexWindow", NoIndexes)

    def run(self, args, what="", **k):
        if args[0] != "ping":
            events.append("copy")
    monkeypatch.setattr(movers._Streaming, "run", run)

    class Window:
        def __enter__(self):
            events.append("keys off")
            return self

        def __exit__(self, kind, *exc):
            events.append("keys back" if kind is None else "keys held")
            return False
    monkeypatch.setattr(movers, "_fk_window", lambda *a, **k: Window())
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "carry_database_objects",
                        lambda self, db, log=None: None)
    return hop, events


def test_the_streaming_copy_loads_inside_the_key_window(monkeypatch,
                                                        tmp_path):
    hop, events = _streaming(monkeypatch, tmp_path, ["public.t <- public.c"])
    got = movers.pgcopydb_move(hop, "appdb", 2, True, None)
    assert getattr(got, "via", "pgcopydb") == "pgcopydb"
    # off before the first row is emptied, back before the copy is done
    assert events == ["keys off", "truncate", "copy", "keys back"], events
    plan = movers.pgcopydb_move(hop, "appdb", 2, False, None)
    assert any("come off before the first row" in str(s) for s in plan)


def test_no_window_where_no_key_is_in_the_way(monkeypatch, tmp_path):
    hop, events = _streaming(monkeypatch, tmp_path, [])
    movers.pgcopydb_move(hop, "appdb", 2, True, None)
    assert events == ["truncate", "copy"], events


def test_a_copy_that_dies_holds_the_keys(monkeypatch, tmp_path):
    hop, events = _streaming(monkeypatch, tmp_path, ["public.t <- public.c"])

    def dies(self, args, what="", **k):
        if args[0] != "ping":
            raise RuntimeError("server closed the connection")
    monkeypatch.setattr(movers._Streaming, "run", dies)
    with pytest.raises(RuntimeError):
        movers.pgcopydb_move(hop, "appdb", 2, True, None)
    assert events == ["keys off", "truncate", "keys held"], events


# --- movers.choice ---------------------------------------------------------

def test_an_untimed_mover_has_its_one_trial_and_a_failed_one_is_kept(
        monkeypatch, tmp_path):
    monkeypatch.setattr(movers, "which", lambda n: "/bin/" + n)
    monkeypatch.setattr(movers, "pgcopydb_available", lambda: False)
    got = movers.choice("postgres")
    assert got.path == "pgdump" and got.trial == decide.TRIAL_SECONDS
    assert "never ran here" in got.reason

    class H:
        def report_dir(self, db=None):
            return tmp_path
    hop = H()
    assert "pgdump" in movers._owed_trial(movers._ladder(), None, hop)[0]
    movers.trial_failed(hop, "pgdump", "it exited 1")
    assert movers.trials_failed(hop) == {"pgdump": "it exited 1"}
    owed = movers._owed_trial(movers._ladder(), None, hop)
    assert owed is None or "pgdump" not in owed[0]
    # held below the copier from now on
    got = decide.climb("appdb", movers._ladder(), {"engine": "postgres"},
                       trial=owed)
    assert got.path == "builtin", got


# --- reshard ---------------------------------------------------------------

def test_a_spread_that_cannot_be_asked_refuses_with_why(monkeypatch):
    monkeypatch.setattr(reshard, "_target_table", lambda pair, db, t: t)

    def down(side, db, table):
        raise OSError("could not connect to server: timeout")
    g = _group(None)
    g._pairs[(0, 0)].dst_engine.distribution = down
    monkeypatch.setattr(g, "reader", lambda home=0, g=g: g._pairs[(0, 0)].src_engine)
    fn, choice = reshard.function(g, "public.orders")
    # not a flat "spreads nothing": the reason, and the next rung
    assert fn.way == "hash", choice
    assert any("could not connect to server" in why
               for _, why in choice.passed), choice.passed
    g = _group(None, by="target")
    g._pairs[(0, 0)].dst_engine.distribution = down
    monkeypatch.setattr(g, "reader", lambda home=0, g=g: g._pairs[(0, 0)].src_engine)
    fn, choice = reshard.function(g, "public.orders")
    assert fn is None and "could not say how it spreads" in choice.reason
