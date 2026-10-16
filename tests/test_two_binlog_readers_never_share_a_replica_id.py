"""Two binlog readers on one source never attach under one replica id
(field run 2026-10-16: two databases followed at once from one source, and
the server ended the first reader when the second attached under the same
fixed id). The id is a rung (`decide.reader_server_id`) on what the source
and this machine say is taken, kept in the run store; a named id is
honoured or refused; the server ending a reader for another under its id
is caught and the reader attaches again under a fresh id once, then stops
with the line."""
import threading

import pymysql
import pytest

from migkit import config, decide
from migkit.config import Hop, Endpoint
from migkit.engines.mysql import MySQLEngine


def _hop(tmp_path, name, db="shop", **options):
    hop = Hop(name=name, engine="mysql",
              source=Endpoint(host="10.0.0.1", port=3306, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=3306, user="u",
                              password="CHANGE_ME"),
              databases=[db], options=options)

    def report_dir(d=""):
        got = tmp_path / name / d if d else tmp_path / name
        got.mkdir(parents=True, exist_ok=True)
        return got
    hop.report_dir = report_dir
    return hop


class _Source(MySQLEngine):
    """A stand-in source answering what the account may read."""
    replicas = ()

    def _q(self, side, sql, args=None):
        if sql == "select @@server_id":
            return [(1,)]
        if sql in ("show replicas", "show slave hosts"):
            return [(r, "", 3306, 1, "") for r in self.replicas]
        raise AssertionError(sql)


@pytest.fixture
def reports(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    return tmp_path


def test_the_rung_walks_past_every_id_taken():
    choice, first = decide.reader_server_id(
        {"identity": "a/shop/src/tail", "taken": {}})
    assert choice.path == "derived"
    _, again = decide.reader_server_id(
        {"identity": "a/shop/src/tail", "taken": {first: "a replica"}})
    assert again != first
    assert decide.READER_IDS[0] <= again <= decide.READER_IDS[1]
    _, other = decide.reader_server_id(
        {"identity": "b/shop/src/tail", "taken": {}})
    assert other != first


def test_a_recorded_id_is_kept_while_free_and_left_when_held():
    choice, got = decide.reader_server_id(
        {"recorded": 2 ** 31 + 7, "identity": "a", "taken": {1: "x"}})
    assert (choice.path, got) == ("recorded", 2 ** 31 + 7)
    choice, got = decide.reader_server_id(
        {"recorded": 2 ** 31 + 7, "identity": "a",
         "taken": {2 ** 31 + 7: "hop b's tail reader"}})
    assert choice.path == "derived" and got != 2 ** 31 + 7
    assert any("hop b's tail reader" in why for _, why in choice.passed)


def test_a_named_id_is_honoured_or_refused_saying_who_holds_it(
        reports, tmp_path):
    eng = _Source(_hop(tmp_path, "a", server_id=4379))
    assert eng.reader_id("src", "tail", "shop") == 4379
    held = _Source(_hop(tmp_path, "b", server_id=4379))
    held.replicas = (4379,)
    with pytest.raises(SystemExit, match="4379 is held by a replica the"
                       " server lists"):
        held.reader_id("src", "tail", "shop")


def test_two_hops_and_two_purposes_on_one_source_get_four_ids(
        reports, tmp_path):
    a = _Source(_hop(tmp_path, "a", "customer"))
    b = _Source(_hop(tmp_path, "b", "tracking"))
    ids = {a.reader_id("src", "tail", "customer"),
           a.reader_id("src", "first event"),
           b.reader_id("src", "tail", "tracking"),
           b.reader_id("src", "first event")}
    assert len(ids) == 4 and 1 not in ids
    # recorded in each hop's run store, and read back the same
    again = _Source(_hop(tmp_path, "a", "customer"))
    assert again.reader_id("src", "tail", "customer") == \
        a.reader_id("src", "tail", "customer")


def test_a_listed_replica_and_another_hops_record_are_avoided(
        reports, tmp_path):
    a = _Source(_hop(tmp_path, "a"))
    mine = a.reader_id("src", "tail", "shop")
    # a fresh engine of hop a, the source now listing its id as another's
    listed = _Source(_hop(tmp_path, "a"))
    listed.replicas = (mine,)
    assert listed.reader_id("src", "tail", "shop") != mine
    choice = listed._reader_choices[("src", "tail", "shop")]
    assert "a replica the server lists" in " ".join(
        why for _, why in choice.passed)
    # hop c on the same source steps round what hop a recorded
    c = _Source(_hop(tmp_path, "c"))
    taken = c._ids_taken("src")
    assert taken[listed.reader_id("src", "tail", "shop")] == \
        "hop a's tail reader"


def test_the_server_ending_a_reader_for_another_is_retried_once(
        reports, tmp_path):
    eng = _Source(_hop(tmp_path, "a"))
    first = eng.reader_id("src", "tail", "shop")
    collided = pymysql.err.OperationalError(
        1236, "A slave with the same server_uuid/server_id as this slave"
        " has connected to the master")
    purged = pymysql.err.OperationalError(
        1236, "Could not find first log file name in binary log index file")
    assert not eng.reader_collided("src", "tail", purged, "shop")
    assert eng.reader_collided("src", "tail", collided, "shop")
    fresh = eng.reader_id("src", "tail", "shop")
    assert fresh != first
    with pytest.raises(SystemExit, match="ended twice"):
        eng.reader_collided("src", "tail", collided, "shop")


def test_a_named_id_the_server_ends_stops_with_the_line(reports, tmp_path):
    eng = _Source(_hop(tmp_path, "a", server_id=77))
    eng.reader_id("src", "tail", "shop")
    with pytest.raises(SystemExit, match="server_id the hop names"):
        eng.reader_collided("src", "tail", pymysql.err.OperationalError(
            1236, "A slave with the same server_uuid/server_id as this"
            " slave has connected to the master"), "shop")


# --- on a server (lane 7) ---------------------------------------------------

from tests import servers  # noqa: E402
from tests.conftest import needs_docker  # noqa: E402

#: the binlog with its full row metadata, as the tail needs
pair = servers.fixture("mysql-rowmeta")


@needs_docker
def test_two_hops_follow_one_source_at_once_and_both_get_their_rows(
        pair, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    dbs = (pair.name, pair.name + "_b")
    pair.src.sql(f"create database if not exists {dbs[1]}")
    for d in dbs:
        pair.src.sql("drop table if exists t; create table t (id int"
                     " primary key)", d)
    engines = []
    for k, d in enumerate(dbs):
        hop = Hop(name=f"hop{k}", engine="mysql",
                  source=pair.src.endpoint(), target=pair.dst.endpoint(),
                  databases=[d])
        hop.report_dir = (lambda n: lambda x="": (
            tmp_path / n / x).mkdir(parents=True, exist_ok=True)
            or tmp_path / n / x)(hop.name)
        engines.append(MySQLEngine(hop))
    tokens = [e.change_point("src", d) for e, d in zip(engines, dbs)]
    got, errors = [[], []], []
    try:
        for round_ in range(3):
            for d in dbs:
                pair.src.sql(f"insert into t values ({round_})", d)

            def read(k):
                try:
                    out, tokens[k] = engines[k].neutral_changes(
                        "src", dbs[k], tokens[k], limit=100)
                    got[k].extend(out)
                except BaseException as e:  # noqa: BLE001 - asserted
                    errors.append(e)
            threads = [threading.Thread(target=read, args=(k,))
                       for k in range(2)]
            [t.start() for t in threads]
            [t.join(120) for t in threads]
        assert not errors, errors
        ids = {e.reader_id("src", "tail", d) for e, d in zip(engines, dbs)}
        assert len(ids) == 2
        assert all(len(g) == 3 for g in got), [len(g) for g in got]
    finally:
        for e in engines:
            e.release_changes()
        pair.src.sql(f"drop database if exists {dbs[1]}")
