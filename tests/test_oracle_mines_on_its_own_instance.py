"""Oracle's redo mined where the source refuses to mine it (backlog
LT.C5): on a physical standby, or on migkit's own mining database the
source's archived logs are shipped to - each rung with what it needs, the
positions the source's SCNs end to end, a gap in the shipped logs a stop
and never silence, and the lag of each place measured and ranking it.

Stand-ins only: no Oracle server runs here, so LogMiner itself reading a
shipped log on another database is not proved by this module - the
choice, the ship, the gap and alignment checks and the window's bounds
are. The ship moves real files between real directories.
"""
import datetime

import pytest

from migkit.engines import oracle_logminer as lm

PROVES = [("tail.follows", "oracle")]

T0 = datetime.datetime(2026, 10, 1, 12, 0, 0)


def _log(thread, seq, first, nxt, minutes, dictionary="NO"):
    return (thread, seq, first, nxt, f"/u01/arch/{thread}_{seq}.arc", 4096,
            dictionary, T0 + datetime.timedelta(minutes=minutes))


class _Ep:
    def __init__(self, **options):
        self.options = options
        self.user, self.password = "app", "CHANGE_ME"

    def option_secret(self, key):
        return self.options.get(key)


class _Cursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, args=None):
        self.conn.said.append((sql, list(args or [])))
        self.sql = sql
        return self

    def fetchall(self):
        if "v$logmnr_logs" in self.sql:
            return self.conn.logs
        return []

    def fetchone(self):
        return ("APP",)

    def close(self):
        pass


class _MiningDb:
    def __init__(self, logs=()):
        self.logs = list(logs)
        self.said = []

    def cursor(self):
        return _Cursor(self)

    def close(self):
        pass


class _Oracle:
    """The source as the reader asks it: its grants, its archived logs,
    the dictionary it wrote into them, its SCN."""

    def __init__(self, ep, grants=(), logs=(), dictionary=100, now=10_000):
        self.hop = object()
        self.ep, self.grants, self.logs = ep, set(grants), list(logs)
        self.dictionary, self.now = dictionary, now
        self.mining = _MiningDb()
        self.dsns = []

    def _endpoint(self, side):
        return self.ep

    def _owner(self, side, db):
        return "APP"

    def _dedicated(self, side, dsn=None, user=None, password=None):
        self.dsns.append(dsn)
        return self.mining

    def _rows(self, side, db, sql, args=()):
        if "get_system_change_number" in sql:
            return [(self.now,)]
        if "v$transaction" in sql:
            return [(None,)]
        if "dictionary_begin = 'YES'" in sql:
            if self.dictionary is None:
                return [(None,)]
            return [(self.dictionary,)]
        if "from v$archived_log" in sql:
            return [g for g in self.logs if g[3] > args[0]]
        if "con_name" in sql:
            return [("APP",)]
        if "privilege" in sql or "role" in sql:
            return [(g,) for g in self.grants]
        raise AssertionError(sql)


ALL = ("LOGMINING", "SELECT ANY TRANSACTION")


def _shipping(tmp_path, **more):
    src, dst = tmp_path / "arch", tmp_path / "mine"
    src.mkdir()
    dst.mkdir()
    return src, dst, _Ep(mine_instance="10.0.0.5:1521/FREEPDB1",
                         archive_from=str(src), archive_to=str(dst),
                         archive_seen_as="/opt/oracle/shipped", **more)


def test_the_source_mines_where_its_account_may():
    eng = _Oracle(_Ep(), grants=ALL)
    assert lm.place(eng, "src", "app").path == lm.SOURCE


def test_a_named_standby_mines_before_the_source():
    eng = _Oracle(_Ep(mine_standby="10.0.0.6:1521/ORCL"), grants=ALL)
    assert lm.place(eng, "src", "app").path == lm.STANDBY


def test_a_source_without_the_grants_and_nowhere_else_is_said_so():
    eng = _Oracle(_Ep(), grants=("LOGMINING",))
    with pytest.raises(SystemExit) as e:
        lm.place(eng, "src", "app")
    assert "SELECT ANY TRANSACTION" in str(e.value)
    assert "mine_instance" in str(e.value)


def test_a_source_that_mines_nothing_ships_to_the_mining_database(tmp_path):
    _, _, ep = _shipping(tmp_path, mine_off_source="true")
    eng = _Oracle(ep, grants=ALL)
    assert lm.place(eng, "src", "app").path == lm.INSTANCE


def test_no_dictionary_in_the_redo_stops_the_mining_database(tmp_path):
    _, _, ep = _shipping(tmp_path)
    eng = _Oracle(ep, grants=(), dictionary=None)
    with pytest.raises(SystemExit) as e:
        lm.place(eng, "src", "app")
    assert "dictionary" in str(e.value)
    assert "store_in_redo_logs" in str(e.value)


def test_the_grants_are_not_asked_of_a_source_mined_elsewhere(tmp_path):
    _, _, ep = _shipping(tmp_path)
    eng = _Oracle(ep, grants=())
    have = lm.granted(eng, "src", "app")
    short = [p for p in lm.MINING_GRANTS if p not in have]
    assert short and lm._elsewhere(ep)
    assert not lm._elsewhere(_Ep())


def test_a_skipped_sequence_is_a_gap_never_silence():
    logs = [dict(zip(("thread", "seq", "first", "next"), g[:4]))
            for g in (_log(1, 10, 100, 200, 0), _log(1, 12, 300, 400, 2))]
    with pytest.raises(lm.ShipGap) as e:
        lm.contiguous(logs, 150)
    assert "sequence 10" in str(e.value)


def test_an_scn_range_no_log_covers_is_a_gap():
    logs = [dict(zip(("thread", "seq", "first", "next"), g[:4]))
            for g in (_log(1, 10, 100, 200, 0), _log(1, 11, 210, 300, 1))]
    with pytest.raises(lm.ShipGap):
        lm.contiguous(logs, 150)


def test_logs_that_begin_after_the_read_stands_are_a_gap():
    logs = [dict(zip(("thread", "seq", "first", "next"), g[:4]))
            for g in (_log(1, 10, 100, 200, 0),)]
    with pytest.raises(lm.ShipGap) as e:
        lm.contiguous(logs, 50)
    assert "begins at SCN 100" in str(e.value)
    lm.contiguous(logs, 150)


def test_the_window_ends_where_every_thread_has_archived():
    logs = [{"thread": 1, "next": 500}, {"thread": 1, "next": 900},
            {"thread": 2, "next": 700}]
    assert lm.reach(logs) == 699
    assert lm.reach([]) is None


def test_the_switch_is_measured_from_the_logs():
    logs = [{"at": T0}, {"at": T0 + datetime.timedelta(seconds=40)},
            {"at": T0 + datetime.timedelta(seconds=100)}]
    assert lm.switch_seconds(logs) == 50
    assert lm.switch_seconds([{"at": T0}]) is None


def _archive(src, logs):
    for g in logs:
        (src / f"{g[0]}_{g[1]}.arc").write_bytes(
            bytes([g[1] % 251]) * 4096 + str(g[2]).encode())


def test_a_log_is_shipped_whole_and_once(tmp_path, monkeypatch):
    from migkit import carriers
    src, dst, ep = _shipping(tmp_path)
    rows = [_log(1, 10, 100, 200, 0, "YES"), _log(1, 11, 200, 300, 1)]
    _archive(src, rows)
    eng = _Oracle(ep, logs=rows)
    moved = []
    real = carriers.move_file
    monkeypatch.setattr(carriers, "move_file",
                        lambda *a, **k: moved.append(a[1]) or real(*a, **k))
    ship = lm.Ship(eng, "src", "app")
    logs = lm.archived(eng, "src", "app", 100)
    paths = ship.ship(logs)
    assert paths == ["/opt/oracle/shipped/1_10.arc",
                     "/opt/oracle/shipped/1_11.arc"]
    for g in rows:
        name = f"{g[0]}_{g[1]}.arc"
        assert (dst / name).read_bytes() == (src / name).read_bytes()
    assert moved == ["1_10.arc", "1_11.arc"]
    ship.ship(logs)
    assert moved == ["1_10.arc", "1_11.arc"]
    # a mover that starts over finds them there whole, and moves none
    again = lm.Ship(eng, "src", "app")
    again.ship(logs)
    assert moved == ["1_10.arc", "1_11.arc"]
    # one cut short is moved again
    (dst / "1_11.arc").write_bytes(b"short")
    lm.Ship(eng, "src", "app").ship(logs)
    assert moved[-1] == "1_11.arc"
    assert (dst / "1_11.arc").read_bytes() == (src / "1_11.arc").read_bytes()


def test_a_shipped_log_read_at_other_scns_stops_and_is_shipped_again(
        tmp_path):
    src, _, ep = _shipping(tmp_path)
    rows = [_log(1, 10, 100, 200, 0, "YES")]
    _archive(src, rows)
    eng = _Oracle(ep, logs=rows)
    ship = lm.Ship(eng, "src", "app")
    logs = lm.archived(eng, "src", "app", 100)
    ship.ship(logs)
    lm.aligned(_MiningDb([(1, 10, 100, 200)]), logs, ship)
    with pytest.raises(lm.ShipGap) as e:
        lm.aligned(_MiningDb([(1, 10, 100, 180)]), logs, ship)
    assert "(100, 180)" in str(e.value) and "(100, 200)" in str(e.value)
    assert (1, 10) not in ship.shipped


def test_the_mining_database_mines_the_shipped_logs_with_their_dictionary(
        tmp_path):
    src, dst, ep = _shipping(tmp_path)
    rows = [_log(1, 10, 100, 200, 0, "YES"), _log(1, 11, 200, 300, 1),
            _log(1, 12, 300, 400, 2)]
    _archive(src, rows)
    eng = _Oracle(ep, grants=(), logs=rows, now=10_000)
    eng.mining.logs = [(1, 10, 100, 200), (1, 11, 200, 300),
                       (1, 12, 300, 400)]
    lm._LAGS.clear()
    miner = lm.Miner(eng, "src", "app")
    miner._window({"scn": 150})
    assert eng.dsns == ["10.0.0.5:1521/FREEPDB1"]
    added = [a[0] for s, a in eng.mining.said if "add_logfile" in s]
    assert added == ["/opt/oracle/shipped/1_10.arc",
                     "/opt/oracle/shipped/1_11.arc",
                     "/opt/oracle/shipped/1_12.arc"]
    start = [(s, a) for s, a in eng.mining.said if "start_logmnr" in s]
    assert start[0][1] == [150, 399]
    assert "dict_from_redo_logs" in start[0][0]
    assert "dict_from_online_catalog" not in start[0][0]
    # nothing is said of a commit beyond the archived redo
    assert miner.samples[-1][1] == 399
    contents = [(s, a) for s, a in eng.mining.said
                if "v$logmnr_contents" in s]
    assert "src_con_name" in contents[0][0] and contents[0][1][1] == "APP"
    assert lm._LAGS[(id(eng.hop), "src", "app")][lm.INSTANCE][0] >= 60
    assert sorted(p.name for p in dst.iterdir()) == [
        "1_10.arc", "1_11.arc", "1_12.arc"]


def test_a_gap_in_the_archive_stops_the_window(tmp_path):
    src, _, ep = _shipping(tmp_path)
    rows = [_log(1, 10, 100, 200, 0, "YES"), _log(1, 12, 300, 400, 2)]
    _archive(src, rows)
    eng = _Oracle(ep, logs=rows)
    with pytest.raises(lm.ShipGap):
        lm.Miner(eng, "src", "app")._window({"scn": 150})
    assert not any("start_logmnr" in s for s, _ in eng.mining.said)


def test_the_lag_measured_ranks_the_places():
    eng = _Oracle(_Ep(), grants=ALL)
    lm._LAGS.clear()
    lm.lagged(eng, "src", "app", lm.SOURCE, 2.0)
    for _ in range(30):
        lm.lagged(eng, "src", "app", lm.SOURCE, 3.0)
    assert len(lm._LAGS[(id(eng.hop), "src", "app")][lm.SOURCE]) == 20
    assert lm.place(eng, "src", "app").path == lm.SOURCE
