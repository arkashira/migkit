"""The copiers' writes reconciled, their sizes retuned, their width
throttled, and the ledger of owned work kept through a torn file (backlog
P0-RESILIENCE-ALL, P0-AUTOTUNE-ALL)."""
import json
import threading

import pytest

from migkit import attempts, decide, faults, tuning


class Lost(Exception):
    """A link dropped after the write was sent: MySQL's 2013."""

    def __init__(self):
        super().__init__(2013, "Lost connection to MySQL server during query")


@pytest.fixture(autouse=True)
def _closed():
    faults.reset()
    yield
    faults.reset()


def test_an_uncertain_write_is_reconciled_then_not_repeated(tmp_path):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    target, writes = [], []

    def write():
        writes.append(1)
        target.extend(range(5))
        raise Lost()

    owned = faults.Owned("dst", "copy batch", "t", "0")
    got = faults.retried(write, writes=True, side="dst", owned=owned,
                         ledger=ledger, wait=lambda s: None,
                         check=faults.landed(lambda: 5, lambda: len(target),
                                             before=0))
    assert got == 5
    assert len(writes) == 1 and len(target) == 5
    assert ledger.owned(owned.key)["state"] == faults.COMMITTED
    # the same work in a later run: reconciled, never written again
    faults.retried(write, writes=True, side="dst", owned=owned,
                   ledger=attempts.Ledger(str(tmp_path / "attempts.json")),
                   wait=lambda s: None,
                   check=faults.landed(lambda: 5, lambda: len(target)))
    assert len(writes) == 1 and len(target) == 5


def test_an_uncertain_write_that_is_not_there_goes_again_once():
    target, writes = [], []

    def write():
        writes.append(1)
        if len(writes) == 1:
            raise Lost()
        target.extend(range(5))
        return "written"

    got = faults.retried(write, writes=True, side="dst", wait=lambda s: None,
                         check=faults.landed(lambda: 5, lambda: len(target),
                                             before=0))
    assert got == "written"
    assert len(writes) == 2 and len(target) == 5


def test_a_partial_uncertain_write_is_stopped_never_run_on_a_guess():
    target = []

    def write():
        target.extend(range(2))
        raise Lost()

    with pytest.raises(Lost):
        faults.retried(write, writes=True, side="dst", wait=lambda s: None,
                       check=faults.landed(lambda: 5, lambda: len(target),
                                           before=0))
    assert len(target) == 2


def test_a_packet_too_large_is_retuned_once_and_completes():
    retune = tuning.Retune("db.t", rows=1000, nbytes=8 * 2 ** 20)
    seen = []

    def write():
        if seen[-1]["bytes"] >= 8 * 2 ** 20:
            raise Exception(1153, "Got a packet bigger than"
                                  " 'max_allowed_packet' bytes")
        return "done"

    got = tuning.sized(retune, "range 1", write, seen.append,
                       sent=lambda: 8 * 2 ** 20, rows=lambda: 1000)
    assert got == "done"
    assert len(seen) == 2
    assert seen[1]["bytes"] < seen[0]["bytes"]
    assert [a.cause.kind for a in retune.history] == [tuning.PACKET]


def test_a_failure_no_size_answers_is_not_retuned():
    retune = tuning.Retune("db.t", rows=1000)
    tried = []

    def write():
        tried.append(1)
        raise Exception(1045, "Access denied for user")

    with pytest.raises(Exception):
        tuning.sized(retune, "range 1", write, lambda s: None)
    assert len(tried) == 1 and retune.history == []


def test_a_statement_timeout_is_a_sizing_cause():
    cause = tuning.cause_of(Exception(3024, "Query execution was"
                                            " interrupted, maximum statement"
                                            " execution time exceeded"))
    assert cause.kind == tuning.TIMEOUT and cause.sizing
    assert tuning.cause_of(Exception("lock wait timeout exceeded")).kind \
        == tuning.LOCK
    assert tuning.cause_of(MemoryError()).kind == tuning.MEMORY
    assert not tuning.cause_of(Exception("syntax error")).sizing
    assert not hasattr(tuning, "SIGNATURES")


class Strained:
    """A server that says how many sessions run on its CPUs, counted."""

    def __init__(self, running):
        self.running, self.asked = running, 0

    def capacity(self, side, db):
        self.asked += 1
        return {"cpus": 2, "running": self.running}

    def io_wait(self, side, db):
        return {}


def test_one_controller_moves_the_width_through_the_catalogue():
    from migkit import sizing
    sizing._ASKED.clear()
    eng = Strained(running=64)
    now = [0.0]
    pace = sizing.Pace(8, 8, clock=lambda: now[0], resources={
        sizing.CATALOGUE: sizing.catalogue(eng, "db")})

    def window():
        for _ in range(4):
            now[0] += 1.0
            pace.done(1)
        return pace.limit
    cut = window()
    assert cut < 8
    assert any(faults.CATALOGUE["cpu_pressure"].said in (why or "")
               for _, _, why in pace.history)
    eng.running = 0
    sizing._ASKED.clear()
    widths = [window() for _ in range(14)]
    # held while the cut holds, then one back a window, never a leap
    assert widths[:pace.HOLD] == [cut] * pace.HOLD
    back = widths[:widths.index(8) + 1]
    steps = [b - a for a, b in zip([cut] + back, back)]
    assert all(0 <= s <= 1 for s in steps)


def test_a_server_is_asked_about_itself_once_a_window():
    from migkit import sizing
    sizing._ASKED.clear()
    eng = Strained(running=0)
    for _ in range(5):
        sizing.side_strain(eng, "dst", "db")
    assert eng.asked == 1
    sizing._ASKED.clear()
    sizing.capacity(eng, "dst", "db")
    assert eng.asked == 2


def test_a_bare_timeout_is_told_by_the_phase_it_was_met_in():
    import socket
    late = socket.timeout("timed out")
    assert tuning.cause_of(late, during="statement").kind == tuning.TIMEOUT
    assert tuning.cause_of(late, during="statement").sizing
    assert tuning.cause_of(late, during="connect").kind == tuning.CONNECTION
    assert faults.classify(late, during="connect").phase == "connecting"
    # a link's own words win over the phase
    assert tuning.cause_of(Exception("connection timed out"),
                           during="statement").kind == tuning.CONNECTION
    # a statement's own time limit, whose words hold the connection's
    assert faults.classify(Exception(
        "Execution Timeout Expired. The timeout period elapsed")).fault \
        .name == "statement_timeout"
    assert faults.classify(Exception("timeout expired")).fault.name \
        == "connectivity"


class Cursor:
    def __init__(self, rows):
        self.rows, self.description = rows, [("a",)]

    def fetchall(self):
        return self.rows


def _mssql(fail_first):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine.__new__(MSSQLEngine)
    ran, forgot = [], []

    class Conn:
        def cursor(self):
            return Cursor([(1,), (2,)])

    def run(cur, sql, args):
        ran.append(sql)
        if fail_first and len(ran) == 1:
            raise Exception("DBPROCESS is dead or not enabled")
    eng._kept = lambda side, db: Conn()
    eng._forget = lambda side, db: forgot.append(side)
    eng._run = run
    return eng, ran, forgot


def test_a_server_read_whose_link_dropped_is_read_again():
    eng, ran, forgot = _mssql(fail_first=True)
    assert eng._kept_rows("src", "db", "select a from t") == [[1], [2]]
    assert len(ran) == 2 and forgot == ["src"]


def test_a_server_write_lost_after_it_was_sent_is_never_run_twice():
    eng, ran, _ = _mssql(fail_first=True)
    with pytest.raises(RuntimeError, match="target refused"):
        eng._kept_rows("dst", "db", "insert into t values (1)")
    assert len(ran) == 1


def _pg(tmp_path, landed, pre_sql_clears):
    from types import SimpleNamespace

    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine.__new__(PostgresEngine)
    src, dst, copies = list(range(5)), [], []

    def pipe(db, select, qt, pre_sql, columns=None):
        copies.append(select)
        if pre_sql_clears:
            dst.clear()
        dst.extend(src[:landed] if len(copies) == 1 else src)
        if len(copies) == 1:
            raise Exception("server closed the connection unexpectedly")
        return "sent"
    eng._copy_pipe = pipe
    eng._psql = lambda side, db, sql: str(len(src if side == "src"
                                              else dst))
    eng._copy_out_tally = lambda side, db, select: "read back"
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    eng.attempts = lambda: SimpleNamespace(ledger=ledger)
    return eng, dst, copies, ledger


def test_a_range_whose_load_landed_whole_is_read_back_not_copied_again(
        tmp_path):
    eng, dst, copies, ledger = _pg(tmp_path, landed=5, pre_sql_clears=True)
    got = eng._pipe_retried("db", "select * from t where id < 5", "t",
                            "delete from t where id < 5", ["id"], "id < 5",
                            "t range 1", None)
    assert got == "read back" and len(copies) == 1 and len(dst) == 5
    key = faults.Owned("dst", "copy range", "t", "id < 5").key
    assert ledger.owned(key)["state"] == faults.COMMITTED


def test_a_range_part_landed_with_no_clear_of_its_span_stops(tmp_path):
    eng, dst, copies, _ = _pg(tmp_path, landed=2, pre_sql_clears=False)
    with pytest.raises(Exception, match="server closed"):
        eng._pipe_retried("db", "select * from t", "t", "", ["id"], "",
                          "t range 1", None)
    assert len(copies) == 1 and len(dst) == 2


def test_a_range_that_left_nothing_goes_again_once(tmp_path):
    eng, dst, copies, _ = _pg(tmp_path, landed=0, pre_sql_clears=True)
    got = eng._pipe_retried("db", "select * from t", "t", "truncate t",
                            ["id"], "", "t range 1", None)
    assert got == "sent" and len(copies) == 2 and len(dst) == 5


def test_a_torn_ledger_is_read_from_its_snapshot(tmp_path):
    path = tmp_path / "attempts.json"
    ledger = attempts.Ledger(str(path))
    ledger.own("op:1", state="began", tries=1)
    assert json.loads((tmp_path / "attempts.json.bak").read_text())[
        "operations"]["op:1"]["state"] == "began"
    path.write_text(path.read_text()[:17])
    back = attempts.Ledger(str(path))
    assert back.read_from == f"{path}.bak"
    assert back.owned("op:1")["state"] == "began"
    assert "op:1" in back.uncertain()


def test_a_ledger_torn_with_its_snapshot_stops(tmp_path):
    from migkit.durable import Torn
    path = tmp_path / "attempts.json"
    attempts.Ledger(str(path)).own("op:1", state="began")
    path.write_text("{")
    (tmp_path / "attempts.json.bak").write_text("{")
    with pytest.raises(Torn):
        attempts.Ledger(str(path))


def test_nothing_answers_a_one_row_packet_on_a_target_without_pieces():
    retune = tuning.Retune("db.t", rows=1, nbytes=1024)

    def write():
        raise Exception(1153, "Got a packet bigger than"
                              " 'max_allowed_packet' bytes")

    with pytest.raises(decide.Blocked):
        tuning.sized(retune, "range 1", write, lambda s: None,
                     sent=lambda: 1024, rows=lambda: 1)
