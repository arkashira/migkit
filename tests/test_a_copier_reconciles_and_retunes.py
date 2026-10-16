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


def test_a_kept_statement_failed_on_purpose_is_read_again(monkeypatch):
    from migkit import failpoint
    failpoint.reset()
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mssql.statement:1:fault=connectivity")
    eng, ran, forgot = _mssql(fail_first=False)
    assert eng._kept_rows("src", "db", "select a from t") == [[1], [2]]
    # the first try never reached the server; the second read it
    assert len(ran) == 1 and forgot == ["src"]


def test_one_place_reached_is_counted_once_whatever_names_it(monkeypatch):
    from migkit import failpoint
    failpoint.reset()
    monkeypatch.setenv("MIGKIT_FAILPOINT", "x:1:raise,x:3:raise")
    got = []
    for _ in range(4):
        try:
            failpoint.hit("x")
            got.append("ok")
        except RuntimeError:
            got.append("failed")
    assert got == ["failed", "ok", "failed", "ok"]


def test_an_open_breaker_asks_the_recovery_again_and_keeps_the_path(
        tmp_path):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    breaker = faults.Breaker()
    breaker.failed("src", 0)    # the side failed past what one op may try
    calls = []

    def read():
        calls.append(1)
        raise faults.Injected("connectivity")
    owned = faults.Owned("src", "read range", "t", "0..10@None")
    with pytest.raises(faults.Injected):
        faults.retried(read, tries=3, side="src", ledger=ledger,
                       owned=owned, breaker=breaker, wait=lambda s: None,
                       backoff=lambda i: 5.0)
    rec = ledger.owned(owned.key)
    paths = [(d["path"], d["breaker"]) for d in rec["decided"]]
    # retried while closed; with the breaker open the next choice is a
    # different path, named, and the side is not tried again in a loop
    assert paths[0] == ("retry", "closed")
    assert paths[1:] and all(p == ("pause", "open") for p in paths[1:])
    assert len(calls) == 1 and rec["decided"][1]["class"] == "connectivity"


def test_the_tries_a_unit_may_make_are_what_its_forecast_pays_for():
    import inspect
    r = tuning.Retune("t", rows=100, nbytes=10 ** 6)
    unmeasured = r.tries_paid("u", 100, 1000)
    assert unmeasured == inspect.signature(
        faults.retried).parameters["tries"].default
    r.succeeded("a", 100, 100 * 50, 0.5)
    # 1,000 rows left is worth 11 tries, but no recovery was measured: the
    # catalogue's default bounds them, never the work remaining
    assert r.tries_paid("u", 100, 1000, width=50) == unmeasured
    assert r.tries_paid("u", 100, 50, width=50) == 2   # one try + one more
    eta, took = r.bounds[1]["eta"], r.bounds[1]["took"]
    assert 1 + round(eta / took) == 11 and r.bounds[1]["rate"] == r.rate
    assert r.tries_paid("u", 100, 10 ** 6, width=50) == unmeasured


def test_more_tries_are_paid_only_by_a_measured_recovery_and_bounds_trim():
    import inspect
    default = inspect.signature(faults.retried).parameters["tries"].default
    r = tuning.Retune("t", rows=100, nbytes=10 ** 6)
    r.succeeded("a", 100, 100 * 50, 0.5)
    # a failure met and retuned, then the unit ended well: one recovery
    # measured of one failure met - the default may double, no further
    r.failed("b", RuntimeError("Got a packet bigger than"
                               " 'max_allowed_packet' bytes"),
             sent=10 ** 6, rows=100)
    assert r.open
    r.succeeded("b", 100, 100 * 50, 0.5)
    assert r.recoveries and r.recoveries[0]["unit"] == "b"
    got = r.tries_paid("u", 100, 10 ** 6, width=50)
    assert default < got <= 2 * default
    assert r.bounds[-1]["recovered"] == 1.0
    # a second failure that never recovered halves the share paid for
    r.failed("c", RuntimeError("Got a packet bigger than"
                               " 'max_allowed_packet' bytes"),
             sent=r.size["bytes"], rows=100)
    assert len(r.history) == 2 and len(r.recoveries) == 1
    assert r.tries_paid("u", 100, 10 ** 6, width=50) <= got
    for i in range(3 * r.BOUNDS_KEPT):
        r.tries_paid(f"u{i}", 100, 1000, width=50)
    assert len(r.bounds) == r.BOUNDS_KEPT
    assert r.bounds[-1]["unit"] == f"u{3 * r.BOUNDS_KEPT - 1}"


def test_a_write_that_left_nothing_after_a_refused_size_is_retuned(
        tmp_path):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    r = tuning.Retune("t", rows=1000, nbytes=10 ** 6)
    target, sizes = [], []

    def write():
        s = r.settings()
        sizes.append(s["bytes"])
        if len(sizes) == 1:
            raise faults.Injected("packet")
        target.extend(range(10))
        return "written"
    owned = faults.Owned("dst", "copy batch", "t", "0")
    got = tuning.sized(r, "u", lambda: faults.retried(
        write, writes=True, side="dst", ledger=ledger, owned=owned,
        wait=lambda s: None, check=faults.landed(lambda: 10,
                                                 lambda: len(target),
                                                 idempotent=True)),
        lambda s: None, rows=lambda: 10,
        reconcile=lambda u: len(target))
    assert got == "written" and len(target) == 10
    # the size that failed was never sent again
    assert sizes[1] < sizes[0] and len(sizes) == 2
    assert [d["path"] for d in ledger.owned(owned.key)["decided"]][0] \
        in ("replan", "stop", "reconcile")


def test_the_tails_read_is_an_owned_operation_read_again_where_lost_says(
        tmp_path):
    from migkit import tailctl
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    asked = []

    def look(token):
        asked.append(token)
        if len(asked) == 1:
            raise ConnectionResetError("connection reset by peer")
        return token + 1, 1, True
    seen, ended = tailctl.run_loop(
        tmp_path, "db", look, lambda m: None, "changes", go=False,
        token=0, lost=lambda e, t: t + 100, reached=lambda t: True,
        until_="caught_up", ledger=ledger)
    assert (seen, ended) == (1, "caught up")
    # read again from where a lost link resumes, not the stale token
    assert asked == [0, 100]
    rec = ledger.owned(faults.Owned("src", "tail read", "db", "0").key)
    assert rec["state"] == "done" and rec["tries"] == 2
    assert rec["decided"][0]["path"] == "retry"


def test_a_reconciled_failure_keeps_one_decision_with_what_followed(
        tmp_path):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    target, writes = [], []

    def write():
        writes.append(1)
        if len(writes) == 1:
            raise Lost()
        target.extend(range(5))
        return "written"
    owned = faults.Owned("dst", "copy batch", "t", "0")
    got = faults.retried(write, writes=True, side="dst", owned=owned,
                         ledger=ledger, wait=lambda s: None,
                         check=faults.landed(lambda: 5, lambda: len(target),
                                             before=0))
    assert got == "written" and len(writes) == 2
    kept = ledger.owned(owned.key)["decided"]
    # one failure, one decision: the reconcile's answer and the choice it
    # led to are on it, not a second entry with the same tries left
    assert len(kept) == 1, kept
    assert kept[0]["path"] == "reconcile"
    assert kept[0]["reconciled"] == faults.NOTHING and kept[0]["then"]


def test_an_open_breaker_that_stops_names_it_on_the_raised_error(
        tmp_path, monkeypatch):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    breaker = faults.Breaker()
    breaker.failed("src", 0)
    real = decide.recovery

    def recovery(facts):
        got = real(facts)
        if facts["breaker"] == "open":
            # the side keeps failing and no wait answers it: what the
            # caller is handed must name the path taken
            return decide.climb("recovery", (decide.Rung(
                "isolate", "isolate the side", because="it keeps failing"),),
                facts)
        return got
    monkeypatch.setattr(decide, "recovery", recovery)

    def read():
        raise faults.Injected("connectivity")
    owned = faults.Owned("src", "read range", "t", "0..10@None")
    with pytest.raises(faults.Injected) as e:
        faults.retried(read, tries=3, side="src", ledger=ledger,
                       owned=owned, breaker=breaker, wait=lambda s: None,
                       backoff=lambda i: 5.0)
    assert e.value.recovery == "isolate"
    assert e.value.fault == "connectivity"
    paths = [d["path"] for d in ledger.owned(owned.key)["decided"]]
    assert paths == ["retry", "isolate"]


def test_a_lock_is_told_by_its_code_and_its_words():
    for err in (Exception(1205, "Lock wait timeout exceeded; try"
                                " restarting transaction"),
                Exception(1213, "Deadlock found when trying to get lock"),
                RuntimeError("ERROR: deadlock detected"),
                RuntimeError("Transaction (Process ID 61) was deadlocked"
                             " on lock resources with another process and"
                             " has been chosen as the deadlock victim")):
        assert faults.classify(err).fault.name == "lock", err
    assert "isolate" in faults.CATALOGUE["lock"].admits
    assert faults.CATALOGUE["lock"].transient


def test_an_open_breaker_isolates_a_unit_held_by_a_lock(tmp_path):
    ledger = attempts.Ledger(str(tmp_path / "attempts.json"))
    breaker = faults.Breaker()
    breaker.failed("dst", 0)    # the side failed past what one op may try
    calls = []

    def write():
        calls.append(1)
        raise faults.Injected("lock")
    owned = faults.Owned("dst", "copy batch", "t", "0..10@None")
    with pytest.raises(faults.Injected) as got:
        faults.retried(write, tries=3, side="dst", writes=True,
                       atomic=True, ledger=ledger, owned=owned,
                       breaker=breaker, wait=lambda s: None,
                       backoff=lambda i: 5.0)
    rec = ledger.owned(owned.key)
    paths = [(d["path"], d["breaker"]) for d in rec["decided"]]
    # retried while the side answered; once its breaker is open the same
    # lock is not met again on it: the unit is set aside, the rest go on
    assert paths == [("retry", "closed"), ("isolate", "open")], paths
    assert got.value.recovery == "isolate" and got.value.fault == "lock"
    assert len(calls) == 1


def test_an_open_breaker_withholds_only_the_retry():
    told = faults.classify(faults.Injected("connectivity"))
    shut = faults._facts(told, None, None, None, 2, "closed")
    shown = faults._facts(told, None, None, None, 2, "open")
    assert "retry" in shut["admits"] and "retry" not in shown["admits"]
    assert decide.recovery(shown).path == "pause"


def test_the_tails_lost_link_wait_is_costed_from_measured_reconnects():
    s, got = decide.lost_link_wait([2.5, 4.0], down=1.0)
    # the longest reconnect measured, less what this loss has waited
    assert got.path == "measured" and s == pytest.approx(3.0)
    assert "2.5s, 4.0s" in got.reason and "1.0s" in got.reason
    # outlasted every one measured, or none measured: as long again
    s, got = decide.lost_link_wait([2.0], down=3.0)
    assert got.path == "as long again" and s == pytest.approx(3.0)
    s, got = decide.lost_link_wait([], down=1.5)
    assert got.path == "as long again" and s == pytest.approx(1.5)
    # the facts it was costed on are the measured times and this loss's
    assert got.asked == ("reconnects", "down")
    s, got = decide.lost_link_wait((), down=0.0)
    assert got.path == "at once" and s == 0.0


def test_a_tail_waits_what_the_decision_chose_never_a_constant(
        tmp_path, monkeypatch):
    from migkit import tailctl
    from tests.test_every_tail_goes_round_one_loop import _hop, _Looks
    naps, asked = [], []
    real = decide.lost_link_wait

    def spy(reconnects, down=0.0):
        got = real(reconnects, down)
        asked.append((tuple(reconnects), down, got[0]))
        return got
    monkeypatch.setattr(decide, "lost_link_wait", spy)
    monkeypatch.setattr(tailctl, "_nap", lambda where, s, *a: naps.append(s))
    looks = _Looks(tmp_path, last=1,
                   raise_at={k: "connection refused" for k in range(1, 5)})
    tailctl.run_loop(tmp_path, "d", looks, lambda s: None, "changes",
                     hop=_hop(tmp_path, follow_until="caught_up"),
                     token=0, save=lambda a: None)
    assert len(asked) == 1 and naps[-1] == asked[0][2]
    # nothing came back yet: the wait is this loss's own length, measured
    assert asked[0][0] == () and asked[0][2] == pytest.approx(asked[0][1])


def test_a_pairs_inner_engines_say_their_side():
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="10.0.0.5", port=1, user="u", password="CHANGE_ME")
    e = HeteroEngine(Hop(name="p", engine="hetero", source=ep, target=ep,
                         options={"source_engine": "mssql",
                                  "target_engine": "mssql"}))
    # one engine both sides: the options alone would name both "src"
    assert attempts.pair_side(e.src_engine) == "src"
    assert attempts.pair_side(e.dst_engine) == "dst"


def test_the_sql_server_row_text_check_keeps_its_ranges(tmp_path,
                                                        monkeypatch):
    from types import SimpleNamespace

    from migkit import scanplan
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine.__new__(MSSQLEngine)
    eng.hop = SimpleNamespace(report_dir=lambda s: tmp_path)

    def cmd(side, db, sql):
        if sql == "cq":
            raise RuntimeError("the server cannot fold its rows")
        return [["1 a"], ["2 b"]]
    eng._cmd = cmd
    eng.scan_cuts = lambda db, t, most: [""]
    eng._qualified = lambda side, db, t: t
    monkeypatch.setattr(scanplan, "plan_most", lambda e, db: 1)
    seen = {}
    real = scanplan.row_text

    def spy(read, preds, **kw):
        seen.update(kw)
        return real(read, preds, key_width=kw["key_width"], most=1)
    monkeypatch.setattr(scanplan, "row_text", spy)
    got = eng._row_parity("db", "dbo.t", "row", "cq", "")
    assert got[0] == got[1]
    assert seen["table"] == "db.dbo.t" and seen["eng"] is eng
    assert isinstance(seen["kept"], scanplan.RangeTexts)
    assert seen["kept"].path == tmp_path / "row-text-ranges.json"


def test_a_sql_server_statement_refused_for_its_size_is_retuned(
        monkeypatch):
    import contextlib

    from migkit import failpoint
    from migkit.engines.mssql import MSSQLEngine
    failpoint.reset()
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mssql.rows:2:fault=packet")
    eng = MSSQLEngine.__new__(MSSQLEngine)
    eng.hop = None
    units, written = [], []

    def unit(kind, name, level, parent, parts):
        units.append(name)
        return contextlib.nullcontext()
    eng.work_unit = unit
    real = tuning.Retune.__init__
    kept = {}

    def small(self, work, *a, **kw):
        kw["nbytes"] = 40_000
        real(self, work, *a, **kw)
        kept["r"] = self
    monkeypatch.setattr(tuning.Retune, "__init__", small)
    rows = [[i, "x" * 300] for i in range(1, 201)]
    got = eng._statements_sized("dst", "db", "dbo.t", [], rows,
                                lambda s, d, t, c, part: written.extend(
                                    r[0] for r in part))
    assert got == 200 and written == list(range(1, 201))
    r = kept["r"]
    assert len(r.history) == 1
    assert r.history[0].after["bytes"] < r.history[0].before["bytes"]
    # the part committed before the refusal is kept; the rest went again
    # from there, in smaller parts, each a unit of the ledger
    first = int(units[0].split("..")[1])
    assert units[1].startswith(f"dbo.t rows {first + 1}..")
    assert int(units[2].split("..")[1]) - first < first
