"""A tail batch goes to a PostgreSQL target as typed columns, the change
records only where the columns cannot carry it exactly; the write way of
each run is forecast from its width and the target's measured round trip;
the log is read the way that measured faster on the slot itself, paced
by the measured change rate; and the position moves only after the batch
is on the target and its unit closed in the ledger. Fakes only."""
import contextlib

import pytest

from migkit import attempts, decide, faults, pgslot, tailctl
from migkit.engines.postgres import PostgresEngine

pa = pytest.importorskip("pyarrow")

PROVES = [("tail.follows", "postgres")]


def _ch(table, op, key, values=None, **kw):
    return {"table": table, "op": op, "key": key, "values": values or {},
            **kw}


def _batch():
    return [_ch("t", "insert", {"id": 1}, {"id": 1, "v": "a"}),
            _ch("t", "insert", {"id": 2}, {"id": 2, "v": "b"}),
            _ch("t", "delete", {"id": 1}),
            _ch("t", "insert", {"id": 1}, {"id": 1, "v": "c"}),
            _ch("u", "update", {"k": "x"}, {"k": "x", "n": 5})]


def test_ordered_columns_keep_the_changes_order_and_their_values():
    runs = pgslot.columnar(_batch(), ordered=True)
    assert [(t, o, b.num_rows) for t, o, b in runs] == [
        ("t", "insert", 2), ("t", "delete", 1), ("t", "insert", 1),
        ("u", "update", 1)]
    # canon's records keep the row under "values": carried, not dropped
    assert runs[0][2].column("row.v").to_pylist() == ["a", "b"]
    assert runs[2][2].column("row.v").to_pylist() == ["c"]
    assert pgslot.exact(_batch(), runs) is None


def test_a_key_seen_again_ends_the_run():
    twice = [_ch("t", "update", {"id": 1}, {"v": 1}),
             _ch("t", "update", {"id": 1}, {"v": 2})]
    runs = pgslot.columnar(twice, ordered=True)
    assert [b.num_rows for _, _, b in runs] == [1, 1]
    assert runs[1][2].column("row.v").to_pylist() == [2]


def test_a_value_the_columns_would_change_is_refused():
    mixed = [_ch("t", "insert", {"id": 1}, {"v": 1}),
             _ch("t", "insert", {"id": 2}, {"v": 1.5})]
    runs = pgslot.columnar(mixed, ordered=True)
    # Arrow types the column double: the integer would land as 1.0
    assert "not typed exactly" in pgslot.exact(mixed, runs)
    moved = [_ch("t", "update", {"id": 2}, {"v": 1}, before={"id": 1})]
    assert "moved a row's key" in pgslot.exact(
        moved, pgslot.columnar(moved, ordered=True))


class _Target:
    LANES_FROM = 2000

    def __init__(self, columns=True, refuse=False):
        self.rows, self.calls, self.refuse = {}, [], refuse
        self.ledger = attempts.Ledger()
        if not columns:
            self.apply_columns = None

    def attempts(self):
        return self

    def apply_columns(self, side, db, batches):
        if self.refuse:
            raise tailctl.NotColumns("refused before writing")
        self.calls.append("columns")
        n = 0
        for table, op, batch in batches:
            for r in batch.to_pylist():
                key = (table, tuple(sorted((k, v) for k, v in r.items()
                                           if k.startswith("key."))))
                if op == "delete":
                    self.rows.pop(key, None)
                else:
                    self.rows[key] = {k[4:]: v for k, v in r.items()
                                      if k.startswith("row.")}
                n += 1
        return n

    def records(self, changes):
        self.calls.append("records")
        for c in changes:
            key = (c["table"], tuple(sorted(
                (f"key.{k}", v) for k, v in c["key"].items())))
            if c["op"] == "delete":
                self.rows.pop(key, None)
            else:
                self.rows[key] = dict(c["values"])
        return len(changes)


def test_the_unit_hands_the_target_columns_when_it_writes_them():
    eng = _Target()
    assert tailctl.apply_unit(eng, "d", "0/10", _batch(), eng.records) == 5
    assert eng.calls == ["columns"]
    assert eng.rows[("t", (("key.id", 1),))] == {"id": 1, "v": "c"}
    assert eng.rows[("t", (("key.id", 2),))] == {"id": 2, "v": "b"}


def test_records_are_the_fallback_rung():
    for eng in (_Target(columns=False), _Target(refuse=True)):
        tailctl.apply_unit(eng, "d", "0/10", _batch(), eng.records)
        assert eng.calls == ["records"]
        assert eng.rows[("t", (("key.id", 1),))] == {"id": 1, "v": "c"}
    eng = _Target()
    big = [_ch("t", "insert", {"id": 2 ** 70}, {"id": 2 ** 70})]
    tailctl.apply_unit(eng, "d", "0/10", big, eng.records)
    assert eng.calls == ["records"]
    choice = decide.climb("applying a tail batch", decide.TAIL_APPLIES,
                          tailctl._columns(eng, big)[1])
    assert choice.path == "records"


def test_the_position_moves_only_after_the_batch_is_in_the_ledger(
        monkeypatch, tmp_path):
    eng, saved = _Target(), []
    changes = _batch()

    def look(token):
        tailctl.apply_unit(eng, "d", "0/20", changes, eng.records)
        return "0/20", len(changes), True

    def run():
        return tailctl.run_loop(tmp_path, "d", look, lambda s: None,
                                "changes", go=True, token="0/10",
                                save=saved.append, until_="caught_up",
                                reached=lambda t: True)
    owned = faults.Owned("dst", "tail batch", "d", '"0/20"').key
    # the target committed, the fault lands before the ledger closes it
    monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.unit_applied:1+:raise")
    with pytest.raises(RuntimeError, match="tail.unit_applied"):
        run()
    assert saved == []
    assert eng.ledger.owned(owned).get("state") != "done"
    once = dict(eng.rows)
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    assert run() == (5, "caught up")
    # replayed from the saved position: the same rows, never doubled, and
    # the position saved once the unit was closed
    assert eng.rows == once
    assert eng.ledger.owned(owned).get("state") == "done"
    assert saved == ["0/20"]


def _pg():
    eng = PostgresEngine.__new__(PostgresEngine)
    eng.written = []
    eng._apply_session = lambda side, db: contextlib.nullcontext()
    eng._answered = lambda: None
    eng.named_write = lambda *a, **k: None
    eng._apply_upserts = lambda side, db, table, rows, way=None: \
        eng.written.append(("upsert", table, rows, way))
    eng._apply_deletes = lambda side, db, table, keys: \
        eng.written.append(("delete", table, keys, None))
    return eng


def test_the_postgres_writer_applies_runs_in_order_and_tries_each_way():
    eng = _pg()
    eng._latency = lambda side, db: 0.01
    runs = pgslot.columnar(_batch(), ordered=True)
    assert eng.apply_columns("dst", "d", runs) == 5
    assert [(w[0], w[1]) for w in eng.written] == [
        ("upsert", "t"), ("delete", "t"), ("upsert", "t"), ("upsert", "u")]
    assert eng.written[0][2] == [({"id": 1}, {"id": 1, "v": "a"}),
                                 ({"id": 2}, {"id": 2, "v": "b"})]
    assert eng.written[1][2] == [{"id": 1}]
    # neither measured: the one that leaves no stage behind, then the
    # other once, then the forecast - here one round trip against two
    assert [w[3] for w in eng.written if w[0] == "upsert"] == [
        "statements", "copy", "statements"]
    assert set(eng._column_rates[("dst", "d", "t")]) == {
        "copy", "statements"}


@pytest.mark.parametrize("rtt,copy_rate,stmt_rate,way", [
    # a long link and a narrow run: the stage's second round trip costs
    (0.05, 1e-9, 2e-9, "statements"),
    # a short link and wide rows the copy writes faster
    (0.0001, 1e-9, 5e-8, "copy")])
def test_the_write_way_is_forecast_from_width_and_latency(
        rtt, copy_rate, stmt_rate, way):
    eng = _pg()
    eng._latency = lambda side, db: rtt
    eng._column_rates = {("dst", "d", "t"): {"copy": copy_rate,
                                             "statements": stmt_rate}}
    rows = [_ch("t", "insert", {"id": i}, {"id": i, "v": "x" * 200})
            for i in range(500)]
    batch = pgslot.columnar(rows, ordered=True)[0][2]
    assert eng._column_way("dst", "d", "t", batch) == way


class _Stream:
    opened = []

    def __init__(self, connect, slot, start=None, options=None, pace=None):
        self.slot, self.start, self.closed, self.confirmed = slot, start, \
            False, []
        _Stream.opened.append(self)

    def rows(self, limit, wait):
        return [("0/1", "7", "BEGIN 7"), ("0/2", "7", "table t: INSERT"),
                ("0/3", "7", "COMMIT 7")]

    def confirm(self, lsn):
        self.confirmed.append(lsn)

    def close(self):
        self.closed = True


@pytest.mark.parametrize("peek_s,stream_s,way", [
    (0.5, 0.001, "stream"), (0.0001, 0.6, "poll")])
def test_the_read_is_the_way_measured_faster_on_the_slot(
        monkeypatch, peek_s, stream_s, way):
    pytest.importorskip("psycopg2.extras")
    clock = [0.0]
    monkeypatch.setattr("migkit.engines.postgres.time.monotonic",
                        lambda: clock[0])
    eng = PostgresEngine.__new__(PostgresEngine)

    def rows(side, db, sql, args=None):
        if "rolreplication" in sql:
            return [(True,)]
        clock[0] += peek_s
        return [("0/1",), ("0/2",), ("0/3",)]
    eng._slot_rows = rows

    class Timed(_Stream):
        def rows(self, limit, wait):
            clock[0] += stream_s
            return super().rows(limit, wait)
    monkeypatch.setattr(pgslot, "Stream", Timed)
    _Stream.opened = []
    choice = eng._change_read("src", "d", "s1", "0/1")
    assert choice.path == way
    # measured without moving the slot: never confirmed, let go
    assert [(s.slot, s.start, s.confirmed, s.closed)
            for s in _Stream.opened] == [("s1", "0/1", [], True)]


def test_an_idle_log_leaves_the_read_to_the_grant(monkeypatch):
    pytest.importorskip("psycopg2.extras")
    eng = PostgresEngine.__new__(PostgresEngine)
    eng._slot_rows = lambda side, db, sql, args=None: \
        [(True,)] if "rolreplication" in sql else []
    _Stream.opened = []
    monkeypatch.setattr(pgslot, "Stream", _Stream)
    assert eng._read_costs("src", "d", "s1", None) is None
    assert eng._change_read("src", "d", "s1", None).path == "stream"
    assert _Stream.opened == []


def test_the_stream_heartbeat_follows_the_measured_pace():
    clock = [0.0]
    pace = tailctl.Pace(clock=lambda: clock[0])
    assert pace.heartbeat() == pace.most
    for _ in range(5):
        clock[0] += 0.1
        pace.saw(100)
    assert pace.least <= pace.heartbeat() < pace.most

    class Cur:
        sent = []

        def send_feedback(self, **kw):
            Cur.sent.append(kw)
    s = pgslot.Stream.__new__(pgslot.Stream)
    s.cur, s.pace, s.said, s.confirmed = Cur(), pace, None, "0/10"
    s.beat(1.0)
    s.beat(1.0 + pace.heartbeat() / 2)
    s.beat(1.0 + pace.heartbeat() * 1.01)
    assert len(Cur.sent) == 2
    # the confirmed position, never anything read past it
    assert {k["flush_lsn"] for k in Cur.sent} == {pgslot.lsn_int("0/10")}
