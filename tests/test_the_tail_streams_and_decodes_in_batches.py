"""A tail reads its source's log as the server streams it where the grant
allows (`decide.change_read`), decodes a batch into typed columns two
ways that agree (`pgslot.columnar`), waits between reads at a pace the
change rate sets (`tailctl.Pace`), and applies each batch as a unit of
its own under the fault catalogue (`tailctl.apply_unit`). Fake servers
only: no database is reached."""
import pytest

from migkit import decide, faults, pgslot, tailctl
from migkit.attempts import Ledger


def test_the_stream_is_chosen_only_where_the_grant_allows():
    yes = decide.change_read({"stream client": True,
                              "stream granted": True})
    assert yes.path == "stream"
    no = decide.change_read({"stream client": True, "stream granted": False,
                             "stream why": "no REPLICATION"})
    assert no.path == "poll"
    assert "no REPLICATION" in no.reason
    assert decide.change_read({"stream client": False,
                               "stream granted": True}).path == "poll"


def test_lsns_go_both_ways():
    assert pgslot.lsn_text(pgslot.lsn_int("16/B374D848")) == "16/B374D848"
    assert pgslot.lsn_int("0/10") == 16


class _Msg:
    def __init__(self, lsn, payload):
        self.data_start, self.payload = pgslot.lsn_int(lsn), payload


class _Cursor:
    def __init__(self, msgs):
        self.msgs, self.fed, self.started = list(msgs), [], None
        self.wal_end = pgslot.lsn_int("0/900")

    def start_replication(self, **kw):
        self.started = kw

    def read_message(self):
        return self.msgs.pop(0) if self.msgs else None

    def send_feedback(self, flush_lsn=0, reply=False):
        self.fed.append(flush_lsn)


class _Conn:
    def __init__(self, cur):
        self.cur, self.closed = cur, False

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True


def _stream(msgs, start=None):
    cur = _Cursor(msgs)
    return pgslot.Stream(lambda: _Conn(cur), "s1", start), cur


def test_the_stream_hands_back_what_a_peek_would_and_cuts_at_a_commit():
    s, cur = _stream([_Msg("0/10", "BEGIN 7"),
                      _Msg("0/11", "table public.t: INSERT: id[integer]:1"),
                      _Msg("0/12", "table public.t: INSERT: id[integer]:2"),
                      _Msg("0/13", "COMMIT 7"),
                      _Msg("0/20", "BEGIN 8"),
                      _Msg("0/21", "table public.t: DELETE: id[integer]:1"),
                      _Msg("0/22", "COMMIT 8")], start="0/F")
    assert cur.started["start_lsn"] == pgslot.lsn_int("0/F")
    # the limit falls inside the first transaction: the batch ends at
    # its commit, not on half of it
    got = s.rows(2, 0, wait_for=lambda _: None)
    assert [r[0] for r in got] == ["0/10", "0/11", "0/12", "0/13"]
    assert {r[1] for r in got} == {"7"}
    rest = s.rows(100, 0, wait_for=lambda _: None)
    assert [r[2] for r in rest][-1] == "COMMIT 8" and rest[1][1] == "8"
    assert s.rows(100, 0, wait_for=lambda _: None) == []
    assert s.end == "0/900"


def test_the_slot_moves_only_by_what_was_confirmed_applied():
    s, cur = _stream([])
    s.rows(10, 0, wait_for=lambda _: None)
    assert cur.fed == []
    s.confirm("0/13")
    s.confirm("0/13")
    assert cur.fed == [pgslot.lsn_int("0/13")]


def test_a_stream_waits_for_a_transaction_to_end():
    msgs = [_Msg("0/10", "BEGIN 7"), _Msg("0/11", "table public.t: INSERT:"
                                                   " id[integer]:1")]
    s, cur = _stream(msgs)
    waited = []

    def wait_for(sec):
        waited.append(sec)
        cur.msgs.append(_Msg("0/12", "COMMIT 7"))
    got = s.rows(100, 0, wait_for=wait_for)
    assert waited and got[-1][2] == "COMMIT 7"


def _changes():
    return [{"table": "t", "op": "insert", "key": {"id": i},
             "row": {"id": i, "name": f"n{i}", "at": 1.5 * i}}
            for i in range(5)] + [
        {"table": "t", "op": "delete", "key": {"id": 9}, "row": None},
        {"table": "u", "op": "update", "key": {"k": "a"},
         "row": {"k": "a", "v": None}}]


def test_the_compiled_and_the_pure_columns_are_equal():
    pytest.importorskip("pyarrow")
    fast = pgslot.columnar(_changes())
    pure = pgslot.columnar(_changes(), compiled=False)
    assert len(fast) == len(pure) == 3
    for (t1, o1, b1), (t2, o2, b2) in zip(fast, pure):
        assert (t1, o1) == (t2, o2)
        assert b1.schema.equals(b2.schema)
        assert b1.equals(b2)
    inserts = fast[0][2]
    assert inserts.num_rows == 5
    assert str(inserts.schema.field("row.at").type) == "double"
    assert inserts.column("key.id").to_pylist() == [0, 1, 2, 3, 4]


def test_the_wait_follows_the_change_rate():
    now = [0.0]
    pace = tailctl.Pace(0.02, 1.0, clock=lambda: now[0])
    assert pace.wait() == 1.0
    for _ in range(5):
        now[0] += 0.1
        pace.saw(10)
    busy = pace.wait()
    assert 0.02 <= busy < 0.2
    for _ in range(30):
        now[0] += 1.0
        pace.saw(0)
    assert pace.wait() > busy


def test_a_batch_a_fault_cut_is_applied_again_and_converges(tmp_path):
    target, tries = {}, [0]

    def apply(changes):
        for c in changes:
            target[c["key"]["id"]] = c["row"]
            if tries[0] == 0 and c["key"]["id"] == 2:
                tries[0] += 1
                raise ConnectionResetError("connection reset by peer")
        return len(changes)

    class Eng:
        def attempts(self):
            return type("A", (), {"ledger": Ledger(tmp_path / "l.json")})()
    eng = Eng()
    changes = _changes()[:5]
    got = tailctl.apply_unit(eng, "db", {"log_pos": 4}, changes, apply)
    assert got == 5 and tries[0] == 1
    assert sorted(target) == [0, 1, 2, 3, 4]


def test_a_batch_that_names_no_row_is_not_applied_twice_on_a_guess(
        tmp_path):
    ledger = Ledger(tmp_path / "l.json")
    owned = faults.Owned("dst", "tail batch", "db", '{"log_pos": 4}')
    ledger.own(owned.key, state="began")

    class Eng:
        def attempts(self):
            return type("A", (), {"ledger": ledger})()
    ran = []
    with pytest.raises(RuntimeError):
        tailctl.apply_unit(Eng(), "db", {"log_pos": 4},
                           [{"table": "t", "op": "insert", "key": {},
                             "row": {"a": 1}}], ran.append)
    assert ran == []
