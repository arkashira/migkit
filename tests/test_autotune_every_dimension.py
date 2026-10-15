"""Every tunable dimension inventoried, bounded by facts, and retuned from
the cause a unit failed for (backlog P0-AUTOTUNE-ALL).

The servers here are fakes that refuse the way the real ones were seen
to: MySQL a statement past `max_allowed_packet` (or a lost connection with
no word of it), PostgreSQL an allocation past its memory and a statement
past `statement_timeout`. Each scenario checks the cause is named, the
work is never run again as it failed, every value arrives whole and the
target is counted whole before the tuning is called proved. What the
sandbox's own servers have not yet shown is `tuning.SANDBOX_OWED`.
"""
import pytest

from migkit import decide, pieces, tuning
from migkit.tuning import (DIMENSIONS, INVARIANTS, LOCAL, OWNERS, TUNABLES,
                           UNPROVED, Retune)

KIB, MIB = 1024, 1024 * 1024
#: the real `observe`, before the fixture holds what a run keeps
_OBSERVE = tuning.observe


@pytest.fixture(autouse=True)
def _kept(monkeypatch):
    """What a run would keep for this machine, held in the test."""
    seen = []
    monkeypatch.setattr(tuning, "observe",
                        lambda name, measured, hop=None, weight=1.0:
                        seen.append((name, measured)))
    return seen


class FakeTarget:
    """A target that refuses a batch past its packet, memory or time, and
    keeps a batch whole or not at all."""

    def __init__(self, packet=None, memory=None, most_rows=None,
                 packet_words="Got a packet bigger than 'max_allowed_packet'"
                              " bytes (1153)"):
        self.packet, self.memory, self.most_rows = packet, memory, most_rows
        self.words = packet_words
        self.rows, self.tries = {}, []

    def write(self, name, batch, s):
        at = len(self.rows.get(name, ()))
        self.tries.append((name, at, len(batch), s["way"], s["piece"]))
        if self.most_rows and len(batch) > self.most_rows:
            raise RuntimeError("ERROR: canceling statement due to statement"
                               " timeout (57014)")
        held = sum(pieces.row_bytes(r) for r in batch)
        if self.memory and held > self.memory:
            raise RuntimeError("ERROR: invalid memory alloc request size"
                               f" {held}")
        if s["way"] == "pieces":
            for _, value in batch:
                for start in range(0, max(len(value), 1), s["piece"]):
                    part = value[start:start + s["piece"]]
                    self._send(len(part) * pieces.SENT_FACTOR)
        else:
            self._send(held * pieces.SENT_FACTOR)
        self.rows.setdefault(name, []).extend(
            (k, bytes(v)) for k, v in batch)

    def _send(self, n):
        if self.packet and n + pieces.HEADROOM > self.packet:
            raise RuntimeError(self.words)

    def held(self, name):
        return len(self.rows.get(name, ()))


def _source(widths):
    return [(i, bytes([65 + i % 26]) * w) for i, w in enumerate(widths)]


def _never_again_as_failed(target):
    """No batch was sent again exactly as the one before it."""
    for a, b in zip(target.tries, target.tries[1:]):
        if a[:2] == b[:2]:
            assert a != b, (a, b)


def _whole(src, target):
    for name, rows in src.items():
        assert target.rows[name] == [(k, bytes(v)) for k, v in rows], name


# --- the inventory ---------------------------------------------------------

def test_every_dimension_the_owner_named_is_inventoried():
    for name in ("large values", "payload and statement", "batch",
                 "partitions and ranges", "concurrency", "queues and memory",
                 "timeouts and retries", "flow control",
                 "transport and compression", "conversion and validation",
                 "consistency boundary", "session settings"):
        d = DIMENSIONS[name]
        assert d.keeps and d.decided_by and d.owner in OWNERS, name


def test_every_tunable_says_its_facts_guarantee_owner_and_proof():
    tests = {n for n in globals() if n.startswith("test_")}
    for name, t in TUNABLES.items():
        assert t.dimension in DIMENSIONS, name
        assert t.keeps and t.owner in OWNERS, name
        assert t.bounds[0] < 1.0 < t.bounds[1], name
        assert name not in INVARIANTS, name
        if t.scenario:
            assert t.scenario in tests, (name, t.scenario)
        else:
            assert t.dimension in UNPROVED or DIMENSIONS[
                t.dimension].scenario, name
    for d in DIMENSIONS.values():
        assert (d.scenario in tests) if d.scenario else d.name in UNPROVED
    assert DIMENSIONS["payload and statement"].tunables == (
        "pieces.BATCH_BYTES",)


def test_a_correctness_bound_is_never_tuned():
    with pytest.raises(ValueError, match="correctness"):
        _OBSERVE("pieces.SENT_FACTOR", 3)


def test_a_global_setting_waits_for_authorization():
    assert tuning.may_set("postgres", "statement_timeout") is None
    why = tuning.may_set("mysql", "max_allowed_packet")
    assert "authoriz" in why
    assert tuning.may_set("mysql", "max_allowed_packet",
                          authorized=("max_allowed_packet",)) is None
    assert "not a setting" in tuning.may_set("mysql", "no_such")
    assert tuning.setting("postgres", "wal_sender_timeout").scope == \
        tuning.GLOBAL
    assert LOCAL in OWNERS


# --- a large value's bound -------------------------------------------------

def test_a_sampled_largest_certifies_nothing():
    got = tuning.payload_bound("mysql", "json",
                               {"largest": 900, "exact": False})
    assert not got.certified and "sample" in got.said
    sampled = tuning.payload_bound("mysql", "longblob",
                                   {"largest": 900, "exact": False})
    assert sampled.certified and sampled.most == 4294967295
    assert "certifies nothing" in sampled.said


def test_every_row_measured_is_an_exact_bound_and_expansion_counts():
    got = tuning.payload_bound("mysql", "mediumblob",
                               {"largest": 3 * MIB, "exact": True})
    assert got.certified and got.most == 3 * MIB
    assert got.sent == 3 * MIB * pieces.SENT_FACTOR
    assert tuning.payload_bound("mysql", "varchar(100)").most == 400
    jsonb = tuning.payload_bound("postgres", "jsonb",
                                 {"largest": 10, "exact": True})
    assert jsonb.most == 1 << 30
    assert jsonb.sent == (1 << 30) * pieces.SENT_FACTOR * \
        tuning.JSON_EXPANSION


def test_a_value_past_the_packet_goes_in_pieces_or_is_blocked_whole():
    packet = pieces.Limit("max_allowed_packet", 4 * MIB, "sent")
    bound = tuning.payload_bound("mysql", "mediumblob",
                                 {"largest": 3 * MIB, "exact": True})
    assert tuning.payload_way(bound, [packet], appendable=True).path == \
        "pieces"
    small = tuning.payload_bound("mysql", "mediumblob",
                                 {"largest": KIB, "exact": True})
    assert tuning.payload_way(small, [packet]).path == "whole"
    with pytest.raises(decide.Blocked) as no:
        tuning.payload_way(bound, [packet], engine="mysql")
    said = str(no.value)
    assert "max_allowed_packet" in said and "authoriz" in said
    assert "Nothing is cut short" in said


# --- causes ----------------------------------------------------------------

def test_a_cause_is_read_from_what_the_server_said():
    packet = pieces.Limit("max_allowed_packet", MIB, "sent")
    assert tuning.cause_of("Lost connection to MySQL server during query",
                           sent=2 * MIB, limits=[packet]).kind == \
        tuning.PACKET
    assert tuning.cause_of("Lost connection to MySQL server during query"
                           ).kind == tuning.CONNECTION
    assert tuning.cause_of("canceling statement due to statement timeout"
                           ).kind == tuning.TIMEOUT
    assert tuning.cause_of("Lock wait timeout exceeded (1205)").kind == \
        tuning.LOCK
    assert tuning.cause_of(MemoryError()).kind == tuning.MEMORY
    assert not tuning.cause_of("connection reset by peer").sizing


def test_a_cause_no_size_answers_is_left_to_the_retry():
    r = Retune("t", rows=10, nbytes=MIB)
    assert r.failed("u", "connection reset by peer", sent=KIB,
                    rows=10) is None
    assert r.settings()["rows"] == 10


def test_one_row_past_its_time_is_blocked_not_run_again():
    r = Retune("t", rows=1, nbytes=MIB)
    with pytest.raises(decide.Blocked, match="nothing that answers it"):
        r.failed("u", "statement timeout", sent=KIB, rows=1)


# --- the scenarios ---------------------------------------------------------

def test_a_mis_sized_large_value_on_mysql_goes_in_pieces():
    """The target's packet is 1 MiB and was never read; values run to
    3 MiB. Batches shrink, then the rows past the packet alone go in
    pieces - and every value arrives whole."""
    src = {f"r{n}": _source([10 * KIB] * 8 + [3 * MIB] + [10 * KIB] * 8)
           for n in range(3)}
    target = FakeTarget(packet=MIB)
    r = Retune("t.blobs", rows=1000, nbytes=64 * MIB, appendable=True,
               engine="mysql")
    got = tuning.run(r, src, target.write, target.held, target.held)
    _whole(src, target)
    _never_again_as_failed(target)
    assert {a.cause.kind for a in r.history} == {tuning.PACKET}
    assert "pieces" in {a.rung for a in r.history}
    assert r.settings()["piece"] * pieces.SENT_FACTOR + pieces.HEADROOM \
        <= MIB
    assert r.verified and all(v.ok for v in r.verified)
    assert all(a.before != a.after for a in r.history)
    assert all(v is None or v.ok for v in got.values())
    assert r.units["r0"].attempts > 1 and r.units["r0"].state == "done"


def test_a_lost_connection_past_a_known_packet_is_the_packet():
    """MySQL 8.4 said nothing of the packet: the limit read off the server
    says it, and the batch is cut under that limit before anything runs."""
    src = {"r": _source([200 * KIB] * 20)}
    packet = pieces.Limit("max_allowed_packet", MIB, "sent")
    target = FakeTarget(packet=MIB, packet_words="Lost connection to MySQL"
                        " server during query (2013)")
    r = Retune("t.blobs", rows=1000, nbytes=8 * MIB, limits=[packet],
               engine="mysql")
    assert r.settings()["bytes"] * pieces.SENT_FACTOR + pieces.HEADROOM \
        <= MIB
    tuning.run(r, src, target.write, target.held, target.held)
    _whole(src, target)
    assert all(a.cause.kind == tuning.PACKET for a in r.history)


def test_a_packet_refusal_halves_the_batch_and_completes(_kept):
    src = {"r": _source([100 * KIB] * 30)}
    target = FakeTarget(packet=MIB)
    r = Retune("t", rows=1000, nbytes=8 * MIB, engine="mysql")
    tuning.run(r, src, target.write, target.held, target.held)
    _whole(src, target)
    _never_again_as_failed(target)
    assert r.history and r.history[0].rung == "smaller batch"
    assert r.settings()["way"] == "whole"
    assert r.ceiling["bytes"] < 8 * MIB
    assert ("pieces.BATCH_BYTES", r.settings()["bytes"]) in _kept


def test_a_failed_unit_goes_on_from_what_the_target_holds():
    """Eight small rows land, the next batch is refused: the unit goes on
    from row eight, and the rows already held are never written again."""
    src = {"r": _source([KIB] * 10 + [100 * KIB] * 20)}
    target = FakeTarget(packet=MIB)
    r = Retune("t", rows=8, nbytes=8 * MIB, engine="mysql")
    tuning.run(r, src, target.write, target.held, target.held)
    assert [a.resume_from for a in r.history][0] == 8
    first = [t for t in target.tries if t[1] == 0]
    assert len(first) == 1
    after = target.tries[target.tries.index(first[0]) + 1:]
    assert after and all(t[1] >= 8 for t in after)
    _whole(src, target)


def test_a_lost_connection_is_retuned_as_the_packet_it_was():
    packet = pieces.Limit("max_allowed_packet", MIB, "sent")
    r = Retune("t", rows=1000, nbytes=8 * MIB, limits=[packet],
               engine="mysql")
    adj = r.failed("u", "Lost connection to MySQL server during query"
                   " (2013)", sent=MIB, rows=4)
    assert adj.cause.kind == tuning.PACKET and adj.rung == "smaller batch"
    assert adj.after["bytes"] < MIB and adj.after["rows"] < 4


def test_memory_pressure_on_postgres_cuts_the_batch_bytes():
    src = {f"p{n}": _source([600 * KIB] * 12) for n in range(2)}
    target = FakeTarget(memory=2 * MIB)
    r = Retune("t.docs", rows=500, nbytes=32 * MIB, engine="postgres")
    tuning.run(r, src, target.write, target.held, target.held)
    _whole(src, target)
    _never_again_as_failed(target)
    assert {a.cause.kind for a in r.history} == {tuning.MEMORY}
    assert r.settings()["bytes"] <= 2 * MIB


def test_a_timeout_on_postgres_cuts_the_batch_not_the_rows(_kept):
    src = {"p": _source([100] * 500)}
    target = FakeTarget(most_rows=50)
    r = Retune("t", rows=400, nbytes=64 * MIB, engine="postgres")
    tuning.run(r, src, target.write, target.held, target.held)
    _whole(src, target)
    _never_again_as_failed(target)
    assert {a.cause.kind for a in r.history} == {tuning.TIMEOUT}
    assert r.settings()["rows"] <= 50 and r.ceiling["rows"] > 50
    assert ("pieces.PAIR_ROWS", r.settings()["rows"]) in _kept


def test_a_tuning_is_not_proved_until_the_target_is_counted_whole():
    r = Retune("t", rows=100, nbytes=MIB)
    r.failed("u", "statement timeout", sent=KIB, rows=100)
    got = r.succeeded("u", 100, KIB, 1.0, expected=100, counted=99)
    assert got is not None and not got.ok and "not counted whole" in got.why
    r.failed("v", "statement timeout", sent=KIB, rows=50)
    assert not r.succeeded("v", 50, KIB, 1.0).ok


def test_a_skewed_range_is_cut_where_it_is_slow():
    r = Retune("t", rows=100, nbytes=MIB)
    done = {"a": (1000, 1.0), "b": (1100, 1.1), "c": (900, 0.9)}
    got = r.rebalance(done, {"d": 1000, "e": 10000, "f": 1500})
    assert got == {"e": 10}
    assert r.rebalance({"a": (1000, 1.0)}, {"e": 10000}) == {}


def test_the_sizes_settle_instead_of_swinging():
    """The true limit sits under the prior: the sizes fall, grow back only
    after quiet units, never reach a size that failed, and a growth that
    fails again doubles the wait - a few failures, then none."""
    limit = 600_000
    r = Retune("t", rows=1000, nbytes=1_000_000)
    failures, changes, sizes = 0, [], []
    for i in range(200):
        s = r.settings()
        sizes.append(s["bytes"])
        if s["bytes"] > limit:
            failures += 1
            r.failed(f"u{i}", "packet bigger than max_allowed_packet",
                     sent=s["bytes"], rows=s["rows"])
        else:
            r.succeeded(f"u{i}", s["rows"], s["bytes"], 1.0,
                        expected=s["rows"], counted=s["rows"])
        if i and sizes[-1] != sizes[-2]:
            changes.append(i)
    assert 1 <= failures <= 3
    grew = [(a, b) for a, b in zip(changes, changes[1:])
            if sizes[b] > sizes[b - 1]]
    assert grew and all(b - a >= Retune.STEADY for a, b in grew)
    assert sizes[-1] <= limit and len(set(sizes[-100:])) == 1
    assert r.steady > Retune.STEADY
    assert all(x < r.ceiling["bytes"] for x in sizes[-100:])
