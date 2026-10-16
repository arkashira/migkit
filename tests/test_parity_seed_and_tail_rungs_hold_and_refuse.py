"""The row-parity, Redis first-fill and quiet-tail rungs each held and
refused on the facts that decide them (backlog AUD-Z.rungs): a table is
proved by ranges only where a single integer key makes them the same rows
on both sides, a snapshot fill only where room and memory were read and
every change is carried, and a quiet tail is called idle or elsewhere only
past the measured write interval. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, LACKS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("ROW_PARITY", "server digest", "holds"),
    ("ROW_PARITY", "server digest", "refuses"),
    ("ROW_PARITY", "key-range digest", "holds"),
    ("ROW_PARITY", "key-range digest", "refuses"),
    ("ROW_PARITY", "row text", "holds"), ("ROW_PARITY", "row text", "refuses"),
    ("redis_seed", "snapshot", "holds"), ("redis_seed", "snapshot", "refuses"),
    ("redis_seed", "scan", "holds"),
    ("tail_state", "detached", "holds"), ("tail_state", "detached", "refuses"),
    ("tail_state", "elsewhere", "holds"),
    ("tail_state", "elsewhere", "refuses"),
    ("tail_state", "idle", "holds"), ("tail_state", "idle", "refuses"),
    ("tail_state", "following", "holds"),
]

RESUMED = decide.ROW_PARITY_NEED + ("resume by chunk",)


def _parity(facts, need=decide.ROW_PARITY_NEED):
    return decide.climb("t", decide.ROW_PARITY, decide.Facts(facts), need,
                        prove=False)


def test_a_resumed_check_goes_by_ranges_only_on_one_integer_key():
    with watched() as seen:
        got = _parity({"server digest": True, "key": "id"}, RESUMED)
    assert got.path == "key-range digest"
    assert branch(seen, "key-range digest") == [HOLDS]
    # the whole-table digest cannot keep a range: out before its needs
    assert branch(seen, "server digest") == [LACKS]
    with watched() as seen:
        got = _parity({"server digest": True, "key": None,
                       "rows read here": True}, RESUMED)
    assert got.path is None
    assert branch(seen, "key-range digest") == [REFUSES]
    assert "no single integer key" in why(seen, "key-range digest")


def test_a_server_that_cannot_fold_is_proved_by_rows_read_here():
    with watched() as seen:
        got = _parity({"server digest": True, "rows read here": True})
    assert branch(seen, "server digest") == [HOLDS]
    assert branch(seen, "row text") == [HOLDS]
    with watched() as seen:
        got = _parity({"server digest": False, "rows read here": True})
    assert branch(seen, "server digest") == [REFUSES]
    assert "cannot fold the row digest" in why(seen, "server digest")
    assert got.path == "row text"


def test_no_reader_and_no_digest_proves_nothing():
    with watched() as seen:
        got = _parity({"server digest": False, "rows read here": False})
    assert got.path is None
    assert branch(seen, "row text") == [REFUSES]
    assert why(seen, "row text") == ("no reader brings this table's rows"
                                     " here as canonical text")


ROOM = {"local free": 10 ** 9, "snapshot bytes": 10 ** 6,
        "local memory": 10 ** 9, "memory needs": 10 ** 6}


def test_a_snapshot_fill_needs_room_memory_read_and_every_change():
    with watched() as seen:
        got = decide.redis_seed(dict(ROOM))
    assert got.path == "snapshot"
    assert branch(seen, "snapshot") == [HOLDS]
    with watched() as seen:
        got = decide.redis_seed(dict(ROOM, **{"local free": 10}))
    assert got.path == "scan"
    assert branch(seen, "snapshot") == [REFUSES]
    assert "1,000,000 bytes kept here" in why(seen, "snapshot")
    assert branch(seen, "scan") == [HOLDS]
    # memory not read is not taken as enough
    with watched() as seen:
        got = decide.redis_seed(dict(ROOM, **{"local memory": None}))
    assert got.path == "scan"
    assert why(seen, "snapshot") == "this machine's free memory was not read"
    with watched() as seen:
        got = decide.redis_seed(dict(ROOM, **{"every change carried": False}))
    assert got.path == "scan"
    assert "keeps some of the source's changes off" in why(seen, "snapshot")


def test_a_reader_not_attached_is_said_before_any_quiet():
    with watched() as seen:
        got = decide.tail_state({"attached": False, "moved": 0,
                                 "quiet_for": 100, "interval": 10})
    assert got.path == "detached"
    assert branch(seen, "detached") == [HOLDS]
    with watched() as seen:
        got = decide.tail_state({"attached": None, "moved": 0,
                                 "quiet_for": 100, "interval": 10})
    # a reader that cannot be told is not called detached
    assert branch(seen, "detached") == [REFUSES]
    assert got.path == "idle"
    assert branch(seen, "idle") == [HOLDS]


def test_a_quiet_is_said_only_past_the_measured_interval():
    with watched() as seen:
        got = decide.tail_state({"attached": True, "moved": 500,
                                 "quiet_for": 100, "interval": 10})
    assert got.path == "elsewhere"
    assert branch(seen, "elsewhere") == [HOLDS]
    assert why(seen, "idle") == "the source's log moved or was not read"
    with watched() as seen:
        got = decide.tail_state({"attached": True, "moved": 500,
                                 "quiet_for": 5, "interval": 10})
    assert got.path == "following"
    assert why(seen, "elsewhere") == ("the quiet is within the measured"
                                      " interval")
    assert branch(seen, "following") == [HOLDS]


def test_a_log_not_read_is_neither_idle_nor_elsewhere():
    with watched() as seen:
        got = decide.tail_state({"attached": True, "moved": None,
                                 "quiet_for": 100, "interval": 10})
    assert got.path == "following"
    assert branch(seen, "elsewhere") == [REFUSES]
    assert branch(seen, "idle") == [REFUSES]
