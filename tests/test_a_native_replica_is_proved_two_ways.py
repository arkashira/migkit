"""A native MySQL replica proved through the replication stream, and every
chunk held to migkit's own digest (backlog DEC.ptcs): both agreeing is
the proof, either disagreeing is a difference, a chunk read one way only
is not proved; the lag brake pauses migkit's own reading."""
from pathlib import Path

import pytest

from migkit import decide, throttle, tooluse
from migkit import replicasum as rs

FIXTURE = Path(__file__).parent / "data" / "replica_checksums.tsv"


def _chunks():
    out = []
    for line in FIXTURE.read_text().splitlines():
        if line.startswith("#"):
            continue
        n, idx, lo, hi, mc, tc, mcrc, tcrc = line.split("\t")
        out.append(rs.Chunk("app", "t", int(n), idx,
                            None if lo == "\\N" else lo,
                            None if hi == "\\N" else hi,
                            (int(mc), int(tc)), (mcrc, tcrc)))
    return out


EQ = ((1000, "x"), (1000, "x"))
NE = ((1000, "x"), (1000, "y"))


@pytest.mark.parametrize("theirs,ours,want", [
    (True, EQ, "ok"), (True, NE, "diff"), (False, EQ, "diff"),
    (False, NE, "diff"), (True, None, "warn"), (None, EQ, "warn")])
def test_the_two_readings_pair_as_a_matrix(theirs, ours, want):
    c = _chunks()[0]
    if theirs is False:
        c.crcs = ("a", "b")
    elif theirs is None:
        c.crcs = ("a", None)
    verdict, lines = rs.pair([c], {} if ours is None else {c.chunk: ours})
    assert verdict == want, lines
    if theirs is False and ours == EQ:
        assert "stream's checksum differs" in lines[0]
    if theirs is True and ours == NE:
        assert "migkit's digest differs" in lines[0]
    if theirs is False and ours == NE:
        assert "both readings differ" in lines[0]


def test_the_fixture_reads_back_as_the_programs_chunks():
    chunks = _chunks()
    assert [c.program_agrees for c in chunks] == [True, False, True, True]
    verdict, lines = rs.pair(chunks, {c.chunk: EQ for c in chunks})
    assert verdict == "diff" and "t chunk 2" in lines[0]
    assert rs.pair([], {})[0] == "warn"


def test_a_chunks_boundaries_are_the_same_rows():
    cols = [("id", "int")]
    assert rs.chunk_where(cols, "1", "1000") == (
        "(`id`) >= (%s) and (`id`) <= (%s)", ("1", "1000"))
    assert rs.chunk_where(cols, None, "1") == ("(`id`) < (%s)", ("1",))
    assert rs.chunk_where(cols, "2000", None) == ("(`id`) > (%s)",
                                                  ("2000",))
    two = [("a", "int"), ("b", "varchar")]
    assert rs.chunk_where(two, "1,x\\,y", "2,z")[1] == ("1", "x,y", "2", "z")
    # a NULL boundary, a value that swallowed its separator, a float
    assert rs.chunk_where(two, "1,\\N", "2,z") is None
    assert rs.chunk_where(two, "1,a\\", "2,z") is None
    assert rs.chunk_where(two, "x\\,y", "2,z") is None
    assert rs.chunk_where([("f", "double")], "1", "2") is None
    assert rs.deserialize("\\N,a\\,b") == [None, "a,b"]


def test_max_lag_and_load_come_from_the_throttle():
    assert rs.max_lag(None) == max(1, int(throttle.REPLICA_RISE))
    assert rs.max_lag(2.4) == 8
    assert rs.max_load(1000) == ("Threads_running="
                                 f"{int(throttle.BUSY_RATIO * 1000)}")
    cmd = rs.argv(("h=s", "h=r"), "app", "t", "app.migkit_checksums", 8,
                  "Threads_running=750")
    assert cmd[cmd.index("--max-lag") + 1] == "8"
    assert cmd[cmd.index("--check-replica-lag") + 1] == "h=r"
    assert cmd[cmd.index("--recursion-method") + 1] == "none"


def test_a_lagging_replica_pauses_migkits_own_reading():
    lags = iter([0.0, 9.0, 9.0, 0.0])
    now = [0.0]
    slept = []

    def sleeper(s):
        slept.append(s)
        now[0] += throttle.PROBE_EVERY

    gate = throttle.Throttle(1, probe=lambda: throttle.Health(
        lag_seconds=next(lags, 0.0), replica=True),
        clock=lambda: now[0], sleeper=sleeper)
    with gate.unit():
        pass
    now[0] += throttle.PROBE_EVERY
    with gate.unit():
        pass
    assert slept and gate.waits == 1
    assert "further behind" in next(iter(gate.reasons))


def _facts(**over):
    base = {"native replica": True, "migkit writes": "", "installed": True,
            "statement binlog": True, "replicate": "app.migkit_checksums"}
    return decide.Facts({**base, **over})


def test_the_rung_holds_only_on_a_native_replica_migkit_does_not_write():
    got = decide.climb("app", rs.RUNGS, _facts())
    assert got.path == "replica checksum"
    for over, said in (({"native replica": False}, "replicate from"),
                       ({"migkit writes": "renamed"}, "migkit writes"),
                       ({"installed": False}, "doctor --install"),
                       ({"replicate": ""}, "table on the source")):
        got = decide.climb("app", rs.RUNGS, _facts(**over))
        assert got.path == "own digest"
        assert said in got.reason, got.reason
    failed = _facts(**{"replica checksum failed": "no chunk came back"})
    assert decide.climb("app", rs.RUNGS, failed).path == "own digest"


def test_the_program_is_accounted_for_and_installed_by_doctor():
    from migkit import tools
    assert tooluse.unexplained(rs.PROGRAM) == []
    assert tooluse.stale(rs.PROGRAM) == []
    assert {"--max-lag", "--replicate"} <= set(tooluse.used(rs.PROGRAM))
    assert rs.PROGRAM in tools.PROGRAMS
    assert any(rs.PROGRAM in opt for _, _, _, opt in tools.CAPABILITIES)
    assert "pt-" not in rs.SAID and "percona" not in rs.SAID
