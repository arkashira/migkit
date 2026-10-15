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
            "statement binlog": True, "replicate": "app.migkit_checksums",
            "db": "app", "account": "'app'@'%'",
            decide.answer_key(rs.TABLE.name): "yes"}
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


def test_the_checksum_table_is_a_change_the_operator_allows():
    unasked = _facts()
    unasked.pop(decide.answer_key(rs.TABLE.name))
    got = decide.climb("app", rs.RUNGS, unasked)
    assert got.path == "own digest"
    assert "offered, never assumed" in got.reason, got.reason
    assert "checksum_table: yes | no | dba" in got.reason
    offer = decide.offered("app", rs.RUNGS, _facts(**{
        decide.answer_key(rs.TABLE.name): None}))
    assert offer is not None and offer.rung.name == "replica checksum"
    assert offer.instead.path == "own digest"
    no = _facts(**{decide.answer_key(rs.TABLE.name): "no"})
    got = decide.climb("app", rs.RUNGS, no)
    assert got.path == "own digest" and "answer is no" in got.reason
    # nowhere to write: only a DBA, with the grant to run
    dba = decide.climb("app", rs.RUNGS, _facts(replicate=""))
    assert "only a DBA" in dba.reason
    assert ("grant create, insert, update, delete, select on"
            " `app`.`migkit_checksums` to 'app'@'%';") in dba.reason
    # migkit's own table is dropped after; the usual one is left as found
    assert rs.TABLE.statements(_facts(), "undo") == [
        "drop table if exists `app`.`migkit_checksums`"]
    assert rs.TABLE.statements(_facts(replicate="percona.checksums"),
                               "undo") == []


def test_an_untimed_replica_checksum_is_tried_before_the_digest():
    # the digest is no native copier: a way never timed is not held below
    # it for ever
    assert rs.RUNGS[1].native_copier is False
    assert not decide._is_native_copier(rs.RUNGS[1])
    assert decide.rank(rs.RUNGS)[0].name == "replica checksum"
    timed_digest = decide.Costs({"own digest": 1e-6})
    assert decide.climb("app", rs.RUNGS, _facts(),
                        costs=timed_digest).path == "replica checksum"


def test_both_timed_the_cheaper_proof_is_taken_unless_the_source_moves():
    both = decide.Costs({"replica checksum": 3e-5, "own digest": 1e-5})
    assert decide.climb("app", rs.RUNGS, _facts(),
                        costs=both).path == "own digest"
    cheaper = decide.Costs({"replica checksum": 1e-5, "own digest": 3e-5})
    assert decide.climb("app", rs.RUNGS, _facts(),
                        costs=cheaper).path == "replica checksum"
    # a source being written: only the stream proves it at one point
    got = decide.climb("app", rs.RUNGS, _facts(), need=("online",),
                       costs=both)
    assert got.path == "replica checksum"
    blocked = decide.climb("app", rs.RUNGS, _facts(installed=False),
                           need=("online",), costs=both)
    assert blocked.path is None


class _Replica:
    """A MariaDB or MySQL pair, as the facts and the check ask it."""
    _REPL_FIELDS = {"sql": ("Replica_SQL_Running", "Slave_SQL_Running")}

    def __init__(self, brand="mysql", status=None, src=None, dst=None,
                 positions=("0-1-5",), sizes=None, chunks=None):
        from types import SimpleNamespace
        self.brand = brand
        self.status = status if status is not None else [{
            "Source_UUID": "u-1", "Replica_SQL_Running": "Yes"}]
        self.src = {"@@server_uuid": "u-1", "@@server_id": 1,
                    "@@gtid_current_pos": "0-1-90",
                    "current_user()": "app@%", **(src or {})}
        self.dst = {"@@gtid_slave_pos": "0-1-90", **(dst or {})}
        self.positions = iter(positions)
        self.last = None
        self.sizes = sizes or {"big": 1000, "small": 10}
        self.chunks = chunks or {}
        self.sql = []
        self.hop = SimpleNamespace(excluded=lambda db, t: False,
                                   source="s", target="t", options={})

    def _brands(self):
        from types import SimpleNamespace
        return (SimpleNamespace(name=self.brand),
                SimpleNamespace(name=self.brand))

    def _q(self, side, sql, params=None):
        self.sql.append((side, sql))
        for k, v in (self.src if side == "src" else self.dst).items():
            if sql == f"select {k}":
                return [(v,)]
        if sql.startswith("show grants"):
            return [("GRANT ALL PRIVILEGES ON *.* TO `app`@`%`",)]
        if "information_schema.tables" in sql:
            return [(0,)]
        if sql == "select @@max_connections":
            return [(100,)]
        if "information_schema.statistics" in sql:
            return [("id", "int")]
        if sql.startswith("digest"):
            return [(10, "same")]
        if "migkit_checksums` where" in sql:
            return [(1, "PRIMARY", "1", "10", n, n, "c", "c")
                    for n in [self.chunks.get(params[1], 10)]]
        return [()]

    def _q_named(self, side, sql):
        self.sql.append((side, sql))
        return self.status

    _repl_field = staticmethod(lambda row, names: next(
        (row[n] for n in names if n in row), None))

    def src_lsn(self, db):
        self.last = next(self.positions, self.last)
        return self.last

    def fence_wait(self, db, at, timeout=300):
        return True

    def native_replica_unsafe(self):
        return None

    def maps_columns(self):
        return False

    def _health(self, side):
        return throttle.Health(lag_seconds=0.0, replica=True)

    def _tables(self, side, db):
        return sorted(self.sizes, reverse=True)

    def table_facts(self, side, db):
        return {t: {"rows": n} for t, n in self.sizes.items()}

    def _range_digest_sql(self, side, db, t, where):
        return f"digest {side} {t}"


def test_a_mariadb_replica_is_told_by_its_gtid_positions():
    ok = {"Master_Server_Id": "1", "Using_Gtid": "Slave_Pos",
          "Slave_SQL_Running": "Yes"}
    assert rs.facts(_Replica("mariadb", [ok]), "app")["native replica"]
    # MariaDB has no server UUID: never read there
    eng = _Replica("mariadb", [ok])
    rs.facts(eng, "app")["native replica"]
    assert ("src", "select @@server_uuid") not in eng.sql
    for status, dst in (
            ([{**ok, "Master_Server_Id": "7"}], None),
            ([{**ok, "Using_Gtid": "No"}], None),
            ([{**ok, "Slave_SQL_Running": "No"}], None),
            ([], None),
            # a domain the source never had, a position past the source's
            ([ok], {"@@gtid_slave_pos": "0-1-90,3-9-4"}),
            ([ok], {"@@gtid_slave_pos": "0-1-91"}),
            ([ok], {"@@gtid_slave_pos": ""})):
        got = rs.facts(_Replica("mariadb", status, dst=dst), "app")
        assert got["native replica"] is False, (status, dst)
    assert rs._gtid_set("0-1-90, 2-3-4") == {0: (1, 90), 2: (3, 4)}
    assert rs._gtid_set("junk") == {}


def _checked(monkeypatch, eng, answer="yes"):
    import contextlib
    import time as clock
    from migkit import movers, sourcechange, util
    ran = []

    def run(cmd, check=False, timeout=None):
        from types import SimpleNamespace
        ran.append(cmd[cmd.index("--tables") + 1])
        clock.sleep(0.02)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(util, "run", run)
    monkeypatch.setattr(util, "quiet_argv", lambda cmd: cmd)
    monkeypatch.setattr(util, "which", lambda p: "/bin/" + p)
    monkeypatch.setattr(movers, "client_defaults",
                        lambda s, t: contextlib.nullcontext((None, None)))
    monkeypatch.setattr(movers, "client_dsn", lambda side, f: "h=x")
    monkeypatch.setattr(sourcechange, "answers", lambda hop: {
        rs.TABLE.name: (answer, "the test")} if answer else {})
    monkeypatch.setattr(rs, "WRITE_PROBE", 0)
    monkeypatch.setattr(rs, "max_lag", lambda base, hop=None: 5)
    said, costs = [], decide.Costs()
    got = rs.check(eng, "app", log=said.append, costs=costs)
    return got, ran, costs, said


def test_one_bounded_trial_then_the_measured_cheaper_proof(monkeypatch):
    eng = _Replica()
    got, ran, costs, _ = _checked(monkeypatch, eng)
    # tried on the smallest table only; the larger one went to the digest,
    # measured cheaper on the same chunks
    assert ran == ["app.small"], ran
    assert set(costs) == {"replica checksum", "own digest"}
    assert costs["replica checksum"] > costs["own digest"]
    assert got.status == "ok", got
    assert "1 tables by migkit's own digest alone" in got.detail
    assert ("src", "drop table if exists `app`.`migkit_checksums`") \
        in eng.sql


def test_a_source_being_written_is_proved_through_the_stream(monkeypatch):
    eng = _Replica(positions=("u-1:1-5", "u-1:1-9"))
    got, ran, _, _ = _checked(monkeypatch, eng)
    assert ran == ["app.small", "app.big"], ran
    assert got.status == "ok" and "alone" not in got.detail


def test_unanswered_the_table_is_offered_and_nothing_runs(monkeypatch):
    eng = _Replica()
    got, ran, costs, said = _checked(monkeypatch, eng, answer=None)
    assert got is None and ran == [] and not costs
    assert said and "source_changes: {checksum_table" in said[0], said
    assert not [s for _, s in eng.sql if s.startswith("drop")]
    eng = _Replica()
    got, ran, _, _ = _checked(monkeypatch, eng, answer="no")
    assert got is None and ran == []


def test_the_program_is_accounted_for_and_installed_by_doctor():
    from migkit import tools
    assert tooluse.unexplained(rs.PROGRAM) == []
    assert tooluse.stale(rs.PROGRAM) == []
    assert {"--max-lag", "--replicate"} <= set(tooluse.used(rs.PROGRAM))
    assert rs.PROGRAM in tools.PROGRAMS
    assert any(rs.PROGRAM in opt for _, _, _, opt in tools.CAPABILITIES)
    assert "pt-" not in rs.SAID and "percona" not in rs.SAID
