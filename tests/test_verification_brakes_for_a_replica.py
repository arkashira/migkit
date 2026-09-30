"""The verification's load on its source: a brake where it had none, and
one that knows the source is a replica (problems C4).

A check pointed at a replica to spare the primary makes the replica fall
behind: a full scan on a PostgreSQL standby holds its replay back. The
throttle braked only on a lag past 30 seconds, and read a standby's lag
as the time since its last replayed commit - which grows by itself while
the primary is idle, so every table of a check waited for nothing. Now a
replica's lag rising above the least the run has seen of it brakes, and a
standby that has replayed all it received has none. SQL Server's check
and the cross-engine check had no brake at all.
"""
from migkit.throttle import REPLICA_RISE, Health, Throttle


class _Clock:
    def __init__(self):
        self.now, self.slept = 0.0, 0.0

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.slept += s
        self.now += s


def _replica(lags):
    it = iter(lags)
    last = [None]

    def probe():
        last[0] = next(it, last[0])
        return Health(busy_ratio=0.0, lag_seconds=last[0], replica=True)
    return probe


def test_a_replica_that_falls_behind_while_read_brakes():
    """Well under the 30 seconds that brake anything, and rising: the read
    is what makes it fall behind."""
    c = _Clock()
    t = Throttle(2, probe=_replica([1.0, 1.0, 1.0 + REPLICA_RISE + 1]),
                 clock=c, sleeper=c.sleep)
    with t.unit():
        c.now += 6
    with t.unit():
        c.now += 6
    before = c.slept
    with t.unit():
        pass
    assert c.slept > before, "it went on while the replica fell behind"
    assert any("fell" in r and "further behind its primary" in r
               for r in t.summary()["reasons"]), t.summary()


def test_a_replica_steadily_behind_is_not_braked():
    """Five seconds behind before the check began and five after: not the
    check's doing."""
    c = _Clock()
    t = Throttle(2, probe=_replica([5.0] * 10), clock=c, sleeper=c.sleep)
    for _ in range(4):
        with t.unit():
            c.now += 6
    assert c.slept == 0.0 and t.summary() == {}, t.summary()


def test_the_same_lag_on_a_primary_is_not_the_replica_brake():
    """A primary's lag is its replicas': the rise alone does not brake it,
    only the absolute bound does."""
    h = Health(lag_seconds=12.0)
    assert h.stressed(least_lag=0.0) == ""
    assert Health(lag_seconds=12.0, replica=True).stressed(0.0) != ""


def test_a_standby_idle_primary_reads_no_lag():
    """The statement's own rule, read off it: nothing where every byte
    received is replayed, else the time since the last replayed commit."""
    from migkit.engines.postgres import PostgresEngine
    sql = PostgresEngine.HEALTH_SQL
    assert "pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() then 0" \
        in sql, sql
    assert "pg_is_in_recovery()::int" in sql, sql


def test_sql_servers_own_load_is_read(tmp_path, monkeypatch):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mssql import MSSQLEngine
    hop = Hop(name="m", engine="mssql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["d"])
    eng = MSSQLEngine(hop)
    monkeypatch.setattr(eng, "_rows", lambda side, db, sql: [(6, 4, 12.5)])
    h = eng._health("src", "d")
    assert h.busy_ratio == 1.5 and h.lag_seconds == 12.5 and h.replica
    assert h.stressed() != ""
    monkeypatch.setattr(eng, "_rows", lambda side, db, sql: [(0, 4, None)])
    h = eng._health("src", "d")
    assert h.busy_ratio == 0.0 and h.lag_seconds is None and not h.replica
    assert h.stressed() == ""

    def refused(side, db, sql):
        raise RuntimeError("VIEW SERVER STATE permission was denied")
    monkeypatch.setattr(eng, "_rows", refused)
    assert eng._health("src", "d") is None


def test_the_pair_asks_each_side_in_its_database(tmp_path):
    """A PostgreSQL side's probe asks in a database: called with the side
    alone it raised, and the cross-engine path read with no brake from
    that side."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host=str(tmp_path / "a.db"), port=0, user="",
                              password=""),
              target=Endpoint(host=str(tmp_path / "b.db"), port=0, user="",
                              password=""),
              databases=["main"],
              options={"source_engine": "sqlite", "target_engine": "sqlite"})
    eng = HeteroEngine(hop)
    asked = []
    eng.src_engine._health = lambda side, db: asked.append((side, db)) or \
        Health(busy_ratio=0.9)
    got = eng._health("src", "main")
    assert asked == [("src", "main")] and got.busy_ratio == 0.9
    eng.dst_engine._health = lambda side: Health(busy_ratio=0.1)
    assert eng._health("dst", "main").busy_ratio == 0.1
