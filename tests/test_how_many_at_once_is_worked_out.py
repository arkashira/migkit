"""How many things a move does at once is worked out, not asked for.

Measured before: every hop ran `workers` (4 unless set) whatever the
machines were - on a 2-CPU sandbox MySQL server the table copier took 10.9s
at one worker and 6.8s at four, and a production source with two CPUs got
the same four as a target with forty. The start is now the least of every
factor migkit can read, and the pace while the move runs climbs while more
is faster and backs off when a server is under strain.
"""
import pytest

from migkit import sizing
from migkit.config import Endpoint, Hop


class _Hop:
    workers, workers_set = 4, False


def _plan(src=None, dst=None, units=None, here=None, hop=None):
    return sizing.estimate(hop or _Hop(), None, "app", units=units,
                           here=here or {"cpus": 16, "memory": 32 * 2 ** 30},
                           sides={"src": src or {}, "dst": dst or {}})


def test_the_start_is_the_least_of_what_can_be_read():
    got = _plan(src={"cpus": 2, "free_connections": 90, "running": 0},
                dst={"cpus": 8, "free_connections": 400})
    # a primary source of 2 CPUs starts it at 2; the pace may go as far as
    # the firm factors - here the source's share of its connections
    assert (got.start, got.most) == (2, 11), got.factors
    assert "the source's CPUs" in got.line()
    # a replica takes more than a primary serving an application
    got = _plan(src={"cpus": 2, "free_connections": 90, "replica": True},
                dst={"cpus": 8, "free_connections": 400})
    assert got.start == 4


def test_firm_factors_are_never_passed():
    # a source with 10 free connections: a quarter of them, two a worker
    got = _plan(src={"cpus": 32, "free_connections": 10},
                dst={"cpus": 32, "free_connections": 1000})
    assert (got.start, got.most) == (1, 1), got.factors
    # little memory here
    got = _plan(here={"cpus": 16, "memory": sizing.RESERVE_MEMORY
                      + 3 * sizing.WORKER_MEMORY},
                src={"cpus": 32}, dst={"cpus": 32})
    assert got.most == 3
    # a small database: no more at once than there is work
    got = _plan(src={"cpus": 32}, dst={"cpus": 32}, units=2)
    assert (got.start, got.most) == (2, 2)
    # a hop that set `workers` set a ceiling
    hop = _Hop()
    hop.workers, hop.workers_set = 3, True
    assert _plan(src={"cpus": 32}, dst={"cpus": 32}, hop=hop).most == 3


def test_what_cannot_be_read_counts_as_the_old_default():
    got = _plan(here={"cpus": 16})
    assert (got.start, got.most) == (sizing.UNREAD, 2 * sizing.UNREAD)
    assert "not readable" in got.line()


def test_a_server_running_more_than_its_cpus_take_is_under_strain():
    class Eng:
        def __init__(self, src, dst):
            self.said = {"src": src, "dst": dst}

        def capacity(self, side, db):
            return self.said[side]
    assert sizing.strain(Eng({"cpus": 2, "running": 3},
                             {"cpus": 8, "running": 1}), "app") == ""
    assert sizing.strain(Eng({"cpus": 2, "running": 4},
                             {}), "app") == \
        "source: 4 sessions running on 2 CPUs"


def test_a_containers_cpu_quota_is_what_it_may_use(tmp_path, monkeypatch):
    monkeypatch.setattr(sizing.os, "sched_getaffinity",
                        lambda pid: set(range(16)), raising=False)
    (tmp_path / "cpu.max").write_text("150000 100000\n")
    assert sizing._cpus(str(tmp_path)) == 2
    (tmp_path / "cpu.max").write_text("max 100000\n")
    assert sizing._cpus(str(tmp_path)) == 16
    (tmp_path / "cpu.max").unlink()
    (tmp_path / "cpu").mkdir()
    (tmp_path / "cpu" / "cpu.cfs_quota_us").write_text("300000")
    (tmp_path / "cpu" / "cpu.cfs_period_us").write_text("100000")
    assert sizing._cpus(str(tmp_path)) == 3


def test_a_hop_from_the_configuration_sets_a_ceiling_only_if_it_says_one(
        tmp_path):
    from migkit.config import load_hops
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  a:\n    engine: postgres\n    source: {host:"
                    " 10.0.0.1}\n    target: {host: 10.0.0.2}\n  b:\n"
                    "    engine: postgres\n    workers: 6\n    source: {host:"
                    " 10.0.0.1}\n    target: {host: 10.0.0.2}\n")
    hops = load_hops(conf)
    assert (hops["a"].workers_set, hops["b"].workers_set) == (False, True)


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def _run(curve, start, most, rounds=600, stress=None):
    """A pace fed by a system whose rows a second at n at once is
    `curve(n)`: each unit is 1,000 rows, n of them in flight."""
    clock = _Clock()
    pace = sizing.Pace(start, most, stress=stress, clock=clock)
    seen = []
    for i in range(rounds):
        n = pace.limit
        rate = curve(n)
        clock.t += 1000 / rate
        pace.done(1000, n * 1000 / rate)
        seen.append(pace.limit)
    return pace, seen


def test_the_pace_climbs_to_where_it_is_fastest_and_stays_near_it():
    # faster up to 6 at once, slower after - never stopping altogether
    def curve(n):
        return 1000 * max(0.5, n if n <= 6 else 6 - 0.8 * (n - 6))
    pace, seen = _run(curve, 2, 32, rounds=1500)
    tail = seen[-500:]
    assert max(seen) <= 32 and min(seen) >= 1
    assert 5 <= sum(tail) / len(tail) <= 8, pace.history
    assert pace.line().startswith("at a time: started at 2")


def test_the_pace_stops_climbing_where_more_is_no_faster():
    def curve(n):
        return 1000 * min(n, 3)
    pace, seen = _run(curve, 1, 32, rounds=1500)
    tail = seen[-500:]
    assert sum(tail) / len(tail) <= 5, pace.history


def test_the_pace_holds_near_the_best_through_noise():
    import random
    rng = random.Random(7)

    def curve(n):
        return (1000 * max(0.5, n if n <= 10 else 10 - 0.3 * (n - 10))
                * rng.uniform(0.9, 1.1))
    pace, seen = _run(curve, 2, 32, rounds=1500)
    tail = seen[-500:]
    assert 7 <= sum(tail) / len(tail) <= 14, pace.history[-12:]


def test_a_server_under_strain_cuts_the_pace_at_once():
    strained = {"on": False}

    def stress():
        return "source: sessions at 90% of the limit" if strained["on"] \
            else ""

    def curve(n):
        return 1000 * n
    clock = _Clock()
    pace = sizing.Pace(8, 16, stress=stress, clock=clock)
    for _ in range(20):
        clock.t += 1
        pace.done(1000, 1)
    before = pace.limit
    strained["on"] = True
    for _ in range(8):
        clock.t += 1
        pace.done(1000, 1)
    assert pace.limit <= int(before * 0.7), pace.history
    assert any("sessions at 90%" in why for _, _, why in pace.history)


def test_slots_follow_the_pace():
    import threading
    import time

    from migkit import ranges
    clock = _Clock()
    pace = sizing.Pace(2, 8, clock=clock)
    slots = ranges.Slots(8, pace=pace)
    now, most, lock = [0], [0], threading.Lock()

    def work(i):
        with lock:
            now[0] += 1
            most[0] = max(most[0], now[0])
        time.sleep(0.02)
        with lock:
            now[0] -= 1
        return 500
    slots.each(range(30), work)
    assert most[0] <= 2, most[0]
    pace.limit = 5
    most[0] = 0
    slots.each(range(30), work)
    assert 3 <= most[0] <= 5, most[0]


@pytest.mark.docker
def test_postgres_says_what_it_can_give(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=pg_pair["src"], user="postgres",
                  password="test")
    got = PostgresEngine(Hop(name="c", engine="postgres", source=ep,
                             target=ep, databases=["postgres"])
                         ).capacity("src", "postgres")
    # the sandbox's server: 100 connections less what is reserved and in
    # use, its CPUs read from the server's own files by a superuser
    assert 80 <= got["free_connections"] < 100, got
    assert got["cpus"] >= 1 and got["replica"] is False, got
