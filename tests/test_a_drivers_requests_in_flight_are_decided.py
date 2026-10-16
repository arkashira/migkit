"""A driver's requests in flight and the installer's fetches at once are
decisions on what the cluster and the machine say (backlog AUD-Z.workers):
`decide.request_width` from the nodes' own pools, the service's rate, the
measured latency and the free memory, asked again before every chunk;
`decide.fetch_width` from the cores and the memory. Fakes only: no server."""
import itertools

import pytest

from migkit import decide, sizing, tools
from migkit.engines.cassandra import CassandraEngine
from migkit.engines.keyspaces import KeyspacesEngine
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("request_width", "measured", "holds"),
    ("request_width", "measured", "refuses"),
    ("request_width", "the cluster's pools", "holds"),
    ("request_width", "the cluster's pools", "refuses"),
    ("request_width", "the service's rate", "holds"),
    ("request_width", "the service's rate", "refuses"),
    ("request_width", "one a node", "holds"),
    ("request_width", "one a node", "refuses"),
    ("request_width", "one at a time", "holds"),
    ("fetch_width", "the machine's cores", "holds"),
    ("fetch_width", "the machine's cores", "refuses"),
    ("fetch_width", "one at a time", "holds"),
]



def test_nothing_measured_is_one_request_and_says_so():
    n, choice = decide.request_width({})
    assert n == 1 and choice.path == "one at a time"
    n, choice = decide.request_width({"nodes": 3})
    assert n == 3 and choice.path == "one a node"


def test_the_nodes_own_pools_give_the_start_shared_by_side_and_readers():
    facts = {"nodes": 3, "per_node": 32}
    dst, c = decide.request_width({**facts, "side": "dst"})
    src, _ = decide.request_width({**facts, "side": "src"})
    assert c.path == "the cluster's pools"
    assert dst == int(3 * 32 * sizing.SHARE_OTHER)
    assert src == int(3 * 32 * sizing.SHARE_PRIMARY) < dst
    two, _ = decide.request_width({**facts, "side": "dst", "readers": 2})
    assert two == dst // 2


def test_a_service_rate_keeps_as_many_busy_as_its_latency_holds():
    n, c = decide.request_width({"rate": 3000 * 9, "latency": 0.002,
                                 "nodes": 9})
    assert c.path == "the service's rate" and n == 54
    slower, _ = decide.request_width({"rate": 3000 * 9, "latency": 0.004,
                                      "nodes": 9})
    assert slower == 108


def test_a_measured_width_grows_while_the_latency_holds_and_falls_as_it_rises():
    held, c = decide.request_width({"last": 8, "base": 0.010,
                                    "latency": 0.011})
    assert c.path == "measured" and held == 8 * decide.PAGE_GROWTH
    queued, _ = decide.request_width({"last": 8, "base": 0.010,
                                      "latency": 0.040})
    assert queued == 2
    capped, _ = decide.request_width({"last": 64, "base": 0.01,
                                      "latency": 0.01, "nodes": 1,
                                      "per_node": 32, "side": "dst"})
    assert capped == int(32 * sizing.SHARE_OTHER)


def test_the_memory_and_the_requests_left_bound_every_rung():
    n, _ = decide.request_width({"nodes": 3, "per_node": 32, "side": "dst",
                                 "free": decide.PAGE_MEMORY_SHARE * 3 * 100,
                                 "bytes": 100})
    assert n == 3
    n, _ = decide.request_width({"nodes": 3, "per_node": 32, "side": "dst",
                                 "left": 5})
    assert n == 5


def test_each_chunk_is_sized_again_from_what_the_last_measured(monkeypatch):
    ticks = itertools.count(step=0.5)
    monkeypatch.setattr(decide.time, "monotonic", lambda: next(ticks))
    asked = []

    def ask(facts):
        asked.append(dict(facts))
        return decide.request_width(facts)
    req = decide.Requests({"nodes": 2}, ask=ask, memory=lambda: None,
                          seconds=1.0)
    sent = []

    def call(chunk, width):
        sent.append((len(chunk), width))
        return [(True, x * 10) for x in chunk]
    got = decide.in_flight(call, range(40), req)
    assert got == [(True, x * 10) for x in range(40)]
    assert sum(n for n, _ in sent) == 40
    assert asked[0]["last"] is None and asked[0]["base"] is None
    assert asked[1]["last"] == 2 and asked[1]["latency"] == 0.5
    assert asked[1]["base"] == 0.5
    assert sent[1][1] == 4      # the latency held: the width doubled


def test_a_chunk_untimed_leaves_the_measure_as_it_was():
    req = decide.Requests({})
    req.measured([], 4, 1.0)
    req.measured([1, 2], 2, 0)
    assert req.latency is None and req.base is None


class _Hosts:
    def __init__(self, n):
        self.hosts = [type("H", (), {"is_up": True})() for _ in range(n)] \
            + [type("H", (), {"is_up": False})()]

    def all_hosts(self):
        return self.hosts


class _Rows:
    def __init__(self, row):
        self.row = row

    def one(self):
        return self.row


class _Session:
    def __init__(self, hosts, settings):
        self.cluster = type("C", (), {"metadata": _Hosts(hosts)})()
        self.settings, self.asked = settings, []

    def execute(self, text, args=()):
        self.asked.append(text)
        if "system_views.settings" in text:
            if self.settings is None:
                raise RuntimeError("no such table")
            return _Rows((self.settings[args[0]],))
        return _Rows(("4.1",))


def _engine(cls, session):
    eng = cls.__new__(cls)
    eng._session = lambda side: session
    eng._pools = {}
    return eng


def test_cassandra_reads_its_nodes_and_their_pools_once_a_session():
    s = _Session(3, {"concurrent_reads": "32", "concurrent_writes": "48"})
    eng = _engine(CassandraEngine, s)
    got = eng._cluster_facts("dst", "write")
    assert got["nodes"] == 3 and got["per_node"] == 48
    assert got["rate"] is None and got["latency"] is not None
    assert eng._cluster_facts("dst", "write")["per_node"] == 48
    assert sum("settings" in q for q in s.asked) == 1
    assert eng._cluster_facts("src", "read")["per_node"] == 32
    n, choice = decide.request_width(eng._cluster_facts("dst", "write"))
    assert choice.path == "the cluster's pools"
    assert n == int(3 * 48 * sizing.SHARE_OTHER)


def test_keyspaces_is_sized_by_its_documented_rate_not_a_node_pool():
    eng = _engine(KeyspacesEngine, _Session(9, None))
    got = eng._cluster_facts("dst", "write")
    assert got["per_node"] is None
    assert got["rate"] == KeyspacesEngine.SERVICE_RATE * 9


def test_the_engines_requests_are_sent_at_the_decided_width():
    eng = _engine(CassandraEngine, _Session(1, {"concurrent_writes": "8"}))
    widths = []

    def call(chunk, width):
        widths.append(width)
        return [(True, c) for c in chunk]
    assert eng._in_flight("dst", "write", call, []) == []
    assert eng._in_flight("dst", "write", call, range(3)) == [
        (True, 0), (True, 1), (True, 2)]
    assert widths[0] == min(3, int(8 * sizing.SHARE_OTHER))


@pytest.mark.parametrize("cpus,builds,want", [(2, 3, 4), (8, 3, 5),
                                               (None, 3, 1)])
def test_the_installer_fetches_as_many_as_the_machine_takes(cpus, builds,
                                                            want):
    jobs = [(i, f"c{i}", i < builds) for i in range(5)]
    got = tools._fetch_width(jobs, host=lambda: {"cpus": cpus,
                                                 "memory": None})
    assert got == want


def test_the_installer_fetches_are_held_to_the_free_memory():
    n, choice = decide.fetch_width({"jobs": 6, "builds": 0, "cpus": 8,
                                    "free": 2 * sizing.WORKER_MEMORY})
    assert n == 2 and choice.path == "the machine's cores"


def test_every_width_rung_is_held_and_refused_on_the_facts_it_names():
    with watched() as seen:
        decide.request_width({"last": 8, "base": 0.010, "latency": 0.011})
    assert branch(seen, "measured") == [HOLDS]
    with watched() as seen:
        decide.request_width({"nodes": 3, "per_node": 32, "side": "dst"})
    assert branch(seen, "measured") == [REFUSES]
    assert branch(seen, "the cluster's pools") == [HOLDS]
    with watched() as seen:
        decide.request_width({"rate": 3000 * 9, "latency": 0.002,
                              "nodes": 9})
    assert branch(seen, "the cluster's pools") == [REFUSES]
    assert branch(seen, "the service's rate") == [HOLDS]
    with watched() as seen:
        decide.request_width({"nodes": 3})
    assert branch(seen, "the service's rate") == [REFUSES]
    assert branch(seen, "one a node") == [HOLDS]
    with watched() as seen:
        decide.request_width({})
    assert branch(seen, "one a node") == [REFUSES]
    assert branch(seen, "one at a time") == [HOLDS]
    with watched() as seen:
        decide.fetch_width({"jobs": 6, "builds": 0, "cpus": 8})
    assert branch(seen, "the machine's cores") == [HOLDS]
    with watched() as seen:
        decide.fetch_width({"jobs": 6, "builds": 0, "cpus": None})
    assert branch(seen, "the machine's cores") == [REFUSES]
    assert branch(seen, "one at a time") == [HOLDS]
