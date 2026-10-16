"""`migkit estate` (backlog W2): the servers an operator names are knocked
on and listed with their version and size, a closed port said as
unreachable, each probe bounded and kept with its time; the time of each
move comes from the ledger's measured runs of that engine pair (the
fastest and the slowest its bounds), "no history" and the assessment's
class where none was kept; its cost is the catalog's price over that
measured time; an account's instances come from the provider's own API."""
import json
import socket

import pytest
from click.testing import CliRunner

from migkit import cloudapi, decide, engineat, estate, machine
from migkit.cli import main
from migkit.diagnostics import ProbeAccumulator

MIB = 2 ** 20
GIB = 2 ** 30


def _closed_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


#: the most one knock may take: every protocol's question at its wait,
#: and the sign-in's own timeout - with a second for the machine
BOUND = len(engineat.KNOCKS) * engineat.WAIT + engineat.WAIT * 4 + 1


@pytest.mark.docker
def test_the_lanes_servers_are_listed_with_version_and_size(monkeypatch):
    from tests import servers
    pg = next(s for s in servers.servers_of("pg") if s.role == "src")
    my = next(s for s in servers.servers_of("mysql") if s.role == "src")
    closed = _closed_port()
    monkeypatch.setenv("MIGKIT_SOURCE_PASSWORD", servers.PASSWORD)
    probes = ProbeAccumulator()
    got = estate.discover([f"{pg.user}@{pg.host}:{pg.port}",
                           f"{my.user}@{my.host}:{my.port}",
                           f"127.0.0.1:{closed}"],
                          password=servers.PASSWORD, probes=probes)
    by = {f.port: f for f in got}
    assert by[pg.port].engine == "postgres", by[pg.port]
    assert by[pg.port].version.startswith("16")
    assert by[my.port].engine == "mysql"
    assert by[my.port].version.startswith("8.4")
    # the size is the server's own: PostgreSQL's databases hold pages
    assert by[pg.port].size > MIB
    assert by[my.port].size is not None and by[my.port].size >= 0
    assert not by[closed].reached and by[closed].note == "unreachable"
    for f in got:
        kept = probes.get("estate.knock", f.address)
        assert kept is not None and kept.took == pytest.approx(f.took)
        assert f.took <= BOUND, (f.address, f.took)
    assert by[closed].took <= engineat.WAIT + 1
    # the command prints one fleet: both servers, the closed port too
    out = CliRunner().invoke(main, [
        "estate", f"{pg.user}@{pg.host}:{pg.port}",
        f"{my.user}@{my.host}:{my.port}", f"127.0.0.1:{closed}"],
        env={"MIGKIT_SOURCE_PASSWORD": servers.PASSWORD})
    assert out.exit_code == 0, out.output
    lines = out.output.splitlines()
    pg_line = next(x for x in lines if f":{pg.port} " in x)
    my_line = next(x for x in lines if f":{my.port} " in x)
    assert "postgres" in pg_line and by[pg.port].version in pg_line
    assert "mysql" in my_line and by[my.port].version in my_line
    assert "unreachable" in next(x for x in lines if f":{closed} " in x)


def _ledger(tmp_path, runs):
    hop = tmp_path / "some-hop"
    hop.mkdir(parents=True)
    (hop / "throughput.json").write_text(json.dumps(runs))
    return tmp_path


def _run(kind, mib, seconds, at="2026-10-01T10:00:00"):
    return {"rows": 1000, "bytes": mib * MIB, "seconds": seconds,
            "kind": kind, "at": at}


def test_the_time_comes_from_the_planted_runs_of_the_pair(tmp_path):
    reports = _ledger(tmp_path, {
        "bulk": [_run("postgres", 100, 10), _run("postgres", 100, 40)],
        "logical": [_run("mysql", 100, 1)]})
    priced = estate._priced("aws", "us-east-1")
    assert priced, "the shipped catalog prices AWS in us-east-1"
    f = estate.size(estate.Found("10.0.0.5", 5432, engine="postgres",
                                 size=GIB, reached=True),
                    reports=reports, priced=priced)
    assert f.sized_by == "pair"
    # the fastest run (10 MiB/s) and the slowest (2.5 MiB/s): the MySQL
    # run of another pair is not this one's
    assert f.eta == pytest.approx((GIB / (10 * MIB), GIB / (2.5 * MIB)))
    # the cost is the catalog's price over the measured time, the cheapest
    # type at each end
    for (adv, region), rate in zip(f.cost, (10 * MIB, 2.5 * MIB)):
        assert region == "us-east-1"
        want = min(m.price(region) * (GIB / rate) / 3600
                   for m in priced[("aws", region)]
                   if machine.seconds_on(m, machine.Stages(beyond=rate),
                                         GIB)[1] == machine.BEYOND)
        assert adv.cost == pytest.approx(want)
    table = estate.fleet([f])
    assert "$" in table and "sized from the measured runs of this engine" \
        " pair (2 runs)" in table


def test_another_target_s_runs_size_it_where_the_pair_never_ran(tmp_path):
    reports = _ledger(tmp_path, {"bulk": [_run("postgres->mysql", 50, 10)]})
    f = estate.size(estate.Found("10.0.0.5", 5432, engine="postgres",
                                 size=GIB, reached=True),
                    reports=reports, priced={})
    assert f.sized_by == "engine"
    assert f.eta == pytest.approx((GIB / (5 * MIB),) * 2)
    # nothing priced: the time stands, no cost is said
    assert f.cost is None and "not priced" in estate.fleet([f])
    # sized for that target, the pair's own runs are those
    f = estate.size(estate.Found("10.0.0.5", 5432, engine="postgres",
                                 size=GIB, reached=True), to="mysql",
                    reports=reports, priced={})
    assert f.sized_by == "pair"


def test_no_history_is_said_with_the_assessment_s_class(tmp_path):
    f = estate.size(estate.Found("10.0.0.5", 3306, engine="mysql", size=GIB,
                                 reached=True), reports=tmp_path, priced={})
    assert f.eta is None and f.sized_by == "none" and f.cost is None
    line = estate.fleet([f]).splitlines()[1]
    assert "no history" in line and "the target's class" in line
    # a size nobody signed in to read holds no time, even with runs kept
    reports = _ledger(tmp_path / "r", {"bulk": [_run("mysql", 10, 1)]})
    f = estate.size(estate.Found("10.0.0.5", 3306, engine="mysql",
                                 reached=True), reports=reports, priced={})
    assert f.eta is None
    choice, _ = decide.estate_eta({"size": None, "pair": [_run("mysql", 1,
                                                                1)]})
    assert "nobody signed in" in " ".join(w for _, w in choice.passed)


def test_a_closed_port_is_unreachable_without_a_server(monkeypatch):
    closed = _closed_port()
    got = estate.knock("127.0.0.1", closed)
    assert not got.reached and got.note == "unreachable"
    assert got.took <= engineat.WAIT + 1


def test_a_network_is_knocked_at_every_engine_port(monkeypatch):
    asked = []
    monkeypatch.setattr(engineat, "_reachable",
                        lambda h, p, wait=engineat.WAIT:
                        asked.append((h, p)) or False)
    assert estate.discover(["10.0.0.0/30"]) == []
    hosts = {h for h, _ in asked}
    assert hosts == {"10.0.0.1", "10.0.0.2"}
    assert {p for _, p in asked} == set(estate._ports())


def test_an_account_s_instances_are_read_through_its_api(monkeypatch):
    class Rds:
        def describe_db_instances(self, **kw):
            if not kw:
                return {"DBInstances": [
                    {"DBInstanceIdentifier": "shop-1", "Engine": "postgres",
                     "Endpoint": {"Address": "shop-1.example.com",
                                  "Port": 5432}},
                    {"DBInstanceIdentifier": "c-1", "Engine": "aurora-mysql",
                     "DBClusterIdentifier": "orders",
                     "Endpoint": {"Address": "c-1.example.com",
                                  "Port": 3306}}], "Marker": "m"}
            return {"DBInstances": [
                {"DBInstanceIdentifier": "c-2", "Engine": "aurora-mysql",
                 "DBClusterIdentifier": "orders",
                 "Endpoint": {"Address": "c-2.example.com", "Port": 3306}}]}
    monkeypatch.setattr(cloudapi.Aws, "client", lambda self, s=None: Rds())
    rows, why = estate._account("aws:rds:us-east-1")
    assert [(r["id"], r["host"], r["port"]) for r in rows] == [
        ("shop-1", "shop-1.example.com", 5432),
        ("orders", "c-1.example.com", 3306)]
    assert "not run against a real AWS account" in why
    assert estate.targets([], rows, "app") == [
        ("app", "shop-1.example.com", 5432), ("app", "c-1.example.com",
                                              3306)]
    rows, why = estate._account("nowhere:rds:x")
    assert rows == [] and "no provider" in why
