"""The machine to run migkit on, and the class the target needs (backlog
R22 item 6): the shipped catalog holds what the providers publish, a long
move is timed at a type's baseline and a short one at its burst, a server
that binds is said instead of a bigger machine sold, the catalog is
refreshed from AWS's own API where credentials are present (moto for the
description, a stub for the price list - never a real account), and the
target's class is sized from the source's observed load."""
import json
import math

import pytest

from migkit import machine
from migkit.config import Endpoint, Hop

GB = 1e9


def _hop(tmp_path, monkeypatch, **options):
    monkeypatch.setattr("migkit.config.REPORTS", tmp_path / "reports")
    return Hop(name="m", engine="postgres",
               source=Endpoint(host="db.example.com", port=5432, user="u",
                               password="p"),
               target=Endpoint(host="10.0.0.9", port=5432, user="u",
                               password="p"),
               databases=["app"], options=options)


def test_the_catalog_holds_what_the_providers_publish():
    cat = json.loads(machine.CATALOG.read_text())
    for cloud in ("aws", "gcp", "azure"):
        src = cat["sources"][cloud]
        assert src["specs"] and src["prices"], cloud
        types = cat["types"][cloud]
        assert len(types) >= 30, (cloud, len(types))
        for name, t in types.items():
            net, disk = t["network_gbps"], t["disk_mb_s"]
            assert 0 < net["baseline"] <= net["burst"], (name, net)
            assert 0 < disk["baseline"] <= disk["burst"], (name, disk)
            assert t["vcpu"] >= 2 and t["memory_gib"] > 0, name
            assert t["price_hour"] and all(
                p > 0 for p in t["price_hour"].values()), name
    # a published "up to": the burst far over the baseline, as AWS says of
    # an m6i.large - the case the advice has to get right
    m6i = cat["types"]["aws"]["m6i.large"]
    assert m6i["network_gbps"] == {"baseline": 0.781, "burst": 12.5}
    assert m6i["price_hour"]["us-east-1"] == 0.096
    assert cat["burst_minutes"]["aws"] == {"network": 5, "disk": 30}


def _m6i_large():
    return next(m for m in machine.machines("aws", "us-east-1")
                if m.name == "m6i.large")


def test_a_short_move_gets_the_burst_and_a_long_one_the_baseline():
    m = _m6i_large()
    st = machine.Stages(wire=1.0)
    short, binds, rate = machine.seconds_on(m, st, 10 * GB)
    assert binds == "its network" and rate == m.net[1]
    assert short == pytest.approx(10 * GB / m.net[1])
    long_, binds, rate = machine.seconds_on(m, st, 2000 * GB)
    # five minutes at the burst, the rest at the baseline
    burst_part = m.net[1] * m.net_burst_s
    assert rate == m.net[0]
    assert long_ == pytest.approx(
        m.net_burst_s + (2000 * GB - burst_part) / m.net[0])
    # the numbers in `machine`'s docstring: 6.4 seconds, 4.4 hours, and
    # the 21 minutes a catalog of "up to" numbers would promise
    assert short == pytest.approx(6.4)
    assert long_ / 3600 == pytest.approx(4.44, abs=0.01)
    assert 2000 * GB / m.net[1] / 60 == pytest.approx(21.3, abs=0.1)


def test_the_type_chosen_for_a_long_move_holds_the_deadline_at_its_baseline():
    types = machine.machines("aws", "us-east-1")
    st = machine.Stages(wire=1.0)
    size, deadline = 2000 * GB, 3 * 3600
    got = machine.advise(st, size, deadline, types, "us-east-1")
    m = got.machine
    assert m is not None and got.seconds <= deadline, got.words
    # the baseline carries it: what the burst adds is five minutes' worth
    assert m.net[0] * (deadline - m.net_burst_s) \
        + m.net[1] * m.net_burst_s >= size
    # the types whose "up to" would meet the deadline and whose baseline
    # does not: there are some, read as bursts they would cost less than
    # the choice, and none of them is chosen
    tempting = [t for t in types if t.net[1] * deadline >= size
                and machine.seconds_on(t, st, size)[0] > deadline]
    assert tempting, "nothing tempting: the test proves nothing"
    naive = min(t.price("us-east-1") * size / t.net[1] / 3600
                for t in tempting)
    assert naive < got.cost
    assert m not in tempting


def _only(*names):
    return [t for t in machine.machines("aws", "us-east-1")
            if t.name in names]


def test_the_burst_is_counted_while_it_lasts_and_not_after():
    small, big = _only("c6g.large", "m6i.4xlarge")
    st = machine.Stages(wire=1.0)
    # 50 GB in five minutes: 1.3 Gbps, over the small type's 0.75 Gbps
    # baseline and well under its 10 Gbps burst
    got = machine.advise(st, 50 * GB, 300, [small, big], "us-east-1")
    assert got.machine is small, got.words
    assert small.net[0] * 300 < 50 * GB
    # 2 TB in three hours: the burst is spent in five minutes, and the
    # small type's baseline would need nearly 5 hours
    got = machine.advise(st, 2000 * GB, 3 * 3600, [small, big], "us-east-1")
    assert got.machine is big, got.words
    assert machine.seconds_on(small, st, 2000 * GB)[0] > 4.8 * 3600
    assert "sustained" in got.words


def test_a_server_that_binds_says_no_bigger_machine_helps():
    types = machine.machines("aws", "us-east-1")
    st = machine.Stages(source=50 * 2 ** 20, wire=1.0, per_core=400 * 2 ** 20)
    got = machine.advise(st, 1000 * GB, 3600, types, "us-east-1")
    assert got.server_bound and got.machine is None
    assert "a bigger machine would not help" in got.words
    assert "the source's reads bind at ~50 MB/s" in got.words, got.words
    assert "A larger source instance" in got.words
    # the same servers with time enough: a machine, the smallest that keeps
    # up, and the reason nothing bigger would be faster
    ok = machine.advise(st, 1000 * GB, 10 * 3600, types, "us-east-1")
    assert not ok.server_bound and ok.machine is not None
    assert ok.binds == "the source's reads"
    assert "so a bigger machine would not be faster" in ok.words
    assert ok.seconds == pytest.approx(1000 * GB / (50 * 2 ** 20))
    # the cheapest by the hour, since every type takes as long
    assert ok.machine.price("us-east-1") == min(
        t.price("us-east-1") for t in types
        if machine.seconds_on(t, st, 1000 * GB)[1] == "the source's reads")


def test_this_machine_binding_says_so_and_a_bigger_one_is_chosen():
    types = machine.machines("aws", "us-east-1")
    # migkit's own work: 30 MB/s a core - the CPUs bind below 16 cores
    st = machine.Stages(target=900 * 2 ** 20, per_core=30 * 2 ** 20)
    got = machine.advise(st, 500 * GB, 3600, types, "us-east-1")
    assert got.machine is not None
    assert got.binds in ("its CPUs", "the target's writes"), got.words
    need = 500 * GB / 3600
    assert got.machine.vcpu * 30 * 2 ** 20 >= need
    small = machine.advise(st, 500 * GB, 24 * 3600, types, "us-east-1")
    assert small.machine.vcpu < got.machine.vcpu


def test_rows_that_go_server_to_server_need_the_smallest_machine():
    types = machine.machines("aws", "us-east-1")
    got = machine.advise(machine.Stages(wire=0.0), 100 * GB, 3600, types,
                         "us-east-1")
    assert "server to server" in got.words
    assert got.machine.price("us-east-1") == min(
        t.price("us-east-1") for t in types)


def test_the_deadline_is_a_duration_or_a_moment(tmp_path, monkeypatch):
    for said, secs in (("6h", 21600), ("90m", 5400), ("2d", 172800),
                       (3600, 3600), ("45", 45)):
        assert machine.deadline(_hop(tmp_path, monkeypatch,
                                     deadline=said)) == secs
    assert machine.deadline(_hop(tmp_path, monkeypatch)) is None
    past = machine.deadline(_hop(tmp_path, monkeypatch,
                                 deadline="2020-01-01T00:00"))
    assert past == 0
    ahead = machine.deadline(_hop(tmp_path, monkeypatch,
                                  deadline="2030-01-01T00:00"),
                             now=0)
    assert ahead > 0
    with pytest.raises(SystemExit):
        machine.deadline(_hop(tmp_path, monkeypatch, deadline="soonish"))


def test_the_cloud_is_read_from_the_endpoints(tmp_path, monkeypatch):
    hop = _hop(tmp_path, monkeypatch)
    assert machine.where(hop) == (None, None)
    hop.target.host = "app.cluster-example.eu-west-1.rds.amazonaws.com"
    assert machine.where(hop) == ("aws", "eu-west-1")
    hop.target.host = "app.postgres.database.azure.com"
    assert machine.where(hop) == ("azure", "eastus")
    # the source in another cloud lends the target no region of its own
    hop.source.host = "db.example.eu-west-1.rds.amazonaws.com"
    assert machine.where(hop) == ("azure", "eastus")
    hop.options = {"cloud": "gcp", "region": "us-central1"}
    assert machine.where(hop) == ("gcp", "us-central1")
    # a private address the server itself named, as doctor recorded it
    from migkit.guide import record
    hop.options, hop.target.host, hop.source.host = {}, "10.0.0.9", "10.0.0.8"
    assert machine.where(hop) == (None, None)
    record(hop, {"target": {"where": {"cloud": "aws", "service": "rds",
                                      "region": ""}}})
    assert machine.where(hop) == ("aws", "us-east-1")
    hop.target.options = {"aws_region": "ap-southeast-1"}
    assert machine.where(hop) == ("aws", "ap-southeast-1")


def _moto():
    moto = pytest.importorskip("moto")
    return moto


class _Session:
    """What `refresh` asks of a boto3 session: the EC2 client moto serves
    and a price list client whose answers are stubbed."""

    def __init__(self, ec2, pricing):
        self.ec2, self.pricing = ec2, pricing

    def client(self, name, region_name=None):
        return {"ec2": self.ec2, "pricing": self.pricing}[name]


def _price_item(name, usd):
    return json.dumps({
        "product": {"attributes": {"instanceType": name}},
        "terms": {"OnDemand": {"X.Y": {"priceDimensions": {"X.Y.Z": {
            "pricePerUnit": {"USD": str(usd)}}}}}}})


def test_the_catalog_is_refreshed_from_the_providers_api(tmp_path,
                                                          monkeypatch):
    moto = _moto()
    import boto3
    from botocore.stub import ANY, Stubber
    monkeypatch.setenv("MIGKIT_INSTANCES", str(tmp_path / "fresh.json"))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    names = ["m6i.large", "c7gn.large", "no.such-type"]
    with moto.mock_aws():
        ec2 = boto3.client("ec2", region_name="eu-west-1")
        pricing = boto3.client("pricing", region_name="us-east-1")
        stub = Stubber(pricing)
        stub.add_response("get_products", {
            "FormatVersion": "aws_v1",
            "PriceList": [_price_item("m6i.large", 0.2345),
                          _price_item("c7gn.large", 0.4567)]},
            {"ServiceCode": "AmazonEC2", "Filters": ANY,
             "FormatVersion": "aws_v1", "MaxResults": 100})
        with stub:
            n = machine.refresh("eu-west-1", _Session(ec2, pricing), names)
    assert n == 2
    kept = json.loads((tmp_path / "fresh.json").read_text())
    got = kept["types"]["aws"]
    assert got["c7gn.large"]["network_gbps"] == {"baseline": 6.25,
                                                 "burst": 30.0}
    assert got["c7gn.large"]["arch"] == "arm64"
    assert got["m6i.large"]["disk_mb_s"] == {"baseline": 81.25,
                                             "burst": 1250.0}
    assert got["m6i.large"]["price_hour"] == {"eu-west-1": 0.2345}
    # taken over the shipped catalog: the refreshed price where it was
    # asked, the shipped ones elsewhere
    cat = machine.catalog()
    m6i = cat["types"]["aws"]["m6i.large"]
    assert m6i["price_hour"]["eu-west-1"] == 0.2345
    assert m6i["price_hour"]["us-east-1"] == 0.096
    # too old, and the shipped catalog stands again
    kept["refreshed_at"] = 0
    (tmp_path / "fresh.json").write_text(json.dumps(kept))
    assert machine.catalog()["types"]["aws"]["m6i.large"]["price_hour"][
        "eu-west-1"] == 0.107


def test_no_refresh_unless_asked(tmp_path, monkeypatch):
    import boto3
    monkeypatch.setenv("MIGKIT_INSTANCES", str(tmp_path / "fresh.json"))
    monkeypatch.delenv("MIGKIT_CATALOG_REFRESH", raising=False)
    monkeypatch.setattr(machine, "refresh",
                        lambda *a, **k: pytest.fail("refreshed unasked"))

    class Held:
        def get_credentials(self):
            return object()
    monkeypatch.setattr(boto3.session, "Session", lambda: Held())
    assert machine.maybe_refresh("aws", "us-east-1") == ""
    assert not (tmp_path / "fresh.json").exists()


def test_no_credentials_no_refresh(tmp_path, monkeypatch):
    import boto3
    monkeypatch.setenv("MIGKIT_INSTANCES", str(tmp_path / "fresh.json"))
    monkeypatch.setenv("MIGKIT_CATALOG_REFRESH", "1")
    monkeypatch.setattr(machine, "refresh",
                        lambda *a, **k: pytest.fail("refreshed"))

    class Bare:
        def get_credentials(self):
            return None
    monkeypatch.setattr(boto3.session, "Session", lambda: Bare())
    assert machine.maybe_refresh("aws", "us-east-1") == ""
    assert not (tmp_path / "fresh.json").exists()
    assert machine.maybe_refresh("gcp", "us-central1") == ""


def _seen(running, iops, conns, cache_gib, share, hit, at="2026-09-20"):
    return {"at": f"{at}T10:00:00", "running": running,
            "running_most": running, "iops": iops, "connections": conns,
            "cache_bytes": cache_gib * 2 ** 30, "cache_share": share,
            "hit": hit}


def test_the_target_class_comes_from_the_sources_busiest_load():
    types = machine.machines("aws", "us-east-1")
    quiet = [_seen(1, 800, 40, 2, 0.25, 0.999)]
    need, small, said = machine.target_class(quiet, types, "postgres", "aws")
    assert need["vcpu"] == 2 and need["memory_gib"] == pytest.approx(8)
    assert small is not None and small.memory_gib >= 8
    busy = quiet + [_seen(6, 9000, 700, 8, 0.25, 0.999, "2026-09-28")]
    need, big, said = machine.target_class(busy, types, "postgres", "aws")
    assert need["vcpu"] == 10                    # 6 on a CPU at 60%
    assert need["memory_gib"] == pytest.approx(32)
    assert need["connections"] == math.ceil(700 * 1.25)
    assert big.vcpu >= 10 and big.memory_gib >= 32 * 0.98
    assert big.iops[0] >= 9000
    assert big.name.startswith(("m", "r")), big.name
    assert (big.vcpu, big.memory_gib) > (small.vcpu, small.memory_gib)
    assert "2 observations from 2026-09-20" in said
    assert f"db.{big.name}" in said
    # reads that miss the cache want more memory than the source has
    missing = [_seen(1, 800, 40, 2, 0.25, 0.80)]
    need, _, said = machine.target_class(missing, types, "postgres", "aws")
    assert need["memory_gib"] == pytest.approx(16)
    assert "miss it 20% of the time" in said


def test_a_class_whose_default_connection_limit_is_too_low_is_said():
    types = machine.machines("aws", "us-east-1")
    # few CPUs and little cache, a great many connections
    seen = [_seen(1, 500, 2000, 2, 0.75, 0.999)]
    _, best, said = machine.target_class(seen, types, "mysql", "aws")
    default = int(best.memory_gib * 2 ** 30 / 12582880)
    assert default < 2500
    assert "raise max_connections" in said, said
    enough = [_seen(1, 500, 50, 2, 0.75, 0.999)]
    _, _, said = machine.target_class(enough, types, "mysql", "aws")
    assert "raise max_connections" not in said


def test_observe_turns_counters_into_rates():
    class Eng:
        def __init__(self):
            self.n = 0

        def load_sample(self, side, db):
            self.n += 1
            return {"xacts": 100 * self.n, "reads": 50.0 * self.n,
                    "writes": 25.0 * self.n, "running": self.n % 3,
                    "connections": 10 + self.n, "max_connections": 100,
                    "cache_bytes": 2 ** 30, "cache_share": 0.25,
                    "hit": 0.99}
    clock = iter([0.0, 0.0, 1.0, 2.0, 3.0, 3.0])
    got = machine.observe(Eng(), "app", seconds=3,
                          clock=lambda: next(clock), sleep=lambda s: None)
    # four readings over three seconds: 3 x 75 block reads and writes
    assert got["iops"] == pytest.approx(75.0)
    assert got["tps"] == pytest.approx(100.0)
    assert got["running_most"] == 2 and got["connections"] == 14


def test_both_engines_read_their_load(monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"))
    pg = PostgresEngine(hop)
    asked = []
    monkeypatch.setattr(pg, "_server_version", lambda side, db: 150004)
    monkeypatch.setattr(pg, "_psql", lambda side, db, sql:
                        asked.append(sql) or
                        "1200|300|80|2|17|100|134217728|0.97")
    got = pg.load_sample("src", "app")
    assert got == {"xacts": 1200.0, "reads": 300.0, "writes": 80.0,
                   "running": 2, "connections": 17, "max_connections": 100,
                   "cache_bytes": 134217728, "cache_share": 0.25,
                   "hit": 0.97}
    # before 16 there is no view of every backend's reads and writes
    assert "pg_stat_io" not in asked[0] and "pg_stat_bgwriter" in asked[0]
    monkeypatch.setattr(pg, "_server_version", lambda side, db: 160002)
    pg.__dict__.pop("_load_version")
    pg.load_sample("src", "app")
    assert "pg_stat_io" in asked[1]
    my = MySQLEngine(hop)

    def q(side, sql, args=None, fresh=False):
        if sql.startswith("show global status"):
            return [("Com_commit", "900"), ("Com_rollback", "100"),
                    ("Innodb_data_reads", "4000"),
                    ("Innodb_data_writes", "600"),
                    ("Threads_connected", "33"), ("Threads_running", "5"),
                    ("Innodb_buffer_pool_read_requests", "10000"),
                    ("Innodb_buffer_pool_reads", "250")]
        return [(151, 3 * 2 ** 30)]
    monkeypatch.setattr(my, "_q", q)
    got = my.load_sample("src", "app")
    assert got["xacts"] == 1000 and got["reads"] == 4000
    assert got["running"] == 4 and got["connections"] == 33
    assert got["cache_bytes"] == 3 * 2 ** 30 and got["cache_share"] == 0.75
    assert got["hit"] == pytest.approx(0.975)


def test_the_stages_come_from_the_hops_own_runs(tmp_path, monkeypatch):
    from migkit import planner
    hop = _hop(tmp_path, monkeypatch, cloud="aws", region="us-east-1",
               deadline="1h")
    assert machine.stages_for(hop, "builtin") is None
    assert machine.plan_line(hop, "builtin", 0) == ""
    said = machine.plan_line(hop, "builtin", 100 * GB)
    assert "needs ~26 MB/s end to end" in said, said
    # a run whose CPUs were a quarter busy: what lay beyond this machine
    # held it, and migkit's own work a core is its bytes over its CPU time
    run = 50 * 2 ** 20 * 100
    planner.record_rate(hop, "builtin", 1_000_000, 100, size=run,
                        cpu_seconds=100, cpus=4)
    st = machine.stages_for(hop, "builtin")
    assert st.beyond == pytest.approx(50 * 2 ** 20)
    assert st.per_core == pytest.approx(50 * 2 ** 20)
    said = machine.plan_line(hop, "builtin", 500 * GB)
    assert "no machine for migkit meets the deadline of 1.0 hours" in said
    assert "the servers and the link together bind at ~50 MB/s" in said
    # its CPUs all busy: this machine held it, and a bigger one is named
    planner.record_rate(hop, "builtin", 1_000_000, 100, size=run,
                        cpu_seconds=390, cpus=4)
    st = machine.stages_for(hop, "builtin")
    assert st.beyond is None
    said = machine.plan_line(hop, "builtin", 500 * GB)
    assert "no machine for migkit meets" not in said
    assert "its CPUs bind there" in said or "its network binds" in said, said
    # faster than the servers were ever seen to go is said as unproven
    assert "if the servers and the link give it: they gave ~50 MB/s" in said
    # a dump path writes the local copy and reads it back
    assert machine.stages_for(hop, "pgdump") is None
    planner.record_rate(hop, "pgdump", 10, 10, size=10 * 2 ** 20,
                        cpu_seconds=1, cpus=4)
    assert machine.stages_for(hop, "pgdump").spill == 2.0


def test_assess_says_the_class_and_the_machine(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_INSTANCES", str(tmp_path / "none.json"))
    monkeypatch.setattr(machine, "OBSERVE_SECONDS", 0.0)
    # never a real account: the refresh is its own test's, under moto
    asked = []
    monkeypatch.setattr(machine, "maybe_refresh",
                        lambda cloud, region: asked.append((cloud, region))
                        or "")
    monkeypatch.setattr("migkit.movers.chosen",
                        lambda engine, table="", hop=None, go=False:
                        "builtin")
    hop = _hop(tmp_path, monkeypatch, cloud="aws", region="us-east-1",
               deadline="2h")

    class Quiet:
        def table_facts(self, side, db):
            return {"public.t": {"bytes": 40 * 10 ** 9, "rows": 1}}

    got = {i["item"]: i["detail"] for i in machine.assess_items(
        hop, Quiet(), "app")}
    assert got["the target's class"].startswith(
        "the source's engine does not say its load")
    assert "the deadline of 2.0 hours needs ~5.3 MB/s" in \
        got["the machine to run migkit on"]

    class Loaded(Quiet):
        def load_sample(self, side, db):
            return {"xacts": 1, "reads": 1, "writes": 1, "running": 3,
                    "connections": 50, "max_connections": 100,
                    "cache_bytes": 4 * 2 ** 30, "cache_share": 0.25,
                    "hit": 0.999}

    for _ in range(2):
        got = {i["item"]: i["detail"] for i in machine.assess_items(
            hop, Loaded(), "app")}
    said = got["the target's class"]
    assert "2 observations" in said and "5 vCPU" in said, said
    assert "16 GiB of memory" in said and "db." in said
    assert len(machine.kept(hop)) == 2
    assert asked == [("aws", "us-east-1")] * 2


def test_a_hop_in_no_named_cloud_is_told_only_what_binds(tmp_path,
                                                         monkeypatch):
    from migkit import planner
    hop = _hop(tmp_path, monkeypatch)
    planner.record_rate(hop, "builtin", 1_000_000, 100,
                        size=50 * 2 ** 20 * 100, cpu_seconds=100, cpus=4)
    # nothing to name and nothing binding past a deadline: nothing said
    assert machine.plan_line(hop, "builtin", 500 * GB) == ""
    hop.options = {"deadline": "1h"}
    said = machine.plan_line(hop, "builtin", 500 * GB)
    assert "the servers and the link together bind at ~50 MB/s" in said
    assert "the hop does not say which cloud it runs in" in said
    # a deadline the servers meet: no machine to name, nothing to say
    hop.options = {"deadline": "4h"}
    assert machine.plan_line(hop, "builtin", 500 * GB) == ""


class _Stage:
    """The resource model's stage as the advisor reads it: the bytes of
    the move a second it allows, and whether the probes saw it bind."""

    def __init__(self, bound, at_least=False, truth="measured"):
        self._bound, self.at_least, self.truth = bound, at_least, truth

    def bound(self):
        return self._bound


class _Model:
    def __init__(self, **stages):
        self.stages = {k.replace("_", ":").replace("target:check",
                                                   "target-check"): v
                       for k, v in stages.items()}


def test_the_resource_models_stages_are_read_not_measured_again():
    mb = 2 ** 20
    m = _Model(source=_Stage(300 * mb), target=_Stage(40 * mb),
               target_check=_Stage(90 * mb), link_src=_Stage(120 * mb),
               link_dst=_Stage(500 * mb, at_least=True),
               cpu=_Stage(160 * mb))
    st = machine.from_model(m, cpus=4)
    assert st.source == 300 * mb and st.target == 40 * mb
    assert st.link == 120 * mb
    # a stage the probes never saw bind is a floor, never the binding one
    assert st.at_least == 500 * mb
    assert st.per_core == 40 * mb
    assert st.wire == 1.0 and "measured" in st.measured
    got = machine.advise(st, 500 * GB, 24 * 3600,
                         machine.machines("aws", "us-east-1"), "us-east-1")
    assert got.binds == "the target's writes"
    assert "so a bigger machine would not be faster" in got.words
    # the rows server to server: nothing crosses this machine's network
    assert machine.from_model(m, direct=True).wire == 0.0
    assert machine.from_model(None) is None
    assert machine.from_model(_Model()) is None


def test_no_model_here_is_no_model(tmp_path, monkeypatch):
    import builtins
    hop = _hop(tmp_path, monkeypatch)
    real = builtins.__import__

    def without_model(name, globals=None, locals=None, fromlist=(),
                      level=0):
        if level and fromlist and "model" in fromlist:
            raise ImportError("no model module")
        return real(name, globals, locals, fromlist, level)
    monkeypatch.setattr(builtins, "__import__", without_model)
    assert machine._kept_model(hop, "app") is None
    assert machine._kept_model(hop, None) is None

