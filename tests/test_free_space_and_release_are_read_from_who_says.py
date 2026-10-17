"""Free space from whoever can say it, and a release never run on refused
in words (backlog 7.free, 26.versions).

The free space comes from the first source that answers
(`decide.free_space_source`): the server's own report, this machine's
look at a data directory proved the server's own, the provider's metric -
and nobody, said so, never a guess. A server's release is held to the
releases migkit has run its engine on (`decide.version_seen`), and the
version-keyed choices it takes are listed (`decide.version_choices`)."""
import shutil
import types

import pytest

from migkit import decide, ready, sizing
from tests import servers
from tests.conftest import needs_docker


class _Eng:
    hop = types.SimpleNamespace(source=None, target=None, options={})


def test_the_servers_own_report_is_taken_first(monkeypatch):
    monkeypatch.setattr(sizing, "_provider_free",
                        lambda e, s: pytest.fail("the provider was asked"))
    e = _Eng()
    runs = []

    def server():
        runs.append(1)
        return 5 << 30, None
    got = sizing.free_bytes(e, "dst", "d", server,
                            lambda: pytest.fail("the directory was looked"))
    assert got == 5 << 30
    assert e.free_choice[("dst", "d")][1].path == "server"
    # the answer stands for the run: the server's program runs once
    assert sizing.free_bytes(e, "dst", "d", server, None) == 5 << 30
    assert runs == [1]


def test_a_server_that_will_not_say_falls_to_the_directory(monkeypatch):
    monkeypatch.setattr(sizing, "_provider_free", lambda e, s: (None, "x"))
    e = _Eng()
    got = sizing.free_bytes(e, "dst", "d",
                            lambda: (None, "the account may not run it"),
                            lambda: (7 << 20, None))
    assert got == 7 << 20
    c = e.free_choice[("dst", "d")][1]
    assert c.path == "directory" and "may not run it" in c.reason


def test_nobody_saying_is_none_with_every_why_never_a_guess(monkeypatch):
    monkeypatch.setattr(sizing, "_provider_free",
                        lambda e, s: (None, "not a managed service"))

    def broken():
        raise RuntimeError("permission denied for function pg_stat_file")
    e = _Eng()
    assert sizing.free_bytes(e, "dst", "d", lambda: (None, "no right"),
                             broken) is None
    c = e.free_choice[("dst", "d")][1]
    assert c.path == "none"
    for why in ("no right", "permission denied", "not a managed service"):
        assert why in c.reason, c.reason


def test_a_directory_of_the_same_name_that_is_not_the_servers_is_not_read(
        tmp_path):
    got, why = sizing.local_free(str(tmp_path), lambda: False)
    assert got is None and "not the server's own" in why
    got, why = sizing.local_free(str(tmp_path / "absent"), lambda: True)
    assert got is None and "not on this machine" in why
    got, why = sizing.local_free(str(tmp_path), lambda: True)
    assert got == shutil.disk_usage(tmp_path).free and why is None


def test_a_release_run_on_is_taken_and_one_outside_is_refused_in_words():
    assert decide.version_seen({"family": "postgres",
                                "version": (16, 4)}).path == "seen"
    newer = decide.version_seen({"family": "postgres", "version": (19, 0)})
    assert newer.rung is None and "newer than 17" in newer.reason
    older = decide.version_seen({"family": "mariadb", "version": (10, 6, 1)})
    assert older.rung is None and "older than 10.11" in older.reason
    assert decide.version_seen({"family": "mysql",
                                "version": None}).path == "unread"


def test_the_version_keyed_choices_are_listed_against_the_release():
    got = {w: t for _, w, t in decide.version_choices("postgres", (14, 2))}
    assert got["procedures told from functions"] is True
    assert got["the subscriber applies streamed transactions in"
               " parallel"] is False
    assert not any(t for _, _, t in decide.version_choices("postgres", None))


def _gate(src, dst):
    e = types.SimpleNamespace(server_release=lambda side, db: (
        dst if side == "dst" else src))
    return {i.what: i for i in ready._releases(e, ["d"])}


def test_a_release_not_run_on_goes_on_said_unproved_with_its_choices():
    by = _gate(("postgres", (16, 1)), ("postgres", (19, 0)))
    assert by["the source's release"].level == "pass"
    assert "procedures told from functions" in by["the source's"
                                                  " release"].detail
    t = by["the target's release"]
    assert t.level == "warn" and not t.gates
    assert t.detail.startswith("release 19.0 not run on here; the choices"
                               " keyed on version are: ")
    assert "unproved on this run" in t.detail
    assert "in parallel (taken)" in t.detail
    # MariaDB keys nothing on its version: an old one goes on, said so
    m = _gate(("mariadb", (10, 6, 1)), ("mariadb", (11, 4, 2)))
    assert m["the source's release"].level == "warn"
    assert m["the target's release"].level == "pass"


def test_only_a_release_below_every_version_keyed_choice_is_refused():
    by = _gate(("postgres", (9, 6)), ("mysql", (8, 0, 12)))
    s, t = by["the source's release"], by["the target's release"]
    assert s.level == "fail" and s.gates and "older than 10.0" in s.detail
    assert t.level == "fail" and "older than 8.0.17" in t.detail
    # 8.0.17 takes every MySQL choice but no lane runs 8.0: built
    # anyway, said unproved on this run
    s = _gate(("mysql", (8, 0, 17)), ("mysql", (8, 4, 3)))[
        "the source's release"]
    assert s.level == "warn" and "unproved on this run" in s.detail


# ---- on the lanes (not run in the ticket: built, owed a lane run) --------

pg = servers.fixture("pg")
my = servers.fixture("mysql")
maria = servers.fixture("mariadb")


@needs_docker
def test_on_postgres_the_server_says_its_free_space_and_release(pg):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="free", engine="postgres", source=pg.src.endpoint(),
              target=pg.dst.endpoint(), databases=[pg.name])
    eng = PostgresEngine(hop)
    got = eng.free_bytes("dst", pg.name)
    assert eng.free_choice[("dst", pg.name)][1].path == "server"
    data = pg.dst.sql("show data_directory")[0][0]
    df = pg.dst.exec("df", "-Pk", data).stdout.splitlines()[-1].split()
    assert abs(got - int(df[3]) * 1024) < 64 << 20
    fam, ver = eng.server_release("dst", pg.name)
    assert decide.version_seen({"family": fam, "version": ver}).path == "seen"


@needs_docker
@pytest.mark.parametrize("which", ["my", "maria"])
def test_on_mysql_and_mariadb_the_release_is_held_and_free_space_said(
        which, request):
    from migkit.config import Hop
    from migkit.engines.mysql import MySQLEngine
    s = request.getfixturevalue(which)
    hop = Hop(name="free", engine="mysql", source=s.src.endpoint(),
              target=s.dst.endpoint(), databases=[s.name])
    eng = MySQLEngine(hop)
    fam, ver = eng.server_release("dst", s.name)
    assert fam == ("mariadb" if which == "maria" else "mysql")
    assert decide.version_seen({"family": fam, "version": ver}).path == "seen"
    assert eng.free_bytes("dst", s.name) is None
    assert "does not report" in eng.free_choice[("dst", s.name)][1].reason


def _provider(monkeypatch, adapter):
    from migkit import managed
    monkeypatch.setattr(managed, "api_conf", lambda h, e, s: ("x", {}))
    monkeypatch.setattr(managed, "adapter", lambda h, e, s: adapter)


def test_the_providers_metric_is_read_where_the_server_and_directory_cannot(
        monkeypatch):
    from migkit import cloudapi
    asked = []

    class Watch:
        def __init__(self, points):
            self.points = points

        def get_metric_statistics(self, **kw):
            asked.append(kw)
            return {"Datapoints": self.points}
    aws = cloudapi.Aws({"product": "rds", "id": "db1", "region": "r"})
    monkeypatch.setattr(aws, "_rds", lambda: ("instance", {}))
    monkeypatch.setattr(aws, "client", lambda s=None: Watch([
        {"Timestamp": 2, "Minimum": 9 << 30},
        {"Timestamp": 1, "Minimum": 1}]))
    _provider(monkeypatch, aws)
    e = _Eng()
    got = sizing.free_bytes(e, "dst", "d", lambda: (None, "no right"),
                            lambda: (None, "not here"))
    assert got == 9 << 30 and e.free_choice[("dst", "d")][1].path == \
        "provider"
    assert asked[0]["MetricName"] == "FreeStorageSpace"
    assert asked[0]["Dimensions"] == [{"Name": "DBInstanceIdentifier",
                                       "Value": "db1"}]
    # an Aurora cluster is read by its volume's bytes left
    monkeypatch.setattr(aws, "_rds", lambda: ("cluster", {}))
    aws.free_bytes()
    assert asked[-1]["MetricName"] == "AuroraVolumeBytesLeftTotal"
    # a metric with no point: nobody says, and the plan says so
    monkeypatch.setattr(aws, "client", lambda s=None: Watch([]))
    e = _Eng()
    assert sizing.free_bytes(e, "dst", "d", lambda: (None, "no right"),
                             lambda: (None, "not here")) is None
    c = e.free_choice[("dst", "d")][1]
    assert c.path == "none" and "does not say" in str(c.passed) + c.reason
    assert "free" in cloudapi.Aws.UNPROVED_ITEMS


def test_a_cloud_sql_instances_free_space_is_its_quota_less_what_it_uses(
        monkeypatch):
    from migkit import cloudapi
    said = {"bytes_used": 3 << 30, "quota": 10 << 30}

    class Api:
        def projects(self):
            return self

        def timeSeries(self):
            return self

        def list(self, **kw):
            self.metric = kw["filter"].split("/disk/")[1].split('"')[0]
            assert '"p:db1"' in kw["filter"]
            return self

        def execute(self):
            got = said.get(self.metric)
            return {"timeSeries": [{"points": [
                {"value": {"int64Value": str(got)}}]}]} if got else {}
    gcp = cloudapi.Gcp({"product": "cloudsql", "id": "db1", "region": "r"})
    monkeypatch.setattr(gcp, "_monitoring", lambda: Api())
    monkeypatch.setattr(gcp, "_project", lambda: "p")
    assert gcp.free_bytes() == 7 << 30
    del said["quota"]
    assert gcp.free_bytes() is None


def test_a_release_not_read_is_said_through_the_rung():
    class Eng:
        def server_release(self, side, db):
            raise RuntimeError("permission denied for function version")
    got = ready._releases(Eng(), ["d"])
    assert [i.level for i in got] == ["warn", "warn"]
    assert "nothing keyed on a version is chosen" in got[0].detail
    assert "permission denied" in got[0].detail


def test_the_releases_seen_are_the_lanes_images():
    """MySQL runs here on 8.4 and 9.4 (`tests/servers.py`): an 8.0 server
    is one no version-keyed choice was run on."""
    assert decide.VERSIONS_SEEN["mysql"] == ((8, 4), (9, 4))
    assert decide.version_seen({"family": "mysql",
                                "version": (8, 0, 36)}).rung is None


def test_a_service_with_no_class_to_copy_from_takes_no_neutral_way_unread():
    for shape in ("keyless", "no unique index", "wide key"):
        assert decide.WAYS[shape]["synapse"][0] == "yes", shape


def test_azure_flexible_servers_free_space_is_read_from_metrics(monkeypatch):
    from types import SimpleNamespace
    from migkit import cloudapi

    class Monitor:
        class metrics:
            @staticmethod
            def list(*a, **kw):
                class Point:
                    minimum = 8 << 30

                class TimeSeries:
                    data = [Point()]

                class Item:
                    timeseries = [TimeSeries()]

                return SimpleNamespace(value=[Item()])

    az = cloudapi.Azure({"product": "flexible-postgres", "id": "db1",
                         "resource_group": "rg", "subscription": "sub"})
    monkeypatch.setattr(az, "_monitoring", lambda: Monitor())
    assert az.free_bytes() == 8 << 30
    assert cloudapi.Azure.UNPROVED_ITEMS == {"free": "free.provider-metric"}


def test_tencent_cloud_free_space_is_read_from_monitor(monkeypatch):
    from migkit import cloudapi

    class Monitor:
        def GetMonitorData(self, req):
            return {"DataPoints": [{"Values": [12 << 30]}]}

    tc = cloudapi.Tencent({"product": "cdb", "id": "cdb-1",
                          "region": "ap-guangzhou"})
    monkeypatch.setattr(tc, "_monitoring", lambda: Monitor())
    assert tc.free_bytes() == 12 << 30
    assert cloudapi.Tencent.UNPROVED_ITEMS == {"free": "free.provider-metric"}


def test_alibaba_cloud_free_space_is_read_from_cloud_monitor(monkeypatch):
    import json
    from types import SimpleNamespace
    from migkit import cloudapi

    class Cms:
        def describe_metric_last(self, req):
            return SimpleNamespace(body=SimpleNamespace(
                datapoints=json.dumps([{"Minimum": 15 << 30}])))

    ali = cloudapi.Alibaba({"product": "rds", "id": "rm-1",
                            "region": "cn-hangzhou"})
    monkeypatch.setattr(ali, "_monitoring", lambda: Cms())
    assert ali.free_bytes() == 15 << 30
    assert cloudapi.Alibaba.UNPROVED_ITEMS == {"free": "free.provider-metric"}


def test_huawei_cloud_free_space_is_read_from_ces(monkeypatch):
    from migkit import cloudapi

    class Ces:
        def show_metric_data(self, **kw):
            return {"datapoints": [{"min": 20 << 30}]}

    hw = cloudapi.Huawei({"product": "rds", "id": "inst-1",
                          "region": "cn-north-4"})
    monkeypatch.setattr(hw, "_monitoring", lambda: Ces())
    assert hw.free_bytes() == 20 << 30
    assert cloudapi.Huawei.UNPROVED_ITEMS == {"free": "free.provider-metric"}


def test_oracle_and_ibm_free_space_read(monkeypatch):
    from types import SimpleNamespace
    from migkit import cloudapi

    class OciMon:
        def summarize_metrics_data(self, *a, **kw):
            return SimpleNamespace(data=[SimpleNamespace(
                aggregated_datapoints=[SimpleNamespace(value=25 << 30)])])

    ora = cloudapi.Oracle({"product": "mysql", "id": "ocid1",
                           "region": "us-ashburn-1"})
    monkeypatch.setattr(ora, "_monitoring", lambda: OciMon())
    assert ora.free_bytes() == 25 << 30

    class IbmMon:
        def get_metric(self, id, metric):
            return 30 << 30

    ibm = cloudapi.Ibm({"product": "icd", "id": "crn1", "region": "us-south"})
    monkeypatch.setattr(ibm, "_monitoring", lambda: IbmMon())
    assert ibm.free_bytes() == 30 << 30


def test_account_listing_beyond_aws_rds(monkeypatch):
    from types import SimpleNamespace
    from migkit import cloudapi

    class GcpApi:
        class instances:
            @staticmethod
            def list(**kw):
                class Req:
                    @staticmethod
                    def execute():
                        return {"items": [{
                            "name": "sql1", "databaseVersion": "POSTGRES_15",
                            "ipAddresses": [{"ipAddress": "10.0.0.1"}]}]}
                return Req()

    gcp = cloudapi.Gcp({"product": "cloudsql", "id": "sql1", "region": "r"})
    monkeypatch.setattr(gcp, "api", lambda: GcpApi())
    monkeypatch.setattr(gcp, "_project", lambda: "p")
    assert gcp.endpoints() == [{
        "id": "sql1", "engine": "POSTGRES_15", "host": "10.0.0.1",
        "port": 5432}]

    class AzureClient:
        class servers:
            @staticmethod
            def list():
                return [SimpleNamespace(
                    name="pg1",
                    fully_qualified_domain_name="pg1.postgres.database.azure.com")]

    az = cloudapi.Azure({"product": "flexible-postgres", "id": "pg1",
                         "resource_group": "rg", "subscription": "sub"})
    monkeypatch.setattr(az, "client", lambda: AzureClient())
    assert az.endpoints() == [{
        "id": "pg1", "engine": "postgres",
        "host": "pg1.postgres.database.azure.com", "port": 5432}]

    tc = cloudapi.Tencent({"product": "cdb", "id": "cdb1", "region": "r"})
    monkeypatch.setattr(tc, "_all", lambda *a, **k: [{
        "InstanceId": "cdb1", "Vip": "10.0.0.2", "Vport": 3306,
        "EngineVersion": "8.0"}])
    assert tc.endpoints() == [{
        "id": "cdb1", "engine": "8.0", "host": "10.0.0.2", "port": 3306}]


def test_every_cloudapi_adapter_has_free_bytes_and_declares_unproved():
    from migkit import capabilities as caps, cloudapi
    assert "cloudapi.py" in caps.DECLARING
    for who, cls in cloudapi.ADAPTERS.items():
        assert hasattr(cls, "free_bytes")
        assert callable(getattr(cls, "free_bytes"))
        assert who in cloudapi.UNPROVED_ON
        assert who in cloudapi.UNPROVED_ITEMS
        assert getattr(cls, "UNPROVED_ITEMS") == {
            "free": "free.provider-metric"}
        why, cells = caps.UNPROVED_ELSEWHERE[who]
        assert "free.provider-metric" in cells
        assert why == cloudapi.UNPROVED_ON[who]

