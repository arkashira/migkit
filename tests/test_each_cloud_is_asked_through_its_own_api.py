"""Each cloud's managed databases are asked through that cloud's own API
(`migkit.cloudapi`): the instance's facts, a restore point made and waited
for, the restore points listed, a copy made from one, parameters set and
waited for until applied, an instance found by its address, and the
credentials found where the provider's own CLI finds them.

Nothing here reaches a cloud. AWS runs against moto where moto implements
the call, and against botocore's own stubber - which holds every request
and answer to the service's model - where a wait has to see an answer
change. Every other provider runs against a stand-in that answers in the
recorded shape of its API and checks what it is asked against the SDK
itself: Google's discovery documents, Azure's operation groups and models,
Tencent Cloud's and Alibaba Cloud's request and response models, Oracle
Cloud's clients and models, IBM Cloud's own request building - so a field
name the provider does not have fails here. Huawei Cloud's SDK lives in an
environment of its own; its program's request building is proved with the
models it is given, and its adapter against the SDK 3.1.216 fields
recorded below. Every chain of credentials runs against a home directory
and an environment made for the test, the instance's metadata stubbed.
"""
import datetime
import importlib
import inspect
import io
import json
import types
from pathlib import Path

import pytest

from migkit import cloudapi
from migkit.cloudapi import NotOffered, RestorePoint

#: every product each provider's adapter answers for
PRODUCTS = {
    "aws": {"rds", "docdb", "elasticache", "memorydb", "redshift", "dynamodb",
            "opensearch", "msk", "neptune", "keyspaces"},
    "gcp": {"cloudsql", "alloydb", "memorystore", "bigquery", "spanner",
            "firestore"},
    "azure": {"flexible-postgres", "flexible-mysql", "sql", "cosmosdb",
              "redis"},
    "tencent": {"cdb", "postgres", "mongodb", "redis", "cynosdb", "sqlserver",
                "mariadb"},
    "alibaba": {"rds", "dds", "polardb", "kvstore", "polardbx"},
    "huawei": {"rds", "dds", "gaussdb", "geminidb"},
    "oracle": {"mysql", "autonomous"},
    "ibm": {"icd"},
}

AT = "2026-10-01T10:00:00Z"
#: the same moment as Tencent Cloud's API writes it, in Beijing time
AT_BEIJING = "2026-10-01 18:00:00"


@pytest.fixture(autouse=True)
def quick(monkeypatch):
    monkeypatch.setenv("MIGKIT_CLOUD_POLL", "0")
    monkeypatch.setenv("MIGKIT_CLOUD_WAIT", "60")


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A home directory of the test's own, and an environment with no
    provider's credentials in it."""
    monkeypatch.setenv("HOME", str(tmp_path))
    for key in list(__import__("os").environ):
        if key.startswith(("AWS_", "GOOGLE_", "CLOUDSDK_", "AZURE_",
                           "TENCENTCLOUD_", "TCCLI_", "ALIBABA_CLOUD_",
                           "ALIBABACLOUD_", "ALICLOUD_", "HUAWEICLOUD_",
                           "OCI_", "IBMCLOUD_", "IC_API_KEY")):
            monkeypatch.delenv(key)
    return tmp_path


def _log():
    said = []
    return said, said.append


def _within(given, got, where="answer"):
    """Every field of `given` is in `got` with the same value: a recorded
    answer's field the SDK's model does not have is dropped by it, and
    fails here."""
    if isinstance(given, dict):
        assert isinstance(got, dict), (where, got)
        for key, value in given.items():
            assert key in got and got[key] is not None, (where, key, got)
            _within(value, got[key], f"{where}.{key}")
    elif isinstance(given, list):
        assert isinstance(got, list) and len(got) == len(given), (where, got)
        for i, (a, b) in enumerate(zip(given, got)):
            _within(a, b, f"{where}[{i}]")
    else:
        assert given == got, (where, given, got)


def _next(answer, asked):
    """An answer: one of a list in turn, the last one kept; a callable asked
    with what was sent; else the answer itself."""
    if callable(answer):
        return answer(asked)
    if isinstance(answer, list):
        return answer.pop(0) if len(answer) > 1 else answer[0]
    return answer


# --- the registry ------------------------------------------------------------

def test_every_product_is_there_and_offered_reads_the_adapters_own_table():
    assert set(cloudapi.ADAPTERS) == set(PRODUCTS)
    kinds = ("snapshot", "backup", "point in time", "")
    for provider, products in PRODUCTS.items():
        cls = cloudapi.ADAPTERS[provider]
        assert cls.provider == provider
        assert set(cls.PRODUCTS) == products, provider
        for product, abilities in cls.PRODUCTS.items():
            assert set(abilities) == {"restore_point", "clone", "parameters"}
            assert abilities["restore_point"] in kinds, (provider, product)
            for what, value in abilities.items():
                assert cloudapi.offered(provider, product, what) is \
                    bool(value), (provider, product, what)
    # what the catalogue of managed services relies on
    assert cloudapi.Aws.PRODUCTS["rds"]["restore_point"] == "snapshot"
    assert cloudapi.offered("aws", "rds", "clone")
    assert not cloudapi.offered("aws", "opensearch", "restore_point")
    assert not cloudapi.offered("aws", "msk", "restore_point")
    assert cloudapi.Gcp.PRODUCTS["bigquery"]["restore_point"] == \
        "point in time"
    assert not cloudapi.offered("gcp", "memorystore", "restore_point")
    assert not cloudapi.offered("azure", "redis", "restore_point")
    assert not cloudapi.offered("tencent", "sqlserver", "clone")
    # a provider or a product there is not is offered nothing
    assert not cloudapi.offered("nowhere", "rds", "restore_point")
    assert not cloudapi.offered("aws", "no-such-product", "clone")


def test_an_unknown_provider_or_product_is_refused_naming_what_there_is():
    with pytest.raises(SystemExit) as e:
        cloudapi.adapter("nowhere", {"product": "rds", "id": "x"})
    for name in PRODUCTS:
        assert name in str(e.value), e.value
    with pytest.raises(SystemExit) as e:
        cloudapi.adapter("aws", {"product": "no-such", "id": "x"})
    assert "no-such" in str(e.value) and "docdb" in str(e.value), e.value
    assert isinstance(cloudapi.adapter("AWS", {"product": "rds", "id": "x"}),
                      cloudapi.Aws)


NOT_OFFERED = [
    ("aws", "opensearch", "restore_point"), ("aws", "msk", "clone"),
    ("aws", "dynamodb", "parameters"), ("gcp", "memorystore",
                                        "restore_point"),
    ("gcp", "bigquery", "clone"), ("azure", "redis", "clone"),
    ("azure", "sql", "parameters"), ("tencent", "sqlserver", "clone"),
    ("tencent", "mariadb", "restore_point"), ("huawei", "dds", "parameters"),
    ("oracle", "autonomous", "parameters"), ("aws", "keyspaces",
                                             "parameters"),
    ("gcp", "spanner", "parameters"), ("gcp", "firestore", "parameters")]


@pytest.mark.parametrize("provider,product,what", NOT_OFFERED)
def test_what_a_products_api_has_not_is_said_and_nothing_is_asked(
        provider, product, what):
    a = cloudapi.adapter(provider, {"product": product, "id": "x"})
    ask = {"restore_point": lambda: a.restore_point("rp", lambda m: None),
           "clone": lambda: a.clone("copy", None, lambda m: None),
           "parameters": lambda: a.set_parameters({"x": "1"},
                                                  lambda m: None)}[what]
    with pytest.raises(NotOffered) as e:
        ask()
    said = str(e.value)
    assert said.startswith(f"{a.WHO}'s API offers no"), said
    assert product in said and cloudapi.ABILITY[what] in said, said
    # refused from the table: no credentials were even looked for
    assert a._found is None


@pytest.mark.parametrize("provider,product", [("azure", "sql"),
                                              ("gcp", "bigquery")])
def test_a_point_in_time_product_has_its_restore_point_now_without_a_call(
        provider, product):
    a = cloudapi.adapter(provider, {"product": product, "id": "x"})
    before = cloudapi._now()
    got = a.restore_point("rp", lambda m: None)
    assert got.kind == "point in time" and got.ready
    assert got.id == got.at
    assert cloudapi._time(got.at) >= before
    assert a._found is None


def test_the_wait_asks_again_until_an_answer_and_says_what_was_late():
    answers = iter([None, None, "ready"])
    assert cloudapi.until(lambda: next(answers), "x", lambda m: None,
                          poll=0) == "ready"
    with pytest.raises(StopIteration):
        next(answers)
    with pytest.raises(SystemExit, match="the copy was not ready within"):
        cloudapi.until(lambda: None, "the copy", lambda m: None, poll=0,
                       most=0.05)
    seen = iter([("creating", 1), ("failed", 2)])
    with pytest.raises(SystemExit, match="the backup is failed"):
        cloudapi._ready(lambda: next(seen), "the backup", lambda m: None,
                        ("available",), ("failed",))


def test_every_sdk_is_accounted_for_and_read_off_the_code():
    from migkit import tooluse
    assert "migkit.cloudapi" in tooluse.CONTRIBUTORS
    for name in cloudapi.SDKS:
        tool = tooluse.REGISTRY[name]
        assert tool.licence, name
        assert tooluse.unexplained(name) == [], name
        assert tooluse.stale(name) == [], name
        used = [k for k, (state, _) in tooluse.accounts(name).items()
                if state == tooluse.USED]
        assert len(used) >= 10, (name, used)
    # read off the code: a call the code makes is used, and its name in
    # the table alone would not make it so
    assert tooluse.entry("aws-sdk", "rds", "create_db_snapshot")[0] == \
        tooluse.USED
    assert tooluse.entry("aws-sdk", "rds", "copy_db_snapshot")[0] == \
        tooluse.UNUSED
    assert tooluse.entry("tencentcloud-sdk", "cdb",
                         "CreateCloneInstance")[0] == tooluse.USED


# --- AWS ---------------------------------------------------------------------

REGION = "eu-west-1"


@pytest.fixture
def aws(monkeypatch):
    moto = pytest.importorskip("moto")
    for key, value in (("AWS_ACCESS_KEY_ID", "testing"),
                       ("AWS_SECRET_ACCESS_KEY", "testing"),
                       ("AWS_DEFAULT_REGION", REGION)):
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with moto.mock_aws():
        yield


def _boto(service):
    import boto3
    return boto3.client(service, region_name=REGION)


def _pg(name="pg1", group="app-pg", make_group=True):
    rds = _boto("rds")
    if make_group:
        rds.create_db_parameter_group(DBParameterGroupName=group,
                                      DBParameterGroupFamily="postgres16",
                                      Description="the app's")
        # each seeded with the other's way of applying, so the way a
        # change is asked with is the adapter's, not the seed's
        rds.modify_db_parameter_group(DBParameterGroupName=group, Parameters=[
            {"ParameterName": "work_mem", "ParameterValue": "4096",
             "ApplyMethod": "pending-reboot", "ApplyType": "dynamic",
             "IsModifiable": True},
            {"ParameterName": "shared_buffers", "ParameterValue": "16384",
             "ApplyMethod": "immediate", "ApplyType": "static",
             "IsModifiable": True}])
    return rds.create_db_instance(
        DBInstanceIdentifier=name, Engine="postgres", EngineVersion="16.3",
        DBInstanceClass="db.r6g.large", MasterUsername="app",
        MasterUserPassword="CHANGE_ME1", AllocatedStorage=100,
        DBParameterGroupName=group,
        PreferredMaintenanceWindow="sun:03:00-sun:04:00")["DBInstance"]


def _aws(product="rds", ident="pg1", **conf):
    return cloudapi.adapter("aws", {"product": product, "id": ident,
                                    "region": REGION, **conf})


def test_an_rds_instances_facts_and_parameters_are_read(aws):
    made = _pg()
    got = _aws().instance(["work_mem", "shared_buffers", "no_such"])
    assert (got.provider, got.product, got.id) == ("aws", "rds", "pg1")
    assert (got.engine, got.version, got.klass) == ("postgres", "16.3",
                                                    "db.r6g.large")
    # the class's size, as EC2 describes the machine it runs on
    assert (got.cpu, got.memory_gb) == (2, 16.0)
    assert got.storage_gb == 100
    assert got.maintenance == "sun:03:00-sun:04:00"
    assert (got.host, got.port) == (made["Endpoint"]["Address"],
                                    made["Endpoint"]["Port"])
    assert got.group == "app-pg" and got.group_shared is False
    assert got.parameters["work_mem"] == cloudapi.Param("4096", True, False)
    assert got.parameters["shared_buffers"] == cloudapi.Param("16384", True,
                                                              True)
    assert "no_such" not in got.parameters
    # a second instance on the same group shares it
    _pg("pg2", make_group=False)
    assert _aws().instance().group_shared is True


def test_an_rds_parameter_is_set_and_waited_for_until_it_reads_back(aws):
    _pg()
    said, log = _log()
    before = _aws().set_parameters({"work_mem": 8192}, log)
    assert before == {"work_mem": "4096"}
    assert _aws().instance(["work_mem"]).parameters["work_mem"].value == \
        "8192"
    # a static one is set to wait for the next restart, and that is said
    _aws().set_parameters({"shared_buffers": "32768"}, log)
    rows = _boto("rds").describe_db_parameters(
        DBParameterGroupName="app-pg")["Parameters"]
    applied = {r["ParameterName"]: r.get("ApplyMethod") for r in rows}
    assert applied == {"work_mem": "immediate",
                       "shared_buffers": "pending-reboot"}
    with pytest.raises(SystemExit, match="has no parameter"):
        _aws().set_parameters({"no_such": "1"}, log)


def test_an_rds_snapshot_is_made_listed_found_and_copied(aws):
    _pg()
    said, log = _log()
    point = _aws().restore_point("migkit-rp", log)
    assert (point.id, point.kind, point.ready) == ("migkit-rp", "snapshot",
                                                   True)
    assert point.at.endswith("Z")
    listed = {p.id: p for p in _aws().restore_points()}
    assert listed["migkit-rp"].ready
    host = _boto("rds").describe_db_instances(DBInstanceIdentifier="pg1")[
        "DBInstances"][0]["Endpoint"]["Address"]
    assert _aws(ident="").find(host) == "pg1"
    assert _aws(ident="").find("10.0.0.99") is None
    copy = _aws().clone("pg1-copy", point, log)
    assert copy["id"] == "pg1-copy" and copy["kind"] == "instance"
    real = _boto("rds").describe_db_instances(
        DBInstanceIdentifier="pg1-copy")["DBInstances"][0]
    assert copy["host"] == real["Endpoint"]["Address"]


def test_an_aurora_cluster_is_cloned_copy_on_write_with_a_writer(
        aws, monkeypatch):
    import migkit.config as cfg
    rds = _boto("rds")
    rds.create_db_cluster(DBClusterIdentifier="app",
                          Engine="aurora-postgresql", MasterUsername="app",
                          MasterUserPassword="CHANGE_ME1")
    rds.create_db_instance(DBInstanceIdentifier="app-1",
                           DBClusterIdentifier="app",
                           Engine="aurora-postgresql",
                           DBInstanceClass="db.r6g.large")
    asked, made = [], cfg._aws_client

    def spied(service, region=None, role=None):
        c = made(service, region, role)
        c.meta.events.register(
            "provide-client-params.rds.RestoreDBClusterToPointInTime",
            lambda params, **kw: asked.append(dict(params)))
        return c
    monkeypatch.setattr(cfg, "_aws_client", spied)
    got = _aws(ident="app").clone("app-copy", None, lambda m: None)
    assert len(asked) == 1 and asked[0]["RestoreType"] == "copy-on-write"
    assert asked[0]["UseLatestRestorableTime"] is True
    writer = rds.describe_db_instances(DBInstanceIdentifier="app-copy-1")[
        "DBInstances"][0]
    assert writer["DBClusterIdentifier"] == "app-copy"
    assert writer["DBInstanceClass"] == "db.r6g.large"
    cluster = rds.describe_db_clusters(DBClusterIdentifier="app-copy")[
        "DBClusters"][0]
    assert got == {"host": cluster["Endpoint"], "port": cluster["Port"],
                   "id": "app-copy", "kind": "cluster"}


def _stubbed(monkeypatch, service="rds"):
    """One client of the service, every request and answer held to the
    service's model by botocore's stubber, handed out for every client
    the adapter asks for."""
    import boto3
    from botocore.stub import Stubber

    import migkit.config as cfg
    client = boto3.client(service, region_name=REGION,
                          aws_access_key_id="testing",
                          aws_secret_access_key="testing")
    stub = Stubber(client)
    monkeypatch.setattr(cfg, "_aws_client", lambda s, r=None, role=None:
                        client)
    return stub


def _no_cluster(stub):
    stub.add_client_error("describe_db_clusters",
                          service_error_code="DBClusterNotFoundFault",
                          http_status_code=404,
                          expected_params={"DBClusterIdentifier": "pg1"})
    stub.add_response("describe_db_instances", {"DBInstances": [
        {"DBInstanceIdentifier": "pg1", "Engine": "postgres"}]},
        {"DBInstanceIdentifier": "pg1"})


def test_an_rds_snapshot_is_waited_for_while_it_is_being_made(monkeypatch):
    stub = _stubbed(monkeypatch)
    _no_cluster(stub)
    stub.add_response("create_db_snapshot", {"DBSnapshot": {
        "DBSnapshotIdentifier": "rp", "Status": "creating"}},
        {"DBSnapshotIdentifier": "rp", "DBInstanceIdentifier": "pg1"})
    stub.add_response("describe_db_snapshots", {"DBSnapshots": [{
        "DBSnapshotIdentifier": "rp", "Status": "creating"}]},
        {"DBSnapshotIdentifier": "rp"})
    stub.add_response("describe_db_snapshots", {"DBSnapshots": [{
        "DBSnapshotIdentifier": "rp", "Status": "available",
        "SnapshotCreateTime": datetime.datetime(
            2026, 10, 1, 10, 0, tzinfo=datetime.timezone.utc)}]},
        {"DBSnapshotIdentifier": "rp"})
    with stub:
        got = _aws().restore_point("rp", lambda m: None)
        stub.assert_no_pending_responses()
    assert got == RestorePoint("rp", "snapshot", "available", AT, True)


def test_an_rds_snapshot_that_fails_ends_the_wait(monkeypatch):
    stub = _stubbed(monkeypatch)
    _no_cluster(stub)
    stub.add_response("create_db_snapshot", {"DBSnapshot": {
        "DBSnapshotIdentifier": "rp", "Status": "creating"}})
    stub.add_response("describe_db_snapshots", {"DBSnapshots": [{
        "DBSnapshotIdentifier": "rp", "Status": "failed"}]})
    with stub, pytest.raises(SystemExit, match="the snapshot rp is failed"):
        _aws().restore_point("rp", lambda m: None)


def test_a_docdb_cluster_snapshot_is_made_and_waited_for(monkeypatch):
    stub = _stubbed(monkeypatch, "docdb")
    stub.add_response("create_db_cluster_snapshot", {"DBClusterSnapshot": {
        "DBClusterSnapshotIdentifier": "rp", "Status": "creating"}},
        {"DBClusterSnapshotIdentifier": "rp", "DBClusterIdentifier": "docs"})
    for status in ("creating", "available"):
        stub.add_response("describe_db_cluster_snapshots", {
            "DBClusterSnapshots": [{"DBClusterSnapshotIdentifier": "rp",
                                    "Status": status}]},
            {"DBClusterSnapshotIdentifier": "rp"})
    with stub:
        got = _aws("docdb", "docs").restore_point("rp", lambda m: None)
        stub.assert_no_pending_responses()
    assert (got.id, got.kind, got.ready) == ("rp", "snapshot", True)


def test_a_neptune_cluster_snapshot_is_made_and_its_cluster_found_as_its_own(
        monkeypatch):
    """Neptune's API is RDS's in shape and lists every engine's clusters:
    an address is looked for among Neptune's alone."""
    stub = _stubbed(monkeypatch, "neptune")
    stub.add_response("create_db_cluster_snapshot", {"DBClusterSnapshot": {
        "DBClusterSnapshotIdentifier": "rp", "Status": "creating"}},
        {"DBClusterSnapshotIdentifier": "rp", "DBClusterIdentifier": "graph"})
    for status in ("creating", "available"):
        stub.add_response("describe_db_cluster_snapshots", {
            "DBClusterSnapshots": [{"DBClusterSnapshotIdentifier": "rp",
                                    "Status": status}]},
            {"DBClusterSnapshotIdentifier": "rp"})
    engine = {"Filters": [{"Name": "engine", "Values": ["neptune"]}]}
    stub.add_response("describe_db_clusters", {"DBClusters": [{
        "DBClusterIdentifier": "graph", "Engine": "neptune", "Port": 8182,
        "Endpoint": "graph.cluster-abc.eu-west-1.neptune.amazonaws.com",
        "ReaderEndpoint": "graph.cluster-ro-abc.eu-west-1.neptune.amazonaws"
                          ".com"}]}, engine)
    with stub:
        a = _aws("neptune", "graph")
        got = a.restore_point("rp", lambda m: None)
        assert a.find("graph.cluster-abc.eu-west-1.neptune.amazonaws.com",
                      8182) == "graph"
        stub.assert_no_pending_responses()
    assert (got.id, got.kind, got.ready) == ("rp", "snapshot", True)


def test_a_neptune_copy_is_a_cluster_cloned_with_a_writer_of_its_own(
        monkeypatch):
    stub = _stubbed(monkeypatch, "neptune")
    cluster = {"DBClusterIdentifier": "graph", "Engine": "neptune",
               "Status": "available", "DBClusterParameterGroup": "g1",
               "DBClusterMembers": [{"DBInstanceIdentifier": "graph-1",
                                     "IsClusterWriter": True}]}
    stub.add_response("describe_db_clusters", {"DBClusters": [cluster]},
                      {"DBClusterIdentifier": "graph"})
    stub.add_response("describe_db_instances", {"DBInstances": [{
        "DBInstanceIdentifier": "graph-1", "DBInstanceClass": "db.r6g.large",
        "Engine": "neptune"}]}, {"DBInstanceIdentifier": "graph-1"})
    stub.add_response("restore_db_cluster_to_point_in_time", {"DBCluster": {
        "DBClusterIdentifier": "graph-copy"}}, {
        "DBClusterIdentifier": "graph-copy",
        "SourceDBClusterIdentifier": "graph", "RestoreType": "copy-on-write",
        "UseLatestRestorableTime": True})
    stub.add_response("describe_db_clusters", {"DBClusters": [{
        "DBClusterIdentifier": "graph-copy", "Status": "available"}]},
        {"DBClusterIdentifier": "graph-copy"})
    stub.add_response("create_db_instance", {"DBInstance": {
        "DBInstanceIdentifier": "graph-copy-1"}}, {
        "DBInstanceIdentifier": "graph-copy-1",
        "DBClusterIdentifier": "graph-copy", "Engine": "neptune",
        "DBInstanceClass": "db.r6g.large"})
    stub.add_response("describe_db_instances", {"DBInstances": [{
        "DBInstanceIdentifier": "graph-copy-1",
        "DBInstanceStatus": "available"}]},
        {"DBInstanceIdentifier": "graph-copy-1"})
    stub.add_response("describe_db_clusters", {"DBClusters": [{
        "DBClusterIdentifier": "graph-copy", "Status": "available",
        "Endpoint": "graph-copy.cluster-abc.example.com", "Port": 8182}]},
        {"DBClusterIdentifier": "graph-copy"})
    with stub:
        got = _aws("neptune", "graph").clone("graph-copy", None,
                                             lambda m: None)
        stub.assert_no_pending_responses()
    assert got == {"host": "graph-copy.cluster-abc.example.com",
                   "port": 8182, "id": "graph-copy", "kind": "cluster"}


def _ks_table(name, pitr="ENABLED", at=None, status="ACTIVE"):
    got = {"keyspaceName": "shop", "tableName": name, "status": status,
           "resourceArn": f"arn:aws:cassandra:{REGION}:000000000000:"
                          f"/keyspace/shop/table/{name}",
           "pointInTimeRecovery": {"status": pitr}}
    if at:
        got["pointInTimeRecovery"]["earliestRestorableTimestamp"] = at
    return got


def test_a_keyspace_without_point_in_time_recovery_has_no_restore_point(
        monkeypatch):
    stub = _stubbed(monkeypatch, "keyspaces")
    stub.add_response("list_tables", {"tables": [
        {"keyspaceName": "shop", "tableName": t,
         "resourceArn": f"arn:aws:cassandra:{REGION}:000000000000:/"
                        f"keyspace/shop/table/{t}"}
        for t in ("orders", "items")]}, {"keyspaceName": "shop"})
    stub.add_response("get_table", _ks_table("orders"),
                      {"keyspaceName": "shop", "tableName": "orders"})
    stub.add_response("get_table", _ks_table("items", "DISABLED"),
                      {"keyspaceName": "shop", "tableName": "items"})
    with stub, pytest.raises(SystemExit, match="off on items"):
        _aws("keyspaces", "shop").restore_point("rp", lambda m: None)


def test_a_keyspace_is_copied_table_by_table_at_one_time(monkeypatch):
    stub = _stubbed(monkeypatch, "keyspaces")
    early = datetime.datetime(2026, 9, 30, 10, 0,
                              tzinfo=datetime.timezone.utc)
    later = datetime.datetime(2026, 10, 1, 9, 0,
                              tzinfo=datetime.timezone.utc)
    listing = {"tables": [
        {"keyspaceName": "shop", "tableName": t,
         "resourceArn": f"arn:aws:cassandra:{REGION}:000000000000:/"
                        f"keyspace/shop/table/{t}"}
        for t in ("orders", "items")]}
    # the restore points: the latest of the tables' earliest times
    stub.add_response("list_tables", listing, {"keyspaceName": "shop"})
    stub.add_response("get_table", _ks_table("orders", at=early))
    stub.add_response("get_table", _ks_table("items", at=later))
    # the copy
    stub.add_response("list_tables", listing, {"keyspaceName": "shop"})
    stub.add_response("get_table", _ks_table("orders", at=early))
    stub.add_response("get_table", _ks_table("items", at=later))
    stub.add_response("create_keyspace", {
        "resourceArn": f"arn:aws:cassandra:{REGION}:000000000000:/keyspace/"
                       "shop_copy_20261001"},
        {"keyspaceName": "shop_copy_20261001"})
    stub.add_client_error("get_keyspace",
                          service_error_code="ResourceNotFoundException",
                          http_status_code=404)
    stub.add_response("get_keyspace", {
        "keyspaceName": "shop_copy_20261001",
        "resourceArn": f"arn:aws:cassandra:{REGION}:000000000000:/keyspace/"
                       "shop_copy_20261001",
        "replicationStrategy": "SINGLE_REGION"})
    at = datetime.datetime(2026, 10, 1, 10, 0, tzinfo=datetime.timezone.utc)
    for t in ("orders", "items"):
        stub.add_response("restore_table", {
            "restoredTableARN": f"arn:aws:cassandra:{REGION}:000000000000:/"
                                f"keyspace/shop_copy_20261001/table/{t}"},
            {"sourceKeyspaceName": "shop", "sourceTableName": t,
             "targetKeyspaceName": "shop_copy_20261001",
             "targetTableName": t, "restoreTimestamp": at})
    for t in ("orders", "items"):
        stub.add_response("get_table", {**_ks_table(t, status="RESTORING"),
                                        "keyspaceName": "shop_copy_20261001"})
        stub.add_response("get_table", {**_ks_table(t),
                                        "keyspaceName": "shop_copy_20261001"})
    with stub:
        a = _aws("keyspaces", "shop")
        assert a.restore_points() == [RestorePoint(
            "2026-10-01T09:00:00Z", "point in time", "",
            "2026-10-01T09:00:00Z", True)]
        got = a.clone("shop-copy-20261001", AT, lambda m: None)
        stub.assert_no_pending_responses()
    assert got == {"host": f"cassandra.{REGION}.amazonaws.com", "port": 9142,
                   "id": "shop_copy_20261001", "kind": "keyspace"}
    with pytest.raises(NotOffered):
        a.set_parameters({"x": 1}, lambda m: None)


def test_a_parameter_of_the_providers_default_group_is_never_changed(
        monkeypatch):
    stub = _stubbed(monkeypatch)
    stub.add_response("describe_db_parameters", {"Parameters": [{
        "ParameterName": "work_mem", "ParameterValue": "4096",
        "ApplyType": "dynamic", "IsModifiable": True}]},
        {"DBParameterGroupName": "default.postgres16"})
    monkeypatch.setattr(cloudapi.Aws, "_describe", lambda self: {
        "kind": "instance", "it": {}, "writer": {},
        "groups": [("db", "default.postgres16")]})
    with stub, pytest.raises(SystemExit, match="default parameter group"):
        _aws().set_parameters({"work_mem": "8192"}, lambda m: None)


def test_a_dynamodb_backup_is_made_listed_and_restored_as_a_new_table(aws):
    ddb = _boto("dynamodb")
    ddb.create_table(TableName="orders", BillingMode="PAY_PER_REQUEST",
                     KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                     AttributeDefinitions=[{"AttributeName": "id",
                                            "AttributeType": "S"}])
    a = _aws("dynamodb", "orders")
    point = a.restore_point("rp", lambda m: None)
    assert point.kind == "backup" and point.ready
    assert point.id.startswith("arn:aws:dynamodb:")
    assert [p.id for p in a.restore_points()] == [point.id]
    got = a.clone("orders-copy", point, lambda m: None)
    assert got["id"] == "orders-copy" and got["kind"] == "table"
    assert ddb.describe_table(TableName="orders-copy")["Table"][
        "TableStatus"] == "ACTIVE"
    with pytest.raises(NotOffered):
        a.set_parameters({"x": 1}, lambda m: None)


def test_aws_credentials_are_found_where_the_cli_finds_them(home,
                                                            monkeypatch):
    import botocore.utils
    fetched = []
    monkeypatch.setattr(botocore.utils.InstanceMetadataFetcher,
                        "retrieve_iam_role_credentials",
                        lambda self: fetched.append(1) or {})
    # the environment first
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "CHANGE_ME")
    cred, where = _aws().credentials()
    assert cred.access_key == "AKIAENV" and "environment" in where
    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY")
    # then the CLI's files, the named profile among them
    (home / ".aws").mkdir()
    (home / ".aws" / "credentials").write_text(
        "[default]\naws_access_key_id = AKIAFILE\n"
        "aws_secret_access_key = CHANGE_ME\n"
        "[ops]\naws_access_key_id = AKIAOPS\n"
        "aws_secret_access_key = CHANGE_ME\n")
    cred, where = _aws().credentials()
    assert cred.access_key == "AKIAFILE"
    assert where == "the CLI's credentials file, profile default"
    cred, where = _aws(profile="ops").credentials()
    assert cred.access_key == "AKIAOPS" and where.endswith("profile ops")
    with pytest.raises(SystemExit, match="no profile nope"):
        _aws(profile="nope").credentials()
    (home / ".aws" / "credentials").unlink()
    # then the instance's role, from its metadata
    monkeypatch.setattr(
        botocore.utils.InstanceMetadataFetcher,
        "retrieve_iam_role_credentials", lambda self: {
            "role_name": "app", "access_key": "ASIAROLE",
            "secret_key": "CHANGE_ME", "token": "t",
            "expiry_time": "2099-01-01T00:00:00Z"})
    cred, where = _aws().credentials()
    assert where == "the instance's role"
    assert cred.get_frozen_credentials().access_key == "ASIAROLE"
    # and nothing found says every place it looked
    monkeypatch.setattr(botocore.utils.InstanceMetadataFetcher,
                        "retrieve_iam_role_credentials", lambda self: {})
    with pytest.raises(SystemExit) as e:
        _aws().credentials()
    for place in ("environment", "credentials and config files",
                  "single sign-on", "web identity", "container's role",
                  "instance's role"):
        assert place in str(e.value), e.value


def test_an_aws_role_is_assumed_over_what_was_found(home, monkeypatch):
    import boto3.session
    from botocore.stub import Stubber
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "CHANGE_ME")
    role = "arn:aws:iam::222222222222:role/migkit"  # an example account
    real, stubs = boto3.session.Session.client, []

    def client(self, service, *args, **kw):
        c = real(self, service, *args, **kw)
        if service == "sts":
            s = Stubber(c)
            s.add_response("assume_role", {"Credentials": {
                "AccessKeyId": "ASIAROLE", "SecretAccessKey": "CHANGE_ME",
                "SessionToken": "t", "Expiration": datetime.datetime(
                    2099, 1, 1, tzinfo=datetime.timezone.utc)}},
                {"RoleArn": role, "RoleSessionName": "migkit"})
            s.activate()
            stubs.append(s)
        return c
    monkeypatch.setattr(boto3.session.Session, "client", client)
    cred, where = _aws(role=role).credentials()
    assert stubs, "the role was not asked for"
    stubs[0].assert_no_pending_responses()
    assert cred.access_key == "ASIAROLE"
    assert where == (f"the role {role}, assumed over the environment"
                     " (AWS_ACCESS_KEY_ID)")


# --- Google Cloud ------------------------------------------------------------

DOCS = Path(importlib.import_module("googleapiclient").__file__).parent / \
    "discovery_cache" / "documents"


class Discovery:
    """A discovery-built client's stand-in, held to the service's own
    discovery document: a resource or a method it does not have, a
    parameter it does not take, a required one left out and a body field
    its request's schema does not have all fail."""

    def __init__(self, doc, answers):
        self.doc = json.loads((DOCS / doc).read_text())
        self.answers, self.calls = answers, []

    def __getattr__(self, name):
        if name.startswith("_") or name not in self.doc["resources"]:
            raise AttributeError(name)
        return lambda: _Resource(self, [name], self.doc["resources"][name])

    def asked(self, path):
        return [kw for p, kw in self.calls if p == path]


class _Resource:
    def __init__(self, api, path, res):
        self.api, self.path, self.res = api, path, res

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self.res.get("resources", {}):
            return lambda: _Resource(self.api, self.path + [name],
                                     self.res["resources"][name])
        method = self.res["methods"][name]

        def call(**kw):
            params = method.get("parameters", {})
            allowed = set(params) | ({"body"} if method.get("request")
                                     else set())
            assert set(kw) <= allowed, (name, set(kw) - allowed)
            missing = [p for p, spec in params.items()
                       if spec.get("required") and p not in kw]
            assert not missing, (name, missing)
            if "body" in kw:
                schema = self.api.doc["schemas"][method["request"]["$ref"]]
                assert set(kw["body"]) <= set(schema["properties"]), \
                    (name, set(kw["body"]) - set(schema["properties"]))
            path = ".".join(self.path + [name])
            self.api.calls.append((path, kw))
            got = _next(self.api.answers[path], kw)
            return types.SimpleNamespace(execute=lambda: got)
        return call


def _gcp(product, api, **conf):
    a = cloudapi.adapter("gcp", {"product": product, "id": "app-db",
                                 "project": "proj",
                                 "region": "europe-west1", **conf})
    a._found, a._api = ("credentials", "a test's"), api
    return a


SQL_INSTANCE = {
    "name": "app-db", "databaseVersion": "POSTGRES_16", "state": "RUNNABLE",
    "settings": {"tier": "db-custom-4-16384", "dataDiskSizeGb": "100",
                 "maintenanceWindow": {"day": 7, "hour": 3},
                 "databaseFlags": [
                     {"name": "max_connections", "value": "200"},
                     {"name": "log_min_duration_statement",
                      "value": "1000"}]},
    "ipAddresses": [{"type": "PRIMARY", "ipAddress": "203.0.113.5"},
                    {"type": "PRIVATE", "ipAddress": "10.0.0.5"}]}
SQL_FLAGS = {"items": [
    {"name": "max_connections", "requiresRestart": True},
    {"name": "log_min_duration_statement", "requiresRestart": False},
    {"name": "work_mem", "requiresRestart": False}]}


def test_a_cloud_sql_instances_facts_and_flags_are_read():
    api = Discovery("sqladmin.v1beta4.json", {
        "instances.get": SQL_INSTANCE, "flags.list": SQL_FLAGS})
    got = _gcp("cloudsql", api).instance(["max_connections", "work_mem",
                                          "no_such"])
    assert (got.engine, got.version, got.klass) == ("postgres", "16",
                                                    "db-custom-4-16384")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 16.0, 100)
    assert got.maintenance == "Sunday 03:00 UTC"
    assert (got.host, got.port, got.status) == ("10.0.0.5", 5432,
                                                "RUNNABLE")
    assert (got.group, got.group_shared) == ("", False)
    assert got.parameters == {
        "max_connections": cloudapi.Param("200", True, True),
        "work_mem": cloudapi.Param(None, True, False),
        "no_such": cloudapi.Param(None, False, None)}
    assert api.asked("flags.list") == [{"databaseVersion": "POSTGRES_16"}]


def test_a_cloud_sql_backup_is_made_and_its_operation_waited_for():
    api = Discovery("sqladmin.v1beta4.json", {
        "backupRuns.insert": {"name": "op-1", "status": "PENDING"},
        "operations.get": [{"name": "op-1", "status": "RUNNING"},
                           {"name": "op-1", "status": "DONE",
                            "backupContext": {"backupId": "42"}}],
        "backupRuns.list": {"items": [
            {"id": "41", "status": "SUCCESSFUL",
             "endTime": "2026-09-30T10:00:00Z"},
            {"id": "42", "status": "SUCCESSFUL", "endTime": AT,
             "description": "rp"}]}})
    a = _gcp("cloudsql", api)
    got = a.restore_point("rp", lambda m: None)
    assert got == RestorePoint("42", "backup", "SUCCESSFUL", AT, True)
    assert api.asked("backupRuns.insert") == [
        {"project": "proj", "instance": "app-db",
         "body": {"description": "rp"}}]
    assert len(api.asked("operations.get")) == 2
    assert [p.id for p in a.restore_points()] == ["41", "42"]


def test_a_cloud_sql_operation_that_fails_ends_the_wait():
    api = Discovery("sqladmin.v1beta4.json", {
        "backupRuns.insert": {"name": "op-1"},
        "operations.get": {"name": "op-1", "status": "DONE", "error": {
            "errors": [{"message": "the backup quota is spent"}]}}})
    with pytest.raises(SystemExit, match="failed: the backup quota is spent"):
        _gcp("cloudsql", api).restore_point("rp", lambda m: None)


def test_a_cloud_sql_instance_is_cloned_to_a_time_and_never_from_a_backup():
    copy = {"name": "app-db-copy", "databaseVersion": "POSTGRES_16",
            "ipAddresses": [{"type": "PRIVATE", "ipAddress": "10.0.0.6"}]}
    api = Discovery("sqladmin.v1beta4.json", {
        "instances.clone": {"name": "op-2"},
        "operations.get": [{"name": "op-2", "status": "PENDING"},
                           {"name": "op-2", "status": "DONE"}],
        "instances.get": lambda kw: copy if kw["instance"] == "app-db-copy"
        else SQL_INSTANCE})
    a = _gcp("cloudsql", api)
    got = a.clone("app-db-copy", AT, lambda m: None)
    assert got == {"host": "10.0.0.6", "port": 5432, "id": "app-db-copy",
                   "kind": "instance"}
    body = api.asked("instances.clone")[0]["body"]["cloneContext"]
    assert body["destinationInstanceName"] == "app-db-copy"
    assert body["pointInTime"] == AT
    with pytest.raises(NotOffered, match="point in time"):
        a.clone("x", RestorePoint("42", "backup", ready=True), lambda m: None)


def test_a_cloud_sql_flag_is_set_with_every_other_flag_kept():
    api = Discovery("sqladmin.v1beta4.json", {
        "instances.get": SQL_INSTANCE, "flags.list": SQL_FLAGS,
        "instances.patch": {"name": "op-3"},
        "operations.get": [{"name": "op-3", "status": "RUNNING"},
                           {"name": "op-3", "status": "DONE"}]})
    said, log = _log()
    before = _gcp("cloudsql", api).set_parameters({"max_connections": 300},
                                                  log)
    assert before == {"max_connections": "200"}
    flags = api.asked("instances.patch")[0]["body"]["settings"][
        "databaseFlags"]
    assert sorted(flags, key=lambda f: f["name"]) == [
        {"name": "log_min_duration_statement", "value": "1000"},
        {"name": "max_connections", "value": "300"}]
    assert len(api.asked("operations.get")) == 2
    assert any("restarts it for max_connections" in s for s in said), said
    with pytest.raises(SystemExit, match="is no flag"):
        _gcp("cloudsql", api).set_parameters({"no_such": 1}, log)


def test_a_cloud_sql_instance_is_found_by_its_address():
    api = Discovery("sqladmin.v1beta4.json", {
        "instances.list": {"items": [SQL_INSTANCE]}})
    assert _gcp("cloudsql", api).find("10.0.0.5") == "app-db"
    assert _gcp("cloudsql", api).find("10.0.0.99") is None


def test_an_alloydb_backup_is_made_and_its_operation_waited_for():
    where = "projects/proj/locations/europe-west1"
    api = Discovery("alloydb.v1.json", {
        "projects.locations.backups.create": {
            "name": f"{where}/operations/op-9", "done": False},
        "projects.locations.operations.get": [
            {"name": f"{where}/operations/op-9", "done": False},
            {"name": f"{where}/operations/op-9", "done": True}],
        "projects.locations.backups.get": {
            "name": f"{where}/backups/rp", "state": "READY",
            "createCompletionTime": AT}})
    got = _gcp("alloydb", api).restore_point("rp", lambda m: None)
    assert got == RestorePoint("rp", "backup", "READY", AT, True)
    made = api.asked("projects.locations.backups.create")[0]
    assert made["parent"] == where and made["backupId"] == "rp"
    assert made["body"] == {"clusterName": f"{where}/clusters/app-db",
                            "type": "ON_DEMAND"}
    assert len(api.asked("projects.locations.operations.get")) == 2
    api.answers["projects.locations.operations.get"] = {
        "name": "x", "done": True, "error": {"message": "no room"}}
    with pytest.raises(SystemExit, match="failed: no room"):
        _gcp("alloydb", api).restore_point("rp", lambda m: None)


SPANNER = "projects/proj/instances/main"


def test_a_spanner_backup_is_made_at_a_version_time_and_waited_for(
        monkeypatch):
    monkeypatch.setattr(cloudapi, "_now", lambda: datetime.datetime(
        2026, 10, 1, 10, 0, tzinfo=datetime.timezone.utc))
    backup = {"name": f"{SPANNER}/backups/rp", "database":
              f"{SPANNER}/databases/shop", "versionTime": AT}
    api = Discovery("spanner.v1.json", {
        "projects.instances.backups.create": {
            "name": f"{SPANNER}/backups/rp/operations/o1"},
        "projects.instances.backups.get": [
            {**backup, "state": "CREATING"}, {**backup, "state": "READY"}],
        "projects.instances.backups.list": {"backups": [
            {**backup, "state": "READY"}]}})
    a = _gcp("spanner", api, id="main/shop", keep_days=3)
    got = a.restore_point("rp", lambda m: None)
    assert got == RestorePoint("rp", "backup", "READY", AT, True)
    made = api.asked("projects.instances.backups.create")[0]
    assert (made["parent"], made["backupId"]) == (SPANNER, "rp")
    assert made["body"] == {"database": f"{SPANNER}/databases/shop",
                            "versionTime": AT,
                            "expireTime": "2026-10-04T10:00:00Z"}
    assert len(api.asked("projects.instances.backups.get")) == 2
    assert [p.id for p in a.restore_points()] == ["rp"]
    assert api.asked("projects.instances.backups.list")[0]["filter"] == \
        f'database:"{SPANNER}/databases/shop"'
    with pytest.raises(NotOffered, match="ALTER DATABASE"):
        a.set_parameters({"version_retention_period": "7d"}, lambda m: None)
    with pytest.raises(SystemExit, match="<instance>/<database>"):
        _gcp("spanner", api, id="main").restore_point("rp", lambda m: None)


def test_a_spanner_copy_at_a_time_is_a_backup_at_that_time_restored(
        monkeypatch):
    """Spanner restores only a backup: a copy asked for at a time is a
    backup made at that version time first - and a name it does not take
    is made one it takes."""
    backup = {"name": f"{SPANNER}/backups/x", "versionTime": AT,
              "state": "READY"}
    api = Discovery("spanner.v1.json", {
        "projects.instances.backups.create": {"name": "op"},
        "projects.instances.backups.get": lambda kw: {
            **backup, "name": kw["name"]},
        "projects.instances.databases.restore": {"name": "op2"},
        "projects.instances.databases.get": [{"state": "CREATING"},
                                             {"state": "READY_OPTIMIZING"}]})
    a = _gcp("spanner", api, id="main/shop")
    got = a.clone("Shop_Copy.2026", AT, lambda m: None)
    made = api.asked("projects.instances.backups.create")[0]
    assert made["backupId"] == "shop-copy-2026-at"
    assert made["body"]["versionTime"] == AT
    restored = api.asked("projects.instances.databases.restore")[0]
    assert restored == {"parent": SPANNER, "body": {
        "databaseId": "shop-copy-2026",
        "backup": f"{SPANNER}/backups/shop-copy-2026-at"}}
    assert got == {"host": "spanner.googleapis.com", "port": 443,
                   "id": "main/shop-copy-2026", "kind": "database"}
    # from a backup it keeps: no backup made first
    api.calls.clear()
    a.clone("again", RestorePoint("rp", "backup", ready=True),
            lambda m: None)
    assert not api.asked("projects.instances.backups.create")
    assert api.asked("projects.instances.databases.restore")[0]["body"][
        "backup"] == f"{SPANNER}/backups/rp"
    assert len(cloudapi._fit("migkit-" + "h" * 40 + "-source-20261001100000",
                             30)) == 30


FIRESTORE = {"name": "projects/proj/databases/(default)",
             "locationId": "eur3", "type": "FIRESTORE_NATIVE",
             "earliestVersionTime": "2026-09-24T10:00:00Z",
             "pointInTimeRecoveryEnablement":
                 "POINT_IN_TIME_RECOVERY_ENABLED"}


def test_a_firestore_restore_point_is_a_time_held_only_with_seven_days():
    off = {**FIRESTORE, "pointInTimeRecoveryEnablement":
           "POINT_IN_TIME_RECOVERY_DISABLED"}
    api = Discovery("firestore.v1.json", {"projects.databases.get": off})
    with pytest.raises(SystemExit, match="kept an hour only"):
        _gcp("firestore", api, id="").restore_point("rp", lambda m: None)
    api = Discovery("firestore.v1.json", {
        "projects.databases.get": FIRESTORE,
        "projects.locations.backups.list": {"backups": [
            {"name": "projects/proj/locations/eur3/backups/b1",
             "database": FIRESTORE["name"], "state": "READY",
             "snapshotTime": AT},
            {"name": "projects/proj/locations/eur3/backups/other",
             "database": "projects/proj/databases/other", "state": "READY",
             "snapshotTime": AT}]}})
    a = _gcp("firestore", api, id="")
    got = a.restore_point("rp", lambda m: None)
    assert got.kind == "point in time" and got.ready
    assert api.asked("projects.databases.get") == [
        {"name": "projects/proj/databases/(default)"}]
    assert [(p.id, p.kind) for p in a.restore_points()] == [
        ("2026-09-24T10:00:00Z", "point in time"), ("b1", "backup")]
    assert api.asked("projects.locations.backups.list")[0] == {
        "parent": "projects/proj/locations/eur3"}


def test_a_firestore_copy_is_cloned_at_a_time_or_restored_from_a_backup(
        monkeypatch):
    monkeypatch.setattr(cloudapi, "_now", lambda: datetime.datetime(
        2026, 10, 1, 12, 0, tzinfo=datetime.timezone.utc))
    api = Discovery("firestore.v1.json", {
        "projects.databases.get": FIRESTORE,
        "projects.databases.clone": {"name": "projects/proj/databases/"
                                             "copy/operations/o1"},
        "projects.databases.restore": {"name": "projects/proj/databases/"
                                               "copy2/operations/o2"},
        "projects.databases.operations.get": [{"done": False},
                                              {"done": True}]})
    a = _gcp("firestore", api, id="")
    said, log = _log()
    got = a.clone("copy", "2026-10-01T09:30:20Z", log)
    sent = api.asked("projects.databases.clone")[0]
    assert sent == {"parent": "projects/proj", "body": {
        "databaseId": "copy", "pitrSnapshot": {
            "database": FIRESTORE["name"],
            "snapshotTime": "2026-10-01T09:30:00Z"}}}
    assert any("whole minute" in s for s in said), said
    assert got == {"host": "firestore.googleapis.com", "port": 443,
                   "id": "copy", "kind": "database"}
    # within the hour, the time as it is asked
    api.answers["projects.databases.operations.get"] = {"done": True}
    a.clone("copy", "2026-10-01T11:30:20Z", log)
    assert api.asked("projects.databases.clone")[1]["body"]["pitrSnapshot"][
        "snapshotTime"] == "2026-10-01T11:30:20Z"
    a.clone("copy2", RestorePoint("b1", "backup", ready=True), log)
    assert api.asked("projects.databases.restore")[0]["body"] == {
        "databaseId": "copy2",
        "backup": "projects/proj/locations/eur3/backups/b1"}
    api.answers["projects.databases.operations.get"] = {
        "done": True, "error": {"message": "no room"}}
    with pytest.raises(SystemExit, match="failed: no room"):
        a.clone("copy3", None, log)


ADC = {"type": "authorized_user", "client_id": "id.example.com",
       "client_secret": "CHANGE_ME", "refresh_token": "CHANGE_ME"}


def test_google_credentials_are_found_where_gcloud_finds_them(home,
                                                              monkeypatch):
    import google.auth._cloud_sdk
    import google.auth._default
    import google.auth.credentials
    monkeypatch.setattr(google.auth._cloud_sdk, "get_project_id",
                        lambda: "proj")
    monkeypatch.setattr(google.auth._default, "_get_gce_credentials",
                        lambda *a, **k: (None, None))
    # a key file the environment names, first
    key = home / "key.json"
    key.write_text(json.dumps(ADC))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "proj")
    _, where = _gcp_bare().credentials()
    assert where == "the key file GOOGLE_APPLICATION_CREDENTIALS names"
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS")
    # then gcloud's application default login
    adc = home / ".config" / "gcloud" / \
        "application_default_credentials.json"
    adc.parent.mkdir(parents=True)
    adc.write_text(json.dumps(ADC))
    _, where = _gcp_bare().credentials()
    assert where == "gcloud's application default login"
    adc.unlink()
    # then the instance's service account, from its metadata
    monkeypatch.setattr(google.auth._default, "_get_gce_credentials",
                        lambda *a, **k: (
                            google.auth.credentials.AnonymousCredentials(),
                            "proj"))
    _, where = _gcp_bare().credentials()
    assert where == "the instance's service account"
    # and nothing found says every place it looked
    monkeypatch.setattr(google.auth._default, "_get_gce_credentials",
                        lambda *a, **k: (None, None))
    with pytest.raises(SystemExit) as e:
        _gcp_bare().credentials()
    for place in ("GOOGLE_APPLICATION_CREDENTIALS", "application default",
                  "instance's service account"):
        assert place in str(e.value), e.value


def _gcp_bare(**conf):
    return cloudapi.adapter("gcp", {"product": "cloudsql", "id": "app-db",
                                    "project": "proj", **conf})


def test_a_google_service_account_is_impersonated_over_what_was_found(
        home, monkeypatch):
    import google.auth._cloud_sdk
    monkeypatch.setattr(google.auth._cloud_sdk, "get_project_id",
                        lambda: "proj")
    adc = home / ".config" / "gcloud" / \
        "application_default_credentials.json"
    adc.parent.mkdir(parents=True)
    adc.write_text(json.dumps(ADC))
    role = "migkit@proj.iam.gserviceaccount.com"
    creds, where = _gcp_bare(role=role).credentials()
    assert where == (f"the service account {role}, impersonated over"
                     " gcloud's application default login")
    asked = []

    def request(url, method="GET", body=None, headers=None, timeout=None,
                **kw):
        asked.append((method, url))
        data = ({"accessToken": "CHANGE_ME", "expireTime":
                 "2099-01-01T00:00:00Z"} if "generateAccessToken" in url
                else {"access_token": "CHANGE_ME", "expires_in": 3600})
        return types.SimpleNamespace(status=200, headers={},
                                     data=json.dumps(data).encode())
    creds.refresh(request)
    assert ("POST", "https://iamcredentials.googleapis.com/v1/projects/-/"
                    f"serviceAccounts/{role}:generateAccessToken") in asked


# --- Azure -------------------------------------------------------------------

class Management:
    """A management client's stand-in: each call is held to the real
    client's operation group - the method there, its arguments bound to
    its signature - recorded, and answered."""

    def __init__(self, product, answers):
        from azure.core.credentials import AccessToken

        class Token:
            def get_token(self, *scopes, **kw):
                return AccessToken("CHANGE_ME", 4102444800)
        module, name = cloudapi.Azure.CLIENTS[product]
        self.real = getattr(importlib.import_module(module), name)(
            Token(), "00000000-0000-0000-0000-000000000000")
        self.answers, self.calls = answers, []

    def __getattr__(self, group):
        if group.startswith("_"):
            raise AttributeError(group)
        return _Group(self, group, getattr(self.real, group))

    def asked(self, path):
        return [args for p, args in self.calls if p == path]


class _Group:
    def __init__(self, client, group, ops):
        self.client, self.group, self.ops = client, group, ops

    def __getattr__(self, method):
        real = getattr(self.ops, method)

        def call(*args, **kw):
            inspect.signature(real).bind(*args, **kw)
            path = f"{self.group}.{method}"
            self.client.calls.append((path, args))
            return _next(self.client.answers[path], args)
        return call


class Poller:
    """A long-running operation's poller, done after the states given."""

    def __init__(self, result, states=("InProgress", "Succeeded")):
        self.states, self._result, self.asked = list(states), result, 0
        self.state = None

    def done(self):
        self.asked += 1
        self.state = self.states.pop(0) if len(self.states) > 1 \
            else self.states[0]
        return self.state != "InProgress"

    def status(self):
        return self.state

    def result(self):
        return self._result


PG_ID = ("/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups"
         "/rg-app/providers/Microsoft.DBforPostgreSQL/flexibleServers/app-db")


def _pg_server(name="app-db", host="app-db.postgres.database.azure.com"):
    import azure.mgmt.postgresqlflexibleservers.models as pm
    s = pm.Server(location="eastus", version="16",
                  sku=pm.Sku(name="Standard_D4ds_v5", tier="GeneralPurpose"),
                  storage=pm.Storage(storage_size_gb=128),
                  maintenance_window=pm.MaintenanceWindow(
                      custom_window="Enabled", day_of_week=0, start_hour=3,
                      start_minute=30))
    s.fully_qualified_domain_name, s.state, s.id, s.name = \
        host, "Ready", PG_ID.replace("app-db", name), name
    return s


def _pg_config(value, dynamic=True, read_only=False):
    import azure.mgmt.postgresqlflexibleservers.models as pm
    c = pm.Configuration(value=value, source="system-default")
    c.is_dynamic_config, c.is_read_only = dynamic, read_only
    return c


def _azure(product, client, **conf):
    a = cloudapi.adapter("azure", {"product": product, "id": "app-db",
                                   "resource_group": "rg-app",
                                   "subscription": "sub", **conf})
    a._found, a._client = ("credentials", "a test's"), client
    return a


def test_a_flexible_servers_facts_and_parameters_are_read():
    configs = {"timezone": _pg_config("UTC"), "work_mem": _pg_config("4096"),
               "shared_buffers": _pg_config("16384", dynamic=False)}
    client = Management("flexible-postgres", {
        "servers.get": _pg_server(),
        "capabilities_by_location.list": [types.SimpleNamespace(
            as_dict=lambda: {"supported_server_editions": [{
                "name": "GeneralPurpose", "supported_server_skus": [{
                    "name": "Standard_D4ds_v5", "v_cores": 4,
                    "supported_memory_per_vcore_mb": 4096}]}]})],
        "configurations.get": lambda args: configs[args[2]]})
    got = _azure("flexible-postgres", client).instance(["work_mem",
                                                         "shared_buffers"])
    assert (got.engine, got.version, got.klass) == ("postgres", "16",
                                                    "Standard_D4ds_v5")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 16.0, 128)
    assert got.maintenance == "Sunday 03:30 UTC"
    assert (got.host, got.port, got.status) == (
        "app-db.postgres.database.azure.com", 5432, "Ready")
    assert got.time_zone == "UTC"
    assert got.parameters == {
        "work_mem": cloudapi.Param("4096", True, False),
        "shared_buffers": cloudapi.Param("16384", True, True)}
    assert client.asked("servers.get") == [("rg-app", "app-db")]


def test_a_flexible_server_backup_is_made_and_waited_for():
    import azure.mgmt.postgresqlflexibleservers.models as pm
    made = pm.BackupAutomaticAndOnDemand(
        backup_type="Customer_On_Demand",
        completed_time=cloudapi._time(AT))
    poller = Poller(made, ("InProgress", "InProgress", "Succeeded"))
    client = Management("flexible-postgres", {
        "backups_automatic_and_on_demand.begin_create": poller})
    got = _azure("flexible-postgres", client).restore_point("rp",
                                                           lambda m: None)
    assert got == RestorePoint("rp", "backup", "succeeded", AT, True)
    assert poller.asked == 3
    assert client.asked("backups_automatic_and_on_demand.begin_create") == \
        [("rg-app", "app-db", "rp")]
    client.answers["backups_automatic_and_on_demand.begin_create"] = Poller(
        None, ("InProgress", "Failed"))
    with pytest.raises(SystemExit, match="the backup rp is failed"):
        _azure("flexible-postgres", client).restore_point("rp",
                                                          lambda m: None)


def test_a_flexible_servers_backups_are_listed():
    import azure.mgmt.postgresqlflexibleservers.models as pm
    b = pm.BackupAutomaticAndOnDemand(backup_type="Full",
                                      completed_time=cloudapi._time(AT))
    b.name = "backup_20261001"
    client = Management("flexible-postgres", {
        "backups_automatic_and_on_demand.list_by_server": [b]})
    got = _azure("flexible-postgres", client).restore_points()
    assert got == [RestorePoint("backup_20261001", "backup", "Full", AT,
                                True)]


def test_a_flexible_server_is_copied_to_a_point_in_time():
    made = []

    def create(args):
        made.append(args)
        return Poller(_pg_server("app-db-copy",
                                 "app-db-copy.postgres.database.azure.com"))
    client = Management("flexible-postgres", {
        "servers.get": _pg_server(), "servers.begin_create_or_update": create})
    got = _azure("flexible-postgres", client).clone("app-db-copy", AT,
                                                    lambda m: None)
    assert got == {"host": "app-db-copy.postgres.database.azure.com",
                   "port": 5432, "id": "app-db-copy", "kind": "server"}
    rg, name, body = made[0]
    assert (rg, name) == ("rg-app", "app-db-copy")
    assert body.create_mode == "PointInTimeRestore"
    assert body.source_server_resource_id == PG_ID
    assert body.point_in_time_utc == cloudapi._time(AT)


def test_a_flexible_servers_parameter_is_set_and_read_back():
    import azure.mgmt.postgresqlflexibleservers.models as pm
    configs = {"work_mem": _pg_config("4096")}
    sent = []

    def update(args):
        sent.append(args[3])
        configs[args[2]] = _pg_config(args[3].value)
        return Poller(None, ("Succeeded",))
    client = Management("flexible-postgres", {
        "configurations.get": lambda args: configs[args[2]],
        "configurations.begin_update": update})
    before = _azure("flexible-postgres", client).set_parameters(
        {"work_mem": 8192}, lambda m: None)
    assert before == {"work_mem": "4096"}
    assert isinstance(sent[0], pm.ConfigurationForUpdate)
    assert (sent[0].value, sent[0].source) == ("8192", "user-override")
    assert configs["work_mem"].value == "8192"
    configs["locked"] = _pg_config("1", read_only=True)
    with pytest.raises(SystemExit, match="locked cannot be changed"):
        _azure("flexible-postgres", client).set_parameters({"locked": 2},
                                                           lambda m: None)


def test_a_flexible_server_is_found_by_its_address():
    client = Management("flexible-postgres", {
        "servers.list_by_resource_group": [_pg_server("other", "o.example"),
                                           _pg_server()]})
    a = _azure("flexible-postgres", client)
    assert a.find("app-db.postgres.database.azure.com") == "app-db"
    assert a.find("10.0.0.99") is None


def test_a_cosmos_account_has_a_point_in_time_only_with_continuous_backup():
    import azure.mgmt.cosmosdb.models as cm

    def account(policy):
        return cm.DatabaseAccountGetResults(
            location="eastus", kind="MongoDB",
            properties=cm.DatabaseAccountGetProperties(backup_policy=policy))
    client = Management("cosmosdb", {"database_accounts.get": account(
        cm.PeriodicModeBackupPolicy())})
    with pytest.raises(NotOffered, match="periodic backups"):
        _azure("cosmosdb", client).restore_point("rp", lambda m: None)
    client.answers["database_accounts.get"] = account(
        cm.ContinuousModeBackupPolicy())
    got = _azure("cosmosdb", client).restore_point("rp", lambda m: None)
    assert got.kind == "point in time" and got.ready
    assert _azure("cosmosdb", client).instance().engine == "mongodb"


AZURE_CHAIN = ("EnvironmentCredential", "WorkloadIdentityCredential",
               "ManagedIdentityCredential", "SharedTokenCacheCredential",
               "VisualStudioCodeCredential", "AzureCliCredential",
               "AzurePowerShellCredential", "AzureDeveloperCliCredential",
               "InteractiveBrowserCredential")


def _azure_chain(monkeypatch, working=None):
    """Every member of the default chain stood in for: only `working`
    gives a token."""
    import azure.identity
    from azure.core.credentials import AccessToken, AccessTokenInfo
    from azure.identity import CredentialUnavailableError
    from azure.identity._credentials.broker import BrokerCredential
    for name in AZURE_CHAIN + ("BrokerCredential",):
        cls = BrokerCredential if name == "BrokerCredential" else \
            getattr(azure.identity, name)
        ok = name == working

        def token(self, *scopes, _ok=ok, **kw):
            if not _ok:
                raise CredentialUnavailableError("not here")
            return AccessToken("CHANGE_ME", 4102444800)

        def info(self, *scopes, _ok=ok, **kw):
            if not _ok:
                raise CredentialUnavailableError("not here")
            return AccessTokenInfo("CHANGE_ME", 4102444800)
        monkeypatch.setattr(cls, "get_token", token)
        monkeypatch.setattr(cls, "get_token_info", info, raising=False)


@pytest.mark.parametrize("member,where", [
    ("EnvironmentCredential",
     "the environment's service principal (AZURE_CLIENT_ID)"),
    ("AzureCliCredential", "the az CLI's login"),
    ("ManagedIdentityCredential", "the instance's managed identity")])
def test_azure_credentials_say_which_of_the_chain_found_them(
        home, monkeypatch, member, where):
    _azure_chain(monkeypatch, member)
    got = cloudapi.adapter("azure", {"product": "sql", "id": "db",
                                     "server": "s"}).credentials()
    assert got[1] == where


def test_azure_credentials_found_nowhere_say_every_place_and_no_role(
        home, monkeypatch):
    _azure_chain(monkeypatch)
    with pytest.raises(SystemExit) as e:
        cloudapi.adapter("azure", {"product": "sql", "id": "db"}) \
            .credentials()
    for place in ("AZURE_CLIENT_ID", "managed identity", "az CLI's login",
                  "PowerShell", "developer CLI"):
        assert place in str(e.value), e.value
    with pytest.raises(SystemExit, match="assumes no role"):
        cloudapi.adapter("azure", {"product": "sql", "id": "db",
                                   "role": "x"}).credentials()


def test_the_azure_subscription_is_the_az_clis_default_where_not_named(
        home, monkeypatch):
    (home / ".azure").mkdir()
    (home / ".azure" / "azureProfile.json").write_text(
        chr(0xFEFF) + json.dumps({"subscriptions": [
            {"id": "11111111-1111-1111-1111-111111111111",
             "isDefault": False},
            {"id": "22222222-2222-2222-2222-222222222222",
             "isDefault": True}]}), encoding="utf-8")
    a = cloudapi.adapter("azure", {"product": "sql", "id": "db"})
    assert a._subscription() == "22222222-2222-2222-2222-222222222222"
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-from-env")
    assert a._subscription() == "sub-from-env"


# --- Tencent Cloud -----------------------------------------------------------

class TencentApi:
    """A Tencent Cloud client's stand-in: an action the real client does not
    have fails, the request has to be that action's own model, and each
    answer is read through the action's response model - a recorded field
    the model does not have fails."""

    def __init__(self, product, answers):
        pkg, version, name = cloudapi.TENCENT[product]
        self.real = getattr(importlib.import_module(
            f"tencentcloud.{pkg}.{version}.{pkg}_client"), name)
        self.models = importlib.import_module(
            f"tencentcloud.{pkg}.{version}.models")
        self.answers, self.calls = answers, []

    def __getattr__(self, action):
        if action.startswith("_") or not hasattr(self.real, action):
            raise AttributeError(action)

        def call(req):
            assert type(req) is getattr(self.models, action + "Request")
            sent = {k: v for k, v in json.loads(req.to_json_string()).items()
                    if v is not None}
            self.calls.append((action, sent))
            given = _next(self.answers[action], sent)
            resp = getattr(self.models, action + "Response")()
            resp._deserialize(given)
            _within(given, json.loads(resp.to_json_string()), action)
            return resp
        return call

    def asked(self, action):
        return [sent for a, sent in self.calls if a == action]


def _tencent(product, api, ident="cdb-1", **conf):
    a = cloudapi.adapter("tencent", {"product": product, "id": ident,
                                     "region": "ap-guangzhou", **conf})
    a._found, a._client = ("credentials", "a test's"), api
    return a


CDB = {"InstanceId": "cdb-1", "EngineVersion": "8.0",
       "DeviceType": "UNIVERSAL", "Cpu": 4, "Memory": 16384, "Volume": 200,
       "Vip": "10.0.0.7", "Vport": 3306, "Status": 1, "UniqVpcId": "vpc-1",
       "UniqSubnetId": "subnet-1"}


def _cdb_params(value="800"):
    return {"TotalCount": 2, "Items": [
        {"Name": "max_connections", "CurrentValue": value, "NeedReboot": 0,
         "IsNotSupportEdit": False},
        {"Name": "innodb_buffer_pool_size", "CurrentValue": "8589934592",
         "NeedReboot": 1, "IsNotSupportEdit": False}]}


def test_a_tencent_mysql_instances_facts_and_parameters_are_read():
    api = TencentApi("cdb", {
        "DescribeDBInstances": {"TotalCount": 1, "Items": [CDB]},
        "DescribeDBInstanceCharset": {"Charset": "UTF8MB4"},
        "DescribeTimeWindow": {day: ["03:00-04:00"] for day in
                               cloudapi.DAYS},
        "DescribeInstanceParams": _cdb_params()})
    got = _tencent("cdb", api).instance(["max_connections",
                                         "innodb_buffer_pool_size",
                                         "no_such"])
    assert (got.engine, got.version, got.klass) == ("mysql", "8.0",
                                                    "UNIVERSAL")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 16.0, 200)
    assert (got.host, got.port, got.status) == ("10.0.0.7", 3306,
                                                "running")
    assert got.charset == "UTF8MB4"
    assert got.maintenance == "every day 03:00-04:00 Beijing time"
    assert got.parameters == {
        "max_connections": cloudapi.Param("800", True, False),
        "innodb_buffer_pool_size": cloudapi.Param("8589934592", True, True),
        "no_such": cloudapi.Param()}
    assert api.asked("DescribeDBInstances") == [{"InstanceIds": ["cdb-1"]}]


def test_a_field_the_request_model_has_not_is_refused_before_sending():
    api = TencentApi("cdb", {})
    with pytest.raises(ValueError, match="has no Bogus"):
        _tencent("cdb", api)._call("DescribeDBInstances", Bogus=1)
    assert api.calls == []


def test_a_tencent_mysql_backup_is_made_and_waited_for_in_beijing_time():
    api = TencentApi("cdb", {
        "DescribeDBInstances": {"TotalCount": 1, "Items": [CDB]},
        "CreateBackup": {"BackupId": 99},
        "DescribeBackups": [
            {"TotalCount": 1, "Items": [{"BackupId": 99, "Status": "RUNNING",
                                         "Date": AT_BEIJING}]},
            {"TotalCount": 1, "Items": [{"BackupId": 99, "Status": "SUCCESS",
                                         "Date": AT_BEIJING}]}]})
    got = _tencent("cdb", api).restore_point("rp", lambda m: None)
    assert got == RestorePoint("99", "backup", "SUCCESS", AT, True)
    assert api.asked("CreateBackup") == [{
        "InstanceId": "cdb-1", "BackupMethod": "physical",
        "ManualBackupName": "rp"}]
    assert len(api.asked("DescribeBackups")) == 2
    assert _tencent("cdb", api).restore_points() == [got]
    api.answers["DescribeBackups"] = {"TotalCount": 1, "Items": [
        {"BackupId": 99, "Status": "FAILED", "Date": AT_BEIJING}]}
    with pytest.raises(SystemExit, match="the backup rp is failed"):
        _tencent("cdb", api).restore_point("rp", lambda m: None)


def test_a_tencent_mysql_instance_is_cloned_to_a_time_and_from_a_backup():
    new = {"InstanceId": "cdb-2", "Status": 1, "Vip": "10.0.0.8",
           "Vport": 3306}

    def instances(sent):
        if sent.get("InstanceNames"):
            return {"TotalCount": 1, "Items": [{"InstanceId": "cdb-2"}]}
        return {"TotalCount": 1, "Items": [
            CDB if sent.get("InstanceIds") == ["cdb-1"] else new]}
    jobs = [{"TotalCount": 1, "Items": [{"DstInstanceId": "cdb-2",
                                         "TaskStatus": "running"}]},
            {"TotalCount": 1, "Items": [{"DstInstanceId": "cdb-2",
                                         "TaskStatus": "success"}]}]
    api = TencentApi("cdb", {
        "DescribeDBInstances": instances,
        "CreateCloneInstance": {"AsyncRequestId": "req-1"},
        "DescribeCloneList": list(jobs)})
    got = _tencent("cdb", api).clone("copy", AT, lambda m: None)
    assert got == {"host": "10.0.0.8", "port": 3306, "id": "cdb-2",
                   "kind": "instance"}
    asked = api.asked("CreateCloneInstance")[0]
    _within({"InstanceId": "cdb-1", "SpecifiedRollbackTime": AT_BEIJING,
             "InstanceName": "copy", "UniqVpcId": "vpc-1",
             "UniqSubnetId": "subnet-1", "Memory": 16384, "Volume": 200,
             "Cpu": 4, "DeviceType": "UNIVERSAL"}, asked)
    assert "SpecifiedBackupId" not in asked
    assert len(api.asked("DescribeCloneList")) == 2
    api.answers["DescribeCloneList"] = jobs[1]
    _tencent("cdb", api).clone("copy", RestorePoint("99", "backup",
                                                    ready=True),
                               lambda m: None)
    asked = api.asked("CreateCloneInstance")[1]
    assert asked["SpecifiedBackupId"] == 99
    assert "SpecifiedRollbackTime" not in asked


def test_a_tencent_mysql_parameter_is_set_and_waited_for():
    now = {"max_connections": "800"}

    def modify(sent):
        for p in sent["ParamList"]:
            now[p["Name"]] = p["CurrentValue"]
        return {"AsyncRequestId": "req-2"}
    api = TencentApi("cdb", {
        "DescribeInstanceParams": lambda sent: _cdb_params(
            now["max_connections"]),
        "ModifyInstanceParam": modify,
        "DescribeAsyncRequestInfo": [{"Status": "RUNNING"},
                                     {"Status": "SUCCESS"}]})
    before = _tencent("cdb", api).set_parameters({"max_connections": 1000},
                                                 lambda m: None)
    assert before == {"max_connections": "800"}
    assert api.asked("ModifyInstanceParam") == [{
        "InstanceIds": ["cdb-1"], "WaitSwitch": 0,
        "ParamList": [{"Name": "max_connections", "CurrentValue": "1000"}]}]
    assert api.asked("DescribeAsyncRequestInfo") == [
        {"AsyncRequestId": "req-2"}] * 2
    api.answers["DescribeAsyncRequestInfo"] = {"Status": "FAILED"}
    with pytest.raises(SystemExit, match="is failed"):
        _tencent("cdb", api).set_parameters({"max_connections": 900},
                                            lambda m: None)


def test_a_tencent_mysql_instance_is_found_by_its_address():
    api = TencentApi("cdb", {"DescribeDBInstances": lambda sent: {
        "TotalCount": 1, "Items": [CDB] if sent.get("Vips") == [
            "10.0.0.7"] else []}})
    assert _tencent("cdb", api).find("10.0.0.7", 3306) == "cdb-1"
    assert _tencent("cdb", api).find("10.0.0.7", 3307) is None
    assert _tencent("cdb", api).find("10.0.0.99") is None


def test_a_tencent_mongodb_spec_says_its_connection_limit():
    api = TencentApi("mongodb", {
        "DescribeDBInstances": {"TotalCount": 1, "InstanceDetails": [{
            "InstanceId": "cmgo-1", "MongoVersion": "MONGO_60_WT",
            "CpuNum": 4, "Memory": 8192, "Volume": 102400,
            "MachineType": "HIO10G", "Vip": "10.0.0.11", "Vport": 27017,
            "Status": 2, "Zone": "ap-guangzhou-3", "ClusterType": 0,
            "MaintenanceStart": "03:00", "MaintenanceEnd": "04:00"}]},
        "DescribeSpecInfo": {"SpecInfoList": [{
            "Zone": "ap-guangzhou-3", "SpecItems": [
                {"Cpu": 8, "Memory": 16384, "MongoVersionCode": "MONGO_60_WT",
                 "ClusterType": 0, "Conns": 20000},
                {"Cpu": 4, "Memory": 8192, "MongoVersionCode": "MONGO_60_WT",
                 "ClusterType": 0, "Conns": 10000}]}]}})
    got = _tencent("mongodb", api, "cmgo-1").instance()
    assert (got.engine, got.version, got.max_connections) == ("mongodb",
                                                             "6.0", 10000)
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 8.0, 100.0)
    assert got.maintenance == "every day 03:00-04:00 Beijing time"
    assert api.asked("DescribeSpecInfo") == [{"Zone": "ap-guangzhou-3"}]


def _tccli(home, name, credential, region=None):
    (home / ".tccli").mkdir(exist_ok=True)
    (home / ".tccli" / f"{name}.credential").write_text(json.dumps(credential))
    if region:
        (home / ".tccli" / f"{name}.configure").write_text(json.dumps(
            {"_sys_param": {"region": region, "output": "json"}}))


def test_tencent_credentials_are_found_where_its_cli_finds_them(
        home, monkeypatch):
    from tencentcloud.common import credential
    answers = []

    def metadata(url, *args, **kw):
        if not answers:
            raise OSError("no metadata service here")
        return io.BytesIO(answers.pop(0))
    monkeypatch.setattr(credential, "urlopen", metadata)

    def found(**conf):
        a = cloudapi.adapter("tencent", {"product": "cdb", "id": "cdb-1",
                                         **conf})
        return (*a.credentials(), a.region)
    # the environment first, its region with it
    monkeypatch.setenv("TENCENTCLOUD_SECRET_ID", "AKIDENV")
    monkeypatch.setenv("TENCENTCLOUD_SECRET_KEY", "CHANGE_ME")
    monkeypatch.setenv("TENCENTCLOUD_REGION", "ap-shanghai")
    cred, where, region = found()
    assert (cred.secret_id, where, region) == (
        "AKIDENV", "the environment (TENCENTCLOUD_SECRET_ID)", "ap-shanghai")
    for key in ("TENCENTCLOUD_SECRET_ID", "TENCENTCLOUD_SECRET_KEY",
                "TENCENTCLOUD_REGION"):
        monkeypatch.delenv(key)
    # then the CLI's profile, its region from the profile's configuration
    _tccli(home, "default", {"secretId": "AKIDCLI", "secretKey": "CHANGE_ME"},
           "ap-singapore")
    _tccli(home, "ops", {"secretId": "AKIDOPS", "secretKey": "CHANGE_ME"},
           "ap-bangkok")
    cred, where, region = found()
    assert (cred.secret_id, where, region) == (
        "AKIDCLI", "the CLI's profile default", "ap-singapore")
    monkeypatch.setenv("TCCLI_PROFILE", "ops")
    cred, where, region = found()
    assert (cred.secret_id, where, region) == (
        "AKIDOPS", "the CLI's profile ops", "ap-bangkok")
    monkeypatch.delenv("TCCLI_PROFILE")
    with pytest.raises(SystemExit, match="profile nope holds no"):
        found(profile="nope")
    for name in ("default", "ops"):
        (home / ".tccli" / f"{name}.credential").unlink()
    # then the SDK's own file
    (home / ".tencentcloud").mkdir()
    (home / ".tencentcloud" / "credentials").write_text(
        "[default]\nsecret_id = AKIDSDK\nsecret_key = CHANGE_ME\n")
    cred, where, region = found(region="ap-guangzhou")
    assert (cred.secret_id, where) == ("AKIDSDK",
                                       "the SDK's own credentials file")
    (home / ".tencentcloud" / "credentials").unlink()
    # then the instance's role, from its metadata
    answers[:] = [b"app-role", json.dumps({
        "Code": "Success", "TmpSecretId": "AKIDROLE",
        "TmpSecretKey": "CHANGE_ME", "Token": "CHANGE_ME",
        "ExpiredTime": 4102444800}).encode()]
    cred, where, region = found(region="ap-guangzhou")
    assert where == "the instance's role" and cred.secret_id == "AKIDROLE"
    # and nothing found says every place it looked
    with pytest.raises(SystemExit) as e:
        found(region="ap-guangzhou")
    for place in ("TENCENTCLOUD_SECRET_ID", "CLI's profile default",
                  "SDK's own credentials file", "instance's role"):
        assert place in str(e.value), e.value


def test_a_tencent_role_is_assumed_signed_with_what_was_found(
        home, monkeypatch):
    from tencentcloud.common.common_client import CommonClient
    asked = []

    def call_json(self, action, params, *a, **kw):
        asked.append((action, params, self.credential.secret_id))
        return {"Response": {"Credentials": {
            "Token": "CHANGE_ME", "TmpSecretId": "AKIDTMP",
            "TmpSecretKey": "CHANGE_ME"}, "ExpiredTime": 4102444800}}
    monkeypatch.setattr(CommonClient, "call_json", call_json)
    monkeypatch.setenv("TENCENTCLOUD_SECRET_ID", "AKIDENV")
    monkeypatch.setenv("TENCENTCLOUD_SECRET_KEY", "CHANGE_ME")
    role = "qcs::cam::uin/100000000001:roleName/migkit"  # an example
    cred, where = cloudapi.adapter("tencent", {
        "product": "cdb", "id": "cdb-1", "region": "ap-guangzhou",
        "role": role}).credentials()
    assert where == (f"the role {role}, assumed over the environment"
                     " (TENCENTCLOUD_SECRET_ID)")
    assert cred.secret_id == "AKIDTMP"
    assert asked == [("AssumeRole", {"RoleArn": role,
                                     "RoleSessionName": "migkit",
                                     "DurationSeconds": 7200}, "AKIDENV")]


# --- Alibaba Cloud -----------------------------------------------------------

class AlibabaApi:
    """An Alibaba Cloud client's stand-in: a method the real client does not
    have fails, the request has to be the action's own model, and each
    answer is read through the action's response body model - a recorded
    field the model does not have fails."""

    def __init__(self, product, answers):
        module = cloudapi.ALIBABA[product]
        self.real = importlib.import_module(module + ".client").Client
        self.models = importlib.import_module(module + ".models")
        self.answers, self.calls = answers, []

    def __getattr__(self, method):
        if method.startswith("_") or not hasattr(self.real, method):
            raise AttributeError(method)

        def call(req):
            action = type(req).__name__[:-len("Request")]
            assert cloudapi._snake(action) == method, (action, method)
            assert type(req) is getattr(self.models, action + "Request")
            sent = req.to_map()
            self.calls.append((action, sent))
            given = _next(self.answers[action], sent)
            body = getattr(self.models, action + "ResponseBody")().from_map(
                given)
            _within(given, body.to_map(), action)
            return getattr(self.models, action + "Response")(
                headers={}, status_code=200, body=body)
        return call

    def asked(self, action):
        return [sent for a, sent in self.calls if a == action]


def _alibaba(product, api, ident="rm-1", **conf):
    a = cloudapi.adapter("alibaba", {"product": product, "id": ident,
                                     "region": "cn-hangzhou", **conf})
    a._found, a._client = ("credentials", "a test's"), api
    return a


RDS = {"DBInstanceId": "rm-1", "Engine": "MySQL", "EngineVersion": "8.0",
       "DBInstanceClass": "mysql.n4.large.2c", "DBInstanceCPU": "4",
       "DBInstanceMemory": 16384, "DBInstanceStorage": 200,
       "MaxConnections": 4000, "MaintainTime": "18:00Z-22:00Z",
       "DBInstanceStatus": "Running", "Collation": "utf8mb4_general_ci",
       "TimeZone": "UTC", "ConnectionString": "rm-1.mysql.rds.example.com",
       "Port": "3306", "Category": "HighAvailability",
       "ZoneId": "cn-hangzhou-h", "VpcId": "vpc-1", "VSwitchId": "vsw-1",
       "DBInstanceStorageType": "cloud_essd"}


def _rds_net(host="rm-1.mysql.rds.example.com"):
    return {"DBInstanceNetInfos": {"DBInstanceNetInfo": [
        {"IPType": "Private", "ConnectionString": host,
         "IPAddress": "10.0.0.8", "Port": "3306"}]}}


def _rds_params(value="4000"):
    return {"RunningParameters": {"DBInstanceParameter": [
        {"ParameterName": "max_connections", "ParameterValue": value},
        {"ParameterName": "innodb_buffer_pool_size",
         "ParameterValue": "8589934592"}]}}


RDS_RULES = {"Parameters": {"TemplateRecord": [
    {"ParameterName": "max_connections", "ForceModify": "true",
     "ForceRestart": "false"},
    {"ParameterName": "innodb_buffer_pool_size", "ForceModify": "true",
     "ForceRestart": "true"}]}}


def test_an_alibaba_rds_instances_facts_and_parameters_are_read():
    api = AlibabaApi("rds", {
        "DescribeDBInstanceAttribute": {"Items": {"DBInstanceAttribute": [
            RDS]}},
        "DescribeDBInstanceNetInfo": _rds_net(),
        "DescribeParameters": _rds_params(),
        "DescribeParameterTemplates": RDS_RULES})
    got = _alibaba("rds", api).instance(["max_connections",
                                         "innodb_buffer_pool_size"])
    assert (got.engine, got.version, got.klass) == ("mysql", "8.0",
                                                    "mysql.n4.large.2c")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 16.0, 200)
    assert got.max_connections == 4000
    assert (got.charset, got.time_zone) == ("utf8mb4_general_ci", "UTC")
    assert got.maintenance == "every day 18:00-22:00 UTC"
    assert (got.host, got.port, got.status) == (
        "rm-1.mysql.rds.example.com", 3306, "Running")
    assert got.parameters == {
        "max_connections": cloudapi.Param("4000", True, False),
        "innodb_buffer_pool_size": cloudapi.Param("8589934592", True, True)}
    assert api.asked("DescribeParameterTemplates") == [{
        "Engine": "MySQL", "EngineVersion": "8.0",
        "Category": "HighAvailability"}]
    with pytest.raises(ValueError, match="has no Bogus"):
        _alibaba("rds", api)._call("DescribeDBInstanceAttribute", Bogus=1)


def test_an_alibaba_rds_backup_is_made_and_its_task_waited_for():
    api = AlibabaApi("rds", {
        "CreateBackup": {"BackupJobId": "123"},
        "DescribeBackupTasks": [
            {"Items": {"BackupJob": [{"BackupJobId": "123",
                                      "BackupStatus": "Uploading"}]}},
            {"Items": {"BackupJob": [{"BackupJobId": "123",
                                      "BackupStatus": "Finished",
                                      "BackupId": "555"}]}}],
        "DescribeBackups": {"TotalRecordCount": 1, "Items": {"Backup": [
            {"BackupId": "555", "BackupStatus": "Success",
             "BackupEndTime": AT}]}}})
    got = _alibaba("rds", api).restore_point("rp", lambda m: None)
    assert got == RestorePoint("555", "backup", "Success", AT, True)
    assert api.asked("CreateBackup") == [{"DBInstanceId": "rm-1",
                                          "BackupMethod": "Physical"}]
    assert api.asked("DescribeBackupTasks") == [
        {"DBInstanceId": "rm-1", "BackupJobId": 123}] * 2
    assert _alibaba("rds", api).restore_points() == [got]
    assert api.asked("DescribeBackups")[-1] == {
        "DBInstanceId": "rm-1", "PageNumber": 1, "PageSize": 100}
    api.answers["DescribeBackupTasks"] = {"Items": {"BackupJob": [
        {"BackupJobId": "123", "BackupStatus": "Failed"}]}}
    with pytest.raises(SystemExit, match="the backup rp is failed"):
        _alibaba("rds", api).restore_point("rp", lambda m: None)


def test_an_alibaba_rds_instance_is_cloned_and_waited_for():
    states = ["Creating", "Running"]

    def attribute(sent):
        if sent["DBInstanceId"] == "rm-1":
            return {"Items": {"DBInstanceAttribute": [RDS]}}
        state = states.pop(0) if len(states) > 1 else states[0]
        return {"Items": {"DBInstanceAttribute": [
            {"DBInstanceId": "rm-2", "DBInstanceStatus": state}]}}
    api = AlibabaApi("rds", {
        "DescribeDBInstanceAttribute": attribute,
        "CloneDBInstance": {"DBInstanceId": "rm-2"},
        "DescribeDBInstanceNetInfo": lambda sent: _rds_net(
            f"{sent['DBInstanceId']}.mysql.rds.example.com")})
    got = _alibaba("rds", api).clone("copy", AT, lambda m: None)
    assert got == {"host": "rm-2.mysql.rds.example.com", "port": 3306,
                   "id": "rm-2", "kind": "instance"}
    assert api.asked("CloneDBInstance") == [{
        "DBInstanceId": "rm-1", "PayType": "Postpaid", "RestoreTime": AT,
        "DBInstanceClass": "mysql.n4.large.2c", "DBInstanceStorage": 200,
        "DBInstanceStorageType": "cloud_essd", "ZoneId": "cn-hangzhou-h",
        "VPCId": "vpc-1", "VSwitchId": "vsw-1",
        "InstanceNetworkType": "VPC"}]
    assert len([s for s in api.asked("DescribeDBInstanceAttribute")
                if s["DBInstanceId"] == "rm-2"]) == 2


def test_an_alibaba_rds_parameter_is_set_and_waited_for():
    now = {"max_connections": "4000"}

    def modify(sent):
        now.update(json.loads(sent["Parameters"]))
        return {"RequestId": "r"}
    api = AlibabaApi("rds", {
        "DescribeDBInstanceAttribute": {"Items": {"DBInstanceAttribute": [
            RDS]}},
        "DescribeParameters": lambda sent: _rds_params(
            now["max_connections"]),
        "DescribeParameterTemplates": RDS_RULES,
        "ModifyParameter": modify})
    before = _alibaba("rds", api).set_parameters({"max_connections": 5000},
                                                 lambda m: None)
    assert before == {"max_connections": "4000"}
    assert api.asked("ModifyParameter") == [{
        "DBInstanceId": "rm-1", "Parameters": '{"max_connections": "5000"}',
        "Forcerestart": False}]
    assert now["max_connections"] == "5000"


def test_an_alibaba_rds_instance_is_found_by_its_address():
    api = AlibabaApi("rds", {
        "DescribeDBInstances": lambda sent: {"Items": {"DBInstance": [
            {"DBInstanceId": "rm-1"}] if sent.get("ConnectionString") ==
            "rm-1.mysql.rds.example.com" else []}},
        "DescribeDBInstanceNetInfo": _rds_net()})
    assert _alibaba("rds", api).find("rm-1.mysql.rds.example.com") == "rm-1"
    assert _alibaba("rds", api).find("10.0.0.99") is None


PXC = {"DBInstance": {
    "Id": "pxc-1", "Engine": "polarx", "EngineVersion": "5.7",
    "CnNodeClassCode": "polarx.x4.medium.2e", "CnNodeCount": 2,
    "DnNodeClassCode": "mysql.n4.medium.25", "DnNodeCount": 2,
    "TopologyType": "3azones", "Series": "enterprise", "Status": "Running",
    "PrimaryZone": "cn-hangzhou-g", "SecondaryZone": "cn-hangzhou-h",
    "TertiaryZone": "cn-hangzhou-i", "ZoneId": "cn-hangzhou-g",
    "VPCId": "vpc-1", "VSwitchId": "vsw-1", "MaintainStartTime": "18:00Z",
    "MaintainEndTime": "19:00Z", "StorageUsed": 1 << 30,
    "ConnAddrs": [{"Type": "VPC", "Port": "3306",
                   "ConnectionString": "pxc-1.polarx.rds.example.com"}]}}


def test_a_polardbx_backup_set_is_made_and_waited_for_by_its_status_code():
    api = AlibabaApi("polardbx", {
        "CreateBackup": {"Success": True, "Data": {"BackupSetId": "77"}},
        "DescribeBackupSetList": [
            {"Success": True, "Data": [{"BackupSetId": 77, "Status": 0,
                                        "EndTime": 0}]},
            {"Success": True, "Data": [{"BackupSetId": 77, "Status": 1,
                                        "EndTime": 1790848800000}]}],
        "DescribeDBInstanceAttribute": PXC})
    a = _alibaba("polardbx", api, ident="pxc-1")
    got = a.restore_point("rp", lambda m: None)
    assert got == RestorePoint("77", "backup", "success",
                               "2026-10-01T10:00:00Z", True)
    assert api.asked("CreateBackup") == [{"DBInstanceName": "pxc-1",
                                          "BackupType": "0",
                                          "RegionId": "cn-hangzhou"}]
    assert len(api.asked("DescribeBackupSetList")) == 2
    api.answers["DescribeBackupSetList"] = {"Success": True, "Data": [
        {"BackupSetId": 78, "Status": 2}]}
    api.answers["CreateBackup"] = {"Success": True,
                                   "Data": {"BackupSetId": "78"}}
    with pytest.raises(SystemExit, match="the backup rp is failed"):
        a.restore_point("rp", lambda m: None)
    got = a.instance()
    assert (got.engine, got.host, got.port, got.status) == (
        "polardbx", "pxc-1.polarx.rds.example.com", 3306, "Running")


def test_a_polardbx_copy_is_a_clone_to_a_time_or_from_a_backup_set():
    states = ["Creating", "Running"]
    api = AlibabaApi("polardbx", {
        "DescribeDBInstanceAttribute": lambda sent: PXC if sent[
            "DBInstanceName"] == "pxc-1" else {"DBInstance": {
                **PXC["DBInstance"], "Status": states.pop(0) if len(states)
                > 1 else states[0]}},
        "RestoreDBInstance": {"DBInstanceName": "pxc-copy"}})
    a = _alibaba("polardbx", api, ident="pxc-1")
    got = a.clone("pxc-copy", AT, lambda m: None)
    sent = api.asked("RestoreDBInstance")[0]
    assert sent["RecoveryTypeCode"] == "Clone"
    assert sent["CloneInstanceName"] == "pxc-1"
    assert sent["RestoreTime"] == AT and "BackupSetId" not in sent
    assert (sent["TopologyType"], sent["CNNodeCount"], sent["DnClass"]) == (
        "3azones", "2", "mysql.n4.medium.25")
    assert got["id"] == "pxc-copy" and got["kind"] == "instance"
    a.clone("pxc-copy", RestorePoint("77", "backup", ready=True),
            lambda m: None)
    sent = api.asked("RestoreDBInstance")[1]
    assert sent["BackupSetId"] == "77" and "RestoreTime" not in sent


def test_a_polardbx_parameter_is_set_at_the_level_it_lives_at():
    compute = {"Data": {"RunningParameters": [
        {"ParameterName": "XA_RECOVER_INTERVAL", "ParameterValue": "5"}]}}
    storage = {"Data": {"RunningParameters": [
        {"ParameterName": "innodb_lock_wait_timeout",
         "ParameterValue": "50"}]}}
    changed = {"XA_RECOVER_INTERVAL": "5", "innodb_lock_wait_timeout": "50"}

    def describe(sent):
        rows = compute if sent["ParamLevel"] == "compute" else storage
        level = rows["Data"]["RunningParameters"]
        return {"Data": {"RunningParameters": [
            {**r, "ParameterValue": changed[r["ParameterName"]]}
            for r in level]}}

    def modify(sent):
        changed.update(json.loads(sent["Parameters"]))
        return {"RequestId": "r"}
    api = AlibabaApi("polardbx", {"DescribeParameters": describe,
                                  "ModifyParameter": modify})
    a = _alibaba("polardbx", api, ident="pxc-1")
    before = a.set_parameters({"XA_RECOVER_INTERVAL": 9,
                               "innodb_lock_wait_timeout": 120},
                              lambda m: None)
    assert before == {"XA_RECOVER_INTERVAL": "5",
                      "innodb_lock_wait_timeout": "50"}
    asked = {s["ParamLevel"]: json.loads(s["Parameters"])
             for s in api.asked("ModifyParameter")}
    assert asked == {"compute": {"XA_RECOVER_INTERVAL": "9"},
                     "storage": {"innodb_lock_wait_timeout": "120"}}


def _tea(monkeypatch, answer):
    """The SDK's own HTTP call stood in for: `answer(request)` gives the
    status and the body."""
    from Tea.core import TeaCore
    asked = []

    def do_action(request, runtime=None):
        asked.append(request)
        status, body = answer(request)
        return types.SimpleNamespace(status_code=status, headers={},
                                     body=json.dumps(body).encode()
                                     if isinstance(body, dict) else body)
    monkeypatch.setattr(TeaCore, "do_action", staticmethod(do_action))
    return asked


def test_alibaba_credentials_are_found_where_its_cli_finds_them(
        home, monkeypatch):
    def nothing(request):
        raise OSError("no metadata service here")
    _tea(monkeypatch, nothing)

    def found(**conf):
        a = cloudapi.adapter("alibaba", {"product": "rds", "id": "rm-1",
                                         **conf})
        cred, where = a.credentials()
        return cred.get_credential().access_key_id, where, a.region
    # the environment first
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "LTAIENV")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "CHANGE_ME")
    monkeypatch.setenv("ALIBABA_CLOUD_REGION_ID", "cn-shanghai")
    assert found() == ("LTAIENV",
                       "the environment (ALIBABA_CLOUD_ACCESS_KEY_ID)",
                       "cn-shanghai")
    for key in ("ALIBABA_CLOUD_ACCESS_KEY_ID",
                "ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ALIBABA_CLOUD_REGION_ID"):
        monkeypatch.delenv(key)
    # then the CLI's profile, its region with it
    (home / ".aliyun").mkdir()
    (home / ".aliyun" / "config.json").write_text(json.dumps({
        "current": "default", "profiles": [
            {"name": "default", "mode": "AK", "access_key_id": "LTAICLI",
             "access_key_secret": "CHANGE_ME", "region_id": "cn-beijing"},
            {"name": "ops", "mode": "AK", "access_key_id": "LTAIOPS",
             "access_key_secret": "CHANGE_ME", "region_id": "cn-shenzhen"}]}))
    assert found() == ("LTAICLI", "the CLI's profile default", "cn-beijing")
    assert found(profile="ops") == ("LTAIOPS", "the CLI's profile ops",
                                    "cn-shenzhen")
    (home / ".aliyun" / "config.json").unlink()
    # then the SDK's own file
    (home / ".alibabacloud").mkdir()
    (home / ".alibabacloud" / "credentials.ini").write_text(
        "[default]\ntype = access_key\naccess_key_id = LTAISDK\n"
        "access_key_secret = CHANGE_ME\n")
    assert found(region="cn-hangzhou") == (
        "LTAISDK", "the SDK's own credentials file", "cn-hangzhou")
    (home / ".alibabacloud" / "credentials.ini").unlink()

    # then the instance's RAM role, from its metadata
    def metadata(request):
        if request.method == "PUT":
            return 200, b"CHANGE_ME"
        if request.pathname.endswith("/security-credentials/"):
            return 200, b"app-role"
        return 200, {"Code": "Success", "AccessKeyId": "STS.ROLE",
                     "AccessKeySecret": "CHANGE_ME",
                     "SecurityToken": "CHANGE_ME",
                     "Expiration": "2099-01-01T00:00:00Z"}
    _tea(monkeypatch, metadata)
    assert found(region="cn-hangzhou") == (
        "STS.ROLE", "the instance's RAM role", "cn-hangzhou")
    # and nothing found says every place it looked
    _tea(monkeypatch, nothing)
    with pytest.raises(SystemExit) as e:
        found(region="cn-hangzhou")
    for place in ("ALIBABA_CLOUD_ACCESS_KEY_ID", "OIDC token", "CLI's profile",
                  "SDK's own credentials file", "RAM role",
                  "credentials address"):
        assert place in str(e.value), e.value


def test_an_alibaba_ram_role_is_assumed_signed_with_what_was_found(
        home, monkeypatch):
    asked = _tea(monkeypatch, lambda request: (200, {"Credentials": {
        "AccessKeyId": "STS.TMP", "AccessKeySecret": "CHANGE_ME",
        "SecurityToken": "CHANGE_ME", "Expiration": "2099-01-01T00:00:00Z"}}))
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "LTAIENV")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "CHANGE_ME")
    role = "acs:ram::1000000000000001:role/migkit"
    cred, where = cloudapi.adapter("alibaba", {
        "product": "rds", "id": "rm-1", "region": "cn-hangzhou",
        "role": role}).credentials()
    assert where == (f"the RAM role {role}, assumed over the environment"
                     " (ALIBABA_CLOUD_ACCESS_KEY_ID)")
    assert cred.get_credential().access_key_id == "STS.TMP"
    query = asked[-1].query
    assert (query["Action"], query["RoleArn"], query["RoleSessionName"],
            query["AccessKeyId"]) == ("AssumeRole", role, "migkit",
                                      "LTAIENV")


# --- Huawei Cloud ------------------------------------------------------------

#: the fields of each request the adapter sends, as the Huawei Cloud SDK
#: 3.1.216's request models have them (`openapi_types`), read off the SDK
#: in an environment of its own: it cannot be imported beside migkit
HUAWEI_FIELDS = {
    ("rds", "list_instances"): {
        "x_language", "id", "name", "type", "datastore_type", "eps_id",
        "vpc_id", "subnet_id", "offset", "limit", "tags", "group_type"},
    ("rds", "show_instance_configuration"): {"x_language", "instance_id"},
    ("rds", "update_instance_configuration"): {"x_language", "instance_id",
                                               "body"},
    ("rds", "create_manual_backup"): {"x_language", "body"},
    ("rds", "list_backups"): {
        "x_language", "instance_id", "backup_id", "backup_type", "status",
        "offset", "limit", "begin_time", "end_time"},
    ("rds", "create_restore_instance"): {"x_language", "body"},
    ("rds", "list_job_info"): {"x_language", "id"},
    ("dds", "list_instances"): {"id", "name", "mode", "datastore_type",
                                "vpc_id", "subnet_id", "offset", "limit",
                                "tags"},
    ("dds", "create_manual_backup"): {"body"},
    ("dds", "list_backups"): {
        "instance_id", "backup_id", "backup_type", "offset", "limit",
        "begin_time", "end_time", "mode", "order_field", "order_rule",
        "backup_status", "backup_name", "backup_description",
        "instance_name"},
    ("dds", "restore_new_instance"): {"body"},
    ("dds", "show_job_detail"): {"id"},
    ("iam", "create_temporary_access_key_by_agency"): {"body"},
    ("gaussdb", "list_instances"): {
        "x_language", "id", "name", "type", "datastore_type", "vpc_id",
        "subnet_id", "offset", "limit", "tags", "charge_mode"},
    ("gaussdb", "show_instance_configuration"): {"x_language",
                                                 "instance_id"},
    ("gaussdb", "update_instance_configuration"): {"x_language",
                                                   "instance_id", "body"},
    ("gaussdb", "create_manual_backup"): {"x_language", "body"},
    ("gaussdb", "list_backups"): {
        "x_language", "instance_id", "backup_id", "backup_type", "offset",
        "limit", "begin_time", "end_time"},
    ("gaussdb", "create_restore_instance"): {"x_language", "body"},
    ("gaussdb", "show_job_detail"): {"id"},
    ("geminidb", "list_instances"): {"id", "name", "datastore_type", "mode",
                                     "vpc_id", "subnet_id", "offset",
                                     "limit"},
    ("geminidb", "show_instance_configuration"): {"instance_id"},
    ("geminidb", "update_instance_configuration"): {"instance_id", "body"},
    ("geminidb", "create_back"): {"instance_id", "body"},
    ("geminidb", "list_backups"): {
        "instance_id", "datastore_type", "backup_id", "backup_type", "type",
        "limit", "offset", "begin_time", "end_time"},
    ("geminidb", "create_instance"): {"body"},
    ("geminidb", "list_jobs"): {"id", "start_time", "end_time", "status",
                                "name", "offset", "limit"},
}
HUAWEI_BODIES = {
    ("rds", "update_instance_configuration"): {"values"},
    ("rds", "create_manual_backup"): {
        "instance_id", "name", "description", "databases",
        "backup_database_individually"},
    ("rds", "create_restore_instance"): {
        "name", "datastore", "ha", "configuration_id", "port", "password",
        "backup_strategy", "enterprise_project_id", "disk_encryption_id",
        "flavor_ref", "volume", "region", "availability_zone", "vpc_id",
        "subnet_id", "data_vip", "security_group_id", "charge_info",
        "time_zone", "dsspool_id", "restore_point", "collation", "tags",
        "serverless_info", "dry_run", "is_auto_upgrade"},
    ("dds", "create_manual_backup"): {"backup"},
    ("dds", "restore_new_instance"): {
        "name", "availability_zone", "vpc_id", "subnet_id",
        "security_group_id", "password", "disk_encryption_id",
        "configurations", "flavor", "backup_strategy",
        "enterprise_project_id", "ssl_option", "dss_pool_id",
        "server_group_policies", "restore_point", "charge_info"},
    ("iam", "create_temporary_access_key_by_agency"): {"auth"},
    ("gaussdb", "update_instance_configuration"): {"values"},
    ("gaussdb", "create_manual_backup"): {"instance_id", "name",
                                          "description"},
    ("gaussdb", "create_restore_instance"): {
        "name", "availability_zone", "flavor_ref", "volume",
        "disk_encryption_id", "vpc_id", "subnet_id", "security_group_id",
        "password", "charge_info", "restore_point", "backup_strategy",
        "enable_parallel_restore", "configuration_id",
        "enterprise_project_id", "port", "time_zone", "enable_force_switch"},
    ("geminidb", "update_instance_configuration"): {"values"},
    ("geminidb", "create_back"): {"name", "description", "database_tables"},
    ("geminidb", "create_instance"): {
        "name", "datastore", "region", "availability_zone", "vpc_id",
        "subnet_id", "security_group_id", "password", "mode",
        "product_type", "flavor", "disk_encryption_id", "configuration_id",
        "backup_strategy", "enterprise_project_id", "dedicated_resource_id",
        "ssl_option", "charge_info", "restore_info", "dr_info", "port",
        "availability_zone_detail", "lb_access_control_settings"},
}
#: the nested models the program builds a GaussDB or GeminiDB request
#: from, as 3.1.216 has them
HUAWEI_NESTED = {
    ("gaussdb", "restore_point"): {"instance_id", "backup_id"},
    ("gaussdb", "volume"): {"type", "size"},
    ("geminidb", "datastore"): {"type", "version", "storage_engine"},
    ("geminidb", "flavor"): {"num", "storage", "size", "spec_code"},
    ("geminidb", "restore_info"): {"backup_id", "source_instance_id",
                                   "restore_time"},
}


class HuaweiRun:
    """The SDK's program stood in for: each request held to the recorded
    fields, recorded, and answered."""

    def __init__(self, answers):
        self.answers, self.asks = answers, []

    def __call__(self, adapter, ask):
        self.asks.append(ask)
        if ask["service"] == "credentials":
            return _next(self.answers["credentials"], ask)
        key = (ask["service"], ask["action"])
        request = ask["request"]
        assert set(request) <= HUAWEI_FIELDS[key], (key, request)
        if "body" in request:
            assert set(request["body"]) <= HUAWEI_BODIES[key], (key, request)
        return _next(self.answers[ask["action"]], request)

    def asked(self, action):
        return [a["request"] for a in self.asks if a.get("action") == action]


def _huawei(product, run, monkeypatch, ident="i-1", **conf):
    monkeypatch.setattr(cloudapi.Huawei, "_run",
                        lambda self, ask: run(self, ask))
    a = cloudapi.adapter("huawei", {"product": product, "id": ident,
                                    "region": "ap-southeast-1", **conf})
    a._found = ({"ak": "AKTEST", "sk": "CHANGE_ME"}, "a test's")
    return a


HW_RDS = {"id": "i-1", "status": "ACTIVE", "private_ips": ["10.0.0.9"],
          "port": 5432, "datastore": {"type": "PostgreSQL", "version": "16"},
          "flavor_ref": "rds.pg.n1.large.2", "cpu": "4", "mem": "16",
          "volume": {"type": "CLOUDSSD", "size": 100},
          "maintenance_window": "02:00-06:00", "time_zone": "UTC",
          "nodes": [{"availability_zone": "ap-southeast-1a"}],
          "vpc_id": "vpc-1", "subnet_id": "subnet-1",
          "security_group_id": "sg-1", "type": "Single"}


def _hw_config(work_mem="4MB"):
    return {"configuration_parameters": [
        {"name": "max_connections", "value": "500", "restart_required": True,
         "readonly": False},
        {"name": "work_mem", "value": work_mem, "restart_required": False,
         "readonly": False}]}


def test_a_huawei_rds_instances_facts_and_parameters_are_read(monkeypatch):
    run = HuaweiRun({"list_instances": {"instances": [HW_RDS],
                                        "total_count": 1},
                     "show_instance_configuration": _hw_config()})
    got = _huawei("rds", run, monkeypatch).instance(["max_connections",
                                                     "work_mem"])
    assert (got.engine, got.version, got.klass) == ("postgres", "16",
                                                    "rds.pg.n1.large.2")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 16, 100)
    assert (got.host, got.port, got.status) == ("10.0.0.9", 5432, "ACTIVE")
    assert got.maintenance == "every day 02:00-06:00 UTC"
    assert got.parameters == {
        "max_connections": cloudapi.Param("500", True, True),
        "work_mem": cloudapi.Param("4MB", True, False)}
    first = run.asks[0]
    assert (first["service"], first["action"], first["request"],
            first["region"], first["ak"]) == (
        "rds", "list_instances", {"id": "i-1"}, "ap-southeast-1", "AKTEST")


def test_a_huawei_rds_backup_is_made_and_waited_for(monkeypatch):
    run = HuaweiRun({
        "create_manual_backup": {"backup": {"id": "b1",
                                            "status": "BUILDING"}},
        "list_backups": [
            {"backups": [{"id": "b1", "status": "BUILDING"}],
             "total_count": 1},
            {"backups": [{"id": "b1", "status": "COMPLETED",
                          "end_time": "2026-10-01T18:00:00+0800"}],
             "total_count": 1}]})
    a = _huawei("rds", run, monkeypatch)
    got = a.restore_point("rp", lambda m: None)
    assert got == RestorePoint("b1", "backup", "COMPLETED", AT, True)
    assert run.asked("create_manual_backup") == [{"body": {
        "instance_id": "i-1", "name": "rp",
        "description": "a restore point"}}]
    assert [r["backup_id"] for r in run.asked("list_backups")] == ["b1"] * 2
    assert a.restore_points() == [got]
    run.answers["list_backups"] = {"backups": [{"id": "b1",
                                                "status": "FAILED"}]}
    with pytest.raises(SystemExit, match="the backup rp is failed"):
        a.restore_point("rp", lambda m: None)


def test_a_huawei_rds_instance_is_restored_as_a_new_one(monkeypatch):
    new = {**HW_RDS, "id": "i-2", "private_ips": ["10.0.0.10"]}
    run = HuaweiRun({
        "list_instances": lambda req: {"instances": [
            HW_RDS if req["id"] == "i-1" else new], "total_count": 1},
        "create_restore_instance": {"instance": {"id": "i-2"},
                                    "job_id": "j1"},
        "list_job_info": [{"job": {"id": "j1", "status": "Running"}},
                          {"job": {"id": "j1", "status": "Completed"}}]})
    a = _huawei("rds", run, monkeypatch)
    got = a.clone("copy", RestorePoint("b1", "backup", ready=True),
                  lambda m: None)
    assert got == {"host": "10.0.0.10", "port": 5432, "id": "i-2",
                   "kind": "instance"}
    body = run.asked("create_restore_instance")[0]["body"]
    assert body["restore_point"] == {"instance_id": "i-1", "type": "backup",
                                     "backup_id": "b1"}
    assert (body["name"], body["flavor_ref"], body["availability_zone"]) == \
        ("copy", "rds.pg.n1.large.2", "ap-southeast-1a")
    assert body["volume"] == {"type": "CLOUDSSD", "size": 100}
    assert "ha" not in body
    assert len(run.asked("list_job_info")) == 2
    a.clone("copy", AT, lambda m: None)
    body = run.asked("create_restore_instance")[1]["body"]
    assert body["restore_point"] == {
        "instance_id": "i-1", "type": "timestamp",
        "restore_time": int(cloudapi._time(AT).timestamp() * 1000)}


def test_a_huawei_rds_parameter_is_set_and_read_back(monkeypatch):
    now = {"work_mem": "4MB"}

    def update(req):
        now.update(req["body"]["values"])
        return {"restart_required": False}
    run = HuaweiRun({
        "show_instance_configuration": lambda req: _hw_config(
            now["work_mem"]),
        "update_instance_configuration": update})
    before = _huawei("rds", run, monkeypatch).set_parameters(
        {"work_mem": "8MB"}, lambda m: None)
    assert before == {"work_mem": "4MB"} and now["work_mem"] == "8MB"
    assert run.asked("update_instance_configuration") == [{
        "instance_id": "i-1", "body": {"values": {"work_mem": "8MB"}}}]


def test_a_huawei_rds_instance_is_found_by_its_address(monkeypatch):
    run = HuaweiRun({"list_instances": lambda req: {
        "instances": [HW_RDS] if req["offset"] == 0 else [],
        "total_count": 1}})
    a = _huawei("rds", run, monkeypatch, ident="")
    assert a.find("10.0.0.9", 5432) == "i-1"
    assert a.find("10.0.0.99") is None


def _nested(product, body):
    """Each nested model of a request's body held to its recorded fields."""
    for key, fields in HUAWEI_NESTED.items():
        if key[0] == product and key[1] in body:
            for one in (body[key[1]] if isinstance(body[key[1]], list)
                        else [body[key[1]]]):
                assert set(one) <= fields, (key, one)


HW_GAUSS = {"id": "g-1", "status": "ACTIVE", "private_ips": ["10.0.0.21"],
            "port": 8000, "datastore": {"type": "GaussDB", "version": "8.201"},
            "flavor_ref": "gaussdb.opengauss.ee.dn.m6.2xlarge.8.in",
            "flavor_info": {"vcpu": 8, "mem": 64},
            "volume": {"type": "ULTRAHIGH", "size": 480},
            "maintenance_window": "02:00-06:00", "time_zone": "UTC+08:00",
            "nodes": [{"availability_zone": "ap-southeast-1a"},
                      {"availability_zone": "ap-southeast-1b"}],
            "vpc_id": "vpc-1", "subnet_id": "subnet-1",
            "security_group_id": "sg-1"}


def test_a_gaussdb_backup_is_made_and_a_copy_made_from_one_only(monkeypatch):
    run = HuaweiRun({
        "list_instances": lambda req: {"instances": [
            HW_GAUSS if req["id"] == "g-1" else
            {**HW_GAUSS, "id": req["id"], "private_ips": ["10.0.0.22"]}],
            "total_count": 1},
        "create_manual_backup": {"backup": {"id": "b9",
                                            "status": "BUILDING"},
                                 "job_id": "j0"},
        "list_backups": [
            {"backups": [{"id": "b9", "status": "BUILDING"}],
             "total_count": 1},
            {"backups": [{"id": "b9", "status": "COMPLETED",
                          "end_time": "2026-10-01T10:00:00Z"}],
             "total_count": 1}],
        "create_restore_instance": {"instance": {"id": "g-2"},
                                    "job_id": "j1"},
        "show_job_detail": [{"job": {"status": "Running"}},
                            {"job": {"status": "Completed"}}],
        "show_instance_configuration": _hw_config()})
    a = _huawei("gaussdb", run, monkeypatch, ident="g-1")
    got = a.restore_point("migkit-" + "h" * 70, lambda m: None)
    assert (got.id, got.ready) == ("b9", True)
    sent = run.asked("create_manual_backup")[0]["body"]
    assert sent["instance_id"] == "g-1" and len(sent["name"]) == 64
    facts = a.instance(["work_mem"])
    assert (facts.engine, facts.cpu, facts.memory_gb, facts.storage_gb,
            facts.host, facts.port) == ("gaussdb", 8, 64, 480, "10.0.0.21",
                                        8000)
    assert facts.parameters == {"work_mem": cloudapi.Param("4MB", True,
                                                           False)}
    # a copy at a time is refused: its API restores a backup only
    with pytest.raises(NotOffered, match="restore point it keeps"):
        a.clone("g-copy", AT, lambda m: None)
    copy = a.clone("g-copy", RestorePoint("b9", "backup", ready=True),
                   lambda m: None)
    body = run.asked("create_restore_instance")[0]["body"]
    _nested("gaussdb", body)
    assert body["restore_point"] == {"instance_id": "g-1",
                                     "backup_id": "b9"}
    assert body["availability_zone"] == "ap-southeast-1a,ap-southeast-1b"
    assert body["volume"] == {"type": "ULTRAHIGH", "size": 480}
    assert copy == {"host": "10.0.0.22", "port": 8000, "id": "g-2",
                    "kind": "instance"}
    assert run.asked("show_job_detail")[0] == {"id": "j1"}


HW_GEMINI = {"id": "c-1", "status": "normal", "port": "8635",
             "datastore": {"type": "cassandra", "version": "3.11"},
             "mode": "Cluster", "availability_zone": "ap-southeast-1a",
             "vpc_id": "vpc-1", "subnet_id": "subnet-1",
             "security_group_id": "sg-1",
             "groups": [{"id": "g", "volume": {"size": "500"}, "nodes": [
                 {"id": "n1", "private_ip": "10.0.0.31", "role": "",
                  "spec_code": "geminidb.cassandra.xlarge.4",
                  "availability_zone": "ap-southeast-1a"},
                 {"id": "n2", "private_ip": "10.0.0.32", "role": "",
                  "spec_code": "geminidb.cassandra.xlarge.4",
                  "availability_zone": "ap-southeast-1a"},
                 {"id": "n3", "private_ip": "10.0.0.33", "role": "",
                  "spec_code": "geminidb.cassandra.xlarge.4",
                  "availability_zone": "ap-southeast-1a"}]}]}


def test_a_geminidb_backup_and_copy_keep_the_instances_api_and_nodes(
        monkeypatch):
    run = HuaweiRun({
        "list_instances": lambda req: {"instances": [
            HW_GEMINI if req.get("id") == "c-1" else
            {**HW_GEMINI, "id": req.get("id"), "lb_ip_address": "10.0.0.40",
             "lb_port": "8635"}], "total_count": 1},
        "create_back": {"job_id": "j0", "backup_id": "bk1"},
        "list_backups": {"backups": [{"id": "bk1", "status": "COMPLETED",
                                      "end_time": "2026-10-01 10:00:00"}],
                         "total_count": 1},
        "create_instance": {"id": "c-2", "job_id": "j2"},
        "list_jobs": [{"jobs": [{"id": "j2", "status": "Running"}],
                       "total_count": 1},
                      {"jobs": [{"id": "j2", "status": "Completed"}],
                       "total_count": 1}],
        "show_instance_configuration": _hw_config()})
    a = _huawei("geminidb", run, monkeypatch, ident="c-1")
    got = a.restore_point("rp", lambda m: None)
    assert (got.id, got.ready) == ("bk1", True)
    assert run.asked("create_back")[0] == {
        "instance_id": "c-1", "body": {"name": "rp",
                                       "description": "a restore point"}}
    facts = a.instance()
    assert (facts.engine, facts.host, facts.port, facts.storage_gb) == (
        "geminidb-cassandra", "10.0.0.31", 8635, 500)
    copy = a.clone("c-copy", AT, lambda m: None)
    body = run.asked("create_instance")[0]["body"]
    _nested("geminidb", body)
    assert body["restore_info"] == {
        "source_instance_id": "c-1",
        "restore_time": int(cloudapi._time(AT).timestamp() * 1000)}
    assert body["flavor"] == [{"num": "3", "storage": "ULTRAHIGH",
                               "size": "500",
                               "spec_code": "geminidb.cassandra.xlarge.4"}]
    assert body["datastore"]["type"] == "cassandra"
    assert copy == {"host": "10.0.0.40", "port": 8635, "id": "c-2",
                    "kind": "instance"}
    a.clone("c-copy", RestorePoint("bk1", "backup", ready=True),
            lambda m: None)
    assert run.asked("create_instance")[1]["body"]["restore_info"] == {
        "backup_id": "bk1"}
    # found by any node's address or its load balancer's
    listed = HuaweiRun({"list_instances": lambda req: {
        "instances": [{**HW_GEMINI, "lb_ip_address": "10.0.0.40"}]
        if req["offset"] == 0 else [], "total_count": 1}})
    a = _huawei("geminidb", listed, monkeypatch, ident="")
    assert a.find("10.0.0.32") == "c-1"
    assert a.find("10.0.0.40") == "c-1"
    assert a.find("10.0.0.99") is None


def test_the_huawei_sdk_is_run_as_a_program_in_its_own_environment(
        tmp_path, monkeypatch):
    import subprocess
    python = tmp_path / "python"
    python.write_text("")
    monkeypatch.setenv("MIGKIT_HUAWEI_SDK_PYTHON", str(python))
    ran = []

    def run(argv, input=None, **kw):
        ran.append((argv, json.loads(input)))
        out = ('{"error": "Forbidden (IAM.0001)"}' if len(ran) > 1
               else '{"instances": [], "total_count": 0}')
        return subprocess.CompletedProcess(argv, int(len(ran) > 1), out, "")
    monkeypatch.setattr(subprocess, "run", run)
    a = cloudapi.adapter("huawei", {"product": "rds", "id": "i-1",
                                    "region": "ap-southeast-1"})
    a._found = ({"ak": "AKTEST", "sk": "CHANGE_ME"}, "a test's")
    assert a._call("list_instances", id="i-1") == {"instances": [],
                                                   "total_count": 0}
    argv, ask = ran[0]
    assert argv == [str(python), str(cloudapi.Huawei.SCRIPT)]
    assert ask == {"service": "rds", "action": "list_instances",
                   "request": {"id": "i-1"}, "region": "ap-southeast-1",
                   "ak": "AKTEST", "sk": "CHANGE_ME"}
    with pytest.raises(SystemExit, match="refused list_instances for i-1:"
                                         " Forbidden"):
        a._call("list_instances", id="i-1")
    monkeypatch.setenv("MIGKIT_HUAWEI_SDK_PYTHON", str(tmp_path / "gone"))
    with pytest.raises(SystemExit, match="migkit doctor --install"):
        a._call("list_instances", id="i-1")


def test_the_huawei_program_builds_each_request_from_the_sdks_models():
    from migkit import huawei_sdk

    class Body:
        openapi_types = {"instance_id": "str", "name": "str"}

        def __init__(self, **kw):
            self.kw = kw

    class Request:
        openapi_types = {"body": "Body", "tags": "list[str]",
                         "values": "dict(str, str)"}

        def __init__(self, **kw):
            self.kw = kw
    models = types.SimpleNamespace(Body=Body,
                                   CreateManualBackupRequest=Request)
    got = huawei_sdk.model(models, "CreateManualBackupRequest", {
        "body": {"instance_id": "i-1", "name": "rp"}, "tags": ["a"],
        "values": {"k": "v"}})
    assert isinstance(got.kw["body"], Body)
    assert got.kw["body"].kw == {"instance_id": "i-1", "name": "rp"}
    assert (got.kw["tags"], got.kw["values"]) == (["a"], {"k": "v"})
    with pytest.raises(ValueError, match="Body has no bogus"):
        huawei_sdk.model(models, "CreateManualBackupRequest",
                         {"body": {"bogus": 1}})
    assert huawei_sdk.request_class("create_temporary_access_key_by_agency") \
        == "CreateTemporaryAccessKeyByAgencyRequest"


def test_the_huawei_program_answers_one_request_through_the_sdk_given(
        monkeypatch, capsys):
    from migkit import huawei_sdk
    made = {}

    class Basic:
        def __init__(self, ak, sk, project_id=None):
            made["keys"] = (ak, sk, project_id)

        def with_security_token(self, token):
            made["token"] = token
            return self

        def with_project_id(self, project):
            made["project"] = project
            return self

    class Request:
        openapi_types = {"id": "str"}

        def __init__(self, **kw):
            self.kw = kw

    class Built:
        def list_instances(self, req):
            made["request"] = req.kw
            return types.SimpleNamespace(to_dict=lambda: {
                "instances": [{"id": "i-1"}], "total_count": 1})

    class Builder:
        def with_credentials(self, cred):
            made["credentials"] = cred
            return self

        def with_region(self, region):
            made["region"] = region
            return self

        def build(self):
            return Built()
    modules = {
        "huaweicloudsdkcore.auth.credentials": types.SimpleNamespace(
            BasicCredentials=Basic),
        "huaweicloudsdkrds.v3.model": types.SimpleNamespace(
            ListInstancesRequest=Request),
        "huaweicloudsdkrds.v3.region.rds_region": types.SimpleNamespace(
            RdsRegion=types.SimpleNamespace(value_of=lambda r: f"at {r}")),
        "huaweicloudsdkrds.v3": types.SimpleNamespace(
            RdsClient=types.SimpleNamespace(new_builder=Builder))}
    got = huawei_sdk.answer({
        "service": "rds", "action": "list_instances",
        "request": {"id": "i-1"}, "region": "ap-southeast-1", "ak": "AKTEST",
        "sk": "CHANGE_ME", "project_id": "p-1",
        "security_token": "CHANGE_ME"}, load=modules.__getitem__)
    assert got == {"instances": [{"id": "i-1"}], "total_count": 1}
    assert made["keys"] == ("AKTEST", "CHANGE_ME", "p-1")
    assert (made["token"], made["project"], made["region"],
            made["request"]) == ("CHANGE_ME", "p-1", "at ap-southeast-1",
                                 {"id": "i-1"})
    monkeypatch.setattr("sys.stdin", io.StringIO('{"service": "nowhere"}'))
    assert huawei_sdk.main() == 1
    assert "error" in json.loads(capsys.readouterr().out)


def test_huawei_credentials_are_found_where_its_cli_and_sdk_find_them(
        home, monkeypatch):
    run = HuaweiRun({"credentials": {"found": False}})
    monkeypatch.setattr(cloudapi.Huawei, "_run",
                        lambda self, ask: run(self, ask))

    def found(**conf):
        a = cloudapi.adapter("huawei", {"product": "rds", "id": "i-1",
                                        **conf})
        return (*a.credentials(), a.region)
    monkeypatch.setenv("HUAWEICLOUD_SDK_AK", "AKENV")
    monkeypatch.setenv("HUAWEICLOUD_SDK_SK", "CHANGE_ME")
    monkeypatch.setenv("HUAWEICLOUD_SDK_PROJECT_ID", "p-env")
    assert found(region="ap-southeast-1") == (
        {"ak": "AKENV", "sk": "CHANGE_ME", "project_id": "p-env"},
        "the environment (HUAWEICLOUD_SDK_AK)", "ap-southeast-1")
    for key in ("HUAWEICLOUD_SDK_AK", "HUAWEICLOUD_SDK_SK",
                "HUAWEICLOUD_SDK_PROJECT_ID"):
        monkeypatch.delenv(key)
    (home / ".hcloud").mkdir()
    (home / ".hcloud" / "config.json").write_text(json.dumps({
        "current": "default", "profiles": [
            {"name": "default", "accessKeyId": "AKCLI",
             "secretAccessKey": "CHANGE_ME", "region": "ap-southeast-3",
             "projectId": "p-cli"}]}))
    assert found() == ({"ak": "AKCLI", "sk": "CHANGE_ME",
                        "project_id": "p-cli"}, "the CLI's profile default",
                       "ap-southeast-3")
    (home / ".hcloud" / "config.json").unlink()
    (home / ".huaweicloud").mkdir()
    (home / ".huaweicloud" / "credentials").write_text(
        "[basic]\nak = AKSDK\nsk = CHANGE_ME\nproject_id = p-sdk\n")
    assert found(region="ap-southeast-1")[:2] == (
        {"ak": "AKSDK", "sk": "CHANGE_ME", "project_id": "p-sdk"},
        "the SDK's own credentials file")
    (home / ".huaweicloud" / "credentials").unlink()
    run.answers["credentials"] = {"found": True}
    assert found(region="ap-southeast-1")[:2] == ({}, "the instance's agency")
    run.answers["credentials"] = {"found": False}
    with pytest.raises(SystemExit) as e:
        found(region="ap-southeast-1")
    for place in ("HUAWEICLOUD_SDK_AK", "CLI's profile",
                  "SDK's own credentials file", "instance's agency"):
        assert place in str(e.value), e.value


def test_a_huawei_agency_is_assumed_with_what_was_found(home, monkeypatch):
    run = HuaweiRun({"create_temporary_access_key_by_agency": {
        "credential": {"access": "AKTMP", "secret": "CHANGE_ME",
                       "securitytoken": "CHANGE_ME",
                       "expires_at": "2099-01-01T00:00:00Z"}}})
    monkeypatch.setattr(cloudapi.Huawei, "_run",
                        lambda self, ask: run(self, ask))
    monkeypatch.setenv("HUAWEICLOUD_SDK_AK", "AKENV")
    monkeypatch.setenv("HUAWEICLOUD_SDK_SK", "CHANGE_ME")
    role = "iam::0123456789abcdef:agency:migkit"
    keys, where = cloudapi.adapter("huawei", {
        "product": "rds", "id": "i-1", "region": "ap-southeast-1",
        "role": role}).credentials()
    assert keys == {"ak": "AKTMP", "sk": "CHANGE_ME",
                    "security_token": "CHANGE_ME"}
    assert where == (f"the agency {role}, assumed over the environment"
                     " (HUAWEICLOUD_SDK_AK)")
    ask = run.asks[-1]
    assert (ask["service"], ask["ak"]) == ("iam", "AKENV")
    assert ask["request"]["body"]["auth"]["identity"]["assume_role"] == {
        "agency_name": "migkit", "domain_id": "0123456789abcdef",
        "duration_seconds": 43200}


# --- Oracle Cloud ------------------------------------------------------------

class OciClients:
    """Oracle Cloud's clients stood in for: a method the real client does
    not have fails, the arguments are bound to its signature, and each
    answer is the SDK's own model."""

    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def __call__(self, adapter, name):
        module, cls = name.rsplit(".", 1)
        real = getattr(importlib.import_module(f"oci.{module}"), cls)
        clients = self

        class Client:
            def __getattr__(self, method):
                if method.startswith("_"):
                    raise AttributeError(method)
                bound = getattr(real, method)

                def call(*args, **kw):
                    inspect.signature(bound).bind(None, *args, **kw)
                    clients.calls.append((method, args, kw))
                    return types.SimpleNamespace(data=_next(
                        clients.answers[method], (args, kw)))
                return call
        return Client()

    def asked(self, method):
        return [(args, kw) for m, args, kw in self.calls if m == method]


DB_ID = "ocid1.mysqldbsystem.oc1..app"
CFG_ID = "ocid1.mysqlconfiguration.oc1..current"


def _db_system(ident=DB_ID, state="ACTIVE", ip="10.0.0.10"):
    import oci
    m = oci.mysql.models
    return m.DbSystem(
        id=ident, compartment_id="ocid1.compartment.oc1..x",
        shape_name="MySQL.4", mysql_version="8.4.0",
        data_storage_size_in_gbs=100, lifecycle_state=state,
        maintenance=m.MaintenanceDetails(window_start_time="sun 03:00"),
        endpoints=[m.DbSystemEndpoint(ip_address=ip, port=3306,
                                      hostname="app")],
        configuration_id=CFG_ID, subnet_id="ocid1.subnet.oc1..s",
        availability_domain="AD-1")


def _oracle(product, clients, monkeypatch, **conf):
    monkeypatch.setattr(cloudapi.Oracle, "client",
                        lambda self, name: clients(self, name))
    a = cloudapi.adapter("oracle", {"product": product, "id": DB_ID,
                                    "region": "us-ashburn-1", **conf})
    a._found = ({"config": {}, "signer": None}, "a test's")
    return a


def test_an_oracle_mysql_db_systems_facts_and_variables_are_read(
        monkeypatch):
    import oci
    m = oci.mysql.models
    clients = OciClients({
        "get_db_system": _db_system(),
        "list_shapes": [m.ShapeSummary(name="MySQL.4", cpu_core_count=4,
                                       memory_size_in_gbs=64)],
        "list_db_systems": [m.DbSystemSummary(id=DB_ID),
                            m.DbSystemSummary(id="ocid1.mysqldbsystem."
                                                 "oc1..other")],
        "get_configuration": m.Configuration(
            id=CFG_ID, variables=m.ConfigurationVariables(
                max_connections=1000))})
    got = _oracle("mysql", clients, monkeypatch).instance(["max_connections",
                                                           "no_such"])
    assert (got.engine, got.version, got.klass) == ("mysql", "8.4.0",
                                                    "MySQL.4")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (4, 64, 100)
    assert (got.host, got.port, got.status) == ("10.0.0.10", 3306, "ACTIVE")
    assert got.maintenance == "sun 03:00 UTC"
    assert (got.group, got.group_shared) == (CFG_ID, True)
    assert got.parameters == {
        "max_connections": cloudapi.Param(1000, True),
        "no_such": cloudapi.Param(None, False)}
    assert clients.asked("list_db_systems") == [((), {
        "compartment_id": "ocid1.compartment.oc1..x",
        "configuration_id": CFG_ID})]


def test_an_oracle_mysql_backup_is_made_and_waited_for(monkeypatch):
    import oci
    m = oci.mysql.models
    when = cloudapi._time(AT)
    clients = OciClients({
        "get_db_system": _db_system(),
        "list_backups": [m.BackupSummary(id="b1", lifecycle_state="ACTIVE",
                                         time_created=when)],
        "create_backup": m.Backup(id="b1", lifecycle_state="CREATING"),
        "get_backup": [m.Backup(id="b1", lifecycle_state="CREATING"),
                       m.Backup(id="b1", lifecycle_state="ACTIVE",
                                time_created=when)]})
    a = _oracle("mysql", clients, monkeypatch)
    got = a.restore_point("rp", lambda m: None)
    assert got == RestorePoint("b1", "backup", "ACTIVE", AT, True)
    details = clients.asked("create_backup")[0][0][0]
    assert (details.db_system_id, details.backup_type,
            details.display_name) == (DB_ID, "FULL", "rp")
    assert len(clients.asked("get_backup")) == 2
    assert a.restore_points() == [got]
    assert clients.asked("list_backups") == [((), {
        "compartment_id": "ocid1.compartment.oc1..x",
        "db_system_id": DB_ID})]
    clients.answers["get_backup"] = m.Backup(id="b1",
                                             lifecycle_state="FAILED")
    with pytest.raises(SystemExit, match="the backup rp is failed"):
        a.restore_point("rp", lambda m: None)


def test_an_oracle_mysql_db_system_is_restored_from_a_backup(monkeypatch):
    import oci
    new_id = "ocid1.mysqldbsystem.oc1..copy"
    states = ["CREATING", "ACTIVE"]

    def get(asked):
        if asked[0][0] == DB_ID:
            return _db_system()
        state = states.pop(0) if len(states) > 1 else states[0]
        return _db_system(new_id, state, "10.0.0.11")
    clients = OciClients({
        "get_db_system": get,
        "create_db_system": _db_system(new_id, "CREATING", "10.0.0.11")})
    got = _oracle("mysql", clients, monkeypatch).clone(
        "copy", RestorePoint("b1", "backup", ready=True), lambda m: None)
    assert got == {"host": "10.0.0.11", "port": 3306, "id": new_id,
                   "kind": "db system"}
    details = clients.asked("create_db_system")[0][0][0]
    assert isinstance(details.source,
                      oci.mysql.models.CreateDbSystemSourceFromBackupDetails)
    assert details.source.backup_id == "b1"
    assert (details.display_name, details.shape_name,
            details.subnet_id) == ("copy", "MySQL.4", "ocid1.subnet.oc1..s")


def test_an_oracle_mysql_variable_is_set_through_a_configuration_of_its_own(
        monkeypatch):
    import oci
    m = oci.mysql.models
    clients = OciClients({
        "get_db_system": [_db_system(), _db_system(state="UPDATING"),
                          _db_system()],
        "get_configuration": m.Configuration(
            id=CFG_ID, variables=m.ConfigurationVariables(
                max_connections=1000)),
        "create_configuration": m.Configuration(
            id="ocid1.mysqlconfiguration.oc1..new"),
        "update_db_system": None})
    before = _oracle("mysql", clients, monkeypatch).set_parameters(
        {"max_connections": "2000"}, lambda m: None)
    assert before == {"max_connections": 1000}
    made = clients.asked("create_configuration")[0][0][0]
    assert made.parent_configuration_id == CFG_ID
    assert made.variables.max_connections == 2000
    ident, update = clients.asked("update_db_system")[0][0]
    assert (ident, update.configuration_id) == (
        DB_ID, "ocid1.mysqlconfiguration.oc1..new")
    assert len(clients.asked("get_db_system")) == 3
    with pytest.raises(SystemExit, match="is no variable"):
        _oracle("mysql", clients, monkeypatch).set_parameters(
            {"no_such": 1}, lambda m: None)


def test_an_oracle_mysql_db_system_is_found_by_its_address(monkeypatch):
    import oci
    m = oci.mysql.models
    clients = OciClients({"list_db_systems": [m.DbSystemSummary(
        id=DB_ID, endpoints=[m.DbSystemEndpoint(ip_address="10.0.0.10",
                                                hostname="app")])]})
    a = _oracle("mysql", clients, monkeypatch,
                compartment="ocid1.compartment.oc1..x")
    assert a.find("10.0.0.10") == DB_ID
    assert a.find("10.0.0.99") is None
    assert clients.asked("list_db_systems")[0][1] == {
        "compartment_id": "ocid1.compartment.oc1..x"}


def test_oracle_credentials_are_found_where_its_cli_finds_them(
        home, monkeypatch):
    import oci
    (home / ".oci").mkdir()
    (home / ".oci" / "key.pem").write_text("CHANGE_ME")
    (home / ".oci" / "config").write_text(
        "[DEFAULT]\nuser=ocid1.user.oc1..aaaa\n"
        "fingerprint=" + ":".join(["aa"] * 16) + "\n"
        "tenancy=ocid1.tenancy.oc1..aaaa\nregion=us-ashburn-1\n"
        f"key_file={home / '.oci' / 'key.pem'}\n")

    def found(**conf):
        a = cloudapi.adapter("oracle", {"product": "mysql", "id": DB_ID,
                                        **conf})
        return (*a.credentials(), a.region)
    got, where, region = found()
    assert (where, region) == ("the CLI's config file, profile DEFAULT",
                               "us-ashburn-1")
    assert got["config"]["user"] == "ocid1.user.oc1..aaaa"
    assert got["signer"] is None
    with pytest.raises(SystemExit, match="no usable profile nope"):
        found(profile="nope")
    (home / ".oci" / "config").unlink()

    class Principal:
        region = "eu-frankfurt-1"
    monkeypatch.setattr(oci.auth.signers,
                        "InstancePrincipalsSecurityTokenSigner", Principal)
    got, where, region = found()
    assert (where, region) == ("the instance's principal", "eu-frankfurt-1")
    assert isinstance(got["signer"], Principal)

    def nowhere():
        raise OSError("no metadata service here")
    monkeypatch.setattr(oci.auth.signers,
                        "InstancePrincipalsSecurityTokenSigner", nowhere)
    with pytest.raises(SystemExit) as e:
        found()
    for place in ("CLI's config file", "instance's principal"):
        assert place in str(e.value), e.value
    with pytest.raises(SystemExit, match="assumes no role"):
        found(role="x")


# --- IBM Cloud ---------------------------------------------------------------

CRN = "crn:v1:bluemix:public:databases-for-postgresql:us-south:a/abc:guid-1::"
NEW_CRN = CRN.replace("guid-1", "guid-2")
ICD = "https://api.us-south.databases.cloud.ibm.com/v5/ibm"
RC = "https://resource-controller.cloud.ibm.com"


def _escaped(crn):
    from urllib.parse import quote
    return quote(crn, safe="")


def _icd_send(monkeypatch, answers):
    """The core library's sending stood in for: the request is its own
    `prepare_request`'s, recorded and answered by method and address."""
    from ibm_cloud_sdk_core import BaseService, DetailedResponse
    asked = []

    def send(self, request, **kw):
        data = request.get("data")
        asked.append((request["method"], request["url"],
                      json.loads(data) if data else None))
        got = _next(answers[(request["method"], request["url"])], request)
        return DetailedResponse(response=got, status_code=200)
    monkeypatch.setattr(BaseService, "send", send)
    return asked


def _ibm(**conf):
    from ibm_cloud_sdk_core.authenticators import NoAuthAuthenticator
    a = cloudapi.adapter("ibm", {"product": "icd", "id": CRN,
                                 "region": "us-south", **conf})
    a._found = (NoAuthAuthenticator(), "a test's")
    return a


def _connection(crn, host):
    return {("GET", f"{ICD}/deployments/{_escaped(crn)}/users/database/admin"
                    "/connections/private"):
            {"connection": {"postgres": {"hosts": [{"hostname": host,
                                                    "port": 31000}]}}}}


def test_an_ibm_deployments_facts_and_settings_are_read(monkeypatch):
    e = _escaped(CRN)
    asked = _icd_send(monkeypatch, {
        ("GET", f"{ICD}/deployments/{e}"): {"deployment": {
            "id": CRN, "type": "databases-for-postgresql", "version": "16"}},
        ("GET", f"{ICD}/deployments/{e}/groups"): {"groups": [{
            "id": "member", "count": 2, "members": {"allocation_count": 2},
            "memory": {"allocation_mb": 8192}, "cpu": {"allocation_count": 6},
            "disk": {"allocation_mb": 20480}}]},
        ("GET", f"{ICD}/deployments/{e}/configuration/schema"): {"schema": {
            "max_connections": {"type": "integer",
                                "requires_restart": True}}},
        **_connection(CRN, "db.example.com")})
    got = _ibm().instance(["max_connections", "no_such"])
    assert (got.engine, got.version) == ("postgresql", "16")
    assert (got.cpu, got.memory_gb, got.storage_gb) == (3, 4.0, 10.0)
    assert (got.host, got.port) == ("db.example.com", 31000)
    assert got.parameters == {
        "max_connections": cloudapi.Param(None, True, True),
        "no_such": cloudapi.Param(None, False, None)}
    # the deployment's CRN is escaped into every address
    assert all(e in url for _, url, _ in asked)


def test_an_ibm_backup_is_made_its_task_waited_for_and_found(monkeypatch):
    e = _escaped(CRN)
    answers = {
        ("POST", f"{ICD}/deployments/{e}/backups"): {"task": {
            "id": "t1", "created_at": AT}},
        ("GET", f"{ICD}/tasks/t1"): [{"task": {"status": "running"}},
                                    {"task": {"status": "completed"}}],
        ("GET", f"{ICD}/deployments/{e}/backups"): {"backups": [
            {"id": "old", "type": "on_demand", "status": "completed",
             "created_at": "2026-09-01T00:00:00Z"},
            {"id": "scheduled", "type": "scheduled", "status": "completed",
             "created_at": "2026-10-01T11:00:00Z"},
            {"id": "made", "type": "on_demand", "status": "completed",
             "is_restorable": True, "created_at": "2026-10-01T10:00:05Z"}]}}
    asked = _icd_send(monkeypatch, answers)
    got = _ibm().restore_point("rp", lambda m: None)
    assert got == RestorePoint("made", "backup", "completed",
                               "2026-10-01T10:00:05Z", True)
    assert [u for m, u, _ in asked].count(f"{ICD}/tasks/t1") == 2
    assert {p.id for p in _ibm().restore_points()} == {"old", "scheduled",
                                                       "made"}
    answers[("GET", f"{ICD}/tasks/t1")] = {"task": {"status": "failed"}}
    with pytest.raises(SystemExit, match="is failed"):
        _ibm().restore_point("rp", lambda m: None)


def test_an_ibm_deployment_is_copied_by_the_resource_controller(monkeypatch):
    e = _escaped(CRN)
    asked = _icd_send(monkeypatch, {
        ("GET", f"{ICD}/deployments/{e}"): {"deployment": {
            "id": CRN, "type": "databases-for-postgresql"}},
        ("POST", f"{RC}/v2/resource_instances"): {"guid": "guid-2",
                                                  "crn": NEW_CRN},
        ("GET", f"{RC}/v2/resource_instances/guid-2"): [
            {"state": "provisioning"}, {"state": "active", "crn": NEW_CRN}],
        **_connection(NEW_CRN, "copy.example.com")})
    got = _ibm(resource_group="rg-1").clone(
        "copy", RestorePoint("made", "backup", ready=True), lambda m: None)
    assert got == {"host": "copy.example.com", "port": 31000, "id": NEW_CRN,
                   "kind": "deployment"}
    body = next(d for m, u, d in asked if m == "POST")
    assert body == {"name": "copy", "target": "us-south",
                    "resource_group": "rg-1",
                    "resource_plan_id": "databases-for-postgresql-standard",
                    "parameters": {"backup_id": "made"}}
    with pytest.raises(SystemExit, match="resource group is not named"):
        _ibm().clone("copy", None, lambda m: None)


def test_an_ibm_setting_is_changed_and_its_task_waited_for(monkeypatch):
    e = _escaped(CRN)
    said, log = _log()
    asked = _icd_send(monkeypatch, {
        ("GET", f"{ICD}/deployments/{e}/configuration/schema"): {"schema": {
            "max_connections": {"type": "integer",
                                "requires_restart": True}}},
        ("PATCH", f"{ICD}/deployments/{e}/configuration"): {"task": {
            "id": "t2"}},
        ("GET", f"{ICD}/tasks/t2"): [{"task": {"status": "queued"}},
                                    {"task": {"status": "completed"}}]})
    before = _ibm().set_parameters({"max_connections": "300"}, log)
    assert before == {"max_connections": None}
    assert [d for m, u, d in asked if m == "PATCH"] == [
        {"configuration": {"max_connections": 300}}]
    assert any("does not say their values before" in s for s in said), said
    assert [u for m, u, _ in asked].count(f"{ICD}/tasks/t2") == 2
    with pytest.raises(SystemExit, match="is no setting"):
        _ibm().set_parameters({"no_such": 1}, log)


def test_an_ibm_deployment_is_found_by_its_address(monkeypatch):
    other = CRN.replace("us-south", "eu-de")
    _icd_send(monkeypatch, {
        ("GET", f"{RC}/v2/resource_instances?type=service_instance"): {
            "resources": [{"crn": other, "region_id": "eu-de"},
                          {"crn": "crn:v1:bluemix:public:cloud-object-"
                                  "storage:global:a/abc:x::",
                           "region_id": "us-south"},
                          {"crn": CRN, "region_id": "us-south"}]},
        **_connection(CRN, "db.example.com")})
    assert _ibm(id="").find("db.example.com") == CRN
    assert _ibm(id="").find("10.0.0.99") is None


def _jwt():
    import time

    import jwt
    now = int(time.time())
    return jwt.encode({"iat": now, "exp": now + 3600},
                      "migkit-test-key-long-enough-for-hs256-0123",
                      algorithm="HS256")


def test_ibm_credentials_are_found_where_its_cli_finds_them(home,
                                                            monkeypatch):
    from ibm_cloud_sdk_core.authenticators import (BearerTokenAuthenticator,
                                                   IAMAuthenticator,
                                                   VPCInstanceAuthenticator)
    from ibm_cloud_sdk_core.token_managers.vpc_instance_token_manager import \
        VPCInstanceTokenManager

    def found(**conf):
        a = cloudapi.adapter("ibm", {"product": "icd", "id": CRN, **conf})
        return (*a.credentials(), a.region)
    monkeypatch.setenv("IBMCLOUD_API_KEY", "CHANGE_ME")
    auth, where, _ = found(region="us-south")
    assert isinstance(auth, IAMAuthenticator)
    assert where == "the environment (IBMCLOUD_API_KEY)"
    monkeypatch.delenv("IBMCLOUD_API_KEY")
    (home / ".bluemix").mkdir()
    token = _jwt()
    (home / ".bluemix" / "config.json").write_text(json.dumps({
        "IAMToken": f"Bearer {token}", "Region": "eu-de"}))
    auth, where, region = found()
    assert isinstance(auth, BearerTokenAuthenticator)
    assert (auth.bearer_token, where, region) == (token, "the CLI's login",
                                                  "eu-de")
    (home / ".bluemix" / "config.json").unlink()
    monkeypatch.setattr(VPCInstanceTokenManager, "get_token",
                        lambda self: token)
    auth, where, _ = found(region="us-south")
    assert isinstance(auth, VPCInstanceAuthenticator)
    assert where == "the instance's identity"

    def nowhere(self):
        raise OSError("no metadata service here")
    monkeypatch.setattr(VPCInstanceTokenManager, "get_token", nowhere)
    with pytest.raises(SystemExit) as e:
        found(region="us-south")
    for place in ("IBMCLOUD_API_KEY", "CLI's login", "instance's identity"):
        assert place in str(e.value), e.value


def test_an_ibm_trusted_profile_is_assumed_with_the_api_key(home,
                                                           monkeypatch):
    from ibm_cloud_sdk_core.token_managers.jwt_token_manager import \
        JWTTokenManager
    asked = []

    def request(self, method, url, *, headers=None, params=None, data=None,
                auth_tuple=None, **kw):
        asked.append(dict(data or {}))
        return {"access_token": _jwt(), "token_type": "Bearer",
                "expires_in": 3600}
    monkeypatch.setattr(JWTTokenManager, "_request", request)
    monkeypatch.setenv("IBMCLOUD_API_KEY", "CHANGE_ME")
    role = "crn:v1:bluemix:public:iam-identity::a/abc::profile:Profile-1"
    auth, where = cloudapi.adapter("ibm", {
        "product": "icd", "id": CRN, "region": "us-south",
        "role": role}).credentials()
    assert where == (f"the trusted profile {role}, assumed over the"
                     " environment (IBMCLOUD_API_KEY)")
    auth.token_manager.get_token()
    assert asked[0]["apikey"] == "CHANGE_ME"
    assert (asked[-1]["grant_type"], asked[-1]["profile_crn"]) == (
        "urn:ibm:params:oauth:grant-type:assume", role)


# --- the environment of its own ----------------------------------------------

def test_the_huawei_sdk_gets_an_environment_of_its_own_counted_not_named(
        home, monkeypatch):
    from migkit import tools
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    monkeypatch.delenv("MIGKIT_HUAWEI_SDK_PYTHON", raising=False)
    ran = []
    # each step of the install, as the runner would run it
    monkeypatch.setattr(tools, "_step", lambda argv, log, failed, **kw:
                        ran.append(argv) or True)
    monkeypatch.setattr(tools, "which", lambda n: "/x/uv" if n == "uv"
                        else None)
    said = []
    assert tools.own_env_python(tools.HUAWEI_SDK) is None
    assert tools.install_own_env(
        tools.own_env(tools.HUAWEI_SDK), tools.HUAWEI_SDK_PACKAGES,
        said.append, ("installing component 1 of 1 in an environment of its"
                      " own ...", "  component 1 did not install")) is True
    home_env = home / ".migkit" / "huawei-sdk"
    assert ran[0] == ["/x/uv", "venv", "-q", "--python", "3.12",
                      str(home_env)]
    assert ran[1][:3] == ["/x/uv", "pip", "install"]
    assert ran[1][-len(tools.HUAWEI_SDK_PACKAGES):] == list(
        tools.HUAWEI_SDK_PACKAGES)
    assert ran[1][ran[1].index("--python") + 1] == str(home_env / "bin" /
                                                      "python")
    # a program the machine does not have is a failed install, said so
    monkeypatch.setattr(tools, "_step", lambda argv, log, failed, **kw: (
        _ for _ in ()).throw(FileNotFoundError(argv[0])))
    said.clear()
    assert tools.install_own_env(home_env, ("x",), said.append,
                                 ("first", "  it did not install")) is False
    assert said == ["first", "  it did not install"]
    # made, it is found where the adapter runs it from
    (home_env / "bin").mkdir(parents=True)
    (home_env / "bin" / "python").write_text("")
    assert tools.own_env_python(tools.HUAWEI_SDK) == str(home_env / "bin" /
                                                         "python")
    low = " ".join(said).lower()
    assert not [t for t in TOOLS + ("huawei",) if t in low], said


def test_doctor_install_makes_each_missing_environment_counted(
        home, monkeypatch):
    from migkit import tools
    made = []
    monkeypatch.setattr(tools, "install_own_env",
                        lambda env, packages, log, said: made.append(
                            (env, packages, said)) or True)
    monkeypatch.setattr(tools, "which", lambda n: "/bin/" + n)
    monkeypatch.setattr(tools, "second_reader_present", lambda: True)
    monkeypatch.setattr(tools, "ase_driver_present", lambda: True)
    monkeypatch.setattr(tools, "install_second_reader",
                        lambda log=None: pytest.fail("nothing to install"))
    # nothing else of the machine's is installed or asked for here
    for name in ("install_from_source", "install_vendor", "install_ibm",
                 "install_redo_reader"):
        monkeypatch.setattr(tools, name, lambda *a, _n=name, **k:
                            pytest.fail(f"{_n} was not to run"))
    monkeypatch.setattr(tools, "built_program", lambda n: (["/bin/" + n],
                                                           {}))
    monkeypatch.setattr(tools, "ibmi_odbc_present", lambda: True)
    monkeypatch.setattr(tools, "db2_connect_present", lambda: True)
    monkeypatch.setattr(tools, "_redo_reader_here", lambda: False)
    monkeypatch.setattr(tools, "accept_terms", lambda *a, **k: {})
    tools.install_missing(lambda m: None)
    assert [(env, packages) for env, packages, _ in made] == [
        (home / ".migkit" / "huawei-sdk", tools.HUAWEI_SDK_PACKAGES)]
    first, failed = made[0][2]
    assert first == ("installing component 1 of 1 in an environment of its"
                     " own ...")
    assert "migkit doctor --install" in failed
    # installed already: nothing made again
    python = home / ".migkit" / "huawei-sdk" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("")
    made.clear()
    tools.install_missing(lambda m: None)
    assert made == []


