"""A cloud's own copy seeds the target through its API (R19 lever 1): the
Aurora clone, the RDS/Aurora snapshot shared, copied across accounts and
regions and restored, Azure's point-in-time restore, Cloud SQL's clone -
then the same exact continuation as a copy of the pages.

The AWS control plane runs against moto wherever moto implements the call
(snapshot, share, copy, restore, clone, the writer, the describes); what
it does not - RAM invitations, a clone from another account, the RDS log
download - runs against a stand-in that records what was asked. Azure and
Google run against stand-ins shaped like their clients. The SQL each seed
reads on the restored server is answered by a stub: the real servers'
side is `test_a_physical_seed_goes_on_exactly_pg.py`. What only a real
account proves is said in `cloudseed`'s docstring.
"""
import os
import types

import pytest

from migkit import cloudseed, physical
from migkit.config import Endpoint, Hop

moto = pytest.importorskip("moto")

SRC_REGION, DST_REGION = "eu-west-1", "eu-central-1"
#: the target's account, made up (twelve digits, as AWS has them)
THEIRS = "2" * 12
MINE = "1" * 12
ROLE = f"arn:aws:iam::{THEIRS}:role/migkit"


@pytest.fixture(autouse=True)
def aws(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for k, v in (("AWS_ACCESS_KEY_ID", "testing"),
                 ("AWS_SECRET_ACCESS_KEY", "testing"),
                 ("AWS_DEFAULT_REGION", SRC_REGION),
                 ("MIGKIT_CLOUD_POLL", "0")):
        monkeypatch.setenv(k, v)
    calls = []
    real = cfg._aws_client

    def spied(service, region=None, role=None):
        c = real(service, region, role)

        def seen(params, model, **kw):
            calls.append((model.name, dict(params), region, role))
        c.meta.events.register(f"provide-client-params.{service}.*", seen)
        return c
    monkeypatch.setattr(cfg, "_aws_client", spied)
    with moto.mock_aws():
        yield calls


def _hop(engine="postgres", **opts):
    host = opts.pop("host", "10.0.0.2")
    return Hop(name="cloud", engine=engine,
               source=Endpoint(host="10.0.0.1", port=5432, user="app",
                               password="CHANGE_ME"),
               target=Endpoint(host=host, port=5432, user="app",
                               password="CHANGE_ME"),
               options={"physical": {"cloud": "aws", "region": SRC_REGION,
                                     **opts}})


def _rds(region=SRC_REGION):
    import boto3
    return boto3.client("rds", region_name=region)


def _aurora(name="app-db", engine="aurora-postgresql", encrypted=False):
    return _rds().create_db_cluster(
        DBClusterIdentifier=name, Engine=engine, MasterUsername="app",
        MasterUserPassword="CHANGE_ME1", StorageEncrypted=encrypted)[
        "DBCluster"]


def _instance(name="pg1", engine="postgres"):
    return _rds().create_db_instance(
        DBInstanceIdentifier=name, Engine=engine,
        DBInstanceClass="db.t3.micro", MasterUsername="app",
        MasterUserPassword="CHANGE_ME1", AllocatedStorage=20)["DBInstance"]


def _asked(calls, name):
    return [c for c in calls if c[0] == name]


def test_an_aurora_clone_is_copy_on_write_with_a_writer(aws):
    _aurora()
    hop = _hop(source_id="app-db", target_id="app-db-seeded",
               instance_class="db.r6g.large")
    cloud = cloudseed.provider(hop, "postgres")
    got = cloud.copy("clone", cloud.source(), lambda m: None)
    restore = _asked(aws, "RestoreDBClusterToPointInTime")
    assert len(restore) == 1, aws
    params = restore[0][1]
    assert params["RestoreType"] == "copy-on-write"
    assert params["UseLatestRestorableTime"] is True
    assert params["SourceDBClusterIdentifier"] == "app-db"
    writer = _asked(aws, "CreateDBInstance")[0][1]
    assert writer["DBClusterIdentifier"] == "app-db-seeded"
    assert writer["DBInstanceClass"] == "db.r6g.large"
    real = _rds().describe_db_clusters(DBClusterIdentifier="app-db-seeded")[
        "DBClusters"][0]
    assert (got["host"], got["port"]) == (real["Endpoint"], real["Port"])


def test_an_instance_snapshot_in_one_account_is_restored(aws):
    _instance()
    hop = _hop(source_id="pg1", target_id="pg1-seeded",
               instance_class="db.t3.large")
    cloud = cloudseed.provider(hop, "postgres")
    got = cloud.copy("snapshot", cloud.source(), lambda m: None)
    assert _asked(aws, "CreateDBSnapshot")
    assert not _asked(aws, "CopyDBSnapshot")
    assert not _asked(aws, "ModifyDBSnapshotAttribute")
    restored = _asked(aws, "RestoreDBInstanceFromDBSnapshot")[0][1]
    assert restored["DBInstanceIdentifier"] == "pg1-seeded"
    assert restored["DBInstanceClass"] == "db.t3.large"
    real = _rds().describe_db_instances(DBInstanceIdentifier="pg1-seeded")[
        "DBInstances"][0]["Endpoint"]
    assert (got["host"], got["port"]) == (real["Address"], real["Port"])


@pytest.mark.parametrize("kind", ["instance", "cluster"])
def test_a_snapshot_crosses_regions(aws, kind):
    if kind == "instance":
        _instance()
        sid = "pg1"
    else:
        _aurora()
        sid = "app-db"
    hop = _hop(source_id=sid, target_id=f"{sid}-seeded",
               instance_class="db.r6g.large", target_region=DST_REGION)
    cloud = cloudseed.provider(hop, "postgres")
    got = cloud.copy("snapshot", cloud.source(), lambda m: None)
    word = "DBClusterSnapshot" if kind == "cluster" else "DBSnapshot"
    # one account: nothing shared
    assert not _asked(aws, f"Modify{word}Attribute")
    copied = _asked(aws, f"Copy{word}")[0]
    assert copied[1]["SourceRegion"] == SRC_REGION
    assert copied[1][f"Source{word}Identifier"].startswith("arn:aws:rds:")
    assert copied[2] == DST_REGION
    restore = "RestoreDBClusterFromSnapshot" if kind == "cluster" else \
        "RestoreDBInstanceFromDBSnapshot"
    assert _asked(aws, restore)[0][2] == DST_REGION
    assert got["host"].endswith(f"{DST_REGION}.rds.amazonaws.com"), got


class _Recorded:
    """A stand-in for one account's client where moto stops (a copy of a
    snapshot another account shared): every call recorded, the waits
    answered at once."""

    def __init__(self, asked, where, service="rds"):
        self.asked, self.where, self.service = asked, where, service

    def __getattr__(self, name):
        def call(**kw):
            self.asked.append((self.where, name, kw))
            snap = {"Status": "available",
                    "DBSnapshotArn": "arn:aws:rds:eu-west-1:1:snapshot:s",
                    "DBClusterSnapshotArn":
                        "arn:aws:rds:eu-west-1:1:cluster-snapshot:s"}
            return {"DBSnapshots": [snap], "DBClusterSnapshots": [snap],
                    "DBInstances": [{"DBInstanceStatus": "available",
                                     "Endpoint": {"Address": "t.example.com",
                                                  "Port": 5432}}],
                    "DBClusters": [{"Status": "available",
                                    "Endpoint": "t.example.com",
                                    "Port": 5432}]}
        return call


@pytest.mark.parametrize("kind", ["instance", "cluster"])
def test_a_snapshot_crosses_accounts_under_the_hops_key(kind):
    asked = []
    key = f"arn:aws:kms:{DST_REGION}:{THEIRS}:key/CHANGE_ME"
    hop = _hop(source_id="src", target_id="new",
               instance_class="db.r6g.large", target_region=DST_REGION,
               target_role_arn=ROLE, kms_key=key)
    cloud = cloudseed.provider(hop, "postgres")
    cloud.client = lambda where="source", service="rds": _Recorded(asked,
                                                                   where)
    got = cloud.copy("snapshot", {"kind": kind, "engine": "postgres"},
                     lambda m: None)
    word = "db_cluster_snapshot" if kind == "cluster" else "db_snapshot"
    shared = [a for a in asked if a[1] == f"modify_{word}_attribute"][0]
    assert shared[0] == "source"
    assert shared[2]["AttributeName"] == "restore"
    assert shared[2]["ValuesToAdd"] == [THEIRS]
    copied = [a for a in asked if a[1] == f"copy_{word}"][0]
    # copied by the target's account, under its key, from the source region
    assert copied[0] == "target"
    assert copied[2]["KmsKeyId"] == key
    assert copied[2]["SourceRegion"] == SRC_REGION
    restored = [a for a in asked if a[1].startswith("restore_")][0]
    assert restored[0] == "target"
    assert got["host"] == "t.example.com"


def test_a_clone_into_another_account_goes_through_a_share(monkeypatch):
    """RAM invitations and a clone of another account's cluster are not in
    moto: a stand-in records what is asked."""
    asked = []

    class Client:
        def __init__(self, where, service):
            self.where, self.service = where, service

        def __getattr__(self, name):
            def call(**kw):
                asked.append((self.where, self.service, name, kw))
                if name == "create_resource_share":
                    return {"resourceShare": {"resourceShareArn": "share"}}
                if name == "get_resource_share_invitations":
                    return {"resourceShareInvitations": [
                        {"status": "PENDING",
                         "resourceShareInvitationArn": "inv"}]}
                if name in ("describe_db_clusters",):
                    return {"DBClusters": [{
                        "Status": "available", "Endpoint": "x.example.com",
                        "Port": 5432}]}
                if name == "describe_db_instances":
                    return {"DBInstances": [{"DBInstanceStatus":
                                             "available"}]}
                return {}
            return call
    hop = _hop(source_id="app-db", target_id="app-db-seeded",
               instance_class="db.r6g.large", target_role_arn=ROLE)
    cloud = cloudseed.provider(hop, "postgres")
    monkeypatch.setattr(cloud, "client",
                        lambda where="source", service="rds":
                        Client(where, service))
    arn = f"arn:aws:rds:eu-west-1:{MINE}:cluster:app-db"
    got = cloud.clone({"arn": arn, "engine": "aurora-postgresql"},
                      lambda m: None)
    share = [a for a in asked if a[2] == "create_resource_share"][0]
    assert share[0] == "source" and share[3]["resourceArns"] == [arn]
    assert share[3]["principals"] == [THEIRS]
    accept = [a for a in asked if a[2] == "accept_resource_share_invitation"]
    assert accept and accept[0][0] == "target"
    restore = [a for a in asked
               if a[2] == "restore_db_cluster_to_point_in_time"][0]
    # the other account names the source by its ARN
    assert restore[0] == "target"
    assert restore[3]["SourceDBClusterIdentifier"] == arn
    assert got["host"] == "x.example.com"


# --- the needs, against what the API says ------------------------------------

def _refused(hop, name, engine="postgres", mode="full", **facts):
    rung = next(r for r in physical.rungs(engine) if r.name == name)
    f = cloudseed.facts_for(hop, None, mode)
    f.update(covers=True, **facts)
    return rung.refused(f)


def test_a_cluster_is_cloned_and_an_instance_is_not(aws):
    _aurora()
    _instance()
    ok = _hop(source_id="app-db", target_id="new", instance_class="db.r6g.l")
    assert _refused(ok, "cloud-aws-clone") is None
    why = _refused(_hop(source_id="pg1", target_id="new"), "cloud-aws-clone")
    assert "only an Aurora cluster is cloned" in why, why
    assert _refused(_hop(source_id="pg1", target_id="new"),
                    "cloud-aws-snapshot") is None


def test_a_clone_stays_in_its_region(aws):
    _aurora()
    why = _refused(_hop(source_id="app-db", target_id="new",
                        instance_class="c", target_region=DST_REGION),
                   "cloud-aws-clone")
    assert "stays in its source's region" in why, why


def test_a_target_that_exists_is_refused(aws):
    _aurora()
    _aurora("taken")
    why = _refused(_hop(source_id="app-db", target_id="taken",
                        instance_class="c"), "cloud-aws-clone")
    assert "taken exists already" in why, why


def test_a_source_the_credentials_cannot_see_is_refused(aws):
    why = _refused(_hop(source_id="nowhere", target_id="new",
                        instance_class="c"), "cloud-aws-snapshot")
    assert "source could not be read" in why and "nowhere" in why, why


def test_an_encrypted_source_across_accounts_needs_a_key(aws):
    _aurora(encrypted=True)
    hop = _hop(source_id="app-db", target_id="new", instance_class="c",
               target_role_arn=ROLE)
    why = _refused(hop, "cloud-aws-snapshot")
    assert "kms_key" in why and "default RDS key cannot be" in why, why


def test_a_source_another_cloud_runs_is_refused(aws):
    """The address names who runs the source (`clouds`, the one place
    that tells them apart): a hop asking AWS to copy a Cloud SQL source is
    refused before anything is asked of AWS; a source nothing names is
    left to AWS's own answer."""
    _aurora()
    hop = _hop(source_id="app-db", target_id="new", instance_class="c")
    hop.source.host = "app.example.sql.goog"
    why = _refused(hop, "cloud-aws-clone")
    assert "asks aws to copy the source" in why and "gcp" in why, why
    hop.source.host = "db.example.com"
    assert _refused(hop, "cloud-aws-clone") is None


def test_a_source_of_another_engine_is_refused(aws):
    _aurora()
    why = _refused(_hop("mysql", source_id="app-db", target_id="new",
                        instance_class="c"), "cloud-aws-snapshot", "mysql",
                   gtid_mode="ON", log_bin=True, enhanced_binlog=False)
    assert "the hop moves mysql" in why, why


def test_enhanced_binlog_without_gtids_is_refused(aws):
    _aurora(engine="aurora-mysql")
    hop = _hop("mysql", source_id="app-db", target_id="new",
               instance_class="c")
    why = _refused(hop, "cloud-aws-snapshot", "mysql", "full+cdc",
                   gtid_mode="OFF", log_bin=True, enhanced_binlog=True)
    assert "enhanced binlog" in why, why
    assert _refused(hop, "cloud-aws-snapshot", "mysql", "full+cdc",
                    gtid_mode="ON", log_bin=True,
                    enhanced_binlog=True) is None


# --- the whole move, the control plane real and the SQL stubbed --------------

class _Pg:
    """The SQL side of a PostgreSQL hop, answered: what the source and the
    restored target would say, and every statement run, recorded."""

    def __init__(self, eng):
        self.ran = []
        eng._psql = self.psql
        eng.apply_replication_stmt = lambda side, db, stmt: self.ran.append(
            (side, db, stmt))
        eng._subscription_streaming = lambda db: None
        eng.replication_status = lambda db, sql: "streaming"
        eng.table_facts = lambda side, db: {"public.t": {"rows": 1000}}
        # the tables a publication lists, read in one attempt of its own
        eng._plan_rows = lambda side, db, sql: [("public.t",)]
        # the one way a slot a copy follows is made
        eng._slot_with_its_snapshot = (
            lambda side, db, name, plugin=None: self.ran.append(
                (side, db, f"slot {name} made with its snapshot, {plugin}")))

    def psql(self, side, db, sql, statement_timeout=0):
        self.ran.append((side, db, sql))
        if "aurora_volume_logical_start_lsn" in sql:
            return "0/7A000028"
        if "show wal_level" in sql:
            return "logical"
        if "system_identifier" in sql:
            return "7400000000000000001"
        return ""


def test_an_aurora_clone_goes_on_from_its_volumes_start(aws, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    _aurora()
    got = []
    hop = _hop(source_id="app-db", target_id="app-db-seeded",
               instance_class="db.r6g.large")
    hop.databases = ["app"]
    eng = PostgresEngine(hop)
    sql = _Pg(eng)
    monkeypatch.setattr(physical.PgFacts, "_src_databases",
                        lambda self: ["app"])
    slots_when_copied = []
    real_copy = cloudseed.Aws.copy

    def copy(self, way, src, log):
        slots_when_copied.append(len([
            s for s in sql.ran if "made with its snapshot" in s[2]]))
        return real_copy(self, way, src, log)
    monkeypatch.setattr(cloudseed.Aws, "copy", copy)
    # the slot before the copy, the copy - and the hop's target is not the
    # copy yet: said, and nothing continued
    with pytest.raises(SystemExit) as e:
        physical.carry(hop, eng, "", "full+cdc", True, got.append)
    endpoint = _rds().describe_db_clusters(
        DBClusterIdentifier="app-db-seeded")["DBClusters"][0]["Endpoint"]
    assert endpoint in str(e.value) and "Point the hop's target there" in \
        str(e.value), e.value
    slot = [s for s in sql.ran if "made with its snapshot" in s[2]]
    assert slot and slot[0][0] == "src", sql.ran
    assert slot[0][2].endswith("pgoutput"), slot
    # made before the provider was asked for the copy
    assert slots_when_copied == [1], slots_when_copied
    assert not [s for s in sql.ran if "create subscription" in s[2]]
    # pointed there, the move goes on from the copy without a second one
    hop.target.host = endpoint
    assert physical.carry(hop, eng, "", "full+cdc", True, got.append)
    assert slots_when_copied == [1]
    assert len(_asked(aws, "RestoreDBClusterToPointInTime")) == 1
    subs = [s for s in sql.ran if s[0] == "dst"]
    stmts = [s[2] for s in subs]
    assert any("create_slot = false" in s and "enabled = false" in s
               for s in stmts), stmts
    advance = [s for s in stmts if "pg_replication_origin_advance" in s]
    assert advance and "'0/7A000028'" in advance[0], stmts
    rec = physical.seeded(hop)
    assert rec["lsn"] == "0/7A000028" and rec["target"]["host"] == endpoint


def test_the_first_carry_says_the_copy_is_elsewhere(aws, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    _aurora()
    hop = _hop(source_id="app-db", target_id="app-db-seeded",
               instance_class="db.r6g.large")
    hop.databases = ["app"]
    eng = PostgresEngine(hop)
    _Pg(eng)
    monkeypatch.setattr(physical.PgFacts, "_src_databases",
                        lambda self: ["app"])
    with pytest.raises(SystemExit) as e:
        physical.carry(hop, eng, "", "full+cdc", True, lambda m: None)
    assert "app-db-seeded" in str(e.value) and "is kept" in str(e.value)


# --- the seed position on MySQL ----------------------------------------------

def test_the_restored_servers_own_log_says_its_position(monkeypatch):
    hop = _hop("mysql", source_id="app-db", target_id="new")
    cloud = cloudseed.provider(hop, "mysql")

    class Logs:
        def __init__(self, text):
            self.text = text

        def describe_db_log_files(self, **kw):
            return {"DescribeDBLogFiles": [
                {"LogFileName": "slowquery/mysql-slowquery.log"},
                {"LogFileName": "error/mysql-error-running.log"}]}

        def download_db_log_file_portion(self, **kw):
            return {"LogFileData": self.text}
    for text, want in (
            ("[Note] Binlog position from crash recovery is"
             " /rdsdbdata/log/binlog/mysql-bin-changelog.000042 4278\n",
             {"log_file": "mysql-bin-changelog.000042", "log_pos": 4278}),
            ("[InnoDB] Last MySQL binlog file position 0 1893, file name"
             " mysql-bin-changelog.000007\n",
             {"log_file": "mysql-bin-changelog.000007", "log_pos": 1893}),
            ("nothing here\n", None)):
        monkeypatch.setattr(cloud, "client",
                            lambda where="source", service="rds", t=text:
                            Logs(t))
        assert cloud.binlog_position({"id": "new"}, lambda m: None) == want


U = "3e11fa47-71ca-11e1-9e33-c80aa9429562"


def test_a_gtid_set_is_placed_in_the_sources_log():
    """The newest binlog whose previous set the copy holds whole, then the
    first transaction in it the copy lacks."""
    class My:
        hop = _hop("mysql")

        def __init__(self, oldest=""):
            self.oldest = oldest

        def _q(self, side, sql):
            if sql == "show binary logs":
                return [("binlog.000001", 1), ("binlog.000002", 1),
                        ("binlog.000003", 1)]
            name = sql.split("'")[1]
            prev = {"binlog.000001": self.oldest,
                    "binlog.000002": f"{U}:1-40",
                    "binlog.000003": f"{U}:1-95"}[name]
            return [(name, 4, "Format_desc", 1, 126, ""),
                    (name, 126, "Previous_gtids", 1, 157, prev)]
    seen = {}

    def scan(eng, name, have):
        seen["from"] = name
        return {"log_file": name, "log_pos": 777}
    got = cloudseed.position_of(My(), physical.gtid_set(f"{U}:1-60"),
                                scan=scan)
    assert seen["from"] == "binlog.000002" and got["log_pos"] == 777
    got = cloudseed.position_of(My(), physical.gtid_set(f"{U}:1-30"),
                                scan=scan)
    assert seen["from"] == "binlog.000001"
    # the log that held what the copy lacks was purged: nothing carries it
    with pytest.raises(SystemExit) as e:
        cloudseed.position_of(My(oldest=f"{U}:1-10"),
                              physical.gtid_set(f"{U}:1-5"), scan=scan)
    assert "oldest binlog" in str(e.value)


# --- Azure and Google, through stand-ins shaped like their clients -----------

def test_azure_restores_the_source_to_now():
    made = {}

    class Poller:
        def result(self):
            return types.SimpleNamespace(
                fully_qualified_domain_name="new.postgres.database.azure.com")

    class Servers:
        def get(self, rg, name):
            if name != "src":
                err = type("ResourceNotFoundError", (Exception,), {})
                raise err(name)
            return types.SimpleNamespace(id="/subscriptions/x/src",
                                         location="westeurope", version="16")

        def begin_create_or_update(self, rg, name, body):
            made.update(rg=rg, name=name, body=body)
            return Poller()

    hop = _hop(cloud="azure", subscription="s", resource_group="rg",
               source_id="src", target_id="new")
    hop.options["physical"]["cloud"] = "azure"
    cloud = cloudseed.provider(hop, "postgres")
    cloud.client = lambda: types.SimpleNamespace(servers=Servers())
    src = cloud.source()
    assert cloud.target_exists() is False
    got = cloud.copy("restore", src, lambda m: None)
    body = made["body"]
    assert body.create_mode == "PointInTimeRestore"
    assert body.source_server_resource_id == "/subscriptions/x/src"
    assert body.point_in_time_utc is not None
    assert body.location == "westeurope"
    assert got == {"host": "new.postgres.database.azure.com", "port": 5432,
                   "id": "new", "kind": "server"}


def test_azure_mysql_names_its_restore_point_its_own_way():
    made = {}

    class Servers:
        def begin_create(self, rg, name, body):
            made["body"] = body
            return types.SimpleNamespace(result=lambda: types.SimpleNamespace(
                fully_qualified_domain_name="new.mysql.database.azure.com"))
    hop = _hop("mysql", subscription="s", resource_group="rg",
               source_id="src", target_id="new")
    hop.options["physical"]["cloud"] = "azure"
    cloud = cloudseed.provider(hop, "mysql")
    cloud.client = lambda: types.SimpleNamespace(servers=Servers())
    got = cloud.copy("restore", {"id": "/x/src", "location": "l"},
                     lambda m: None)
    assert made["body"].restore_point_in_time is not None
    assert got["port"] == 3306


def test_cloud_sql_clones_mysql_at_the_coordinates_it_was_given():
    asked = {}

    class Call:
        def __init__(self, answer):
            self.answer = answer

        def execute(self):
            return self.answer

    class Instances:
        def clone(self, project, instance, body):
            asked["clone"] = (project, instance, body)
            return Call({"name": "op1"})

        def get(self, project, instance):
            return Call({"ipAddresses": [
                {"type": "PRIMARY", "ipAddress": "203.0.113.9"},
                {"type": "PRIVATE", "ipAddress": "10.0.0.7"}]})

    class Operations:
        n = 0

        def get(self, project, operation):
            Operations.n += 1
            return Call({"status": "DONE" if Operations.n > 1 else
                         "RUNNING"})

    api = types.SimpleNamespace(instances=Instances, operations=Operations)
    hop = _hop("mysql", project="p", source_id="src", target_id="new")
    hop.options["physical"]["cloud"] = "google"
    cloud = cloudseed.provider(hop, "mysql")
    cloud.client = lambda: types.SimpleNamespace(
        instances=lambda: api.instances(), operations=lambda: api.operations())
    got = cloud.copy("clone", {}, lambda m: None,
                     at={"log_file": "mysql-bin.000009", "log_pos": 1234})
    project, instance, body = asked["clone"]
    ctx = body["cloneContext"]
    assert (project, instance) == ("p", "src")
    assert ctx["destinationInstanceName"] == "new"
    assert ctx["binLogCoordinates"] == {"kind": "sql#binLogCoordinates",
                                        "binLogFileName": "mysql-bin.000009",
                                        "binLogPosition": "1234"}
    assert got["host"] == "10.0.0.7" and got["port"] == 3306
    assert Operations.n == 2


def test_a_failed_cloud_sql_clone_says_the_providers_words():
    class Call:
        def __init__(self, answer):
            self.answer = answer

        def execute(self):
            return self.answer
    cloud = cloudseed.Google(None, {"project": "p", "source_id": "s",
                                    "target_id": "t"}, "postgres")
    cloud.client = lambda: types.SimpleNamespace(
        instances=lambda: types.SimpleNamespace(
            clone=lambda **kw: Call({"name": "op"})),
        operations=lambda: types.SimpleNamespace(
            get=lambda **kw: Call({"status": "DONE", "error": {"errors": [
                {"message": "quota exceeded"}]}})))
    with pytest.raises(SystemExit) as e:
        cloud.copy("clone", {}, lambda m: None)
    assert "quota exceeded" in str(e.value)


def test_every_provider_is_a_rung_for_both_engines():
    names = {(r.name, r.engine) for r in physical.rungs() if r.cloud}
    for name in ("cloud-aws-clone", "cloud-aws-snapshot",
                 "cloud-azure-restore", "cloud-google-clone"):
        assert {(name, "postgres"), (name, "mysql")} <= names, name
    assert os.environ.get("MIGKIT_CLOUD_POLL") == "0"
