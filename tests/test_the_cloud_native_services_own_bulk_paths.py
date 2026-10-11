"""The cloud-native services' own bulk paths (backlog P0-CLOUDS, CN1),
without a server: each a rung of its engine's table ladder before the
table copier, its needs said where it cannot run, and what fails before
anything is made on the target handed to the copier.

Spanner's whole database restored from a backup of the source; a Bigtable
table restored from its backup; Firestore's and Datastore mode's export
and import; GaussDB's COPY read straight into the target's; Neptune's bulk
loader from a file of the elements; Timestream's batch load from a file of
the records. Each runs against stand-ins shaped as the SDKs answer.
"""
import json
import types

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop

PROVES = [
    ("move.bulk", ("spanner", "bigtable", "firestore", "datastore",
                   "gaussdb", "neptune", "timestream")),
]


def _hop(engine, tmp_path, src=None, dst=None, **opts):
    hop = Hop(name="bulk", engine=engine,
              source=Endpoint(host="h1", options=dict(src or {})),
              target=Endpoint(host="h2", options=dict(dst or {})),
              databases=["shop"], options=dict(opts))
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_each_service_with_a_bulk_path_moves_in_bulk():
    from migkit import capabilities, movers
    for name in ("spanner", "bigtable", "firestore", "datastore", "gaussdb",
                 "neptune", "timestream", "geminidb-mongo"):
        assert capabilities.implemented(name, "bulk-move"), name
    assert movers.supported("geminidb-mongo", "mongodump")
    assert not movers.supported("geminidb-redis", "riotx")
    m = capabilities.matrix()
    assert m["lindorm"]["bulk-move"] == capabilities.NOT_APPLICABLE
    assert m["geminidb-redis"]["bulk-move"] == capabilities.NOT_APPLICABLE


# ---- Spanner: the whole database from a backup ------------------------------

class _Op:
    def __init__(self, log, what):
        self.log, self.what = log, what

    def result(self, timeout=None):
        self.log.append(self.what)
        return None


class _Backup:
    def __init__(self, log, inst, backup_id, fails=False):
        self.log, self.name = log, f"{inst}/backups/{backup_id}"
        self.fails = fails

    def create(self):
        if self.fails:
            raise RuntimeError("backups are not supported")
        return _Op(self.log, f"made {self.name}")


class _Instance:
    def __init__(self, name, log, fails=False):
        self.name, self.log, self.fails = name, log, fails

    def backup(self, backup_id, database=None, expire_time=None,
               version_time=None):
        self.log.append(("backup", self.name, version_time is not None))
        return _Backup(self.log, self.name, backup_id, self.fails)

    def copy_backup(self, backup_id, source_backup, expire_time=None):
        self.log.append(("copy", self.name, source_backup))
        return _Backup(self.log, self.name, backup_id)


class _Database:
    def __init__(self, log, there=False):
        self.log, self.there = log, there

    def exists(self):
        return self.there

    def restore(self, backup):
        self.log.append(("restore", backup.name))
        return _Op(self.log, "restored")

    def update_ddl(self, statements):
        self.log.append(("ddl", list(statements)))
        return _Op(self.log, "ddl done")


def _spanner(monkeypatch, tmp_path, there=False, fails=False,
             instances=("p/instances/a", "p/instances/b")):
    from migkit.engines.spanner import SpannerEngine
    log = []
    eng = SpannerEngine(_hop("spanner", tmp_path))
    insts = {"src": _Instance(instances[0], log, fails),
             "dst": _Instance(instances[1], log)}
    dbs = {"src": _Database(log, True), "dst": _Database(log, there)}
    monkeypatch.setattr(eng, "_instance", lambda side: insts[side])
    monkeypatch.setattr(eng, "_database", lambda side, db: dbs[side])
    monkeypatch.setattr(eng, "_tables_named", lambda side, db: {
        "Singers": {}, "Albums": {}})
    monkeypatch.setattr(eng, "_execute", lambda side, db, sql: [["cs_all"]])
    monkeypatch.setattr(eng, "_qd", lambda side, db, name: f"`{name}`")
    copied = []
    monkeypatch.setattr(eng, "copy_by_copier",
                        lambda db, t, log, held: copied.append(t) or 1)
    return eng, log, copied


def test_a_spanner_database_new_on_the_target_is_restored_from_a_backup(
        monkeypatch, tmp_path):
    eng, log, copied = _spanner(monkeypatch, tmp_path)
    said = []
    steps = eng.native_bulk("shop", ["Albums", "Singers"], True, said.append)
    assert steps == ["shop: restored whole from a backup of the source"
                     " taken now"]
    assert log[0] == ("backup", "p/instances/a", True)
    # another instance: the backup copied there first
    assert ("copy", "p/instances/b", log[1].split(" ", 1)[1]) in log
    assert ("restore", next(e[2] for e in log if e[0] == "copy")
            .replace("p/instances/a", "p/instances/b")) in log
    # the source's change streams are its own, not the target's
    assert ("ddl", ["drop change stream `cs_all`"]) in log
    assert copied == []


def test_a_spanner_database_there_or_taken_in_part_goes_table_by_table(
        monkeypatch, tmp_path):
    eng, log, copied = _spanner(monkeypatch, tmp_path, there=True)
    assert "there already" in eng._restore_unfit("shop", ["Albums",
                                                          "Singers"])
    eng.native_bulk("shop", ["Albums", "Singers"], True, [].append)
    assert sorted(copied) == ["Albums", "Singers"]
    assert not [e for e in log if e[0] == "backup"]
    eng, log, copied = _spanner(monkeypatch, tmp_path)
    assert "part of the database" in eng._restore_unfit("shop", ["Albums"])
    eng.hop.source.options["emulator"] = "127.0.0.1:9010"
    assert "emulator" in eng._restore_unfit("shop", ["Albums", "Singers"])


def test_a_spanner_backup_that_fails_hands_the_tables_to_the_copier(
        monkeypatch, tmp_path):
    eng, log, copied = _spanner(monkeypatch, tmp_path, fails=True)
    said = []
    eng.native_bulk("shop", ["Albums", "Singers"], True, said.append)
    assert sorted(copied) == ["Albums", "Singers"]
    assert not [e for e in log if isinstance(e, tuple) and e[0] in (
        "restore", "copy")]
    assert any("could not be backed up" in s for s in said), said


# ---- Bigtable: a table from its backup ---------------------------------------

def test_a_bigtable_table_is_restored_from_a_backup_copied_across_projects(
        monkeypatch, tmp_path):
    from migkit.engines.bigtable import BigtableEngine
    hop = _hop("bigtable", tmp_path, src={"project": "p1"},
               dst={"project": "p2"})
    eng = BigtableEngine(hop)
    log = []

    class Table:
        def __init__(self, side, there=False):
            self.side, self.there = side, there

        def exists(self):
            return self.there

        def backup(self, backup_id, cluster_id=None, expire_time=None):
            log.append(("backup", self.side, cluster_id))
            return types.SimpleNamespace(
                name=f"projects/p1/instances/shop/clusters/{cluster_id}"
                     f"/backups/{backup_id}",
                create=lambda c: _Op(log, "backed up"))

        def restore(self, new_id, backup_name=None):
            log.append(("restore", self.side, new_id, backup_name))
            return _Op(log, "restored")

    class Admin:
        def copy_backup(self, parent, backup_id, source_backup, expire_time):
            log.append(("copy", parent, source_backup))
            return _Op(log, "copied")
    monkeypatch.setattr(eng, "_table", lambda side, db, t: Table(side))
    monkeypatch.setattr(eng, "_first_cluster",
                        lambda side, db: f"{side}-c1")
    monkeypatch.setattr(eng, "_described", lambda side, db, t: None)
    monkeypatch.setattr(eng, "_client", lambda side: types.SimpleNamespace(
        table_admin_client=Admin()))
    assert eng._restore_unfit("shop", "events") is None
    eng._restored("shop", "events", [].append)
    assert log[0] == ("backup", "src", "src-c1")
    copy = next(e for e in log if e[0] == "copy")
    assert copy[1] == "projects/p2/instances/shop/clusters/dst-c1"
    restore = next(e for e in log if e[0] == "restore")
    assert restore[:3] == ("restore", "dst", "events")
    assert restore[3].startswith(copy[1] + "/backups/migkit-copy-events-")
    # a table already on the target is restored onto by nothing
    monkeypatch.setattr(eng, "_table", lambda side, db, t: Table(side, True))
    assert "there already" in eng._restore_unfit("shop", "events")
    monkeypatch.setattr(eng, "_described", lambda side, db, t: ([], []))
    assert "another engine's rows" in eng._restore_unfit("shop", "events")


# ---- Firestore and Datastore mode: the export and the import ----------------

def test_firestore_collections_go_by_the_services_export_and_import(
        monkeypatch, tmp_path):
    from migkit.engines.firestore import DatastoreEngine, FirestoreEngine
    calls = []

    class Admin:
        def __init__(self, side):
            self.side = side

        def export_documents(self, request):
            calls.append(("export", self.side, request))
            return types.SimpleNamespace(result=lambda timeout=None:
                                         types.SimpleNamespace(
                                             output_uri_prefix="gs://b/x/1"))

        def import_documents(self, request):
            calls.append(("import", self.side, request))
            return types.SimpleNamespace(result=lambda timeout=None: None)
    eng = FirestoreEngine(_hop("firestore", tmp_path,
                               export_bucket="gs://b/x/"))
    monkeypatch.setattr(eng, "_admin", lambda side: Admin(side))
    monkeypatch.setattr(eng, "_db_name",
                        lambda side, db: f"projects/p/databases/{side}")
    monkeypatch.setattr(eng, "_described", lambda side, db, t: None)
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["users"])
    emptied = []
    monkeypatch.setattr(eng, "neutral_empty",
                        lambda side, db, t, where=None: emptied.append(t))
    copied = []
    monkeypatch.setattr(eng, "copy_by_copier",
                        lambda db, t, log, held: copied.append(t))
    eng.native_bulk("shop", ["users"], True, [].append)
    export, imported = calls
    assert export[1] == "src" and export[2]["collection_ids"] == ["users"]
    assert export[2]["output_uri_prefix"].startswith("gs://b/x/bulk-shop-"
                                                     "users-")
    assert imported[1:] == ("dst", {"name": "projects/p/databases/dst",
                                    "collection_ids": ["users"],
                                    "input_uri_prefix": "gs://b/x/1"})
    assert emptied == ["users"] and copied == []
    # no bucket named: the copier carries it, and says why
    eng.hop.options.pop("export_bucket")
    assert "export_bucket" in eng._export_unfit("shop", "users")
    # Datastore mode by the same admin API, held to its namespace
    ds = DatastoreEngine(_hop("datastore", tmp_path, dst={"namespace": "n1"},
                              export_bucket="gs://b/x"))
    monkeypatch.setattr(ds, "_db_name", lambda side, db: "projects/p/"
                        "databases/(default)")
    assert ds._held_to("dst", "shop", "Order") == {
        "name": "projects/p/databases/(default)",
        "collection_ids": ["Order"], "namespace_ids": ["n1"]}


# ---- GaussDB: the servers' own COPY -------------------------------------------

def test_a_gaussdb_table_is_carried_by_the_servers_own_copy(monkeypatch,
                                                            tmp_path):
    from migkit.engines.gaussdb import GaussEngine
    eng = GaussEngine(_hop("gaussdb", tmp_path))
    ran = []

    class Cur:
        def __init__(self, side):
            self.side, self.rowcount = side, -1

        def copy_expert(self, sql, f):
            ran.append((self.side, sql))
            if self.side == "src":
                f.write(b"PGCOPY\n\xff\r\n\x00" + b"rows")
            else:
                assert f.read() == b"PGCOPY\n\xff\r\n\x00" + b"rows"
                self.rowcount = 3

    class Conn:
        def __init__(self, side):
            self.side, self.cur = side, Cur(side)

        def cursor(self):
            return self.cur

        def commit(self):
            ran.append((self.side, "commit"))

        def close(self):
            pass
    monkeypatch.setattr(eng, "_connect", lambda side, db: Conn(side))
    monkeypatch.setattr(eng, "neutral_columns", lambda side, db, t: [
        ("id", "integer"), ("n", "text")])
    emptied = []
    monkeypatch.setattr(eng, "neutral_empty", lambda side, db, t, where=None:
                        emptied.append((side, t)))
    assert eng._streamed("shop", "public.t", [].append) == 3
    assert emptied == [("dst", "public.t")]
    assert ('src', 'copy "public"."t" ("id", "n") to stdout with'
                   ' (format binary)') in ran
    assert ('dst', 'copy "public"."t" ("id", "n") from stdin with'
                   ' (format binary)') in ran
    assert ("dst", "commit") in ran
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: [])
    assert "no such table" in eng._copy_unfit("shop", "public.t")


# ---- Neptune: the bulk loader -----------------------------------------------

def _neptune(tmp_path, **opts):
    from migkit.engines.neptune import NeptuneEngine
    hop = _hop("neptune", tmp_path, dst={
        "iam_role": "arn:aws:iam::<account>:role/loader"}, **opts)
    hop.target.host = "g.cluster-abc.us-east-1.neptune.amazonaws.com"
    return NeptuneEngine(hop)


def test_neptune_elements_are_written_as_the_loaders_own_file(
        monkeypatch, tmp_path):
    from migkit.engines.neptune import properties
    eng = _neptune(tmp_path, export_bucket="s3://b/p")
    rows = {"vertices": [["v1", "person", properties(
                {"name": ["Ann; B", "a,b"], "age": 31})],
                         ["v2", "person", properties({"name": 'say "hi"'})]],
            "edges": [["e1", "knows", "v1", "v2", properties(
                {"since": 2020})]],
            "quads": [["<s>", "<p>", '"o"', ""], ["<s>", "<p>", "<o>",
                                                  "<g>"]]}
    monkeypatch.setattr(eng, "_rows", lambda side, db, t: rows[t])
    fmt, text, count = eng._loader_text("graph", "vertices")
    lines = text.splitlines()
    assert (fmt, count) == ("csv", 2)
    assert lines[0] == "~id,~label,age:Long[],name:String[]"
    assert lines[1] == 'v1,person,31,"Ann\\; B;a,b"'
    assert lines[2] == 'v2,person,,"say ""hi"""'
    fmt, text, _ = eng._loader_text("graph", "edges")
    assert text.splitlines() == ["~id,~label,~from,~to,since:Long",
                                 "e1,knows,v1,v2,2020"]
    fmt, text, _ = eng._loader_text("rdf", "quads")
    assert fmt == "nquads" and text == '<s> <p> "o" .\n<s> <p> <o> <g> .\n'
    # a kind the loader's file has no type for stops it before any load
    from decimal import Decimal
    rows["vertices"] = [["v3", "x", properties({"price": Decimal("1.5")})]]
    with pytest.raises(decide.Blocked, match="decimal"):
        eng._loader_text("graph", "vertices")
    rows["edges"] = [["e2", "x", "v1", "v2", properties({"w": [1, 2]})]]
    with pytest.raises(decide.Blocked, match="many values"):
        eng._loader_text("graph", "edges")


def test_neptune_loads_the_file_and_waits_for_the_load(monkeypatch,
                                                       tmp_path):
    import boto3
    eng = _neptune(tmp_path, export_bucket="s3://b/p")
    monkeypatch.setattr(eng, "_loader_text", lambda db, t: (
        "csv", "~id,~label\nv1,a\n", 1))
    put = []
    monkeypatch.setattr(boto3, "client", lambda name, **kw:
                        types.SimpleNamespace(put_object=lambda **k:
                                              put.append(k)))
    statuses = ["LOAD_IN_PROGRESS", "LOAD_COMPLETED"]
    started = []

    class Data:
        def start_loader_job(self, **kw):
            started.append(kw)
            return {"status": "200", "payload": {"loadId": "L1"}}

        def get_loader_job_status(self, loadId):
            return {"status": "200", "payload": {"overallStatus": {
                "status": statuses.pop(0) if len(statuses) > 1
                else statuses[0], "totalRecords": 1, "parsingErrors": 0}}}
    monkeypatch.setattr(eng, "_data", lambda side: Data())
    monkeypatch.setattr("migkit.engines.neptune.time.sleep", lambda s: None)
    assert eng._loaded("graph", "vertices", [].append) == 1
    assert put[0]["Bucket"] == "b" and put[0]["Key"].startswith(
        "p/migkit-bulk-vertices-") and put[0]["Body"] == b"~id,~label\nv1,a\n"
    assert started[0]["source"] == f"s3://b/{put[0]['Key']}"
    assert started[0]["iamRoleArn"].endswith(":role/loader")
    assert started[0]["format"] == "csv" and started[0]["mode"] == "NEW"
    statuses[:] = ["LOAD_FAILED"]
    with pytest.raises(SystemExit, match="LOAD_FAILED"):
        eng._loaded("graph", "vertices", [].append)
    eng.hop.options.pop("export_bucket")
    assert "export_bucket" in eng._load_unfit("graph", "vertices")


# ---- Timestream: the batch load -----------------------------------------------

def test_timestream_records_go_by_the_services_batch_load(monkeypatch,
                                                         tmp_path):
    import datetime

    import boto3

    from migkit.engines.timestream import TimestreamEngine
    eng = TimestreamEngine(_hop("timestream", tmp_path,
                                src={"region": "us-east-1"},
                                dst={"region": "us-east-1"},
                                export_bucket="s3://b/loads"))
    cols = [("host", "varchar"), ("measure_name", "varchar"),
            ("time", "timestamp"), ("cpu", "double"), ("up", "boolean")]
    monkeypatch.setattr(eng, "neutral_columns", lambda side, db, t: cols)
    monkeypatch.setattr(eng, "_parts", lambda side, db, t, c: (
        ["host"], "measure_name", "time", [("cpu", "double"),
                                           ("up", "boolean")]))
    when = datetime.datetime(2026, 10, 1, 10, 0,
                             tzinfo=datetime.timezone.utc)
    monkeypatch.setattr(eng, "neutral_batches", lambda side, db, t, c, n: [
        [["h1", "load", when, 0.5, True], ["h2", "load", when, None, False]]])
    put = []
    monkeypatch.setattr(boto3, "client", lambda name, **kw:
                        types.SimpleNamespace(put_object=lambda **k:
                                              put.append(k)))
    made = []

    class Write:
        def create_batch_load_task(self, **kw):
            made.append(kw)
            return {"TaskId": "T1"}

        def describe_batch_load_task(self, TaskId):
            return {"BatchLoadTaskDescription": {"TaskStatus": "SUCCEEDED"}}
    monkeypatch.setattr(eng, "_client", lambda side, kind: Write())
    assert eng._batch_loaded("metrics", "cpu", [].append) == 2
    body = put[0]["Body"].decode().splitlines()
    assert body == ["host,measure_name,time,cpu,up",
                    "h1,load,1790848800000000000,0.5,true",
                    "h2,load,1790848800000000000,,false"]
    model = made[0]["DataModelConfiguration"]["DataModel"]
    assert model["TimeUnit"] == "NANOSECONDS"
    assert model["MeasureNameColumn"] == "measure_name"
    assert model["DimensionMappings"] == [{"SourceColumn": "host",
                                           "DestinationColumn": "host"}]
    assert [m["MeasureValueType"] for m in model["MultiMeasureMappings"][
        "MultiMeasureAttributeMappings"]] == ["DOUBLE", "BOOLEAN"]
    assert made[0]["DataSourceConfiguration"]["DataSourceS3Configuration"][
        "BucketName"] == "b"
    assert json.dumps(made[0]["ReportConfiguration"]).count("/report") == 1
