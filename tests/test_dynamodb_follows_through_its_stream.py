"""DynamoDB follows through its stream: a table's writes read from its
own change log, in order per key, applied to the target by key - and the
same reading fences, confirms and verifies only what changed. Its tables'
settings are compared, and a snapshot is taken before a repair.

Measured before: a DynamoDB hop could copy and compare, and nothing more:
no follow, no fence, no verify of what changed, no settings compared
(a table on the target with no time to live keeps every item the source
expires), and no snapshot. Against DynamoDB Local, which keeps streams.
"""
import ctypes
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

DDB, PORT = "migkit-test-ddbstream", 15929


@pytest.fixture(scope="module")
def dynamo():
    import boto3
    subprocess.run(["docker", "rm", "-f", "-v", DDB], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", DDB, "-p",
                    f"{PORT}:8000", "amazon/dynamodb-local:latest", "-jar",
                    "DynamoDBLocal.jar", "-inMemory", "-sharedDb"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 60
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        client = boto3.client("dynamodb",
                              endpoint_url=f"http://127.0.0.1:{PORT}",
                              region_name="us-east-1",
                              aws_access_key_id="local",
                              aws_secret_access_key="local")
        for _ in range(30):
            try:
                client.list_tables()
                break
            except Exception:
                time.sleep(1)
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", DDB], capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _table(client, name, stream=True):
    kw = {"StreamSpecification": {"StreamEnabled": True,
                                  "StreamViewType": "NEW_AND_OLD_IMAGES"}} \
        if stream else {}
    client.create_table(
        TableName=name, BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        **kw)


def _put(client, name, i, v):
    client.put_item(TableName=name, Item={"id": {"N": str(i)},
                                          "v": {"S": v}})


def _items(client, name):
    got, start = {}, None
    while True:
        page = client.scan(TableName=name, **({"ExclusiveStartKey": start}
                                              if start else {}))
        got.update({int(i["id"]["N"]): i["v"]["S"] for i in page["Items"]})
        start = page.get("LastEvaluatedKey")
        if not start:
            return got


def _engine(tmp_path, db):
    from migkit.engines.dynamodb import DynamoDBEngine
    ep = Endpoint(user="local", password="local",
                  options={"endpoint_url": f"http://127.0.0.1:{PORT}"})
    hop = Hop(name="ddbs", engine="dynamodb", source=ep, target=ep,
              databases=[db], db_map={db: f"{db}copy"})
    hop.report_dir = lambda d=None: tmp_path
    return DynamoDBEngine(hop)


def _tail(eng, db, tmp_path):
    ended = {}

    def run():
        try:
            eng.tail_apply(db, True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


def test_a_copy_and_its_stream_end_equal(dynamo, tmp_path):
    from migkit.cli import _Checkpoint
    _table(dynamo, "shop.orders")
    for i in range(60):
        _put(dynamo, "shop.orders", i, f"v{i}")
    eng = _engine(tmp_path, "shop")
    token = tmp_path / "tail-token.json"
    # the position first, then the copy, then what changed during and
    # after it replayed by key on top of it
    eng.tail_start("shop", token)
    _put(dynamo, "shop.orders", 5, "changed while copying")
    eng.move_table("shop", "", "orders", 1000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    thread, ended = _tail(eng, "shop", tmp_path)
    try:
        _put(dynamo, "shop.orders", 7, "changed after")
        _put(dynamo, "shop.orders", 1000, "new")
        dynamo.delete_item(TableName="shop.orders", Key={"id": {"N": "3"}})
        time.sleep(1)
        at = eng.src_lsn("shop")
        assert at is not None
        assert eng.fence_wait("shop", at, timeout=60) is True
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    assert _items(dynamo, "shopcopy.orders") == _items(dynamo, "shop.orders")
    got = _items(dynamo, "shopcopy.orders")
    assert got[7] == "changed after" and 3 not in got and got[1000] == "new"


def test_a_table_with_its_stream_off_is_refused_not_switched_on(dynamo,
                                                                 tmp_path):
    _table(dynamo, "quiet.t", stream=False)
    eng = _engine(tmp_path, "quiet")
    with pytest.raises(SystemExit, match="has no stream on"):
        eng.tail_start("quiet", tmp_path / "tail-token.json")
    got = dynamo.describe_table(TableName="quiet.t")["Table"]
    assert not (got.get("StreamSpecification") or {}).get("StreamEnabled")


def test_only_what_the_stream_names_is_verified(dynamo, tmp_path):
    from migkit.cli import _Checkpoint
    _table(dynamo, "dv.t")
    for i in range(20):
        _put(dynamo, "dv.t", i, "x")
    eng = _engine(tmp_path, "dv")
    eng.move_table("dv", "", "t", 1000, _Checkpoint(tmp_path / "m.json"),
                   [].append)
    first = eng.delta_verify("dv")
    assert first[0].status == "ok" and "baseline" in first[0].detail
    _put(dynamo, "dv.t", 4, "moved on")
    got = eng.delta_verify("dv")
    assert "diff" in [r.status for r in got], [r.detail for r in got]
    _put(dynamo, "dvcopy.t", 4, "moved on")
    got = eng.delta_verify("dv")
    assert all(r.status == "ok" for r in got), [r.detail for r in got]


def test_a_table_that_expires_items_differently_is_named(dynamo, tmp_path):
    _table(dynamo, "ttl.t")
    _table(dynamo, "ttlcopy.t")
    dynamo.update_time_to_live(TableName="ttl.t", TimeToLiveSpecification={
        "Enabled": True, "AttributeName": "expires"})
    got = _engine(tmp_path, "ttl").check_params("ttl")
    assert got[0].status == "diff", got[0].detail
    assert "t.ttl src=expires dst=off" in got[0].detail, got[0].detail
    saved = json.loads((tmp_path / "params.json").read_text())
    assert saved["t.pitr"]["src"].startswith("not answered"), saved


def test_a_snapshot_says_what_it_could_keep(dynamo, tmp_path):
    _table(dynamo, "snap.t")
    _table(dynamo, "snapcopy.t")
    for i in range(3):
        _put(dynamo, "snapcopy.t", i, "x")
    point = tmp_path / "point"
    point.mkdir()
    _engine(tmp_path, "snap").snapshot_state("snap", point)
    got = json.loads((point / "dst-tables.json").read_text())
    assert got["t"]["items"] == 3
    # DynamoDB Local takes no backups: said, not pretended
    assert "backup" not in got["t"] and got["t"]["no_backup"], got


NESTED = {"id": {"N": "1"},
          "profile": {"M": {"name": {"S": "Ann"}, "tags": {"L": [
              {"S": "a"}, {"N": "2"}, {"BOOL": True}]},
              "raw": {"B": b"\x00\xff"}}},
          "colours": {"SS": ["red", "blue"]},
          "sizes": {"NS": ["10", "2.5"]},
          "blobs": {"BS": [b"\x01", b"\x02"]},
          "gone": {"NULL": True}}


def test_maps_lists_and_sets_cross_whole(dynamo, tmp_path):
    """A map, a list or a set has no neutral class, and was written as the
    text of a Python dict: between two DynamoDB tables each crosses as the
    attribute it was, and a value changed deep in a map is a difference."""
    from migkit.cli import _Checkpoint
    _table(dynamo, "nest.t")
    dynamo.put_item(TableName="nest.t", Item=NESTED)
    eng = _engine(tmp_path, "nest")
    eng.move_table("nest", "", "t", 1000, _Checkpoint(tmp_path / "m.json"),
                   [].append)
    got = dynamo.get_item(TableName="nestcopy.t",
                          Key={"id": {"N": "1"}})["Item"]
    want = dict(NESTED)
    want.pop("gone")
    got.pop("gone", None)
    assert got["profile"] == want["profile"], got
    assert sorted(got["colours"]["SS"]) == ["blue", "red"]
    assert sorted(got["sizes"]["NS"], key=float) == ["2.5", "10"]
    assert sorted(bytes(b) for b in got["blobs"]["BS"]) == [b"\x01", b"\x02"]
    ok = [r for r in eng.check_data("nest") if r.check == "data"]
    assert ok and all(r.status == "ok" for r in ok), [r.detail for r in ok]
    dynamo.update_item(TableName="nestcopy.t", Key={"id": {"N": "1"}},
                       UpdateExpression="SET profile.tags[1] = :v",
                       ExpressionAttributeValues={":v": {"N": "3"}})
    bad = [r for r in eng.check_data("nest") if r.check == "data"]
    assert "diff" in [r.status for r in bad], [r.detail for r in bad]


def test_a_whole_move_copies_items_as_they_are(dynamo, tmp_path,
                                               monkeypatch):
    """The move's own path for DynamoDB: a parallel scan written back as
    the items are, the target table made with the source's key and
    secondary indexes, which the table copier does not make. Measured
    against DynamoDB Local, 5,000 items: 2.4s against the table copier's
    2.3s - one local process is the limit there; a parallel scan is how
    the service itself says a large table is read faster."""
    import migkit.config as cfg
    from click.testing import CliRunner

    from migkit import cli
    dynamo.create_table(
        TableName="bulk.t", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"},
                              {"AttributeName": "kind", "AttributeType": "S"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "by_kind",
            "KeySchema": [{"AttributeName": "kind", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}}])
    for i in range(0, 5000, 25):
        dynamo.batch_write_item(RequestItems={"bulk.t": [
            {"PutRequest": {"Item": {**NESTED, "id": {"N": str(j)},
                                     "kind": {"S": f"k{j % 4}"}}}}
            for j in range(i, i + 25)]})
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  bulk:\n    engine: dynamodb\n"
        f"    source: {{host: 'http://127.0.0.1:{PORT}', user: local,"
        " password: local}\n"
        f"    target: {{host: 'http://127.0.0.1:{PORT}', user: local,"
        " password: local}\n"
        "    databases: [bulk]\n    db_map: {bulk: bulkcopy}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    got = CliRunner().invoke(cli.main, ["move", "bulk", "--mode", "full",
                                        "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    assert "t: 5,000 items copied" in said, said
    d = dynamo.describe_table(TableName="bulkcopy.t")["Table"]
    assert d["ItemCount"] == 5000 or dynamo.scan(
        TableName="bulkcopy.t", Select="COUNT")["Count"] == 5000
    assert [i["IndexName"] for i in d.get("GlobalSecondaryIndexes", [])] \
        == ["by_kind"]
    one = dynamo.get_item(TableName="bulkcopy.t",
                          Key={"id": {"N": "7"}})["Item"]
    assert one["profile"] == NESTED["profile"]
    got = CliRunner().invoke(cli.main, ["check", "bulk"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said


def test_a_dynamodb_table_streams_into_postgresql(dynamo, pg_pair,
                                                  tmp_path):
    """The stream is a change log like any other: a pair with DynamoDB on
    the source side follows it into PostgreSQL through the same tail."""
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    from tests.conftest import psql
    _table(dynamo, "feed.events")
    for i in range(30):
        _put(dynamo, "feed.events", i, f"e{i}")
    psql(pg_pair["dst"], "drop database if exists feed")
    assert psql(pg_pair["dst"], "create database feed").returncode == 0
    hop = Hop(name="ddbpg", engine="hetero",
              options={"source_engine": "dynamodb",
                       "target_engine": "postgres"},
              source=Endpoint(user="local", password="local", options={
                  "endpoint_url": f"http://127.0.0.1:{PORT}"}),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["feed"])
    hop.report_dir = lambda d=None: tmp_path
    eng = HeteroEngine(hop)
    token = tmp_path / "tail-token.json"
    eng.tail_start("feed", token)
    eng.create_missing("feed", [].append)
    for sch, t in eng.list_move_tables("feed"):
        eng.move_table("feed", sch, t, 1000,
                       _Checkpoint(tmp_path / "move.json"), [].append)
    thread, ended = _tail(eng, "feed", tmp_path)
    try:
        _put(dynamo, "feed.events", 3, "edited")
        dynamo.delete_item(TableName="feed.events", Key={"id": {"N": "4"}})
        _put(dynamo, "feed.events", 99, "late")
        time.sleep(1)
        at = eng.src_lsn("feed")
        assert eng.fence_wait("feed", at, timeout=60) is True
    finally:
        _stop(thread)
    got = psql(pg_pair["dst"], "select count(*), string_agg(id || '=' || v,"
                               " ',' order by id) filter (where id in (3, 4,"
                               " 99)) from events", db="feed").stdout.strip()
    assert got == "30|3=edited,99=late", got
