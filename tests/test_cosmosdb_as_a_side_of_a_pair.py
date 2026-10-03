"""Azure Cosmos DB for NoSQL as a side of a pair (backlog ER5), against
its Linux emulator (`azure-cosmos-emulator:vnext-preview`, x64 and
arm64), which answers on this machine only, over HTTP, with the key every
emulator has.

A container is read feed range by feed range, a key holding `id` kept as
Cosmos DB keeps one (so a container copied comes back partitioned and
identified as it was), compared item by item. Its change feed is followed
in latest-version mode here - the emulator may not serve all versions and
deletes - and that mode's loss is proved as well as its reach: a delete
is not followed, and check finds it.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.fence", "cosmosdb"),
    ("tail.follows", "cosmosdb"),
    ("schema.create", "cosmosdb"),
    ("move.table-copy", "cosmosdb"),
    ("cutover.drain", "cosmosdb"),
]

pytestmark = needs_docker

EMU, PORT = "migkit-test-cosmos", 16345
IMAGE = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"
#: the key every Cosmos DB emulator takes, published with it
EMULATOR_KEY = ("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mG"
                "GyPMbIZnqyMsEcaGQy67XIw/Jw==")
URL = f"http://127.0.0.1:{PORT}/"


@pytest.fixture(scope="module")
def emulator():
    subprocess.run(["docker", "rm", "-f", "-v", EMU], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", EMU, "-p",
                    f"127.0.0.1:{PORT}:{PORT}", IMAGE, "--port", str(PORT),
                    "--protocol", "http"], check=True, capture_output=True)
    try:
        from azure.cosmos import CosmosClient
        end = time.time() + 180
        client = None
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    try:
                        client = CosmosClient(URL, credential=EMULATOR_KEY)
                        list(client.list_databases())
                        break
                    except Exception:
                        pass
            time.sleep(2)
        assert client is not None
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", EMU],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _ep(**opts):
    return Endpoint(host="127.0.0.1", port=PORT, user="",
                    password=EMULATOR_KEY, options={"url": URL, **opts})


def _engine(tmp_path, **opts):
    from migkit.engines.cosmosdb import CosmosEngine
    hop = Hop(name="cosmos", engine="cosmosdb", source=_ep(**opts),
              target=_ep(), databases=["csrc"], db_map={"csrc": "cdst"})
    hop.report_dir = lambda db=None: tmp_path
    return CosmosEngine(hop)


@pytest.fixture
def shop(emulator):
    from azure.cosmos import PartitionKey
    for name in ("csrc", "cdst"):
        try:
            emulator.delete_database(name)
        except Exception:
            pass
    db = emulator.create_database("csrc")
    emulator.create_database("cdst")
    c = db.create_container("orders", PartitionKey(path="/region"))
    for i in range(30):
        c.upsert_item({"id": f"o{i}", "region": ["eu", "us"][i % 2],
                       "qty": i, "price": i * 1.25, "paid": i % 3 == 0,
                       "lines": [{"sku": "a", "n": i}], "note": None})
    return c


def _move(eng, tmp_path):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing("csrc", lambda m: None)
    for sch, t in eng.list_move_tables("csrc"):
        eng.move_table("csrc", sch, t, 10, ck, lambda m: None)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("csrc")
            if r.check == "data"}


def test_a_container_copied_as_it_was_and_compared_item_by_item(
        shop, emulator, tmp_path):
    eng = _engine(tmp_path)
    _move(eng, tmp_path)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    made = emulator.get_database_client("cdst").get_container_client(
        "orders").read()
    # the key's `id` the item's id, the rest of the key its partition key
    assert made["partitionKey"]["paths"] == ["/region"]
    params = {r.scope: r.status for r in eng.check_params("csrc")}
    assert set(params.values()) == {"ok"}, params
    emulator.get_database_client("cdst").get_container_client(
        "orders").upsert_item({"id": "o4", "region": "eu", "qty": 44,
                               "price": 5.0, "paid": False,
                               "lines": [{"sku": "a", "n": 4}],
                               "note": None})
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_a_container_is_digested_one_feed_range_at_a_time(shop, tmp_path):
    eng = _engine(tmp_path)
    cols = [(n, c) for n, c in eng._classes("src", "csrc", "orders").items()]
    whole = eng.neutral_digest("src", "csrc", "orders", cols)
    assert whole[0] == 30
    shop.upsert_item({"id": "o1", "region": "us", "qty": 1, "price": 1.26,
                      "paid": False, "lines": [{"sku": "a", "n": 1}],
                      "note": None})
    assert eng.neutral_digest("src", "csrc", "orders", cols) != whole


def test_the_latest_version_feed_follows_and_its_loss_is_found(shop,
                                                               tmp_path):
    eng = _engine(tmp_path, change_feed="latest")
    token = tmp_path / "tail-token.json"
    eng.tail_start("csrc", token)
    _move(eng, tmp_path)
    ended = {}

    def go():
        try:
            eng.tail_apply("csrc", True, token, lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=go, daemon=True)
    thread.start()
    try:
        shop.upsert_item({"id": "o100", "region": "eu", "qty": 100,
                          "price": 1.0, "paid": True, "lines": [],
                          "note": "new"})
        shop.upsert_item({"id": "o2", "region": "eu", "qty": -2,
                          "price": 2.5, "paid": False, "lines": [],
                          "note": "changed"})
        time.sleep(2)
        at = eng.src_lsn("csrc")
        assert at is not None
        assert eng.fence_wait("csrc", at, timeout=90) is True
        got = _data(_engine(tmp_path))
        assert set(got.values()) == {"ok"}, got
        # a delete: the latest-version feed carries none
        shop.delete_item("o3", partition_key="us")
        at = eng.src_lsn("csrc")
        assert eng.fence_wait("csrc", at, timeout=60) is True
        got = _data(_engine(tmp_path))
        assert "diff" in got.values(), got
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended


def test_a_follow_without_deletes_is_refused_unless_the_hop_says_so(
        shop, tmp_path):
    eng = _engine(tmp_path)
    container = eng._container("src", "csrc", "orders")
    try:
        mode = eng._mode("src", "csrc", container)
    except SystemExit as e:
        # the emulator without the all-versions-and-deletes feed: refused,
        # with both ways out named
        assert "change_feed: latest" in str(e) and "continuous" in str(e)
    else:
        assert mode == "AllVersionsAndDeletes"
    assert _engine(tmp_path, change_feed="latest")._mode(
        "src", "csrc", container) == "LatestVersion"
