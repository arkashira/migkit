"""Firestore as a side of a pair (backlog P0-CLOUDS), in both its modes,
against the Cloud SDK's emulator (`gcloud emulators firestore`), on this
machine only; the two sides are two projects of one emulator.

Native mode: a collection group copied document for document, each field
keeping its kind - a reference a reference, an integer inside a map never
a double - and compared; a map's integer made a double on the target is a
difference. Followed by a listener begun before the copy, to a fence.
Datastore mode: a kind copied entity for entity, keys with their
ancestors, and compared.
"""
import ctypes
import datetime
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"
NATIVE, NATIVE_PORT = "migkit-test-firestore", 16452
DSMODE, DSMODE_PORT = "migkit-test-datastore", 16453
UTC = datetime.timezone.utc


def _start(name, port, *extra):
    import subprocess
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                    f"127.0.0.1:{port}:8080", IMAGE, "gcloud", "emulators",
                    "firestore", "start", "--host-port=0.0.0.0:8080",
                    *extra], check=True, capture_output=True)


def _stop(name):
    import subprocess
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _ep(project, port, **opts):
    return Endpoint(host=project, options={"emulator": f"127.0.0.1:{port}",
                                           **opts})


@pytest.fixture(scope="module")
def native():
    from google.cloud import firestore

    from migkit.engines import gcp
    _start(NATIVE, NATIVE_PORT)
    try:
        client = gcp.client(firestore.Client, "FIRESTORE_EMULATOR_HOST",
                            _ep("src-proj", NATIVE_PORT), project="src-proj")
        end = time.time() + 180
        ready = False
        while time.time() < end and not ready:
            try:
                list(client.collections())
                ready = True
            except Exception:
                time.sleep(2)
        assert ready
        yield client
    finally:
        _stop(NATIVE)


@pytest.fixture(scope="module")
def dsmode():
    from google.cloud import datastore

    from migkit.engines import gcp
    _start(DSMODE, DSMODE_PORT, "--database-mode=datastore-mode")
    try:
        client = gcp.client(datastore.Client, "DATASTORE_EMULATOR_HOST",
                            _ep("src-proj", DSMODE_PORT), project="src-proj")
        end = time.time() + 180
        ready = False
        while time.time() < end and not ready:
            try:
                list(client.query(kind="__kind__").fetch())
                ready = True
            except Exception:
                time.sleep(2)
        assert ready
        yield client
    finally:
        _stop(DSMODE)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _engine(tmp_path, cls, port, **opts):
    hop = Hop(name="fs", engine=cls.CANON_ENGINE,
              source=_ep("src-proj", port, **opts),
              target=_ep("dst-proj", port, **opts),
              databases=["(default)"])
    hop.report_dir = lambda db=None: tmp_path
    return cls(hop)


def _wipe(eng):
    for side in ("src", "dst"):
        client = eng._client(side, "(default)")
        for c in client.collections():
            client.recursive_delete(c)


@pytest.fixture
def orders(native, tmp_path):
    from google.cloud.firestore_v1 import GeoPoint

    from migkit.engines.firestore import FirestoreEngine
    eng = _engine(tmp_path, FirestoreEngine, NATIVE_PORT,
                  collection_groups=["lines"])
    _wipe(eng)
    client = eng._client("src", "(default)")
    for i in range(1, 11):
        doc = client.collection("orders").document(f"o{i}")
        doc.set({"n": i, "price": i + 0.25, "whole": float(i),
                 "at": datetime.datetime(2026, 1, 1, 0, 0, i, 123456,
                                         tzinfo=UTC),
                 "raw": bytes([i, 0]), "owner": client.document(
                     f"people/p{i % 3}"),
                 "where": GeoPoint(1.0 + i, 2.0),
                 "meta": {"k": 3, "at": datetime.datetime(2026, 2, 1,
                                                          tzinfo=UTC)}})
        doc.collection("lines").document("l1").set({"qty": i})
    return eng


def _move(eng, tmp_path, db="(default)"):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing(db, lambda m: None)
    for sch, t in eng.list_move_tables(db):
        eng.move_table(db, sch, t, 4, ck, lambda m: None)


def _data(eng, db="(default)"):
    return {r.scope: r.status for r in eng.check_data(db)
            if r.check == "data"}


def test_a_group_is_copied_each_field_keeping_its_kind(orders, tmp_path):
    from google.cloud.firestore_v1 import DocumentReference
    _move(orders, tmp_path)
    got = _data(orders)
    assert got and set(got.values()) == {"ok"}, got
    assert any("lines" in s for s in got), got
    dst = orders._client("dst", "(default)")
    copy = dst.document("orders/o4").get().to_dict()
    assert isinstance(copy["owner"], DocumentReference)
    assert copy["owner"].path == "people/p1"
    assert isinstance(copy["n"], int) and isinstance(copy["whole"], float)
    assert copy["meta"]["k"] == 3 and isinstance(copy["meta"]["k"], int)
    schema = {r.scope: r.status for r in orders.check_schema("(default)")
              if r.check == "schema"}
    assert "diff" not in schema.values(), schema
    # an integer inside a map made a double: the same number, another kind
    dst.document("orders/o4").update({"meta.k": 3.0})
    got = _data(orders)
    assert "diff" in got.values(), got


def test_a_listener_begun_before_the_copy_follows_to_a_fence(orders,
                                                             tmp_path):
    token = tmp_path / "tail-token.json"
    orders.tail_start("(default)", token)
    _move(orders, tmp_path)
    ended = {}

    def go():
        try:
            orders.tail_apply("(default)", True, token, lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=go, daemon=True)
    thread.start()
    try:
        src = orders._client("src", "(default)")
        src.document("orders/o2").update({"n": 200})
        src.document("orders/o3").delete()
        src.document("orders/o99").set({"n": 99})
        time.sleep(2)
        at = orders.src_lsn("(default)")
        assert orders.fence_wait("(default)", at, timeout=120) is True
        got = _data(orders)
        assert set(got.values()) == {"ok"}, got
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended


def test_a_kind_is_copied_entity_for_entity_with_its_ancestors(dsmode,
                                                               tmp_path):
    from google.cloud import datastore

    from migkit.engines.firestore import DatastoreEngine
    eng = _engine(tmp_path, DatastoreEngine, DSMODE_PORT)
    for side in ("src", "dst"):
        client = eng._client(side, "(default)")
        q = client.query(kind="Order")
        q.keys_only()
        client.delete_multi([e.key for e in q.fetch()])
    src = eng._client("src", "(default)")
    parent = src.key("Customer", "c1")
    ents = []
    for i in range(1, 9):
        e = datastore.Entity(key=src.key("Order", i, parent=parent),
                             exclude_from_indexes=("note",))
        e.update({"n": i, "note": "x" * 2000, "when": datetime.datetime(
            2026, 3, 1, i, tzinfo=UTC), "tags": ["a", str(i)]})
        ents.append(e)
    src.put_multi(ents)
    _move(eng, tmp_path)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    dst = eng._client("dst", "(default)")
    one = dst.get(dst.key("Customer", "c1", "Order", 3))
    assert one is not None and one["n"] == 3
    one["n"] = 30
    dst.put(one)
    got = _data(eng)
    assert "diff" in got.values(), got
