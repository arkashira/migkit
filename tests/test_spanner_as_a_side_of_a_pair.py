"""Google Cloud Spanner as a side of a pair (backlog P0-CLOUDS), against its
emulator (`gcr.io/cloud-spanner-emulator/emulator`), which answers on this
machine only, with no credentials.

A database of each dialect copied to another of its own, made as the
source is (its interleaved table too), and compared row by row; the
digest worked out by the server in GoogleSQL, held to the one folded here
and to a timestamp's ninth digit; the change stream followed to a fence.
What the emulator cannot say - Data Boost, the IAM a real instance
answers, version retention past an hour - is proved only on a real
instance.
"""
import base64
import ctypes
import datetime
import threading
import time
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PROVES = [
    ("move.table-copy", "spanner"),
    ("schema.create", "spanner"),
    ("tail.follows", "spanner"),
    ("verify.fence", "spanner"),
    ("cutover.drain", "spanner"),
]

pytestmark = needs_docker

EMU, PORT = "migkit-test-spanner", 16450
IMAGE = "gcr.io/cloud-spanner-emulator/emulator:latest"
PROJECT, INSTANCE = "migkit-test", "inst"
UTC = datetime.timezone.utc

GOOGLE_DDL = [
    "CREATE TABLE orders (id INT64 NOT NULL, name STRING(MAX), amount"
    " NUMERIC, at TIMESTAMP, raw BYTES(MAX), tags ARRAY<INT64>, doc JSON,"
    " ok BOOL, day DATE) PRIMARY KEY (id)",
    "CREATE TABLE lines (id INT64 NOT NULL, n INT64 NOT NULL, qty INT64)"
    " PRIMARY KEY (id, n), INTERLEAVE IN PARENT orders ON DELETE CASCADE",
    "CREATE CHANGE STREAM migkit_all FOR ALL",
]
PG_DDL = [
    "CREATE TABLE items (id bigint NOT NULL, name character varying(40),"
    " price numeric, at timestamptz, PRIMARY KEY (id))",
]


def _client():
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import spanner
    return spanner.Client(project=PROJECT,
                          credentials=AnonymousCredentials(),
                          client_options={"api_endpoint":
                                          f"127.0.0.1:{PORT}"})


@pytest.fixture(scope="module")
def emulator():
    import subprocess
    subprocess.run(["docker", "rm", "-f", "-v", EMU], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", EMU, "-p",
                    f"127.0.0.1:{PORT}:9010", IMAGE], check=True,
                   capture_output=True)
    try:
        client = _client()
        inst = client.instance(
            INSTANCE, configuration_name=f"projects/{PROJECT}/"
                                         "instanceConfigs/emulator-config",
            display_name="migkit", node_count=1)
        end = time.time() + 120
        while time.time() < end:
            try:
                if inst.exists():
                    break
                inst.create().result(60)
            except Exception:
                time.sleep(2)
        assert inst.exists()
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", EMU],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _fresh(client, name, ddl, pg=False):
    from google.cloud.spanner_admin_database_v1 import DatabaseDialect
    inst = client.instance(INSTANCE)
    for n in (name, name + "_copy"):
        d = inst.database(n)
        if d.exists():
            d.drop()
    if pg:
        d = inst.database(name, database_dialect=DatabaseDialect.POSTGRESQL)
        d.create().result(120)
        d.update_ddl(ddl).result(120)
    else:
        d = inst.database(name, ddl_statements=ddl)
        d.create().result(120)
    return d


def _ep():
    return Endpoint(host=PROJECT, options={"instance": INSTANCE,
                                           "emulator": f"127.0.0.1:{PORT}"})


def _engine(tmp_path, db):
    from migkit.engines.spanner import SpannerEngine
    hop = Hop(name="spanner", engine="spanner", source=_ep(), target=_ep(),
              databases=[db], db_map={db: db + "_copy"})
    hop.report_dir = lambda d=None: tmp_path
    return SpannerEngine(hop)


def _ts(ns_extra=0):
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds
    return DatetimeWithNanoseconds(2026, 10, 1, 12, 30, 45,
                                   nanosecond=123456789 + ns_extra,
                                   tzinfo=UTC)


@pytest.fixture
def shop(emulator):
    from google.cloud.spanner_v1 import JsonObject
    d = _fresh(emulator, "shop", GOOGLE_DDL)
    with d.batch() as b:
        b.insert("orders", columns=("id", "name", "amount", "at", "raw",
                                    "tags", "doc", "ok", "day"),
                 values=[[i, f"o{i}", Decimal(f"{i}.50"), _ts(i),
                          base64.b64encode(bytes([i, 0, 255])), [i, i + 1],
                          JsonObject({"n": i, "tag": "x"}), i % 2 == 0,
                          datetime.date(2026, 1, 1 + i)]
                         for i in range(1, 21)])
        b.insert("lines", columns=("id", "n", "qty"),
                 values=[[i, n, i * n] for i in range(1, 21)
                         for n in (1, 2)])
    return d


def _move(eng, tmp_path, db):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing(db, lambda m: None)
    for sch, t in eng.list_move_tables(db):
        eng.move_table(db, sch, t, 10, ck, lambda m: None)


def _data(eng, db):
    return {r.scope: r.status for r in eng.check_data(db)
            if r.check == "data"}


def test_a_database_is_made_as_the_source_is_and_compared_row_by_row(
        shop, emulator, tmp_path):
    eng = _engine(tmp_path, "shop")
    _move(eng, tmp_path, "shop")
    got = _data(eng, "shop")
    assert got and set(got.values()) == {"ok"}, got
    # made from the source's own definition: interleaved, change stream
    # left out
    copy = emulator.instance(INSTANCE).database("shop_copy")
    copy.reload()
    ddl = " ".join(copy.ddl_statements)
    assert "INTERLEAVE IN PARENT orders" in ddl
    assert "CHANGE STREAM" not in ddl
    params = {r.scope: r.status for r in eng.check_params("shop")}
    assert set(params.values()) == {"ok"}, params
    # one nanosecond moved on the target is a difference
    with copy.batch() as b:
        b.update("orders", columns=("id", "at"), values=[[3, _ts(4)]])
    got = _data(_engine(tmp_path, "shop"), "shop")
    assert "diff" in got.values(), got


def test_the_server_works_the_digest_out_as_it_is_folded_here(shop,
                                                              tmp_path):
    eng = _engine(tmp_path, "shop")
    cols = [("id", "integer"), ("name", "text"), ("at", "timestamp"),
            ("raw", "bytes"), ("ok", "boolean"), ("day", "date")]
    server = eng.neutral_digest("src", "shop", "orders", cols)
    assert eng.folded_last is False, "the GoogleSQL digest was not proved"
    here = eng._fold_digest("src", "shop", "orders", cols)
    assert server == here and server[0] == 20
    with shop.batch() as b:
        b.update("orders", columns=("id", "at"), values=[[5, _ts(6)]])
    moved = eng.neutral_digest("src", "shop", "orders", cols)
    assert moved != server and eng.folded_last is False
    # JSON is folded here on every engine: so is a table holding it
    with_json = eng.neutral_digest("src", "shop", "orders",
                                   cols + [("doc", "json")])
    assert eng.folded_last is True and with_json[0] == 20


def test_a_postgresql_dialect_database_is_copied_and_folded_here(
        emulator, tmp_path):
    d = _fresh(emulator, "pgshop", PG_DDL, pg=True)
    with d.batch() as b:
        b.insert("items", columns=("id", "name", "price", "at"),
                 values=[[i, f"i{i}", Decimal(f"{i}.25"), _ts(i)]
                         for i in range(1, 11)])
    eng = _engine(tmp_path, "pgshop")
    assert eng._pg("src", "pgshop")
    _move(eng, tmp_path, "pgshop")
    got = _data(eng, "pgshop")
    assert got and set(got.values()) == {"ok"}, got
    assert eng._pg("dst", "pgshop")
    eng.neutral_digest("src", "pgshop", "items", [("id", "integer")])
    assert eng.folded_last is True


def test_the_change_stream_is_followed_to_a_fence(shop, emulator, tmp_path):
    eng = _engine(tmp_path, "shop")
    token = tmp_path / "tail-token.json"
    eng.tail_start("shop", token)
    _move(eng, tmp_path, "shop")
    ended = {}

    def go():
        try:
            eng.tail_apply("shop", True, token, lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=go, daemon=True)
    thread.start()
    try:
        with shop.batch() as b:
            b.insert("orders", columns=("id", "name", "at"),
                     values=[[100, "new", _ts(7)]])
            b.update("orders", columns=("id", "name"), values=[[2, "changed"]])
            b.delete("lines", _keyset([[4, 1]]))
        at = eng.src_lsn("shop")
        assert at is not None
        assert eng.fence_wait("shop", at, timeout=120) is True
        got = _data(_engine(tmp_path, "shop"), "shop")
        assert set(got.values()) == {"ok"}, got
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended


def _keyset(keys):
    from google.cloud.spanner_v1 import KeySet
    return KeySet(keys=keys)


def test_a_database_without_a_stream_is_refused_with_the_statement(
        emulator, tmp_path):
    _fresh(emulator, "nostream", GOOGLE_DDL[:2])
    eng = _engine(tmp_path, "nostream")
    with pytest.raises(SystemExit, match="create change stream"):
        eng.change_point("src", "nostream")
