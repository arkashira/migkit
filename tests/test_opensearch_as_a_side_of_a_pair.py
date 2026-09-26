"""OpenSearch as one side of a pair (backlog 34).

PostgreSQL moves into OpenSearch. A document's id is the row's key, so a
batch written again replaces itself instead of adding documents. A
decimal is kept as its text in `_source`, and a JSON number would have
kept neither its scale nor its digits. A changed document is a
difference. Then an index OpenSearch made itself, with no migkit
description, is read by its mapping and keyed by `_id`.
"""
import json
import subprocess
import time
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

OS, OS_PORT = "migkit-test-opensearch", 15836


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _http(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{OS_PORT}{path}", method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


@pytest.fixture(scope="module")
def opensearch():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", OS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", OS, "-p",
                    f"{OS_PORT}:9200", "-e", "discovery.type=single-node",
                    "-e", "DISABLE_SECURITY_PLUGIN=true", "-e",
                    "DISABLE_INSTALL_DEMO_CONFIG=true", "-e",
                    "OPENSEARCH_JAVA_OPTS=-Xms256m -Xmx256m",
                    "opensearchproject/opensearch:2.19.1"], check=True,
                   capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            try:
                if _http("GET", "/_cluster/health").get("status") in (
                        "green", "yellow"):
                    break
            except Exception:
                pass
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", OS], capture_output=True)


@pytest.fixture(scope="module")
def source(pg_pair):
    port = pg_pair["src"]
    psql(port, "drop database if exists ossrc")
    assert psql(port, "create database ossrc").returncode == 0
    made = psql(port, """
        create table products (sku text primary key,
          price numeric(20,4), stock int, live boolean,
          added timestamp(6), launch date, blurb text, img bytea,
          score double precision);
        insert into products
          select 'sku-' || g, g * 1.2345, g, g % 2 = 0,
                 timestamp '2024-02-29 00:00:00' + g * interval '1 second',
                 date '2020-01-01' + g, 'about ' || g,
                 decode(lpad(to_hex(g), 4, '0'), 'hex'), g / 9.0
            from generate_series(1, 1200) g;
        insert into products values ('big', 1234567890123456.7890, null,
          null, null, null, null, null, null)""", db="ossrc")
    assert made.returncode == 0, made.stderr
    return port


def _pair(src, dst, src_ep, dst_ep, tmp_path, **extra):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="os", engine="hetero",
              options={"source_engine": src, "target_engine": dst},
              source=src_ep, target=dst_ep, databases=["ossrc"], **extra)
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _os():
    return Endpoint(host="127.0.0.1", port=OS_PORT)


def _data(eng, db="ossrc"):
    return {r.scope: r for r in eng.check_data(db) if r.check == "data"}


def test_postgresql_into_opensearch(opensearch, source, tmp_path):
    from migkit.cli import _Checkpoint
    pg = Endpoint(host="127.0.0.1", port=source, user="postgres",
                  password="test")
    eng = _pair("postgres", "opensearch", pg, _os(), tmp_path)
    ck = _Checkpoint(tmp_path / "move.json")
    said = []
    eng.create_missing("ossrc", said.append)
    for sch, t in eng.list_move_tables("ossrc"):
        eng.move_table("ossrc", sch, t, 400, ck, said.append)
    got = _data(eng)
    assert [r.status for r in got.values()] == ["ok"], \
        [r.__dict__ for r in got.values()]
    doc = _http("GET", "/ossrc.products/_doc/"
                + urllib.request.quote("8:sku-1000", safe=""))
    # 1234.5 would be a JSON number: the text keeps the scale, and the
    # biggest value keeps every digit a double would lose
    assert doc["_source"]["price"] == "1234.5000", doc
    big = _http("POST", "/ossrc.products/_search",
                {"query": {"term": {"_id": "3:big"}}})["hits"]["hits"]
    assert big[0]["_source"]["price"] == "1234567890123456.7890", big
    ck["ossrc.products"] = {}
    ck.save()
    eng.move_table("ossrc", "public", "products", 400, ck, said.append)
    _http("POST", "/ossrc.products/_refresh")
    assert _http("GET", "/ossrc.products/_count")["count"] == 1201
    _http("POST", "/ossrc.products/_update/"
          + urllib.request.quote("7:sku-100", safe="")
          + "?refresh=true", {"doc": {"stock": 1}})
    assert _data(eng)["ossrc.products"].status == "diff"


def test_an_index_opensearch_made_is_read_by_its_mapping(opensearch,
                                                         tmp_path):
    from migkit.engines.opensearch import OpenSearchEngine
    _http("PUT", "/native.logs", {"mappings": {"properties": {
        "level": {"type": "keyword"}, "took": {"type": "long"},
        "at": {"type": "date"}, "tags": {"type": "object"}}}})
    for i in range(5):
        _http("PUT", f"/native.logs/_doc/l{i}?refresh=true",
              {"level": "info", "took": i})
    ep = _os()
    eng = OpenSearchEngine(Hop(name="n", engine="opensearch", source=ep,
                               target=ep, databases=["native"]))
    cols = dict(eng.neutral_columns("src", "native", "logs"))
    assert cols == {"_id": "text", "at": "timestamp", "level": "text",
                    "took": "integer", "tags": "object"}, cols
    assert eng.neutral_key("src", "native", "logs") == ["_id"]
    got = eng.neutral_rows_by_key("src", "native", "logs",
                                  [("_id", "text"), ("took", "integer")],
                                  ["_id"], [("l3",), ("l9",)])
    assert list(got.values()) == [["l3", 3]], got
