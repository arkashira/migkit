"""A managed brand is told from its open-source engine by what its server
says - a documented sign of its own - with the address and the provider's
API weighed beside it, and an absence never the sign. Where nothing tells
them apart, every brand-dependent step refuses to act on a brand and says
the next step: name the service in the hop.

The managed halves run on fakes answering the documented signs (no
sandbox runs DocumentDB, MSK or Redis Enterprise: `variants.UNPROVED_ON`);
the open-source halves run on the sandbox's own servers."""
import pathlib
import tempfile

import pytest

from migkit import decide, variants
from migkit.config import Endpoint, Hop



def _hop(engine, host="10.0.0.5", port=1, **src_options):
    hop = Hop(name="b", engine=engine,
              source=Endpoint(host=host, port=port, user="", password="",
                              options=src_options),
              target=Endpoint(host=host, port=port, user="", password=""),
              db_map={"0": "0"})
    hop.report_dir = lambda db=None: pathlib.Path(tempfile.mkdtemp())
    return hop


# ---- the documented signs, as recorded answers ------------------------------

DOCDB = {"version": "5.0.0", "ok": 1, "list_change_streams": "[]",
         "_asked": "True"}
MSK = {"cluster_id": "kxS3MskClusterId", "_asked": "True",
       "sasl_mechanisms": "AWS_MSK_IAM,OAUTHBEARER"}
ENTERPRISE = {"redis_version": "7.2.4", "_asked": "True",
              "executable": "/opt/redislabs/bin/redis-server-7.2",
              "config_file": "/opt/redislabs/config/redis.conf"}


@pytest.mark.parametrize("family,raw,brand", [
    ("mongodb", DOCDB, "documentdb"), ("kafka", MSK, "msk"),
    ("redis", ENTERPRISE, "redis-enterprise")])
def test_a_documented_sign_names_the_managed_brand(family, raw, brand):
    got = variants.identify(family, raw)
    assert got.name == brand and got.told
    # the server's sign alone: likely; with its address agreeing: sure
    assert got.confidence == "likely"
    at = {"documentdb": "documentdb", "msk": "msk",
          "redis-enterprise": "redis-enterprise"}[brand]
    sure = variants.identify(family, {**raw, "_endpoint": at})
    assert sure.confidence == "sure"
    assert any("the server" in s for s in sure.sources)
    assert any("the address" in s for s in sure.sources)
    row = [r for r in variants.rows(got, None) if r["item"] == "source brand"]
    assert variants.UNPROVED_ON[brand] in row[0]["detail"]


@pytest.mark.parametrize("family,raw", [
    ("mongodb", {"version": "5.0.0", "_asked": "True"}),
    ("kafka", {"_asked": "True"}),
    ("redis", {"_asked": "True"})])
def test_nothing_distinctive_is_not_told_apart_and_nothing_acts_on_it(
        family, raw):
    got = variants.identify(family, raw)
    assert got.confidence == "not told apart" and not got.told
    said = variants.acts_on(got, "the DocumentDB pre-flight")
    assert "not taken" in said and "managed: {service:" in said
    row = variants.rows(got, None)[0]
    assert row["level"] == "warn" and "not told apart" in row["detail"]
    # the DocumentDB pre-flight reads the confidence and refuses
    from migkit.engines.mongodb import MongoEngine
    rows = MongoEngine.documentdb_preflight("app", ["a"], [], 1, 100, got)
    assert rows[0].status == "warn" and "name the service" in rows[0].detail
    # the address alone is a fact of the endpoint: likely, not sure
    alone = variants.identify("mongodb", {**raw, "_endpoint": "documentdb"})
    if family == "mongodb":
        assert (alone.name, alone.confidence) == ("documentdb", "likely")


def test_the_hop_naming_the_service_and_a_contradiction():
    named = variants.identify("mongodb", {"version": "5.0.0",
                                          "_named": "documentdb"})
    assert (named.name, named.confidence) == ("documentdb", "sure")
    # a server sign the hop contradicts: the brand is not told apart
    clash = variants.identify("mongodb", {**DOCDB, "_named": "cosmosdb"})
    assert clash.confidence == "not told apart"
    # the provider's own API is the stronger rung where it answered
    api = decide.brand_confidence({"server": "msk", "managed": True,
                                   "api": "msk"})
    assert api.rung.name == "api"
    assert decide.BRAND_CONFIDENCE[api.rung.name] == "sure"


def test_a_documentdb_fake_is_told_through_the_engines_own_probe():
    from pymongo.errors import OperationFailure
    from migkit.engines.mongodb import MongoEngine

    class Admin:
        def __init__(self, docdb):
            self.docdb = docdb

        def command(self, name):
            if name == "buildInfo":
                return {"version": "5.0.0", "ok": 1} if self.docdb else {
                    "version": "7.0.4", "gitVersion": "abc",
                    "storageEngines": ["wiredTiger"]}
            raise OperationFailure("no such command", code=59)

        def aggregate(self, pipeline):
            if self.docdb:
                return iter([{"database": "app", "collection": ""}])
            raise OperationFailure("Unrecognized pipeline stage name",
                                   code=40324)

    class Client:
        def __init__(self, docdb):
            self.admin = Admin(docdb)
    for docdb, brand, sure in ((True, "documentdb", "likely"),
                               (False, "mongodb", "sure")):
        eng = MongoEngine(_hop("mongodb"))
        eng._client = lambda side, d=docdb: Client(d)
        got = eng._brands()[0]
        assert (got.name, got.confidence) == (brand, sure)


# ---- the open-source halves, on the sandbox ---------------------------------

@pytest.mark.docker
@pytest.mark.parametrize("key,engine,brand", [
    ("redis", "redis", "redis"), ("mongo", "mongodb", "mongodb"),
    ("kafka", "kafka", "kafka")])
def test_the_open_source_server_is_told_as_itself_and_sure(key, engine,
                                                           brand):
    from tests import servers
    from migkit.engines import get_engine
    with servers.space(key, __name__, own=False) as got:
        hop = _hop(engine, host=got.src.host, port=got.src.port)
        hop.target = Endpoint(host=got.dst.host, port=got.dst.port,
                              user="", password="")
        if engine == "mongodb":
            hop.source.user = hop.target.user = got.src.user or ""
            hop.source.password = hop.target.password = \
                got.src.password or ""
        b = get_engine(hop)._brands()[0]
    assert (b.name, b.confidence) == (brand, "sure"), b
    assert b.sources and not variants.acts_on(b, "a step")


# --- W17F2: the provider's own API, asked behind the egress gate -----------

def _docdb_raw():
    from migkit import variants as v
    for name, sigs, _ in v.SIGNATURES["mongodb"]:
        if name == "documentdb":
            key, needle = sigs[0]
            return {key: needle or "yes", "version": "5.0.0"}
    raise AssertionError("no DocumentDB sign")


def test_the_provider_confirming_the_brand_makes_it_sure(monkeypatch):
    from types import SimpleNamespace
    from migkit import egress, managed, variants as v
    side = SimpleNamespace(host="10.0.0.5", port=27017, options={})
    hop = SimpleNamespace(name="hx", options={}, source=side, target=side)
    eng = SimpleNamespace(hop=hop)
    monkeypatch.setattr(managed, "api_conf", lambda h, e, w: ("aws", {}))
    asked = []

    class Fake:
        def instance(self):
            asked.append(1)
            return SimpleNamespace(product="docdb", host="10.0.0.5")

        def _describe(self):
            return {"groups": [("cluster", "g")]}

        def _params(self, d, names):
            return {"change_stream_log_retention_duration":
                    {"value": "10800"}}
    monkeypatch.setattr(managed, "adapter", lambda h, e, w: Fake())
    raw = {**_docdb_raw(), **v.context(side, eng, "src")}
    got = v.identify("mongodb", raw)
    assert got.name == "documentdb" and got.confidence == "sure"
    assert any("provider's own API" in s for s in got.sources)
    assert v.api_facts(hop, eng, "src")["_retention"] == 10800
    # unreachable: the fact unknown, the server's sign alone - likely
    monkeypatch.setattr(managed, "adapter", lambda h, e, w: (_ for _ in ())
                        .throw(OSError("no route")))
    got = v.identify("mongodb", {**_docdb_raw(),
                                 **v.context(side, eng, "src")})
    assert got.confidence == "likely", got
    # refused by the egress: not asked, said, confidence unchanged
    monkeypatch.setattr(managed, "adapter", lambda h, e, w: Fake())
    monkeypatch.setenv("MIGKIT_EGRESS", "none")
    egress._SAID.clear()
    n = len(asked)
    got = v.identify("mongodb", {**_docdb_raw(),
                                 **v.context(side, eng, "src")})
    assert got.confidence == "likely" and len(asked) == n
    assert any("not asked" in s and "MIGKIT_EGRESS" in s
               for s in got.sources), got.sources
