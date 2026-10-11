"""The cloud-native services as sides of a pair (backlog P0-CLOUDS part 2),
without a server: what each is registered as, how each reads its values
and its changes, and the one guard every digest a server works out stands
behind - a server that cannot render a row is never trusted to have.

Spanner, Bigtable, Firestore, GaussDB, OceanBase, PolarDB-X, TDSQL,
Keyspaces, Neptune, Timestream, Lindorm and GeminiDB's APIs.
"""
import datetime
import json
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

NEW = ("spanner", "bigtable", "firestore", "datastore", "gaussdb",
       "oceanbase", "oceanbase-oracle", "polardbx", "dcdb", "keyspaces",
       "neptune", "timestream", "lindorm", "geminidb-cassandra",
       "geminidb-redis", "geminidb-mongo")

PROVES = [
    ("security.tls", ("spanner", "bigtable", "firestore", "datastore",
                      "gaussdb", "oceanbase", "oceanbase-oracle", "polardbx",
                      "dcdb", "keyspaces", "neptune", "timestream",
                      "lindorm", "geminidb-cassandra", "geminidb-redis",
                      "geminidb-mongo")),
    ("cutover.counters", ("spanner", "gaussdb", "oceanbase-oracle",
                          "polardbx", "dcdb")),
    ("cutover.rollback", ("spanner", "gaussdb", "firestore",
                          "oceanbase-oracle", "dcdb", "timestream")),
    ("verify.structure.indexes", ("gaussdb", "firestore", "lindorm")),
    ("scope.listings", "bigtable"),
    ("tail.follows", ("bigtable", "keyspaces", "neptune")),
    ("schema.create", "dcdb"),
]


def _hop(engine, **opts):
    return Hop(name="cn", engine=engine,
               source=Endpoint(host="h1", port=1, user="u", password="p",
                               options=dict(opts)),
               target=Endpoint(host="h2", port=2, user="u", password="p"),
               databases=["shop"])


# ---- registered, every one of them ------------------------------------------

def test_every_service_is_an_engine_with_a_scheme_and_a_title():
    from migkit.engines import NAMES, SCHEMES, TITLES, _class_for
    for name in NEW:
        assert name in NAMES, name
        assert name in SCHEMES.values(), name
        assert TITLES.get(name), name
        assert _class_for(name) is not None, name


def test_what_each_does_not_do_is_declared_and_every_shape_has_a_way():
    from migkit import capabilities, decide
    assert not [c for c in capabilities.undeclared() if c[0] in NEW]
    assert not [c for c in capabilities.stale() if c[0] in NEW]
    assert not [c for c in decide.uncovered() if c[0] in NEW]
    # read off the code, not declared: Spanner follows, Datastore cannot
    assert capabilities.implemented("spanner", "stream")
    assert not capabilities.implemented("datastore", "stream")
    assert capabilities.matrix()["datastore"]["stream"] == \
        capabilities.NOT_APPLICABLE


def test_an_address_names_each_service():
    from migkit import addresses
    engine, ep, dbs = addresses.parse("spanner://proj/inst/shop,orders")
    assert (engine, ep["host"], ep["instance"], dbs) == (
        "spanner", "proj", "inst", ["shop", "orders"])
    engine, ep, dbs = addresses.parse("timestream://us-east-1/metrics")
    assert (engine, ep["region"], dbs) == ("timestream", "us-east-1",
                                           ["metrics"])
    engine, ep, dbs = addresses.parse("bigtable://proj/inst1")
    assert (engine, ep["host"], dbs) == ("bigtable", "proj", ["inst1"])
    engine, ep, dbs = addresses.parse(
        "spanner://proj/inst/db?emulator=127.0.0.1:9010")
    assert ep["emulator"] == "127.0.0.1:9010"
    with pytest.raises(SystemExit, match="has no option"):
        addresses.parse("spanner://proj/inst/db?emulater=127.0.0.1:9010")


def test_each_managed_service_is_named_by_its_address():
    from migkit import clouds
    for host, label in (
            ("cassandra.eu-west-1.amazonaws.com", "Amazon Keyspaces"),
            ("db-1.cluster-abc.us-east-1.neptune.amazonaws.com",
             "Amazon Neptune"),
            ("pxc-xyz.polarx.rds.aliyuncs.com", "Alibaba PolarDB-X"),
            ("ld-abc.lindorm.rds.aliyuncs.com", "Alibaba Lindorm"),
            ("abc.gaussdb.ap-southeast-2.myhuaweicloud.com",
             "Huawei GaussDB")):
        assert clouds.from_host(host).label == label, host


def test_every_sdk_is_accounted_for_option_by_option():
    from migkit import tooluse
    rows = tooluse.tools_used()
    for name in ("google-cloud-spanner", "google-cloud-bigtable",
                 "google-cloud-firestore", "google-cloud-datastore",
                 "gremlinpython", "neptunedata", "cassandra-sigv4",
                 "timestream"):
        assert rows[name]["used"], name
        assert tooluse.stale(name) == [], name


def test_a_licence_written_as_an_expression_in_the_free_text_field():
    """aiohttp, which the Gremlin driver needs, writes `Apache-2.0 AND MIT`
    in its licence field: both, each one migkit may be combined with."""
    from migkit import tools

    class Meta(dict):
        def get_all(self, name):
            return []

    class Dist:
        metadata = Meta({"License": "Apache-2.0 AND MIT"})
        files = []
    assert tools._licences_of(Dist()) == ["Apache-2.0", "MIT"]
    Dist.metadata = Meta({"License": "BSD-3-Clause OR GPL-2.0-only"})
    assert tools._licences_of(Dist()) == ["BSD-3-Clause"]


def test_each_says_whether_its_connection_is_checked(monkeypatch):
    """The assessment's word on each side's connection, from the engine's
    own: Google's services over the TLS their SDKs check unless an
    emulator is named, Neptune over wss and https unless told not,
    Timestream through its SDK unless another address is named, and the
    rest as the wire they speak - read off the options their own
    connections are made with. Nothing is asked of the network."""
    from migkit import tls
    from migkit.engines import _class_for

    def refuse(*a, **k):
        raise AssertionError("asked the network")
    monkeypatch.setattr(tls, "_probe", refuse)

    def said(name, **opts):
        tls._SEEN.clear()
        hop = Hop(name="t", engine=name,
                  source=Endpoint(host="10.0.0.7", port=1, user="u",
                                  password="p", options=dict(opts)),
                  target=Endpoint(host="127.0.0.1", port=1),
                  databases=["shop"])
        rows = {r["item"]: r["level"]
                for r in _class_for(name)(hop)._leg_items()}
        return next(k for k in rows if k.startswith("source")), rows
    for name in ("spanner", "bigtable", "firestore", "datastore"):
        assert said(name)[0] == "source connection verified", name
        assert said(name, emulator="10.0.0.8:9010")[0] == \
            "source connection NOT encrypted", name
    assert said("neptune")[0] == "source connection verified"
    assert said("neptune", tls=False)[0] == "source connection NOT encrypted"
    assert said("timestream")[0] == "source connection verified"
    assert said("timestream", endpoint_url="http://10.0.0.8:8086")[0] == \
        "source connection NOT encrypted"
    for name in ("oceanbase", "polardbx", "dcdb", "oceanbase-oracle",
                 "lindorm"):
        assert said(name, ssl_ca="/ca.pem")[0] == \
            "source connection verified", name
        item, rows = said(name, ssl=True)
        assert item == "source connection NOT verified", name
        assert rows[item] == "fail", name
    assert said("gaussdb", sslmode="verify-full",
                sslrootcert="/ca.pem")[0] == "source connection verified"
    assert said("gaussdb", sslmode="disable")[0] == \
        "source connection NOT encrypted"
    for name in ("keyspaces", "geminidb-cassandra", "geminidb-redis",
                 "geminidb-mongo"):
        assert said(name, tls_ca_file="/ca.pem")[0] == \
            "source connection verified", name
        assert said(name, tls=True, tls_insecure=True)[0] == \
            "source connection NOT verified", name
    tls._SEEN.clear()


def test_a_service_on_another_api_is_paired_as_itself():
    """Keyspaces' values are Cassandra's in every way canon reads them,
    and its same-engine pair is still made of Keyspaces sides - its TLS,
    its signature and its LOCAL_QUORUM."""
    from migkit.engines import same_engine
    from migkit.engines.keyspaces import KeyspacesEngine
    eng = KeyspacesEngine(_hop("keyspaces"))
    assert eng.CANON_ENGINE == "cassandra"
    pair = eng._as_pair()
    assert type(pair.src_engine) is KeyspacesEngine
    assert type(pair.dst_engine) is KeyspacesEngine
    assert same_engine(pair.src_engine) and same_engine(eng)


def test_each_is_held_by_the_hold_of_the_api_it_speaks_or_said_not_held():
    from migkit import freeze
    from migkit.engines.gaussdb import GaussEngine
    from migkit.engines.keyspaces import KeyspacesEngine
    held = freeze.hold_for(_hop("gaussdb"), GaussEngine(_hop("gaussdb")))
    assert isinstance(held, freeze.PostgresHold)
    not_held = freeze.hold_for(_hop("keyspaces"),
                               KeyspacesEngine(_hop("keyspaces")))
    assert type(not_held) is freeze.Hold
    assert "access policies" in freeze.not_held("keyspaces")


def test_geminidb_makes_no_replica_and_follows_nothing_it_cannot():
    from migkit import capabilities
    from migkit.engines.geminidb import (GeminiCassandraEngine,
                                         GeminiRedisEngine)
    assert not hasattr(GeminiRedisEngine, "replicate_sql")
    assert not hasattr(GeminiRedisEngine, "fence_wait")
    assert not hasattr(GeminiCassandraEngine, "tail_apply")
    for name in ("geminidb-redis", "geminidb-cassandra"):
        assert not capabilities.implemented(name, "stream"), name
        assert capabilities.implemented(name, "data"), name


# ---- a digest a server works out is proved before it is trusted -------------

class _Proved:
    """A server that speaks MySQL's SQL, answering from rows held here:
    `texts(row)` is what it renders a row as, and the digest query
    answers `SERVER`, so which way was taken shows."""

    def __new__(cls, rows, texts):
        from migkit.engines.base import Engine
        from migkit.engines.provedsql import ProvedDigest

        class Fake(ProvedDigest, Engine):
            PROVED_DIALECT = "mysql"

            def neutral_key(self, side, db, table):
                return ["id"]

            def neutral_read(self, side, db, table, columns, after=None,
                             limit=1000, where=None):
                return [list(r) for r in rows[:limit]], None

            def neutral_batches(self, side, db, table, columns, size=1000,
                                where=None):
                yield [list(r) for r in rows]

            def _proved_rows(self, side, db, sql):
                if sql.startswith("select count(*)"):
                    return [[len(rows), "SERVER"]]
                return [[texts(r)] for r in rows]

            def _proved_table(self, side, db, table):
                return f"`{table}`"

            def _proved_quote(self, name):
                return f"`{name}`"
        return Fake(_hop("oceanbase"))


def test_a_server_that_renders_nothing_is_never_trusted_with_the_digest():
    """A server missing a function the rendering uses answers NULL for
    every row: summed, 0 on both sides, and any two tables would be one.
    Its first rows are asked for first, and NULL is not their text."""
    from migkit import canon
    cols = [("id", "integer"), ("name", "text")]
    rows = [[1, "a"], [2, "b"]]
    eng = _Proved(rows, lambda r: None)
    got = eng.neutral_digest("src", "shop", "t", cols)
    assert got == canon.fold_batches(cols, [rows])
    assert got[1] != "SERVER" and eng.folded_last is True
    # a table differing in one value folds to another number
    other = _Proved([[1, "a"], [2, "c"]], lambda r: None)
    assert other.neutral_digest("src", "shop", "t", cols) != got


def test_a_server_whose_first_rows_render_as_here_works_the_digest_out():
    from migkit import canon, rowtext
    cols = [("id", "integer"), ("name", "text")]
    rows = [[1, "a"], [2, "b"]]

    def same(r):
        return rowtext.encode([canon.render_value(c, v)
                               for (_, c), v in zip(cols, r)])
    eng = _Proved(rows, same)
    assert eng.neutral_digest("src", "shop", "t", cols) == (2, "SERVER")
    assert eng.folded_last is False
    # one row rendered otherwise - a trailing space - and it is folded
    off = _Proved(rows, lambda r: same(r) + (" " if r[0] == 2 else ""))
    assert off.neutral_digest("src", "shop", "t", cols)[1] != "SERVER"


def test_an_empty_table_proves_nothing_about_the_server():
    eng = _Proved([], lambda r: "x")
    got = eng.neutral_digest("src", "shop", "t", [("id", "integer")])
    assert got[1] != "SERVER"


# ---- Spanner ------------------------------------------------------------------

def test_spanner_types_are_read_as_migkit_reads_them():
    from migkit import canon
    from migkit.engines.spanner import _kind, normal_type
    assert normal_type("ARRAY<STRING(MAX)>") == "STRING(MAX)[]"
    assert normal_type("PROTO<shop.Order>") == "PROTO"
    assert normal_type("ARRAY<FLOAT32>(vector_length=>3)") == "FLOAT32[]"
    for declared, cls in (("INT64", "integer"), ("STRING(MAX)", "text"),
                          ("NUMERIC", "decimal"), ("TIMESTAMP", "timestamp"),
                          ("BYTES(16)", "bytes"), ("INT64[]", "json"),
                          ("character varying(20)", "text"),
                          ("timestamp with time zone", "timestamp"),
                          ("jsonb", "json"), ("bigint[]", "json")):
        assert canon.type_class("spanner", declared) == cls, declared
    assert _kind("INT64[]") == ("array", "int")
    assert _kind("numeric") == ("numeric", None)


def test_a_spanner_timestamp_keeps_its_nanoseconds_both_ways():
    from migkit import canon
    from migkit.engines.spanner import _nanotime, at_ns, ns_of
    ns = 1_700_000_000_123_456_789
    back = at_ns(ns)
    assert ns_of(back) == ns
    held = _nanotime(back)
    assert held.nanosecond == 789
    assert canon.render_value("timestamp", held).endswith(".123456789")
    assert ns_of(held) == ns
    # one nanosecond away is another value
    assert canon.render_value("timestamp", _nanotime(at_ns(ns + 1))) != \
        canon.render_value("timestamp", held)


def test_the_spanner_digest_renders_nine_digits_and_no_float():
    from migkit.engines import warehouse_sql as w
    text = w.column_text("spanner", "`t`", "timestamp", "TIMESTAMP")
    assert "%E9S" in text and "ENDS_WITH" in text
    assert w.column_text("spanner", "`f`", "float", "FLOAT64") is None
    assert w.column_text("spanner", "`e`", "integer", "ENUM") is None
    got = w.digest_select("spanner", [("id", "integer", "INT64"),
                                      ("n", "number", "NUMERIC")],
                          lambda n: f"`{n}`")
    assert "AS NUMERIC" in got and "BIGNUMERIC" not in got
    assert w.digest_select("spanner", [("f", "float", "FLOAT64")],
                           lambda n: f"`{n}`") is None


def test_spanner_change_records_read_the_same_from_either_dialect():
    from google.api_core.datetime_helpers import DatetimeWithNanoseconds
    from google.cloud.spanner_v1 import JsonObject

    from migkit.changetopic import Mark, SpannerChanges
    from migkit.engines.spanner import _data_record
    ts = DatetimeWithNanoseconds(2026, 10, 1, 12, 0, 0, nanosecond=5,
                                 tzinfo=datetime.timezone.utc)
    google = _data_record({"commit_timestamp": ts, "table_name": "orders",
                           "mod_type": "DELETE",
                           "mods": [{"keys": JsonObject({"id": "7"})}]})
    pg = _data_record({"commit_timestamp": "2026-10-01T12:00:00.000000005Z",
                       "table_name": "orders", "mod_type": "DELETE",
                       "mods": [{"keys": {"id": "7"}}]})
    assert google == pg
    got = SpannerChanges().decode(None, google)
    assert got[0].op == "delete" and got[0].key == {"id": "7"}
    assert got[0].version == google["commit_ns"]
    assert isinstance(got[-1], Mark) and got[-1].version == \
        google["commit_ns"] - 1


class _Partitions:
    """A change stream's partitions as `SpannerLanes._query` would read
    them: (records, heartbeats, children, ended) by token."""

    def __init__(self, plan, now):
        self.plan, self.now = plan, now

    def engine(self):
        from migkit.engines.spanner import SpannerLanes

        class Eng:
            def _now_ns(inner, side, db):
                return self.now

            def _retention_s(inner, db, stream):
                return 86400.0
        lanes = SpannerLanes(Eng(), "shop", "s")
        lanes._query = lambda token, start, end: self.plan.pop(
            token, ([], [], [], False))
        return lanes


def test_spanner_partitions_are_handed_on_in_commit_order():
    """Partition b's change at 30 waits until a's mark passes it, though
    b was read first; the root's children begin their lanes at their start
    and the root, ended, is no lane any more."""
    from migkit.changetopic import Follower, SpannerChanges

    def rec(ts, key):
        return {"commit_ns": ts, "table_name": "t", "mod_type": "UPDATE",
                "transaction": "x", "mods": [{"keys": {"id": key}}]}
    plan = {"": ([], [], [("a", 10), ("b", 10)], True),
            "a": ([rec(40, "3")], [20], [], False),
            "b": ([rec(30, "2")], [], [], False)}
    parts = _Partitions(plan, now=100)
    lanes = parts.engine()
    order = []

    def back(table, rows, version):
        order.extend(r.key["id"] for r in rows)
        return [(r, dict(r.key), {"id": r.key["id"]}) for r in rows]
    f = Follower(SpannerChanges(), lanes, name=lambda r: r.table,
                 read_back=back)
    token = {"via": "spanner", "at": {"": {"at": 5}}, "fence": 5,
             "marks": {}, "seen": {}}
    out, token = f.changes(token, 100)
    assert "" not in token["at"] and {"a", "b"} <= set(token["at"])
    # the root read and its children found: their windows read to now
    # mark both at 100, so both changes go, in commit order
    for _ in range(3):
        if out:
            break
        out, token = f.changes(token, 100)
    assert order == ["2", "3"], order
    assert [c["key"]["id"] for c in out] == ["2", "3"]


# ---- Bigtable, Firestore, Datastore ----------------------------------------

def test_a_bigtable_qualifier_is_text_where_it_can_be_and_comes_back_whole():
    from migkit.engines.bigtable import qualifier_bytes, qualifier_text
    for q in (b"name", b"\xff\x00", b"~tilde", "é".encode()):
        assert qualifier_bytes(qualifier_text(q)) == q, q
    assert qualifier_text(b"name") == "name"
    assert qualifier_text(b"~tilde").startswith("~") and \
        qualifier_text(b"~tilde") != "~tilde"


def test_a_firestore_value_keeps_its_kind_inside_a_map():
    from google.cloud.firestore_v1 import GeoPoint

    from migkit.engines.firestore import from_ext, fs_kind, to_ext
    when = datetime.datetime(2026, 1, 2, 3, 4, 5, 678901,
                             tzinfo=datetime.timezone.utc)
    value = {"at": when, "raw": b"\x01\x02", "where": GeoPoint(1.5, -2.0),
             "n": 3, "x": 3.0, "list": [when, {"deep": b"z"}]}
    ext = to_ext(value)
    assert ext["at"] == {"$timestamp": "2026-01-02 03:04:05.678901"}
    assert ext["raw"] == {"$bytes": "AQI="}
    back = from_ext(json.loads(json.dumps(ext)))
    assert back["at"] == when and back["raw"] == b"\x01\x02"
    assert (back["where"].latitude, back["where"].longitude) == (1.5, -2.0)
    assert back["list"][1] == {"deep": b"z"}
    # an integer and a double are two kinds, and two texts
    assert fs_kind(3) == "integer" and fs_kind(3.0) == "double"
    from migkit import canon
    assert canon.render_value("json", to_ext({"v": 3})) != \
        canon.render_value("json", to_ext({"v": 3.0}))


def test_a_described_table_reads_back_each_column_by_its_class(tmp_path,
                                                                monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    from migkit import described
    described.keep("bigtable", "e/p/i", "t", [("id", "integer"),
                                              ("amount", "decimal")], ["id"])
    cols, key = described.read("bigtable", "e/p/i", "t")
    assert cols == [("id", "integer"), ("amount", "decimal")]
    assert key == ["id"]
    raw = described.cell_of("decimal", Decimal("12.50"))
    assert described.value_of("decimal", raw) == Decimal("12.50")
    assert str(described.value_of("decimal", raw)) == "12.50"
    classes = dict(cols)
    text = described.key_text(classes, ["id"], {"id": 42})
    assert described.key_values(classes, ["id"], text) == {"id": 42}
    # a description torn by a crash stops rather than reading as none
    path = described._path("bigtable", "e/p/i")
    path.write_text('{"t": {"columns": ')
    with pytest.raises(SystemExit, match="cannot be read whole"):
        described.read("bigtable", "e/p/i", "t")


# ---- GaussDB ----------------------------------------------------------------

def test_openGauss_json_decoding_reads_as_test_decoding_reads():
    from migkit import pgslot
    from migkit.engines.gaussdb import lsn_int, mppdb_line
    line = json.dumps({
        "table_name": "public.t", "op_type": "UPDATE",
        "columns_name": ["id", "note", "gone"],
        "columns_type": ["integer", "text", "text"],
        "columns_val": ["1", "'it''s: [x]'", "null"],
        "old_keys_name": ["id"], "old_keys_type": ["integer"],
        "old_keys_val": ["1"]})
    parsed = mppdb_line(line)
    change = pgslot.change(parsed, ["id"], txn=9)
    assert change["key"] == {"id": 1}
    assert change["values"]["note"] == "it's: [x]"
    assert change["values"]["gone"] is None
    quoted = mppdb_line(json.dumps({
        "table_name": "public.t", "op_type": "INSERT",
        "columns_name": ["id", "note"], "columns_type": ["integer", "text"],
        "columns_val": ["2", "'null'"]}))
    # the quoted word is text, the bare one NULL
    assert pgslot.change(quoted, ["id"])["values"]["note"] == "null"
    deleted = mppdb_line(json.dumps({
        "table_name": "public.t", "op_type": "DELETE",
        "old_keys_name": ["id"], "old_keys_type": ["integer"],
        "old_keys_val": ["3"]}))
    assert pgslot.change(deleted, ["id"])["op"] == "delete"
    assert mppdb_line("BEGIN 12") is None
    assert lsn_int("1/0") > lsn_int("0/FFFFFFFF") > lsn_int("0/10")


# ---- TDSQL, PolarDB-X ---------------------------------------------------------

def test_a_tdsql_table_is_made_sharded_by_its_key():
    from migkit.engines.distmysql import TdsqlEngine
    eng = TdsqlEngine(_hop("dcdb"))
    cols = [("id", "integer", ()), ("note", "text", (20,))]
    assert eng.neutral_create_sql("dst", "shop", "t", cols, ["id"]) \
        .endswith(" shardkey=id")
    eng.hop.target.options["shardkey"] = {"t": "none"}
    assert "shardkey" not in eng.neutral_create_sql("dst", "shop", "t",
                                                    cols, ["id"])
    eng.hop.target.options["shardkey"] = "note"
    assert eng.neutral_create_sql("dst", "shop", "t", cols, ["id"]) \
        .endswith(" shardkey=note")
    eng.hop.target.options.pop("shardkey")
    with pytest.raises(SystemExit, match="no key to shard by"):
        eng.neutral_create_sql("dst", "shop", "t", cols, [])


def test_a_tdsql_position_is_reached_only_when_every_set_is():
    from migkit.engines.distmysql import TdsqlEngine
    want = {"sets": {"a": {"log_file": "b.000002", "log_pos": 10},
                     "b": {"log_file": "b.000001", "log_pos": 500}}}
    have = {"sets": {"a": {"log_file": "b.000002", "log_pos": 12},
                     "b": {"log_file": "b.000001", "log_pos": 400}}}
    assert TdsqlEngine.position_reached(have, want) is False
    have["sets"]["b"]["log_pos"] = 500
    assert TdsqlEngine.position_reached(have, want) is True
    assert TdsqlEngine.position_reached({"sets": {"a": want["sets"]["a"]}},
                                        want) is False


def test_a_tdsql_source_without_its_sets_says_how_to_follow_it():
    from migkit.engines.distmysql import TdsqlEngine
    with pytest.raises(SystemExit, match="each set keeps its own"):
        TdsqlEngine(_hop("dcdb")).change_point("src", "shop")


#: what a distributed TDSQL instance's proxy answers of itself, in the
#: shape its documentation gives (two sets, each its hash range)
PROXY_TWO_SETS = {"cluster": "group_1499858589_10",
                  "set_1499858817_1:ip": "10.0.0.11:4002;s1@10.0.0.12:4002",
                  "set_1499858817_1:hash_range": "0---31",
                  "set_1499859173_3:ip": "10.0.0.13:4002",
                  "set_1499859173_3:hash_range": "32---63"}


def test_a_tdsql_proxy_says_it_spreads_tables_and_a_mysql_server_does_not():
    from migkit.engines.distmysql import sets_of, spread
    assert sets_of(PROXY_TWO_SETS) == ["set_1499858817_1",
                                       "set_1499859173_3"]
    assert spread(PROXY_TWO_SETS)
    # a set's hash range alone is enough; one set without one is not
    assert spread({"set_1:hash_range": "0---63"})
    assert not spread({"cluster": "g", "set_1:ip": "10.0.0.11:4002"})
    # what a MySQL server answers the same statement: its own status
    assert not spread({"Aborted_clients": "0", "Com_set_option": "12",
                       "Uptime": "3600"})


def test_a_hop_named_tdsql_is_moved_as_the_server_says_it_is(monkeypatch):
    from migkit import engines
    from migkit.engines.distmysql import TdsqlEngine
    from migkit.engines.mysql import MySQLEngine
    asked = []

    def says(answer):
        def server_says_so(cls, hop, side):
            asked.append(side)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return classmethod(server_says_so)
    monkeypatch.setattr(TdsqlEngine, "server_says_so", says(True))
    hop = _hop("tdsql")
    monkeypatch.setattr("migkit.tunnel.through", lambda h: None)
    assert type(engines.get_engine(hop)) is TdsqlEngine
    assert hop.engine == "dcdb" and asked == ["src"]
    # asked once a hop: the name is the server's from then on
    engines.get_engine(hop)
    assert asked == ["src"]
    monkeypatch.setattr(TdsqlEngine, "server_says_so", says(False))
    hop, asked[:] = _hop("tdsql"), []
    assert type(engines.get_engine(hop)) is MySQLEngine
    assert hop.engine == "mysql" and asked == ["src", "tgt"]
    # a server that cannot be asked leaves the name, its alias's engine
    monkeypatch.setattr(TdsqlEngine, "server_says_so",
                        says(ConnectionError("refused")))
    hop = _hop("tdsql")
    assert type(engines.get_engine(hop)) is MySQLEngine
    assert hop.engine == "tdsql"
    # a cross-engine hop's side is asked of its own side alone
    monkeypatch.setattr(TdsqlEngine, "server_says_so", says(True))
    hop, asked[:] = _hop("hetero", ), []
    hop.options = {"source_engine": "postgres", "target_engine": "tdsql"}
    engines.said_by_server(hop)
    assert hop.options["target_engine"] == "dcdb" and asked == ["tgt"]


def test_a_tdsql_hops_options_are_its_distributed_engines_too():
    from migkit import hopkeys
    from migkit.engines import may_be
    assert may_be("tdsql") == {"mysql", "dcdb"}
    assert may_be("mysql") == {"mysql"}
    keys = hopkeys.endpoint_keys_for("tdsql")
    assert {"sets", "shardkey"} <= keys
    assert "sets" not in hopkeys.endpoint_keys_for("mysql")
    hop = _hop("tdsql", sets=["10.0.0.11:4002"])
    said = dict(hopkeys.said_by_doctor(hop, {"options": {}}))
    assert "source.sets" not in said, said
    hop = _hop("mysql", sets=["10.0.0.11:4002"])
    said = dict(hopkeys.said_by_doctor(hop, {"options": {}}))
    assert "source.sets" in said, said


def test_a_tdsql_side_is_known_by_its_proxy_not_its_version(monkeypatch):
    from migkit.engines import distmysql
    eng = distmysql.TdsqlEngine(_hop("dcdb"))
    monkeypatch.setattr(distmysql, "proxy_status",
                        lambda e, side: dict(PROXY_TWO_SETS))
    assert eng._is_it("src", "5.7.17-11-log", "") is True
    assert eng._cluster("src") == "group_1499858589_10"
    monkeypatch.setattr(distmysql, "proxy_status",
                        lambda e, side: {"Uptime": "1"})
    assert eng._is_it("src", "8.0.36", "MySQL Community Server") is False

    def refused(e, side):
        raise PermissionError("denied")
    monkeypatch.setattr(distmysql, "proxy_status", refused)
    assert eng._is_it("src", "8.0.36", "") is None


def test_polardbx_reads_its_binlog_without_asking_mysql_settings(monkeypatch):
    """Its global binlog carries whole rows always: none of MySQL's binlog
    settings is asked (a server may not have them at all)."""
    from migkit.engines.distmysql import PolarDBXEngine
    from migkit.engines.mysql import MySQLEngine
    assert PolarDBXEngine.BINLOG_FULL == ()
    assert MySQLEngine.BINLOG_FULL == ("binlog_row_metadata",
                                       "binlog_row_image")


def test_polardbx_compares_the_sequence_a_table_numbers_its_rows_from(
        monkeypatch):
    from migkit.engines.distmysql import PolarDBXEngine
    eng = PolarDBXEngine(_hop("polardbx"))
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["t", "u"])

    def q(side, sql, args=None):
        if "information_schema.columns" in sql:
            return [("t", "id"), ("u", "id")]
        return [(100,)] if "`t`" in sql else [(7,)]
    monkeypatch.setattr(eng, "_q", q)
    monkeypatch.setattr(eng, "_q_named", lambda side, sql: [
        {"NAME": "AUTO_SEQ_t", "VALUE": 101},
        {"NAME": "AUTO_SEQ_u", "VALUE": 5}])
    got = {r.scope: r.status for r in eng.check_autoinc("shop")}
    assert got == {"shop.t": "ok", "shop.u": "diff"}


# ---- Keyspaces ------------------------------------------------------------------

def test_a_keyspaces_stream_cell_reads_as_the_driver_reads_it():
    from migkit.engines.keyspaces import cell_value
    assert cell_value({"value": {"bigintT": "9007199254740993"}}) == \
        9007199254740993
    assert cell_value({"value": {"decimalT": "1.50"}}) == Decimal("1.50")
    assert str(cell_value({"value": {"decimalT": "1.50"}})) == "1.50"
    assert cell_value({"value": {"blobT": b"\x00"}}) == b"\x00"
    assert cell_value({"value": {"timestampT": "1700000000123"}}) == \
        datetime.datetime(2023, 11, 14, 22, 13, 20, 123000)
    assert cell_value({"value": {"listT": [{"value": {"intT": "1"}},
                                           {"value": {"intT": "2"}}]}}) \
        == [1, 2]
    assert cell_value({"value": {"mapT": [
        {"key": {"value": {"textT": "a"}},
         "value": {"value": {"intT": "1"}}}]}}) == {"a": 1}
    assert cell_value(None) is None


# ---- Neptune ----------------------------------------------------------------

def test_neptune_terms_and_quads_read_as_n_triples():
    from migkit.engines.neptune import NEPTUNE_DEFAULT, nquad, term
    assert term({"type": "uri", "value": "http://x/a"}) == "<http://x/a>"
    assert term({"type": "bnode", "value": "b0"}) == "_:b0"
    assert term({"type": "literal", "value": 'say "hi"\n',
                 "xml:lang": "en"}) == '"say \\"hi\\"\\n"@en'
    assert term({"type": "literal", "value": "5", "datatype":
                 "http://www.w3.org/2001/XMLSchema#integer"}) == \
        '"5"^^<http://www.w3.org/2001/XMLSchema#integer>'
    assert term({"type": "literal", "value": "s", "datatype":
                 "http://www.w3.org/2001/XMLSchema#string"}) == '"s"'
    assert nquad('<http://a> <http://b> "x\\"y . z"@en <http://g> .') == (
        "<http://a>", "<http://b>", '"x\\"y . z"@en', "<http://g>")
    assert nquad(f"_:b1 <http://b> <http://c> {NEPTUNE_DEFAULT} .")[3] == ""


def test_neptune_properties_compare_whatever_order_a_set_comes_in():
    from gremlin_python.statics import long

    from migkit.engines.neptune import properties, typed, untyped
    a = properties({"tag": ["x", "y"], "n": [1]})
    b = properties({"tag": ["y", "x"], "n": 1})
    assert a == b
    assert properties({"n": [1]}) != properties({"n": [1.0]})
    big = untyped(typed(2 ** 40))
    assert isinstance(big, long) and big == 2 ** 40
    when = datetime.datetime(2026, 1, 1, 0, 0, 0, 5000)
    assert untyped(typed(when)) == when


def test_neptune_stream_changes_are_elements_or_quads():
    from migkit.changetopic import Mark, NeptuneChanges
    from migkit.engines.neptune import OPS, NeptuneLanes

    lanes = NeptuneLanes(None, "graph")
    got = lanes._record({"eventId": {"commitNum": 7, "opNum": 2}, "op": "ADD",
                         "data": {"id": "e1", "type": "ep", "key": "w",
                                  "value": {"value": 1, "dataType": "Int"}}})
    assert got["table"] == "edges" and got["key"] == {"id": "e1"}
    assert got["version"] == 7 * OPS + 2
    rows = NeptuneChanges().decode(None, got)
    assert rows[0].version == 7 * OPS + 2 and isinstance(rows[1], Mark)
    rdf = NeptuneLanes(None, "rdf")._record({
        "eventId": {"commitNum": 8, "opNum": 1}, "op": "REMOVE",
        "data": {"stmt": "<http://a> <http://b> \"c\" ."}})
    assert rdf["op"] == "delete" and rdf["key"]["graph"] == ""
    from migkit.engines.neptune import NeptuneEngine
    key, values = NeptuneEngine._shape("quads", NeptuneChanges().decode(
        None, rdf)[0])
    assert values is None and key["object"] == '"c"'
    # an element is read back: its change carries no values
    assert NeptuneEngine._shape("edges", rows[0]) is None


# ---- Timestream ---------------------------------------------------------------

def test_a_timestream_record_is_written_whole_at_a_higher_version(
        monkeypatch):
    from migkit.engines.timestream import TimestreamEngine
    eng = TimestreamEngine(_hop("timestream"))
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: [])
    cols = [("sensor", "text"), ("time", "timestamp"), ("temp", "float"),
            ("ok", "boolean")]
    parts = eng._parts("dst", "shop", "readings", cols)
    assert parts == (["sensor"], None, "time",
                     [("temp", "double"), ("ok", "boolean")])
    from migkit import nanotime
    when = nanotime.parse("2026-10-01 00:00:00.123456789")
    one = eng._record(parts, "readings", [c for c, _ in cols],
                      ["s1", when, 21.5, True])
    two = eng._record(parts, "readings", [c for c, _ in cols],
                      ["s1", when, 21.5, True])
    assert one["Time"] == "1790812800123456789"
    assert one["TimeUnit"] == "NANOSECONDS"
    assert one["MeasureValueType"] == "MULTI"
    assert {m["Name"]: m["Value"] for m in one["MeasureValues"]} == {
        "temp": "21.5", "ok": "true"}
    assert two["Version"] >= one["Version"]
    with pytest.raises(SystemExit, match="no timestamp column named time"):
        eng._parts("dst", "shop", "x", [("id", "integer")])


def test_timestream_reads_every_page_of_a_query(monkeypatch):
    from migkit.engines.timestream import TimestreamEngine

    class Query:
        def __init__(self):
            self.calls = []

        def query(self, QueryString, NextToken=None):
            self.calls.append(NextToken)
            info = [{"Name": "sensor", "Type": {"ScalarType": "VARCHAR"}},
                    {"Name": "time", "Type": {"ScalarType": "TIMESTAMP"}},
                    {"Name": "v", "Type": {"ScalarType": "BIGINT"}}]
            if NextToken is None:
                return {"ColumnInfo": info, "NextToken": "p2", "Rows": [
                    {"Data": [{"ScalarValue": "a"},
                              {"ScalarValue":
                               "2026-10-01 00:00:00.000000001"},
                              {"ScalarValue": "1"}]}]}
            return {"ColumnInfo": info, "Rows": [
                {"Data": [{"ScalarValue": "b"},
                          {"ScalarValue": "2026-10-01 00:00:00.000000002"},
                          {"NullValue": True}]}]}
    eng = TimestreamEngine(_hop("timestream"))
    q = Query()
    monkeypatch.setattr(eng, "_client", lambda side, kind: q)
    cols, rows = eng._query("src", "select 1")
    assert q.calls == [None, "p2"]
    assert [r[0] for r in rows] == ["a", "b"] and rows[1][2] is None
    assert rows[0][1].nanosecond == 1 and rows[1][1].nanosecond == 2


# ---- the feeds, from recorded answers ---------------------------------------

class _Obj:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Resp:
    """A `ReadChangeStreamResponse` as the SDK hands one over."""

    def __init__(self, kind, **kw):
        self._pb = _Obj(WhichOneof=lambda name, k=kind: k)
        setattr(self, kind, _Obj(**kw))


def test_bigtable_partitions_follow_their_splits_by_continuation_token():
    from migkit.engines.bigtable import BigtableLanes
    utc = datetime.timezone.utc
    at = datetime.datetime(2026, 10, 1, tzinfo=utc)
    rng = _Obj(start_key_closed=b"", end_key_open=b"m")

    class Data:
        def __init__(self):
            self.requests = []

        def generate_initial_change_stream_partitions(self, request):
            return [_Obj(partition=_Obj(row_range=rng))]

        def read_change_stream(self, request):
            self.requests.append(request)
            return [
                _Resp("data_change", row_key=b"k1", done=False),
                _Resp("data_change", row_key=b"", done=True, token="t1",
                      commit_timestamp=at, estimated_low_watermark=at),
                _Resp("heartbeat", estimated_low_watermark=at
                      + datetime.timedelta(seconds=1),
                      continuation_token=_Obj(token="t2")),
                _Resp("close_stream", continuation_tokens=[
                    _Obj(partition=_Obj(row_range=_Obj(
                        start_key_closed=b"", end_key_open=b"f")),
                         token="c1"),
                    _Obj(partition=_Obj(row_range=_Obj(
                        start_key_closed=b"f", end_key_open=b"m")),
                         token="c2")],
                      new_partitions=[
                          _Obj(row_range=_Obj(start_key_closed=b"",
                                              end_key_open=b"f")),
                          _Obj(row_range=_Obj(start_key_closed=b"f",
                                              end_key_open=b"m"))])]
    data = Data()

    class Eng:
        def _client(self, side):
            return _Obj(table_data_client=data)

        def _table(self, side, db, table):
            return _Obj(name="projects/p/instances/i/tables/t")
    lanes = BigtableLanes(Eng(), "i", "t")
    [lane] = lanes.lanes()
    out, nxt, _ = lanes.read({lane: {"at": 1, "since": 1}}, 100, 5)
    change = [v for _, _, _, v, _ in out if "row_key" in v]
    assert change == [{"row_key": "azE=", "commit_us": 1790812800000000}]
    marks = [v["mark"] for _, _, _, v, _ in out if "mark" in v]
    assert marks == [1790812801000000]
    # the split's two halves take the stream on from their own tokens;
    # the partition split is no lane any more
    assert lane not in nxt and len(nxt) == 2
    assert sorted(p["token"] for p in nxt.values()) == ["c1", "c2"]
    assert set(lanes.lanes()) == set(nxt)
    # a first read starts at a time; one with a token goes on from it
    assert "start_time" in data.requests[0]
    lanes.read(nxt, 100, 5)
    assert all("continuation_tokens" in r for r in data.requests[1:])


def _keyspaces_client():
    import boto3
    from botocore.stub import ANY, Stubber
    client = boto3.client("keyspacesstreams", region_name="us-east-1",
                          aws_access_key_id="x", aws_secret_access_key="y")
    stub = Stubber(client)
    arn = ("arn:aws:cassandra:us-east-1:<account>:/keyspace/ks/table/t"
           "/stream/1")
    stub.add_response("list_streams", {"streams": [{
        "streamArn": arn, "keyspaceName": "ks", "tableName": "t",
        "streamLabel": "2026-10-01"}]}, {"keyspaceName": "ks",
                                         "tableName": "t"})
    shard = {"shardId": "s1", "parentShardIds": []}
    stream = {"streamArn": arn, "streamLabel": "2026-10-01",
              "streamStatus": "ENABLED", "streamViewType": "NEW_IMAGE",
              "creationRequestDateTime": datetime.datetime(2026, 10, 1),
              "keyspaceName": "ks", "tableName": "t", "shards": [shard]}
    stub.add_response("get_stream", stream, {"streamArn": arn})
    stub.add_response("get_shard_iterator", {"shardIterator": "it1"},
                      {"streamArn": arn, "shardId": "s1",
                       "shardIteratorType": "TRIM_HORIZON"})
    stub.add_response("get_records", {"changeRecords": [
        {"sequenceNumber": "100",
         "partitionKeys": {"id": {"value": {"intT": "1"}}},
         "newImage": {"valueCells": {"n": {"value": {"textT": "a"}}}}},
        {"sequenceNumber": "101",
         "partitionKeys": {"id": {"value": {"intT": "2"}}}}],
        "nextShardIterator": "it2"},
        {"shardIterator": "it1", "maxResults": ANY})
    stub.add_response("get_records", {"changeRecords": []},
                      {"shardIterator": "it2", "maxResults": ANY})
    stub.activate()
    return client, stub


def test_a_keyspaces_stream_is_followed_shard_by_shard():
    """A new image is the row's values; a record with none, in a stream of
    new images, is a delete. The shard read to its end says when, and the
    fence is held to that."""
    from migkit.changetopic import Follower, KeyspacesChanges
    from migkit.engines.keyspaces import KeyspacesEngine, KeyspacesLanes
    client, stub = _keyspaces_client()
    eng = KeyspacesEngine(_hop("keyspaces"))
    eng.__dict__["_streams_client"] = client
    lanes = KeyspacesLanes(eng, "ks", "t")
    f = Follower(KeyspacesChanges(), lanes, name=lambda r: r.table,
                 shape=lambda t, r: eng._shape("ks", t, r))
    token = {"via": "keyspaces", "at": {"s1": {"from_start": True}},
             "fence": None, "marks": {}, "seen": {}}
    got, token = f.changes(token, 100)
    stub.assert_no_pending_responses()
    assert [(c["op"], c["key"]) for c in got] == [
        ("update", {"id": 1}), ("delete", {"id": 2})]
    assert got[0]["values"] == {"n": "a", "id": 1}
    caught = token["at"]["s1"]["caught"]
    assert KeyspacesEngine.position_reached(token, {"time": caught - 1})
    assert not KeyspacesEngine.position_reached(token,
                                                {"time": caught + 3600})


def _neptune_client(records):
    import boto3
    from botocore.stub import ANY, Stubber
    client = boto3.client("neptunedata", region_name="us-east-1",
                          endpoint_url="https://db.example.com:8182",
                          aws_access_key_id="x", aws_secret_access_key="y")
    stub = Stubber(client)

    def answer(recs, last):
        return {"lastEventId": {"commitNum": str(last[0]),
                                "opNum": str(last[1])},
                "lastTrxTimestampInMillis": 1, "format": "PG_JSON",
                "records": recs, "totalRecords": len(recs)}
    for _ in range(2):
        stub.add_response("get_propertygraph_stream", answer([], (5, 1)),
                          {"iteratorType": "LATEST", "limit": 1})
    stub.add_response("get_propertygraph_stream", answer(records, (7, 1)),
                      {"iteratorType": "AFTER_SEQUENCE_NUMBER",
                       "commitNum": 5, "opNum": 1, "limit": ANY})
    stub.add_client_error("get_propertygraph_stream",
                          service_error_code="StreamRecordsNotFoundException",
                          http_status_code=404)
    stub.activate()
    return client, stub


def test_neptune_streams_carry_elements_in_the_order_they_were_committed():
    from migkit.changetopic import Follower, NeptuneChanges
    from migkit.engines.neptune import OPS, NeptuneEngine, NeptuneLanes

    def rec(commit, op, eid, kind):
        return {"commitTimestampInMillis": 1, "op": "ADD",
                "eventId": {"commitNum": str(commit), "opNum": str(op)},
                "data": {"id": eid, "type": kind, "key": "k",
                         "value": {"value": "1", "dataType": "String"}}}
    records = [rec(6, 1, "v1", "vl"), rec(6, 2, "v1", "vp"),
               rec(7, 1, "e1", "e")]
    client, stub = _neptune_client(records)
    eng = _Obj(_data=lambda side: client)
    lanes = NeptuneLanes(eng, "graph")
    seen = []

    def back(table, rows, version):
        seen.append((table, [r.key["id"] for r in rows]))
        return [(r, dict(r.key), {"id": r.key["id"]}) for r in rows]
    f = Follower(NeptuneChanges(), lanes, name=lambda r: r.table,
                 shape=NeptuneEngine._shape, read_back=back)
    token = f.point(fence=lanes.latest)
    assert token["fence"] == 5 * OPS + 1
    got, token = f.changes(token, 100)
    stub.assert_no_pending_responses()
    # an element read back once, whatever number of its changes the
    # batch holds
    assert sorted(seen) == [("edges", ["e1"]), ("vertices", ["v1"])]
    assert sorted(c["key"]["id"] for c in got) == ["e1", "v1"]
    assert token["fence"] == 7 * OPS + 1
    assert NeptuneEngine.position_reached(token, {"fence": 7 * OPS + 1})
    assert not NeptuneEngine.position_reached(token, {"fence": 8 * OPS})


# ---- Lindorm -------------------------------------------------------------------

class _Cursor:
    def __init__(self, log, answers):
        self.log, self.answers = log, answers
        self.description, self.rows = None, []

    def execute(self, sql, args=None):
        self.log.append((sql, args))
        got = self.answers.get(sql.split(" ")[0].lower())
        if got:
            names, self.rows = got
            self.description = [(n,) for n in names]

    def executemany(self, sql, rows):
        self.log.append((sql, list(rows)))

    def fetchall(self):
        return list(self.rows)


class _Conn:
    def __init__(self, log, answers):
        self.log, self.answers = log, answers

    def cursor(self):
        return _Cursor(self.log, self.answers)

    def commit(self):
        self.log.append(("commit", None))

    def close(self):
        pass


def test_lindorm_writes_by_upsert_and_reads_its_columns_off_describe(
        monkeypatch):
    from migkit.engines.lindorm import LindormEngine
    log = []
    answers = {"describe": (["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                             "TYPE", "IS_PRIMARY_KEY"],
                            [("shop", "t", "id", "BIGINT", "true"),
                             ("shop", "t", "n", "VARCHAR", "false")])}
    eng = LindormEngine(_hop("lindorm"))
    monkeypatch.setattr(eng, "_connect",
                        lambda side, db: _Conn(log, answers))
    assert eng.neutral_columns("src", "shop", "t") == [("id", "bigint"),
                                                        ("n", "varchar")]
    assert eng.neutral_key("src", "shop", "t") == ["id"]
    eng.neutral_write("dst", "shop", "t", [("id", "integer"),
                                           ("n", "text")], [[1, "a"]])
    sql, rows = log[-2]
    assert sql.startswith("upsert into `shop`.`t` (`id`, `n`) values")
    assert rows == [(1, "a")]
    with pytest.raises(SystemExit, match="primary key"):
        eng.neutral_create_sql("dst", "shop", "t", [("id", "integer", ())])


# ---- their settings, deep checks, statistics and state before a repair ------

def _reported(eng, tmp_path):
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


def _by(results):
    return {r.scope: r for r in results}


class _Said:
    """A connection whose statements are answered by `answer(sql)` -
    (column names, rows) - each statement kept."""

    def __init__(self, answer, log):
        self.answer, self.log = answer, log

    def cursor(self):
        conn = self

        class Cur:
            description, rows = None, []

            def execute(self, sql, args=None):
                conn.log.append(sql)
                names, self.rows = conn.answer(sql)
                self.description = [(n,) for n in names] or None

            def fetchall(self):
                return list(self.rows)
        return Cur()

    def commit(self):
        self.log.append("commit")

    def close(self):
        pass


def test_a_gaussdb_database_made_with_another_compatibility_is_a_diff(
        monkeypatch, tmp_path):
    """An A-compatible database keeps '' as NULL and a PG-compatible one
    keeps them apart: the same rows mean other things."""
    from migkit.engines.gaussdb import GaussEngine
    eng = _reported(GaussEngine(_hop("gaussdb")), tmp_path)
    compat = {"src": "A", "dst": "PG"}

    def rows(side, db, sql, args=None):
        if "pg_settings" in sql:
            return [("timezone", "UTC"), ("shared_buffers", "1GB" if side
                                          == "src" else "4GB")]
        if "datcompatibility" in sql:
            return [(compat[side],)]
        raise AssertionError(sql)
    monkeypatch.setattr(eng, "_rows", rows)
    got = eng.check_params("shop")
    assert got[0].status == "diff" and "datcompatibility" in got[0].detail
    compat["dst"] = "A"
    got = eng.check_params("shop")
    assert got[0].status == "ok" and "none behavior-critical" in got[0].detail

    def refused(side, db, sql, args=None):
        raise PermissionError("permission denied for pg_settings")
    monkeypatch.setattr(eng, "_rows", refused)
    assert eng.check_params("shop")[0].status == "error"


def test_a_gaussdb_deep_check_finds_an_index_lacking_or_left_invalid(
        monkeypatch, tmp_path):
    from migkit.engines.gaussdb import GaussEngine
    eng = _reported(GaussEngine(_hop("gaussdb")), tmp_path)
    monkeypatch.setattr(eng, "neutral_tables",
                        lambda side, db: ["public.t", "public.u"])
    indexes = {"src": [("t_n", False, ["n"], True)], "dst": []}
    monkeypatch.setattr(eng, "neutral_indexes",
                        lambda side, db, t: indexes[side] if t == "public.t"
                        else [])
    state = {"invalid": [], "wal": "logical"}

    def rows(side, db, sql, args=None):
        if "indisvalid" in sql:
            return state["invalid"]
        if sql == "show wal_level":
            return [(state["wal"],)]
        raise AssertionError(sql)
    monkeypatch.setattr(eng, "_rows", rows)
    got = _by(eng.check_deep("shop"))
    assert got["shop indexes"].status == "diff"
    assert "public.t.t_n" in got["shop indexes"].detail
    indexes["dst"] = [("t_n", False, ["n"], True)]
    state["invalid"] = [("public", "u", "u_x")]
    got = _by(eng.check_deep("shop"))
    assert got["shop indexes"].status == "diff"
    assert "public.u.u_x" in got["shop indexes"].detail
    # an index of a table the hop leaves out is the target's own
    eng.hop.exclude = ["shop.public.u"]
    got = _by(eng.check_deep("shop"))
    assert got["shop indexes"].status == "ok"
    state["wal"] = "hot_standby"
    got = _by(eng.check_deep("shop"))
    assert got["shop source log"].status == "warn"


def test_gaussdb_statistics_and_sequences_kept_and_put_back(monkeypatch,
                                                            tmp_path):
    from migkit.engines.base import RepairAction
    from migkit.engines.gaussdb import GaussEngine
    eng = _reported(GaussEngine(_hop("gaussdb")), tmp_path)
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["public.t"])
    log = []
    monkeypatch.setattr(eng, "_connect", lambda side, db: _Said(
        lambda sql: ([], []), log))
    assert "1 tables" in eng.settle_target("shop")
    assert log == ['analyze "public"."t"', "commit"]

    def rows(side, db, sql, args=None):
        if "relkind = 'S'" in sql:
            return [("public", "t_id_seq"), ("other", "x_seq")]
        assert sql.endswith('"public"."t_id_seq"'), sql
        return [(41, True)]
    monkeypatch.setattr(eng, "_rows", rows)
    eng.snapshot_state("shop", tmp_path)
    assert (tmp_path / "dst-gauss-sequences.txt").read_text() == \
        "public.t_id_seq|41|t\n"
    back = eng.restore_sequences("shop", tmp_path)
    assert back == ["""select setval('"public"."t_id_seq"', 41, true);"""]
    ran = []
    monkeypatch.setattr(eng, "_psql", lambda side, db, sql: ran.append(
        (side, sql)))
    monkeypatch.setattr(eng, "_as_pair", lambda: (_ for _ in ()).throw(
        AssertionError("a sequence goes through no pair")))
    eng.apply("shop", RepairAction("shop", "sequences", back, []))
    assert ran == [("dst", back[0])]


def test_a_spanner_snapshot_keeps_its_counters_and_says_why_no_backup(
        monkeypatch, tmp_path):
    from migkit.engines.base import RepairAction
    from migkit.engines.spanner import SpannerEngine
    eng = SpannerEngine(_hop("spanner"))
    monkeypatch.setattr(eng, "_sequences", lambda side, db: {"seq_a": 7})
    monkeypatch.setattr(eng, "_pg", lambda side, db: False)

    class NoBackups:
        def backup(self, *a, **kw):
            raise NotImplementedError("backups are not supported by the"
                                      " emulator")
    monkeypatch.setattr(eng, "_instance", lambda side: NoBackups())
    monkeypatch.setattr(eng, "_database", lambda side, db: object())
    eng.snapshot_state("shop", tmp_path)
    kept = json.loads((tmp_path / "dst-spanner.json").read_text())
    assert kept["sequences"] == {"seq_a": 7}
    assert "emulator" in kept["no_backup"]
    back = eng.restore_sequences("shop", tmp_path)
    assert back == ["alter sequence `seq_a` set options"
                    " (start_with_counter = 8)"]
    ran = []
    monkeypatch.setattr(eng, "execute_ddl", lambda side, db, sql: ran.append(
        (side, sql)))
    eng.apply("shop", RepairAction("shop", "sequences", back, []))
    assert ran == [("dst", back[0])]


def test_a_bigtable_copy_not_ready_on_a_cluster_is_a_diff(monkeypatch,
                                                         tmp_path):
    import types

    from google.cloud.bigtable.enums import Table as T

    from migkit.engines.bigtable import BigtableEngine
    eng = BigtableEngine(_hop("bigtable"))
    states = {"c1": T.ReplicationState.READY,
              "c2": T.ReplicationState.INITIALIZING}

    class Table:
        def get_cluster_states(self):
            return {c: types.SimpleNamespace(replication_state=s)
                    for c, s in states.items()}
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["t"])
    monkeypatch.setattr(eng, "_table", lambda side, db, t: Table())
    monkeypatch.setattr(eng, "_described", lambda side, db, t: None)
    retention = {"t": None}
    monkeypatch.setattr(eng, "_retention_s", lambda db, t: retention[t])
    got = _by(eng.check_deep("shop"))
    assert got["shop target replication"].status == "diff"
    assert "c2 initializing" in got["shop target replication"].detail
    assert got["shop source change streams"].status == "warn"
    states["c2"] = T.ReplicationState.READY
    retention["t"] = 86400.0
    got = _by(eng.check_deep("shop"))
    assert got["shop target replication"].status == "ok"
    assert "shop source change streams" not in got


def test_firestore_indexes_and_time_to_live_both_sides(monkeypatch,
                                                       tmp_path):
    import enum
    import types

    from migkit.engines.firestore import FirestoreEngine

    class E(enum.IntEnum):
        UNSET = 0
        ASCENDING = 1
        COLLECTION = 2
        READY = 3
        CREATING = 4
        ACTIVE = 5
        FIRESTORE_NATIVE = 6
        OPTIMISTIC = 7
        POINT_IN_TIME_RECOVERY_ENABLED = 8

    def index(group, field, state):
        f = types.SimpleNamespace(field_path=field, order=E.ASCENDING,
                                  array_config=E.UNSET)
        return types.SimpleNamespace(
            name=f"projects/p/databases/(default)/collectionGroups/{group}"
                 "/indexes/i1", fields=[f], query_scope=E.COLLECTION,
            state=state)
    have = {"src": [index("users", "age", E.READY),
                    index("audit", "at", E.READY)],
            "dst": [index("users", "age", E.CREATING)]}
    ttl = {"src": ["users/fields/expires"], "dst": []}

    class Admin:
        def __init__(self, side):
            self.side = side

        def list_indexes(self, parent):
            assert parent.endswith("/collectionGroups/-")
            return have[self.side]

        def list_fields(self, parent, filter):
            assert filter == "ttlConfig:*"
            return [types.SimpleNamespace(
                name=f"projects/p/databases/(default)/collectionGroups/{f}",
                ttl_config=types.SimpleNamespace(state=E.ACTIVE))
                for f in ttl[self.side]]

        def get_database(self, name):
            return types.SimpleNamespace(
                type=E.FIRESTORE_NATIVE, concurrency_mode=E.OPTIMISTIC,
                point_in_time_recovery_enablement=E
                .POINT_IN_TIME_RECOVERY_ENABLED,
                app_engine_integration_mode=E.UNSET,
                delete_protection_state=E.UNSET, location_id="eur3")
    eng = _reported(FirestoreEngine(_hop("firestore")), tmp_path)
    monkeypatch.setattr(eng, "_admin", lambda side: Admin(side))
    monkeypatch.setattr(eng, "_db_name",
                        lambda side, db: "projects/p/databases/(default)")
    eng.hop.exclude = ["shop.audit"]
    deep = eng.check_deep("shop")[0]
    assert deep.status == "diff" and "users(age) CREATING" in deep.detail
    have["dst"] = [index("users", "age", E.READY)]
    deep = eng.check_deep("shop")[0]
    assert deep.status == "ok", deep.detail
    # a field the source deletes documents by, and the target does not
    params = eng.check_params("shop")[0]
    assert params.status == "diff" and "users.expires ttl" in params.detail
    ttl["dst"] = ["users/fields/expires"]
    assert eng.check_params("shop")[0].status == "ok"
    eng.snapshot_state("shop", tmp_path)
    kept = json.loads((tmp_path / "dst-firestore.json").read_text())
    assert kept["kept"] == "seven days" and kept["at"].endswith("Z")


def test_neptune_settings_and_its_stream_read_off_its_engine_status(
        monkeypatch, tmp_path):
    from migkit.engines.neptune import NeptuneEngine
    hop = _hop("neptune")
    hop.source.host = "src.cluster-abc.us-east-1.neptune.amazonaws.com"
    hop.target.host = "dst.cluster-abc.us-east-1.neptune.amazonaws.com"
    eng = _reported(NeptuneEngine(hop), tmp_path)
    status = {
        side: {"status": "healthy", "dbEngineVersion": "1.3.2.0",
               "dfeQueryEngine": "viaQueryHint",
               "settings": {"clusterQueryTimeoutInMs": timeout},
               "labMode": {"ObjectIndex": "disabled"},
               "features": {"Streams": "disabled"},
               "gremlin": {"version": "tinkerpop-3.7.2"}}
        for side, timeout in (("src", "120000"), ("dst", "60000"))}

    class Data:
        def __init__(self, side):
            self.side = side

        def get_engine_status(self):
            return status[self.side]

        def get_propertygraph_statistics(self):
            return {"status": "200", "payload": {"active": False}}
    monkeypatch.setattr(eng, "_data", lambda side: Data(side))
    monkeypatch.setattr(eng, "_model", lambda side, db: "graph")
    got = eng.check_params("shop")[0]
    assert got.status == "diff" and "clusterQueryTimeoutInMs" in got.detail
    deep = _by(eng.check_deep("shop"))
    assert deep["shop target statistics"].status == "warn"
    assert deep["shop source stream"].status == "warn"
    status["src"]["features"]["Streams"] = {"enabled": True}
    deep = _by(eng.check_deep("shop"))
    assert "shop source stream" not in deep
    # a Gremlin server keeps none of Neptune's settings: said, not passed
    hop.source.host = hop.target.host = "127.0.0.1"
    assert eng.check_params("shop")[0].status == "error"


def test_lindorm_table_attributes_and_indexes_both_sides(monkeypatch,
                                                         tmp_path):
    from migkit.engines.lindorm import LindormEngine
    eng = _reported(LindormEngine(_hop("lindorm")), tmp_path)
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["t"])
    state = {"src ttl": "2592000", "dst ttl": "", "dst index": "BUILDING"}

    def answer_for(side):
        def answer(sql):
            if sql.startswith("show table variables"):
                return (["VARIABLE_NAME", "VALUE"],
                        [("TTL", state[f"{side} ttl"]),
                         ("COMPRESSION", "ZSTD")])
            if sql.startswith("show index"):
                return (["TABLE_NAME", "INDEX_NAME", "INDEX_TYPE",
                         "INDEX_STATE", "INDEX_PROGRESS"],
                        [("t", "idx_n", "SECONDARY",
                          "ACTIVE" if side == "src" else state["dst index"],
                          "100%" if side == "src" else "N/A")])
            raise AssertionError(sql)
        return answer
    monkeypatch.setattr(eng, "_connect", lambda side, db: _Said(
        answer_for(side), []))
    got = eng.check_params("shop")[0]
    assert got.status == "diff" and "t TTL" in got.detail
    state["dst ttl"] = "2592000"
    assert eng.check_params("shop")[0].status == "ok"
    deep = eng.check_deep("shop")[0]
    assert deep.status == "diff" and "t.idx_n BUILDING" in deep.detail
    state["dst index"] = "ACTIVE"
    assert eng.check_deep("shop")[0].status == "ok"


def test_oceanbase_oracle_mode_sequences_kept_compared_and_put_back(
        monkeypatch, tmp_path):
    from migkit.engines.oceanbase import OceanBaseOracleEngine
    eng = _reported(OceanBaseOracleEngine(_hop("oceanbase-oracle")),
                    tmp_path)
    last = {"src": [("ORDERS_SEQ", 1020), ("AUDIT_SEQ", 5)],
            "dst": [("ORDERS_SEQ", 1000)]}
    monkeypatch.setattr(eng, "_rows", lambda side, db, sql, args=None:
                        last[side] if "all_sequences" in sql else [])
    got = eng.check_autoinc("shop")[0]
    assert got.status == "diff"
    # a name in capitals is said in small letters, written in capitals
    assert "orders_seq source next 1020 target 1000" in got.detail
    assert "audit_seq source next 5 target missing" in got.detail
    assert got.fix_hint == ('alter sequence "SHOP"."ORDERS_SEQ" restart'
                            ' start with 1020')
    last["dst"] = [("ORDERS_SEQ", 1040), ("AUDIT_SEQ", 5)]
    assert eng.check_autoinc("shop")[0].status == "ok"
    eng.snapshot_state("shop", tmp_path)
    back = eng.restore_sequences("shop", tmp_path)
    assert back == [eng._restart_sql("shop", "AUDIT_SEQ", 5) + ";",
                    eng._restart_sql("shop", "ORDERS_SEQ", 1040) + ";"]
    assert back[1].startswith("alter sequence ") and back[1].endswith(
        "ORDERS_SEQ\" restart start with 1040;")


def test_a_distributed_mysql_targets_counters_kept_and_put_back(
        monkeypatch, tmp_path):
    from migkit.engines.base import RepairAction
    from migkit.engines.distmysql import PolarDBXEngine, TdsqlEngine
    eng = TdsqlEngine(_hop("dcdb"))
    ran = []

    def q(side, sql, args=None, fresh=False):
        ran.append(sql)
        if "auto_increment" in sql and "information_schema" in sql:
            return [("orders", 501), ("left_out", 9)]
        if sql.startswith("show create table"):
            return [("orders", "CREATE TABLE `orders` (`id` int)"
                               " shardkey=id")]
        return []
    monkeypatch.setattr(eng, "_q", q)
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["orders"])
    eng.hop.exclude = ["shop.left_out"]
    eng.snapshot_state("shop", tmp_path)
    assert (tmp_path / "dst-wire-counters.txt").read_text() == "orders|501\n"
    assert "shardkey=id" in (tmp_path / "dst-schema.sql").read_text()
    back = eng.restore_sequences("shop", tmp_path)
    assert back == ["alter table `shop`.`orders` auto_increment = 501;"]
    ran.clear()
    monkeypatch.setattr(eng, "_as_pair", lambda: (_ for _ in ()).throw(
        AssertionError("a counter goes through no pair")))
    eng.apply("shop", RepairAction("shop", "sequences", back, []))
    assert ran == ["alter table `shop`.`orders` auto_increment = 501"]
    pxc = PolarDBXEngine(_hop("polardbx"))
    monkeypatch.setattr(pxc, "_sequences", lambda side: {
        "AUTO_SEQ_ORDERS": 77})
    monkeypatch.setattr(pxc, "neutral_tables", lambda side, db: ["orders"])
    assert pxc._counters("dst", "shop") == {"orders": 77}
    assert pxc._restart_sql("shop", "orders", 77) == \
        "alter sequence AUTO_SEQ_orders start with 77"


def test_a_timestream_snapshot_counts_each_tables_records(monkeypatch,
                                                         tmp_path):
    from migkit.engines.timestream import TimestreamEngine
    eng = TimestreamEngine(_hop("timestream"))
    monkeypatch.setattr(eng, "neutral_tables", lambda side, db: ["cpu"])
    monkeypatch.setattr(eng, "_settings", lambda side, db: {
        "cpu memory hours": "24"})
    monkeypatch.setattr(eng, "_query", lambda side, sql: (
        [("_col0", "BIGINT")], [[1234]]))
    eng.snapshot_state("shop", tmp_path)
    kept = json.loads((tmp_path / "dst-timestream.json").read_text())
    assert kept == {"records": {"cpu": 1234},
                    "settings": {"cpu memory hours": "24"}}


def test_a_name_on_another_engines_wire_is_read_as_that_ones():
    from migkit.util import split_table
    assert split_table("oceanbase", "orders.v1") == ("", "orders.v1")
    assert split_table("gaussdb", "public.orders") == ("public", "orders")
    assert split_table("keyspaces", "orders.v1") == ("", "orders.v1")
    assert split_table("bigtable", "events.2026") == ("", "events.2026")
    assert _hop("oceanbase").whole_name() == 2
    assert _hop("keyspaces").whole_name() == 2
    assert _hop("bigtable").whole_name() == 0
    assert _hop("gaussdb").whole_name() == 3


def test_a_cloud_native_listing_takes_only_what_the_hop_takes(monkeypatch):
    import types

    from migkit.engines.bigtable import BigtableEngine
    eng = BigtableEngine(_hop("bigtable"))
    eng.hop.include = ["shop.orders"]

    class Instance:
        def list_tables(self):
            return [types.SimpleNamespace(table_id=t)
                    for t in ("orders", "audit")]
    monkeypatch.setattr(eng, "_instance", lambda side, db: Instance())
    assert eng.neutral_tables("src", "shop") == ["orders"]
