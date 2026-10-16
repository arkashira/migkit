"""The schema comparer reads every engine's column rules (NULL, default,
numbered) even where the engine has no one-pass catalogue, and the
engines that keep settings beyond columns give them (time to live, table
options, row deletion policy, index settings); a renamed table carries
its settings across the rename (backlog AUD-H, W14F)."""
from types import SimpleNamespace

from migkit import schemaformats, verifiers
from migkit.engines.cassandra import CassandraEngine
from migkit.engines.cosmosdb import CosmosEngine
from migkit.engines.dynamodb import DynamoDBEngine
from migkit.engines.hetero import HeteroEngine
from migkit.engines.opensearch import OpenSearchEngine
from migkit.engines.spanner import SpannerEngine

class Hop:
    def __init__(self, name):
        self.name = name

    def takes_table(self, db, table):
        return True


class Side:
    """A neutral reader with no one-pass catalogue, as Teradata's is."""

    CANON_ENGINE = "teradata"

    def __init__(self, rules, tables=None, traits=None, broken_rules=False):
        self.rules, self.traits = rules, traits or {}
        self.tables = tables or {"orders": ["id", "status"]}
        self.broken_rules = broken_rules
        if traits is not None:
            self.neutral_traits = lambda side, db, t: self.traits.get(t, {})

    def neutral_tables(self, side, db):
        return list(self.tables)

    def neutral_columns(self, side, db, table):
        return [(c, "VARCHAR(10)") for c in self.tables[table]]

    def neutral_key(self, side, db, table):
        return ["id"]

    def neutral_indexes(self, side, db, table):
        return []

    def neutral_column_rules(self, side, db, table):
        if self.broken_rules:
            raise TimeoutError("catalogue timed out")
        return self.rules.get(table, {})


def _rules(null=True, default=None, identity=False):
    return {"orders": {
        "id": {"null": False, "default": None, "identity": identity},
        "status": {"null": null, "default": default, "identity": False}}}


def _check(name, src, dst, **extra):
    both = SimpleNamespace(hop=Hop(name), src_engine=src, dst_engine=dst,
                           **extra)
    return verifiers.compare_schema(both, "app")


def test_an_equal_pair_with_rules_says_ok():
    got = _check("rules-equal", Side(_rules(False, "'new'")),
                 Side(_rules(False, "'new'")))
    assert [r.status for r in got] == ["ok"]


def test_a_column_made_not_null_on_the_source_only_is_a_diff():
    got = _check("rules-null", Side(_rules(False)), Side(_rules(True)))
    assert got[0].status == "diff" and got[0].scope == "app.orders"
    assert "status is NOT NULL on the source" in got[0].detail


def test_a_default_only_one_side_has_is_a_diff():
    got = _check("rules-default", Side(_rules(True, "'new'")),
                 Side(_rules(True)))
    assert got[0].status == "diff"
    assert "status defaults to 'new' on the source" in got[0].detail


def test_a_default_that_differs_on_one_engine_is_a_diff():
    got = _check("rules-default-value", Side(_rules(True, "'new'")),
                 Side(_rules(True, "'old'")))
    assert got[0].status == "diff" and "'old' on the target" in got[0].detail


def test_a_column_numbered_on_one_side_only_is_a_diff():
    got = _check("rules-identity", Side(_rules(identity=True)),
                 Side(_rules()))
    assert got[0].status == "diff"
    assert "id is identity on the source and plain on the target" \
        in got[0].detail


def test_an_engine_that_cannot_say_its_rules_is_read_as_before():
    got = _check("rules-none", Side({}), Side({}))
    assert [r.status for r in got] == ["ok"]


def test_rules_that_could_not_be_read_leave_the_table_named_not_ok():
    got = _check("rules-broken", Side(_rules()),
                 Side(_rules(), broken_rules=True))
    assert [r.status for r in got] == ["skip"]
    assert "orders" in got[0].detail and "could not be read" in got[0].detail


def test_the_column_helper_reads_a_rule_into_the_catalogue_column():
    c = verifiers.column_with_rules(
        "n", "int", {"null": False, "default": 7, "identity": True})
    assert (c.nullable, c.default, c.extra) == (False, "7", "identity")
    plain = verifiers.column_with_rules("n", "int", None)
    assert (plain.nullable, plain.default, plain.extra) == (True, None, "")


# --- each engine's traits, read through its own method on a fake service ----

def _bare(cls, **attrs):
    one = object.__new__(cls)
    one.__dict__.update(attrs)
    return one


class FakeDynamo:
    def __init__(self, ttl="DISABLED", stream=False, ask_ttl=True):
        self.ttl, self.stream, self.ask_ttl = ttl, stream, ask_ttl

    def describe_table(self, TableName):
        return {"Table": {"StreamSpecification": {
            "StreamEnabled": self.stream, "StreamViewType": "NEW_IMAGE"}}}

    def describe_time_to_live(self, TableName):
        if not self.ask_ttl:
            raise RuntimeError("UnknownOperationException: not here")
        return {"TimeToLiveDescription": {
            "TimeToLiveStatus": self.ttl, "AttributeName": "expires_at"}}


def _dynamo(client):
    return _bare(DynamoDBEngine, _client=lambda side: client,
                 _name=lambda side, db, t: t)


def test_dynamodb_gives_the_expiry_attribute_and_the_stream():
    one = _dynamo(FakeDynamo("ENABLED", True))
    assert one.neutral_traits("src", "app", "orders") == {
        "time to live": "expires_at", "stream": "NEW_IMAGE"}
    assert _dynamo(FakeDynamo()).neutral_traits("src", "app", "orders") == {}


def test_a_dynamodb_time_to_live_only_the_source_has_stops_the_cutover():
    src = _dynamo(FakeDynamo("ENABLED"))
    dst = _dynamo(FakeDynamo("DISABLED"))
    for e in (src, dst):
        e.neutral_tables = lambda side, db: ["orders"]
        e.neutral_columns = lambda side, db, t: [("id", "S")]
        e.neutral_key = lambda side, db, t: ["id"]
        e.neutral_indexes = lambda side, db, t: []
        e.CANON_ENGINE = "dynamodb"
    got = _check("dyn-ttl", src, dst)
    assert got[0].status == "diff"
    assert "time to live is expires_at on the source and not set" \
        in got[0].detail


def test_dynamodb_that_does_not_answer_time_to_live_is_named_not_passed():
    one = _dynamo(FakeDynamo(ask_ttl=False))
    try:
        one.neutral_traits("src", "app", "orders")
    except RuntimeError as e:
        assert "not here" in str(e)
    else:
        raise AssertionError("an unanswered time to live read as none")


class FakeContainer:
    def __init__(self, props):
        self.props = props

    def read(self):
        return self.props


def test_cosmosdb_gives_both_time_to_lives_and_none_when_unset():
    one = _bare(CosmosEngine, _container=lambda side, db, t: FakeContainer(
        {"defaultTtl": -1, "analyticalStorageTtl": 3600}))
    assert one.neutral_traits("src", "app", "c") == {
        "default ttl": "-1", "analytical storage ttl": "3600"}
    bare = _bare(CosmosEngine,
                 _container=lambda side, db, t: FakeContainer({}))
    assert bare.neutral_traits("src", "app", "c") == {}


def test_cosmosdb_ttl_zero_is_a_value_not_an_absence():
    one = _bare(CosmosEngine, _container=lambda side, db, t: FakeContainer(
        {"defaultTtl": 0}))
    assert one.neutral_traits("src", "app", "c") == {"default ttl": "0"}


def test_cassandra_gives_every_tables_options_from_one_listing():
    rows = [{"table_name": "t", "default_time_to_live": 86400,
             "gc_grace_seconds": 864000,
             "compaction": {"class": "TimeWindowCompactionStrategy"},
             "compression": {"sstable_compression": "LZ4"}, "cdc": None,
             "bloom_filter_fp_chance": 0.01},
            {"table_name": "t_scylla_cdc_log", "default_time_to_live": 1}]
    asked = []
    one = _bare(CassandraEngine,
                _table_rows=lambda side, db: asked.append(side) or rows)
    got = one.neutral_traits_all("src", "app")
    assert asked == ["src"] and list(got) == ["t"]
    assert got["t"]["default_time_to_live"] == "86400"
    assert got["t"]["compaction"] == '{"class": "TimeWindowCompactionStrategy"}'
    assert "cdc" not in got["t"] and "bloom_filter_fp_chance" not in got["t"]


def test_a_cassandra_default_ttl_that_differs_names_both_values():
    def side(ttl):
        return _bare(CassandraEngine, CANON_ENGINE="cassandra",
                     neutral_tables=lambda s, d: ["t"],
                     neutral_columns=lambda s, d, t: [("id", "int")],
                     neutral_key=lambda s, d, t: ["id"],
                     neutral_indexes=lambda s, d, t: [],
                     _table_rows=lambda s, d: [{
                         "table_name": "t", "default_time_to_live": ttl}])
    got = _check("cass-ttl", side(0), side(3600))
    assert got[0].status == "diff"
    assert "default_time_to_live is 0 on the source and 3600" in got[0].detail


def test_spanner_gives_the_deletion_policy_and_the_interleave():
    tables = {"orders": {"row_deletion_policy_expression":
                         "OLDER_THAN(created, INTERVAL 30 DAY)",
                         "parent_table_name": "", "on_delete_action": "NO ACTION"},
              "lines": {"parent_table_name": "orders",
                        "on_delete_action": "CASCADE"}}
    one = _bare(SpannerEngine, _tables_named=lambda side, db: tables)
    got = one.neutral_traits_all("src", "app")
    assert got["orders"] == {
        "row deletion policy": "OLDER_THAN(created, INTERVAL 30 DAY)"}
    assert got["lines"] == {"parent table": "orders", "on delete": "CASCADE"}


def test_opensearch_gives_storage_settings_analysis_and_mapping_options():
    described = {
        "settings": {"index": {
            "number_of_shards": "3", "number_of_replicas": "1",
            "codec": "best_compression", "default_pipeline": "stamp",
            "analysis": {"analyzer": {"folded": {"type": "custom"}}}}},
        "mappings": {"dynamic": "strict", "properties": {}}}
    one = _bare(OpenSearchEngine,
                _described_index=lambda side, db, t: described)
    got = one.neutral_traits("src", "app", "orders")
    assert got["setting number_of_shards"] == "3"
    assert got["setting codec"] == "best_compression"
    assert got["setting default_pipeline"] == "stamp"
    assert "setting number_of_replicas" not in got
    assert "folded" in got["analysis"]
    assert got["mapping dynamic"] == '"strict"'


# --- a mapping that renames a table carries its traits ----------------------

class Renaming(SimpleNamespace):
    match_tables = staticmethod(HeteroEngine.match_tables)

    def _renamer(self, db):
        return lambda i: {"orders": "orders_v2"}.get(i, i)

    def _mapped_types(self, db, table, cols):
        return None, {}


class Keeper:
    CANON_ENGINE = "dynamodb"

    def __init__(self, table, traits, broken=False):
        self.table, self.traits, self.broken = table, traits, broken

    def neutral_tables(self, side, db):
        return [self.table]

    def neutral_columns(self, side, db, t):
        return [("id", "S")]

    def neutral_key(self, side, db, t):
        return ["id"]

    def neutral_indexes(self, side, db, t):
        return []

    def neutral_traits(self, side, db, t):
        if self.broken:
            raise TimeoutError("settings timed out")
        return dict(self.traits)


def _renamed(src, dst):
    both = Renaming(hop=Hop("renamed"), src_engine=src, dst_engine=dst)
    return verifiers.compare_schema(both, "app")


def test_a_renamed_table_with_equal_traits_says_ok():
    ttl = {"time to live": "expires_at"}
    got = _renamed(Keeper("orders", ttl), Keeper("orders_v2", ttl))
    assert [r.status for r in got] == ["ok"]


def test_a_renamed_table_whose_trait_differs_is_a_diff_under_the_source_name():
    got = _renamed(Keeper("orders", {"time to live": "expires_at"}),
                   Keeper("orders_v2", {}))
    assert got[0].status == "diff" and got[0].scope == "app.orders"
    assert "time to live is expires_at on the source and not set" \
        in got[0].detail


def test_a_renamed_table_unread_on_one_side_is_no_false_difference():
    got = _renamed(Keeper("orders", {"time to live": "expires_at"}),
                   Keeper("orders_v2", {}, broken=True))
    assert [r.status for r in got] == ["warn"]
    assert "settings" in got[0].detail


def test_a_renamed_source_unread_does_not_leave_the_target_compared():
    got = _renamed(Keeper("orders", {}, broken=True),
                   Keeper("orders_v2", {"time to live": "expires_at"}))
    assert all(r.status != "diff" for r in got)
    assert got[0].status == "warn"


def test_a_trait_naming_a_renamed_table_is_carried_with_it():
    def shapes(parent):
        t = schemaformats.NeutralTable([], traits={"parent table": parent})
        return {"lines": t}
    a, b = shapes("orders"), shapes("orders_v2")
    same = schemaformats.compare(a, b, True, [("lines", "lines")])
    assert [d.kind for d in same.diffs()] == ["setting"]
    carried = schemaformats.compare(
        a, b, True, [("lines", "lines"), ("orders", "orders_v2")])
    assert [d for d in carried.diffs() if d.kind == "setting"] == []
