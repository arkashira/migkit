"""The services on another's wire take large values and partitions the way
their documentation says (P0-CLOUDS.native, P0-DE.4): the parent's adapter
where the wire is the same, the service's own bound and catalogue where it
differs, each a rung on a fact read off the service. Run on fakes: no
sandbox here runs these services, and each module says so (`UNPROVED_ON`).
"""
from types import SimpleNamespace

from migkit import capabilities, decide, pieces
from migkit.engines.gaussdb import GaussEngine
from migkit.engines.oceanbase import OceanBaseEngine, OceanBaseOracleEngine

SERVICES = ("gaussdb", "oceanbase", "oceanbase-oracle", "polardbx", "dcdb",
            "lindorm", "geminidb-cassandra", "geminidb-redis",
            "geminidb-mongo", "keyspaces")


def test_oceanbase_widest_value_is_the_rung_its_version_holds():
    assert decide.ob_version("5.7.25-OceanBase-v4.2.1.0") == (4, 2)
    assert decide.ob_version("OceanBase 3.2.4.0") == (3, 2)
    assert decide.ob_version("8.0.36") is None
    four = decide.oceanbase_value_bound({"version": (4, 2)})
    three = decide.oceanbase_value_bound({"version": (3, 2)})
    unread = decide.oceanbase_value_bound({"version": None})
    assert (four.rung.name, three.rung.name, unread.rung.name) == (
        "v4", "v0", "unread")
    assert decide.oceanbase_value_most(four) == 512 * 2 ** 20
    assert decide.oceanbase_value_most(three) == 48 * 2 ** 20
    # a version not read is no guessed bound: the packet alone holds
    assert decide.oceanbase_value_most(unread) is None


def _mysql_mode(version):
    eng = object.__new__(OceanBaseEngine)
    eng._q = lambda side, sql: ([("67108864",)] if "max_allowed_packet"
                                in sql else [(version,)])
    return eng


def test_a_mysql_mode_tenant_holds_the_packet_and_its_versions_bound():
    got = {(l.name, l.most, l.bounds, l.read)
           for l in _mysql_mode("5.7.25-OceanBase-v4.2.1.0").piece_limits(
               "dst", "app")}
    assert ("max_allowed_packet", 67108864, "sent", True) in got
    assert ("OceanBase's widest value", 512 * 2 ** 20, "row", False) in got
    old = _mysql_mode("5.7.25-OceanBase-v3.2.4.0").piece_limits("dst", "a")
    assert [l.most for l in old if l.bounds == "row"] == [48 * 2 ** 20]
    # the MySQL wire's adapter is inherited, not copied
    assert OceanBaseEngine.LARGE_VALUES is not None
    assert "LARGE_VALUES" not in vars(OceanBaseEngine)


def _oracle_mode(rows):
    eng = object.__new__(OceanBaseOracleEngine)
    eng._rows = rows
    eng._owner = lambda side, db: "APP"
    eng._said = lambda n: str(n).lower()
    eng._q = lambda n: '"' + str(n).upper() + '"'
    eng.in_scope = lambda db, t: True
    return eng


def test_an_oracle_mode_tenant_sizes_a_lob_by_the_server_and_bounds_it():
    way = OceanBaseOracleEngine.LARGE_VALUES
    expr = way.row_expr(lambda n: f'"{n}"', [("doc", "CLOB"), ("id", "NUMBER")])
    # a character LOB at four bytes a character: never under what it stores
    assert "4 * dbms_lob.getlength(\"doc\")" in expr and "8" in expr
    assert way.piece is None  # read whole: no part of a LOB past a string
    eng = _oracle_mode(lambda side, db, sql, args=None: [("OceanBase 3.2.4.0",)])
    assert [l.most for l in eng.piece_limits("src", "app")] == [48 * 2 ** 20]
    eng = _oracle_mode(lambda side, db, sql, args=None: [("Oracle",)])
    assert eng.piece_limits("src", "app") == []


def _hop(tmp_path):
    return SimpleNamespace(name="h", report_dir=lambda db: tmp_path)


def test_an_oracle_mode_partition_that_arrived_short_is_named(tmp_path):
    asked = []

    def rows(side, db, sql, args=None):
        asked.append(sql)
        if "all_tab_partitions" in sql:
            return [("ORDERS", "RANGE", "P1", "100"),
                    ("ORDERS", "RANGE", "P2", "MAXVALUE")]
        short = side == "dst" and '"P2"' in sql
        return [(3 if short else 5,)]
    eng = _oracle_mode(rows)
    eng.hop = _hop(tmp_path)
    got = eng._deep_partitions("app")
    assert got.status == "diff" and "P2" in got.detail
    assert any('partition ("P2")' in s for s in asked)
    eng._rows = lambda side, db, sql, args=None: (
        rows("src", db, sql) if "all_tab_partitions" in sql else [(5,)])
    assert eng._deep_partitions("app").status == "ok"


def _gauss(tmp_path, catalogue, counts):
    asked = []

    def rows(side, db, sql, args=None):
        asked.append(sql)
        if "relname in ('pg_partition'" in sql:
            return [(n,) for n in catalogue]
        if "from pg_partition p" in sql or "from pg_partitioned_table" in sql:
            return [("app.orders", "r", "p1", "100"),
                    ("app.orders", "r", "p2", "200")]
        return [(counts(side, sql),)]
    eng = object.__new__(GaussEngine)
    eng._rows = rows
    eng._quote_ident = lambda n: f'"{n}"'
    eng.in_scope = lambda db, t: True
    eng.hop = _hop(tmp_path)
    return eng, asked


def test_gaussdb_partitions_are_read_from_the_catalogue_it_keeps(tmp_path):
    eng, asked = _gauss(tmp_path, ["pg_partition", "pg_partitioned_table"],
                        lambda side, sql: 4)
    assert eng._deep_partitions("app").status == "ok"
    # its own PARTITION BY: each partition counted by PARTITION (p)
    assert any('"app"."orders" partition ("p1")' in s for s in asked)
    # a server with only the declarative catalogue: the child tables
    eng, asked = _gauss(tmp_path, ["pg_partitioned_table"],
                        lambda side, sql: 2 if side == "dst" and '"p2"' in sql
                        else 4)
    got = eng._deep_partitions("app")
    assert got.status == "diff" and "p2" in got.detail
    assert any(s.endswith('from "app"."p2"') for s in asked)
    eng, _ = _gauss(tmp_path, [], lambda side, sql: 4)
    assert eng._deep_partitions("app").detail == "no partitioned tables"


def test_the_catalogue_rung_turns_on_what_the_server_has():
    pick = decide.gauss_partition_catalogue
    assert pick({"pg_partition": True, "pg_partitioned_table": True}
                ).rung.name == "own"
    assert pick({"pg_partition": False, "pg_partitioned_table": True}
                ).rung.name == "declarative"
    assert pick({}).rung is None


def test_a_gaussdb_field_is_bounded_by_the_services_quota():
    eng = object.__new__(GaussEngine)
    (limit,) = eng.piece_limits("dst", "app")
    assert (limit.most, limit.bounds, limit.read) == (2 ** 30, "row", False)
    way = GaussEngine.LARGE_VALUES
    assert way.holds("clob") and way.holds("nvarchar2") and way.holds("bytea")
    assert isinstance(way, pieces.Sql)


def test_each_service_cell_is_a_way_or_says_why_not():
    for name in SERVICES:
        for shape in ("large values", "partitioned"):
            state, why = decide.cell(name, shape)
            assert state in ("yes", decide.NOT_APPLICABLE, decide.NOT_YET)
            if state != "yes":
                assert why
    assert decide.cell("keyspaces", "large values")[0] == decide.NOT_APPLICABLE
    assert decide.cell("gaussdb", "partitioned") == ("yes", "@_deep_partitions")


def test_each_service_module_says_which_server_it_was_not_run_on():
    for name in SERVICES:
        assert capabilities.UNPROVED_WHY[name].startswith(
            "built; not run on a real ")
    assert capabilities.depth_why("gaussdb", "move.large-values").startswith(
        "built; not run on a real GaussDB")
    assert not any(capabilities.depth_failing().values())
