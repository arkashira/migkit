"""The bulk paths take exactly what the hop takes (backlog P0-SCOPE.6d).

The audit at ee1032d found the whole-database ways of moving reading the
hop's scope their own way, each wrong where the check was right:

* A partition was judged by its own name. The hop's rules name the table
  the application names - `exclude`, `include`, `mapping.where`, a column
  rule, a rename all say `public.events` - so an excluded `events` the
  target owns had its partitions emptied and filled with the source's
  rows, an `include` of it carried none of them, and a filter on it
  carried them unfiltered. `-T public.events` left out the parent's own
  rows, which it has none of.
* The one-pass MySQL-to-PostgreSQL load asked `exclude` alone, so a hop
  that only `include`s got the whole database, and the step emptying the
  target judged `public.orders` against `appdb.orders`.
* `-T public.AuditLog` went to the dump unquoted, which folds it to lower
  case and leaves `"AuditLog"` in; `excluded_tables` split names at every
  dot.
* A table the hop renames went every same-engine way under the source's
  name, and was made under it when the target lacked it.
* A filtered table was emptied whole before the load, though the move
  carries only the rows its filter selects.
* A table the target has and the source does not was emptied, and its
  indexes and triggers taken off, as "a table the hop does not exclude".
* On an empty target, a view or a foreign key over a table the hop leaves
  out made the restore of the schema fail - the key after the rows were in.
* The online sync and the parallel dump were given names resolved before
  they started, where they read patterns.
* The parallel dump's filter file could hold two `where =` lines for one
  table.
"""
import re
from fnmatch import fnmatchcase

import pytest

from migkit import movers, planner
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _hop(engine="postgres", **kw):
    return Hop(name="sc", engine=engine,
               source=Endpoint(host="127.0.0.1", port=1, user="u",
                               password="p"),
               target=Endpoint(host="127.0.0.1", port=2, user="u",
                               password="p"),
               databases=["appdb"], **kw)


ROOTS = {"public.events_2024": "public.events",
         "public.events_2025": "public.events"}
TABLES = ["public.events", "public.events_2024", "public.events_2025",
          "public.orders"]


# ---- a partition goes where its table goes ---------------------------------

def test_an_excluded_table_leaves_its_partitions_out_with_it():
    """Without the roots, the parent alone: its partitions - every row it
    has - were dumped, and emptied on a target that owns them."""
    hop = _hop(exclude=["public.events"])
    assert movers.excluded_tables(hop, "appdb", TABLES) == ["public.events"]
    assert movers.excluded_tables(hop, "appdb", TABLES, roots=ROOTS) == [
        "public.events", "public.events_2024", "public.events_2025"]


def test_an_included_table_takes_its_partitions_with_it():
    """Asked by their own names, an `include` of `public.events` left out
    both partitions: the table was never carried, nor emptied."""
    hop = _hop(include=["public.events"])
    assert movers.excluded_tables(hop, "appdb", TABLES, roots=ROOTS) == [
        "public.orders"]


def test_a_filtered_table_routes_its_partitions_to_the_copier():
    """The dump cannot filter rows; the partitions went the fast way,
    every row of them."""
    hop = _hop(mapping={"where": {"public.events": "id > 1"}})
    got = {d.table: d.path for d in planner.plan(
        hop, "appdb", "pgdump", TABLES, roots=ROOTS)}
    assert got == {"public.events": planner.COPIER,
                   "public.events_2024": planner.COPIER,
                   "public.events_2025": planner.COPIER,
                   "public.orders": planner.BULK}, got
    assert movers.routed_to_copier(hop, "appdb", "pgcopydb", TABLES,
                                   roots=ROOTS) == TABLES[:3]


def test_the_dump_names_every_partition_it_leaves_out(tmp_path,
                                                      monkeypatch):
    import migkit.engines.postgres as pg

    class Fake:
        # every table to its root, as the catalogue names them
        _split = staticmethod(pg.PostgresEngine._split)

        def __init__(self, hop):
            pass

        def _all_tables(self, side, db):
            return list(TABLES)

        def partition_roots(self, side, db):
            return {t: ROOTS.get(t, t) for t in TABLES}

    monkeypatch.setattr(pg, "PostgresEngine", Fake)
    monkeypatch.setattr(movers, "_pg_sequences_left_out",
                        lambda hop, db: [])
    hop = _hop(exclude=["events"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    steps = movers.pgdump_move(hop, "appdb", 2, False, None)
    dump = next(s for s in steps if getattr(s, "argv", None)
                and s.argv[0] == "pg_dump").command
    for name in ("events", "events_2024", "events_2025"):
        assert f'-T "public"."{name}"' in dump, dump
    assert '"orders"' not in dump, dump
    assert any("3 tables the hop excludes are not dumped" in s
               for s in steps), steps


def test_the_streaming_copys_filter_file_names_them_too():
    ini = movers.pgcopydb_filters(_hop(exclude=["events"]), "appdb", TABLES,
                                  roots=ROOTS)
    assert ini.splitlines()[1:] == ["public.events", "public.events_2024",
                                    "public.events_2025"], ini


# ---- the one-pass load and include ------------------------------------------

def test_a_hop_that_only_includes_does_not_get_the_one_pass_load():
    hop = _hop("hetero", include=["appdb.orders"],
               options={"source_engine": "mysql",
                        "target_engine": "postgres"})
    via, why = movers.fitted(hop, "hetero", "pgloader")
    assert via == "builtin", via
    assert "includes" in why, why


# ---- names with case, quotes and dots ---------------------------------------

def test_a_name_is_quoted_for_the_dump_however_it_arrives():
    """Unquoted, the dump folds it to lower case: `-T public.AuditLog` left
    `"AuditLog"` in, and a quoted one was split at its quote."""
    assert movers._pg_pattern('public."AuditLog"') == '"public"."AuditLog"'
    assert movers._pg_pattern("public.AuditLog") == '"public"."AuditLog"'
    assert movers._pg_pattern('public."v1.orders"') == '"public"."v1.orders"'
    assert movers._pg_pattern("orders") == '"public"."orders"'


def test_a_quoted_name_is_judged_whole_and_given_back_as_it_came():
    hop = _hop(exclude=["AuditLog", "v1.orders"])
    got = movers.excluded_tables(hop, "appdb", ['public."AuditLog"',
                                                'public."v1.orders"',
                                                "public.orders"])
    assert got == ['public."AuditLog"', 'public."v1.orders"'], got


# ---- renames ----------------------------------------------------------------

def test_a_renamed_table_is_left_to_the_copier_that_renames_it():
    hop = _hop(mapping={"tables": {"public.orders": "public.orders_v2"}})
    assert movers._routes_any(hop, "appdb"), "a rename was never asked"
    for via in ("pgdump", "pgcopydb", "pgpull", "pgsubscribe", "native"):
        got = {d.table: d.path for d in planner.plan(
            hop, "appdb", via, ["public.orders", "public.people"],
            facts={"public.orders": {"key": True},
                   "public.people": {"key": True}})}
        assert got == {"public.orders": planner.COPIER,
                       "public.people": planner.BULK}, (via, got)
    why = planner.plan(hop, "appdb", "pgdump", ["public.orders"])[0].reason
    assert why.startswith("the hop gives it another name on the target"), \
        why
    mine = _hop("mysql", mapping={"tables": {"orders": "orders_v2"}})
    for via in ("mydumper", "mysqlsh"):
        assert "orders" in movers.routed_to_copier(
            mine, "appdb", via, ["orders", "people"], qualifier=None), via
    assert movers.routed_to_copier(mine, "appdb", "mydumper",
                                   ["orders", "people"],
                                   qualifier=None) == ["orders"]


def test_the_restore_that_renames_keeps_its_renamed_collections():
    hop = _hop("mongodb", mapping={"tables": {"orders": "orders_v2"}})
    got = {d.table: d.path for d in planner.plan(
        hop, "appdb", "mongodump", ["orders", "people"], qualifier=None)}
    assert got == {"orders": planner.BULK, "people": planner.BULK}, got


def test_a_renamed_table_goes_through_the_pair_that_renames_it():
    """The engines' own copiers, checks and tails write and read the
    source's name; the pair reads the mapping."""
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    pg = PostgresEngine(_hop(mapping={"tables": {"orders": "orders_v2"}}))
    assert pg.through_pair("appdb", "public.orders")
    assert not pg.through_pair("appdb", "public.people")
    assert pg.maps_columns()
    my = MySQLEngine(_hop("mysql", mapping={"tables": {"orders": "o2"}}))
    assert my.through_pair("appdb", "orders") and my.maps_columns()
    plain = PostgresEngine(_hop())
    assert not plain.through_pair("appdb", "public.orders")
    assert not plain.maps_columns()


def test_a_rule_that_renames_nothing_sends_nothing_through_the_pair():
    """A hop whose rules name tables by their own names goes every way it
    went without them: the servers' own replication, the bulk copy."""
    from migkit.engines.postgres import PostgresEngine
    same = _hop(mapping={"tables": {"orders": "orders",
                                    "appdb.public.people": "public.people"}})
    assert not movers.renames_any(same)
    assert not PostgresEngine(same).maps_columns()
    assert not PostgresEngine(same).through_pair("appdb", "public.orders")
    assert movers.renames_any(_hop(mapping={"tables": {"orders": "o2"}}))


def test_a_name_holding_a_dot_is_not_read_as_renamed():
    """Split at its dots, `v1.orders` and `fs.chunks` counted one part too
    many against their own names and read as renamed with no rule."""
    assert not movers._renamed(_hop(), "appdb", ["public", "v1.orders"])
    assert not movers._renamed(_hop("mongodb"), "appdb", ["fs.chunks"])
    same = _hop(mapping={"tables": {"orders": "orders"}})
    assert not movers._renamed(same, "appdb", ["public", "orders"])
    moved = _hop(mapping={"tables": {"public.orders": "archive.orders"}})
    assert movers._renamed(moved, "appdb", ["public", "orders"])


def test_the_online_sync_is_not_given_a_rename():
    hop = _hop("mongodb", mapping={"tables": {"orders": "orders_v2"}},
               options={"mongodb_entitlement": "atlas"})
    why = movers._mongosync_unfit(hop)
    assert why and "renames" in why, why


def test_the_restore_writes_a_renamed_collection_under_its_new_name(
        tmp_path, monkeypatch):
    """Its own pair of names last: the restore takes the last pair a name
    matches (measured with the Database Tools 100.16 against MongoDB 7.0:
    `appdb.orders -> first.orders_v2` before `appdb.* -> second.*`
    restored `second.orders`), and the database's pair after it took it
    under its old name."""
    _collections(monkeypatch)
    hop = _hop("mongodb", mapping={"tables": {"orders": "orders_v2"}},
               db_map={"appdb": "appdb2"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    steps = movers.mongodump_move(hop, "appdb", 2, False, None)
    argv = next(s.argv for s in steps if getattr(s, "argv", None))
    renames = [a for a in argv if a.startswith(("--nsFrom", "--nsTo"))]
    assert renames == ["--nsFrom=appdb.*", "--nsTo=appdb2.*",
                       "--nsFrom=appdb.orders", "--nsTo=appdb2.orders_v2"], \
        renames


@needs_docker
def test_the_restore_takes_the_last_pair_a_name_matches(tmp_path):
    """The measurement the order above rests on, on the sandbox's own
    server and this machine's programs."""
    import shutil
    import subprocess
    from tests import servers
    if not (shutil.which("mongodump") and shutil.which("mongorestore")):
        pytest.skip("the dump and restore programs are not installed here")
    with servers.space("mongo", __name__) as mongo:
        coll = mongo.src.client()["measure"]["orders"]
        coll.insert_one({"_id": 1})
        uri = (f"mongodb://{mongo.src.host}:{mongo.src.port}/"
               "?directConnection=true")
        archive = tmp_path / "a.archive"
        subprocess.run(["mongodump", f"--uri={uri}", "--db=measure",
                        f"--archive={archive}", "--quiet"], check=True)
        subprocess.run(["mongorestore", f"--uri={uri}", f"--archive={archive}",
                        "--quiet", "--nsInclude=measure.*",
                        "--nsFrom=measure.*", "--nsTo=second.*",
                        "--nsFrom=measure.orders", "--nsTo=first.orders_v2"],
                       check=True)
        client = mongo.src.client()
        assert client["first"].list_collection_names() == ["orders_v2"]
        assert "orders" not in client["second"].list_collection_names()


def _collections(monkeypatch):
    """A source database of three collections and a view, as the server
    lists them."""
    from migkit.engines.mongodb import MongoEngine

    class Db:
        def list_collections(self):
            return [{"name": n, "type": "collection"}
                    for n in ("orders", "people", "fs.chunks")] + [
                {"name": "big_orders", "type": "view"}]
    monkeypatch.setattr(MongoEngine, "_client",
                        lambda self, side: {"appdb": Db()})


def test_a_view_the_hop_does_not_take_is_neither_dumped_nor_restored(
        tmp_path, monkeypatch):
    """`objects: {views: false}`: the view was dumped and restored, as a
    collection of the same name would have been."""
    _collections(monkeypatch)
    hop = _hop("mongodb")
    hop.objects = {"views": False}
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    argv = next(s.argv for s in movers.mongodump_move(hop, "appdb", 2, False,
                                                      None)
                if getattr(s, "argv", None))
    assert "--excludeCollection=big_orders" in argv, argv
    assert "--nsExclude=appdb.big_orders" in argv, argv
    assert "--excludeCollection=orders" not in argv, argv


def test_a_renamed_collections_name_is_escaped_for_the_restore():
    assert movers._ns("app", "a*b$c") == r"app.a\*b\$c"


# ---- the step that empties the target, said --------------------------------

def test_the_plan_says_only_the_source_s_tables_are_emptied():
    plain = movers._truncate_step(_hop(), "appdb")
    assert plain.startswith("# empty all the target's appdb user tables the"
                            " source has too"), plain
    scoped = movers._truncate_step(_hop(include=["orders"]))
    assert "except the ones the hop excludes or does not take" in scoped
    filtered = movers._truncate_step(
        _hop(mapping={"where": {"orders": "x > 1"}}), "appdb")
    assert "only the rows its filter selects" in filtered, filtered


# ---- what depends on a table the target is not given ------------------------

def test_the_dumps_list_loses_exactly_the_entries_named():
    listing = (";\n; Archive created at 2026-10-03\n;\n"
               "215; 1255 16385 FUNCTION public f(integer) postgres\n"
               "216; 2606 16400 FK CONSTRAINT public orders orders_c_fkey"
               " postgres\n"
               "217; 1259 16390 TABLE public orders postgres\n")
    text, dropped = movers._toc_without(listing, {"2606 16400": {}})
    assert dropped == ["2606 16400"]
    assert ";216; 2606 16400 FK CONSTRAINT" in text
    assert "\n215; 1255 16385 FUNCTION" in text
    assert "\n217; 1259 16390 TABLE" in text


def test_a_mysql_key_into_a_table_not_given_is_taken_out_of_its_table():
    ddl = ("CREATE TABLE `orders` (\n  `id` int NOT NULL,\n"
           "  `cid` int DEFAULT NULL,\n  PRIMARY KEY (`id`),\n"
           "  KEY `c` (`cid`),\n"
           "  CONSTRAINT `c` FOREIGN KEY (`cid`) REFERENCES `customers`"
           " (`id`)\n) ENGINE=InnoDB")
    got, keys = movers._my_without_keys(ddl, {"customers"})
    assert keys == [("c", "customers")]
    assert "FOREIGN KEY" not in got
    assert "  KEY `c` (`cid`)\n) ENGINE=InnoDB" in got, got
    assert movers._my_without_keys(ddl, {"other"}) == (ddl, [])
    elsewhere = ddl.replace("REFERENCES `customers`",
                            "REFERENCES `crm`.`customers`")
    assert movers._my_without_keys(elsewhere, {"customers"})[1] == []


# ---- patterns where the program reads them ----------------------------------

NAMES = ["orders", "audit_x", "audit", "tmp1", "tmp12", "a.tmp", "x.tmp",
         "lines_a", "lines_old", "appdb.tmp1", "people", "fs.chunks"]


@pytest.mark.parametrize("pattern", ["audit_*", "appdb.audit_*", "a*",
                                     "*.tmp", "app[!x]b.?*", "?pp*.t*",
                                     "[ab]*", "appdb", "b*", "*"])
def test_a_pattern_after_the_database_takes_what_it_took_before(pattern):
    """`excluded` holds a collection to the pattern by its own name and by
    `database.name`: told only names inside the database, the sync is told
    the second as patterns of their own (`config.pattern_after`, read as
    the sync's patterns are made)."""
    from migkit.config import pattern_after
    rest = [r for r in pattern_after(pattern, "appdb.") if r]
    for name in NAMES:
        assert any(fnmatchcase(name, r) for r in rest) == \
            fnmatchcase("appdb." + name, pattern), (pattern, name, rest)


@pytest.mark.parametrize("scope", [
    {"exclude": ["audit*", "appdb.tmp?"]},
    {"include": ["orders", "lines_*"], "exclude": ["lines_old"]},
    {"include": ["appdb.*"], "exclude": ["*.tmp"]},
    {"include": ["other.*"]},
])
def test_the_dumps_pattern_takes_what_the_hop_takes(scope):
    hop = _hop("mysql", **scope)
    rx = movers._mydumper_regex(hop, "appdb")
    # the dump reads its pattern without case unless told otherwise
    # (measured on 1.0.5); Python's reads it with case already, and has
    # no bare `(?-i)`
    assert rx.startswith("(?-i)"), rx
    rx = rx[len("(?-i)"):]
    for name in NAMES:
        assert bool(re.search(rx, f"appdb.{name}")) == \
            (not hop.excluded("appdb", name)), (scope, name, rx)
    # a table named otherwise in case is another table, taken as the hop
    # takes it
    assert bool(re.search(rx, "appdb.AUDIT_x")) == \
        (not hop.excluded("appdb", "AUDIT_x"))


def test_a_hop_taking_everything_gives_the_dump_no_pattern():
    assert movers._mydumper_regex(_hop("mysql"), "appdb") is None
    assert movers._mydumper_regex(_hop("mysql", include=["appdb"]),
                                  "appdb") is None


def test_the_dump_is_given_its_pattern_where_its_build_reads_one(
        monkeypatch, tmp_path):
    have = {"--regex", "--omit-from-file", "--trx-tables", "--checksum"}
    monkeypatch.setattr(movers, "_long_options", lambda p, *a: have)
    hop = _hop("mysql", exclude=["audit_*"])
    rx = movers._mydumper_regex(hop, "appdb")
    dump, _ = movers._mydumper_commands(hop, "appdb", 2, tmp_path,
                                        logs=False, regex=rx)
    assert dump[dump.index("--regex") + 1] == rx
    have.discard("--regex")
    dump, _ = movers._mydumper_commands(hop, "appdb", 2, tmp_path,
                                        logs=False, regex=rx)
    assert "--regex" not in dump, "a build without it stopped at its options"


@pytest.mark.parametrize("scope", [
    {"exclude": ["audit*", "a*.tmp"]},
    {"include": ["orders*", "fs.*"], "exclude": ["fs.files"]},
    {"include": ["appdb.*"]},
])
def test_the_online_sync_is_told_the_hops_patterns(scope, monkeypatch):
    """Online for days: told names resolved at its start, it synced a
    collection made later under an excluded pattern."""
    monkeypatch.setattr(movers, "program_version", lambda p: "1.21.0")
    hop = _hop("mongodb", **scope)
    skip = [n for n in NAMES if hop.excluded("appdb", n)]
    got = movers.mongosync_filters(hop, "appdb", skip)
    inc = got["includeNamespaces"][0]
    exc = (got.get("excludeNamespaces") or [{}])[0]
    for name in NAMES:
        taken = ("collectionsRegex" not in inc or re.search(
            inc["collectionsRegex"]["pattern"], name)) and not (
            exc and re.search(exc["collectionsRegex"]["pattern"], name))
        assert bool(taken) == (not hop.excluded("appdb", name)), (scope, name)


def test_an_older_sync_is_told_names(monkeypatch):
    monkeypatch.setattr(movers, "program_version", lambda p: "1.5.2")
    got = movers.mongosync_filters(_hop("mongodb", exclude=["audit*"]),
                                   "appdb", ["audit_x"])
    assert got == {"includeNamespaces": [{"database": "appdb"}],
                   "excludeNamespaces": [{"database": "appdb",
                                          "collections": ["audit_x"]}]}


def test_a_hop_taking_everything_tells_the_sync_nothing_more():
    assert movers.mongosync_filters(_hop("mongodb"), "appdb", []) == {
        "includeNamespaces": [{"database": "appdb"}]}


# ---- who asks for a partition's root, and what a contract keeps ------------

def test_only_a_hop_with_a_rule_a_root_answers_asks_for_roots():
    """A hop with none goes as it always went: its partitions as
    themselves, and the source not asked."""
    assert not movers.rules_by_name(_hop())
    for kw in ({"exclude": ["x"]}, {"include": ["x"]},
               {"objects": {"views": False}},
               {"mapping": {"where": {"x": "id > 1"}}},
               {"mapping": {"columns": {"x": {"drop": ["y"]}}}},
               {"mapping": {"tables": {"x": "y"}}}):
        assert movers.rules_by_name(_hop(**kw)), kw
    assert movers._pg_roots(_hop(), "appdb") == {}


def test_a_table_whose_changes_are_carried_in_part_is_not_emptied_over():
    """`mapping.ops` without delete keeps on the target what the source
    deletes; emptying the table for a load took exactly those rows."""
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    movers._refuse_kept_rows([])
    with pytest.raises(SystemExit) as e:
        movers._refuse_kept_rows(["public.orders"])
    said = str(e.value)
    assert "public.orders" in said and "mapping.ops" in said, said
    assert "Nothing has been emptied" in said, said
    assert not [t for t in TOOLS if t in said.lower()], said


# ---- one filter a table in the dump's file ---------------------------------

def test_a_table_two_keys_name_has_one_filter_the_most_particular():
    """`orders` and `appdb.orders` both applied: two `where =` lines in one
    section, and which the dump went by was its own business."""
    hop = _hop("mysql", mapping={"where": {"orders": "a = 1",
                                           "appdb.orders": "b = 2"}})
    for got in (movers.mydumper_defaults(hop, "appdb"),
                movers.mydumper_defaults(hop, "appdb",
                                         tables=["orders", "people"])):
        section = got.split("[`appdb`.`orders`]")[1]
        assert section.count("where =") == 1, got
        assert "where = b = 2" in section, got
        assert hop.row_filter("appdb", "orders") == "b = 2"


def test_only_the_tables_dumped_have_a_section():
    hop = _hop("mysql", mapping={"where": {"orders": "a = 1",
                                           "people": "b = 2"}})
    got = movers.mydumper_defaults(hop, "appdb", tables=["people"])
    assert "`orders`" not in got and "[`appdb`.`people`]" in got, got


# ---- against PostgreSQL -----------------------------------------------------

PARTITIONED = """
create table events (id int, at date, v text) partition by range (at);
create table events_2024 partition of events
  for values from ('2024-01-01') to ('2025-01-01');
create table events_2025 partition of events
  for values from ('2025-01-01') to ('2026-01-01');
create table orders (id int primary key, v text);
"""


def _pg(pg_pair, tmp_path, **kw):
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2, **kw)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _both(pg_pair, sql):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, sql)
        assert got.returncode == 0, got.stderr


def _on(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _partitioned(pg_pair):
    _both(pg_pair, PARTITIONED)
    _on(pg_pair["src"], "insert into events values (1, '2024-03-01', 's'),"
                        " (2, '2025-03-01', 's'); insert into orders values"
                        " (1, 's')")
    _on(pg_pair["dst"], "insert into events values (7, '2024-05-01', 'own'),"
                        " (8, '2025-05-01', 'own'); insert into orders"
                        " values (9, 'own')")


def _events(pg_pair):
    return _on(pg_pair["dst"], "select string_agg(id || v, ',' order by id)"
                               " from events")


@needs_docker
def test_partition_roots_are_read_from_the_catalogue(pg_pair, tmp_path):
    """Every level up to the top, and only for a hop with a rule a root
    answers: the rest go as themselves, and the source is not asked."""
    _both(pg_pair, PARTITIONED + "create table deep (id int, k int)"
                   " partition by list (k); create table deep_1 partition"
                   " of deep for values in (1) partition by list (id);"
                   " create table deep_1_1 partition of deep_1 for values"
                   " in (1);")
    got = movers._pg_roots(_pg(pg_pair, tmp_path, exclude=["nothing"]),
                           "postgres")
    assert got == {"public.events_2024": "public.events",
                   "public.events_2025": "public.events",
                   "public.deep_1": "public.deep",
                   "public.deep_1_1": "public.deep"}, got
    assert movers._pg_roots(_pg(pg_pair, tmp_path), "postgres") == {}
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_pg(pg_pair, tmp_path, exclude=["events"]))
    assert not eng.in_scope("postgres", "public.events_2024")
    assert eng.in_scope("postgres", "public.deep_1_1")


@needs_docker
def test_a_partitioned_table_the_target_owns_keeps_its_rows(pg_pair,
                                                            tmp_path):
    _partitioned(pg_pair)
    movers.pgdump_move(_pg(pg_pair, tmp_path, exclude=["events"]),
                       "postgres", 2, True, None)
    assert _events(pg_pair) == "7own,8own", "its partitions were replaced"
    assert _on(pg_pair["dst"], "select string_agg(id || v, ',')"
                               " from orders") == "1s"


@needs_docker
def test_an_included_partitioned_table_is_carried_and_nothing_else(
        pg_pair, tmp_path):
    _partitioned(pg_pair)
    movers.pgdump_move(_pg(pg_pair, tmp_path, include=["public.events"]),
                       "postgres", 2, True, None)
    assert _events(pg_pair) == "1s,2s", "its partitions were left out"
    assert _on(pg_pair["dst"], "select string_agg(id || v, ',')"
                               " from orders") == "9own"


@needs_docker
def test_a_filter_on_a_partitioned_table_reaches_its_partitions(
        pg_pair, tmp_path, monkeypatch):
    """End to end: the partitions go to the copier with their table, which
    reads through the filter; the target's own rows outside it stay."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    _partitioned(pg_pair)
    _on(pg_pair["src"], "insert into events values (3, '2024-04-01', 's')")
    _on(pg_pair["dst"], "insert into events values (0, '2024-06-01', 'own')")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  part:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n"
        "    mapping: {where: {public.events: 'id > 1'}}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    got = CliRunner().invoke(cli.main, ["move", "part", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output + str(got.exception or "")
    # the source's 1 is outside the filter and not carried; the target's 7
    # and 8 are inside it and replaced; its 0 is outside it, and its own
    assert _events(pg_pair) == "0own,2s,3s", _events(pg_pair)


@needs_docker
def test_a_table_only_the_target_has_is_not_emptied(pg_pair, tmp_path):
    _both(pg_pair, "create table orders (id int primary key, v text)")
    _on(pg_pair["dst"], "create table notes (id int primary key);"
                        " insert into notes values (1), (2);"
                        " insert into orders values (5, 'stale')")
    stmt = movers._pg_truncate_target(_pg(pg_pair, tmp_path), "postgres")
    assert stmt == "truncate table public.orders cascade", stmt
    assert _on(pg_pair["dst"], "select count(*) from notes") == "2"
    assert _on(pg_pair["dst"], "select count(*) from orders") == "0"


@needs_docker
def test_a_cascade_into_a_table_only_the_target_has_is_refused(pg_pair,
                                                               tmp_path):
    _both(pg_pair, "create table orders (id int primary key, v text)")
    _on(pg_pair["dst"], "insert into orders values (1, 'x');"
                        " create table notes (id int primary key,"
                        " oid int references orders(id));")
    hop = _pg(pg_pair, tmp_path)
    # empty, it costs nothing to empty it
    movers._pg_truncate_target(hop, "postgres")
    _on(pg_pair["dst"], "insert into orders values (1, 'x');"
                        " insert into notes values (1, 1)")
    with pytest.raises(SystemExit) as e:
        movers._pg_truncate_target(hop, "postgres")
    assert "public.notes" in str(e.value), e.value
    assert "the source has no such table" in str(e.value), e.value
    assert _on(pg_pair["dst"], "select count(*) from notes") == "1"


@needs_docker
def test_a_filtered_table_keeps_the_rows_outside_its_filter(pg_pair,
                                                            tmp_path):
    _both(pg_pair, "create table orders (id int primary key, v text);"
                   " create table people (id int primary key)")
    _on(pg_pair["dst"], "insert into orders values (1, 'own'), (2, 'old'),"
                        " (3, 'old'); insert into people values (1)")
    hop = _pg(pg_pair, tmp_path, mapping={"where": {"orders": "id > 1"}})
    lines = []
    stmt = movers._pg_truncate_target(hop, "postgres", lines.append)
    assert stmt == ("truncate table public.people cascade; delete from"
                    " public.orders where id > 1"), stmt
    assert _on(pg_pair["dst"], "select string_agg(id || v, ',')"
                               " from orders") == "1own"
    assert any("keep their rows outside the filter" in ln for ln in lines)


@needs_docker
def test_a_mixed_case_table_the_hop_excludes_is_not_dumped(pg_pair,
                                                           tmp_path):
    _both(pg_pair, 'create table "AuditLog" (id int primary key);'
                   " create table orders (id int primary key)")
    _on(pg_pair["src"], 'insert into "AuditLog" values (1);'
                        " insert into orders values (1)")
    _on(pg_pair["dst"], 'insert into "AuditLog" values (7), (8)')
    movers.pgdump_move(_pg(pg_pair, tmp_path, exclude=["AuditLog"]),
                       "postgres", 2, True, None)
    assert _on(pg_pair["dst"], 'select string_agg(id::text, \',\''
                               ' order by id) from "AuditLog"') == "7,8"
    assert _on(pg_pair["dst"], "select count(*) from orders") == "1"


@needs_docker
def test_a_renamed_table_lands_under_its_new_name(pg_pair, tmp_path,
                                                  monkeypatch):
    """Through the dump and the copier that renames: the dump leaves it
    out, the target's missing table is made under the new name, and the
    table under the old one - the target's own - is not touched."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    _on(pg_pair["src"], "create table orders (id int primary key, v text);"
                        " insert into orders values (1, 's'), (2, 's');"
                        " create table people (id int primary key);"
                        " insert into people values (1)")
    _on(pg_pair["dst"], "create table orders (id int primary key, v text);"
                        " insert into orders values (9, 'own');"
                        " create table people (id int primary key)")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  ren:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n"
        "    mapping: {tables: {public.orders: public.orders_v2}}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    got = CliRunner().invoke(cli.main, ["move", "ren", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output + str(got.exception or "")
    assert _on(pg_pair["dst"], "select string_agg(id || v, ',' order by id)"
                               " from orders_v2") == "1s,2s"
    assert _on(pg_pair["dst"], "select string_agg(id || v, ',')"
                               " from orders") == "9own"
    assert _on(pg_pair["dst"], "select count(*) from people") == "1"


@needs_docker
def test_what_reads_a_table_left_out_is_left_out_of_an_empty_target(
        pg_pair, tmp_path):
    """A view over it made the restore refuse the schema; a foreign key
    into it stopped the move once the rows were in."""
    _on(pg_pair["src"], "create table customers (id int primary key);"
                        " create table orders (id int primary key,"
                        " cid int references customers(id));"
                        " create view big as select * from customers;"
                        " create view bigger as select * from big;"
                        " create view mine as select * from orders;"
                        " insert into customers values (1);"
                        " insert into orders values (1, 1)")
    hop = _pg(pg_pair, tmp_path, exclude=["customers"])
    lines = []
    movers._pg_create_missing(hop, "postgres", lines.append)
    movers._pg_finish_created(hop, "postgres", lines.append)
    there = _on(pg_pair["dst"], "select string_agg(relname, ',' order by"
                                " relname) from pg_class where relname in"
                                " ('customers', 'orders', 'big', 'bigger',"
                                " 'mine')")
    assert there == "mine,orders", there
    assert _on(pg_pair["dst"], "select count(*) from pg_constraint"
                               " where contype = 'f'") == "0"
    said = "\n".join(str(ln) for ln in lines)
    assert "1 foreign keys are not made on the target" in said, said
    assert "orders_cid_fkey" in said and "-> customers" in said, said
    # in the server's own words, the schema named where it is not visible
    for view in ("big", "bigger"):
        assert re.search(rf"\bview (public\.)?{view}\b", said), said


@needs_docker
def test_the_index_window_leaves_a_table_only_the_target_has(pg_pair,
                                                             tmp_path):
    _both(pg_pair, "create table orders (id int primary key, v text);"
                   " create index orders_v on orders (v)")
    _on(pg_pair["dst"], "create table notes (id int primary key, n text);"
                        " create unique index notes_n on notes (n)")
    lines = []
    with movers._IndexWindow(_pg(pg_pair, tmp_path), "postgres", 2,
                             lines.append) as w:
        assert _on(pg_pair["dst"], "select count(*) from pg_indexes"
                                   " where indexname = 'notes_n'") == "1"
    assert w.dropped == ["public.orders_v"], w.dropped
    assert any("tables the move does not fill were left in place" in ln
               for ln in lines), lines


@needs_docker
def test_a_table_kept_by_its_contract_is_not_emptied_over_its_rows(
        pg_pair, tmp_path):
    """Its deletes not carried, the target's rows of it are the ones the
    source deleted, kept; held empty, it is filled as a copy."""
    _both(pg_pair, "create table orders (id int primary key, v text);"
                   " create table people (id int primary key)")
    _on(pg_pair["dst"], "insert into orders values (9, 'kept');"
                        " insert into people values (1)")
    hop = _pg(pg_pair, tmp_path,
              mapping={"ops": {"orders": "insert, update"}})
    with pytest.raises(SystemExit) as e:
        movers._pg_truncate_target(hop, "postgres")
    assert "public.orders" in str(e.value), e.value
    assert _on(pg_pair["dst"], "select count(*) from orders") == "1"
    assert _on(pg_pair["dst"], "select count(*) from people") == "1", \
        "something was emptied before the refusal"
    _on(pg_pair["dst"], "delete from orders")
    assert movers._pg_truncate_target(hop, "postgres") == (
        "truncate table public.orders, public.people cascade")


@needs_docker
def test_what_the_hops_objects_leave_out_is_not_made_on_an_empty_target(
        pg_pair, tmp_path):
    """`objects: {views: false, routines: false}`: the view, the routine
    and what reads them stay off the target; the tables are made."""
    _on(pg_pair["src"], "create table orders (id int primary key, v text);"
                        " create view open_orders as select * from orders;"
                        " create function one() returns int language sql"
                        " return 1;"
                        " create view ones as select one() as n")
    hop = _pg(pg_pair, tmp_path, objects={"views": False,
                                          "routines": False})
    lines = []
    movers._pg_create_missing(hop, "postgres", lines.append)
    movers._pg_finish_created(hop, "postgres", lines.append)
    there = _on(pg_pair["dst"], "select string_agg(relname, ',' order by"
                                " relname) from pg_class where relname in"
                                " ('orders', 'open_orders', 'ones')")
    assert there == "orders", there
    assert _on(pg_pair["dst"], "select count(*) from pg_proc where proname"
                               " = 'one'") == "0"
    said = "\n".join(str(ln) for ln in lines)
    assert "objects the hop does not take are not made there" in said, said
