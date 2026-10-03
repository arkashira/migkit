"""The servers' own replication, the pipeline and the slot's reader, held
to the hop's scope and mapping (backlog P0-SCOPE.6e), without a server:

* a PostgreSQL publication and a MySQL replica apply whole rows under the
  source's names: a hop that filters rows, renames a table or carries
  only some changes is refused by them, and the ladder of change ways
  takes migkit's own tail for it - `where: {orders: "region='TH'"}`
  under `--mode cdc` carried every row;
* a MySQL replica's filters are the hop's own patterns, not a list read
  once, and a pattern its filters cannot hold is said;
* the pipeline reads the hop's patterns in every database, takes the
  columns it keeps by name, leaves out the changes it does not carry, and
  writes into the hop's database on the target - one database a hop;
* a partition's changes are its root's, a truncate of some of its
  partitions copies it again, the publication lists the roots;
* a large value the log left out as unchanged is never written as the
  decoder's placeholder;
* a DTS job's DDL options become the hop's `ddl`, and it is checked.
"""
import json
import re

import pytest

from migkit import canon, hopkeys, imports, movers, pgslot
from migkit.config import Endpoint, Hop, pattern_after

EP = Endpoint(host="10.0.0.1", port=1, user="u", password="CHANGE_ME")


def _hop(engine="postgres", **kw):
    return Hop(name="own", engine=engine, source=EP, target=EP,
               databases=kw.pop("databases", ["appdb"]), **kw)


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import tools
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    monkeypatch.setattr(tools, "terms_accepted", lambda n: True)


# ---- a pattern read after its database --------------------------------------

def test_a_pattern_written_with_its_database_is_read_after_it():
    assert pattern_after("shop.orders", "shop.") == ["orders"]
    assert pattern_after("crm.orders", "shop.") == []
    # a star takes the database, or the database and more
    assert pattern_after("*.orders", "shop.") == ["orders", "*.orders"]
    assert pattern_after("sho*rders", "shop.") == ["*rders"]
    assert pattern_after("s[ho]op.*", "shop.") == ["*"]


# ---- what the servers' replication cannot carry -----------------------------

def test_a_table_carrying_only_some_changes_is_refused_by_a_whole_stream():
    hop = _hop(mapping={"ops": {"public.orders": "insert, update"}})
    got = movers.stream_cannot_carry(hop, "appdb",
                                     ["public.orders", "public.items"])
    assert got == [("public.orders", "it carries only inserts, updates")]
    # a stream that leaves the same changes out of every table holds one
    # whose tables all carry the same, and refuses one whose do not
    alike = _hop(mapping={"ops": {"*": "insert, update"}})
    assert movers.stream_cannot_carry(alike, "appdb", ["public.a",
                                                       "public.b"],
                                      ops_alike=True) == []
    assert movers.stream_cannot_carry(hop, "appdb", ["public.orders",
                                                     "public.items"],
                                      ops_alike=True) == [
        ("public.items", "it carries other changes than the hop's other"
                         " tables")]


class _Listed:
    """An engine that lists its tables, and says when it is asked to."""

    def __init__(self, names, unsafe=None):
        self.names, self.unsafe, self.asked = names, unsafe, 0

    def databases(self):
        return ["appdb"]

    def neutral_tables(self, side, db):
        self.asked += 1
        return list(self.names)

    def native_scope_unsafe(self):
        return self.unsafe


def test_the_servers_own_replication_is_refused_where_the_hop_filters():
    from migkit import cli
    eng = _Listed(["public.orders", "public.items"])
    hop = _hop(mapping={"where": {"public.orders": "region = 'TH'"}})
    said = cli._own_refused(hop, eng, "appdb")
    assert "public.orders (its rows are filtered)" in said
    # a hop that asks none of it is not even listed
    plain = _Listed(["public.orders"])
    assert cli._own_refused(_hop(), plain, "appdb") == ""
    assert plain.asked == 0
    # a scope the replica's own filters cannot hold
    scoped = _Listed([], unsafe="the replica cannot hold exclude *_log")
    assert "cannot hold" in cli._own_refused(_hop(exclude=["*_log"]),
                                             scoped, "appdb")


def test_the_ladder_takes_migkit_s_own_tail_where_the_servers_cannot():
    """PostgreSQL's subscription and MySQL's replica each passed a hop
    whose rows they would carry whole; the ladder now passes them over,
    with the reason, for the engine's tail as a pair of itself."""
    why = "the servers' own replication cannot carry public.orders"
    got = movers.stream_way(_hop(), "postgres", True, False, False,
                            refused=lambda: why, by_pair=lambda: True)
    assert got.path == "tail", got
    assert why in dict((r.name, w) for r, w in got.passed)["subscription"]
    got = movers.stream_way(_hop("mysql"), "mysql", True, False, False,
                            refused=lambda: why, by_pair=lambda: True)
    assert got.path == "tail", got
    # no pair to follow it: the replica is passed over, never taken
    got = movers.stream_way(_hop("mysql"), "mysql", True, False, False,
                            refused=lambda: why, by_pair=lambda: False)
    assert got.path != "replica"
    assert why in dict((r.name, w) for r, w in got.passed)["replica"]
    # a hop that asks none of it takes the way it always took
    assert movers.stream_way(_hop(), "postgres", True, False,
                             False).path == "subscription"
    assert movers.stream_way(_hop("mysql"), "mysql", True, False,
                             False).path == "replica"


# ---- a MySQL replica's own filters ------------------------------------------

def _my(monkeypatch, tables=("orders", "audit_log", "x_log"), **kw):
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(MySQLEngine, "_all_tables",
                        lambda self, side, db: list(tables))
    return MySQLEngine(_hop("mysql", databases=["shop"], **kw))


def test_a_replica_s_filters_are_the_hop_s_patterns(monkeypatch):
    """The ignore list was read once: a table made later under an excluded
    pattern was written by the replica, and `include` was not read at
    all."""
    from migkit.engines.mysql import _like
    assert _like("order_items") == "order\\_items"
    assert _like("*_log") == "%\\_log"
    assert _like("t?") == "t_"
    eng = _my(monkeypatch, include=["shop.orders", "shop.lines_*"])
    rewrite, wild, ignore, exact, cannot = eng._replica_filters()
    assert exact == ["shop.orders"]
    assert wild == ["shop.lines\\_%"]
    assert cannot == []
    stmt = eng._replica_filter_sql("mysql")[0][0]
    assert "REPLICATE_DO_TABLE = (shop.orders)" in stmt
    assert "REPLICATE_WILD_DO_TABLE = ('shop.lines\\_%')" in stmt
    # an exact exclude holds against a wildcard take: the server reads it
    # first
    eng = _my(monkeypatch, exclude=["audit_log"])
    _, wild, ignore, exact, cannot = eng._replica_filters()
    assert wild == ["shop.%"] and "shop.audit_log" in ignore and not cannot
    assert eng.native_scope_unsafe() is None


def test_a_pattern_a_replica_cannot_hold_is_said(monkeypatch):
    """A wildcard ignore is read after a wildcard take, which takes what
    both name; a `[...]` class has no form in a filter."""
    eng = _my(monkeypatch, exclude=["*_log"])
    said = eng.native_scope_unsafe()
    assert said and "exclude *_log" in said
    eng = _my(monkeypatch, include=["shop.[ab]*"])
    assert "a [...] class" in eng.native_scope_unsafe()
    # a hop that leaves nothing out is filtered as it always was
    eng = _my(monkeypatch)
    assert eng._replica_filters()[1] == ["shop.%"]
    assert eng.native_scope_unsafe() is None


# ---- the pipeline -----------------------------------------------------------

def test_the_pipeline_reads_the_hop_s_patterns_in_every_database():
    """`table.exclude.list` was the first database's excluded tables, read
    once: the second's were streamed, and a table made later outside the
    hop's include or under its exclude was streamed and made on the
    target by the sink."""
    hop = _hop("mysql", databases=["sales", "crm"],
               include=["sales.*", "crm.orders"], exclude=["*_log"])
    got = movers.debezium_tables(hop, ["sales", "crm"],
                                 {"sales": [], "crm": []}, True)
    lists = got["table.include.list"].split(",")

    def taken(name):
        return any(re.fullmatch(x, name) for x in lists)
    for name in ("sales.orders", "sales.made_later", "crm.orders"):
        assert taken(name), name
    for name in ("sales.audit_log", "crm.items", "crm.later_log",
                 "other.orders"):
        assert not taken(name), name
    # by every reading the hop makes of a name, on PostgreSQL too
    pg = _hop(include=["public.orders", "appdb.billing.*"],
              exclude=["billing.secret*"])
    got = movers.debezium_tables(pg, ["appdb"], {"appdb": []}, False)
    for name in ("public.orders", "billing.a", "billing.new",
                 "billing.secret1", "public.x", "other.orders"):
        want = not pg.excluded("appdb", *name.split("."))
        assert bool(re.fullmatch(got["table.include.list"], name)) == want


def test_the_pipeline_follows_one_database_into_the_hop_s_name_for_it(
        tmp_path, monkeypatch):
    monkeypatch.setattr(movers, "_stream_facts", lambda hop, dbs, my: (
        {d: [] for d in dbs}, None, None))
    monkeypatch.setattr(movers, "_gtid_on", lambda hop: False)
    with pytest.raises(SystemExit) as e:
        movers.stream_codegen(_hop("mysql", databases=["a", "b"]),
                              ["a", "b"], "mysql")
    assert "every database's tables would land in the first's" in \
        str(e.value)
    out = movers.stream_codegen(_hop("mysql", databases=["shop"],
                                     db_map={"shop": "shop_new"}),
                                ["shop"], "mysql")
    sink = json.loads((out / "sink-connector.json").read_text())["config"]
    assert sink["connection.url"].split("?")[0].endswith("/shop_new")


def test_the_pipeline_leaves_out_the_changes_the_hop_does_not_carry(
        tmp_path, monkeypatch):
    monkeypatch.setattr(movers, "_stream_facts", lambda hop, dbs, my: (
        {d: ["public.a", "public.b"] for d in dbs}, None, None))
    monkeypatch.setattr(movers, "_pg_snapshot_watermarks",
                        lambda hop, db: False)

    def source(hop):
        out = movers.stream_codegen(hop, ["appdb"], "postgres")
        return json.loads((out / "source-connector.json").read_text())[
            "config"]
    assert source(_hop())["skipped.operations"] == "none"
    # an archive: deletes, and the truncates that take what deletes would
    got = source(_hop(mapping={"ops": {"*": "insert, update"}}))
    assert got["skipped.operations"] == "d,t"
    with pytest.raises(SystemExit) as e:
        source(_hop(mapping={"ops": {"public.a": "insert"}}))
    assert "carries other changes than the hop's other tables" in \
        str(e.value)


# ---- a PostgreSQL source's partitions and large values ----------------------

def _pg(monkeypatch, roots, **kw):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(**kw))
    leaves = {}
    for t, r in roots.items():
        leaves.setdefault(r, set()).add(t)
    for r, got in leaves.items():
        if len(got) > 1:
            # a partitioned table holds no rows of its own
            got.discard(r)
    monkeypatch.setattr(PostgresEngine, "_roots",
                        lambda self, side, db, ask=None: (roots, leaves))
    return eng


ROOTS = {"public.orders": "public.orders",
         "public.orders_2024": "public.orders",
         "public.orders_2025": "public.orders",
         "public.items": "public.items"}


def test_a_partition_s_changes_are_its_root_s(monkeypatch):
    """test_decoding names the partition: `include: [public.orders]` left
    out every change to `orders_2024`, and a row filter keyed on the
    parent never applied."""
    eng = _pg(monkeypatch, ROOTS, include=["public.orders"])
    monkeypatch.setattr(type(eng), "neutral_key",
                        lambda self, side, db, table, ask=None: ["id"])
    rows = [("0/1", "7", "BEGIN 7"),
            ("0/2", "7", "table public.orders_2024: INSERT:"
                         " id[integer]:1 v[text]:'a'"),
            ("0/3", "7", "table public.items: INSERT: id[integer]:2"),
            ("0/4", "7", "COMMIT 7")]
    out, _ = eng._slot_lines("src", "appdb", rows, None, False)
    assert [(c["table"], c["key"]) for c in out] == [
        ("public.orders", {"id": 1})]


def test_a_truncate_of_some_partitions_copies_the_table_again(monkeypatch):
    """Emptied in place, the target's whole table lost the rows of the
    partitions the source still has."""
    eng = _pg(monkeypatch, ROOTS)
    root_of, leaves_of = eng._root_reader("src", "appdb")
    one = eng._truncated("appdb", ["public.orders_2024"], "9", root_of,
                         leaves_of)
    assert [(c["table"], c["kind"]) for c in one] == [
        ("public.orders", "reload")]
    every = eng._truncated("appdb", ["public.orders_2024",
                                     "public.orders_2025", "public.items"],
                           "9", root_of, leaves_of)
    assert [(c["table"], c["kind"]) for c in every] == [
        ("public.orders", "truncate"), ("public.items", "truncate")]


def test_the_publication_lists_the_roots_and_publishes_under_them(
        monkeypatch):
    eng = _pg(monkeypatch, ROOTS, exclude=["public.orders"])
    monkeypatch.setattr(type(eng), "_plan_rows", lambda self, side, db, sql:
                        [(t,) for t in sorted(ROOTS)]
                        if "pg_tables" in sql else [])
    monkeypatch.setattr(type(eng), "_server_version",
                        lambda self, side, db: 160000)
    # `exclude: [public.orders]` published every partition of it
    assert eng.published_tables("appdb") == ["public.items"]
    assert eng._via_root_of("appdb")
    made = eng._publication_sql("migkit_own", ["public.orders"], True)
    assert "with (publish_via_partition_root = true)" in made
    # a hop with no partitions: the publication as it always was
    plain = _pg(monkeypatch, {"public.items": "public.items"})
    monkeypatch.setattr(type(plain), "_plan_rows", lambda self, side, db,
                        sql: [("public.items",)])
    assert plain.published_tables("appdb") == ["public.items"]
    assert "publish_via" not in plain._publication_sql(
        "migkit_own", ["public.items"], plain._via_root_of("appdb"))


def test_a_subscription_lets_go_of_tables_its_publication_left(monkeypatch):
    """`set table` that only took tables away asked for no refresh, and
    the stream made ahead read as knowing tables its publication did
    not."""
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop())

    def rows(self, side, db, sql):
        if "pg_subscription_rel" in sql:
            return [("public.a",), ("public.gone",)]
        return [(1,)]
    monkeypatch.setattr(PostgresEngine, "_plan_rows", rows)
    assert eng._refresh_sql("appdb", "s", ["public.a"]) == [
        "alter subscription s refresh publication with (copy_data = true);"]
    assert eng._refresh_sql("appdb", "s", ["public.a", "public.gone"]) == []


def test_an_unchanged_large_value_is_never_written_as_the_placeholder():
    """test_decoding prints a value stored out of line that an UPDATE did
    not touch as `unchanged-toast-datum`, unquoted: read as text, it was
    written over the target's value."""
    line = ("table public.docs: UPDATE: id[integer]:1 n[integer]:2"
            " body[text]:unchanged-toast-datum")
    got = pgslot.change(pgslot.parse_line(line), ["id"])
    assert got["values"] == {"id": 1, "n": 2}
    assert got[canon.UNCHANGED] == ["body"]
    # the whole previous row holds it, where the table keeps one
    full = ("table public.docs: UPDATE: old-key: id[integer]:1 n[integer]:1"
            " body[text]:'the large value' new-tuple: id[integer]:1"
            " n[integer]:2 body[text]:unchanged-toast-datum")
    got = pgslot.change(pgslot.parse_line(full), ["id"])
    assert got["values"]["body"] == "the large value"
    assert canon.UNCHANGED not in got
    # a value of that text is quoted, and is a value
    quoted = ("table public.docs: UPDATE: id[integer]:1"
              " body[text]:'unchanged-toast-datum'")
    got = pgslot.change(pgslot.parse_line(quoted), ["id"])
    assert got["values"]["body"] == "unchanged-toast-datum"


# ---- a DTS job's DDL, and the hop's own check of it -------------------------

def _tencent(options):
    return {"Response": {"TotalCount": 1, "JobList": [{
        "JobId": "sync-1", "JobName": "s", "SrcDatabaseType": "mysql",
        "DstDatabaseType": "mysql",
        "SrcInfo": {"Ip": "10.0.0.31", "Port": 3306, "User": "rep"},
        "Options": options,
        "Objects": {"Mode": "Partial", "Databases": [
            {"DbName": "shop", "DbMode": "All"}]}}]}}


def test_a_tencent_job_s_ddl_options_become_the_hop_s_ddl():
    got = imports.from_tencent_dts(_tencent({
        "OpTypes": ["Insert", "Update", "Delete", "DDL"]}))
    assert got.raw["options"]["ddl"] == "apply"
    got = imports.from_tencent_dts(_tencent({
        "OpTypes": ["Insert", "Update", "Delete", "PartialDDL"],
        "DdlOptions": [{"DdlObject": "Table",
                        "DdlValue": ["Create", "Alter", "Rename"]}]}))
    assert got.raw["options"]["ddl"] == {"default": "apply",
                                         "skip": ["truncate", "drop"]}
    got = imports.from_tencent_dts(_tencent({
        "OpTypes": ["Insert", "Update", "Delete"]}))
    assert "ddl" not in (got.raw.get("options") or {})
    assert "carries no DDL" in " | ".join(got.notes)


def test_an_alibaba_job_s_ddl_op_becomes_the_hop_s_ddl():
    job = {"DtsJobName": "a",
           "SourceEndpoint": {"EngineName": "MySQL", "Ip": "10.0.0.41",
                              "Port": 3306, "UserName": "dts"},
           "DbObject": json.dumps({"shop": {"name": "shop", "all": False,
                                            "Table": {"orders": {
                                                "name": "orders",
                                                "all": True,
                                                "ddl_op": "ct,at,rt"}}}})}
    got = imports.from_alibaba_dts(job)
    assert got.raw["options"]["ddl"] == {"default": "apply",
                                         "skip": ["truncate", "drop"]}


def test_the_hop_s_ddl_is_checked_before_anything_runs():
    def problems(ddl):
        raw = {"engine": "mysql", "source": "mysql://u@10.0.0.1/shop",
               "target": "mysql://u@10.0.0.2/shop",
               "options": {"ddl": ddl}}
        return [p.text for p in hopkeys.check_hop("h", raw)
                if p.where.startswith("hops.h.options.ddl")]
    assert problems("park") == []
    assert problems({"default": "park", "billing.*": "apply",
                     "skip": ["drop", "truncate"]}) == []
    assert problems("carry")
    assert problems({"billing.*": "copy"})
    assert any("an ALTER or a rename left out" in t
               for t in problems({"skip": ["alter"]}))


def test_two_hops_that_are_each_other_s_way_back_take_the_same_tables():
    src = {"host": "10.0.0.1", "port": 5432, "user": "u"}
    dst = {"host": "10.0.0.2", "port": 5432, "user": "u"}

    def file(back_exclude):
        return {"hops": {
            "ab": {"engine": "postgres", "source": src, "target": dst,
                   "db_map": {"shop": "shop_new"},
                   "exclude": ["shop.public.audit"],
                   "options": {"two_way": {"on_conflict": "overwrite"}}},
            "ba": {"engine": "postgres", "source": dst, "target": src,
                   "db_map": {"shop_new": "shop"},
                   "exclude": back_exclude,
                   "options": {"two_way": {"on_conflict": "ignore"}}}}}

    def apart(back_exclude):
        return [p.text for p in hopkeys.check(file(back_exclude))
                if p.where == "hops.ab and hops.ba"]
    # the same table, named as each side names its database
    assert apart(["shop_new.public.audit"]) == []
    got = apart([])
    assert got and "carried one way and never back" in got[0]
