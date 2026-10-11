"""What a Cassandra keyspace holds beside its rows is compared, and the
statements that make the target like the source are written for it
(P0-PARITY.verify): secondary indexes, triggers, materialized views, the
user types the taken tables use, functions and aggregates - each held to
the hop's `objects` and to the tables it takes - the options of a table that
decide what it keeps (its time to live, its grace for a deleted row, its
compaction) and the order its rows are clustered in, and the schema as a
whole, object by object, with `structural-fix.cql` and the statements that
undo it.

No server: the cluster is `tests/cassandra_fake.py`. The schema tables
these read (`system_schema.indexes`, `.triggers`, `.views`, `.types`,
`.functions`, `.aggregates`) are statements this engine has not yet sent
to a real cluster, so the cells they stand behind are built and not yet
proved: they wait in `capabilities.UNPROVED` for a test on a server, and
this module names none in `PROVES`.
"""
from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from tests import cassandra_fake as fake

COLS = {"id": "int", "name": "text"}


def _clusters():
    src, dst = fake.Cluster(), fake.Cluster()
    for cl in (src, dst):
        cl.table("shop", "orders", COLS, ["id"])
        cl.table("shop", "audit_log", COLS, ["id"])
    return src, dst


def _engine(tmp_path, monkeypatch, src, dst, **kw):
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop)


def _found(eng, scope):
    got = [r for r in eng.check_deep("shop") if r.scope == f"shop {scope}"]
    assert len(got) == 1, [r.scope for r in eng.check_deep("shop")]
    return got[0]


def _index(cl, table, name, target="name", klass=None):
    opts = {"target": target, **({"class_name": klass} if klass else {})}
    cl.keyspaces["shop"][table].indexes[name] = {
        "kind": "CUSTOM" if klass else "COMPOSITES", "options": opts}


def test_nothing_beside_the_rows_is_found_equal_when_it_is(tmp_path,
                                                           monkeypatch):
    src, dst = _clusters()
    for cl in (src, dst):
        _index(cl, "orders", "by_name")
        cl.keyspaces["shop"]["orders"].triggers["audit"] = {"class": "A"}
        cl.add_view("shop", "recent", "orders")
        cl.add_type("shop", "address", [("street", "text")])
        cl.add_function("shop", "fn", [("a", "int")], "int")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    for word in ("indexes", "triggers", "matviews", "objects"):
        assert _found(eng, word).status == "ok", word
    assert _found(eng, "(structural)").status == "ok"
    assert not (tmp_path / "structural-fix.cql").exists()


def test_an_index_the_target_lacks_is_named_and_a_statement_written(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    _index(src, "orders", "by_name")
    _index(src, "orders", "by_text", target="name", klass="StorageAttached")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "indexes")
    assert got.status == "diff" and "orders.by_name: not on the target" \
        in got.detail
    fix = (tmp_path / "structural-fix.cql").read_text()
    assert 'create index "by_name" on "shop"."orders" (name);' in fix
    assert ('create custom index "by_text" on "shop"."orders" (name)'
            " using 'StorageAttached';") in fix
    revert = (tmp_path / "structural-fix.revert.cql").read_text()
    assert 'drop index "shop"."by_name";' in revert


def test_an_index_made_otherwise_is_named_not_remade(tmp_path, monkeypatch):
    src, dst = _clusters()
    _index(src, "orders", "by_name", target="name")
    _index(dst, "orders", "by_name", target="keys(name)")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "indexes")
    assert got.status == "diff" and "on the source" in got.detail
    assert not (tmp_path / "structural-fix.cql").exists()
    assert "by hand" in _found(eng, "(structural)").detail


def test_an_index_of_a_table_the_hop_leaves_out_is_not_a_finding(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    _index(src, "audit_log", "by_name")
    eng = _engine(tmp_path, monkeypatch, src, dst, exclude=["audit_log"])
    assert _found(eng, "indexes").status == "ok"


def test_a_trigger_the_target_lacks_is_named_and_a_statement_written(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.keyspaces["shop"]["orders"].triggers["audit"] = {"class": "A"}
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "triggers")
    assert got.status == "diff" and "orders.audit" in got.detail
    assert ("create trigger \"audit\" on \"shop\".\"orders\" using 'A';"
            in (tmp_path / "structural-fix.cql").read_text())
    assert 'drop trigger "audit" on "shop"."orders";' in \
        (tmp_path / "structural-fix.revert.cql").read_text()


def test_a_materialized_view_the_target_lacks_is_named_for_hand_work(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.add_view("shop", "recent", "orders")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "matviews")
    assert got.status == "diff" and "recent: not on the target" in got.detail
    assert "by hand: views recent" in _found(eng, "(structural)").detail


def test_a_hop_that_carries_no_views_is_not_held_to_them(tmp_path,
                                                         monkeypatch):
    src, dst = _clusters()
    src.add_view("shop", "recent", "orders")
    eng = _engine(tmp_path, monkeypatch, src, dst, objects={"views": False})
    assert _found(eng, "matviews").status == "ok"


def test_a_view_over_a_table_the_hop_leaves_out_is_not_a_finding(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.add_view("shop", "old", "audit_log")
    eng = _engine(tmp_path, monkeypatch, src, dst, exclude=["audit_log"])
    assert _found(eng, "matviews").status == "ok"


def test_a_user_type_a_taken_table_uses_is_needed_and_one_it_does_not_is_not(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.table("shop", "people", {"id": "int", "home": "frozen<address>"},
              ["id"])
    dst.table("shop", "people", {"id": "int", "home": "frozen<address>"},
              ["id"])
    src.add_type("shop", "address", [("street", "text"),
                                     ("geo", "frozen<point>")])
    src.add_type("shop", "point", [("x", "double"), ("y", "double")])
    src.add_type("shop", "unused", [("a", "int")])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "objects")
    assert got.status == "diff"
    assert "address" in got.detail and "point" in got.detail
    assert "unused" not in got.detail
    fix = (tmp_path / "structural-fix.cql").read_text()
    assert 'create type "shop"."address" ("street" text, "geo" ' \
        "frozen<point>);" in fix


def test_a_function_or_aggregate_the_target_lacks_is_named_with_its_statement(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.add_function("shop", "double_it", [("a", "int")], "int",
                     body="return a * 2;")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "objects")
    assert got.status == "diff" and "double_it(int)" in got.detail
    fix = (tmp_path / "structural-fix.cql").read_text()
    assert ('create or replace function "shop"."double_it" ("a" int) '
            "returns null on null input returns int language java as "
            "$$return a * 2;$$;") in fix


def test_a_function_with_another_body_is_named_not_replaced(tmp_path,
                                                            monkeypatch):
    src, dst = _clusters()
    src.add_function("shop", "f", [("a", "int")], "int", body="return 1;")
    dst.add_function("shop", "f", [("a", "int")], "int", body="return 2;")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "objects")
    assert got.status == "diff" and "f(int)" in got.detail
    assert not (tmp_path / "structural-fix.cql").exists()


def test_a_hop_that_carries_no_functions_is_not_held_to_them(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    src.add_function("shop", "f", [("a", "int")], "int")
    eng = _engine(tmp_path, monkeypatch, src, dst,
                  objects={"functions": False})
    assert _found(eng, "objects").status == "ok"


def test_a_kind_that_could_not_be_read_is_an_error_not_a_pass(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    dst.broken.add("system_schema.triggers")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "triggers")
    assert got.status == "error" and "could not read triggers" in got.detail
    assert _found(eng, "(structural)").status == "error"
    assert _found(eng, "indexes").status == "ok"


def test_the_fix_script_of_a_run_that_found_nothing_is_taken_away(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    _index(src, "orders", "by_name")
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert _found(eng, "indexes").status == "diff"
    assert (tmp_path / "structural-fix.cql").exists()
    _index(dst, "orders", "by_name")
    assert _found(eng, "indexes").status == "ok"
    assert not (tmp_path / "structural-fix.cql").exists()
    assert not (tmp_path / "structural-fix.revert.cql").exists()


def test_an_option_that_decides_what_a_table_keeps_is_named_with_its_statement(
        tmp_path, monkeypatch):
    src, dst = _clusters()
    t = src.keyspaces["shop"]["orders"]
    t.options["default_time_to_live"] = 3600
    t.options["compaction"] = {"class": "TimeWindowCompactionStrategy",
                               "compaction_window_size": "1"}
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "table options")
    assert got.status == "diff"
    assert got.category == "structure.collection-options"
    assert "orders.default_time_to_live: 3600 on the source, 0 on the" \
        " target" in got.detail
    fix = (tmp_path / "structural-fix.cql").read_text()
    assert 'alter table "shop"."orders" with default_time_to_live = 3600;' \
        in fix
    assert ("alter table \"shop\".\"orders\" with compaction = {'class':"
            " 'TimeWindowCompactionStrategy', 'compaction_window_size':"
            " '1'};") in fix


def test_options_that_are_the_same_are_found_equal(tmp_path, monkeypatch):
    src, dst = _clusters()
    for cl in (src, dst):
        cl.keyspaces["shop"]["orders"].options["default_time_to_live"] = 60
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert _found(eng, "table options").status == "ok"


def test_a_clustering_order_that_differs_is_named_not_altered(tmp_path,
                                                              monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    for cl in (src, dst):
        cl.table("shop", "events", {"id": "int", "day": "int"}, ["id"],
                 ["day"])
    src.keyspaces["shop"]["events"].orders = {"day": "desc"}
    dst.keyspaces["shop"]["events"].orders = {"day": "asc"}
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = _found(eng, "table options")
    assert got.status == "diff"
    assert "events.day: clustered desc on the source, asc on the target" \
        in got.detail
    assert not (tmp_path / "structural-fix.cql").exists()
