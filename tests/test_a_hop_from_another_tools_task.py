"""A hop from another tool's task (backlog R21: switching costs nothing).

The sample files under `tests/samples` were written for these tests (no
real system's): a replication task with its endpoints, a change-capture
connector's JSON and properties, a LOAD DATABASE command file.
"""
import json
from pathlib import Path

import pytest

from migkit import hopkeys, imports
from migkit.config import build_hop

SAMPLES = Path(__file__).resolve().parent / "samples"


def _hop(got, target=None):
    raw = dict(got.raw)
    if target:
        raw["target"] = target
    assert not [p for p in hopkeys.check({"hops": {got.name: raw}})
                if p.level == "error"], raw
    return build_hop(got.name, raw)


def test_a_replication_task_with_its_endpoints():
    got = imports.read(SAMPLES / "dms-task.json")
    assert got.name == "orders-to-aurora"
    assert got.raw["goal"] == "cutover"
    assert got.raw["source"] == (
        "postgres://dms_reader@orders.example.eu-west-1.rds.amazonaws.com"
        "/orders?sslmode=verify-full")
    assert got.raw["target"].startswith(
        "mysql://dms_writer@orders.cluster-example.")
    assert got.raw["exclude"] == ["public.tmp*"]
    m = got.raw["mapping"]
    assert m["tables"] == {"public.order_items": "public.lines"}
    assert m["columns"] == {"public.orders": {"drop": ["notes"]}}
    # the conditions of a column any of them, the columns all - each name
    # and value quoted as the source's engine quotes them
    assert m["where"] == {"public.orders": "(\"region\" = 'EU' or"
                                           " \"region\" = 'UK') and"
                                           " \"id\" >= '1000'"}
    # what the task took is the hop's own include, not a guess made later
    assert got.raw["include"] == ["public.orders", "public.order_items"]
    assert got.include == []
    # a name rule for every schema is the hop's own name rule
    assert m["names"] == {"schemas": {"case": "lower"}}
    said = " | ".join(got.notes)
    assert "convert-lowercase" not in said
    assert "DROP_AND_CREATE" in said and "LOB" in said, said
    hop = _hop(got)
    assert hop.engine == "hetero" and hop.databases == ["orders"]
    assert hop.options["reverse"] == "at_cutover"
    assert hop.row_filter("public", "orders").startswith('("region"')
    assert hop.target.mysql_tls()["ssl"] is not None


def test_a_task_alone_leaves_its_endpoints_to_be_asked():
    task = json.loads((SAMPLES / "dms-task.json").read_text())
    del task["Endpoints"]
    got = imports.from_dms(task)
    assert "source" not in got.raw and "target" not in got.raw
    assert sum("names no source" in n or "names no target" in n
               for n in got.notes) == 2


def test_a_task_with_what_migkit_cannot_carry_says_so():
    task = {"ReplicationTaskIdentifier": "t", "MigrationType": "full-load",
            "TableMappings": {"rules": [
                {"rule-type": "selection", "rule-id": "1",
                 "rule-name": "all", "rule-action": "include",
                 "object-locator": {"schema-name": "%",
                                    "table-name": "%"},
                 "filters": [{"filter-type": "source", "column-name": "id",
                              "filter-conditions": [{
                                  "filter-operator": "gte",
                                  "value": "5"}]}]}]}}
    got = imports.from_dms(task)
    assert "goal" not in got.raw and "mapping" not in got.raw
    assert got.include == []
    assert any("not carried" in n for n in got.notes)


def test_a_connector_as_json():
    got = imports.read(SAMPLES / "connector-postgres.json")
    assert got.name == "inventory-connector"
    assert got.raw["source"] == ("postgres://cdc@10.0.0.12:5433/inventory"
                                 "?sslmode=require")
    assert "target" not in got.raw
    assert got.raw["goal"] == "keep-in-sync"
    assert got.raw["exclude"] == ["public.audit_2019"]
    # each pattern the task took is one a shell pattern says: the hop's
    # own include, which holds whether or not the guide reaches the source
    assert got.raw["include"] == ["public.orders", "public.customers",
                                  "public.audit_*"]
    assert got.include == []
    assert any("password" in n for n in got.notes)
    # and the password in the file is nowhere in the hop
    assert "CHANGE_ME" not in json.dumps(got.raw)
    hop = _hop(got, "postgres://app@10.0.0.13/inventory")
    assert hop.source.port == 5433 and hop.databases == ["inventory"]


def test_a_connector_as_properties():
    got = imports.read(SAMPLES / "connector-mysql.properties")
    assert got.name == "shop-cdc"
    assert got.raw["source"] == "mysql://cdc@mysql.example.com:3307/shop"
    assert got.raw["exclude"] == ["shop.sessions"]
    # a regex a shell pattern cannot say is kept as the regex
    assert got.exclude_re == ["shop\\.cache_[0-9]+"]
    assert any("snapshot.mode never" in n for n in got.notes)


def test_a_load_command_file():
    got = imports.read(SAMPLES / "sakila.load")
    assert got.raw["source"] == "mysql://root@mysql.example.com:3306/sakila"
    assert got.raw["target"] == ("postgresql://app@pg.example.com:5432/"
                                 "pagila?sslmode=require")
    assert got.raw["mapping"] == {"tables": {"actor": "actors"}}
    assert got.include == [("search", "film"), ("glob", "actor"),
                           ("glob", "rental")]
    said = " | ".join(got.notes)
    assert "password" in said and "CAST" in said and "BEFORE LOAD" in said
    assert "CHANGE_ME" not in json.dumps(got.raw)
    hop = _hop(got)
    assert hop.databases == ["sakila"] and hop.db_map == {"sakila": "pagila"}
    assert hop.target.libpq_tls() == {"sslmode": "require"}


def test_what_a_task_took_becomes_what_the_hop_leaves_out():
    """migkit's hop says what it leaves out; a task that listed what it
    took is turned into that against the source's own tables."""
    load = imports.read(SAMPLES / "sakila.load")
    ids = ["sakila.actor", "sakila.film", "sakila.film_text",
           "sakila.film_actor", "sakila.rental", "sakila.payment",
           "sakila.staff"]
    assert imports.left_out(load, ids) == ["sakila.film_text",
                                           "sakila.payment", "sakila.staff"]
    dms = imports.read(SAMPLES / "dms-task.json")
    assert imports.left_out(dms, ["public.orders", "public.order_items",
                                  "public.users"]) == ["public.users"]
    conn = imports.read(SAMPLES / "connector-mysql.properties")
    assert imports.left_out(conn, ["shop.orders", "shop.cache_12",
                                   "shop.cache_x"]) == ["shop.cache_12"]
    # a task that took everything leaves nothing out
    assert imports.left_out(imports.Imported("x", {}), ids) == []


@pytest.mark.parametrize("text,why", [
    ('{"a": 1}', "neither a replication task"),
    ("LOAD CSV FROM 'x.csv' INTO postgresql:///db", "only LOAD DATABASE"),
    ("hello", "not a file migkit reads a hop from"),
])
def test_what_is_not_a_task_is_refused(tmp_path, text, why):
    f = tmp_path / "task.txt"
    f.write_text(text)
    with pytest.raises(SystemExit) as e:
        imports.read(f)
    assert why in str(e.value)


def test_nothing_said_of_an_import_names_a_program():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    for name in ("dms-task.json", "connector-postgres.json",
                 "connector-mysql.properties", "sakila.load"):
        got = imports.read(SAMPLES / name)
        said = " ".join(got.notes).lower()
        assert not [t for t in TOOLS if t in said], (name, said)


# ---- audit 2026-10-01: DU4, K16 -------------------------------------------

def _filter(column, value, engine):
    return imports._dms_filter([{
        "filter-type": "source", "column-name": column,
        "filter-conditions": [{"filter-operator": "eq", "value": value}]}],
        engine)


def test_a_filter_names_its_column_and_value_as_the_source_quotes_them():
    """`user` bare on PostgreSQL is the session's user, so a filter on
    the column read zero rows on both sides and said "same" over
    nothing; `order` was a syntax error; MySQL read `'C:\\new'` with a
    newline in it."""
    assert _filter("user", "bob", "postgres") == "\"user\" = 'bob'"
    assert _filter("order", "x", "mysql") == "`order` = 'x'"
    # MySQL reads a backslash in a string as an escape: doubled
    assert _filter("path", "C:\\new", "mysql") == "`path` = 'C:\\\\new'"
    # PostgreSQL does not: left as it is
    assert _filter("path", "C:\\new", "postgres") == "\"path\" = 'C:\\new'"
    assert _filter("it's", "o'k", "postgres") == "\"it's\" = 'o''k'"


def test_a_wildcard_database_list_is_carried_not_dropped(tmp_path):
    f = tmp_path / "c.properties"
    f.write_text("name=x\n"
                 "connector.class=io.example.connector.mysql.MySqlConnector\n"
                 "database.hostname=10.0.0.5\n"
                 "database.include.list=shop_.*,crm\n")
    got = imports.read(f)
    # every database read, the hop's include keeping the tables of those
    # the patterns match - it was the literal names only, `shop_.*` left
    # out without a word and its databases never moved
    assert "databases" not in got.raw
    assert got.raw["include"] == ["shop_*.*", "crm.*"], got.raw
    assert any("database.include.list holds patterns" in n
               for n in got.notes), got.notes
    hop = build_hop("x", {**got.raw, "target": "mysql://10.0.0.6"})
    assert not hop.excluded("shop_eu", "orders")
    assert hop.excluded("billing", "orders")
    assert not hop.excluded("crm", "people")


def test_every_pattern_a_hop_cannot_say_is_said():
    """A regular expression or a search the hop's include cannot carry
    stays for the guide to apply, and each is said - where the guide
    cannot reach the source, the hop would take every table."""
    load = imports.read(SAMPLES / "sakila.load")
    assert "include" not in load.raw
    said = [n for n in load.notes if "include cannot say" in n]
    assert len(said) == 1 and "film" in said[0], load.notes
    conn = imports.read(SAMPLES / "connector-mysql.properties")
    said = [n for n in conn.notes if "exclude cannot say" in n]
    assert len(said) == 1 and "cache_[0-9]+" in said[0], conn.notes
