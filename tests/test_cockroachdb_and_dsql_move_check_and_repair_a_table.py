"""A table made, copied, compared and repaired by the engines on PostgreSQL's
wire (P0-PARITY.move, .verify), on a throwaway PostgreSQL 16 server.

* a table the target lacks is made from the source's definition before it
  is filled, under the name the hop gives it, and each table is copied in
  batches that a stop between two leaves resumable from the last;
* the check pairs a renamed table with what it was renamed to, names the
  table a finding is about as its source names it, and counts the rows
  apart one by one;
* the repair carries the rows the last check named, writing what it
  overwrites and removes to the undo file first;
* rows are sized where they lie, views and foreign keys are read.

It is PostgreSQL, not DSQL or CockroachDB: it proves the shared code of
`pgwire` over what the three share, through each engine's own class
(`tests/pgwire_rig.py`), and what only a DSQL cluster answers (its limits,
`ASYNC` indexes, `sys.jobs`) or a CockroachDB cluster (`UPSERT`, `AS OF
SYSTEM TIME`, `crdb_internal`) is not run. Skips where no PostgreSQL server
programs are installed.
"""
import json

import pytest

from migkit.cli import _Checkpoint
from tests import pg_local
from tests.pg_local import server  # noqa: F401 - the module's fixture
from tests.pgwire_rig import pair, shared_cockroach  # noqa: F401

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("move.table-copy", "dsql"),
    ("move.create-missing", "dsql"),
    ("move.sized", "dsql"),
    ("scope.names", "dsql"),
    ("verify.repair", "dsql"),
    ("verify.repair-undo", "dsql"),
    ("verify.rows-apart", "dsql"),
    ("verify.table", "dsql"),
    ("schema.foreign-keys", "dsql"),
    ("schema.views", "dsql"),
]

pytestmark = pg_local.needs_server

ENGINES = ["dsql", "cockroachdb"]

SCHEMA = """
create table public.parent(id bigint primary key, name text);
create table public.items(id bigint primary key,
    pid bigint references public.parent(id), name text, amt numeric(12,2),
    big bytea);
create table public.plain(id bigint primary key, v text);
create view public.big_items as select id, name from public.items
    where amt > 10;
"""
ROWS = """
insert into parent select g, 'p' || g from generate_series(1, 3) g;
insert into items select g, 1 + g % 3, 'n' || g, g * 1.5,
    decode(repeat('ab', g * 10), 'hex') from generate_series(1, 12) g;
insert into plain select g, 'v' || g from generate_series(1, 4) g;
"""


class Stop(Exception):
    pass


def _source(server, tmp_path, name, **options):
    eng, src, dst = pair(server, tmp_path, name, **options)
    server.run(SCHEMA, src)
    server.run(ROWS, src)
    return eng, src, dst


def _copy(eng, src, ck, log, chunk=5):
    for sch, tbl in eng.list_move_tables(src):
        eng.move_table(src, sch, tbl, chunk, ck, log)


@pytest.fixture(scope="module", params=ENGINES)
def moved(request, server, tmp_path_factory):
    """A source copied to an empty target through a stop in the middle of
    one table, and the engine, its log and the rows each side holds."""
    tmp = tmp_path_factory.mktemp("move")
    eng, src, dst = _source(server, tmp, request.param,
                            mapping={"tables": {
                                "public.items": "public.items_v2"}})
    log = []
    eng.create_missing(src, log.append)
    made = log[:]
    seen = []

    def stopper(line):
        log.append(line)
        seen.append(line)
        if len(seen) == 3:
            raise Stop()
    ck = _Checkpoint(tmp / "move.json")
    with pytest.raises(Stop):
        _copy(eng, src, ck, stopper)
    stopped_at = dict(_Checkpoint(tmp / "move.json"))
    _copy(eng, src, _Checkpoint(tmp / "move.json"), log.append)
    return {"eng": eng, "src": src, "dst": dst, "log": log, "made": made,
            "stopped_at": stopped_at, "server": server, "tmp": tmp}


def test_a_table_the_target_lacks_is_made_under_the_name_the_hop_gives_it(
        moved):
    made = " ".join(moved["made"])
    assert 'create table "public"."items_v2"' in made
    assert 'create table "public"."plain"' in made
    got = moved["server"].run(
        "select table_name from information_schema.tables where"
        " table_schema = 'public' order by 1", moved["dst"])
    assert [r[0] for r in got] == ["items_v2", "parent", "plain"]


def test_a_stop_between_two_batches_is_resumed_from_the_last(moved):
    # the first table's batches of five stopped on the third line said
    assert moved["stopped_at"], moved["stopped_at"]
    pieces = [l for l in moved["log"] if "rows," in l]
    assert any("5 rows" in l for l in pieces), pieces
    counted = moved["server"].run("select count(*) from items_v2",
                                  moved["dst"])
    assert counted == [(12,)]


def test_the_check_pairs_a_renamed_table_with_what_it_was_renamed_to(moved):
    eng, src = moved["eng"], moved["src"]
    counts = eng.check_counts(src)
    assert [r.status for r in counts] == ["ok"], [r.detail for r in counts]
    assert counts[0].detail.startswith("rows 19==19")
    data = {r.table: r for r in eng.check_data(src)}
    assert data["public.items"].status == "ok"
    assert data["public.plain"].status == "ok"


def test_a_difference_names_its_table_and_counts_the_rows_apart(moved):
    eng, src, dst = moved["eng"], moved["src"], moved["dst"]
    moved["server"].run("update plain set v = 'x' where id = 2;"
                        " delete from plain where id = 4;"
                        " insert into plain values (9, 'z')", dst)
    found = {r.table: r for r in eng.check_data(src)}
    assert found["public.plain"].status == "diff"
    assert found["public.plain"].rows_apart == {
        "public.plain": {"missing": 1, "extra": 1, "changed": 1,
                         "exact": True}}
    assert found["public.items"].status == "ok"
    assert found["public.items"].rows_apart == {}


def test_the_repair_carries_the_named_rows_and_writes_the_undo_first(moved):
    eng, src, dst = moved["eng"], moved["src"], moved["dst"]
    server = moved["server"]
    server.run("truncate plain; insert into plain values (1, 'v1'),"
               " (2, 'x'), (3, 'v3'), (9, 'z')", dst)
    assert [r.status for r in eng.check_data(src) if r.table ==
            "public.plain"] == ["diff"]
    actions = eng.repair_plan(src, "rows")
    assert len(actions) == 1 and "1 missing, 1 changed, 1 extra" in \
        actions[0].note
    eng.apply(src, actions[0])
    after = [r for r in eng.check_data(src) if r.table == "public.plain"]
    assert [r.status for r in after] == ["ok"]
    assert server.run("select * from plain order by 1", dst) == [
        (1, "v1"), (2, "v2"), (3, "v3"), (4, "v4")]
    # what it overwrote and what it removed is the undo's: the row that was
    # only missing had nothing to save
    undo = moved["tmp"] / "undo" / "plain.rows.jsonl"
    saved = {tuple(json.loads(l)["key"]): json.loads(l)["row"]
             for l in undo.read_text().splitlines()}
    assert sorted(saved) == [("2",), ("9",)], sorted(saved)
    assert saved[("2",)]["v"] == "x" and saved[("9",)]["v"] == "z"
    assert eng.repair_plan(src, "rows") == []


def test_rows_are_sized_where_they_lie(moved):
    sizes = moved["eng"].row_sizes("src", moved["src"])
    assert sizes["public.items"]["largest"] >= 120
    assert sizes["public.items"]["exact"] is True
    assert sizes["public.plain"]["largest"] < sizes["public.items"]["largest"]


def test_foreign_keys_and_views_are_read(moved):
    eng, src = moved["eng"], moved["src"]
    assert eng.foreign_keys("src", src) == [
        ("public.items", ["pid"], "public.parent", ["id"])]
    views = dict(eng.neutral_views("src", src))
    assert list(views) == ["public.big_items"]
    assert "amt" in views["public.big_items"]


@pytest.mark.parametrize("name", ENGINES)
def test_a_view_the_hop_does_not_carry_is_not_read(server, tmp_path, name):
    eng, src, dst = _source(server, tmp_path, name,
                            objects={"views": False})
    assert eng.neutral_views("src", src) == []
