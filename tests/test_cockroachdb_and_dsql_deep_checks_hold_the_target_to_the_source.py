"""The deep checks of the engines on PostgreSQL's wire hold a target to its
source (P0-PARITY.verify), read off the catalogue both sides keep.

What a count and a checksum do not say: a table with no key, a column that
is narrower, nullable or defaulted otherwise on the target, an index or a
foreign key the target lacks, a maximum key past the source's, a collation
or an encoding that differs, a generated column plain on the other side,
NULL and the empty string swapped, a float that drifted, a timestamp shifted
a zone's offset, the table's owner and who may use it - and, for the server
that keeps them, triggers, materialized views, deferrable and unvalidated
foreign keys.

It runs on a throwaway PostgreSQL 16 server (`tests/pg_local.py`), not on
CockroachDB or Aurora DSQL: it proves the shared code of `pgwire` over the
surface the three share (`information_schema`, `pg_catalog`, standard SQL),
through each engine's own class (`tests/pgwire_rig.py`). Statements only a
CockroachDB cluster or a DSQL cluster answers - `crdb_internal`, `sys.jobs`,
`ASYNC` indexes - are not run, and no cell here is proved on those servers.
The tests skip where no PostgreSQL server programs are installed.
"""
import itertools

import pytest

from migkit.engines.base import CATEGORIES
from tests import pg_local
from tests.pg_local import server  # noqa: F401 - the module's fixture
from tests.pgwire_rig import pair, shared_cockroach  # noqa: F401

#: what this module proves, item by engine (`capabilities.proofs`): the
#: shared checks over the catalogue a DSQL cluster documents (`pg_class`,
#: `pg_attribute`, `pg_constraint`, `pg_index`, `pg_tables`, `pg_attrdef`)
#: and standard SQL, run through the engine's own class
PROVES = [
    ("verify.structure.keys", "dsql"),
    ("verify.structure.columns", "dsql"),
    ("verify.value.narrowing", "dsql"),
    ("verify.structure.indexes", "dsql"),
    ("verify.movement.target-ahead", "dsql"),
    ("verify.value.null-empty", "dsql"),
    ("verify.value.precision", "dsql"),
    ("verify.value.timezone", "dsql"),
    ("verify.access.object-ownership", "dsql"),
    ("verify.structure.foreign-keys", "dsql"),
    ("verify.structure.deferrable", "dsql"),
    ("verify.structure.unvalidated-constraints", "dsql"),
    ("verify.structure.schema-diff", "dsql"),
    ("verify.value.collation", "dsql"),
    ("verify.value.generated", "dsql"),
    ("verify.value.rendering", "dsql"),
]

pytestmark = pg_local.needs_server

ENGINES = ["dsql", "cockroachdb"]
ROLES = itertools.count(1)

SOURCE = """
create table public.parent(id bigint primary key, name varchar(40),
    amt real, ts timestamp, note text, g int generated always as (id * 2)
    stored);
create table public.orders(id bigint primary key,
    pid bigint references public.parent(id) deferrable initially deferred,
    name text, amount numeric(12,2));
create index orders_pid on public.orders(pid);
create unique index orders_name on public.orders(name);
create table public.nokey(a int, b text);
"""
TARGET = """
create table public.parent(id bigint primary key, name varchar(10),
    amt double precision, ts timestamp, note text, g int generated always
    as (id * 2) stored);
create table public.orders(id bigint primary key, pid bigint,
    name text, amount numeric(10,2) not null);
create unique index orders_name on public.orders(name);
create table public.nokey(a int, b text);
"""


def _setup(server, tmp_path, name, source=SOURCE, target=TARGET, **options):
    eng, src, dst = pair(server, tmp_path, name, **options)
    server.run(source, src)
    server.run(target, dst)
    return eng, src, dst


def _by_word(results):
    """{the scope's last word: finding}, the unreadable ones among them."""
    return {r.scope.split()[-1]: r for r in results}


def _different(server, src, dst):
    server.run("insert into parent(id, name, amt, ts, note) select g,"
               " 'p' || g, g * 1.1, '2020-01-01 10:00'::timestamp + g *"
               " interval '1 hour', case when g % 2 = 0 then '' else null"
               " end from generate_series(1, 5) g", src)
    server.run("insert into parent(id, name, amt, ts, note) select g,"
               " 'p' || g, g * 1.1 + 0.001, '2020-01-01 13:00'::timestamp +"
               " g * interval '1 hour', case when g % 2 = 0 then null else"
               " '' end from generate_series(1, 5) g", dst)
    server.run("insert into orders values (1, 1, 'a', 1.5)", src)
    server.run("insert into orders values (1, 1, 'a', 1.5), (9, 1, 'z',"
               " 2.5)", dst)


@pytest.fixture(scope="module", params=ENGINES)
def differing(request, server, tmp_path_factory):
    """The source and a target that differs in every way the checks read,
    and the deep checks run over them once."""
    eng, src, dst = _setup(server, tmp_path_factory.mktemp("deep"),
                           request.param)
    _different(server, src, dst)
    return eng, src, dst, eng._shared_deep(src)


def _one(eng, src, check):
    """What one deep check says, run alone."""
    return check(src, eng._deep_pairs(src))


def test_every_deep_check_finds_what_a_count_does_not(differing):
    got = _by_word(differing[3])
    detail = {w: r.detail for w, r in got.items()}
    assert got["keys"].status == "diff" and "public.nokey" in detail["keys"]
    assert got["columns"].status == "diff"
    for part in ("public.orders.amount", "public.parent.name",
                 "public.parent.amt", "null allowed -> refused"):
        assert part in detail["columns"], detail["columns"]
    # the whole of what differs is kept in a file of the report
    assert "public.parent.name" in open(got["columns"].report).read()
    assert got["narrowing"].status == "diff"
    assert "varchar" in detail["narrowing"] or "character" in \
        detail["narrowing"]
    assert got["indexes"].status == "diff"
    assert "index orders_pid is not on the target" in detail["indexes"]
    assert got["boundary"].status == "diff"
    assert "dst_max=9" in detail["boundary"]
    assert got["nullempty"].status == "diff"
    assert "source null/empty=3/2, target=2/3" in detail["nullempty"]
    assert got["float"].status == "diff" and "public.parent.amt" in \
        detail["float"]
    assert got["timeshift"].status == "diff"
    assert "10800s" in detail["timeshift"]
    assert got["fk"].status == "diff"
    assert "public.orders(pid) -> public.parent(id)" in detail["fk"]
    # what is the same is said to be
    assert got["collation"].status == "ok"
    assert got["encoding"].status == "ok"
    assert got["generated"].status == "ok"
    assert got["ownership"].status == "ok"


@pytest.mark.parametrize("name", ENGINES)
def test_a_target_that_is_the_same_is_clean(server, tmp_path, name):
    eng, src, dst = _setup(server, tmp_path, name, SOURCE, SOURCE)
    server.run("insert into parent(id, name, amt, ts, note) select g,"
               " 'p' || g, g * 1.1, now(), case when g % 2 = 0 then ''"
               " else null end from generate_series(1, 5) g", src)
    server.run("insert into parent(id, name, amt, ts, note) select g,"
               " 'p' || g, g * 1.1, now(), case when g % 2 = 0 then ''"
               " else null end from generate_series(1, 5) g", dst)
    found = {r.scope.split()[-1]: (r.status, r.detail)
             for r in eng._shared_deep(src)}
    bad = {w: v for w, v in found.items()
           # a table with no key is the source's own, and is said
           if v[0] not in ("ok",) and w != "keys"}
    assert bad == {}, bad
    assert found["keys"][0] == "diff"


def test_the_deep_checks_say_what_they_are_about(differing):
    """Every finding's scope ends in a word that names a category, so a
    report and the depth matrix read what it is about."""
    for r in differing[3]:
        assert r.category != "deep.unclassified", r.scope
        assert r.category in set(CATEGORIES.values()), (r.scope, r.category)


@pytest.mark.parametrize("name", ENGINES)
def test_a_table_the_hop_leaves_out_is_never_read(server, tmp_path, name,
                                                  monkeypatch):
    eng, src, dst = _setup(server, tmp_path, name, exclude=["public.orders"])
    asked = []
    real = type(eng)._rows

    def spy(self, side, db, sql, args=None):
        asked.append(" ".join(sql.split()))
        return real(self, side, db, sql, args)
    monkeypatch.setattr(type(eng), "_rows", spy)
    for r in eng._shared_deep(src):
        assert "orders" not in r.detail, (r.scope, r.detail)
    # no per-table statement names it
    assert not [s for s in asked if '"orders"' in s]


def test_an_unvalidated_constraint_on_the_target_is_a_difference(
        server, tmp_path):
    eng, src, dst = _setup(server, tmp_path, "dsql", SOURCE, SOURCE)
    server.run("alter table orders add constraint pos check (amount > 0)"
               " not valid", dst)
    got = _one(eng, src, eng._deep_unvalidated)
    assert got.status == "diff" and "public.orders.pos" in got.detail
    assert "VALIDATE CONSTRAINT" in got.fix_hint
    server.run("alter table orders validate constraint pos", dst)
    assert _one(eng, src, eng._deep_unvalidated).status == "ok"


def test_a_foreign_key_that_is_not_deferrable_there_is_a_difference(
        server, tmp_path):
    """DSQL keeps foreign keys, and lets one be deferred to the end of a
    transaction: code that inserts a child before its parent works on one
    side and is refused on the other."""
    eng, src, dst = _setup(
        server, tmp_path, "dsql", SOURCE,
        SOURCE.replace("deferrable initially deferred", ""))
    got = _one(eng, src, eng._deep_deferrable)
    assert got.status == "diff" and "deferrable/initially deferred=True/" \
        "True on the source, False/False on the target" in got.detail


def test_a_foreign_key_that_acts_differently_is_a_difference(server,
                                                             tmp_path):
    eng, src, dst = _setup(
        server, tmp_path, "dsql", SOURCE,
        SOURCE.replace("deferrable initially deferred", "on delete cascade"))
    got = _one(eng, src, eng._deep_foreign_keys)
    assert got.status == "diff" and "acts differently on delete or " \
        "update" in got.detail


def test_cockroachdb_checks_its_triggers_and_materialized_views(
        server, tmp_path):
    eng, src, dst = _setup(
        server, tmp_path, "cockroachdb",
        SOURCE + """
        create materialized view public.mv as select id from public.parent;
        create function public.tf() returns trigger language plpgsql as
            $$ begin return new; end $$;
        create trigger tg before insert on public.orders for each row
            execute function public.tf();
        """, SOURCE)
    triggers = _one(eng, src, eng._deep_triggers)
    assert triggers.status == "diff"
    assert "public.orders.tg: not on the target" in triggers.detail
    views = _one(eng, src, eng._deep_matviews)
    assert views.status == "diff"
    assert "public.mv: not on the target" in views.detail


def test_a_trigger_switched_off_on_the_target_is_said(server, tmp_path):
    both = SOURCE + """
        create function public.tf() returns trigger language plpgsql as
            $$ begin return new; end $$;
        create trigger tg before insert on public.orders for each row
            execute function public.tf();
        """
    eng, src, dst = _setup(server, tmp_path, "cockroachdb", both, both)
    server.run("alter table orders disable trigger tg", dst)
    got = _one(eng, src, eng._deep_triggers)
    assert got.status == "diff" and "disabled on the target" in got.detail


def test_dsql_has_no_triggers_or_materialized_views_to_check(server,
                                                             tmp_path):
    eng, src, dst = _setup(server, tmp_path, "dsql")
    words = {w for w, _ in eng._deep_checks()}
    assert not words & {"triggers", "matviews"}


def test_a_grant_the_target_lacks_is_a_difference(server, tmp_path):
    eng, src, dst = _setup(server, tmp_path, "dsql", SOURCE, SOURCE)
    role = f"app_{next(ROLES)}_{id(server) % 10000}"
    server.run(f'create role "{role}"')
    server.run(f'grant select, insert on public.orders to "{role}"', src)
    got = _one(eng, src, eng._deep_grants)
    assert got.status == "diff"
    assert f"public.orders: {role} lacks INSERT, SELECT" in got.detail
    server.run(f'grant select, insert on public.orders to "{role}"', dst)
    assert _one(eng, src, eng._deep_grants).status == "ok"


def test_a_table_another_account_owns_is_a_difference(server, tmp_path):
    eng, src, dst = _setup(server, tmp_path, "dsql", SOURCE, SOURCE)
    role = f"owner_{next(ROLES)}_{id(server) % 10000}"
    server.run(f'create role "{role}"')
    server.run(f'alter table public.parent owner to "{role}"', dst)
    got = _one(eng, src, eng._deep_ownership)
    assert got.status == "diff"
    assert f"public.parent: owned by postgres on the source, {role} on the" \
           " target" in got.detail


def test_the_structure_is_one_answer_with_its_differences_kept(
        server, tmp_path):
    eng, src, dst = _setup(server, tmp_path, "dsql")
    server.run("create view public.v1 as select id from public.parent", src)
    got = eng.check_schema(src)[-1]
    assert got.scope.endswith("(structural)") and got.status == "diff"
    assert got.category == "structure.schema-diff"
    import json
    kept = json.load(open(got.report))["differences"]
    seen = {(d["table"], d["object"]) for d in kept}
    assert ("public.orders", "index orders_pid") in seen
    assert ("public.parent", "column name") in seen
    assert ("public.v1", "view") in seen


def test_a_same_structure_is_clean(server, tmp_path):
    eng, src, dst = _setup(server, tmp_path, "dsql", SOURCE, SOURCE)
    assert eng.check_schema(src)[-1].status == "ok"


# --- the values a checksum of the exact value cannot explain ------------------

TEXTS = "create table public.t(id bigint primary key, k text{c}, g int{g}," \
        " span interval)"


def _texts(server, tmp_path, name, source=("", ""), target=("", "")):
    eng, src, dst = pair(server, tmp_path, name)
    server.run(TEXTS.format(c=source[0], g=source[1]), src)
    server.run(TEXTS.format(c=target[0], g=target[1]), dst)
    return eng, src, dst


@pytest.mark.parametrize("name", ENGINES)
def test_a_text_column_ordered_otherwise_on_the_target_is_a_difference(
        server, tmp_path, name):
    eng, src, dst = _texts(server, tmp_path, name,
                           source=(' collate "C"', ""))
    got = _one(eng, src, eng._deep_collation)
    assert got.status == "diff" and got.category == "value.collation"
    assert "public.t.k: collation C on the source, the default on the " \
        "target" in got.detail


@pytest.mark.parametrize("name", ENGINES)
def test_a_column_generated_on_one_side_only_is_a_difference(
        server, tmp_path, name):
    eng, src, dst = _texts(
        server, tmp_path, name,
        source=("", " generated always as (id * 2) stored"))
    got = _one(eng, src, eng._deep_generated)
    assert got.status == "diff" and got.category == "value.generated"
    assert "public.t.g: generated on the source, plain or missing on the " \
        "target" in got.detail


@pytest.mark.parametrize("name", ENGINES)
def test_a_generated_column_made_plain_on_the_target_is_a_difference(
        server, tmp_path, name):
    same = " generated always as (id * 2) stored"
    eng, src, dst = _texts(server, tmp_path, name, ("", same), ("", same))
    server.run("insert into t(id) values (1), (2)", dst)
    assert _one(eng, src, eng._deep_generated).status == "ok"
    server.run("alter table t alter column g drop expression", dst)
    got = _one(eng, src, eng._deep_generated)
    assert got.status == "diff" and "public.t.g: generated on the source, " \
        "plain or missing on the target" in got.detail


@pytest.mark.parametrize("name", ENGINES)
def test_a_value_that_prints_differently_is_found_by_its_text(
        server, tmp_path, name):
    eng, src, dst = _texts(server, tmp_path, name)
    server.run("insert into t(id, span) values (1, '1 year 2 days')", src)
    server.run("insert into t(id, span) values (1, '1 year 2 days')", dst)
    assert _one(eng, src, eng._deep_render).status == "ok"
    # the same value, printed by a target set to another interval style
    server.run(f'alter database "{dst}" set intervalstyle = '
               "'sql_standard'")
    got = _one(eng, src, eng._deep_render)
    assert got.status == "diff" and got.category == "value.rendering"
    assert "public.t.span: renders differently" in got.detail


@pytest.mark.parametrize("name", ENGINES)
def test_replacement_characters_the_target_has_more_of_are_found(
        server, tmp_path, name):
    eng, src, dst = _texts(server, tmp_path, name)
    server.run("insert into t(id, k) values (1, 'abc')", src)
    server.run("insert into t(id, k) values (1, 'ab' || chr(65533))", dst)
    got = _one(eng, src, eng._deep_encoding)
    assert got.status == "diff" and got.category == "value.charset"
    assert "public.t.k: 1 extra U+FFFD replacement characters" in got.detail
