"""DDL migkit itself generates and then tells you to apply.

`check schema` writes `structural-fix.sql` and says *review, then apply on
the target*. One shape in it does not survive contact with a target that
already has rows, and the two engines break differently. Measured, same
statement, same data:

    PostgreSQL 16   alter table t add column note text not null;
                    ERROR:  column "note" of relation "t" contains null
                            values
                    (the same statement on an *empty* table succeeds)

    MySQL 8, STRICT_TRANS_TABLES set
                    alter table t add column note text not null;
                    Query OK - and every existing row now holds '',
                    length 0, not null

PostgreSQL refuses out loud; MySQL invents a value for every row that was
already there. The second is the quieter failure and the worse one: the
column exists, the counts match, nothing errors, and the contents are made
up.

migkit generates exactly this whenever the source has a `NOT NULL` column
with no default - a column the application fills - and the target does not
have it yet. Verified end to end: a source with `note text not null` and
`tagged text not null default 'x'`, a target of three rows with neither,
and the generated file was

    alter table "public"."t" add column "note" text not null;
    alter table "public"."t" add column "tagged" text not null default
        'x'::text;

Applying the first of those to the target failed. The second applied
cleanly. Nothing in the report said which was which.

**This is where `atlas migrate lint` was going to come from, and it cannot.**
Measured on the installed atlas v1.2.4: `migrate lint` aborts with *"Starting
with v0.38, 'atlas migrate lint' is available only to Atlas Pro users"* and
asks for `atlas login`. A core check cannot sit behind a paid account and an
interactive login, so the capability is built here instead - and the built
version is the stronger one anyway: a linter reads the SQL and can only say
*might*, while migkit asks the target whether those tables have rows and
says *will*.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit import ddl
from tests.conftest import needs_docker, psql

GENERATED = (
    'alter table "public"."t" add column "note" text not null;\n\n'
    'alter table "public"."t" add column "tagged" text not null'
    " default 'x'::text;\n"
)


def test_the_statement_that_will_not_apply_is_picked_out():
    got = ddl.needs_backfill(GENERATED)
    assert [(t, c) for t, c, _ in got] == [("public.t", "note")], got


def test_a_default_answers_the_question_and_is_left_alone():
    """`tagged` says what the rows already there should hold, so it is not
    a finding - flagging it would make the warning noise."""
    assert not [c for _, c, _ in ddl.needs_backfill(GENERATED)
                if c == "tagged"]


@pytest.mark.parametrize("stmt", [
    "alter table t add column x int not null default 0;",
    "alter table t add column x int generated always as identity not null;",
    "alter table t add column x int not null auto_increment;",
    "alter table t add column x int;",
    "alter table t add column x text null;",
    "alter table t drop column x;",
    "create table t (id int not null);",
])
def test_statements_that_are_not_this_problem(stmt):
    assert not ddl.needs_backfill(stmt), stmt


@pytest.mark.parametrize("stmt,table,column", [
    ('alter table "public"."t" add column "n" text not null;', "public.t", "n"),
    ("alter table `d`.`t` add column `n` text not null;", "d.t", "n"),
    ("ALTER TABLE t ADD COLUMN n TEXT NOT NULL;", "t", "n"),
    ("alter table t add n text not null;", "t", "n"),
])
def test_the_shapes_it_has_to_read(stmt, table, column):
    """Both engines quote their identifiers, and an operator may have
    edited the file by hand into the shorter form."""
    got = ddl.needs_backfill(stmt)
    assert [(t, c) for t, c, _ in got] == [(table, column)], got


def test_a_table_with_no_rows_is_not_warned_about():
    """The measured reason: the same statement on an empty table applies
    cleanly on PostgreSQL, and has nothing to fabricate on MySQL. A warning
    there would be a linter's hedge, not a check."""
    found = ddl.needs_backfill(GENERATED)
    assert ddl.backfill_warning("postgres", found, set()) == ""
    assert ddl.backfill_warning("postgres", found, {"other.table"}) == ""


def test_each_engine_is_told_what_its_own_database_does():
    found = ddl.needs_backfill(GENERATED)
    pg = ddl.backfill_warning("postgres", found, {"public.t"})
    my = ddl.backfill_warning("mysql", found, {"public.t"})
    assert "public.t.note" in pg and "public.t.note" in my
    assert "refuses" in pg and "nothing after it in the script runs" in pg
    assert "fills the gap itself" in my, my
    assert "a value nobody chose" in my, my
    # and the two are genuinely different advice, not one sentence twice
    assert pg != my


def test_an_engine_nobody_measured_says_so_rather_than_guessing():
    """A new engine inheriting PostgreSQL's answer would be a confident
    statement about something never tried."""
    said = ddl.backfill_warning("oracle", ddl.needs_backfill(GENERATED),
                                {"public.t"})
    assert "has not measured" in said, said
    assert "refuses" not in said and "fills" not in said, said


def test_one_finding_reads_as_one():
    found = ddl.needs_backfill(GENERATED)
    one = ddl.backfill_warning("postgres", found, {"public.t"})
    assert "1 of the generated statements adds" in one, one
    two = ddl.backfill_warning("postgres", found + [("public.t", "more", "")],
                               {"public.t"})
    assert "2 of the generated statements add " in two, two


def test_the_warning_names_no_tool():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    for family in ("postgres", "mysql", "oracle"):
        said = ddl.backfill_warning(family, ddl.needs_backfill(GENERATED),
                                    {"public.t"}).lower()
        assert not [t for t in TOOLS if t in said], (family, said)


# ---- against real servers ----

SRC_DDL = ("create table ddlrisk (id bigint primary key,"
           " note text not null, tagged text not null default 'x')")
DST_DDL = "create table ddlrisk (id bigint primary key)"


@pytest.fixture
def mismatched(pg_pair):
    psql(pg_pair["src"], "drop table if exists ddlrisk")
    psql(pg_pair["dst"], "drop table if exists ddlrisk")
    assert psql(pg_pair["src"], SRC_DDL).returncode == 0
    assert psql(pg_pair["dst"], DST_DDL).returncode == 0
    assert psql(pg_pair["dst"], "insert into ddlrisk values (1),(2),(3)"
                ).returncode == 0
    yield pg_pair
    psql(pg_pair["src"], "drop table if exists ddlrisk")
    psql(pg_pair["dst"], "drop table if exists ddlrisk")


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="l", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


@needs_docker
def test_the_statement_really_does_fail_on_that_target(mismatched):
    """The premise, asserted against the server rather than quoted."""
    bad = psql(mismatched["dst"],
               "alter table ddlrisk add column note text not null")
    assert bad.returncode != 0
    assert "contains null values" in (bad.stderr + bad.stdout), bad.stderr
    good = psql(mismatched["dst"], "alter table ddlrisk add column tagged"
                                   " text not null default 'x'")
    assert good.returncode == 0, good.stderr
    psql(mismatched["dst"], "alter table ddlrisk drop column tagged")


@needs_docker
def test_migkit_generates_it_and_now_says_so(mismatched, tmp_path):
    """End to end on the file migkit writes and the line it prints."""
    got = _engine(mismatched, tmp_path).check_structural_diff("postgres")
    assert got.status == "diff", got.detail
    written = (tmp_path / "structural-fix.sql").read_text()
    assert "add column \"note\" text not null" in written, written
    assert "ddlrisk.note" in got.detail, got.detail
    assert "nothing after it in the script runs" in got.detail, got.detail


@needs_docker
def test_an_empty_target_gets_no_such_warning(pg_pair, tmp_path):
    """Same two schemas, no rows on the target - the statement applies
    cleanly there, so saying it will fail would be false."""
    psql(pg_pair["src"], "drop table if exists ddlrisk")
    psql(pg_pair["dst"], "drop table if exists ddlrisk")
    assert psql(pg_pair["src"], SRC_DDL).returncode == 0
    assert psql(pg_pair["dst"], DST_DDL).returncode == 0
    try:
        got = _engine(pg_pair, tmp_path).check_structural_diff("postgres")
        assert "add column \"note\" text not null" in (
            tmp_path / "structural-fix.sql").read_text()
        assert "without saying what those rows should hold" not in got.detail
    finally:
        psql(pg_pair["src"], "drop table if exists ddlrisk")
        psql(pg_pair["dst"], "drop table if exists ddlrisk")


@needs_docker
def test_rows_present_answers_only_for_tables_that_have_rows(mismatched,
                                                             tmp_path):
    eng = _engine(mismatched, tmp_path)
    psql(mismatched["dst"], "drop table if exists ddlempty")
    assert psql(mismatched["dst"], "create table ddlempty (id int)"
                ).returncode == 0
    try:
        got = eng.rows_present("postgres", ["public.ddlrisk",
                                            "public.ddlempty",
                                            "public.no_such_table"])
        assert got == {"public.ddlrisk"}, got
    finally:
        psql(mismatched["dst"], "drop table if exists ddlempty")


def test_every_engine_that_generates_ddl_can_be_asked_about_rows():
    """The warning is built once on the base; an engine that generates DDL
    and cannot answer this would silently never warn."""
    from migkit.engines import _class_for
    from migkit.engines.base import Engine
    for name in ("postgres", "mysql"):
        cls = _class_for(name)
        assert cls.rows_present is not Engine.rows_present, name
        assert name in ddl.BEHAVIOUR, name
