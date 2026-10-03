"""The hop's scope, held on real servers by the deep check, the objects
inventory and the repair.

The scenario the audit found: a DTS job imported as a hop takes three
tables, and one keyless table it never took made `deep keys` say
different; `exclude: [audit_log]` for a table on the target alone was
"table extra: audit_log" in `objects.json` on every check, with its
trigger, its view and its sequence beside it; and a check run before the
exclude was added left a drilldown that `sync --kind rows --apply` read
to delete the target's own rows. Each is answered now through the
engine's one scope answer (`Engine.in_scope`, backlog P0-SCOPE.6b), and
the same server without the scope still finds each one - the scope is
what changed the answer, not a check that stopped looking.

Run one engine at a time (`-k postgres`, `-k mysql`): one pair of
containers at once.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import LEFT_OUT
from tests import mysql_pair
from tests.conftest import psql, verdict
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.deep", "postgres"),
    ("verify.structure.columns", "postgres"),
    ("verify.structure.keys", "postgres"),
    ("verify.structure.objects", ("postgres", "mysql")),
]

pytestmark = pytest.mark.docker


# ---- PostgreSQL ------------------------------------------------------------

def _pg(pg_pair, side, sql):
    got = psql(pg_pair[side], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg_engine(pg_pair, tmp_path, **kw):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="scope", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


TAKEN = ["postgres.public.orders", "postgres.public.lines"]


def test_postgres_a_keyless_table_the_hop_does_not_take_is_not_a_finding(
        pg_pair, tmp_path):
    for side in ("src", "dst"):
        _pg(pg_pair, side, "create table orders (id int primary key);"
                           " create table lines (id int primary key);"
                           " create table event_log (at timestamp, v text)")
    keys = verdict(_pg_engine(pg_pair, tmp_path, include=TAKEN)
                   .check_deep("postgres"), "postgres keys")
    assert keys.status == "ok", keys.detail
    # the same pair, every table taken: the keyless table is a finding
    keys = verdict(_pg_engine(pg_pair, tmp_path).check_deep("postgres"),
                   "postgres keys")
    assert keys.status == "diff" and "event_log" in keys.detail, keys.detail


def test_postgres_drifted_columns_of_a_table_left_out_are_not_named(
        pg_pair, tmp_path):
    for side in ("src", "dst"):
        _pg(pg_pair, side, "create table orders (id int primary key,"
                           " note varchar(40))")
    _pg(pg_pair, "src", "create table event_log (id int primary key,"
                        " body varchar(200))")
    _pg(pg_pair, "dst", "create table event_log (id int primary key,"
                        " body varchar(20))")
    got = _pg_engine(pg_pair, tmp_path, exclude=["event_log"]).check_deep(
        "postgres")
    for scope in ("postgres columns", "postgres narrowing"):
        found = verdict(got, scope)
        assert found.status == "ok", (scope, found.detail)
    # the same pair, every table taken: the narrowed column is named
    got = _pg_engine(pg_pair, tmp_path).check_deep("postgres")
    found = verdict(got, "postgres narrowing")
    assert found.status == "diff" and "event_log" in found.detail, \
        found.detail


def test_postgres_what_the_target_keeps_for_itself_is_not_in_the_inventory(
        pg_pair, tmp_path):
    """The target's audit log, with its sequence, index, view and the
    function its trigger runs - all named in `exclude` - are not extra."""
    _pg(pg_pair, "src", "create table orders (id serial primary key)")
    _pg(pg_pair, "dst",
        "create table orders (id serial primary key);"
        " create table audit_log (id serial primary key, what text);"
        " create view audit_v as select * from audit_log;"
        " create function f_audit() returns trigger language plpgsql as"
        " $$ begin insert into audit_log (what) values (tg_op);"
        " return new; end $$;"
        " create trigger trg_audit after insert on orders"
        " for each row execute function f_audit()")
    eng = _pg_engine(pg_pair, tmp_path,
                     exclude=["audit_log", "audit_v", "f_audit"])
    got = eng.check_objects("postgres")
    # the trigger sits on the orders, which the hop takes: still extra
    assert got.status == "diff", got.detail
    assert "trg_audit" in got.detail, got.detail
    for name in ("audit_log", "audit_v", "f_audit", "audit_log_id_seq"):
        assert name not in got.detail, (name, got.detail)
    _pg(pg_pair, "dst", "drop trigger trg_audit on orders")
    assert eng.check_objects("postgres").status == "ok"
    # without the exclude, every one of them is extra
    got = _pg_engine(pg_pair, tmp_path).check_objects("postgres")
    for name in ("audit_log", "audit_v", "f_audit", "audit_log_id_seq"):
        assert name in (tmp_path / "objects.json").read_text(), name
    assert got.status == "diff"


def test_postgres_a_repair_leaves_a_table_left_out_as_the_target_has_it(
        pg_pair, tmp_path):
    for side in ("src", "dst"):
        _pg(pg_pair, side, "create table orders (id int primary key,"
                           " n int)")
    _pg(pg_pair, "dst",
        "create table audit_log (id int primary key, n int);"
        " insert into audit_log values (1, -5);"
        " alter table audit_log add constraint ck_n check (n > 0)"
        " not valid")
    # what a check run before the exclude was added left behind
    (tmp_path / "data-public.audit_log.extra").write_text("1\n")
    eng = _pg_engine(pg_pair, tmp_path, exclude=["audit_log"])
    plan = eng.repair_plan("postgres", "rows")
    assert [a for a in plan if a.kind == "rows"] == []
    said = [a for a in plan if a.kind == LEFT_OUT]
    assert said and "public.audit_log" in said[0].note
    assert eng._constraint_repair("postgres") is None
    assert eng._unvalidated_checks("postgres") == []
    # the target's own row is still there
    assert _pg(pg_pair, "dst", "select count(*) from audit_log") == "1"
    # the same target, the table taken: the check would be validated
    assert _pg_engine(pg_pair, tmp_path)._unvalidated_checks("postgres")


# ---- MySQL -----------------------------------------------------------------

def _my_engine(tmp_path, db, **kw):
    eng = mysql_pair.engine(db)
    for k, v in kw.items():
        setattr(eng.hop, k, v)
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


def _my_target_keeps_an_audit_log():
    for side in ("src", "dst"):
        my(side, "drop database if exists sc; create database sc; use sc;"
                 " create table orders (id int primary key);")
    my("dst", "use sc; create table audit_log (id int primary key,"
              " what varchar(20));"
              " create table audit_note (id int primary key);"
              " create view v_audit as select * from audit_log;"
              " create trigger trg_audit after insert on audit_note"
              " for each row insert into audit_log values (new.id, 'x');"
              " create event ev_purge_audit on schedule every 1 day"
              " disable do delete from audit_log;")


AUDIT = ["audit_log", "audit_note", "v_audit", "ev_purge_audit"]


def test_mysql_a_table_the_hop_excludes_on_the_target_alone_is_not_extra(
        my_pair, tmp_path):
    _my_target_keeps_an_audit_log()
    got = _my_engine(tmp_path, "sc", exclude=AUDIT).check_objects("sc")
    assert got.status == "ok", got.detail
    got = _my_engine(tmp_path, "sc").check_objects("sc")
    assert got.status == "diff", got.detail
    for name in ("audit_log", "v_audit", "trg_audit", "ev_purge_audit"):
        assert name in got.detail, (name, got.detail)


def test_mysql_a_stale_drilldown_of_an_excluded_table_deletes_nothing(
        my_pair, tmp_path):
    """The scenario itself: `data-audit_log.extra` from a check made
    before `exclude: [audit_log]`, and the target's own rows."""
    _my_target_keeps_an_audit_log()
    my("dst", "use sc; insert into audit_log values (1, 'kept')")
    (tmp_path / "data-audit_log.extra").write_text("")
    eng = _my_engine(tmp_path, "sc", exclude=AUDIT)
    plan = eng.repair_plan("sc", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == []
    assert any("audit_log" in a.note for a in plan if a.kind == LEFT_OUT)
    # nor is the target's purge event made again from the source's
    my("src", "use sc; create event ev_purge_audit on schedule every 1 day"
              " disable do delete from orders;")
    my("dst", "use sc; drop event ev_purge_audit")
    assert eng._event_repair("sc") is None
    assert my("dst", "select count(*) from sc.audit_log") == "1"
