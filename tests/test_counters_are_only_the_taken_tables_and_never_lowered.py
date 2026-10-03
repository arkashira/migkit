"""A counter is checked, set and carried only for the tables the hop
takes, and never set back (backlog P0-SCOPE.6a).

Found by the audit of scope: PostgreSQL's sequence repair listed every
sequence the source has. With `exclude: [job_queue]` - a table
the target writes itself, its keys at 9,000 there and its source's
sequence at 100 - the plan said REFUSED, and the cutover's counter gate,
which reads that plan, failed the cutover over a table the hop does not
move. Not refused, it set the target's counter back to below the keys the
target had handed out. The parity check called the same sequence
different on every run, `watch --verify` included; MySQL's raised the
AUTO_INCREMENT of tables the hop leaves out (the `keys: apart` margin
too), and SQL Server's reseeded them, back below the target's own where
the target's stood higher.

A data-only dump told to leave a table out still wrote the value of the
table's serial sequence (it dumps a sequence its table owns with its own
flags as well as the table's), so a move through it reset the target's
counter of that table to the source's. The sequence is now left out of the
dump by name, and out of the carry after it.

A sequence goes with the table that owns it; one no table owns goes with
the tables whose defaults draw from it, and one nothing draws from is
judged by its own name. The docker tests run one engine at a time
(`-k postgres`, `-k mysql`).
"""
import pytest
from click.testing import CliRunner

from migkit.config import Endpoint, Hop
from tests import mysql_pair
from tests.conftest import psql
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture


def _pg(pg_pair, tmp_path, **scope):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="ctr", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], **scope)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return PostgresEngine(hop)


def _value(port, seq):
    return int(psql(port, f"select last_value from {seq}").stdout.strip())


@pytest.fixture
def target_owned(pg_pair):
    """`job_queue` is the target's own: its keys reach 9,000
    there and its counter 12,000, the source's 100. `orders` is moved."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.job_queue (id serial"
                   " primary key, v text);"
                   " create table public.orders (id serial primary key,"
                   " v text)")
    psql(pg_pair["src"], "insert into public.job_queue (v) select"
                         " 's' from generate_series(1, 100);"
                         " insert into public.orders (v) select 'o' from"
                         " generate_series(1, 50)")
    psql(pg_pair["dst"], "insert into public.job_queue select g,"
                         " 't' from generate_series(1, 9000) g;"
                         " select setval('public.job_queue_id_seq',"
                         " 12000);"
                         " insert into public.orders select g, 'o' from"
                         " generate_series(1, 50) g")
    yield


def test_postgres_an_excluded_tables_sequence_is_not_planned(
        pg_pair, tmp_path, target_owned):
    eng = _pg(pg_pair, tmp_path, exclude=["job_queue"])
    plan = eng.repair_plan("postgres", "sequences")
    said = " ".join(a.note for a in plan)
    stmts = [s for a in plan for s in a.statements]
    assert "REFUSED" not in said, said
    assert not [s for s in stmts if "job_queue" in s], stmts
    assert [s for s in stmts if "orders_id_seq', 50," in s], stmts
    # set, the target's own counter stays where its writer left it
    for a in plan:
        eng.apply("postgres", a)
    assert _value(pg_pair["dst"], "public.job_queue_id_seq") \
        == 12000
    assert _value(pg_pair["dst"], "public.orders_id_seq") == 50
    # and the check reads the moved table's counter only
    got = eng.check_autoinc("postgres")
    assert [r.status for r in got] == ["ok", "ok"], \
        [(r.scope, r.status, r.detail) for r in got]
    assert "job_queue" not in " ".join(r.detail for r in got)


def test_postgres_the_cutovers_counter_gate_passes_an_excluded_table(
        pg_pair, tmp_path, target_owned):
    from migkit import cutover
    eng = _pg(pg_pair, tmp_path, exclude=["job_queue"])
    rows = cutover._counter_rows(eng.hop, eng, "postgres", None, "fail")
    assert not [r for r in rows if r[0] == "fail"], rows
    # the gate still refuses where a table the hop takes is ahead
    psql(pg_pair["dst"], "insert into public.orders values (7000, 'x')")
    rows = cutover._counter_rows(eng.hop, eng, "postgres", None, "fail")
    assert [r for r in rows if r[0] == "fail" and "orders" in r[1]], rows


def test_postgres_a_counter_is_never_set_back(pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.lines (id serial primary key,"
                   " n int)")
    psql(pg_pair["src"], "insert into public.lines (n) select 1 from"
                         " generate_series(1, 20)")
    # the target handed out keys to 500 that no row holds any more
    psql(pg_pair["dst"], "insert into public.lines select g, 1 from"
                         " generate_series(1, 20) g;"
                         " select setval('public.lines_id_seq', 500)")
    eng = _pg(pg_pair, tmp_path)
    plan = eng.repair_plan("postgres", "sequences")
    assert not [s for a in plan for s in a.statements], plan
    said = " ".join(a.note for a in plan)
    assert "never lowered" in said and "lines_id_seq" in said, said
    assert "REFUSED" not in said, said
    for a in plan:
        eng.apply("postgres", a)
    assert _value(pg_pair["dst"], "public.lines_id_seq") == 500


def test_postgres_a_sequence_no_table_owns_goes_with_what_draws_from_it(
        pg_pair, tmp_path):
    ddl = ("create sequence public.order_no;"
           " create table public.orders (id int primary key default"
           " nextval('public.order_no'), v text);"
           " create sequence public.stray;"
           " create table public.audit_log (id serial primary key)")
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, ddl)
    psql(pg_pair["src"], "insert into public.orders (v) select 'o' from"
                         " generate_series(1, 30);"
                         " select setval('public.stray', 77);"
                         " insert into public.audit_log select"
                         " nextval('public.audit_log_id_seq') from"
                         " generate_series(1, 10)")
    eng = _pg(pg_pair, tmp_path, include=["postgres.public.orders"])
    stmts = [s for a in eng.repair_plan("postgres", "sequences")
             for s in a.statements]
    # the hop takes `orders`, and the counter its default draws from
    assert [s for s in stmts if "'public.order_no', 30," in s], stmts
    # and neither the table it leaves nor a counter nothing it takes uses
    assert not [s for s in stmts if "stray" in s or "audit_log" in s], stmts


def test_postgres_the_dump_leaves_an_excluded_tables_sequence_out(
        pg_pair, tmp_path, target_owned):
    from migkit import movers
    eng = _pg(pg_pair, tmp_path, exclude=["job_queue"])
    assert movers._pg_sequences_left_out(eng.hop, "postgres") == \
        ["public.job_queue_id_seq"]
    steps = movers.pgdump_move(eng.hop, "postgres", 1, False, None)
    argv = next(s.argv for s in steps if getattr(s, "argv", None)
                and s.argv[0] == "pg_dump")
    at = argv.index('"public"."job_queue_id_seq"')
    assert argv[at - 1] == "-T", argv
    assert '"public"."orders_id_seq"' not in argv, argv
    # a hop that leaves nothing out dumps every sequence, as before
    steps = movers.pgdump_move(_pg(pg_pair, tmp_path).hop, "postgres", 1,
                               False, None)
    argv = next(s.argv for s in steps if getattr(s, "argv", None)
                and s.argv[0] == "pg_dump")
    assert not [a for a in argv if a.endswith('_seq"')], argv


def test_postgres_a_move_by_the_dump_keeps_the_targets_own_counter(
        pg_pair, tmp_path, monkeypatch, target_owned):
    from migkit import movers
    if not movers.which("pg_dump"):
        pytest.skip("the PostgreSQL dump programs are not installed")
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  ctr:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        "    exclude: [job_queue]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    psql(pg_pair["dst"], "truncate public.orders")
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "ctr", "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    assert _value(pg_pair["dst"], "public.job_queue_id_seq") \
        == 12000
    assert psql(pg_pair["dst"], "select count(*) from"
                                " public.job_queue").stdout.strip() \
        == "9000"
    assert _value(pg_pair["dst"], "public.orders_id_seq") == 50


# -- MySQL ---------------------------------------------------------------------

def _my(exclude=()):
    eng = mysql_pair.engine("ctr")
    eng.hop.exclude = list(exclude)
    return eng


@pytest.fixture
def my_target_owned(my_pair):
    ddl = ("drop database if exists ctr; create database ctr; use ctr;"
           " create table job_queue (id int auto_increment"
           " primary key, v int);"
           " create table orders (id int auto_increment primary key, v int);")
    rows = ("insert into {t} (v) select 1 from (with recursive g(i) as"
            " (select 1 union all select i + 1 from g where i < {n})"
            " select i from g) x;")
    my("src", ddl + rows.format(t="job_queue", n=100)
       + rows.format(t="orders", n=50))
    # the target's own table, its counter at 41; the moved one behind
    my("dst", ddl + rows.format(t="job_queue", n=40)
       + rows.format(t="orders", n=10))
    yield
    for side in ("src", "dst"):
        my(side, "drop database if exists ctr")


def test_mysql_an_excluded_tables_counter_is_neither_read_nor_raised(
        my_target_owned):
    eng = _my(exclude=["job_queue"])
    plan = eng.repair_plan("ctr", "sequences")
    stmts = [s for a in plan for s in a.statements]
    said = " ".join(a.note for a in plan)
    assert not [s for s in stmts if "job_queue" in s], stmts
    assert "job_queue" not in said, said
    assert [s for s in stmts if "`orders` auto_increment = 51" in s], stmts
    for a in plan:
        eng.apply("ctr", a)
    assert my("dst", "set session information_schema_stats_expiry = 0;"
                     " select auto_increment from information_schema.tables"
                     " where table_schema = 'ctr' and table_name ="
                     " 'job_queue'") == "41"
    got = eng.check_autoinc("ctr")
    assert [r.status for r in got] == ["ok", "ok"], \
        [(r.scope, r.status, r.detail) for r in got]
    assert "job_queue" not in " ".join(r.detail for r in got)


def test_mysql_without_the_exclude_the_table_is_read_as_before(
        my_target_owned):
    eng = _my()
    stmts = [s for a in eng.repair_plan("ctr", "sequences")
             for s in a.statements]
    assert [s for s in stmts if "job_queue" in s], stmts
    parity = [r for r in eng.check_autoinc("ctr")
              if r.scope == "ctr parity"]
    assert parity[0].status == "diff" and \
        "job_queue" in parity[0].detail, parity


# -- SQL Server, its catalogue's answers given ---------------------------------

def _ms(answers, exclude=()):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(Hop(name="ms", engine="mssql", source=Endpoint(),
                          target=Endpoint(), databases=["app"],
                          exclude=list(exclude)))
    eng._cmd = answers
    return eng


def test_sql_server_reseeds_only_the_taken_tables_and_never_back():
    seeds = {"src": [["dbo.orders", "50"], ["dbo.job_queue", "100"],
                     ["dbo.lines", "20"]],
             "dst": [["dbo.orders", "10"],
                     ["dbo.job_queue", "12000"],
                     ["dbo.lines", "500"]]}
    eng = _ms(lambda side, db, sql: seeds[side],
              exclude=["dbo.job_queue"])
    plan = eng.repair_plan("app", "sequences")
    stmts = [s for a in plan for s in a.statements]
    assert stmts == ["dbcc checkident ('[dbo].[orders]', reseed, 50);"
                     "  -- dst now 10"], stmts
    said = " ".join(a.note for a in plan)
    assert "never lowered" in said and "dbo.lines" in said, said
    assert "job_queue" not in said, said


def test_sql_server_checks_the_identities_of_the_taken_tables_only():
    ident = {"src": [["dbo.orders", "id", "50"],
                     ["dbo.job_queue", "id", "100"]],
             "dst": [["dbo.orders", "id", "50"],
                     ["dbo.job_queue", "id", "12000"]]}
    read = []

    def answers(side, db, sql):
        if "identity_columns" in sql:
            return ident[side]
        read.append(sql)
        return [["50"]]
    got = _ms(answers, exclude=["job_queue"]).check_autoinc("app")
    assert [r.status for r in got] == ["ok", "ok"], got
    assert not [q for q in read if "job_queue" in q], read
