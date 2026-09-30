"""Every way an engine switched from as it ran, by a try and an except of
its own, now switches through the one decision (`decide.attempt`, by way
of `Engine._attempt`): the same ways in the same order for the same
reasons, one line said in migkit's words, the last way that holds failing
in its own words - and a verdict, a difference found, never switched from.

No servers: each site's work is given, and made to fail on purpose.
"""
import contextlib
import time

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop

EP = Endpoint(host="10.0.0.5", port=1, user="app", password="CHANGE_ME")


def _hop(engine, **kw):
    return Hop(name="h", engine=engine, source=EP, target=EP,
               databases=["a"], **kw)


# --- the rules every engine site stands on --------------------------------

def _plain():
    from migkit.engines.base import Engine
    return Engine(_hop("postgres"))


A = decide.Rung("a", "the fast way")
B = decide.Rung("b", "the plain way")


def test_the_last_way_that_holds_fails_in_its_own_words():
    """After the switch is said, what stops the last way is raised as it
    is - a server's refusal, which callers read as one - not wrapped."""
    eng, said, ran = _plain(), [], []

    def run(choice):
        ran.append(choice.path)
        raise OSError(f"{choice.path} refused")
    with pytest.raises(OSError, match="b refused"):
        eng._attempt("t", (A, B), run, log=said.append)
    assert ran == ["a", "b"]
    assert said == ["t: the fast way could not run (a refused) - switching"
                    " to the plain way"]


def test_a_verdict_is_never_switched_from():
    eng, ran = _plain(), []

    def run(choice):
        ran.append(choice.path)
        raise decide.Verdict("3 rows differ")
    with pytest.raises(decide.Verdict, match="3 rows differ"):
        eng._attempt("t", (A, B), run, log=pytest.fail)
    assert ran == ["a"]


def test_a_lent_log_hears_a_works_switch_once_a_given_one_every_time():
    eng, lent, given = _plain(), [], []
    eng.lend_log(lent.append)

    def run(choice):
        if choice.path == "a":
            raise decide.Blocked("refused")
        return "done"
    for _ in range(3):
        assert eng._attempt("t", (A, B), run)[1] == "done"
    assert lent == ["t: the fast way could not run (refused) - switching to"
                    " the plain way"]
    for _ in range(2):
        eng._attempt("t", (A, B), run, log=given.append)
    assert len(given) == 2 and len(lent) == 1


def test_duckdbs_and_redshifts_refusals_are_ways_that_could_not_run():
    import duckdb
    import redshift_connector
    assert decide.operational(duckdb.CatalogException("no such table"))
    assert decide.operational(
        redshift_connector.error.ProgrammingError("permission denied"))
    assert not decide.operational(TypeError("a fault in migkit"))


# --- SQL Server: its bulk load, and its move between servers ---------------

def _mssql(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql"))
    shape = {"declared": {"id": "int", "n": "int"}, "key": ["id"],
             "identity": None, "unwritable": set(), "utf8": set()}
    monkeypatch.setattr(MSSQLEngine, "_shape",
                        lambda self, side, db, table: shape)
    monkeypatch.setattr(MSSQLEngine, "_bulk_unusable",
                        staticmethod(lambda: None))
    # the bulk load the cheaper at any size, until timed here
    monkeypatch.setattr(MSSQLEngine, "WRITE_COSTS",
                        {"statements": (1.0, 1.0), "bulk load": (0.0, 0.0)})
    return eng


def _stage(monkeypatch, load=None, promote=None):
    from migkit.engines import mssql
    seen = {"loads": [], "promotes": 0}

    class Stage:
        def __init__(self, eng, side, db, table, names, carried=None):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

        def load(self, rows):
            seen["loads"].append(len(rows))
            if load:
                raise load

        def promote(self):
            seen["promotes"] += 1
            if promote:
                raise promote
    monkeypatch.setattr(mssql, "_Stage", Stage)
    return seen


def _statements(monkeypatch):
    from migkit.engines import dbapi
    written = []
    monkeypatch.setattr(dbapi.DbapiRows, "neutral_write",
                        lambda self, side, db, table, columns, rows:
                        written.append([list(r) for r in rows])
                        or len(rows))
    return written


COLS = [("id", "integer"), ("n", "integer")]


def test_a_batch_the_sql_server_load_refuses_goes_by_statements(
        monkeypatch):
    from migkit.engines import mssql
    eng = _mssql(monkeypatch)
    seen = _stage(monkeypatch,
                  load=mssql._NotLoaded("invalid data for column n"))
    written = _statements(monkeypatch)
    said = []
    eng.lend_log(said.append)
    assert eng.write_way("dst", "a", "dbo.c", COLS, 2)[0] == "bulk load"
    assert eng.neutral_write("dst", "a", "dbo.c", COLS,
                             [[1, 12], [2, 13]]) == 2
    assert seen == {"loads": [2], "promotes": 0}
    assert written == [[[1, 12], [2, 13]]]
    assert said == ["dbo.c: SQL Server's bulk load, staged and promoted"
                    " could not run (the bulk load refused a value of it:"
                    " invalid data for column n) - switching to statements"]
    # the rest of the table goes by statements this run, and says nothing
    # more
    assert eng.write_way("dst", "a", "dbo.c", COLS, 2) == (
        "statements", "the bulk load refused a value of it: invalid data"
                      " for column n")
    eng.neutral_write("dst", "a", "dbo.c", COLS, [[3, 14]])
    assert seen["loads"] == [2] and len(written) == 2 and len(said) == 1


def test_what_the_sql_server_table_refuses_is_never_switched_from(
        monkeypatch):
    """The promote is the table's own insert: a constraint that refuses
    it would refuse the statements too, and is said as it is."""
    eng = _mssql(monkeypatch)
    seen = _stage(monkeypatch, promote=RuntimeError(
        "The INSERT statement conflicted with the CHECK constraint"))
    written = _statements(monkeypatch)
    said = []
    eng.lend_log(said.append)
    with pytest.raises(RuntimeError, match="CHECK constraint"):
        eng.neutral_write("dst", "a", "dbo.c", COLS, [[1, -1]])
    assert seen["promotes"] == 1 and written == [] and said == []


def _native(monkeypatch, arrow_error, copier_error=None):
    from migkit.engines import mssql
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql"))
    monkeypatch.setattr(MSSQLEngine, "_native_facts", lambda self, db, t:
                        decide.Facts({"driver": None, "there": True,
                                      "unproved": None}))

    def arrow(self, db, t, log):
        raise mssql._NotLoaded(arrow_error)
    monkeypatch.setattr(MSSQLEngine, "_native_table", arrow)
    moved = []

    class Pair:
        READ_ROWS = 5000

        def move_table(self, db, sch, tbl, chunk, ck, log):
            moved.append((db, sch, tbl))
            if copier_error:
                raise copier_error
    monkeypatch.setattr(MSSQLEngine, "_as_pair",
                        lambda self, within=None: Pair())
    return eng, moved


def test_a_table_the_arrow_load_refuses_is_copied_table_by_table(
        monkeypatch):
    eng, moved = _native(monkeypatch, "a time with a zone, for a column"
                                      " without one")
    said = []
    eng.native_bulk("a", ["dbo.t"], True, said.append)
    assert moved == [("a", "dbo", "t")]
    assert said[0] == ("dbo.t: read as Arrow and loaded by SQL Server's bulk"
                       " load could not run (the bulk load refused a value"
                       " of it: a time with a zone, for a column without"
                       " one) - switching to table by table")
    assert said[1].startswith("dbo.t: copied table by table - the bulk load"
                              " refused a value of it"), said


def test_the_sql_server_copier_after_a_switch_fails_in_its_own_words(
        monkeypatch):
    eng, moved = _native(monkeypatch, "refused",
                         copier_error=RuntimeError("the link went down"))
    with pytest.raises(RuntimeError, match="the link went down"):
        eng.native_bulk("a", ["dbo.t"], True, [].append)
    assert moved == [("a", "dbo", "t")]


# --- ClickHouse: the target's pull, then the table copier ------------------

def _clickhouse(monkeypatch, tmp_path, pull):
    from migkit.engines.clickhouse import ClickHouseEngine
    eng = ClickHouseEngine(_hop("clickhouse"))
    eng.hop.report_dir = lambda db=None: tmp_path
    for name, fn in (
            ("target_missing", lambda self, db: False),
            ("neutral_tables", lambda self, side, db: ["t"]),
            ("_engine_of", lambda self, side, db, t: "MergeTree"),
            ("_reach", lambda self: {"fn": "remote",
                                     "address": "10.0.0.5:9000",
                                     "timeout_ms": 1000}),
            ("prepare_target", lambda self, db: None),
            ("_pull_table", pull)):
        monkeypatch.setattr(ClickHouseEngine, name, fn)
    copied = []
    monkeypatch.setattr(ClickHouseEngine, "move_table",
                        lambda self, db, sch, t, chunk, ck, log:
                        copied.append(t))
    return eng, copied


def test_a_pull_the_target_is_refused_goes_by_the_table_copier(
        monkeypatch, tmp_path):
    ran = []

    def pull(self, db, t, reach, log):
        ran.append(t)
        raise decide.Blocked("t: partition 202401 could not be copied:"
                             " Code: 497. Not enough privileges")
    eng, copied = _clickhouse(monkeypatch, tmp_path, pull)
    said = []
    steps = eng.native_bulk("a", ["t"], True, said.append)
    assert steps == ["t: pulled by the target from the source, a partition"
                     " at a time"]
    assert ran == ["t"] and copied == ["t"]
    assert said == ["t: the target pulling from the source could not run"
                    " (t: partition 202401 could not be copied: Code: 497."
                    " Not enough privileges) - switching to the table"
                    " copier"]


def test_a_partition_that_differs_after_its_pull_is_never_switched_from(
        monkeypatch, tmp_path):
    def pull(self, db, t, reach, log):
        raise SystemExit("t: partition 202401 differs from the source after"
                         " the copy, and the source did not change")
    eng, copied = _clickhouse(monkeypatch, tmp_path, pull)
    said = []
    with pytest.raises(SystemExit, match="differs from the source"):
        eng.native_bulk("a", ["t"], True, said.append)
    assert copied == [] and said == []


def test_a_pull_statement_the_target_refuses_is_a_way_blocked(monkeypatch):
    """What the pull raises where the target refuses its statement: a
    `decide.Blocked`, which a follow with no other way stops on in the
    same words as before."""
    from migkit.engines.clickhouse import ClickHouseEngine
    eng = ClickHouseEngine(_hop("clickhouse"))

    class Client:
        def command(self, sql, settings=None):
            raise Exception("Code: 497. DB::Exception: app: Not enough"
                            " privileges. (ACCESS_DENIED)")

        def close(self):
            pass
    for name, fn in (
            ("_insertable", lambda self, db, t: ["id"]),
            ("_pull_wheres", lambda self, db, t, pid: ["_partition_id = 1"]),
            ("_parts", lambda self, side, db, t=None: {}),
            ("_next_copy", lambda self, db, t, pid: 1),
            ("_clear_partition", lambda self, db, t, pid: None),
            ("_client", lambda self, side, db=None, wait=None: Client())):
        monkeypatch.setattr(ClickHouseEngine, name, fn)
    with pytest.raises(decide.Blocked) as e:
        eng._pull_partition("a", "t", "1", {"fn": "remote",
                                            "address": "10.0.0.5:9000"},
                            {}, True)
    assert isinstance(e.value, SystemExit)
    assert str(e.value).startswith("t: partition 1 could not be copied:"
                                   " Code: 497")
    assert "CHANGE_ME" not in str(e.value)


# --- MySQL: the load, the range, the staged upserts ------------------------

def _mysql(monkeypatch, load):
    from migkit.engines import mysql
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql"))
    eng._loads_ok = True
    monkeypatch.setattr(MySQLEngine, "LOAD_FROM", 1)
    monkeypatch.setattr(MySQLEngine, "_cheaper",
                        lambda self, key, ways, measured=None: ways[0].name)
    monkeypatch.setattr(MySQLEngine, "_load_types",
                        lambda self, cur, db, table: {})
    tried = []

    def loaded(cur, db, table, names, rows, types=None):
        tried.append(len(rows))
        raise load
    monkeypatch.setattr(mysql, "_load", loaded)
    return eng, tried


class _Cur:
    def __init__(self):
        self.connection, self.many, self.run = object(), [], []

    def executemany(self, sql, rows):
        self.many.append([list(r) for r in rows])

    def execute(self, sql, args=None):
        self.run.append(sql)


ROWS = [[1, "a"], [2, "b"]]


def test_a_batch_the_mysql_load_cannot_carry_goes_as_inserts(monkeypatch):
    from migkit.engines import mysql
    eng, tried = _mysql(monkeypatch, mysql._Unfit())
    said, cur = [], _Cur()
    eng.lend_log(said.append)
    assert eng._put_rows(cur, "d", "t", ["id", "v"], ROWS, "insert") == 2
    assert tried == [2] and cur.many == [ROWS]
    assert said == ["d.t: the server's bulk load, sent only the rows made"
                    " for it could not run (a value the load's text cannot"
                    " carry as an insert would) - switching to the rows as"
                    " multi-row inserts"]


def test_a_mysql_load_taken_with_warnings_is_rolled_back_then_inserted(
        monkeypatch):
    from migkit.engines import mysql
    eng, tried = _mysql(monkeypatch,
                        mysql._LoadRefused("2 of 2 rows taken, 1 warnings"))
    said, cur, undone = [], _Cur(), []
    eng.lend_log(said.append)
    eng._put_rows(cur, "d", "t", ["id", "v"], ROWS, "insert",
                  undo=lambda: undone.append(1))
    assert undone == [1] and cur.many == [ROWS]
    assert "could not run (the bulk load was not taken as it was (2 of 2"\
           " rows taken, 1 warnings))" in said[0], said
    # where nothing can put the transaction back, it is the caller's: the
    # range's, which rolls back whole
    eng, _ = _mysql(monkeypatch,
                    mysql._LoadRefused("2 of 2 rows taken, 1 warnings"))
    cur = _Cur()
    with pytest.raises(mysql._LoadRefused):
        eng._put_rows(cur, "d", "t", ["id", "v"], ROWS, "insert")
    assert cur.many == []


def test_a_server_refusing_the_mysql_load_gets_inserts_from_then_on(
        monkeypatch):
    import pymysql
    eng, tried = _mysql(monkeypatch, pymysql.err.OperationalError(
        1148, "The used command is not allowed with this MySQL version"))
    cur = _Cur()
    eng._put_rows(cur, "d", "t", ["id", "v"], ROWS, "insert")
    eng._put_rows(cur, "d", "t", ["id", "v"], ROWS, "insert")
    assert tried == [2] and eng._loads_ok is False
    assert cur.many == [ROWS, ROWS]


class _Conn:
    def __init__(self, rows=()):
        self.rows, self.done = list(rows), []

    def cursor(self, *a):
        conn = self

        class C(_Cur):
            fed = False

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                pass

            def fetchmany(self, size):
                if self.fed:
                    return []
                self.fed = True
                return conn.rows
        return C()

    def commit(self):
        self.done.append("commit")

    def rollback(self):
        self.done.append("rollback")


def test_a_range_whose_load_is_not_taken_as_it_was_goes_as_inserts(
        monkeypatch):
    from migkit.engines import mysql
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql", options={"verify_batches": False}))
    puts = []

    def put(self, cur, db, table, names, rows, insert, undo=None):
        puts.append(len(rows))
        raise mysql._LoadRefused("1 of 1 rows taken, 1 warnings")
    monkeypatch.setattr(MySQLEngine, "_put_rows", put)
    src, dst = _Conn([(1, "a")]), _Conn()
    said = []
    sent = eng._range_checked(src, dst, "a", "t", "`id`, `v`", "%s, %s",
                              " where `id` > %s and `id` <= %s", (0, 10),
                              "delete from t", "id > 0", "a.t id 1 to 10",
                              said.append)
    assert puts == [1] and sent.n == 1
    assert dst.done == ["rollback", "commit"]
    assert said == ["a.t: its batches each by the way measured cheaper"
                    " could not run (the bulk load was not taken as it was"
                    " (1 of 1 rows taken, 1 warnings) in a.t id 1 to 10) -"
                    " switching to every batch as inserts"]


def test_a_run_of_upserts_whose_stage_cannot_be_made_goes_as_statements(
        monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop("mysql"))
    eng._loads_ok = True
    cur = _Conn().cursor()

    @contextlib.contextmanager
    def writer(self, side, db):
        class W:
            def cursor(self):
                return cur
        yield W()
    for name, fn in (
            ("_writer", writer),
            ("_uniques", lambda self, side, db, table: 1),
            ("_unwritable_columns", lambda self, side, db, table: set()),
            ("_cheaper", lambda self, key, ways, measured=None:
             ways[0].name),
            ("_stage", lambda self, cur, db, table, names: None)):
        monkeypatch.setattr(MySQLEngine, name, fn)
    monkeypatch.setattr(MySQLEngine, "STAGE_FROM", 1)
    said = []
    eng.lend_log(said.append)
    eng._apply_upserts("dst", "a", "t", [({"id": 1}, {"v": "x"}),
                                         ({"id": 2}, {"v": "y"})])
    assert len(cur.run) == 1 and "on duplicate key update" in cur.run[0]
    assert said == ["a.t: the rows loaded into a temporary table and moved"
                    " on in one statement could not run (the target would"
                    " not make a temporary table of its columns) -"
                    " switching to the rows as multi-row upserts"]


# --- MongoDB: the id hash walk, then the bytes, on both sides --------------

def _mongo(monkeypatch, tmp_path, walk):
    from migkit.engines.base import Result
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb"))
    eng.hop.report_dir = lambda db=None: tmp_path
    walked = []

    def drilldown(self, db, name, exact=False):
        walked.append("bytes" if exact else "id hash")
        return walk(exact, Result)
    monkeypatch.setattr(MongoEngine, "_drilldown", drilldown)
    monkeypatch.setattr(MongoEngine, "_db_hashes", lambda self, db, names:
                        ({"t": "a" * 32}, {"t": "b" * 32}))
    return eng, walked


def test_an_id_hash_a_server_will_not_work_out_walks_by_the_bytes_once(
        monkeypatch, tmp_path):
    """Both sides by the bytes - never one side's hashes against the
    other's bytes - and `dbHash` still asked again after it: a switch
    never ends the comparison early."""
    import pymongo.errors

    def walk(exact, Result):
        if not exact:
            raise pymongo.errors.OperationFailure(
                "Unrecognized expression '$toHashedIndexKey'")
        return Result("data", "shop.t", "ok",
                      "docs 3 compared by their bytes, 1 ranges")
    eng, walked = _mongo(monkeypatch, tmp_path, walk)
    r = eng._compare("shop", "t", True)
    assert walked == ["id hash", "bytes"]
    # dbHash still differs: the answer is a difference, not ok
    assert r.status == "diff" and "differs, not localized" in r.detail
    assert "shop.t: each document's hash worked out by the servers could"\
           " not run (Unrecognized expression '$toHashedIndexKey')" \
           " - switching to each document's stored bytes" in r.detail, \
        r.detail


def test_a_difference_the_id_hash_finds_is_the_answer(monkeypatch, tmp_path):
    def walk(exact, Result):
        return Result("data", "shop.t", "diff", "missing=1 extra=0"
                      " changed=0 over 1 ranges")
    eng, walked = _mongo(monkeypatch, tmp_path, walk)
    r = eng._compare("shop", "t", True)
    assert walked == ["id hash"] and r.status == "diff"
    assert "switching" not in r.detail


# --- PostgreSQL: the consistent pass, the repair, the roles, the probe -----

def _postgres(tmp_path, **kw):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres", **kw))
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


def test_a_side_that_will_not_export_its_snapshot_is_read_in_one_go(
        monkeypatch, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _postgres(tmp_path, workers=4)
    for name, fn in (
            ("_psql", lambda self, side, db, sql: "public.a\npublic.b"),
            ("_keep_tbl", lambda self, db, t: True),
            ("through_pair", lambda self, db, t: False),
            ("_row_hash_expr", lambda self, side, db, t: "h"),
            ("_summed", lambda self, h: "s"),
            ("_scope", lambda self, db, t: t),
            ("_export_snapshot", lambda self, side, db: (None, None)),
            ("_snapshot_scripts", lambda self, *a: pytest.fail(
                "lanes run without a snapshot")),
            ("_psql_script", lambda self, side, db, sc: sc)):
        monkeypatch.setattr(PostgresEngine, name, fn)
    scripts = []
    monkeypatch.setattr(PostgresEngine, "_run_lanes", lambda self, procs,
                        deadline: scripts.append(len(procs)) or [
                            (0, "LSN|0/1\npublic.a|1|5\npublic.b|2|7", "")])
    rc, text = eng._fast_consistent("app")
    assert rc == 0 and scripts == [1, 1]
    for side in ("src", "dst"):
        assert (f"the {side}'s consistent pass: several connections sharing"
                " one exported snapshot could not run (the side would not"
                " export its snapshot for the lanes to share) - switching to"
                " one transaction reading every table in turn") in text, text


def _repair(monkeypatch, tmp_path, refused):
    from migkit.engines.postgres import PostgresEngine
    eng = _postgres(tmp_path)
    (tmp_path / "data-public.t.extra").write_text("5\n")
    monkeypatch.setattr(PostgresEngine, "_report", lambda self, db: tmp_path)
    monkeypatch.setattr(PostgresEngine, "_pk_cols_of",
                        lambda self, db, t: ["id"])
    ran = []

    def psql_run(self, side, db, script):
        ran.append(script)
        if script.startswith("set session_replication_role"):
            raise RuntimeError(refused)
        return ""
    monkeypatch.setattr(PostgresEngine, "_psql_run", psql_run)
    return eng, ran


def test_a_repair_whose_target_refuses_the_replica_role_runs_as_it_is(
        monkeypatch, tmp_path):
    eng, ran = _repair(monkeypatch, tmp_path, 'ERROR:  permission denied to'
                       ' set parameter "session_replication_role"')
    said = []
    eng.lend_log(said.append)
    eng._repair_rows_native("app", "public.t")
    assert len(ran) == 2 and ran[1] == ran[0].split("\n", 1)[1]
    assert said == ["public.t: the target's triggers and foreign keys held"
                    " quiet as a replica's are could not run (the target's"
                    " user may not set the replica role: ERROR:  permission"
                    " denied to set parameter \"session_replication_role\")"
                    " - switching to the target's triggers and foreign keys"
                    " as they are"]


def test_a_repair_the_target_refuses_otherwise_is_never_run_again(
        monkeypatch, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng, ran = _repair(monkeypatch, tmp_path, "unused")

    def refused(self, side, db, script):
        ran.append(script)
        raise RuntimeError("ERROR:  update or delete on table \"t\" violates"
                           " foreign key constraint")
    monkeypatch.setattr(PostgresEngine, "_psql_run", refused)
    with pytest.raises(RuntimeError, match="foreign key constraint"):
        eng._repair_rows_native("app", "public.t")
    assert len(ran) == 1


def test_roles_a_side_will_not_show_the_hashes_of_are_read_by_name_and_said(
        monkeypatch, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = _postgres(tmp_path)

    def psql(self, side, db, sql):
        if "pg_authid" in sql:
            if side == "dst":
                raise RuntimeError("ERROR:  permission denied for table"
                                   " pg_authid")
            return "app|SCRAM-SHA-256$4096:x"
        return "app\nreport"
    monkeypatch.setattr(PostgresEngine, "_psql", psql)
    src, dst, said = eng._login_roles()
    assert src == dst == {"app": "", "report": ""}
    assert said == ["the login roles: the roles with their password hashes"
                    " could not run (ERROR:  permission denied for table"
                    " pg_authid) - switching to the roles by name alone"]


def test_the_write_probe_makes_its_table_where_the_target_lets_it(
        monkeypatch, tmp_path):
    import psycopg2

    from migkit.engines.postgres import PostgresEngine
    eng = _postgres(tmp_path)
    made = []

    class Cur:
        rowcount = 0

        def execute(self, sql, args=None):
            if sql.startswith("create"):
                made.append(sql)
                if not sql.startswith("create temp"):
                    raise psycopg2.ProgrammingError(
                        "permission denied for schema app")

        def copy_expert(self, sql, f, size=None):
            pass

    class Conn:
        def cursor(self):
            return Cur()

        def rollback(self):
            pass

        def close(self):
            pass
    monkeypatch.setattr(PostgresEngine, "_probe_shape", lambda self, db, t: (
        [("id", "integer"), ("v", "text")], ["id"]))
    monkeypatch.setattr(PostgresEngine, "_probe_conn",
                        lambda self, side, db, seconds: Conn())
    said = []
    eng.lend_log(said.append)
    got = eng.probe_write("app", "app.t", b"1\tx\n", apply=False)
    assert got["how"] == "a temporary table, which the target writes no log" \
                         " for"
    assert len(made) == 5 and made[-1].startswith("create temp table")
    assert "integer" in made[-1]
    assert len(said) == 4 and said[-1].endswith(
        "switching to a temporary table of the source's column types"), said


# --- Redis: the moment each key falls due, then the time left --------------

class _Pipe:
    def __init__(self, got=(), refuse=None):
        self.got, self.refuse, self.sent = list(got), refuse, []

    def __getattr__(self, name):
        return lambda *a, **k: self.sent.append((name, a, k))

    def execute(self, raise_on_error=True):
        if self.refuse and any(k.get("absttl") for _, _, k in self.sent):
            raise self.refuse
        return self.got


def _redis_carry(refused):
    from migkit.engines.redis import RedisEngine
    eng = RedisEngine(_hop("redis"))
    due = int(time.time() * 1000) + 600_000
    src = type("S", (), {"pipeline": lambda self, transaction=False:
                         _Pipe([5, due, b"payload"])})()
    pipes = []

    def pipeline(self, transaction=False):
        pipes.append(_Pipe(refuse=refused))
        return pipes[-1]
    dst = type("D", (), {"pipeline": pipeline})()
    how = {"expiry": "pexpiretime", "history": "idletime", "abs": True}
    return eng, src, dst, how, pipes


def test_a_target_that_takes_no_absolute_expiry_is_sent_the_time_left():
    import redis
    eng, src, dst, how, pipes = _redis_carry(
        redis.ResponseError("ERR syntax error"))
    said = []
    assert eng._carry(src, dst, [b"k"], how, "db0", said.append) == 1
    assert how["abs"] is False and len(pipes) == 2
    (_, args, kw), = pipes[1].sent
    assert "absttl" not in kw and 0 < args[1] <= 600_000
    assert said == ["db0: the moment each key falls due, with its access"
                    " history could not run (the target takes no absolute"
                    " expiry or access history on a restore) - switching to"
                    " the time left to each key's expiry"]


def test_a_key_the_redis_target_refuses_otherwise_stops_as_before():
    import redis
    eng, src, dst, how, pipes = _redis_carry(redis.ResponseError(
        "ERR DUMP payload version or checksum are wrong"))
    with pytest.raises(SystemExit, match="refused a key it was handed"):
        eng._carry(src, dst, [b"k"], how, "db0", pytest.fail)
    assert how["abs"] is True and len(pipes) == 1


# --- the pair's localization, a warehouse's digest, DynamoDB's tags --------

def _pair():
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(_hop("hetero", options={
        "source_engine": "postgres", "target_engine": "mysql"}))


def test_leaves_that_cannot_be_read_are_walked_and_said(monkeypatch):
    from migkit import leaves
    from migkit.engines.hetero import HeteroEngine
    eng = _pair()

    def refused(*a, **k):
        raise RuntimeError("the leaves' query was refused")
    monkeypatch.setattr(leaves, "drill", refused)
    monkeypatch.setattr(HeteroEngine, "_row_scope",
                        lambda self, db, t: (None, None))
    monkeypatch.setattr(HeteroEngine, "_drill_rows",
                        lambda self, *a: "; missing=1")
    got = eng._drill("app", "public.t", "t", [("id", "integer")],
                     [("id", "integer")], 10)
    assert got.startswith("; missing=1; ")
    assert "the rows found from the leaves of its digest tree could not run"\
           " (the leaves' query was refused) - switching to the rows walked"\
           " by key" in got, got
    # a fault in migkit is seen, not walked around
    monkeypatch.setattr(leaves, "drill", lambda *a, **k: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        eng._drill("app", "public.t", "t", [("id", "integer")],
                   [("id", "integer")], 10)


def test_a_digest_the_warehouse_refuses_on_a_later_row_is_folded_here():
    from migkit import canon, rowtext
    from migkit.engines import engine_named
    eng = engine_named("snowflake", Hop(
        name="w", engine="snowflake", source=Endpoint(),
        target=Endpoint(options={"database": "a", "account": "x"}),
        databases=["sales"]))
    eng.neutral_columns = lambda side, db, table: [("id", "INTEGER"),
                                                   ("name", "TEXT(40)")]

    def rows(side, db, sql, args=None):
        if "limit 64" in sql:
            return [[1, "a", rowtext.encode(["1", "a"])]]
        raise RuntimeError("Numeric value 'x' is not recognized")
    eng._rows = rows
    eng.neutral_batches = lambda side, db, table, columns, size=1000, \
        where=None: iter([[[1, "a"]]])
    eng._unpad = lambda side, db, table, columns, rows: rows
    said = []
    eng.lend_log(said.append)
    got = eng.neutral_digest("dst", "sales", "orders",
                             [("id", "integer"), ("name", "text")])
    n, total = canon.fold_rows(["integer", "text"], [[1, "a"]])
    assert got == (n, str(total)) and eng.folded_last is True
    assert said == ["orders: the digest worked out by the server in its own"
                    " SQL could not run (Numeric value 'x' is not"
                    " recognized) - switching to the rows read and folded"
                    " here"]


def test_a_dynamodb_endpoint_without_tags_keeps_the_table_described_here(
        monkeypatch, tmp_path):
    import json

    import migkit.config as cfg
    from migkit.engines.dynamodb import DynamoDBEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    eng = DynamoDBEngine(Hop(
        name="d", engine="dynamodb", source=Endpoint(),
        target=Endpoint(options={"endpoint_url": "http://127.0.0.1:1"}),
        databases=["app"]))
    arn = "arn:aws:dynamodb:ddblocal:table/app.t"

    class Client:
        def create_table(self, **kw):
            pass

        def get_waiter(self, name):
            return type("W", (), {"wait": lambda self, **kw: None})()

        def describe_table(self, TableName):
            return {"Table": {"TableArn": arn}}

        def tag_resource(self, **kw):
            raise Exception("Tagging is not currently supported in DynamoDB"
                            " Local.")
    monkeypatch.setattr(DynamoDBEngine, "_client", lambda self, side:
                        Client())
    monkeypatch.setattr(DynamoDBEngine, "neutral_tables",
                        lambda self, side, db: [])
    said = []
    eng.lend_log(said.append)
    eng.neutral_create("dst", "app", "t", [("id", "integer", ()),
                                           ("v", "text", ())], ["id"])
    kept = json.loads(eng._kept_here("dst").read_text())
    assert kept[arn] == {"columns": [["id", "integer"], ["v", "text"]],
                         "key": ["id"]}
    assert said == ["app.t: the table's own tags could not run (the endpoint"
                    " takes no tags) - switching to a file of migkit's for"
                    " the endpoint"]
    # a copy's description with no tags of the source's to put on it goes
    # to the file at once, nothing tried and nothing to say
    eng._keep_description("dst", "app.u", arn + "u", [],
                          [("id", "integer")], ["id"])
    kept = json.loads(eng._kept_here("dst").read_text())
    assert kept[arn + "u"] == {"columns": [["id", "integer"]], "key": ["id"]}
    assert len(said) == 1
