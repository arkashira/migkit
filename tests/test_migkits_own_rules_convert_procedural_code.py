"""migkit's own rules for MySQL's and SQL Server's procedural code into
PL/pgSQL (backlog R11): each rule's shape, held here without a server.
What each rule means is held by running the code on both engines
(`test_stored_code_is_proved_by_running_it`, and the conversion's own
proof); here each is held to be there, and each refusal to be one.
"""
import pytest

from migkit.storedcode.routines import Param, Routine
from migkit.storedcode.translate import Refused


def _my(body, kind="procedure", params=(), returns=None, ctx=None):
    from migkit.storedcode import mysql_pg
    r = Routine(kind, "r", list(params), returns, body=body)
    return "\n".join(mysql_pg.convert(r, ctx).statements)


def _ts(definition, kind="procedure", params=(), returns=None, ctx=None):
    from migkit.storedcode import tsql_pg
    r = Routine(kind, "r", list(params), returns, definition=definition)
    return "\n".join(tsql_pg.convert(r, ctx).statements)


def test_a_continue_handler_rolls_back_only_the_statement_that_failed():
    got = _my("begin declare continue handler for sqlexception set @e = 1;"
              " insert into t values (1); insert into t values (2);"
              " set @d = 3; end")
    # three statements under the handler: three blocks of their own, so a
    # failure rolls back one, not the two before it
    assert got.count("exception\n") == 3, got
    assert got.count("_at_e := '1'") == 0 and got.count("_at_e := 1") == 3, \
        got


def test_an_exit_handler_leaves_the_block_that_declared_it():
    got = _my("lbl: begin declare exit handler for sqlexception set @e = 1;"
              " insert into t values (1); end lbl")
    assert "exit lbl;" in got, got


def test_select_into_keeps_the_variables_where_no_row_comes():
    got = _my("begin declare v int default 7; select a into v from t where"
              " id = 0; end")
    assert "INTO STRICT v" in got, got
    assert "when no_data_found then\n" in got, got
    assert "when too_many_rows then" in got and "errcode = '42000'" in got, \
        got


def test_fetch_past_the_last_row_keeps_the_variables():
    got = _my("begin declare done int default 0; declare v int;"
              " declare c cursor for select id from t;"
              " declare continue handler for not found set done = 1;"
              " open c; fetch c into v; close c; end")
    assert "fetch c into _migkit_f" in got, got
    assert "if found then\n" in got and "v := _migkit_f" in got, got
    assert "done := 1;" in got.split("else", 1)[1], got


def test_a_declared_scale_and_length_are_applied_on_the_way_in():
    got = _my("return x * 1.07", kind="function",
              params=[Param("x", "decimal(10,2)")], returns="decimal(10,2)")
    assert "x := cast(x as numeric(10,2));" in got, got
    assert "return cast(x * 1.07 as numeric(10,2));" in got, got


def test_mysql_meaning_kept_where_the_function_names_agree():
    got = _my("return length(s)", kind="function",
              params=[Param("s", "varchar(20)")], returns="int")
    assert "octet_length(s)" in got.lower(), got
    got = _my("return a / b", kind="function",
              params=[Param("a", "int"), Param("b", "int")],
              returns="decimal(10,4)")
    assert "CAST(a AS DECIMAL) / NULLIF(b, 0)" in got, got


def test_a_write_limited_to_n_rows_touches_n_rows():
    got = _my("delete from t where a < 0 limit 10")
    assert "ctid IN (SELECT ctid FROM t WHERE a < 0 LIMIT 10)" in got, got


def test_on_duplicate_key_names_the_one_key_mysql_hits():
    from migkit.storedcode.mysql_pg import Context
    ctx = Context(keys={"t": [["id"]]})
    got = _my("insert into t (id, n) values (1, 2) on duplicate key update"
              " n = values(n)", ctx=ctx)
    assert "ON CONFLICT(id) DO UPDATE SET n = excluded.n" in got, got
    with pytest.raises(Refused, match="2 unique keys, not one"):
        _my("insert into t (id, n) values (1, 2) on duplicate key update"
            " n = 3", ctx=Context(keys={"t": [["id"], ["n"]]}))


def test_a_result_set_goes_to_the_caller_as_a_cursor():
    got = _my("begin select 1; select 2; end")
    assert "inout _migkit_rs1 refcursor default null" in got, got
    assert "inout _migkit_rs2 refcursor default null" in got, got
    assert "open _migkit_rs2 for SELECT 2;" in got, got


@pytest.mark.parametrize("body,why", [
    ("begin prepare s from 'select 1'; execute s; end", "prepare s from"),
    ("begin declare continue handler for 9999 set @e = 1; select 1; end",
     "handles MySQL error 9999"),
    ("begin while 1 do select 1; end while; end", "from inside a loop"),
    ("begin declare continue handler for sqlexception begin end; commit;"
     " end", "under a handler")])
def test_what_has_no_rule_is_refused_by_name(body, why):
    with pytest.raises(Refused, match=why):
        _my(body)


def test_coverage_probes_go_in_a_copy_where_they_change_nothing():
    from migkit.storedcode import mysql_pg
    body = "begin if x > 1 then set @a = 1; end if; end"
    r = Routine("procedure", "p", [Param("x", "int")], body=body,
                definition=f"CREATE PROCEDURE `p`(x int) {body}")
    made, probes = mysql_pg.instrument(r)
    assert made.startswith("CREATE PROCEDURE `p`(x int) begin"), made
    # the block, the THEN, and the ELSE the IF did not have
    assert [w for _, _, w in probes] == ["block", "then", "else (none)"], \
        probes
    assert made.count("insert into migkit_cover") == 3, made
    reads = Routine("procedure", "q", [], body="begin update t set a = 1;"
                    " select row_count(); end", definition="x")
    assert mysql_pg.instrument(reads) is None


def test_tsql_try_keeps_the_writes_before_the_failure():
    got = _ts("CREATE PROCEDURE r @i int AS BEGIN TRY INSERT INTO t VALUES"
              " (@i) INSERT INTO t VALUES (@i) END TRY BEGIN CATCH SELECT 1"
              " END CATCH", params=[Param("i", "int")])
    assert got.count("exit _migkit_try1;") == 2, got
    assert got.count("exception when others then") == 2, got


def test_tsql_variables_are_apart_from_columns():
    got = _ts("CREATE PROCEDURE r @id int AS UPDATE t SET n = @id WHERE"
              " id = @id SET @id = @@ROWCOUNT", params=[Param("id", "int")])
    assert "#variable_conflict use_column" in got, got
    assert "UPDATE t SET n = p_id WHERE id = p_id;" in got, got
    assert "p_id := _migkit_rowcount;" in got, got


def test_tsql_string_plus_len_and_top():
    got = _ts("CREATE FUNCTION r(@s varchar(10)) RETURNS int AS BEGIN"
              " DECLARE @t varchar(20) SET @t = @s + 'x' RETURN LEN(@t)"
              " END", kind="function", params=[Param("s", "varchar(10)")],
              returns="int")
    assert "v_t := p_s || 'x';" in got, got
    assert "LENGTH(rtrim(CAST(v_t AS TEXT)))" in got, got
    got = _ts("CREATE PROCEDURE r AS DELETE TOP (3) FROM t WHERE a < 0")
    assert "ctid IN (SELECT ctid FROM t WHERE a < 0 LIMIT 3)" in got, got


def test_tsql_trigger_reads_transition_tables_one_per_event():
    got = _ts("CREATE TRIGGER r ON t AFTER INSERT, DELETE AS INSERT INTO"
              " a SELECT id FROM inserted UNION ALL SELECT id FROM deleted",
              kind="trigger")
    assert 'create trigger "r" after insert on t referencing new table as' \
        ' inserted for each statement' in got, got
    assert 'create trigger "r_delete" after delete on t referencing old' \
        ' table as deleted' in got, got
    # the insert's version reads no rows as deleted
    insert_fn = got.split('create function "r_delete"')[0]
    assert "(select * from t where false) as deleted" in insert_fn, got


def test_tsql_return_status_is_refused():
    with pytest.raises(Refused, match="status code"):
        _ts("CREATE PROCEDURE r AS RETURN 5")
