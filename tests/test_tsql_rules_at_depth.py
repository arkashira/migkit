"""T-SQL at depth by migkit's own rules (backlog W3): the procedure-wide
transaction under TRY/CATCH, OUTPUT into a table variable and to the
caller, MERGE by the target's version, a table variable through a
ROLLBACK, and an END after a semicolon.

Each version is held to PostgreSQL's own parser here; what it means is
the proof's, on servers (`test_sql_server_code_at_depth_proved.py`).
"""
import pytest

from migkit.storedcode import tsql_pg
from migkit.storedcode.plpgsql import unparsed
from migkit.storedcode.routines import Param, Routine
from migkit.storedcode.translate import Refused


class Ctx:
    local = None
    tables = {}
    routines = {}

    def __init__(self, version=17):
        self.pg_version = version


def _convert(definition, params=(), version=17, kind="procedure"):
    r = Routine(kind=kind, name="p", params=[Param(*p) for p in params],
                definition=definition)
    got = tsql_pg.convert(r, Ctx(version))
    assert unparsed(got.statements) is None, got.sql
    return got


TRANSFER = """create procedure p @id int, @amt decimal(10,2) as
begin
  set nocount on;
  begin try
    begin tran;
    update accounts set balance = balance - @amt where id = @id;
    insert into ledger (account, amount) values (@id, @amt);
    commit;
  end try
  begin catch
    if @@trancount > 0 rollback;
    throw;
  end catch
end"""


def test_the_procedure_wide_transaction_is_one_block_without_a_commit():
    got = _convert(TRANSFER, [("@id", "int"), ("@amt", "decimal(10,2)")])
    body = got.sql.lower()
    # the handler's rollback of the block is the ROLLBACK; the error raised
    # again as it came; no COMMIT a caller's transaction would refuse
    assert "exception when others then" in body
    assert "raise exception using errcode = _migkit_state" in body
    assert "commit" not in body and "rollback" not in body
    assert not got.commits
    assert ("tsql.try-transaction", 3) in got.constructs, got.constructs
    # each statement of the TRY is not a block of its own here: the whole
    # transaction goes back on a failure, as SQL Server's ROLLBACK takes it
    assert body.count("exception when others then") == 1


def test_a_catch_that_writes_and_raises_again_is_refused():
    with pytest.raises(Refused, match="writes after the ROLLBACK"):
        _convert(TRANSFER.replace(
            "throw;", "insert into errors (msg) values (error_message());"
                      " throw;"), [("@id", "int"),
                                   ("@amt", "decimal(10,2)")])
    # one that writes and swallows the error keeps both sides' meaning
    got = _convert(TRANSFER.replace(
        "throw;", "insert into errors (msg) values (error_message());"),
        [("@id", "int"), ("@amt", "decimal(10,2)")])
    assert "insert into errors" in got.sql.lower()
    assert "_migkit_msg" in got.sql


def test_a_write_before_the_transaction_is_refused_when_the_catch_raises():
    with pytest.raises(Refused, match="writes before its transaction"):
        _convert(TRANSFER.replace(
            "set nocount on;", "insert into audit (what) values ('start');"),
            [("@id", "int"), ("@amt", "decimal(10,2)")])


def test_output_lands_where_it_is_sent():
    got = _convert("""create procedure p @name varchar(20) as
begin
  declare @t table (id int, name varchar(20));
  insert into items (name) output inserted.id, inserted.name into @t (id, name)
    values (@name);
  select id, name from @t;
end""", [("@name", "varchar(20)")])
    assert "returning id, name) insert into _migkit_tmp_tv_t (id, name)" \
        " select * from" in got.sql, got.sql
    got = _convert("""create procedure p @id int as
begin
  delete from items output deleted.id, deleted.name as gone where id = @id;
end""", [("@id", "int")])
    # to the caller: a result set read from the rows the write put aside
    assert got.result_sets == ["_migkit_rs1"]
    assert "create temporary table _migkit_out1 as select items.id," \
        " items.name as gone from items where false" in got.sql, got.sql
    assert "open _migkit_rs1 for select * from _migkit_out1" in got.sql


def test_the_row_before_an_update_is_returned_only_by_a_target_that_can():
    update = """create procedure p @id int as
begin
  update items set name = 'x' output deleted.name as before,
    inserted.name as after where id = @id;
end"""
    with pytest.raises(Refused, match="from version 18 and the target runs"
                                      " 17"):
        _convert(update, [("@id", "int")], version=17)
    got = _convert(update, [("@id", "int")], version=18)
    assert "returning old.name as before, new.name as after" in got.sql


def test_merge_is_carried_by_what_the_target_runs():
    merge = """create procedure p as
begin
  merge into items as t using (select 1 as id, 'a' as name) as s
    on t.id = s.id
  when matched then update set name = s.name
  when not matched then insert (id, name) values (s.id, s.name)
  when not matched by source then delete;
end"""
    with pytest.raises(Refused, match="from version 17 and the target runs"
                                      " 16"):
        _convert(merge, version=16)
    got = _convert(merge, version=17)
    assert "WHEN NOT MATCHED BY SOURCE THEN" in got.sql.upper()
    with pytest.raises(Refused, match="from version 15"):
        _convert(merge.replace(" when not matched by source then delete",
                               ""), version=14)
    got = _convert(merge.replace(
        " when not matched by source then delete",
        " output $action as did"), version=17)
    assert "merge_action() as did" in got.sql


def test_a_table_variable_through_a_rollback_is_refused():
    with pytest.raises(Refused, match="keeps its rows through a"):
        _convert("""create procedure p as
begin
  declare @t table (id int);
  begin tran;
  insert into @t values (1);
  rollback;
  select id from @t;
end""")


def test_an_end_after_a_semicolon_is_read_as_end():
    # the translator's own tokenizer kept `END TRY BEGIN CATCH ...` after a
    # semicolon as one piece of text: no TRY could be read
    toks = [t.text.upper() for t in tsql_pg._tokenize(
        "commit;\n end try begin catch select 1; end catch")]
    assert toks[:5] == ["COMMIT", ";", "END", "TRY", "BEGIN"], toks
    got = _convert("""create procedure p as
begin
  begin try
    update items set qty = qty - 1 where id = 1;
  end try
  begin catch
    update items set qty = 0 where id = 1;
  end catch
end""")
    assert ("tsql.try-catch", 2) in got.constructs, got.constructs
