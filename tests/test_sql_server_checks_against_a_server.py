"""The native checks of a SQL Server hop, against a server (Azure SQL
Edge, SQL Server's engine in the build that runs on arm64).

Run against one for the first time, three things were wrong:
* the client refused the server's own certificate (`x509: negative serial
  number`, since Go 1.23), so no check ran at all
* a statement that failed exited 0 with its message printed as rows -
  `Msg 8134 ... Divide by zero` - and both sides of a comparison can print
  the same message
* a constraint whose name the server made up was compared by that name:
  the same primary key made in two databases was `PK__o__3213E83F720CC3A2`
  in one and `PK__o__3213E83FA19E373F` in the other, one missing and one
  extra. Such a constraint is named by what it is now - its kind, table
  and columns
"""
import pathlib
import tempfile

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.test_sql_server_follows_through_change_tracking import (PORT, PW,
                                                                   _sql,
                                                                   server)

pytestmark = needs_docker

__all__ = ["server"]


@pytest.fixture
def hop(server):
    for db in ("shop", "shop_new"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
        _sql(db, "create table dbo.o (id int identity primary key,"
                 " v nvarchar(10))",
             "create table dbo.p (id int primary key, code nvarchar(5)"
             " unique, n int default 0 check (n >= 0), o_id int references"
             " dbo.o(id), check (code <> ''))")
    _sql("shop", "insert into dbo.o (v) values ('a'), ('b'), ('c')")
    _sql("shop_new", "set identity_insert dbo.o on; insert into dbo.o (id, v)"
                     " values (1, 'a'), (2, 'b'); set identity_insert dbo.o"
                     " off")
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    eng = MSSQLEngine(Hop(name="ms", engine="mssql", source=ep, target=ep,
                          databases=["shop"], db_map={"shop": "shop_new"}))
    eng.hop.report_dir = lambda db=None: pathlib.Path(tempfile.mkdtemp())
    return eng


def test_constraints_the_server_named_are_compared_by_what_they_are(hop):
    assert [(r.status, r.detail) for r in hop.check_schema("shop")] == [
        ("ok", "9 objects, 3 indexes")]
    _sql("shop", "alter table dbo.p add constraint named_ck check (id > 0)")
    got = hop.check_schema("shop")
    assert [(r.status, r.detail) for r in got] == [
        ("diff", "missing CHECK_CONSTRAINT dbo.named_ck")], got


def test_a_statement_that_fails_is_an_error_not_rows(hop):
    with pytest.raises(RuntimeError) as e:
        hop._cmd("src", "shop", "select 1/0")
    assert "Divide by zero" in str(e.value)


def test_counts_identities_and_a_rollback_of_them(hop, tmp_path):
    from migkit.engines.base import RepairAction
    assert [(r.status, r.detail) for r in hop.check_counts("shop")] == [
        ("diff", "dbo.o src=3 dst=2")]
    assert [(r.status, r.detail) for r in hop.check_autoinc("shop")] == [
        ("ok", "1 identity tables clear their column max, no collision"),
        ("diff", "dbo.o src=3 dst=2")]
    hop.snapshot_state("shop", tmp_path, kind="sequences")
    _sql("shop_new", "insert into dbo.o (v) values ('x'), ('y')",
         "delete from dbo.o where id > 2")
    assert _sql("shop_new", "select ident_current('dbo.o')")[0][0] == 4
    hop.apply("shop", RepairAction("shop", "sequences",
                                   hop.restore_sequences("shop", tmp_path),
                                   [], "rollback restore"))
    assert _sql("shop_new", "select ident_current('dbo.o')")[0][0] == 2
