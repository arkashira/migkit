"""SAP ASE (Sybase) as one side of a pair (backlog 34), held to what does
not need a server: SAP's ASE image runs on x86 only, and this machine is
arm64.

ASE is reached through FreeTDS's ODBC driver at protocol 5.0: the TDS
driver that installs with pip stops at SQL Server's 7.x (measured,
`unrecognized tds version: 5.0`). What is pinned: how the connection is
asked for, how a parameter and a name are written, how its types are
classed, and what a table migkit creates there looks like.
"""
import pytest

from migkit.config import Endpoint, Hop


def _ase(**options):
    from migkit.engines import engine_named
    return engine_named("sybase", Hop(
        name="ase", engine="sybase",
        source=Endpoint(host="10.0.0.7", port=5000, user="sa",
                        password="CHANGE_ME}x", options=options),
        target=Endpoint(host="10.0.0.8", port=5000, user="sa",
                        password="CHANGE_ME"),
        databases=["shop"], db_map={"shop": "shop_new"}))


def test_the_connection_it_asks_for():
    eng = _ase(odbc_driver="/opt/freetds/lib/libtdsodbc.so")
    assert eng._connection_string("src", "shop") == (
        "DRIVER={/opt/freetds/lib/libtdsodbc.so};SERVER=10.0.0.7;PORT=5000;"
        "TDS_Version=5.0;UID=sa;PWD={CHANGE_ME}}x};DATABASE=shop;"
        "ClientCharset=UTF-8")


def test_no_driver_is_said_with_where_to_get_one(monkeypatch):
    import os
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    with pytest.raises(SystemExit) as e:
        _ase()._connection_string("dst", "shop")
    assert str(e.value).startswith("FreeTDS's ODBC driver is not on this"
                                   " machine"), e.value


def test_marks_names_and_the_target_database():
    from migkit.engines.dbapi import MARK
    eng = _ase()
    assert eng._sql(f"select 1 from t where a = {MARK}", True) == \
        "select 1 from t where a = ?"
    assert eng._qualified("dst", "shop", "dbo.orders") == '"dbo"."orders"'
    assert eng._select("a", "t", "", " order by a", 50) == \
        "select top (50) a from t order by a"
    assert eng._d("dst", "shop") == "shop_new"


@pytest.mark.parametrize("declared,cls", [
    ("numeric(10,2)", "decimal"), ("money", "decimal"),
    ("univarchar(20)", "text"), ("image", "bytes"), ("bit", "boolean"),
    ("bigdatetime", "timestamp"), ("unsigned int", "integer"),
    ("timestamp", None)])
def test_types(declared, cls):
    from migkit import canon
    assert canon.type_class("ase", declared) == cls


def test_the_table_it_would_create():
    eng = _ase()
    assert eng.neutral_create_sql(
        "dst", "shop", "dbo.orders",
        [("id", "integer", (), {"null": False}),
         ("amount", "decimal", (12, 2)), ("name", "text", (40,)),
         ("at", "timestamp", ())], key=("id",)) == (
        'create table "dbo"."orders" ("id" bigint not null,'
        ' "amount" numeric(12,2), "name" varchar(40),'
        ' "at" bigdatetime, primary key ("id"))')


def test_odbc_that_cannot_load_is_said_with_what_to_install(monkeypatch):
    import builtins
    real = builtins.__import__

    def refuse(name, *a, **k):
        if name == "pyodbc":
            raise ImportError("dlopen(pyodbc.so): Library not loaded:"
                              " libodbc.2.dylib")
        return real(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", refuse)
    with pytest.raises(SystemExit) as e:
        _ase(odbc_driver="/x/libtdsodbc.so")._connect("src", "shop")
    assert str(e.value).startswith("ODBC is not usable on this machine"
                                   " (dlopen(pyodbc.so)"), e.value
    assert "unixodbc" in str(e.value)
