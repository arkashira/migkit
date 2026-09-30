"""Teradata against a real system: a Vantage Express VM, which runs on x86
only (VirtualBox or VMware; UTM on Apple Silicon emulates it too slowly to
test with), named by `MIGKIT_TERADATA_HOST` - with `MIGKIT_TERADATA_USER`
and `MIGKIT_TERADATA_PASSWORD` (dbc and dbc on the VM as Teradata ships
it). Marked `teradata` by `tests/conftest.py`; skipped where no system is
named or Teradata's driver is not installed (`migkit doctor --install`
puts it in place once its terms are accepted).

Two databases of one system are the two sides: a table is copied from one
into the other by the table copier - its batches bulk-loaded where they
are large enough - its target made by migkit; the check proves the two
equal on the servers, and says "different" once one value on the target
is changed.
"""
import os
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

HOST = os.environ.get("MIGKIT_TERADATA_HOST", "")
USER = os.environ.get("MIGKIT_TERADATA_USER", "dbc")
PASSWORD = os.environ.get("MIGKIT_TERADATA_PASSWORD", "dbc")

pytestmark = [
    pytest.mark.skipif(not HOST, reason="no Teradata system is named"
                                        " (MIGKIT_TERADATA_HOST)"),
]

SRC_DB, DST_DB = "migkit_src", "migkit_dst"


def _sql(*statements):
    import teradatasql
    with teradatasql.connect(host=HOST, user=USER, password=PASSWORD,
                             tmode="ANSI") as conn:
        cur = conn.cursor()
        out = None
        for st in statements:
            cur.execute(st)
            out = cur.fetchall() if cur.description else None
        return out


@pytest.fixture(scope="module")
def system():
    pytest.importorskip("teradatasql")
    for db in (DST_DB, SRC_DB):
        try:
            _sql(f"delete database {db}", f"drop database {db}")
        except Exception:  # noqa: BLE001 - not there yet
            pass
    for db in (SRC_DB, DST_DB):
        _sql(f"create database {db} as perm = 100e6")
    _sql(f"create multiset table {SRC_DB}.orders (id integer not null,"
         " code char(6) character set latin not casespecific, name"
         " varchar(60) character set unicode, amount decimal(12,2), span"
         " period(date), primary key (id))")
    for i in range(1, 400):
        _sql(f"insert into {SRC_DB}.orders values ({i}, 'C{i % 50}',"
             f" 'Name {i}', {i}.25, period(date '2024-01-01',"
             " date '2024-02-01'))")
    yield
    for db in (DST_DB, SRC_DB):
        try:
            _sql(f"delete database {db}", f"drop database {db}")
        except Exception:  # noqa: BLE001 - left as it is, and said
            pass


def _hop(tmp_path):
    hop = Hop(name="td", engine="teradata",
              source=Endpoint(host=HOST, user=USER, password=PASSWORD),
              target=Endpoint(host=HOST, user=USER, password=PASSWORD),
              databases=[SRC_DB], db_map={SRC_DB: DST_DB})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_a_table_is_copied_and_proved_on_the_servers(system, tmp_path,
                                                     monkeypatch):
    from migkit.cli import _Checkpoint
    from migkit.engines import teradata
    from migkit.engines.teradata import TeradataEngine
    # batches large enough for the bulk load on a table this size
    monkeypatch.setattr(teradata, "BULK_ROWS", 100)
    eng = TeradataEngine(_hop(tmp_path))
    pair = eng._as_pair()
    pair.create_missing(SRC_DB, print)
    ck = _Checkpoint(tmp_path / "move.json")
    for sch, t in eng.list_move_tables(SRC_DB):
        eng.move_table(SRC_DB, sch, t, 150, ck, print)
    got = {r.scope: r for r in eng.check_data(SRC_DB)}
    assert got[f"{SRC_DB}.orders"].status == "ok", got
    assert _sql(f"select count(*) from {DST_DB}.orders")[0][0] == 399
    assert _sql(f"select amount from {DST_DB}.orders where id = 7")[0][0] \
        == Decimal("7.25")
    # one value different on the target is a difference
    _sql(f"update {DST_DB}.orders set name = 'name 7' where id = 7")
    got = {r.scope: r for r in TeradataEngine(_hop(tmp_path))
           .check_data(SRC_DB)}
    assert got[f"{SRC_DB}.orders"].status == "diff", got
