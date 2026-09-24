"""`check --consistent` compares the tables and rows every other check
compares.

The consistent pass built its own list of tables and read each one whole.
Every other pass reads through `_keep_tbl` (the hop's exclude list) and
`_scope` (its row filters). So on a target that was exactly what the hop
asked for, the consistent pass called two tables different:
* a table the hop excludes, whose rows the target owns
* a table under a row filter, whose target holds only the rows it selects

Both the single-script pass and the lanes sharing an exported snapshot
read the same way now.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _eng(pg_pair, tmp_path, workers):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="cs", engine="postgres", workers=workers,
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], exclude=["public.audit"],
              mapping={"where": {"public.orders": "region = 'apac'"}})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@pytest.mark.parametrize("workers", [1, 2])
def test_the_hop_as_asked_is_the_same(pg_pair, tmp_path, workers):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.orders (id int primary key,"
                   " region text);"
                   " create table public.audit (id int primary key);"
                   " create table public.plain (id int primary key)")
    psql(pg_pair["src"], "insert into public.orders values (1, 'apac'),"
                         " (2, 'emea'); insert into public.plain values (1)")
    # what the hop asked for: the filtered rows, and the target's own audit
    psql(pg_pair["dst"], "insert into public.orders values (1, 'apac');"
                         " insert into public.audit values (7);"
                         " insert into public.plain values (1)")
    eng = _eng(pg_pair, tmp_path, workers)
    got = eng.check_data("postgres", consistent=True)
    data = [r for r in got if r.check == "data"]
    assert [r.status for r in data] == ["ok"], [r.__dict__ for r in data]
    # and a real difference is still one
    psql(pg_pair["dst"], "update public.orders set region = 'apac '")
    got = eng.check_data("postgres", consistent=True)
    data = [r for r in got if r.check == "data"]
    assert [r.status for r in data] == ["diff"], [r.__dict__ for r in data]
    assert "public.orders" in data[0].detail, data[0].detail
    assert "audit" not in data[0].detail, data[0].detail


def test_the_columns_named_are_the_ones_that_differ_in_scope(pg_pair,
                                                            tmp_path):
    """After a difference, the check names the columns that differ. It
    read the whole table: under a row filter the whole source differs from
    the target in every column, and all of them were named."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.orders (id int primary key,"
                   " region text, v int)")
    psql(pg_pair["src"], "insert into public.orders values"
                         " (1, 'apac', 10), (2, 'emea', 20)")
    psql(pg_pair["dst"], "insert into public.orders values (1, 'apac', 11)")
    eng = _eng(pg_pair, tmp_path, 1)
    eng._column_fingerprint("postgres", "public.orders")
    got = (tmp_path / "data-public.orders.columns").read_text()
    assert got.split("\n")[1:] == ["v", ""], got
