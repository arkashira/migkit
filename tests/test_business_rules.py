"""The operator's own business rules, asked of both sides (backlog 8).

Row comparison proves the rows match; it cannot know that the business
means revenue per day or orders per status. A hop's `rules` carries those
questions as SQL. Each runs on both sides in a transaction that cannot
write, and the answers are compared by value: a sum written `1.5000` on one
server and `1.5` on the other is the same sum, and rows are compared as a
set because servers group in their own order.
"""
import sqlite3

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _lite(tmp_path, rules, src_rows, dst_rows):
    from migkit.engines.sqlite import SQLiteEngine
    for name, rows in (("a.db", src_rows), ("b.db", dst_rows)):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table orders (id integer primary key,"
                    " status text, amount numeric)")
        con.executemany("insert into orders values (?, ?, ?)", rows)
        con.commit()
        con.close()
    hop = Hop(name="r", engine="sqlite",
              source=Endpoint(host=str(tmp_path / "a.db"), port=0, user="",
                              password=""),
              target=Endpoint(host=str(tmp_path / "b.db"), port=0, user="",
                              password=""),
              databases=["main"], options={"rules": rules})
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


ROWS = [(1, "paid", 10.5), (2, "open", 3), (3, "paid", 1)]
RULES = {"orders by status":
         "select status, count(*), sum(amount) from orders group by status"}


def test_a_rule_that_holds_is_ok(tmp_path):
    eng = _lite(tmp_path, RULES, ROWS, list(reversed(ROWS)))
    assert "rules" in eng.planned_checks()
    got = eng.check_rules("main")
    assert [r.status for r in got] == ["ok"], [(r.status, r.detail)
                                              for r in got]


def test_a_rule_that_breaks_names_the_rows(tmp_path):
    changed = [(1, "paid", 10.5), (2, "open", 3), (3, "open", 1)]
    got = _lite(tmp_path, RULES, ROWS, changed).check_rules("main")
    assert got[0].status == "diff", got[0].detail
    assert "paid" in got[0].detail and "open" in got[0].detail


def test_a_rule_cannot_write(tmp_path):
    eng = _lite(tmp_path, {"sneaky": "delete from orders"}, ROWS, ROWS)
    got = eng.check_rules("main")
    assert got[0].status == "error", got[0].detail
    con = sqlite3.connect(tmp_path / "a.db")
    assert con.execute("select count(*) from orders").fetchone()[0] == 3


def test_no_rules_no_check(tmp_path):
    eng = _lite(tmp_path, {}, ROWS, ROWS)
    assert "rules" not in eng.planned_checks()


def test_values_meet_across_spellings():
    from decimal import Decimal

    from migkit import rules
    assert rules.value(Decimal("1.5000")) == rules.value(1.5)
    assert rules.value(3) == rules.value(Decimal("3.0"))
    assert rules.value(True) == rules.value(1)


@needs_docker
def test_postgres_rules_run_read_only(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.sales (amount numeric(12,4));"
                   " insert into public.sales values (1.5), (2.25)")
    hop = Hop(name="rp", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              options={"rules": {"revenue": "select sum(amount) from sales",
                                 "sneaky": "delete from sales"}})
    hop.report_dir = lambda db=None: tmp_path
    got = {r.scope: r for r in PostgresEngine(hop).check_rules("postgres")}
    assert got["postgres rule revenue"].status == "ok", got
    assert got["postgres rule sneaky"].status == "error", got
    assert "read-only" in got["postgres rule sneaky"].detail, got
    assert psql(pg_pair["src"], "select count(*) from sales"
                ).stdout.strip() == "2"
