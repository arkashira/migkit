"""On MySQL: a hop that carries no deletes is checked and repaired as what
it is, and a scope that leaves out what its tables need is said before
the move - read from `information_schema` (a view's tables from
`view_table_usage`, a routine's and a trigger's from their bodies).

The drilldown counts a differing table's rows key by key, so its shape is
said as counted (`kind=` from the counts), never only inferred from the
key sums - which is what a hop that keeps the source's deletes is held to.
"""
from migkit import verdict
from tests import mysql_pair
from tests.conftest import needs_docker
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

pytestmark = needs_docker


def _engine(tmp_path, **kw):
    import migkit.config as cfg
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    cfg.REPORTS = tmp_path / "reports"
    hop = Hop(name="keeps", engine="mysql",
              source=Endpoint(host="127.0.0.1",
                              port=mysql_pair._side("src").port,
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1",
                              port=mysql_pair._side("dst").port,
                              user="root", password="test"),
              databases=["shop"], **kw)
    return MySQLEngine(hop)


NO_DELETES = {"mapping": {"ops": {"shop.d": "insert, update"}}}


def _fresh():
    for side in ("src", "dst"):
        my(side, "drop database if exists shop; create database shop;"
                 " use shop; create table d (id int primary key,"
                 " v varchar(20)); insert into d values (1, 'a'), (2, 'b'),"
                 " (3, 'c'), (4, 'd');")


def test_the_drilldown_says_the_shape_it_counted(my_pair, tmp_path):
    _fresh()
    my("src", "use shop; delete from d where id = 2;"
              " insert into d values (9, 'z');")
    r, _, _ = _engine(tmp_path)._diff_table("shop", "d")
    assert r.status == "diff"
    assert "missing=1 extra=1 changed=0 kind=rows-replaced" in r.detail, \
        r.detail


def test_a_row_the_source_deleted_is_kept_not_different(my_pair, tmp_path):
    _fresh()
    my("src", "use shop; delete from d where id = 2;")
    eng = _engine(tmp_path, **NO_DELETES)
    rs = [r.__dict__ for r in eng.check_data("shop")]
    got = verdict.decide(rs, eng.hop)
    assert got.status == "same", rs
    row = [r for r in rs if r["scope"] == "shop.d"][0]
    assert row["kept_by_hop"] == ("kept by the hop: 1 row the source deleted"
                                  " - deletes are not carried")


def test_the_repair_leaves_the_kept_row_and_carries_the_missing_one(
        my_pair, tmp_path):
    _fresh()
    my("src", "use shop; delete from d where id = 2;"
              " insert into d values (9, 'z');")
    eng = _engine(tmp_path, **NO_DELETES)
    eng.check_data("shop")
    rows = [a for a in eng.repair_plan("shop", "rows") if a.kind == "rows"]
    assert len(rows) == 1, rows
    assert "1 row the source deleted left as they are" in \
        " ".join(rows[0].statements)
    eng.apply("shop", rows[0])
    there = my("dst", "select group_concat(id order by id) from shop.d"
                      " where id in (2, 9);")
    assert there == "2,9", there


SCHEMA = """
drop database if exists shop; create database shop; use shop;
create table customers (id int primary key);
create table orders (id int primary key, customer_id int,
                     foreign key (customer_id) references customers (id));
insert into customers values (1), (2);
insert into orders values (1, 1), (2, 1), (3, 2), (4, null);
create table audit_log (id int auto_increment primary key,
                        what varchar(20));
create trigger orders_audit after insert on orders for each row
  insert into audit_log (what) values ('order');
create function count_customers() returns int reads sql data
  return (select count(*) from customers);
create view order_totals as select c.id, count(*) as n
  from customers c join orders o on o.customer_id = c.id group by c.id;
"""


def test_what_the_taken_tables_need_is_said_before_the_move(my_pair,
                                                           tmp_path):
    my("src", SCHEMA)
    eng = _engine(tmp_path, exclude=["shop.customers", "shop.audit_log"])
    items = [i for i in eng._include_items() if i["item"] == "dependencies"]
    said = " | ".join(i["detail"] for i in items if i["level"] == "warn")
    assert "orders (customer_id) -> customers - 3 of its rows hold a key" \
           " into it" in said, said
    assert "take shop.customers out of exclude" in said
    assert "trigger orders_audit on orders names audit_log" in said
    assert "count_customers names customers" in said
    assert "view order_totals reads customers" in said
    # a sequence and a partition never cross a scope here: nothing is
    # said of them as not checked
    assert "not checked" not in said
