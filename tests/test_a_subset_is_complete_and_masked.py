"""Copies for testing (backlog R23, 5): the rows chosen and every row they
refer to, masked where the hop says, proved row by row.

Four tables that refer to each other - customers, products, orders and the
items of each order - and one nothing refers to. A subset starting from ten
orders holds those orders and the customers they name, and nothing else;
one starting from a product with `children: true` takes the items that
name it, their orders, the customers of those, and closes there. The
customers' email and phone are masked on the way; the check holds them to
the rule and every other value to the source's, and says so when a row
goes missing or a masked value comes back.

The rule parsing, the order tables are written in and the share taken by
a hash of the key need no server; the rest runs PostgreSQL 16 into
PostgreSQL 16.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker

SRC, DST = ("migkit-test-subset-src", 16334), ("migkit-test-subset-dst",
                                               16335)


# --- the rules, without a server -------------------------------------------------

def test_each_rule_is_read_as_it_is_written():
    from migkit import subset
    assert subset.rule("t", "created >= '2026-09-01'").kind == "where"
    assert subset.rule("t", "10%") == subset.Rule("t", "share", 10.0)
    assert subset.rule("t", 500) == subset.Rule("t", "rows", 500)
    assert subset.rule("t", "500") == subset.Rule("t", "rows", 500)
    assert subset.rule("t", "all").kind == "all"
    for wrong in ("0%", "150%", 0, ""):
        with pytest.raises(SystemExit):
            subset.rule("t", wrong)


def test_parents_are_written_before_what_refers_to_them():
    from migkit import subset
    edges = [("items", ["order_id"], "orders", ["id"]),
             ("orders", ["customer_id"], "customers", ["id"]),
             ("items", ["product_id"], "products", ["id"])]
    got = subset._order(["items", "orders", "customers", "products"], edges)
    assert got.index("customers") < got.index("orders") < got.index("items")
    assert got.index("products") < got.index("items")
    # a cycle is written all the same, in the order it is met
    assert sorted(subset._order(["a", "b"], [("a", ["x"], "b", ["id"]),
                                             ("b", ["y"], "a", ["id"])])) \
        == ["a", "b"]


def test_a_kept_value_comes_back_as_it_was():
    import datetime
    import decimal

    from migkit import subset
    for v in (None, 5, "x", True, 1.25, decimal.Decimal("10.50"),
              datetime.datetime(2026, 9, 30, 12, 0, 1),
              datetime.date(2026, 9, 30), b"\x00\xff"):
        assert subset.back_value(subset.keep_value(v)) == v


class _Source:
    """One table of 2,000 keyed rows, as the neutral contract reads it."""
    CANON_ENGINE = "postgres"

    def _as_pair(self):
        return self

    @property
    def src_engine(self):
        return self

    def neutral_tables(self, side, db):
        return ["public.t"]

    def foreign_keys(self, side, db):
        return []

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("v", "text")]

    def neutral_key(self, side, db, table):
        return ["id"]

    def neutral_read(self, side, db, table, columns, after=None,
                     limit=1000, where=None):
        start = (after[0] if after else 0) + 1
        rows = [[i, f"v{i}"] for i in range(start, min(start + limit, 2001))]
        return rows, ((rows[-1][0],) if rows else None)


def test_a_share_is_the_same_rows_every_run():
    from migkit import subset
    from migkit.config import Endpoint, Hop
    hop = Hop(name="s", engine="postgres", source=Endpoint(),
              target=Endpoint(), goal="subset",
              options={"subset": {"start": {"t": "10%"}}})
    a = subset.plan(hop, _Source(), "app")
    b = subset.plan(hop, _Source(), "app")
    assert sorted(a.rows["public.t"]) == sorted(b.rows["public.t"])
    # a tenth of the rows, by the keys' hash: near 200, never all or none
    assert 120 < len(a.rows["public.t"]) < 280, len(a.rows["public.t"])
    first = subset.plan(Hop(name="s", engine="postgres", source=Endpoint(),
                            target=Endpoint(), goal="subset",
                            options={"subset": {"start": {"t": 7}}}),
                        _Source(), "app")
    assert sorted(int(k[0]) for k in first.rows["public.t"]) == \
        list(range(1, 8))
    # past its bound the plan stops, and says by how much
    with pytest.raises(SystemExit, match="more than 100 rows"):
        subset.plan(Hop(name="s", engine="postgres", source=Endpoint(),
                        target=Endpoint(), goal="subset",
                        options={"subset": {"start": {"t": "all"},
                                            "most": 100}}),
                    _Source(), "app")


def test_the_goal_needs_its_rows_chosen():
    from migkit import hopkeys
    base = {"source": "postgres://app@10.0.0.1/shop",
            "target": "postgres://app@10.0.0.2/shop"}
    got = [(p.level, p.where) for p in hopkeys.check_hop(
        "dev", {**base, "goal": "subset"})]
    assert ("error", "hops.dev.options.subset") in got
    got = [(p.level, p.where) for p in hopkeys.check_hop(
        "dev", {**base, "goal": "subset", "options": {
            "subset": {"start": {"orders": "id <= 10"}},
            "reverse": "at_cutover"}})]
    assert ("error", "hops.dev.options.reverse") in got
    assert ("error", "hops.dev.options.subset") not in got
    got = [(p.level, p.where) for p in hopkeys.check_hop(
        "dev", {**base, "options": {"subset": {"start": {"t": 5}}}})]
    assert ("warn", "hops.dev.options.subset") in got


# --- PostgreSQL into PostgreSQL ------------------------------------------------

def _sql(name, sql, db="shop"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create table customers (id int primary key, email text not null,
                        phone text);
create table products (id int primary key, name text);
create table orders (id int primary key,
                     customer_id int not null references customers,
                     created date);
create table items (id int primary key,
                    order_id int not null references orders,
                    product_id int not null references products, qty int);
create table audit (id int primary key, note text);
insert into customers select i, 'c' || i || '@example.com', '555-' || i
  from generate_series(1, 100) i;
insert into products select i, 'p' || i from generate_series(1, 50) i;
insert into orders select i, (i % 100) + 1, date '2026-01-01' + i
  from generate_series(1, 500) i;
insert into items select i, (i % 500) + 1, (i % 50) + 1, i % 7
  from generate_series(1, 2000) i;
insert into audit select i, 'n' || i from generate_series(1, 10) i;
"""


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16"],
                           check=True, capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"], capture_output=True
                                  ).returncode == 0:
                    break
                time.sleep(1)
            time.sleep(2)
        _sql(SRC[0], "create database shop", "postgres")
        _sql(SRC[0], SEED)
        for db in ("shop", "shop_kids", "shop_big"):
            _sql(DST[0], f"create database {db}", "postgres")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def conf(pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = ("{{host: 127.0.0.1, port: {port}, user: postgres,"
            " password: test}}")
    path.write_text(
        "hops:\n"
        "  dev:\n    engine: postgres\n    goal: subset\n"
        f"    source: {side.format(port=SRC[1])}\n"
        f"    target: {side.format(port=DST[1])}\n"
        "    databases: [shop]\n"
        "    options:\n"
        "      subset: {start: {orders: 'id <= 10'}}\n"
        "      mask_on_write: {customers.email: constant"
        " masked@example.com, customers.phone: 'null'}\n"
        "  kids:\n    engine: postgres\n    goal: subset\n"
        f"    source: {side.format(port=SRC[1])}\n"
        f"    target: {side.format(port=DST[1])}\n"
        "    databases: [shop]\n    db_map: {shop: shop_kids}\n"
        "    options:\n"
        "      subset: {start: {products: 'id = 1'}, children: true}\n"
        "  big:\n    engine: postgres\n    goal: subset\n"
        f"    source: {side.format(port=SRC[1])}\n"
        f"    target: {side.format(port=DST[1])}\n"
        "    databases: [shop]\n    db_map: {shop: shop_big}\n"
        "    options:\n"
        "      subset: {start: {products: 'id = 1'}, children: true,"
        " most: 30}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    return CliRunner().invoke(cli.main, list(argv))


def _count(table, db="shop"):
    return int(_sql(DST[0], f"select count(*) from {table}", db) or 0)


@needs_docker
def test_the_plan_writes_nothing(conf):
    got = _run("move", "dev", "--mode", "full")
    assert got.exit_code == 0, got.output
    out = " ".join(got.output.split())
    assert "public.orders: 10 rows chosen - where id <= 10" in out, out
    assert "public.customers: 10 rows in the subset (referred to by" \
        " public.orders)" in out, out
    assert "masked on the way - email a constant, phone null" in out, out
    assert "dry-run: 20 rows in 2 tables would be written" in out, out
    assert _sql(DST[0], "select count(*) from pg_tables where schemaname ="
                        " 'public'") == "0"


@needs_docker
def test_ten_orders_bring_their_customers_masked_and_nothing_else(conf):
    got = _run("move", "dev", "--mode", "full", "--go")
    assert got.exit_code == 0, got.output
    assert "20 rows written and proved" in got.output, got.output
    assert _count("orders") == 10 and _count("customers") == 10
    for table in ("products", "items", "audit"):
        assert _count(table) == 0, table
    assert _sql(DST[0], "select string_agg(distinct email, ',') ||"
                        " '|' || count(phone) from customers") == \
        "masked@example.com|0"
    # the customers are the ones the orders name
    assert _sql(DST[0], "select count(*) from orders o left join customers"
                        " c on c.id = o.customer_id where c.id is null") == \
        "0"
    checked = _run("check", "dev")
    assert checked.exit_code == 0, checked.output
    out = " ".join(checked.output.split())
    assert "shop.public.customers: OK 10 rows, each as the source's," \
        " email, phone masked as the hop says" in out, out
    assert "shop references: OK all 10 references among the subset's" \
        " rows resolve inside it" in out, out


@needs_docker
def test_the_check_says_what_the_copy_no_longer_holds(conf):
    assert _run("move", "dev", "--mode", "full", "--go").exit_code == 0
    # a customer gone behind the check's back, and a masked value put back
    _sql(DST[0], "alter table orders drop constraint"
                 " orders_customer_id_fkey")
    _sql(DST[0], "delete from customers where id = (select min(id) from"
                 " customers)")
    _sql(DST[0], "update customers set email = 'c5@example.com' where id ="
                 " (select max(id) from customers)")
    got = _run("check", "dev")
    assert got.exit_code != 0, got.output
    out = " ".join(got.output.split())
    assert "1 rows missing on the target" in out, out
    assert "1 rows differ from the source's as it is now" in out, out
    assert "the target holds 9 rows and the subset 10" in out, out
    assert "references in the copy name no row of it" in out, out


@needs_docker
def test_children_close_over_what_refers_to_the_rows_taken(conf):
    got = _run("move", "kids", "--mode", "full", "--go")
    assert got.exit_code == 0, got.output
    counts = {t: _count(t, "shop_kids") for t in ("products", "items",
                                                  "orders", "customers")}
    # product 1, the 40 items naming it, their 10 orders and the 2
    # customers of those - whose own orders are those same 10
    assert counts == {"products": 1, "items": 40, "orders": 10,
                      "customers": 2}, counts
    # the masking of another hop is not this one's: values as they are
    assert _sql(DST[0], "select count(*) from customers where email like"
                        " 'c%@example.com'", "shop_kids") == "2"
    assert _run("check", "kids").exit_code == 0


@needs_docker
def test_a_subset_past_its_bound_writes_nothing(conf):
    got = _run("move", "big", "--mode", "full", "--go")
    assert got.exit_code != 0
    assert "would hold more than 30 rows" in got.output, got.output
    assert _sql(DST[0], "select count(*) from pg_tables where schemaname ="
                        " 'public'", "shop_big") == "0"


@needs_docker
def test_a_subset_is_not_synced(conf):
    got = _run("sync", "dev", "--apply")
    assert got.exit_code != 0 and "goal subset" in got.output, got.output


def test_a_subset_is_not_followed_or_taken_a_table_alone():
    from migkit import subset
    from migkit.config import Endpoint, Hop
    hop = Hop(name="s", engine="postgres", source=Endpoint(),
              target=Endpoint(), goal="subset",
              options={"subset": {"start": {"t": 5}}})
    # refused before any server is asked: there is no engine to ask
    with pytest.raises(SystemExit, match=r"taken once \(--mode full\) and"
                                         " not followed"):
        subset.move(hop, None, ["app"], "full+cdc", "", True, print)
    with pytest.raises(SystemExit, match="Leave out --table"):
        subset.move(hop, None, ["app"], "full", "public.t", True, print)
