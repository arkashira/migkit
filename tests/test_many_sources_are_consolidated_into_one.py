"""Many databases or tenants into one (backlog P0-SCENARIOS): a tenant
column filled per source, keys two sources hold found before the move and
refused or kept apart by rule, counters raised past every source's keys,
every source's rows proved in the target under its value.

The rule's reading, the group's refusals and the choice of how keys are
kept apart run without a server; the rest runs three PostgreSQL 16
databases into one, across two hops.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import consolidate
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-consolidate-src", 16464), (
    "migkit-test-consolidate-dst", 16465)


def _hop(name="a", dbs=("x",), opts=None, host="10.0.0.9"):
    return Hop(name=name, engine="postgres", source=Endpoint(host="10.0.0.1",
                                                            port=5432),
               target=Endpoint(host=host, port=5432), goal="consolidate",
               databases=list(dbs), db_map={d: "all" for d in dbs},
               options={"consolidate": {"column": "tenant_id",
                                        **(opts or {})}})


def test_the_rule_is_read_as_it_is_written():
    got = consolidate.settings(_hop(opts={"keys": "offset", "offsets":
                                          {"eu": 0, "us": "1000"}}))
    assert got["keys"] == "offset" and got["offsets"] == {"eu": 0,
                                                          "us": 1000}
    for wrong in ({"keys": "merge"}, {"offsets": {"eu": "many"}}):
        with pytest.raises(SystemExit):
            consolidate.settings(_hop(opts=wrong))
    plain = Hop(name="p", engine="postgres", source=Endpoint(),
                target=Endpoint(), goal="copy")
    assert consolidate.settings(plain) is None


def test_each_source_has_a_value_of_its_own():
    one = _hop(dbs=("shop_eu", "shop_us"), opts={
        "values": {"shop_eu": "eu"}, "value": "all"})
    got = {s.db: s.value for s in consolidate.sources(one, one.databases)}
    assert got == {"shop_eu": "eu", "shop_us": "all"}
    by_name = _hop(dbs=("shop_eu", "shop_us"))
    got = {s.db: s.value for s in consolidate.sources(by_name,
                                                      by_name.databases)}
    assert got == {"shop_eu": "shop_eu", "shop_us": "shop_us"}
    same = _hop(dbs=("a", "b"), opts={"value": "eu"})
    with pytest.raises(SystemExit, match="each source needs a value of its"
                                         " own"):
        consolidate.sources(same, same.databases)


def test_the_hops_it_names_hold_together_or_are_refused():
    a = _hop("a", ("x",), {"value": "eu", "with": ["b", "c", "d", "e"]})
    b = _hop("b", ("y",), {"value": "us", "with": ["a"]})
    c = Hop(name="c", engine="postgres", source=Endpoint(),
            target=Endpoint(), goal="copy")
    d = _hop("d", ("z",), {"value": "ap", "keys": "tenant"})
    e = _hop("e", ("w",), {"value": "sa"}, host="10.0.0.8")
    with pytest.raises(SystemExit) as got:
        consolidate.sources(a, ["x"], {"a": a, "b": b, "c": c, "d": d,
                                       "e": e})
    said = str(got.value)
    assert "with: c's goal is not consolidate" in said
    assert "with: d holds its rows under 'tenant_id' by keys: tenant" in said
    assert "with: e writes another server" in said
    # the hop that holds together is not among the reasons
    assert "with: b" not in said
    a = _hop("a", ("x",), {"value": "eu", "with": ["b"]})
    ok = consolidate.sources(a, ["x"], {"a": a, "b": b})
    assert [(s.hop.name, s.db, s.value) for s in ok] == [
        ("a", "x", "eu"), ("b", "y", "us")]


def test_the_way_keys_are_kept_apart_is_chosen_by_rule_and_key(monkeypatch):
    monkeypatch.setattr(consolidate, "_integer_key",
                        lambda pair, db, t: "id" if t == "ints" else None)

    def way(rule, table, clash):
        hop = _hop(opts={"keys": rule, "offsets": {"x": 5}})
        return consolidate.key_way(hop, None, "x", table, clash)
    assert way("tenant", "ints", True).path == "tenant"
    assert way("offset", "ints", True).path == "offset"
    assert way("remap", "ints", False).path == "offset"
    # a text key under an offset rule: kept as it is where no two sources
    # hold one, refused where they do - with why every way could not
    assert way("offset", "names", False).path == "apart"
    got = way("offset", "names", True)
    assert got.rung is None
    assert "only a whole number is shifted" in got.reason
    assert "sources hold the same keys in it" in got.reason
    got = way("refuse", "ints", True)
    assert got.rung is None and "the hop's rule is not tenant" in got.reason
    assert way("refuse", "ints", False).path == "apart"


def test_a_shifted_key_and_its_references_move_together():
    p = consolidate.TablePlan("orders", "orders", ["id"], "offset",
                              {"customer_id": "customers"})
    got = consolidate._shift([7, 3, None], ["id", "customer_id", "note"], p,
                             {"orders": 100, "customers": 1000})
    assert got == [107, 1003, None]
    tenant = consolidate.TablePlan("orders", "orders", ["id"], "tenant")
    assert consolidate._shift([7, 3], ["id", "customer_id"], tenant,
                              {"orders": 0}) == [7, 3]


# --- three PostgreSQL databases into one ------------------------------------------

def _sql(name, sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create table customers (id int generated by default as identity primary key,
                        name text);
create table orders (id int generated by default as identity primary key,
                     customer_id int references customers, total
                     numeric(10,2));
insert into customers (name) values ('a'), ('b'), ('c');
insert into orders (customer_id, total) values (1, 10), (2, 20), (3, 30);
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
        for db in ("shop_eu", "shop_us", "shop_ap"):
            _sql(SRC[0], f"create database {db}")
            _sql(SRC[0], SEED, db)
        for db in ("shop", "shop_r", "shop_o"):
            _sql(DST[0], f"create database {db}")
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
    head = ("    engine: postgres\n    goal: consolidate\n"
            f"    source: {side.format(port=SRC[1])}\n"
            f"    target: {side.format(port=DST[1])}\n")
    path.write_text(
        "hops:\n"
        "  eu-us:\n" + head + "    databases: [shop_eu, shop_us]\n"
        "    db_map: {shop_eu: shop, shop_us: shop}\n"
        "    options:\n      consolidate: {column: tenant_id, values:"
        " {shop_eu: eu, shop_us: us}, keys: tenant, with: [ap]}\n"
        "  ap:\n" + head + "    databases: [shop_ap]\n"
        "    db_map: {shop_ap: shop}\n"
        "    options:\n      consolidate: {column: tenant_id, value: ap,"
        " keys: tenant, with: [eu-us]}\n"
        "  refused:\n" + head + "    databases: [shop_eu, shop_us]\n"
        "    db_map: {shop_eu: shop_r, shop_us: shop_r}\n"
        "    options:\n      consolidate: {column: tenant_id}\n"
        "  remapped:\n" + head + "    databases: [shop_eu, shop_us]\n"
        "    db_map: {shop_eu: shop_o, shop_us: shop_o}\n"
        "    options:\n      consolidate: {column: tenant_id, keys:"
        " remap}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


@needs_docker
def test_keys_two_sources_hold_are_found_before_the_move(conf):
    got, out = _run("move", "refused", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "nothing was written" in out, out
    assert "3 keys held by more than one source ((1) in refused/shop_eu" \
        " and refused/shop_us" in out, out
    assert _sql(DST[0], "select count(*) from pg_tables where schemaname ="
                        " 'public'", "shop_r") == "0"


@needs_docker
def test_three_sources_into_one_under_their_values_proved(conf):
    got, out = _run("move", "eu-us", "--mode", "full")
    assert got.exit_code == 0, out
    assert "a consolidation: 3 sources into shop" in out, out
    for hop in ("eu-us", "ap"):
        got, out = _run("move", hop, "--mode", "full", "--go")
        assert got.exit_code == 0, out
    assert _sql(DST[0], "select string_agg(tenant_id || ':' || id, ','"
                        " order by tenant_id, id) from customers",
                "shop") == "ap:1,ap:2,ap:3,eu:1,eu:2,eu:3,us:1,us:2,us:3"
    got, out = _run("check", "eu-us")
    assert got.exit_code == 0, out
    assert "shop_eu.public.customers tenant_id=eu: OK 3 rows as the" \
        " source's" in out, out
    assert "shop_us.public.orders tenant_id=us: OK 3 rows" in out, out
    assert "every row on the target is under one of the 3 sources'" \
        " values" in out, out
    # a row of no source, and a source's row changed, are named
    _sql(DST[0], "insert into customers (tenant_id, id, name) values"
                 " ('zz', 9, 'x'); update customers set name = 'B' where"
                 " tenant_id = 'eu' and id = 2", "shop")
    got, out = _run("check", "eu-us")
    assert got.exit_code != 0, out
    assert "1 rows on the target are under a tenant_id no source of the" \
        " consolidation has" in out, out
    assert "1 rows differ ((2))" in out, out


@needs_docker
def test_keys_remapped_apart_with_their_references_and_counters(conf):
    got, out = _run("move", "remapped", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    assert _sql(DST[0], "select string_agg(id::text, ',' order by id) from"
                        " customers", "shop_o") == "1,2,3,101,102,103"
    # every order names its own source's customer, shifted with it
    assert _sql(DST[0], "select count(*) from orders o join customers c on"
                        " c.id = o.customer_id and c.tenant_id ="
                        " o.tenant_id", "shop_o") == "6"
    # the counters are past every source's keys
    assert _sql(DST[0], "insert into customers (tenant_id, name) values"
                        " ('new', 'n') returning id", "shop_o") == "104"
    got, out = _run("check", "remapped")
    assert "keys shifted by 100" in out, out
