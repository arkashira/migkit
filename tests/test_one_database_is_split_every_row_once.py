"""One database into many (backlog P0-SCENARIOS): each part's rows by its
rule, every row placed exactly once - counted on the source before the
move, proved on the parts' targets after it - and the references a split
cuts said.

The parts' rules, the SQL that counts a row against them and the hop's
words run without a server; the rest splits one PostgreSQL 16 database
by region into two.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import split
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-split-src", 16466), ("migkit-test-split-dst",
                                             16467)


def _part(name, where=None, exclude=(), opts=None, host="10.0.0.1"):
    return Hop(name=name, engine="postgres",
               source=Endpoint(host=host, port=5432),
               target=Endpoint(host="10.0.0.9", port=5432), goal="split",
               databases=["shop"], exclude=list(exclude),
               mapping={"where": dict(where or {})},
               options={"split": dict(opts or {})})


def test_a_row_is_counted_against_every_parts_rule():
    eu = split.Part(_part("eu", {"orders": "region = 'eu'"}))
    us = split.Part(_part("us", {"orders": "region = 'us'"},
                          exclude=["audit"]))
    got = split._taken([eu, us], "shop", "orders")
    assert got == ("(case when (region = 'eu') then 1 else 0 end) +"
                   " (case when (region = 'us') then 1 else 0 end)")
    # a part taking the table whole counts every row once; one leaving it,
    # not at all
    assert split._taken([eu, us], "shop", "audit") == "1"
    assert split._taken([us], "shop", "audit") == "0"


def test_the_parts_hold_together_or_are_refused():
    a = _part("a", opts={"with": ["b", "c", "d", "e"]})
    b = _part("b", opts={"with": ["a"]})
    c = Hop(name="c", engine="postgres", source=Endpoint(host="10.0.0.1",
                                                        port=5432),
            target=Endpoint(), goal="copy", databases=["shop"])
    e = _part("e", host="10.0.0.2")
    with pytest.raises(SystemExit) as got:
        split.parts(a, {"a": a, "b": b, "c": c, "e": e})
    said = str(got.value)
    assert "with: c's goal is not split" in said
    assert "with: d is not a hop the configuration has" in said
    assert "with: e reads another source" in said
    assert "with: b" not in said
    a = _part("a", opts={"with": ["b"]})
    assert [p.name for p in split.parts(a, {"a": a, "b": b})] == ["a", "b"]


def test_the_hop_says_what_stays_and_what_is_shared():
    got = split.settings(_part("a", opts={"shared": ["countries"],
                                          "rest": "stays"}))
    assert got == {"with": [], "shared": ["countries"], "rest": "stays"}
    with pytest.raises(SystemExit):
        split.settings(_part("a", opts={"rest": "somewhere"}))
    assert split._shared(_part("a", opts={"shared": ["countries"]}),
                         "public.countries")
    assert not split._shared(_part("a", opts={"shared": ["countries"]}),
                             "public.orders")


def test_a_row_nowhere_or_twice_is_refused_unless_said(monkeypatch):
    rows = [split.Placed("public.orders", 10, 2, 0, [(7,), (9,)], [], False),
            split.Placed("public.countries", 3, 0, 3, [], [(1,)], True),
            split.Placed("public.customers", 5, 0, 1, [], [(4,)], False)]
    monkeypatch.setattr(split, "placement", lambda *a, **k: rows)
    monkeypatch.setattr(split, "parts", lambda hop, siblings=None: [])
    got = split.problems(_part("a"), None, "shop")
    assert any("public.orders: 2 of its 10 rows no part takes (by key:"
               " (7), (9))" in w for w in got), got
    assert any("public.customers: 1 rows more than one part takes (by"
               " key: (4))" in w for w in got), got
    # every part taking a shared table whole is what it is for
    assert not any("public.countries" in w for w in got), got
    got = split.problems(_part("a", opts={"rest": "stays"}), None, "shop")
    assert not any("no part takes" in w for w in got), got
    assert any("more than one part takes" in w for w in got), got


# --- one PostgreSQL database by region into two -----------------------------------

def _sql(name, sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create table countries (code text primary key, name text);
insert into countries values ('fr', 'France'), ('us', 'United States');
create table customers (id int primary key, region text, name text);
insert into customers values (1, 'eu', 'a'), (2, 'eu', 'b'), (3, 'us', 'c'),
                             (4, 'apac', 'd');
create table orders (id int primary key, customer_id int references
                     customers, region text);
insert into orders values (1, 1, 'eu'), (2, 2, 'eu'), (3, 3, 'us'),
                          (4, 3, 'eu');
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
        _sql(SRC[0], "create database shop")
        _sql(SRC[0], SEED, "shop")
        for db in ("shop_eu", "shop_us", "shop_all"):
            _sql(DST[0], f"create database {db}")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _conf(tmp_path, monkeypatch, rest):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = ("{{host: 127.0.0.1, port: {port}, user: postgres,"
            " password: test}}")
    head = ("    engine: postgres\n    goal: split\n"
            f"    source: {side.format(port=SRC[1])}\n"
            f"    target: {side.format(port=DST[1])}\n"
            "    databases: [shop]\n")
    split_ = f"{{shared: [countries], rest: {rest}"
    path.write_text(
        "hops:\n"
        "  eu:\n" + head + "    db_map: {shop: shop_eu}\n"
        "    mapping:\n      where: {customers: \"region = 'eu'\","
        " orders: \"region = 'eu'\"}\n"
        f"    options:\n      split: {split_}, with: [us]}}\n"
        "  us:\n" + head + "    db_map: {shop: shop_us}\n"
        "    mapping:\n      where: {customers: \"region = 'us'\","
        " orders: \"region = 'us'\"}\n"
        f"    options:\n      split: {split_}, with: [eu]}}\n"
        "  both:\n" + head + "    db_map: {shop: shop_all}\n"
        "    mapping:\n      where: {customers: \"region in ('eu', 'us')\","
        " orders: \"region in ('eu', 'us')\"}\n"
        f"    options:\n      split: {split_}, with: [us]}}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


@needs_docker
def test_rows_no_part_takes_stop_the_split(pair, tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch, "refuse")
    got, out = _run("move", "eu", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "nothing was written" in out, out
    assert "public.customers: 1 of its 4 rows no part takes (by key:" \
        " (4))" in out, out


@needs_docker
def test_rows_two_parts_take_stop_the_split(pair, tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch, "stays")
    got, out = _run("move", "both", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "public.customers: 1 rows more than one part takes (by key:" \
        " (3))" in out, out


@needs_docker
def test_each_row_in_exactly_one_part_proved(pair, tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch, "stays")
    got, out = _run("assess", "eu")
    out = " ".join(got.output.split())
    # an order of the eu part naming a customer the us part takes
    assert "public.orders -> public.customers in eu" in out, out
    assert "1 rows of public.orders eu takes name by (customer_id) a row" \
        " of public.customers eu does not take" in out, out
    for hop in ("eu", "us"):
        got, out = _run("move", hop, "--mode", "full", "--go")
        assert got.exit_code == 0, out
    got, out = _run("check", "eu")
    assert "shop.public.customers: OK 3 rows, each in exactly one part's" \
        " target of 2" in out, out
    assert "shop.public.countries: OK 2 rows, each in every part's target" \
        " (shared)" in out, out
    # a row put in the other part's target too is placed twice
    _sql(DST[0], "insert into customers values (1, 'eu', 'a')", "shop_us")
    got, out = _run("check", "eu")
    assert got.exit_code != 0, out
    assert "1 keys in more than one part's target" in out, out
