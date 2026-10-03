"""Views and one-expression functions carried to another engine, and held
to what they answer (backlog 39).

A view or a function converted by a translator is a guess about meaning
until it has been asked the same question on both sides. Two of the
guesses the translator makes on this pair are wrong: MySQL's `length` is
bytes and PostgreSQL's is characters, and PostgreSQL ignores the scale a
function declares for its decimal arguments and result, where MySQL rounds
to it. The conversion states the scale in the body; the length is left to
the proof, which asks every converted object the same inputs on both sides
and says which ones answer differently.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.views", "mysql")]

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-code-my", "migkit-test-code-pg"
MY_PORT, PG_PORT = 16080, 16081


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


def _pg(sql):
    got = _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", "cx",
              "-At", "-v", "ON_ERROR_STOP=1", "-c", sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database="cx", autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
    finally:
        conn.close()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MY, PG):
        _sh("docker", "rm", "-f", "-v", n)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"{MY_PORT}:3306", "mysql:8.4")
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"{PG_PORT}:5432", "postgres:16")
    try:
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "create database if not exists cx").returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "create database cx").returncode == 0:
                break
            time.sleep(2)
        for port in (MY_PORT, PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _my("create table orders (id int primary key, amount decimal(10,2),"
            " cust varchar(20))",
            "insert into orders values (1, 5.00, 'Ann'), (2, 20.50, 'bob'),"
            " (3, 10.00, 'Céline'), (4, 99.99, null)",
            "create view big_orders as select id, amount from orders"
            " where amount > 10",
            # sorts before the view it reads: the order they are created in
            # has to come from what each reads, not from their names
            "create view big_ids as select id from big_orders",
            "create function add_tax(x decimal(10,2)) returns decimal(10,2)"
            " deterministic return x * 1.07",
            "create function name_len(s varchar(20)) returns int"
            " deterministic return length(s)",
            "create function shout(s varchar(20)) returns varchar(40)"
            " deterministic return concat(upper(s), '!')",
            "create function steps(x int) returns int deterministic"
            " begin declare y int; set y = x + 1; return y; end",
            "create procedure ping() begin select 1; end")
        yield
    finally:
        for n in (MY, PG):
            _sh("docker", "rm", "-f", "-v", n)


def _engine(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="code", engine="hetero",
              options={"source_engine": "mysql",
                       "target_engine": "postgres"},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _verdicts(tmp_path):
    import json
    got = json.loads((tmp_path / "code-proof.json").read_text())
    return {v["routine"]: v for v in got["verdicts"]}


def test_carried_in_order_and_the_rest_named(pair, tmp_path):
    stmts = _engine(tmp_path).convert_ddl("cx")
    heads = [s.split("(")[0].split(" as ")[0] for s in stmts]
    views = [h for h in heads if h.startswith("create view")]
    assert views == ["create view big_orders", "create view big_ids"], heads
    made = [s for s in stmts if s.startswith("create function")]
    assert sorted(s.split("(")[0] for s in made) == [
        'create function "add_tax"', 'create function "name_len"',
        'create function "shout"'], made
    # the first line says why; migkit's rules have a version of each, kept
    # under it commented out - no sandbox is named to prove it in, and
    # nothing unproved is applied
    named = [s for s in stmts if s.startswith("--")]
    assert sorted(s.splitlines()[0] for s in named) == [
        "-- function steps not converted: its body is statements, not one"
        " expression",
        "-- procedure ping not converted: its body is statements, not one"
        " expression"], named
    assert all(ln.startswith("--") for s in named for ln in s.splitlines()), \
        named
    assert "-- create function \"steps\"(x integer) returns integer" in \
        "\n".join(named), named


def test_the_proof_finds_the_translation_that_changed_meaning(pair,
                                                              tmp_path):
    eng = _engine(tmp_path)
    # nothing built on the target yet: said, not passed
    _pg("create table if not exists orders (id bigint primary key,"
        " amount numeric(10,2), cust varchar(20))")
    got = eng.prove_converted("cx")
    assert got.status == "warn", got.__dict__
    assert got.detail.startswith("7 missing;"), got.detail

    for s in eng.convert_ddl("cx"):
        if not s.startswith("--") and not s.startswith("create table"):
            eng.dst_engine.neutral_create_code("dst", "cx", s)
    _pg("insert into orders values (1, 5.00, 'Ann'), (2, 20.50, 'bob'),"
        " (3, 10.00, 'Céline'), (4, 99.99, null)")
    got = eng.prove_converted("cx")
    # length: bytes on one side, characters on the other, and only the
    # input wider than a byte tells them apart - and the verdict names it
    assert got.status == "diff", got.__dict__
    by = _verdicts(tmp_path)
    assert [n for n, v in by.items() if v["status"] == "diff"] == [
        "function name_len"], by
    assert by["function name_len"]["reason"] == (
        "called with (s='é'): returned '2' against '1'"), by

    # the one a person rewrites, and the two the translator could not
    # carry, written by hand: then everything answers the same, and what
    # a person wrote is held to the source as the rest is
    _pg("create or replace function name_len(s varchar) returns bigint"
        " language sql as $$ select octet_length(s) $$")
    got = eng.prove_converted("cx")
    assert got.status == "warn", got.__dict__
    by = _verdicts(tmp_path)
    assert sorted(n for n, v in by.items() if v["status"] == "missing") == \
        ["function steps", "procedure ping"], by
    # an INT stays 32 bits: the proof calls it with 2147483647, where
    # MySQL's `x + 1` fails and a bigint's would not
    _pg("create function steps(x integer) returns integer language plpgsql"
        " as $$ begin return x + 1; end $$;"
        " create procedure ping() language sql as $$ select 1 $$")
    got = eng.prove_converted("cx")
    # a procedure nobody ran is not the same as one that answered the same:
    # no sandbox is named here, so it is not proved, and the line says so
    assert got.status == "warn", got.__dict__
    by = _verdicts(tmp_path)
    assert by["procedure ping"]["status"] == "not proved", by
    assert by["function name_len"]["status"] == "ok", by
    # a function of statements answers the same on the hop's own servers,
    # read-only, but which of its statements ran is not measured there
    assert by["function steps"]["status"] == "warn", by
    assert "not measured: no sandbox is named" in by["function steps"]["reason"], by
    assert sum(v["status"] == "ok" for v in by.values()) == 5, by


def test_a_decimal_rounds_where_the_source_rounds(pair, tmp_path):
    eng = _engine(tmp_path)
    made = [s for s in eng.convert_ddl("cx") if '"add_tax"' in s]
    assert made and "AS DECIMAL(10, 2)" in made[0].upper(), made
    # without the scale said in the body, 1.555 goes in unrounded and the
    # answer carries four places: the proof has to see that
    _pg("create or replace function add_tax(x numeric) returns numeric"
        " language sql as $$ select x * 1.07 $$")
    try:
        got = eng.prove_converted("cx")
        assert got.status == "diff", got.__dict__
        by = _verdicts(tmp_path)
        assert [n for n, v in by.items() if v["status"] == "diff"] == [
            "function add_tax"], by
    finally:
        _pg("drop function add_tax(numeric)")
        eng.dst_engine.neutral_create_code("dst", "cx", made[0])
    eng.prove_converted("cx")
    assert _verdicts(tmp_path)["function add_tax"]["status"] == "ok"


def test_a_view_that_selects_other_rows_is_a_difference(pair, tmp_path):
    eng = _engine(tmp_path)
    _pg("create or replace view big_orders as select id, amount from orders"
        " where amount >= 10")
    try:
        got = eng.prove_converted("cx")
        assert got.status == "diff", got.__dict__
        by = _verdicts(tmp_path)
        assert sorted(n for n, v in by.items() if v["status"] == "diff") == \
            ["view big_ids", "view big_orders"], by
    finally:
        _pg("create or replace view big_orders as select id, amount from"
            " orders where amount > 10")
    eng.prove_converted("cx")
    by = _verdicts(tmp_path)
    assert by["view big_orders"]["status"] == "ok", by
    assert by["view big_ids"]["status"] == "ok", by


def test_the_deep_check_carries_the_proof(pair, tmp_path):
    got = [r for r in _engine(tmp_path).check_deep("cx")]
    mine = [r for r in got if r.scope == "cx converted code"]
    # the procedure nobody ran keeps the line from `ok`
    assert [r.status for r in mine] == ["warn"], [r.__dict__ for r in got]
    assert "procedure ping: not proved" in mine[0].detail, mine[0].detail
    # beside the checks that were there, not instead of them
    assert len(got) > 1, [r.__dict__ for r in got]


def test_the_schema_command_applies_them_on_the_target(pair, tmp_path,
                                                        monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    _pg("drop view big_ids; drop view big_orders;"
        " drop function shout(varchar)")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  code:\n    engine: hetero\n"
        "    options: {source_engine: mysql, target_engine: postgres}\n"
        f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {PG_PORT}, user: postgres,"
        " password: test}\n"
        "    databases: [cx]\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    _pg("drop table orders cascade")
    got = CliRunner().invoke(cli.main, ["schema", "code", "--convert",
                                        "--apply"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    assert "function steps not converted" in said, said
    assert "4 created on the target" in said, said
    # the one rewritten by hand is not overwritten by the conversion
    assert "already on the target, left as they are: add_tax, name_len" \
        in said, said
    assert "octet_length" in _pg("select prosrc from pg_proc where"
                                 " proname = 'name_len'")
    assert _pg("select string_agg(viewname, ',' order by viewname) from"
               " pg_views where schemaname = 'public'") == \
        "big_ids,big_orders"
    assert _pg("select shout('ab')") == "AB!"
