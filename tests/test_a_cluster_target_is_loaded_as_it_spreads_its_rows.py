"""A reshard whose one target is a cluster loads it as the cluster spreads
its rows (backlog W7): the cluster's own distribution read - Citus's
distribution column and shard placements, MongoDB's shard key and chunks,
Cassandra's partitioner token, Vitess's vindex - each row's owner asked of
the cluster's own function, each batch sent to the node that owns its rows
where migkit reaches it and through the cluster's router where it does
not, and every part read back on its node and held to the cluster's
function.

The chunk ranges, the values a token is asked with, the climb of a
table's way and the batches' grouping run on stand-ins; the rest moves a
PostgreSQL 16 database into a single-node Citus that holds its own shards,
writing each shard's rows on the node directly.
"""
import subprocess
import time
from decimal import Decimal

import pytest
from click.testing import CliRunner

from migkit import decide, reshard
from migkit.config import Endpoint, Hop
from migkit.engines.base import Spread
from tests.conftest import needs_docker

PROVES = [("move.spread", "postgres"), ("move.spread", "hetero")]

SRC, DST = ("migkit-test-spread-src", 16532), ("migkit-test-spread-citus",
                                              16533)


# --- MongoDB's chunks ----------------------------------------------------------------

class MinKey:
    pass


class MaxKey:
    pass


def test_a_documents_chunk_is_found_by_its_keys_value():
    from migkit.engines.mongodb import chunk_of
    lo, hi = MinKey(), MaxKey()
    numbers = [(lo, 10, 0), (10, 100, 1), (100, hi, 0)]
    assert [chunk_of(str(v), numbers) for v in (-5, 9, 10, 99, 100, 10**9)] \
        == [0, 0, 1, 1, 0, 0]
    # a key's value that is not a number among numeric bounds: not placed
    assert chunk_of("abc", numbers) is None
    texts = [(lo, "g", 1), ("g", "p", 0), ("p", hi, 2)]
    assert [chunk_of(v, texts) for v in ("a", "g", "o", "p", "zz")] == \
        [1, 0, 0, 2, 2]
    # bounds of two types are ordered by type first: the router's
    assert chunk_of("5", [(lo, 10, 0), (10, "m", 1), ("m", hi, 2)]) is None
    # one chunk holds every key
    assert chunk_of("anything", [(lo, hi, 3)]) == 3
    assert chunk_of(None, numbers) is None


def test_a_cassandra_key_is_bound_as_its_column_takes_it():
    import datetime
    import uuid

    from migkit.engines.cassandra import CassandraEngine
    v = CassandraEngine._cql_value
    assert v("int", "42") == 42 and v("bigint", "-7") == -7
    assert v("decimal", "1.50") == Decimal("1.50")
    assert v("uuid", "6f1c2c4e-1d0a-4bde-9b5e-0c1f4c3c2a11") == uuid.UUID(
        "6f1c2c4e-1d0a-4bde-9b5e-0c1f4c3c2a11")
    assert v("date", "2026-10-04") == datetime.date(2026, 10, 4)
    assert v("boolean", "true") is True and v("text", "x") == "x"
    assert v("int", None) is None


# --- the way a table is placed, and the batches -----------------------------------

class _Side:
    CANON_ENGINE = "postgres"

    def __init__(self, spread=None):
        self.spread = spread

    def neutral_key(self, side, db, table):
        return ["id"]

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("customer_id", "integer"),
                ("total", "numeric")]

    def distribution(self, side, db, table):
        return self.spread


class _Pair:
    def __init__(self, spread=None):
        self.src_engine = _Side()
        self.dst_engine = _Side(spread)

    def _mapped_types(self, db, table, types):
        return dict(types), {k: k for k in types}


def _group(spread, by="auto", targets=1):
    hop = Hop(name="r", engine="postgres", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.9"), goal="reshard",
              databases=["shop"], options={"reshard": {"by": by}})
    pair = _Pair(spread)
    g = reshard.Group("reshard", hop, [hop],
                      [reshard.Side("r", hop, "shop")],
                      [reshard.Side(f"t{j}", hop, "shop")
                       for j in range(targets)], reshard.settings(hop))
    for j in range(targets):
        g._pairs[(0, j)] = pair
    return g


def _spread(reference=False, address=None):
    return Spread(["customer_id"], "the cluster's hash of customer_id",
                  [("10.0.0.21:5432", "public.orders_1"),
                   ("10.0.0.22:5432", "public.orders_2")],
                  lambda texts: [int(t[0]) % 2 if t else None
                                 for t in texts], reference, address)


def test_a_table_the_cluster_spreads_is_placed_by_the_clusters_own_way(
        monkeypatch):
    monkeypatch.setattr(reshard, "_target_table", lambda pair, db, t: t)
    fn, choice = reshard.function(_group(_spread()), "public.orders")
    assert fn.way == "owned" and fn.key == ["customer_id"], choice
    assert fn.said == "the cluster's hash of customer_id"
    # every row goes to the one target: the cluster places it there
    assert fn.place(("7",)) == 0
    fn, _ = reshard.function(_group(_spread(reference=True)),
                             "public.countries")
    assert fn.way == "whole"
    # a target that spreads nothing: the hop's own function
    fn, choice = reshard.function(_group(None), "public.orders")
    assert fn.way == "hash", choice
    # the hop's own function where it names one, the cluster's word aside
    fn, _ = reshard.function(_group(_spread(), by="hash"), "public.orders")
    assert fn.way == "hash"
    # by: target and no spread: no way, and the climb says why
    fn, choice = reshard.function(_group(None, by="target"),
                                  "public.orders")
    assert fn is None
    assert "does not spread the table" in choice.reason, choice.reason
    assert decide.cell("postgres", "spread target")[0] == "yes"
    assert decide.cell("sqlite", "spread target")[0] == \
        decide.NOT_APPLICABLE


def test_each_batch_goes_to_the_node_owning_its_rows_or_the_router(
        monkeypatch, tmp_path):
    sent, routed = [], []

    class _Node:
        def __init__(self, k, fails):
            self.k, self.fails = k, fails
            outer = self

            class _Dst:
                def neutral_apply(self, side, db, recs):
                    if outer.fails:
                        raise OSError("connection refused")
                    sent.append((outer.k, [r["table"] for r in recs],
                                 [r["key"] for r in recs]))
            self.dst_engine = _Dst()
    nodes = {0: _Node(0, False), 1: _Node(1, True)}
    monkeypatch.setattr(reshard, "_node_pair",
                        lambda g, i, k, sp: nodes[k])
    monkeypatch.setattr(reshard, "_write",
                        lambda g, i, j, table, cols, rows, log:
                        routed.append([r[0] for r in rows]) or len(rows))
    monkeypatch.setattr(reshard, "_target_table", lambda pair, db, t: t)
    g = _group(_spread(address=lambda k: ("10.0.0.2" + str(k), 5432)))
    fn, _ = reshard.function(g, "public.orders")
    cols = [("customer_id", "integer"), ("id", "integer"),
            ("total", "decimal")]
    rows = [[c, 100 + c, Decimal(1)] for c in range(6)]
    said = []
    n = reshard._write_spread(g, 0, "public.orders", fn, cols, cols, rows,
                              said.append, tmp_path / "copy.json")
    assert n == 6
    # node 0 owns the even customers and takes them itself
    assert sent == [(0, ["public.orders_1"] * 3,
                     [{"id": 100}, {"id": 102}, {"id": 104}])]
    # node 1 cannot be reached: its rows through the router, said once
    assert routed == [[1, 3, 5]]
    assert any("switching to through the cluster's router" in s
               for s in said), said


# --- PostgreSQL into a Citus that holds its own shards ----------------------------

def _sql(name, sql, db="postgres", port=None):
    env = ["-e", "PGPASSWORD=test"] + (["-e", f"PGPORT={port}"] if port
                                       else [])
    got = subprocess.run(["docker", "exec", *env, name, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create table countries (code text primary key, name text);
insert into countries values ('fr', 'France'), ('us', 'United States');
create table orders (id int not null, customer_id int not null,
                     total numeric(10, 2), primary key (customer_id, id));
insert into orders select g, g % 23, g * 1.5 from generate_series(1, 400) g;
"""
CITUS = """
select citus_set_coordinator_host('127.0.0.1', %d);
select citus_set_node_property('127.0.0.1', %d, 'shouldhaveshards', true);
create table countries (code text primary key, name text);
select create_reference_table('countries');
create table orders (id int not null, customer_id int not null,
                     total numeric(10, 2), primary key (customer_id, id));
select create_distributed_table('orders', 'customer_id', shard_count := 8);
""" % (DST[1], DST[1])


def _up(name, port=None):
    for _ in range(90):
        args = ["docker", "exec", name, "pg_isready", "-U", "postgres"]
        if port:
            args[3:3] = ["-e", f"PGPORT={port}"]
        if subprocess.run(args, capture_output=True).returncode == 0:
            return
        time.sleep(1)
    raise AssertionError(f"{name} did not start")


@pytest.fixture(scope="module")
def citus():
    try:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", SRC[0], "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{SRC[1]}:5432", "postgres:16"],
                       check=True, capture_output=True)
        # the node listens on the port it is reached at from here, so the
        # address Citus names it by is one migkit reaches it at
        subprocess.run(["docker", "run", "-d", "--name", DST[0], "-e",
                        "POSTGRES_PASSWORD=test", "-e", f"PGPORT={DST[1]}",
                        "-p", f"127.0.0.1:{DST[1]}:{DST[1]}",
                        "citusdata/citus:12.1"], check=True,
                       capture_output=True)
        _up(SRC[0])
        _up(DST[0], DST[1])
        time.sleep(3)
        _sql(SRC[0], "create database shop")
        _sql(SRC[0], SEED, "shop")
        _sql(DST[0], CITUS, port=DST[1])
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _conf(tmp_path, monkeypatch):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = ("{{host: 127.0.0.1, port: {port}, user: postgres,"
            " password: test}}")
    path.write_text(
        "hops:\n"
        "  into-citus:\n    engine: postgres\n    goal: reshard\n"
        f"    source: {side.format(port=SRC[1])}\n"
        f"    target: {side.format(port=DST[1])}\n"
        "    databases: [shop]\n    db_map: {shop: postgres}\n"
        "    options:\n      target_rows: replace\n"
        "      reshard: {by: auto}\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


def _shards():
    """[(shard id, its table, its rows, its rows Citus's own function
    places in another shard)] read on the node."""
    got = _sql(DST[0], "select shardid, shard_name(logicalrelid, shardid)"
                       " from pg_dist_shard where logicalrelid ="
                       " 'orders'::regclass order by shardid", port=DST[1])
    out = []
    for line in got.splitlines():
        shard, name = line.split("|")
        rows = _sql(DST[0], "set citus.show_shards_for_app_name_prefixes ="
                            f" '*'; select count(*), count(*) filter (where"
                            " get_shard_id_for_distribution_column("
                            f"'orders', customer_id) <> {shard}) from"
                            f" {name}", port=DST[1])
        n, wrong = rows.splitlines()[-1].split("|")
        out.append((int(shard), name, int(n), int(wrong)))
    return out


@needs_docker
def test_rows_are_written_on_the_node_into_the_shard_that_owns_them(
        citus, tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch)
    got, out = _run("move", "into-citus", "--mode", "full", "--db", "shop",
                    "--go")
    assert got.exit_code == 0, out
    assert "the target cluster's own distribution" in out, out
    # every shard holds rows, and only those Citus places in it
    shards = _shards()
    assert sum(n for _, _, n, _ in shards) == 400, shards
    assert all(n for _, _, n, _ in shards), shards
    assert [w for _, _, _, w in shards] == [0] * len(shards), shards
    assert _sql(DST[0], "select count(*) from countries",
                port=DST[1]) == "2"
    got, out = _run("check", "into-citus", "--db", "shop")
    assert got.exit_code == 0, out
    assert "each in the part the cluster places it in" in out, out
    # a row written into a shard Citus would never look in: the proof of
    # the parts says so, though every row is there once
    first, last = shards[0][1], shards[-1][1]
    cust = int(_sql(DST[0], "select customer_id from orders where"
                            " get_shard_id_for_distribution_column("
                            f"'orders', customer_id) = {shards[0][0]}"
                            " limit 1", port=DST[1]))
    _sql(SRC[0], f"insert into orders values (9001, {cust}, 1)", "shop")
    _sql(DST[0], "set citus.enable_manual_changes_to_shards = on; insert"
                 f" into {last} values (9001, {cust}, 1)", port=DST[1])
    assert first != last
    got, out = _run("check", "into-citus", "--db", "shop")
    assert got.exit_code != 0, out
    assert "sit in a part the cluster's own function does not place" \
        in out, out
