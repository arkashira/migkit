"""Resharding two shards into three on real PostgreSQL servers (backlog
W7, R3.topo): the lane's two servers are the two source shards - each one
database of the module's own name - and three databases on the second are
the new shards, placed by a hash of a column that is not the key.

Every row lands once, on the shard the function names; the follow runs
while both sources are written, a row whose shard column is updated is
taken off its old shard before it is written on its new one; each shard's
proof is keyed against the rows the function assigns it and names a row
planted on the wrong shard by its key and its shard; a stop between the
removal from the old shard and the write on the new one is resumed to one
copy; the cutover holds every source's writes and drains the follow past
every source's position before the proof across the shards.

Only the PostgreSQL lane: MySQL and the warehouses run the same code, and
their lane is owed.
"""
import threading

import pytest
from click.testing import CliRunner

from migkit import decide, failpoint, reshard, tailctl
from tests import servers
from tests.conftest import needs_docker
from tests.waits import until

pair = servers.fixture("pg")

N = 3
SEED = """
create table countries (code text primary key, name text);
insert into countries values ('fr', 'France'), ('us', 'United States');
create table orders (id int primary key, customer_id int not null,
                     total numeric(10, 2));
insert into orders select g, g % 23, g * 1.5
  from generate_series({lo}, {hi}) g;
"""


def _shards(pair):
    return [f"{pair.name}_t{j + 1}" for j in range(N)]


@pytest.fixture(scope="module")
def lane(pair):
    pair.src.sql(SEED.format(lo=1, hi=240), pair.name)
    pair.dst.sql(SEED.format(lo=10001, hi=10240), pair.name)
    for db in _shards(pair):
        pair.dst.sql(f'create database "{db}"')
    return pair


def _conf(pair, tmp_path, monkeypatch, until_="caught_up", cutover=False):
    import migkit.config as cfg
    def side(port):
        return ("{host: 127.0.0.1, port: %d, user: postgres, password: %s}"
                % (port, servers.PASSWORD))
    # a and c read the first server, b the second: two sources, three
    # targets, each a database of the second server
    reads = {"a": pair.src.port, "b": pair.dst.port, "c": pair.src.port}
    hops = "hops:\n"
    for name, db in zip("abc", _shards(pair)):
        hops += (f"  {name}:\n    engine: postgres\n    goal: reshard\n"
                 f"    source: {side(reads[name])}\n"
                 f"    target: {side(pair.dst.port)}\n"
                 f"    databases: [{pair.name}]\n"
                 f"    db_map: {{{pair.name}: {db}}}\n"
                 "    options:\n"
                 f"      follow_until: {until_}\n"
                 "      target_rows: replace\n"
                 "      reshard: {with: [a, b, c], by: hash, key: {orders:"
                 " customer_id}, shared: [countries]}\n"
                 + ("      cutover: {budget: 120, freeze: database}\n"
                    if cutover else ""))
    path = tmp_path / "hops.yaml"
    path.write_text(hops)
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return cfg.load_hops()


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


def _rows(server, db):
    return {int(i): int(c) for i, c in server.sql(
        "select id, customer_id from orders", db)}


def _sources(pair):
    out = _rows(pair.src, pair.name)
    out.update(_rows(pair.dst, pair.name))
    return out


def _targets(pair):
    return [_rows(pair.dst, db) for db in _shards(pair)]


def _held_once(pair):
    """Every source row on exactly the one shard a hash of its customer
    names, the shards together the sources - or AssertionError."""
    src, shards = _sources(pair), _targets(pair)
    seen = {}
    for j, rows in enumerate(shards):
        for oid, cust in rows.items():
            assert oid not in seen, f"order {oid} on shards {seen[oid]}, {j}"
            seen[oid] = j
            assert reshard.hash_place((str(cust),), N) == j, (oid, cust, j)
    assert sum(len(r) for r in shards) == len(src)
    assert {o: c for r in shards for o, c in r.items()} == src
    return shards


def _settled(pair):
    def ok():
        try:
            return _held_once(pair)
        except AssertionError:
            return None
    return until(ok, 120, "every source row once on its shard")


def _moved_customer(cust):
    """A customer a hash places on another shard than `cust`'s."""
    j = reshard.hash_place((str(cust),), N)
    return next(c for c in range(100, 200)
                if reshard.hash_place((str(c),), N) != j)


def _following(g):
    said = []
    t = threading.Thread(target=reshard.follow, args=(g, True, said.append),
                         daemon=True)
    t.start()
    until(lambda: tailctl.alive(g.report_dir(), g.lead), 60,
          "the group's follow running")
    return t, said


def _end(g, t):
    tailctl.stop(g.report_dir())
    tailctl.resume(g.report_dir())
    t.join(timeout=120)
    assert not t.is_alive()


@needs_docker
def test_two_shards_become_three_followed_and_proved_per_shard(
        lane, tmp_path, monkeypatch):
    pair = lane
    _conf(pair, tmp_path, monkeypatch)
    got, out = _run("move", "a", "--mode", "full+cdc", "--db", pair.name,
                    "--go")
    assert got.exit_code == 0, out
    assert "onto 3 targets" in out, out
    shards = _held_once(pair)
    assert all(shards), shards
    for db in _shards(pair):
        assert pair.dst.sql("select count(*) from countries", db) == [(2,)]
    hops = _conf(pair, tmp_path, monkeypatch, until_="stopped")
    g = reshard.group(hops["a"], pair.name, hops)
    assert len(g.sources) == 2 and len(g.targets) == N
    t, said = _following(g)
    # written while the follow runs, on both sources: rows added, rows
    # removed, and one key whose shard column moves it to another shard
    old = shards[0]
    oid = min(old)
    cust = _moved_customer(old[oid])
    pair.src.sql(f"update orders set customer_id = {cust} where id = {oid}",
                 pair.name)
    pair.src.sql("insert into orders select g, g % 29, 1 from"
                 " generate_series(241, 300) g", pair.name)
    pair.dst.sql("insert into orders select g, g % 31, 2 from"
                 " generate_series(10241, 10300) g", pair.name)
    pair.dst.sql("delete from orders where id between 10010 and 10030",
                 pair.name)
    try:
        shards = _settled(pair)
    finally:
        _end(g, t)
    j = reshard.hash_place((str(cust),), N)
    assert oid in shards[j] and all(oid not in s for k, s in
                                    enumerate(shards) if k != j)
    # each shard's proof: keyed against the rows the function gives it
    got = [r for r in reshard.prove(g) if ".orders on " in r.scope]
    assert len(got) == N and all(r.status == "ok" for r in got), got
    # the follow's reads were sized by the decision engine, recorded
    works = [c.get("work") for c in decide.recorded(g.lead)]
    assert "how many changes a source of a reshard is read next" in works
    # a row planted on a shard the function does not name: that shard's
    # proof names its key, and the shard it belongs on says it is missing
    pair.src.sql("insert into orders values (9999, 1, 0)", pair.name)
    right = reshard.hash_place(("1",), N)
    wrong = (right + 1) % N
    # named columns: the shard leads with the column it is placed by
    pair.dst.sql("insert into orders (id, customer_id, total) values"
                 " (9999, 1, 0)", _shards(pair)[wrong])
    by = {r.scope.split(".")[-1]: r for r in reshard.prove(g)
          if ".orders on " in r.scope}
    bad = by[f"orders on {g.targets[wrong].name}"]
    assert bad.status == "diff" and "another target" in bad.detail \
        and "9999" in bad.detail, bad
    miss = by[f"orders on {g.targets[right].name}"]
    assert miss.status == "diff" and "not here" in miss.detail \
        and "9999" in miss.detail, miss
    pair.dst.sql("delete from orders where id = 9999", _shards(pair)[wrong])
    pair.src.sql("delete from orders where id = 9999", pair.name)


@needs_docker
def test_a_stop_between_the_old_shard_and_the_new_is_resumed_to_one_copy(
        lane, tmp_path, monkeypatch):
    pair = lane
    _conf(pair, tmp_path, monkeypatch)
    # settle what an earlier test left
    got, out = _run("move", "a", "--mode", "cdc", "--db", pair.name, "--go")
    assert got.exit_code == 0, out
    shards = _held_once(pair)
    oid = min(o for o in _rows(pair.dst, pair.name))
    old = next(j for j, s in enumerate(shards) if oid in s)
    cust = _moved_customer(shards[old][oid])
    new = reshard.hash_place((str(cust),), N)
    pair.dst.sql(f"update orders set customer_id = {cust} where id = {oid}",
                 pair.name)
    failpoint.reset()
    monkeypatch.setenv("MIGKIT_FAILPOINT", "reshard.moved_out:1:raise")
    try:
        got, out = _run("move", "b", "--mode", "cdc", "--db", pair.name,
                        "--go")
    finally:
        monkeypatch.delenv("MIGKIT_FAILPOINT")
        failpoint.reset()
    assert got.exit_code != 0, out
    # the stop came after the removal from the old shard and before the
    # write on the new one: the row is on neither, and its position was
    # not kept
    now = _targets(pair)
    assert oid not in now[old] and oid not in now[new], (old, new)
    got, out = _run("move", "b", "--mode", "cdc", "--db", pair.name, "--go")
    assert got.exit_code == 0, out
    shards = _held_once(pair)
    assert [j for j, s in enumerate(shards) if oid in s] == [new]
    assert shards[new][oid] == cust


def _totals(pair):
    out = {}
    for db in _shards(pair):
        out.update({int(i): float(v) for i, v in pair.dst.sql(
            "select id, total from orders where id < 50 or id between 10001"
            " and 10049", db)})
    return out


def _source_totals(pair):
    out = {int(i): float(v) for i, v in pair.src.sql(
        "select id, total from orders where id < 50", pair.name)}
    out.update({int(i): float(v) for i, v in pair.dst.sql(
        "select id, total from orders where id < 10050", pair.name)})
    return out


def _app(pair, server):
    """The application's own account on a source: it writes the orders."""
    role = f"{pair.name}_app"
    server.sql(f'create role "{role}" login password'
               f" '{servers.PASSWORD}'")
    server.sql(f'grant connect on database "{pair.name}" to "{role}"')
    server.sql(f'grant usage on schema public to "{role}";'
               f' grant select, insert, update on orders to "{role}"',
               pair.name)


def _as_app(pair, server, statement):
    import psycopg2
    c = psycopg2.connect(host=server.host, port=server.port,
                         user=f"{pair.name}_app", password=servers.PASSWORD,
                         dbname=pair.name, connect_timeout=5)
    try:
        with c, c.cursor() as cur:
            cur.execute(statement)
    finally:
        c.close()


@needs_docker
def test_the_cutover_holds_every_source_and_drains_every_shard(
        lane, tmp_path, monkeypatch):
    pair = lane
    _conf(pair, tmp_path, monkeypatch, cutover=True)
    got, out = _run("move", "a", "--mode", "cdc", "--db", pair.name, "--go")
    assert got.exit_code == 0, out
    # the verdict the cutover starts from: every shard proved
    got, out = _run("check", "a", "--db", pair.name)
    assert got.exit_code == 0, out
    for server in (pair.src, pair.dst):
        _app(pair, server)
        # the application writes the source before the cutover
        _as_app(pair, server, "update orders set total = total where id = 1")
    hops = _conf(pair, tmp_path, monkeypatch, until_="stopped",
                 cutover=True)
    g = reshard.group(hops["a"], pair.name, hops)
    t, _ = _following(g)
    try:
        pair.src.sql("update orders set total = total + 1 where id < 50",
                     pair.name)
        pair.dst.sql("update orders set total = total + 1 where id < 10050",
                     pair.name)
        until(lambda: _totals(pair) == _source_totals(pair), 120,
              "the updates on every shard")
        got, out = _run("move", "a", "--mode", "cdc", "--drop", "--db",
                        pair.name, "--go")
    finally:
        if t.is_alive():
            _end(g, t)
    assert got.exit_code == 0, out
    assert "cut over" in out, out
    assert "found the two the same" in out, out
    shards = _held_once(pair)
    # every target at the mark: the updates written before the hold are on
    # the shard each row is on
    assert _totals(pair) == _source_totals(pair)
    assert sum(len(s) for s in shards) == len(_sources(pair))
    # every source held: the application's write to either is refused
    # after the cutover
    for server in (pair.src, pair.dst):
        with pytest.raises(Exception):
            _as_app(pair, server, "insert into orders (id, customer_id,"
                                  " total) values (99999, 1, 0)")
    assert not tailctl.alive(g.report_dir(), g.lead)
