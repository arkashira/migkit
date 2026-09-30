"""The target pulls the rows from the source itself, where the hop allows
it (backlog R22 item 4): through a foreign server, range by range, or a
temporary subscription; either way the target then holds exactly what the
source does, every object either way made is gone after it, the plan says
what it makes before anything is made, and a target that cannot reach the
source falls a rung and says why.

The link between the servers is held up by a proxy (Toxiproxy, driven
over its HTTP API) for the measurement: a range pulled from server to
server against the same range through migkit.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent
PROXY, API, SRC_LISTEN, DST_LISTEN = ("migkit-test-inst-toxi", 16230, 16231,
                                      16232)


def _api(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{API}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        text = r.read().decode()
        return json.loads(text) if text else None


def _ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module")
def proxy(pg_pair):
    """The proxy's address as the target container reaches it."""
    subprocess.run(["docker", "rm", "-f", PROXY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PROXY,
                    "-p", f"127.0.0.1:{API}:8474",
                    "-p", f"127.0.0.1:{SRC_LISTEN}:{SRC_LISTEN}",
                    "-p", f"127.0.0.1:{DST_LISTEN}:{DST_LISTEN}",
                    "ghcr.io/shopify/toxiproxy:2.12.0"],
                   check=True, capture_output=True)
    try:
        for _ in range(30):
            try:
                for name, listen, box in (
                        ("source", SRC_LISTEN, "migkit-test-pg-src"),
                        ("target", DST_LISTEN, "migkit-test-pg-dst")):
                    _api("POST", "/proxies",
                         {"name": name, "listen": f"0.0.0.0:{listen}",
                          "upstream": f"{_ip(box)}:5432"})
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)
        else:
            pytest.fail("the proxy never answered")
        yield _ip(PROXY)
    finally:
        subprocess.run(["docker", "rm", "-f", PROXY], capture_output=True)


def _delay(name, ms):
    for t in _api("GET", f"/proxies/{name}/toxics") or []:
        _api("DELETE", f"/proxies/{name}/toxics/{t['name']}")
    for stream in ("upstream", "downstream") if ms else ():
        _api("POST", f"/proxies/{name}/toxics",
             {"name": f"late-{stream}", "type": "latency", "stream": stream,
              "attributes": {"latency": ms}})


def _migkit(tmp, *args, **env):
    """The command's run, its output with the terminal's line wrapping
    taken out as `said`."""
    full = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
                MIGKIT_REPORTS=str(tmp / "reports"), COLUMNS="250",
                # never a real cloud account: no credentials to find
                AWS_SHARED_CREDENTIALS_FILE=str(tmp / "no-credentials"),
                AWS_CONFIG_FILE=str(tmp / "no-config"),
                AWS_EC2_METADATA_DISABLED="true",
                MIGKIT_INSTANCES=str(tmp / "instances.json"), **env)
    for k in ("AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
              "AWS_SESSION_TOKEN"):
        full.pop(k, None)
    got = subprocess.run([sys.executable, "-c",
                          "from migkit.cli import main; main()", *args],
                         env=full, cwd=ROOT, capture_output=True, text=True,
                         timeout=900)
    got.said = " ".join(got.stdout.split())
    return got


SEED = """
create table big (id bigint primary key, payload text);
insert into big select g, repeat('x', 150) || g from generate_series(1, 300000) g;
create table keyless (a int, b text);
insert into keyless select g % 700, 'k' || (g % 700) from generate_series(1, 5000) g;
create table made (id int generated always as identity primary key, x int,
                   y int generated always as (x * 2) stored);
insert into made (x) select g from generate_series(1, 1000) g;
create table filtered (id int primary key, v int);
insert into filtered select g, g from generate_series(1, 1000) g;
create table theirs (id int primary key, note text);
insert into theirs values (1, 'source');
analyze;
"""

Q = {"big": "id", "keyless": "a, b", "made": "id", "filtered": "id",
     "theirs": "id"}


def _digest(port, table, where=""):
    got = psql(port, f"select count(*) || ':' || coalesce(md5(string_agg("
                     f"t::text, ',' order by {Q[table]})), '') from"
                     f" public.{table} t {where}")
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


#: milliseconds the proxy adds each way to put migkit far from a side:
#: a statement's round trip then passes `legs.FAR_MS`
FAR_EACH_WAY = 15


def _far(on):
    """migkit across a wide network from both sides - both reached
    through the proxy, held up each way - while the servers still reach
    each other directly: far from the target, where the servers pull."""
    for name in ("source", "target"):
        _delay(name, FAR_EACH_WAY if on else 0)


def _hop(tmp, pg_pair, s2s, extra="", far=False):
    src, dst = ((SRC_LISTEN, DST_LISTEN) if far
                else (pg_pair["src"], pg_pair["dst"]))
    (tmp / "hops.yaml").write_text(
        "hops:\n  pull:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {src},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {dst},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    exclude: [public.theirs]\n"
        "    mapping:\n      where:\n        public.filtered: v > 500\n"
        f"    options:\n      server_to_server: {s2s}\n" + extra)


def _prepare(pg_pair):
    got = psql(pg_pair["src"], SEED)
    assert got.returncode == 0, got.stderr
    # the target keeps its own rows of the table the hop leaves out
    got = psql(pg_pair["dst"], "create table theirs (id int primary key,"
                               " note text); insert into theirs values"
                               " (1, 'target'), (2, 'target')")
    assert got.returncode == 0, got.stderr


def _nothing_left(pg_pair):
    left = psql(pg_pair["dst"], "select (select count(*) from"
                                " pg_foreign_server) || '|' || (select"
                                " count(*) from pg_user_mappings) || '|' ||"
                                " (select count(*) from pg_namespace where"
                                " nspname like 'migkit\\_%') || '|' ||"
                                " (select count(*) from pg_extension where"
                                " extname = 'postgres_fdw') || '|' ||"
                                " (select count(*) from pg_subscription)"
                ).stdout.strip()
    assert left == "0|0|0|0|0", left
    src = psql(pg_pair["src"], "select (select count(*) from"
                               " pg_publication where pubname like"
                               " 'migkit\\_%') || '|' || (select count(*)"
                               " from pg_replication_slots where slot_name"
                               " like 'migkit\\_%')").stdout.strip()
    assert src == "0|0", src


def _same(pg_pair):
    for table in ("big", "keyless", "made"):
        assert _digest(pg_pair["src"], table) == \
            _digest(pg_pair["dst"], table), table
    assert _digest(pg_pair["src"], "filtered", "where v > 500") == \
        _digest(pg_pair["dst"], "filtered")
    assert psql(pg_pair["dst"], "select string_agg(note, ',' order by id)"
                                " from theirs").stdout.strip() == \
        "target,target"
    # the identity's sequence carried: the next row does not collide
    got = psql(pg_pair["dst"], "insert into made (x) values (0)"
                               " returning id")
    assert got.returncode == 0 and int(got.stdout.split()[0]) > 1000, \
        got.stdout + got.stderr


def test_beside_the_target_migkit_copies_the_rows_itself(pg_pair, proxy,
                                                         tmp_path):
    """Measured faster there (`legs`' table): the hop's leave is not
    enough, and the plan says why the servers do not pull."""
    _prepare(pg_pair)
    _hop(tmp_path, pg_pair,
         f"{{source: '{_ip('migkit-test-pg-src')}:5432'}}")
    plan = _migkit(tmp_path, "move", "pull", "--mode", "full")
    assert plan.returncode == 0, plan.stdout + plan.stderr
    assert "the rows go through this machine: this machine is" in \
        plan.said, plan.said
    assert "ms from the target, where its own copy measured faster" in \
        plan.said, plan.said
    assert "the target pulling the tables" not in plan.said
    _nothing_left(pg_pair)


def test_the_target_pulls_through_a_foreign_server(pg_pair, proxy,
                                                   tmp_path):
    _prepare(pg_pair)
    _far(True)
    _hop(tmp_path, pg_pair,
         f"{{source: '{_ip('migkit-test-pg-src')}:5432'}}", far=True)
    try:
        _pulled(pg_pair, tmp_path)
    finally:
        _far(False)


def _pulled(pg_pair, tmp_path):
    plan = _migkit(tmp_path, "move", "pull", "--mode", "full")
    assert plan.returncode == 0, plan.stdout + plan.stderr
    out = plan.said
    assert "the target pulling the tables from the source" in out, out
    assert "a user mapping holding postgres's credentials - dropped the" \
           " moment the last row is in" in out
    assert "public.filtered: table by table" in out
    # the plan made nothing
    _nothing_left(pg_pair)
    got = _migkit(tmp_path, "move", "pull", "--mode", "full", "--go")
    assert got.returncode == 0, got.stdout + got.stderr
    assert "the target pulling the tables from the source" in got.said
    assert "public.big: 300,000 rows pulled" in got.said, got.stdout
    _same(pg_pair)
    _nothing_left(pg_pair)
    # the proof's round trips from the target: the servers' own view
    seen = json.loads((tmp_path / "reports" / "pull" /
                       "servers-seen.json").read_text())
    assert seen["target_to_source_ms"] < 20


def test_the_target_copies_through_a_temporary_subscription(pg_pair, proxy,
                                                            tmp_path):
    _prepare(pg_pair)
    _hop(tmp_path, pg_pair,
         f"{{source: '{_ip('migkit-test-pg-src')}:5432'}}")
    got = _migkit(tmp_path, "move", "pull", "--mode", "full", "--go",
                  MIGKIT_MOVER="pgsubscribe")
    assert got.returncode == 0, got.stdout + got.stderr
    assert "through a temporary subscription" in got.said
    # the table without a key is not published - in a publication the
    # source would refuse its updates - and goes by the table copier
    assert "2 of 2 tables copied" in got.said, got.stdout
    _same(pg_pair)
    _nothing_left(pg_pair)


def test_a_target_that_cannot_reach_the_source_falls_a_rung(pg_pair, proxy,
                                                            tmp_path):
    _prepare(pg_pair)
    # an address the target container has no route to
    _far(True)
    _hop(tmp_path, pg_pair, "{source: '192.0.2.1:5432'}", far=True)
    try:
        got = _migkit(tmp_path, "move", "pull", "--mode", "full", "--go")
    finally:
        _far(False)
    assert got.returncode == 0, got.stdout + got.stderr
    said = got.said
    assert "the rows go through this machine" in said, said
    assert "the target pulling from the source did not prove itself: the" \
           " target could not read the source through it" in said, said
    assert "did not prove itself: the target could not reach the source" \
           " to subscribe" in said, said
    assert "the target pulling the tables" not in said
    _same(pg_pair)
    _nothing_left(pg_pair)


def test_assess_observes_the_source_and_sizes_the_target(pg_pair, tmp_path):
    _prepare(pg_pair)
    _hop(tmp_path, pg_pair, "false",
         "      cloud: aws\n      region: us-east-1\n      deadline: 1h\n")
    for _ in range(2):
        got = _migkit(tmp_path, "assess", "pull")
        assert "Traceback" not in got.stdout + got.stderr, got.stderr
    out = got.said
    assert "the target's class" in out, out
    assert "2 observations from" in out, out
    assert "the smallest with that:" in out and "db." in out, out
    assert "the machine to run migkit on" in out, out
    assert "the deadline of 1.0 hours needs" in out, out
    seen = json.loads((tmp_path / "reports" / "pull" / "load.json"
                       ).read_text())
    assert len(seen) == 2
    # the source's own counters: its cache, its limit, its connections
    assert seen[-1]["cache_bytes"] == 128 * 2 ** 20
    assert seen[-1]["max_connections"] == 100
    assert seen[-1]["connections"] >= 1 and seen[-1]["iops"] is not None


RANGE = 200_000


def _timed(fn):
    began = time.monotonic()
    fn()
    return round(time.monotonic() - began, 2)


def test_a_range_pulled_server_to_server_against_through_migkit(
        pg_pair, proxy, tmp_path, monkeypatch, capsys):
    """The numbers in `legs`' docstring: one range of 200,000 rows pulled
    by the target from the source, and the same range through migkit
    beside the target and far from both, the link to the source (and to
    the target, far from both) held up by the proxy each way."""
    from migkit import legs
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr("migkit.config.REPORTS", tmp_path / "reports")
    got = psql(pg_pair["src"], "create table big (id bigint primary key,"
                               " payload text); insert into big select g,"
                               " repeat('x', 150) || g from"
                               f" generate_series(1, {RANGE}) g; analyze big")
    assert got.returncode == 0, got.stderr
    size = int(psql(pg_pair["src"], "select pg_table_size('big')"
                    ).stdout.strip())
    psql(pg_pair["dst"], "create table big (id bigint primary key,"
                         " payload text)")
    rows_bytes = size / RANGE

    def hop(far):
        return Hop(name="measure", engine="postgres",
                   source=Endpoint(host="127.0.0.1", port=SRC_LISTEN,
                                   user="postgres", password="test"),
                   target=Endpoint(host="127.0.0.1",
                                   port=DST_LISTEN if far else pg_pair["dst"],
                                   user="postgres", password="test"),
                   databases=["postgres"],
                   options={"server_to_server": {
                       "source": f"{proxy}:{SRC_LISTEN}"}})

    def empty():
        psql(pg_pair["dst"], "truncate big")

    def pull(h, fetch):
        empty()
        with legs.Footprint(h, "postgres") as fp:
            local = fp.import_tables("public", ["big"], {"big": fetch})
            with fp.conn.cursor() as cur:
                return _timed(lambda: cur.execute(
                    f"insert into public.big select * from {local}.big"))

    def through(h):
        empty()
        eng = PostgresEngine(h)
        return _timed(lambda: eng._copy_pipe(
            "postgres", 'select "id", "payload" from "public"."big"',
            '"public"."big"', columns=["id", "payload"]))

    def subscribed():
        empty()
        psql(pg_pair["src"], "create publication m for table big")
        began = time.monotonic()
        got = psql(pg_pair["dst"], "create subscription m connection"
                   f" 'host={proxy} port={SRC_LISTEN} dbname=postgres"
                   " user=postgres password=test' publication m with"
                   " (copy_data = true, binary = true)")
        assert got.returncode == 0, got.stderr
        while psql(pg_pair["dst"], "select count(*) from pg_subscription_rel"
                   " where srsubstate <> 'r'").stdout.strip() != "0":
            time.sleep(0.05)
        took = round(time.monotonic() - began, 2)
        psql(pg_pair["dst"], "drop subscription m")
        psql(pg_pair["src"], "drop publication m")
        return took

    table = {}
    for ms in (0, 5, 25):
        _delay("source", ms)
        _delay("target", 0)
        near = hop(False)
        assert legs.prove_pull(near, "postgres") is None
        seen = legs.servers_seen(near)
        chosen = legs._fetch_rows(rows_bytes, max(2 * ms, 0.2) / 1000)
        row = {"seen_ms": seen, "fetch": chosen,
               "pull": pull(near, chosen)}
        if ms <= 5:
            row["pull_fetch_100"] = pull(near, 100)
        row["subscription"] = subscribed()
        row["migkit_beside_target"] = through(near)
        _delay("target", ms)
        row["migkit_far_from_both"] = through(hop(True))
        _delay("source", 0)
        row["migkit_beside_source"] = through(hop(True))
        table[2 * ms] = row
        assert psql(pg_pair["dst"], "select count(*) from big"
                    ).stdout.strip() == str(RANGE)
    _delay("source", 0)
    _delay("target", 0)
    with capsys.disabled():
        print(f"\n{size:,} bytes a range")
        print(json.dumps(table, indent=1))
    # the servers' own round trip, as the target measured it
    assert table[50]["seen_ms"] == pytest.approx(50, rel=0.4)
    assert table[0]["seen_ms"] < 5
    # a fetch sized to the round trip, against the foreign server's own
    # hundred rows: a round trip a hundred rows
    assert 5 * table[10]["pull"] < table[10]["pull_fetch_100"]
    # beside the target migkit's own streaming copy is the faster, which
    # is why the servers pull only for a machine far from the target
    assert table[50]["migkit_beside_target"] < table[50]["pull"]
    # its writes across the link are what cost it: beside the source and
    # far from both it is slower than beside the target
    assert table[50]["migkit_beside_target"] < \
        table[50]["migkit_beside_source"]
    assert table[50]["migkit_beside_target"] < \
        table[50]["migkit_far_from_both"]
    # the subscription's copy takes round trips of its own for each table
    assert table[50]["pull"] < table[50]["subscription"]
