"""The cutover as one timed path, on PostgreSQL (research: cutover,
section 14): `move --mode cdc --drop --go` on a hop with `cutover:`.

An application keeps writing through all of it, one row at a time, and
records every attempt, so what is measured is what the application saw:
how long it went without an acknowledged write, and whether every row it
was told was written is where it now writes - once. Every step is then
broken on purpose, by raising at it and by killing migkit at it, and each
time the source has to come back writable and as it was: the same
grants, the same settings, the same objects, and no row migkit wrote.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
import pytest
import yaml
from click.testing import CliRunner

from tests import cutover_harness as harness
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.drain", "postgres")]

pytestmark = needs_docker

SRC, DST = "migkit-test-pg-src", "migkit-test-pg-dst"
BOUNCER, BOUNCER_PORT = "migkit-test-cut-bouncer", 16074
HOP = "pgc"


def _ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


def _one(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


STATE = """
select json_build_object(
  'grants', (select coalesce(json_agg(g order by g), '[]') from (
     select grantee||':'||table_name||':'||privilege_type g from
     information_schema.role_table_grants where grantee = 'app') x),
  'datacl', (select datacl::text from pg_database
             where datname = 'postgres'),
  'role_settings', (select coalesce(json_agg(s order by s), '[]') from (
     select coalesce(r.rolname, '-')||':'||array_to_string(d.setconfig, ',')
     s from pg_db_role_setting d left join pg_roles r on r.oid = d.setrole)
     y),
  'server', (select coalesce(json_agg(name||'='||setting order by name),
             '[]') from pg_file_settings
             where sourcefile like '%postgresql.auto.conf'),
  'subscriptions', (select coalesce(json_agg(subname order by subname),
                    '[]') from pg_subscription),
  'publications', (select coalesce(json_agg(pubname order by pubname), '[]')
                   from pg_publication))
"""


def _state(port):
    """What a cutover may change on a side and an undo must put back."""
    return json.loads(_one(port, STATE))


def _tags(port):
    return _one(port, "select coalesce(string_agg(tag, ' '), '') from"
                      " public.orders").split()


@pytest.fixture
def cut(pg_pair, tmp_path, monkeypatch):
    """The pair as a hop with a stream running from the source to the
    target, the target held from the application, and the application
    writing to the source."""
    import migkit.config as cfg
    routes = {f"127.0.0.1:{pg_pair['src']}": f"{_ip(SRC)}:5432",
              f"127.0.0.1:{pg_pair['dst']}": f"{_ip(DST)}:5432"}
    harness.install(routes, monkeypatch)
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    # the account carried as `migkit users` carries it, its password's hash
    # and all: a pooler signs in to the server with what the source's hash
    # gave it, and a hash salted anew on the target refuses that (measured,
    # PgBouncer 1.25: `password authentication failed` after the repoint)
    psql(pg_pair["src"], "drop role if exists app")
    _one(pg_pair["src"], "create role app login password 'CHANGE_ME'")
    secret = _one(pg_pair["src"], "select rolpassword from pg_authid"
                                  " where rolname = 'app'")
    psql(pg_pair["dst"], "drop role if exists app")
    _one(pg_pair["dst"], f"create role app login password '{secret}'")
    for port in (pg_pair["src"], pg_pair["dst"]):
        _one(port, "create table public.orders (id bigserial primary key,"
                   " tag text not null);"
                   " grant select, insert on public.orders to app;"
                   " grant usage on sequence public.orders_id_seq to app")
    _one(pg_pair["src"], "insert into public.orders (tag) select 'seed'||g"
                         " from generate_series(1, 200) g")
    # no background analyze writing to the log while a test waits for it
    # to stand still
    _one(pg_pair["src"], "alter table public.orders set"
                         " (autovacuum_enabled = false)")
    route = tmp_path / "route"
    route.write_text("src")

    class Cut:
        pass
    c = Cut()
    c.pair, c.tmp, c.route, c.routes = pg_pair, tmp_path, route, routes
    c.reports = tmp_path / "reports" / HOP

    def hop(cutover, **more):
        options = {"protect_target": True, "reverse": "at_cutover",
                   "cutover": cutover,
                   "state": {"mirror": str(tmp_path / "mirror")}, **more}
        conf = tmp_path / "hops.yaml"
        conf.write_text(yaml.safe_dump({"hops": {HOP: {
            "engine": "postgres",
            "source": {"host": "127.0.0.1", "port": pg_pair["src"],
                       "user": "postgres", "password": "test"},
            "target": {"host": "127.0.0.1", "port": pg_pair["dst"],
                       "user": "postgres", "password": "test"},
            "databases": ["postgres"], "options": options}}}))
        monkeypatch.setattr(cfg, "CONF", str(conf))
        c.conf = conf
    c.hop = hop

    def connect(where, port=None):
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=port or (pg_pair["src"] if where == "src"
                          else pg_pair["dst"]),
            user="app", password="CHANGE_ME", dbname="postgres",
            connect_timeout=3)
        return conn
    c.connect = connect
    yield c
    # every subscription on both sides first: one still running on the
    # target keeps its slot on the source active, and an active slot is
    # not dropped - the next test's subscription found it there
    for port in (pg_pair["src"], pg_pair["dst"]):
        for name in _one(port, "select string_agg(subname, ' ') from"
                               " pg_subscription").split():
            psql(port, f"alter subscription {name} disable;"
                       f" alter subscription {name} set (slot_name = none);"
                       f" drop subscription {name}")
    for port in (pg_pair["src"], pg_pair["dst"]):
        for name in _one(port, "select string_agg(pubname, ' ') from"
                               " pg_publication").split():
            psql(port, f"drop publication {name}")
        # a walsender outlives its subscription for a moment, and a slot
        # still active then is not dropped - the next test's subscription
        # would find it there
        harness.wait_for(lambda: _one(port, "select count(*) from"
                                            " pg_replication_slots where"
                                            " active") == "0", 30)
        psql(port, "select pg_drop_replication_slot(slot_name) from"
                   " pg_replication_slots where not active")
        psql(port, "alter system reset default_transaction_read_only")
        psql(port, "select pg_reload_conf()")
        psql(port, "alter role postgres in database postgres reset all")
        psql(port, "grant connect on database postgres to public")
        psql(port, "drop table if exists public.orders;"
                   " drop owned by app; drop role if exists app")


def _cli(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _follow(c):
    got, said = _cli("move", HOP, "--mode", "full+cdc", "--go")
    assert got.exit_code == 0, said
    want = _one(c.pair["src"], "select count(*) from public.orders")
    assert harness.wait_for(lambda: _one(
        c.pair["dst"], "select count(*) from public.orders") == want), said


def _writing(c):
    w = harness.Writer(c.connect, c.route)
    w.start()
    assert harness.wait_for(lambda: len(w.acked()) >= 30, 30), w.errors()[-3:]
    return w


def _arrived(c, tags, timeout=60):
    return harness.wait_for(
        lambda: set(tags) <= set(_tags(c.pair["dst"])), timeout)


def _nothing_lost_or_twice(c, w, port):
    have = _tags(port)
    assert len(have) == len(set(have)), "a row written twice"
    assert set(w.acked()) <= set(have), \
        f"{len(set(w.acked()) - set(have))} acknowledged rows missing"
    seeds = {f"seed{i}" for i in range(1, 201)}
    assert set(have) <= w.attempted() | seeds, "rows nobody wrote"


def _approver(tmp, name):
    keys = tmp / "keys"
    keys.mkdir(exist_ok=True)
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                    name, "-f", str(keys / name)], check=True)
    pub = (keys / f"{name}.pub").read_text().split()
    signers = keys / "allowed_signers"
    signers.write_text(f'{name} namespaces="migkit-approve" {pub[0]}'
                       f" {pub[1]}\n")
    return keys / name, signers


def _sign(key, request):
    got = subprocess.run(["ssh-keygen", "-Y", "sign", "-n", "migkit-approve",
                          "-f", str(key)], input=request.read_bytes(),
                         capture_output=True)
    assert got.returncode == 0, got.stderr
    return got.stdout


def test_the_cutover_moves_the_application_and_loses_nothing(cut):
    c = cut
    key, signers = _approver(c.tmp, "ann")
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"},
          approvals={"steps": ["cutover"], "signers": str(signers)})
    _follow(c)
    before = _state(c.pair["src"])
    w = _writing(c)
    try:
        # asked for approval of this plan, and nothing done without it
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code != 0 and "needs 1 approval and has 0" in said, \
            said
        assert _state(c.pair["src"]) == before
        request = c.reports / "approvals" / "cutover-all.request.json"
        asked = json.loads(request.read_text())
        assert asked["detail"]["budget"] == 20 and asked["detail"]["reverse"]
        (request.parent / "cutover-all.ann.sig").write_bytes(
            _sign(key, request))
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0, said
        assert "cut over:" in said and "of write outage" in said, said
        flipped = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 30,
                                30), w.errors()[-3:]
    finally:
        w.stop()
    outage = w.outage()
    print(f"\nmeasured from the application: {outage:.2f}s without an"
          f" acknowledged write; {len(w.errors())} writes refused")
    assert 0 < outage < 20
    after = [e for e in w.log if e[2] == "dst"]
    # the target's counters were past the source's: its first keys took
    assert after and all(e[3] == "ok" for e in after), \
        [e for e in after if e[3] != "ok"][:3]
    _nothing_lost_or_twice(c, w, c.pair["dst"])
    # the stream back carried what the new primary took to the old source
    assert harness.wait_for(lambda: _tags(c.pair["src"]) and
                            sorted(_tags(c.pair["src"]))
                            == sorted(_tags(c.pair["dst"]))), \
        "the old source did not follow the new primary"
    # and the old source refuses the application: a straggler fails there
    with pytest.raises(psycopg2.Error):
        conn = c.connect("src")
        conn.cursor().execute("insert into public.orders (tag) values"
                              " ('straggler')")
        conn.commit()
    run = [json.loads(x) for x in
           (c.reports / "cutover-runs.jsonl").read_text().splitlines()][-1]
    assert run["state"] == "flipped" and run["outage"] < 20, run
    assert set(run["took"]) >= {"freeze", "drain", "verify", "counters",
                                "stream", "reverse", "flip"}, run["took"]
    record = [json.loads(x) for x in
              (c.reports / "changelog.jsonl").read_text().splitlines()]
    steps = [r.get("phase") for r in record if r.get("op") == "cutover"
             and r.get("event") == "done"]
    assert steps == ["prepare", "jobs_off", "freeze", "drain", "verify",
                     "counters", "stream", "reverse", "jobs_on"], steps
    assert all(r.get("approved_by") == ["ann"] for r in record
               if r.get("op") == "cutover"), record[-3:]
    # going back: the old source's counters past the new primary's, and
    # the source given back to the application
    got, said = _cli("rollback", HOP, "--db", "postgres", "--apply")
    assert got.exit_code == 0, said
    conn = c.connect("src")
    cur = conn.cursor()
    cur.execute("insert into public.orders (tag) values ('back') returning"
                " id")
    assert cur.fetchone()[0] > int(_one(c.pair["dst"],
                                        "select max(id) from public.orders"))
    conn.commit()
    conn.close()


#: "prepare" first: the hop streams back, and the stream back is made
#: ahead of the clock where it can wait idle
PHASES = ("prepare", "jobs_off", "freeze", "drain", "verify", "counters",
          "stream", "reverse", "jobs_on", "flip")


def test_a_failure_at_any_step_undoes_every_step(cut, monkeypatch):
    from migkit import failpoint
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    before = _state(c.pair["src"])
    held = _state(c.pair["dst"])
    w = _writing(c)
    try:
        for phase in PHASES:
            failpoint._counts.clear()
            monkeypatch.setenv("MIGKIT_FAILPOINT", f"cutover.{phase}:1:raise")
            got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
            assert got.exit_code != 0, (phase, said)
            assert "the cutover was undone" in said, (phase, said)
            back = time.time()
            assert c.route.read_text() == "src", phase
            # writable again, where the application writes
            assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10,
                                    30), (phase, w.errors()[-3:])
            # and as it was
            assert _state(c.pair["src"]) == before, phase
            assert _state(c.pair["dst"]) == held, phase
            assert not (c.reports / "cutover.json").exists(), phase
            assert not list(c.reports.rglob("freeze-src*.json")), phase
            # the stream forward carries on
            assert _arrived(c, w.acked()), phase
    finally:
        monkeypatch.delenv("MIGKIT_FAILPOINT", raising=False)
        w.stop()
    _nothing_lost_or_twice(c, w, c.pair["src"])
    assert _arrived(c, _tags(c.pair["src"]))
    runs = [json.loads(x) for x in
            (c.reports / "cutover-runs.jsonl").read_text().splitlines()]
    assert [r["state"] for r in runs] == ["undone"] * len(PHASES)


def _killed(c, phase):
    """migkit run in a process of its own and killed, as by kill -9, when
    it reaches `phase`."""
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "MIGKIT_CONF": str(c.conf),
           "MIGKIT_REPORTS": str(c.tmp / "reports"),
           "MIGKIT_FAILPOINT": f"cutover.{phase}:1:exit",
           "MIGKIT_TEST_ROUTES": ",".join(f"{a}={b}" for a, b in
                                          c.routes.items()),
           "PYTHONPATH": str(root)}
    return subprocess.run([sys.executable, str(Path(harness.__file__)),
                           "move", HOP, "--mode", "cdc", "--drop", "--go"],
                          env=env, capture_output=True, text=True,
                          timeout=300)


def test_a_cutover_killed_anywhere_is_undone_by_the_next_run(cut):
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    _follow(c)
    before = _state(c.pair["src"])
    w = _writing(c)
    try:
        for phase in ("freeze", "drain", "stream", "reverse", "flip"):
            got = _killed(c, phase)
            assert got.returncode == 137, (phase, got.stdout[-500:],
                                           got.stderr[-500:])
            left = json.loads((c.reports / "cutover.json").read_text())
            assert left["state"] == "running" and phase in left["phases"]
            got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
            assert got.exit_code != 0, said
            assert "the interrupted cutover was undone" in said, said
            back = time.time()
            assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10,
                                    30), (phase, w.errors()[-3:])
            assert _state(c.pair["src"]) == before, phase
            assert _arrived(c, w.acked()), phase
    finally:
        w.stop()
    _nothing_lost_or_twice(c, w, c.pair["src"])


def test_a_drain_that_does_not_finish_in_the_budget_is_undone(cut):
    """The target stops answering the moment the source is frozen (the
    container paused): its stream never confirms the position taken after
    the freeze, the budget runs out, and the source is given back while
    the application keeps trying."""
    c = cut
    c.hop({"budget": 3})
    _follow(c)
    before = _state(c.pair["src"])
    w = _writing(c)
    try:
        harness.when_phase(c.reports, "freeze", lambda: subprocess.run(
            ["docker", "pause", DST], capture_output=True))
        began = time.time()
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        took = time.time() - began
    finally:
        subprocess.run(["docker", "unpause", DST], capture_output=True)
    try:
        assert got.exit_code != 0, said
        assert "the stream did not pass" in said and "within the budget" \
            in said, said
        back = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10, 30), \
            w.errors()[-3:]
    finally:
        w.stop()
    assert _state(c.pair["src"]) == before
    run = [json.loads(x) for x in
           (c.reports / "cutover-runs.jsonl").read_text().splitlines()][-1]
    assert run["state"] == "undone" and 3 <= run["outage"] < 10, run
    assert took < 60, took
    print(f"\nbudget of 3s blown: undone after {run['outage']:.2f}s of"
          f" outage; the application saw {w.outage():.2f}s")
    assert _arrived(c, w.acked())
    _nothing_lost_or_twice(c, w, c.pair["src"])


def test_a_flip_command_that_fails_gives_the_source_back(cut):
    c = cut
    c.hop({"budget": 20, "flip": "echo the load balancer said no; exit 3"})
    _follow(c)
    before = _state(c.pair["src"])
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code != 0, said
        assert "the flip command failed: exited 3" in said, said
        assert "the load balancer said no" in said, said
        back = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(back)) >= 10, 30)
    finally:
        w.stop()
    assert _state(c.pair["src"]) == before
    # the target is held from the application again
    with pytest.raises(psycopg2.Error):
        conn = c.connect("dst")
        conn.cursor().execute("insert into public.orders (tag) values"
                              " ('early')")
        conn.commit()
    _nothing_lost_or_twice(c, w, c.pair["src"])
    assert _arrived(c, w.acked())


def test_a_prepared_transaction_stops_it_before_anything_is_written(cut):
    c = cut
    c.hop({"budget": 20})
    _follow(c)
    psql(c.pair["src"], "alter system set max_prepared_transactions = 5")
    try:
        subprocess.run(["docker", "restart", SRC], capture_output=True)
        harness.wait_for(lambda: psql(c.pair["src"], "select 1")
                         .returncode == 0, 60)
        before = _state(c.pair["src"])
        _one(c.pair["src"], "begin; insert into public.orders (tag) values"
                            " ('prepared'); prepare transaction 'held'")
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code != 0, said
        assert "prepared transaction held" in said, said
        assert "nothing was written" in said, said
        assert _state(c.pair["src"]) == before
    finally:
        psql(c.pair["src"], "rollback prepared 'held'")
        psql(c.pair["src"], "alter system reset max_prepared_transactions")
        subprocess.run(["docker", "restart", SRC], capture_output=True)
        harness.wait_for(lambda: psql(c.pair["src"], "select 1")
                         .returncode == 0, 60)


def test_without_cutover_the_stream_stays_up_while_the_source_is_written(
        cut):
    """A hop without `cutover:` is told the steps and proved around: the
    stream is not torn down while the application still writes, and is
    once it has stopped and the stream has caught up."""
    c = cut
    c.hop(None)
    _follow(c)
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop")
    assert got.exit_code == 0, said
    assert "stop the application's writes to the source" in said, said
    assert "dry-run" in said, said
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code != 0, said
        assert "the stream was left up" in said and "still being written" \
            in said, said
        assert _one(c.pair["dst"], "select count(*) from pg_subscription"
                    ) == "1"
    finally:
        w.stop()
    # the application stopped, as step 2 says; its last counts reach the
    # server's statistics as its sessions end
    time.sleep(2)
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0, said
    assert "the stream passed where the source stands" in said, said
    _nothing_lost_or_twice(c, w, c.pair["dst"])
    # nothing of the source was touched by migkit but the stream it asked
    assert _one(c.pair["src"], "select count(*) from pg_file_settings where"
                               " name = 'default_transaction_read_only'"
                ) == "0"


def test_a_pooler_in_front_holds_the_writes_instead_of_failing_them(cut):
    """PgBouncer paused at the start of the outage: the application's
    writes wait in it, the flip points it at the target, and they go on
    there - measured as how long the application's writes waited."""
    c = cut
    src_ip, dst_ip = _ip(SRC), _ip(DST)
    subprocess.run(["docker", "rm", "-f", "-v", BOUNCER],
                   capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", BOUNCER, "-p",
                    f"127.0.0.1:{BOUNCER_PORT}:5432", "-e",
                    "DB_USER=postgres", "-e", "DB_PASSWORD=test", "-e",
                    f"DB_HOST={src_ip}", "-e", "DB_NAME=postgres", "-e",
                    "POOL_MODE=transaction", "-e", "AUTH_TYPE=scram-sha-256",
                    "edoburu/pgbouncer:latest"], check=True,
                   capture_output=True)
    try:
        assert harness.wait_for(lambda: _console("show version"), 60)
        bouncer_ip = _ip(BOUNCER)
        flip = (f"docker exec {BOUNCER} sed -i"
                f" 's/host={src_ip} /host={dst_ip} /'"
                " /etc/pgbouncer/pgbouncer.ini && docker exec -e"
                f" PGPASSWORD=test {SRC} psql -h {bouncer_ip} -U postgres"
                " -d pgbouncer -c RELOAD")
        c.hop({"budget": 20, "flip": flip,
               "pooler": {"kind": "pgbouncer", "host": "127.0.0.1",
                          "port": BOUNCER_PORT, "user": "postgres",
                          "password": "test", "databases": ["postgres"]}})
        _follow(c)
        # the same password made again on the target is salted anew: the
        # pooler could not sign the application in there, and it is said
        # before anything is done
        secret = _one(c.pair["dst"], "select rolpassword from pg_authid"
                                     " where rolname = 'app'")
        _one(c.pair["dst"], "alter role app password 'CHANGE_ME'")
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code != 0, said
        assert "cannot sign in on the target" in said, said
        assert "nothing was written" in said, said
        _one(c.pair["dst"], f"alter role app password '{secret}'")
        w = harness.Writer(lambda where: c.connect(where, BOUNCER_PORT),
                           c.route)
        w.start()
        assert harness.wait_for(lambda: len(w.acked()) >= 30, 30), \
            w.errors()[-3:]
        try:
            got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
            assert got.exit_code == 0, said
            assert "pooler: paused" in said, said
            flipped = time.time()
            assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 30,
                                    30), w.errors()[-3:]
        finally:
            w.stop()
        print(f"\nthrough the pooler: {w.outage():.2f}s the application's"
              f" writes waited; {len(w.errors())} refused")
        assert w.errors() == [], w.errors()[:3]
        assert w.outage() < 20
        _nothing_lost_or_twice(c, w, c.pair["dst"])
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", BOUNCER],
                       capture_output=True)


def _console(sql):
    try:
        conn = psycopg2.connect(host="127.0.0.1", port=BOUNCER_PORT,
                                user="postgres", password="test",
                                dbname="pgbouncer", connect_timeout=3)
    except psycopg2.Error:
        return False
    conn.autocommit = True
    try:
        conn.cursor().execute(sql)
        return True
    finally:
        conn.close()


def _as(role, sql):
    got = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=CHANGE_ME", SRC, "psql", "-h",
         "127.0.0.1", "-U", role, "-d", "postgres", "-At", "-c", sql],
        capture_output=True, text=True)
    return got.returncode, (got.stdout + got.stderr).strip()


def test_a_role_that_owns_its_tables_is_kept_out_and_let_back(
        pg_pair, tmp_path):
    """An owner writes whatever is revoked, and the read-only default is
    one `begin read write` away - measured with the server's default on:
    a session's insert in `begin read write` went in. So on a
    source frozen per database it is kept from connecting, the session it
    had open is ended with what it had not committed, and everything is
    given back as it was."""
    from migkit import freeze
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    src = pg_pair["src"]
    psql(src, "drop table if exists public.owned; drop role if exists"
              " owner_app")
    _one(src, "create role owner_app login password 'CHANGE_ME';"
              " create table public.owned (v int);"
              " alter table public.owned owner to owner_app")
    ep = {"host": "127.0.0.1", "user": "postgres", "password": "test"}
    hop = Hop(name="pgown", engine="postgres",
              source=Endpoint(port=src, **ep),
              target=Endpoint(port=pg_pair["dst"], **ep),
              databases=["postgres"], options={"cutover": True})
    hop.report_dir = lambda db="": tmp_path
    eng = PostgresEngine(hop)
    try:
        held = subprocess.Popen(
            ["docker", "exec", "-e", "PGPASSWORD=CHANGE_ME", SRC, "psql",
             "-h", "127.0.0.1", "-U", "owner_app", "-d", "postgres", "-c",
             "begin; insert into public.owned values (1);"
             " select pg_sleep(20); commit;"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert harness.wait_for(lambda: _one(
            src, "select count(*) from pg_stat_activity where usename ="
                 " 'owner_app' and backend_xid is not null") == "1", 20)
        said = []
        freeze.freeze_source(hop, eng, ["postgres"], said.append, "database")
        assert held.wait(10) != 0, "its open session was ended"
        proved = freeze.proof(hop, eng, "postgres", "database")
        assert all(ok for ok, _ in proved), proved
        rc, out = _as("owner_app", "begin read write; insert into"
                                   " public.owned values (2); commit")
        assert rc != 0 and "permission denied" in out, out
        assert _one(src, "select count(*) from public.owned") == "0"
        # migkit's own account is not frozen
        assert _one(src, "select 1") == "1"
        freeze.thaw_source(hop, eng, ["postgres"], said.append)
        rc, out = _as("owner_app", "insert into public.owned values (3)")
        assert rc == 0, out
        assert freeze.state(hop, "postgres", "src") is None
        assert " ".join(said).count("kept from connecting") == 1, said
    finally:
        psql(src, "grant connect on database postgres to public")
        psql(src, "drop table if exists public.owned;"
                  " drop role if exists owner_app")


def test_an_account_that_cannot_freeze_is_named_before_anything_moves(
        pg_pair, tmp_path):
    """A source account that could revoke half and end no session would
    hold the application out and prove nothing: what it lacks is said in
    the preflight, and the cutover does not start."""
    from migkit import freeze
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    src = pg_pair["src"]
    psql(src, "drop table if exists public.theirs; drop role if exists"
              " migkit_lim")
    _one(src, "create role migkit_lim login password 'CHANGE_ME';"
              " create table public.theirs (v int)")

    def eng(user, password):
        hop = Hop(name="pglim", engine="postgres",
                  source=Endpoint(host="127.0.0.1", port=src, user=user,
                                  password=password),
                  target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                                  user="postgres", password="test"),
                  databases=["postgres"], options={"cutover": True})
        hop.report_dir = lambda db="": tmp_path
        return hop, PostgresEngine(hop)
    try:
        hop, lim = eng("migkit_lim", "CHANGE_ME")
        said = freeze.cannot(hop, lim, "postgres")
        assert any("cannot end the application's sessions" in s
                   for s in said), said
        assert any("does not own the database" in s for s in said), said
        assert any("grants on" in s and "tables it does not own" in s
                   for s in said), said
        _one(src, "grant pg_signal_backend to migkit_lim")
        said = freeze.cannot(hop, lim, "postgres")
        assert not any("end the application's sessions" in s
                       for s in said), said
        hop, sup = eng("postgres", "test")
        assert freeze.cannot(hop, sup, "postgres") == []
    finally:
        psql(src, "drop table if exists public.theirs;"
                  " drop role if exists migkit_lim")


# the stream back made ahead of the clock against one made at the flip:
# each way's run keeps its numbers here, and the test after both says
# the difference
_MEASURED = {}


@pytest.mark.parametrize("made", ["ahead", "at the flip"])
def test_the_stream_back_made_ahead_takes_less_of_the_outage(cut,
                                                              monkeypatch,
                                                              made):
    from migkit import cli
    c = cut
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"})
    if made == "at the flip":
        real = cli._stream_leg

        def at_the_flip(*a, **k):
            leg = real(*a, **k)
            leg.back_arm = leg.back_fire = leg.back_proof = None
            return leg
        monkeypatch.setattr(cli, "_stream_leg", at_the_flip)
    _follow(c)
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0, said
        flipped = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 30,
                                30), w.errors()[-3:]
    finally:
        w.stop()
    _nothing_lost_or_twice(c, w, c.pair["dst"])
    # made ahead or not, the stream back carries what the new primary took
    assert harness.wait_for(lambda: sorted(_tags(c.pair["src"]))
                            == sorted(_tags(c.pair["dst"]))), \
        "the old source did not follow the new primary"
    run = [json.loads(x) for x in
           (c.reports / "cutover-runs.jsonl").read_text().splitlines()][-1]
    assert run["back"] == ("armed" if made == "ahead" else "at_flip"), run
    assert ("prepare" in run["took"]) == (made == "ahead"), run["took"]
    _MEASURED[made] = {"outage": w.outage(), "run": run["outage"],
                       "stream": run["took"]["stream"],
                       "reverse": run["took"]["reverse"]}
    print(f"\nthe stream back made {made}: {w.outage():.2f}s without an"
          f" acknowledged write; stream {run['took']['stream']:.2f}s,"
          f" reverse {run['took']['reverse']:.2f}s")


def test_the_measured_difference_of_the_stream_back_made_ahead():
    if len(_MEASURED) < 2:
        pytest.skip("measured by the two runs above, in this session")
    ahead, flip = _MEASURED["ahead"], _MEASURED["at the flip"]
    print(f"\noutage: made ahead {ahead['outage']:.2f}s, at the flip"
          f" {flip['outage']:.2f}s; the stream step {ahead['stream']:.2f}s"
          f" against {flip['stream']:.2f}s")
    # the flip starts what was made ahead: it does not make it
    assert ahead["stream"] < flip["stream"], _MEASURED


def test_the_point_of_no_return_gives_the_old_source_back(cut):
    c = cut
    key, signers = _approver(c.tmp, "ann")
    c.hop({"budget": 20, "flip": f"printf dst > {c.route}"},
          approvals={"steps": ["point-of-no-return"],
                     "signers": str(signers)})
    _follow(c)
    w = _writing(c)
    try:
        got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
        assert got.exit_code == 0 and "cut over:" in said, said
        flipped = time.time()
        assert harness.wait_for(lambda: len(w.oks_after(flipped)) >= 20,
                                30), w.errors()[-3:]
    finally:
        w.stop()
    frozen = _state(c.pair["src"])
    # shown, and nothing done
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop")
    assert got.exit_code == 0, said
    assert "point of no return" in said and "the stream back: dropped" \
        in said, said
    assert _state(c.pair["src"]) == frozen
    # asked for its own approval, and nothing done without it
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
    assert got.exit_code != 0 and "needs 1 approval and has 0" in said, said
    assert _state(c.pair["src"]) == frozen
    request = c.reports / "approvals" / "point-of-no-return-all.request.json"
    (request.parent / "point-of-no-return-all.ann.sig").write_bytes(
        _sign(key, request))
    got, said = _cli("move", HOP, "--mode", "cdc", "--drop", "--go")
    assert got.exit_code == 0 and "past the point of no return" in said, said
    # the old source is the application's again, and follows nothing
    conn = c.connect("src")
    conn.cursor().execute("insert into public.orders (tag) values"
                          " ('given back')")
    conn.commit()
    conn.close()
    assert _one(c.pair["src"], "select count(*) from pg_subscription") == "0"
    assert harness.wait_for(lambda: _one(
        c.pair["dst"], "select count(*) from pg_replication_slots") == "0",
        30), "the stream back's slot is left on the new primary"
    assert "given back" not in _tags(c.pair["dst"])
    # going back is refused from here
    got, said = _cli("rollback", HOP, "--db", "postgres", "--apply")
    assert got.exit_code != 0 and "past the point of no return" in said, said
    runs = [json.loads(x) for x in
            (c.reports / "cutover-runs.jsonl").read_text().splitlines()]
    assert [r["state"] for r in runs][-2:] == ["flipped", "no-return"]
    assert runs[-1]["approved_by"] == ["ann"]
