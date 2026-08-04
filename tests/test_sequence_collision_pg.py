"""The single most common migration outage: DMS/DTS copy rows with their id
values but never advance the target's sequence, so the first insert after
cutover duplicate-keys. migkit must catch this as a USABLE failure (not just a
parity mismatch), repair it to GREATEST(source, target-max) so nextval clears
the column with no wasted gap, and REFUSE to bump silently when the target has
writes the source doesn't (a writer landed on the target)."""
import os
import subprocess
import textwrap

from tests.conftest import needs_docker, psql, MIGKIT

pytestmark = needs_docker


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [MIGKIT, *args],
        capture_output=True, text=True, env=env)


def _conf(tmp_path, pg_pair):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    return conf


def test_sequence_collision_detected_and_repaired(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    # source: serial pk advanced to 1000, with gaps from deletes (101, 330)
    psql(pg_pair["src"], "create table orders(id serial primary key, amt int);"
         " insert into orders(amt) select g from generate_series(1,1000) g;"
         " delete from orders where id in (101,330)")
    # target: DMS-style - rows copied WITH their ids, but the sequence is left
    # sitting at 1 (DMS migrates data + pk, never the sequence state)
    psql(pg_pair["dst"], "create table orders(id serial primary key, amt int);"
         " insert into orders(id,amt) select g,g from generate_series(1,1000) g;"
         " delete from orders where id in (101,330);"
         " select setval('orders_id_seq', 1, false)")

    # the killer must surface as a USABLE (will-collide) verdict
    r = _migkit(conf, "check", "t", "--only", "autoinc")
    assert r.returncode == 1, r.stdout
    assert "collide" in r.stdout.lower(), r.stdout

    # repair sets GREATEST(source=1000, target-max=1000)=1000 -> nextval 1001
    r = _migkit(conf, "sync", "t", "--db", "postgres",
                "--kind", "sequences", "--apply")
    assert "setval('public.orders_id_seq', 1000" in r.stdout, r.stdout

    # re-check is green: usable (1001 > 1000) and parity (dst == src == 1000)
    r = _migkit(conf, "check", "t", "--only", "autoinc")
    assert r.returncode == 0, r.stdout

    # prove it: next value clears the column max, a real insert does not collide
    nxt = psql(pg_pair["dst"], "select nextval('orders_id_seq')").stdout.strip()
    assert nxt == "1001", nxt
    psql(pg_pair["dst"], "insert into orders(amt) values (1)")
    mx = psql(pg_pair["dst"], "select max(id) from orders").stdout.strip()
    assert int(mx) >= 1002, mx


def test_sequence_repair_refuses_when_target_ahead(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    # source sequence at 1000
    psql(pg_pair["src"], "create table orders(id serial primary key, amt int);"
         " insert into orders(amt) select g from generate_series(1,1000) g")
    # target has a row (id=2000) beyond anything on source: a writer landed here
    psql(pg_pair["dst"], "create table orders(id serial primary key, amt int);"
         " insert into orders(id,amt) select g,g from generate_series(1,1000) g;"
         " insert into orders(id,amt) values (2000,1)")

    _migkit(conf, "check", "t", "--only", "autoinc")  # writes summary
    # dry-run repair must refuse rather than silently bump past the source
    r = _migkit(conf, "sync", "t", "--db", "postgres", "--kind", "sequences")
    assert "REFUSED" in r.stdout, r.stdout
    assert "2000" in r.stdout, r.stdout
