"""A lossy transcode on load leaves U+FFFD replacement characters scattered
through text - the rows are 'present' so counts pass, but the data is quietly
corrupted. The deep check counts U+FFFD per text column and flags any excess on
the target over the source."""
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


def test_replacement_char_excess_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    psql(pg_pair["src"], "create table t(id int primary key, name text);"
         " insert into t values (1,'clean'),(2,'ok')")
    # target: value 1 got a replacement char from a lossy load
    psql(pg_pair["dst"], "create table t(id int primary key, name text);"
         " insert into t values (1,'clean'||U&'\\FFFD'),(2,'ok')")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "encoding" in out and ("u+fffd" in out or "replacement" in out), r.stdout
    assert "t.name" in r.stdout, r.stdout
