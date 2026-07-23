"""Realistic-data integration: Faker generates every column shape, we load
identical data both sides plus deliberate diffs, and assert migkit's verify
and repair handle bytea, jsonb, unicode, nulls and PK gaps correctly.
"""
import os
import subprocess
import tempfile
import textwrap

from tests import datagen
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [os.path.join(base, ".venv", "bin", "migkit"), *args],
        capture_output=True, text=True, env=env)


def _conf(tmp_path, ports):
    c = tmp_path / "hops.yaml"
    c.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {ports['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {ports['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    return c


def _load(port, rows):
    container = f"migkit-test-pg-{'src' if port == 55432 else 'dst'}"
    body = datagen.pg_copy_body(rows)
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
        f.write(body)
        path = f.name
    subprocess.run(["docker", "cp", path, f"{container}:/tmp/load.csv"],
                   check=True, capture_output=True)
    r = subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", container, "psql",
         "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1",
         "-c", "\\copy people from '/tmp/load.csv' (format csv, null '')"],
        capture_output=True, text=True)
    os.unlink(path)
    assert r.returncode == 0, r.stderr


def test_faker_identical_data_verifies_clean(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    # id 500 deleted on both sides: the gap must be identical
    rows = datagen.rows(2000, skip_ids={500})
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, datagen.PG_DDL)
        _load(p, rows)

    r = _migkit(conf, "check", "t", "--only", "counts,data")
    assert r.returncode == 0, r.stdout
    assert "DIFF" not in r.stdout
    assert "2000" in r.stdout


def test_faker_diffs_detected_and_repaired(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    src_rows = datagen.rows(1500)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, datagen.PG_DDL)
    _load(pg_pair["src"], src_rows)
    # target: drop 3 rows (missing), keep an extra, mutate one (changed)
    dst_rows = [r for r in src_rows if r["id"] not in (10, 20, 30)]
    for r in dst_rows:
        if r["id"] == 40:
            r["note"] = "MUTATED on target"
    dst_rows.append(dict(src_rows[0], id=99999, note="extra only on target"))
    _load(pg_pair["dst"], dst_rows)

    r = _migkit(conf, "check", "t", "--only", "data")
    assert "DIFF" in r.stdout

    r = _migkit(conf, "repair", "t", "--db", "postgres", "--kind", "rows",
                "--apply")
    assert "applied" in r.stdout

    r = _migkit(conf, "check", "t", "--only", "counts,data")
    assert r.returncode == 0, r.stdout


def test_faker_move_roundtrips_all_types(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    rows = datagen.rows(3000, skip_ids={7, 77, 777})
    psql(pg_pair["src"], datagen.PG_DDL)
    _load(pg_pair["src"], rows)
    psql(pg_pair["dst"], datagen.PG_DDL)

    r = _migkit(conf, "move", "t", "--table", "public.people", "--go",
                "--chunk", "1000")
    assert "move complete" in r.stdout, r.stdout + r.stderr

    r = _migkit(conf, "check", "t", "--only", "counts,data")
    assert r.returncode == 0, r.stdout
    # verify a jsonb + bytea row survived byte-for-byte
    a = psql(pg_pair["src"],
             "select md5(payload) from people where id = 100").stdout
    b = psql(pg_pair["dst"],
             "select md5(payload) from people where id = 100").stdout
    assert a == b and a.strip()
