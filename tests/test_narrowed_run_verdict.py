"""What a narrowed check says about the part it never opened.

Found by running two commands that should not be able to agree, on one
pair: `good` holds 100 rows on both sides, `bad` holds 100 on the source
and 40 on the target.

    check sc --only data                     data  DIFF public.bad
                                             verdict: different
    check sc --table public.good --only data data  OK missing=0 extra=0
                                             verdict: same

The second is not wrong about the table it was asked about. The artifact
is. With the timestamp, fingerprint and tool version removed, that run's
`verdict.json` is **byte-identical** to one from a genuinely clean database
checked in full - same `status`, same `has_differences`, same totals, with
60 rows missing from a table the run never opened. A CI gate reading
`has_differences == false` passes both.

`test_the_narrowed_verdict_is_indistinguishable` is the unusual one here:
it pins behaviour that is **wrong**, so the measurement cannot quietly
decay into a memory. It is written to fail the moment the envelope learns
to describe its own coverage, and its docstring says what to replace it
with. The fix is designed in catalogue D14 and deliberately not shipped in
the same tick as the measurement - 24 test files invoke `--only` or
`--table`, and changing the artifact every check writes deserves the whole
suite rather than two adjacent files.

The other two tests pin what migkit already gets right, so that fix cannot
take them away by accident.
"""
import json
import socket
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15533, 15534
NAMES = {SRC: "migkit-test-nar-src", DST: "migkit-test-nar-dst"}


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", "-i", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)


@pytest.fixture(scope="module")
def nar_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _seed(pair, bad_rows_on_target):
    both = ("drop table if exists good; drop table if exists bad;"
            " create table good (id int primary key, v text);"
            " insert into good select g, 'row'||g"
            " from generate_series(1,100) g;"
            " create table bad (id int primary key, v text);")
    got = q(pair["src"], both + " insert into bad select g, 'row'||g"
                                " from generate_series(1,100) g;")
    assert got.returncode == 0, got.stderr
    got = q(pair["dst"], both + " insert into bad select g, 'row'||g from"
                                f" generate_series(1,{bad_rows_on_target}) g;")
    assert got.returncode == 0, got.stderr


def _run(pair, tmp_path, monkeypatch, args, reports):
    from click.testing import CliRunner

    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  sc:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / reports)
    out = CliRunner().invoke(cli.main, args).output
    env = json.loads((tmp_path / reports / "sc" / "verdict.json").read_text())
    for noise in ("generated", "fingerprint", "tool_version"):
        env.pop(noise, None)
    return out, env


def test_the_pair_really_is_broken_and_the_narrow_one_really_is_clean(
        nar_pair, tmp_path, monkeypatch):
    """The control. Without it the comparison below could hold because
    nothing was ever wrong."""
    _seed(nar_pair, 40)
    counts = [q(p, "select count(*) from bad").stdout.strip()
              for p in (nar_pair["src"], nar_pair["dst"])]
    assert counts == ["100", "40"], counts

    out, env = _run(nar_pair, tmp_path, monkeypatch,
                    ["check", "sc", "--only", "data"], "full")
    assert env["status"] == "different", env
    assert "public.bad" in out, out


def test_the_narrowed_verdict_is_indistinguishable(nar_pair, tmp_path,
                                                     monkeypatch):
    """**This pins a defect, on purpose.**

    When `verdict.summarize` learns to record what a run covered, this test
    fails - and that is the signal to replace it with its opposite: assert
    that the narrowed envelope carries a `coverage` block naming
    `public.good`, and that its status is `incomplete` rather than `same`.
    Written this way because a measurement nobody re-runs becomes a memory,
    and this one is the whole argument for the change.
    """
    _seed(nar_pair, 40)
    _, narrowed = _run(nar_pair, tmp_path, monkeypatch,
                       ["check", "sc", "--table", "public.good", "--only",
                        "data"], "narrow")
    assert narrowed["status"] == "same", narrowed
    assert narrowed["has_differences"] is False, narrowed

    # now a database where nothing is wrong at all, checked in full
    for port in NAMES:
        assert q(port, "drop table bad").returncode == 0
    _, healthy = _run(nar_pair, tmp_path, monkeypatch,
                      ["check", "sc", "--only", "data"], "healthy")
    assert healthy["status"] == "same", healthy

    assert narrowed == healthy, (
        "the defect this pins has been fixed - replace this test with the"
        " assertion that the narrowed run records its coverage")


def test_a_mistyped_table_errors_rather_than_passing(nar_pair, tmp_path,
                                                       monkeypatch):
    """Already right, pinned so the coverage fix cannot take it away. A
    filter that matches nothing is the classic way a verifier reports a
    clean nothing."""
    _seed(nar_pair, 40)
    out, env = _run(nar_pair, tmp_path, monkeypatch,
                    ["check", "sc", "--table", "public.gooood", "--only",
                     "data"], "typo")
    assert env["status"] == "error", env
    assert env["has_differences"] is True, env
    assert "ERROR" in out, out


def test_counts_still_sweeps_the_whole_database(nar_pair, tmp_path,
                                                  monkeypatch):
    """Also already right: `--table` narrows the checksum pass but not the
    counts, which is why hiding `bad` above needed `--only data` as well.
    Pinned because it is the reason a narrowed run is usually still
    honest."""
    _seed(nar_pair, 40)
    out, env = _run(nar_pair, tmp_path, monkeypatch,
                    ["check", "sc", "--table", "public.good", "--only",
                     "counts,data"], "sweep")
    assert "public.bad src=100 dst=40" in out, out
    assert env["status"] == "different", env
