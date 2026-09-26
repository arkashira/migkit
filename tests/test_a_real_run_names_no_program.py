"""What an operator actually reads during a real move names no program.

The static scan reads the strings written into `Result`, `SystemExit`,
`print` and `log` calls. It cannot see a name that arrives through a
variable - which is how `the pgdump bulk path needs pg_dump`, `$ pg_dump
-h ...` and `pgcopydb cannot reach both endpoints` reached the screen. So
this runs the command itself, against a live pair, down every PostgreSQL
path, and reads everything it printed.
"""
import pytest

from tests.conftest import needs_docker, psql
from tests.test_the_report_does_not_name_its_tools import TOOLS

pytestmark = needs_docker

#: labels inside migkit that mean nothing to an operator either
LABELS = ("pgdump", "builtin", "mover")


def _run(tmp_path, monkeypatch, pg_pair, via, go):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lab:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 2\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", via)
    argv = ["move", "lab", "--mode", "full"] + (["--go"] if go else [])
    return CliRunner().invoke(cli.main, argv)


def _seed(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.orders (id int primary key,"
                   " v text)")
    psql(pg_pair["src"], "insert into public.orders select g, 'v'||g"
                         " from generate_series(1, 500) g")


@pytest.mark.parametrize("via", ["pgdump", "pgcopydb", "builtin"])
@pytest.mark.parametrize("go", [False, True])
def test_every_postgres_path_speaks_in_migkit_words(pg_pair, tmp_path,
                                                    monkeypatch, via, go):
    from migkit import movers
    if via == "pgcopydb" and not movers.pgcopydb_available():
        pytest.skip("the streaming copy is not available on this machine")
    _seed(pg_pair)
    got = _run(tmp_path, monkeypatch, pg_pair, via, go)
    said = got.output + str(got.exception or "")
    assert got.exit_code == 0, said
    low = said.lower()
    assert not [t for t in TOOLS + LABELS if t in low], said
    assert ":test@" not in said and "password" not in low, said
    if go:
        rows = psql(pg_pair["dst"], "select count(*) from public.orders")
        assert rows.stdout.strip() == "500", said


def test_the_dump_path_says_how_far_it_has_got(pg_pair, tmp_path,
                                               monkeypatch):
    """Each table read and each table loaded is a line in migkit's words,
    taken from what the programs underneath print as they go."""
    for port in (pg_pair["src"], pg_pair["dst"]):
        for t in ("a", "b", "c"):
            psql(port, f"create table public.{t} (id int primary key)")
    for t in ("a", "b", "c"):
        psql(pg_pair["src"], f"insert into public.{t} values (1), (2)")
    got = _run(tmp_path, monkeypatch, pg_pair, "pgdump", True)
    said = got.output + str(got.exception or "")
    assert got.exit_code == 0, said
    for t in ("a", "b", "c"):
        assert f"public.{t}: read (" in said, said
        assert f"public.{t}: loaded (" in said, said
    # counted against the source's own catalogue: tables and their bytes
    # (backlog 0c), where the program's lines alone gave neither total
    assert "3 of 3 tables, " in said and " (100%)" in said, said
