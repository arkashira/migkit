"""Orphans behind a validated foreign key are looked for.

PostgreSQL's orphan scan read only NOT VALID keys, on the grounds that a
validated key cannot have orphans under it. But every load migkit makes
writes as a replica, or with the triggers off, and a foreign key is a
trigger: it does not look. Measured: under a row filter that keeps one
parent and both children, the child whose parent stayed behind landed
behind a validated key. The check read "all fk constraints validated, no
orphans possible". Every key is scanned now, each within a time budget.
"""
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_an_orphan_a_filter_made_is_found(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    ddl = ("create table public.parent (id int primary key);"
           " create table public.child (id int primary key,"
           "  pid int references public.parent (id))")
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, ddl)
    psql(pg_pair["src"], "insert into public.parent values (1), (2);"
                         " insert into public.child values (1, 1), (2, 2)")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  fo:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        "    mapping:\n      where:\n        parent: \"id <= 1\"\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    try:
        got, said = _run("move", "fo", "--go")
        assert got.exit_code == 0, said
        # the premise: the key is validated, and the child is there anyway
        got = psql(pg_pair["dst"], "select convalidated from pg_constraint"
                                   " where conname = 'child_pid_fkey';"
                                   " select count(*) from public.child")
        assert got.stdout.split() == ["t", "2"], got.stdout
        got, said = _run("check", "fo", "--only", "deep")
        assert "child_pid_fkey: 1 orphan rows behind a validated key" in \
            said, said
    finally:
        for port in (pg_pair["src"], pg_pair["dst"]):
            psql(port, "drop table if exists public.child, public.parent")
