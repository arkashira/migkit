"""The PostgreSQL table copier keeps the target's triggers quiet too.

The bulk paths load as a replica, which a trigger does not fire for; the
table copier - a single table's move, the tables routed around the bulk
copy, the resumable path - loaded as an ordinary session. Measured: a
`BEFORE INSERT` trigger setting `at = now()` rewrote every row it copied,
and the move said complete. It loads as a replica now, where the target's
user may, and is refused before copying where it may not and the table has
triggers.
"""
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def test_the_copier_leaves_the_rows_as_the_source_dated_them(
        pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.notes (id int primary key,"
                   " at timestamp)")
    psql(pg_pair["src"], "insert into public.notes values"
                         " (1, '2001-01-01'), (2, '2002-02-02')")
    psql(pg_pair["dst"], "create function public.stamp() returns trigger"
                         " language plpgsql as $$ begin new.at := now();"
                         " return new; end $$; create trigger stamp before"
                         " insert on public.notes for each row execute"
                         " function public.stamp()")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  pq:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    try:
        got = CliRunner().invoke(cli.main, ["move", "pq", "--go"])
        said = got.output + str(got.exception or "")
        assert got.exit_code == 0, said
        assert psql(pg_pair["dst"], "select string_agg(at::date::text, ','"
                    " order by id) from public.notes").stdout.strip() == \
            "2001-01-01,2002-02-02"
    finally:
        psql(pg_pair["dst"], "drop function if exists public.stamp()"
                             " cascade")
