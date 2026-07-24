"""CLI surface tests: exactly 11 visible commands, legacy names stay
invocable as hidden aliases, quiet flag, deep/counts-merge plumbing.
No database or docker needed."""
from click.testing import CliRunner

from migkit.cli import main

VISIBLE = {"doctor", "advise", "assess", "schema", "check", "move",
           "watch", "sync", "report", "history", "rollback"}
LEGACY = {"hops", "setup-target", "repair", "replicate", "tail",
          "convert-schema", "gen-migration", "sample-diff", "ui",
          "state", "monitor"}


def test_visible_commands_are_the_eleven():
    listed = {n for n, c in main.commands.items() if not c.hidden}
    assert listed == VISIBLE


def test_legacy_names_still_resolve_hidden():
    for name in LEGACY:
        cmd = main.commands[name]
        assert cmd.hidden, name
        r = CliRunner().invoke(main, [name, "--help"])
        assert r.exit_code == 0, name


def test_quiet_flag_accepted():
    r = CliRunner().invoke(main, ["-q", "--help"])
    assert r.exit_code == 0


def test_base_engine_deep_defaults_to_skip():
    from migkit.engines.base import Engine
    r = Engine(None).check_deep("x")[0]
    assert r.check == "deep"
    assert r.status == "skip"


def test_counts_ride_along_with_checksum_engines():
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    assert PostgresEngine.counts_from_data
    assert MySQLEngine.counts_from_data
    assert MongoEngine.counts_from_data


def test_pg_fast_output_parses_counts():
    from migkit.engines.postgres import PostgresEngine
    out = ("public.a: OK rows=100 checksum=123 (3s)\n"
           "public.b: DIFF src=50|9f dst=49|a0 (2s)\n"
           "public.c: ERROR (see above)")
    n, rs, rd, bad = PostgresEngine._parse_fast(out)
    assert (n, rs, rd) == (2, 150, 149)
    assert bad == ["public.b src=50 dst=49"]


def test_check_drill_requires_db_and_table(tmp_path):
    import os
    import subprocess
    import textwrap
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent("""
        hops:
          t:
            engine: postgres
            source: {host: h, user: u, password: p}
            target: {host: h, user: u, password: p}
    """))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(tmp_path / "reports"))
    r = subprocess.run([os.path.join(base, ".venv", "bin", "migkit"),
                        "check", "t", "--drill"],
                       capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "--db and --table" in (r.stdout + r.stderr)
