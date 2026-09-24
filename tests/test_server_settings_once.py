"""Server-wide settings are compared once per check, not once per database.

A MySQL hop moving twenty databases reported the same `show global
variables` difference twenty times, and made twenty pairs of reads to find
it. The settings belong to the server, so one comparison answers for every
database. The other databases say where the comparison was made. Engines
whose settings a database can override (PostgreSQL's `alter database
set`, a SQLite file's own pragmas) still compare every database.
"""
from click.testing import CliRunner


def _run(tmp_path, monkeypatch, engine_cls, engine, extra=""):
    import migkit.config as cfg
    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        f"hops:\n  h:\n    engine: {engine}\n"
        "    source: {host: 10.0.0.1, port: 3306, user: u,"
        " password: CHANGE_ME}\n"
        "    target: {host: 10.0.0.2, port: 3306, user: u,"
        " password: CHANGE_ME}\n"
        "    databases: [a, b, c]\n" + extra)
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    asked = []

    def params(self, db):
        from migkit.engines.base import Result
        asked.append(db)
        return [Result("params", f"{db} params", "diff", "time_zone differs")]
    monkeypatch.setattr(engine_cls, "check_params", params)
    got = CliRunner().invoke(cli.main, ["check", "h", "--only", "params",
                                        "--workers", "1"])
    return asked, " ".join(got.output.split())


def test_a_server_wide_setting_is_compared_once(tmp_path, monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    asked, said = _run(tmp_path, monkeypatch, MySQLEngine, "mysql")
    assert asked == ["a"], asked
    assert said.count("time_zone differs") == 1, said
    assert "compared under a" in said, said


def test_a_per_database_setting_is_compared_in_each(tmp_path, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    asked, said = _run(tmp_path, monkeypatch, PostgresEngine, "postgres")
    assert sorted(asked) == ["a", "b", "c"], asked
