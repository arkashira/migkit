"""The settings a SQLite file carries in itself, compared like every
engine's server settings.

`user_version` is where applications keep their schema version. A target
file that carries another one has the application's migrations run again
on start, or refused - and nothing compared it.
"""
import sqlite3

from migkit.config import Endpoint, Hop


def _engine(tmp_path, src_version, dst_version, dst_exists=True):
    from migkit.engines.sqlite import SQLiteEngine
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    for path, version in ((src, src_version), (dst, dst_version)):
        if path == dst and not dst_exists:
            continue
        con = sqlite3.connect(path)
        con.execute("create table t (id integer primary key)")
        con.execute(f"pragma user_version = {version}")
        con.commit()
        con.close()
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              db_map={"main": "main"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return SQLiteEngine(hop)


def _said(got):
    return " | ".join(f"{r.status} {r.detail}" for r in got)


def test_a_different_user_version_fails_the_check(tmp_path):
    got = _engine(tmp_path, 7, 3).check_params("main")
    assert any(r.status == "diff" for r in got), _said(got)
    assert "user_version" in _said(got), _said(got)


def test_the_same_settings_pass(tmp_path):
    got = _engine(tmp_path, 7, 7).check_params("main")
    assert got and all(r.status == "ok" for r in got), _said(got)


def test_a_file_that_is_not_there_is_not_a_match(tmp_path):
    got = _engine(tmp_path, 7, 7, dst_exists=False).check_params("main")
    assert got and all(r.status != "ok" for r in got), _said(got)


def test_a_plain_check_compares_the_settings_without_being_asked(
        tmp_path, monkeypatch):
    """They were compared only under `--only params`, so a check run the
    ordinary way never looked at them."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    _engine(tmp_path, 7, 3)
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got = CliRunner().invoke(cli.main, ["check", "lite"])
    said = " ".join(got.output.split())
    assert "user_version" in said, said
