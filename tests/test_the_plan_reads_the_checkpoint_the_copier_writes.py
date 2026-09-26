"""The dry run of a move reads the checkpoint under the names the copier
writes it with.

The plan looked entries up as `schema.table`; every copier but
PostgreSQL's writes `database.table`, and Redis `db<n>`. So a copy half
done, or done, read as `todo` on every other engine, and a copy resumed
by a text key would have stopped the plan on formatting it as a number.
"""
import json
import sqlite3

from click.testing import CliRunner


def test_done_and_half_done_are_read_back(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (k text primary key)")
        con.execute("create table u (id integer primary key)")
        con.commit()
        con.close()
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    ck = tmp_path / "reports" / "lite" / "main" / "move.json"
    ck.parent.mkdir(parents=True)
    ck.write_text(json.dumps({"main.t": {"last": ["k-0042"], "moved": 42},
                              "main.u": {"done": True}}))
    got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full"])
    said = " ".join(got.output.split())
    assert got.exit_code == 0, (said, got.exception)
    assert "main: 2 tables, 1 already done in checkpoint" in said, said
    assert ".t: resume after ['k-0042']" in said, said
    assert ".u: done" in said, said
