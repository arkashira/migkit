"""How long a move will take, from how long the last one on the same path
took (backlog 7 and 40).

Nothing is assumed about throughput. A finished copy records the rows it
carried and the time it took, for its path and its hop; the next plan
divides the rows it is about to carry by that rate. Before any copy has
been measured, the plan says so instead of inventing a number.
"""
import sqlite3


def _hop(tmp_path, monkeypatch, rows):
    import migkit.config as cfg
    src = tmp_path / "a.db"
    con = sqlite3.connect(src)
    con.execute("create table t (id integer primary key, v text)")
    con.executemany("insert into t values (?, ?)",
                    [(i, "x") for i in range(rows)])
    con.commit()
    con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _move(*extra):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full",
                                        *extra])
    return got, " ".join(got.output.split())


def test_before_any_copy_no_time_is_invented(tmp_path, monkeypatch):
    _hop(tmp_path, monkeypatch, 500)
    got, said = _move()
    assert got.exit_code == 0, said
    assert "about 500 rows; no copy on this path measured yet" in said, said


def test_after_a_copy_the_next_plan_has_a_time(tmp_path, monkeypatch):
    _hop(tmp_path, monkeypatch, 500)
    got, said = _move("--go")
    assert got.exit_code == 0, said
    got, said = _move()
    assert "rows/s measured on this path last time" in said, said


def test_the_estimate_divides_rows_by_the_measured_rate(tmp_path):
    from migkit import planner
    from migkit.config import Endpoint, Hop
    hop = Hop(name="e", engine="postgres",
              source=Endpoint(host="a", port=1, user="u", password="p"),
              target=Endpoint(host="b", port=1, user="u", password="p"))
    hop.report_dir = lambda db=None: tmp_path
    planner.record_rate(hop, "bulk", 1_000_000, 100)
    got = planner.estimate(hop, "bulk", 6_000_000)
    assert "about 10min at the 10,000 rows/s" in got, got
    assert "no copy on this path" in planner.estimate(hop, "other", 5)
