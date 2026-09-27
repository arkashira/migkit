"""The files migkit writes that hold the application's values are kept
encrypted to the recipients the hop names (backlog R17b): a drilldown's
keys, a repair's undo statements, a two-way tail's conflicts.

Asked by the owner: a database's rows should reach nobody who was not
meant to have them. Before, every one of those files lay in the reports in
the clear - on a laptop, in a backup of it, in a bundle attached to a
ticket. Now they are age files to the hop's SSH or age keys, read back
with the operator's own (`MIGKIT_IDENTITY`), and a key that is not one of
the recipients reads nothing.
"""
import sqlite3
import subprocess

import pytest

from migkit import evidence


def _key(tmp_path, name):
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(tmp_path / name), "-C", name], check=True)
    return (tmp_path / f"{name}.pub").read_text().strip()


def _sealed(raw):
    """A whole age file, or a record sealed a line at a time."""
    return raw.startswith(evidence.AGE) or (bool(raw) and all(
        line.startswith(evidence.LINE.encode())
        for line in raw.splitlines()))


def _run(*argv):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    for path, rows in ((src, [(1, "ann"), (2, "bo"), (3, "cy")]),
                       (dst, [(1, "ann"), (2, "BO")])):
        with sqlite3.connect(path) as con:
            con.execute("create table people (id integer primary key,"
                        " name text)")
            con.executemany("insert into people values (?, ?)", rows)
    ann = _key(tmp_path, "ann")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}}}\n    target: {{host: {dst}}}\n"
        "    databases: [main]\n"
        f"    options: {{at_rest: {{recipients: ['{ann}']}}}}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "ann"))
    return tmp_path


def test_a_drilldown_is_written_sealed_and_read_back(lite):
    got, said = _run("check", "lite")
    drills = list((lite / "reports" / "lite").rglob("data-*"))
    assert drills, said
    for f in drills:
        raw = f.read_bytes()
        assert raw.startswith(evidence.AGE), (f, raw[:40])
        assert b"cy" not in raw and b"BO" not in raw
    # the repair reads them back through the same paths, and puts the
    # target right
    got, said = _run("sync", "lite", "--apply")
    assert got.exit_code == 0, said
    with sqlite3.connect(lite / "b.db") as con:
        assert con.execute("select id, name from people order by id"
                           ).fetchall() == [(1, "ann"), (2, "bo"),
                                            (3, "cy")]
    undo = list((lite / "reports" / "lite").rglob("undo/*"))
    assert undo and all(_sealed(f.read_bytes()) for f in undo), [
        f.read_bytes()[:60] for f in undo]


def test_a_key_that_is_not_a_recipient_reads_nothing(lite, monkeypatch):
    _run("check", "lite")
    _key(lite, "eve")
    monkeypatch.setenv("MIGKIT_IDENTITY", str(lite / "eve"))
    f = next((lite / "reports" / "lite").rglob("data-*"))
    from migkit.config import get_hop
    path = get_hop("lite").report_dir("main") / f.name
    with pytest.raises(SystemExit, match="not one of them"):
        path.read_text()


def test_a_line_at_a_time_record_is_sealed_line_by_line(tmp_path):
    from migkit.config import Endpoint, Hop
    ann = _key(tmp_path, "ann")
    hop = Hop(name="x", engine="sqlite", source=Endpoint(host="a"),
              target=Endpoint(host="b"),
              options={"at_rest": {"recipients": [ann]}})
    root = evidence.report_path(hop, tmp_path / "r")
    (tmp_path / "r").mkdir()
    import os
    os.environ["MIGKIT_IDENTITY"] = str(tmp_path / "ann")
    try:
        with (root / "conflicts.jsonl").open("a") as f:
            f.write('{"n": 1, "v": "secret one"}\n')
        with (root / "conflicts.jsonl").open("a") as f:
            f.write('{"n": 2, "v": "secret two"}\n')
        raw = (tmp_path / "r" / "conflicts.jsonl").read_bytes()
        assert b"secret" not in raw and raw.count(b"\n") == 2
        assert (root / "conflicts.jsonl").read_text().splitlines() == [
            '{"n": 1, "v": "secret one"}', '{"n": 2, "v": "secret two"}']
        # a file that holds no values is written as it was
        (root / "summary.json").write_text("{}")
        assert (tmp_path / "r" / "summary.json").read_text() == "{}"
    finally:
        del os.environ["MIGKIT_IDENTITY"]
