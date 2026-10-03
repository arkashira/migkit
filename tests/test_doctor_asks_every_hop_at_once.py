"""`doctor` (audit V6, V12, V17): every hop and each hop's two sides asked
at once, with one engine a hop; a reader told by what the server says of
itself on every engine that says; and a side that fails ends the command
as a stop.

It asked every hop and side one after another - a host that did not
answer cost its whole connect wait each time - built three engines a hop,
asked PostgreSQL alone whether it was a reader, and ended 0 whatever it
found.
"""
import threading

import pytest

from migkit import cli, verdict


@pytest.fixture
def two_hops(tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n"
        + "".join(f"  {n}:\n    engine: sqlite\n"
                  f"    source: {{path: {tmp_path / (n + '-a.db')}}}\n"
                  f"    target: {{path: {tmp_path / (n + '-b.db')}}}\n"
                  "    databases: [main]\n" for n in ("one", "two")))
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    from migkit import tools
    monkeypatch.setattr(tools, "capabilities", lambda: [])
    monkeypatch.setattr(tools, "schema_reading_note", lambda: "")


def _doctor():
    from click.testing import CliRunner
    got = CliRunner().invoke(cli.main, ["doctor"])
    return got, got.output


def test_every_hop_is_asked_at_once(two_hops, monkeypatch):
    both = threading.Barrier(2, timeout=10)

    def hop(name, h, raw):
        # one after another, the first waits here alone and gives up
        both.wait()
        return [f"{name}: asked"], False
    monkeypatch.setattr(cli, "_doctor_hop", hop)
    got, said = _doctor()
    assert got.exit_code == 0, said
    assert "one: asked" in said and "two: asked" in said, said


def test_a_side_that_fails_ends_doctor_as_a_stop(two_hops, monkeypatch):
    monkeypatch.setattr(cli, "_doctor_hop", lambda name, h, raw: (
        [f"{name} src: [red]FAIL[/red] nothing answers"], name == "two"))
    got, said = _doctor()
    assert got.exit_code == verdict.EXIT["failed"], said


def test_a_reader_is_told_by_what_the_server_says(monkeypatch):
    from migkit import sizing
    from migkit.config import Endpoint, Hop
    hop = Hop(name="r", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=3306, user="u"),
              target=Endpoint(host="10.0.0.2", port=3306, user="u"),
              databases=["shop"])

    class Eng:
        def databases(self):
            return ["shop"]
    monkeypatch.setattr(sizing, "capacity",
                        lambda e, side, db: {"replica": side == "dst"})
    monkeypatch.setattr(cli, "_link_said", lambda *a: ("", None))
    monkeypatch.setattr(cli, "_leg_said", lambda *a: "")
    monkeypatch.setattr(cli, "_two_way_said", lambda *a: "")
    src = cli._doctor_side("r", hop, Eng(), "src", hop.source)
    dst = cli._doctor_side("r", hop, Eng(), "dst", hop.target)
    assert "writer" in src[0][0] and "READER" not in src[0][0], src
    assert "READER/read-only" in dst[0][0], dst


def test_a_hop_is_asked_through_one_engine(two_hops, monkeypatch):
    made = []
    real = cli.get_engine

    def engine(hop):
        made.append(hop.name)
        return real(hop)
    monkeypatch.setattr(cli, "get_engine", engine)
    _doctor()
    assert sorted(made) == ["one", "two"], made
