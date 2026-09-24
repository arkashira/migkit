"""What a report shows from the rows can be masked (G2).

A drilldown is meant to be pasted into a ticket, and it showed keys and
values as they were: an email address as a key, a card number in a value.
The hop option `mask` - `all`, or a list of `column` / `table.column` -
shows those values as `masked:` and a salted hash instead. Equal values
show equal, so rows still line up and a difference is still one.
"""
import sqlite3

import pytest
from click.testing import CliRunner


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name, rows in (("a.db", [("ann@example.com", "x"),
                                 ("bo@example.com", "y")]),
                       ("b.db", [("ann@example.com", "x")])):
        con = sqlite3.connect(tmp_path / name)
        con.executescript("create table people (email text primary key,"
                          " note text)")
        con.executemany("insert into people values (?, ?)", rows)
        con.commit()
        con.close()

    def conf(mask):
        text = ("hops:\n  lite:\n    engine: sqlite\n"
                f"    source: {{host: {tmp_path / 'a.db'}, user: x,"
                " password: x}\n"
                f"    target: {{host: {tmp_path / 'b.db'}, user: x,"
                " password: x}\n"
                "    databases: [main]\n")
        if mask is not None:
            text += f"    options: {{mask: {mask}}}\n"
        (tmp_path / "hops.yaml").write_text(text)
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return conf


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_the_key_is_shown_until_the_hop_masks_it(lite, tmp_path):
    lite(None)
    got, said = _run("check", "lite", "--only", "data")
    assert got.exit_code != 0 and "bo@example.com" in said, said
    lite("[people.email]")
    got, said = _run("check", "lite", "--only", "data")
    assert got.exit_code != 0, said
    assert "bo@example.com" not in said and "masked:" in said, said
    for report in (tmp_path / "reports").rglob("*.json"):
        assert "bo@example.com" not in report.read_text(), report
    # the repair plan reads the same keys and shows them the same way
    got, said = _run("sync", "lite", "--db", "main", "--kind", "rows")
    assert "bo@example.com" not in said and "masked:" in said, said
    # and the repair itself still has the real keys to act on
    got, said = _run("sync", "lite", "--db", "main", "--kind", "rows",
                     "--apply")
    assert got.exit_code == 0, said
    con = sqlite3.connect(tmp_path / "b.db")
    assert con.execute("select count(*) from people").fetchone()[0] == 2
    con.close()


def _hop(tmp_path, mask):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="x", port=0, user="", password="")
    hop = Hop(name="m", engine="sqlite", source=ep, target=ep,
              options={"mask": mask})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def test_equal_values_show_equal_and_the_salt_is_private(tmp_path):
    import os
    import stat

    from migkit import masking
    hop = _hop(tmp_path, ["email"])
    a, b = masking.token(hop, "ann@x"), masking.token(hop, "ann@x")
    assert a == b and a != masking.token(hop, "bo@x")
    assert masking.token(hop, None) is None
    mode = stat.S_IMODE(os.stat(tmp_path / "mask.salt").st_mode)
    assert mode == 0o600, oct(mode)
    # another hop's salt gives other tokens for the same value
    other = tmp_path / "other"
    other.mkdir()
    assert masking.token(_hop(other, ["email"]), "ann@x") != a


def test_which_columns_and_keys_are_masked(tmp_path):
    from migkit import masking
    hop = _hop(tmp_path, ["people.email", "ssn"])
    assert masking.column(hop, "people", "email")
    assert masking.column(hop, "public.people", "email")
    assert not masking.column(hop, "orders", "email")
    assert masking.column(hop, "orders", "ssn")
    assert masking.keys(hop, "people") and masking.keys(hop, "orders")
    only = _hop(tmp_path, ["people.email"])
    assert not masking.keys(only, "orders")
    assert not masking.active(_hop(tmp_path, None))
    assert masking.column(_hop(tmp_path, "all"), "any", "thing")


def test_examples_in_deep_findings_are_masked(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    eng = SQLiteEngine(_hop(tmp_path, ["people.name"]))
    got = eng._mojibake_result(
        "main", [("people", "name", 3, 0, "JosÃ©", "José")], 3, "")
    assert "JosÃ©" not in got.detail and "masks" in got.detail, got.detail
    got = eng._duplicate_hunt_result(
        "main", [("target", "people", "people_name_key", "name", 2,
                  '{"name": "Ann"}')], 1, 0, "", "")
    assert "Ann" not in got.detail and "masked:" in got.detail, got.detail


def test_the_invisible_section_masks_what_it_prints(tmp_path):
    import pandas as pd

    from migkit.engines.sqlite import SQLiteEngine
    eng = SQLiteEngine(_hop(tmp_path, ["people.note"]))
    src = pd.DataFrame({"id": [1], "note": ["secret "]})
    dst = pd.DataFrame({"id": [1], "note": ["secret"]})
    got = eng._invisible_section(src, dst, ["id"], "people")
    assert "secret" not in got and "masked:" in got, got
    assert "trailing" in got.lower() or "space" in got.lower(), got
