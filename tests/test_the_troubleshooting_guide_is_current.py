"""The troubleshooting guide is the code's refusals, not a copy of them
(backlog 42). `python -m migkit.troubleshooting` writes it; this fails
when a refusal is added, changed or removed and the guide was not written
again - so it cannot fall behind the code the way a hand-kept one does."""
from pathlib import Path

from migkit import troubleshooting
from migkit.movers import DRIVEN

GUIDE = Path(__file__).parent.parent / "docs" / "troubleshooting.md"


def test_the_guide_matches_the_code():
    assert GUIDE.read_text() == troubleshooting.render(), (
        "docs/troubleshooting.md is behind the code: run"
        " `python -m migkit.troubleshooting`")


def test_it_has_every_refusal_written_where_it_is_raised():
    got = troubleshooting.refusals()
    assert len(got) > 100, len(got)
    texts = [t for _, _, t in got]
    assert any("Nothing has been written" in t for t in texts), texts[:5]


def test_it_names_no_program():
    text = GUIDE.read_text().lower()
    for name in list(DRIVEN) + ["reladiff", "debezium", "mongodump",
                                "mongorestore", "myloader"]:
        assert name.lower() not in text, name
