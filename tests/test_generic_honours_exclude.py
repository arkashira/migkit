"""`exclude` on the generic engine means what it means everywhere else.

The generic engine compares the tables the hop lists in `options.tables`,
and read no exclude list at all, so a pattern such as `tmp_*` excluded a
table on every engine but this one. Every check here walks `_tables()`,
which is where the list is narrowed.
"""
import pytest

from migkit.config import Endpoint, Hop


def _engine(tables, exclude=()):
    from migkit.engines.generic import GenericEngine
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"url": "postgresql://u@10.0.0.1/a"}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"url": "postgresql://u@10.0.0.2/a"}),
              exclude=list(exclude), options={"tables": list(tables)})
    return GenericEngine(hop)


def test_a_pattern_narrows_the_listed_tables():
    eng = _engine(["orders", "tmp_load", "public.people"],
                  ["tmp_*", "public.people"])
    assert eng._tables() == ["orders"]


def test_without_an_exclude_list_the_list_is_untouched():
    assert _engine(["orders", "tmp_load"])._tables() == ["orders", "tmp_load"]


def test_excluding_everything_listed_is_said_not_run():
    with pytest.raises(SystemExit) as e:
        _engine(["orders"], ["orders"])._tables()
    assert "nothing to compare" in str(e.value)
