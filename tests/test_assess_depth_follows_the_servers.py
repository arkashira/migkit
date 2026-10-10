"""The assessment's depth is chosen from what the servers answer."""
from types import SimpleNamespace

from migkit import ready


class Eng:
    def __init__(self, rows=None, error=None):
        self.rows, self.error = rows, error

    def assess(self):
        if self.error:
            raise RuntimeError(self.error)
        return self.rows


HOP = SimpleNamespace(name="h")


def test_servers_that_answer_get_the_full_check_and_are_asked_once():
    rows = [{"level": "pass", "item": "x"}]
    quick, got, _why = ready.depth(HOP, Eng(rows))
    assert not quick and got == rows


def test_servers_that_do_not_answer_fall_to_the_configuration_check():
    quick, got, why = ready.depth(HOP, Eng(error="connection refused"))
    assert quick and got is None
    assert "connection refused" in why
