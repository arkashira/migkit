"""An Oracle engine whose catalogue is answered in-process: the tests of
what migkit reads from a schema and makes of it, without a server."""
import pytest

from migkit.engines.oracle import OracleEngine


class Catalogue:
    """Answers for the statements an engine sends: (a word of the
    statement, the side it is for or None, rows - or a function of the
    side and the statement - or an exception to raise)."""

    def __init__(self):
        self.answers = []
        self.sent = []

    def on(self, word, rows, side=None):
        self.answers.append((word, side, rows))
        return self

    def rows(self, engine, side, db, sql, args=None):
        self.sent.append((side, sql))
        for word, who, out in self.answers:
            if word in sql and who in (None, side):
                if isinstance(out, Exception):
                    raise out
                return out(side, sql) if callable(out) else out
        raise AssertionError(f"not answered ({side}): {sql}")


@pytest.fixture
def cat(monkeypatch):
    got = Catalogue()
    monkeypatch.setattr(OracleEngine, "_rows",
                        lambda self, side, db, sql, args=None:
                        got.rows(self, side, db, sql, args))
    return got
