"""A SQL Server that answers from a script, for the tests that need no
server: the engine's catalogue reads and checks are run against rows the
test gives, and every statement the engine sent is kept to be read back.

A rule is a pattern found in the statement (and the side it is for, when
that matters) and the rows it answers - or a function of (side, statement)
that does. The first rule that matches answers; a statement nothing
matches answers no rows. What is proved with it is the engine's logic over
what a server says - which statement is sent when, and what is made of the
rows that come back - never that the server says it: where a statement was
not run on a server, the depth matrix keeps it in `capabilities.UNPROVED`.
"""
import re

from migkit.config import Endpoint, Hop


class Server:
    """The scripted answers of both sides."""

    def __init__(self):
        self.rules = []
        self.asked = []

    def on(self, pattern, rows, side=None):
        """`rows` for a statement holding `pattern` (a regular expression,
        case ignored), on `side` ("src" or "dst") or either."""
        self.rules.append((side, re.compile(pattern, re.S | re.I), rows))
        return self

    def ask(self, side, sql):
        self.asked.append((side, sql))
        for want, pattern, rows in self.rules:
            if (want is None or want == side) and pattern.search(sql):
                return rows(side, sql) if callable(rows) else rows
        return []

    def sent(self, pattern, side=None):
        """The statements sent that hold `pattern`."""
        found = re.compile(pattern, re.S | re.I)
        return [s for w, s in self.asked
                if (side is None or w == side) and found.search(s)]


def hop(tmp_path, engine="mssql", **kw):
    ep = Endpoint(host="10.0.0.1", port=1433, user="migkit",
                  password="CHANGE_ME")
    got = Hop(name="ms", engine=engine, source=ep,
              target=Endpoint(host="10.0.0.2", port=1433, user="migkit",
                              password="CHANGE_ME"),
              databases=["appdb"], **kw)
    got.report_dir = lambda db=None: tmp_path
    return got


def engine(monkeypatch, tmp_path, server, **kw):
    """An `MSSQLEngine` over `hop(tmp_path, **kw)` whose every read goes
    to `server`. Set on the class, so the engines a pair makes of its two
    sides answer from it too."""
    from migkit.engines.mssql import MSSQLEngine

    def rows(self, side, db, sql, args=None):
        return [list(r) for r in server.ask(side, sql)]

    def cmd(self, side, db, sql):
        return [[MSSQLEngine._cell(v) for v in r]
                for r in server.ask(side, sql)]
    monkeypatch.setattr(MSSQLEngine, "_rows", rows)
    monkeypatch.setattr(MSSQLEngine, "_cmd", cmd)
    monkeypatch.setattr(MSSQLEngine, "_kept_rows", rows)
    return MSSQLEngine(hop(tmp_path, **kw))
