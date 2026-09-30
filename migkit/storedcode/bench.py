"""The sandbox's two servers holding the fixture, with a runner on each
watching its tables: what the schema's own proofs run on - constraints
held to the rows they accept and refuse (`constraints`), collations to
the values they compare and sort (`collations`), and the columns sized
from the data to the values they now refuse (`sizing`).

It stands between the sandbox's making and the routines': no routine is
made in either server yet, so a trigger the conversion got wrong cannot
be taken for a constraint that differs.
"""
from . import harness


class Bench:
    """`a` runs on the source engine's sandbox, `b` on the target's; each
    watches the fixture's tables by the source's table name
    (`a.watched[t]`, `b.watched[t]`: this side's name, the columns in one
    order on both, the key). `tables` is {source table: target table}."""

    def __init__(self, proof):
        self.pair = proof.pair
        self.db = proof.db
        self.hop = proof.pair.hop
        self.sandbox = proof.sandbox
        self.tables = dict(self.sandbox.tables)
        wsrc, wdst = proof._watched()
        self.a = harness.runner(self.sandbox.src, self.db, wsrc).open()
        self.b = harness.runner(self.sandbox.dst, self.sandbox.tdb,
                                wdst).open()
        self.a.baseline()
        self.b.baseline()

    def close(self):
        for runner in (self.a, self.b):
            if runner is not None:
                runner.close()
        self.a = self.b = None
