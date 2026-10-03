"""A consolidation's proof reads the counters of the tables it takes only
(backlog P0-SCOPE.6a).

The proof of a consolidation into MySQL asked every AUTO_INCREMENT column
of the target whether it was past the rows the sources wrote - the tables
the hop leaves out too, which the target writes itself and no source
does. One of those whose counter sat at its own rows read as a counter
behind every source's, and the proof said different over a table the
consolidation never took.
"""
from migkit import consolidate
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


class _Target(Engine):
    """A MySQL target as the proof reads it: two tables with counters,
    each counter at its own table's largest key."""
    CANON_ENGINE = "mysql"

    def __init__(self, hop):
        super().__init__(hop)
        self.read = []

    def _d(self, side, db):
        return self.hop.target_db(db)

    @staticmethod
    def _quote_ident(name):
        return f"`{name}`"

    def _q(self, side, sql, args=None):
        self.read.append(sql)
        if "information_schema.columns" in sql:
            return [("orders", "id", 51), ("pick_dispatch_queue", "id", 41)]
        return [(51,)] if "`orders`" in sql else [(41,)]


def _hop(exclude=()):
    return Hop(name="cs", engine="mysql", source=Endpoint(),
               target=Endpoint(), databases=["shop"], exclude=list(exclude))


def test_a_table_the_hop_leaves_out_is_not_asked():
    dst = _Target(_hop(exclude=["pick_dispatch_queue"]))
    got = consolidate._mysql_counters(dst, "shop")
    assert got.status == "diff" and "orders.id" in got.detail, got
    assert "pick_dispatch_queue" not in got.detail, got
    assert not [q for q in dst.read if "`pick_dispatch_queue`" in q], \
        dst.read


def test_a_hop_that_takes_every_table_asks_every_counter():
    dst = _Target(_hop())
    got = consolidate._mysql_counters(dst, "shop")
    assert got.status == "diff", got
    assert "orders.id" in got.detail and \
        "pick_dispatch_queue.id" in got.detail, got
