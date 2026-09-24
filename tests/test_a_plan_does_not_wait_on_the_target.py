"""A dry run says what it can find out quickly, and does not wait for the
rest.

The plan counts the tables the target lacks, and on PostgreSQL asks
whether the target's foreign keys rule out the streaming copy. Against a
target that could not be reached yet, the engines' own connections retry
for a minute and more. Measured in the suite: a PostgreSQL plan took 65
seconds, and a MySQL one about five minutes, to say what it would do.
"""
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


def _hop(engine, tmp_path):
    hop = Hop(name="h", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="app",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="app",
                              password="CHANGE_ME"),
              databases=["appdb"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.mark.parametrize("mover,engine,needs", [
    ("pgdump_move", "postgres", ("pg_dump",)),
    ("mydumper_move", "mysql", ("mydumper", "myloader")),
    ("pgcopydb_move", "postgres", ("pgcopydb",)),
])
def test_the_plan_is_back_within_seconds(tmp_path, mover, engine, needs):
    if not all(movers.which(n) for n in needs):
        pytest.skip(f"{needs} not installed here")
    began = time.time()
    steps = getattr(movers, mover)(_hop(engine, tmp_path), "appdb", 2,
                                   False, None)
    took = time.time() - began
    assert took < 30, f"{took:.0f}s for a plan"
    # the line is still there, without a count it could not get
    assert any("create the tables the target does not have yet" in str(s)
               for s in steps), steps
