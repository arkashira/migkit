"""The warehouse ladders' rungs each held and refused on the facts that
decide them (backlog AUD-Z.rungs-owed): a continuous hop lands its batches
only by a way measured to keep up and to land each batch once, a
provider's integration feeds a warehouse only where it keeps every promise
asked, a source is followed only from the history it keeps, a writer is
held to its number only by an isolation or transaction the account runs,
and a rollback point is a clone only where time travel is kept. Fakes
only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("warehouse_landing", "stream", "holds"),
    ("warehouse_landing", "stream", "refuses"),
    ("warehouse_landing", "files", "holds"),
    ("warehouse_landing", "files", "refuses"),
    ("warehouse_landing", "merge", "holds"),
    ("warehouse_landing", "merge", "refuses"),
    ("warehouse_feed", "provider", "holds"),
    ("warehouse_feed", "provider", "refuses"),
    ("warehouse_feed", "own", "holds"),
    ("warehouse_feed", "own", "refuses"),
    ("warehouse_follow_way", "changes", "holds"),
    ("warehouse_follow_way", "changes", "refuses"),
    ("warehouse_follow_way", "landing", "holds"),
    ("warehouse_follow_way", "landing", "refuses"),
    ("warehouse_follow_way", "none", "holds"),
    ("warehouse_fence_way", "serializable", "holds"),
    ("warehouse_fence_way", "serializable", "refuses"),
    ("warehouse_fence_way", "transaction", "holds"),
    ("warehouse_fence_way", "transaction", "refuses"),
    ("warehouse_fence_way", "none", "holds"),
    ("warehouse_snapshot_way", "travel", "holds"),
    ("warehouse_snapshot_way", "travel", "refuses"),
    ("warehouse_snapshot_way", "counts", "holds"),
]


def _landing(**over):
    facts = {"rate": 100.0, "stream_offsets": True, "stage": "s3://x",
             "ledger": True, "stream_startup": 0.2, "stream_per_row": 0.001,
             "files_startup": 2.0, "files_per_row": 0.0001,
             "merge_startup": 1.0, "merge_per_row": 0.002}
    facts.update(over)
    return facts


def test_every_landing_measured_to_keep_up_holds_and_the_least_lag_wins():
    with watched() as seen:
        got, sized = decide.warehouse_landing(_landing())
    for name in ("stream", "files", "merge"):
        assert branch(seen, name) == [HOLDS], name
    assert got.path in ("stream", "files", "merge")
    rows, interval, lag = sized
    assert rows >= 1 and lag > 0


def test_a_landing_without_its_device_ledger_or_pace_is_passed_over():
    with watched() as seen:
        got, sized = decide.warehouse_landing(_landing(
            stream_offsets=False, ledger=False))
    assert got.path is None and sized is None
    assert "no streaming insert that keeps its offset" in why(seen, "stream")
    assert "needs the ledger written" in why(seen, "files")
    assert "needs the ledger written" in why(seen, "merge")
    with watched() as seen:
        got, _ = decide.warehouse_landing(_landing(
            stream_per_row=0.01, files_startup=None, stage=""))
    # 0.01s a row at 100 rows a second takes the whole interval: behind
    assert "would fall behind" in why(seen, "stream")
    assert "names no stage" in why(seen, "files")
    assert got.path == "merge"
    with watched() as seen:
        decide.warehouse_landing(_landing(stage="s3://x",
                                          files_startup=None))
    assert why(seen, "files") == "the landing was not measured"


def _feed(**over):
    facts = {"offered": True, "runs": True, "tail": True,
             "provider_keeps": decide.FEED_PROMISES,
             "provider_lag": 5, "own_lag": 30}
    facts.update(over)
    return facts


def test_a_provider_feed_keeping_every_promise_ranks_beside_the_own_hop():
    with watched() as seen:
        got = decide.warehouse_feed(_feed())
    assert branch(seen, "provider") == [HOLDS]
    assert branch(seen, "own") == [HOLDS]
    assert got.path == "provider"


def test_a_feed_short_of_a_promise_or_a_tail_is_refused_by_name():
    with watched() as seen:
        got = decide.warehouse_feed(_feed(provider_keeps=("every row",)))
    assert got.path == "own"
    assert why(seen, "provider") == ("it does not keep deletes, the"
                                     " source's order, verified")
    with watched() as seen:
        got = decide.warehouse_feed(_feed(offered=False, tail=False))
    assert got.path is None
    assert why(seen, "provider") == "the provider offers none for this pair"
    assert why(seen, "own") == "the source gives no change tail"


def test_a_warehouse_source_is_followed_by_changes_only_within_its_history():
    base = {"tables": 3, "tracked": 3, "streams": True, "retention": 86400,
            "gap": 3600}
    with watched() as seen:
        got = decide.warehouse_follow_way(base)
    assert got.path == "changes"
    assert branch(seen, "changes") == [HOLDS]
    assert "the account reads changes" in why(seen, "landing")
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        got = decide.warehouse_follow_way(dict(base, gap=172800))
    assert got.path == "none"
    assert "forgotten before it reads it" in why(seen, "changes")
    with watched() as seen:
        got = decide.warehouse_follow_way(dict(base, tracked=1,
                                               untracked=["a", "b"]))
    assert "2 tables track no changes: alter table a, b" in why(seen,
                                                                "changes")
    with watched() as seen:
        got = decide.warehouse_follow_way(dict(base, streams=False,
                                               refused="edition"))
    assert got.path == "landing"
    assert branch(seen, "landing") == [HOLDS]
    assert "(edition)" in why(seen, "changes")


def test_a_writer_is_held_by_serializable_isolation_or_a_transaction():
    with watched() as seen:
        got = decide.warehouse_fence_way({"isolation": "SERIALIZABLE",
                                          "transactions": True})
    assert got.path == "serializable"
    assert branch(seen, "transaction") == [HOLDS]
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        got = decide.warehouse_fence_way({"isolation": "snapshot isolation",
                                          "transactions": True})
    assert got.path == "transaction"
    assert why(seen, "serializable").startswith(
        "the database runs snapshot isolation")
    with watched() as seen:
        got = decide.warehouse_fence_way({})
    assert got.path == "none"
    assert why(seen, "serializable") == "the isolation was not answered"
    assert "no transaction of several" in why(seen, "transaction")


def test_a_rollback_point_is_a_clone_only_where_time_travel_is_kept():
    with watched() as seen:
        got = decide.warehouse_snapshot_way({"travel_hours": 24})
    assert got.path == "travel"
    assert branch(seen, "counts") == [HOLDS]
    with watched() as seen:
        got = decide.warehouse_snapshot_way({"travel_hours": 0})
    assert got.path == "counts"
    assert branch(seen, "travel") == [REFUSES]
    assert "keeps no time travel" in why(seen, "travel")
