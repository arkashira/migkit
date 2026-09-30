import threading

from migkit import decide


def test_tables_copied_side_by_side_all_keep_their_way(tmp_path):
    """Every table of a move keeps its way beside the checkpoint, from
    threads writing at once: none lost to another's read-modify-write, and
    no torn file left behind."""
    where = tmp_path / "move.json"
    where.write_text("{}")
    names = [f"t{i}" for i in range(64)]

    def keep(n):
        decide.remember(str(where), n, f"way-{n}")
        decide._keep_given_up(str(where), n, "other", {"why": "x"})

    threads = [threading.Thread(target=keep, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for n in names:
        assert decide.remembered(str(where), n) == f"way-{n}"
        assert decide.given_up(str(where), n) == {"other": {"why": "x"}}
    assert not list(tmp_path.glob("*.tmp"))
