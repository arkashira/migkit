"""`migkit init` between two places a hop already joins updates that hop
where it is (audit V15, V12).

It added the same hop again as `a-2`, and the `--force` it suggested
rewrote every other hop out of the file. And the plan asked the servers
again for what the survey had just read: each database's table facts for
the time, and a cutover's needs of the users.
"""
from migkit import guide
from tests.test_init_is_the_guide import _guide, _sqlite


def test_the_same_two_addresses_update_the_hop_in_place(tmp_path,
                                                        monkeypatch):
    from migkit.config import load_hops
    _sqlite(tmp_path / "a.db", 5)
    conf = tmp_path / "conf" / "hops.yaml"
    _guide(tmp_path, [f"sqlite://{tmp_path}/a.db",
                      f"sqlite://{tmp_path}/b.db"], monkeypatch)
    # another hop beside it, and a person's own note in the file
    _guide(tmp_path, [f"sqlite://{tmp_path}/b.db",
                      f"sqlite://{tmp_path}/c.db"], monkeypatch)
    conf.write_text(conf.read_text() + "# kept by hand\n")
    got, _, out = _guide(tmp_path, [f"sqlite://{tmp_path}/a.db",
                                    f"sqlite://{tmp_path}/b.db"],
                         monkeypatch)
    assert got["name"] == "a", out
    assert "updated in place" in out, out
    hops = load_hops(conf)
    assert set(hops) == {"a", "b"}, set(hops)
    assert "# kept by hand" in conf.read_text()


def test_the_same_place_is_read_one_way():
    a = {"source": "postgres://app@DB1.example.com:5432/shop",
         "target": "mysql://app@db2.example.com:3306/shop"}
    # whoever signs in, and however the host is written, it is one place
    b = {"source": "postgres://other@db1.example.com:5432/shop",
         "target": "mysql://app@db2.example.com:3306/shop"}
    assert guide._same_places(a, {"shop": b}) == "shop"
    assert guide._same_places(a, {"shop": a}) == "shop"
    elsewhere = dict(a, target="mysql://app@db3.example.com/shop")
    assert guide._same_places(elsewhere, {"shop": a}) is None


def test_a_hop_s_block_is_replaced_and_nothing_else():
    text = ("hops:\n  a:\n    source: x\n    target: y\n"
            "  # between them\n  b:\n    source: y\n    target: z\n")
    got = guide._replaced(text, "a", "  a:\n    source: x\n    target: w\n")
    assert got == ("hops:\n  a:\n    source: x\n    target: w\n"
                   "  # between them\n  b:\n    source: y\n    target: z\n")
    assert guide._replaced(text, "c", "  c: {}\n") is None


def test_the_plan_reads_what_the_survey_read(tmp_path, monkeypatch):
    """Its time and its needs from the survey's facts: nothing is asked
    of the servers twice."""
    from migkit import dba
    asked = []
    real = dba.needs

    def needs(*a, **k):
        asked.append((a[2] if len(a) > 2 else k.get("goal")))
        return real(*a, **k)
    monkeypatch.setattr(dba, "needs", needs)
    _sqlite(tmp_path / "a.db", 5)
    _sqlite(tmp_path / "b.db", 5)
    monkeypatch.setattr(guide, "propose", lambda found: "cutover")
    got, _, out = _guide(tmp_path, [f"sqlite://{tmp_path}/a.db",
                                    f"sqlite://{tmp_path}/b.db"],
                         monkeypatch)
    assert got["goal"] == "cutover", out
    assert asked == ["cutover"], asked
    assert got["found"]["sides"]["source"]["by_db"], got["found"]["sides"]
