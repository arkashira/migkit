"""The index builds of a table the streaming copy has finished start while
it goes on with the next, where `decide` admits it from the facts the
move's catalogues give (`guarantees.breaks`), and wait for the copy's end
where it does not (R20.5, first step). Fakes: no server is reached; the
copy's own catalogue is read-only evidence of which table is whole."""
import threading

from migkit import guarantees, jointplan, movers


class Fake(movers._IndexWindow):
    def __init__(self):
        self.hop = self.db = self.log = self.table = None
        self.workers, self.only_left = 2, False
        self.dropped = ["public.a_v", "public.a_w", "public.b_v"]
        self.of = {"public.a_v": "public.a", "public.a_w": "public.a",
                   "public.b_v": "public.b"}
        self.ddl = {i: f"create index {i}" for i in self.dropped}
        self.built = {}
        self._lock = threading.Lock()
        self.ran = []

    def _build(self, names):
        self.ran += names
        return list(names), []


def test_a_finished_tables_indexes_are_built_and_the_rest_stay_owed():
    w = Fake()
    assert sorted(w.build_table("public.a")) == ["public.a_v", "public.a_w"]
    assert w.dropped == ["public.b_v"] and "public.a" in w.built
    began, ended = w.built["public.a"]
    assert began <= ended
    assert w.build_table("public.a") == []      # nothing built twice
    assert w.ran == ["public.a_v", "public.a_w"]


def test_a_build_that_fails_stays_owed_to_the_end():
    w = Fake()
    w._build = lambda names: ([], list(names))
    assert w.build_table("public.b") == []
    assert w.dropped.count("public.b_v") == 1


def _sem(**gaps):
    s = {t: guarantees.Semantics(table=t, key=("id",)) for t in ("a", "b")}
    for t, why in gaps.items():
        s[t].gaps.append(guarantees.Gap(t, "dependencies", why))
    return s


def test_the_admit_reads_the_catalogues_facts_not_a_flag(monkeypatch):
    asked = []
    monkeypatch.setattr(jointplan, "semantics_of",
                        lambda eng, db, **kw: asked.append(db) or _sem())
    monkeypatch.setattr("migkit.engines.postgres.PostgresEngine",
                        lambda hop: object())
    said = []
    got = movers._builds_beside_the_copy(None, "app", 2, True, said.append)
    assert asked == ["app"] and got is not None and got.way in (
        "free", "waves")
    assert any(x.startswith("index builds:") for x in said)


def test_a_catalogue_that_cannot_be_read_overlaps_nothing(monkeypatch):
    def boom(eng, db, **kw):
        raise RuntimeError("no catalogue")
    monkeypatch.setattr(jointplan, "semantics_of", boom)
    monkeypatch.setattr("migkit.engines.postgres.PostgresEngine",
                        lambda hop: object())
    assert movers._builds_beside_the_copy(None, "app", 2, True) is None
