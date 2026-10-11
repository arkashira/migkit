"""MySQL snapshots its target before a cutover so the cutover can be rolled
back (P0-PARITY.all).

The cutover raises the target's AUTO_INCREMENT counters past the source's
and keeps a restore point first (`rollback --state pre-cutover`); a repair
keeps one before it writes. The point is the counters as the target held
them, and - for a repair of more than the counters - the target's schema.
It is written whole or not at all and on the disk before the work it undoes
begins (`durable.write_atomic`), and `rollback` reads the same point back
into the statements that put the counters where they were.

Pure: the catalogue read and the schema dump are answered in-process.
"""
import time

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("cutover.rollback", "mysql")]

COUNTERS = [("orders", 4101), ("lines", 90001)]


def _engine(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit.engines.mysql import MySQLEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = Hop(name="h", engine="mysql", source=Endpoint(), target=Endpoint(),
              databases=["appdb"], workers=1,
              options={"state": {"mirror": str(tmp_path / "mirror")}})
    hop.report_dir = lambda db=None: tmp_path / "reports"
    eng = MySQLEngine(hop)
    asked = []

    def ask(side, sql, args=None, fresh=False):
        asked.append((side, fresh))
        assert side == "dst", "the snapshot is of the target"
        return COUNTERS
    eng._q = ask
    eng._dump_schema = lambda side, db, physical=None: "create table t;\n"
    eng.asked = asked
    return eng


def _point(eng, kind):
    from migkit.state import get_store
    store = get_store(eng.hop)
    point = store.new_point("appdb", "pre-cutover")
    eng.snapshot_state("appdb", point.dir, kind)
    point.commit(time.strftime("%F %T"))
    return store


def test_the_counters_are_kept_as_the_target_held_them(tmp_path,
                                                        monkeypatch):
    eng = _engine(tmp_path, monkeypatch)
    store = _point(eng, "sequences")
    (meta,) = store.list("appdb")
    state = store.fetch("appdb", meta["ts"])
    assert (state / "dst-autoinc.txt").read_text() == (
        "lines|90001\norders|4101\n")
    # read fresh: a counter the server had cached is not the counter now
    assert eng.asked == [("dst", True)]
    # a counters-only point does not dump a schema that buys nothing
    assert not (state / "dst-schema.sql").exists()


def test_a_repair_of_more_keeps_the_schema_too(tmp_path, monkeypatch):
    eng = _engine(tmp_path, monkeypatch)
    store = _point(eng, "all")
    (meta,) = store.list("appdb")
    state = store.fetch("appdb", meta["ts"])
    assert (state / "dst-schema.sql").read_text() == "create table t;\n"
    assert (state / "dst-autoinc.txt").exists()


def test_a_point_is_written_whole_or_not_at_all(tmp_path, monkeypatch):
    """The counters go through `durable.write_atomic`: no half-written file
    for a rollback to read, and a stray temporary left by a crash is not
    the point."""
    from migkit import durable
    eng = _engine(tmp_path, monkeypatch)
    wrote = []
    real = durable.write_atomic
    monkeypatch.setattr(durable, "write_atomic",
                        lambda path, data, *a, **k: (
                            wrote.append(str(path)), real(path, data, *a,
                                                          **k))[1])
    state = tmp_path / "point"
    state.mkdir()
    eng.snapshot_state("appdb", state, "all")
    assert [p.rsplit("/", 1)[-1] for p in wrote] == [
        "dst-autoinc.txt", "dst-schema.sql"]
    assert [p.name for p in state.iterdir() if p.name.startswith(".")] == []


def test_rollback_reads_the_point_into_the_statements_that_restore_it(
        tmp_path, monkeypatch):
    from migkit import cli
    eng = _engine(tmp_path, monkeypatch)
    _point(eng, "sequences")
    said = []
    monkeypatch.setattr(cli, "say_raw", said.append)
    cli._restore_target(eng.hop, eng, "appdb", "", False, True)
    assert sorted(s.strip() for s in said) == [
        "alter table `appdb`.`lines` auto_increment = 90001;",
        "alter table `appdb`.`orders` auto_increment = 4101;"]
