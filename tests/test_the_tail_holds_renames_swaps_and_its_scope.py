"""The change tail and DDL, held to the hop's scope (backlog P0-SCOPE.6e),
without a server:

* a table renamed stops the tail under the default `ddl` (`DDL_STOPS`): a
  `RENAME TABLE products TO products_old, products_new TO products` left
  the target's products holding the old rows under the new table's
  changes, and an excluded `staging` renamed into scope joined live with
  none of its rows;
* the swap an online schema change ends with is the table's new content -
  no stop under stop, a copy under park and apply - and never a working
  table taken into the tail;
* on a source whose log names no rename (PostgreSQL), a table swapped in
  under a name is seen by its own id changing;
* a scope or a mapping changed while the tail was stopped is a change, and
  so is one made to the hop's file while it runs - the mapping too;
* a table made on a source whose log says nothing of a table's making is
  copied, never taken live with none of the rows it was made with;
* every table is known by its whole name: two schemas' `orders` were one
  state;
* `ddl` said per table and kind (`policy`, `skipped`), and a truncate of a
  table whose deletes the hop does not carry, left out and counted;
* a table that joins has its target's triggers held off as those it began
  with were.
"""
import json
import os

import pytest

from migkit import canon, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine

EP = Endpoint(host="x", port=1, user="x", password="x")


class _Src:
    """A PostgreSQL-like source: tables as `schema.table`, each with an id
    of its own (`table_oids`), and no rename in its log."""
    CANON_ENGINE = "postgres"
    REREADS_FROM_A_POSITION = False

    def __init__(self, names=("public.a", "public.b")):
        self.names = list(names)
        self.oids = {n: str(i + 1) for i, n in enumerate(self.names)}

    def snapshot_mark(self, side, db):
        return None

    def mark_covers(self, side, db, mark, change):
        return None

    def position_lost(self, side, db, at):
        return None

    def neutral_columns(self, side, db, table):
        return [("id", "integer")]

    def log_position(self, side, db):
        return "0/10"

    def neutral_tables(self, side, db):
        return list(self.names)

    def table_oids(self, side, db):
        return dict(self.oids)


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, src):
        self.hop, self.src_engine = hop, src

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def _tables(tmp_path, src=None, said=None, tables=None, **kw):
    hop = Hop(name="tt", engine="postgres", source=EP, target=EP,
              databases=["d"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    got = tailtables.TableSet(_Pair(hop, src or _Src()), "d",
                              tmp_path / "tail-token.json",
                              said.append if said is not None
                              else (lambda m: None))
    got.file.said = None
    # the jobs a state asks for are not this test's
    got._settle = lambda: None
    if tables is not None:
        got.tables = {k: dict(v) for k, v in tables.items()}
        got.fresh = False
    return got


LIVE = {"public.orders": {"state": "live", "name": "public.orders"}}


def test_a_rename_stops_the_tail_under_the_default_ddl(tmp_path):
    t = _tables(tmp_path, tables=LIVE)
    states = t.begin()
    with pytest.raises(SystemExit) as e:
        t.ddl(states, canon.ddl("public.orders", "rename",
                                to="public.orders_old"), "0/1")
    assert "renamed to public.orders_old" in str(e.value)
    assert "ddl: apply" in str(e.value)


def test_an_excluded_table_renamed_into_scope_stops_or_is_copied(tmp_path):
    """It joined live with none of its rows: the target's table was never
    copied, and only the changes after the rename reached it."""
    t = _tables(tmp_path, tables=LIVE, exclude=["public.staging"])
    states = t.begin()
    with pytest.raises(SystemExit) as e:
        t.ddl(states, canon.ddl("public.staging", "rename",
                                to="public.orders_new"), "0/1")
    assert "public.staging was renamed" in str(e.value)
    t = _tables(tmp_path, tables=LIVE, exclude=["public.staging"],
                options={"ddl": "park"})
    states = t.begin()
    assert t.ddl(states, canon.ddl("public.staging", "rename",
                                   to="public.orders_new"), "0/1") is None
    assert states["public.orders_new"]["state"] == "copying"
    # a table the hop never took is not one the tail lets go of
    assert "public.staging" not in states


def test_the_policy_of_a_rename_into_scope_is_the_name_it_comes_in_under(
        tmp_path):
    t = _tables(tmp_path, tables=LIVE, exclude=["public.staging"],
                options={"ddl": {"default": "stop",
                                 "public.orders*": "apply"}})
    states = t.begin()
    t.ddl(states, canon.ddl("public.staging", "rename",
                            to="public.orders_new"), "0/1")
    assert states["public.orders_new"]["state"] == "copying"


def test_an_online_schema_change_s_swap_is_the_table_s_new_content(
        tmp_path):
    """gh-ost ends `RENAME TABLE orders TO _orders_del, _orders_gho TO
    orders`. Read as two renames, orders left the tail and its working
    table was copied into the target; under stop the tail stopped on a
    table migkit was told to follow through such changes."""
    t = _tables(tmp_path, tables=LIVE)
    states = t.begin()
    swap = [canon.ddl("public.orders", "rename", to="public._orders_del"),
            canon.ddl("public._orders_gho", "rename", to="public.orders")]
    for c in swap:
        assert t.ddl(states, c, "0/1") is None
    assert states == LIVE
    t = _tables(tmp_path, tables=LIVE, options={"ddl": "park"})
    states = t.begin()
    for c in swap:
        t.ddl(states, c, "0/1")
    assert set(states) == {"public.orders"}
    assert states["public.orders"]["state"] == "copying"
    assert "online schema change" in states["public.orders"]["why"]


def test_a_table_swapped_in_under_its_name_is_seen_by_its_id(tmp_path):
    """PostgreSQL's log carries no DDL and a shape is keyed by name: two
    renames that swap a table in under the name kept its name and
    columns, and the target went on holding the old table's rows."""
    src = _Src()
    t = _tables(tmp_path, src=src, options={"ddl": "park"})
    t.boundary("0/1", None, force=True)
    assert t.tables["public.a"]["oid"] == "1"
    src.oids["public.a"] = "99"
    t.scope_at = 0
    t.boundary("0/2", None, force=True)
    assert t.tables["public.a"]["state"] == "copying"
    assert "replaced on the source" in t.tables["public.a"]["why"]
    assert t.tables["public.a"]["oid"] == "99"
    assert t.tables["public.b"]["state"] == "live"
    # under stop, the tail stops before the batch
    src = _Src()
    t = _tables(tmp_path / "stop", src=src)
    t.boundary("0/1", None, force=True)
    src.oids["public.b"] = "7"
    with pytest.raises(SystemExit) as e:
        t.boundary("0/2", None, force=True)
    assert "public.b was replaced on the source" in str(e.value)


def test_a_scope_changed_while_the_tail_was_stopped_is_a_change(tmp_path):
    """On a restart the hop file was the baseline: a table the hop took
    while the tail was down was marked live, with none of its rows."""
    t = _tables(tmp_path, include=["public.a"])
    t.boundary("0/1", None, force=True)
    t.save()
    assert json.loads((tmp_path / tailtables.STATE).read_text())[
        "scope"]["include"] == ["public.a"]
    said = []
    t = _tables(tmp_path, said=said, include=["public.a", "public.b"])
    assert not t.fresh and t.was["include"] == ["public.a"]
    assert t.boundary("0/2", None, force=True) is True
    assert t.tables["public.b"]["state"] == "copying"
    assert t.tables["public.b"]["why"] == "added to the hop's scope"
    assert any("changed while the tail was stopped" in s for s in said)


def test_a_mapping_changed_takes_again_the_tables_it_changes(tmp_path):
    t = _tables(tmp_path)
    t.boundary("0/1", None, force=True)
    t.save()
    said = []
    t = _tables(tmp_path, said=said,
                mapping={"where": {"public.a": "id > 10"}})
    t.boundary("0/2", None, force=True)
    assert t.tables["public.a"]["state"] == "copying"
    assert t.tables["public.b"]["state"] == "live"
    # and the tail reads each table's target, columns and filter again
    assert t.take_remapped() is True and t.take_remapped() is False
    assert any("mapping changed: public.a taken again" in s for s in said)


def test_the_hop_file_s_mapping_is_read_again(tmp_path, monkeypatch):
    """`_HopFile` re-read the scope and not the mapping: a filter changed
    under a running tail was never applied to it."""
    import migkit.config
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  tt:\n    exclude: [b]\n")
    monkeypatch.setattr(migkit.config, "CONF", conf)
    hop = Hop(name="tt", engine="postgres", source=EP, target=EP,
              exclude=["b"])
    got = tailtables._HopFile(hop)
    conf.write_text("hops:\n  tt:\n    exclude: [b]\n    mapping:\n"
                    "      where: {a: \"id > 1\"}\n")
    os.utime(conf, ns=(1, 10 ** 18))
    # the scope is the same: not a scope changed
    assert got.refresh() is False
    assert hop.mapping == {"where": {"a": "id > 1"}}
    assert got.take_remapped() == {}
    assert got.take_remapped() is None


def test_a_table_made_where_the_log_says_nothing_of_it_is_copied(tmp_path):
    """CREATE TABLE AS writes its rows in no change PostgreSQL decodes:
    taken live under stop, the target never had them."""
    src = _Src()
    src.CREATES_UNSEEN = True
    t = _tables(tmp_path, src=src)
    t.boundary("0/1", None, force=True)
    src.names.append("public.made")
    src.oids["public.made"] = "3"
    t.scope_at = 0
    t.boundary("0/2", None, force=True)
    assert t.tables["public.made"]["state"] == "copying"
    # a source whose log carries the making: as the tail always did
    src = _Src()
    t = _tables(tmp_path / "other", src=src)
    t.boundary("0/1", None, force=True)
    src.names.append("public.made")
    t.boundary("0/2", None, force=True)
    assert t.tables["public.made"]["state"] == "live"


def test_creates_left_out_stay_out_until_the_scope_changes(tmp_path):
    src = _Src()
    t = _tables(tmp_path, src=src,
                options={"ddl": {"default": "apply", "skip": ["create"]}})
    t.boundary("0/1", None, force=True)
    src.names.append("public.made")
    t.boundary("0/2", None, force=True)
    st = t.tables["public.made"]
    assert st["state"] == "removed" and st["left"]
    # read again, it is not put back
    t.boundary("0/3", None, force=True)
    assert t.tables["public.made"]["state"] == "removed"
    states = t.begin()
    out, left = t.route(states, [canon.change("insert", "public.made",
                                              {"id": 1}, {"id": 1})], "0/3")
    assert out == [] and left[tailtables.OUT_OF_SCOPE]["public.made"] == 1


def test_two_schemas_tables_of_one_name_are_two_tables(tmp_path):
    """Keyed by the last part alone, `sales.orders` and `crm.orders` shared
    one state - one's DDL parked the other."""
    src = _Src(["sales.orders", "crm.orders"])
    t = _tables(tmp_path, src=src, options={"ddl": "park"})
    t.boundary("0/1", None, force=True)
    assert set(t.tables) == {"sales.orders", "crm.orders"}
    states = t.begin()
    t.park(states, t.key("sales.orders"), "0/2", "the target lacks w")
    out, _ = t.route(states, [
        canon.change("insert", "sales.orders", {"id": 1}, {"id": 1}),
        canon.change("insert", "crm.orders", {"id": 2}, {"id": 2})], "0/2")
    assert [c["table"] for c in out] == ["crm.orders"]
    # a quoted name and the catalogue's are one table
    assert t.key('crm."Orders"') == "crm.Orders"


def test_a_state_kept_by_its_last_part_moves_to_its_whole_name(tmp_path):
    (tmp_path / tailtables.STATE).write_text(json.dumps({"tables": {
        "t2": {"state": "parked", "name": "public.t2"}}}))
    spool = tmp_path / tailtables.SPOOLS / "t2.spool"
    spool.parent.mkdir()
    spool.write_bytes(b"held")
    t = _tables(tmp_path)
    assert set(t.tables) == {"public.t2"}
    assert t.spool("public.t2").path.read_bytes() == b"held"
    assert not spool.exists()


def test_the_ddl_said_per_table_and_kind():
    hop = Hop(name="p", engine="postgres", source=EP, target=EP, options={
        "ddl": {"default": "park", "billing.*": "apply",
                "logistics.orders": "stop", "skip": ["drop", "truncate"]}})
    assert tailtables.policy(hop, ["shop", "billing", "invoices"]) == "apply"
    assert tailtables.policy(hop, ["shop", "logistics", "orders"]) == "stop"
    # the most particular key wins
    hop.options["ddl"]["billing.secret"] = "stop"
    assert tailtables.policy(hop, "shop.billing.secret") == "stop"
    assert tailtables.policy(hop, ["shop", "x", "y"]) == "park"
    assert tailtables.policy(hop) == "park"
    assert tailtables.skipped(hop) == {"drop", "truncate", "reload"}
    # one word, as it always was
    plain = Hop(name="p", engine="postgres", source=EP, target=EP,
                options={"ddl": "apply"})
    assert tailtables.policy(plain, ["d", "t"]) == "apply"
    assert tailtables.skipped(plain) == frozenset()
    for bad in ({"ddl": "carry"}, {"ddl": {"default": "nope"}},
                {"ddl": {"skip": ["alter"]}}):
        hop = Hop(name="p", engine="postgres", source=EP, target=EP,
                  options=bad)
        with pytest.raises(SystemExit):
            tailtables.policy(hop, ["d", "t"])
            tailtables.skipped(hop)


def test_a_kind_the_hop_leaves_out_is_counted_and_never_applied(tmp_path):
    t = _tables(tmp_path, tables=LIVE,
                options={"ddl": {"default": "stop",
                                 "skip": ["truncate", "drop"]}})
    states = t.begin()
    assert t.ddl(states, canon.ddl("public.orders", "truncate"),
                 "0/1") is None
    assert t.ddl(states, canon.ddl("public.orders", "drop"), "0/1") is None
    assert states["public.orders"]["state"] == "live"
    assert t.left_ddl() == {
        ("public.orders", "truncate left out by the hop's ddl"): 1,
        ("public.orders", "drop left out by the hop's ddl"): 1}


def test_a_table_whose_deletes_are_kept_is_never_emptied(tmp_path):
    """An archive kept on the target (`mapping.ops` without delete): a
    truncate on the source stopped the tail under stop, and under apply
    emptied the archive."""
    for ddl in ("stop", "apply"):
        t = _tables(tmp_path, tables=LIVE, options={"ddl": ddl},
                    mapping={"ops": {"public.orders": "insert, update"}})
        states = t.begin()
        assert t.ddl(states, canon.ddl("public.orders", "truncate"),
                     "0/1") is None
        assert list(t.left_ddl().values()) == [1]


def test_a_table_that_joins_has_its_target_s_triggers_held_off(tmp_path):
    """`load_window` was decided over the tables there were when the tail
    began: a table that joined later had its target's triggers fire for
    every row written into it."""
    t = _tables(tmp_path, tables=LIVE, options={"ddl": "park"})
    del t._settle
    asked = []
    t.quiet = asked.append
    t._start = lambda leaf, st, fresh: None
    t._add(t.tables, "public.new", "public.new", "0/1", "added")
    t._settle()
    assert asked == ["public.orders", "public.new"]
    t._settle()
    assert asked == ["public.orders", "public.new"]

    def refuse(name):
        raise SystemExit("the user may not write as a replica")
    t.quiet = refuse
    t._add(t.tables, "public.other", "public.other", "0/1", "added")
    t._settle()
    assert t.tables["public.other"]["state"] == "parked"
    assert "triggers would fire" in t.tables["public.other"]["why"]


def test_a_table_renamed_in_from_another_database_is_a_rename(tmp_path):
    """MySQL's log names it a table made here (`_ddl_of`), and under stop
    it joined live without the rows it had there."""
    from tests.test_a_ddl_is_read_at_its_place_in_the_log import (_mysql,
                                                                   _Query)
    got = _mysql()._ddl_of(_Query("rename table other.v to cx.v", "other"),
                           "src", "cx", None, {})
    assert got[0]["kind"] == "create" and got[0][canon.CAME_FROM] == \
        "other.v"
    t = _tables(tmp_path, tables=LIVE)
    states = t.begin()
    with pytest.raises(SystemExit) as e:
        t.ddl(states, got[0], "0/1")
    assert "renamed in from other.v" in str(e.value)
    t = _tables(tmp_path, tables=LIVE, options={"ddl": "apply"})
    states = t.begin()
    t.ddl(states, got[0], "0/1")
    assert states["v"]["state"] == "copying"


def test_a_mongodb_collection_renamed_into_scope_reaches_the_tail():
    """The event was dropped because the collection it came from is one
    the hop leaves out: it joined with none of its documents under every
    `ddl`."""
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(Hop(name="m", engine="mongodb", source=EP, target=EP,
                          databases=["app"],
                          exclude=["staging", "archive_*"]))
    into = {"operationType": "rename", "ns": {"db": "app",
                                              "coll": "staging"},
            "to": {"db": "app", "coll": "orders"}}
    assert eng._renamed_in("app", into)
    # from one the hop leaves out to another: nothing of the tail's
    away = dict(into, to={"db": "app", "coll": "archive_1"})
    assert not eng._renamed_in("app", away)
    elsewhere = dict(into, to={"db": "other", "coll": "orders"})
    assert not eng._renamed_in("app", elsewhere)
