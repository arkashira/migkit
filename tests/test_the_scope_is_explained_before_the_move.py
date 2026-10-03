"""Before the move, a hop's scope is said so it can be checked at a
glance - and what it leaves out that its tables need is named.

`include` and `exclude` were read right and said little: a count of the
names, and a warning for an include entry that took nothing. Whether
`audit_*` took the three tables meant or thirty, which tables a database
kept back, that `Orders_Log` was spelt `orders_log` - all of it was found
after the move, by the check that compares only what was taken. And a
scope that took `orders` and not `customers` moved rows pointing at
parents the target never got.

`assess` says now, per database, how many of the source's tables the hop
takes and leaves (the first named, all of them in the run's decision
record); per entry what it matched, a pattern said to take tables made
later, one that matches nothing given the closest real names; and, where
the engine can list them, the foreign keys, views, routines, triggers,
sequences and partitions that cross the scope - each with both ways out.
The source's every table is listed once, by a twin of the hop that takes
everything and has no target.
"""
from migkit import hopkeys
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


def _hop(tmp_path, include=(), exclude=(), engine="postgres", **kw):
    h = Hop(name="h", engine=engine, source=Endpoint(host="src.example"),
            target=Endpoint(host="dst.example"), include=list(include),
            exclude=list(exclude), **kw)
    h.report_dir = lambda db="": tmp_path
    return h


HELD = {"appdb": ["public.orders", "public.lines", "public.customers",
                  "public.event_log", "audit.Orders_Log"],
        "logs": ["public.lines"]}


def _engine(hop, held=HELD, fks=(), deps=None, asked=None):
    """An engine over `held`, its scope applied the way every engine
    applies it; `asked` records each listing and the hop it was made
    for."""
    class One(Engine):
        CANON_ENGINE = "postgres"
        DEPENDENCY_KINDS = ("view", "routine", "trigger", "sequence",
                            "partition") if deps is not None else ()

        def databases(self):
            # as the engines answer: the hop's own list, or every
            # database it does not leave out whole
            if self.hop.databases:
                return list(self.hop.databases)
            return [d for d in held if not self.hop.excluded(d)]

        def neutral_tables(self, side, db):
            if asked is not None:
                asked.append((side, db, self.hop.target.host,
                              tuple(self.hop.include)))
            return [t for t in held[db]
                    if not self.hop.excluded(db, *t.split("."))]

        def rows_holding(self, side, db, table, columns, most):
            return 3

    if fks:
        One.foreign_keys = lambda self, side, db: list(fks)
    if deps is not None:
        One.dependencies = lambda self, side, db, tables=(): list(deps)
    return One(hop)


def _rows(items, item="scope"):
    return [(i["level"], i["detail"]) for i in items if i["item"] == item]


def test_each_database_says_what_it_takes_and_leaves(tmp_path):
    h = _hop(tmp_path, include=["appdb.public.orders", "appdb.public.lines"],
             databases=["appdb", "logs"])
    rows = _rows(_engine(h)._include_items())
    assert ("pass", "appdb: takes 2 of the source's 5 tables, leaves 3 out:"
                    " public.customers, public.event_log,"
                    " audit.Orders_Log") in rows, rows
    assert ("warn", "include takes no table of database logs - it is opened"
                    " and nothing in it is moved or checked  [fix] name its"
                    " tables in include, or leave it out of databases") \
        in rows


def test_the_first_left_out_are_named_and_the_rest_counted(tmp_path):
    held = {"big": [f"public.t{i:02}" for i in range(30)]}
    h = _hop(tmp_path, exclude=["big.public.t1*", "big.public.t2*"])
    rows = _rows(_engine(h, held)._include_items())
    line = [d for _, d in rows if d.startswith("big:")][0]
    assert "leaves 20 out" in line and line.endswith("and 10 more"), line


def test_each_entry_says_what_it_matched(tmp_path):
    h = _hop(tmp_path, include=["appdb.public.orders", "appdb.audit.*"],
             exclude=["appdb.public.event_log"])
    rows = dict((d.split(":")[0], (lvl, d)) for lvl, d in
                _rows(_engine(h)._include_items()))
    assert rows["include 'appdb.public.orders' takes 1"][1].endswith(
        "appdb.public.orders")
    pattern = rows["include 'appdb.audit.*' takes 1"][1]
    assert "appdb.audit.Orders_Log, and tables made later that it names:" \
           " a pattern is read again every run" in pattern, pattern
    assert rows["exclude 'appdb.public.event_log' leaves out 1"][0] == "pass"


def test_an_entry_that_matches_nothing_is_offered_the_real_name(tmp_path):
    h = _hop(tmp_path, include=["appdb.public.ordrs", "appdb.audit.orders_log",
                                "appdb.public.lines"],
             exclude=["appdb.public.evnt_log"])
    warn = [d for lvl, d in _rows(_engine(h)._include_items())
            if lvl == "warn"]
    said = " | ".join(warn)
    # by case first: the name the source has
    assert "'appdb.audit.orders_log', which takes no table on the source" \
           in said and "did you mean appdb.audit.Orders_Log?" in said, said
    # then by spelling, among the names under the same schema
    assert "did you mean appdb.public.orders?" in said
    assert "exclude names 'appdb.public.evnt_log', which leaves out no" \
           " table on the source" in said
    assert "did you mean appdb.public.event_log?" in said


def test_an_include_entry_exclude_takes_all_of_back_is_said(tmp_path):
    h = _hop(tmp_path, include=["appdb.audit.*", "appdb.public.orders"],
             exclude=["appdb.audit.Orders_Log"])
    warn = [d for lvl, d in _rows(_engine(h)._include_items())
            if lvl == "warn"]
    assert any("include 'appdb.audit.*' takes 1 tables and exclude takes"
               " every one back" in d for d in warn), warn


def test_the_whole_source_is_listed_once_and_never_the_target(tmp_path):
    asked = []
    h = _hop(tmp_path, include=["appdb.public.orders"])
    rows = _rows(_engine(h, asked=asked)._include_items())
    # one listing of the database the hop opens, by a twin that takes
    # everything and has no target to reach
    assert asked == [("src", "appdb", "", ())], asked
    # and the database it leaves out whole is named, not listed
    assert ("pass", "the source's databases left out whole: logs") in rows


def test_every_name_is_in_the_decision_record_and_shown_when_asked(
        tmp_path, monkeypatch):
    import json
    monkeypatch.delenv("MIGKIT_EXPLAIN", raising=False)
    h = _hop(tmp_path, exclude=["appdb.public.event_log"])
    assert not [i for i in _engine(h)._include_items()
                if i["item"] == "scope, in full"]
    record = [json.loads(line) for line in
              (tmp_path / "decisions.jsonl").read_text().splitlines()]
    scope = [r for r in record if r["work"] == "the hop's scope"][-1]
    assert scope["detail"]["databases"]["appdb"]["leaves"] == \
        ["public.event_log"]
    monkeypatch.setenv("MIGKIT_EXPLAIN", "1")
    full = [i["detail"] for i in _engine(h)._include_items()
            if i["item"] == "scope, in full"]
    assert any("public.event_log" in line for line in full), full


def test_a_hop_that_takes_everything_says_nothing(tmp_path):
    assert _engine(_hop(tmp_path))._include_items() == []


FKS = [("public.lines", ["order_id"], "public.orders", ["id"]),
       ("public.orders", ["customer_id"], "public.customers", ["id"])]


def test_a_parent_left_out_is_named_with_its_childrens_rows(tmp_path):
    h = _hop(tmp_path, include=["appdb.public.orders", "appdb.public.lines"])
    rows = _rows(_engine(h, fks=FKS)._include_items(), "dependencies")
    warn = [d for lvl, d in rows if lvl == "warn"]
    fk = [d for d in warn if "foreign keys across the scope" in d][0]
    assert "public.orders (customer_id) -> public.customers - 3 of its" \
           " rows hold a key into it, each pointing at nothing on the" \
           " target" in fk, fk
    assert "[fix] take the closure: add appdb.public.customers to include;" \
           " or accept it: closure: accept in the hop's options" in fk
    # lines -> orders stays inside the scope and is not named
    assert "public.lines" not in fk
    # this engine lists no views, routines, triggers, sequences or
    # partitions: said as not checked, never as clear
    assert any("not checked on this engine" in d for d in warn)


def test_what_a_scope_leaves_by_exclude_is_taken_back_out_of_it(tmp_path):
    h = _hop(tmp_path, exclude=["appdb.public.customers"])
    rows = _rows(_engine(h, fks=FKS)._include_items(), "dependencies")
    assert any("take appdb.public.customers out of exclude" in d
               for _, d in rows), rows


def test_views_routines_triggers_sequences_and_partitions_across(tmp_path):
    deps = [("view", "public.v_orders", "public.v_orders",
             "public.event_log"),
            ("routine", "public.f", "public.f", "public.event_log"),
            ("trigger", "orders_audit", "public.orders", "public.event_log"),
            ("sequence", "public.shared_seq", "public.lines",
             "public.event_log"),
            ("partition", "public.lines_2026", "public.lines",
             "public.lines_2026"),
            # inside the scope on both ends: not named
            ("trigger", "lines_touch", "public.lines", "public.orders")]
    h = _hop(tmp_path, exclude=["appdb.public.event_log",
                                "appdb.public.lines_2026"])
    rows = _rows(_engine(h, deps=deps)._include_items(), "dependencies")
    said = " | ".join(d for lvl, d in rows if lvl == "warn")
    for words in ("view public.v_orders reads public.event_log",
                  "public.f names public.event_log",
                  "trigger orders_audit on public.orders names"
                  " public.event_log",
                  "public.lines draws from public.shared_seq, which"
                  " public.event_log owns",
                  "public.lines is taken and its partition"
                  " public.lines_2026 is not"):
        assert words in said, (words, said)
    assert "lines_touch" not in said


def test_a_closure_the_hop_accepts_is_said_as_accepted(tmp_path):
    h = _hop(tmp_path, include=["appdb.public.orders"],
             options={"closure": "accept"})
    rows = _rows(_engine(h, fks=FKS, deps=[])._include_items(),
                 "dependencies")
    assert rows and all(lvl == "pass" for lvl, _ in rows), rows
    assert any("accepted (closure: accept)" in d for _, d in rows)


def test_a_scope_with_nothing_across_says_what_it_read(tmp_path):
    h = _hop(tmp_path, exclude=["appdb.public.event_log"])
    rows = _rows(_engine(h, {"appdb": HELD["appdb"]}, fks=FKS,
                         deps=[])._include_items(), "dependencies")
    assert rows == [("pass", "appdb: nothing the hop takes needs a table it"
                             " leaves out (foreign keys, views, routines,"
                             " triggers, sequences, partitions)")], rows


def test_an_engine_that_lists_nothing_says_it_did_not_look(tmp_path):
    h = _hop(tmp_path, exclude=["appdb.public.event_log"])
    rows = _rows(_engine(h)._include_items(), "dependencies")
    assert rows == [("warn", "what the tables the hop takes need of the ones"
                             " it leaves out - foreign keys, views, routines,"
                             " triggers, sequences, partitions - not checked"
                             " on this engine")], rows


def test_a_routines_text_names_the_tables_it_reads_and_writes():
    tables = ["public.orders", "public.audit_log", "audit.orders",
              "public.customers"]
    body = ("begin insert into audit_log (what) values ('x');"
            " update public.orders set n = 1;"
            " select * from \"public\".\"customers\" c join x.y on true;"
            " end")
    got = Engine._tables_named(body, tables, own="public.orders")
    assert got == ["public.audit_log", "public.customers"], got
    # a name with its schema names that schema's table alone
    assert Engine._tables_named("delete from audit.orders", tables) == \
        ["audit.orders"]


# -- the hop file: what a scope says against itself ------------------------

BASE = {"engine": "postgres",
        "source": {"host": "10.0.0.1", "user": "app"},
        "target": {"host": "10.0.0.2", "user": "app"},
        "databases": ["appdb"]}


def _problems(raw):
    return [(p.level, p.where, p.text)
            for p in hopkeys.check({"hops": {"h": raw}})]


def test_a_changes_rule_for_a_table_the_hop_leaves_out_is_refused():
    errs = [p for p in _problems({
        **BASE, "exclude": ["appdb.public.audit"],
        "mapping": {"ops": {"appdb.public.audit": "insert, update",
                            "appdb.*": "insert"}}}) if p[0] == "error"]
    assert [e[1] for e in errs] == ["hops.h.mapping.ops.appdb.public.audit"]
    assert "also excluded" in errs[0][2]
    errs = [p for p in _problems({
        **BASE, "include": ["appdb.public.orders"],
        "mapping": {"ops": {"appdb.public.lines": "insert"}}})
        if p[0] == "error"]
    assert [e[1] for e in errs] == ["hops.h.mapping.ops.appdb.public.lines"]


def test_an_exclude_that_takes_back_all_under_an_include_is_said():
    warns = [p for p in _problems({**BASE, "include": ["appdb.billing"],
                                   "exclude": ["appdb.billing.*"]})
             if p[1] == "hops.h.include[0]"]
    assert warns and warns[0][0] == "warn", warns
    assert "leaves out every table under appdb.billing" in warns[0][2]
    # exclude taking part of it back is the point of the two
    assert not [p for p in _problems({**BASE, "include": ["appdb.billing"],
                                      "exclude": ["appdb.billing.tmp_*"]})
                if p[1] == "hops.h.include[0]"]


def test_the_closure_option_is_warn_or_accept():
    assert not [p for p in _problems({**BASE,
                                      "options": {"closure": "accept"}})
                if "closure" in p[1]]
    errs = [p for p in _problems({**BASE, "options": {"closure": "maybe"}})
            if "closure" in p[1]]
    assert errs and errs[0][0] == "error" and "warn, accept" in errs[0][2]
