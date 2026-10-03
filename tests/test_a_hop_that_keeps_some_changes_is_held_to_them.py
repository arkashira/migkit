"""A hop that does not carry every change is held to what it carries.

`mapping.ops` lets a hop leave a change out for a table - an archive kept
on the target carries no deletes, as a DTS job told to skip them does -
and nothing judged by it: the target held the rows the source deleted,
the check called that a difference, and such a hop could never pass. The
judgement is one function (`verdict.hold`) every reader of the answers
goes through (`decide`, `summarize`, the check's own run).

What it may say, and what it may never say:
* rows apart counted one by one, every one a kind the hop leaves (rows
  only on the target where deletes are not carried) - a note saying so,
  never a `same` row and never a difference;
* a shape read from counts and key sums - not proved, so it is something
  to read (`incomplete`), never a note;
* any row apart of a kind the hop does carry - a difference, said with
  what the hop keeps beside it.
"""
from migkit import report, verdict
from migkit.config import Endpoint, Hop


def _hop(engine="mysql", ops=None, **kw):
    return Hop(name="h", engine=engine, source=Endpoint(), target=Endpoint(),
               mapping={"ops": ops} if ops else {}, **kw)


NO_DELETES = {"shop.orders": "insert, update"}


def _data(detail, scope="shop.orders", status="diff"):
    return {"check": "data", "scope": scope, "status": status,
            "detail": detail, "fix_hint": "migkit sync h --db shop"}


def test_the_kinds_a_hop_keeps_follow_its_carried_changes():
    h = _hop(ops={"shop.orders": "insert, update", "shop.feed": "i",
                  "shop.audit": "insert, delete"})
    assert verdict.kept_kinds(h, "shop", "orders") == {"extra"}
    assert verdict.kept_kinds(h, "shop", "feed") == {"extra", "changed"}
    assert verdict.kept_kinds(h, "shop", "audit") == {"changed"}
    assert verdict.kept_kinds(h, "shop", "lines") == frozenset()
    # a hop that carries every change keeps nothing
    assert verdict.kept_kinds(_hop(), "shop", "orders") == frozenset()


def test_a_pair_naming_a_table_by_its_own_name_is_held_to_a_schema_rule():
    """A pair names a PostgreSQL source's table by its leaf
    (`HeteroEngine._leaf`), and says its source name beside it
    (`Result.table`): a rule naming its schema names that table."""
    h = _hop("hetero", ops={"public.orders": "insert,update"},
             options={"source_engine": "postgres",
                      "target_engine": "mysql"})
    assert verdict.kept_kinds(h, "shop", "public.orders") == {"extra"}
    assert verdict.kept_kinds(h, "shop", "archive.orders") == frozenset()
    assert verdict.kept_kinds(h, "shop", "lines") == frozenset()


def test_a_leaf_alone_is_held_to_every_schema_it_may_be():
    """`orders` alone may be `public.orders`, which keeps deletes, or
    `archive.orders`, which carries them: a row is kept only where every
    reading keeps it - a guess of one schema called another schema's
    deleted rows kept, and a difference a `same`."""
    h = _hop("hetero", ops={"public.orders": "insert,update"},
             options={"source_engine": "postgres",
                      "target_engine": "mysql"})
    assert verdict.kept_kinds(h, "shop", "orders") == frozenset()
    both = _hop("hetero", ops={"public.orders": "insert,update",
                               "archive.orders": "insert"},
                options={"source_engine": "postgres",
                         "target_engine": "mysql"})
    # a schema neither rule names carries every change, so the leaf
    # alone keeps nothing
    assert verdict.kept_kinds(both, "shop", "orders") == frozenset()
    star = _hop("hetero", ops={"*.orders": "insert,update"},
                options={"source_engine": "postgres",
                         "target_engine": "mysql"})
    assert verdict.kept_kinds(star, "shop", "orders") == {"extra"}


def test_another_schemas_table_of_the_same_leaf_is_a_difference():
    """The pair's answer names `orders`; its source name says which."""
    h = _hop("hetero", ops={"public.orders": "insert,update"},
             options={"source_engine": "postgres",
                      "target_engine": "mysql"})
    kept = _data("missing=0 extra=5 changed=0", scope="shop.orders")
    kept["table"] = "public.orders"
    assert verdict.decide([kept], h).status == "same"
    other = _data("missing=0 extra=5 changed=0", scope="shop.orders")
    other["table"] = "archive.orders"
    assert verdict.decide([other], h).status == "different"
    unsaid = _data("missing=0 extra=5 changed=0", scope="shop.orders")
    assert verdict.decide([unsaid], h).status == "different"


def test_rows_counted_by_the_check_are_read_from_its_fields():
    """A check that counted the rows says them as fields
    (`Result.rows_apart`), read as they are and not back out of words."""
    h = _hop(ops=NO_DELETES)
    r = _data("5 rows only on the target")
    r["rows_apart"] = {"orders": {"missing": 0, "extra": 5, "changed": 0,
                                  "exact": True}}
    assert verdict.decide([r], h).status == "same"
    assert r["kept_by_hop"]
    r = _data("5 rows only on the target, 1 missing")
    r["rows_apart"] = {"orders": {"missing": 1, "extra": 5, "changed": 0,
                                  "exact": True}}
    assert verdict.decide([r], h).status == "different"
    r = _data("at least 5 rows only on the target")
    r["rows_apart"] = {"orders": {"missing": 0, "extra": 5, "changed": 0,
                                  "exact": False}}
    assert verdict.decide([r], h).status != "same"


def test_rows_the_source_deleted_counted_one_by_one_are_a_note():
    h = _hop(ops=NO_DELETES)
    r = _data("missing=0 extra=5 changed=0 kind=rows-extra by=5")
    got = verdict.decide([r], h)
    assert got.status == "same" and got.severity == ("note",), got
    assert "hold only what the hop keeps" in got.why
    assert r["status"] == "warn" and r["fix_hint"] == ""
    assert r["kept_by_hop"] == ("kept by the hop: 5 rows the source deleted"
                                " - deletes are not carried")
    assert r["detail"].startswith("kept by the hop: 5 rows")


def test_the_same_rows_on_a_hop_that_carries_every_change_differ():
    r = _data("missing=0 extra=5 changed=0 kind=rows-extra by=5")
    got = verdict.decide([r], _hop())
    assert got.status == "different"
    assert r["status"] == "diff" and "kept_by_hop" not in r


def test_a_shape_read_from_sums_is_not_proof():
    """`kind=rows-extra by=5` from key sums may be 3 missing and 8 extra:
    read, never noted."""
    h = _hop(ops=NO_DELETES)
    r = _data("checksum differs kind=rows-extra by=5")
    got = verdict.decide([r], h)
    assert got.status == "incomplete", got
    assert r["status"] == "warn" and "kept_by_hop" not in r
    assert "not proved" in r["detail"]


def test_rows_missing_beside_the_kept_ones_are_a_difference():
    h = _hop(ops=NO_DELETES)
    r = _data("missing=2 extra=5 changed=0 kind=rows-missing by=2,"
              "rows-extra by=5")
    got = verdict.decide([r], h)
    assert got.status == "different"
    assert "kept by the hop: 5 rows the source deleted" in r["detail"]
    assert "2 rows missing on the target are a difference - inserts are" \
           " carried" in r["detail"]


def test_rows_only_on_the_target_are_a_difference_where_deletes_are_carried():
    h = _hop(ops={"shop.orders": "update, delete"})
    r = _data("missing=0 extra=5 changed=0")
    assert verdict.decide([r], h).status == "different"
    assert "deletes are carried" in r["detail"]


def test_inserts_or_updates_not_carried_keep_their_own_rows():
    h = _hop(ops={"shop.orders": "update, delete", "shop.lines": "i,d"})
    a = _data("missing=3 extra=0 changed=0")
    b = _data("missing=0 extra=0 changed=4", scope="shop.lines")
    assert verdict.decide([a, b], h).status == "same"
    assert "3 rows the source inserted - inserts are not carried" in \
        a["kept_by_hop"]
    assert "4 rows as they were before the source updated them" in \
        b["kept_by_hop"]


def test_rows_not_all_counted_are_not_proof():
    h = _hop(ops=NO_DELETES)
    r = _data("rows src=100 dst=105; 5 only on the target (1, 2, 3, 4 ...);"
              " missing=0 extra=5 changed=0; stopped after 50,000 rows on"
              " the target, so there may be more")
    assert verdict.decide([r], h).status == "incomplete"
    assert "kept_by_hop" not in r


def test_a_target_row_outside_the_filter_is_a_difference_beside_them():
    h = _hop(ops=NO_DELETES)
    r = _data("rows src=100 dst=105; missing=0 extra=5 changed=0; 3 rows on"
              " the target lie outside the hop's row filter, which the move"
              " does not put there")
    assert verdict.decide([r], h).status == "different"


def test_a_count_is_held_to_the_same_tables_counted_rows():
    h = _hop(ops=NO_DELETES)
    counts = {"check": "counts", "scope": "shop", "status": "diff",
              "detail": "orders src=100 dst=105"}
    data = _data("missing=0 extra=5 changed=0 kind=rows-extra by=5")
    got = verdict.decide([counts, data], h)
    assert got.status == "same", got
    assert counts["kept_by_hop"].startswith("orders: kept by the hop")


def test_a_count_alone_is_to_be_read_and_fewer_rows_differ():
    h = _hop(ops=NO_DELETES)
    more = {"check": "counts", "scope": "shop", "status": "diff",
            "detail": "orders src=100 dst=105"}
    assert verdict.decide([more], h).status == "incomplete"
    fewer = {"check": "counts", "scope": "shop", "status": "diff",
             "detail": "orders src=105 dst=100"}
    assert verdict.decide([fewer], h).status == "different"
    assert "at least 5 rows missing on the target" in fewer["detail"]


def test_a_count_whose_rows_add_up_otherwise_is_not_explained():
    h = _hop(ops=NO_DELETES)
    counts = {"check": "counts", "scope": "shop", "status": "diff",
              "detail": "orders src=100 dst=108"}
    data = _data("missing=0 extra=5 changed=0")
    got = verdict.decide([counts, data], h)
    assert got.status == "incomplete" and "kept_by_hop" not in counts


def test_a_list_cut_short_is_never_wholly_explained():
    """A count answer naming ten tables and how many more: the ones not
    named were not held to anything."""
    from migkit.engines.base import clauses
    h = _hop(ops={"shop.*": "insert, update"})
    detail = clauses([f"t{i} src=1 dst=2" for i in range(12)])
    assert detail.endswith("; and 2 more")
    counts = {"check": "counts", "scope": "shop", "status": "diff",
              "detail": detail}
    rows = [_data("missing=0 extra=1 changed=0", scope=f"shop.t{i}")
            for i in range(12)]
    verdict.decide([counts] + rows, h)
    assert counts["status"] == "diff"
    assert all(r.get("kept_by_hop") for r in rows)


def test_a_database_answer_naming_its_tables_is_held_table_by_table():
    """PostgreSQL's data check answers once a database and counts each
    table's rows key by key (`rows counted:`)."""
    h = _hop("postgres", ops={"public.orders": "i,u"})
    one = {"check": "data", "scope": "shop", "status": "diff",
           "detail": "tables differ: public.orders (pk-level files written);"
                     " rows counted: public.orders missing=0 extra=3"
                     " changed=0"}
    assert verdict.decide([one], h).status == "same"
    assert one["kept_by_hop"].startswith("public.orders: kept by the hop")
    two = {"check": "data", "scope": "shop", "status": "diff",
           "detail": "tables differ: public.orders, public.lines (pk-level"
                     " files written); rows counted: public.orders missing=0"
                     " extra=3 changed=0, public.lines missing=1 extra=0"
                     " changed=0"}
    assert verdict.decide([two], h).status == "different"
    assert "public.orders: kept by the hop" in two["detail"]
    # a table whose rows were not counted (no key) is not explained
    three = {"check": "data", "scope": "shop", "status": "diff",
             "detail": "tables differ: public.orders, public.nokey (pk-level"
                       " files written); rows counted: public.orders"
                       " missing=0 extra=3 changed=0"}
    h3 = _hop("postgres", ops={"public.*": "i,u"})
    assert verdict.decide([three], h3).status == "different"


def test_a_second_reading_that_names_the_same_rows_is_kept_too():
    h = _hop(ops=NO_DELETES)
    data = _data("missing=0 extra=2 changed=0")
    agrees = {"check": "second reading", "scope": "shop.shop.orders rows",
              "status": "diff", "detail": "read a second way, by each row's"
                                          " hash: the same 2 rows differ"}
    assert verdict.decide([data, agrees], h).status == "same"
    assert agrees["kept_by_hop"]
    data = _data("missing=0 extra=2 changed=0")
    apart = {"check": "second reading", "scope": "shop.shop.orders rows",
             "status": "diff", "detail": "read two ways, with two answers: 1"
                                         " rows differ only when read the"
                                         " second way"}
    assert verdict.decide([data, apart], h).status == "different"


def test_held_once_whoever_asks_again():
    h = _hop(ops=NO_DELETES)
    r = _data("missing=2 extra=5 changed=0")
    verdict.decide([r], h)
    said = r["detail"]
    verdict.decide([r], h)
    verdict.summarize(h, [r])
    assert r["detail"] == said


def test_the_verdict_file_and_the_report_say_the_hops_contract(tmp_path):
    h = _hop(ops=NO_DELETES)
    h.report_dir = lambda db="": tmp_path
    r = _data("missing=0 extra=5 changed=0")
    env = verdict.summarize(h, [r])
    assert env["status"] == "same" and not env["has_differences"]
    assert env["findings"][0]["kept_by_hop"]
    assert env["scope"]["changes_carried"] == [
        "shop.orders: insert, update; deletes are not carried - the target"
        " keeps the rows the source deletes"]
    html = report.render(h, [r])
    assert "KEPT" in html and "deletes are not carried" in html


def test_a_hop_that_carries_every_change_reads_as_before():
    h = _hop()
    r = _data("missing=0 extra=5 changed=0")
    before = dict(r)
    assert verdict.hold(h, [r]) == [r] and r == before
