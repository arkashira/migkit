"""The MongoDB deep check names what a document's values became on the
target, in the words every engine's deep check says them in
(P0-PARITY.verify): a 64-bit integer held as a 32-bit one (narrowing), a
decimal128 or a long past 2^53 held as a double (precision), a changed
collation, the text of a type a driver renders by its own rule, dates
moved by a zone's offset and replacement characters a lossy transcode
left.

The row comparison, by the stored bytes, already sees that a document
differs; these say which field, which way, and what was lost. The
server is one in memory (`tests/mongo_fake.py`); the types are read the
way the engine reads them (`field_types`, the whole collection).
"""
import datetime
from decimal import Decimal

from bson.decimal128 import Decimal128
from bson.int64 import Int64

from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.value.narrowing", "mongodb"),
          ("verify.value.precision", "mongodb"),
          ("verify.value.collation", "mongodb"),
          ("verify.value.rendering", "mongodb"),
          ("verify.value.timezone", "mongodb")]


def _deep(tmp_path, src_docs, dst_docs, src_kw=None, dst_kw=None, **hop):
    eng, src, dst = engine(hop, tmp_path)
    src.db("shop").add("orders", src_docs, **(src_kw or {}))
    dst.db("shop").add("orders", dst_docs, **(dst_kw or {}))
    got = {r.scope.split(" ", 1)[1]: r for r in eng.check_deep("shop")
           if r.scope.startswith("shop ")}
    return got, eng


def test_a_long_held_as_an_int_is_a_narrowing(tmp_path):
    got, _ = _deep(tmp_path, [{"_id": 1, "big": Int64(2 ** 40), "n": 1}],
                   [{"_id": 1, "big": 7, "n": 1}])
    r = got["narrowing"]
    assert r.status == "diff" and r.category == "value.narrowing"
    assert "orders.big: long -> int (overflow)" in r.detail
    assert "orders.n" not in r.detail


def test_the_same_types_narrow_nothing(tmp_path):
    got, _ = _deep(tmp_path, [{"_id": 1, "big": Int64(1), "n": 1}],
                   [{"_id": 1, "big": Int64(1), "n": 1}])
    assert got["narrowing"].status == "ok"
    assert got["float"].status == "ok"


def test_a_widened_integer_is_not_a_narrowing(tmp_path):
    got, _ = _deep(tmp_path, [{"_id": 1, "n": 1}],
                   [{"_id": 1, "n": Int64(1)}])
    assert got["narrowing"].status == "ok"


def test_a_decimal_held_as_a_double_loses_digits(tmp_path):
    got, _ = _deep(
        tmp_path,
        [{"_id": 1, "price": Decimal128(Decimal("12.34567890123456789")),
          "id64": Int64(2 ** 60), "ok": Int64(3)}],
        [{"_id": 1, "price": 12.345678901234567, "id64": 2.0 ** 60,
          "ok": Int64(3)}])
    r = got["float"]
    assert r.status == "diff" and r.category == "value.precision"
    assert "orders.price: decimal -> double" in r.detail
    assert "orders.id64: long -> double" in r.detail
    assert "orders.ok" not in r.detail


def test_a_double_cut_to_an_integer_loses_its_fraction(tmp_path):
    got, _ = _deep(tmp_path, [{"_id": 1, "w": 1.5}], [{"_id": 1, "w": 1}])
    assert "orders.w: double -> int (the fraction is cut)" in \
        got["float"].detail


def test_a_collation_that_changed_is_named(tmp_path):
    collation = {"locale": "en", "strength": 2, "version": "57.1"}
    got, _ = _deep(
        tmp_path, [{"_id": 1, "name": "A"}], [{"_id": 1, "name": "A"}],
        src_kw={"options": {"collation": collation},
                "indexes": {"name_1": {"key": {"name": 1},
                                       "collation": collation}}},
        dst_kw={"indexes": {"name_1": {"key": {"name": 1}}}})
    r = got["collation"]
    assert r.status == "diff" and r.category == "value.collation"
    assert "orders: default collation src=" in r.detail
    assert "dst=binary" in r.detail
    assert "orders.name_1: index collation" in r.detail


def test_a_collation_of_another_version_is_the_same_collation(tmp_path):
    got, _ = _deep(
        tmp_path, [{"_id": 1}], [{"_id": 1}],
        src_kw={"options": {"collation": {"locale": "en", "version": "57.1"}}},
        dst_kw={"options": {"collation": {"locale": "en", "version": "60.2"}}})
    assert got["collation"].status == "ok"


def test_a_decimal_that_prints_another_scale_renders_differently(tmp_path):
    got, _ = _deep(
        tmp_path, [{"_id": 1, "p": Decimal128(Decimal("1.50"))}],
        [{"_id": 1, "p": Decimal128(Decimal("1.5"))}])
    r = got["render"]
    assert r.status == "diff" and r.category == "value.rendering"
    assert "orders.p: renders differently" in r.detail
    # and the types are the same, so nothing is narrowed or lost
    assert got["float"].status == "ok" and got["narrowing"].status == "ok"


def test_values_that_render_alike_say_how_many_were_sampled(tmp_path):
    same = Decimal128(Decimal("1.50"))
    got, _ = _deep(tmp_path, [{"_id": 1, "p": same}], [{"_id": 1, "p": same}])
    assert got["render"].status == "ok"
    assert "1 fields of types a driver renders" in got["render"].detail


def test_dates_all_shifted_by_the_same_hours_are_a_zone_not_row_damage(
        tmp_path):
    base = datetime.datetime(2026, 1, 1, 7)
    day = datetime.timedelta(days=1)
    src = [{"_id": i, "at": base + i * day} for i in range(1, 7)]
    dst = [{"_id": i, "at": base + i * day + datetime.timedelta(hours=7)}
           for i in range(1, 7)]
    got, _ = _deep(tmp_path, src, dst)
    r = got["timeshift"]
    assert r.status == "diff" and r.category == "value.timezone"
    assert "orders.at: every row shifted 25200s" in r.detail


def test_dates_that_agree_are_not_a_shift(tmp_path):
    base = datetime.datetime(2026, 1, 1, 7)
    docs = [{"_id": i, "at": base + datetime.timedelta(days=i)}
            for i in range(1, 7)]
    got, _ = _deep(tmp_path, docs, docs)
    assert got["timeshift"].status == "ok"


def test_dates_each_moved_by_their_own_amount_are_left_to_the_row_check(
        tmp_path):
    base = datetime.datetime(2026, 1, 1)
    src = [{"_id": i, "at": base} for i in range(1, 7)]
    dst = [{"_id": i, "at": base + datetime.timedelta(minutes=i * 7)}
           for i in range(1, 7)]
    got, _ = _deep(tmp_path, src, dst)
    assert got["timeshift"].status == "ok"


def test_replacement_characters_the_target_has_more_of_are_named(tmp_path):
    got, _ = _deep(tmp_path, [{"_id": 1, "t": "café"}],
                   [{"_id": 1, "t": "caf�"}])
    r = got["encoding"]
    assert r.status == "diff" and r.category == "value.charset"
    assert "orders.t: 1 more documents" in r.detail


def test_a_field_whose_types_were_unreadable_is_said_not_passed(
        tmp_path, monkeypatch):
    from migkit.engines.mongodb import MongoEngine
    real = MongoEngine.field_types

    def refuse(self, side, db, collection):
        if side == "dst":
            raise RuntimeError("not authorized on shop")
        return real(self, side, db, collection)
    monkeypatch.setattr(MongoEngine, "field_types", refuse)
    eng, src, dst = engine({}, tmp_path)
    src.db("shop").add("orders", [{"_id": 1}])
    dst.db("shop").add("orders", [{"_id": 1}])
    warn = [r for r in eng.check_deep("shop") if r.status == "warn"]
    assert warn and "orders (not authorized on shop)" in warn[0].detail
    assert warn[0].category == "value.narrowing"
