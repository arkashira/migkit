"""The structural diff's record is written with privileges compared: the
comparison library keys a privilege by a tuple, which its own record's
JSON refused ('keys must be str ... not tuple', W16N-DEFECTS); migkit
reads the record once (`postgres._diff_metadata`) with string keys."""
import json

from migkit.engines import postgres


class _Def:
    CATEGORIES = ("privileges",)
    pg_version = 16

    def __init__(self, privs):
        self.privileges = privs

    def as_definition(self):
        return self

    def to_dict(self):
        return {"privileges": self.privileges}


class _Changes:
    def __init__(self, a, b):
        self.i_from, self.i_target = a, b


class _Migration:
    sql = "grant select on o to app_reader;"

    def __init__(self, a, b):
        self.changes = _Changes(a, b)


def test_a_privilege_keyed_by_a_tuple_is_written_and_counted():
    grant = ("table", '"public"."o"', "app_reader")
    m = _Migration(_Def({}), _Def({grant: {"privilege": "select"}}))
    meta = postgres._diff_metadata(m, {"privileges": True})
    text = json.dumps(meta, sort_keys=True)
    assert json.loads(text)["totals"] == {"added": 1, "removed": 0,
                                          "modified": 0}
    assert meta["object_counts"] == {"privileges": {
        "added": 1, "removed": 0, "modified": 0}}
    assert meta["source"]["fingerprint"] != meta["target"]["fingerprint"]
    assert postgres._str_keys({grant: [{(1, 2): "x"}]}) == {
        'table."public"."o".app_reader': [{"1.2": "x"}]}
