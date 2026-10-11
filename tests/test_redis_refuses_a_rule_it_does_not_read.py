"""Row filters, column rules, masked values and renames are read by no
Redis path: a hop that asks for one is stopped before anything is read or
written, never carried whole in its place (P0-PARITY.scope). Run against a
real server, started here."""
import pytest

from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

pytestmark = needs_server


class _Saved(dict):
    def save(self):
        pass


@pytest.mark.parametrize("hop", [
    {"mapping": {"where": {"db0": "x > 1"}}},
    {"mapping": {"tables": {"db0": "db1"}}},
    {"mapping": {"columns": {"db0": {"drop": ["a"]}}}},
    {"mapping": {"names": {"table": {"case": "lower"}}}},
    {"options": {"mask_on_write": {"db0.a": "null"}}},
])
def test_the_copy_and_the_checks_stop(clean, tmp_path, hop):
    src, dst = (p.client() for p in clean)
    src.set("k", "v")
    eng = engine(clean, tmp_path, **hop)
    for call in (lambda: eng.check_counts("0"),
                 lambda: eng.check_data("0"),
                 lambda: eng.move_table("0", "", "0", 1000, _Saved(),
                                        lambda m: None)):
        with pytest.raises(SystemExit) as stopped:
            call()
        assert "which this engine does not read" in str(stopped.value)
    assert dst.dbsize() == 0


def test_a_hop_that_asks_for_none_of_them_goes_on(clean, tmp_path):
    src, dst = (p.client() for p in clean)
    src.set("k", "v")
    dst.set("k", "v")
    eng = engine(clean, tmp_path, exclude=["cache:*"])
    assert eng.check_counts("0")[0].status == "ok"
