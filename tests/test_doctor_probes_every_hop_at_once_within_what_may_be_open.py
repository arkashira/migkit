"""Doctor's probes of the hops are as wide as the hops, held back only by
the descriptors this process may have open."""
import resource

from migkit import decide


def test_every_hop_is_probed_at_once_where_the_limit_allows(monkeypatch):
    monkeypatch.setattr(resource, "getrlimit", lambda _w: (65536, 65536))
    assert decide.probe_width(40, 8) == 40


def test_a_tight_descriptor_limit_narrows_it_but_never_to_nothing(monkeypatch):
    monkeypatch.setattr(resource, "getrlimit", lambda _w: (200, 200))
    assert decide.probe_width(40, 8) == 17
    monkeypatch.setattr(resource, "getrlimit", lambda _w: (10, 10))
    assert decide.probe_width(40, 8) == 1


def test_no_hops_is_one_probe_not_zero():
    assert decide.probe_width(0) == 1
