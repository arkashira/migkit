"""The port a hop's change-stream runtime answers on is the hop's own fact
(AUD-C): kept in its stack, found free by a rung (`decide.connector_port`)
and never taken from another hop's stack - so two hops on one machine run
two runtimes, and no code names one number for all of them."""
import re
import socket
from pathlib import Path

from migkit import decide, movers, ui

PROVES = []


def _stack(root, hop):
    d = root / hop / "stream"
    d.mkdir(parents=True)
    return d


def test_two_hops_on_one_machine_get_two_runtimes_ports(tmp_path):
    a, b = _stack(tmp_path, "a"), _stack(tmp_path, "b")
    pa, pb = movers.connector_ports(a, make=True), \
        movers.connector_ports(b, make=True)
    assert len(set(pa) | set(pb)) == 4, (pa, pb)
    # a restart finds each where it was left
    assert movers.connector_ports(a, make=True) == pa
    assert movers.connector_ports(b) == pb


def test_a_recorded_port_another_program_holds_is_replaced(tmp_path):
    a = _stack(tmp_path, "a")
    api, readings = movers.connector_ports(a, make=True)
    with socket.socket() as held:
        held.bind(("127.0.0.1", api))
        held.listen()
        again = movers.connector_ports(a, make=True)
    assert again[0] != api and again[1] == readings
    assert movers.connector_ports(a) == again


def test_the_rung_takes_the_recorded_port_while_it_is_free_or_the_hops():
    free = decide.connector_port({"recorded": 4000, "free": True,
                                  "found": 5000})
    assert free.path == "recorded"
    mine = decide.connector_port({"recorded": 4000, "mine": True,
                                  "found": 5000})
    assert mine.path == "recorded"
    held = decide.connector_port({"recorded": 4000, "found": 5000})
    assert held.path == "found"
    assert any("another program holds" in why for _, why in held.passed)
    assert decide.connector_port({}).path is None


def test_the_metrics_ask_the_hops_own_port(tmp_path, monkeypatch):
    where = tmp_path / "h"
    stack = _stack(tmp_path, "h")
    (stack / "docker-compose.yml").write_text("services: {}\n")
    ports = movers.connector_ports(stack, make=True)
    asked = []
    monkeypatch.setattr(movers, "stream_parts",
                        lambda name, port, timeout=2:
                        asked.append(("parts", port)) or [("source", False)])
    monkeypatch.setattr(movers, "stream_readings",
                        lambda *a, **k: asked.append(("readings", a[1]))
                        or {})
    ui._stream_metrics(where, {})
    assert asked == [("parts", ports[0])]
    # a hop with no stack asks nothing, there being no port to ask
    (stack / "connector.ports").unlink()
    asked.clear()
    ui._stream_metrics(where, {})
    assert asked == []


def test_a_call_with_no_port_is_refused_not_sent_to_a_default():
    import pytest
    with pytest.raises(ValueError):
        movers._connect_api("GET", "/")


def test_no_code_names_one_port_for_every_hop():
    """The runtime image's own port inside its container is named once
    (`RUNTIME_INSIDE`); nowhere else is a connector port a number."""
    root = Path(movers.__file__).parent
    hits = []
    for f in root.rglob("*.py"):
        for n, line in enumerate(f.read_text().splitlines(), 1):
            if re.search(r"\b(8083|8778)\b", line) \
                    and "RUNTIME_INSIDE = " not in line:
                hits.append(f"{f.name}:{n}")
    assert hits == []
