"""A fork change, Redis with its JSON module into Valkey (backlog
P0-SCENARIOS): both brands read from their own INFO, the plan saying "a
fork change", every command migkit sends asked of the target, and a module
the source loads and the target does not held to whether the source's
keys hold its types - a module loaded and unused is no refusal, one whose
type a key holds is.

Before: the brands were read, and nothing said which of the source's key
types the target could not take - found as a RESTORE the target refused,
key by key, half way through the copy.
"""
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

SRC = ("migkit-test-fork-redis-json", 16404,
       "redis/redis-stack-server:7.4.0-v1")
DST = ("migkit-test-fork-valkey", 16405, "valkey/valkey:8")


@pytest.fixture(scope="module")
def servers():
    for name, port, image in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"127.0.0.1:{port}:6379", image], check=True,
                       capture_output=True)
    try:
        import redis
        for _, port, _ in (SRC, DST):
            for _ in range(60):
                try:
                    redis.Redis(host="127.0.0.1", port=port).ping()
                    break
                except Exception:
                    time.sleep(1)
        yield
    finally:
        for name, _, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="fork-redis", engine="redis", goal="cutover",
              source=Endpoint(host="127.0.0.1", port=SRC[1]),
              target=Endpoint(host="127.0.0.1", port=DST[1]),
              databases=["0"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _rows(hop):
    from migkit import case
    from migkit.engines.redis import RedisEngine
    return {r["item"]: r for r in case.checks(hop, RedisEngine(hop), ["0"])}


def test_a_module_the_source_uses_is_refused_and_one_it_only_loads_is_not(
        servers, tmp_path):
    import redis
    hop = _hop(tmp_path)
    by = _rows(hop)
    named = by["what the two servers are"]["detail"]
    assert named.startswith("a fork change: Redis 7.4"), named
    assert "to Valkey 8" in named
    assert by["commands"]["level"] == "pass", by["commands"]
    # loaded on the source, no key of its type: said, not refused
    assert by["module rejson"]["level"] == "pass", by["module rejson"]
    redis.Redis(host="127.0.0.1", port=SRC[1]).execute_command(
        "JSON.SET", "doc:1", "$", '{"a": 1}')
    by = _rows(hop)
    got = by["module rejson"]
    assert got["level"] == "fail", got
    assert "keys of its types are refused by the target" in got["detail"]
    assert "[fix] load rejson on the target" in got["detail"]
