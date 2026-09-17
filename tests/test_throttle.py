"""The verifier must not be able to take down what it is verifying.

Reproduces the shape of the real incident: a check with more concurrency than
the instance can serve, and a server reporting that it is in trouble.
"""
from migkit.throttle import Health, Throttle


class _Clock:
    """Controllable time, so the tests never actually sleep."""

    def __init__(self):
        self.now = 0.0
        self.slept = 0.0

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.slept += s
        self.now += s


def test_healthy_server_is_not_throttled():
    c = _Clock()
    t = Throttle(4, probe=lambda: Health(busy_ratio=0.1, lag_seconds=0.0),
                 clock=c, sleeper=c.sleep)
    for _ in range(6):
        with t.unit():
            c.now += 1.0
    assert c.slept == 0.0
    assert t.summary() == {}
    assert t.line() == ""


def test_session_pressure_narrows_concurrency_and_waits():
    c = _Clock()
    t = Throttle(8, probe=lambda: Health(busy_ratio=0.95), clock=c,
                 sleeper=c.sleep)
    with t.unit():
        c.now += 1.0
    assert c.slept > 0                      # it backed off
    assert t.narrowed_to < 8                # and used less of the server
    s = t.summary()
    assert s["concurrency_from"] == 8
    assert "sessions at 95% of the limit" in s["reasons"]
    assert "throttled" in t.line()


def test_replication_lag_counts_as_stress():
    c = _Clock()
    t = Throttle(4, probe=lambda: Health(lag_seconds=120.0), clock=c,
                 sleeper=c.sleep)
    with t.unit():
        pass
    assert any("behind" in r for r in t.summary()["reasons"])


def test_recovery_widens_again():
    c = _Clock()
    state = {"busy": 0.95}
    t = Throttle(4, probe=lambda: Health(busy_ratio=state["busy"]), clock=c,
                 sleeper=c.sleep)
    with t.unit():
        pass
    narrowed = t.permits
    assert narrowed < 4
    state["busy"] = 0.05
    c.now += 100          # past the probe cache window
    for _ in range(6):
        with t.unit():
            pass
    assert t.permits > narrowed


def test_own_latency_is_the_signal_when_the_server_reports_nothing():
    """Managed providers hide load. Our own query time still tells us."""
    c = _Clock()
    t = Throttle(4, probe=None, clock=c, sleeper=c.sleep)
    for _ in range(3):                      # establish a fast baseline
        t.observe(0.1)
    assert t.stress() == ""
    for _ in range(3):                      # then everything crawls
        t.observe(5.0)
    assert "slower" in t.stress()


def test_a_probe_that_raises_never_blocks_the_check():
    def boom():
        raise RuntimeError("permission denied for pg_stat_activity")
    c = _Clock()
    t = Throttle(4, probe=boom, clock=c, sleeper=c.sleep)
    with t.unit():
        pass
    assert c.slept == 0.0          # unknown health is not stress


def test_never_narrows_below_one_permit():
    c = _Clock()
    t = Throttle(2, probe=lambda: Health(busy_ratio=1.0), clock=c,
                 sleeper=c.sleep)
    for _ in range(10):
        with t.unit():
            pass
    assert t.permits >= 1


def test_missing_signals_are_not_read_as_healthy_or_stressed():
    assert Health().stressed() == ""
    assert Health(busy_ratio=None, lag_seconds=None).stressed() == ""
    assert Health(busy_ratio=0.99).stressed() != ""


def test_a_permanently_busy_server_still_gets_verified():
    """Backing off forever would mean the check never runs. It must finish,
    slowly, and say that it ran while the server was unhappy."""
    c = _Clock()
    t = Throttle(8, probe=lambda: Health(busy_ratio=0.99), clock=c,
                 sleeper=c.sleep)
    for _ in range(3):
        with t.unit():
            pass
    s = t.summary()
    assert t.proceeded_under_stress == 3
    assert s["proceeded_under_stress"] == 3
    assert s["concurrency_to"] == 1          # down to the floor
    assert c.slept > 0                       # and it did wait first
