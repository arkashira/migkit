"""What the cutover tests share: an application that keeps writing and
records every write, the addresses the two containers reach each other
by, and a runner for a migkit killed half way.

Run as a program it is migkit's command line with those addresses in
place (`MIGKIT_TEST_ROUTES`), which is how a test runs a cutover in a
process of its own to kill it with `MIGKIT_FAILPOINT=...:exit`.
"""
import json
import os
import sys
import threading
import time
import uuid


def rewrite(stmt, routes):
    """A replication statement's address of a side, as this machine
    reaches it, made the one the other container reaches it by."""
    for mine, theirs in routes.items():
        host, port = mine.split(":")
        to_host, to_port = theirs.split(":")
        stmt = stmt.replace(f"host={host} port={port}",
                            f"host={to_host} port={to_port}")
        stmt = stmt.replace(f"SOURCE_HOST = '{host}', SOURCE_PORT = {port}",
                            f"SOURCE_HOST = '{to_host}',"
                            f" SOURCE_PORT = {to_port}")
    return stmt


def install(routes, monkeypatch=None):
    """Every engine's replication statements run with `routes` applied."""
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    for cls in (PostgresEngine, MySQLEngine):
        real = cls.apply_replication_stmt

        def routed(self, side, db, stmt, real=real):
            return real(self, side, db, rewrite(stmt, routes))
        if monkeypatch is not None:
            monkeypatch.setattr(cls, "apply_replication_stmt", routed)
        else:
            cls.apply_replication_stmt = routed


class Writer(threading.Thread):
    """The application: one row at a time, each committed alone, to
    whichever side the route file names, as the account it writes with.
    Every attempt is recorded - when it began and ended, where it went,
    and whether it was acknowledged - so the outage is measured where the
    application sees it, and every acknowledged row can be looked for."""

    def __init__(self, connect, route, table="public.orders"):
        super().__init__(daemon=True)
        self.connect, self.route, self.table = connect, route, table
        self.log = []
        self.halt = threading.Event()
        self.conn = {}

    def _conn(self, where):
        if where not in self.conn:
            self.conn[where] = self.connect(where)
        return self.conn[where]

    def run(self):
        while not self.halt.is_set():
            where = self.route.read_text().strip()
            tag = uuid.uuid4().hex
            began = time.time()
            try:
                conn = self._conn(where)
                cur = conn.cursor()
                cur.execute(f"insert into {self.table} (tag) values (%s)",
                            (tag,))
                conn.commit()
                self.log.append((began, time.time(), where, "ok", tag))
            except Exception as e:  # noqa: BLE001 - the application's error
                self.log.append((began, time.time(), where, "error",
                                 f"{tag} {str(e).strip()[:120]}"))
                old = self.conn.pop(where, None)
                try:
                    old.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.05)
            time.sleep(0.01)

    def stop(self):
        self.halt.set()
        self.join(30)
        for conn in self.conn.values():
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def acked(self):
        return [e[4] for e in self.log if e[3] == "ok"]

    def attempted(self):
        return {e[4].split()[0] for e in self.log}

    def errors(self):
        return [e for e in self.log if e[3] == "error"]

    def outage(self):
        """The longest the application went without an acknowledged
        write, in seconds: from the last before the cutover to the first
        after it."""
        done = sorted(e[1] for e in self.log if e[3] == "ok")
        return max((b - a for a, b in zip(done, done[1:])), default=0.0)

    def oks_after(self, when):
        return [e for e in self.log if e[3] == "ok" and e[0] > when]


def wait_for(fn, timeout=60, every=0.25):
    end = time.time() + timeout
    while time.time() < end:
        got = fn()
        if got:
            return got
        time.sleep(every)
    return fn()


def when_phase(report_dir, phase, act, timeout=120):
    """Run `act` once a cutover recorded in `report_dir` begins `phase`:
    a fault made at the moment the path reaches it."""
    path = report_dir / "cutover.json"

    def watch():
        end = time.time() + timeout
        while time.time() < end:
            try:
                rec = json.loads(path.read_text())
            except (OSError, ValueError):
                rec = {}
            if phase in (rec.get("phases") or {}):
                act()
                return
            time.sleep(0.02)
    t = threading.Thread(target=watch, daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    install(dict(x.split("=", 1) for x in
                 os.environ.get("MIGKIT_TEST_ROUTES", "").split(",") if x))
    from migkit.cli import main
    sys.exit(main())
