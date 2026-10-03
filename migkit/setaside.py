"""What a load takes off a target to put back afterwards, kept on disk per
process.

A load that drops the target's secondary indexes or triggers saves their
definitions first and puts them back on the way out. A process killed in
between does not come back to do it. The next load used to save its own
definitions over the same file, and the dead one's were gone - nothing
left said those indexes or triggers had ever been there. So each load
writes its own file, named for its host and process:
* a later load finds the files whose process is gone and puts back what
  they list and the target no longer has, at its own end
* a load that runs beside a live one (a repair beside a change tail)
  leaves that one's file alone, since what it lists is off on purpose
* `check` reads the same files
"""
import json
import os
import socket


def owner_of(host, pid):
    return f"{host}:{pid}"


class SetAside:
    """One load's record of what it took off, under `kind` - the file-name
    stem, `dropped-indexes` or `dropped-triggers`."""

    _opened = 0

    def __init__(self, hop, db, kind):
        self.hop = hop
        self.dir = hop.report_dir(db)
        self.kind = kind
        self.path = None
        self._leases = None

    def records(self):
        """[(file, its process still running, {name: definition} or None
        where the file cannot be read)], this load's own file included."""
        out = []
        try:
            found = sorted(self.dir.rglob(f"{self.kind}.*.json"))
        except OSError:
            # a report directory that cannot be read holds no record; the
            # save that follows fails the same way and nothing is dropped
            found = []
        self._leases = None
        for path in found:
            owner = path.name[len(self.kind) + 1:-len(".json")]
            host, pid = (owner.rsplit(".", 2) + ["", ""])[:2]
            try:
                defs = json.loads(path.read_text())
            except (OSError, ValueError):
                defs = None
            out.append((path, self._alive(host, pid), defs))
        return out

    def _alive(self, host, pid):
        """Whether the load that wrote a record may still run - one answer
        with every other "is it still running?" (`lease.liveness`, audit
        ST10). On this machine its process is asked. On another, its
        lease says: every load that sets something aside holds one while
        it runs - the hop's (a move, a repair) or its tail's - renewed by
        a heartbeat, and a pod replaced on Kubernetes is a new host whose
        records were alive for good while another host's counted as
        running whatever became of it."""
        from . import lease
        if host == socket.gethostname():
            return lease.liveness({"holder": owner_of(host, pid),
                                   "host": host, "pid": pid})
        for rec in self._held():
            if str(rec.get("host")) == host and str(rec.get("pid")) == \
                    str(pid):
                return lease.liveness(rec)
        # no lease names it: stopped - leases are taken before anything is
        # set aside and let go after it is put back
        return False

    def _held(self):
        """The lease records under the hop's reports: its own, each
        database's tail's (`tailctl.PID`), and the hop's in its shared run
        state where it keeps one."""
        if self._leases is not None:
            return self._leases
        from . import lease, tailctl
        from .state import run_state
        out = []
        root = self.hop.report_dir()
        paths = [root / "lease.json"] + sorted(root.glob(f"*/{tailctl.PID}"))
        for path in paths:
            rec = lease._FileRecord(path, tailctl._legacy).read()
            if isinstance(rec, dict):
                out.append(rec)
        try:
            remote = run_state(self.hop)
        except (Exception, SystemExit):  # noqa: BLE001 - not reachable
            remote = None
        if remote is not None:
            try:
                rec, _ = remote.read_record(root / "lease.json")
            except Exception:  # noqa: BLE001 - not reachable: none known
                rec = None
            if isinstance(rec, dict):
                out.append(rec)
        self._leases = out
        return out

    def left_behind(self, present=()):
        """({name: definition} a load that died took off and the target
        does not have now, [its files]) - to be put back by this one."""
        defs, files = {}, []
        for path, alive, got in self.records():
            if alive or got is None or path == self.path:
                continue
            defs.update({n: d for n, d in got.items() if n not in present})
            files.append(path)
        return defs, files

    def save(self, defs, stale=()):
        """Write this load's file before anything is taken off; then the
        dead loads' files, whose contents it now holds, go. False when it
        could not be written, and then nothing may be taken off."""
        from . import indexes as _ix
        if self.path is None:
            SetAside._opened += 1
            self.path = self.dir / (f"{self.kind}.{socket.gethostname()}"
                                    f".{os.getpid()}.{SetAside._opened}"
                                    ".json")
        if not _ix.saved(self.path, defs):
            self.path = None
            return False
        for path in stale:
            path.unlink(missing_ok=True)
        return True

    def done(self):
        """Everything went back: this load's record is no longer needed."""
        if self.path is not None:
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                pass
