"""One run started from the view, in a process of its own:
`python -m migkit.web.runner <run.json>` (`runs.start`).

It runs the command line's own `main` with the run's arguments - the
command an operator types, nothing else - and writes to the run's record
that it started (its pid) and how it ended (its exit code), so the view
knows both after a restart of its own. Its output is the run's output
file, which the view opened for it.
"""
import os
import signal
import socket
import sys
import time
import traceback
from pathlib import Path


def _write(path, **changes):
    from .. import durable
    rec = durable.read_state(path, "cache") or {}
    rec.update(changes)
    durable.write_json(path, rec, indent=1)
    return rec


def _as_ctrl_c(signum, frame):
    raise KeyboardInterrupt


def main(argv):
    from .. import durable
    # stopped from the view as a terminal stops a run: what ctrl-c does,
    # the command's own clean-up included
    signal.signal(signal.SIGTERM, _as_ctrl_c)
    path = Path(argv[1])
    rec = durable.read_state(path, "state")
    _write(path, state="running", pid=os.getpid(),
           host=socket.gethostname(), started=time.time())
    from ..verdict import EXIT, Refused, Usage
    code = EXIT["done"]
    try:
        from ..cli import main as cli
        cli.main(args=list(rec["argv"]), prog_name="migkit",
                 standalone_mode=True)
    except SystemExit as e:
        if e.code is None:
            code = EXIT["done"]
        elif isinstance(e.code, int):
            code = e.code
        else:
            # migkit stopping, in words: said where the run's output is,
            # ended with the code the command line's own table gives it
            print(e.code, file=sys.stderr)
            code = (EXIT["refused"] if isinstance(e, Refused) else
                    EXIT["usage"] if isinstance(e, Usage) else
                    EXIT["failed"])
    except KeyboardInterrupt:
        code = EXIT["interrupted"]
    except BaseException:  # noqa: BLE001 - said, and the run marked ended
        traceback.print_exc()
        code = EXIT["failed"]
    sys.stdout.flush()
    sys.stderr.flush()
    _write(path, state="ended", exit=code, ended=time.time())
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv))
