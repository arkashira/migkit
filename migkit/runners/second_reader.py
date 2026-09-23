"""Runs inside the second reader's own environment, never migkit's.

The second reader pins a stack older than migkit's (numpy, pandas, pyarrow,
sqlglot), so it lives in a virtual environment of its own and migkit talks
to it through this file: a job as JSON on stdin, findings as JSON on
stdout. Nothing here imports migkit.

What it must never do, read in the reader's source before it was trusted:
* print its results table - the collector below takes the frame instead
* write results into a database - its postgres result handler would; the
  collector replaces every handler
* keep connections - they are written to a directory made for this run,
  owner-only, and removed when the run ends
"""
import json
import os
import shutil
import sys
import tempfile


class _Collect:
    """A result handler that keeps the frame and does nothing else."""

    def execute(self, result_df):
        return result_df


def _records(df):
    out = []
    for row in df.to_dict(orient="records"):
        out.append({k: (None if v != v else v) if isinstance(v, float)
                    else (v if isinstance(v, (int, str, bool, type(None)))
                          else str(v))
                    for k, v in row.items()})
    return out


def run(job):
    home = tempfile.mkdtemp(prefix="migkit-reader-")
    os.chmod(home, 0o700)
    try:
        # the variable's name read from the reader itself: written by hand
        # it was wrong, and the reader went looking in the user's home
        from data_validation import consts
        os.environ[consts.ENV_DIRECTORY_VAR] = home
        from data_validation import __main__ as dv
        from data_validation import cli_tools, state_manager
        from data_validation.data_validation import DataValidation
        state = state_manager.StateManager(home)
        state.create_connection("src", job["source"])
        state.create_connection("dst", job["target"])
        argv = ["validate", job.get("kind", "column"), "-sc", "src",
                "-tc", "dst", "-tbls", ",".join(job["tables"])]
        argv += job.get("args", ["--count", "*"])
        sys.argv = ["data-validation"] + argv
        args = cli_tools.get_parsed_args()
        results = []
        for manager in dv.build_config_managers_from_args(args):
            with DataValidation(manager.config, validation_builder=None,
                                result_handler=_Collect()) as validator:
                results.extend(_records(validator.execute()))
        return {"ok": True, "results": results}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"[:500]}
    finally:
        shutil.rmtree(home, ignore_errors=True)


if __name__ == "__main__":
    json.dump(run(json.load(sys.stdin)), sys.stdout, default=str)
