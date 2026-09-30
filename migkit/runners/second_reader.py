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

What it does with the reader's own machinery rather than beside it:
* every validation of the run is written into one batch file in the
  reader's own format, and the run is executed from that file, the way
  its `configs run` executes one - so the file handed back is exactly
  what was read, and can be run again
* each validation carries a label naming what migkit asked it for, and
  comes back under it
* only the statuses a validation asks for come back. Read in its source
  (8.9.3): that filter is applied by the result handler, which the
  collector replaces, so without applying it here a row validation asked
  for its failures alone would hand back every row it read
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


def _validations(job):
    """The job's validations; a job of the first shape is one of them."""
    if job.get("validations"):
        return job["validations"]
    return [{"label": "column", "kind": job.get("kind", "column"),
             "tables": job.get("tables") or [],
             "args": job.get("args", ["--count", "*"])}]


def _argv(v):
    kind = v.get("kind", "column").split()
    argv = ["validate"] + kind + ["-sc", "src", "-tc", "dst"]
    if v.get("tables"):
        argv += ["-tbls", ",".join(v["tables"])]
    return argv + list(v.get("args") or []) + ["--labels",
                                                f"migkit={v['label']}"]


def run(job):
    home = tempfile.mkdtemp(prefix="migkit-reader-")
    os.chmod(home, 0o700)
    try:
        # the variable's name read from the reader itself: written by hand
        # it was wrong, and the reader went looking in the user's home
        from data_validation import consts
        os.environ[consts.ENV_DIRECTORY_VAR] = home
        from argparse import Namespace

        from data_validation import __main__ as dv
        from data_validation import cli_tools, state_manager
        from data_validation.data_validation import DataValidation
        state = state_manager.StateManager(home)
        state.create_connection("src", job["source"])
        state.create_connection("dst", job["target"])
        blocks, broken = [], []
        for v in _validations(job):
            sys.argv = ["data-validation"] + _argv(v)
            try:
                args = cli_tools.get_parsed_args()
                built = dv.build_config_managers_from_args(args)
            except BaseException as e:  # noqa: BLE001 - argparse exits
                broken.append({"label": v["label"], "table": None,
                               "error": f"{type(e).__name__}: {e}"[:300]})
                continue
            blocks += dv.convert_config_to_yaml(args, built)[
                consts.YAML_VALIDATIONS]
        if not blocks:
            return {"ok": False, "error": "nothing could be set up to read:"
                                          f" {json.dumps(broken)[:400]}",
                    "broken": broken}
        batch = os.path.join(home, "batch.yaml")
        cli_tools.store_validation(batch, {
            consts.YAML_SOURCE: "src", consts.YAML_TARGET: "dst",
            consts.YAML_RESULT_HANDLER: None,
            consts.YAML_VALIDATIONS: blocks}, include_log=False)
        with open(batch) as fh:
            config = fh.read()
        results, ran = [], []
        for manager in dv.build_config_managers_from_yaml(
                Namespace(config_dir=None, verbose=False), batch):
            label = dict(manager.config.get(consts.CONFIG_LABELS)
                         or []).get("migkit", "?")
            table = manager.full_source_table or label
            try:
                with DataValidation(manager.config, validation_builder=None,
                                    result_handler=_Collect()) as validator:
                    df = validator.execute()
                wanted = manager.filter_status
                if wanted:
                    df = df[df.validation_status.isin(wanted)]
                got = _records(df)
            except Exception as e:  # noqa: BLE001 - said per validation
                broken.append({"label": label, "table": table,
                               "error": f"{type(e).__name__}: {e}"[:300]})
                continue
            for r in got:
                r["migkit_label"] = label
            results.extend(got)
            # read, whether or not anything came back: a validation asked
            # for its failures alone answers nothing when there are none
            ran.append({"label": label, "table": table})
        return {"ok": True, "results": results, "ran": ran,
                "broken": broken, "config": config}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"[:500]}
    finally:
        shutil.rmtree(home, ignore_errors=True)


if __name__ == "__main__":
    job = json.load(sys.stdin)
    # the answer alone on the stream migkit reads it from: whatever the
    # reader or a library it loads prints - from Python or below it - goes
    # to the other stream, where a stray line cannot break the answer
    answer = os.fdopen(os.dup(1), "w")
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    json.dump(run(job), answer, default=str)
    answer.flush()
