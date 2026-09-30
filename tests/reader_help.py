"""Run by the second reader's own Python, never migkit's: every flag its
command line documents, one line, for
`test_every_verifier_feature_is_used_or_explained`. Only what follows
each help's `command:` line is read - above it are the reader's own
examples, which use flags of engines migkit does not hand it."""
import contextlib
import io
import re
import sys

from data_validation import cli_tools

found = set()
for sub in (["validate", "column"], ["validate", "row"],
            ["validate", "schema"], ["validate", "custom-query", "column"],
            ["validate", "custom-query", "row"], ["configs", "run"],
            ["configs", "list"], ["configs", "get"],
            ["generate-table-partitions"], ["find-tables"], ["query"],
            ["connections", "add"], ["connections", "list"],
            ["connections", "delete"], ["connections", "describe"]):
    out = io.StringIO()
    sys.argv = ["data-validation"] + sub + ["--help"]
    with contextlib.redirect_stdout(out):
        try:
            cli_tools.get_parsed_args()
        except SystemExit:
            pass
    text = out.getvalue().split("command:", 1)[-1]
    found |= set(re.findall(r"(?<![\w-])(--[a-z][a-z0-9_-]*)", text))
print(" ".join(sorted(found)))
