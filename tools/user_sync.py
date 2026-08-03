#!/usr/bin/env python3
"""Thin wrapper around `migkit users` - the logic lives in migkit/users.py.
Kept so the check can run standalone: user_sync.py <hop> {test|create|verify|rollback}
[--apply] [--passwords pw.yaml]. Same behavior as the migkit command.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from migkit.users import run  # noqa: E402

if __name__ == "__main__":
    a = sys.argv[1:]
    if len(a) < 2 or a[1] not in ("test", "create", "verify", "rollback"):
        print("usage: user_sync.py <hop> {test|create|verify|rollback} [--apply] [--passwords pw.yaml]")
        sys.exit(1)
    pwf = a[a.index("--passwords") + 1] if "--passwords" in a else ""
    try:
        run(a[0], a[1], "--apply" in a, pwf)
    except SystemExit:
        raise
    except Exception as _e:
        print(f"ERROR: {type(_e).__name__}: {str(_e)[:140]}")
        sys.exit(1)
