#!/usr/bin/env python3
"""Stamp `__version__` in sql_process.py with the current git short
SHA + commit date.

Run from the repo root or this script's directory:

    python examples/sql_process/scripts/stamp_version.py

This script is also useful as a pre-commit / post-commit hook. Add
to `.git/hooks/post-commit`:

    #!/bin/sh
    python examples/sql_process/scripts/stamp_version.py
    git add examples/sql_process/sql_process.py
    git commit --amend --no-edit --no-verify

(or run the script before staging so the stamp lands in the same
commit as your changes).
"""
from __future__ import annotations

import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

TARGET = Path(__file__).resolve().parent.parent / "sql_process.py"
STAMP_MARKER = "## STAMP-MARKER ##"
STAMP_RE = re.compile(
    r'^__version__\s*=\s*".+?"\s*#\s*auto-updated on commit\s*#{2}\s*STAMP-MARKER\s*#{2}\s*$',
    re.MULTILINE,
)


def short_sha() -> str:
    """Current git short SHA. Returns 'dev' when not in a git checkout."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short=8", "HEAD"],
            cwd=TARGET.parent,
            stderr=subprocess.DEVNULL,
        )
        return out.decode().strip()
    except Exception:  # noqa: BLE001
        return "dev"


def commit_date() -> str:
    """ISO date of HEAD's commit (UTC)."""
    try:
        out = subprocess.check_output(
            ["git", "show", "-s", "--format=%cd", "--date=format-local:%Y-%m-%d", "HEAD"],
            cwd=TARGET.parent,
            env={"TZ": "UTC", **__import__("os").environ},
            stderr=subprocess.DEVNULL,
        )
        return out.decode().strip()
    except Exception:  # noqa: BLE001
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main() -> int:
    if not TARGET.is_file():
        print(f"Target not found: {TARGET}", file=sys.stderr)
        return 1
    sha = short_sha()
    date = commit_date()
    new_stamp = f'__version__ = "{sha} ({date})"  # auto-updated on commit  {STAMP_MARKER}'

    text = TARGET.read_text(encoding="utf-8")
    if not STAMP_RE.search(text):
        print(
            f"Stamp marker not found in {TARGET}. "
            f"Expected a line ending with: {STAMP_MARKER}",
            file=sys.stderr,
        )
        return 1
    if new_stamp in text:
        print(f"Version already current: {sha} ({date})")
        return 0

    updated = STAMP_RE.sub(new_stamp, text)
    TARGET.write_text(updated, encoding="utf-8", newline="\n")
    print(f"Stamped {TARGET.name} with {sha} ({date})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
