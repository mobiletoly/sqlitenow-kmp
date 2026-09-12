#!/usr/bin/env python3
"""Run the retained qualification and repository-hygiene validation boundary."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
COMMANDS = (
    [sys.executable, "-m", "unittest", "discover", "-s", "tools/qualification/tests", "-p", "test_*.py"],
    [sys.executable, "-m", "unittest", "discover", "-s", "tools/tests", "-p", "test_*.py"],
    [sys.executable, "tools/check_repository_hygiene.py"],
)


def main() -> int:
    for command in COMMANDS:
        completed = subprocess.run(command, cwd=ROOT, check=False)
        if completed.returncode:
            return completed.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
