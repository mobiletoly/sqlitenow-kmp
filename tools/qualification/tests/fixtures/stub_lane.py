#!/usr/bin/env python3
"""Harmless child-process stub used by qualification planner runner tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time


parser = argparse.ArgumentParser()
parser.add_argument("--sentinel", type=Path, required=True)
parser.add_argument("--status", type=int, default=0)
parser.add_argument("--label", default="stub")
parser.add_argument("--sleep-seconds", type=float, default=0)
arguments = parser.parse_args()
with arguments.sentinel.open("a", encoding="utf-8") as output:
    output.write(json.dumps({"label": arguments.label, "status": arguments.status}, sort_keys=True) + "\n")
if arguments.sleep_seconds:
    time.sleep(arguments.sleep_seconds)
raise SystemExit(arguments.status)
