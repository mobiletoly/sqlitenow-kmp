#!/usr/bin/env python3
"""Harmless controlled process used only by qualification infrastructure tests."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--status", type=int, default=0)
    parser.add_argument("--wait-for", type=Path)
    parser.add_argument("--wait-for-start-count", type=int)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--child-pid-file", type=Path)
    options = parser.parse_args()
    if options.pid_file:
        options.pid_file.write_text(str(os.getpid()), encoding="utf-8")
    options.events.parent.mkdir(parents=True, exist_ok=True)
    with options.events.open("a", encoding="utf-8") as output:
        output.write(f"start:{options.label}\n")
        output.flush()
    child: subprocess.Popen[bytes] | None = None
    if options.child:
        arguments = [
            sys.executable, __file__, "--events", str(options.events),
            "--label", f"child-{options.label}", "--sleep", "60",
        ]
        if options.child_pid_file:
            arguments += ["--pid-file", str(options.child_pid_file)]
        child = subprocess.Popen(arguments)

    def stop(signum: int, frame: object) -> None:
        del frame
        if child is not None:
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while options.wait_for and not options.wait_for.exists():
        time.sleep(0.01)
    while options.wait_for_start_count:
        starts = sum(
            line.startswith("start:")
            for line in options.events.read_text(encoding="utf-8").splitlines()
        )
        if starts >= options.wait_for_start_count:
            break
        time.sleep(0.01)
    if options.sleep:
        time.sleep(options.sleep)
    with options.events.open("a", encoding="utf-8") as output:
        output.write(f"finish:{options.label}\n")
    return options.status


if __name__ == "__main__":
    raise SystemExit(main())
