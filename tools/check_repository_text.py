#!/usr/bin/env python3
"""Check maintained Git-visible text and paths for caller-supplied forbidden tokens."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--forbidden-token', action='append', required=True)
    parser.add_argument('--include', action='append', default=[])
    args = parser.parse_args()
    root = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip())
    paths = set(filter(None, subprocess.check_output(
        ['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard']).decode().split('\0')))
    paths.update(args.include)
    tokens = [token.casefold() for token in args.forbidden_token]
    if any(not token for token in tokens):
        parser.error('tokens must not be empty')
    matches, checked = [], 0
    for relative in sorted(paths):
        path = root / relative
        if path.is_symlink() or not path.is_file():
            continue
        if not path.resolve().is_relative_to(root):
            parser.error('included paths must remain inside the repository')
        if any(token in relative.casefold() for token in tokens):
            matches.append({'path': relative, 'kind': 'path'})
        raw = path.read_bytes()
        if b'\0' in raw:
            continue
        try:
            lines = raw.decode('utf-8').splitlines()
        except UnicodeDecodeError:
            continue
        checked += 1
        for number, line in enumerate(lines, 1):
            if any(token in line.casefold() for token in tokens):
                matches.append({'path': relative, 'line': number, 'kind': 'text'})
    print(json.dumps({'status': 'FAIL' if matches else 'PASS', 'checked_text_files': checked, 'matches': matches}, indent=2))
    return 1 if matches else 0


if __name__ == '__main__':
    raise SystemExit(main())
