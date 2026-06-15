#!/usr/bin/env python3
"""
Read package name and version from pyproject.toml.
Used by python-build.yml to avoid shell/Python heredoc quoting issues.

Usage:
    python scripts/read_pkg_meta.py --field name
    python scripts/read_pkg_meta.py --field version
"""
import argparse
import sys
import tomllib
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field", choices=["name", "version"], required=True)
    args = parser.parse_args()

    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        print(f"ERROR: pyproject.toml not found in {Path.cwd()}", file=sys.stderr)
        sys.exit(1)

    with open(pyproject, "rb") as f:
        data = tomllib.load(f)

    value = data["project"][args.field]
    print(value)


if __name__ == "__main__":
    main()