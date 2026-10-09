#!/usr/bin/env python3
"""
scripts/check_adapter_checklist.py
=====================================
Enforces docs/adapter-checklist.md mechanically, instead of relying on a
human (or a subagent) re-deriving it correctly for every new package.

Every check below exists because it is the exact shape of a real bug this
repo has already shipped and had to fix — see .github/CHANGELOG.md for
each one's full incident writeup. This script does not invent new rules;
it just makes sure the ones already paid for in bugs don't recur silently.

    CHECK                                   BUG IT CATCHES (CHANGELOG entry)
    --------------------------------------  --------------------------------
    CI matrix membership                    nats/rabbitmq shipped with no
                                             CI/CD wiring at all — tests
                                             silently never ran, nothing
                                             would ever have published.
                                             [nats 0.1.0 / rabbitmq 0.1.0]
    root pythonpath / pyrightconfig         9 packages built since Wave A
                                             were never added to the
                                             monorepo-wide pytest/pyright
                                             path lists.
                                             [influxdb 0.1.0 / meta 2.5.0]
    .gitignore swallowing source            An unanchored `rabbitmq/`
                                             .gitignore pattern (meant for
                                             a local broker's runtime data)
                                             silently excluded the
                                             RabbitMQ *adapter's own
                                             source* from git — never
                                             pushed, CI saw no package.
                                             (found interactively, not yet
                                             in CHANGELOG at time of writing)
    meta package extras + pin staleness     Every new package's extras
                                             entry in meta/pyproject.toml
                                             started as a placeholder pin
                                             (>=1.1,<2) that didn't match
                                             the package's real starting
                                             version (0.1.0) — corrected by
                                             hand, repeatedly, across nearly
                                             every wave.
    bare `from conftest import X`           RabbitMQ's test_consumer.py
                                             imported a bare helper function
                                             from conftest, which only works
                                             under pytest's default import
                                             mode and silently breaks under
                                             --import-mode=importlib (what
                                             the root-level `pytest
                                             packages/` run uses).
                                             [influxdb 0.1.0 / meta 2.5.0]
    plugin.py / test version drift          Redis's plugin.py version was
                                             bumped 2.0.5 -> 2.0.6 but
                                             test_plugin.py's hardcoded
                                             assertion was not — caught only
                                             by actually re-running the
                                             suite. [redis 2.0.6]
    dev extras missing a test-runner floor  Never shipped broken, but is
                                             exactly the kind of silent
                                             per-package drift the other
                                             fixes above all stem from.
    openframe-core floor vs. port usage     Not yet a real incident, but
                                             the same class as the dev
                                             extras check: a package using
                                             BaseVectorStore/BaseGraphStore
                                             with a floor below 3.5/3.6
                                             would resolve to a core
                                             version where the port doesn't
                                             exist yet.

Usage:
    python scripts/check_adapter_checklist.py              # all packages
    python scripts/check_adapter_checklist.py --package openframe-adapters-db-falkordb
    python scripts/check_adapter_checklist.py --quiet      # only print failures/warnings

Exit code: 0 if no FAIL-level findings (WARNs don't fail the build),
1 otherwise. Suitable as a CI gate — wire it into app-test.yml as a
step that runs before the test matrix, or as a pre-commit hook.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGES_DIR = REPO_ROOT / "packages"
META_PYPROJECT = REPO_ROOT / "meta" / "openframe-adapters" / "pyproject.toml"
APP_TEST_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "app-test.yml"
PYTHON_BUILD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "python-build.yml"
ROOT_PYPROJECT = REPO_ROOT / "pyproject.toml"
PYRIGHTCONFIG = REPO_ROOT / "pyrightconfig.json"

REQUIRED_DEV_FLOORS = {
    "pytest": (8, 0),
    "pytest-asyncio": (0, 23),
    "pytest-mock": (3, 14),
}

# import name -> minimum openframe-core (major, minor) that defines it.
PORT_FLOOR_REQUIREMENTS = {
    "BaseVectorStore": (3, 5),
    "BaseGraphStore": (3, 6),
}


@dataclass
class Finding:
    package: str
    severity: str  # "FAIL" or "WARN"
    check: str
    message: str


def load_toml(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def discover_packages() -> list[Path]:
    if not PACKAGES_DIR.is_dir():
        return []
    return sorted(p for p in PACKAGES_DIR.iterdir() if p.is_dir() and (p / "pyproject.toml").exists())


def parse_version(v: str) -> tuple[int, int, int]:
    m = re.match(r"(\d+)\.(\d+)\.(\d+)", v)
    if not m:
        return (0, 0, 0)
    return tuple(int(x) for x in m.groups())  # type: ignore[return-value]


def extract_yaml_list(text: str, anchor: str) -> list[str]:
    """
    Minimal, dependency-free extraction of a YAML block-list under a given
    key (e.g. "package:"), for workflow files this repo controls the exact
    formatting of. Not a general YAML parser — deliberately narrow.
    """
    lines = text.splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.strip() == anchor)
    except StopIteration:
        return []
    items = []
    for line in lines[start + 1 :]:
        stripped = line.strip()
        if stripped.startswith("- "):
            items.append(stripped[2:].strip().strip('"'))
        elif stripped and not stripped.startswith("#"):
            break
    return items


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_ci_matrices(pkg_name: str) -> list[Finding]:
    findings = []
    app_test_text = APP_TEST_WORKFLOW.read_text()
    test_pkgs = extract_yaml_list(app_test_text, "package:")
    if pkg_name not in test_pkgs:
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "ci-app-test-matrix",
                f"Not in app-test.yml's test matrix — CI will never run this package's tests. "
                f"(This is exactly how nats/rabbitmq shipped with zero CI coverage.)",
            )
        )

    build_text = PYTHON_BUILD_WORKFLOW.read_text()
    build_pkgs = extract_yaml_list(build_text, "package:")
    expected_build_entry = f"packages/{pkg_name}"
    if expected_build_entry not in build_pkgs:
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "ci-python-build-matrix",
                f"Not in python-build.yml's release matrix as '{expected_build_entry}' — "
                f"will never be built or published to PyPI.",
            )
        )
    return findings


def check_meta_release_matrix() -> list[Finding]:
    """meta/openframe-adapters must itself be in the release matrix."""
    build_text = PYTHON_BUILD_WORKFLOW.read_text()
    build_pkgs = extract_yaml_list(build_text, "package:")
    if "meta/openframe-adapters" not in build_pkgs:
        return [
            Finding(
                "meta/openframe-adapters",
                "FAIL",
                "ci-python-build-matrix",
                "meta/openframe-adapters is not in python-build.yml's release matrix — "
                "the meta package will never be published.",
            )
        ]
    return []


def check_root_path_configs(pkg_name: str) -> list[Finding]:
    findings = []
    data = load_toml(ROOT_PYPROJECT)
    pythonpath = data.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("pythonpath", [])
    expected = f"packages/{pkg_name}"
    if expected not in pythonpath:
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "root-pythonpath",
                f"'{expected}' missing from root pyproject.toml's [tool.pytest.ini_options] "
                f"pythonpath — `pytest packages/` from the repo root will not find this "
                f"package's tests at all (9 packages shipped with this gap before it was caught).",
            )
        )

    pyright = json.loads(PYRIGHTCONFIG.read_text())
    extra_paths = pyright.get("extraPaths", [])
    normalized = {p.rstrip("/") for p in extra_paths}
    if expected not in normalized:
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "root-pyrightconfig",
                f"'{expected}' missing from root pyrightconfig.json's extraPaths — "
                f"type-checking across the monorepo will silently skip this package.",
            )
        )
    return findings


def check_gitignore_collision(pkg_dir: Path, pkg_name: str) -> list[Finding]:
    """
    Run `git check-ignore` against every real .py source file (not
    __pycache__) under this package's openframe/ tree. Catches the exact
    bug class that broke RabbitMQ: an unrelated .gitignore pattern (meant
    for a local broker's runtime data directory) silently matching a
    source directory of the same name anywhere in the repo.

    Caveat, verified directly: `git check-ignore` does not flag a path
    that is already tracked in the index, even if a .gitignore pattern
    would otherwise match it. This check is therefore only meaningful
    for a package's FIRST commit — run it before `git add`, not after.
    A package that's already committed will always read clean here
    regardless of .gitignore content, by git's own design.
    """
    openframe_dir = pkg_dir / "openframe"
    if not openframe_dir.is_dir():
        return []

    try:
        subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=REPO_ROOT, capture_output=True, check=True, text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []  # not a git repo / git unavailable — skip silently

    py_files = [p for p in openframe_dir.rglob("*.py") if "__pycache__" not in p.parts]
    if not py_files:
        return []

    result = subprocess.run(
        ["git", "check-ignore", "-v", *[str(p) for p in py_files]],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return []  # nothing ignored

    findings = []
    for line in result.stdout.strip().splitlines():
        # format: <.gitignore path>:<line no>:<pattern>\t<matched file>
        gitignore_ref, _, matched_file = line.partition("\t")
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "gitignore-collision",
                f"{matched_file} is gitignored by rule '{gitignore_ref}' — this real source "
                f"file will never be pushed to git, so CI's checkout will not have it. "
                f"(This exact bug broke RabbitMQ: an unanchored `rabbitmq/` pattern meant for "
                f"a broker's local runtime data matched the adapter's own source directory.)",
            )
        )
    return findings


def check_bare_conftest_imports(pkg_dir: Path, pkg_name: str) -> list[Finding]:
    """
    `from conftest import X` (or `from .conftest import X`) only works
    under pytest's default/prepend import mode. The root-level monorepo
    run uses --import-mode=importlib, under which this silently breaks.
    Shared test helpers must be pytest fixtures, not bare importable names.
    """
    tests_dir = pkg_dir / "tests"
    if not tests_dir.is_dir():
        return []

    findings = []
    pattern = re.compile(r"^\s*from\s+\.?conftest\s+import\s+(\w+)", re.MULTILINE)
    for test_file in sorted(tests_dir.glob("*.py")):
        text = test_file.read_text()
        for match in pattern.finditer(text):
            findings.append(
                Finding(
                    pkg_name,
                    "FAIL",
                    "bare-conftest-import",
                    f"{test_file.relative_to(REPO_ROOT)}: `from conftest import {match.group(1)}` "
                    f"is a bare import of a test helper — breaks under --import-mode=importlib "
                    f"(what the root `pytest packages/` run uses). Convert to a @pytest.fixture "
                    f"instead (exactly what broke RabbitMQ's test_consumer.py).",
                )
            )
    return findings


def check_meta_extras(pkg_dir: Path, pkg_name: str, pkg_version: str) -> list[Finding]:
    findings = []
    meta_data = load_toml(META_PYPROJECT)
    extras = meta_data.get("project", {}).get("optional-dependencies", {})

    # openframe-adapters-db-postgres -> postgres ; openframe-adapters-queue-kafka -> kafka
    m = re.match(r"openframe-adapters-(?:db|queue)-(.+)", pkg_name)
    extra_key = m.group(1) if m else None

    if extra_key is None or extra_key not in extras:
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "meta-extras-missing",
                f"No '{extra_key or '<unrecognised>'}' entry in meta/openframe-adapters/pyproject.toml's "
                f"[project.optional-dependencies] — `pip install openframe-adapters[{extra_key}]` "
                f"would not install this package.",
            )
        )
        return findings

    spec = extras[extra_key][0] if extras[extra_key] else ""
    name_match = re.match(r"([\w.-]+)", spec)
    if not name_match or name_match.group(1) != pkg_name:
        findings.append(
            Finding(
                pkg_name,
                "FAIL",
                "meta-extras-name-mismatch",
                f"meta extras['{extra_key}'] = {spec!r} does not reference distribution "
                f"'{pkg_name}' — likely a copy-paste error from an adjacent extra.",
            )
        )

    range_match = re.search(r">=\s*([\d.]+)\s*,\s*<\s*([\d.]+)", spec)
    if range_match:
        floor_str, ceiling_str = range_match.groups()
        floor = parse_version(floor_str + ".0" * (2 - floor_str.count(".")))
        ceiling_major = int(ceiling_str.split(".")[0])
        actual = parse_version(pkg_version)
        if not (floor <= actual) or actual[0] >= ceiling_major:
            findings.append(
                Finding(
                    pkg_name,
                    "FAIL",
                    "meta-extras-stale-pin",
                    f"meta extras['{extra_key}'] pin is '{spec}' but the package's actual "
                    f"current version is {pkg_version} — the pin doesn't cover it. This is the "
                    f"exact 'placeholder pin left over from before the package existed for "
                    f"real' bug fixed repeatedly across nearly every wave this repo has shipped.",
                )
            )
    return findings


def check_dev_extras_floors(data: dict, pkg_name: str) -> list[Finding]:
    dev_deps = data.get("project", {}).get("optional-dependencies", {}).get("dev", [])
    present = {}
    for spec in dev_deps:
        m = re.match(r"([\w-]+)\s*>=\s*([\d.]+)", spec)
        if m:
            present[m.group(1)] = m.group(2)

    findings = []
    for required_pkg, min_floor in REQUIRED_DEV_FLOORS.items():
        if required_pkg not in present:
            findings.append(
                Finding(
                    pkg_name,
                    "WARN",
                    "dev-extras-missing",
                    f"[project.optional-dependencies].dev has no '{required_pkg}' entry at all.",
                )
            )
            continue
        actual = tuple(int(x) for x in present[required_pkg].split(".")[:2])
        actual = actual + (0,) * (2 - len(actual))
        if actual < min_floor:
            findings.append(
                Finding(
                    pkg_name,
                    "WARN",
                    "dev-extras-floor-low",
                    f"'{required_pkg}>={present[required_pkg]}' is below this repo's "
                    f"established floor of >={'.'.join(map(str, min_floor))} used by every "
                    f"other package — inconsistent, may mask a real compatibility issue.",
                )
            )
    return findings


def check_version_drift(pkg_dir: Path, pkg_name: str, pkg_version: str) -> list[Finding]:
    """
    plugin.py's hardcoded `version` attribute, and any test asserting it
    directly, must match pyproject.toml's version. Caught Redis shipping
    a version bump where the test's hardcoded assertion lagged behind.
    """
    findings = []
    plugin_files = list((pkg_dir / "openframe").rglob("plugin.py")) if (pkg_dir / "openframe").is_dir() else []
    version_attr_pattern = re.compile(r'version\s*:?\s*(?:str\s*)?=\s*"([\d.]+)"')

    for plugin_file in plugin_files:
        text = plugin_file.read_text()
        for match in version_attr_pattern.finditer(text):
            if match.group(1) != pkg_version:
                findings.append(
                    Finding(
                        pkg_name,
                        "FAIL",
                        "version-drift-plugin",
                        f"{plugin_file.relative_to(REPO_ROOT)} hardcodes version "
                        f"'{match.group(1)}' but pyproject.toml says '{pkg_version}'.",
                    )
                )

    tests_dir = pkg_dir / "tests"
    if tests_dir.is_dir():
        assert_version_pattern = re.compile(r'\.version\s*==\s*"([\d.]+)"')
        for test_file in sorted(tests_dir.glob("test_*.py")):
            text = test_file.read_text()
            for match in assert_version_pattern.finditer(text):
                if match.group(1) != pkg_version:
                    findings.append(
                        Finding(
                            pkg_name,
                            "FAIL",
                            "version-drift-test",
                            f"{test_file.relative_to(REPO_ROOT)} asserts version == "
                            f"'{match.group(1)}' but pyproject.toml says '{pkg_version}' — "
                            f"this test will fail on the next real run. (Exactly what happened "
                            f"with Redis 2.0.5 -> 2.0.6: plugin.py was bumped, the test's "
                            f"hardcoded assertion was not, caught only by re-running the suite.)",
                        )
                    )
    return findings


def check_port_floor(pkg_dir: Path, pkg_name: str, data: dict) -> list[Finding]:
    deps = data.get("project", {}).get("dependencies", [])
    core_spec = next((d for d in deps if d.startswith("openframe-core")), None)
    if core_spec is None:
        return [Finding(pkg_name, "FAIL", "no-core-dependency", "No openframe-core dependency declared at all.")]

    floor_match = re.search(r">=\s*([\d.]+)", core_spec)
    if not floor_match:
        return []
    floor = parse_version(floor_match.group(1) + ".0" * (2 - floor_match.group(1).count(".")))

    openframe_dir = pkg_dir / "openframe"
    if not openframe_dir.is_dir():
        return []
    source_text = "\n".join(p.read_text() for p in openframe_dir.rglob("*.py") if "__pycache__" not in p.parts)

    findings = []
    for port_name, required_floor in PORT_FLOOR_REQUIREMENTS.items():
        if port_name in source_text and floor < required_floor:
            findings.append(
                Finding(
                    pkg_name,
                    "FAIL",
                    "core-floor-too-low",
                    f"Uses {port_name} but openframe-core floor is '{core_spec}' — "
                    f"{port_name} only exists from openframe-core "
                    f"{'.'.join(map(str, required_floor))}+. A fresh install resolving to an "
                    f"older core version would hit ImportError at import time.",
                )
            )
    return findings


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_all_checks(only_package: str | None) -> list[Finding]:
    findings: list[Finding] = []
    packages = discover_packages()
    if only_package:
        packages = [p for p in packages if p.name == only_package]
        if not packages:
            print(f"error: no package named '{only_package}' under packages/", file=sys.stderr)
            sys.exit(2)

    for pkg_dir in packages:
        pkg_name = pkg_dir.name
        data = load_toml(pkg_dir / "pyproject.toml")
        pkg_version = data.get("project", {}).get("version", "0.0.0")

        findings += check_ci_matrices(pkg_name)
        findings += check_root_path_configs(pkg_name)
        findings += check_gitignore_collision(pkg_dir, pkg_name)
        findings += check_bare_conftest_imports(pkg_dir, pkg_name)
        findings += check_meta_extras(pkg_dir, pkg_name, pkg_version)
        findings += check_dev_extras_floors(data, pkg_name)
        findings += check_version_drift(pkg_dir, pkg_name, pkg_version)
        findings += check_port_floor(pkg_dir, pkg_name, data)

    if not only_package:
        findings += check_meta_release_matrix()

    return findings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--package", help="Check only this one package (by directory name).")
    parser.add_argument("--quiet", action="store_true", help="Only print FAIL/WARN findings, suppress the all-clear per-package lines.")
    args = parser.parse_args()

    findings = run_all_checks(args.package)

    by_package: dict[str, list[Finding]] = {}
    for f in findings:
        by_package.setdefault(f.package, []).append(f)

    checked_names = (
        [args.package] if args.package else [p.name for p in discover_packages()] + ["meta/openframe-adapters"]
    )

    fail_count = sum(1 for f in findings if f.severity == "FAIL")
    warn_count = sum(1 for f in findings if f.severity == "WARN")

    for name in checked_names:
        pkg_findings = by_package.get(name, [])
        if not pkg_findings:
            if not args.quiet:
                print(f"\033[32m✓\033[0m {name} — clean")
            continue
        print(f"\033[31m✗\033[0m {name}")
        for f in sorted(pkg_findings, key=lambda x: x.severity):
            color = "\033[31m" if f.severity == "FAIL" else "\033[33m"
            print(f"    {color}[{f.severity}]\033[0m {f.check}: {f.message}")

    print()
    print(f"{len(checked_names)} package(s) checked — {fail_count} failure(s), {warn_count} warning(s).")

    sys.exit(1 if fail_count else 0)


if __name__ == "__main__":
    main()
