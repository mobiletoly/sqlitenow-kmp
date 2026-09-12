"""Focused infrastructure tests for the qualification planner.

Every executed child is the harmless fixture stub. Product commands are inspected as data only.
"""

from __future__ import annotations

import copy
import datetime as dt
import errno
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from tools.qualification import planner


ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = ROOT / "tools/qualification/lanes.json"
README_PATH = ROOT / "tools/qualification/README.md"
STUB_PATH = ROOT / "tools/qualification/tests/fixtures/stub_lane.py"


def run_git(root: Path, *arguments: str) -> None:
    subprocess.run(["git", "-C", str(root), *arguments], check=True, stdout=subprocess.DEVNULL)


def create_repository(path: Path, files: dict[str, str] | None = None) -> None:
    path.mkdir()
    run_git(path, "init", "-q")
    run_git(path, "config", "user.email", "planner@example.invalid")
    run_git(path, "config", "user.name", "Planner Test")
    for name, content in (files or {"README.md": "fixture\n"}).items():
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    run_git(path, "add", ".")
    run_git(path, "commit", "-qm", "fixture")


def minimal_manifest(sentinel: Path, statuses: tuple[int, int] = (0, 0)) -> dict:
    common = {
        "repository": "sqlite-now",
        "cwd": ".",
        "environment_allowlist": ["PATH"],
        "environment": {},
        "toolchains": ["python"],
        "environment_class": "fixture",
        "classification": "infrastructure",
        "live": False,
        "heavy": False,
        "static_inputs": ["src/**"],
        "depends_on": [],
        "subsumes": [],
        "timeout_seconds": 10,
        "resource_locks": ["fixture"],
        "scheduling_class": "fixture",
        "memory_class": "light",
        "campaign_eligible": True,
    }

    def lane(lane_id: str, status: int) -> dict:
        return {
            **common,
            "id": lane_id,
            "description": f"stub {lane_id}",
            "argv": [
                sys.executable,
                str(STUB_PATH),
                "--sentinel",
                str(sentinel),
                "--status",
                str(status),
                "--label",
                lane_id,
            ],
        }

    lanes = [lane("lane-a", statuses[0]), lane("lane-b", statuses[1])]
    lanes.append({
        **lane("aggregate", 0),
        "subsumes": ["lane-a", "lane-b"],
    })
    return {
        "schema_version": 3,
        "default_timeout_seconds": 10,
        "campaign_max_workers": 4,
        "resources": {"fixture": 4, "measurement-exclusive": 1},
        "repositories": [
            {"id": "sqlite-now", "path": "sqlitenow-kmp", "read_only": False, "roles": ["planning", "execution"]},
            {"id": "go-oversync", "path": "go-oversync", "read_only": True, "roles": ["planning", "existing-command-execution"]},
        ],
        "toolchains": [
            {"id": "python", "repository": "sqlite-now", "cwd": ".", "argv": [sys.executable, "--version"]},
        ],
        "lanes": lanes,
        "path_rules": [
            {
                "id": "source-a",
                "repository": "sqlite-now",
                "includes": ["src/a.py"],
                "excludes": [],
                "lanes": ["lane-a"],
                "reason_code": "source_a",
                "reason": "source A changed",
                "validation_paths": ["src/a.py"],
                "class": "source",
            },
            {
                "id": "source-b",
                "repository": "sqlite-now",
                "includes": ["src/b.py"],
                "excludes": [],
                "lanes": ["lane-b"],
                "reason_code": "source_b",
                "reason": "source B changed",
                "validation_paths": ["src/b.py"],
                "class": "source",
            },
            {
                "id": "all-source",
                "repository": "sqlite-now",
                "includes": ["src/all.py"],
                "excludes": [],
                "lanes": ["aggregate", "lane-a", "lane-b"],
                "reason_code": "all_source",
                "reason": "aggregate source changed",
                "validation_paths": ["src/all.py"],
                "class": "source",
            },
            {
                "id": "go-doc",
                "repository": "go-oversync",
                "includes": ["README.md"],
                "excludes": [],
                "lanes": ["lane-a"],
                "reason_code": "ordinary_document_change",
                "reason": "ordinary document",
                "validation_paths": ["README.md"],
                "class": "ordinary-docs",
            },
        ],
        "fallback_lane_ids": ["aggregate"],
        "forbidden_planning_validation_prefixes": [["./gradlew"], ["go", "test"]],
    }


class FixtureCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.parent = Path(self.temporary.name)
        create_repository(
            self.parent / "sqlitenow-kmp",
            {"README.md": "sqlite\n", "src/a.py": "A = 1\n", "src/b.py": "B = 1\n", "src/all.py": "ALL = 1\n"},
        )
        create_repository(self.parent / "go-oversync", {"README.md": "go\n"})
        self.sentinel = self.parent / "sentinel.jsonl"
        self.manifest = minimal_manifest(self.sentinel)
        self.manifest_path = self.parent / "manifest.json"
        self.write_manifest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_manifest(self) -> str:
        self.manifest_path.write_bytes(planner.canonical_bytes(self.manifest))
        return planner.sha256_bytes(planner.canonical_bytes(self.manifest))

    def plan(self, paths: list[str], records: Path | None = None) -> dict:
        digest = self.write_manifest()
        return planner.build_plan(
            self.manifest,
            digest,
            "HEAD",
            injected_paths=paths,
            records=records,
            workspace_root=self.parent,
        )

    def save_plan(self, plan: dict) -> Path:
        path = self.parent / "plan.json"
        path.write_bytes(planner.canonical_bytes(plan))
        return path


class ManifestTests(FixtureCase):
    def test_valid_manifest(self) -> None:
        planner.validate_manifest(self.manifest)

    def test_lane_required_fields(self) -> None:
        invalid = copy.deepcopy(self.manifest)
        del invalid["lanes"][0]["argv"]
        with self.assertRaisesRegex(planner.QualificationError, "missing"):
            planner.validate_manifest(invalid)

    def test_invalid_manifests(self) -> None:
        mutations = []
        duplicate = copy.deepcopy(self.manifest)
        duplicate["lanes"][1]["id"] = "lane-a"
        mutations.append(duplicate)
        cycle = copy.deepcopy(self.manifest)
        cycle["lanes"][0]["depends_on"] = ["lane-b"]
        cycle["lanes"][1]["depends_on"] = ["lane-a"]
        mutations.append(cycle)
        missing = copy.deepcopy(self.manifest)
        missing["lanes"][0]["argv"] = []
        mutations.append(missing)
        escaping = copy.deepcopy(self.manifest)
        escaping["lanes"][0]["cwd"] = "../outside"
        mutations.append(escaping)
        uncovered = copy.deepcopy(self.manifest)
        uncovered["path_rules"][0]["lanes"] = []
        mutations.append(uncovered)
        for manifest in mutations:
            with self.subTest():
                with self.assertRaises(planner.QualificationError):
                    planner.validate_manifest(manifest)

    def test_deterministic_hash(self) -> None:
        first = planner.sha256_bytes(planner.canonical_bytes(self.manifest))
        reordered = json.loads(json.dumps(self.manifest, sort_keys=True))
        second = planner.sha256_bytes(planner.canonical_bytes(reordered))
        self.assertEqual(first, second)

    def test_schema_timeout_and_reachable_fixture_validation(self) -> None:
        invalid = copy.deepcopy(self.manifest)
        invalid["default_timeout_seconds"] = 0
        with self.assertRaisesRegex(planner.QualificationError, "timeout"):
            planner.validate_manifest(invalid)

        invalid = copy.deepcopy(self.manifest)
        del invalid["path_rules"][0]["validation_paths"]
        with self.assertRaisesRegex(planner.QualificationError, "incomplete"):
            planner.validate_manifest(invalid)

        invalid = copy.deepcopy(self.manifest)
        invalid["path_rules"][0]["validation_paths"] = ["src/not-owned.py"]
        with self.assertRaisesRegex(planner.QualificationError, "reach"):
            planner.validate_manifest(invalid)

        invalid = copy.deepcopy(self.manifest)
        invalid["lanes"][0]["ignored_static_inputs"] = ["../outside/**"]
        with self.assertRaises(planner.QualificationError):
            planner.validate_manifest(invalid)

        invalid = copy.deepcopy(self.manifest)
        invalid["lanes"][0]["classification"] = "product"
        invalid["lanes"][0]["argv"] = ["unsafe-product-command"]
        with self.assertRaisesRegex(planner.QualificationError, "refusal prefix"):
            planner.validate_manifest(invalid)

    def test_cross_repo_conformance_is_pinned_and_sibling_bound(self) -> None:
        manifest, _ = planner.load_manifest(MANIFEST_PATH)
        lane = next(item for item in manifest["lanes"] if item["id"] == "cross-repo-conformance")
        self.assertIn("GOTOOLCHAIN", lane["environment_allowlist"])
        self.assertEqual(lane["environment"]["GOTOOLCHAIN"], "go1.25.12")
        self.assertNotIn("GO_OVERSYNC_REPO", lane["environment_allowlist"])
        self.assertFalse(any(argument.startswith("-PgoOversyncRepo") for argument in lane["argv"]))

        invalid, _ = planner.load_manifest(MANIFEST_PATH)
        invalid = copy.deepcopy(invalid)
        cross_repo = next(item for item in invalid["lanes"] if item["id"] == "cross-repo-conformance")
        cross_repo["environment_allowlist"] = ["PATH"]
        cross_repo["environment"] = {}
        with self.assertRaisesRegex(planner.QualificationError, "cross-repo-conformance must pin"):
            planner.validate_manifest(invalid)


class MappingTests(FixtureCase):
    def test_all_rule_classes(self) -> None:
        manifest, _ = planner.load_manifest(MANIFEST_PATH)
        self.assertEqual(
            {rule["class"] for rule in manifest["path_rules"]},
            {"ordinary-docs", "governing-docs", "source", "test", "fixture-contract", "generated-support", "build-toolchain", "planner"},
        )

    def test_unknown_path_fallback(self) -> None:
        plan = self.plan(["sqlite-now:mystery/new.file"])
        self.assertEqual(plan["unknown_paths"][0]["path"], "mystery/new.file")
        selected = {lane["id"] for lane in plan["lanes"] if lane["selected"]}
        self.assertEqual(selected, {"aggregate"})

    def test_injected_paths(self) -> None:
        plan = self.plan(["sqlite-now:src/a.py"])
        self.assertEqual(plan["changed_paths"][0]["status"], "injected")
        self.assertEqual(plan["changed_paths"][0]["matched_rules"], ["source-a"])

    def test_shared_and_unknown_expansion(self) -> None:
        plan = self.plan(["sqlite-now:src/all.py", "sqlite-now:unknown.bin"])
        selected = {lane["id"] for lane in plan["lanes"] if lane["selected"]}
        self.assertEqual(selected, {"aggregate", "lane-a", "lane-b"})


class IdentityTests(FixtureCase):
    def add_governing_rule(self, path: str) -> None:
        self.manifest["path_rules"].append({
            "id": f"governing-{path.replace('/', '-')}",
            "repository": "sqlite-now",
            "includes": [path],
            "excludes": [],
            "lanes": ["lane-a"],
            "reason_code": "governing_fixture",
            "reason": "governing fixture",
            "validation_paths": [path],
            "class": "governing-docs",
        })

    def test_relevant_digest_is_deterministic(self) -> None:
        first = self.plan(["sqlite-now:src/a.py"])
        second = self.plan(["sqlite-now:src/a.py"])
        self.assertEqual(first, second)

    def test_default_plan_ignores_unselected_root_specs(self) -> None:
        before = self.plan(["sqlite-now:src/a.py"])
        root = self.parent / "sqlitenow-kmp"
        (root / "specs").mkdir()
        authority = root / "specs/local-authority.md"
        authority.write_text("first\n", encoding="utf-8")
        after_add = self.plan(["sqlite-now:src/a.py"])
        authority.write_text("second\n", encoding="utf-8")
        after_change = self.plan(["sqlite-now:src/a.py"])
        authority.unlink()
        after_remove = self.plan(["sqlite-now:src/a.py"])
        for candidate in (after_add, after_change, after_remove):
            self.assertEqual(
                [lane["id"] for lane in before["lanes"] if lane["selected"]],
                [lane["id"] for lane in candidate["lanes"] if lane["selected"]],
            )
            self.assertEqual(before["lanes"][0]["relevant_input_sha256"], candidate["lanes"][0]["relevant_input_sha256"])

    def test_explicit_governing_inputs_are_canonical_and_content_bound(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        authority = root / "local-authority.md"
        authority.write_text("accepted\n", encoding="utf-8")
        digest = self.write_manifest()
        plan = planner.build_plan(
            self.manifest,
            digest,
            "HEAD",
            injected_paths=["sqlite-now:src/a.py"],
            workspace_root=self.parent,
            explicit_governing_input_selections=[
                "sqlite-now:local-authority.md",
                "sqlite-now:local-authority.md",
            ],
        )
        self.assertEqual(len(plan["explicit_governing_inputs"]), 1)
        self.assertEqual(plan["explicit_governing_inputs"][0]["sha256"], planner.sha256_file(authority))
        plan_path = self.save_plan(plan)
        authority.write_text("changed\n", encoding="utf-8")
        with self.assertRaisesRegex(planner.QualificationError, "explicitly selected governing input changed"):
            planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)

    def test_instruction_inputs_capture_absence_and_presence(self) -> None:
        absent = self.plan(["sqlite-now:src/a.py"])
        self.assertEqual(
            [(entry["repository"], entry["state"]) for entry in absent["instruction_inputs"]],
            [("go-oversync", "absent"), ("sqlite-now", "absent")],
        )
        agents = self.parent / "sqlitenow-kmp/AGENTS.md"
        agents.write_text("fixture instructions\n", encoding="utf-8")
        present = self.plan(["sqlite-now:src/a.py"])
        captured = next(entry for entry in present["instruction_inputs"] if entry["repository"] == "sqlite-now")
        self.assertEqual(captured["state"], "file")
        self.assertEqual(captured["sha256"], planner.sha256_file(agents))

    def test_audit_and_governing_identity(self) -> None:
        plan = self.plan(["sqlite-now:src/a.py"])
        identity = plan["repositories"]["sqlite-now"]
        self.assertRegex(identity["head"], r"^[0-9a-f]{40}$")
        self.assertRegex(identity["porcelain_v2_sha256"], r"^[0-9a-f]{64}$")
        live, _ = planner.load_manifest(MANIFEST_PATH)
        governing = next(rule for rule in live["path_rules"] if rule["id"] == "governing-documents")
        self.assertIn("docs-engineering/qualification.md", governing["includes"])
        live_plan = planner.build_plan(
            live,
            planner.sha256_bytes(planner.canonical_bytes(live)),
            "HEAD",
            injected_paths=["sqlite-now:tools/qualification/planner.py"],
            workspace_root=ROOT.parent,
        )
        governed = {
            (item["repository"], item["path"]): item for item in live_plan["governing_inputs"]
        }
        contract = governed[("sqlite-now", "docs-engineering/qualification.md")]
        self.assertEqual(contract["sha256"], planner.sha256_file(ROOT / "docs-engineering/qualification.md"))
        self.assertEqual(contract["state"], "file")

    def test_index_content_is_part_of_relevant_identity(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        (root / "src/a.py").write_text("A = 2\n", encoding="utf-8")
        run_git(root, "add", "src/a.py")
        (root / "src/a.py").write_text("A = 1\n", encoding="utf-8")
        staged_plan = self.plan(["sqlite-now:src/a.py"])
        (root / "src/a.py").write_text("A = 3\n", encoding="utf-8")
        unstaged_plan = self.plan(["sqlite-now:src/a.py"])
        staged_lane = next(item for item in staged_plan["lanes"] if item["id"] == "lane-a")
        unstaged_lane = next(item for item in unstaged_plan["lanes"] if item["id"] == "lane-a")
        self.assertNotEqual(staged_lane["relevant_input_sha256"], unstaged_lane["relevant_input_sha256"])

    def test_ignored_static_input_is_hashed(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        (root / ".gitignore").write_text("ignored/\n", encoding="utf-8")
        run_git(root, "add", ".gitignore")
        run_git(root, "commit", "-qm", "ignore fixture")
        (root / "ignored").mkdir()
        ignored = root / "ignored/input.txt"
        ignored.write_text("one\n", encoding="utf-8")
        self.manifest["lanes"][0]["ignored_static_inputs"] = ["ignored/**"]
        first = self.plan(["sqlite-now:src/a.py"])
        ignored.write_text("two\n", encoding="utf-8")
        second = self.plan(["sqlite-now:src/a.py"])
        first_lane = next(item for item in first["lanes"] if item["id"] == "lane-a")
        second_lane = next(item for item in second["lanes"] if item["id"] == "lane-a")
        self.assertNotEqual(first_lane["relevant_input_sha256"], second_lane["relevant_input_sha256"])

    def test_outside_symlink_fails_closed(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        outside = self.parent / "outside.txt"
        outside.write_text("secret\n", encoding="utf-8")
        (root / "escape").symlink_to(outside)
        self.manifest["lanes"][0]["static_inputs"] = ["escape"]
        with self.assertRaisesRegex(planner.QualificationError, "escapes repository"):
            self.plan(["sqlite-now:src/a.py"])

    def test_toolchain_probe_uses_effective_lane_environment(self) -> None:
        self.manifest["lanes"][0]["environment_allowlist"].append("GOTOOLCHAIN")
        self.manifest["lanes"][0]["environment"]["GOTOOLCHAIN"] = "go1.25.12"
        observed: list[dict[str, str | None]] = []

        def probe(toolchain: dict, root: Path, environment: dict[str, str | None]) -> dict:
            del root
            observed.append(environment)
            return {
                "id": toolchain["id"],
                "argv": toolchain["argv"],
                "cwd": toolchain["cwd"],
                "environment_sha256": planner.sha256_bytes(planner.canonical_bytes(environment)),
                "exit_code": 0,
                "output_sha256": "0" * 64,
            }

        with mock.patch("tools.qualification.planner.toolchain_identity", side_effect=probe):
            self.plan(["sqlite-now:src/a.py"])
        self.assertEqual(observed[0]["GOTOOLCHAIN"], "go1.25.12")

    def test_toolchain_probe_sandboxes_missing_user_config(self) -> None:
        captured: dict[str, str] = {}
        completed = mock.Mock(returncode=0, stdout=b"probe\n")

        def capture(*args, **kwargs):
            del args
            captured.update(kwargs["env"])
            return completed

        toolchain = {
            "id": "probe",
            "repository": "sqlite-now",
            "cwd": ".",
            "argv": [sys.executable, "--version"],
        }
        with mock.patch("tools.qualification.planner.subprocess.run", side_effect=capture):
            planner.toolchain_identity(toolchain, (self.parent / "sqlitenow-kmp").resolve(), {"PATH": "/bin"})
        self.assertNotEqual(captured["HOME"], str(self.parent / "sqlitenow-kmp"))
        self.assertNotEqual(captured["XDG_CONFIG_HOME"], str(self.parent / "sqlitenow-kmp"))
        self.assertFalse(Path(captured["HOME"]).exists())
        self.assertFalse(Path(captured["XDG_CONFIG_HOME"]).exists())

    def test_toolchain_probe_removes_read_only_cache(self) -> None:
        toolchain = {
            "id": "read-only-cache",
            "cwd": ".",
            "argv": [
                sys.executable,
                "-c",
                "import os; from pathlib import Path; root = Path(os.environ['HOME']) / 'pkg' / 'mod'; root.mkdir(parents=True); (root / 'cache').write_text('x'); root.chmod(0o555)",
            ],
        }
        result = planner.toolchain_identity(
            toolchain,
            (self.parent / "sqlitenow-kmp").resolve(),
            {"PATH": os.environ["PATH"]},
        )
        self.assertEqual(result["exit_code"], 0)

    def test_temporary_state_cleanup_retries_telemetry_race(self) -> None:
        root = self.parent / "temporary-state"
        root.mkdir()
        with mock.patch("tools.qualification.planner.shutil.rmtree", side_effect=[OSError(errno.ENOTEMPTY, "not empty"), None]) as remove, mock.patch("tools.qualification.planner.time.sleep") as pause:
            planner._remove_temporary_state(root)
        self.assertEqual(remove.call_count, 2)
        pause.assert_called_once_with(0.05)

    def test_required_governing_inputs_fail_closed(self) -> None:
        cases = ("missing.md", "unreadable")
        for path in cases:
            with self.subTest(path=path):
                self.manifest = minimal_manifest(self.sentinel)
                self.add_governing_rule(path)
                if path == "unreadable":
                    (self.parent / "sqlitenow-kmp" / path).mkdir()
                with self.assertRaisesRegex(planner.QualificationError, "governing input"):
                    self.plan(["sqlite-now:src/a.py"])

    def test_deleted_governing_input_fails_closed(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        governed = root / "governed.md"
        governed.write_text("governed\n", encoding="utf-8")
        run_git(root, "add", "governed.md")
        self.add_governing_rule("governed.md")
        governed.unlink()
        with self.assertRaisesRegex(planner.QualificationError, "governing input .*deleted"):
            self.plan(["sqlite-now:src/a.py"])

    def test_symlink_governing_input_fails_closed(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        (root / "governed-target.md").write_text("target\n", encoding="utf-8")
        (root / "governed.md").symlink_to("governed-target.md")
        self.add_governing_rule("governed.md")
        with self.assertRaisesRegex(planner.QualificationError, "governing input .*symlink"):
            self.plan(["sqlite-now:src/a.py"])

    def test_gitlink_governing_input_fails_closed(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        subrepo = self.parent / "governed-subrepo"
        create_repository(subrepo)
        object_id = subprocess.check_output(
            ["git", "-C", str(subrepo), "rev-parse", "HEAD"], text=True
        ).strip()
        run_git(root, "update-index", "--add", "--cacheinfo", f"160000,{object_id},governed-link")
        self.add_governing_rule("governed-link")
        with self.assertRaisesRegex(planner.QualificationError, "governing input .*gitlink"):
            self.plan(["sqlite-now:src/a.py"])


class GitPlanningTests(FixtureCase):
    def test_base_to_current_state(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        (root / "src/a.py").write_text("A = 2\n", encoding="utf-8")
        run_git(root, "add", "src/a.py")
        (root / "src/b.py").write_text("B = 2\n", encoding="utf-8")
        (root / "unknown.txt").write_text("new\n", encoding="utf-8")
        digest = self.write_manifest()
        plan = planner.build_plan(self.manifest, digest, "HEAD", workspace_root=self.parent)
        paths = {(item["path"], item["status"]) for item in plan["changed_paths"]}
        self.assertIn(("src/a.py", "M"), paths)
        self.assertIn(("src/b.py", "M"), paths)
        self.assertIn(("unknown.txt", "untracked"), paths)

    def test_rename_and_staged_deletion_identity(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        run_git(root, "mv", "src/a.py", "src/renamed.py")
        renamed = planner.changed_paths(root, "HEAD")
        self.assertTrue({item["status"] for item in renamed if item["path"] == "src/a.py"}.pop().startswith("R"))
        self.assertTrue({item["status"] for item in renamed if item["path"] == "src/renamed.py"}.pop().startswith("R"))

        run_git(root, "rm", "src/b.py")
        index = planner._index_entries(root)
        self.assertEqual(planner._file_identity(root, "src/b.py", index)["state"], "deleted")

    def test_no_change(self) -> None:
        digest = self.write_manifest()
        plan = planner.build_plan(self.manifest, digest, "HEAD", workspace_root=self.parent)
        self.assertEqual(plan["changed_paths"], [])
        self.assertFalse(any(lane["selected"] for lane in plan["lanes"]))

    def test_unmerged_index_fails_closed(self) -> None:
        root = self.parent / "sqlitenow-kmp"
        branch = subprocess.check_output(
            ["git", "-C", str(root), "branch", "--show-current"], text=True
        ).strip()
        run_git(root, "checkout", "-qb", "conflict-side")
        (root / "src/a.py").write_text("A = side\n", encoding="utf-8")
        run_git(root, "add", "src/a.py")
        run_git(root, "commit", "-qm", "conflict side")
        run_git(root, "checkout", "-q", branch)
        (root / "src/a.py").write_text("A = main\n", encoding="utf-8")
        run_git(root, "add", "src/a.py")
        run_git(root, "commit", "-qm", "conflict main")
        merge = subprocess.run(
            ["git", "-C", str(root), "merge", "conflict-side"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.assertNotEqual(merge.returncode, 0)
        with self.assertRaisesRegex(planner.QualificationError, "unmerged index"):
            self.plan(["sqlite-now:src/a.py"])


class AggregateTests(FixtureCase):
    def test_live_manifest_edges(self) -> None:
        manifest, _ = planner.load_manifest(MANIFEST_PATH)
        lanes = {lane["id"]: lane for lane in manifest["lanes"]}
        build = (ROOT / "build.gradle.kts").read_text(encoding="utf-8")
        self.assertEqual(
            set(lanes["core-platform-all"]["subsumes"]),
            {
                "core-platform-jvm", "core-platform-android", "core-platform-ios-simulator",
                "core-platform-macos", "core-platform-linux-x64", "core-platform-linux-arm64",
                "core-platform-js-node", "core-platform-wasm-browser",
            },
        )
        self.assertEqual(
            set(lanes["oversqlite-platform-all"]["subsumes"]),
            {
                "oversqlite-platform-jvm", "oversqlite-platform-android",
                "oversqlite-platform-ios-simulator", "oversqlite-platform-macos",
                "oversqlite-platform-js-node", "oversqlite-platform-js-browser",
                "oversqlite-platform-wasm-browser",
            },
        )
        self.assertEqual(
            set(lanes["oversqlite-realserver-all"]["subsumes"]),
            {
                "oversqlite-realserver-jvm", "oversqlite-realserver-android",
                "oversqlite-realserver-jvm-harness", "oversqlite-realserver-ios-simulator",
                "oversqlite-realserver-macos", "oversqlite-realserver-js-node",
                "oversqlite-realserver-js-browser", "oversqlite-realserver-wasm-browser",
            },
        )
        self.assertIn("swift-generated-sync", lanes["swift-hybrid"]["subsumes"])
        self.assertEqual(
            set(lanes["swift-realserver"]["subsumes"]),
            {"swift-hybrid", "swift-generated-realserver"},
        )
        self.assertEqual(lanes["go-full"]["subsumes"], [])
        self.assertNotIn("go-mobile", lanes["go-full"]["subsumes"])
        self.assertEqual(lanes["oversqlite-realserver-all-heavy"]["subsumes"], [])
        for aggregate_id in (
            "core-platform-all", "oversqlite-platform-all", "oversqlite-realserver-all",
            "swift-hybrid", "swift-realserver",
        ):
            for child_id in lanes[aggregate_id]["subsumes"]:
                child = lanes[child_id]
                command = child["argv"][1] if child["argv"][0] == "./gradlew" else child["argv"][0]
                self.assertIn(f'"{command}"', build, (aggregate_id, child_id, command))
        go_script = (ROOT.parent / "go-oversync/scripts/test-lane.sh").read_text(encoding="utf-8")
        self.assertIn("full)", go_script)
        self.assertIn('"$test_runner" ./...', go_script)

    def test_selected_aggregate_subsumes_child(self) -> None:
        plan = self.plan(["sqlite-now:src/all.py"])
        lanes = {lane["id"]: lane for lane in plan["lanes"]}
        self.assertEqual(lanes["aggregate"]["disposition"], "selected")
        self.assertEqual(lanes["lane-a"]["disposition"], "subsumed")
        self.assertEqual(lanes["lane-b"]["subsumed_by"], "aggregate")

    def test_heavy_aggregate_remains_visible(self) -> None:
        manifest, digest = planner.load_manifest(MANIFEST_PATH)
        rule = next(item for item in manifest["path_rules"] if item["id"] == "governing-documents")
        rule["lanes"].append("oversqlite-realserver-all-heavy")
        digest = planner.sha256_bytes(planner.canonical_bytes(manifest))
        with mock.patch("tools.qualification.planner.toolchain_identity", side_effect=lambda toolchain, root, environment: {
            "id": toolchain["id"], "argv": toolchain["argv"], "cwd": toolchain["cwd"],
            "exit_code": 0, "output_sha256": "0" * 64,
        }):
            with mock.patch("tools.qualification.planner._listed_files", return_value=[]):
                with mock.patch("tools.qualification.planner._ignored_files", return_value=[]):
                    plan = planner.build_plan(
                        manifest, digest, "HEAD",
                        injected_paths=["sqlite-now:docs-engineering/qualification.md"],
                        workspace_root=ROOT.parent,
                    )
        lanes = {item["id"]: item for item in plan["lanes"]}
        self.assertEqual(lanes["oversqlite-realserver-all-heavy"]["disposition"], "selected")
        self.assertIsNone(lanes["oversqlite-realserver-all-heavy"]["subsumed_by"])


class ReportingTests(FixtureCase):
    def test_all_dispositions_have_reasons(self) -> None:
        plans = [
            self.plan(["sqlite-now:src/a.py"]),
            self.plan(["sqlite-now:src/all.py"]),
            self.plan(["sqlite-now:unknown.bin"]),
        ]
        for plan in plans:
            for lane in plan["lanes"]:
                self.assertTrue(lane["reasons"])
                self.assertTrue(all(reason["reason_code"] and reason["reason"] for reason in lane["reasons"]))

    def test_deterministic_json_and_terminal_report(self) -> None:
        plan = self.plan(["sqlite-now:src/a.py"])
        self.assertEqual(planner.canonical_bytes(plan), planner.canonical_bytes(json.loads(planner.canonical_bytes(plan))))
        report = planner.concise_report(plan)
        self.assertIn("advisory", report)
        self.assertIn("selected: lane-a", report)


class AuthorityTests(FixtureCase):
    def test_advisory_labels(self) -> None:
        plan = self.plan(["sqlite-now:src/a.py"])
        self.assertEqual(plan["authority"], "advisory_only_reviewer_confirmation_required")
        readme = README_PATH.read_text(encoding="utf-8")
        self.assertIn("no acceptance or closure authority", readme)

    def test_go_repository_is_read_only(self) -> None:
        manifest, _ = planner.load_manifest(MANIFEST_PATH)
        go_repo = next(repo for repo in manifest["repositories"] if repo["id"] == "go-oversync")
        self.assertTrue(go_repo["read_only"])

    def test_plan_never_executes_lane(self) -> None:
        lane_argv = self.manifest["lanes"][0]["argv"]
        with mock.patch("tools.qualification.planner.subprocess.Popen", wraps=subprocess.Popen) as popen:
            self.plan(["sqlite-now:src/a.py"])
        self.assertFalse(any(call.args and call.args[0] == lane_argv for call in popen.call_args_list))
        self.assertFalse(self.sentinel.exists())

    def test_qualification_reviewer_warning(self) -> None:
        self.assertIn("qualification reviewer must confirm every reuse recommendation", README_PATH.read_text(encoding="utf-8"))

    def test_schema_versions_are_documented(self) -> None:
        text = README_PATH.read_text(encoding="utf-8")
        self.assertIn("manifest and plan schema version is 3", text)
        self.assertIn("attempt and campaign record schema version remains 2", text)


class RunnerTests(FixtureCase):
    def test_explicit_lane_acceptance_required(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        with self.assertRaisesRegex(planner.QualificationError, "either --lane or --accept-selected"):
            planner.run_plan(plan_path, self.manifest_path, [], False, self.parent / "records", False, self.parent)
        with self.assertRaisesRegex(planner.QualificationError, "unselected"):
            planner.run_plan(plan_path, self.manifest_path, ["lane-b"], False, self.parent / "records", False, self.parent)

    def test_exact_argv_without_shell(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        result = planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(self.sentinel.read_text(encoding="utf-8")), {"label": "lane-a", "status": 0})
        started = json.loads((self.parent / "records/attempts.jsonl").read_text().splitlines()[0])
        self.assertEqual(started["argv"], self.manifest["lanes"][0]["argv"])

    def test_failure_policies(self) -> None:
        self.manifest = minimal_manifest(self.sentinel, statuses=(7, 0))
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))
        result = planner.run_plan(plan_path, self.manifest_path, ["lane-a", "lane-b"], False, self.parent / "stop", False, self.parent)
        self.assertEqual(result, 10)
        self.assertEqual(len(self.sentinel.read_text().splitlines()), 1)
        self.sentinel.unlink()
        result = planner.run_plan(plan_path, self.manifest_path, ["lane-a", "lane-b"], False, self.parent / "continue", True, self.parent)
        self.assertEqual(result, 11)
        self.assertEqual(len(self.sentinel.read_text().splitlines()), 2)

    def test_command_receives_effective_allowlisted_environment(self) -> None:
        self.manifest["lanes"][0]["environment_allowlist"].append("GOTOOLCHAIN")
        self.manifest["lanes"][0]["environment"]["GOTOOLCHAIN"] = "go1.25.12"
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        observed: list[dict[str, str]] = []
        real_popen = subprocess.Popen

        def capture(*args, **kwargs):
            if args and args[0] == self.manifest["lanes"][0]["argv"]:
                observed.append(kwargs["env"])
            return real_popen(*args, **kwargs)

        with mock.patch("tools.qualification.planner.subprocess.Popen", side_effect=capture):
            self.assertEqual(
                planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent),
                0,
            )
        self.assertEqual(observed[0]["GOTOOLCHAIN"], "go1.25.12")

    def test_manifest_drift_after_start_is_terminal(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        real_popen = subprocess.Popen

        def mutate_after_start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            if args and args[0] == self.manifest["lanes"][0]["argv"]:
                self.manifest["lanes"][0]["description"] = "changed during execution"
                self.write_manifest()
            return process

        with mock.patch("tools.qualification.planner.subprocess.Popen", side_effect=mutate_after_start):
            with self.assertRaisesRegex(planner.QualificationError, "manifest changed during execution"):
                planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)
        self.assertEqual(len(self.sentinel.read_text(encoding="utf-8").splitlines()), 1)
        finished = json.loads((self.parent / "records/attempts.jsonl").read_text().splitlines()[1])
        self.assertIn("postflight manifest_stale", finished["harness_error"])

    def test_relevant_drift_after_start_is_terminal(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        root = self.parent / "sqlitenow-kmp"
        real_popen = subprocess.Popen

        def mutate_after_start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            if args and args[0] == self.manifest["lanes"][0]["argv"]:
                (root / "src/a.py").write_text("A = changed\n", encoding="utf-8")
            return process

        with mock.patch("tools.qualification.planner.subprocess.Popen", side_effect=mutate_after_start):
            with self.assertRaisesRegex(planner.QualificationError, "relevant inputs changed"):
                planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)

    def test_branch_and_go_read_only_drift_after_start_is_terminal(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        root = self.parent / "sqlitenow-kmp"
        real_popen = subprocess.Popen

        def switch_branch_after_start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            if args and args[0] == self.manifest["lanes"][0]["argv"]:
                run_git(root, "checkout", "-qb", "drift")
            return process

        with mock.patch("tools.qualification.planner.subprocess.Popen", side_effect=switch_branch_after_start):
            with self.assertRaisesRegex(planner.QualificationError, "repository branch"):
                planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "branch-records", False, self.parent)

        self.sentinel.unlink()
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        go_root = self.parent / "go-oversync"

        def change_go_after_start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            if args and args[0] == self.manifest["lanes"][0]["argv"]:
                (go_root / "drift.txt").write_text("go drift\n", encoding="utf-8")
                run_git(go_root, "add", "drift.txt")
                run_git(go_root, "commit", "-qm", "go drift")
            return process

        with mock.patch("tools.qualification.planner.subprocess.Popen", side_effect=change_go_after_start):
            with self.assertRaisesRegex(planner.QualificationError, "read-only repository Git identity"):
                planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "go-records", False, self.parent)

    def test_toolchain_environment_drift_after_start_is_terminal(self) -> None:
        self.manifest["lanes"][0]["environment_allowlist"].append("QUALIFICATION_TEST_ENV")
        plan_path = self.parent / "plan.json"
        with mock.patch.dict(os.environ, {"QUALIFICATION_TEST_ENV": "before"}):
            plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
            real_popen = subprocess.Popen

            def mutate_after_start(*args, **kwargs):
                process = real_popen(*args, **kwargs)
                if args and args[0] == self.manifest["lanes"][0]["argv"]:
                    os.environ["QUALIFICATION_TEST_ENV"] = "after"
                return process

            with mock.patch("tools.qualification.planner.subprocess.Popen", side_effect=mutate_after_start):
                with self.assertRaisesRegex(planner.QualificationError, "toolchains changed|relevant inputs changed"):
                    planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)

    def test_continue_on_failure_refuses_later_lane_after_identity_drift(self) -> None:
        self.manifest = minimal_manifest(self.sentinel, statuses=(0, 0))
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))
        original_refresh = planner._refresh_execution_snapshot
        calls = 0

        def refresh(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 3:
                (self.parent / "sqlitenow-kmp/src/b.py").write_text("B = changed\n", encoding="utf-8")
            return original_refresh(*args, **kwargs)

        with mock.patch("tools.qualification.planner._refresh_execution_snapshot", side_effect=refresh):
            with self.assertRaisesRegex(planner.QualificationError, "relevant inputs changed"):
                planner.run_plan(plan_path, self.manifest_path, ["lane-a", "lane-b"], False, self.parent / "records", True, self.parent)
        self.assertEqual(len(self.sentinel.read_text(encoding="utf-8").splitlines()), 1)

    def test_product_and_go_execution_boundaries(self) -> None:
        self.manifest["lanes"][0]["classification"] = "product"
        self.manifest["forbidden_planning_validation_prefixes"].append([sys.executable, str(STUB_PATH)])
        product_plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        with self.assertRaisesRegex(planner.QualificationError, "authorize-product"):
            planner.run_plan(product_plan, self.manifest_path, ["lane-a"], False, self.parent / "product", False, self.parent)
        self.assertEqual(
            planner.run_plan(
                product_plan, self.manifest_path, ["lane-a"], False, self.parent / "product-authorized",
                False, self.parent, authorize_product=True,
            ),
            0,
        )

        self.manifest = minimal_manifest(self.sentinel)
        self.manifest["lanes"][0]["repository"] = "go-oversync"
        self.manifest["lanes"][0]["campaign_eligible"] = False
        self.manifest["path_rules"][0]["repository"] = "go-oversync"
        self.manifest["path_rules"][0]["includes"] = ["src/a.py"]
        go_root = self.parent / "go-oversync"
        (go_root / "src").mkdir()
        (go_root / "src/a.py").write_text("A = 1\n", encoding="utf-8")
        run_git(go_root, "add", "src/a.py")
        run_git(go_root, "commit", "-qm", "go fixture source")
        go_plan = self.save_plan(self.plan(["go-oversync:src/a.py"]))
        with self.assertRaisesRegex(planner.QualificationError, "planning-only"):
            planner.run_plan(go_plan, self.manifest_path, ["lane-a"], False, self.parent / "go", False, self.parent)

    def test_timeout_records_and_kills_process_group(self) -> None:
        self.manifest["default_timeout_seconds"] = 1
        del self.manifest["lanes"][0]["timeout_seconds"]
        self.manifest["lanes"][0]["argv"] += ["--sleep-seconds", "2"]
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        records = self.parent / "timeout-records"
        self.assertEqual(planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, records, False, self.parent), 10)
        events = [json.loads(line) for line in (records / "attempts.jsonl").read_text().splitlines()]
        self.assertTrue(events[1]["timed_out"])
        self.assertEqual(events[1]["exit_code"], -15)

    def test_tampered_saved_plan_is_rejected(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        lane["argv"] = ["tampered"]
        without_digest = dict(plan)
        without_digest.pop("plan_sha256")
        plan["plan_sha256"] = planner.sha256_bytes(planner.canonical_bytes(without_digest))
        plan_path.write_bytes(planner.canonical_bytes(plan))
        with self.assertRaisesRegex(planner.QualificationError, "saved lane"):
            planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "tampered", False, self.parent)

    def test_unrelated_sqlitenow_drift_is_allowed(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        root = self.parent / "sqlitenow-kmp"
        (root / "README.md").write_text("unrelated change\n", encoding="utf-8")
        run_git(root, "add", "README.md")
        run_git(root, "commit", "-qm", "unrelated drift")
        self.assertEqual(
            planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent),
            0,
        )

    def test_relevant_sqlitenow_drift_is_rejected(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        (self.parent / "sqlitenow-kmp/src/a.py").write_text("A = changed\n", encoding="utf-8")
        with self.assertRaisesRegex(planner.QualificationError, "relevant inputs"):
            planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)

    def test_record_directory_symlink_is_rejected(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        target = self.parent / "real-records"
        target.mkdir()
        records = self.parent / "records-link"
        records.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(planner.QualificationError, "records directory is a symlink"):
            planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, records, False, self.parent)

    def test_record_ancestor_symlink_is_rejected(self) -> None:
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        target = self.parent / "real-records"
        target.mkdir()
        link = self.parent / "records-link"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(planner.QualificationError, "symlink"):
            planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, link / "nested", False, self.parent)

    def test_plan_output_temp_symlink_is_not_followed(self) -> None:
        plan = self.plan(["sqlite-now:src/a.py"])
        output = self.parent / "plan.json"
        outside = self.parent / "outside.json"
        outside.write_text("outside\n", encoding="utf-8")
        temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}-0")
        temporary.symlink_to(outside)
        with mock.patch("sys.stdout", new=io.StringIO()):
            planner.write_plan(plan, str(output))
        self.assertEqual(outside.read_text(encoding="utf-8"), "outside\n")
        self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["schema_version"], 3)
        self.assertTrue(temporary.is_symlink())


class RecordTests(FixtureCase):
    def execute(self, records: Path | None = None) -> tuple[Path, list[dict]]:
        records = records or self.parent / "records"
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        self.assertEqual(planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, records, False, self.parent), 0)
        events = [json.loads(line) for line in (records / "attempts.jsonl").read_text().splitlines()]
        return records, events

    def test_started_event(self) -> None:
        _, events = self.execute()
        started = events[0]
        for field in ("argv", "cwd", "environment", "toolchains", "relevant_input_sha256", "start_utc"):
            self.assertIn(field, started)

    def test_finished_event(self) -> None:
        records, events = self.execute()
        finished = events[1]
        self.assertEqual(finished["exit_code"], 0)
        self.assertGreaterEqual(finished["duration_seconds"], 0)
        self.assertEqual(planner.sha256_file(records / finished["log"]), finished["log_sha256"])

    def test_append_only_attempts(self) -> None:
        records, events = self.execute()
        first_bytes = (records / "attempts.jsonl").read_bytes()
        self.assertEqual([event["event"] for event in events], ["started", "finished"])
        self.execute(records)
        updated = (records / "attempts.jsonl").read_bytes()
        self.assertTrue(updated.startswith(first_bytes))
        self.assertEqual(len(updated.splitlines()), 4)

    def test_failed_then_repeated_attempt(self) -> None:
        self.manifest = minimal_manifest(self.sentinel, statuses=(3, 0))
        records = self.parent / "records"
        failed_plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        self.assertEqual(planner.run_plan(failed_plan, self.manifest_path, ["lane-a"], False, records, False, self.parent), 10)
        self.manifest = minimal_manifest(self.sentinel, statuses=(0, 0))
        passed_plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        self.assertEqual(planner.run_plan(passed_plan, self.manifest_path, ["lane-a"], False, records, False, self.parent), 0)
        events = [json.loads(line) for line in (records / "attempts.jsonl").read_text().splitlines()]
        self.assertEqual(len(events), 4)
        self.assertEqual([event.get("exit_code") for event in events if event["event"] == "finished"], [3, 0])


class ReuseTests(FixtureCase):
    def execute(self, records: Path | None = None) -> tuple[Path, list[dict]]:
        records = records or self.parent / "records"
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        self.assertEqual(planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, records, False, self.parent), 0)
        events = [json.loads(line) for line in (records / "attempts.jsonl").read_text().splitlines()]
        return records, events

    @staticmethod
    def write_events(records: Path, events: list[dict]) -> None:
        (records / "attempts.jsonl").write_bytes(
            b"".join(planner.canonical_bytes(event) for event in events)
        )

    def test_reuse_is_advisory(self) -> None:
        records = self.parent / "records"
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        self.assertEqual(planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, records, False, self.parent), 0)
        reused = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in reused["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["disposition"], "reuse_recommended")
        self.assertIn("reviewer confirmation required", lane["reasons"][-1]["reason"])

    def test_malformed_and_stale_records(self) -> None:
        records = self.parent / "records"
        records.mkdir()
        (records / "attempts.jsonl").write_text("{bad json\n", encoding="utf-8")
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["disposition"], "invalidated")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

    def test_schema_duplicate_and_external_log_evidence_is_rejected(self) -> None:
        records, events = self.execute()
        events[0]["schema_version"] = 1
        (records / "attempts.jsonl").write_text(
            "".join(planner.canonical_bytes(event).decode("utf-8") for event in events), encoding="utf-8"
        )
        stale = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in stale["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_schema_stale")

        records, _ = self.execute(self.parent / "duplicate-records")
        attempts = records / "attempts.jsonl"
        original = attempts.read_text(encoding="utf-8")
        attempts.write_text(original + original.splitlines()[0] + "\n", encoding="utf-8")
        duplicate = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in duplicate["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "duplicate_event")

        records, events = self.execute(self.parent / "external-log-records")
        events[0]["log"] = "../outside.log"
        events[1]["log"] = "../outside.log"
        (records / "attempts.jsonl").write_text(
            "".join(planner.canonical_bytes(event).decode("utf-8") for event in events), encoding="utf-8"
        )
        external = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in external["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "log_invalid")

    def test_malformed_timestamps_never_receive_reuse_advice(self) -> None:
        mutations = (
            ("started", "start_utc", 123),
            ("started", "start_epoch_seconds", "not-a-number"),
            ("finished", "end_utc", 123),
            ("finished", "end_epoch_seconds", "not-a-number"),
        )
        for event_name, field, value in mutations:
            with self.subTest(field=field):
                records, events = self.execute(self.parent / f"invalid-{field}")
                next(event for event in events if event["event"] == event_name)[field] = value
                (records / "attempts.jsonl").write_text(
                    "".join(planner.canonical_bytes(event).decode("utf-8") for event in events),
                    encoding="utf-8",
                )
                plan = self.plan(["sqlite-now:src/a.py"], records)
                lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
                self.assertEqual(lane["disposition"], "invalidated")
                self.assertNotEqual(lane["disposition"], "reuse_recommended")

    def test_semantically_invalid_records_never_receive_reuse_advice(self) -> None:
        records, events = self.execute(self.parent / "fake-identity")
        fake = copy.deepcopy(events[0]["repositories_before"])
        for identity in fake.values():
            identity["head"] = "0" * 40
            identity["head_tree"] = "0" * 40
            identity["base"] = "0" * 40
        events[0]["repositories_before"] = fake
        events[1]["repositories_after"] = copy.deepcopy(fake)
        self.write_events(records, events)
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["disposition"], "invalidated")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "repository_identity_invalid")

        records, events = self.execute(self.parent / "inverted-time")
        finished = events[1]
        finished["end_epoch_seconds"] = events[0]["start_epoch_seconds"] - 1
        finished["end_utc"] = dt.datetime.fromtimestamp(
            finished["end_epoch_seconds"], tz=dt.timezone.utc
        ).isoformat()
        self.write_events(records, events)
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

        records, events = self.execute(self.parent / "epoch-mismatch")
        events[1]["end_epoch_seconds"] = events[0]["start_epoch_seconds"] + 1
        self.write_events(records, events)
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

        records, events = self.execute(self.parent / "signal-mismatch")
        events[1]["signal"] = 9
        self.write_events(records, events)
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

    def test_duplicate_keys_and_noncanonical_records_are_rejected(self) -> None:
        records, _ = self.execute(self.parent / "duplicate-key")
        attempts = records / "attempts.jsonl"
        first, remainder = attempts.read_bytes().split(b"\n", 1)
        first = first.replace(b'"event":"started"', b'"event":"started","event":"started"', 1)
        attempts.write_bytes(first + b"\n" + remainder)
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

        records, events = self.execute(self.parent / "unknown-field")
        events[0]["unknown"] = True
        self.write_events(records, events)
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

        records, events = self.execute(self.parent / "noncanonical")
        attempts = records / "attempts.jsonl"
        attempts.write_bytes(json.dumps(events[0], sort_keys=True).encode() + b"\n" + planner.canonical_bytes(events[1]))
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "records_malformed")

    def test_missing_log_evidence_is_rejected(self) -> None:
        records, events = self.execute(self.parent / "missing-log")
        (records / events[1]["log"]).unlink()
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "log_invalid")

    def test_interleaved_attempts_are_supported_but_incomplete_attempts_are_rejected(self) -> None:
        records, _ = self.execute(self.parent / "interleaved")
        records, events = self.execute(records)
        self.write_events(records, [events[0], events[2], events[1], events[3]])
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["disposition"], "reuse_recommended")

        records, events = self.execute(self.parent / "incomplete")
        self.write_events(records, [events[0]])
        plan = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in plan["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["rejected_prior_evidence"][0]["reason_code"], "attempt_incomplete")

    def test_reuse_invalidates_changed_worktree_content(self) -> None:
        records, _ = self.execute()
        (self.parent / "sqlitenow-kmp/src/a.py").write_text("A = 2\n", encoding="utf-8")
        changed = self.plan(["sqlite-now:src/a.py"], records)
        lane = next(item for item in changed["lanes"] if item["id"] == "lane-a")
        self.assertEqual(lane["disposition"], "invalidated")
        self.assertNotEqual(lane["disposition"], "reuse_recommended")


class ScenarioTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest, cls.digest = planner.load_manifest(MANIFEST_PATH)

    def plan(self, *paths: str) -> dict:
        stable_probe = lambda toolchain, root, environment: {
            "id": toolchain["id"], "argv": toolchain["argv"], "exit_code": 0,
            "output_sha256": "0" * 64,
            "environment_sha256": planner.sha256_bytes(planner.canonical_bytes(environment)),
        }
        with mock.patch("tools.qualification.planner.toolchain_identity", side_effect=stable_probe):
            with mock.patch("tools.qualification.planner._listed_files", return_value=[]):
                return planner.build_plan(self.manifest, self.digest, "HEAD", injected_paths=list(paths))

    @staticmethod
    def selected(plan: dict) -> set[str]:
        return {lane["id"] for lane in plan["lanes"] if lane["selected"]}

    def test_kotlin_source_selects_core_platform(self) -> None:
        kotlin = self.selected(self.plan("sqlite-now:library-core/src/commonMain/kotlin/Example.kt"))
        self.assertIn("core-platform-all", kotlin)

    def test_swift_support_and_generator(self) -> None:
        support = self.selected(self.plan("sqlite-now:swift/support/Sources/Example.swift"))
        generator = self.selected(self.plan("sqlite-now:sqlitenow-compiler/src/main/kotlin/Emitter.kt"))
        self.assertEqual(support, {"swift-hybrid"})
        self.assertTrue({"compiler", "gradle-plugin", "core-platform-all", "swift-hybrid"}.issubset(generator))

    def test_shared_contracts_and_fixtures(self) -> None:
        selected = self.selected(self.plan("sqlite-now:oversqlite-contracts/schema-evolution/example.json"))
        self.assertTrue({"oversqlite-comprehensive", "swift-hybrid", "go-client-upgrade-conformance"}.issubset(selected))

    def test_build_lock_and_toolchain(self) -> None:
        shared = self.selected(self.plan("sqlite-now:gradle/libs.versions.toml"))
        self.assertIn("core-platform-all", shared)

    def test_test_only_changes(self) -> None:
        product = self.selected(self.plan("sqlite-now:sqlitenow-compiler/src/test/kotlin/OnlyTest.kt"))
        planner = self.selected(self.plan("sqlite-now:tools/qualification/tests/test_planner.py"))
        self.assertEqual(product, {"compiler"})
        self.assertEqual(planner, {"planner-infrastructure"})

    def test_document_classes(self) -> None:
        ordinary = self.selected(self.plan("sqlite-now:docs/guide.md"))
        governing = self.selected(self.plan("sqlite-now:docs-engineering/qualification.md"))
        self.assertEqual(ordinary, {"planner-infrastructure"})
        self.assertIn("oversqlite-realserver-all", governing)
        self.assertIn("go-full", governing)


class ContainmentTests(FixtureCase):
    def test_retired_bare_test_prefixes_are_absent(self) -> None:
        manifest, _ = planner.load_manifest(MANIFEST_PATH)
        self.assertNotIn(["test"], manifest["forbidden_planning_validation_prefixes"])

    def test_no_product_command_executed(self) -> None:
        manifest, _ = planner.load_manifest(MANIFEST_PATH)
        product_argv = [lane["argv"] for lane in manifest["lanes"] if lane["classification"] == "product"]
        prefixes = manifest["forbidden_planning_validation_prefixes"]
        for argv in product_argv:
            self.assertTrue(any(argv[: len(prefix)] == prefix for prefix in prefixes), argv)
        plan_path = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        planner.run_plan(plan_path, self.manifest_path, ["lane-a"], False, self.parent / "records", False, self.parent)
        started = json.loads((self.parent / "records/attempts.jsonl").read_text().splitlines()[0])
        self.assertEqual(started["argv"][0], sys.executable)
        self.assertIn("stub_lane.py", started["argv"][1])


class DocumentationTests(unittest.TestCase):
    def test_required_usage(self) -> None:
        text = README_PATH.read_text(encoding="utf-8")
        for phrase in ("plan --base HEAD", "validate-manifest", "--accept-selected", "append", "advisory"):
            self.assertIn(phrase, text)

    def test_durable_contract_is_spec_independent(self) -> None:
        text = (ROOT / "docs-engineering/qualification.md").read_text(encoding="utf-8")
        self.assertIn("Qualification planning and execution", text)
        self.assertNotIn("specs/", text)


class PerformanceTests(FixtureCase):
    def test_bounded_synthetic_overhead(self) -> None:
        samples = []
        for _ in range(3):
            started = time.perf_counter_ns()
            self.plan(["sqlite-now:src/a.py"])
            samples.append((time.perf_counter_ns() - started) / 1_000_000)
        self.assertLess(max(samples), 2000)


if __name__ == "__main__":
    unittest.main()
