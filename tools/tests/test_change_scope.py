from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from tools import check_change_scope as scope


class ChangeScopeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Scope Test")
        self.git("config", "user.email", "scope@example.invalid")
        (self.root / "tracked.txt").write_text("committed")
        (self.root / ".gitignore").write_text(".artifacts/\nAGENTS.md\n")
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")

    def tearDown(self):
        self.temporary.cleanup()

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args],
                              check=True, capture_output=True)

    def test_existing_dirty_and_untracked_are_preserved_but_later_edits_fail(self):
        (self.root / "tracked.txt").write_text("existing dirty")
        (self.root / "untracked.txt").write_text("existing untracked")
        baseline = scope.snapshot(self.root, ["allowed.txt"], False)
        self.assertEqual(scope.check(self.root, baseline)["status"], "PASS")
        (self.root / "tracked.txt").write_text("external edit")
        (self.root / "untracked.txt").unlink()
        (self.root / "unexpected file.txt").write_text("new")
        self.assertEqual(scope.check(self.root, baseline)["outsideAllowedPaths"],
                         ["tracked.txt", "unexpected file.txt", "untracked.txt"])

    def test_directory_allow_is_bounded_and_allowed_file_can_be_created(self):
        baseline = scope.snapshot(self.root, ["tests/", "allowed.txt"], False)
        (self.root / "tests").mkdir()
        (self.root / "tests/new.txt").write_text("allowed")
        (self.root / "allowed.txt").write_text("allowed")
        self.assertEqual(scope.check(self.root, baseline)["status"], "PASS")
        (self.root / "tests-other.txt").write_text("outside")
        self.assertEqual(scope.check(self.root, baseline)["outsideAllowedPaths"], ["tests-other.txt"])

    def test_explicit_ignored_authority_is_checked_without_build_cache_noise(self):
        (self.root / "AGENTS.md").write_text("rules")
        baseline = scope.snapshot(self.root, ["AGENTS.md"], False)
        (self.root / ".artifacts").mkdir()
        (self.root / ".artifacts/log.txt").write_text("ignored evidence")
        (self.root / "AGENTS.md").write_text("new rules")
        self.assertEqual(scope.check(self.root, baseline)["changedPaths"], ["AGENTS.md"])

    def test_test_only_rejects_product_and_bridge_sources_even_when_allowed(self):
        baseline = scope.snapshot(self.root, ["module/", "swift/"], True)
        paths = ["module/src/commonTest/Allowed.kt", "module/src/commonMain/Product.kt",
                 "module/src/a3Noop/Probe.kt", "swift/Sources/Product.swift"]
        for path in paths:
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("source")
        report = scope.check(self.root, baseline)
        self.assertEqual(report["productionSourceChanges"], sorted(paths[1:]))
        self.assertEqual(report["status"], "FAIL")

    def test_staging_and_head_drift_fail_even_with_allowed_paths(self):
        baseline = scope.snapshot(self.root, ["tracked.txt"], False)
        (self.root / "tracked.txt").write_text("authorized edit")
        self.git("add", "tracked.txt")
        self.assertIn("indexSHA256", scope.check(self.root, baseline)["identityDrift"])
        self.git("commit", "-qm", "changed identity")
        self.assertIn("head", scope.check(self.root, baseline)["identityDrift"])

    def test_modes_and_symlink_targets_are_checked_without_following_target(self):
        target = self.root / "untracked.txt"
        target.write_text("original")
        link = self.root / "link"
        link.symlink_to("untracked.txt")
        baseline = scope.snapshot(self.root, ["allowed.txt"], False)
        target.chmod(target.stat().st_mode | 0o100)
        link.unlink()
        link.symlink_to("allowed.txt")
        self.assertEqual(scope.check(self.root, baseline)["outsideAllowedPaths"], ["link", "untracked.txt"])

    def test_invalid_allowlist_and_wrong_repo_fail(self):
        for path in ["../escape", "/absolute", "src/*", "./file", ""]:
            with self.assertRaises(scope.ScopeError):
                scope.snapshot(self.root, [path], False)
        baseline = scope.snapshot(self.root, ["allowed.txt"], False)
        baseline["root"] = "/different-repository"
        self.assertIn("root", scope.check(self.root, baseline)["identityDrift"])
        with self.assertRaises(scope.ScopeError):
            scope.check(self.root, [])

    def test_cli_cannot_overwrite_baseline_or_widen_scope(self):
        baseline = self.root / ".artifacts/baseline.json"
        baseline.parent.mkdir()
        script = Path(scope.__file__).resolve()
        def run(*args):
            return subprocess.run([sys.executable, str(script), *args, "--root", str(self.root),
                                   "--baseline", str(baseline)], capture_output=True)
        self.assertEqual(run("snapshot", "--allow", "tracked.txt").returncode, 0)
        before = baseline.read_bytes()
        self.assertEqual(run("snapshot", "--allow", "other.txt").returncode, 2)
        self.assertEqual(baseline.read_bytes(), before)
        self.assertEqual(run("check", "--allow", "other.txt").returncode, 2)
        self.assertEqual(run("check").returncode, 0)
        (self.root / "unexpected.txt").write_text("outside")
        self.assertEqual(run("check").returncode, 1)
        baseline.write_text("not JSON")
        self.assertEqual(run("check").returncode, 2)
        baseline.write_text("[]")
        self.assertEqual(run("check").returncode, 2)
        unignored = self.root / "baseline.json"
        result = subprocess.run([sys.executable, str(script), "snapshot", "--root", str(self.root),
                                 "--baseline", str(unignored), "--allow", "tracked.txt"], capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(unignored.exists())

    def test_build_wiring_is_reported_for_manual_review(self):
        baseline = scope.snapshot(self.root, ["build.gradle.kts"], True)
        (self.root / "build.gradle.kts").write_text("test task wiring")
        self.assertEqual(scope.check(self.root, baseline)["buildWiringRequiresReview"], ["build.gradle.kts"])


if __name__ == "__main__":
    unittest.main()
