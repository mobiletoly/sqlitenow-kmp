from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from tools import check_repository_hygiene as hygiene


def run_git(root: Path, *arguments: str) -> None:
    subprocess.run(["git", "-C", str(root), *arguments], check=True, stdout=subprocess.DEVNULL)


class RepositoryHygieneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        run_git(self.root, "init", "-q")
        run_git(self.root, "config", "user.name", "Hygiene Test")
        run_git(self.root, "config", "user.email", "hygiene@example.invalid")
        (self.root / "tools").mkdir()
        self.exceptions = self.root / "tools/repository-hygiene-exceptions.json"
        self.exceptions.write_text("[]\n", encoding="utf-8")
        (self.root / "README.md").write_text("repository\n", encoding="utf-8")
        run_git(self.root, "add", ".")
        run_git(self.root, "commit", "-qm", "fixture")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def messages(self, revision: str | None = None) -> list[str]:
        return hygiene.check(self.root, revision, self.exceptions)

    def test_positive_semantic_versions_case_ids_graphics_and_policy_prose(self) -> None:
        (self.root / "positive.txt").write_text(
            "format_version=2 protocol-v3 C01 C12 CU-R01 U0 U8 M01 M30 #fff \\u001f 1.2.3\n"
            "svg-path=M9,0L9,108\n"
            "Root spec and specs directories are local disposable authoring areas.\n"
            "--governing-input repository-id:relative-file\n",
            encoding="utf-8",
        )
        self.assertEqual(self.messages(), [])

    def test_numbered_names_camel_case_and_concrete_spec_reads_fail(self) -> None:
        numbered = "Phase" + "7Generated.kt"
        (self.root / numbered).write_text("class " + "M" + "0009Thing\n", encoding="utf-8")
        (self.root / "reader.py").write_text(
            "open('" + "specs" + "/temporary-authority.md')\n",
            encoding="utf-8",
        )
        messages = self.messages()
        self.assertTrue(any("temporary milestone token in path" in item for item in messages))
        self.assertTrue(any("temporary milestone token in text" in item for item in messages))
        self.assertTrue(any("concrete dependency or link" in item for item in messages))

    def test_short_fixture_and_r_case_alias_forms_fail(self) -> None:
        (self.root / "aliases.kt").write_text(
            "class " + "M9" + "GenerationOne\n"
            + "val profile = \"" + "M9" + "-D01\"\n"
            + "fun " + "r06" + "CheckpointRecovery() = Unit\n"
            + "fun schemaevolution" + "R07" + "RetainedAuthority() = Unit\n"
            + "val path = \"" + "r07" + "-temporary\"\n",
            encoding="utf-8",
        )
        messages = self.messages()
        tokens = [item.rsplit(": ", 1)[-1] for item in messages]
        self.assertEqual(tokens.count("M9"), 2)
        self.assertIn("r06", tokens)
        self.assertIn("R07", tokens)
        self.assertIn("r07", tokens)

    def test_mixed_case_variants_fail_in_paths_and_text(self) -> None:
        fixtures = (
            ("s" + "E03Thing.kt", "class " + "s" + "E03Thing\n", "s" + "E03"),
            ("q" + "P0.py", "value = '" + "q" + "P0'\n", "q" + "P0"),
            ("p" + "HASE7.txt", "label=" + "p" + "HASE7\n", "p" + "HASE7"),
        )
        for path, content, _token in fixtures:
            (self.root / path).write_text(content, encoding="utf-8")

        messages = self.messages()
        for _path, _content, token in fixtures:
            matching = [item for item in messages if item.endswith(f": {token}")]
            self.assertEqual(len(matching), 2)
            self.assertTrue(any("token in path" in item for item in matching))
            self.assertTrue(any("token in text" in item for item in matching))

    def test_indexed_missing_and_force_added_specs_fail(self) -> None:
        spec_path = self.root / "specs" / "temporary.md"
        spec_path.parent.mkdir()
        spec_path.write_text("temporary\n", encoding="utf-8")
        run_git(self.root, "add", str(spec_path.relative_to(self.root)))
        run_git(self.root, "commit", "-qm", "tracked spec")
        spec_path.unlink()
        self.assertTrue(any("root specification is tracked" in item for item in self.messages()))

        run_git(self.root, "rm", "--cached", "-q", "--", "spec" + "s/temporary.md")
        (self.root / ".gitignore").write_text("/specs/\n", encoding="utf-8")
        spec_path.write_text("temporary\n", encoding="utf-8")
        run_git(self.root, "add", "-f", "spec" + "s/temporary.md")
        self.assertTrue(any("root specification is tracked" in item for item in self.messages()))

    def test_revision_mode_reads_only_the_requested_tree(self) -> None:
        (self.root / "clean.txt").write_text("clean\n", encoding="utf-8")
        run_git(self.root, "add", "clean.txt")
        run_git(self.root, "commit", "-qm", "clean")
        revision = subprocess.check_output(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True
        ).strip()
        (self.root / "clean.txt").write_text("class " + "SE" + "03Thing\n", encoding="utf-8")
        self.assertEqual(self.messages(revision), [])
        self.assertTrue(self.messages())

    def test_revision_mode_reads_exception_registry_from_requested_tree(self) -> None:
        token = "QP" + "0"
        (self.root / "fixture.txt").write_text(token + "\n", encoding="utf-8")
        run_git(self.root, "add", "fixture.txt")
        run_git(self.root, "commit", "-qm", "revision violation")
        revision = subprocess.check_output(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True
        ).strip()

        self.exceptions.write_text(json.dumps([{
            "path": "fixture.txt",
            "token": token,
            "count": 1,
            "reason": "dirty worktree exception",
        }]) + "\n", encoding="utf-8")

        self.assertEqual(self.messages(), [])
        self.assertTrue(any(token in item for item in self.messages(revision)))

    def test_revision_mode_requires_valid_committed_exception_registry(self) -> None:
        run_git(self.root, "rm", "tools/repository-hygiene-exceptions.json")
        run_git(self.root, "commit", "-qm", "missing exception registry")
        missing_revision = subprocess.check_output(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True
        ).strip()
        with self.assertRaises(hygiene.HygieneError):
            self.messages(missing_revision)

        self.exceptions.parent.mkdir(exist_ok=True)
        self.exceptions.write_text("{\n", encoding="utf-8")
        run_git(self.root, "add", "tools/repository-hygiene-exceptions.json")
        run_git(self.root, "commit", "-qm", "invalid exception registry")
        invalid_revision = subprocess.check_output(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True
        ).strip()
        with self.assertRaises(hygiene.HygieneError):
            self.messages(invalid_revision)

    def test_revision_mode_rejects_alternate_worktree_exception_registry(self) -> None:
        token = "QP" + "0"
        (self.root / "fixture.txt").write_text(token + "\n", encoding="utf-8")
        run_git(self.root, "add", "fixture.txt")
        run_git(self.root, "commit", "-qm", "revision violation")
        revision = subprocess.check_output(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True
        ).strip()
        alternate = self.root / "alternate-exceptions.json"
        alternate.write_text(json.dumps([{
            "path": "fixture.txt",
            "token": token,
            "count": 1,
            "reason": "alternate worktree registry",
        }]) + "\n", encoding="utf-8")

        with self.assertRaises(hygiene.HygieneError):
            hygiene.check(self.root, revision, alternate)
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(hygiene.__file__).resolve()),
                "--root",
                str(self.root),
                "--revision",
                revision,
                "--exceptions",
                str(alternate),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("cannot override the committed exception registry", completed.stderr)
        self.assertNotIn("repository hygiene passed", completed.stdout)

    def test_exact_exception_count_and_unused_exception_validation(self) -> None:
        token = "QP" + "0"
        (self.root / "fixture.txt").write_text(token + "\n", encoding="utf-8")
        self.exceptions.write_text(json.dumps([{
            "path": "fixture.txt",
            "token": token,
            "count": 1,
            "reason": "negative scanner fixture",
        }]) + "\n", encoding="utf-8")
        self.assertEqual(self.messages(), [])
        (self.root / "fixture.txt").write_text("clean\n", encoding="utf-8")
        self.assertTrue(any("expected 1 occurrence" in item for item in self.messages()))


if __name__ == "__main__":
    unittest.main()
