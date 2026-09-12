"""Qualification campaign scheduler tests; every child is a controlled Python stub."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from tools.qualification import campaign, planner
from tools.qualification.tests.test_planner import FixtureCase


CONTROLLED = Path(__file__).with_name("fixtures") / "controlled_lane.py"


class CampaignTests(FixtureCase):
    def configure(self, statuses: tuple[int, int] = (0, 0), shared: bool = False) -> Path:
        events = self.parent / "events.txt"
        for lane, status in zip(self.manifest["lanes"][:2], statuses):
            lane["argv"] = [sys.executable, str(CONTROLLED), "--events", str(events), "--label", lane["id"], "--status", str(status), "--sleep", "0.2"]
            lane["resource_locks"] = ["fixture"] if shared else [f"lock-{lane['id']}"]
        self.manifest["resources"] = {"fixture": 1, "lock-lane-a": 1, "lock-lane-b": 1, "measurement-exclusive": 1}
        self.manifest["campaign_max_workers"] = 2
        self.write_manifest()
        return events

    def selected_plan(self) -> Path:
        return self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))

    def assert_process_exited(self, pid_file: Path) -> None:
        for _ in range(100):
            if pid_file.exists():
                pid = int(pid_file.read_text(encoding="utf-8"))
                self.assert_pid_exited(pid)
                return
            time.sleep(0.01)
        self.fail(f"process did not publish {pid_file}")

    def assert_pid_exited(self, pid: int) -> None:
        for _ in range(100):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            status = subprocess.run(
                ["ps", "-o", "stat=", "-p", str(pid)],
                check=False, capture_output=True, text=True,
            ).stdout.strip()
            if not status or status.startswith("Z"):
                return
            time.sleep(0.01)
        details = subprocess.run(
            ["ps", "-o", "pid,ppid,pgid,stat,command", "-p", str(pid)],
            check=False, capture_output=True, text=True,
        ).stdout.strip()
        self.fail(f"process recorded by {pid_file} is still alive: {details}")

    def test_independent_lanes_overlap_and_report_is_deterministic(self) -> None:
        events = self.configure()
        for lane in self.manifest["lanes"][:2]:
            lane["argv"] += ["--wait-for-start-count", "2"]
        self.write_manifest()
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        lines = events.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines[:2], ["start:lane-a", "start:lane-b"])
        first = campaign.campaign_report(campaign_id, self.parent / "records")
        second = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(planner.canonical_bytes(first), planner.canonical_bytes(second))
        self.assertEqual(first["maximum_observed_concurrency"], 2)
        self.assertIn("orchestration_evidence", first["authority"])
        self.assertIn("resource_utilization", first)

    def test_report_command_writes_report_json_without_plan_renderer(self) -> None:
        self.configure()
        records = self.parent / "records"
        campaign_id, code = campaign.run_campaign(
            self.selected_plan(), self.manifest_path, records, ["lane-a", "lane-b"], False,
            workspace_root=self.parent,
        )
        self.assertEqual(code, 0)
        output = self.parent / "report.json"
        self.assertEqual(
            campaign.main(["report", "--campaign", campaign_id, "--records", str(records), "--output", str(output)]),
            0,
        )
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), campaign.campaign_report(campaign_id, records))

    def test_shared_lock_serializes_and_failure_blocks_descendant(self) -> None:
        events = self.configure(shared=True)
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        lines = events.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines, ["start:lane-a", "finish:lane-a", "start:lane-b", "finish:lane-b"])
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(report["counts"]["passed"], 2)
        self.assertGreater(report["lanes"][1]["lock_wait_seconds"], 0)
        second_queued = next(index for index, event in enumerate(report["events"]) if event["event"] == "lane_queued" and event["lane_id"] == "lane-b")
        first_cleanup = next(index for index, event in enumerate(report["events"]) if event["event"] == "lane_cleanup" and event["lane_id"] == "lane-a")
        self.assertLess(second_queued, first_cleanup)

    def test_failed_dependency_blocks_descendant_without_launching_it(self) -> None:
        events = self.configure(statuses=(7, 0))
        self.manifest["lanes"][1]["depends_on"] = ["lane-a"]
        self.write_manifest()
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, continue_on_failure=True, workspace_root=self.parent)
        self.assertEqual(code, 11)
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(report["counts"]["blocked_dependency"], 1)
        self.assertNotIn("start:lane-b", events.read_text(encoding="utf-8"))

    def test_stop_on_failure_preserves_actual_outcome_of_already_running_lane(self) -> None:
        self.configure(statuses=(7, 0))
        for lane in self.manifest["lanes"][:2]:
            lane["argv"] += ["--wait-for-start-count", "2"]
        self.write_manifest()
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 11)
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(report["counts"]["failed"], 1)
        self.assertEqual(report["counts"]["passed"], 1)

    def test_timeout_reaps_the_owned_process_group(self) -> None:
        events = self.configure()
        lane = self.manifest["lanes"][0]
        lane["timeout_seconds"] = 1
        child_pid = self.parent / "child.pid"
        lane["argv"] += ["--child", "--child-pid-file", str(child_pid), "--sleep", "60"]
        self.write_manifest()
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        campaign_id, code = campaign.run_campaign(plan, self.manifest_path, self.parent / "records", ["lane-a"], False, workspace_root=self.parent)
        self.assertEqual(code, 11)
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(report["counts"]["timed_out"], 1)
        self.assertIn("start:lane-a", events.read_text(encoding="utf-8"))
        self.assert_process_exited(child_pid)

    def test_failure_policies_and_authority_refusals(self) -> None:
        self.configure(statuses=(7, 0))
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "stop", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 11)
        stop_report = campaign.campaign_report(campaign_id, self.parent / "stop")
        self.assertEqual(stop_report["counts"]["failed"], 1)
        self.assertEqual(stop_report["counts"]["passed"] + stop_report["counts"]["cancelled"], 1)
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "continue", ["lane-a", "lane-b"], False, continue_on_failure=True, workspace_root=self.parent)
        self.assertEqual(code, 11)
        self.assertEqual(campaign.campaign_report(campaign_id, self.parent / "continue")["counts"]["failed"], 1)
        self.manifest["lanes"][0]["classification"] = "product"
        self.write_manifest()
        with self.assertRaisesRegex(planner.QualificationError, "product"):
            campaign.run_campaign(self.save_plan(self.plan(["sqlite-now:src/a.py"])), self.manifest_path, self.parent / "denied", ["lane-a"], False, workspace_root=self.parent)
        self.manifest["lanes"][0]["classification"] = "infrastructure"
        self.manifest["lanes"][0]["repository"] = "go-oversync"
        self.manifest["lanes"][0]["campaign_eligible"] = False
        self.manifest["lanes"][0]["static_inputs"] = ["README.md"]
        self.write_manifest()
        with self.assertRaisesRegex(planner.QualificationError, "planning-only"):
            campaign.run_campaign(self.save_plan(self.plan(["sqlite-now:src/a.py"])), self.manifest_path, self.parent / "go-denied", ["lane-a"], False, workspace_root=self.parent)

    def test_resume_retains_only_successful_exact_campaign(self) -> None:
        self.configure()
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        self.assertEqual(campaign.resume_campaign(campaign_id, self.manifest_path, self.parent / "records", workspace_root=self.parent), 0)
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(report["counts"]["retained"], 2)

    def test_resume_rejects_missing_log_and_partial_attempt(self) -> None:
        self.configure()
        records = self.parent / "records"
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        campaign_id, code = campaign.run_campaign(plan, self.manifest_path, records, ["lane-a"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        attempts = [json.loads(line) for line in (records / "attempts.jsonl").read_text(encoding="utf-8").splitlines()]
        finished = next(event for event in attempts if event["event"] == "finished")
        (records / finished["log"]).unlink()
        with self.assertRaisesRegex(planner.QualificationError, "log"):
            campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent)

        records2 = self.parent / "partial"
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, records2, ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        lines = (records2 / "attempts.jsonl").read_bytes().splitlines()
        (records2 / "attempts.jsonl").write_bytes(b"\n".join(lines[:-1]) + b"\n")
        with self.assertRaisesRegex(planner.QualificationError, "complete attempt"):
            campaign.resume_campaign(campaign_id, self.manifest_path, records2, workspace_root=self.parent)

    def test_resume_refuses_failed_lane_without_retry(self) -> None:
        self.configure(statuses=(7, 0))
        records = self.parent / "records"
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, records, ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 11)
        before = (records / "attempts.jsonl").read_bytes()
        self.assertEqual(campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent), 11)
        self.assertEqual((records / "attempts.jsonl").read_bytes(), before)

    def test_resume_marks_abruptly_incomplete_lane_unresolved_without_retry(self) -> None:
        self.configure()
        records = self.parent / "records"
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        campaign_id, code = campaign.run_campaign(plan, self.manifest_path, records, ["lane-a"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        attempts_path = records / "attempts.jsonl"
        attempts = [json.loads(line) for line in attempts_path.read_text(encoding="utf-8").splitlines()]
        started = next(event for event in attempts if event["event"] == "started" and event["lane_id"] == "lane-a")
        attempts_path.write_bytes(planner.canonical_bytes(started))
        campaign_path = campaign._campaign_path(records, campaign_id)
        events = [json.loads(line) for line in campaign_path.read_text(encoding="utf-8").splitlines()]
        cutoff = next(index for index, event in enumerate(events) if event["event"] == "lane_started" and event["lane_id"] == "lane-a")
        campaign_path.write_bytes(b"".join(planner.canonical_bytes(event) for event in events[: cutoff + 1]))
        before = attempts_path.read_bytes()
        self.assertEqual(campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent), 11)
        self.assertEqual(attempts_path.read_bytes(), before)
        self.assertEqual(campaign.campaign_report(campaign_id, records)["counts"]["unresolved"], 1)

    def test_resume_rejects_stale_input_and_manifest(self) -> None:
        self.configure()
        records = self.parent / "records"
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, records, ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        source = self.parent / "sqlitenow-kmp/src/a.py"
        source.write_text("A = 2\n", encoding="utf-8")
        with self.assertRaisesRegex(planner.QualificationError, "relevant inputs changed"):
            campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent)
        source.write_text("A = 1\n", encoding="utf-8")
        self.manifest["campaign_max_workers"] = 3
        self.write_manifest()
        with self.assertRaisesRegex(planner.QualificationError, "manifest"):
            campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent)

    def test_resume_rejects_environment_and_read_only_sibling_drift(self) -> None:
        self.configure()
        records = self.parent / "records"
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, records, ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        with mock.patch.dict(os.environ, {"PATH": os.environ["PATH"] + os.pathsep + "/controlled-change"}):
            with self.assertRaises(planner.QualificationError):
                campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent)
        (self.parent / "go-oversync/untracked.txt").write_text("drift\n", encoding="utf-8")
        with self.assertRaisesRegex(planner.QualificationError, "read-only"):
            campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent)

    def test_dependency_digest_is_recorded_and_revalidated(self) -> None:
        self.configure()
        self.manifest["lanes"][1]["depends_on"] = ["lane-a"]
        self.write_manifest()
        records = self.parent / "records"
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))
        campaign_id, code = campaign.run_campaign(plan, self.manifest_path, records, ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        events = [json.loads(line) for line in (records / "attempts.jsonl").read_text(encoding="utf-8").splitlines()]
        lane_b = next(event for event in events if event["event"] == "started" and event["lane_id"] == "lane-b")
        saved = json.loads(plan.read_text(encoding="utf-8"))
        lane_a = next(lane for lane in saved["lanes"] if lane["id"] == "lane-a")
        self.assertEqual(lane_b["dependency_digests"], {"lane-a": lane_a["relevant_input_sha256"]})
        self.assertEqual(campaign.campaign_report(campaign_id, records)["critical_path_observation"], ["lane-a", "lane-b"])
        lane_b["dependency_digests"]["lane-a"] = "f" * 64
        (records / "attempts.jsonl").write_bytes(b"".join(planner.canonical_bytes(event) for event in events))
        with self.assertRaisesRegex(planner.QualificationError, "no longer applicable"):
            campaign.resume_campaign(campaign_id, self.manifest_path, records, workspace_root=self.parent)

    def test_accept_selected_does_not_execute_reuse_recommendations(self) -> None:
        self.configure()
        records = self.parent / "records"
        initial = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        self.assertEqual(planner.run_plan(initial, self.manifest_path, ["lane-a"], False, records, False, self.parent), 0)
        advisory = self.save_plan(self.plan(["sqlite-now:src/a.py"], records))
        with self.assertRaisesRegex(planner.QualificationError, "non-empty"):
            campaign.run_campaign(advisory, self.manifest_path, self.parent / "campaign", [], True, workspace_root=self.parent)

    def test_launch_failure_reaps_already_running_lane(self) -> None:
        self.configure()
        parent_pid = self.parent / "parent.pid"
        self.manifest["lanes"][0]["argv"] += ["--pid-file", str(parent_pid), "--sleep", "60"]
        self.manifest["lanes"][1]["argv"] = [str(self.parent / "does-not-exist")]
        self.write_manifest()
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))
        campaign_id, code = campaign.run_campaign(plan, self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 11)
        self.assert_process_exited(parent_pid)
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertTrue(report["events"][-1]["event"] == "campaign_finished")
        self.assertIn("launch lane-b", report["events"][-1]["orchestrator_error"])

    def test_record_append_exception_reaps_running_lane(self) -> None:
        self.configure()
        parent_pid = self.parent / "append-parent.pid"
        self.manifest["lanes"][0]["argv"] += ["--pid-file", str(parent_pid), "--sleep", "60"]
        self.write_manifest()
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))
        original = campaign._append
        failed = False

        def flaky(path: Path, event: dict) -> None:
            nonlocal failed
            original(path, event)
            if event["event"] == "lane_acquired" and event.get("lane_id") == "lane-b" and not failed:
                failed = True
                raise planner.QualificationError(12, "injected_append_failure", "controlled failure")

        with mock.patch.object(campaign, "_append", side_effect=flaky):
            with self.assertRaisesRegex(planner.QualificationError, "controlled failure"):
                campaign.run_campaign(plan, self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assert_process_exited(parent_pid)
        campaign_files = list((self.parent / "records/campaigns").glob("*.jsonl"))
        self.assertEqual(len(campaign_files), 1)
        self.assertEqual(campaign._read_events(campaign_files[0])[-1]["event"], "campaign_finished")

    def test_terminal_preflight_error_reaps_running_lane(self) -> None:
        self.configure()
        parent_pid = self.parent / "stale-parent.pid"
        self.manifest["lanes"][0]["argv"] += ["--pid-file", str(parent_pid), "--sleep", "60"]
        self.write_manifest()
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py", "sqlite-now:src/b.py"]))
        original = campaign._preflight
        start_process = campaign._start_process
        calls = 0
        launched: list[int] = []

        def stale_on_second_launch(*args: object, **kwargs: object) -> tuple[dict, dict]:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise planner.QualificationError(5, "injected_stale", "controlled identity drift")
            return original(*args, **kwargs)

        def record_launch(argv: list[str], cwd: Path, environment: dict[str, str], log: object) -> subprocess.Popen[bytes]:
            process = start_process(argv, cwd, environment, log)
            launched.append(process.pid)
            return process

        with mock.patch.object(campaign, "_preflight", side_effect=stale_on_second_launch), mock.patch.object(campaign, "_start_process", side_effect=record_launch):
            with self.assertRaisesRegex(planner.QualificationError, "controlled identity drift"):
                campaign.run_campaign(plan, self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(len(launched), 1)
        self.assert_pid_exited(launched[0])
        campaign_path = next((self.parent / "records/campaigns").glob("*.jsonl"))
        recorded = campaign._read_events(campaign_path)
        self.assertTrue(any(event["event"] == "lane_cleanup" and event["lane_id"] == "lane-a" for event in recorded))
        self.assertEqual(recorded[-1]["event"], "campaign_finished")

    def test_launch_audit_observes_only_controlled_fixture(self) -> None:
        self.configure()
        launched: list[list[str]] = []
        original = campaign._start_process

        def audited(argv: list[str], cwd: Path, environment: dict[str, str], log: object) -> subprocess.Popen[bytes]:
            launched.append(list(argv))
            return original(argv, cwd, environment, log)

        with mock.patch.object(campaign, "_start_process", side_effect=audited):
            _, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, self.parent / "records", ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        self.assertEqual(len(launched), 2)
        self.assertTrue(all(str(CONTROLLED) in argv for argv in launched))

    def test_signal_reaps_process_tree_and_records_final_state(self) -> None:
        self.configure()
        child_pid = self.parent / "signal-child.pid"
        self.manifest["lanes"][0]["argv"] += ["--child", "--child-pid-file", str(child_pid), "--sleep", "60"]
        self.write_manifest()
        plan = self.save_plan(self.plan(["sqlite-now:src/a.py"]))
        def signal_after_launch() -> None:
            while not child_pid.exists():
                time.sleep(0.01)
            time.sleep(0.05)
            os.kill(os.getpid(), signal.SIGTERM)

        sender = threading.Thread(target=signal_after_launch)
        sender.start()
        try:
            campaign_id, code = campaign.run_campaign(plan, self.manifest_path, self.parent / "records", ["lane-a"], False, workspace_root=self.parent)
        finally:
            sender.join(timeout=2)
        self.assertEqual(code, 11)
        self.assert_process_exited(child_pid)
        report = campaign.campaign_report(campaign_id, self.parent / "records")
        self.assertEqual(report["events"][-1]["outcome"], "unsuccessful")

    def test_campaign_record_corruption_is_fail_closed(self) -> None:
        self.configure()
        records = self.parent / "records"
        campaign_id, code = campaign.run_campaign(self.selected_plan(), self.manifest_path, records, ["lane-a", "lane-b"], False, workspace_root=self.parent)
        self.assertEqual(code, 0)
        path = campaign._campaign_path(records, campaign_id)
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        events[1]["campaign_id"] = "different"
        path.write_bytes(b"".join(planner.canonical_bytes(event) for event in events))
        with self.assertRaisesRegex(planner.QualificationError, "identity"):
            campaign.campaign_report(campaign_id, records)

    def test_live_manifest_has_complete_campaign_metadata_and_go_refusal(self) -> None:
        manifest, _ = planner.load_manifest(Path(__file__).parents[3] / "tools/qualification/lanes.json")
        self.assertEqual(len(manifest["lanes"]), 69)
        self.assertTrue(all("resource_locks" in lane for lane in manifest["lanes"]))
        self.assertTrue(all(not lane["campaign_eligible"] for lane in manifest["lanes"] if lane["repository"] == "go-oversync"))
        eligible = [set(lane["resource_locks"]) for lane in manifest["lanes"] if lane["campaign_eligible"]]
        self.assertFalse(set.intersection(*eligible))
        self.assertTrue(any(left.isdisjoint(right) for index, left in enumerate(eligible) for right in eligible[index + 1:]))
        locks = {lane["id"]: set(lane["resource_locks"]) for lane in manifest["lanes"]}
        self.assertTrue(locks["compiler"].isdisjoint(locks["go-client-upgrade-conformance"]))
        self.assertIn("live-authority", locks["oversqlite-realserver-jvm"])
        self.assertIn("measurement-exclusive", locks["kmp-resource"])


class ContainmentTests(unittest.TestCase):
    def test_real_product_prefixes_are_data_only(self) -> None:
        manifest, _ = planner.load_manifest(Path(__file__).parents[3] / "tools/qualification/lanes.json")
        forbidden = manifest["forbidden_planning_validation_prefixes"]
        self.assertTrue(forbidden)
        executed = [[sys.executable, str(CONTROLLED), "--events", "/tmp/controlled", "--label", "fixture"]]
        self.assertTrue(all(str(CONTROLLED) in argv for argv in executed))
        self.assertFalse(any(argv[: len(prefix)] == prefix for argv in executed for prefix in forbidden))


if __name__ == "__main__":
    unittest.main()
