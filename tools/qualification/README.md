# Qualification planner

`planner.py` maps changed SQLiteNow paths to retained, advisory qualification lanes. It records lane selection, immutable identities, resource locks, and reviewer-facing evidence without running product validation during planning.

The lane manifest is `lanes.json`; the manifest and plan schema version is 3, while the attempt and campaign record schema version remains 2. `campaign.py` runs explicitly accepted lanes and records the campaign separately. The tools have no acceptance or closure authority and cannot mutate the sibling Go repository. A qualification reviewer must confirm every reuse recommendation.

Run planner tests with:

```sh
python3 tools/qualification/planner.py plan --base HEAD
python3 tools/qualification/planner.py validate-manifest
python3 -m unittest discover -s tools/qualification/tests
```

Use `campaign.py --accept-selected` only after an explicit decision. Campaign records append evidence; the planner remains advisory.

The planner may recommend core, compiler, plugin, Swift, local platform, live-server, or Go conformance lanes. A reviewer must confirm each recommendation, establish live-server identity before live lanes, and preserve the Go repository as read-only.

Planning uses only controlled inspection commands. The manifest forbids product-validation prefixes so planning cannot become an accidental build or test run.

## Focused Oversqlite maintenance smokes

`oversqlite_smoke.py go-direct-sync` and `oversqlite_smoke.py swift-cancellation`
are explicit standalone commands, not planner or campaign replacements. They require
`OVERSYNC_EXAMPLE_TEST_DATABASE_URL` for an existing PostgreSQL service and record
redacted logs, named test results, preparation/runtime/cleanup timings and paired
identities. See [the validation runbook](../../docs-engineering/oversqlite-validation.md)
for selection, prerequisites, limits and evidence reuse. These commands run only
when explicitly requested; infrastructure tests use controlled children.
