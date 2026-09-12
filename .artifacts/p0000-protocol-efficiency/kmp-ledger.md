# p0000 Oversqlite protocol efficiency — KMP ledger

## Current K0 authority and paired freeze — 2026-09-20

This section supersedes the earlier authorization and candidate identities below. The owner
accepted this exact paired authority and authorized K1-K5 implementation by directing completion of
K0 on 2026-09-20. It does not authorize qualification, independent review, implementation
acceptance, staging, commit, push, publication, deployment, or release.

### Accepted authority and repository identities

- Go p0000 SHA-256: `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`.
- SQLiteNow companion SHA-256 after K0 activation:
  `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`.
- Frozen Go repository: branch `ft-migration-support`, revision
  `3839514ef08186900fa72acc05e313760f725d68` (`Implement Oversqlite protocol simplification
  P0-P4`), clean tracked and untracked status before and after K0 verification.
- SQLiteNow implementation base: branch `ft-migration-support`, revision
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd` (`Add Oversqlite P0-P4 companion contract`), clean
  tracked and untracked status before K0. The expected post-K0 state is this ledger modified plus the
  ignored governing companion updated at the hash above; no unrelated or overlapping drift was
  present.
- Go OpenAPI SHA-256: `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- Corpus README SHA-256: `8b2680235351c86b9a675bbc442ac21580107096f06271c1fb91c640bd51a7fa`.
- Corpus manifest SHA-256: `09212ae18a90935352f93c30141a91397783b2260a7360e3c00b42c24b6080e1`.
- `sha256sum -c oversqlite-contracts/MANIFEST.sha256` passed for every listed corpus input.
- The Go repository is frozen read-only for K1-K5. Any Go revision, p0000, or OpenAPI change
  invalidates K0. K1 may update the corpus README, manifest, and fixture inputs only as required by
  the accepted specifications; those changes must remain semantically consistent with frozen p0000,
  land atomically, and be re-pinned in this ledger. Any semantic divergence requires an explicit
  reconciliation before implementation continues.

### Toolchain and execution environment

- Java: OpenJDK `21.0.10`; Gradle `9.5.0`; project Kotlin plugin `2.4.20` (Gradle reports embedded
  Kotlin `2.3.20`).
- Xcode `26.4` build `17E192`; Swift `6.3`.
- Node `v26.8.1`; npm `11.19.0`.
- Browser tooling: Google Chrome `153.0.8010.52`; Safari `26.6` with bundled `safaridriver`.
  Standalone `chromedriver` and `geckodriver` are absent; the repository browser lanes use the
  Gradle/Karma browser setup.
- Android: adb `37.0.1-15733141`; attached device `emulator-5554`
  (`sdk_gphone64_arm64`); SDK emulator `37.1.11.0` is available at
  `/Users/pochkin/Library/Android/sdk/emulator/emulator`. The earlier
  `/Users/pochkin/Library/Android/sdk/tools/emulator` selected by `PATH` is stale and must not be
  used to launch a new emulator.
- Docker client and server: `29.6.2`.

### Fast K0 contract verification

- `OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts GOTOOLCHAIN=go1.25.12 go test ./oversync -run '^(TestSchemaGenerationOpenAPIGolden|TestContractCorpus_ManifestPinnedRoot)$' -count=1` — exit 0.
- `./gradlew :library-oversqlite:jvmTest` with the eight focused OpenAPI/manifest/wire-fixture test
  classes and `--rerun-tasks` — exit 0; 15 tasks executed in 27 seconds.
- These are K0 authority/consumer checks only. Comprehensive, platform, Swift, real-server, heavy,
  and paired qualification lanes were intentionally not run.
- K0 result: `K0 COMPLETE; K1-K5 IMPLEMENTATION AUTHORIZED`.

## Authority and authorization

- Owner acceptance and implementation authorization were received on 2026-09-19 for the exact Go authority SHA-256 `3a3c40b92853da2a4653acd708e6765b301d57c8145317c604ff66e5593a445c` and companion SHA-256 `ed35c4558cbf21c0f33fc585f7add80bfa2592064124624aa812871882d84db4`.
- Direct hash verification at implementation start matched both accepted values; the companion pins the accepted Go authority hash.
- Initial repository identity: `ft-migration-support` at `35ce7982176b4388a9cbbcce86faded700c10c46`; tracked worktree status was clean.
- Paired initial Go identity: `ft-migration-support` at `89e59a688f484cb559f0701ca690c9ce64fb0b3c`; tracked worktree status was clean.

## Progress and invalidation

- K0 initial identity and authority checks: complete.
- K1 corpus/manifest freeze: complete on 2026-09-19. K1 admitted-contract runtime replacement and K2–K5 remain pending. Production changes invalidate affected focused tests and all final paired lanes.
- Drymint pre-edit baseline: `rb0:049173a851ff08c347355b4c` covering `library-oversqlite`, `oversqlite-contracts`, and `docs-engineering`.
- K1 rewrote the named `protocol-handshake`, `protocol-http`, `push`, `pull-snapshot`, and `watch` wire families in place; added complete compact, compression, request-count, fault, limit, and error-precedence families; and added a corpus reader that validates every manifest record before returning a path. Frozen complete corpus manifest SHA-256: `dc416ee09dcab9a3f75fbb9d4a2eeb06f60d5e20d307945b374196d9f006475b`.

## Toolchain evidence

- Java: OpenJDK 21.0.10; Gradle 9.5.0; Kotlin 2.3.20.
- Xcode 26.4; Swift 6.3; Docker 29.6.2.

## K1 / paired P0 evidence

- Frozen paired Go OpenAPI SHA-256: `93b984c9edf120fc3037f809a5249f59396470ca67353e3ea7cd6837f27d2e0b`.
- `OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts GOTOOLCHAIN=go1.25.12 go test ./oversync -run '^(TestSchemaGenerationOpenAPIGolden|TestContractCorpus_ManifestPinnedRoot)$' -count=1` — exit 0.
- `./gradlew :library-oversqlite:jvmTest --tests 'dev.goquick.sqlitenow.oversqlite.SharedContractFixtureManifestTest' --rerun-tasks` — exit 0, 24s, fresh execution; verifies the JVM consumer refuses an invalid corpus before fixture access.

## Superseding P0-P4 rereview remediation — 2026-09-20

This section supersedes the candidate hashes and focused evidence above for the rereview-remediation
candidate. Earlier entries remain historical. This is remediation evidence only: it does not claim
qualification, independent-review readiness, owner acceptance, K0 authorization, or delivery.

- Reproduced pre-edit Go identity: `ft-migration-support` at
  `89e59a688f484cb559f0701ca690c9ce64fb0b3c`; status SHA-256
  `c90d47865919c6e61509b39e6230b4578d9ddc138d631f8ae757874d4e8efc63`; tracked-diff SHA-256
  `e8ac4825a0f616ad8b1baecd27f9940586bc106e8aebf2d506678ce40334e045`; no staged changes.
- Reproduced pre-edit SQLiteNow identity: `ft-migration-support` at
  `35ce7982176b4388a9cbbcce86faded700c10c46`; status SHA-256
  `020a9040b0120464fb9f7df32ed80e28565994d4520e0bb14ffebd9ea0c9d605`; tracked-diff SHA-256
  `4777cda3a92e6e669ec09c082b44e8c5e5e7160db7f6f29afc5ef6fc275be0f3`; no staged changes.
- Drymint pre-edit baselines: Go `rb0:3782e853e58d98a9bfebd7df`; SQLiteNow
  `rb0:400222d5876909180ee50e73`.
- Remediated Go authority SHA-256:
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`.
- Remediated Go OpenAPI SHA-256:
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- Reconciled SQLiteNow companion SHA-256:
  `c34f747341d9bac54108f892bb93c87d1db22de307770a6ca13372743b9592f3`.
- Reconciled corpus README SHA-256:
  `39424ba0972efdaba998474d198317bc639c43f8952fb5dbee1cc5ceb923f467`.
- Regenerated sorted corpus manifest SHA-256:
  `dfe9a9802267c15b94d06ed00ad5aab8f3bac437f9be240f4c10186867cf0778`.
- Focused Go gate with Go 1.25.12 and explicit `OVERSQLITE_CONTRACTS_ROOT`: exit 0. It covered
  OpenAPI/documentation conformance, request-envelope presence/null rules, encoding/size precedence,
  gzip framing, push error mapping, all `TestContractCorpus_*` cases, and retained snapshot-session
  error mapping.
- Focused JVM gate with `--rerun-tasks`: exit 0 in 26s. It covered
  `GoSwaggerConformanceTest`, `SharedContractFixtureManifestTest`,
  `SharedWireContractFixtureTest`, `SharedPushFixtureTest`,
  `SharedProtocolHttpRequestFixtureTest`, and `SharedProtocolHandshakeFixtureTest`.

Not run by this fast remediation gate: complete Go package suites, `oversqliteComprehensive`,
platform lanes, live-server lanes, full paired qualification, fresh independent rereview, owner
acceptance, staging, commit, push, publication, deployment, and release. Those gates remain pending;
SQLiteNow K0 remains unauthorized until a fresh independent rereview returns ready.

## Final fast remediation closeout — 2026-09-20

This section corrects and supersedes the immediately preceding remediation identity/evidence
section. It is remediation evidence only, not qualification or K0 authorization.

- Exact reproduced pre-edit Go identity: branch `ft-migration-support`, HEAD
  `89e59a688f484cb559f0701ca690c9ce64fb0b3c`, status SHA-256
  `c90d47865919c6e61509b39e6230b4578d9ddc138d631f8ae757874d4e8efc63`, tracked binary-diff
  SHA-256 `6bb644a1b5a68acb8cf008d2298699012ebc5009795dada9d031f487d5e1f9c1`, cached-diff SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`, untracked-content manifest
  SHA-256 `8b968b66aebc2105f6ca82e332d058443b3e12c9f311b6c890cc72ee9d0e3e3e`.
- Exact reproduced pre-edit SQLiteNow identity: branch `ft-migration-support`, HEAD
  `35ce7982176b4388a9cbbcce86faded700c10c46`, status SHA-256
  `020a9040b0120464fb9f7df32ed80e28565994d4520e0bb14ffebd9ea0c9d605`, tracked binary-diff
  SHA-256 `b7b5541eb7b7b0a852dc4c2677994d916900a2196b8f69880447a0ce729d7a9a`, cached-diff SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`, untracked-content manifest
  SHA-256 `fa71c129338e150b518c8265772386843c710b3cc6f7f7325d80fb003a913d6d`.
- Final Go identity: the same branch and HEAD; status SHA-256
  `c90d47865919c6e61509b39e6230b4578d9ddc138d631f8ae757874d4e8efc63`; tracked binary-diff
  SHA-256 `5e5a7474a08f7c3c68169e5426c5ef97fb6a1776a1a8a6afdbaf2fb6fbcfe221`; cached-diff SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`; untracked-content manifest
  SHA-256 `3a26bd2b6503a68369f15c98c391d90ec56fafb90331901c57b5bab7cc146c76`; 240 status entries and
  7 untracked files.
- Final SQLiteNow branch/HEAD/status identity: the same branch and HEAD; status SHA-256
  `020a9040b0120464fb9f7df32ed80e28565994d4520e0bb14ffebd9ea0c9d605`; tracked binary-diff
  SHA-256 `34be2a6f91b725a34691904ec15ee31beb77024c0a3704ea0b128389048a3989`; cached-diff SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`; 24 status entries and
  11 untracked files. Its final untracked-content manifest is frozen in the external rereview request,
  avoiding a self-referential claim because this ledger is itself an untracked manifest member.

Authority stayed unchanged: Go specification SHA-256
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`, OpenAPI SHA-256
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`, and SQLiteNow companion
SHA-256 `c34f747341d9bac54108f892bb93c87d1db22de307770a6ca13372743b9592f3`. The executable-trace
corpus README is SHA-256 `8b2680235351c86b9a675bbc442ac21580107096f06271c1fb91c640bd51a7fa`; regenerated sorted
manifest SHA-256 `87d7007ee287fcf28d14aaa6482a632dbe217ac55b823f3a951c302b15ff2ba0`.

- `gofmt` on touched Go files and `git diff --check` in both repositories — exit 0.
- Focused Go 1.25.12 gate with explicit `OVERSQLITE_CONTRACTS_ROOT` — exit 0 in 2.41s. It covered every `TestContractCorpus_*`, both manifest failure suites, scalar query defaults/rejections, strict envelopes/content encoding/gzip limits, push error mapping, OpenAPI, and stale documentation.
- Requested eight-class JVM `--rerun-tasks` gate — exit 0 in 27.21s with 15 tasks executed. It included the previously omitted pull/snapshot and watch fixture classes.
- Drymint pre-edit baselines: Go `rb0:5b0fde70e6edb0e62d07cc3d`; SQLiteNow `rb0:ff12cdd6e62389416afd49bb`. Final baseline-attributed reviews after the emitted one-shot index refreshes: Go `rv0:9e69df6700f0be2da3497332`, SQLiteNow `rv0:fa704f3404efa8951fc45eb2`. Source inspection found no actionable duplicate or dependency issue.

Not run: complete Go package suites, comprehensive KMP suites, platform lanes, live-server lanes,
paired qualification, owner acceptance, staging, commit, push, publication, deployment, and release.
SQLiteNow K0 remains unauthorized until a fresh independent rereview returns
`READY FOR SQLITENOW K0`.

## K1-K5 implementation closeout — 2026-09-20

This section supersedes prior progress statements for the current owner-authorized K1-K5
implementation. It records author implementation evidence only. Section 11 paired qualification,
independent review, owner acceptance, staging, commit, push, publication, deployment, and release
remain separate and were not claimed or performed.

### Frozen authority and final implementation identity

- Frozen read-only Go authority: branch `ft-migration-support`, revision
  `3839514ef08186900fa72acc05e313760f725d68`, clean status, p0000 SHA-256
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`, and OpenAPI SHA-256
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- SQLiteNow base identity remains branch `ft-migration-support`, HEAD
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`. The ignored governing companion remains unchanged at
  SHA-256 `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`.
- Final Git status has 140 entries and SHA-256
  `f1b513960f08e31035b89e073f5355f38f9120a624be510c5286b21c21030c58`. The unstaged tracked
  production diff, excluding this ledger, has SHA-256
  `0c32894613d16a8f7adcb96efa4d5053d4482134dd7a1bf2a91075f02dea96a6`. The staged diff is empty
  with SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- The 12 untracked implementation files have sorted content-manifest SHA-256
  `de9fbf99b655ce715cd6fe014314bbf14f86d28bab810f7c4da0d6663388ae17`:
  `OversqliteGzip.android.kt`, `OversqliteCompactCodec.kt`, `OversqliteDiscoveryValidation.kt`,
  `OversqliteGzip.kt`, `OversqliteStrictJson.kt`, `GenerationDiscoveryValidationTest.kt`,
  `OversqliteGzip.jvm.kt`, `JvmStatementCleanupTestSupport.kt`, `SnapshotMemoryTestSupport.kt`,
  `OversqliteGzip.native.kt`, `OversqliteGzip.web.kt`, and
  `RealServerDirectPushSelectionTest.kt`, at their paths shown by `git status --short`.
- Changed-path inventory by top-level owner is: `.artifacts` 1, `README.md` 1, `docs` 5,
  `docs-engineering` 2, `library-core` 1, `library-oversqlite` 88,
  `oversqlite-contracts` 5, `platform-oversqlite-test` 26, `sqlitenow-compiler` 4, and `swift` 7.
  This is 127 tracked production paths, this ledger, and the 12 untracked paths above. The status
  hash pins the exact path/status inventory without treating later qualification output as source.
- Final corpus identities: `README.md` SHA-256
  `7a0a383da631020ad5aa0a4e295fcae597fcfe74cdaded778a62e87cb57abf7b`,
  `CLASSIFICATIONS.json` SHA-256
  `4adc97f140b67f33991eb1df7cb31ead480698fe042b9c95ed1b6f0f6f4d9487`, and sorted
  `MANIFEST.sha256` SHA-256
  `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f` with 76 records.

### K1-K5 implementation result

- K1: verified and atomically updated the existing corpus rather than recreating it; readers fail
  closed on absent, extra, stale, or mismatched entries. One immutable discovery contract is
  admitted during opening, discovery and connect complete before `Ready`, and ordinary operations
  have no discovery or capabilities preflight.
- K2: implemented the common strict compact table/group/tuple codec and semantic-row expansion
  before hashing, conflict processing, and application. Strict JSON and malformed tuple cases fail
  before mutation. `OversqliteTransferTargets` directly replaces the four old properties across
  Kotlin, generated/exported Swift, samples, and API tests. Push selection is jointly row/decoded-
  byte bounded, with the exact one-row local soft-target exception and terminal server-hard-limit
  behavior before source-ID consumption.
- K3: replaced session/chunk/commit/committed-row push with one strict `POST /sync/push`; replaced
  the outbox with only `none`/`prepared`; and made old receipt-bearing layouts fail closed before
  application access. Validated push effects apply atomically before the outbox clears. Focused
  coverage includes response loss, process death, exact and initialization replay, conflict,
  rejection, retirement, mismatch, and newer-edit protection. Pull and snapshot now decode compact
  groups while retaining recovery/atomicity; snapshot logical bytes and exact one-row decoded-body
  retry are separate from HTTP body accounting.
- K4: implemented the exact 2,048 decoded-byte identity/gzip rule on JVM, Android, native Apple,
  JS, and Wasm: identity below the threshold; at or above it gzip only when strictly smaller. Raw
  transports enforce encoded and decoded ceilings plus gzip-member rules. Fetch-managed targets do
  not set forbidden `Accept-Encoding` or claim encoded-byte inspection. SSE remains identity and
  accepts only the bundle-sequence event. Watch scheduling uses a conflated high-water worker with
  structured cancellation and polling fallback.
- K5: updated public models, clients, coordinators, errors, generated products, Swift wrappers,
  samples, and durable documentation. Obsolete active vocabulary and paths were removed, source was
  reviewed, focused development evidence is stable, and production changes are frozen.

### Focused author-development evidence

Every Gradle command used to claim fresh execution included `--rerun-tasks`. These are development
results, not section 11 qualification:

- Full `:library-oversqlite:jvmTest` against the frozen Go repository: exit 0; 241 tests; 15 tasks;
  2m33s. A preceding 105-test protocol-focused selection also passed freshly.
- JVM platform parity plus Android device-test compilation and iOS-simulator, JS, and Wasm test
  compilation: exit 0; 134 tasks; 1m19s. `YARN_IGNORE_ENGINES=1` was needed because the project-
  managed Node 22.0 is below webpack-dev-server 6's Node 22.15 minimum; qualification must use the
  host Node 26.8.1 or another supported configured Node rather than waiving the engine check.
- `:swift:runtime:sync:swiftTest`: exit 0; 60 tasks; 5 tests; 1m16s. `swift test --package-path
  swift/support`: exit 0; 4 tests, with one environment-dependent live-authority test skipped.
- `:sqlitenow-compiler:test`: exit 0; fresh execution; 53s.
- `:sqlitenow-gradle-plugin:test`: exit 0; fresh and separate from compiler tests; 3m02s.
- Fresh sample compilation passed for desktop, Android main, JS, Wasm, and iOS simulator; 54 tasks.
- Fresh common production compilation passed for JVM, JS, Wasm, iOS simulator, plus Android device
  test compilation. The final vocabulary-only change was checked by fresh protocol validation plus
  Android device-test compilation; 50 tasks; 26s.
- Final Drymint supported-source review `rv0:ed244a976528002e78baf871` returned clean. The only
  reported low-substance overlap was the intentional lifecycle forwarder between generation-opening
  admission and attach. A separately inspected pair of boundary tests was retained because the
  cases prove opposite server-hard and local-soft one-row outcomes.

### Residual legacy classification

The final literal scan covers source, tests, generated resources, samples, durable docs, and the
corpus. Every remaining relevant hit is one of the allowed classes:

- explicit absence assertions in `SharedProtocolHandshakeFixtureTest`,
  `SharedProtocolHttpRequestFixtureTest`, `SharedWireContractFixtureTest`,
  `GoSwaggerConformanceTest`, `SchemaEvolutionLiveAuthorityJvmTest`, and the Swift emitter test;
- explicit obsolete-layout rejection input in `GenerationRetainedObjectsTest`;
- opaque historical worker-migration input in `SqliteWorkerLegacyMigrationTest`, annotated so core
  migration cannot be mistaken for an Oversqlite compatibility path;
- manifest-pinned historical fixture inputs named by `CLASSIFICATIONS.json`, including old runtime
  state/local schema, push/pull/watch behavior, canonical JSON, and client-upgrade exchanges; or
- unrelated browser platform capabilities, SwiftPM plugin capability declarations, snapshot
  chunks, remote-replacement state names, and current `maxSnapshotChunk*` transfer-target names.

Active corpus mentions of removed endpoints/operation names are explicit forbidden-request or
removed-operation assertions. No active compatibility alias, fallback, dual codec, dual reader,
old-database migration, or parallel legacy path remains.

### Qualification matrix prepared but not executed

Use raw logs under
`.artifacts/p0000-protocol-efficiency/qualification-20260920/`. Before every lane, reproduce the
frozen identities above, ensure the corpus manifest validates, and record start/end UTC time, exit
status, tool versions, raw log path, and unchanged identities.

| Lane | Exact command | Budget | Raw log |
| --- | --- | ---: | --- |
| Cross-repository conformance | `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance` | 10m | `cross-repo-conformance.log` |
| Comprehensive host | `./gradlew oversqliteComprehensive --rerun-tasks` | 30m | `comprehensive.log` |
| Generated production outputs | `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` | 20m | `generated-production.log` |
| Gradle plugin | `./gradlew :sqlitenow-gradle-plugin:test --rerun-tasks` | 15m | `gradle-plugin.log` |
| Aggregate platforms | `./gradlew oversqlitePlatformAll --rerun-tasks` | 60m | `platform-all.log` |
| Swift hybrid/export | `./gradlew swiftHybridSupportGate --rerun-tasks` | 45m | `swift-hybrid.log` |
| Real-server aggregate | `./gradlew oversqliteRealserverAll --rerun-tasks` | 60m | `realserver-all.log` |
| Swift real server | `./gradlew swiftHybridSupportRealserverGate --rerun-tasks` | 45m | `swift-realserver.log` |
| Heavy boundedness/stress | `./gradlew oversqliteRealserverAllHeavy --rerun-tasks` | 120m | `realserver-heavy.log` |

Prerequisites are Java 21/Gradle/Kotlin from the wrapper, Go 1.25.12, the exact read-only Go
revision and hashes above, a fresh supported PostgreSQL database and recorded image, recorded Go
server base URLs/logs, Docker, Xcode 26.4/Swift 6.3, Android device or emulator, Chrome and Safari,
and configured Node >=22.15 (host Node 26.8.1 is suitable). The real-server lanes start the frozen
Go server against the fresh database and record server/database identity before clients run.

Invalidation rules: a Go revision/spec/OpenAPI or active-corpus semantic change invalidates all
lanes; a Kotlin/Swift production change invalidates its focused target lanes plus comprehensive,
affected platform/Swift lanes, and paired lanes; codec/persistence changes invalidate every
consumer and real-server lane; transport/watch changes invalidate platform and real-server lanes;
generator/template changes invalidate generated-output, compiler, plugin, Swift, and affected
sample lanes; test-only changes invalidate their owning lanes; docs-only changes require the
residual search/hash/document review; ledger-only changes do not invalidate code execution but do
require refreshed identity output. An aggregate timeout is inconclusive and must be isolated by
family rather than retried unchanged.

### Paste-ready separate qualification request

```text
Use GPT-5.6 Terra with high reasoning. Qualify the frozen SQLiteNow Oversqlite K1-K5
implementation in /Users/pochkin/Projects/my/sqlitenow-kmp against the read-only Go authority in
/Users/pochkin/Projects/my/go-oversync.

Read completely before acting:
- /Users/pochkin/Projects/my/sqlitenow-kmp/AGENTS.md
- /Users/pochkin/Projects/my/sqlitenow-kmp/specs/oversqlite-protocol-simplification-and-efficiency.md
- /Users/pochkin/Projects/my/go-oversync/specs/p0000-oversqlite-protocol-simplification-and-efficiency.md
- /Users/pochkin/Projects/my/sqlitenow-kmp/.artifacts/p0000-protocol-efficiency/kmp-ledger.md
- /Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts/README.md
- /Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts/MANIFEST.sha256
- /Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts/CLASSIFICATIONS.json

Before testing, reproduce every frozen identity and SHA-256 in the ledger. Stop on drift. Keep the
Go repository read-only. Do not edit production or test source during qualification; if a failure
requires implementation work, record the exact blocker and stop for a separate remediation gate.
Do not stage, commit, push, publish, deploy, release, rewrite history, or mutate registries.

Execute section 11 using the exact qualification matrix, prerequisites, budgets, log locations,
and invalidation rules in the ledger. Use --rerun-tasks wherever specified. Start the exact frozen
Go server against a fresh supported PostgreSQL database for real-server lanes. Capture the exact
request-count traces, gzip/identity byte artifacts, bounded-memory evidence, snapshot logical-byte
and one-row retry evidence, watch-burst evidence, source-ID non-consumption boundary, and residual
legacy searches required by section 11. A host pass does not replace platform or live-server
evidence; UP-TO-DATE is not fresh execution evidence; a skipped mandatory lane is not a pass.

Update only the qualification portion of the ledger with commands, UTC times, exits, raw log paths,
tool/server/database/browser/device identities, evidence classifications, and final unchanged
candidate hashes. End with exactly one qualification verdict: QUALIFICATION PASSED or
QUALIFICATION BLOCKED. Do not claim independent review, owner acceptance, or delivery.
```

Implementation author verdict: K1-K5 are implemented and production changes are frozen for the
separate paired qualification campaign.

## Qualification attempt — 2026-09-21

This is qualification evidence only. The campaign stopped at the first mandatory lane because its
failure requires source or test-contract remediation. No production source, test source, fixture,
generated product, specification, corpus input, Go authority, staging, commit, push, publication,
deployment, release, or package registry was changed by this attempt.

### Frozen identity check before qualification artifacts

- Go was branch `ft-migration-support` at
  `3839514ef08186900fa72acc05e313760f725d68`, with zero porcelain-status lines. Its p0000 and
  OpenAPI SHA-256 values matched the frozen values:
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f` and
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- SQLiteNow was branch `ft-migration-support` at
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`. Before the authorized qualification logs were
  created, its porcelain-status SHA-256 was
  `f1b513960f08e31035b89e073f5355f38f9120a624be510c5286b21c21030c58`; its tracked production
  diff excluding this ledger was
  `0c32894613d16a8f7adcb96efa4d5053d4482134dd7a1bf2a91075f02dea96a6`; the twelve-file untracked
  implementation manifest was
  `de9fbf99b655ce715cd6fe014314bbf14f86d28bab810f7c4da0d6663388ae17`; this ledger was
  `0efa966e0cf4149ca869a3198de2ba901692a8ba442892c74a6ef56f2e537dd8`; and the ignored companion
  was `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`.
- The corpus README, classifications, and manifest-file SHA-256 values matched
  `7a0a383da631020ad5aa0a4e295fcae597fcfe74cdaded778a62e87cb57abf7b`,
  `4adc97f140b67f33991eb1df7cb31ead480698fe042b9c95ed1b6f0f6f4d9487`, and
  `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`; all 76 manifest records
  verified.

### Lane record

| Lane | Required | Command | UTC | Result | Raw evidence |
| --- | --- | --- | --- | --- | --- |
| Cross-repository conformance | yes | `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance` | 2026-09-20T21:12:03Z–2026-09-20T21:13:20Z | **failed**, exit 1, 77s | `qualification-20260921/cross-repo-conformance.log` |
| Comprehensive host | yes | `./gradlew oversqliteComprehensive --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |
| Generated production verification | yes | `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |
| Gradle plugin tests | yes | `./gradlew :sqlitenow-gradle-plugin:test --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |
| Aggregate platform qualification | yes | `./gradlew oversqlitePlatformAll --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |
| Swift hybrid/export qualification | yes | `./gradlew swiftHybridSupportGate --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |
| Real-server aggregate | yes | `./gradlew oversqliteRealserverAll --rerun-tasks` | not started | blocked by the failed required cross-repository lane; no Go server or PostgreSQL database was started | not created |
| Swift real-server gate | yes | `./gradlew swiftHybridSupportRealserverGate --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |
| Heavy boundedness and stress | yes | `./gradlew oversqliteRealserverAllHeavy --rerun-tasks` | not started | blocked by the failed required cross-repository lane | not created |

The cross-repository lane used Node `v26.8.1` and OpenJDK `21.0.10`. It executed the child JVM
conformance task with `--rerun-tasks`. Three `GoSwaggerConformanceTest` cases passed, but
`defaultableQueriesAndNestedReplacementShapeMatchTheWireContract` failed at
`GoSwaggerConformanceTest.kt:167`: `Expected value to be true.` The failing assertion requires the
extracted `SnapshotSessionCreateRequest.source_replacement` OpenAPI block to contain
`additionalProperties: false`. The frozen Go OpenAPI is still at the pinned hash and shows that
property at `swagger/two_way_sync.yaml:456-463`; resolving why the candidate test's extraction
does not satisfy its assertion needs a separate remediation session, not additional qualification
lanes.

### Post-failure preservation check

- The SQLiteNow tracked production diff excluding this ledger remained
  `0c32894613d16a8f7adcb96efa4d5053d4482134dd7a1bf2a91075f02dea96a6`; the twelve-file untracked
  implementation manifest remained
  `de9fbf99b655ce715cd6fe014314bbf14f86d28bab810f7c4da0d6663388ae17`.
- The corpus manifest still verified. Go remained clean at the frozen revision and retained both
  pinned p0000 and OpenAPI SHA-256 values. Qualification logs are new evidence artifacts and are
  intentionally excluded from the frozen implementation manifest; the ledger now requires its own
  refreshed final hash.

### Test-only remediation — 2026-09-20

The qualification blocker was accepted for a separate, narrow remediation. The JVM-only
`GoSwaggerConformanceTest` no longer scrapes YAML blocks with indentation-sensitive regular
expressions. It parses the frozen Go OpenAPI with SnakeYAML's safe constructor (duplicate mapping
keys disabled) and asserts the same protocol properties from parsed maps. The test dependency
`org.yaml:snakeyaml:2.7` is confined to `library-oversqlite` `jvmTest`.

- The first remediation execution, recorded at
  `qualification-20260921/cross-repo-conformance-remediation.log`, failed during compilation only:
  an unescaped Kotlin `"$ref"` map key at `GoSwaggerConformanceTest.kt:166` produced
  `UNRESOLVED_REFERENCE`. It did not execute conformance assertions and is superseded.
- The corrected owning lane, recorded at
  `qualification-20260921/cross-repo-conformance-remediation-rerun.log`, ran
  `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance`
  from 2026-09-20T22:28:14Z through 2026-09-20T22:29:18Z: exit 0, 64 seconds, 15 child JVM tasks
  executed. The frozen Go revision stayed clean and at
  `3839514ef08186900fa72acc05e313760f725d68`; p0000 and OpenAPI remained
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f` and
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- `git diff --check` passed for the remediation paths. The corpus manifest and ignored companion
  remain unchanged. The updated tracked production diff excluding this ledger is
  `8c682941c7e8eef657b85214869f155dc8c31fd9433eb74916a0578700ec220e`.

This test-only change supersedes the failed initial cross-repository lane and invalidates its prior
candidate identity. A fresh candidate baseline and the remaining mandatory qualification campaign
are required before qualification can pass.

## Refrozen remediation candidate — 2026-09-22

The user authorized a new paired freeze after the test-only remediation. This baseline supersedes
the prior implementation identity only for the renewed qualification campaign; historical failed
and superseded evidence remains retained above.

- Go authority: branch `ft-migration-support`, HEAD
  `3839514ef08186900fa72acc05e313760f725d68`, clean porcelain status, p0000 SHA-256
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`, and OpenAPI SHA-256
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- SQLiteNow: branch `ft-migration-support`, HEAD
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; pre-ledger current-status SHA-256
  `39b67e960b9495289d71f9267c41b98cacd91dc9cd573fea44121c3770ec1ff3`; tracked production diff
  excluding this ledger SHA-256 `8c682941c7e8eef657b85214869f155dc8c31fd9433eb74916a0578700ec220e`;
  and the unchanged twelve-file implementation manifest SHA-256
  `de9fbf99b655ce715cd6fe014314bbf14f86d28bab810f7c4da0d6663388ae17`.
- Ignored companion SHA-256 remains
  `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`. Corpus README,
  classifications, and manifest-file SHA-256 values remain
  `7a0a383da631020ad5aa0a4e295fcae597fcfe74cdaded778a62e87cb57abf7b`,
  `4adc97f140b67f33991eb1df7cb31ead480698fe042b9c95ed1b6f0f6f4d9487`, and
  `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`; all 76 records verify.
- Environment: OpenJDK `21.0.10`, Node `v26.8.1`, Xcode `26.4` build `17E192`, Swift `6.3`, and
  Docker client/server `29.6.2`. `adb devices -l` reported no attached device at freeze time.

The corrected cross-repository rerun is superseded by this explicit fresh freeze and will be rerun
as lane 1 before any later lane. Qualification logs remain under `qualification-20260921/` as
requested; they are evidence artifacts, not implementation-manifest inputs.

### Refrozen lane record

| Lane | Required | Command | UTC | Result | Raw evidence |
| --- | --- | --- | --- | --- | --- |
| Cross-repository conformance | yes | `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance` | 2026-09-22T14:43:43Z–2026-09-22T14:45:03Z | passed, exit 0, 80s | `qualification-20260921/cross-repo-conformance-refrozen.log` |
| Comprehensive host | yes | `./gradlew oversqliteComprehensive --rerun-tasks` | 2026-09-22T14:45:21Z–2026-09-22T14:48:35Z | passed, exit 0, 194s | `qualification-20260921/comprehensive-refrozen.log` |
| Generated production verification | yes | `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` | 2026-09-22T14:49:00Z–2026-09-22T14:49:33Z | failed, exit 1 before verification: Kotlin-managed Node 22.0.0 violates `webpack-dev-server@6.0.0`'s `>=22.15.0` engine | `qualification-20260921/generated-production-refrozen.log` |

The first generated-production command was not retried unchanged. A qualification-only system-Node
override used the host Node `v26.8.1` with Yarn engine checks still enabled. Its first command-line
form reached the outer `kotlinNpmInstall`, but `refreshGeneratedProductionOutputs` starts a nested
Gradle process and did not inherit that command-line init script; this is recorded in
`qualification-20260921/generated-production-refrozen-system-node.log`. A temporary, path-scoped
Gradle init hook was then used so the nested process also used system Node; it cleared the Node
engine failure, as shown by both `kotlinNodeJsSetup` tasks being skipped. The hook was removed after
the attempt.

That changed-condition execution failed independently at `:kotlinStoreYarnLock`: its integrity
guard reported `Lock file was changed. Run the kotlinUpgradeYarnLock task to actualize lock file`.
The outer task consequently failed at `:refreshGeneratedProductionOutputs`, exit 1, from
2026-09-22T14:56:15Z through 2026-09-22T14:57:58Z. Evidence is
`qualification-20260921/generated-production-refrozen-system-node-propagated.log`. Updating the
checked-in Yarn lock would mutate the frozen candidate and is not authorized during qualification.
No lock-file update, engine-check bypass, production/test source edit, Go change, staging, commit,
publication, or registry change was performed.

The tracked production diff excluding this ledger remains the refrozen
`8c682941c7e8eef657b85214869f155dc8c31fd9433eb74916a0578700ec220e` after all lane executions.
The next required lane and every later lane are blocked by the mandatory generated-production
failure. A separate remediation/refreeze decision is required before resuming.

## Generated-output remediation — 2026-09-22

The user authorized the blocked Node/Yarn remediation. The Kotlin/JS Node baseline is now pinned
to `22.15.0` at both the root and every target project, so nested Gradle builds and target-owned
webpack tasks use the same supported Node release. `kotlinUpgradeYarnLock --rerun-tasks` completed
successfully from 2026-09-22T15:13:08Z through 2026-09-22T15:13:41Z (exit 0, 33 seconds), updating
the checked-in `kotlin-js-store/yarn.lock` for the resolved `webpack-dev-server@6.0.0` graph.

- Focused confirmation: `:sample-kmp:composeApp:jsBrowserProductionWebpack --rerun-tasks` passed
  from 2026-09-22T15:30:40Z through 2026-09-22T15:32:23Z (exit 0, 103 seconds). Its log confirms
  Node `22.15.0` launches both the Kotlin-managed Yarn process and webpack.
- Required generated-output rerun: `./gradlew verifyGeneratedProductionOutputs --rerun-tasks`
  passed from 2026-09-22T15:32:35Z through 2026-09-22T15:42:50Z (exit 0, 615 seconds), including
  JS and Wasm production bundles for both samples and the final generated-state guards. The bundle
  size messages are webpack performance warnings, not task failures.
- Earlier post-remediation executions that ended with exit 143 were manually interrupted while an
  orphan Kotlin compiler daemon from a previous interruption still consumed the compiler queue;
  they are inconclusive and superseded by the successful focused and exact aggregate reruns.
- `git diff --check -- build.gradle.kts kotlin-js-store/yarn.lock` passed. This remediation changes
  the candidate identity, so none of the prior refrozen qualification lanes can carry forward.

The candidate must be frozen again (with paired Go identity rechecked) before a fresh mandatory
qualification campaign may resume at cross-repository conformance. No staging, commit, Go mutation,
publication, or engine-check bypass was performed.

## Remediation candidate refreeze — 2026-09-22

The user authorized a new paired freeze and qualification restart after the generated-output
remediation. The authoritative baseline is
`qualification-20260921/remediation-refreeze-baseline-rerun.log` (2026-09-22T15:47:10Z–15:47:11Z,
exit 0). Its predecessor `remediation-refreeze-baseline.log` is retained but invalid for the
untracked-manifest field because a shell `path` variable collision hid `shasum`; it is not used as
baseline evidence.

- SQLiteNow: branch `ft-migration-support`, HEAD
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; candidate status SHA-256 excluding evidence
  artifacts `bed97f9c0c5377008b75a2d1329d68d5e9a5d1fcec9548f6a9a35517e9dd1d26`; tracked diff
  excluding evidence artifacts `33ff1d4b6940a61acb6c9dcbd3e132762af2b1b297a091102ffed0a6a4cd6758`;
  staged diff empty; and twelve-file untracked implementation manifest
  `de9fbf99b655ce715cd6fe014314bbf14f86d28bab810f7c4da0d6663388ae17`.
- Read-only Go authority: branch `ft-migration-support`, clean HEAD
  `3839514ef08186900fa72acc05e313760f725d68`; p0000 SHA-256
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`; OpenAPI SHA-256
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
- Ignored SQLiteNow companion SHA-256 is
  `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`. Corpus README,
  classifications, and manifest SHA-256 values are
  `7a0a383da631020ad5aa0a4e295fcae597fcfe74cdaded778a62e87cb57abf7b`,
  `4adc97f140b67f33991eb1df7cb31ead480698fe042b9c95ed1b6f0f6f4d9487`, and
  `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`; all 76 records verify.

All prior qualification lanes remain superseded by this candidate identity. The restarted campaign
begins at cross-repository conformance and fails closed on the first required blocker.

### Restarted lane record

| Lane | Required | Command | UTC | Result | Raw evidence |
| --- | --- | --- | --- | --- | --- |
| Cross-repository conformance | yes | `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance` | 2026-09-22T15:47:39Z–2026-09-22T15:48:35Z | passed, exit 0, 56s; paired identity and corpus rechecked unchanged afterward | `qualification-20260921/cross-repo-conformance-remediation-refreeze.log` |
| Comprehensive host | yes | `./gradlew oversqliteComprehensive --rerun-tasks` | 2026-09-22T15:49:21Z–2026-09-22T15:52:08Z | passed, exit 0, 167s; paired identity and corpus rechecked unchanged afterward | `qualification-20260921/comprehensive-remediation-refreeze.log` |
| Generated production verification | yes | `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` | 2026-09-22T15:52:58Z–2026-09-22T15:56:19Z | **failed**, exit 1, 201s: Kotlin/Wasm compiler OOM (`GC overhead limit exceeded`) at `:sample-kmp:composeApp:compileProductionExecutableKotlinWasmJs`; candidate, Go authority, and corpus rechecked unchanged afterward | `qualification-20260921/generated-production-remediation-refreeze.log` |
| Gradle plugin tests | yes | `./gradlew :sqlitenow-gradle-plugin:test --rerun-tasks` | not started | blocked by required generated-production OOM | not created |
| Aggregate platform qualification | yes | `./gradlew oversqlitePlatformAll --rerun-tasks` | not started | blocked by required generated-production OOM | not created |
| Swift hybrid/export qualification | yes | `./gradlew swiftHybridSupportGate --rerun-tasks` | not started | blocked by required generated-production OOM | not created |
| Real-server aggregate | yes | `./gradlew oversqliteRealserverAll --rerun-tasks` | not started | blocked by required generated-production OOM | not created |
| Swift real-server gate | yes | `./gradlew swiftHybridSupportRealserverGate --rerun-tasks` | not started | blocked by required generated-production OOM | not created |
| Heavy boundedness and stress | yes | `./gradlew oversqliteRealserverAllHeavy --rerun-tasks` | not started | blocked by required generated-production OOM | not created |

The exact generated-output command exhausted Kotlin/Wasm compiler memory. The compiler reports the
configured Kotlin daemon heap was insufficient and recommends increasing `kotlin.daemon.jvmargs`.
No retry or memory-setting change was made within this campaign. The candidate, read-only Go HEAD,
p0000/OpenAPI hashes, twelve-file untracked implementation manifest, and all 76 corpus records
matched the refreeze baseline after the failure. A separate execution-environment or build-settings
remediation decision is required before the campaign can be refrozen and restarted.

## Kotlin/Wasm memory remediation and restarted qualification — 2026-09-22

The user authorized a separate build-settings remediation after the prior generated-production
blocker. The active Kotlin 2.4.20 compiler daemon was capped at 3 GiB while the host has 32 GiB
and the Gradle daemon already has a 4 GiB cap. `gradle.properties` now sets
`kotlin.daemon.jvmargs=-Xmx4g`, matching the Gradle daemon. No compiler execution mode, engine
check, source contract, Go authority, staging, commit, publication, or registry setting changed.

- Remediation validation: `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` passed in
  4m01s. It reran both samples' JS and Wasm production bundles and the final generated-state
  guards, including the exact `compileProductionExecutableKotlinWasmJs` task that previously
  failed with `GC overhead limit exceeded`.
- New source-bearing baseline after remediation: SQLiteNow HEAD
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; tracked diff excluding evidence artifacts
  `073b7c5bd99b06157656eb2c55cfc0d5268d9298a71e790adb25912fe2dadbcd`; twelve-file untracked
  implementation manifest `de9fbf99b655ce715cd6fe014314bbf14f86d28bab810f7c4da0d6663388ae17`;
  ignored companion `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`.
  Generated ignored build output is deliberately excluded because validation recreates it.
- Read-only Go authority remained clean at `3839514ef08186900fa72acc05e313760f725d68`; p0000
  and OpenAPI SHA-256 values remained
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f` and
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.
  All 76 corpus records and the pinned corpus hashes verified before and after the stopped lane.

### Restarted lane record

| Lane | Required | Command | UTC | Result | Raw evidence |
| --- | --- | --- | --- | --- | --- |
| Cross-repository conformance | yes | `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance` | 2026-09-22T16:07:24Z–2026-09-22T16:08:16Z | passed, exit 0, 52s | `qualification-20260921/cross-repo-conformance-memory-remediation-refreeze.log` |
| Comprehensive host | yes | `./gradlew oversqliteComprehensive --rerun-tasks` | 2026-09-22T16:09:05Z–2026-09-22T16:11:37Z | passed, exit 0, 152s | `qualification-20260921/comprehensive-memory-remediation-refreeze.log` |
| Generated production verification | yes | `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` | 2026-09-22T16:11:49Z–2026-09-22T16:15:45Z | passed, exit 0, 236s | `qualification-20260921/generated-production-memory-remediation-refreeze.log` |
| Gradle plugin tests | yes | `./gradlew :sqlitenow-gradle-plugin:test --rerun-tasks` | 2026-09-22T16:15:58Z–2026-09-22T16:18:52Z | passed, exit 0, 174s | `qualification-20260921/gradle-plugin-memory-remediation-refreeze.log` |
| Aggregate platform qualification | yes | `./gradlew oversqlitePlatformAll --rerun-tasks` | 2026-09-22T16:19:03Z–2026-09-22T16:20:20Z | failed, exit 1: no Android device | `qualification-20260921/platform-all-memory-remediation-refreeze.log` |
| Aggregate platform qualification, changed environment | yes | `./gradlew oversqlitePlatformAll --rerun-tasks` | 2026-09-22T16:23:00Z–2026-09-22T16:25:12Z | failed, exit 1: two non-assumption Android test failures on booted local `Pixel_4` API 31 emulator | `qualification-20260921/platform-all-memory-remediation-refreeze-emulator.log` |
| Swift hybrid/export qualification | yes | `./gradlew swiftHybridSupportGate --rerun-tasks` | not started | blocked by required platform failure | not created |
| Real-server aggregate | yes | `./gradlew oversqliteRealserverAll --rerun-tasks` | not started | blocked by required platform failure | not created |
| Swift real-server gate | yes | `./gradlew swiftHybridSupportRealserverGate --rerun-tasks` | not started | blocked by required platform failure | not created |
| Heavy boundedness and stress | yes | `./gradlew oversqliteRealserverAllHeavy --rerun-tasks` | not started | blocked by required platform failure | not created |

The initial platform execution correctly stopped at `connectedAndroidDeviceTest` because no
device was attached. The current SDK emulator launcher booted local `Pixel_4` API 31 successfully,
so the aggregate was rerun only under that changed environment. It then reached device execution
and exposed two non-assumption failures: `AndroidMockWebServerParityTest`
`generatedHelper_pullToStableReEmitsGeneratedReactiveQuery` rejects fixture column `email` as
unknown in `OversqliteCompactCodec`, and `PlatformParityTest`
`chunkedPushPullAndHydrate_convergeAcrossClients` fails the `server.pushRequestCount >= 3`
assertion. The remaining 33 Android report entries are expected disabled-realserver assumptions,
not the aggregate's cause. Candidate, Go, ignored companion, and corpus identities matched after
the failure. No retry or patch was made within this campaign.

QUALIFICATION BLOCKED

## Android platform fixture and bounded-push remediation — 2026-09-22

The user authorized a narrow source/fixture remediation for the two non-assumption Android
failures in the preceding platform lane. No Go source, contract input, staging, commit,
publication, or registry state changed.

- `AndroidMockWebServerParityTest.generatedHelper_pullToStableReEmitsGeneratedReactiveQuery` now
  uses compact table index `7` for `users` in the eight-table production discovery fixture.
  Index `0` denotes `categories`, whose schema legitimately rejects the `email` field.
- `PlatformParityTest.chunkedPushPullAndHydrate_convergeAcrossClients` now invokes
  `pushPending()` three times for six dirty rows with `maxPushRows = 2`, asserts three direct
  pushes, and expects bundle sequence 3. `pushPending()` freezes and commits one bounded outbox
  slice per call; it does not drain all slices in one invocation.
- Drymint prewrite baseline `rb0:a1971732cfbd72360061760b` was rebased after the required local
  index refresh. Its sole duplicate advisory compared this bounded single-writer batch/snapshot
  scenario to the distinct three-writer convergence test; source inspection confirmed they must
  remain separate.
- New source-bearing candidate identity: SQLiteNow HEAD
  `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; tracked diff excluding evidence artifacts
  `e139c7af2332ca99f48887ca01be878c9ae61298566ce260baeb4a27c262ea46`.
  The read-only Go authority remained clean at
  `3839514ef08186900fa72acc05e313760f725d68`; the Go p0000, OpenAPI, ignored Kotlin
  companion, and corpus-manifest hashes remained
  `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
  `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`,
  `02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`, and
  `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.

| Lane | Command | Result | Raw evidence |
| --- | --- | --- | --- |
| Android generated fixture | `:platform-oversqlite-test:composeApp:connectedAndroidTest` filtered to `AndroidMockWebServerParityTest#generatedHelper_pullToStableReEmitsGeneratedReactiveQuery` | passed, exit 0, on Pixel_4 API 31 | Gradle console record, 2026-09-22T16:43Z |
| Android bounded push | `:platform-oversqlite-test:composeApp:connectedAndroidTest` filtered to `PlatformParityTest#chunkedPushPullAndHydrate_convergeAcrossClients` | passed, exit 0, on Pixel_4 API 31 | Gradle console record, 2026-09-22T16:34Z |
| Aggregate platform qualification | `./gradlew oversqlitePlatformAll --rerun-tasks` | Android and iOS simulator lanes passed; browser lane failed with four unrelated existing worker/migration tests, so later lanes did not start | `platform-all-android-fixture-remediation-refreeze.log` |

The browser failures are `MigratedOversqliteStateTest` (three retained/custom-source continuity
cases) and `WorkerOversqliteEvidenceTest.numericBooleanBoundsAndStatementLifecycleUseTheSelectedRuntime`.
They are outside this authorized Android remediation. No browser source was modified.

QUALIFICATION BLOCKED

## Whole-candidate refreeze and qualification — 2026-09-22

The owner authorized qualification of the whole current candidate. The read-only paired snapshot is
`qualification-20260921/remediation-numeric-resume-refreeze.plan.json` (plan SHA-256
`9c6cbc2ccc5ab0d610d979fc203c1b2f2ea47c4547a2cd90d9743d57821fcc9c`). Its complete
tracked, untracked, staged, and governing-input manifests matched fresh preflight and postflight
plans through the first failed live lane, including
`qualification-20260921/whole-candidate-post-live-failure.plan.json`. SQLiteNow remained at
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`, Go remained clean and read-only at
`3839514ef08186900fa72acc05e313760f725d68`. The Go p0000/OpenAPI, ignored Kotlin companion,
and corpus manifest SHA-256 values remained `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`,
`02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`, and
`0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.
All 76 corpus records verified before testing. The only source change since the previous ledger
section was the separately authorized browser, lifecycle, and numeric-wire remediation; no source
was edited during this qualification run.

| Lane | UTC start; result | Evidence |
| --- | --- | --- |
| Cross-repository conformance | Previous turn on this exact snapshot; passed, exit 0, 57s | Gradle console record; pre/post paired plan manifests equal; Go OpenAPI hash logged |
| Comprehensive host | 2026-09-22T18:36:39Z; passed, exit 0, 3m10s, `--rerun-tasks` | `qualification-20260921/comprehensive-whole-candidate.log` |
| Generated production outputs | 2026-09-22T18:41:17Z; passed, exit 0, 7m37s, `--rerun-tasks` | `qualification-20260921/generated-production-whole-candidate.log` |
| Gradle plugin tests | 2026-09-22T18:50:07Z; passed, exit 0, 2m47s, `--rerun-tasks` | `qualification-20260921/gradle-plugin-whole-candidate.log` |
| Aggregate platforms | Previous turn on this exact snapshot; passed, exit 0, 10m14s, `--rerun-tasks`; Android, iOS simulator, JS browser/Node, JVM, macOS arm64, Wasm browser | Gradle console record; pre/post paired plan manifests equal; no separate raw log was captured |
| Swift hybrid/export | 2026-09-22T18:54:16Z; passed, exit 0, 8m04s, `--rerun-tasks`; some environment-dependent tests skipped | `qualification-20260921/swift-hybrid-whole-candidate.log` |
| Real-server aggregate | 2026-09-22T19:04:46Z; **failed**, exit 1, 6m27s at Android device tests; later platform wrappers did not start | `qualification-20260921/realserver-all-whole-candidate.log`, `android-realserver-whole-candidate.xml`, `go-server-whole-candidate-1.log`, `postgres-whole-candidate-1.log` |
| Swift real-server gate | Not started | Blocked by mandatory real-server aggregate failure |
| Heavy boundedness/stress | Not started | Blocked by mandatory real-server aggregate failure |

The live server was the pinned Go `integration/nethttp_server` under Go 1.25.12 on
`http://localhost:8080` (`http://10.0.2.2:8080` from Android). Its health endpoint returned 200
before the lane. A run-owned, fresh `postgres:16` container provided PostgreSQL 16.15, database
`clisync_example` (OID 16384), on host port 65115; image digest was
`sha256:43064e70a75c153a0ea5abed66b08e2981b7b43088bd59fb4b9f1a20ae4b62b0`.
Android ran on the attached Pixel_4 API 31 emulator. The server and database were stopped after
the failed lane; the run-owned `--rm` database container was removed. Existing containers were not
touched.

Android's live XML reports 75 tests, 11 failures, and no skips. Failures include a bounded-push
test expecting seven rows after one push with `maxPushRows=4` but observing four; a watch discovery
test looking for a missing top-level `watch_enabled` key; two shared-connection tests expecting 32
rows but observing four; a detach test expecting `DETACHED` but receiving
`BLOCKED_UNSYNCED_DATA`; offline discovery and rotated snapshot failures; three retries exhausted
on live push; and a three-device pull rejected while 16 dirty rows remained. PostgreSQL recorded
nine `team_members_team_id_fkey` violations during the failed pushes, each at deferred constraint
enforcement. The exact relationship between every failed assertion and the production code still
needs separate diagnosis; this aggregate is a real blocker, not a passing or skipped lane.

The advisory planner selected additional compiler, Core, worker, resource, and Go-owned lanes for
the broad dirty candidate. They were not launched after this mandatory failure. Go-owned lanes
remain planning-only under the read-only Go boundary. The fresh platform and cross-repository
passes on this candidate lack separate raw log files and should be recaptured if a later campaign
requires durable standalone evidence. No independent review, owner acceptance, staging, commit,
push, publication, deployment, or release occurred.

QUALIFICATION BLOCKED

## Android real-server remediation follow-up — 2026-09-22

The prior whole-candidate live failure was diagnosed against the pinned Go server. The KMP
snapshot-replacement request now authenticates with the existing source, while its replacement
source remains in the request body. Bounded push selection now includes dirty new FK parents in
the same slice and fails locally before outbox creation when a dependency cycle exceeds the row
cap. Real-server fixtures now send the required discovery source header and explicitly drain
bounded slices where their scenario needs the entire graph. The offline-resume fixture expects
fresh generation discovery, and the retired-source probe uses a valid replacement request.

Each of the 11 originally failing Android real-server cases passed an individual focused rerun.
The full `RealServerFkTopologyTest` class passed three tests, including a regression for a
new-row FK cycle and a fail-closed regression for a cycle larger than `maxPushRows`. These focused
passes do not substitute for the mandatory fresh real-server aggregate. The prior JVM test pass
predated the StageStore edit and must be rerun.

`git diff --check` passed. Go remains clean at
`3839514ef08186900fa72acc05e313760f725d68`. Drymint's post-edit baseline-backed
review for StageStore was blocked by `baseline_index_mismatch`: other source files changed after
its prewrite baseline, so it provides no usable attribution verdict. No independent review,
owner acceptance, staging, commit, push, publication, deployment, or release occurred.

The remediation refreeze plan is
`qualification-20260921/android-remediation-refreeze.plan.json` (SHA-256
`e6040e6f9389617305163c13af3db89fff6b13f0e65a54ab43ea3af74f8788c2`). The Go p0000,
OpenAPI, ignored KMP companion, and corpus-manifest hashes matched their frozen values; all 76
corpus records matched and no extra file existed. Cross-repository conformance passed (1m15s),
comprehensive host passed (3m33s), and aggregate platform passed (12m36s) on this source snapshot.
Raw logs are `qualification-20260921/crossrepo-android-remediation.log`,
`comprehensive-android-remediation.log`, and `platform-all-android-remediation.log`.

At the owner's request, qualification paused after the Android live device suite. On a fresh
run-owned PostgreSQL 16 container and pinned Go server, the Android sublane of
`oversqliteRealserverAll --rerun-tasks` passed in 7m19s; its copied device XML
`qualification-20260921/android-realserver-android-remediation.xml` reports 77 tests, zero
failures/errors/skips. The aggregate wrapper also automatically completed its iOS simulator
sublane before it could be stopped (1m34s; 36 XML tests, zero failures/errors/skips). The wrapper
was interrupted with exit 130 as JS browser began, so the aggregate itself is **not passed** and
must be rerun on resume. The raw partial aggregate, Go, and PostgreSQL logs are
`qualification-20260921/realserver-all-android-remediation.log`,
`go-server-android-remediation.log`, and `postgres-android-remediation.log`.

Only run-owned server, database container, and emulator processes were stopped. The fresh
run-owned `--rm` PostgreSQL database was removed; unrelated containers were untouched. `git
diff --check` passed, Go remains clean at the pinned HEAD, and no source changed during this
qualification segment. The ledger update changes the saved whole-worktree plan identity, so a
new plan/check is required before resuming. No Swift real-server, heavy stress, independent review,
acceptance, staging, commit, push, publication, deployment, or release occurred.

QUALIFICATION PAUSED AT OWNER REQUEST

## Post-restart real-server qualification — 2026-09-22

The owner resumed qualification. A fresh whole-worktree plan,
`qualification-20260921/post-pause-refreeze.plan.json` (SHA-256
`26a18083294ee92b25e3b76e67b2b30981b67e4ffdeba39fcde7952edc958a76`), captured both
paired repositories and governing inputs. The source-only tracked diff excluding this ledger
remained `7a460d831892d020acf90247df15f1e570e87c86819c4b39f5532a6d45f5a030`.
The Go and KMP HEADs and all four governing hashes remained frozen; Go stayed clean. All 76
corpus records and the exact file set matched. Fresh cross-repository conformance passed.

The mandatory `oversqliteRealserverAll --rerun-tasks` ran against a new run-owned PostgreSQL 16
database and the pinned Go server. Its Android (77 tests, zero failures/errors/skips), iOS
simulator (36 tests, zero failures/errors/skips), JS browser, JS Node, JVM harness, macOS arm64,
and Wasm browser sublanes passed. The aggregate failed at
`:library-oversqlite:oversqliteRealserverJvm`: 14 tests, four failures. Raw evidence is
`qualification-20260921/realserver-all-post-pause.log` (SHA-256
`1b976cba3f963cc42cb70c6388f6a46325ee1337450d8ed2caf6ae00ffaa2de9`),
`android-realserver-post-pause.xml`, the three copied `TEST-*.xml` JVM reports,
`go-server-post-pause.log`, and `postgres-post-pause.log`.

The failing JVM cases are all in retained test fixtures, not the Android harness that had been
remediated: `sameInstall_heavyMultiChunkAlternationAgainstRealServer` pushes one row per
user/post pair with `maxPushRows=1` and then expects detach despite pending dirty rows;
`openIsLocalOnly_andSameUserOfflineResume_workAgainstRealServer` expects offline reattach to
resume without mandatory generation discovery; the rotated-rebuild retirement probe submits a
compact push with a nonmatching canonical hash and receives `push_invalid` 400 before retirement
is checked; and `businessRichV0Schema_pushPullAndRebuild_workAgainstRealServer` pushes once with
the default eight-row cap for a ten-row graph, leaving the `teams` row absent on the reader.
Those diagnoses are from the JVM XML, source inspection, and the Go log; they are not a passing
lane. The analogous bounded-push, offline-discovery, and retirement cases already passed in the
Android/shared harness.

`qualification-20260921/post-jvm-failure.plan.json` is byte-identical to the pre-run plan, so
the failure was not accompanied by candidate/authority drift. Qualification stopped at this first
mandatory blocker. No fixture repair or unchanged aggregate retry occurred within this pass;
Swift real-server, heavy stress, and remaining advisory lanes were not launched. The run-owned
server, `--rm` database container, and emulator were stopped; the run-owned database was removed,
and unrelated containers were not touched. This ledger edit itself requires a fresh identity
capture before a later qualification run. No independent review, owner acceptance, staging,
commit, push, publication, deployment, or release occurred.

QUALIFICATION BLOCKED — JVM REAL-SERVER FIXTURES

## JVM real-server fixture remediation — 2026-09-23

The owner separately authorized a remediation pass for the four JVM fixture failures above.
`RealServerComprehensiveTest` now drains both rows of each `maxPushRows=1` user/post pair before
detach, expects required discovery to fail on offline reattach, and probes a retired source with a
valid snapshot-replacement request rather than a push with an invalid canonical hash.
`RealServerRichSchemaTest` now commits both bounded slices of its ten-row graph and checks the
seed client has no pending sync data before reader assertions. Go source, contracts, runtime
implementation, plugin, and compiler code were not changed in this pass.

Drymint prewrite baseline `rb0:22643992ea1fa1b3703cbc0a` covered the two edited test files.
After its requested one-shot index refresh, baseline-backed review attribution was available.
Its inspect-level duplicate advisory compared the heavy bounded same-install test with the
ordinary alternating-user scenario; source inspection confirmed that their distinct row limits
and multi-chunk behavior make both tests intentional. No refactor or duplicate-policy mutation
was made.

On a fresh run-owned PostgreSQL 16 database and pinned Go server,
`:library-oversqlite:oversqliteRealserverJvm --rerun-tasks` passed all 14 tests with zero
failures/errors/skips, including the four previously failing cases. The raw log is
`qualification-20260921/jvm-realserver-fixture-remediation.log` (SHA-256
`55c2e1d0a94912c4433b9852061c9ae575cb39f23d84c37fd1548a9e7cdda5d2`); copied
`jvm-fixture-remediation-*.xml` reports preserve the test results. The run-owned server and
`--rm` database container were stopped; the run-owned database was removed, and unrelated
containers were untouched.

The new source-bearing candidate remains at SQLiteNow HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd` with tracked diff excluding this ledger
`715c190dbbe3dc74bca3be8ca7af3fec731b0fdcf5f9487ae99a9f2eea8657bc`. Go remains
clean and read-only at `3839514ef08186900fa72acc05e313760f725d68`. The Go p0000,
OpenAPI, ignored KMP companion, and corpus-manifest hashes remain unchanged. `git diff --check`
passed. This focused remediation pass is not whole-candidate qualification: the previous full
aggregate result is superseded for the changed test fixtures, and the mandatory final aggregate,
Swift live gate, heavy stress, and remaining selected lanes are pending a new refreeze and
qualification gate. No independent review, owner acceptance, staging, commit, push,
publication, deployment, or release occurred.

REMEDIATION VERIFIED — QUALIFICATION PENDING

## JVM fixture refreeze qualification — 2026-09-23

The owner resumed whole-candidate qualification without authorizing another source edit. The
refreeze plan is `qualification-20260921/jvm-fixture-remediation-refreeze.plan.json` (SHA-256
`6e41d5ebd8d9bdb9c25c0a75e1990170ed934d3f92071d2a97a9ab4fa77d0cfe`). SQLiteNow
HEAD remains `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; source-bearing tracked diff,
excluding this ledger, is `715c190dbbe3dc74bca3be8ca7af3fec731b0fdcf5f9487ae99a9f2eea8657bc`.
Go remains clean and read-only at `3839514ef08186900fa72acc05e313760f725d68`.
The Go p0000, OpenAPI, ignored KMP companion, and corpus-manifest hashes were unchanged;
all 76 corpus records and the exact file set passed. Fresh cross-repository conformance and
`oversqliteComprehensive --rerun-tasks` passed. Their logs are
`crossrepo-jvm-fixture-refreeze.log` and `comprehensive-jvm-fixture-refreeze.log`.

The mandatory `oversqliteRealserverAll --rerun-tasks` passed in 22m30s on fresh run-owned
PostgreSQL 16 and the pinned Go server. Android reported 77 tests, iOS simulator 36 tests,
and library JVM real-server 14 tests, each with zero failures/errors/skips. JS browser,
JS Node, JVM harness, macOS arm64, and Wasm browser sublanes passed. Raw evidence is
`realserver-all-jvm-fixture-refreeze.log` (SHA-256
`155888d888c0441a4a4cfc8c89ba77adf0f5e05cd5f6d098af58567c82532940`),
`android-realserver-jvm-fixture-refreeze.xml`, copied `jvm-realserver-jvm-fixture-refreeze-*.xml`
reports, and the run-owned server/PostgreSQL logs. The post-aggregate plan was byte-identical
to the refreeze plan.

The required `swiftHybridSupportRealserverGate --rerun-tasks` failed at
`:swift:fixtures:package:sync:swiftTest`, before `swiftRealserverSmoke` started. One of 23
package tests failed: `GeneratedSyncOwnershipScenarios.testGeneratedStreamsTerminateAndQueriesRejectAfterEveryCloseOwner`
timed out after 10 seconds waiting for `generated stream / client close`; five tests were
expectedly skipped. The exact test passed in isolation (0.8s), and its five-test ownership
family passed together (five tests, zero failures). Those diagnostic passes do not replace
the failed gate. The first gate log is `swift-realserver-jvm-fixture-refreeze.log` (SHA-256
`b160570a0996c008241359b36c2c19bbbea67d7693055194694c017d56b9c272`); diagnostic
logs are `swift-generated-close-isolated.log` and
`swift-generated-ownership-family-isolated.log`.

One controlled gate rerun used a fresh PostgreSQL database and serialized Gradle work
(`--max-workers=1 -Dorg.gradle.parallel=false`) to change execution conditions, not to seek
an unchanged aggregate pass. It failed in the same ownership test, this time waiting for
`generated stream / opening close`. Its 23-test package result again had one failure and five
expected skips; `swiftRealserverSmoke` did not start. The log is
`swift-realserver-constrained-jvm-fixture-refreeze.log` (SHA-256
`17302f7ddde12bedc9351fb9431be0ecb5ab352b426759e811c2240bea19c05f`).
This reproducible suite-level timeout is a mandatory Swift qualification blocker; the isolated
passes do not establish whether its cause lies in the fixture, generated stream, or runtime.

`post-swift-constrained-failure.plan.json` is byte-identical to the pre-run plan, confirming
no candidate/authority drift before this ledger update. Qualification stopped: Swift live
smoke, heavy stress, and remaining advisory lanes were not run. Both run-owned Swift servers,
their `--rm` PostgreSQL containers/databases, and the run-owned emulator were stopped; the
temporary databases were removed and unrelated containers were untouched. This ledger edit
requires a new identity capture before a future qualification pass. No independent review,
owner acceptance, staging, commit, push, publication, deployment, or release occurred.

QUALIFICATION BLOCKED — SWIFT GENERATED STREAM CLOSE TIMEOUT

## Swift generated stream-close remediation — 2026-09-23

The owner separately authorized investigation and repair of the blocked Swift close-stream test.
The timeout was caused by a lifecycle gap: `SQLiteNowCoreRuntimeDatabase.observeTables` silently
swallowed parent-scope cancellation, so an active generated Swift stream never finished when
the core wrapper closed. The coordinated opening and its bound client also close the underlying
KMP database without calling the core wrapper's `close()`, leaving that observer scope active.
The prior test's `Task.yield()` did not prove the stream had subscribed and received initial rows;
the first runtime-only repair passed once but the strengthened test reproduced the timeout on
opening and client close. That intermediate passing result is superseded, not qualification.

The final repair notifies table observers of database-close cancellation while preserving silent
per-observer cancellation. The core wrapper exposes a coordinated-close observer shutdown hook;
the Swift sync opening and its bound client invoke it on their respective close paths. The
authored shared Swift ownership fixture now waits for the first stream result before closing
each owner and checks client-close termination before calling opening close. The generated
Swift package files were not edited. Go source, wire contracts, and compiler/plugin sources
were not changed.

Drymint prewrite baseline `rb0:bc9eb32124c8b260adfe6907` attributed the initial core-runtime
edit after its emitted one-shot index refresh. Baseline `rb0:f9110647174abb6db1860e9b`
attributed the subsequent two-Kotlin-file bridge after its own emitted refresh. The final
duplicate gate was clean with an inspect-level cross-domain dependency advisory for the intended
sync-to-core bridge; source inspection confirmed that dependency. Swift test source was outside
Drymint's indexed language coverage and was validated by the Swift suites.

Focused `:swift:runtime:core:swiftTest --rerun-tasks` passed 10 tests, including explicit
database-close notification and silent observer cancellation. Focused
`:swift:fixtures:package:sync:swiftTest --rerun-tasks` passed 23 tests, five expected skips,
zero failures; the strengthened three-owner close case passed. The broader
`swiftHybridSupportGate --rerun-tasks` passed in 10m23s, again including the 23-test generated
sync fixture and 10-test core runtime suite. Logs and SHA-256 values:

| Lane | Log | SHA-256 |
| --- | --- | --- |
| Core runtime | `qualification-20260921/swift-stream-close-runtime-remediation-final.log` | `38144b66bc458bf43c7fd96eec8b1031f64ea48be3286d61445a5b56c7d59177` |
| Generated sync fixture | `qualification-20260921/swift-stream-close-generated-fixture-remediation-final.log` | `522dd2b4d9371f15b1dfe3702486cbbd94814f37d28f59a2d8976eeed1140712` |
| Swift hybrid gate | `qualification-20260921/swift-stream-close-hybrid-remediation.log` | `246174ace591cdd89acd290c1f65fe8d25da178f09ccd84f6443c09424b0f052` |

`git diff --check` passed. The post-remediation advisory plan is
`qualification-20260921/post-swift-stream-close-remediation.plan.json` (SHA-256
`e4adf1652e4756555758f2cac67488f2094898edc294fe1769013b3b0f5527bf`), captured
before this ledger entry. SQLiteNow remains at HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd` with source-bearing tracked diff excluding
the ledger `8517016c923eccfcf0cd5a10fd5c8c49a4d9544195e1658f6ae7a69381877c66`.
The read-only Go checkout remains clean at `3839514ef08186900fa72acc05e313760f725d68`.
This ledger update changes whole-worktree identity and requires a fresh refreeze before
qualification. The previous full real-server aggregate and the failed Swift live gate are
superseded for this source change; the mandatory affected lanes and heavy stress remain to be
qualified separately. No independent review, owner acceptance, staging, commit, push,
publication, deployment, or release occurred.

REMEDIATION VERIFIED — REFREEZE / QUALIFICATION PENDING

## Swift close-stream refreeze qualification — 2026-09-23

The owner authorized the next qualification step without a further source edit. The paired
preflight plan is `qualification-20260921/swift-close-refreeze.plan.json` (SHA-256
`0cbd6d8b9ed082168e1463c044913184a4ae8e8b408f647f5c3f5488744936b7`).
SQLiteNow HEAD is `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; its source-bearing tracked
diff excluding this ledger remained
`8517016c923eccfcf0cd5a10fd5c8c49a4d9544195e1658f6ae7a69381877c66`.
Go remained clean and read-only at `3839514ef08186900fa72acc05e313760f725d68`.
The Go p0000, Go OpenAPI, ignored KMP companion, and corpus-manifest SHA-256 hashes remained
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`,
`02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`, and
`0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.
The manifest verified all 76 records and the exact file set before testing.

| Lane | Result | Evidence SHA-256 |
| --- | --- | --- |
| Cross-repository conformance, `--rerun-tasks` | passed | `crossrepo-swift-close-refreeze.log`: `c0c5ac7fcb4f9b462be64f5d09426b48b8f06561861704a774e2e312a8a5389c` |
| Focused Go schema/corpus tests | passed | `go-focused-swift-close-refreeze.log`: `8fc35bbe508d2a2bbd2615df12dd5e02a012f1d6de1f4ca86d5714359fd49411` |
| `oversqliteRealserverAll --rerun-tasks` | passed in 13m03s; Android, iOS simulator, JS browser/Node, JVM harness/library, macOS arm64, and Wasm browser wrappers passed | `realserver-all-swift-close-refreeze.log`: `5f6d0ccdded22298fc4f61aa9ae19d4168b524282938afe0af79bdf9759dbbe2` |
| `swiftHybridSupportRealserverGate --rerun-tasks` | passed in 5m11s; generated ownership test passed in both package and live smoke runs | `swift-realserver-swift-close-refreeze.log`: `91aa67d08f4f204cc1f91379a2090bcee1479e11c1707c7cbf54a2684f24f461` |
| `oversqliteRealserverAllHeavy --rerun-tasks` | **failed** in 6m29s at Android; later heavy wrappers did not start | `realserver-heavy-swift-close-refreeze.log`: `8c0fad82047e7a1a6b2d3766312e3f9c0670224895393d3e6bd08d23afa199c8` |

The standard real-server aggregate used a fresh run-owned PostgreSQL 16.15 database and the
pinned Go server, with Android on the run-owned Pixel_4 API 31 emulator. Android's copied XML
reports 77 tests, zero failures/errors/skips. Per-target XML is preserved under
`qualification-20260921/realserver-all-swift-close-refreeze-reports/`. The generated rich
numeric production-path test returns early when heavy mode is disabled; the standard aggregate
does not establish that heavy-only path. The Swift gate used a second fresh database; its
generated sync package ran 23 tests with five expected skips and zero failures, while live
smoke ran 23 tests with three expected skips and zero failures. Both runs passed
`testGeneratedStreamsTerminateAndQueriesRejectAfterEveryCloseOwner`.

Heavy stress used a third fresh database and the same pinned Go server. Android ran 77 tests,
one failure, zero errors/skips. The failing heavy-only test was
`RealServerGeneratedRichNumericTest.generatedRichNumericProductionPath_runsAcrossHeavyRealServerTargets`:
the generated `TypedRowQuery.SelectById.asOne()` threw `IllegalStateException: Query returned no
results, but exactly one result was expected`. The test had already asserted successful push,
incremental pull, and rebuild outcomes before checking rows on the three databases; the
report does not identify which database or numeric fixture lacked a row. This is a distinct
mandatory blocker, not a timeout or a passing standard-lane result. The exact failure report
is `qualification-20260921/android-realserver-heavy-swift-close-refreeze.xml` (SHA-256
`6a20f686bbb767f1721ec221a1829edc382209cb1b2ea3d7b212ee50c46f415c`).
No unchanged aggregate retry or source edit was performed. Other heavy targets and remaining
advisory lanes were not started after this mandatory failure.

The post-failure plan `qualification-20260921/post-swift-close-heavy-failure.plan.json` is
byte-identical to preflight (same SHA-256
`0cbd6d8b9ed082168e1463c044913184a4ae8e8b408f647f5c3f5488744936b7`);
SQLiteNow and Go porcelain hashes and the source-bearing diff also remained unchanged before
this ledger update. `git diff --check` passed. Run-owned Go servers, their three `--rm`
PostgreSQL containers/databases, and the run-owned emulator were stopped; unrelated
containers were untouched. Server and PostgreSQL logs were preserved per run. This ledger
edit changes whole-worktree identity and requires a new preflight plan before resumption.
No independent review, owner acceptance, staging, commit, push, publication, deployment,
or release occurred.

QUALIFICATION BLOCKED — ANDROID HEAVY GENERATED RICH NUMERIC ROW MISSING

## Android heavy generated-numeric remediation — 2026-09-23

The owner separately authorized investigation and repair of the heavy-only generated numeric
failure. The fixture inserts ten `typed_rows` scenarios but its coordinated opening configures
`maxPushRows = 8`. `pushPending()` commits one bounded outbound snapshot per call. The failed
run's Go server log recorded an eight-row snapshot for the numeric user, and the test invoked
`pushPending()` only once before expecting all ten rows on the pull and rebuild databases.
This was a test-sequence error, not evidence that the generated selector or server lost an
already committed row. Go source and the paired wire contract were not changed.

The authored common test `RealServerGeneratedRichNumericTest.kt` now asserts pending data
after the first committed push, commits the second bounded push, and asserts no pending data
before pulling and rebuilding. Its existing numeric-value assertions remain unchanged.
Drymint exact-entity prewrite inspection and current-path review were run for this single
test file. The review reported an unattributed, low-confidence expected duplicate against
an unrelated mock hash helper; source inspection found no reuse/refactor rationale.

Focused Android heavy-mode execution of the exact previously failing method passed (one
test, zero failures/errors/skips) on a fresh run-owned PostgreSQL 16.15 database and pinned
Go server. The full `oversqliteRealserverAndroidHeavy --rerun-tasks` lane then passed on a
second fresh database: 77 tests, zero failures/errors/skips. The server's numeric-user
snapshot now records ten rows. Evidence:

| Validation | Log SHA-256 | XML SHA-256 |
| --- | --- | --- |
| Exact Android numeric method | `android-numeric-fix-focused.log`: `109ff3b313700c1d39c83b5ec4646c65babf080d59033fe83f98879bb87d9e37` | `android-numeric-fix-focused.xml`: `4942115cc8b2f07423e7e17e43572667cc3fe6a60468ad5825734d6dbf87b215` |
| Android heavy lane | `android-numeric-fix-heavy-lane.log`: `b92ebfad45970f9d38c997f8fcab20854899627f40329e1fc368cc8a50607fe8` | `android-numeric-fix-heavy-lane.xml`: `0d19b6ad0456962937bdb09527f458196aa113066837549e3aa65b4585403ddb` |

Server and PostgreSQL logs for both runs are preserved in `qualification-20260921/`.
`git diff --check` passed. The post-remediation plan is
`qualification-20260921/post-numeric-fix-remediation.plan.json` (SHA-256
`3c55cf9d2cf888835434293dc5b19f2f43c54e6aa2c60a11b566ab4aa1cb30be`), captured
before this ledger entry. SQLiteNow remains at HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; its source-bearing tracked diff excluding
this ledger is now `137be5d5cd822764080141052af5a47dc6da0365a63920a302888c9fe71e86b1`.
Go remains clean and read-only at `3839514ef08186900fa72acc05e313760f725d68`.
Both run-owned Go servers, their `--rm` PostgreSQL containers/databases, and the run-owned
emulator were stopped; unrelated containers were untouched. The ledger update changes the
whole-worktree identity again. The earlier failed heavy aggregate is superseded for this
test fix, but this author validation is not whole-candidate qualification: a fresh preflight
and remaining required lanes are pending. No independent review, owner acceptance, staging,
commit, push, publication, deployment, or release occurred.

REMEDIATION VERIFIED — REFREEZE / QUALIFICATION PENDING

## Numeric-fix refreeze qualification — 2026-09-23

The owner authorized a fresh read-only qualification pass. The paired preflight plan is
`qualification-20260921/numeric-fix-refreeze.plan.json` (SHA-256
`ef7fc97db58f9b9c53913c098b5829f28dc0713ef1b0df2f580f117654026078`).
SQLiteNow HEAD remained `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd` and its
source-bearing tracked diff excluding this ledger remained
`137be5d5cd822764080141052af5a47dc6da0365a63920a302888c9fe71e86b1`.
Go remained clean and read-only at `3839514ef08186900fa72acc05e313760f725d68`.
The governing Go p0000, Go OpenAPI, ignored KMP companion, and corpus-manifest hashes
were unchanged from the preceding refreeze. All 76 manifest records passed, fresh
`oversqliteCrossRepoConformance --rerun-tasks` passed in 54s, and the focused Go
schema/corpus tests passed. Logs are `crossrepo-numeric-fix-refreeze.log` (SHA-256
`655a1711a1de722d75d9e3b8f0268ff7fa7fa57fc0f3ee31069c100ca1d8e2b1`) and
`go-focused-numeric-fix-refreeze.log` (SHA-256
`ab0cbadad2503e85b9c8cc787e3db2ca642d2fd41ee04bf373fa157fe865dbfb`).

`oversqliteRealserverAllHeavy --rerun-tasks` ran serially against a fresh run-owned
PostgreSQL 16.15 database and pinned Go server; Android used the run-owned Pixel_4 API 31
emulator. Its Android (77 tests, zero failures/errors/skips), iOS simulator, JS Node,
JVM harness, and macOS arm64 heavy wrappers passed. The earlier Android numeric missing-row
failure did not recur. The aggregate then **failed at Wasm browser heavy** after 14m10s:
48 Wasm tests ran, with one failure in
`RealServerGeneratedRichNumericTest.generatedRichNumericProductionPath_runsAcrossHeavyRealServerTargets`.
The XML reports `kotlin.Error: Fail to fetch` about 0.011s into the case; it does not identify
the URL or request stage. This is not the prior `selectById().asOne()` missing-row failure.
The host-side server health endpoint returned 200 with CORS allow-origin `*` during diagnosis,
but that does not prove browser-side reachability or establish the fetch cause. Later library
JVM real-server and JVM-heavy wrappers did not start.

The failed aggregate log is `qualification-20260921/realserver-all-heavy-numeric-refreeze.log`
(SHA-256 `ad36274a77eff79dfdd907bd978d06388df91318f2d9ca546849c6fe07f6f945`).
The exact failing Wasm XML is preserved under
`qualification-20260921/realserver-all-heavy-numeric-refreeze-reports/wasmJsBrowserTest/`
(SHA-256 `137bbc3f16a9cffcc56f84919ddcece02200b14ff572120cc4c148dfa90e5f9c`).
The Android XML is `qualification-20260921/android-all-heavy-numeric-refreeze.xml`
(SHA-256 `4da9a6e6a853a59d330752596b541ec32aca784e6457ebff4fe45e534d9aef35`);
the other completed per-target reports, Go server log, and PostgreSQL log were also preserved.

No unchanged aggregate retry or source edit was performed. The post-failure plan
`qualification-20260921/post-numeric-fix-heavy-wasm-failure.plan.json` is byte-identical
to preflight. The source-bearing diff and clean Go identity were unchanged before this
ledger update; `git diff --check` passed. The run-owned Go server, `--rm` database
container, and emulator were stopped; unrelated containers were untouched. The ledger
edit changes whole-worktree identity and requires a fresh preflight before resumption.
No independent review, owner acceptance, staging, commit, push, publication, deployment,
or release occurred.

QUALIFICATION BLOCKED — WASM BROWSER HEAVY NUMERIC FETCH FAILURE

## Cross-origin gzip preflight remediation — 2026-09-23

The owner separately authorized the sibling Go integration-server edit. Before editing, SQLiteNow
was at HEAD `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd` with source-bearing tracked diff
SHA-256 `137be5d5cd822764080141052af5a47dc6da0365a63920a302888c9fe71e86b1` and
status SHA-256 `2f7b246745f0ab4246a26bfef573536b88366e0ed4d7e87bf3dc9e1fec6cf9be`;
Go was clean at HEAD `3839514ef08186900fa72acc05e313760f725d68`. The pinned Go spec,
SQLiteNow companion spec, OpenAPI, and corpus manifest hashes remained unchanged.

A fresh isolated Wasm reproduction reached the first numeric `POST /sync/push` and failed with
Fetch's `Fail to fetch`. The Go integration server's CORS preflight omitted `Content-Encoding`
from `Access-Control-Allow-Headers`; both governing p0000 specifications require the browser to
send beneficial gzip and require cross-origin deployment to permit that header. The new Go
`TestBrowserTestCORSMiddleware_GzipPushPreflight` failed before the fix specifically because
`content-encoding` was absent. The Go change adds that single allow-list entry; the test now
checks the whole browser preflight header set and that `OPTIONS` does not reach the sync handler.
The red reproduction log is `qualification-20260921/wasm-fetch-investigation-repro.log`.

The fast paired gate passed: all 76 corpus manifest entries, focused Go schema/corpus and bounded
gzip tests, and `oversqliteCrossRepoConformance --rerun-tasks`. A live `OPTIONS /sync/push`
probe returned 204 with `Content-Encoding` allowed. Against a fresh run-owned PostgreSQL 16.15
database and the edited Go server, full Wasm and JS browser heavy tasks passed separately:
48 tests each, zero skips/failures/errors. Both generated rich-numeric cases passed; server
snapshot logs recorded ten rows for each. The Go `integration/nethttp_server/server` package
passed (including the new preflight regression). Evidence:

| Lane | Evidence log SHA-256 |
| --- | --- |
| Wasm browser heavy | `wasm-browser-heavy-cors-validation.log`: `1c2725e1102ff21ec61180cfb536d45a8759cf692f7230d912c573f0a2a44314` |
| JS browser heavy | `js-browser-heavy-cors-validation.log`: `5c4b308811c6641a35a410574638b5d6b208e1fc19fdf60f4f6a627c5e58a7e8` |
| Go integration package | `go-server-package-cors-validation.log`: `84804a189d28256597b422c6d941848ef6b2ea9ee37537f5cd46a81eac5792bc` |

The browser XML reports are preserved in `qualification-20260921/wasm-browser-heavy-cors-reports/`
and `qualification-20260921/js-browser-heavy-cors-reports/`; the Go server and PostgreSQL logs
are preserved in the same evidence directory. `gofmt -l` was empty for both touched Go files and
`git diff --check` passed in both repositories. The Go source diff SHA-256 is
`8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`;
SQLiteNow's source-bearing diff remained `137be5d5cd822764080141052af5a47dc6da0365a63920a302888c9fe71e86b1`.
The run-owned server and `--rm` database were stopped; unrelated listeners and containers were
left alone.

This is author remediation validation, not a completed frozen whole-candidate qualification.
The Go source edit changes paired candidate identity. A fresh paired freeze/preflight and the
mandatory final aggregate (including library JVM real-server wrappers that never started in the
failed run) remain pending; prior unaffected lane results may be retained only with an explicit
invalidation ledger. Independent review, owner acceptance, staging, commit, push, publication,
deployment, and release remain separate and undone.

REMEDIATION VERIFIED — PAIRED REFREEZE / FINAL QUALIFICATION PENDING

## Paired CORS refreeze and final heavy aggregate — 2026-09-23

The owner authorized the next read-only qualification step. Before the run, SQLiteNow
HEAD was `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`, status SHA-256
`2f7b246745f0ab4246a26bfef573536b88366e0ed4d7e87bf3dc9e1fec6cf9be`,
tracked binary diff SHA-256 `cb90d4a129847a80ddbd67dc6764bdad954aaaf9e18a851c02a2002717117343`,
source-bearing tracked diff excluding this ledger SHA-256
`137be5d5cd822764080141052af5a47dc6da0365a63920a302888c9fe71e86b1`,
and untracked content-manifest SHA-256
`0630b2e757fd299c0745048e2674fc81f81cf5d8246cf7883b096b66ca4c9904`.
Go HEAD was `3839514ef08186900fa72acc05e313760f725d68`, status SHA-256
`effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227`,
and tracked binary diff SHA-256
`8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`.
The ignored KMP companion spec, Go spec, OpenAPI, and corpus manifest remained pinned at
`02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`,
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`,
and `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.

The fast paired gate passed: all 76 corpus manifest entries verified, focused Go
schema/corpus/bounded-wire tests passed, and fresh
`oversqliteCrossRepoConformance --rerun-tasks` passed. Logs are
`qualification-20260921/go-fast-paired-cors-refreeze.log` (SHA-256
`4def0357c2d6552b33587d043852c39f3dd781b44537e29c7c3c54568bf3f033`)
and `qualification-20260921/crossrepo-cors-refreeze.log` (SHA-256
`81725f0eb44d07a7ba53682b49e604f4f61339988b96c9f02d9327114bbd1e25`).
The Go CORS server-package test, full JS browser heavy, and full Wasm browser heavy from
the immediately preceding remediation validation remain relevant to the CORS-only Go edit;
prior compiler/plugin/platform/Swift results not dependent on this server edit remain retained
as author-validation evidence, not as substitutes for this final live aggregate.

An unrelated GoLand process owned host port 8080, so the run used a fresh PostgreSQL 16.15
container and pinned Go server in a dedicated Docker network, mapped only to host
`127.0.0.1:8081`. Gradle host wrappers used `http://localhost:8081`; Android used
`http://10.0.2.2:8081` on the run-owned Pixel_4 API 31 emulator. Host and emulator
health/preflight probes confirmed reachability and `Content-Encoding` in the CORS
allow-header list. The unrelated port-8080 listener was untouched.

`oversqliteRealserverAllHeavy --rerun-tasks` **failed** after 16m17s. Its Android heavy
(77 tests, zero failures/errors/skips), iOS simulator heavy, JS Node heavy, JVM harness
heavy, macOS arm64 heavy, Wasm browser heavy (48 tests, zero failures/errors/skips),
and library JVM real-server (14 tests, zero failures/errors/skips) wrappers passed. The
earlier Wasm `Fail to fetch` did not recur. The final
`library-oversqlite:jvmRealServerSharedConnectionStress` wrapper ran two tests and failed
one: `sharedConnectionSurvivesConcurrentAliasStarQueriesWhileSyncingAgainstRealServer`
expected 32 rows but observed 4 at `RealServerSharedConnectionStressTest.kt:78`,
immediately after `activeClient.pullToStable()`, before the concurrent stress body.
This is a real qualification blocker; the cause has not been determined. No unchanged
aggregate retry or source edit was performed.

The failed aggregate log is
`qualification-20260921/realserver-all-heavy-cors-refreeze.log` (SHA-256
`ec65ce2b711009fc1e49b501d4fd577352d3874318cde3769ddc545898c38190`).
The exact failing XML is preserved under
`qualification-20260921/realserver-all-heavy-cors-refreeze-reports/jvmRealServerSharedConnectionStress/`
(test XML SHA-256
`c74f2a93104c3327aed82fe8ba21bf55edb9f8f5fe4964553821792173eaf50a`).
The Android, Wasm, and regular library JVM XML are preserved alongside it; run-owned
Go server and PostgreSQL logs are `go-server-cors-refreeze.log` (SHA-256
`51106e812e470cd25a80ddf2a1cf1242e3c2328d1e1ac78b99edba179fef2472`)
and `postgres-cors-refreeze.log` (SHA-256
`69a2620d25797b65a4fde383ad30c68e301c2b9310db4db1da1a44444381bbe8`).

Before this ledger edit, paired status and source-bearing diff hashes matched preflight;
`git diff --check` passed in both repositories. The run-owned server, database, Docker
network, emulator, and temporary binary were stopped/removed; unrelated processes and
containers were untouched. This ledger edit changes whole-worktree identity, so a future
qualification resumption requires a fresh paired preflight. Independent review, owner
acceptance, staging, commit, push, publication, deployment, and release remain separate
and undone.

QUALIFICATION BLOCKED — JVM SHARED-CONNECTION INITIAL PULL ROW COUNT

## JVM shared-connection blocker isolation — 2026-09-23

The owner authorized diagnosis of the final aggregate blocker, not source remediation.
The focused `RealServerSharedConnectionStressTest.sharedConnectionSurvivesConcurrentAliasStarQueriesWhileSyncingAgainstRealServer`
ran alone with `--rerun-tasks` and heavy mode against a fresh run-owned PostgreSQL 16.15
database and the same pinned Go integration server mapped to host port 8081. It failed
again in 38s at line 78 with `expected:<32> but was:<4>`, before any concurrent writer,
reader, or sync coroutine started. The server's snapshot log for that test user recorded
exactly four rows. The focused log, XML, and server log are
`qualification-20260921/jvm-shared-connection-isolation-20260923.log` (SHA-256
`cef3664d2f1b707a1718977a20879f9651726b461625ff525c9a084e063c30ba`),
`jvm-shared-connection-isolation-20260923.xml` (SHA-256
`bb5caba5d2024ececea2ad94708104aa4b6c36d8d9ee1a8f774b62fb0a282289`),
and `go-server-stress-isolation-20260923.log` (SHA-256
`dc80afe902ffa3d260a705b3427c11890b3594b2cff3f85d97437ab7ace228f8`).
The run-owned PostgreSQL log was also preserved.

Code inspection explains the deterministic count. `seedUserRows()` inserts 16 local and
16 remote rows. Both clients are configured with `maxPushRows = 4`; `pushPending()` is
documented and implemented to freeze and commit one bounded outbound snapshot per call.
The test calls `seedClient.pushPending()` only once before `activeClient.pullToStable()`
and immediately expects all 32 rows. `pullToStable()` can only retrieve the four rows
that were committed to the server. The test already contains
`drainPendingLocalChanges(client, db)`, which repeats bounded pushes until dirty and
outbox rows are zero; using it for the initial seed, then asserting the seed is drained
before the active pull, is the narrow test-setup repair to evaluate in a separately
authorized remediation pass. This diagnosis does not establish that the full stress body
will pass after that repair.

No source was edited. Before this ledger update, SQLiteNow status SHA-256 remained
`2f7b246745f0ab4246a26bfef573536b88366e0ed4d7e87bf3dc9e1fec6cf9be`
and source-bearing tracked diff SHA-256 remained
`137be5d5cd822764080141052af5a47dc6da0365a63920a302888c9fe71e86b1`;
Go status and tracked diff hashes remained
`effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227`
and `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`.
`git diff --check` passed in both repositories. The isolated server, database, network,
and temporary binary were removed; unrelated processes and containers were untouched.

DIAGNOSIS COMPLETE — TEST SEED COMMITS ONE FOUR-ROW BUNDLE; REMEDIATION NOT STARTED

## JVM shared-connection repair and final paired qualification — 2026-09-23

The owner authorized continuous KMP remediation and validation without a pause for each
small step. Go remained read-only. The existing dirty test file already contained the
`uploadLimit`/`downloadLimit` to `maxPushRows`/`maxPullBundles` conversion; this pass
preserved that hunk and changed only the initial seed call from one `pushPending()` to
the test's existing `drainPendingLocalChanges(seedClient, seedDb)`. That helper sends
bounded bundles until dirty/outbox rows are zero, so all 32 seeded rows are committed
before the active client pulls. No production, contract, fixture, or Go source changed.

`drymint-coding` inspected the exact test method and captured a prewrite workspace-delta
baseline. The post-edit review attributed one modified method to the edit after its
emitted one-shot index refresh. Its inspect-level expected-duplicate advisory compared
this test with the unrelated `SnapshotMemoryFixtureProcess.stop`; source inspection found
no reusable behavior or reason to refactor. The functional validation below is separate
from that advisory and from independent review.

Focused heavy-mode execution of the exact failing JVM stress method passed on a fresh
run-owned PostgreSQL 16.15 database and pinned Go server in 47s, one test with zero
failures/errors/skips. Its server snapshot recorded 32 rows, and the concurrent stress
body completed. Evidence is `qualification-20260921/jvm-shared-connection-repair-focused-20260923.log`
(SHA-256 `aad1df573679aac91e810ea16e1694d46f0bc326bcea6ceb3900ca5b0688cf38`),
`jvm-shared-connection-repair-focused-20260923.xml` (SHA-256
`23a408d6102057e0d726442cfb0de3df06b26217c9cf9634c186919c2579a094`),
and `go-server-stress-repair-focused-20260923.log` (SHA-256
`ba4039e70a3de0e5873beb95975fdef66c5abf8ee200b526a08c210da0bf501f`).

For the repaired candidate, SQLiteNow HEAD remained
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; before this ledger edit its
status SHA-256 remained
`2f7b246745f0ab4246a26bfef573536b88366e0ed4d7e87bf3dc9e1fec6cf9be`,
source-bearing tracked diff excluding this ledger was
`1a248b607ce5dad3a3f64c77eacdd3ad2ba651a3c1d6dd745f6de7ac08a321d7`,
and untracked content-manifest SHA-256 remained
`0630b2e757fd299c0745048e2674fc81f81cf5d8246cf7883b096b66ca4c9904`.
Go HEAD, status, and tracked binary diff remained
`3839514ef08186900fa72acc05e313760f725d68`,
`effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227`,
and `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`.
The ignored KMP companion spec, Go spec, OpenAPI, and corpus manifest hashes remained
`02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`,
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`,
and `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.
All 76 corpus manifest records verified. Fresh
`oversqliteCrossRepoConformance --rerun-tasks` passed in 1m27s; log
`crossrepo-stress-repair-refreeze-20260923.log` has SHA-256
`32297a508126bf3bae7c175c8e796706268b20967061533806e17ce629b7bde9`.

The mandatory `oversqliteRealserverAllHeavy --rerun-tasks` passed in 15m14s,
22 tasks executed, against another fresh run-owned PostgreSQL 16.15 database and pinned
Go server isolated on host port 8081. Android used the run-owned Pixel_4 API 31 emulator
through `10.0.2.2:8081`; host wrappers used `localhost:8081`. Android heavy reported
77 tests, Wasm browser 48, regular library JVM real-server 14, and JVM shared-connection
heavy two, each with zero failures/errors/skips. iOS simulator, JS Node, JVM harness,
and macOS arm64 heavy wrappers also passed. In particular, the prior Wasm fetch and
JVM 32-versus-4 failures did not recur. The aggregate log is
`qualification-20260921/realserver-all-heavy-stress-repair-20260923.log` (SHA-256
`b61562fa872b0984afc997662d29802f70b3c3090bcbe03588379d3fa4413964`);
per-target XML is preserved under
`qualification-20260921/realserver-all-heavy-stress-repair-20260923-reports/`.
The stress XML has SHA-256
`6471821f05582b89ac0a2b29251587ec7e3c25d69be33d98818149c03a88b15e`.
Run-owned Go server and PostgreSQL logs are `go-server-final-stress-repair-20260923.log`
(SHA-256 `2e19300d96a23664ad6a93da42f3a8c44108f6f8a6217d1796f4544cf8adb42a`)
and `postgres-final-stress-repair-20260923.log`.

Invalidation is bounded: this test-only change invalidated its owning JVM stress method
and the mandatory final heavy aggregate, both rerun successfully. The previously passed
comprehensive host, generated-production, plugin, general platform, Swift hybrid,
standard real-server, Swift live-server, and browser CORS-specific lanes remain retained
because neither their production inputs nor their exercised test behavior changed.
The earlier Go CORS edit is covered by its focused server-package test, browser-heavy
reruns, the fresh paired preflight, and this final browser-inclusive aggregate.
`git diff --check` passed in both repositories. After preserving logs, the run-owned
servers, databases, Docker networks, emulator, and temporary binaries were removed;
unrelated processes and containers were untouched. The ledger edit changes whole-worktree
identity but not source-bearing identity. No independent review, owner acceptance,
staging, commit, push, publication, deployment, or release occurred.

QUALIFICATION PASSED — AUTHOR VALIDATION; INDEPENDENT REVIEW PENDING

## Independent-review blockage and remediation — 2026-09-23 (in progress)

The independent source/evidence review of the preceding frozen candidate reported five
findings: operation errors fell through to generic push/pull transport errors; byte
batching could freeze only part of an FK dependency group; successful response intake
ignored admitted complete-body ceilings; nested pull bundles and push conflicts accepted
extra members; and the closeout lacked the specified seven-workload, external Fetch, and
current memory artifacts while the configured memory entry points were deleted. The
review's verdict was **REVIEW BLOCKED**. The preceding “QUALIFICATION PASSED — AUTHOR
VALIDATION” line remains a historical statement about its recorded runs, but is
**superseded for acceptance of the revised candidate**. Its final heavy pass is retained
as historical evidence, not carried forward as the revised candidate's final verdict.

The owner authorized KMP-only remediation and requalification. The sibling Go checkout
remains read-only. The first implementation pass now routes recognized operation errors
after the specialized decoders, carries dependency-closed groups into byte batching,
applies admitted push/pull/snapshot complete-body limits, rejects unknown closed-object
members in nested pull bundles and push conflicts, restores compact-protocol memory
entry points, and adds a large-pull profile. A memory-profile failure identified two
additional KMP allocation spikes in the same path: whole-body strict UTF-8 conversion
and whole-bundle canonical hash materialization. Byte validation followed by decoding
retains malformed-UTF-8 rejection; incremental SHA-256 over the same canonical row bytes
retains the bundle hash. These additional fixes invalidate the same KMP lanes and are
included in the revised candidate.

Focused `RemoteApiReviewRegressionTest`, `BundleHashTest`, and the FK-cycle hard-byte
limit test pass. The 25,000-row, 1,024-byte-target compact pull profile passed in a fresh
128 MiB-heap JVM: decoded body 24,875,217 bytes, peak heap 119,286,272 bytes, peak RSS
245,792,768 bytes (below the retained 300 MiB RSS gate). Earlier unchanged-heap attempts
before the two allocation fixes failed with `OutOfMemoryError` and are superseded, not
recorded as passes. Fresh `oversqliteCrossRepoConformance --rerun-tasks` passed with Go
OpenAPI SHA-256 `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`.

The fresh compact snapshot-memory aggregate, broader host/platform/Swift/live-server
lanes, seven workload measurements, external browser capture, final heavy aggregate,
and post-freeze identity audit remain pending. No independent approval, owner acceptance,
staging, commit, push, publication, deployment, or release is claimed here.

### Remediation qualification progress on the revised source

After the final source edit, SQLiteNow HEAD was
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`; its source-bearing tracked diff
excluding this ledger was SHA-256
`0c880bd4d87bc7adacf825319b058ea79703ab2b299d1b6156da658c4766003e`, and
its untracked content-manifest hash was
`acf53e28b21868f26e6e087445c54a9071ae1993fa918a60a3172726b83ef276`.
Before this ledger amendment, its status hash was
`b24d438beb49f5c1f679a5b7f366ca708c913d8a8630a85e1c5e23cec020e113`.
Go HEAD, status hash, and tracked binary diff were respectively
`3839514ef08186900fa72acc05e313760f725d68`,
`effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227`, and
`8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`;
Go source was not edited. Direct SHA-256 of the ignored KMP spec, Go p0000 spec,
Go OpenAPI, and KMP corpus manifest was respectively
`11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`,
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`, and
`0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.
All 76 manifest records verified with `shasum -a 256 -c`.

The focused new HTTP, FK-byte-limit, UTF-8, and streaming-hash regressions passed.
Fresh `oversqliteCrossRepoConformance --rerun-tasks`, `oversqliteComprehensive
--rerun-tasks` (2m45s), and `oversqlitePlatformAll --rerun-tasks` (9m54s) passed.
The platform aggregate included Android, JVM, iOS simulator, macOS arm64, JS Node,
JS browser, and Wasm browser. `oversqliteCompactMemoryJvm --rerun-tasks` passed
all four compact snapshot closure profiles, the sampler control, and the large-pull
profile. Results are preserved in `compact-memory-results-20260923.md` (SHA-256
`1639d465752f6360f569abecb3abe6a607d019aaebc71a5af6666b5596422c94`).

All seven section 9.2 workloads have raw loopback HTTP body counts, identity/gzip
sizes, ratios, selected encoding, and byte/SHA-256 equality in
`wire-workload-measurement-20260923.md` (SHA-256
`094cd08a16d2966a05349505b7a87fb3b47b76a2e6866eb70ec0c25d8d6c644f`).
Request fixture measurements use the compiled KMP encoder; response fixture
measurements use JDK gzip and are not claimed as live Go counts. An external
Chrome Fetch/CDP capture against the pinned Go server is in
`browser-fetch-capture-20260923.md` (SHA-256
`285c780740864e851c4a62f854ac48f4ee6ec72839fed6f6ed52ecfdd7b5a5e6`):
Go emitted a gzip generation descriptor with 751 network response-body bytes while
Fetch exposed 4,152 decoded bytes, and the browser's gzip-marked push obtained a
successful OPTIONS preflight. The deliberately malformed push returned HTTP 400;
this artifact does not claim successful push semantics.

The post-edit Drymint baseline-backed inspection needed one local index refresh,
then attributed the revised source. Its only inspect-level advisory compares two
test client-construction helpers with different mock/live defaults and roles;
source inspection found no safe, relevant refactor for this remediation. This
advisory is not independent acceptance. Compiler/plugin implementation inputs
and the sibling Go source were not edited by this pass, so the preceding separate
compiler/plugin and Go package evidence is retained as historical unchanged-input
evidence; the fresh cross-repository gate was still rerun.

The ordinary real-server, Swift live-server, final heavy, and postflight identity
gates were subsequently completed below. The old heavy pass remains superseded.

### Revised-candidate closeout — author qualification, 2026-09-23

The frozen source candidate passed `swiftHybridSupportGate --rerun-tasks`
(6m41s) and `oversqliteRealserverAll --rerun-tasks` (15m13s). The ordinary
real-server aggregate ran Android, JVM library and harness, iOS simulator,
macOS arm64, JS Node, JS browser, and Wasm browser against the pinned Go server
and an isolated PostgreSQL container. Its log is
`realserver-all-remediation-20260923.log` (SHA-256
`a02eb5de479c70a259296d5da2daf8387209e7cc19bf366ed8f4b57d29301a44`).
The Android `byteTargetKeepsNewForeignKeyCycleInOneFrozenBundle` regression
executed and passed in this live lane.

`swiftHybridSupportRealserverGate --rerun-tasks` passed (6m12s) on that server.
The generated sync-package `swiftRealserverSmoke` ran both real-server tests
(`testFreshReaderRestoresTypedSnapshotExactly` and
`testGeneratedSyncPackagePushesAndPullsAgainstRealserver`) with zero failures;
the ordinary Swift package task's skips are not being counted as live evidence.
Its log is `swift-live-remediation-20260923.log` (SHA-256
`bf42122ef1970b8147d808b8867aba577df5e74b605d918c8aeaf789a2265c7f`).

For the mandatory final heavy aggregate, the run-owned Go process and database
were stopped and a new PostgreSQL 16.15 container was started on a separate
host port. The same pinned Go binary was restarted; `/syncx/status` reported
`committed_bundle_count=0` before tests. `oversqliteRealserverAllHeavy
--rerun-tasks` passed in 14m10s. Android heavy (6m47s), iOS simulator heavy
(1m55s), JS Node heavy (57s), JVM harness heavy (48s), macOS arm64 heavy
(1m46s), Wasm browser heavy (1m24s), JVM real-server, and JVM shared-connection
stress all passed. The defined heavy aggregate has no JS-browser-heavy task;
JS browser passed in the ordinary real-server aggregate. Heavy log:
`realserver-heavy-remediation-20260923.log` (SHA-256
`94bade18c33f9aea21514a4e90b318fdea64203e0e9790c8b788feac0b6bc089`).

The fresh-JVM 128 MiB heap / 300 MiB RSS memory gate remained unchanged and
passed. The largest snapshot closure profile (100,000 rows, 1,024 target bytes)
had a 995,093-byte maximum decoded chunk body, 88,585,536-byte peak heap, and
237,584,384-byte peak RSS. The 25,000-row, 1,024-target-byte large pull had a
24,875,217-byte decoded body, 116,467,712-byte peak heap, and 251,084,800-byte
peak RSS. The complete profile table, sampler control, and provenance are in
`compact-memory-results-20260923.md` at the hash recorded above. The seven
workload and external browser Fetch measurements at their recorded hashes are
part of this author-qualification packet, not a claim of live-Go equivalence
for every synthetic response fixture.

Postflight KMP HEAD, source-diff hash, status hash, and untracked-content
manifest matched the frozen values above. Go HEAD, status hash, and tracked
diff hash also matched; the sibling Go checkout was never edited. The ignored
KMP/Go specifications, Go OpenAPI, and corpus-manifest hashes matched; all 76
manifest records verified again. `git diff --check` passed in both checkouts.
No source change occurred after the freeze, so the focused, contract, host,
platform, Swift, live-server, and memory results are for the same candidate.

**Verdict: AUTHOR QUALIFICATION PASSED for the revised KMP candidate.** The
review blockage is remediated in source and author evidence, but independent
rereview, owner acceptance, and delivery remain separate pending gates. Nothing
was staged, committed, pushed, published, deployed, or released.

After evidence capture, the two run-owned ephemeral PostgreSQL containers,
their temporary databases, the pinned-server process and temporary binary,
the Android emulator, and the iOS simulator were stopped or removed. Their
temporary database contents are not recoverable from those containers; the
ledger and measurement/test logs remain in this checkout. A different `web`
process later held port 8080 and was left untouched.

### Independent-rereview follow-up — malformed terminal errors, 2026-09-23

The independent rereview of the preceding frozen candidate identified one remaining
[P2]: malformed JSON-shaped terminal push/pull HTTP errors could evade typed
decoding or emerge without a non-retryable protocol classification. The owner
authorized remediation. The preceding **AUTHOR QUALIFICATION PASSED** verdict is
historical for its exact candidate and is superseded for acceptance of this edit.
This is KMP-only behavior work; the sibling Go checkout, OpenAPI, and corpus wire
contract remain read-only and unchanged.

The KMP HTTP error boundary now strictly validates JSON-object-shaped bodies before
the specialized, contract, and operation-error decoders. Malformed JSON-shaped
bodies throw a redacted `RemoteResponseDecodeException` in the PROTOCOL category;
plain intermediary text retains the generic HTTP transport fallback. The shared
object-member scanner now validates JSON scalar grammar, including ignored fields,
so an unquoted token, leading-zero number, or incomplete exponent cannot slip
through. Direct error decoders preserve their prior `null` response for non-object
text. `RemoteResponseDecodeException` and `RemoteResponseSemanticException` are
terminal to the automatic-download retry path. HTTP-level tests cover malformed
pull/push bodies, a malformed extra scalar, truncation, terminal classification,
and the plain-text fallback; the direct scalar-grammar and pre-existing typed
terminal/retryable tests remain active.

During repair, a first focused run exposed permissive scalar handling; a second
exposed the plain-text compatibility behavior; an initial comprehensive run hit
the direct-decoder plain-text expectation and was stopped after that failure.
Those runs are failed/superseded investigation evidence, not qualification passes.
Focused HTTP, generation, and contract-transport tests subsequently passed after
the decoder adjustment. A first complete candidate then passed paired conformance,
comprehensive host, all seven platform targets, compact memory, Swift support,
and ordinary real-server aggregate. Its logs are respectively
`crossrepo-error-rereview-fix-20260923.log`,
`comprehensive-error-rereview-fix-20260923.log`,
`platform-all-error-rereview-fix-20260923.log`,
`compact-memory-error-rereview-fix-20260923.log`,
`swift-hybrid-error-rereview-fix-20260923.log`, and
`realserver-all-error-rereview-fix-20260923.log` under
`qualification-20260921/`. Its Swift live rerun was interrupted after a late
source audit found the automatic-download retry gap. These results are now
**superseded for the second source revision**, not silently carried forward.
The run-owned Go server and first isolated PostgreSQL container were stopped;
their artifacts remain available, and no unrelated container was touched.

The second source revision is frozen at KMP HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`, tracked source-diff SHA-256
`1ba8814e69f05242a7a4502cd742ce89d08bf9deae137c3f6962e988b20ddc78`
(excluding this ledger), untracked content-manifest SHA-256
`52a0c31743d1f096894170ab04a33186d9877b5847d8e1db91bc334a01c8ae93`,
and pre-ledger status SHA-256
`b24d438beb49f5c1f679a5b7f366ca708c913d8a8630a85e1c5e23cec020e113`.
Go HEAD, status, and tracked diff remain the pinned
`3839514ef08186900fa72acc05e313760f725d68`,
`effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227`,
and `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`.
The ignored KMP spec, Go spec, OpenAPI, and corpus manifest hashes remain the
four values recorded in the preceding closeout; all 76 manifest entries verify.

On the second revision, the focused HTTP/terminal tests passed and fresh
`oversqliteCrossRepoConformance --rerun-tasks` passed (log
`crossrepo-terminal-error-fix-20260923.log`). The comprehensive, platform,
Swift-facing, real-server, compact-memory, and mandatory final heavy gates are
being rerun or separately evaluated below. The seven-workload and external
browser Fetch artifacts from the preceding closeout are retained as unchanged
wire/Go-input measurements, not newly rerun client-runtime evidence. No
independent rereview of this second revision, owner acceptance, or delivery is
claimed.

### Terminal-error second-revision closeout — author qualification, 2026-09-23

The second frozen KMP source revision passed the final focused HTTP, generation,
and contract-transport tests (`focused-terminal-error-fix-20260923.log`, SHA-256
`14fc7205cde94fcdce163407169b65836cb20c34f2b787fc5a1712c2a5155c12`).
The paired contract gate remained unchanged: all 76 corpus-manifest records
verified, and `oversqliteCrossRepoConformance --rerun-tasks` passed
(`crossrepo-terminal-error-fix-20260923.log`, SHA-256
`ef973fc4eb9d3d767d542d84d60179b797a91f4f195935fce8b2cecedd8235cb`).
Fresh `oversqliteComprehensive --rerun-tasks` passed in 3m
(`comprehensive-terminal-error-fix-20260923.log`, SHA-256
`5876373a997cf797d3cfbf1f467ac970ce5fba06d20c31491375417ba316f5fb`).

`oversqliteCompactMemoryJvm --rerun-tasks` passed all four compact snapshot
closure profiles, the sampler control, and the large-pull profile under the
unchanged fresh-JVM 128 MiB heap / 300 MiB RSS gates. Its log is
`compact-memory-terminal-error-fix-20260923.log`, SHA-256
`10c9b65217c38bfe7cb7dd21b18fabf8035e2bfae682e68cb20fbbedd51eab68`.
The 100,000-row / 1,024-target-byte snapshot profile had a 995,093-byte
maximum decoded chunk, 87,505,952-byte peak heap, and 216,678,400-byte peak
RSS. The 25,000-row / 1,024-target-byte large pull had a 24,875,217-byte
decoded body, 107,496,320-byte peak heap, and 247,824,384-byte peak RSS.

`oversqlitePlatformAll --rerun-tasks` passed in 7m42s across Android, JVM,
iOS simulator, macOS arm64, JS browser, JS Node, and Wasm browser
(`platform-all-terminal-error-fix-20260923.log`, SHA-256
`c4770e5fa0f1319cb4afe65a7366170d76a6d9a93064553b78dc721b16d8e6c2`).
`swiftHybridSupportGate --rerun-tasks` passed in 6m10s
(`swift-hybrid-terminal-error-fix-20260923.log`, SHA-256
`a1bf1defcd94e40903886a07cc596cd6f69a2a8e186263773e4691a574d8f9a1`).

The ordinary `oversqliteRealserverAll --rerun-tasks` passed in 14m37s on a
new run-owned PostgreSQL 16.15 database, the pinned Go integration server,
the run-owned Pixel_4 API 31 emulator, and the iPhone 17 Pro simulator. Before
the run, `/syncx/status` reported `committed_bundle_count=0`. Android, iOS,
JS browser, JS Node, JVM library and harness, macOS arm64, and Wasm browser
wrappers passed. The log is `realserver-all-terminal-error-fix-20260923.log`,
SHA-256 `dda0491c523833d6d41abc93a79c83d57b93045194e9c3072eb2fa2353495452`.
`swiftHybridSupportRealserverGate --rerun-tasks` then passed in 6m11s
(`swift-live-terminal-error-fix-20260923.log`, SHA-256
`30de2b97c39e0727187ad86c1d6471de87f33f08a6936bb5b8aafb78a85e0623`).
Its explicit `swiftRealserverSmoke` ran both generated sync-package tests,
`testFreshReaderRestoresTypedSnapshotExactly` and
`testGeneratedSyncPackagePushesAndPullsAgainstRealserver`, with zero failures;
the ordinary package task's two skips were not counted as live evidence.

The ordinary server/database were stopped, and the mandatory final
`oversqliteRealserverAllHeavy --rerun-tasks` used another fresh PostgreSQL
16.15 database and the same pinned Go binary; `/syncx/status` again reported
`committed_bundle_count=0` before the run. The aggregate passed in 16m42s,
including Android heavy, iOS simulator heavy, JS Node heavy, JVM harness heavy,
macOS arm64 heavy, Wasm browser heavy, JVM real-server, and JVM
shared-connection stress. Its log is
`realserver-heavy-terminal-error-fix-20260923.log`, SHA-256
`a0280090a3bfaab210549172d5d391bc9db81b77bf23d1df468c9e69a0227223`.
Copied reports under `realserver-heavy-terminal-error-fix-20260923-reports/`
show Android heavy 78 tests with zero failures/errors/skips (`android-heavy.xml`,
SHA-256 `5981b99b2a67069243f50522f495447c2486ac577224e509a3e3b8b0845c2e1f`)
and JVM shared-connection stress one test with zero failures/errors/skips
(SHA-256 `45a912e3dbada7f3096594e5d7a4acf5250f4905ef9c6c44cb9991d20ae9ea1e`).
The copied platform XML reports contain no nonzero failure/error entries.
The heavy aggregate does not define a JS-browser-heavy task; JS browser passed
in the ordinary live aggregate.

Run-owned Go server logs for ordinary and heavy are respectively SHA-256
`a415806ea6fb965de99b58aa9ae5ae89038f66797cdb28244389cea78e471aa7`
and `cd36c90653ecb9f1c06c2a043e0fc2d42771831dd208e0512d804b30b0413136`;
PostgreSQL logs are SHA-256
`ec5f38be3ca4879812ceb7699b5c4192c7dd2fddfd4958248bc5a29354b75f73`
and `484a3c1f047d46a55a79ecae512e4e25783b36dce807e9c02e46df1015fea950`.
The seven-workload identity/gzip artifact remains SHA-256
`094cd08a16d2966a05349505b7a87fb3b47b76a2e6866eb70ec0c25d8d6c644f`;
it records decoded/on-wire size, ratio, encoding choice, equality/hash checks,
and provenance for each workload. The external browser Fetch capture remains
SHA-256 `285c780740864e851c4a62f854ac48f4ee6ec72839fed6f6ed52ecfdd7b5a5e6`.
These two artifacts are retained from unchanged encoder/Go wire inputs, not
misrepresented as new measurements of the edited error classifier.

Postflight KMP HEAD, source-bearing tracked diff, untracked content manifest,
and status SHA-256 matched the second-revision freeze above. Go HEAD, status,
and tracked diff also matched; the sibling checkout was not edited. The ignored
KMP/Go specifications, Go OpenAPI, and corpus-manifest hashes remained pinned;
all 76 corpus entries verified again. `git diff --check` passed in both
checkouts. The post-edit Drymint baseline-backed review had an index-snapshot
mismatch after its one-shot refresh; its emitted explicit-path fallback was
clean but cannot provide baseline attribution, so it is not claimed as
independent rereview. No source edit occurred after the second freeze.

**Verdict: AUTHOR QUALIFICATION PASSED for the second-revision KMP candidate.**
The [P2] review issue is remediated in source and author evidence. Independent
rereview of this revised source, owner acceptance, and delivery remain distinct
pending gates. Nothing was staged, committed, pushed, published, deployed, or
released. After evidence capture, the three run-owned temporary PostgreSQL
containers/databases, server processes, temporary Go binary, Android emulator,
and iOS simulator were stopped or removed. Their ephemeral database contents
are not recoverable; logs and copied test reports remain in this checkout.

### Final error-root audit extension — third source revision, 2026-09-23

After the second-revision author pass, a read-only inspection of the HTTP
boundary found that non-object JSON roots (`[]`, a quoted JSON string, a JSON
number, or a Boolean literal) still took the generic transport fallback on
push/pull HTTP errors. This is another shape of the same [P2] finding. The
second-revision **AUTHOR QUALIFICATION PASSED** verdict above is retained as
historical evidence for that exact source but is **superseded for acceptance**.
No independent rereview or owner acceptance is inferred from it.

The KMP error boundary now recognizes object, array, quoted-string, numeric,
and complete Boolean/null JSON roots as JSON-shaped. Its existing strict
scanner and object check reject non-object or malformed roots as terminal
redacted PROTOCOL decode failures. Plain intermediary text, such as
`upstream-unavailable`, still uses the generic HTTP fallback. HTTP regressions
now exercise array, quoted-string, number, and Boolean error bodies as well
as malformed objects. The focused review, generation, and contract-transport
tests passed after this edit. No Go, specification, OpenAPI, or corpus edit
was made.

The third source revision is frozen at KMP HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`, tracked source-diff SHA-256
`0bef5af00d0c4ee5a8a0008df84c413708dd73d9886c8fe4045cb916ac2308bb`
(excluding this ledger), untracked content-manifest SHA-256
`8bfba842522f269858bfaa9f49b6db8c7047f84ff0c3e774bf2b4426f096c7bb`,
and status SHA-256
`b24d438beb49f5c1f679a5b7f366ca708c913d8a8630a85e1c5e23cec020e113`.
The second-revision focused, paired, host, platform, Swift, memory, ordinary
live, Swift live, and heavy runs above are not claimed as third-revision
passes. Their unaffected wire-measurement and Go-input artifacts are retained
with their stated provenance. Fresh invalidated-lane reruns and the mandatory
final heavy aggregate are recorded below.

### Heartbeat test isolation and final test-only adjustment, 2026-09-23

The first third-revision `oversqliteComprehensive --rerun-tasks` attempt failed
one of 241 JVM tests: `automaticDownloads_heartbeatsDoNotPreventEofFallbackPull`
observed `pullRequestCount=0` immediately after a fixed 100 ms delay. Log:
`comprehensive-json-root-fix-20260923.log`. An isolated run of the exact
method, with the run-owned emulator and simulator stopped to reduce load,
failed the same assertion (`isolated-heartbeat-json-root-fix-20260923.log`).
This was not treated as an aggregate timeout or retried unchanged. The mock
records its watch request before the asynchronous response has been consumed;
the test's fixed delay did not prove that EOF fallback had executed. The test
was changed to await an observed watch request **and** the fallback pull, then
assert the first watch cursor was zero. No production runtime code changed.
The exact isolated method passed (`isolated-heartbeat-fixed-json-root-20260923.log`),
and the full `oversqliteComprehensive --rerun-tasks` passed in 2m42s
(`comprehensive-json-root-heartbeat-fix-20260923.log`). The earlier failed
logs are retained as superseded investigation evidence.

The final test-adjusted candidate retains KMP HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd` and the third revision's
untracked content-manifest SHA-256
`8bfba842522f269858bfaa9f49b6db8c7047f84ff0c3e774bf2b4426f096c7bb`.
Its tracked source/test diff excluding this ledger is SHA-256
`095255d472baa5092fc5d55df4fcd42a295b76cd1ce0902e624b1d31672750bc`,
and status SHA-256 remains
`b24d438beb49f5c1f679a5b7f366ca708c913d8a8630a85e1c5e23cec020e113`.
The production runtime source is byte-identical to the third revision; only
the heartbeat test changed. The post-edit Drymint baseline-backed inspection
attributed this test edit and emitted an unrelated inspect-level similarity to
a signin helper; direct source inspection found no relevant refactor. The
third-revision cross-repository/focused evidence predates this test edit, so
the final gate reruns and source postflight are recorded below. The
third-revision verdict remains pending until those gates complete.

### Final test-adjusted candidate qualification and postflight — 2026-09-23

The final candidate above passed fresh focused HTTP/generation/contract tests,
paired conformance, the comprehensive host suite, memory profiles, all-platform
suite, Swift-facing hybrid support, ordinary live-server aggregate, Swift live
gate, and mandatory final heavy aggregate. These are author-validation runs,
not an independent rereview. Logs below are under `qualification-20260921/`.

| Lane | Result | Log and SHA-256 |
| --- | --- | --- |
| Focused JSON-root HTTP/generation/contract tests | pass, 29s | `focused-json-root-heartbeat-fix-20260923.log` `9c1eff1184e8cc6e912c446341ec35f5fe8cac1f35727db7e3f6250f8596eb66` |
| `oversqliteCrossRepoConformance` | pass, 45s | `crossrepo-json-root-heartbeat-fix-20260923.log` `380eb689a7db9859289d3e8a9339bc7a884da7e31708294edb2f7129eb40445d` |
| `oversqliteComprehensive` | pass, 2m42s | `comprehensive-json-root-heartbeat-fix-20260923.log` `a2424dfa1b6b51dc3713ee3e7ccea950e7a5845b8e747eaf187529363c52901a` |
| Compact memory profiles | pass, 1m05s | `compact-memory-json-root-heartbeat-fix-20260923.log` `47422fdc8fe2c1ca7791ff26faf3ee1abf9fc8124eb7b12b653f099729c8d5d9` |
| `oversqlitePlatformAll` | pass, 8m58s | `platform-all-json-root-heartbeat-fix-20260923.log` `8106716ed588b9bc64809110ac00e25ea62514dd0de246d3a1a20372cc5fea09` |
| Swift hybrid support | pass, 6m16s | `swift-hybrid-json-root-heartbeat-fix-20260923.log` `ec7ab44c3dd15b7d8f56e8b3c6aa5debb199454286458466fe0c444e5831d1ea` |
| `oversqliteRealserverAll` | pass, 17m09s | `realserver-all-json-root-heartbeat-fix-20260923.log` `9c02cccd7babe86e19909b0dc3714610797c84f199d505a5cbcc6042c8ca9132` |
| `swiftHybridSupportRealserverGate` | pass, 8m02s | `swift-live-json-root-heartbeat-fix-20260923.log` `ed3ebf9c76f93f952feb56cf73e42d638a124c9f03f1ca268dd36fec37404526` |
| `oversqliteRealserverAllHeavy` | pass, 13m58s | `realserver-heavy-json-root-heartbeat-fix-20260923.log` `caf5b267da453b55c5e23b7e0e88ed3984badf3e557d2308710e200bb11ba242` |

The memory lane retained its 128 MiB heap and 300 MiB RSS gates. Four snapshot
profiles (10,000/100,000 rows at 256/1,024 target bytes) passed, with maximum
decoded chunk bodies 227,091/995,091/227,093/995,093 bytes, peak heaps
80,615,904/81,740,768/84,961,440/89,235,776 bytes, and peak RSS
230,719,488/243,253,248/230,064,128/226,197,504 bytes. The sampler
control passed. Large pull (25,000 rows, 1,024 target bytes) passed with
24,875,217 decoded body bytes, 120,815,624 peak heap bytes, and 248,250,368
peak RSS bytes. No gate was relaxed.

The ordinary live aggregate used a pinned Go binary and fresh, run-owned
PostgreSQL 16 database. Android, iOS simulator, JS browser/Node, JVM
harness/library, macOS arm64, and Wasm browser live lanes passed. The explicit
Swift `swiftRealserverSmoke` then executed both
`testFreshReaderRestoresTypedSnapshotExactly` and
`testGeneratedSyncPackagePushesAndPullsAgainstRealserver` with zero failures.
The ordinary Swift package task skipped those tests without the live selector;
its skips are not counted as live evidence. Other pilot/schema-evolution
selectors remained outside this gate.

The ordinary server/database were stopped. A fresh PostgreSQL 16 heavy
database and the same pinned Go binary were started on the dedicated network;
`/syncx/status` reported `committed_bundle_count=0` before stress. Heavy passed
Android (6m36s), iOS simulator (1m52s), JS Node (56s), JVM harness (49s),
macOS arm64 (1m45s), Wasm browser (1m25s), JVM live-server, and JVM
shared-connection stress. The aggregate defines no JS-browser-heavy task;
JS browser passed in ordinary live. Copied XML in
`realserver-heavy-json-root-heartbeat-fix-20260923-reports/` includes Android
heavy (78 tests, zero failures/errors/skips; SHA-256
`d4507e87a8df19c9551ba7e86b4cf53188fa558e1e70e0ea5a298e354b1bbe9d`),
JVM harness shared-connection stress (one test, zero failures/errors/skips;
`3c12e5fbf63bbdda779160420d4489e9a5f7d803d2cfffef6e2c8af73ff1561d`),
and JVM library shared-connection stress (one test, zero failures/errors/skips;
`2addc0da2cdd2fbd2e80ac12b00cea9b17f7f34f34b3d8d65f3aede88be7c5d0`).

Ordinary Go-server/PostgreSQL logs have SHA-256
`5d71e0a3e417b1a5263090bdee70e5652d4ef77dcc4f6d919a557c9db7afff92` /
`87269275c209f049cd6cf67d5ab3a9dca2997e97073426796342ea55ce3d0cbf`;
heavy logs have SHA-256
`eef9655b4b5e826c964c9c958e3d256c4d0984d144786c6598434cc0f54069f3` /
`79c475fc8fb32039c089ae12d2c6e9bb1d35cd5e468659a050d527db32c447f2`.
The previously recorded seven-workload identity/gzip artifact
`wire-workload-measurement-20260923.md` (SHA-256
`094cd08a16d2966a05349505b7a87fb3b47b76a2e6866eb70ec0c25d8d6c644f`)
retains decoded/on-wire size, ratio, encoding, semantic/hash equality, and
provenance per workload. The external browser Fetch capture
`browser-fetch-capture-20260923.md` (SHA-256
`285c780740864e851c4a62f854ac48f4ee6ec72839fed6f6ed52ecfdd7b5a5e6`)
remains valid for unchanged Go/CORS/encoder inputs. These reused artifacts
do not substitute for the fresh KMP error-classifier tests. Prior generator/Go
evidence was reused only where its inputs were unchanged; the source/test
change triggered the fresh KMP gates above, including mandatory final heavy.

Postflight KMP HEAD remained `34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`,
tracked source/test diff excluding this ledger SHA-256
`095255d472baa5092fc5d55df4fcd42a295b76cd1ce0902e624b1d31672750bc`,
untracked content-manifest SHA-256
`8bfba842522f269858bfaa9f49b6db8c7047f84ff0c3e774bf2b4426f096c7bb`,
and porcelain status SHA-256
`b24d438beb49f5c1f679a5b7f366ca708c913d8a8630a85e1c5e23cec020e113`.
Read-only Go HEAD/status/tracked-diff remained
`3839514ef08186900fa72acc05e313760f725d68` /
`effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227` /
`8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`.
Ignored KMP companion spec, Go p0000 spec, Go OpenAPI, and corpus manifest
remained at `11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`,
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`,
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`,
and `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.
All 76 manifest records verified; `git diff --check` was clean in both
repositories. No Go source, governing contract, or OpenAPI file was modified.

The run-owned Android emulator, iOS simulator, disposable ordinary/heavy Go
servers and PostgreSQL databases, Docker network, and temporary binaries
were stopped and removed after capturing logs. The isolated database contents
were not retained. No unrelated container or process was touched.

**Author qualification passed for the final test-adjusted candidate.** Prior
verdicts and failures remain historical/superseded as marked above. This does
not claim independent rereview, owner acceptance, staging, commit, push,
publication, deployment, release, or delivery.

### Owner acceptance of the author-qualified candidate — 2026-09-23

The owner stated: “I'm willing to accept this work without additional
reviews.” This accepts the final test-adjusted KMP candidate whose author
qualification passed above. The owner elects to proceed without another
independent rereview after the latest one-finding rereview and its remediation.
The final candidate **has not independently passed rereview**; that gate is
waived for this acceptance, not represented as completed.

The accepted identity was reverified before this ledger entry: KMP HEAD
`34b63a0714f4bbe67ed57fb7027d140d5cdf3bfd`, tracked source/test diff
excluding this ledger SHA-256
`095255d472baa5092fc5d55df4fcd42a295b76cd1ce0902e624b1d31672750bc`,
untracked content-manifest SHA-256
`8bfba842522f269858bfaa9f49b6db8c7047f84ff0c3e774bf2b4426f096c7bb`,
and porcelain status SHA-256
`b24d438beb49f5c1f679a5b7f366ca708c913d8a8630a85e1c5e23cec020e113`.
The read-only Go HEAD/status/tracked-diff and both governing specifications,
OpenAPI, and corpus manifest also matched the postflight pins above. This
acceptance covers that candidate only. It does not authorize or imply staging,
commit, push, publication, deployment, release, or delivery.

### Request-gzip platform-engine implementation attempt — blocked on active workload contract, 2026-09-23

This section is an **author-validation blocker**, not a frozen or accepted candidate. The implementation specification `specs/oversqlite-platform-gzip-implementation.md` was present and SHA-256 `7ff96ef7cde457a5f6fe988c1a6f79e03091c53b13d52079aac8757d6b42bbbd`. Preflight KMP HEAD was `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4` with clean tracked, staged, and untracked status. The ignored KMP p0000 specification was `11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`; the 76-entry corpus manifest was `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`. Go HEAD/spec/OpenAPI/status/diff matched the specification's pins, including the two pre-existing `setup.go` and `setup_test.go` edits. Go remained read-only.

Source changes replace the common fixed-Huffman request writer with a suspending selector and capped JVM/Android, zlib Native, JS, and Wasm `CompressionStream` adapters. Connect and snapshot-session creation use the 65,536-byte control limit; push receives the admitted request limit explicitly. Request bytes are selected before Ktor builders. Response decompression was untouched. `RequestGzipTest` exercises the runtime selector, exact and overflowing caps, admission before engine/HTTP, headers, engine error, and cancellation; a large-payload pinned-Go smoke passed after a test-only observer was removed. The initial live smoke failed solely because an observer installed on the source Ktor client did not see requests from the derived production client; `jvm-live-smoke.log` is superseded by `jvm-live-smoke-r2.log`.

| Development/focused lane | Leaf result at the time run | Artifact |
| --- | --- | --- |
| JVM `jvmTest` focused request/snapshot tests | executed, pass; later test-only changes require refresh before freeze | `platform-gzip-20260923/jvm-focused.log` SHA-256 `e3ab5b6cf6e21255d789ace06be091caa6d5b55c403badfa457020c1410f464d` |
| JS Node `jsNodeTest` | executed, pass, including five then-current request-gzip tests | Gradle XML under `library-oversqlite/build/test-results/jsNodeTest/`; later tests changed |
| Wasm browser `wasmJsBrowserTest` | executed, pass, including five then-current request-gzip tests | Gradle XML under `library-oversqlite/build/test-results/wasmJsBrowserTest/`; later tests changed |
| JS browser `jsBrowserTest` | executed, pass, including five then-current request-gzip tests; webpack printed pre-existing Node-only test utility diagnostics | `platform-gzip-20260923/js-browser-focused.log` SHA-256 `911e068d699d95c9daec216ccfac8324e444c5992cc23be726a5d105f9d55a6e` |
| macOS Native `macosArm64Test` and iOS Native `iosSimulatorArm64Test` | executed, pass; test inputs subsequently changed | `platform-gzip-20260923/ios-focused.log` SHA-256 `b9497c1194c86b54a6e5ff20e7c8fdc59354500248e476084ce17dbe7c232238` plus Gradle macOS XML |
| Android `connectedAndroidDeviceTest` | executed on run-owned Pixel_4 emulator, pass; `RequestGzipTest` XML has six tests, zero failures/errors/skips | `platform-gzip-20260923/android-focused.log` SHA-256 `31e5899480fe8d88163c5c943a67b690b461e720a0e5c83cef732ec013035d66` |
| 76-entry manifest integrity | all entries passed `shasum -a 256 -c` | corpus manifest pin above |
| `oversqliteCrossRepoConformance` | executed, pass, no Go edits | `platform-gzip-20260923/crossrepo-conformance.log` SHA-256 `eec3369ad4bc7ff6388424f59f63e85115ad2d901677dbd7cff280d9cdbdd023` |
| JS production library packaging | executed, pass | `platform-gzip-20260923/js-production-package.log` SHA-256 `7a2389f62541fbf09293bbab6d7a721664b207164ab32650a5a99db9147716d5` |
| Narrow JVM pinned-Go smoke | first run failed test observer assertion after successful push; revised test executed and passed | `platform-gzip-20260923/jvm-live-smoke.log` SHA-256 `12d792c2ddf409d40d5cef9fbc95642aa9e35e85aee198d524a05c6b4f4b15f9` superseded by `jvm-live-smoke-r2.log` SHA-256 `d8f33e215cbaed8cf59755c3b211346695c50fd19d3da187d0393056a89c7b97` |

**Contract blocker.** The active `oversqlite-contracts/wire/compression.json` workload `deterministic-incompressible-above-threshold` requires identity. The retained seven-workload generator produces the same compact 5,010-byte ASCII JSON body used in the earlier measurement. With the maintained JVM `GZIPOutputStream` engine, the compiled KMP request selector produced a complete 4,165-byte gzip member and selected gzip (wire ratio 0.8313). The prior fixed-Huffman artifact had selected identity for that body. The refreshed raw-loopback measurement `platform-gzip-20260923/seven-workload.csv` has SHA-256 `5a99cdc8df5456b31452828b5ccb56c59cf4bb73d7389999e0185989bce2232b`; its adapted source `WireWorkloadMeasurement.java` has SHA-256 `0fab083a8d10f5a1c049e68a9b470c879089f4e952dbe27bc8e0ac8416ef3933`. It ran the compiled KMP JVM selector, inflated the selected wire body, and verified byte and SHA-256 equality. This mismatch is not a compression failure: the stronger platform engine follows the p0000 smaller-than-identity rule. The workload's explicit expected identity conflicts with that rule for these inputs. Per the implementation specification's stop condition, no corpus, specification, Go contract, or OpenAPI change was made.

The candidate was **not frozen**. Platform, ordinary live-server, Swift live, Linux runtime, refreshed browser Fetch capture, memory, and mandatory final heavy gates were not started as qualification after the conflict. The earlier seven-workload and browser artifacts remain historical/superseded for request-compressor provenance. The development tests above are not a completed author-validation campaign; late focused-test edits were not uniformly rerun. The final author-validation verdict is **BLOCKED by active workload contract conflict**, requiring a paired contract decision before this implementation can proceed to acceptance. No independent review or owner acceptance is claimed for this candidate.

Postflight KMP HEAD remained `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4`. Tracked binary diff SHA-256 is `8f4b90fe9cf97b487cc1dd4f461a6f67275188155826d15d6d1e35a4b124bf17`, untracked source content-manifest SHA-256 is `643b979e48f72979ad67917ace9621f42a4e668007e8982c78160e71fa42cbad`, and porcelain status SHA-256 is `002bce5c17ed2a6e47e9d8fe031480799b97e8f8716658f0d7f4cb418c8db75a`. `git diff --check` passed. Go HEAD/status/diff remained `3839514ef08186900fa72acc05e313760f725d68` / `effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227` / `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`; Go spec and OpenAPI hashes remained `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f` and `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`. The run-owned Go server, PostgreSQL container, and Android emulator were stopped. No files were staged or committed, and nothing was pushed, published, or deployed.

A post-edit Drymint `review.change.workspace_paths` current-state inspection reported `clean` with no duplicate follow-up; no prewrite baseline was captured, so this is explicitly unattributed to the edit and does not resolve the contract blocker.

### Request-gzip workload-blocker correction — 2026-09-23

The preceding section's **contract blocker and final BLOCKED verdict are superseded**. Its seven-workload generator converted xorshift output to a 94-character printable alphabet, which made its 5,010-byte JSON body compressible. That body did not match the active corpus consumer's 4,096-byte xorshift **byte** generator. The compiled KMP JVM runtime selector chooses identity for the exact 2,048- and 4,096-byte corpus generator bodies; the 4,096-byte body produces a 4,119-byte platform gzip member and therefore correctly selects 4,096-byte identity. This is consistent with the active fixture's selected-encoding expectation and the strictly-smaller rule. No Go or corpus change is needed for this platform-engine implementation.

The corrected local seven-workload raw-loopback measurement is `platform-gzip-20260923/seven-workload-corrected.csv` (SHA-256 `9e7faf99e33d5a6ad4f809521093821cef2df66e1b75198ec927f50754fb694b`) from `WireWorkloadMeasurement.java` (SHA-256 `60167ba24fd9924ec059f5c47c48684125029df13f279f84eccb600cdc30ff46`). It runs the compiled KMP JVM selector, measures selected HTTP body bytes, and verifies byte and SHA-256 equality after decoding. The prior `seven-workload.csv` and its blocker inference are superseded. The corpus fixture's prose describes an ASCII JSON pad, while its executable consumer fills the pad with arbitrary xorshift bytes. The identity row is therefore a synthetic selector/transport probe, not a valid JSON request accepted by Go; live valid-JSON gzip acceptance remains a separate lane. This pre-existing description inconsistency does not prevent the platform engine from satisfying the executable selection case. Author validation resumes; no freeze or aggregate qualification is claimed by this correction.

### Request-gzip platform-engine author qualification — 2026-09-24

**Final author-validation verdict: PASS for the revised request-gzip candidate.** This supersedes the 2026-09-23 BLOCKED verdict, whose measurement input was wrong. It is a new candidate under `specs/oversqlite-platform-gzip-implementation.md` (SHA-256 `7ff96ef7cde457a5f6fe988c1a6f79e03091c53b13d52079aac8757d6b42bbbd`), not the previously accepted p0000 checkpoint. No independent verdict or owner acceptance is claimed for this candidate.

The final production source replaces the common custom fixed-Huffman writer with a suspending selector and bounded JVM/Android `GZIPOutputStream`, Native zlib, JS, and Wasm `CompressionStream` adapters. Connect and snapshot-session use the explicit 65,536-byte limit; push receives its admitted decoded request limit. Each body is selected before its Ktor request builder. Cap overflow selects identity, while cancellation and engine failure remain distinct. Response decompression is unchanged. Tests add endpoint limits, threshold/cap/error/cancellation cases, missing-engine probes, and valid large push/pull observations. Browser test packaging is enabled for the library.

The focused phase completed before broad freeze: JVM request/snapshot/contract tests (`jvm-focused-r2.log`); JS Node, JS browser, and Wasm browser (`web-focused-r3.log`, six `RequestGzipTest` cases per target); macOS and iOS (`native-focused-r2.log`, six cases per target); Android on the run-owned Pixel_4 (`android-focused-r2.log`, 144 tests, zero failures/errors/skips, six request-gzip cases); three JS/Node/Wasm missing-engine probes (`web-availability-refreeze2.log`, one executed per target); and `oversqliteCrossRepoConformance` (`crossrepo-r2.log`). The 76-entry corpus manifest passed `shasum -a 256 -c`. A first narrow JVM live smoke failed only because a test observer watched the wrong Ktor client; the corrected smoke passed (`jvm-live-smoke-r2.log`), and the first log is superseded. The active 4,096-byte xorshift fixture selected identity with a 4,119-byte JVM gzip member. Valid 4,096-character JSON push content was separately accepted by pinned Go in live tests.

| Qualification lane | Fresh leaf outcome and evidence |
| --- | --- |
| `oversqlitePlatformAll` | Passed 9m40s (`platform-all-freeze1.log`, SHA-256 `b9a73364d2590a976e75de9f0f5d3ba763a4c51531b0518b56b8831778965d54`). Android, iOS simulator, JS browser, JS Node, JVM, macOS arm64, and Wasm browser test leaves executed. This preceded only later test-source additions; production source was identical. The later availability probes executed independently, and every affected current live harness executed again below. |
| Linux x64 and ARM64 | Both binaries cross-linked from the macOS checkout and the `RequestGzipTest` filter ran on matching Linux amd64 and arm64 Docker hosts: six tests each, zero failures/skips (`linux-x64-runtime-request-gzip.log` SHA-256 `1540851269a85ccdcc02fcf8ec82afad03c00809912a6f11fe28eb896dcc71d1`; `linux-arm64-runtime-request-gzip.log` SHA-256 `fa762b860b5099207216643f21197aa3d6d53539cda214ef54035890ff39c0eb`). The direct Gradle Linux ARM64 attempt failed because Kotlin/Native did not register a runnable test task on that host; it is not counted as runtime evidence. |
| Ordinary `oversqliteRealserverAll` | Passed 18m26s against fresh PostgreSQL 16.15 and pinned Go (`realserver-all-refreeze2.log`, SHA-256 `25944e296d80905ea2a749746191125cdb1825cc0d066495957072255be1a1a4`). Android, iOS, JS browser, JS Node, JVM library and harness, macOS, and Wasm leaves executed serially. The common harness had ten live tests with zero failures/skips on each platform. Android/iOS/macOS library `RealServerComprehensiveTest` also accepted the valid large push. |
| Swift-facing live | `swiftHybridSupportRealserverGate` passed 7m40s (`swift-realserver-refreeze2.log`, SHA-256 `9b4a3dac2ff4e0d2a32908c71e6c338c448106a25ec6e650efd655d1146d5c6a`). Its dependent support/comprehensive tasks ran. The dedicated real-server selector executed both fresh-reader snapshot and generated push/pull Swift tests with zero failures; the ordinary Swift package's skipped versions do not count as live evidence. The later platform harness test edit does not feed Swift source or fixtures. |
| Browser request network | After the common harness test was extended to push 4,096 characters, both full JS browser and Wasm browser live leaves executed and passed through a streaming local forwarding proxy (`browser-live-proxy-streaming-refreeze3.log`, SHA-256 `a5b58ebe4ec86285f9b0dd1e826675888052dd71a9d1eb928c9059473671607a`). It captured JS browser gzip 313/4,760 wire/decoded bytes and Wasm gzip 315/4,760, both `Content-Encoding: gzip`, preflight 204, Go 200, and exact pulled content. Raw 524-record capture SHA-256 `49809ecf0e565b9d3a2e8c6a013ff8e2fffa30b12e844c90bb22789aec6db647`. Earlier proxy runs failed because `/sync/watch` SSE was buffered and because a browser `--tests` filter did not narrow the suite; those attempts are superseded, not counted. |
| External Fetch capture and size measurement | Clean-profile Chrome Fetch capture records gzip request bytes and CORS preflight (`browser-fetch-capture-refreeze2.json`, SHA-256 `f69ea250dcf2430d92f33a78b4543e340ff4b6d6f4d042ea4bd7fcc6d2ae24f6`). Its intentionally invalid push returned Go 400 and is network evidence, not acceptance evidence. The corrected seven-workload CSV (SHA-256 `9e7faf99e33d5a6ad4f809521093821cef2df66e1b75198ec927f50754fb694b`) records seven selected wire sizes, ratios, encodings, byte/hash equality, and JVM platform-engine provenance; previous seven-workload files are superseded. |
| Memory | `oversqliteCompactMemoryJvm` passed in 53s (`memory-refreeze2.log`, SHA-256 `558554964dc050fe8ed3530fc92c7d06200dda0173ec171250e8a907a97b7170`). Four closure profiles, sampler control, and 25k large pull executed. Large-pull peak heap 117,892,608 bytes and RSS 242,057,216 bytes, below 128 MiB and 300 MiB. |
| Mandatory final `oversqliteRealserverAllHeavy` | Passed 19m10s after the last source/test edit, against a second fresh PostgreSQL database (`realserver-heavy-refreeze3.log`, SHA-256 `96110ea230c195ed8e3ac239547133c3d31d1ed63a797d1bbdad53577b7b48a6`). Executed Android (78 tests), iOS (10), JS Node (10), JVM harness (10), JVM harness shared stress (1), JVM library shared stress (1), macOS (10), and Wasm browser (10), all zero errors/failures/skips in saved leaf XML under `platform-gzip-20260923/heavy-reports/`. The aggregate defines no JS-browser-heavy leaf; the current JS browser live leaf passed above. The database reported zero committed bundles before heavy and 1,762 after it. |

The later common harness edit only increased the first valid live push and added a pulled-content assertion. It did not alter production behavior. The mandatory heavy run re-executed that current harness on Android, iOS, JS Node, JVM, macOS, and Wasm; the streamed browser live run re-executed JS browser and Wasm. Repeating the unaffected platform, ordinary-live, and Swift aggregates would only rebuild campaign labels. An optional broad JS Node library real-server run failed in a browser-only legacy persistence stress case; isolated `RealServerComprehensiveTest` then executed ten JS Node live cases, including the large valid push, with zero failures (`js-node-comprehensive-isolated-r2.log`). Browser library comprehensive attempts that printed server-unavailable skips are not counted. Generator and Go campaigns were reused only because no inputs in those repositories changed.

Postflight KMP HEAD is `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4`. The tracked source/test binary diff excluding this ledger is SHA-256 `179c4795510f9592cfacf356c4bded45e58450e8882a092caba4c73d7d37c901`; the sorted untracked source content manifest is `3112c0538da4e81240e2a198e771ef4d33dd59916a43a6332ec32b1598137cc3`; porcelain status before this ledger append was `7a1e98a19c191b85808a49f609a211bf6a06230dbbf2b4997396554ec013cd48`. Go HEAD/status/tracked diff remain `3839514ef08186900fa72acc05e313760f725d68` / `effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227` / `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`. Ignored KMP companion spec, Go p0000 spec, Go OpenAPI, and corpus manifest remain at their preflight hashes `11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`, `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`, `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`, and `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`. All 76 manifest entries verified and `git diff --check` is clean. Go `setup.go` and `setup_test.go` edits were preserved. No Go authority, OpenAPI, or corpus file changed.

The run-owned server, fresh heavy database container, and Android emulator were stopped after capturing evidence. An isolated `/tmp/sqlitenow-gzip-linux.I3innq` source copy remains because automatic approval review rejected its cleanup command using `rm -rf`; it is outside the checkout and does not affect the candidate. No file was staged, committed, pushed, published, or deployed.

### Independent-review evidence blocker remediation — 2026-09-24

The independent read-only review of the preceding candidate returned **BLOCKED** on two P2 focused-regression evidence gaps: active compressor cancellation with durable push state, and endpoint-level limit mappings. It reported no demonstrated production defect. Its verdict supersedes the preceding author PASS for acceptance purposes. The changes below affect tests only; the request selector, platform adapters, request builders, Go authority, OpenAPI, and corpus are byte-identical to the preceding candidate. The revised candidate still requires a new independent verdict and owner decision.

The review gaps were addressed together before another broad gate. `RequestGzipTest.endpointRequestLimitsAdmitExactBodiesAndStopOverLimitBeforeEngineAndHttp` drives the real `OversqliteRemoteApi`: a fixed-shape connect request reaches HTTP without invoking gzip; a 65,536-byte snapshot-session body is admitted with cap 65,535 and reaches HTTP; its 65,537-byte counterpart invokes neither engine nor HTTP; an admitted push body at its exact measured decoded limit reaches HTTP with cap `size - 1`; reducing that admitted limit by one byte invokes neither engine nor HTTP. Connect has no variable-length body that can approach the control cap. `RequestGzipCancellationJvmTest` cancels a real in-progress JVM `GZIPOutputStream` request and verifies cancellation propagation and a subsequent successful compression. The existing `CrossTargetSyncIntegrationTest.sync_cancellationPropagates_releasesGate_andRecoversFrozenBundle` now verifies the prepared outbox row/state, source ID, and unchanged next source-bundle ID after cancellation, then verifies replay clears the outbox and advances that ID exactly once. The workflow cancellation is induced at the mock push transport; the separate JVM test exercises active compressor cancellation.

Focused development attempts were kept out of the qualification count: `review-regressions-jvm-focused.log` failed to compile a test reference to a private limit constant; `review-regressions-jvm-focused-r2.log` passed before the test was moved to JVM; `review-regressions-web-native-focused.log` and `review-regressions-js-browser-diagnostic.log` failed because Chrome completed an incompressible 16 MiB compression before a timing-based cancellation probe. The flaky common probe was removed; the active-compressor case is JVM-specific. `review-regressions-android-emulator.log` records an obsolete SDK launcher-path failure, superseded by the current launcher in `review-regressions-android-emulator-r2.log`. These are not qualification passes.

The final focused candidate passed `jvmTest` filtered to seven common request-gzip tests, one JVM active-compressor cancellation test, and one durable workflow cancellation test, all executed with zero failures/skips (`review-regressions-jvm-focused-r3.log`, SHA-256 `adc629e1ef7615212ef622ec7b94eaeb50c1ebd64ac58002be360991d3eaf16f`). Full JS Node, JS browser, Wasm browser, macOS arm64, and iOS simulator library test leaves executed and passed (`review-regressions-web-native-focused-r2.log`, SHA-256 `3bad511b5362fa1065886e3acb78fd921bf637dcb2533b5f35c81a3ae7e1d42d`); each saved leaf has seven request-gzip cases and 23 cross-target integration cases, zero failures/skips. Android `connectedAndroidDeviceTest` executed 145 tests, zero failures/skips, including both new common assertions (`review-regressions-android-focused.log`, SHA-256 `fbfe829297a101c8c49a8ed7018ea4f5dc89f54c59aa4dbe956930a28f448c1f`). Linux x64 and ARM64 test binaries were cross-linked from this candidate and executed on matching Linux Docker runtimes: seven request-gzip tests and one workflow cancellation test per target, all passed (`review-regressions-linux-{x64,arm64}-{runtime,workflow}.log`). Saved focused and heavy leaf XML hashes are in `platform-gzip-20260923/review-report-manifest.sha256` (SHA-256 `c3d7e28fafb1d9d9ebd14a214c2fdffbcbc09827e22af6185a7f5589f0bb207a`). A post-edit Drymint current-state review reported low-severity expected classifier guidance from an unrelated test-method resemblance; without a prewrite baseline it is unattributed and is not an independent code review.

Only the library focused leaves and mandatory final heavy gate were invalidated. The production source, platform harness tests, real-server tests, Swift package, browser capture generator, seven-workload generator, and memory workload inputs did not change. The earlier platform, ordinary live, Swift-facing, browser network, measurement, and memory evidence therefore remains pinned to identical inputs; repeating those aggregates would not add execution evidence for these new focused tests.

The mandatory `oversqliteRealserverAllHeavy` was run once after the last test edit against a new PostgreSQL 16.15 database and a Go binary cross-built from the pinned read-only checkout. The run-owned Go container listened on host port 8081 because an unrelated MDChat process occupied 8080; Gradle host and Android URLs were set explicitly. Initial Go status was healthy with zero committed bundles. The aggregate reported BUILD SUCCESSFUL in 9m35s (`review-regressions-final-heavy.log`, SHA-256 `d207ef2c2025a6ad60fd6cdb2202707405dddaf2fa0c89186a7979110be5aa98`). Its first Android leaf produced **zero tests** despite task success; `review-heavy-reports/android-platform-zero-superseded.xml` is explicitly inconclusive. Rather than repeat the aggregate, `oversqliteRealserverAndroidHeavy` ran in isolation on the now-stable emulator and executed 78 tests with zero failures/skips (`review-regressions-android-heavy-isolated.log`, SHA-256 `03597ef5e905efd9fa973ce36ae50c6f66aaec8056e075c2e9de72ec281b01cd`). The other fresh heavy leaves executed iOS (10), JS Node (10), JVM harness (10), JVM harness shared stress (1), JVM library shared stress (1), macOS (10), and Wasm browser (10), all zero failures/skips. The combined qualified heavy leaves total 130 executed tests. Final Go status was healthy with 1,820 committed bundles. The run-owned containers, network, and emulator were stopped/removed after logs were captured; the unrelated MDChat service was untouched.

**Revised author-validation verdict: PASS for the test-remediated candidate.** No required qualification lane is incomplete. The independent BLOCKED verdict remains the latest independent verdict until a fresh read-only review assesses this new candidate. KMP HEAD remains `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4`; tracked source/test binary diff excluding this ledger is SHA-256 `76f8a671a0ae7f9c248c3fd6074466a51bf582cc0047b99d9873b43200e6bddf`, sorted untracked source content manifest `84fb11c66587191e5ae7fd58888869358af4a07da5f0f6903fd975335a9e0573`, and porcelain status `cf60c830cb25cc6cb6634b3e7fd1254ddc87e3cb71a75099ae656044fbde446d`. The implementation spec remains SHA-256 `7ff96ef7cde457a5f6fe988c1a6f79e03091c53b13d52079aac8757d6b42bbbd`; KMP p0000 companion and 76-entry corpus manifest remain at the prior pinned hashes and all manifest entries verified. Go HEAD/status/tracked diff remain `3839514ef08186900fa72acc05e313760f725d68` / `effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227` / `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`; Go spec and OpenAPI hashes also remain pinned. `git diff --check` passed. Nothing was staged, committed, pushed, published, or deployed.

### Second independent-review focused-regression remediation — 2026-09-24

The next independent read-only review returned **BLOCKED** on two P2 focused-regression gaps, while finding no demonstrated production selector or adapter defect. It found that the JVM cancellation signal preceded compression and did not prove prompt interruption, successful stream closure, or durable state preservation when compression itself was cancelled. It also found that connect's 65,536-byte mapping remained unpinned because its wire body is fixed and small. That verdict supersedes the prior author PASS for acceptance purposes. The KMP and Go identities in the review request matched before editing; Go's `setup.go` and `setup_test.go` edits were preserved.

The new JVM regression injects the same JVM compressor loop through the durable client's push path using an internal stream factory. A tracked `GZIPOutputStream` signals only **after** completing its first real 8 KiB input write, then holds the writer while the caller cancels. After release, the test requires the sync job to finish within three seconds with `CancellationException`, a successfully closed stream, fewer input bytes written than the complete request, no push HTTP request, a prepared outbox row and bundle/source IDs unchanged from before cancellation. It then replays the frozen outbox and verifies one push, outbox removal, and exactly one source-bundle-ID increment. The mock server consumes identity JSON, so replay selects identity; the cancelled attempt uses actual JVM gzip. The prior transport-cancellation workflow regression remains separate. The seam changes the JVM adapter's construction site but leaves its loop, cap, cancellation checkpoints, and default `GZIPOutputStream` behavior intact. `DefaultOversqliteClient.forTesting` can supply the engine; its public four-argument construction retains the default adapter.

Connect now calls `OversqliteRemoteApi.prepareConnectBody`, an internal preparation boundary that applies the existing 65,536-byte control limit. The focused test calls that exact boundary with 65,536 and 65,537 UTF-8 bytes: the exact case invokes the engine with cap 65,535, and the over-limit case invokes neither engine nor HTTP. The fixed-shape real connect request still reaches HTTP without gzip. Snapshot-session and push exact/over-limit assertions remain in the same executed test.

Superseded development run `review-regressions-focused-r3-jvm.log` compiled but failed only on recovery: the mock server does not decode a replayed gzip request. The test was corrected to replay as identity while retaining the actual mid-compression cancellation and resource assertions. `review-regressions-focused-r3-jvm-r2.log` then passed nine focused JVM tests; a final strengthening assertion that cancellation consumes less than the full input invalidated only the JVM cancellation test, so `review-regressions-focused-r3-jvm-final.log` reran its class plus `RequestGzipTest`. The final JVM XML records eight executed tests, zero failures/skips, including the one durable mid-compression test. The separate transport-cancellation workflow leaf executed once in the r2 run, zero failures/skips.

Focused `RequestGzipTest` leaves executed seven tests each on Android, JS Node, JS browser, Wasm browser, macOS arm64, iOS simulator arm64, Linux x64, and Linux arm64, all zero failures/skips. The Linux binaries were relinked after these edits, then each seven-test class ran in an Ubuntu 24.04 container on its matching architecture. `review-regressions-focused-r3-web-apple.log`, `review-regressions-focused-r3-android.log`, `review-regressions-focused-r3-linux-link.log`, and the two Linux runtime logs record the tasks. The eight saved XML leaves and all run logs are pinned by `platform-gzip-20260923/review-r3-manifest.sha256` (SHA-256 `ec584eba9a473650e38d86e5d44bde325d03695331c0a1231b3ffb3b50cf07f0`); its verification passed. The 76-entry corpus manifest verification passed (`review-r3-corpus-check.log`, SHA-256 `f0e708d56e0c232192aeb8b917cbf88397dc08d799e9a59919863599aa03edb5`). Android emulator was stopped after the focused run. A baseline-backed Drymint review attributed the edits and returned `inspect` for a pre-existing resemblance between two test-client factory methods; source inspection found no reason to merge their distinct fixture roles.

The owner explicitly requested **only focused tests affected by these fixes**. Therefore the platform aggregate, ordinary live-server, Swift-facing, browser capture, measurement, memory, and final heavy aggregate were not rerun. Their prior reports remain historical evidence for the unchanged protocol behavior and adapters, but the specification's mandatory final-heavy-after-source-change rule has not been satisfied for this new source identity. **Focused remediation verdict: PASS; full-spec author validation: pending that heavy gate unless the owner formally revises that requirement.** No independent PASS or owner acceptance is claimed. This ledger does not count cached, skipped, or zero-test leaves as fresh execution.

Postflight KMP HEAD remains `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4`. For this new candidate, `git diff --binary -- . ':(exclude).artifacts/p0000-protocol-efficiency/kmp-ledger.md'` has SHA-256 `ea8d52d675f40f514b4b83b273d63515e7aa4436bdf7d657f36000bb2f77c16d`; the SHA-256 of the sorted `shasum -a 256` lines for untracked `library-oversqlite/src` files is `bead91c01ed98705ad633e9a20f93c5a634f5b1805256f8a1df0960f2286df59`; `git status --porcelain=v1 -uall` has SHA-256 `f0daacea48cd59195ac2b4f6a16c1c5589c1c3c0dd2f0fa7aba366239a1906fa`. The implementation spec, KMP companion, and corpus manifest remain `7ff96ef7cde457a5f6fe988c1a6f79e03091c53b13d52079aac8757d6b42bbbd`, `11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`, and `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`. Go HEAD/status/diff/spec/OpenAPI remain at the preceding pins. `git diff --check` passed. Nothing was staged, committed, pushed, published, or deployed.

### Final heavy gate after second focused rereview — 2026-09-24

The second independent **narrow read-only rereview** closed both P2 focused-regression findings: mid-compression cancellation with compressor closure and frozen durable push state, and connect's control-limit preparation boundary. It reported no new production behavior or public API defect. This is a verdict on those findings, not an independent whole-spec PASS or owner acceptance. The owner then authorized proceeding with the remaining mandatory final heavy gate. Preflight KMP HEAD/source-diff/untracked-source/status/spec/ledger/report-manifest pins matched the preceding section; Go HEAD/status/diff/spec/OpenAPI and its pre-existing `setup.go`/`setup_test.go` edits also matched.

The heavy run used a **fresh isolated PostgreSQL 16.15 container** on run-owned network `oversqlite-gzip-r4-net` and the pinned read-only Go server binary `go-server-linux-arm64` (SHA-256 `0dcba480017c621dcdeef83a694b5ec2f7722423a429c7e85d665c867d354710`), exposed only at host port 8081. Host tests used `http://127.0.0.1:8081`; Android used `http://10.0.2.2:8081` on the run-owned Pixel_4 emulator. Initial `/syncx/status` was healthy with zero committed bundles (`review-r4-initial-status.json`, SHA-256 `39b6d0b11161be22e20d5f790b9a140eb9cbd5d834a85a4fc86af7d419f7d5e2`). Final status was healthy with 1,821 committed bundles (`review-r4-final-status.json`, SHA-256 `d98d2d081d015c77825d395e91254650ef65d0f4d8690d9382bcf2d455a506db`).

`oversqliteRealserverAllHeavy --max-workers=1 --no-parallel` completed **BUILD SUCCESSFUL in 13m 15s** (`review-r4-final-heavy.log`, SHA-256 `da504cbc8afafbb8bb5775ddad8b054cc9711935965a3e52c0c9f06d3f7e202d`). The required heavy leaves all executed freshly, with zero failures/errors/skips: Android 78, iOS simulator 10, JS Node 10, JVM harness 10, JVM harness shared-connection stress 1, JVM library shared-connection stress 1, macOS arm64 10, and Wasm browser 10: **130 required heavy tests**. The aggregate also executed 14 JVM library real-server tests and one JVM library snapshot profile test, all passing; all 12 saved XML reports total **145 executed tests**. The Android leaf itself reported 78, so no zero-test remediation run was needed. Root Gradle reported 10 executed and 12 up-to-date *tasks*; qualification counts come from the newly timestamped XML, not task labels. The JS-browser live lane remains covered by the prior ordinary live evidence; the heavy aggregate defines no JS-browser-heavy leaf.

All 12 XMLs, the heavy log, initial/final status, Go/PostgreSQL/emulator logs, and server binary are pinned by `platform-gzip-20260923/review-r4-manifest.sha256` (SHA-256 `a40c8aca86ff12f2fe0e0475f6d02d11e6ba904e342239d9b65cb928cd4ed22b`); its `shasum -a 256 -c` verification passed. The 76-entry corpus manifest check passed (`review-r4-corpus-check.log`, SHA-256 `f0e708d56e0c232192aeb8b917cbf88397dc08d799e9a59919863599aa03edb5`). The run-owned server/database containers, Docker network, and emulator were stopped and removed after evidence capture; no other resource was changed.

This late remediation added a test observation seam to the JVM compressor and client factory, plus the connect preparation boundary, without changing the selector, default engine, wire representation, server contract, browser capture generator, seven-workload measurement input, or memory workload. The immediately preceding 64 fresh focused tests cover the changed paths on JVM, Android, JS, Wasm, Apple, and both Linux architectures. Earlier platform aggregate, ordinary live-server, Swift-facing, browser-capture, measurement, and memory evidence remains reused for its identical producing inputs as recorded above; repeating those aggregates would add no evidence for this fix. The previous heavy result is superseded by this new candidate's fresh mandatory heavy run.

**Final author-validation verdict: PASS for the current candidate.** The two P2 findings have an independent narrow CLOSED verdict. No required author-validation lane remains incomplete, but a full-spec independent verdict and owner acceptance are separate pending gates. Postflight KMP HEAD is `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4`; source/test binary diff excluding this ledger is SHA-256 `ea8d52d675f40f514b4b83b273d63515e7aa4436bdf7d657f36000bb2f77c16d`; sorted untracked source content manifest is `bead91c01ed98705ad633e9a20f93c5a634f5b1805256f8a1df0960f2286df59`; porcelain status is `f0daacea48cd59195ac2b4f6a16c1c5589c1c3c0dd2f0fa7aba366239a1906fa`. The implementation spec, KMP p0000 companion, and corpus manifest remain SHA-256 `7ff96ef7cde457a5f6fe988c1a6f79e03091c53b13d52079aac8757d6b42bbbd`, `11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`, and `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`. Go HEAD/status/diff/spec/OpenAPI remain at their preceding pins. `git diff --check` passed. Nothing was staged, committed, pushed, published, or deployed.

### Request-gzip owner acceptance and local closeout — 2026-09-24

The owner supplied the result of a separate, read-only **full-spec independent PASS** for the exact candidate pinned above. That reviewer reported no blocking findings, verified the supplied KMP and Go identities before and after review, audited the full implementation and saved evidence, and ran no builds or tests. This reported full-spec verdict is distinct from the earlier narrow P2 closure and the author-validation PASS.

After receiving that verdict, the owner explicitly stated **“I accept”** in this task. The request-gzip platform-engine implementation is therefore **owner accepted and locally closed** for the reviewed candidate. This acceptance records no new source or test execution and does not authorize staging, commit, push, publication, deployment, or delivery. The ignored implementation specification remains byte-identical so its reviewed SHA-256 continues to identify the accepted requirements.
