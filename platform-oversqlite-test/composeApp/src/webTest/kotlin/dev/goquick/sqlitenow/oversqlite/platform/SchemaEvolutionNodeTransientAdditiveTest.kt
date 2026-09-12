package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.createAuthenticLegacySqlJsFixture
import dev.goquick.sqlitenow.core.worker.bundledSqliteWorkerStartupModeForTest
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningStore
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.localOnlyOpeningDigest
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.AttachmentQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.SchemaEvolutionGenerationTargetDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.PersonQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.ProjectQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.VersionBasedDatabaseMigrations as TargetMigrations
import dev.goquick.sqlitenow.oversqlite.platformSuiteEnabled
import dev.goquick.sqlitenow.oversqlite.sha256Hex
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout

internal class SchemaEvolutionNodeTransientAdditiveTest : PlatformCrossTargetTestSupport() {
    @Test
    fun importedGenerationOneImageRunsTransientAdditiveEdge() = runTest {
        if (!platformSuiteEnabled() || webRuntimeKind() != "js-node") return@runTest

        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("people", "notes", "projects", "attachments"),
            generationResponses = mapOf(1L to schemaevolutionGenerationResponse(1), 2L to schemaevolutionGenerationResponse(2)),
        )
        val sourceHttp = server.newHttpClient()
        val targetHttp = server.newHttpClient()
        val sourceFixture = createAuthenticLegacySqlJsFixture("schemaevolution-node-source")
        var sourceClient: DefaultOversqliteClient? = null
        var targetOpening: dev.goquick.sqlitenow.oversqlite.OversqliteOpening<SchemaEvolutionGenerationTargetDatabase>? = null
        try {
            val source = sourceFixture.connection
            source.execSQL("PRAGMA foreign_keys=ON")
            source.execSQL("CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            source.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,body TEXT NOT NULL)")
            source.execSQL("CREATE TABLE drafts(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL,payload BLOB NOT NULL)")
            source.execSQL("PRAGMA user_version=1")
            val sourceConfig = OversqliteConfig(
                generationId = 1,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                syncTables = listOf(SyncTable("people", "id"), SyncTable("notes", "id")),
            )
            val preparedSourceClient = DefaultOversqliteClient.forTesting(
                db = source,
                config = sourceConfig,
                http = sourceHttp,
                resolver = ServerWinsResolver,
            )
            sourceClient = preparedSourceClient
            preparedSourceClient.open().getOrThrow()
            preparedSourceClient.attach("schemaevolution-node-user").getOrThrow()
            source.execSQL("INSERT INTO people VALUES('$PERSON_ID','Ada')")
            source.execSQL("INSERT INTO notes VALUES('$NOTE_ID','generation one')")
            source.execSQL("INSERT INTO drafts VALUES('$DRAFT_ID','mixed',X'000102FF')")
            preparedSourceClient.sync().getOrThrow()
            val readyOne = assertNotNull(OversqliteOpeningStore(source).readOrNull())
            val localPreservation = localOnlyOpeningDigest(
                source,
                managedTables = setOf("people", "notes"),
                preservedLocalTables = setOf("drafts"),
            )
            source.execSQL("ALTER TABLE drafts ADD COLUMN normalized TEXT")
            source.execSQL("CREATE INDEX drafts_normalized ON drafts(normalized)")
            source.execSQL("UPDATE drafts SET normalized=upper(label)")
            source.execSQL(
                "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                    "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END",
            )
            source.execSQL("PRAGMA user_version=2")
            source.execSQL(
                "UPDATE _sync_contract_identity SET local_version=2 " +
                    "WHERE singleton_key=1 AND generation_id=1 AND local_version=1",
            )
            assertEquals(1L, scalarLong(source, "SELECT changes()"))
            val localPlanDigest = sha256Hex(
                listOf(
                    "sqlitenow-opening-plan-v1",
                    DATABASE_ID,
                    SERVICE_ID,
                    1L,
                    1L,
                    1,
                    2,
                    LOCAL_MIGRATION_ID,
                    "drafts",
                    readyOne.targetFingerprint,
                ).joinToString("\u001f").encodeToByteArray(),
            )
            OversqliteOpeningStore(source).replaceReady(
                readyOne.copy(
                    mode = "local_only",
                    phase = "ready",
                    outcome = "ready",
                    revision = readyOne.revision + 2,
                    planDigest = localPlanDigest,
                    migrationId = LOCAL_MIGRATION_ID,
                    attachedUserId = "",
                    oldSourceId = "",
                    newSourceId = "",
                    replacementReason = "",
                    fromGenerationId = 1,
                    toGenerationId = 1,
                    fromLocalVersion = 1,
                    toLocalVersion = 2,
                    preservedTables = "drafts",
                    oldFingerprint = readyOne.targetFingerprint,
                    preservationDigest = localPreservation,
                    migrationComplete = true,
                    pendingDirtyCount = 0,
                    pendingFrozenCount = 0,
                    pendingConflictCount = 0,
                    drainComplete = true,
                    replacementState = "none",
                    snapshotId = "",
                    snapshotBundleSeq = 0,
                    snapshotRowCount = 0,
                    snapshotByteCount = 0,
                    stagedRowCount = null,
                    stagedByteCount = null,
                    targetReadsAvailable = true,
                    writesAvailable = true,
                    nextAction = "use_ready",
                    pendingRequestHash = "",
                    pendingIntentRevision = 0,
                    failureCode = "",
                    failureMessage = "",
                    initialOldSourceId = "",
                    oldSourceRecoveryReceipt = "",
                ),
                readyOne,
            )
            preparedSourceClient.closeFromCoordinatedOwner()
            sourceClient = null

            val imported = sourceFixture.exportBytes()
            val persistence = OneShotBytesPersistence(imported)
            bundledSqliteWorkerStartupModeForTest = "node-custom-import"
            val target = SchemaEvolutionGenerationTargetDatabase(
                workerOwnedStorage.newDatabaseName(),
                TargetMigrations(),
            )
            target.connectionConfig = SqliteConnectionConfig(persistence = persistence)
            val targetConfig = target.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID)
            targetOpening = target.openForSync(
                targetConfig,
                targetHttp,
                ServerWinsResolver,
                OversqliteUpgradePlan(
                    migrationId = "schemaevolution-node-generation-v1-v2",
                    fromGenerationId = 1,
                    toGenerationId = 2,
                    fromLocalVersion = 2,
                    toLocalVersion = 3,
                    preservedLocalTables = setOf("drafts"),
                ),
            )
            assertNull(targetOpening.readyOrNull())
            assertFalse(target.isOpen())
            assertFailsWith<IllegalStateException> { target.person.selectAll.asList() }
            assertFailsWith<IllegalStateException> {
                target.newOversqliteClient(2, DATABASE_ID, SERVICE_ID, targetHttp, ServerWinsResolver)
            }
            assertFailsWith<IllegalStateException> {
                withTimeout(2_000) { target.person.selectAll.asFlow().first() }
            }

            assertEquals(OversqliteOpeningOutcome.READY, targetOpening.advance().outcome)
            assertNotNull(targetOpening.readyOrNull())
            assertEquals(1, persistence.loadCalls)
            val person = target.person.selectAll.asList().single()
            assertEquals("Ada", person.name)
            assertNull(person.details)
            assertNull(person.projectId)
            val draft = target.draft.selectAll.asList().single()
            assertEquals("MIXED", draft.normalized)
            assertContentEquals(byteArrayOf(0, 1, 2, -1), draft.payload)

            target.project.insert(ProjectQuery.Insert.Params(PROJECT_ID, "Node"))
            target.person.insert(PersonQuery.Insert.Params(PERSON_TWO_ID, "Grace", "transient", PROJECT_ID))
            target.attachment.insert(AttachmentQuery.Insert.Params(ATTACHMENT_ID, PERSON_TWO_ID, "node.bin"))
            assertEquals(PROJECT_ID, target.person.selectAll.asList().single { it.id == PERSON_TWO_ID }.projectId)
            assertEquals(PERSON_TWO_ID, target.attachment.selectAll.asList().single().personId)
            assertEquals(0L, scalarLong(target.connection(), "SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(
                "projects|people|attachments",
                scalarText(
                    target.connection(),
                    "SELECT group_concat(name,'|') FROM (" +
                        "SELECT name FROM sqlite_schema WHERE type='table' " +
                        "AND name IN ('projects','people','attachments') " +
                        "ORDER BY CASE name WHEN 'projects' THEN 1 WHEN 'people' THEN 2 ELSE 3 END)",
                ),
            )
        } finally {
            bundledSqliteWorkerStartupModeForTest = "normal"
            runCatching { targetOpening?.close() }
            runCatching { sourceClient?.closeFromCoordinatedOwner() }
            runCatching { sourceFixture.close() }
            sourceHttp.close()
            targetHttp.close()
            cleanupOwnedWorkerStorage()
        }
    }

    private class OneShotBytesPersistence(private val bytes: ByteArray) : SqlitePersistence {
        var loadCalls = 0
            private set

        override suspend fun load(dbName: String): ByteArray {
            loadCalls += 1
            return bytes.copyOf()
        }

        override suspend fun persist(dbName: String, bytes: ByteArray) = Unit
    }

    private companion object {
        const val DATABASE_ID = "schemaevolution-generated-platform"
        const val SERVICE_ID = "schemaevolution-authority"
        const val LOCAL_MIGRATION_ID = "schemaevolution-node-local-v1-v2"
        const val PERSON_ID = "11111111-1111-4111-8111-111111111111"
        const val NOTE_ID = "22222222-2222-4222-8222-222222222222"
        const val DRAFT_ID = "draft-platform"
        const val PROJECT_ID = "33333333-3333-4333-8333-333333333333"
        const val PERSON_TWO_ID = "44444444-4444-4444-8444-444444444444"
        const val ATTACHMENT_ID = "55555555-5555-4555-8555-555555555555"
    }
}
