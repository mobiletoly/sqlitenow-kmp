package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.OversqliteOpening
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningBusyException
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningNextAction
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlanMismatchException
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.directWorkerStorageCensus
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.DraftQuery as GenerationOneDraftQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.SchemaEvolutionGenerationOneDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.NoteQuery as GenerationOneNoteQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.PersonQuery as GenerationOnePersonQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.VersionBasedDatabaseMigrations as GenerationOneMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.SchemaEvolutionLocalTwoDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.VersionBasedDatabaseMigrations as LocalTwoMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.SchemaEvolutionGenerationTargetDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.VersionBasedDatabaseMigrations as TargetMigrations
import dev.goquick.sqlitenow.oversqlite.openingDurableCommitForTest
import dev.goquick.sqlitenow.oversqlite.sameOriginIframeCanAcquireLifecycleLock
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import io.ktor.client.HttpClient
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest

internal class SchemaEvolutionBrowserOwnershipRecoveryTest : PlatformCrossTargetTestSupport() {
    @Test
    fun rejectedGeneratedTargetRetainsOwnershipUntilExplicitClose() = runTest {
        if (!browserRuntime()) return@runTest

        val databaseName = workerOwnedStorage.newDatabaseName()
        val server = fixtureServer(targetGeneration = 2)
        val seedHttp = server.newHttpClient()
        val rejectedHttp = server.newHttpClient()
        val contenderHttp = server.newHttpClient()
        val successorHttp = server.newHttpClient()
        var rejected: OversqliteOpening<SchemaEvolutionGenerationTargetDatabase>? = null
        var oldSuccessor: OversqliteOpening<SchemaEvolutionLocalTwoDatabase>? = null
        try {
            seedLocalTwo(databaseName, seedHttp, collideWithTarget = true)
            val before = directWorkerStorageCensus(databaseName)
            assertCensus(before)

            val target = targetDatabase(databaseName)
            rejected = target.openForSync(
                target.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID),
                rejectedHttp,
                ServerWinsResolver,
                generationPlan(2, "schemaevolution-browser-collision-v1-v2"),
            )
            val failure = assertFailsWith<OversqliteUpgradePlanMismatchException> { rejected.advance() }
            assertEquals("schema_evolution_compatibility_new_table_namespace", failure.message)
            val status = rejected.status()
            assertEquals(OversqliteOpeningOutcome.UPGRADE_PLAN_MISMATCH, status.outcome)
            assertEquals(OversqliteOpeningNextAction.REPAIR_CONFIGURATION, status.nextAction)
            assertEquals("upgrade_plan_mismatch", status.diagnosticCode)
            assertEquals("schema_evolution_compatibility_new_table_namespace", status.diagnosticMessage)
            assertNull(rejected.readyOrNull())
            assertEquals(before, directWorkerStorageCensus(databaseName))

            assertFalse(sameOriginIframeCanAcquireLifecycleLock(databaseName))
            val contender = targetDatabase(databaseName)
            val busy = assertFails {
                contender.openForSync(
                    contender.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID),
                    contenderHttp,
                    ServerWinsResolver,
                    generationPlan(2, "schemaevolution-browser-collision-v1-v2"),
                )
            }
            assertTrue(busy is SqliteNowLifecycleBusyException || busy is OversqliteOpeningBusyException)
            runCatching { contender.close() }

            rejected.close()
            rejected = null
            assertTrue(sameOriginIframeCanAcquireLifecycleLock(databaseName))

            val old = SchemaEvolutionLocalTwoDatabase(databaseName, localMigrations(collideWithTarget = true))
            oldSuccessor = old.openForSync(
                old.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID),
                successorHttp,
                ServerWinsResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, oldSuccessor.advance().outcome)
            assertEquals(1L, scalarLong(old.connection(), "SELECT count(*) FROM people WHERE name='Ada'"))
            assertEquals(1L, scalarLong(old.connection(), "SELECT count(*) FROM projects WHERE title='collision'"))
            assertEquals(1L, scalarLong(old.connection(), "SELECT count(*) FROM _sync_contract_identity WHERE generation_id=1 AND local_version=2"))
            assertEquals(1L, scalarLong(old.connection(), "SELECT count(*) FROM _sync_opening_state WHERE mode='local_only' AND outcome='ready'"))
            assertEquals(before, directWorkerStorageCensus(databaseName))
        } finally {
            runCatching { oldSuccessor?.close() }
            runCatching { rejected?.close() }
            seedHttp.close()
            rejectedHttp.close()
            contenderHttp.close()
            successorHttp.close()
            cleanupOwnedWorkerStorage()
        }
    }

    @Test
    fun generatedAdditiveAndSparseOwnersRecoverFourDurableDeathBoundaries() = runTest {
        if (!browserRuntime()) return@runTest

        listOf(
            RecoveryCase("additive-after-u0", 2, "after_u0"),
            RecoveryCase("additive-after-u4", 2, "after_u4"),
            RecoveryCase("sparse-after-u6", 3, "after_u6"),
            RecoveryCase("sparse-after-u8-before-publication", 3, "after_u8"),
        ).forEach { scenario -> runRecoveryCase(scenario) }
    }

    private suspend fun runRecoveryCase(scenario: RecoveryCase) {
        val databaseName = workerOwnedStorage.newDatabaseName()
        val server = fixtureServer(scenario.targetGeneration)
        val seedHttp = server.newHttpClient()
        val interruptedHttp = server.newHttpClient()
        val successorHttp = server.newHttpClient()
        var interrupted: OversqliteOpening<SchemaEvolutionGenerationTargetDatabase>? = null
        var successor: OversqliteOpening<SchemaEvolutionGenerationTargetDatabase>? = null
        var checkpoint: JournalWitness? = null
        try {
            seedLocalTwo(databaseName, seedHttp, collideWithTarget = false)
            val before = directWorkerStorageCensus(databaseName)
            assertCensus(before)

            val target = targetDatabase(databaseName)
            val plan = generationPlan(scenario.targetGeneration, "schemaevolution-browser-${scenario.name}")
            interrupted = target.openForSync(
                target.buildOversqliteConfig(scenario.targetGeneration.toLong(), DATABASE_ID, SERVICE_ID),
                interruptedHttp,
                ServerWinsResolver,
                plan,
            )
            openingDurableCommitForTest = { cut, connection ->
                if (cut == scenario.cut) {
                    checkpoint = journalWitness(connection)
                    openingDurableCommitForTest = null
                    (connection.ref as SqliteWorkerSQLiteConnection).forceTerminateOwnerForTest()
                    throw CancellationException("simulated browser owner death at $cut")
                }
            }
            assertFails { interrupted.advance() }
            openingDurableCommitForTest = null

            val death = directWorkerStorageCensus(databaseName)
            assertCensus(death)
            assertTrue(sameOriginIframeCanAcquireLifecycleLock(databaseName))
            runCatching { interrupted.close() }
            interrupted = null

            val recovered = targetDatabase(databaseName)
            successor = recovered.openForSync(
                recovered.buildOversqliteConfig(scenario.targetGeneration.toLong(), DATABASE_ID, SERVICE_ID),
                successorHttp,
                ServerWinsResolver,
                plan,
            )
            assertEquals(OversqliteOpeningOutcome.READY, successor.advance().outcome, scenario.name)
            assertNotNull(successor.readyOrNull())

            val ready = journalWitness(recovered.connection())
            val observedCheckpoint = assertNotNull(checkpoint)
            assertEquals(observedCheckpoint.planDigest, ready.planDigest, scenario.name)
            assertTrue(ready.planDigest.isNotBlank(), scenario.name)
            if (observedCheckpoint.initialOldSourceId.isNotBlank()) {
                assertEquals(observedCheckpoint.initialOldSourceId, ready.initialOldSourceId, scenario.name)
            }
            if (observedCheckpoint.newSourceId.isNotBlank()) {
                assertEquals(observedCheckpoint.newSourceId, ready.newSourceId, scenario.name)
            }
            assertEquals("confirmed", ready.replacementState, scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM schemaevolution_migration_witness"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM people WHERE name='Ada'"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM notes WHERE body='generation one'"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM drafts WHERE normalized='MIXED'"), scenario.name)
            assertContentEquals(byteArrayOf(0, 1, 2, -1), blob(recovered.connection(), "SELECT payload FROM drafts WHERE id='$DRAFT_ID'"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='drafts_normalized'"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='drafts_touch'"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='projects'"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='attachments'"), scenario.name)
            assertEquals(0L, scalarLong(recovered.connection(), "SELECT count(*) FROM pragma_foreign_key_check"), scenario.name)
            assertEquals(1L, scalarLong(recovered.connection(), "SELECT count(*) FROM _sync_opening_state WHERE phase='ready' AND outcome='ready'"), scenario.name)
            assertTrue(server.requestCounts.getOrElse("/sync/generations/1") { 0 } > 0, scenario.name)
            assertTrue(server.requestCounts.getOrElse("/sync/generations/${scenario.targetGeneration}") { 0 } > 0, scenario.name)
            if (scenario.targetGeneration == 3) {
                assertEquals(0, server.requestCounts.getOrElse("/sync/generations/2") { 0 }, scenario.name)
            }

            val recovery = directWorkerStorageCensus(databaseName)
            assertCensus(recovery)
            assertNotEquals("", death, scenario.name)
            assertNotEquals("", recovery, scenario.name)
        } finally {
            openingDurableCommitForTest = null
            runCatching { successor?.close() }
            runCatching { interrupted?.close() }
            seedHttp.close()
            interruptedHttp.close()
            successorHttp.close()
            cleanupOwnedWorkerStorage()
        }
    }

    private suspend fun seedLocalTwo(databaseName: String, http: HttpClient, collideWithTarget: Boolean) {
        var generationOneOpening: OversqliteOpening<SchemaEvolutionGenerationOneDatabase>? = null
        var localOpening: OversqliteOpening<SchemaEvolutionLocalTwoDatabase>? = null
        try {
            val generationOne = SchemaEvolutionGenerationOneDatabase(databaseName, GenerationOneMigrations())
            generationOneOpening = generationOne.openForSync(
                generationOne.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID),
                http,
                ServerWinsResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, generationOneOpening.advance().outcome)
            val client = generationOne.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, http, ServerWinsResolver)
            assertTrue(client.attach("schemaevolution-browser-user").getOrThrow() is AttachResult.Connected)
            generationOne.person.insert(GenerationOnePersonQuery.Insert.Params(PERSON_ID, "Ada"))
            generationOne.note.insert(GenerationOneNoteQuery.Insert.Params(NOTE_ID, "generation one"))
            generationOne.draft.insert(
                GenerationOneDraftQuery.Insert.Params(DRAFT_ID, "mixed", byteArrayOf(0, 1, 2, -1)),
            )
            client.sync().getOrThrow()
            generationOneOpening.close()
            generationOneOpening = null

            val local = SchemaEvolutionLocalTwoDatabase(databaseName, localMigrations(collideWithTarget))
            localOpening = local.openForSync(
                local.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID),
                http,
                ServerWinsResolver,
                OversqliteUpgradePlan(
                    migrationId = "schemaevolution-browser-local-v1-v2",
                    fromGenerationId = 1,
                    toGenerationId = 1,
                    fromLocalVersion = 1,
                    toLocalVersion = 2,
                    preservedLocalTables = setOf("drafts"),
                ),
            )
            assertEquals(OversqliteOpeningOutcome.READY, localOpening.advance().outcome)
            assertEquals("MIXED", scalarText(local.connection(), "SELECT normalized FROM drafts WHERE id='$DRAFT_ID'"))
        } finally {
            runCatching { localOpening?.close() }
            runCatching { generationOneOpening?.close() }
        }
    }

    private fun localMigrations(collideWithTarget: Boolean) = LocalTwoMigrations { step ->
        if (step.toVersion == 2) {
            step.connection.execSQL(
                "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                    "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END",
            )
            if (collideWithTarget) {
                step.connection.execSQL("CREATE TABLE projects(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)")
                step.connection.execSQL("INSERT INTO projects VALUES('collision','collision')")
            }
        }
    }

    private fun targetDatabase(databaseName: String) = SchemaEvolutionGenerationTargetDatabase(
        databaseName,
        TargetMigrations { step ->
            if (step.toVersion == 3) {
                step.connection.execSQL("CREATE TABLE schemaevolution_migration_witness(id INTEGER PRIMARY KEY CHECK(id=1))")
                step.connection.execSQL("INSERT INTO schemaevolution_migration_witness VALUES(1)")
            }
        },
    )

    private fun generationPlan(targetGeneration: Int, migrationId: String) = OversqliteUpgradePlan(
        migrationId = migrationId,
        fromGenerationId = 1,
        toGenerationId = targetGeneration.toLong(),
        fromLocalVersion = 2,
        toLocalVersion = 3,
        preservedLocalTables = setOf("drafts"),
    )

    private fun fixtureServer(targetGeneration: Int): MockSyncServer = MockSyncServer(
        registeredTableSpecs = testRegisteredTableSpecs("people", "notes", "projects", "attachments"),
        generationResponses = buildMap {
            put(1L, schemaevolutionGenerationResponse(1))
            put(targetGeneration.toLong(), schemaevolutionGenerationResponse(targetGeneration.toLong()))
        },
    )

    private suspend fun journalWitness(connection: SafeSQLiteConnection): JournalWitness =
        connection.withExclusiveAccess {
            connection.prepare(
                "SELECT plan_digest,initial_old_source_id,new_source_id,replacement_state " +
                    "FROM _sync_opening_state WHERE singleton_key=1",
            ).use { statement ->
                check(statement.step())
                JournalWitness(statement.getText(0), statement.getText(1), statement.getText(2), statement.getText(3))
            }
        }

    private fun browserRuntime(): Boolean =
        webRuntimeKind() in setOf("js-browser", "wasm-browser")

    private fun assertCensus(census: String) {
        assertTrue(census.isNotBlank(), "the OPFS census must contain database or sidecar bytes")
        census.split('|').forEach { row ->
            val fields = row.split(':')
            assertEquals(3, fields.size, row)
            assertTrue(fields[1].toLong() >= 0L, row)
            assertEquals(64, fields[2].length, row)
        }
    }

    private suspend fun blob(database: SafeSQLiteConnection, sql: String): ByteArray =
        database.withExclusiveAccess {
            database.prepare(sql).use { statement ->
                check(statement.step())
                statement.getBlob(0)
            }
        }

    private data class RecoveryCase(val name: String, val targetGeneration: Int, val cut: String)

    private data class JournalWitness(
        val planDigest: String,
        val initialOldSourceId: String,
        val newSourceId: String,
        val replacementState: String,
    )

    private companion object {
        const val DATABASE_ID = "schemaevolution-browser-owner"
        const val SERVICE_ID = "schemaevolution-browser-authority"
        const val PERSON_ID = "11111111-1111-4111-8111-111111111111"
        const val NOTE_ID = "22222222-2222-4222-8222-222222222222"
        const val DRAFT_ID = "draft-browser"
    }
}
