package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.OversqliteOpening
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.DraftQuery as GenerationOneDraftQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.SchemaEvolutionGenerationOneDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.NoteQuery as GenerationOneNoteQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.PersonQuery as GenerationOnePersonQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.VersionBasedDatabaseMigrations as GenerationOneMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.invalidtarget.SchemaEvolutionInvalidTargetDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.invalidtarget.VersionBasedDatabaseMigrations as InvalidTargetMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.SchemaEvolutionLocalTwoDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.VersionBasedDatabaseMigrations as LocalTwoMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.AttachmentQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.SchemaEvolutionGenerationTargetDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.PersonQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.ProjectQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.VersionBasedDatabaseMigrations as TargetMigrations
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

internal class SchemaEvolutionGeneratedFixtureJvmTest : PlatformCrossTargetTestSupport() {
    @Test
    fun generatedFixtureHasTwoReadyBoundariesInvalidTargetRollbackAndAdditiveReady() = runBlocking {
        val directory = Files.createTempDirectory("sqlitenow-schemaevolution-generated")
        val path = directory.resolve("database.sqlite3").toString()
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("people", "notes", "projects", "attachments"),
            generationResponses = mapOf(1L to schemaevolutionGenerationResponse(1), 2L to schemaevolutionGenerationResponse(2)),
        )
        val http = server.newHttpClient()
        var generationOneOpening: OversqliteOpening<SchemaEvolutionGenerationOneDatabase>? = null
        var localOpening: OversqliteOpening<SchemaEvolutionLocalTwoDatabase>? = null
        var invalidOpening: OversqliteOpening<SchemaEvolutionInvalidTargetDatabase>? = null
        var targetOpening: OversqliteOpening<SchemaEvolutionGenerationTargetDatabase>? = null
        try {
            val generationOne = SchemaEvolutionGenerationOneDatabase(path, GenerationOneMigrations())
            val generationOneConfig = generationOne.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID)
            generationOneOpening = generationOne.openForSync(generationOneConfig, http, ServerWinsResolver)
            assertNull(generationOneOpening.readyOrNull())
            assertFalse(generationOne.isOpen())
            assertEquals(OversqliteOpeningOutcome.READY, generationOneOpening.advance().outcome)
            val generationOneClient = generationOne.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, http, ServerWinsResolver)
            assertTrue(generationOneClient.attach("schemaevolution-generated-user").getOrThrow() is AttachResult.Connected)
            generationOne.person.insert(GenerationOnePersonQuery.Insert.Params(PERSON_ID, "Ada"))
            generationOne.note.insert(GenerationOneNoteQuery.Insert.Params(NOTE_ID, "generation one"))
            generationOne.draft.insert(
                GenerationOneDraftQuery.Insert.Params(DRAFT_ID, "mixed", byteArrayOf(0, 1, 2, -1)),
            )
            generationOneClient.sync().getOrThrow()
            generationOneOpening.close()
            generationOneOpening = null

            val localTwo = SchemaEvolutionLocalTwoDatabase(
                path,
                LocalTwoMigrations { step ->
                    if (step.toVersion == 2) {
                        step.connection.execSQL(
                            """CREATE TRIGGER drafts_touch
                                |AFTER UPDATE OF label ON drafts
                                |BEGIN
                                |  UPDATE drafts SET normalized = upper(new.label) WHERE id = new.id;
                                |END
                            """.trimMargin(),
                        )
                    }
                },
            )
            val localConfig = localTwo.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID)
            localOpening = localTwo.openForSync(
                config = localConfig,
                httpClient = http,
                resolver = ServerWinsResolver,
                upgradePlan = OversqliteUpgradePlan(
                    migrationId = "schemaevolution-drafts-local-v1-v2",
                    fromGenerationId = 1,
                    toGenerationId = 1,
                    fromLocalVersion = 1,
                    toLocalVersion = 2,
                    preservedLocalTables = setOf("drafts"),
                ),
            )
            assertNull(localOpening.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.READY, localOpening.advance().outcome)
            val transformed = localTwo.draft.selectAll.asList().single()
            assertEquals("MIXED", transformed.normalized)
            assertEquals("000102FF", transformed.payload.toHex())
            assertEquals(1L, scalarLong(localTwo, "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='drafts_normalized'"))
            assertEquals(1L, scalarLong(localTwo, "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='drafts_touch'"))
            localOpening.close()
            localOpening = null

            val targetConfigFactory = SchemaEvolutionGenerationTargetDatabase(path, TargetMigrations())
            val targetConfig = targetConfigFactory.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID)
            targetConfigFactory.close()
            val plan = OversqliteUpgradePlan(
                migrationId = "schemaevolution-generation-v1-v2",
                fromGenerationId = 1,
                toGenerationId = 2,
                fromLocalVersion = 2,
                toLocalVersion = 3,
                preservedLocalTables = setOf("drafts"),
            )

            val invalidTarget = SchemaEvolutionInvalidTargetDatabase(path, InvalidTargetMigrations())
            invalidOpening = invalidTarget.openForSync(
                config = targetConfig,
                httpClient = http,
                resolver = ServerWinsResolver,
                upgradePlan = plan,
            )
            assertNull(invalidOpening.readyOrNull())
            assertFailsWith<IllegalArgumentException> { invalidOpening.advance() }
            assertNull(invalidOpening.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.LOCAL_MIGRATION_FAILED, invalidOpening.status().outcome)
            invalidOpening.close()
            invalidOpening = null
            assertEquals(2L, scalarLong(path, "PRAGMA user_version"))
            assertEquals(0L, scalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='projects'"))
            assertEquals("MIXED", scalarText(path, "SELECT normalized FROM drafts WHERE id='$DRAFT_ID'"))

            val target = SchemaEvolutionGenerationTargetDatabase(path, TargetMigrations())
            targetOpening = target.openForSync(targetConfig, http, ServerWinsResolver, plan)
            assertNull(targetOpening.readyOrNull())
            assertFalse(target.isOpen())
            assertFailsWith<IllegalStateException> { target.person.selectAll.asList() }
            assertFailsWith<IllegalStateException> {
                target.newOversqliteClient(2, DATABASE_ID, SERVICE_ID, http, ServerWinsResolver)
            }
            assertFailsWith<IllegalStateException> { withTimeout(2_000) { target.person.selectAll.asFlow().first() } }

            assertEquals(OversqliteOpeningOutcome.READY, targetOpening.advance().outcome)
            assertNotNull(targetOpening.readyOrNull())
            assertNotNull(target.newOversqliteClient(2, DATABASE_ID, SERVICE_ID, http, ServerWinsResolver))
            val people = target.person.selectAll.asList()
            assertEquals(1, people.size)
            assertEquals("Ada", people.single().name)
            assertNull(people.single().details)
            assertNull(people.single().projectId)
            assertEquals("generation one", target.note.selectAll.asList().single().body)
            assertEquals("MIXED", target.draft.selectAll.asList().single().normalized)
            assertEquals(1, withTimeout(2_000) { target.person.selectAll.asFlow().first() }.size)
            assertEquals(0L, scalarLong(target, "SELECT count(*) FROM pragma_foreign_key_check"))

            target.project.insert(ProjectQuery.Insert.Params(PROJECT_ID, "Additive"))
            target.person.insert(PersonQuery.Insert.Params(PERSON_TWO_ID, "Grace", "target", PROJECT_ID))
            target.attachment.insert(AttachmentQuery.Insert.Params(ATTACHMENT_ID, PERSON_TWO_ID, "proof.bin"))
            assertEquals(1, target.attachment.selectAll.asList().size)
            target.connection().execSQL("DELETE FROM projects WHERE id='$PROJECT_ID'")
            assertNull(target.person.selectAll.asList().single { it.id == PERSON_TWO_ID }.projectId)
            target.connection().execSQL("DELETE FROM people WHERE id='$PERSON_TWO_ID'")
            assertTrue(target.attachment.selectAll.asList().isEmpty())
            assertEquals(0L, scalarLong(target, "SELECT count(*) FROM pragma_foreign_key_check"))
        } finally {
            runCatching { targetOpening?.close() }
            runCatching { invalidOpening?.close() }
            runCatching { localOpening?.close() }
            runCatching { generationOneOpening?.close() }
            http.close()
            directory.toFile().deleteRecursively()
        }
    }

    private suspend fun scalarLong(database: dev.goquick.sqlitenow.core.SqliteNowDatabase, sql: String): Long =
        database.connection().withExclusiveAccess {
            database.connection().prepare(sql).use { statement ->
                check(statement.step())
                statement.getLong(0)
            }
        }

    private suspend fun scalarLong(path: String, sql: String): Long =
        androidx.sqlite.driver.bundled.BundledSQLiteDriver().open(path).use { connection ->
            connection.prepare(sql).use { statement -> check(statement.step()); statement.getLong(0) }
        }

    private suspend fun scalarText(path: String, sql: String): String =
        androidx.sqlite.driver.bundled.BundledSQLiteDriver().open(path).use { connection ->
            connection.prepare(sql).use { statement -> check(statement.step()); statement.getText(0) }
        }

    private fun ByteArray.toHex(): String = joinToString("") { byte -> "%02X".format(byte) }

    private companion object {
        const val DATABASE_ID = "schemaevolution-generated"
        const val SERVICE_ID = "schemaevolution-authority"
        const val PERSON_ID = "11111111-1111-4111-8111-111111111111"
        const val NOTE_ID = "22222222-2222-4222-8222-222222222222"
        const val DRAFT_ID = "draft-1"
        const val PROJECT_ID = "33333333-3333-4333-8333-333333333333"
        const val PERSON_TWO_ID = "44444444-4444-4444-8444-444444444444"
        const val ATTACHMENT_ID = "55555555-5555-4555-8555-555555555555"
    }
}
