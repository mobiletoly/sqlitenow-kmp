package dev.goquick.sqlitenow.oversqlite.platform

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
import dev.goquick.sqlitenow.oversqlite.platformSuiteEnabled
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withContext

internal class SchemaEvolutionGeneratedFixturePlatformTest : PlatformCrossTargetTestSupport() {
    @Test
    fun generatedFixtureRunsTheSharedAdditiveGraph() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        if (webRuntimeKind() == "js-node") {
            return@runTest
        }

        val web = webRuntimeKind() in setOf("js-browser", "wasm-browser")
        val path = if (web) {
            workerOwnedStorage.newDatabaseName()
        } else {
            createTempSqliteNowTestDbPath("sqlitenow-schemaevolution-generated")
        }
        val server = fixtureServer()
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
            val generationOneClient = generationOne.newOversqliteClient(
                1,
                DATABASE_ID,
                SERVICE_ID,
                http,
                ServerWinsResolver,
            )
            assertTrue(generationOneClient.attach("schemaevolution-platform-user").getOrThrow() is AttachResult.Connected)
            generationOne.person.insert(GenerationOnePersonQuery.Insert.Params(PERSON_ID, "Ada"))
            generationOne.note.insert(GenerationOneNoteQuery.Insert.Params(NOTE_ID, "generation one"))
            generationOne.draft.insert(
                GenerationOneDraftQuery.Insert.Params(DRAFT_ID, "mixed", byteArrayOf(0, 1, 2, -1)),
            )
            generationOneClient.sync().getOrThrow()
            generationOneOpening.close()
            generationOneOpening = null

            val local = SchemaEvolutionLocalTwoDatabase(
                path,
                LocalTwoMigrations { step ->
                    if (step.toVersion == 2) {
                        step.connection.execSQL(
                            "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                                "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END",
                        )
                    }
                },
            )
            localOpening = local.openForSync(
                config = local.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID),
                httpClient = http,
                resolver = ServerWinsResolver,
                upgradePlan = OversqliteUpgradePlan(
                    migrationId = "schemaevolution-platform-local-v1-v2",
                    fromGenerationId = 1,
                    toGenerationId = 1,
                    fromLocalVersion = 1,
                    toLocalVersion = 2,
                    preservedLocalTables = setOf("drafts"),
                ),
            )
            assertNull(localOpening.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.READY, localOpening.advance().outcome)
            val transformed = local.draft.selectAll.asList().single()
            assertEquals("MIXED", transformed.normalized)
            assertContentEquals(byteArrayOf(0, 1, 2, -1), transformed.payload)
            localOpening.close()
            localOpening = null

            val targetPrototype = SchemaEvolutionGenerationTargetDatabase(path, TargetMigrations())
            val targetConfig = targetPrototype.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID)
            targetPrototype.close()
            val plan = OversqliteUpgradePlan(
                migrationId = "schemaevolution-platform-generation-v1-v2",
                fromGenerationId = 1,
                toGenerationId = 2,
                fromLocalVersion = 2,
                toLocalVersion = 3,
                preservedLocalTables = setOf("drafts"),
            )

            val invalid = SchemaEvolutionInvalidTargetDatabase(path, InvalidTargetMigrations())
            invalidOpening = invalid.openForSync(targetConfig, http, ServerWinsResolver, plan)
            assertNull(invalidOpening.readyOrNull())
            assertFailsWith<IllegalArgumentException> { invalidOpening.advance() }
            assertEquals(OversqliteOpeningOutcome.LOCAL_MIGRATION_FAILED, invalidOpening.status().outcome)
            assertNull(invalidOpening.readyOrNull())
            invalidOpening.close()
            invalidOpening = null

            val target = SchemaEvolutionGenerationTargetDatabase(path, TargetMigrations())
            targetOpening = target.openForSync(targetConfig, http, ServerWinsResolver, plan)
            assertNull(targetOpening.readyOrNull())
            assertFalse(target.isOpen())
            assertFailsWith<IllegalStateException> { target.person.selectAll.asList() }
            assertFailsWith<IllegalStateException> {
                target.newOversqliteClient(2, DATABASE_ID, SERVICE_ID, http, ServerWinsResolver)
            }
            assertFailsWith<IllegalStateException> {
                withContext(Dispatchers.Default.limitedParallelism(1)) {
                    withTimeout(10_000) { target.person.selectAll.asFlow().first() }
                }
            }

            assertEquals(OversqliteOpeningOutcome.READY, targetOpening.advance().outcome)
            assertNotNull(targetOpening.readyOrNull())
            val people = target.person.selectAll.asList()
            assertEquals("Ada", people.single().name)
            assertNull(people.single().details)
            assertNull(people.single().projectId)
            assertEquals(
                1,
                withContext(Dispatchers.Default.limitedParallelism(1)) {
                    withTimeout(10_000) { target.person.selectAll.asFlow().first() }
                }.size,
            )
            assertEquals("generation one", target.note.selectAll.asList().single().body)
            assertEquals("MIXED", target.draft.selectAll.asList().single().normalized)

            target.project.insert(ProjectQuery.Insert.Params(PROJECT_ID, "Additive"))
            target.person.insert(PersonQuery.Insert.Params(PERSON_TWO_ID, "Grace", "target", PROJECT_ID))
            target.attachment.insert(AttachmentQuery.Insert.Params(ATTACHMENT_ID, PERSON_TWO_ID, "proof.bin"))
            assertEquals(1, target.attachment.selectAll.asList().size)
            assertEquals(
                3L,
                scalarLong(
                    target.connection(),
                    "SELECT count(*) FROM sqlite_schema WHERE name IN " +
                        "('projects_title_lookup','active_project_people','drafts_touch')",
                ),
            )
            assertEquals(
                "Grace|Additive",
                scalarText(
                    target.connection(),
                    "SELECT name || '|' || title FROM active_project_people WHERE id='$PERSON_TWO_ID'",
                ),
            )
            target.connection().execSQL("UPDATE drafts SET label='ready' WHERE id='$DRAFT_ID'")
            assertEquals(
                "READY",
                scalarText(target.connection(), "SELECT normalized FROM drafts WHERE id='$DRAFT_ID'"),
            )
            assertEquals(0L, scalarLong(target.connection(), "SELECT count(*) FROM pragma_foreign_key_check"))
            target.connection().execSQL("DELETE FROM projects WHERE id='$PROJECT_ID'")
            assertNull(target.person.selectAll.asList().single { it.id == PERSON_TWO_ID }.projectId)
            target.connection().execSQL("DELETE FROM people WHERE id='$PERSON_TWO_ID'")
            assertTrue(target.attachment.selectAll.asList().isEmpty())
        } finally {
            runCatching { targetOpening?.close() }
            runCatching { invalidOpening?.close() }
            runCatching { localOpening?.close() }
            runCatching { generationOneOpening?.close() }
            http.close()
            if (web) cleanupOwnedWorkerStorage() else deleteTempSqliteNowTestDbArtifacts(path)
        }
    }

    private fun fixtureServer(): MockSyncServer = MockSyncServer(
        registeredTableSpecs = testRegisteredTableSpecs("people", "notes", "projects", "attachments"),
        generationResponses = mapOf(1L to schemaevolutionGenerationResponse(1), 2L to schemaevolutionGenerationResponse(2)),
    )

    private companion object {
        const val DATABASE_ID = "schemaevolution-generated-platform"
        const val SERVICE_ID = "schemaevolution-authority"
        const val PERSON_ID = "11111111-1111-4111-8111-111111111111"
        const val NOTE_ID = "22222222-2222-4222-8222-222222222222"
        const val DRAFT_ID = "draft-platform"
        const val PROJECT_ID = "33333333-3333-4333-8333-333333333333"
        const val PERSON_TWO_ID = "44444444-4444-4444-8444-444444444444"
        const val ATTACHMENT_ID = "55555555-5555-4555-8555-555555555555"
    }
}
