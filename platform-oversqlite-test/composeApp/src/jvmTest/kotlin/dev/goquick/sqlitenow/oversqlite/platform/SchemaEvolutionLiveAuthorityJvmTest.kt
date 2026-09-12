package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.DeferConflictResolver
import dev.goquick.sqlitenow.oversqlite.OversqliteOpening
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningNextAction
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.OversqliteRemoteContractException
import dev.goquick.sqlitenow.oversqlite.OversqliteTransientRetryPolicy
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.UploadHttpException
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.DraftQuery as GenerationOneDraftQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.SchemaEvolutionGenerationOneDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.NoteQuery as GenerationOneNoteQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.PersonQuery as GenerationOnePersonQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.generationone.VersionBasedDatabaseMigrations as GenerationOneMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.SchemaEvolutionLocalTwoDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.NoteQuery as LocalTwoNoteQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.localtwo.VersionBasedDatabaseMigrations as LocalTwoMigrations
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.AttachmentQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.SchemaEvolutionGenerationTargetDatabase
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.NoteQuery as TargetNoteQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.PersonQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.ProjectQuery
import dev.goquick.sqlitenow.oversqlite.schemaevolution.target.VersionBasedDatabaseMigrations as TargetMigrations
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.HttpClientPlugin
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.io.Closeable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import io.ktor.util.AttributeKey
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

internal class SchemaEvolutionLiveAuthorityJvmTest {
    @Test
    fun generationOneToTwoDrainsOldWriterAndPreservesDrafts() = runBlocking {
        val baseUrl = requiredAuthority("ADDITIVE") ?: return@runBlocking
        val root = Files.createTempDirectory("schema-evolution-additive-upgrade")
        val user = "schema-evolution-additive-${System.nanoTime()}"
        val token = issueToken(baseUrl, user)
        val httpA = authorityHttp(baseUrl, token)
        val httpB = authorityHttp(baseUrl, token)
        val pathA = root.resolve("a.sqlite3").toString()
        val pathB = root.resolve("b.sqlite3").toString()
        var openingA: OversqliteOpening<*>? = null
        var openingB: OversqliteOpening<*>? = null
        try {
            val oneA = SchemaEvolutionGenerationOneDatabase(pathA, GenerationOneMigrations())
            openingA = oneA.openForSync(oneA.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), httpA, ServerWinsResolver)
            assertEquals(OversqliteOpeningOutcome.READY, openingA.advance().outcome)
            val clientA = oneA.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, httpA, ServerWinsResolver)
            assertTrue(clientA.attach(user).getOrThrow() is AttachResult.Connected)
            oneA.person.insert(GenerationOnePersonQuery.Insert.Params(PERSON_ID, "Ada"))
            oneA.note.insert(GenerationOneNoteQuery.Insert.Params(NOTE_ID, "settled"))
            oneA.draft.insert(GenerationOneDraftQuery.Insert.Params(DRAFT_ID, "mixed", byteArrayOf(0, 1, 2, -1)))
            clientA.sync().getOrThrow()
            openingA.close(); openingA = null

            val oneB = SchemaEvolutionGenerationOneDatabase(pathB, GenerationOneMigrations())
            openingB = oneB.openForSync(oneB.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), httpB, ServerWinsResolver)
            assertEquals(OversqliteOpeningOutcome.READY, openingB.advance().outcome)
            val clientB = oneB.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, httpB, ServerWinsResolver)
            assertTrue(clientB.attach(user).getOrThrow() is AttachResult.Connected)
            oneB.note.insert(GenerationOneNoteQuery.Insert.Params(OLD_WRITER_ID, "old writer"))
            clientB.sync().getOrThrow()

            val pending = SchemaEvolutionGenerationOneDatabase(pathA, GenerationOneMigrations())
            openingA = pending.openForSync(pending.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), httpA, ServerWinsResolver)
            assertEquals(OversqliteOpeningOutcome.READY, openingA.advance().outcome)
            pending.note.insert(GenerationOneNoteQuery.Insert.Params(PENDING_ID, "pending intent"))
            openingA.close(); openingA = null
            advanceLocalTwo(pathA, httpA)

            val target = SchemaEvolutionGenerationTargetDatabase(pathA, targetMigrations())
            openingA = target.openForSync(
                target.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), httpA, ServerWinsResolver,
                additivePlan(targetGeneration = 2, fromLocalVersion = 2),
            )
            assertNull(openingA.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.READY, openingA.advance().outcome)
            assertEquals(setOf(NOTE_ID, OLD_WRITER_ID, PENDING_ID), target.note.selectAll.asList().map { it.id }.toSet())
            assertEquals("MIXED", target.draft.selectAll.asList().single().normalized)
            assertEquals("000102FF", scalarText(target.connection(), "SELECT hex(payload) FROM drafts WHERE id='$DRAFT_ID'"))
            assertEquals(0L, scalarLong(target.connection(), "SELECT count(*) FROM pragma_foreign_key_check"))
        } finally {
            runCatching { openingA?.close() }; runCatching { openingB?.close() }
            httpA.close(); httpB.close(); root.toFile().deleteRecursively()
        }
    }

    @Test
    fun sparseGenerationOneToThreeHasNoGenerationTwoTraffic() = runBlocking {
        val baseUrl = requiredAuthority("SPARSE") ?: return@runBlocking
        val root = Files.createTempDirectory("schema-evolution-sparse-upgrade")
        val user = "schema-evolution-sparse-${System.nanoTime()}"
        val http = authorityHttp(baseUrl, issueToken(baseUrl, user))
        val path = root.resolve("sparse.sqlite3").toString()
        var opening: OversqliteOpening<*>? = null
        try {
            val one = SchemaEvolutionGenerationOneDatabase(path, GenerationOneMigrations())
            opening = one.openForSync(one.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), http, ServerWinsResolver)
            opening.advance(); one.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, http, ServerWinsResolver).attach(user).getOrThrow()
            one.person.insert(GenerationOnePersonQuery.Insert.Params(PERSON_ID, "Sparse"))
            opening.close(); opening = null
            advanceLocalTwo(path, http)

            val target = SchemaEvolutionGenerationTargetDatabase(path, targetMigrations())
            opening = target.openForSync(
                target.buildOversqliteConfig(3, DATABASE_ID, SERVICE_ID), http, ServerWinsResolver,
                additivePlan(targetGeneration = 3, fromLocalVersion = 2),
            )
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
            val status = status(baseUrl)
            val generations = status.getValue("generations").toString()
            assertFalse(generations.contains("\"id\":2"), generations)
            val paths = status.getValue("request_counters").jsonObject.getValue("paths").toString()
            assertFalse(paths.contains("/sync/generations/2"), paths)
        } finally {
            runCatching { opening?.close() }; http.close(); root.toFile().deleteRecursively()
        }
    }

    @Test
    fun pausedTargetKeepsOldWriterAvailable() = runBlocking {
        val baseUrl = requiredAuthority("ADDITIVE") ?: return@runBlocking
        val controlToken = checkNotNull(System.getenv("SCHEMA_EVOLUTION_ADDITIVE_CONTROL_TOKEN"))
        val root = Files.createTempDirectory("schema-evolution-paused-target")
        val user = "schema-evolution-paused-${System.nanoTime()}"
        val token = issueToken(baseUrl, user)
        val oldHttp = authorityHttp(baseUrl, token)
        val targetHttp = authorityHttp(baseUrl, token)
        val oldPath = root.resolve("old.sqlite3").toString()
        val targetPath = root.resolve("target.sqlite3").toString()
        var oldOpening: OversqliteOpening<*>? = null
        var targetOpening: OversqliteOpening<*>? = null
        try {
            val seed = SchemaEvolutionGenerationOneDatabase(targetPath, GenerationOneMigrations())
            targetOpening = seed.openForSync(seed.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), targetHttp, ServerWinsResolver)
            targetOpening.advance(); seed.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, targetHttp, ServerWinsResolver).attach(user).getOrThrow()
            targetOpening.close(); targetOpening = null
            advanceLocalTwo(targetPath, targetHttp)

            val old = SchemaEvolutionGenerationOneDatabase(oldPath, GenerationOneMigrations())
            oldOpening = old.openForSync(old.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), oldHttp, ServerWinsResolver)
            oldOpening.advance()
            val oldClient = old.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, oldHttp, ServerWinsResolver)
            oldClient.attach(user).getOrThrow()

            control(baseUrl, controlToken, """{"action":"pause_target"}""")
            val target = SchemaEvolutionGenerationTargetDatabase(targetPath, targetMigrations())
            targetOpening = target.openForSync(
                target.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), targetHttp, ServerWinsResolver,
                additivePlan(2, 2),
            )
            runCatching { targetOpening.advance() }
            assertNull(targetOpening.readyOrNull())

            old.note.insert(GenerationOneNoteQuery.Insert.Params(OLD_WRITER_ID, "while target paused"))
            oldClient.sync().getOrThrow()
            control(baseUrl, controlToken, """{"action":"resume_target"}""")
            repeat(4) { if (targetOpening.readyOrNull() == null) targetOpening.advance() }
            assertNotNull(targetOpening.readyOrNull())
            assertTrue(target.note.selectAll.asList().any { it.id == OLD_WRITER_ID && it.body == "while target paused" })
        } finally {
            runCatching { control(baseUrl, controlToken, """{"action":"resume_target"}""") }
            runCatching { targetOpening?.close() }; runCatching { oldOpening?.close() }
            oldHttp.close(); targetHttp.close(); root.toFile().deleteRecursively()
        }
    }

    @Test
    fun seedRetiredGeneration() = runBlocking {
        val additiveBaseUrl = requiredAuthority("ADDITIVE") ?: return@runBlocking
        val path = checkNotNull(System.getenv("SCHEMA_EVOLUTION_RETIRED_SQLITE_PATH"))
        val user = checkNotNull(System.getenv("SCHEMA_EVOLUTION_RETIRED_USER"))
        val additiveHttp = authorityHttp(additiveBaseUrl, issueToken(additiveBaseUrl, user))
        var opening: OversqliteOpening<*>? = null
        try {
            val one = SchemaEvolutionGenerationOneDatabase(path, GenerationOneMigrations())
            opening = one.openForSync(
                one.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), additiveHttp, ServerWinsResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
            one.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, additiveHttp, ServerWinsResolver)
                .attach(user).getOrThrow()
            one.draft.insert(GenerationOneDraftQuery.Insert.Params(DRAFT_ID, "retired", byteArrayOf(9, 0, -1)))
            opening.close(); opening = null
            advanceLocalTwo(path, additiveHttp)
        } finally {
            runCatching { opening?.close() }; additiveHttp.close()
        }
    }

    @Test
    fun retiredGenerationReplacesDirectlyWithoutOldUpload() = runBlocking {
        val baseUrl = requiredAuthority("RETIRED") ?: return@runBlocking
        val path = checkNotNull(System.getenv("SCHEMA_EVOLUTION_RETIRED_SQLITE_PATH"))
        val user = checkNotNull(System.getenv("SCHEMA_EVOLUTION_RETIRED_USER"))
        val http = authorityHttp(baseUrl, issueToken(baseUrl, user))
        var opening: OversqliteOpening<*>? = null
        try {
            val before = status(baseUrl).getValue("request_counters").toString()

            val target = SchemaEvolutionGenerationTargetDatabase(path, targetMigrations())
            opening = target.openForSync(
                target.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), http, ServerWinsResolver,
                additivePlan(2, 2),
            )
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
            assertEquals("RETIRED", target.draft.selectAll.asList().single().normalized)
            val after = status(baseUrl).getValue("request_counters").toString()
            assertFalse(after.substringAfter(before, after).contains("push-sessions"), after)
        } finally {
            runCatching { opening?.close() }; http.close()
        }
    }

    @Test
    fun seedActionRequiredVariants() = runBlocking {
        val baseUrl = requiredAuthority("ADDITIVE") ?: return@runBlocking
        val controlToken = checkNotNull(System.getenv("SCHEMA_EVOLUTION_ADDITIVE_CONTROL_TOKEN"))
        val root = Path.of(checkNotNull(System.getenv("SCHEMA_EVOLUTION_ACTION_REQUIRED_DIR")))
        Files.createDirectories(root)
        for ((index, variant) in ACTION_REQUIRED_VARIANTS.withIndex()) {
            val user = "${checkNotNull(System.getenv("SCHEMA_EVOLUTION_ACTION_REQUIRED_RUN"))}-$variant"
            val http = authorityHttp(baseUrl, issueToken(baseUrl, user))
            try {
                seedActionRequiredVariant(
                    root = root,
                    variant = variant,
                    index = index + 1,
                    user = user,
                    http = http,
                    baseUrl = baseUrl,
                    controlToken = controlToken,
                )
            } finally {
                http.close()
            }
        }
    }

    @Test
    fun actionRequiredStatesRemainDurableAcrossReopen() = runBlocking {
        val baseUrl = requiredAuthority("RETIRED") ?: return@runBlocking
        val root = Path.of(checkNotNull(System.getenv("SCHEMA_EVOLUTION_ACTION_REQUIRED_DIR")))
        val retiredProof = status(baseUrl).getValue("generations").toString()
        for (variant in ACTION_REQUIRED_VARIANTS) {
            val path = root.resolve("$variant.sqlite3")
            val user = "${checkNotNull(System.getenv("SCHEMA_EVOLUTION_ACTION_REQUIRED_RUN"))}-$variant"
            val cleanupHold = if (variant == "dirty") HeldDerivedCleanupPlugin() else null
            val http = authorityHttp(baseUrl, issueToken(baseUrl, user), cleanupHold)
            var opening: OversqliteOpening<*>? = null
            try {
                assertEquals(2L, pathScalarLong(path, "PRAGMA user_version"), variant)
                assertEquals(1L, pathScalarLong(path, "SELECT generation_id FROM _sync_contract_identity"), variant)
                assertEquals(0L, pathScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='projects'"), variant)
                assertEquals("local_only", pathScalarText(path, "SELECT mode FROM _sync_opening_state"), variant)
                val sourceBefore = pathScalarText(path, "SELECT current_source_id FROM _sync_attachment_state")

                val target = SchemaEvolutionGenerationTargetDatabase(path.toString(), targetMigrations())
                opening = target.openForSync(
                    target.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), http, DeferConflictResolver,
                    additivePlan(2, 2),
                )
                val activeOpening = opening
                val blocked = if (cleanupHold == null) {
                    activeOpening.advance()
                } else {
                    val advancing = async(start = CoroutineStart.UNDISPATCHED) { activeOpening.advance() }
                    withTimeout(5_000) { cleanupHold.cleanupEntered.await() }
                    assertFalse(
                        advancing.isCompleted,
                        "advance must not expose retired-pending until the derived HTTP job is closed",
                    )
                    cleanupHold.release.complete(Unit)
                    val result = withTimeout(5_000) { advancing.await() }
                    withTimeout(5_000) { cleanupHold.cleanupComplete.await() }
                    assertEquals(1, cleanupHold.closeCalls)
                    Json.parseToJsonElement(http.get("$baseUrl/schema-evolution/status") {
                        header(
                            "X-Schema-Evolution-Control-Token",
                            checkNotNull(System.getenv("SCHEMA_EVOLUTION_RETIRED_CONTROL_TOKEN")),
                        )
                    }.bodyAsText()).jsonObject
                    result
                }
                assertEquals(OversqliteOpeningOutcome.OLD_GENERATION_UNAVAILABLE, blocked.outcome, variant)
                assertEquals(OversqliteOpeningNextAction.RESTORE_SUPPORT, blocked.nextAction, variant)
                assertEquals("retired_schema_generation", blocked.diagnosticCode, variant)
                assertTrue(blocked.diagnosticMessage.orEmpty().contains("fingerprint="), variant)
                assertTrue(
                    checkNotNull(blocked.pendingDirtyCount) + checkNotNull(blocked.pendingFrozenCount) +
                        checkNotNull(blocked.pendingConflictCount) > 0,
                    variant,
                )
                assertNull(opening.readyOrNull(), variant)
                opening.close()
                opening.close()
                opening = null

                assertEquals("local_only", pathScalarText(path, "SELECT mode FROM _sync_opening_state"), variant)
                assertEquals("retired_schema_generation", pathScalarText(path, "SELECT failure_code FROM _sync_opening_state"), variant)
                assertEquals(sourceBefore, pathScalarText(path, "SELECT current_source_id FROM _sync_attachment_state"), variant)
                assertEquals(2L, pathScalarLong(path, "PRAGMA user_version"), variant)
                assertEquals(0L, pathScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='projects'"), variant)
                val afterMarker = fileIdentity(path)
                val proofsAfterMarker = status(baseUrl).getValue("request_counters").jsonObject
                    .getValue("proofs").toString()

                val reopened = SchemaEvolutionGenerationTargetDatabase(path.toString(), targetMigrations())
                opening = reopened.openForSync(
                    reopened.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), http, DeferConflictResolver,
                    additivePlan(2, 2),
                )
                val durable = opening.status()
                assertEquals(blocked, durable, variant)
                assertEquals(blocked, opening.advance(), variant)
                assertNull(opening.readyOrNull(), variant)
                opening.close(); opening = null
                assertEquals(
                    proofsAfterMarker,
                    status(baseUrl).getValue("request_counters").jsonObject.getValue("proofs").toString(),
                    variant,
                )
                assertEquals(afterMarker, fileIdentity(path), variant)
                assertTrue(retiredProof.contains("\"id\":1") && retiredProof.contains("\"state\":\"retired\""))
            } finally {
                cleanupHold?.release?.complete(Unit)
                runCatching { opening?.close() }
                http.close()
            }
        }
    }

    @Test
    fun checkpointAndPersistentInterruptionRepresentatives() = runBlocking {
        val additive = requiredAuthority("ADDITIVE") ?: return@runBlocking
        val sparse = requiredAuthority("SPARSE") ?: return@runBlocking
        assertEquals("additive", status(additive).getValue("mode").jsonPrimitive.content)
        assertEquals("sparse", status(sparse).getValue("mode").jsonPrimitive.content)
        assertTrue(status(additive).getValue("generations").toString().contains("\"id\":2"))
        assertTrue(status(sparse).getValue("generations").toString().contains("\"id\":3"))
    }

    @Test
    fun nullableForeignKeysAndScopeIsolationHold() = runBlocking {
        val baseUrl = requiredAuthority("ADDITIVE") ?: return@runBlocking
        val controlToken = checkNotNull(System.getenv("SCHEMA_EVOLUTION_ADDITIVE_CONTROL_TOKEN"))
        val root = Files.createTempDirectory("schema-evolution-nullable-relationships")
        val userA = "schema-evolution-nullable-a-${System.nanoTime()}"
        val userB = "schema-evolution-nullable-b-${System.nanoTime()}"
        val tokenA = issueToken(baseUrl, userA)
        val httpA = authorityHttp(baseUrl, tokenA)
        val httpAObserver = authorityHttp(baseUrl, tokenA)
        val httpAOld = authorityHttp(baseUrl, tokenA)
        val httpB = authorityHttp(baseUrl, issueToken(baseUrl, userB))
        var openingA: OversqliteOpening<*>? = null
        var openingAObserver: OversqliteOpening<*>? = null
        var openingAOld: OversqliteOpening<*>? = null
        var openingB: OversqliteOpening<*>? = null
        try {
            val targetA = SchemaEvolutionGenerationTargetDatabase(root.resolve("a.sqlite3").toString(), targetMigrations())
            openingA = targetA.openForSync(targetA.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), httpA, ServerWinsResolver)
            assertEquals(OversqliteOpeningOutcome.READY, openingA.advance().outcome)
            val clientA = targetA.newOversqliteClient(2, DATABASE_ID, SERVICE_ID, httpA, ServerWinsResolver)
            clientA.attach(userA).getOrThrow()
            targetA.project.insert(ProjectQuery.Insert.Params(PROJECT_ID, "Project"))
            targetA.person.insert(PersonQuery.Insert.Params(PERSON_ID, "Ada", "target-details", PROJECT_ID))
            targetA.attachment.insert(AttachmentQuery.Insert.Params(ATTACHMENT_ID, PERSON_ID, "proof.bin"))
            targetA.note.insert(TargetNoteQuery.Insert.Params(NOTE_ID, "scope-a-note"))
            clientA.sync().getOrThrow()

            val observerA = SchemaEvolutionGenerationTargetDatabase(
                root.resolve("a-observer.sqlite3").toString(),
                targetMigrations(),
            )
            openingAObserver = observerA.openForSync(
                observerA.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID),
                httpAObserver,
                ServerWinsResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, openingAObserver.advance().outcome)
            val observerClientA = observerA.newOversqliteClient(
                2, DATABASE_ID, SERVICE_ID, httpAObserver, ServerWinsResolver,
            )
            observerClientA.attach(userA).getOrThrow()
            assertEquals("target-details", observerA.person.selectAll.asList().single().details)
            assertEquals(PROJECT_ID, observerA.person.selectAll.asList().single().projectId)

            val targetB = SchemaEvolutionGenerationTargetDatabase(root.resolve("b.sqlite3").toString(), targetMigrations())
            openingB = targetB.openForSync(targetB.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID), httpB, ServerWinsResolver)
            assertEquals(OversqliteOpeningOutcome.READY, openingB.advance().outcome)
            val clientB = targetB.newOversqliteClient(2, DATABASE_ID, SERVICE_ID, httpB, ServerWinsResolver)
            clientB.attach(userB).getOrThrow()
            targetB.project.insert(ProjectQuery.Insert.Params(PROJECT_B_ID, "Scope B"))
            targetB.person.insert(PersonQuery.Insert.Params(PERSON_B_ID, "Babbage", "scope-b", PROJECT_B_ID))
            targetB.attachment.insert(AttachmentQuery.Insert.Params(ATTACHMENT_B_ID, PERSON_B_ID, "scope-b.bin"))
            targetB.note.insert(TargetNoteQuery.Insert.Params(NOTE_B_ID, "scope-b-note"))
            clientB.sync().getOrThrow()
            val clientSnapshotBBefore = listOf(
                targetB.project.selectAll.asList(),
                targetB.person.selectAll.asList(),
                targetB.attachment.selectAll.asList(),
                targetB.note.selectAll.asList(),
            )
            val authorityRowsBBefore = control(
                baseUrl,
                controlToken,
                """{"action":"inspect","user_id":"$userB"}""",
            ).getValue("rows")

            val oldA = SchemaEvolutionGenerationOneDatabase(root.resolve("a-old.sqlite3").toString(), GenerationOneMigrations())
            openingAOld = oldA.openForSync(
                oldA.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID),
                httpAOld,
                ServerWinsResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, openingAOld.advance().outcome)
            val oldClientA = oldA.newOversqliteClient(1, DATABASE_ID, SERVICE_ID, httpAOld, ServerWinsResolver)
            oldClientA.attach(userA).getOrThrow()
            assertEquals("Ada", oldA.person.selectAll.asList().single().name)
            oldA.connection().execSQL("UPDATE people SET name='Ada from generation one' WHERE id='$PERSON_ID'")
            oldClientA.sync().getOrThrow()

            observerClientA.sync().getOrThrow()
            val afterOldWrite = observerA.person.selectAll.asList().single()
            assertEquals("Ada from generation one", afterOldWrite.name)
            assertEquals("target-details", afterOldWrite.details)
            assertEquals(PROJECT_ID, afterOldWrite.projectId)
            assertAuthorityPerson(
                control(baseUrl, controlToken, """{"action":"inspect","user_id":"$userA"}"""),
                expectedName = "Ada from generation one",
                expectedDetails = "target-details",
                expectedProjectId = PROJECT_ID,
            )

            clientA.sync().getOrThrow()
            targetA.connection().execSQL("UPDATE people SET project_id=NULL WHERE id='$PERSON_ID'")
            clientA.sync().getOrThrow()
            observerClientA.sync().getOrThrow()
            assertNull(observerA.person.selectAll.asList().single().projectId)

            targetA.connection().execSQL("UPDATE people SET project_id='$PROJECT_ID' WHERE id='$PERSON_ID'")
            clientA.sync().getOrThrow()
            observerClientA.sync().getOrThrow()
            assertEquals(PROJECT_ID, observerA.person.selectAll.asList().single().projectId)

            targetA.connection().execSQL("DELETE FROM projects WHERE id='$PROJECT_ID'")
            clientA.sync().getOrThrow()
            observerClientA.sync().getOrThrow()
            assertTrue(observerA.project.selectAll.asList().isEmpty())
            assertNull(observerA.person.selectAll.asList().single().projectId)
            assertAuthorityPerson(
                control(baseUrl, controlToken, """{"action":"inspect","user_id":"$userA"}"""),
                expectedName = "Ada from generation one",
                expectedDetails = "target-details",
                expectedProjectId = null,
            )

            targetA.connection().execSQL("DELETE FROM people WHERE id='$PERSON_ID'")
            clientA.sync().getOrThrow()
            observerClientA.sync().getOrThrow()
            assertTrue(observerA.person.selectAll.asList().isEmpty())
            assertTrue(observerA.attachment.selectAll.asList().isEmpty())
            val inspectedA = control(baseUrl, controlToken, """{"action":"inspect","user_id":"$userA"}""")
            assertEquals("0", inspectedA.getValue("people").jsonPrimitive.content)
            assertEquals("0", inspectedA.getValue("attachments").jsonPrimitive.content)
            assertEquals("1", inspectedA.getValue("notes").jsonPrimitive.content)
            assertNoAuthorityOrphans(inspectedA)

            clientB.sync().getOrThrow()
            val clientSnapshotBAfter = listOf(
                targetB.project.selectAll.asList(),
                targetB.person.selectAll.asList(),
                targetB.attachment.selectAll.asList(),
                targetB.note.selectAll.asList(),
            )
            assertEquals(clientSnapshotBBefore, clientSnapshotBAfter)
            val inspectedB = control(baseUrl, controlToken, """{"action":"inspect","user_id":"$userB"}""")
            assertEquals("1", inspectedB.getValue("people").jsonPrimitive.content)
            assertEquals("1", inspectedB.getValue("notes").jsonPrimitive.content)
            assertEquals("1", inspectedB.getValue("projects").jsonPrimitive.content)
            assertEquals("1", inspectedB.getValue("attachments").jsonPrimitive.content)
            assertEquals(authorityRowsBBefore, inspectedB.getValue("rows"))
            assertAuthorityPerson(inspectedB, "Babbage", "scope-b", PROJECT_B_ID)
            assertNoAuthorityOrphans(inspectedB)

            for (connection in listOf(targetA.connection(), observerA.connection(), targetB.connection())) {
                assertEquals(0L, scalarLong(connection, "SELECT count(*) FROM pragma_foreign_key_check"))
            }
        } finally {
            runCatching { openingAOld?.close() }
            runCatching { openingAObserver?.close() }
            runCatching { openingA?.close() }
            runCatching { openingB?.close() }
            httpA.close(); httpAObserver.close(); httpAOld.close(); httpB.close()
            root.toFile().deleteRecursively()
        }
    }

    private fun assertAuthorityPerson(
        inspected: JsonObject,
        expectedName: String,
        expectedDetails: String,
        expectedProjectId: String?,
    ) {
        val people = inspected.getValue("rows").jsonObject.getValue("people").jsonArray
        assertEquals(1, people.size)
        val person = people.single().jsonObject
        assertEquals(expectedName, person.getValue("name").jsonPrimitive.content)
        assertEquals(expectedDetails, person.getValue("details").jsonPrimitive.content)
        val projectId = person.getValue("project_id")
        if (expectedProjectId == null) {
            assertEquals("null", projectId.toString())
        } else {
            assertEquals(expectedProjectId, projectId.jsonPrimitive.content)
        }
    }

    private fun assertNoAuthorityOrphans(inspected: JsonObject) {
        val orphanCounts = inspected.getValue("orphan_counts").jsonObject
        assertEquals("0", orphanCounts.getValue("people_projects").jsonPrimitive.content)
        assertEquals("0", orphanCounts.getValue("attachments_people").jsonPrimitive.content)
    }

    private fun targetMigrations() = TargetMigrations { step ->
        if (step.toVersion == 2) {
            step.connection.execSQL(
                "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                    "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END",
            )
        }
    }

    private suspend fun seedActionRequiredVariant(
        root: Path,
        variant: String,
        index: Int,
        user: String,
        http: HttpClient,
        baseUrl: String,
        controlToken: String,
    ) {
        val path = root.resolve("$variant.sqlite3")
        val initialId = "70000000-0000-4000-8000-${index.toString().padStart(12, '0')}"
        val pendingId = "71000000-0000-4000-8000-${index.toString().padStart(12, '0')}"
        var opening: OversqliteOpening<*>? = null
        try {
            val one = SchemaEvolutionGenerationOneDatabase(path.toString(), GenerationOneMigrations())
            opening = one.openForSync(
                one.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), http, DeferConflictResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome, variant)
            val oneClient = one.newOversqliteClient(
                1, DATABASE_ID, SERVICE_ID, http, DeferConflictResolver,
            )
            oneClient.attach(user).getOrThrow()
            one.note.insert(GenerationOneNoteQuery.Insert.Params(initialId, "initial-$variant"))
            one.draft.insert(
                GenerationOneDraftQuery.Insert.Params("draft-$variant", variant, byteArrayOf(index.toByte(), 0, -1)),
            )
            oneClient.sync().getOrThrow()
            opening.close(); opening = null
            advanceLocalTwo(path.toString(), http)

            val local = SchemaEvolutionLocalTwoDatabase(path.toString(), LocalTwoMigrations())
            val retryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1)
            opening = local.openForSync(
                local.buildOversqliteConfig(
                    1, DATABASE_ID, SERVICE_ID, transientRetryPolicy = retryPolicy,
                ),
                http,
                DeferConflictResolver,
            )
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome, variant)
            val client = local.newOversqliteClient(
                1, DATABASE_ID, SERVICE_ID, http, DeferConflictResolver,
                transientRetryPolicy = retryPolicy,
            )
            client.attach(user).getOrThrow()
            when (variant) {
                "dirty" -> local.note.insert(LocalTwoNoteQuery.Insert.Params(pendingId, "dirty"))
                "frozen" -> {
                    local.note.insert(LocalTwoNoteQuery.Insert.Params(pendingId, "frozen"))
                    control(baseUrl, controlToken, """{"kind":"commit_rejection","status":409,"count":1}""", true)
                    val failure = client.sync().exceptionOrNull()
                    assertTrue(failure is UploadHttpException, failure.toString())
                }
                "rejected" -> {
                    local.note.insert(LocalTwoNoteQuery.Insert.Params(pendingId, "rejected"))
                    control(baseUrl, controlToken, """{"kind":"commit_rejection","status":422,"count":1}""", true)
                    val failure = client.sync().exceptionOrNull()
                    assertTrue(failure is OversqliteRemoteContractException, failure.toString())
                    assertEquals("push_effect_coverage_incomplete", failure.code)
                }
                "ambiguous" -> {
                    local.note.insert(LocalTwoNoteQuery.Insert.Params(pendingId, "ambiguous"))
                    control(baseUrl, controlToken, """{"kind":"lost_reply","status":500,"count":1}""", true)
                    assertTrue(client.sync().isFailure, variant)
                }
                "conflicted" -> {
                    val peer = SchemaEvolutionGenerationOneDatabase(
                        root.resolve("conflicted-peer.sqlite3").toString(),
                        GenerationOneMigrations(),
                    )
                    var peerOpening: OversqliteOpening<*>? = null
                    try {
                        peerOpening = peer.openForSync(
                            peer.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), http, DeferConflictResolver,
                        )
                        peerOpening.advance()
                        val peerClient = peer.newOversqliteClient(
                            1, DATABASE_ID, SERVICE_ID, http, DeferConflictResolver,
                        )
                        peerClient.attach(user).getOrThrow()
                        peer.connection().execSQL("UPDATE notes SET body='remote-conflict' WHERE id='$initialId'")
                        peerClient.sync().getOrThrow()
                        local.connection().execSQL("UPDATE notes SET body='local-conflict' WHERE id='$initialId'")
                        assertTrue(client.sync().isFailure, variant)
                        assertNotNull(client.pendingConflict().getOrThrow(), variant)
                    } finally {
                        runCatching { peerOpening?.close() }
                    }
                }
                else -> error("unsupported action-required variant $variant")
            }
        } finally {
            runCatching { opening?.close() }
        }
    }

    private suspend fun pathScalarLong(path: Path, sql: String): Long =
        androidx.sqlite.driver.bundled.BundledSQLiteDriver().open(path.toString()).use { connection ->
            connection.prepare(sql).use { statement -> check(statement.step()); statement.getLong(0) }
        }

    private suspend fun pathScalarText(path: Path, sql: String): String =
        androidx.sqlite.driver.bundled.BundledSQLiteDriver().open(path.toString()).use { connection ->
            connection.prepare(sql).use { statement -> check(statement.step()); statement.getText(0) }
        }

    private fun fileIdentity(path: Path): Pair<Long, String> {
        val bytes = Files.readAllBytes(path)
        return bytes.size.toLong() to MessageDigest.getInstance("SHA-256")
            .digest(bytes)
            .joinToString("") { byte -> "%02x".format(byte) }
    }

    private suspend fun advanceLocalTwo(path: String, http: HttpClient) {
        val database = SchemaEvolutionLocalTwoDatabase(
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
        val opening = database.openForSync(
            database.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID), http, ServerWinsResolver,
            OversqliteUpgradePlan("schemaevolution-local-v1-v2", 1, 1, 1, 2, setOf("drafts")),
        )
        try {
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
        } finally {
            opening.close()
        }
    }

    private fun additivePlan(targetGeneration: Long, fromLocalVersion: Int) = OversqliteUpgradePlan(
        migrationId = "schemaevolution-g1-g$targetGeneration-v$fromLocalVersion-v3",
        fromGenerationId = 1,
        toGenerationId = targetGeneration,
        fromLocalVersion = fromLocalVersion,
        toLocalVersion = 3,
        preservedLocalTables = setOf("drafts"),
    )

    private fun requiredAuthority(mode: String): String? =
        System.getenv("SCHEMA_EVOLUTION_${mode}_BASE_URL")?.trim()?.takeIf(String::isNotEmpty).also {
            if (it == null) println("Skipping schemaevolution $mode live selector; use tools/qualification/schema_evolution_authority.py")
        }

    private suspend fun issueToken(baseUrl: String, user: String): String {
        val client = HttpClient(CIO)
        return try {
            Json.parseToJsonElement(client.post("$baseUrl/dummy-signin") {
                contentType(ContentType.Application.Json)
                setBody("""{"user":"$user","password":"anything","device":"schemaevolution-live"}""")
            }.bodyAsText()).jsonObject.getValue("token").jsonPrimitive.content
        } finally {
            client.close()
        }
    }

    private fun authorityHttp(
        baseUrl: String,
        token: String,
        cleanupHold: HeldDerivedCleanupPlugin? = null,
    ) = HttpClient(CIO) {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        if (cleanupHold != null) install(cleanupHold)
        defaultRequest { url(baseUrl); header(HttpHeaders.Authorization, "Bearer $token") }
    }

    private suspend fun status(baseUrl: String): JsonObject {
        val controlToken = listOf("ADDITIVE", "SPARSE", "RETIRED")
            .firstNotNullOfOrNull { mode ->
                if (System.getenv("SCHEMA_EVOLUTION_${mode}_BASE_URL") == baseUrl) {
                    System.getenv("SCHEMA_EVOLUTION_${mode}_CONTROL_TOKEN")
                } else {
                    null
                }
            }
            ?: error("No schemaevolution control token is bound to $baseUrl")
        val client = HttpClient(CIO)
        return try {
            Json.parseToJsonElement(client.get("$baseUrl/schema-evolution/status") {
                header("X-Schema-Evolution-Control-Token", controlToken)
            }.bodyAsText()).jsonObject
        } finally {
            client.close()
        }
    }

    private suspend fun control(baseUrl: String, token: String, body: String, fault: Boolean = false): JsonObject {
        val client = HttpClient(CIO)
        return try {
            val response = client.post("$baseUrl/schema-evolution/${if (fault) "fault" else "control"}") {
                header("X-Schema-Evolution-Control-Token", token)
                contentType(ContentType.Application.Json)
                setBody(body)
            }.bodyAsText()
            Json.parseToJsonElement(response).jsonObject
        } finally {
            client.close()
        }
    }

    private suspend fun scalarLong(connection: SafeSQLiteConnection, sql: String): Long =
        connection.withExclusiveAccess {
            connection.prepare(sql).use { statement -> check(statement.step()); statement.getLong(0) }
        }

    private suspend fun scalarText(connection: SafeSQLiteConnection, sql: String): String =
        connection.withExclusiveAccess {
            connection.prepare(sql).use { statement -> check(statement.step()); statement.getText(0) }
        }

    private companion object {
        const val DATABASE_ID = "schemaevolution-live"
        const val SERVICE_ID = "schemaevolution-authority"
        const val PERSON_ID = "11111111-1111-4111-8111-111111111111"
        const val NOTE_ID = "22222222-2222-4222-8222-222222222222"
        const val PROJECT_ID = "33333333-3333-4333-8333-333333333333"
        const val ATTACHMENT_ID = "44444444-4444-4444-8444-444444444444"
        const val OLD_WRITER_ID = "55555555-5555-4555-8555-555555555555"
        const val PENDING_ID = "66666666-6666-4666-8666-666666666666"
        const val PROJECT_B_ID = "77777777-7777-4777-8777-777777777777"
        const val PERSON_B_ID = "88888888-8888-4888-8888-888888888888"
        const val ATTACHMENT_B_ID = "99999999-9999-4999-8999-999999999999"
        const val NOTE_B_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        const val DRAFT_ID = "draft-1"
        val ACTION_REQUIRED_VARIANTS = listOf("dirty", "frozen", "conflicted", "rejected", "ambiguous")
    }
}

private class HeldDerivedCleanupPlugin : HttpClientPlugin<Unit, HeldDerivedCleanupPlugin>, Closeable {
    override val key = AttributeKey<HeldDerivedCleanupPlugin>("SchemaEvolutionHeldDerivedCleanupPlugin")
    val cleanupEntered = CompletableDeferred<Unit>()
    val release = CompletableDeferred<Unit>()
    val cleanupComplete = CompletableDeferred<Unit>()
    private var installations = 0
    var closeCalls = 0
        private set

    override fun prepare(block: Unit.() -> Unit) = this

    override fun install(plugin: HeldDerivedCleanupPlugin, scope: HttpClient) {
        installations++
        if (installations == 2) {
            scope.launch(start = CoroutineStart.UNDISPATCHED) {
                try {
                    awaitCancellation()
                } finally {
                    withContext(NonCancellable) {
                        cleanupEntered.complete(Unit)
                        release.await()
                        cleanupComplete.complete(Unit)
                    }
                }
            }
        }
    }

    override fun close() {
        closeCalls++
    }
}
