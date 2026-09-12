package dev.goquick.sqlitenow.oversqlite.mixedgeneration

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.DeferConflictResolver
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationone.MixedGenerationOneDatabase
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationone.VersionBasedDatabaseMigrations as GenerationOneMigrations
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationtwo.MixedGenerationTwoDatabase
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationtwo.VersionBasedDatabaseMigrations as GenerationTwoMigrations
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import java.nio.file.Files
import java.util.Collections
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * mixed-generation real-server driver. The qualification launcher supplies a guarded Go test binary and a
 * run-owned mixedgeneration server. It is intentionally inert in ordinary JVM test runs, where those owned
 * resources do not exist; the selected launcher rejects an inert invocation.
 */
class MixedGenerationJvmTest {
    @Test
    fun goG1KmpG2BidirectionalCrud() = runBlocking { runBidirectionalCrud(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2BidirectionalCrud() = runBlocking { runBidirectionalCrud(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2HiddenFieldPreservation() = runBlocking { runHiddenFieldPreservation(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2HiddenFieldPreservation() = runBlocking { runHiddenFieldPreservation(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2DefaultsAndNull() = runBlocking { runDefaultsAndNull(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2DefaultsAndNull() = runBlocking { runDefaultsAndNull(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2NullableRelationships() = runBlocking { runNullableRelationships(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2NullableRelationships() = runBlocking { runNullableRelationships(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2HiddenCheckpoint() = runBlocking { runHiddenCheckpoint(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2HiddenCheckpoint() = runBlocking { runHiddenCheckpoint(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2OfflineConflicts() = runBlocking { runOfflineConflicts(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2OfflineConflicts() = runBlocking { runOfflineConflicts(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2DisjointConflicts() = runBlocking { runDisjointConflicts(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2DisjointConflicts() = runBlocking { runDisjointConflicts(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2LostReplyNewerIntent() = runBlocking { runLostReplyNewerIntent(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2LostReplyNewerIntent() = runBlocking { runLostReplyNewerIntent(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2DeleteConflicts() = runBlocking { runDeleteConflicts(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2DeleteConflicts() = runBlocking { runDeleteConflicts(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2ProcessRecovery() = runBlocking { runProcessRecovery(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2ProcessRecovery() = runBlocking { runProcessRecovery(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2InterruptedRebuild() = runBlocking { runInterruptedRebuild(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2InterruptedRebuild() = runBlocking { runInterruptedRebuild(kmpGeneration = 1, goGeneration = 2) }

    @Test
    fun goG1KmpG2RetryRecovery() = runBlocking { runRetryRecovery(kmpGeneration = 2, goGeneration = 1) }

    @Test
    fun kmpG1GoG2RetryRecovery() = runBlocking { runRetryRecovery(kmpGeneration = 1, goGeneration = 2) }

    private suspend fun runBidirectionalCrud(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { fixture ->
            fixture.kmp.execSQL("INSERT INTO people(id,name) VALUES('$PERSON_ID','insert')")
            fixture.kmp.client.sync().getOrThrow()
            fixture.go("pull")
            assertEquals("insert", fixture.goValue("SELECT name FROM people WHERE id='$PERSON_ID'", "name"))

            fixture.go("exec", "UPDATE people SET name='updated' WHERE id='$PERSON_ID'")
            fixture.go("push")
            fixture.kmp.client.sync().getOrThrow()
            assertEquals("updated", fixture.kmp.scalar("SELECT name FROM people WHERE id='$PERSON_ID'"))

            fixture.kmp.execSQL("DELETE FROM people WHERE id='$PERSON_ID'")
            fixture.kmp.client.sync().getOrThrow()
            fixture.go("pull")
            assertEquals("0", fixture.goValue("SELECT count(*) AS count FROM people WHERE id='$PERSON_ID'", "count"))
        }
    }

    private suspend fun runHiddenFieldPreservation(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { fixture ->
            if (kmpGeneration == 2) {
                fixture.kmp.execSQL("INSERT INTO projects(id,title) VALUES('$PROJECT_ID','Q')")
                fixture.kmp.execSQL("INSERT INTO people(id,name,note,project_id) VALUES('$PERSON_ID','base','secret-2','$PROJECT_ID')")
                fixture.kmp.client.sync().getOrThrow()
                fixture.go("pull")
                fixture.go("exec", "UPDATE people SET name='from-go' WHERE id='$PERSON_ID'")
                fixture.go("push")
                fixture.kmp.client.sync().getOrThrow()
                assertEquals("from-go", fixture.kmp.scalar("SELECT name FROM people WHERE id='$PERSON_ID'"))
                assertEquals("secret-2", fixture.kmp.scalar("SELECT note FROM people WHERE id='$PERSON_ID'"))
            } else {
                fixture.go("exec", "INSERT INTO projects(id,title) VALUES('$PROJECT_ID','Q'); INSERT INTO people(id,name,note,project_id) VALUES('$PERSON_ID','base','secret-2','$PROJECT_ID')")
                fixture.go("push")
                fixture.kmp.client.sync().getOrThrow()
                fixture.kmp.execSQL("UPDATE people SET name='from-kmp' WHERE id='$PERSON_ID'")
                fixture.kmp.client.sync().getOrThrow()
                fixture.go("pull")
                assertEquals("from-kmp", fixture.goValue("SELECT name FROM people WHERE id='$PERSON_ID'", "name"))
                assertEquals("secret-2", fixture.goValue("SELECT note FROM people WHERE id='$PERSON_ID'", "note"))
            }
        }
    }

    private suspend fun runDefaultsAndNull(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            for (note in listOf<String?>(null, "secret-3")) {
                f.exec(2, "UPDATE people SET note=${note?.let { "'$it'" } ?: "NULL"} WHERE id='$PERSON_ID'")
                f.push(2); f.pull(1)
                f.exec(1, "UPDATE people SET name='from-g1' WHERE id='$PERSON_ID'")
                f.push(1); f.pull(2)
                f.assertPerson("from-g1", note, PROJECT_ID)
            }
            f.exec(1, "INSERT INTO people(id,name) VALUES('$OTHER_ID','old')")
            f.push(1); f.pull(2)
            f.assertPerson("old", "server-default", null, OTHER_ID)
            f.exec(2, "INSERT INTO people(id,name,note) VALUES('$NULL_ID','new',NULL)")
            f.push(2); f.settle()
            f.assertPerson("new", null, null, NULL_ID)
        }
    }

    private suspend fun runNullableRelationships(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            for (project in listOf(PROJECT_ID, null, PROJECT_ID)) {
                f.exec(2, "UPDATE people SET project_id=${project?.let { "'$it'" } ?: "NULL"} WHERE id='$PERSON_ID'")
                f.push(2); f.pull(1)
                f.exec(1, "UPDATE people SET name='from-g1' WHERE id='$PERSON_ID'")
                f.push(1); f.pull(2)
                f.assertPerson("from-g1", "secret-1", project)
            }
            for ((table, column) in listOf("projects" to "title", "notes" to "body")) {
                f.exec(2, "INSERT INTO $table(id,$column) VALUES('$OTHER_ID','created')")
                f.push(2); f.pull(1)
                f.exec(2, "UPDATE $table SET $column='updated' WHERE id='$OTHER_ID'")
                f.push(2); f.pull(1)
                assertEquals("updated", f.value(2, "SELECT $column FROM $table WHERE id='$OTHER_ID'"))
                assertEquals("updated", f.authorityValue("SELECT $column FROM mixed.$table WHERE _sync_scope_id='${f.userId}' AND id='$OTHER_ID'"))
                f.exec(2, "DELETE FROM $table WHERE id='$OTHER_ID'")
                f.push(2); f.pull(1)
                assertEquals("0", f.value(2, "SELECT count(*) FROM $table WHERE id='$OTHER_ID'"))
            }
            f.settle()
            assertEquals("0", f.value(2, "SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals("0", f.value(1, "SELECT count(*) FROM pragma_table_info('people') WHERE name IN ('note','project_id')"))
            assertEquals("0", f.value(1, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('projects','notes')"))
        }
    }

    private suspend fun runHiddenCheckpoint(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            val before = f.value(1, "SELECT last_bundle_seq_seen FROM _sync_attachment_state").toLong()
            f.exec(2, "INSERT INTO notes(id,body) VALUES('$OTHER_ID','hidden')")
            f.push(2); f.pull(1)
            val after = f.value(1, "SELECT last_bundle_seq_seen FROM _sync_attachment_state").toLong()
            assertTrue(after > before)
            f.reopen(1)
            assertEquals(after.toString(), f.value(1, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            val versionBefore = f.value(1, "SELECT row_version FROM _sync_row_state WHERE table_name='people' AND key_json LIKE '%$PERSON_ID%'").toLong()
            f.exec(2, "UPDATE people SET note='secret-2' WHERE id='$PERSON_ID'")
            f.push(2); f.settle()
            val versionAfter = f.value(1, "SELECT row_version FROM _sync_row_state WHERE table_name='people' AND key_json LIKE '%$PERSON_ID%'").toLong()
            assertTrue(versionAfter > versionBefore)
            f.assertPerson("base", "secret-2", PROJECT_ID)
        }
    }

    private suspend fun runOfflineConflicts(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        for (order in listOf("A_first", "B_first")) for (decision in listOf("accept", "local", "merged")) {
            fixture(environment, kmpGeneration, goGeneration).use { f ->
                f.seedBaseline()
                f.exec(1, "UPDATE people SET name='A-edit' WHERE id='$PERSON_ID'")
                f.exec(2, "UPDATE people SET name='B-edit',note='secret-2' WHERE id='$PERSON_ID'")
                val winner = if (order == "A_first") 1 else 2
                val loser = if (winner == 1) 2 else 1
                f.push(winner); f.requirePending(loser); f.resolve(loser, decision, "M"); f.push(loser); f.settle()
                val expectedName = when { decision == "merged" -> "M"; decision == "accept" && winner == 1 -> "A-edit"; decision == "accept" -> "B-edit"; loser == 1 -> "A-edit"; else -> "B-edit" }
                val expectedNote = if (decision == "accept" && winner == 1) "secret-1" else "secret-2"
                f.assertPerson(expectedName, expectedNote, PROJECT_ID)
            }
        }
    }

    private suspend fun runDisjointConflicts(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        for (order in listOf("A_first", "B_first")) fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            f.exec(1, "UPDATE people SET name='A-edit' WHERE id='$PERSON_ID'")
            f.exec(2, "UPDATE people SET note='secret-2' WHERE id='$PERSON_ID'")
            val winner = if (order == "A_first") 1 else 2
            val loser = if (winner == 1) 2 else 1
            f.push(winner); f.requirePending(loser); f.resolve(loser, "merged", "A-edit"); f.push(loser); f.settle()
            f.assertPerson("A-edit", "secret-2", PROJECT_ID)
        }
    }

    private suspend fun runLostReplyNewerIntent(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            f.exec(2, "UPDATE people SET note='secret-2' WHERE id='$PERSON_ID'")
            f.push(2); f.pull(1)
            val effectsBefore = f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong()
            f.kmp.execSQL("UPDATE people SET name='A1' WHERE id='$PERSON_ID'")
            f.closeKmp()

            val interrupted = f.startKmp("push", "after_commit_reply")
            interrupted.awaitBoundary("committed_before_reply")
            f.rawKmpExec("UPDATE people SET name='A2' WHERE id='$PERSON_ID'")
            interrupted.crash()

            f.reopen(kmpGeneration, closeExisting = false)
            f.kmp.client.pushPending().getOrThrow()
            f.kmp.client.pushPending().getOrThrow()
            f.settle()
            f.assertPerson("A2", "secret-2", PROJECT_ID)
            assertEquals(effectsBefore + 2L, f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong())
        }
    }

    private suspend fun runDeleteConflicts(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        for (updater in listOf(1, 2)) for (order in listOf("delete_first", "update_first")) for (decision in listOf("accept", "local", "merged")) {
            fixture(environment, kmpGeneration, goGeneration).use { f ->
                f.seedBaseline()
                val deleter = if (updater == 1) 2 else 1
                f.exec(updater, "UPDATE people SET name='updated' WHERE id='$PERSON_ID'")
                f.exec(deleter, "DELETE FROM people WHERE id='$PERSON_ID'")
                val winner = if (order == "delete_first") deleter else updater
                val loser = if (winner == updater) deleter else updater
                f.push(winner); f.requirePending(loser)
                val invalid = decision == "merged" || (order == "delete_first" && decision == "local")
                if (invalid) {
                    assertFalse(f.tryResolve(loser, decision, "M"))
                    f.requirePending(loser)
                    f.resolve(loser, "accept", "M")
                } else f.resolve(loser, decision, "M")
                f.push(loser); f.settle()
                val present = order == "update_first" && decision != "local"
                if (present) f.assertPerson("updated", "secret-1", PROJECT_ID)
                else f.assertAbsent(PERSON_ID)
            }
        }
    }

    private suspend fun runProcessRecovery(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        for ((fault, boundary, beforeRecoveryDelta) in listOf(
            Triple("after_push_reply", "applied_before_reply", 1L),
            Triple("before_push", "prepared_before_push", 0L),
        )) fixture(environment, kmpGeneration, goGeneration, ownServer = true).use { f ->
            f.seedBaseline()
            f.exec(2, "UPDATE people SET note='secret-2' WHERE id='$PERSON_ID'")
            f.push(2); f.pull(1)
            val effectsBefore = f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong()
            f.kmp.execSQL("UPDATE people SET name='interrupted' WHERE id='$PERSON_ID'")
            f.closeKmp()

            val interrupted = f.startKmp("push", fault)
            interrupted.awaitBoundary(boundary)
            assertEquals(effectsBefore + beforeRecoveryDelta, f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong())
            interrupted.crash()
            f.restartServer()
            f.startKmp("sync").awaitExit()
            f.reopen(kmpGeneration, closeExisting = false)
            f.settle()
            f.assertPerson("interrupted", "secret-2", PROJECT_ID)
            assertEquals(effectsBefore + 1L, f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong())

            f.go("exec", "UPDATE people SET name='peer-after-recovery' WHERE id='$PERSON_ID'")
            f.go("push")
            f.kmp.client.pullToStable().getOrThrow()
            f.assertPerson("peer-after-recovery", "secret-2", PROJECT_ID)
        }
        runPartialSuffixRecovery(environment, kmpGeneration, goGeneration)
    }

    private suspend fun runPartialSuffixRecovery(
        environment: Environment,
        kmpGeneration: Int,
        goGeneration: Int,
    ) {
        fixture(environment, kmpGeneration, goGeneration, ownServer = true).use { f ->
            f.seedBaseline()
            f.exec(2, "UPDATE people SET note='secret-2' WHERE id='$PERSON_ID'")
            f.push(2); f.pull(1)
            val effectsBefore = f.authorityValue("SELECT COALESCE(SUM(n),0) FROM mixed.effects").toLong()
            val personEffectsBefore = f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong()
            f.kmp.execSQL("UPDATE people SET name='interrupted' WHERE id='$PERSON_ID'")
            f.kmp.execSQL("INSERT INTO people(id,name) VALUES('$OTHER_ID','suffix')")
            f.closeKmp()

            val interrupted = f.startKmp("push", "after_first_push_reply", maxPushRows = 1)
            interrupted.awaitBoundary("first_push_applied")
            assertEquals(effectsBefore + 1L, f.authorityValue("SELECT COALESCE(SUM(n),0) FROM mixed.effects").toLong())
            interrupted.crash()
            f.restartServer()
            val recovered = f.startKmp("sync", maxPushRows = 1).awaitExit()
            assertEquals(1, recovered.lineSequence().count { it.startsWith("MIXED_GENERATION:push_response=") })
            f.reopen(kmpGeneration, closeExisting = false)
            f.settle()
            f.assertPerson("interrupted", "secret-2", PROJECT_ID)
            f.assertPerson("suffix", "server-default", null, OTHER_ID)
            assertEquals(effectsBefore + 2L, f.authorityValue("SELECT COALESCE(SUM(n),0) FROM mixed.effects").toLong())
            assertEquals(personEffectsBefore + 1L, f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong())

            f.go("exec", "UPDATE people SET name='peer-after-partial-recovery' WHERE id='$PERSON_ID'")
            f.go("push")
            f.kmp.client.pullToStable().getOrThrow()
            f.assertPerson("peer-after-partial-recovery", "secret-2", PROJECT_ID)
        }
    }

    private suspend fun runInterruptedRebuild(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        for ((fault, boundary) in listOf(
            "nonfinal_snapshot_stage" to "nonfinal_snapshot_stage",
            "final_snapshot_stage" to "final_snapshot_stage",
        )) fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            repeat(4) { index ->
                val id = "55555555-5555-4555-8555-${(index + 1).toString().padStart(12, '0')}"
                f.exec(2, "INSERT INTO people(id,name,note,project_id) VALUES('$id','row-$index','secret-$index','$PROJECT_ID')")
            }
            f.push(2); f.pull(1)
            f.kmp.execSQL("CREATE TABLE IF NOT EXISTS local_sentinel(value TEXT NOT NULL)")
            f.kmp.execSQL("INSERT INTO local_sentinel(value) VALUES('kept')")
            val checkpoint = f.kmp.scalar("SELECT last_bundle_seq_seen FROM _sync_attachment_state")
            f.closeKmp()

            val interrupted = f.startKmp("rebuild", fault)
            interrupted.awaitBoundary(boundary)
            interrupted.crash()
            f.reopen(kmpGeneration, closeExisting = false)
            assertEquals("5", f.kmp.scalar("SELECT count(*) FROM people"))
            assertEquals("kept", f.kmp.scalar("SELECT value FROM local_sentinel"))
            assertEquals(checkpoint, f.kmp.scalar("SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertTrue(f.kmp.scalar("SELECT count(*) FROM _sync_snapshot_stage").toLong() > 0L)

            f.kmp.client.rebuild().getOrThrow()
            f.settle()
            assertEquals("5", f.kmp.scalar("SELECT count(*) FROM people"))
            assertEquals("kept", f.kmp.scalar("SELECT value FROM local_sentinel"))
            assertEquals("0", f.kmp.scalar("SELECT count(*) FROM _sync_snapshot_stage"))
        }
    }

    private suspend fun runRetryRecovery(kmpGeneration: Int, goGeneration: Int) {
        val environment = environmentOrNull() ?: return
        fixture(environment, kmpGeneration, goGeneration).use { f ->
            f.seedBaseline()
            val effectsBefore = f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong()
            val checkpoint = f.kmp.scalar("SELECT last_bundle_seq_seen FROM _sync_attachment_state")
            f.go("exec", "INSERT INTO people(id,name) VALUES('$OTHER_ID','remote')")
            f.go("push")
            f.closeKmp()

            val interruptedPull = f.startKmp("pull", "interrupt_pull")
            assertTrue(interruptedPull.awaitExit().contains("TransientRetryExhaustedException"))
            f.reopen(kmpGeneration, closeExisting = false)
            assertEquals(checkpoint, f.kmp.scalar("SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals("base", f.kmp.scalar("SELECT name FROM people WHERE id='$PERSON_ID'"))

            f.kmp.execSQL("UPDATE people SET name='local-after-failure' WHERE id='$PERSON_ID'")
            f.closeKmp()
            val blockedUpload = f.startKmp("push", "block_upload")
            assertTrue(blockedUpload.awaitExit().contains("TransientRetryExhaustedException"))
            f.reopen(kmpGeneration, closeExisting = false)
            val retainedPending = f.kmp.scalar("SELECT count(*) FROM _sync_dirty_rows").toLong() +
                f.kmp.scalar("SELECT count(*) FROM _sync_outbox_rows").toLong()
            assertTrue(retainedPending > 0L)
            f.kmp.client.sync().getOrThrow()
            f.settle()
            f.assertPerson("local-after-failure", "secret-1", PROJECT_ID)
            assertEquals("remote", f.value(1, "SELECT name FROM people WHERE id='$OTHER_ID'"))
            assertEquals("remote", f.value(2, "SELECT name FROM people WHERE id='$OTHER_ID'"))
            assertEquals(effectsBefore + 1L, f.authorityValue("SELECT n FROM mixed.effects WHERE id='$PERSON_ID'").toLong())
        }
    }

    private suspend fun fixture(
        environment: Environment,
        kmpGeneration: Int,
        goGeneration: Int,
        ownServer: Boolean = false,
    ): Fixture {
        val directory = Files.createTempDirectory("sqlitenow-mixed-generation")
        val userId = "mixedgeneration-kmp-${System.nanoTime()}"
        val ownedServer = if (ownServer) startServer(environment) else null
        val baseUrl = ownedServer?.baseUrl ?: environment.baseUrl
        val token = issueToken(baseUrl, userId)
        val http = mixedgenerationAuthorityHttp(baseUrl, token)
        val kmp = openKmp(kmpGeneration, directory.resolve("kmp.sqlite3").toString(), http)
        check(kmp.client.attach(userId).getOrThrow() is AttachResult.Connected)
        val fixture = Fixture(environment, directory, userId, token, kmp, goGeneration, baseUrl, ownedServer)
        fixture.go("init")
        return fixture
    }

    private suspend fun openKmp(generation: Int, path: String, http: HttpClient): KmpPeer = when (generation) {
        1 -> {
            val database = MixedGenerationOneDatabase(path, GenerationOneMigrations())
            val opening = database.openForSync(
                database.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID),
                http,
                DeferConflictResolver,
            )
            check(opening.advance().outcome == OversqliteOpeningOutcome.READY)
            KmpPeer(database, opening.readyOrNull()!!.client) { opening.close() }
        }
        2 -> {
            val database = MixedGenerationTwoDatabase(path, GenerationTwoMigrations())
            val opening = database.openForSync(
                database.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID),
                http,
                DeferConflictResolver,
            )
            check(opening.advance().outcome == OversqliteOpeningOutcome.READY)
            KmpPeer(database, opening.readyOrNull()!!.client) { opening.close() }
        }
        else -> error("unsupported generation $generation")
    }

    private suspend fun issueToken(baseUrl: String, userId: String): String {
        val http = HttpClient(CIO)
        return try {
            val body = http.post("$baseUrl/dummy-signin") {
                contentType(ContentType.Application.Json)
                setBody("""{"user":"$userId","password":"anything","device":"mixed-generation"}""")
            }.bodyAsText()
            Json.parseToJsonElement(body).jsonObject.getValue("token").jsonPrimitive.content
        } finally {
            http.close()
        }
    }

    private fun environmentOrNull(): Environment? {
        val baseUrl = System.getenv("MIXED_GENERATION_BASE_URL")?.trim().orEmpty()
        val peerBinary = System.getenv("MIXED_GENERATION_GO_PEER_BINARY")?.trim().orEmpty()
        val postgresDsn = System.getenv("MIXED_GENERATION_DSN")?.trim().orEmpty()
        val kmpClasspath = System.getenv("MIXED_GENERATION_KMP_CLASSPATH")?.trim().orEmpty()
        if (baseUrl.isEmpty() || peerBinary.isEmpty() || postgresDsn.isEmpty() || kmpClasspath.isEmpty()) {
            check(System.getenv("MIXED_GENERATION_REQUIRED") != "1") {
                "required mixed-generation environment is incomplete"
            }
            println("MixedGeneration-REQUIRED: inert; launcher must supply the guarded Go and KMP process environment")
            return null
        }
        return Environment(baseUrl, peerBinary, postgresDsn, kmpClasspath)
    }

    private data class Environment(
        val baseUrl: String,
        val goPeerBinary: String,
        val postgresDsn: String,
        val kmpClasspath: String,
    )

    private data class OwnedServer(val process: Process, val baseUrl: String)

    private fun startServer(environment: Environment): OwnedServer {
        val process = ProcessBuilder(environment.goPeerBinary, "-test.run=^TestMixedSchemaProcessHelper$", "-test.v")
            .redirectErrorStream(true)
            .apply {
                environment().putAll(
                    mapOf(
                        "MIXED_GENERATION_HELPER" to "current-test-subprocess-v1",
                        "MIXED_GENERATION_ROLE" to "server",
                        "MIXED_GENERATION_DSN" to environment.postgresDsn,
                    ),
                )
            }
            .start()
        val reader = process.inputStream.bufferedReader()
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90)
        while (System.nanoTime() < deadline) {
            val line = reader.readLine() ?: error("owned Go server exited before publishing its URL")
            println("MixedGeneration-OWNED-SERVER: $line")
            if (line.startsWith("MIXED_GENERATION:http")) {
                val server = OwnedServer(process, line.removePrefix("MIXED_GENERATION:"))
                Thread {
                    reader.forEachLine { remaining -> println("MixedGeneration-OWNED-SERVER: $remaining") }
                }.apply { isDaemon = true }.start()
                return server
            }
        }
        process.destroyForcibly()
        error("timed out waiting for owned Go server")
    }

    private class KmpPeer(
        private val database: SqliteNowDatabase,
        val client: OversqliteClient,
        private val closer: suspend () -> Unit,
    ) {
        suspend fun execSQL(sql: String) = database.connection().execSQL(sql)

        suspend fun scalar(sql: String): String = database.connection().withExclusiveAccess {
            database.connection().prepare(sql).use { statement ->
                check(statement.step()) { "query returned no row: $sql" }
                statement.getText(0)
            }
        }

        suspend fun nullableScalar(sql: String): String? = database.connection().withExclusiveAccess {
            database.connection().prepare(sql).use { statement ->
                check(statement.step()) { "query returned no row: $sql" }
                if (statement.isNull(0)) null else statement.getText(0)
            }
        }

        suspend fun close() = closer()
    }

    private inner class Fixture(
        private val environment: Environment,
        private val directory: java.nio.file.Path,
        val userId: String,
        private val token: String,
        var kmp: KmpPeer,
        private val goGeneration: Int,
        private var baseUrl: String,
        private var ownedServer: OwnedServer?,
    ) {
        private var resolutionDiagnostic: String = ""
        private val kmpProcesses = mutableListOf<KmpProcess>()
        private fun goProcess(action: String, sql: String? = null, mergedPayload: String? = null): Pair<Int, String> {
            val process = ProcessBuilder(environment.goPeerBinary, "-test.run=^TestMixedSchemaProcessHelper$", "-test.v")
                .redirectErrorStream(true)
                .apply {
                    environment().putAll(
                        mapOf(
                            "MIXED_GENERATION_HELPER" to "current-test-subprocess-v1",
                            "MIXED_GENERATION_ROLE" to "peer",
                            "MIXED_GENERATION_ACTION" to action,
                            "MIXED_GENERATION_FILE" to directory.resolve("go.sqlite3").toString(),
                            "MIXED_GENERATION_URL" to baseUrl,
                            "MIXED_GENERATION_TOKEN" to token,
                            "MIXED_GENERATION_GENERATION" to goGeneration.toString(),
                        ) + (if (sql == null) emptyMap() else mapOf("MIXED_GENERATION_SQL" to sql)) +
                            (if (mergedPayload == null) emptyMap() else mapOf("MIXED_GENERATION_MERGED_PAYLOAD" to mergedPayload)),
                    )
                }
                .start()
            val output = process.inputStream.bufferedReader().readText()
            check(process.waitFor(120, TimeUnit.SECONDS)) { "Go peer action $action timed out" }
            return process.exitValue() to output
        }

        fun go(action: String, sql: String? = null, mergedPayload: String? = null): String {
            val (exit, output) = goProcess(action, sql, mergedPayload)
            check(exit == 0) { "Go peer action $action failed: $output" }
            return output
        }

        fun startKmp(
            action: String,
            fault: String = "",
            sql: String? = null,
            maxPushRows: Int? = null,
        ): KmpProcess {
            val java = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString()
            val process = ProcessBuilder(
                java,
                "-cp",
                environment.kmpClasspath,
                "dev.goquick.sqlitenow.oversqlite.mixedgeneration.MixedGenerationProcessKt",
            ).redirectErrorStream(true).apply {
                environment().putAll(
                    mapOf(
                        "MIXED_GENERATION_KMP_HELPER" to "current-test-subprocess-v1",
                        "MIXED_GENERATION_KMP_ACTION" to action,
                        "MIXED_GENERATION_KMP_FAULT" to fault,
                        "MIXED_GENERATION_KMP_FILE" to directory.resolve("kmp.sqlite3").toString(),
                        "MIXED_GENERATION_KMP_URL" to baseUrl,
                        "MIXED_GENERATION_KMP_TOKEN" to token,
                        "MIXED_GENERATION_KMP_USER" to userId,
                        "MIXED_GENERATION_KMP_GENERATION" to kmpGeneration().toString(),
                    ) + (sql?.let { mapOf("MIXED_GENERATION_KMP_SQL" to it) } ?: emptyMap()) +
                        (maxPushRows?.let { mapOf("MIXED_GENERATION_KMP_UPLOAD_LIMIT" to it.toString()) } ?: emptyMap()),
                )
            }.start()
            return KmpProcess(process).also(kmpProcesses::add)
        }

        fun rawKmpExec(sql: String) {
            val process = ProcessBuilder("sqlite3", directory.resolve("kmp.sqlite3").toString(), sql)
                .redirectErrorStream(true)
                .start()
            val output = process.inputStream.bufferedReader().readText()
            check(process.waitFor(30, TimeUnit.SECONDS)) { "raw KMP SQLite edit timed out" }
            check(process.exitValue() == 0) { "raw KMP SQLite edit failed: $output" }
        }

        suspend fun closeKmp() {
            kmp.close()
            kmp.client.close()
        }

        fun restartServer() {
            val previous = checkNotNull(ownedServer) { "fixture does not own its server" }
            previous.process.destroy()
            if (!previous.process.waitFor(30, TimeUnit.SECONDS)) {
                previous.process.destroyForcibly()
                check(previous.process.waitFor(30, TimeUnit.SECONDS)) { "owned Go server did not terminate" }
            }
            ownedServer = startServer(environment)
            baseUrl = checkNotNull(ownedServer).baseUrl
        }

        fun goValue(sql: String, field: String): String {
            return checkNotNull(goNullableValue(sql, field)) { "Go peer query field $field was null" }
        }

        fun goNullableValue(sql: String, field: String): String? {
            val output = go("query", sql)
            val line = output.lineSequence().firstOrNull { it.startsWith("MIXED_GENERATION:result=") }
                ?: error("Go peer query produced no result: $output")
            val rows = Json.parseToJsonElement(line.removePrefix("MIXED_GENERATION:result=")).jsonArray
            check(rows.size == 1) { "expected one Go peer query row, got $rows" }
            val value = rows.single().jsonObject.getValue(field)
            return if (value === JsonNull) null else value.jsonPrimitive.content
        }

        suspend fun exec(generation: Int, sql: String) {
            if (generation == kmpGeneration()) kmp.execSQL(sql) else go("exec", sql)
        }

        suspend fun push(generation: Int) {
            if (generation == kmpGeneration()) kmp.client.pushPending().getOrThrow() else go("push")
        }

        suspend fun pull(generation: Int) {
            if (generation == kmpGeneration()) kmp.client.pullToStable().getOrThrow() else go("pull")
        }

        suspend fun value(generation: Int, sql: String): String =
            if (generation == kmpGeneration()) kmp.scalar(sql) else goValue(sql, sqlColumn(sql))

        suspend fun nullableValue(generation: Int, sql: String): String? =
            if (generation == kmpGeneration()) kmp.nullableScalar(sql) else goNullableValue(sql, sqlColumn(sql))

        private fun sqlColumn(sql: String): String {
            val selected = sql.substringAfter("SELECT", "").substringBefore("FROM").trim()
            return selected.substringAfterLast(" AS ", selected.substringAfterLast(" as ", selected))
                .substringAfterLast('.').trim().removeSurrounding("\"")
        }

        private fun kmpGeneration(): Int = if (goGeneration == 1) 2 else 1

        suspend fun seedBaseline() {
            exec(2, "INSERT INTO projects(id,title) VALUES('$PROJECT_ID','Q')")
            exec(2, "INSERT INTO people(id,name,note,project_id) VALUES('$PERSON_ID','base','secret-1','$PROJECT_ID')")
            push(2); pull(1)
            assertPerson("base", "secret-1", PROJECT_ID)
        }

        suspend fun requirePending(generation: Int) {
            if (generation == kmpGeneration()) {
                assertTrue(kmp.client.pushPending().isFailure)
                val pending = kmp.client.pendingConflict().getOrThrow()
                checkNotNull(pending)
                assertTrue(pending.canonicalRequestHash.isNotBlank())
            } else {
                val output = go("pending")
                assertTrue(output.lineSequence().any { it.startsWith("MIXED_GENERATION:pending=") })
            }
        }

        private fun mergedPayload(generation: Int, name: String): String = buildJsonObject {
            put("id", PERSON_ID)
            put("name", name)
            if (generation == 2) {
                put("note", "secret-2")
                put("project_id", PROJECT_ID)
            }
        }.toString()

        suspend fun tryResolve(generation: Int, decision: String, name: String): Boolean {
            if (generation == kmpGeneration()) {
                val pending = checkNotNull(kmp.client.pendingConflict().getOrThrow())
                val result = when (decision) {
                    "accept" -> MergeResult.AcceptServer
                    "local" -> MergeResult.KeepLocal
                    "merged" -> MergeResult.KeepMerged(Json.parseToJsonElement(mergedPayload(generation, name)))
                    else -> error("unknown decision $decision")
                }
                val resolved = kmp.client.resolveConflict(pending, result)
                resolutionDiagnostic = resolved.exceptionOrNull()?.stackTraceToString().orEmpty()
                return resolved.isSuccess
            }
            val action = when (decision) {
                "accept" -> "resolve_accept"
                "local" -> "resolve_local"
                "merged" -> "resolve_merged"
                else -> error("unknown decision $decision")
            }
            val (exit, output) = goProcess(action, mergedPayload = if (decision == "merged") mergedPayload(generation, name) else null)
            resolutionDiagnostic = output
            return exit == 0
        }

        suspend fun resolve(generation: Int, decision: String, name: String) {
            check(tryResolve(generation, decision, name)) {
                "resolution $decision failed for generation $generation: $resolutionDiagnostic"
            }
        }

        suspend fun reopen(generation: Int, closeExisting: Boolean = true) {
            if (generation != kmpGeneration()) return
            if (closeExisting) closeKmp()
            kmp = openKmp(generation, directory.resolve("kmp.sqlite3").toString(), mixedgenerationAuthorityHttp(baseUrl, token))
            check(kmp.client.attach(userId).getOrThrow() is AttachResult.Connected)
        }

        suspend fun settle() {
            pull(1); pull(2)
            for (generation in listOf(1, 2)) {
                assertEquals("0", value(generation, "SELECT count(*) AS count FROM _sync_dirty_rows"))
                assertEquals("0", value(generation, "SELECT count(*) AS count FROM _sync_outbox_rows"))
                if (generation == kmpGeneration()) kmp.client.sync().getOrThrow() else go("sync")
            }
        }

        suspend fun assertPerson(name: String, note: String?, project: String?, id: String = PERSON_ID) {
            assertEquals("1", value(1, "SELECT count(*) AS count FROM people WHERE id='$id'"))
            assertEquals(name, value(1, "SELECT name FROM people WHERE id='$id'"))
            assertEquals(name, value(2, "SELECT name FROM people WHERE id='$id'"))
            assertEquals(note, nullableValue(2, "SELECT note FROM people WHERE id='$id'"))
            assertEquals(project, nullableValue(2, "SELECT project_id FROM people WHERE id='$id'"))
            assertEquals(name, authorityValue("SELECT name FROM mixed.people WHERE _sync_scope_id='$userId' AND id='$id'"))
            assertEquals(note, authorityNullableValue("SELECT note FROM mixed.people WHERE _sync_scope_id='$userId' AND id='$id'"))
            assertEquals(project, authorityNullableValue("SELECT project_id::text FROM mixed.people WHERE _sync_scope_id='$userId' AND id='$id'"))
        }

        suspend fun assertAbsent(id: String) {
            assertEquals("0", value(1, "SELECT count(*) AS count FROM people WHERE id='$id'"))
            assertEquals("0", value(2, "SELECT count(*) AS count FROM people WHERE id='$id'"))
            assertEquals("0", authorityValue("SELECT count(*) FROM mixed.people WHERE _sync_scope_id='$userId' AND id='$id'"))
        }

        fun authorityValue(sql: String): String = checkNotNull(authorityNullableValue(sql)) { "authority query returned NULL: $sql" }

        fun authorityNullableValue(sql: String): String? {
            val process = ProcessBuilder("psql", "--no-psqlrc", "--tuples-only", "--no-align", "--dbname", environment.postgresDsn, "-c", sql)
                .redirectErrorStream(true).start()
            val output = process.inputStream.bufferedReader().readText()
            check(process.waitFor(30, TimeUnit.SECONDS)) { "authority query timed out: $sql" }
            check(process.exitValue() == 0) { "authority query failed: $output" }
            val value = output.trim()
            return value.ifEmpty { null }
        }

        suspend fun use(block: suspend (Fixture) -> Unit) {
            try {
                block(this)
            } finally {
                kmpProcesses.forEach { runCatching { it.terminateIfAlive() } }
                runCatching { kmp.close() }
                runCatching { kmp.client.close() }
                ownedServer?.process?.let { server ->
                    if (server.isAlive) {
                        server.destroy()
                        if (!server.waitFor(30, TimeUnit.SECONDS)) server.destroyForcibly()
                    }
                }
                directory.toFile().deleteRecursively()
            }
        }
    }

    private class KmpProcess(private val process: Process) {
        private val lines = Collections.synchronizedList(mutableListOf<String>())
        private val queue = LinkedBlockingQueue<String>()

        init {
            Thread {
                process.inputStream.bufferedReader().forEachLine { line ->
                    lines += line
                    queue.put(line)
                    println("MixedGeneration-KMP-PROCESS: $line")
                }
            }.apply { isDaemon = true }.start()
        }

        fun awaitBoundary(boundary: String) {
            val expected = "MIXED_GENERATION:boundary=$boundary"
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120)
            while (System.nanoTime() < deadline) {
                val remaining = deadline - System.nanoTime()
                val line = queue.poll(remaining, TimeUnit.NANOSECONDS)
                    ?: error("timed out waiting for $expected; output=${lines.joinToString("\n")}")
                if (line.startsWith(expected)) return
                check(process.isAlive) { "KMP process exited before $expected; output=${lines.joinToString("\n")}" }
            }
            error("timed out waiting for $expected; output=${lines.joinToString("\n")}")
        }

        fun awaitExit(): String {
            check(process.waitFor(120, TimeUnit.SECONDS)) { "KMP process timed out; output=${lines.joinToString("\n")}" }
            check(process.exitValue() == 0) { "KMP process failed: ${lines.joinToString("\n")}" }
            return lines.joinToString("\n")
        }

        fun crash() {
            process.destroyForcibly()
            check(process.waitFor(30, TimeUnit.SECONDS)) { "KMP process did not terminate" }
        }

        fun terminateIfAlive() {
            if (!process.isAlive) return
            process.destroy()
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly()
                check(process.waitFor(30, TimeUnit.SECONDS)) { "KMP process did not terminate during fixture cleanup" }
            }
        }
    }

    private companion object {
        const val DATABASE_ID = "coexistence-client"
        const val SERVICE_ID = "coexistence-service"
        const val PERSON_ID = "11111111-1111-4111-8111-111111111111"
        const val PROJECT_ID = "22222222-2222-4222-8222-222222222222"
        const val OTHER_ID = "33333333-3333-4333-8333-333333333333"
        const val NULL_ID = "44444444-4444-4444-8444-444444444444"
    }
}
