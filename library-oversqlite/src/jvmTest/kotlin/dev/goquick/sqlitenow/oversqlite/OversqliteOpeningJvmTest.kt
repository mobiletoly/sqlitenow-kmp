package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_CREATE
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READWRITE
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqliteNowSyncOpenRequiredException
import io.ktor.client.HttpClient
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.nio.file.Files
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

class OversqliteOpeningJvmTest {
    private data class PreU0Census(
        val databaseBytes: List<Byte>,
        val schema: List<String>,
        val sources: List<String>,
        val journal: List<String>,
    )

    private class PreviewHold {
        val entered = CompletableDeferred<Unit>()
        val cancellationObserved = CompletableDeferred<Unit>()
        val releaseCleanup = CompletableDeferred<Unit>()
        val cleanupComplete = CompletableDeferred<Unit>()
        val cleanupCalls = AtomicInteger()
    }

    private enum class OpeningLostResponsePoint {
        INITIAL_PREVIEW,
        TARGET_PREVIEW,
        REPLACEMENT,
        FINAL_RETURN,
    }

    private enum class PreservationBarrierScenario(
        val cut: String,
        val rejection: Boolean,
        val requiresPendingState: Boolean,
    ) {
        RECORD_REJECTION("record_rejection", rejection = true, requiresPendingState = false),
        RETRY_REJECTION("retry_rejection", rejection = true, requiresPendingState = true),
        DISCARD_REJECTION("discard_rejection", rejection = true, requiresPendingState = true),
        RECORD_CONFLICT("record_conflict", rejection = false, requiresPendingState = false),
        CLEAR_CONFLICT("clear_conflict", rejection = false, requiresPendingState = true),
    }

    @Test
    fun openingSchemaMatchesSharedKmpFixture() {
        val fixture = oversqliteContractFixture("opening-state/schema/v1.sql")
        assertTrue(Files.isRegularFile(fixture), "missing shared opening schema fixture")
        assertEquals(Files.readString(fixture).trim(), createOversqliteOpeningTableSql.trim())
    }

    @Test
    fun businessOnlyExistingFileIsNotFreshAndRemainsUntouchedOffline() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-business-only")
            .resolve("database.sqlite3")
            .toString()
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE).use { connection ->
            connection.prepare("CREATE TABLE business_only(id TEXT PRIMARY KEY)").use { it.step() }
        }
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)

        val failure = assertFailsWith<IllegalArgumentException> {
            database.openForSync(config(), http)
        }
        assertTrue(failure.message.orEmpty().contains("cannot be opened as fresh"))
        assertFalse(database.isOpen())
        assertEquals(1L, externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='business_only'"))
        assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='notes'"))
        assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='_sync_opening_state'"))
        http.close()
    }

    @Test
    fun reservedMarkerCollisionsRejectBeforeMigrationAndInitialization() = runTest {
        val integerColumns = setOf(
            "singleton_key", "format_version", "revision", "from_generation_id",
            "to_generation_id", "from_local_version", "to_local_version",
            "migration_complete", "contract_confirmed", "pending_dirty_count",
            "pending_frozen_count", "pending_conflict_count", "drain_complete",
            "snapshot_bundle_seq", "snapshot_row_count", "snapshot_byte_count",
            "staged_row_count", "staged_byte_count", "target_reads_available",
            "writes_available", "pending_intent_revision",
        )
        fun invalidOpeningInsert(singletonKey: Int): String {
            val values = oversqliteOpeningColumnNames.map { column ->
                when (column) {
                    "singleton_key" -> singletonKey.toString()
                    "format_version", "to_generation_id" -> "3"
                    "mode" -> "'fresh'"
                    "replacement_state" -> "'none'"
                    in integerColumns -> "0"
                    else -> "''"
                }
            }
            return "INSERT INTO _sync_opening_state(${oversqliteOpeningColumnNames.joinToString(",")}) " +
                "VALUES(${values.joinToString(",")})"
        }
        data class Scenario(
            val name: String,
            val objectName: String,
            val objectType: String,
            val markerSql: String,
            val afterCreateSql: List<String> = emptyList(),
        )
        val scenarios = listOf(
            Scenario(
                name = "format one opening journal",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = createOversqliteOpeningTableSql.replace(
                    "CHECK(format_version=3)",
                    "CHECK(format_version=1)",
                ),
            ),
            Scenario(
                name = "prior format two opening journal",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = createOversqliteOpeningTableSql
                    .replace("CHECK(format_version=3)", "CHECK(format_version=2)")
                    .replace(
                        "    target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),",
                        "    old_" + "reads_available INTEGER NOT NULL CHECK(old_" +
                            "reads_available IN (0,1)),\n" +
                            "    target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),",
                    ),
            ),
            Scenario(
                name = "exact empty reserved table",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = createOversqliteOpeningTableSql,
            ),
            Scenario(
                name = "case variant exact empty reserved table",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = createOversqliteOpeningTableSql.replace(
                    "_sync_opening_state",
                    "_SYNC_OPENING_STATE",
                ),
            ),
            Scenario(
                name = "malformed singleton row",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = createOversqliteOpeningTableSql,
                afterCreateSql = listOf(invalidOpeningInsert(1)),
            ),
            Scenario(
                name = "multiple singleton rows",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = createOversqliteOpeningTableSql,
                afterCreateSql = listOf(
                    "PRAGMA ignore_check_constraints=ON",
                    invalidOpeningInsert(1),
                    invalidOpeningInsert(2),
                    "PRAGMA ignore_check_constraints=OFF",
                ),
            ),
            Scenario(
                name = "reserved table",
                objectName = "_sync_opening_state",
                objectType = "table",
                markerSql = "CREATE TABLE _sync_opening_state(marker INTEGER)",
            ),
            Scenario(
                name = "reserved view",
                objectName = "_sync_contract_identity",
                objectType = "view",
                markerSql = "CREATE VIEW _sync_contract_identity AS SELECT 1 AS singleton_key",
            ),
            Scenario(
                name = "reserved index",
                objectName = "_sync_admitted_contracts",
                objectType = "index",
                markerSql = "CREATE INDEX _sync_admitted_contracts ON business_only(id)",
            ),
        )

        scenarios.forEach { scenario ->
            val path = createTempDirectory("sqlitenow-coordinatedopening-marker-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3")
                .toString()
            BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE).use { connection ->
                connection.prepare("CREATE TABLE business_only(id INTEGER PRIMARY KEY)").use { it.step() }
                connection.prepare(scenario.markerSql).use { it.step() }
                scenario.afterCreateSql.forEach { sql ->
                    connection.prepare(sql).use { it.step() }
                }
            }
            val before = externalSchemaRows(path)
            val beforeBytes = Files.readAllBytes(java.nio.file.Path.of(path))
            val migrationCalls = AtomicInteger()
            val requestCount = AtomicInteger()
            val http = noNetworkHttpClient(requestCount)
            val database = database(path, version = 2, migrationCalls = migrationCalls)

            val failure = assertFailsWith<IllegalArgumentException>(scenario.name) {
                database.openForSync(config(), http)
            }
            assertTrue(
                failure.message.orEmpty().contains("recreate") ||
                    failure.message.orEmpty().contains("incompatible") ||
                    failure.message.orEmpty().contains("malformed") ||
                    failure.message.orEmpty().contains("orphan") ||
                    failure.message.orEmpty().contains("journal") ||
                    failure.message.orEmpty().contains("preserve"),
                scenario.name,
            )
            if (scenario.name == "prior format two opening journal") {
                assertTrue(failure.message.orEmpty().contains("fresh database"), scenario.name)
            }
            assertFalse(database.isOpen(), scenario.name)
            assertEquals(0, migrationCalls.get(), scenario.name)
            assertEquals(0, requestCount.get(), scenario.name)
            assertEquals(before, externalSchemaRows(path), scenario.name)
            assertTrue(
                beforeBytes.contentEquals(Files.readAllBytes(java.nio.file.Path.of(path))),
                "${scenario.name}: inspection changed database bytes",
            )
            assertEquals(
                1L,
                externalScalarLong(
                    path,
                    "SELECT count(*) FROM sqlite_schema WHERE lower(type)='${scenario.objectType}' AND lower(name)='${scenario.objectName}'",
                ),
                scenario.name,
            )
            assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='notes'"), scenario.name)
            assertEquals(
                0L,
                externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name LIKE '_sqlitenow_preserve_%'"),
                scenario.name,
            )
            http.close()
        }
    }

    @Test
    fun freshThenUnchangedRemainOfflineAndPublishOnlyAfterReadyCommit() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-opening").resolve("database.sqlite3").toString()
        val migrationCalls = AtomicInteger()
        val http = noNetworkHttpClient()
        val freshDatabase = database(path, version = 2, migrationCalls = migrationCalls)
        val fresh = freshDatabase.openForSync(config(), http)

        try {
            assertFalse(File(path).exists(), "openForSync inspection must not create missing storage")
            assertNull(fresh.status().mode)
            assertEquals("provider_preparation_required", fresh.status().diagnosticCode)
            assertEquals(OversqliteOpeningNextAction.ADVANCE, fresh.status().nextAction)
            assertFalse(freshDatabase.isOpen())
            assertNull(fresh.readyOrNull())
            assertFailsWith<IllegalStateException> { freshDatabase.connection() }
            val ready = fresh.advance()
            assertTrue(File(path).isFile, "first advance performs writable provider preparation")
            assertEquals(OversqliteOpeningOutcome.READY, ready.outcome)
            assertEquals("ready", ready.phase)
            assertTrue(freshDatabase.isOpen())
            assertNotNull(fresh.readyOrNull())
            assertEquals(2L, freshDatabase.connection().scalarLong("PRAGMA user_version"))
            assertEquals(1L, freshDatabase.connection().scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='_sync_opening_state'",
            ))
            assertEquals(1L, freshDatabase.connection().scalarLong(
                "SELECT count(*) FROM _sync_opening_state WHERE outcome='ready' AND integrity_hash!=''",
            ))
            freshDatabase.connection().execSQL("INSERT INTO notes(id,title) VALUES('n1','kept')")
        } finally {
            fresh.close()
        }

        assertFalse(freshDatabase.isOpen())
        assertEquals(1, migrationCalls.get())

        val unchangedDatabase = database(path, version = 2, migrationCalls = migrationCalls)
        val unchanged = unchangedDatabase.openForSync(config(), http)
        try {
            assertEquals(OversqliteOpeningMode.UNCHANGED, unchanged.status().mode)
            assertEquals(OversqliteOpeningOutcome.READY, unchanged.advance().outcome)
            assertNotNull(unchanged.readyOrNull())
            assertEquals(1L, unchangedDatabase.connection().scalarLong("SELECT count(*) FROM notes WHERE id='n1'"))
        } finally {
            unchanged.close()
            http.close()
        }
        assertEquals(1, migrationCalls.get(), "unchanged must not dispatch the migration runner")
    }

    @Test
    fun coordinatedOpeningFreezesCallerOwnedConfiguration() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-frozen-config").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val keyColumns = mutableListOf("id")
        val tables = mutableListOf(SyncTable("notes", syncKeyColumns = keyColumns))
        val callerConfig = OversqliteConfig(
            generationId = 1,
            databaseId = "opening-fixture",
            serviceId = "opening-service",
            syncTables = tables,
        )
        val database = database(path, version = 2)
        val opening = database.openForSync(callerConfig, http)

        tables.clear()
        keyColumns[0] = "changed_after_reservation"

        try {
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
            assertNotNull(opening.readyOrNull())
        } finally {
            opening.close()
            http.close()
        }
    }

    @Test
    fun readyClientCloseRevokesEveryHandleAndFacadeCloseJoinsOwnerCleanup() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-client-close").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        opening.advance()
        val client = assertNotNull(opening.readyOrNull()).client

        client.close()
        assertNull(opening.readyOrNull(), "synchronous client close must revoke the ready facade immediately")
        opening.close()

        assertFalse(database.isOpen())
        assertTrue(client.syncStatus().exceptionOrNull()?.message.orEmpty().contains("closed"))
        opening.close()
        http.close()
    }

    @Test
    fun readyDatabaseCloseJoinsTheSameFacadeShutdown() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-database-close").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        opening.advance()

        database.close()

        assertNull(opening.readyOrNull())
        assertFalse(database.isOpen())
        opening.close()
        http.close()
    }

    @Test
    fun closeCancelsTheAtomicallyRegisteredOperationBeforeItCanStartWork() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-operation-registration")
            .resolve("database.sqlite3")
            .toString()
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        val registered = CompletableDeferred<Unit>()
        openingOperationRegisteredForTest = { label ->
            if (label == "advance") {
                registered.complete(Unit)
                awaitCancellation()
            }
        }
        try {
            val advancing = async { runCatching { opening.advance() }.exceptionOrNull() }
            registered.await()

            opening.close()

            assertTrue(advancing.await() is CancellationException)
            assertNull(opening.readyOrNull())
            assertFalse(database.isOpen())
        } finally {
            openingOperationRegisteredForTest = null
            runCatching { opening.close() }
            http.close()
        }
    }

    @Test
    fun closeWinningAfterCorePublicationCannotBeReopenedByCleanupRetry() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-post-publication")
            .resolve("database.sqlite3")
            .toString()
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        val published = CompletableDeferred<Unit>()
        val releasePublication = CompletableDeferred<Unit>()
        val closeCalls = AtomicInteger()
        openingAfterCorePublicationForTest = {
            published.complete(Unit)
            withContext(NonCancellable) { releasePublication.await() }
        }
        try {
            val advancing = async { runCatching { opening.advance() }.exceptionOrNull() }
            published.await()
            openingBeforeSessionCloseForTest = {
                if (closeCalls.incrementAndGet() == 1) {
                    throw IllegalStateException("COORDINATED_OPENING_POST_PUBLICATION_CLOSE_FAILURE")
                }
            }
            val closing = async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { opening.close() }.exceptionOrNull()
            }
            assertNull(opening.readyOrNull())
            releasePublication.complete(Unit)

            assertNotNull(advancing.await())
            val firstClose = assertNotNull(closing.await())
            assertEquals("COORDINATED_OPENING_POST_PUBLICATION_CLOSE_FAILURE", firstClose.message)
            assertNull(opening.readyOrNull())

            opening.close()
            assertEquals(2, closeCalls.get())
            assertNull(opening.readyOrNull())
            assertFalse(database.isOpen())
        } finally {
            openingAfterCorePublicationForTest = null
            openingBeforeSessionCloseForTest = null
            releasePublication.complete(Unit)
            runCatching { opening.close() }
            http.close()
        }
    }

    @Test
    fun cancelDuringActiveGenerationPreviewStopsPublicationAndAllowsRetry() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-active-preview-cancel")
            .resolve("database.sqlite3")
            .toString()
        val seedHttp = generationHttp(generation(1), includeConnect = true)
        val seedOpening = database(path, version = 2).openForSync(config(), seedHttp)
        seedOpening.advance()
        seedOpening.readyOrNull()!!.client.attach("user-a").getOrThrow()
        seedOpening.close()
        seedHttp.close()

        val previewHold = PreviewHold()
        val targetHttp = generationHttp(generation(2), previewHold = previewHold)
        val opening = database(path, version = 2).openForSync(
            config = config().copy(generationId = 2),
            httpClient = targetHttp,
            upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet()),
        )
        try {
            val advancing = async { runCatching { opening.advance() }.exceptionOrNull() }
            previewHold.entered.await()

            val cancelling = async(start = CoroutineStart.UNDISPATCHED) { opening.cancel() }
            previewHold.cancellationObserved.await()
            assertNull(opening.readyOrNull(), "cancelled preview must not publish a ready facade")

            previewHold.releaseCleanup.complete(Unit)
            assertTrue(advancing.await() is CancellationException)
            cancelling.await()
            previewHold.cleanupComplete.await()
            assertEquals(1, previewHold.cleanupCalls.get())

            var status = opening.status()
            repeat(6) {
                if (status.outcome == OversqliteOpeningOutcome.READY && opening.readyOrNull() != null) return@repeat
                status = opening.advance()
            }
            assertEquals(OversqliteOpeningOutcome.READY, status.outcome)
            assertNotNull(opening.readyOrNull(), "the same opening must remain retryable after cancellation")
        } finally {
            previewHold.releaseCleanup.complete(Unit)
            opening.close()
            targetHttp.close()
        }
    }

    @Test
    fun providerPreparationGuardPrecedesRejectionOperations() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-provider-preparation").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        val rejection = OversqlitePendingRejection(0, "source", 1, "hash", 1, "rejected")

        assertNull(opening.pendingConflict().getOrThrow())
        assertFailsWith<OversqliteProviderPreparationRequiredException> { opening.pendingRejection() }
        assertFailsWith<OversqliteProviderPreparationRequiredException> { opening.discardRejected(rejection) }
        assertFailsWith<OversqliteProviderPreparationRequiredException> { opening.retryRejected(rejection) }
        opening.close()
        assertFailsWith<OversqliteOpeningClosedException> { opening.pendingRejection() }
        assertFailsWith<OversqliteOpeningClosedException> { opening.discardRejected(rejection) }
        assertFailsWith<OversqliteOpeningClosedException> { opening.retryRejected(rejection) }
        http.close()
    }

    @Test
    fun readyOpeningRejectsSecondClientBeforeInitialization() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-one-client").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        opening.advance()
        val second = DefaultOversqliteClient(database.connection(), config(), http)

        assertTrue(second.open().exceptionOrNull() is SqliteNowSyncOpenRequiredException)

        opening.close()
        http.close()
    }

    @Test
    fun publicDirectClientsRejectOrdinaryOpenConnectionsBeforeRuntimeWrites() = runTest {
        val directory = createTempDirectory("sqlitenow-coordinatedopening-direct-client-identity")
        val path = directory.resolve("database.sqlite3").toString()
        val alias = directory.resolve(".").resolve("database.sqlite3").toString()
        val migrations = object : DatabaseMigrations {
            override val targetVersion: Int = 1

            override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                if (currentVersion == -1) {
                    conn.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)")
                    return 1
                }
                return currentVersion
            }
        }
        val firstDatabase = SqliteNowDatabase(path, migrations)
        val secondDatabase = SqliteNowDatabase(alias, migrations)
        val http = noNetworkHttpClient()
        firstDatabase.open()
        secondDatabase.open()
        val first = DefaultOversqliteClient(firstDatabase.connection(), config(), http)
        val second = DefaultOversqliteClient(secondDatabase.connection(), config(), http)

        try {
            assertTrue(first.open().exceptionOrNull() is SqliteNowSyncOpenRequiredException)
            assertTrue(second.open().exceptionOrNull() is SqliteNowSyncOpenRequiredException)
            assertEquals(
                0L,
                firstDatabase.connection().scalarLong(
                    "SELECT count(*) FROM sqlite_schema WHERE name LIKE '_sync_%'",
                ),
            )
        } finally {
            first.close()
            second.close()
            firstDatabase.close()
            secondDatabase.close()
            http.close()
        }
    }

    @Test
    fun localOnlyUpgradeAtomicallyAdvancesVersionAndRetainsRows() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-local-only").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val initial = database(path, version = 2)
        initial.openForSync(config(), http).also { opening ->
            opening.advance()
            initial.connection().execSQL("INSERT INTO notes(id,title) VALUES('n1','kept')")
            initial.connection().execSQL("CREATE TABLE drafts(label TEXT NOT NULL,payload BLOB NOT NULL)")
            initial.connection().withPreparedStatement("INSERT INTO drafts(label,payload) VALUES(?,?)") { statement ->
                statement.bindText(1, "local-only")
                statement.bindBlob(2, byteArrayOf(7, 8, 9))
                statement.step()
            }
            opening.close()
        }

        val upgraded = database(path, version = 3, localOnlyDraftMigration = true)
        val opening = upgraded.openForSync(
            config = config(),
            httpClient = http,
            upgradePlan = OversqliteUpgradePlan(
                migrationId = "notes-v2-v3",
                fromGenerationId = 1,
                toGenerationId = 1,
                fromLocalVersion = 2,
                toLocalVersion = 3,
                preservedLocalTables = setOf("drafts"),
            ),
        )
        try {
            assertEquals(OversqliteOpeningMode.LOCAL_ONLY, opening.status().mode)
            opening.advance()
            assertEquals(3L, upgraded.connection().scalarLong("PRAGMA user_version"))
            assertEquals(1L, upgraded.connection().scalarLong(
                "SELECT count(*) FROM pragma_table_xinfo('drafts') WHERE name='category'",
            ))
            assertEquals(1L, upgraded.connection().scalarLong("SELECT count(*) FROM notes WHERE id='n1' AND title='kept'"))
            assertEquals(1L, upgraded.connection().scalarLong(
                "SELECT count(*) FROM drafts WHERE label='local-only' AND hex(payload)='070809'",
            ))
            assertEquals(3L, upgraded.connection().scalarLong(
                "SELECT local_version FROM _sync_contract_identity WHERE singleton_key=1",
            ))
            assertEquals(3L, upgraded.connection().scalarLong(
                "SELECT to_local_version FROM _sync_opening_state WHERE singleton_key=1 AND outcome='ready'",
            ))
            val journal = externalText(path, "SELECT plan_digest FROM _sync_opening_state")
            opening.close()
            val replayCalls = AtomicInteger()
            val replayDb = database(path, version = 3, migrationCalls = replayCalls, localOnlyDraftMigration = true)
            val replay = replayDb.openForSync(config(), http, upgradePlan = OversqliteUpgradePlan(
                migrationId = "notes-v2-v3", fromGenerationId = 1, toGenerationId = 1,
                fromLocalVersion = 2, toLocalVersion = 3, preservedLocalTables = setOf("drafts")))
            try {
                replay.advance()
                assertNotNull(replay.readyOrNull())
                assertEquals(0, replayCalls.get(), "completed edge must not rerun migration")
                assertEquals(journal, externalText(path, "SELECT plan_digest FROM _sync_opening_state"))
            } finally { replay.close() }
        } finally {
            opening.close()
            http.close()
        }
    }

    @Test
    fun rehashedCompletedJournalWithDifferentDatabaseIdentityIsRejectedBeforeSuccessorAdmission() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-completed-journal").resolve("database.sqlite3").toString()
        val http = generationHttp(generation(1), includeConnect = true)
        val database = database(path, version = 2)
        val opening = database.openForSync(config(), http)
        opening.advance()
        opening.readyOrNull()!!.client.attach("user-a").getOrThrow()
        val record = OversqliteOpeningStore(database.connection()).readOrNull()!!
        val corrupt = record.copy(databaseId = "different-database")
        database.connection().withPreparedStatement(
            "UPDATE _sync_opening_state SET database_id=?,integrity_hash=? WHERE singleton_key=1",
        ) { statement ->
            statement.bindText(1, corrupt.databaseId)
            statement.bindText(2, corrupt.integrityHash())
            statement.step()
        }
        opening.close()
        http.close()

        val retry = database(path, version = 3, localOnlyDraftMigration = true)
        val retryHttp = noNetworkHttpClient()
        try {
            assertFailsWith<IllegalArgumentException> {
                retry.openForSync(
                    config = config(),
                    httpClient = retryHttp,
                    upgradePlan = OversqliteUpgradePlan(
                        migrationId = "successor",
                        fromGenerationId = 1,
                        toGenerationId = 1,
                        fromLocalVersion = 2,
                        toLocalVersion = 3,
                        preservedLocalTables = emptySet(),
                    ),
                )
            }
            assertEquals("different-database", externalText(path, "SELECT database_id FROM _sync_opening_state"))
            assertEquals(2L, externalScalarLong(path, "PRAGMA user_version"))
        } finally {
            retryHttp.close()
        }
    }

    @Test
    fun admissionRechecksRehashedStateAtActualWriterReservation() = runTest {
        data class Mutation(val name: String, val change: (OversqliteOpeningRecord) -> OversqliteOpeningRecord,
            val extraSql: String? = null)
        val mutations = listOf(
            Mutation("database", { it.copy(databaseId = "alien") }),
            Mutation("service", { it.copy(serviceId = "alien") }),
            Mutation("generation", { it.copy(toGenerationId = 99) }),
            Mutation("local version", { it.copy(toLocalVersion = 99) }),
            Mutation("fingerprint", { it.copy(targetFingerprint = "f".repeat(64)) }),
            Mutation("descriptor hash", { it }, "UPDATE _sync_admitted_contracts SET descriptor_hash='${"0".repeat(64)}'"),
            Mutation("descriptor bytes", { it }, "UPDATE _sync_admitted_contracts SET descriptor='{}'"),
            Mutation("confirmation", { it }, "UPDATE _sync_contract_identity SET fingerprint=''"),
            Mutation("installed identity", { it }, "UPDATE _sync_contract_identity SET physical_shape='alien'"),
            Mutation("physical schema", { it }, "ALTER TABLE notes ADD COLUMN unexpected TEXT"),
        )
        for (variant in listOf(
            "local_journal", "local_migration", "generation_u0", "generation_u4",
            "offline_local_journal", "offline_local_migration",
        )) for (mutation in mutations) {
            val confirmed = !variant.startsWith("offline_")
            if (!confirmed && mutation.name in setOf("descriptor hash", "descriptor bytes", "confirmation")) continue
            val mode = variant.removePrefix("offline_")
            val scenario = "$variant / ${mutation.name}"
            val path = createTempDirectory("coordinatedopening-reservation").resolve("database.sqlite3").toString()
            val seedHttp = generationHttp(generation(1), includeConnect = true)
            val seedDb = database(path, 2)
            val seed = seedDb.openForSync(config(), seedHttp)
            seed.advance()
            if (confirmed) seed.readyOrNull()!!.client.attach("user").getOrThrow()
            seedDb.connection().execSQL("CREATE TABLE drafts(label TEXT NOT NULL)")
            seed.close(); seedHttp.close()
            val calls = AtomicInteger()
            val httpCalls = AtomicInteger()
            val generationChange = mode == "generation_u0" || mode == "generation_u4"
            val localChange = mode == "local_journal"
            val targetDb = database(path, if (localChange) 3 else 2, calls, localOnlyDraftMigration = localChange)
            val targetConfig = config().copy(generationId = if (generationChange) 2 else 1)
            val plan = if (mode == "local_migration") null else OversqliteUpgradePlan(
                migrationId = "successor", fromGenerationId = 1, toGenerationId = targetConfig.generationId,
                fromLocalVersion = 2, toLocalVersion = if (localChange) 3 else 2,
                preservedLocalTables = if (generationChange) setOf("drafts") else emptySet(),
            )
            val http = generationHttp(generation(if (generationChange) 2 else 1), requestCount = httpCalls)
            val opening = targetDb.openForSync(targetConfig, http, upgradePlan = plan)
            var cutCalls = 0
            var requestsAtCut = -1
            var witness: List<String>? = null
            openingAdmissionReservationForTest = { cut, connection ->
                if (cut == mode) {
                    openingAdmissionReservationForTest = null
                    cutCalls++
                    val live = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
                    val changed = mutation.change(live)
                    externalExecute(path, "UPDATE _sync_opening_state SET database_id='${changed.databaseId}'," +
                        "service_id='${changed.serviceId}',to_generation_id=${changed.toGenerationId}," +
                        "to_local_version=${changed.toLocalVersion},target_fingerprint='${changed.targetFingerprint}'," +
                        "integrity_hash='${changed.integrityHash()}'")
                    mutation.extraSql?.let { externalExecute(path, it) }
                    witness = externalAdmissionWitness(path)
                    requestsAtCut = httpCalls.get()
                }
            }
            try {
                val failure = runCatching { opening.advance() }.exceptionOrNull()
                if (mutation.name in setOf("descriptor hash", "descriptor bytes")) {
                    assertTrue(
                        failure is IllegalArgumentException ||
                            failure is OversqliteUpgradePreconditionException,
                        "$scenario: $failure",
                    )
                } else {
                    assertTrue(failure is IllegalArgumentException, "$scenario: $failure")
                }
                assertEquals(1, cutCalls, scenario)
                assertEquals(0, calls.get(), scenario)
                assertEquals(requestsAtCut, httpCalls.get(), scenario)
                assertNull(opening.readyOrNull(), scenario)
                assertEquals(witness, externalAdmissionWitness(path), scenario)
            } finally {
                openingAdmissionReservationForTest = null
                opening.close(); http.close()
            }
        }
    }

    @Test
    fun completedGenerationReplayRechecksTheWholeDurableEdgeBeforePublication() = runTest {
        data class Mutation(
            val name: String,
            val change: (OversqliteOpeningRecord) -> OversqliteOpeningRecord,
            val extraSql: String? = null,
        )
        val mutations = listOf(
            Mutation("database", { it.copy(databaseId = "alien") }),
            Mutation("service", { it.copy(serviceId = "alien") }),
            Mutation("generation", { it.copy(toGenerationId = 99) }),
            Mutation("local version", { it.copy(toLocalVersion = 99) }),
            Mutation("fingerprint", { it.copy(targetFingerprint = "f".repeat(64)) }),
            Mutation("descriptor hash", { it }, "UPDATE _sync_admitted_contracts SET descriptor_hash='${"0".repeat(64)}' WHERE generation_id=2"),
            Mutation("descriptor bytes", { it }, "UPDATE _sync_admitted_contracts SET descriptor='{}' WHERE generation_id=2"),
            Mutation("confirmation", { it }, "UPDATE _sync_contract_identity SET fingerprint=''"),
            Mutation("installed identity", { it }, "UPDATE _sync_contract_identity SET physical_shape='alien'"),
            Mutation("physical schema", { it }, "ALTER TABLE notes ADD COLUMN unexpected TEXT"),
        )
        val root = createTempDirectory("coordinatedopening-completed-replay")
        val seedPath = root.resolve("seed.sqlite3").toString()
        val firstHttp = generationHttp(generation(1), includeConnect = true)
        val firstDb = database(seedPath, 2)
        val first = firstDb.openForSync(config(), firstHttp)
        first.advance()
        first.readyOrNull()!!.client.attach("user").getOrThrow()
        first.close()
        firstHttp.close()
        val targetPlan = OversqliteUpgradePlan("edge", 1, 2, 2, 2, emptySet())
        val targetHttp = generationHttp(generation(2))
        val targetDb = database(seedPath, 2)
        val target = targetDb.openForSync(
            config().copy(generationId = 2), targetHttp, upgradePlan = targetPlan,
        )
        assertEquals(OversqliteOpeningOutcome.READY, target.advance().outcome)
        targetDb.connection().execSQL("CREATE TABLE drafts(label TEXT NOT NULL)")
        target.close()
        targetHttp.close()

        for (mutation in mutations) {
            val scenarioPath = root.resolve("${mutation.name.replace(' ', '-')}.sqlite3").toString()
            Files.copy(java.nio.file.Path.of(seedPath), java.nio.file.Path.of(scenarioPath))
            val calls = AtomicInteger()
            val requests = AtomicInteger()
            val replayHttp = generationHttp(generation(2), requestCount = requests)
            val replay = database(scenarioPath, 2, calls).openForSync(
                config().copy(generationId = 2), replayHttp, upgradePlan = targetPlan,
            )
            try {
                var cutCalls = 0
                openingAdmissionReservationForTest = { cut, connection ->
                    if (cut == "completed_generation_replay") {
                        openingAdmissionReservationForTest = null
                        cutCalls++
                        val live = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
                        val changed = mutation.change(live)
                        externalExecute(
                            scenarioPath,
                            "UPDATE _sync_opening_state SET database_id='${changed.databaseId}'," +
                                "service_id='${changed.serviceId}',to_generation_id=${changed.toGenerationId}," +
                                "to_local_version=${changed.toLocalVersion},target_fingerprint='${changed.targetFingerprint}'," +
                                "integrity_hash='${changed.integrityHash()}'",
                        )
                        mutation.extraSql?.let { externalExecute(scenarioPath, it) }
                    }
                }
                assertFailsWith<IllegalArgumentException>(mutation.name) { replay.advance() }
                assertEquals(1, cutCalls, mutation.name)
                assertEquals(0, calls.get(), mutation.name)
                assertEquals(0, requests.get(), mutation.name)
                assertNull(replay.readyOrNull(), mutation.name)
            } finally {
                openingAdmissionReservationForTest = null
                replay.close()
                replayHttp.close()
            }
        }

        val successorPath = root.resolve("valid-distinct-successor.sqlite3").toString()
        Files.copy(java.nio.file.Path.of(seedPath), java.nio.file.Path.of(successorPath))
        val successorCalls = AtomicInteger()
        val successorDatabase = database(
            successorPath,
            version = 3,
            migrationCalls = successorCalls,
            localOnlyDraftMigration = true,
        )
        val successorHttp = noNetworkHttpClient()
        val successor = successorDatabase.openForSync(
            config = config().copy(generationId = 2),
            httpClient = successorHttp,
            upgradePlan = OversqliteUpgradePlan(
                migrationId = "successor-after-edge",
                fromGenerationId = 2,
                toGenerationId = 2,
                fromLocalVersion = 2,
                toLocalVersion = 3,
                preservedLocalTables = setOf("drafts"),
            ),
        )
        try {
            assertEquals(OversqliteOpeningOutcome.READY, successor.advance().outcome)
            assertTrue(successorCalls.get() > 0)
            assertEquals("successor-after-edge", externalText(successorPath, "SELECT migration_id FROM _sync_opening_state"))
        } finally {
            successor.close()
            successorHttp.close()
        }
    }

    private fun externalAdmissionWitness(path: String): List<String> =
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            fun values(sql: String): List<String> = connection.prepare(sql).use { statement ->
                buildList { while (statement.step()) add(statement.getText(0)) }
            }
            val schema = values("SELECT quote(type)||quote(name)||quote(tbl_name)||quote(rootpage)||quote(sql) " +
                "FROM sqlite_schema ORDER BY type,name")
            val tables = values("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            schema + tables.flatMap { table ->
                val quotedTable = "\"${table.replace("\"", "\"\"")}\""
                val columns = values("SELECT name FROM pragma_table_info('$table')").map {
                    "quote(\"${it.replace("\"", "\"\"")}\")"
                }
                values("SELECT ${columns.joinToString("||'|'||")} FROM $quotedTable ORDER BY 1").map { "$table:$it" }
            } + values("PRAGMA user_version")
        }

    @Test
    fun failedFreshMigrationRetainsJournalAndResumesAcrossReopen() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-fresh-recovery").resolve("database.sqlite3").toString()
        val failOnce = AtomicInteger(1)
        val firstHttp = noNetworkHttpClient()
        val firstDatabase = database(path, version = 2, failMigrationOnce = failOnce)
        val first = firstDatabase.openForSync(config(), firstHttp)
        try {
            assertFailsWith<IllegalStateException> { first.advance() }
            assertEquals("migrating_local", first.status().phase)
            assertEquals(2L, first.status().revision)
            assertEquals(OversqliteOpeningOutcome.LOCAL_MIGRATION_FAILED, first.status().outcome)
            assertEquals("local_migration_failed", first.status().diagnosticCode)
            assertEquals("local_migration_failed", first.status().diagnosticMessage)
            assertEquals(1L, first.status().targetGenerationId)
            assertEquals(2, first.status().targetLocalVersion)
        } finally {
            first.close()
            firstHttp.close()
        }

        val retryHttp = noNetworkHttpClient()
        val retryDatabase = database(path, version = 2, failMigrationOnce = failOnce)
        val retry = retryDatabase.openForSync(config(), retryHttp)
        try {
            val ready = retry.advance()
            assertEquals(OversqliteOpeningOutcome.READY, ready.outcome)
            assertEquals(4L, ready.revision)
            assertEquals(2L, retryDatabase.connection().scalarLong("PRAGMA user_version"))
            assertEquals(4L, retryDatabase.connection().scalarLong(
                "SELECT revision FROM _sync_opening_state WHERE singleton_key=1 AND outcome='ready'",
            ))
            assertEquals(1L, retryDatabase.connection().scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='notes'",
            ))
        } finally {
            retry.close()
            retryHttp.close()
        }
        assertEquals(0, failOnce.get())
    }

    @Test
    fun failureStatusRechecksAdmissionAndPreservationAfterACommittedEdgeChange() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-failure-status").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val seedDatabase = database(path, version = 2)
        val seed = seedDatabase.openForSync(config(), http)
        seed.advance()
        seedDatabase.connection().execSQL("CREATE TABLE drafts(label TEXT NOT NULL)")
        seed.close()

        val failOnce = AtomicInteger(1)
        val targetDatabase = database(
            path,
            version = 3,
            failMigrationOnce = failOnce,
            localOnlyDraftMigration = true,
        )
        val opening = targetDatabase.openForSync(
            config = config(),
            httpClient = http,
            upgradePlan = OversqliteUpgradePlan(
                migrationId = "notes-v2-v3",
                fromGenerationId = 1,
                toGenerationId = 1,
                fromLocalVersion = 2,
                toLocalVersion = 3,
                preservedLocalTables = setOf("drafts"),
            ),
        )
        var cutCalls = 0
        var journalAtCut: OversqliteOpeningRecord? = null
        openingAdmissionReservationForTest = { cut, connection ->
            if (cut == "failure_status") {
                openingAdmissionReservationForTest = null
                cutCalls++
                val live = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
                journalAtCut = live
                externalExecute(path, "INSERT INTO drafts(label) VALUES('late')")
            }
        }
        try {
            val failure = assertFailsWith<IllegalStateException> { opening.advance() }
            assertTrue(
                failure.message.orEmpty().contains("preservation validation failed"),
                "unexpected failure: ${failure.message}; cause=${failure.cause?.message}",
            )
            assertEquals(1, cutCalls)
            val unchanged = checkNotNull(journalAtCut)
            assertEquals(unchanged.revision, externalScalarLong(path, "SELECT revision FROM _sync_opening_state"))
            assertEquals(unchanged.outcome, externalText(path, "SELECT outcome FROM _sync_opening_state"))
            assertEquals(
                unchanged.preservationDigest,
                externalText(path, "SELECT preservation_digest FROM _sync_opening_state"),
            )
            assertEquals(unchanged.integrityHash(), externalText(path, "SELECT integrity_hash FROM _sync_opening_state"))
            assertEquals(unchanged.failureCode, externalText(path, "SELECT failure_code FROM _sync_opening_state"))
            assertEquals(unchanged.failureMessage, externalText(path, "SELECT failure_message FROM _sync_opening_state"))
            assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM _sync_opening_state WHERE outcome='ready'"))
            assertNull(opening.readyOrNull())
            assertEquals(0, failOnce.get())
        } finally {
            openingAdmissionReservationForTest = null
            opening.close()
            http.close()
        }
    }

    @Test
    fun generationChangeCommitsU0ThroughU8AndPreservesLocalState() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-generation").resolve("database.sqlite3").toString()
        val oldGeneration = generation(1)
        val targetGeneration = generation(2)
        val initialHttp = generationHttp(oldGeneration, includeConnect = true)
        val initialDatabase = database(path, version = 2)
        val initial = initialDatabase.openForSync(config(), initialHttp)
        initial.advance()
        val attached = initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        assertTrue(attached is AttachResult.Connected)
        val oldSourceId = initial.readyOrNull()!!.client.sourceInfo().getOrThrow().currentSourceId
        initialDatabase.connection().execSQL(
            "CREATE TABLE drafts(label TEXT,payload BLOB)",
        )
        initialDatabase.connection().execSQL("CREATE INDEX drafts_label_idx ON drafts(label)")
        initialDatabase.connection().execSQL(
            "CREATE TRIGGER drafts_insert_trace AFTER INSERT ON drafts BEGIN SELECT 1; END",
        )
        initialDatabase.connection().withPreparedStatement("INSERT INTO drafts(label,payload) VALUES(?,?)") { statement ->
            repeat(2) {
                statement.bindText(1, "nul\u0000text")
                statement.bindBlob(2, byteArrayOf(0, 1, 2, -1))
                statement.step()
                statement.reset()
                statement.clearBindings()
            }
        }
        initial.close()
        initialHttp.close()

        val targetRequests = AtomicInteger()
        val snapshotBodies = mutableListOf<String>()
        val targetHttp = generationHttp(
            targetGeneration,
            requestCount = targetRequests,
            snapshotRows = listOf(
                SnapshotRow(
                    schema = "business",
                    table = "notes",
                    key = mapOf("id" to "33333333-3333-4333-8333-333333333333"),
                    rowVersion = 1,
                    payload = buildJsonObject {
                        put("id", "33333333-3333-4333-8333-333333333333")
                        put("title", "recovered")
                    },
                ),
            ),
            expireFirstSnapshot = true,
            snapshotRequestBodies = snapshotBodies,
        )
        val targetDatabase = database(path, version = 2)
        val opening = targetDatabase.openForSync(
            config = config().copy(generationId = 2),
            httpClient = targetHttp,
            upgradePlan = OversqliteUpgradePlan(
                migrationId = "notes-g1-g2",
                fromGenerationId = 1,
                toGenerationId = 2,
                fromLocalVersion = 2,
                toLocalVersion = 2,
                preservedLocalTables = setOf("drafts"),
            ),
        )
        try {
            val recovery = opening.advance()
            assertEquals("rebuild_running", recovery.phase)
            assertNull(recovery.outcome)
            assertEquals(2, snapshotBodies.size)
        } finally {
            opening.close()
        }

        val resumedTargetDatabase = database(path, version = 2)
        val resumedOpening = resumedTargetDatabase.openForSync(
            config = config().copy(generationId = 2),
            httpClient = targetHttp,
            upgradePlan = OversqliteUpgradePlan(
                migrationId = "notes-g1-g2",
                fromGenerationId = 1,
                toGenerationId = 2,
                fromLocalVersion = 2,
                toLocalVersion = 2,
                preservedLocalTables = setOf("drafts"),
            ),
        )
        try {
            assertEquals("rebuild_running", resumedOpening.status().phase)
            val status = resumedOpening.advance()
            assertEquals(OversqliteOpeningOutcome.READY, status.outcome)
            assertEquals("ready", status.phase)
            assertEquals(13L, status.revision)
            assertEquals(true, status.drainComplete)
            assertEquals(true, status.targetReadsAvailable)
            assertEquals(true, status.writesAvailable)
            assertNotNull(resumedOpening.readyOrNull())
            assertTrue(targetRequests.get() > 8, "expiry recovery must perform a bounded second snapshot request")
            assertEquals(2, snapshotBodies.size)
            assertTrue(Json.parseToJsonElement(snapshotBodies.first()).jsonObject["source_replacement"] != JsonNull)
            assertTrue(Json.parseToJsonElement(snapshotBodies.last()).jsonObject["source_replacement"].let {
                it == null || it == JsonNull
            })
            assertEquals(1L, resumedTargetDatabase.connection().scalarLong(
                "SELECT count(*) FROM notes WHERE id='33333333-3333-4333-8333-333333333333' AND title='recovered'",
            ))
            assertEquals(2L, resumedTargetDatabase.connection().scalarLong("SELECT count(*) FROM drafts"))
            assertEquals(2L, resumedTargetDatabase.connection().scalarLong("SELECT count(*) FROM drafts WHERE instr(label,char(0))=4"))
            assertEquals(2L, resumedTargetDatabase.connection().scalarLong("SELECT count(*) FROM drafts WHERE hex(payload)='000102FF'"))
            assertEquals(3L, resumedTargetDatabase.connection().scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE lower(tbl_name)='drafts' AND type IN ('table','index','trigger')",
            ))
        } finally {
            resumedOpening.close()
            targetHttp.close()
        }

        val inspect = database(path, version = 2)
        val inspectRequestPaths = mutableListOf<String>()
        val inspectHttp = generationHttp(targetGeneration, requestPaths = inspectRequestPaths)
        val resumed = inspect.openForSync(
            config = config().copy(generationId = 2),
            httpClient = inspectHttp,
            upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, setOf("drafts")),
        )
        try {
            assertEquals("ready", resumed.status().phase)
            assertEquals(13L, resumed.status().revision)
            assertEquals(true, resumed.status().drainComplete)
            val ready = resumed.advance()
            assertEquals(OversqliteOpeningOutcome.READY, ready.outcome)
            assertNotNull(resumed.readyOrNull())
            assertEquals(
                listOf("/sync/generations/2", "/sync/connect"),
                inspectRequestPaths,
                "completed generation replay must revalidate discovery and connect before Ready",
            )
        } finally {
            resumed.close()
            inspectHttp.close()
        }
        assertTrue(oldSourceId.isNotBlank())
    }

    @Test
    fun generationOpeningRecoversLostPreviewReplacementAndFinalReturnResponses() = runTest {
        OpeningLostResponsePoint.entries.forEach { point ->
            val path = createTempDirectory("sqlitenow-coordinatedopening-lost-${point.name.lowercase()}")
                .resolve("database.sqlite3").toString()
            val oldAuthority = generationHttp(generation(1), includeConnect = true)
            val oldDatabase = database(path, version = 2)
            val oldOpening = oldDatabase.openForSync(config(), oldAuthority)
            oldOpening.advance()
            oldOpening.readyOrNull()!!.client.attach("user-a").getOrThrow()
            oldOpening.close()
            oldAuthority.close()

            val lost = AtomicInteger()
            val snapshotBodies = mutableListOf<String>()
            val authority = generationHttp(
                generation = generation(2),
                snapshotRows = listOf(
                    SnapshotRow(
                        schema = "business",
                        table = "notes",
                        key = mapOf("id" to "33333333-3333-4333-8333-333333333333"),
                        rowVersion = 1,
                        payload = buildJsonObject {
                            put("id", "33333333-3333-4333-8333-333333333333")
                            put("title", "recovered")
                        },
                    ),
                ),
                snapshotRequestBodies = snapshotBodies,
                lostResponsePoint = point,
                lostResponseCount = lost,
            )
            val plan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet())
            val opening = database(path, version = 2).openForSync(
                config = config().copy(generationId = 2),
                httpClient = authority,
                upgradePlan = plan,
            )
            try {
                if (point == OpeningLostResponsePoint.FINAL_RETURN) {
                    openingDurableCommitForTest = { cut, _ ->
                        if (cut == "after_u8" && lost.compareAndSet(0, 1)) {
                            error("injected lost final U8 return")
                        }
                    }
                }
                assertFailsWith<IllegalStateException>(point.name) { opening.advance() }
                assertNull(opening.readyOrNull(), point.name)
            } finally {
                openingDurableCommitForTest = null
                opening.close()
            }

            val resumed = database(path, version = 2).openForSync(
                config = config().copy(generationId = 2),
                httpClient = authority,
                upgradePlan = plan,
            )
            try {
                var status = resumed.status()
                repeat(6) {
                    if (status.outcome == OversqliteOpeningOutcome.READY && resumed.readyOrNull() != null) return@repeat
                    status = resumed.advance()
                }
                assertEquals(OversqliteOpeningOutcome.READY, status.outcome, point.name)
                assertNotNull(resumed.readyOrNull(), point.name)
                assertEquals(1, lost.get(), point.name)
                if (point == OpeningLostResponsePoint.REPLACEMENT) {
                    val replacementBodies = snapshotBodies.filter {
                        Json.parseToJsonElement(it).jsonObject.containsKey("source_replacement")
                    }
                    assertEquals(2, replacementBodies.size)
                    assertEquals(1, replacementBodies.toSet().size, "replacement retry tuple changed")
                }
            } finally {
                resumed.close()
                authority.close()
            }
        }
    }

    @Test
    fun generationAdmissionRaceBeforeU0RollsBackWithoutFreezingTheCandidate() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-u0-race").resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initialDatabase = database(path, version = 2)
        val initial = initialDatabase.openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()

        val targetHttp = generationHttp(
            generation = generation(2),
            onGenerationDiscovery = {
                BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { external ->
                    external.prepare(
                        "UPDATE _sync_attachment_state SET last_bundle_seq_seen=last_bundle_seq_seen+1 WHERE singleton_key=1",
                    ).use { statement -> statement.step() }
                }
            },
        )
        val targetDatabase = database(path, version = 2)
        val opening = targetDatabase.openForSync(
            config = config().copy(generationId = 2),
            httpClient = targetHttp,
            upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet()),
        )
        try {
            val failure = assertFailsWith<IllegalArgumentException> { opening.advance() }
            assertTrue(failure.message.orEmpty().contains("attachment state changed before U0"))
            assertEquals(OversqliteOpeningOutcome.READY, opening.status().outcome)
            assertEquals(
                1L,
                externalScalarLong(path, "SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1"),
            )
            assertEquals(
                0L,
                externalScalarLong(path, "SELECT count(*) FROM _sync_opening_state WHERE phase='upgrade_started'"),
            )
        } finally {
            opening.close()
            targetHttp.close()
        }
    }

    @Test
    fun malformedU0TargetSelectionsLeaveNoArtifactsAndAllowCorrectedRetry() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-u0-selection").resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initialDatabase = database(path, version = 2)
        val initial = initialDatabase.openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()
        externalExecute(path, "DROP TABLE _sync_opening_state")

        data class Scenario(val name: String, val tables: List<SyncTable>)
        val scenarios = listOf(
            Scenario(
                "duplicate table",
                listOf(
                    SyncTable("notes", syncKeyColumnName = "id"),
                    SyncTable("notes", syncKeyColumnName = "id"),
                ),
            ),
            Scenario(
                "duplicate ordered key",
                listOf(SyncTable("notes", syncKeyColumns = listOf("id", "id"))),
            ),
            Scenario(
                "noncanonical table",
                listOf(SyncTable("Notes", syncKeyColumnName = "id")),
            ),
        )
        scenarios.forEach { scenario ->
            val requestPaths = mutableListOf<String>()
            val http = generationHttp(generation(2), requestPaths = requestPaths)
            val opening = database(path, version = 2).openForSync(
                config = config().copy(generationId = 2, syncTables = scenario.tables),
                httpClient = http,
                upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet()),
            )
            try {
                assertFailsWith<IllegalArgumentException>(scenario.name) { opening.advance() }
                assertEquals(
                    1L,
                    externalScalarLong(path, "SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1"),
                    scenario.name,
                )
                assertEquals(
                    0L,
                    externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name LIKE '_sqlitenow_preserve_%'"),
                    scenario.name,
                )
                assertEquals(
                    0L,
                    externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE name='_sync_opening_state'"),
                    scenario.name,
                )
                assertEquals(
                    0L,
                    externalScalarLong(path, "SELECT count(*) FROM _sync_admitted_contracts WHERE generation_id=2"),
                    scenario.name,
                )
                assertTrue(
                    requestPaths.all { it == "/sync/generations/2" },
                    scenario.name,
                )
            } finally {
                opening.close()
                http.close()
            }
        }

        val retryHttp = generationHttp(generation(2))
        val retry = database(path, version = 2).openForSync(
            config = config().copy(generationId = 2),
            httpClient = retryHttp,
            upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet()),
        )
        try {
            assertEquals(OversqliteOpeningOutcome.READY, retry.advance().outcome)
        } finally {
            retry.close()
            retryHttp.close()
        }
    }

    @Test
    fun schemaevolutionProfileRejectsDescriptorDeltasBeforeU0() = runTest {
        data class Scenario(val name: String, val target: SyncGeneration)
        val base = generation(2)
        val withChangedKey = base.copy(
            tables = base.tables.map { table ->
                table.copy(
                    syncKeyColumns = listOf("alternate_id"),
                    columns = table.columns + GenerationColumn("alternate_id", "uuid", false),
                )
            },
        ).withComputedFingerprint()
        val withChangedCodec = base.copy(
            tables = base.tables.map { table ->
                table.copy(columns = table.columns.map { column ->
                    if (column.name == "title") column.copy(wireType = "json") else column
                })
            },
        ).withComputedFingerprint()
        val withChangedNullability = base.copy(
            tables = base.tables.map { table ->
                table.copy(columns = table.columns.map { column ->
                    if (column.name == "title") column.copy(nullable = true) else column
                })
            },
        ).withComputedFingerprint()
        val withForbiddenForeignKey = base.copy(
            tables = base.tables.map { table ->
                table.copy(
                    columns = table.columns + GenerationColumn("parent_id", "uuid", false),
                    foreignKeys = listOf(
                        GenerationForeignKey(
                            columns = listOf("parent_id"),
                            referencedSchema = "business",
                            referencedTable = "notes",
                            referencedColumns = listOf("id"),
                            onDelete = "cascade",
                            onUpdate = "cascade",
                        ),
                    ),
                )
            },
        ).withComputedFingerprint()
        val scenarios = listOf(
            Scenario("sync key", withChangedKey),
            Scenario("shared codec", withChangedCodec),
            Scenario("shared nullability", withChangedNullability),
            Scenario("non-null added foreign key", withForbiddenForeignKey),
        )

        scenarios.forEach { scenario ->
            val path = createTempDirectory("sqlitenow-schemaevolution-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            val initialHttp = generationHttp(generation(1), includeConnect = true)
            val initialDatabase = database(path, version = 2)
            val initial = initialDatabase.openForSync(config(), initialHttp)
            initial.advance()
            initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
            initial.close()
            initialHttp.close()

            val beforeSchema = externalSchemaRows(path)
            val beforeRevision = externalScalarLong(path, "SELECT revision FROM _sync_opening_state")
            val http = generationHttp(scenario.target)
            val opening = database(path, version = 2).openForSync(
                config = config().copy(generationId = 2),
                httpClient = http,
                upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet()),
            )
            try {
                assertFailsWith<OversqliteUpgradePlanMismatchException>(scenario.name) { opening.advance() }
                val status = opening.status()
                assertEquals(OversqliteOpeningOutcome.UPGRADE_PLAN_MISMATCH, status.outcome, scenario.name)
                assertEquals(OversqliteOpeningNextAction.REPAIR_CONFIGURATION, status.nextAction, scenario.name)
                assertNull(opening.readyOrNull(), scenario.name)
                assertEquals(beforeSchema, externalSchemaRows(path), scenario.name)
                assertEquals(beforeRevision, externalScalarLong(path, "SELECT revision FROM _sync_opening_state"), scenario.name)
                assertEquals(1L, externalScalarLong(path, "SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1"), scenario.name)
                assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM _sync_admitted_contracts WHERE generation_id=2"), scenario.name)
            } finally {
                opening.close()
                http.close()
            }
        }
    }

    @Test
    fun schemaevolutionTableOmissionAndExistingForeignKeyEvolutionRejectArtifactFree() = runTest {
        val parent = GenerationTable(
            "business",
            "parents",
            listOf("id"),
            listOf(GenerationColumn("id", "uuid", false)),
            emptyList(),
        )
        val childForeignKey = GenerationForeignKey(
            columns = listOf("parent_id"),
            referencedSchema = "business",
            referencedTable = "parents",
            referencedColumns = listOf("id"),
            onDelete = "set_null",
            onUpdate = "cascade",
        )
        val child = GenerationTable(
            "business",
            "children",
            listOf("id"),
            listOf(
                GenerationColumn("id", "uuid", false),
                GenerationColumn("parent_id", "uuid", true),
            ),
            listOf(childForeignKey),
        )
        val old = SyncGeneration("1", "0".repeat(64), listOf(parent, child)).withComputedFingerprint()
        val oldConfig = config().copy(
            syncTables = listOf(SyncTable("parents", "id"), SyncTable("children", "id")),
        )
        val oldSchema = listOf(
            "CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL)",
            "CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT " +
                "REFERENCES parents(id) ON DELETE SET NULL ON UPDATE CASCADE)",
        )
        data class Scenario(val name: String, val target: SyncGeneration, val targetTables: List<SyncTable>)
        val otherParent = parent.copy(table = "other_parents")
        val scenarios = listOf(
            Scenario(
                "table omission",
                SyncGeneration("2", "0".repeat(64), listOf(parent)).withComputedFingerprint(),
                listOf(SyncTable("parents", "id")),
            ),
            Scenario(
                "existing foreign key removal",
                SyncGeneration("2", "0".repeat(64), listOf(parent, child.copy(foreignKeys = emptyList())))
                    .withComputedFingerprint(),
                oldConfig.syncTables,
            ),
            Scenario(
                "existing foreign key retarget",
                SyncGeneration(
                    "2",
                    "0".repeat(64),
                    listOf(
                        parent,
                        otherParent,
                        child.copy(foreignKeys = listOf(childForeignKey.copy(referencedTable = "other_parents"))),
                    ),
                ).withComputedFingerprint(),
                listOf(
                    SyncTable("parents", "id"),
                    SyncTable("other_parents", "id"),
                    SyncTable("children", "id"),
                ),
            ),
            Scenario(
                "existing foreign key action",
                SyncGeneration(
                    "2",
                    "0".repeat(64),
                    listOf(parent, child.copy(foreignKeys = listOf(childForeignKey.copy(onDelete = "cascade")))),
                ).withComputedFingerprint(),
                oldConfig.syncTables,
            ),
        )

        for (scenario in scenarios) {
            val path = createTempDirectory("sqlitenow-schemaevolution-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            val initialHttp = generationHttp(old, includeConnect = true)
            val initial = database(path, 2, freshSchemaSql = oldSchema).openForSync(oldConfig, initialHttp)
            initial.advance()
            initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
            initial.close()
            initialHttp.close()
            val beforeSchema = externalSchemaRows(path)
            val beforeSource = externalText(path, "SELECT current_source_id FROM _sync_attachment_state")
            val beforeRevision = externalScalarLong(path, "SELECT revision FROM _sync_opening_state")

            val targetHttp = generationHttp(scenario.target, installedGeneration = old)
            val rejected = database(path, 2, freshSchemaSql = oldSchema).openForSync(
                oldConfig.copy(generationId = 2, syncTables = scenario.targetTables),
                targetHttp,
                upgradePlan = OversqliteUpgradePlan("rejected-${scenario.name}", 1, 2, 2, 2, emptySet()),
            )
            try {
                assertFailsWith<OversqliteUpgradePlanMismatchException>(scenario.name) { rejected.advance() }
                assertEquals(OversqliteOpeningOutcome.UPGRADE_PLAN_MISMATCH, rejected.status().outcome, scenario.name)
                assertEquals(OversqliteOpeningNextAction.REPAIR_CONFIGURATION, rejected.status().nextAction, scenario.name)
                assertNull(rejected.readyOrNull(), scenario.name)
                assertEquals(beforeSchema, externalSchemaRows(path), scenario.name)
                assertEquals(beforeSource, externalText(path, "SELECT current_source_id FROM _sync_attachment_state"), scenario.name)
                assertEquals(beforeRevision, externalScalarLong(path, "SELECT revision FROM _sync_opening_state"), scenario.name)
                assertEquals(2L, externalScalarLong(path, "PRAGMA user_version"), scenario.name)
                assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM _sync_admitted_contracts WHERE generation_id=2"), scenario.name)
            } finally {
                rejected.close()
                targetHttp.close()
            }

            val retryHttp = generationHttp(old)
            val retry = database(path, 2, freshSchemaSql = oldSchema).openForSync(oldConfig, retryHttp)
            try {
                assertEquals(OversqliteOpeningOutcome.READY, retry.advance().outcome, scenario.name)
                val ready = checkNotNull(retry.readyOrNull())
                ready.client.attach("user-a").getOrThrow()
                ready.database.connection().execSQL(
                    "INSERT INTO parents(id) VALUES('88888888-8888-4888-8888-888888888888')",
                )
                assertEquals(
                    1L,
                    ready.database.connection().scalarLong(
                        "SELECT count(*) FROM parents WHERE id='88888888-8888-4888-8888-888888888888'",
                    ),
                    scenario.name,
                )
                assertEquals(
                    1L,
                    ready.database.connection().scalarLong("SELECT count(*) FROM _sync_dirty_rows"),
                    scenario.name,
                )
            } finally {
                retry.close()
                retryHttp.close()
            }
        }
    }

    @Test
    fun schemaevolutionAddedTableNamespaceCollisionsRejectBeforeU0AndWriterRecheck() = runTest {
        data class Collision(val name: String, val sql: List<String>)
        val target = generation(2).copy(
            tables = generation(2).tables + GenerationTable(
                schema = "business",
                table = "tags",
                syncKeyColumns = listOf("id"),
                columns = listOf(GenerationColumn("id", "uuid", false)),
                foreignKeys = emptyList(),
            ),
        ).withComputedFingerprint()
        val targetConfig = config().copy(
            generationId = 2,
            syncTables = listOf(SyncTable("notes", "id"), SyncTable("tags", "id")),
        )
        val collisions = listOf(
            Collision("empty table", listOf("CREATE TABLE tags(id TEXT PRIMARY KEY)")),
            Collision(
                "populated table",
                listOf("CREATE TABLE tags(id TEXT PRIMARY KEY)", "INSERT INTO tags VALUES('local')"),
            ),
            Collision("case variant table", listOf("CREATE TABLE TaGs(id TEXT PRIMARY KEY)")),
            Collision("view", listOf("CREATE VIEW tags AS SELECT id FROM notes")),
            Collision("index", listOf("CREATE INDEX tags ON notes(title)")),
            Collision("trigger", listOf("CREATE TRIGGER tags AFTER INSERT ON notes BEGIN SELECT 1; END")),
        )

        suspend fun seed(path: String) {
            val http = generationHttp(generation(1), includeConnect = true)
            val db = database(path, 2)
            val opening = db.openForSync(config(), http)
            opening.advance()
            opening.readyOrNull()!!.client.attach("user-a").getOrThrow()
            opening.close()
            http.close()
        }

        for (scenario in collisions) {
            val path = createTempDirectory("sqlitenow-schemaevolution-collision-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            seed(path)
            scenario.sql.forEach { externalExecute(path, it) }
            val beforeSchema = externalSchemaRows(path)
            val beforeRevision = externalScalarLong(path, "SELECT revision FROM _sync_opening_state")
            val http = generationHttp(target)
            val opening = database(
                path,
                3,
                generationMigrationSql = listOf("CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL)"),
            ).openForSync(
                targetConfig,
                http,
                upgradePlan = OversqliteUpgradePlan("add-tags", 1, 2, 2, 3, emptySet()),
            )
            try {
                val failure = assertFailsWith<OversqliteUpgradePlanMismatchException>(scenario.name) {
                    opening.advance()
                }
                assertEquals("schema_evolution_compatibility_new_table_namespace", failure.message, scenario.name)
                assertEquals(OversqliteOpeningOutcome.UPGRADE_PLAN_MISMATCH, opening.status().outcome, scenario.name)
                assertEquals(OversqliteOpeningNextAction.REPAIR_CONFIGURATION, opening.status().nextAction, scenario.name)
                assertEquals(beforeSchema, externalSchemaRows(path), scenario.name)
                assertEquals(beforeRevision, externalScalarLong(path, "SELECT revision FROM _sync_opening_state"), scenario.name)
                assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM _sync_admitted_contracts WHERE generation_id=2"), scenario.name)
                assertNull(opening.readyOrNull(), scenario.name)
            } finally {
                opening.close()
                http.close()
            }
        }

        val racePath = createTempDirectory("sqlitenow-schemaevolution-collision-race")
            .resolve("database.sqlite3").toString()
        seed(racePath)
        val beforeRevision = externalScalarLong(racePath, "SELECT revision FROM _sync_opening_state")
        val http = generationHttp(target)
        val opening = database(
            racePath,
            3,
            generationMigrationSql = listOf("CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL)"),
        ).openForSync(
            targetConfig,
            http,
            upgradePlan = OversqliteUpgradePlan("add-tags", 1, 2, 2, 3, emptySet()),
        )
        try {
            var raceCalls = 0
            openingAdmissionReservationForTest = { cut, _ ->
                if (cut == "generation_u0") {
                    openingAdmissionReservationForTest = null
                    raceCalls++
                    externalExecute(racePath, "CREATE TABLE tags(id TEXT PRIMARY KEY)")
                }
            }
            val failure = assertFailsWith<OversqliteUpgradePlanMismatchException> { opening.advance() }
            assertEquals("schema_evolution_compatibility_new_table_namespace", failure.message)
            assertEquals(1, raceCalls)
            assertEquals(beforeRevision, externalScalarLong(racePath, "SELECT revision FROM _sync_opening_state"))
            assertEquals(0L, externalScalarLong(racePath, "SELECT count(*) FROM _sync_admitted_contracts WHERE generation_id=2"))
            assertEquals(1L, externalScalarLong(racePath, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='tags'"))
        } finally {
            openingAdmissionReservationForTest = null
            opening.close()
            http.close()
        }
    }

    @Test
    fun schemaevolutionAddedTableCompletesU4ThroughU8AndPublishesOnlyWhenReady() = runTest {
        val path = createTempDirectory("sqlitenow-schemaevolution-added-table")
            .resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initialDb = database(path, 2)
        val initial = initialDb.openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()

        val target = generation(2).copy(
            tables = generation(2).tables + GenerationTable(
                schema = "business",
                table = "tags",
                syncKeyColumns = listOf("id"),
                columns = listOf(
                    GenerationColumn("id", "uuid", false),
                    GenerationColumn("label", "text", false),
                ),
                foreignKeys = emptyList(),
            ),
        ).withComputedFingerprint()
        val targetConfig = config().copy(
            generationId = 2,
            syncTables = listOf(SyncTable("notes", "id"), SyncTable("tags", "id")),
        )
        val targetHttp = generationHttp(
            target,
            snapshotRows = listOf(
                SnapshotRow(
                    schema = "business",
                    table = "notes",
                    key = mapOf("id" to "33333333-3333-4333-8333-333333333333"),
                    rowVersion = 1,
                    payload = buildJsonObject {
                        put("id", "33333333-3333-4333-8333-333333333333")
                        put("title", "ready note")
                    },
                ),
                SnapshotRow(
                    schema = "business",
                    table = "tags",
                    key = mapOf("id" to "44444444-4444-4444-8444-444444444444"),
                    rowVersion = 1,
                    payload = buildJsonObject {
                        put("id", "44444444-4444-4444-8444-444444444444")
                        put("label", "added")
                    },
                ),
            ),
        )
        val targetDb = database(
            path,
            3,
            generationMigrationSql = listOf(
                "CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL)",
            ),
        )
        val opening = targetDb.openForSync(
            targetConfig,
            targetHttp,
            upgradePlan = OversqliteUpgradePlan("add-tags", 1, 2, 2, 3, emptySet()),
        )
        try {
            assertNull(opening.readyOrNull())
            val ready = opening.advance()
            assertEquals(OversqliteOpeningOutcome.READY, ready.outcome)
            assertEquals("ready", ready.phase)
            assertNotNull(opening.readyOrNull())
            assertEquals(1L, targetDb.connection().scalarLong("SELECT count(*) FROM notes WHERE title='ready note'"))
            assertEquals(1L, targetDb.connection().scalarLong("SELECT count(*) FROM tags WHERE label='added'"))
        } finally {
            opening.close()
            targetHttp.close()
        }
    }

    @Test
    fun schemaevolutionSparseGenerationOneToThreeUsesOneDirectAdditiveEdge() = runTest {
        val path = createTempDirectory("sqlitenow-schemaevolution-sparse")
            .resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initial = database(path, 2).openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()

        val target = generation(3).copy(
            tables = generation(3).tables + GenerationTable(
                "business",
                "tags",
                listOf("id"),
                listOf(GenerationColumn("id", "uuid", false)),
                emptyList(),
            ),
        ).withComputedFingerprint()
        val requestPaths = mutableListOf<String>()
        val targetHttp = generationHttp(target, requestPaths = requestPaths)
        val targetConfig = config().copy(
            generationId = 3,
            syncTables = listOf(SyncTable("notes", "id"), SyncTable("tags", "id")),
        )
        val opening = database(
            path,
            3,
            generationMigrationSql = listOf("CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL)"),
        ).openForSync(
            targetConfig,
            targetHttp,
            upgradePlan = OversqliteUpgradePlan("notes-tags-g1-g3", 1, 3, 2, 3, emptySet()),
        )
        try {
            assertNull(opening.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
            assertNotNull(opening.readyOrNull())
            assertTrue(requestPaths.any { it == "/sync/generations/3" })
            assertTrue(requestPaths.none { it.contains("/generations/2") })
            assertEquals(3L, externalScalarLong(path, "SELECT generation_id FROM _sync_contract_identity"))
            assertEquals(1L, externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='tags'"))
        } finally {
            opening.close()
            targetHttp.close()
        }
    }

    @Test
    fun schemaevolutionNullableForeignKeysWorkForRetainedAndNewParents() = runTest {
        data class Scenario(val name: String, val parentIsRetained: Boolean)
        for (scenario in listOf(
            Scenario("retained parent", true),
            Scenario("new parent", false),
        )) {
            val path = createTempDirectory("sqlitenow-schemaevolution-fk-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            val notes = generation(1).tables.single()
            val parents = GenerationTable(
                schema = "business",
                table = "parents",
                syncKeyColumns = listOf("id"),
                columns = listOf(GenerationColumn("id", "uuid", false)),
                foreignKeys = emptyList(),
            )
            val old = SyncGeneration(
                id = "1",
                fingerprint = "0".repeat(64),
                tables = if (scenario.parentIsRetained) listOf(notes, parents) else listOf(notes),
            ).withComputedFingerprint()
            val targetNotes = notes.copy(
                columns = notes.columns + GenerationColumn("parent_id", "uuid", true),
                foreignKeys = listOf(
                    GenerationForeignKey(
                        columns = listOf("parent_id"),
                        referencedSchema = "business",
                        referencedTable = "parents",
                        referencedColumns = listOf("id"),
                        onDelete = "set_null",
                        onUpdate = "cascade",
                    ),
                ),
            )
            val target = SyncGeneration(
                id = "2",
                fingerprint = "0".repeat(64),
                tables = listOf(targetNotes, parents),
            ).withComputedFingerprint()
            val oldConfig = config().copy(
                syncTables = old.tables.map { SyncTable(it.table, it.syncKeyColumns.single()) },
            )
            val initialHttp = generationHttp(old, includeConnect = true)
            val initialDb = database(
                path,
                2,
                freshSchemaSql = buildList {
                    add("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)")
                    if (scenario.parentIsRetained) add("CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL)")
                },
            )
            val initial = initialDb.openForSync(oldConfig, initialHttp)
            initial.advance()
            initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
            initial.close()
            initialHttp.close()

            val parentId = "55555555-5555-4555-8555-555555555555"
            val targetHttp = generationHttp(
                target,
                installedGeneration = old,
                snapshotRows = listOf(
                    SnapshotRow(
                        schema = "business",
                        table = "parents",
                        key = mapOf("id" to parentId),
                        rowVersion = 1,
                        payload = buildJsonObject { put("id", parentId) },
                    ),
                    SnapshotRow(
                        schema = "business",
                        table = "notes",
                        key = mapOf("id" to "66666666-6666-4666-8666-666666666666"),
                        rowVersion = 1,
                        payload = buildJsonObject {
                            put("id", "66666666-6666-4666-8666-666666666666")
                            put("title", "linked")
                            put("parent_id", parentId)
                        },
                    ),
                    SnapshotRow(
                        schema = "business",
                        table = "notes",
                        key = mapOf("id" to "77777777-7777-4777-8777-777777777777"),
                        rowVersion = 1,
                        payload = buildJsonObject {
                            put("id", "77777777-7777-4777-8777-777777777777")
                            put("title", "unlinked")
                            put("parent_id", JsonNull)
                        },
                    ),
                ),
            )
            val targetConfig = config().copy(
                generationId = 2,
                syncTables = target.tables.map { SyncTable(it.table, it.syncKeyColumns.single()) },
            )
            val targetDb = database(
                path,
                3,
                generationMigrationSql = buildList {
                    if (!scenario.parentIsRetained) {
                        add("CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL)")
                    }
                    add(
                        "ALTER TABLE notes ADD COLUMN parent_id TEXT " +
                            "REFERENCES parents(id) ON DELETE SET NULL ON UPDATE CASCADE",
                    )
                },
            )
            val opening = targetDb.openForSync(
                targetConfig,
                targetHttp,
                upgradePlan = OversqliteUpgradePlan("add-parent-${scenario.name}", 1, 2, 2, 3, emptySet()),
            )
            try {
                assertNull(opening.readyOrNull(), scenario.name)
                assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome, scenario.name)
                assertNotNull(opening.readyOrNull(), scenario.name)
                assertEquals(1L, targetDb.connection().scalarLong("SELECT count(*) FROM notes WHERE parent_id='$parentId'"), scenario.name)
                assertEquals(1L, targetDb.connection().scalarLong("SELECT count(*) FROM notes WHERE parent_id IS NULL"), scenario.name)
                assertEquals(0L, targetDb.connection().scalarLong("SELECT count(*) FROM pragma_foreign_key_check"), scenario.name)
                val updatedParentId = "88888888-8888-4888-8888-888888888888"
                targetDb.connection().execSQL(
                    "UPDATE parents SET id='$updatedParentId' WHERE id='$parentId'",
                )
                assertEquals(
                    1L,
                    targetDb.connection().scalarLong("SELECT count(*) FROM notes WHERE parent_id='$updatedParentId'"),
                    "${scenario.name}: ON UPDATE CASCADE",
                )
                targetDb.connection().execSQL("DELETE FROM parents WHERE id='$updatedParentId'")
                assertEquals(
                    2L,
                    targetDb.connection().scalarLong("SELECT count(*) FROM notes WHERE parent_id IS NULL"),
                    "${scenario.name}: ON DELETE SET NULL",
                )
                assertEquals(0L, targetDb.connection().scalarLong("SELECT count(*) FROM pragma_foreign_key_check"), scenario.name)
            } finally {
                opening.close()
                targetHttp.close()
            }
        }
    }

    @Test
    fun generationCaptureHandoffFailureAndCancellationRollBackEntireU4() = runTest {
        for (cut in listOf("before_u4_identity", "after_u4_identity", "before_u4_capture", "after_u4_capture")) {
            for (cancel in listOf(false, true)) {
                val path = createTempDirectory("sqlitenow-capture-handoff-$cut-$cancel").resolve("database.sqlite3").toString()
                val initialHttp = generationHttp(generation(1), includeConnect = true)
                val initial = database(path, 2).openForSync(config(), initialHttp)
                initial.advance()
                initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
                initial.close()
                initialHttp.close()
                val target = generation(2).let { it.copy(tables = it.tables.map { table ->
                    table.copy(columns = table.columns + GenerationColumn("extra", "text", true))
                }).withComputedFingerprint() }
                val http = generationHttp(target)
                val plan = OversqliteUpgradePlan("capture-handoff", 1, 2, 2, 3, emptySet())
                val migration = listOf("ALTER TABLE notes ADD COLUMN extra TEXT")
                val opening = database(path, 3, generationMigrationSql = migration).openForSync(config().copy(generationId = 2), http, upgradePlan = plan)
                var beforeIdentity = ""
                var beforeCapture = ""
                var beforeJournal = ""
                try {
                    openingDurableCommitForTest = { edge, connection ->
                        if (edge == "after_u3") {
                            beforeIdentity = connection.queryList("SELECT generation_id,local_version,physical_shape,fingerprint FROM _sync_contract_identity") {
                                "${it.getLong(0)}|${it.getLong(1)}|${it.getText(2)}|${it.getText(3)}"
                            }.single()
                            beforeCapture = connection.queryList("SELECT sql FROM sqlite_schema WHERE type='trigger' AND tbl_name='notes' ORDER BY name") { it.getText(0) }.joinToString("\n")
                            beforeJournal = connection.queryList("SELECT phase,revision,migration_complete FROM _sync_opening_state") {
                                "${it.getText(0)}|${it.getLong(1)}|${it.getLong(2)}"
                            }.single()
                          }
                        if (edge == cut) {
                            if (cancel) throw kotlinx.coroutines.CancellationException("injected $cut cancellation")
                            error("injected $cut failure")
                          }
                      }
                    if (cancel) assertFailsWith<kotlinx.coroutines.CancellationException> { opening.advance() }
                    else assertFailsWith<IllegalStateException> { opening.advance() }
                    assertNull(opening.readyOrNull())
                    assertEquals(2L, externalScalarLong(path, "PRAGMA user_version"))
                    assertEquals(0L, externalScalarLong(path, "SELECT count(*) FROM pragma_table_info('notes') WHERE name='extra'"))
                    assertEquals(0L, externalScalarLong(path, "SELECT apply_mode FROM _sync_apply_state"))
                    assertEquals(beforeIdentity, externalText(path, "SELECT generation_id||'|'||local_version||'|'||physical_shape||'|'||fingerprint FROM _sync_contract_identity"))
                    // Failure reporting is a separate writer transaction after U4 rolls back.
                    // It advances only the diagnostic revision, never the migration phase.
                    val journal = beforeJournal.split("|")
                    val reportedJournal = "${journal[0]}|${journal[1].toLong() + 1L}|${journal[2]}"
                    assertEquals(reportedJournal, externalText(path, "SELECT phase||'|'||revision||'|'||migration_complete FROM _sync_opening_state"))
                    assertEquals(beforeCapture, externalText(path, "SELECT group_concat(sql,char(10)) FROM (SELECT sql FROM sqlite_schema WHERE type='trigger' AND tbl_name='notes' ORDER BY name)"))
                } finally { openingDurableCommitForTest = null; opening.close() }
                val resumedDb = database(path, 3, generationMigrationSql = migration)
                val resumed = resumedDb.openForSync(config().copy(generationId = 2), http, upgradePlan = plan)
                try {
                    assertEquals(OversqliteOpeningOutcome.READY, resumed.advance().outcome)
                    assertNotNull(resumed.readyOrNull())
                    assertEquals(3L, externalScalarLong(path, "PRAGMA user_version"))
                    assertEquals(1L, externalScalarLong(path, "SELECT count(*) FROM pragma_table_info('notes') WHERE name='extra'"))
                    inspectRetainedRuntime(resumedDb.connection())
                } finally { resumed.close(); http.close() }
            }
        }
    }


    @Test
    fun schemaevolutionAddedTableU4FailureRollsBackAndRetriesWithSamePlan() = runTest {
        val path = createTempDirectory("sqlitenow-schemaevolution-u4-retry")
            .resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initialDb = database(path, 2)
        val initial = initialDb.openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()

        val target = generation(2).copy(
            tables = generation(2).tables + GenerationTable(
                "business",
                "tags",
                listOf("id"),
                listOf(GenerationColumn("id", "uuid", false)),
                emptyList(),
            ),
        ).withComputedFingerprint()
        val targetConfig = config().copy(
            generationId = 2,
            syncTables = listOf(SyncTable("notes", "id"), SyncTable("tags", "id")),
        )
        val targetHttp = generationHttp(target)
        val plan = OversqliteUpgradePlan("add-tags-u4-retry", 1, 2, 2, 3, emptySet())
        val broken = database(path, 3).openForSync(targetConfig, targetHttp, upgradePlan = plan)
        try {
            assertFailsWith<IllegalArgumentException> { broken.advance() }
            assertEquals(OversqliteOpeningOutcome.LOCAL_MIGRATION_FAILED, broken.status().outcome)
            assertEquals("migrating_local", externalText(path, "SELECT phase FROM _sync_opening_state"))
            assertEquals(2L, externalScalarLong(path, "PRAGMA user_version"))
            assertEquals(
                0L,
                externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='tags'"),
            )
            assertNull(broken.readyOrNull())
        } finally {
            broken.close()
        }

        val repaired = database(
            path,
            3,
            generationMigrationSql = listOf("CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL)"),
        ).openForSync(targetConfig, targetHttp, upgradePlan = plan)
        try {
            assertEquals("migrating_local", repaired.status().phase)
            assertEquals(OversqliteOpeningOutcome.READY, repaired.advance().outcome)
            assertEquals(3L, externalScalarLong(path, "PRAGMA user_version"))
            assertEquals(
                1L,
                externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='tags'"),
            )
            assertNotNull(repaired.readyOrNull())
        } finally {
            repaired.close()
            targetHttp.close()
        }
    }

    @Test
    fun schemaevolutionU4FailureMatrixRollsBackAndRetryPublishesExactlyOnce() = runTest {
        val notes = generation(1).tables.single()
        val parents = GenerationTable(
            "business", "parents", listOf("id"),
            listOf(GenerationColumn("id", "uuid", false)), emptyList(),
        )
        val children = GenerationTable(
            "business", "children", listOf("id"),
            listOf(
                GenerationColumn("id", "uuid", false),
                GenerationColumn("parent_id", "uuid", true),
            ),
            listOf(
                GenerationForeignKey(
                    columns = listOf("parent_id"),
                    referencedSchema = "business",
                    referencedTable = "parents",
                    referencedColumns = listOf("id"),
                    onDelete = "set_null",
                    onUpdate = "cascade",
                ),
            ),
        )
        val metrics = GenerationTable(
            "business", "metrics", listOf("id"),
            listOf(
                GenerationColumn("id", "uuid", false),
                GenerationColumn("base", "text", false),
                GenerationColumn("value", "text", false),
            ),
            emptyList(),
        )
        val target = SyncGeneration(
            id = "2",
            fingerprint = "0".repeat(64),
            tables = listOf(notes, parents, children, metrics),
        ).withComputedFingerprint()
        val targetConfig = config().copy(
            generationId = 2,
            syncTables = target.tables.map { SyncTable(it.table, it.syncKeyColumns.single()) },
        )
        val correctSql = listOf(
            "CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL)",
            "CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT " +
                "REFERENCES parents(id) ON DELETE SET NULL ON UPDATE CASCADE)",
            "CREATE TABLE metrics(id TEXT PRIMARY KEY NOT NULL,base TEXT NOT NULL,value TEXT NOT NULL)",
            "CREATE TABLE schemaevolution_migration_witness(id INTEGER PRIMARY KEY CHECK(id=1))",
            "INSERT INTO schemaevolution_migration_witness(id) VALUES(1)",
        )
        data class Scenario(
            val name: String,
            val sql: List<String>,
            val failMigrationOnce: AtomicInteger? = null,
            val mutatePreservedRowAtValidation: Boolean = false,
            val expectedOutcome: OversqliteOpeningOutcome = OversqliteOpeningOutcome.LOCAL_MIGRATION_FAILED,
            val expectedAction: OversqliteOpeningNextAction = OversqliteOpeningNextAction.ADVANCE,
        )
        val correctParents = correctSql[0]
        val correctMetrics = correctSql[2]
        val witnessSql = correctSql.drop(3)
        val preservationFailure = OversqliteOpeningOutcome.LOCAL_PRESERVATION_FAILED
        val inspectIntegrity = OversqliteOpeningNextAction.INSPECT_INTEGRITY
        val scenarios = listOf(
            Scenario(
                "missing foreign key",
                listOf(
                    correctParents,
                    "CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT)",
                    correctMetrics,
                ) + witnessSql,
            ),
            Scenario(
                "wrong foreign key target",
                listOf(
                    correctParents,
                    "CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT " +
                        "REFERENCES notes(id) ON DELETE SET NULL ON UPDATE CASCADE)",
                    correctMetrics,
                ) + witnessSql,
            ),
            Scenario(
                "wrong foreign key action",
                listOf(
                    correctParents,
                    "CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT " +
                        "REFERENCES parents(id) ON DELETE CASCADE ON UPDATE CASCADE)",
                    correctMetrics,
                ) + witnessSql,
            ),
            Scenario(
                "invalid child value",
                correctSql + "INSERT INTO children(id,parent_id) VALUES('child-a','missing-parent')",
            ),
            Scenario(
                "hidden generated column",
                listOf(
                    correctParents,
                    correctSql[1],
                    "CREATE TABLE metrics(id TEXT PRIMARY KEY NOT NULL,base TEXT NOT NULL," +
                        "value TEXT GENERATED ALWAYS AS (base) STORED)",
                ) + witnessSql,
            ),
            Scenario(
                "incompatible storage",
                listOf(
                    correctParents,
                    correctSql[1],
                    "CREATE TABLE metrics(id TEXT PRIMARY KEY NOT NULL,base INTEGER NOT NULL,value TEXT NOT NULL)",
                ) + witnessSql,
            ),
            Scenario("migration callback failure", correctSql, failMigrationOnce = AtomicInteger(1)),
            Scenario(
                "preserved trigger mutation",
                correctSql + "DROP TRIGGER drafts_touch",
                expectedOutcome = preservationFailure,
                expectedAction = inspectIntegrity,
            ),
            Scenario(
                "preserved row mutation",
                correctSql,
                mutatePreservedRowAtValidation = true,
                expectedOutcome = preservationFailure,
                expectedAction = inspectIntegrity,
            ),
        )

        scenarios.forEach { scenario ->
            val path = createTempDirectory("sqlitenow-schemaevolution-u4-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            val freshSchema = listOf(
                "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)",
                "CREATE TABLE drafts(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL,payload BLOB NOT NULL)",
                "CREATE INDEX drafts_label ON drafts(label)",
                "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                    "BEGIN UPDATE drafts SET label=upper(new.label) WHERE id=new.id; END",
            )
            val seedHttp = generationHttp(generation(1), includeConnect = true)
            val seed = database(path, 2, freshSchemaSql = freshSchema).openForSync(config(), seedHttp)
            seed.advance()
            val seeded = checkNotNull(seed.readyOrNull())
            seeded.database.connection().execSQL(
                "INSERT INTO drafts(id,label,payload) VALUES('draft-a','stable',x'000102FF')",
            )
            seeded.client.attach("user-a").getOrThrow()
            seed.close()
            seedHttp.close()

            val http = generationHttp(target)
            val plan = OversqliteUpgradePlan(
                "u4-${scenario.name.replace(' ', '-')}", 1, 2, 2, 3, setOf("drafts"),
            )
            val migrationCalls = AtomicInteger()
            val broken = database(
                path,
                3,
                migrationCalls = migrationCalls,
                failMigrationOnce = scenario.failMigrationOnce,
                generationMigrationSql = scenario.sql,
                freshSchemaSql = freshSchema,
            ).openForSync(targetConfig, http, upgradePlan = plan)
            try {
                if (scenario.mutatePreservedRowAtValidation) {
                    openingPreservationCheckForTest = { cut, connection ->
                        if (cut == "generation_u4") {
                            openingPreservationCheckForTest = null
                            preservedLocalTableGuardNames(setOf("drafts")).forEach { guard ->
                                connection.execSQL("DROP TRIGGER \"$guard\"")
                            }
                            connection.execSQL("UPDATE drafts SET label='changed' WHERE id='draft-a'")
                        }
                    }
                }
                assertFailsWith<Throwable>(scenario.name) { broken.advance() }
                val status = broken.status()
                assertEquals(scenario.expectedOutcome, status.outcome, scenario.name)
                assertEquals(scenario.expectedAction, status.nextAction, scenario.name)
                assertNull(broken.readyOrNull(), scenario.name)
                assertEquals(2L, externalScalarLong(path, "PRAGMA user_version"), scenario.name)
                assertEquals(1L, externalScalarLong(
                    path,
                    "SELECT count(*) FROM _sync_contract_identity WHERE generation_id=1 AND local_version=2",
                ), scenario.name)
                assertEquals(0L, externalScalarLong(
                    path,
                    "SELECT count(*) FROM sqlite_schema WHERE name IN " +
                        "('parents','children','metrics','schemaevolution_migration_witness')",
                ), scenario.name)
                assertEquals("stable", externalText(path, "SELECT label FROM drafts WHERE id='draft-a'"), scenario.name)
                assertEquals(1L, externalScalarLong(
                    path,
                    "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='drafts_touch'",
                ), scenario.name)
                assertEquals(1L, externalScalarLong(
                    path,
                    "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='drafts_label'",
                ), scenario.name)
            } finally {
                openingPreservationCheckForTest = null
                broken.close()
            }

            val publications = AtomicInteger()
            openingAfterCorePublicationForTest = { publications.incrementAndGet() }
            val repaired = database(
                path,
                3,
                migrationCalls = migrationCalls,
                generationMigrationSql = correctSql,
                freshSchemaSql = freshSchema,
            ).openForSync(targetConfig, http, upgradePlan = plan)
            try {
                assertEquals(OversqliteOpeningOutcome.READY, repaired.advance().outcome, scenario.name)
                assertNotNull(repaired.readyOrNull(), scenario.name)
                assertEquals(1L, externalScalarLong(path, "SELECT count(*) FROM schemaevolution_migration_witness"), scenario.name)
                assertEquals(1, publications.get(), scenario.name)
                assertEquals(2, migrationCalls.get(), scenario.name)
            } finally {
                openingAfterCorePublicationForTest = null
                repaired.close()
                http.close()
            }
        }
    }

    @Test
    fun schemaevolutionLocalOnlyTransformCompletesBeforeAdditiveGenerationEdge() = runTest {
        val path = createTempDirectory("sqlitenow-schemaevolution-local-then-generation")
            .resolve("database.sqlite3").toString()
        val initialSchema = listOf(
            "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)",
            "CREATE TABLE drafts(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL,payload BLOB NOT NULL)",
        )
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initial = database(path, 2, freshSchemaSql = initialSchema).openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.database.connection().execSQL(
            "INSERT INTO drafts(id,label,payload) VALUES('draft-a','mixed',x'000102FF')",
        )
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()

        val localHttp = noNetworkHttpClient()
        val localPlan = OversqliteUpgradePlan("drafts-v2-v3", 1, 1, 2, 3, setOf("drafts"))
        val localOpening = database(
            path,
            3,
            generationMigrationSql = listOf(
                "ALTER TABLE drafts ADD COLUMN normalized TEXT",
                "UPDATE drafts SET normalized=upper(label)",
                "CREATE INDEX drafts_normalized ON drafts(normalized)",
                "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                    "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END",
            ),
            freshSchemaSql = initialSchema,
        ).openForSync(config(), localHttp, upgradePlan = localPlan)
        try {
            assertNull(localOpening.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.READY, localOpening.advance().outcome)
            val localReady = checkNotNull(localOpening.readyOrNull())
            assertEquals(1L, localReady.database.connection().scalarLong(
                "SELECT count(*) FROM drafts WHERE normalized='MIXED' AND hex(payload)='000102FF'",
            ))
            assertEquals(1L, localReady.database.connection().scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='drafts_normalized'",
            ))
            assertEquals(1L, localReady.database.connection().scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='drafts_touch'",
            ))
        } finally {
            localOpening.close()
            localHttp.close()
        }

        val target = generation(2).copy(
            tables = generation(2).tables + GenerationTable(
                "business",
                "tags",
                listOf("id"),
                listOf(
                    GenerationColumn("id", "uuid", false),
                    GenerationColumn("label", "text", false),
                ),
                emptyList(),
            ),
        ).withComputedFingerprint()
        val targetConfig = config().copy(
            generationId = 2,
            syncTables = listOf(SyncTable("notes", "id"), SyncTable("tags", "id")),
        )
        val targetHttp = generationHttp(
            target,
            snapshotRows = listOf(
                SnapshotRow(
                    schema = "business",
                    table = "tags",
                    key = mapOf("id" to "99999999-9999-4999-8999-999999999999"),
                    rowVersion = 1,
                    payload = buildJsonObject {
                        put("id", "99999999-9999-4999-8999-999999999999")
                        put("label", "preserved")
                    },
                ),
            ),
        )
        val generationOpening = database(
            path,
            4,
            generationMigrationSql = listOf(
                "CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL)",
            ),
            freshSchemaSql = initialSchema,
        ).openForSync(
            targetConfig,
            targetHttp,
            upgradePlan = OversqliteUpgradePlan("notes-tags-g1-g2", 1, 2, 3, 4, setOf("drafts")),
        )
        try {
            assertNull(generationOpening.readyOrNull())
            assertEquals(OversqliteOpeningOutcome.READY, generationOpening.advance().outcome)
            val ready = checkNotNull(generationOpening.readyOrNull())
            assertEquals(1L, ready.database.connection().scalarLong(
                "SELECT count(*) FROM drafts WHERE normalized='MIXED' AND hex(payload)='000102FF'",
            ))
            assertEquals(2L, ready.database.connection().scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE name IN ('drafts_normalized','drafts_touch')",
            ))
            assertEquals(1L, ready.database.connection().scalarLong(
                "SELECT count(*) FROM tags WHERE label='preserved'",
            ))
        } finally {
            generationOpening.close()
            targetHttp.close()
        }
    }

    @Test
    fun schemaevolutionResumeAfterU4DoesNotReapplyNewTableAbsenceCheck() = runTest {
        val path = createTempDirectory("sqlitenow-schemaevolution-post-u4-resume")
            .resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initialDb = database(path, 2)
        val initial = initialDb.openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initial.close()
        initialHttp.close()

        val target = generation(2).copy(
            tables = generation(2).tables + GenerationTable(
                "business",
                "tags",
                listOf("id"),
                listOf(GenerationColumn("id", "uuid", false)),
                emptyList(),
            ),
        ).withComputedFingerprint()
        val targetConfig = config().copy(
            generationId = 2,
            syncTables = listOf(SyncTable("notes", "id"), SyncTable("tags", "id")),
        )
        val targetHttp = generationHttp(target)
        val plan = OversqliteUpgradePlan("add-tags-resume", 1, 2, 2, 3, emptySet())
        val first = database(
            path,
            3,
            generationMigrationSql = listOf("CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL)"),
        ).openForSync(targetConfig, targetHttp, upgradePlan = plan)
        try {
            openingDurableCommitForTest = { cut, _ ->
                if (cut == "before_u5") {
                    openingDurableCommitForTest = null
                    error("injected failure before U5")
                }
            }
            assertFailsWith<IllegalStateException> { first.advance() }
            assertEquals("local_migrated", externalText(path, "SELECT phase FROM _sync_opening_state"))
            assertEquals(OversqliteOpeningOutcome.UPGRADE_INTEGRITY_FAILED, first.status().outcome)
            assertEquals(OversqliteOpeningNextAction.INSPECT_INTEGRITY, first.status().nextAction)
            assertEquals(1L, externalScalarLong(path, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='tags'"))
        } finally {
            openingDurableCommitForTest = null
            first.close()
        }

        val resumed = database(
            path,
            3,
            generationMigrationSql = listOf("CREATE TABLE tags(id TEXT PRIMARY KEY NOT NULL)"),
        ).openForSync(targetConfig, targetHttp, upgradePlan = plan)
        try {
            assertEquals("local_migrated", resumed.status().phase)
            assertEquals(OversqliteOpeningOutcome.READY, resumed.advance().outcome)
            assertNotNull(resumed.readyOrNull())
        } finally {
            resumed.close()
            targetHttp.close()
        }
    }

    @Test
    fun explicitPlanAndGeneratedTargetMustMatchBeforeMutation() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-plan-mismatch").resolve("database.sqlite3").toString()
        val http = noNetworkHttpClient()
        val initial = database(path, version = 2)
        initial.openForSync(config(), http).also { opening -> opening.advance(); opening.close() }
        val target = database(path, version = 3)

        assertFailsWith<IllegalArgumentException> {
            target.openForSync(
                config(),
                http,
                upgradePlan = OversqliteUpgradePlan("wrong", 1, 1, 2, 4, emptySet()),
            )
        }
        assertFalse(target.isOpen())
        val verify = database(path, version = 2)
        val verifyOpening = verify.openForSync(config(), http)
        try {
            assertEquals(2, verifyOpening.status().installedLocalVersion)
        } finally {
            verifyOpening.close()
            http.close()
        }
    }

    @Test
    fun schemaevolutionMissingDirectEdgeAndWrongTargetRejectBeforeU0WithoutMutation() = runTest {
        data class Scenario(
            val name: String,
            val targetVersion: Int,
            val targetGeneration: Long,
            val plan: OversqliteUpgradePlan?,
        )
        val scenarios = listOf(
            Scenario("missing direct edge", 3, 2, null),
            Scenario(
                "plan targets wrong generation",
                3,
                2,
                OversqliteUpgradePlan("wrong-target", 1, 3, 2, 3, emptySet()),
            ),
        )

        scenarios.forEach { scenario ->
            val path = createTempDirectory("sqlitenow-schemaevolution-plan-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            val seedHttp = generationHttp(generation(1), includeConnect = true)
            val seed = database(path, 2).openForSync(config(), seedHttp)
            seed.advance()
            seed.readyOrNull()!!.client.attach("user-a").getOrThrow()
            seed.close()
            seedHttp.close()

            val before = preU0Census(path)
            val requests = AtomicInteger()
            val http = generationHttp(generation(scenario.targetGeneration), requestCount = requests)
            val failure = assertFailsWith<OversqliteUpgradePlanMismatchException>(scenario.name) {
                database(path, scenario.targetVersion).openForSync(
                    config().copy(generationId = scenario.targetGeneration),
                    http,
                    upgradePlan = scenario.plan,
                )
            }
            assertTrue(failure.message.orEmpty().contains("plan") || failure.message.orEmpty().contains("target"))
            assertEquals(0, requests.get(), scenario.name)
            assertEquals(before, preU0Census(path), scenario.name)
            http.close()
        }
    }

    @Test
    fun schemaevolutionMissingOrCorruptRetainedAuthorityRequiresRestoreBeforeU0() = runTest {
        data class Scenario(val name: String, val mutation: String)
        val scenarios = listOf(
            Scenario("missing retained descriptor", "DELETE FROM _sync_admitted_contracts WHERE generation_id=1"),
            Scenario("corrupt retained descriptor", "UPDATE _sync_admitted_contracts SET descriptor='{}' WHERE generation_id=1"),
        )

        scenarios.forEach { scenario ->
            val path = createTempDirectory("sqlitenow-schemaevolution-authority-${scenario.name.replace(' ', '-')}")
                .resolve("database.sqlite3").toString()
            val seedHttp = generationHttp(generation(1), includeConnect = true)
            val seed = database(path, 2).openForSync(config(), seedHttp)
            seed.advance()
            seed.readyOrNull()!!.client.attach("user-a").getOrThrow()
            seed.close()
            seedHttp.close()
            externalExecute(path, scenario.mutation)

            val before = preU0Census(path)
            val requests = AtomicInteger()
            val http = generationHttp(generation(2), requestCount = requests)
            val failure = assertFailsWith<OversqliteUpgradePreconditionException>(scenario.name) {
                database(path, 3).openForSync(
                    config().copy(generationId = 2),
                    http,
                    upgradePlan = OversqliteUpgradePlan("retained-authority-g1-g2", 1, 2, 2, 3, emptySet()),
                )
            }
            assertTrue(failure.message.orEmpty().contains("restore"), scenario.name)
            assertEquals(0, requests.get(), scenario.name)
            http.close()
            assertEquals(before, preU0Census(path), scenario.name)
        }
    }

    @Test
    fun definitiveOldIntentRejectionRequiresExplicitRetryOrDiscard() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-rejection").resolve("database.sqlite3").toString()
        val initialHttp = generationHttp(generation(1), includeConnect = true)
        val initialDatabase = database(path, version = 2)
        val initial = initialDatabase.openForSync(config(), initialHttp)
        initial.advance()
        initial.readyOrNull()!!.client.attach("user-a").getOrThrow()
        initialDatabase.connection().execSQL(
            "INSERT INTO notes(id,title) VALUES('44444444-4444-4444-8444-444444444444','rejected')",
        )
        initial.close()
        initialHttp.close()

        val rejectedCommits = AtomicInteger(2)
        val requestCount = AtomicInteger()
        val targetHttp = generationHttp(
            generation = generation(2),
            requestCount = requestCount,
            rejectPushCommits = rejectedCommits,
        )
        val targetDatabase = database(path, version = 2)
        val opening = targetDatabase.openForSync(
            config = config().copy(generationId = 2),
            httpClient = targetHttp,
            upgradePlan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, emptySet()),
        )
        try {
            val rejected = opening.advance()
            assertEquals(OversqliteOpeningOutcome.NEEDS_RESOLUTION, rejected.outcome)
            assertEquals(OversqliteOpeningNextAction.RESOLVE_REJECTION, rejected.nextAction)
            val first = assertNotNull(opening.pendingRejection())
            assertEquals("push_effect_coverage_incomplete", first.rejectionCode)
            assertEquals(1L, opening.status().pendingFrozenCount)

            val afterRetry = opening.retryRejected(first)
            assertEquals(OversqliteOpeningNextAction.ADVANCE, afterRetry.nextAction)
            assertNull(opening.pendingRejection())
            assertEquals(0L, opening.status().pendingDirtyCount)
            assertEquals(1L, opening.status().pendingFrozenCount)

            assertEquals(OversqliteOpeningOutcome.NEEDS_RESOLUTION, opening.advance().outcome)
            val second = assertNotNull(opening.pendingRejection())
            assertFailsWith<IllegalArgumentException> { opening.discardRejected(first) }
            val requestsBeforeDiscard = requestCount.get()
            val afterDiscard = opening.discardRejected(second)
            assertEquals(OversqliteOpeningNextAction.ADVANCE, afterDiscard.nextAction)
            assertEquals(requestsBeforeDiscard, requestCount.get(), "discard must be local-only")
            assertEquals(0L, opening.status().pendingDirtyCount)
            assertEquals(0L, opening.status().pendingFrozenCount)
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
        } finally {
            opening.close()
            targetHttp.close()
        }
    }

    @Test
    fun recordingAndOwnerDecisionsRequireCurrentPreservationWitnesses() = runTest {
        val root = createTempDirectory("coordinatedopening-preservation-barriers")
        val seedPath = root.resolve("seed.sqlite3").toString()
        val seedHttp = generationHttp(generation(1), includeConnect = true)
        val seedDb = database(seedPath, version = 2)
        val seed = seedDb.openForSync(config(), seedHttp)
        seed.advance()
        seed.readyOrNull()!!.client.attach("user-a").getOrThrow()
        seedDb.connection().execSQL("CREATE TABLE drafts(label TEXT NOT NULL)")
        seedDb.connection().execSQL("INSERT INTO drafts VALUES('preserved')")
        seedDb.connection().execSQL(
            "INSERT INTO notes(id,title) VALUES('44444444-4444-4444-8444-444444444444','preservation')",
        )
        seed.close()
        seedHttp.close()
        val plan = OversqliteUpgradePlan("notes-g1-g2", 1, 2, 2, 2, setOf("drafts"))
        val guards = preservedLocalTableGuardNames(setOf("drafts")).sorted()

        PreservationBarrierScenario.entries.forEach { scenario ->
            val path = root.resolve("${scenario.cut}.sqlite3").toString()
            Files.copy(java.nio.file.Path.of(seedPath), java.nio.file.Path.of(path))
            val remoteOutcome = AtomicInteger(1)
            val http = if (scenario.rejection) {
                generationHttp(generation(2), rejectPushCommits = remoteOutcome)
            } else {
                generationHttp(generation(2), conflictPushCommits = remoteOutcome)
            }
            val db = database(path, version = 2)
            val opening = db.openForSync(
                config().copy(generationId = 2), http, upgradePlan = plan,
            )
            try {
                if (scenario.requiresPendingState) {
                    assertEquals(OversqliteOpeningOutcome.NEEDS_RESOLUTION, opening.advance().outcome)
                }
                val selectedRejection = if (scenario.rejection && scenario.requiresPendingState) {
                    assertNotNull(opening.pendingRejection())
                } else null
                val selectedConflict = if (!scenario.rejection && scenario.requiresPendingState) {
                    assertNotNull(opening.pendingConflict().getOrThrow())
                } else null
                var cutCalls = 0
                openingPreservationCheckForTest = { observed, connection ->
                    if (observed == scenario.cut) {
                        openingPreservationCheckForTest = null
                        cutCalls++
                        guards.forEach { connection.execSQL("DROP TRIGGER \"$it\"") }
                        connection.execSQL("UPDATE drafts SET label='changed'")
                    }
                }
                val failure = when (scenario) {
                    PreservationBarrierScenario.RECORD_REJECTION,
                    PreservationBarrierScenario.RECORD_CONFLICT,
                    -> runCatching { opening.advance() }.exceptionOrNull()
                    PreservationBarrierScenario.RETRY_REJECTION ->
                        runCatching { opening.retryRejected(checkNotNull(selectedRejection)) }.exceptionOrNull()
                    PreservationBarrierScenario.DISCARD_REJECTION ->
                        runCatching { opening.discardRejected(checkNotNull(selectedRejection)) }.exceptionOrNull()
                    PreservationBarrierScenario.CLEAR_CONFLICT ->
                        opening.retryConflict(checkNotNull(selectedConflict)).exceptionOrNull()
                }
                assertTrue(
                    failure is IllegalStateException &&
                        failure.message.orEmpty().contains("preservation validation failed"),
                    "${scenario.cut}: $failure",
                )
                assertEquals(1, cutCalls, scenario.cut)
                assertEquals("preserved", externalText(path, "SELECT label FROM drafts"), scenario.cut)
                assertEquals(3L, externalScalarLong(
                    path,
                    "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name LIKE '_sqlitenow_preserve_%'",
                ), scenario.cut)
                assertEquals(1L, externalScalarLong(path, "SELECT count(*) FROM _sync_outbox_rows"), scenario.cut)
                if (selectedRejection != null) {
                    assertEquals(selectedRejection, opening.pendingRejection(), scenario.cut)
                }
                if (selectedConflict != null) {
                    assertEquals(selectedConflict, opening.pendingConflict().getOrThrow(), scenario.cut)
                }
                when (scenario) {
                    PreservationBarrierScenario.RECORD_REJECTION,
                    PreservationBarrierScenario.RECORD_CONFLICT,
                    -> Unit
                    PreservationBarrierScenario.RETRY_REJECTION ->
                        opening.retryRejected(checkNotNull(selectedRejection))
                    PreservationBarrierScenario.DISCARD_REJECTION ->
                        opening.discardRejected(checkNotNull(selectedRejection))
                    PreservationBarrierScenario.CLEAR_CONFLICT ->
                        opening.retryConflict(checkNotNull(selectedConflict)).getOrThrow()
                }
            } finally {
                openingPreservationCheckForTest = null
                opening.close()
                http.close()
            }
        }
    }

    private fun database(
        path: String,
        version: Int,
        migrationCalls: AtomicInteger = AtomicInteger(),
        failMigrationOnce: AtomicInteger? = null,
        localOnlyDraftMigration: Boolean = false,
        generationMigrationSql: List<String> = emptyList(),
        freshSchemaSql: List<String> = listOf(
            "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)",
        ),
    ): SqliteNowDatabase = SqliteNowDatabase(
        dbName = path,
        migration = object : DatabaseMigrations {
            override val targetVersion: Int = version

            override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                migrationCalls.incrementAndGet()
                if (currentVersion == -1) {
                    for (sql in freshSchemaSql) conn.execSQL(sql)
                    if (failMigrationOnce?.compareAndSet(1, 0) == true) {
                        error("injected migration failure after opening journal commit")
                    }
                    return version
                }
                if ((currentVersion == 2 && version == 3) || (currentVersion == 3 && version == 4)) {
                    if (generationMigrationSql.isNotEmpty()) {
                        for (sql in generationMigrationSql) conn.execSQL(sql)
                    } else {
                        conn.execSQL(
                            if (localOnlyDraftMigration) {
                                "ALTER TABLE drafts ADD COLUMN category TEXT NOT NULL DEFAULT ''"
                            } else {
                                "ALTER TABLE notes ADD COLUMN body TEXT NOT NULL DEFAULT ''"
                            },
                        )
                    }
                    if (failMigrationOnce?.compareAndSet(1, 0) == true) {
                        error("injected migration failure after opening journal commit")
                    }
                    return version
                }
                return currentVersion
            }
        },
        syncManaged = true,
    )

    private fun config(): OversqliteConfig = OversqliteConfig(
        generationId = 1,
        databaseId = "opening-fixture",
        serviceId = "opening-service",
        syncTables = listOf(SyncTable("notes", syncKeyColumnName = "id")),
    )

    private fun externalExecute(path: String, sql: String) {
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            connection.prepare(sql).use { statement -> statement.step() }
        }
    }

    private fun externalScalarLong(path: String, sql: String): Long =
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            connection.prepare(sql).use { statement ->
                check(statement.step()) { "scalar query returned no row" }
                statement.getLong(0)
            }
        }

    private fun externalText(path: String, sql: String): String =
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            connection.prepare(sql).use { statement ->
                check(statement.step()) { "scalar query returned no row" }
                statement.getText(0)
            }
        }

    private fun externalSchemaRows(path: String): List<String> =
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            connection.prepare("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name").use { statement ->
                buildList {
                    while (statement.step()) {
                        add(listOf(statement.getText(0), statement.getText(1), statement.getText(2), statement.getText(3)).joinToString("|"))
                    }
                }
            }
        }

    private fun preU0Census(path: String): PreU0Census {
        val schema = externalSchemaRows(path)
        val sources = externalSingleColumnRows(
            path,
            """
            SELECT quote(source_id)||'|'||quote(next_source_bundle_id)||'|'||
                   quote(replaced_by_source_id)||'|'||quote(created_at)
            FROM _sync_source_state ORDER BY source_id
            """.trimIndent(),
        )
        val journal = externalSingleColumnRows(
            path,
            """
            SELECT quote(mode)||'|'||quote(phase)||'|'||quote(outcome)||'|'||quote(revision)||'|'||
                   quote(plan_digest)||'|'||quote(migration_id)||'|'||quote(from_generation_id)||'|'||
                   quote(to_generation_id)||'|'||quote(from_local_version)||'|'||quote(to_local_version)||'|'||
                   quote(old_source_id)||'|'||quote(new_source_id)||'|'||quote(next_action)
            FROM _sync_opening_state ORDER BY singleton_key
            """.trimIndent(),
        )
        return PreU0Census(
            databaseBytes = Files.readAllBytes(java.nio.file.Path.of(path)).toList(),
            schema = schema,
            sources = sources,
            journal = journal,
        )
    }

    private fun externalSingleColumnRows(path: String, sql: String): List<String> =
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            connection.prepare(sql).use { statement ->
                buildList {
                    while (statement.step()) add(statement.getText(0))
                }
            }
        }

    private fun noNetworkHttpClient(requestCount: AtomicInteger? = null): HttpClient = HttpClient(MockEngine) {
        engine {
            addHandler {
                requestCount?.incrementAndGet()
                error("openForSync local modes must not use HTTP")
            }
        }
    }

    private fun generation(id: Long): SyncGeneration {
        val provisional = SyncGeneration(
            id = id.toString(),
            fingerprint = "0".repeat(64),
            tables = listOf(
                GenerationTable(
                    schema = "business",
                    table = "notes",
                    syncKeyColumns = listOf("id"),
                    columns = listOf(
                        GenerationColumn("id", "uuid", false),
                        GenerationColumn("title", "text", false),
                    ),
                    foreignKeys = emptyList(),
                ),
            ),
        )
        return provisional.copy(fingerprint = provisional.computeFingerprint())
    }

    private fun SyncGeneration.withComputedFingerprint(): SyncGeneration = copy(
        fingerprint = copy(fingerprint = "0".repeat(64)).computeFingerprint(),
    )

    private fun generationHttp(
        generation: SyncGeneration,
        installedGeneration: SyncGeneration = this@OversqliteOpeningJvmTest.generation(1),
        includeConnect: Boolean = false,
        requestCount: AtomicInteger = AtomicInteger(),
        requestPaths: MutableList<String> = mutableListOf(),
        snapshotRows: List<SnapshotRow> = emptyList(),
        expireFirstSnapshot: Boolean = false,
        snapshotRequestBodies: MutableList<String> = mutableListOf(),
        rejectPushCommits: AtomicInteger? = null,
        conflictPushCommits: AtomicInteger? = null,
        onGenerationDiscovery: suspend () -> Unit = {},
        previewHold: PreviewHold? = null,
        lostResponsePoint: OpeningLostResponsePoint? = null,
        lostResponseCount: AtomicInteger = AtomicInteger(),
    ): HttpClient {
        val discovery = Json.encodeToString(GenerationDiscovery("v1", generation, testWireLimits(), false))
        val installedDiscovery = Json.encodeToString(
            GenerationDiscovery("v1", installedGeneration, testWireLimits(), false),
        )
        val snapshotCreationCount = AtomicInteger()
        val expiredFirstSnapshot = AtomicInteger()
        val generationDiscoveryCallbackCount = AtomicInteger()
        var conflictRow: kotlinx.serialization.json.JsonObject? = null
        val snapshotBundleRows = snapshotRows.map { row ->
            BundleRow(row.schema, row.table, row.key, "INSERT", row.rowVersion, row.payload)
        }
        val snapshotByteCount = OversqliteCompactCodec.snapshotLogicalByteCount(generation, snapshotBundleRows)
        return HttpClient(MockEngine { request ->
            requestCount.incrementAndGet()
            requestPaths += request.url.encodedPath
            if (generation.id != "1" && request.url.encodedPath == "/sync/generations/1") {
                return@MockEngine respond(
                    installedDiscovery,
                    headers = headersOf(HttpHeaders.ContentType, "application/json"),
                )
            }
            when (request.url.encodedPath) {
                "/sync/generations/${generation.id}" -> {
                    val previewIndex = generationDiscoveryCallbackCount.getAndIncrement() + 1
                    if (previewIndex == 1) onGenerationDiscovery()
                    if (previewIndex == 1 && previewHold != null) {
                        previewHold.entered.complete(Unit)
                        try {
                            awaitCancellation()
                        } finally {
                            withContext(NonCancellable) {
                                previewHold.cancellationObserved.complete(Unit)
                                previewHold.releaseCleanup.await()
                                previewHold.cleanupCalls.incrementAndGet()
                                previewHold.cleanupComplete.complete(Unit)
                            }
                        }
                    }
                    val selectedPreview = when (lostResponsePoint) {
                        OpeningLostResponsePoint.INITIAL_PREVIEW -> 1
                        OpeningLostResponsePoint.TARGET_PREVIEW -> 2
                        else -> -1
                    }
                    if (previewIndex == selectedPreview && lostResponseCount.compareAndSet(0, 1)) {
                        error("injected lost target preview response $previewIndex")
                    }
                    respond(
                        discovery,
                        headers = headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                }
                "/sync/connect" -> {
                    respond(
                        Json.encodeToString(
                            ConnectResponse(
                                resolution = if (includeConnect) "initialize_empty" else "remote_authoritative",
                            ),
                        ),
                        headers = headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                }
                "/sync/push" -> {
                    val body = Json.parseToJsonElement(request.bodyText()).jsonObject
                    val expanded = OversqliteCompactCodec.decodeBundleRows(generation, body.getValue("rows"))
                    conflictRow = expanded.single().let { row -> buildJsonObject {
                        put("schema", row.schema)
                        put("table", row.table)
                        put("key", buildJsonObject { row.key.forEach { (key, value) -> put(key, value) } })
                        put("op", row.op)
                        put("base_row_version", row.rowVersion)
                        put("payload", checkNotNull(row.payload))
                    } }
                    if (conflictPushCommits?.getAndDecrement()?.let { it > 0 } == true) {
                        val row = checkNotNull(conflictRow)
                        respond(
                            buildJsonObject {
                                put("error", "push_conflict")
                                put("message", "fixture conflict")
                                put("conflict", buildJsonObject {
                                    put("schema", row.getValue("schema"))
                                    put("table", row.getValue("table"))
                                    put("key", row.getValue("key"))
                                    put("op", row.getValue("op"))
                                    put("base_row_version", row.getValue("base_row_version"))
                                    put("server_row_version", 1)
                                    put("server_row_deleted", false)
                                    put("server_row", row.getValue("payload"))
                                })
                            }.toString(),
                            status = HttpStatusCode.Conflict,
                            headers = headersOf(HttpHeaders.ContentType, "application/json"),
                        )
                    } else {
                        check(rejectPushCommits?.getAndDecrement()?.let { it > 0 } == true)
                        respond(
                            """{"error":"push_effect_coverage_incomplete","message":"fixture definitive rejection","remediation":"correct_server_write_logic","retryable":false}""",
                            status = HttpStatusCode.UnprocessableEntity,
                            headers = headersOf(HttpHeaders.ContentType, "application/json"),
                        )
                    }
                }
                "/sync/snapshot-sessions" -> {
                    snapshotRequestBodies += request.bodyText()
                    val sequence = snapshotCreationCount.incrementAndGet()
                    if (lostResponsePoint == OpeningLostResponsePoint.REPLACEMENT &&
                        lostResponseCount.compareAndSet(0, 1)
                    ) {
                        error("injected lost source replacement response")
                    }
                    respond(
                        Json.encodeToString(
                            SnapshotSession(
                                snapshotId = "snapshot-g${generation.id}-$sequence",
                                snapshotBundleSeq = if (snapshotRows.isEmpty()) 0 else 1,
                                rowCount = snapshotRows.size.toLong(),
                                logicalByteCount = snapshotByteCount,
                                expiresAt = "2099-01-01T00:00:00Z",
                            ),
                        ),
                        headers = headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                }
                "/sync/snapshot-sessions/snapshot-g${generation.id}-1",
                "/sync/snapshot-sessions/snapshot-g${generation.id}-2" -> when (request.method.value) {
                    "GET" -> respond(
                        content = if (expireFirstSnapshot && request.url.encodedPath.endsWith("-1") &&
                            expiredFirstSnapshot.compareAndSet(0, 1)
                        ) {
                            """{"error":"snapshot_session_expired","message":"target snapshot expired"}"""
                        } else {
                            buildJsonObject {
                                put("rows", OversqliteCompactCodec.encodeBundleRows(
                                    generation, snapshotBundleRows, firstOrdinal = 1, snapshot = true,
                                ))
                                put("next_row_ordinal", snapshotRows.size)
                                put("has_more", false)
                                put("logical_byte_count", snapshotByteCount)
                            }.toString()
                        },
                        status = if (expireFirstSnapshot && request.url.encodedPath.endsWith("-1") &&
                            expiredFirstSnapshot.get() == 1
                        ) HttpStatusCode.Gone else HttpStatusCode.OK,
                        headers = headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                    "DELETE" -> respond("", status = HttpStatusCode.NoContent)
                    else -> error("unexpected snapshot method ${request.method.value}")
                }
                else -> error("unexpected generation test request ${request.method.value} ${request.url.encodedPath}")
            }
        }) {
            install(ContentNegotiation) { json() }
            defaultRequest { url("https://sync.test/") }
        }
    }
}
