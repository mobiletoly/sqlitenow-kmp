package dev.goquick.sqlitenow.core

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.sqlite.use
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async

@OptIn(SqliteNowInternalApi::class)
class SqliteNowUnpublishedSessionTest {
    @Test
    fun preservesFormerJvmConstructorDescriptors() {
        val type = SqliteNowDatabase::class.java
        type.getDeclaredConstructor(
            String::class.java,
            DatabaseMigrations::class.java,
            SqliteConnectionProvider::class.java,
        )
        type.getDeclaredConstructor(
            String::class.java,
            DatabaseMigrations::class.java,
            Boolean::class.javaPrimitiveType,
            SqliteConnectionProvider::class.java,
        )
    }

    @Test
    fun unsupportedProviderFailsBeforeOrdinaryOpenSideEffects() = runTest {
        val openCalls = AtomicInteger()
        val path = createTempDirectory("sqlitenow-coordinatedopening-provider-capability")
            .resolve("database.sqlite3")
            .toString()
        val provider = SqliteConnectionProvider { _, _, _ ->
            openCalls.incrementAndGet()
            error("ordinary open must not be called")
        }
        val database = SqliteNowDatabase(path, TestMigrations, provider)
        val session = database.reserveUnpublishedSession()

        val failure = assertFailsWith<UnsupportedOperationException> { session.acquire() }
        assertTrue(failure.message.orEmpty().contains("coordinated-opening inspection"))
        assertEquals(0, openCalls.get())
        assertFalse(File(path).exists())
        session.close()
    }

    @Test
    fun reservationKeepsConnectionUnpublishedUntilExplicitPublication() = runTest {
        val database = testDatabase("publication")
        val session = database.reserveUnpublishedSession()

        try {
            assertEquals(2, session.migrationTargetVersion)
            assertFalse(database.isOpen())
            assertFailsWith<IllegalStateException> { database.connection() }
            assertFailsWith<IllegalStateException> { database.open() }
            assertFailsWith<IllegalStateException> { database.reserveUnpublishedSession() }

            session.acquire()
            assertFalse(database.isOpen())
            assertTrue(session.preparationRequired)
            assertFalse(session.inspectionAvailable)
            session.prepare()
            session.withConnection { connection ->
                assertEquals(0, connection.readVersionForTest())
                assertFalse(connection.hasTableForTest("notes"))
            }

            session.migrateAndValidate { connection ->
                assertEquals(2, connection.readVersionForTest())
                assertTrue(connection.hasTableForTest("notes"))
            }
            assertFalse(database.isOpen())
            assertFailsWith<IllegalStateException> { database.connection() }

            session.publish()
            assertTrue(database.isOpen())
            assertEquals(2, database.connection().readVersionForTest())
        } finally {
            database.close()
        }
    }

    @Test
    fun failedFinalValidationRollsBackAndCloseReleasesObjectReservation() = runTest {
        val database = testDatabase("rollback")
        val session = database.reserveUnpublishedSession()
        session.acquire()
        session.prepare()

        assertFailsWith<IllegalStateException> {
            session.migrateAndValidate {
                error("final validation failed")
            }
        }
        session.withConnection { connection ->
            assertEquals(0, connection.readVersionForTest())
            assertFalse(connection.hasTableForTest("notes"))
        }
        session.close()

        database.open()
        try {
            assertTrue(database.isOpen())
            assertEquals(2, database.connection().readVersionForTest())
        } finally {
            database.close()
        }
    }

    @Test
    fun coordinatedPolicyAppliesToNumberedMigrationStatementsAndRollsBack() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-policy").resolve("database.sqlite3")
        val database = SqliteNowDatabase(
            dbName = path.toString(),
            migration = object : DatabaseMigrations {
                override val targetVersion: Int = 1
                override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                    conn.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY)")
                    conn.execSQL("DELETE FROM _sync_opening_state")
                    return targetVersion
                }
            },
        )
        val session = database.reserveUnpublishedSession()
        session.acquire()
        session.prepare()

        val failure = assertFailsWith<SqliteNowCoordinatedMigrationException> {
            session.migrateAndValidate(
                migrationSqlPolicy = SqliteNowCoordinatedMigrationSqlPolicy(setOf("_sync_opening_state")),
            )
        }
        assertEquals("coordinated_migration_protected_object_forbidden", failure.diagnosticCode)
        session.withConnection { connection ->
            assertFalse(connection.hasTableForTest("notes"))
            assertEquals(0, connection.readVersionForTest())
        }
        session.close()
    }

    @Test
    fun migrationCannotReentrantlyCloseDatabaseAndRollsBack() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-reentrant-close")
            .resolve("database.sqlite3")
            .toString()
        lateinit var database: SqliteNowDatabase
        database = SqliteNowDatabase(
            dbName = path,
            migration = object : DatabaseMigrations {
                override val targetVersion: Int = 1

                override suspend fun applyMigration(
                    conn: SafeSQLiteConnection,
                    currentVersion: Int,
                ): Int {
                    conn.execSQL("CREATE TABLE reentrant_close_probe(id INTEGER PRIMARY KEY)")
                    database.close()
                    return targetVersion
                }
            },
        )
        val session = database.reserveUnpublishedSession()
        session.acquire()
        if (session.preparationRequired) session.prepare()

        val failure = assertFailsWith<SqliteNowCoordinatedMigrationException> {
            session.migrateAndValidate()
        }

        assertEquals("coordinated_migration_reentrant_close_forbidden", failure.diagnosticCode)
        session.withConnection { connection ->
            assertFalse(connection.hasTableForTest("reentrant_close_probe"))
            assertEquals(0, connection.readVersionForTest())
        }
        session.close()
    }

    @Test
    fun databaseCloseJoinsAndClosesReservedSession() = runTest {
        val database = testDatabase("database-close")
        val session = database.reserveUnpublishedSession()
        session.acquire()

        database.close()

        assertFalse(database.isOpen())
        assertFailsWith<IllegalStateException> { session.withConnection {} }
        database.open()
        database.close()
    }

    @Test
    fun coordinatedCloseRevokesDatabaseAndCapturedConnectionBeforeOwnerCleanupWait() = runTest {
        val database = testDatabase("coordinated-immediate-revocation")
        val session = database.reserveUnpublishedSession()
        session.acquire()
        if (session.preparationRequired) session.prepare()
        session.migrateAndValidate()
        val closeEntered = CompletableDeferred<Unit>()
        val releaseClose = CompletableDeferred<Unit>()
        session.bindPublishedCloseHandler {
            closeEntered.complete(Unit)
            releaseClose.await()
            session.close()
        }
        session.publish()
        val captured = database.connection()

        val closing = async(start = CoroutineStart.UNDISPATCHED) { database.close() }
        closeEntered.await()

        assertFalse(database.isOpen())
        assertFailsWith<IllegalStateException> { database.connection() }
        assertFailsWith<IllegalStateException> { captured.execSQL("SELECT 1") }

        releaseClose.complete(Unit)
        closing.await()
    }

    @Test
    fun directConnectionCloseImmediatelyMarksDatabaseClosed() = runTest {
        val database = testDatabase("direct-connection-close")
        database.open()

        database.connection().close()

        assertFalse(database.isOpen())
        assertFailsWith<IllegalStateException> { database.connection() }
        database.close()
    }

    @Test
    fun overlappingProviderCloseCallersObserveTheSameFailure() = runTest {
        val hook = BlockingFailOnceCloseHook()
        val path = createTempDirectory("sqlitenow-coordinatedopening-provider-close-attempt")
            .resolve("database.sqlite3")
            .toString()
        val inspection = BundledSqliteConnectionProvider.openConnection(
            path,
            debug = false,
            config = SqliteConnectionConfig(executionContextHook = hook),
        )
        val session = DefaultSqliteNowCoordinatedProviderSession(
            inspectionConnection = inspection,
            preparationRequired = false,
            prepare = { error("replacement must not be opened") },
        )

        val first = async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { session.close() }.exceptionOrNull()
        }
        hook.closeEntered.await()
        val second = async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { session.close() }.exceptionOrNull()
        }
        hook.releaseClose.complete(Unit)

        val firstFailure = first.await()
        assertTrue(firstFailure is IllegalStateException)
        assertTrue(firstFailure.message.orEmpty().contains("COORDINATED_OPENING_PROVIDER_CLOSE_FAILURE"))
        assertSame(firstFailure, second.await())
        session.close()
    }

    @Test
    fun failedPreparationEntersCleanupOnlyWithoutOpeningReplacement() = runTest {
        val hook = FailOnceCloseHook().also { it.failNextClose = true }
        val path = createTempDirectory("sqlitenow-coordinatedopening-provider-cleanup-only")
            .resolve("database.sqlite3")
            .toString()
        val inspection = BundledSqliteConnectionProvider.openConnection(
            path,
            debug = false,
            config = SqliteConnectionConfig(executionContextHook = hook),
        )
        val prepareCalls = AtomicInteger()
        val session = DefaultSqliteNowCoordinatedProviderSession(
            inspectionConnection = inspection,
            preparationRequired = false,
            prepare = {
                prepareCalls.incrementAndGet()
                BundledSqliteConnectionProvider.openConnection(path, debug = false)
            },
        )

        assertFailsWith<IllegalStateException> { session.prepareConnection() }
        assertEquals(0, prepareCalls.get())
        assertFailsWith<IllegalStateException> { session.prepareConnection() }
        assertEquals(0, prepareCalls.get())
        session.close()
    }

    @Test
    fun syncManagedOrdinaryOpenFailsBeforePreInitOrMigration() = runTest {
        var preInitCalled = false
        val path = createTempDirectory("sqlitenow-coordinatedopening-sync-managed").resolve("database.sqlite3")
        val database = SqliteNowDatabase(
            dbName = path.toString(),
            migration = TestMigrations,
            connectionProvider = BundledSqliteConnectionProvider,
            syncManaged = true,
        )

        assertFailsWith<SqliteNowSyncOpenRequiredException> {
            database.open { preInitCalled = true }
        }
        assertFalse(preInitCalled)
        assertFalse(database.isOpen())
    }

    @Test
    fun retainedSyncIdentityBlocksUnawareOrdinaryOpen() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-retained").resolve("database.sqlite3").toString()
        val seedConnection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
        try {
            val connection = seedConnection
            connection.execSQL("CREATE TABLE _sync_contract_identity(singleton_key INTEGER PRIMARY KEY)")
            connection.execSQL("CREATE TABLE unrelated(value TEXT)")
        } finally {
            seedConnection.close()
        }
        val database = SqliteNowDatabase(
            dbName = path,
            migration = TestMigrations,
            connectionProvider = BundledSqliteConnectionProvider,
        )

        assertFailsWith<SqliteNowSyncOpenRequiredException> { database.open() }
        val verifyConnection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
        try {
            val connection = verifyConnection
            assertTrue(connection.hasTableForTest("unrelated"))
            assertFalse(connection.hasTableForTest("notes"))
        } finally {
            verifyConnection.close()
        }
    }

    @Test
    fun reservedSyncMarkersBlockOrdinaryOpenBeforePreInitOrMigration() = runTest {
        data class Scenario(val name: String, val markerSql: String)
        val scenarios = listOf(
            Scenario("table", "CREATE TABLE _sync_opening_state(value INTEGER)"),
            Scenario(
                "view",
                "CREATE VIEW _sync_contract_identity AS SELECT value FROM business_seed",
            ),
            Scenario(
                "index",
                "CREATE INDEX _sync_admitted_contracts ON business_seed(value)",
            ),
        )

        scenarios.forEach { scenario ->
            val path = createTempDirectory("sqlitenow-coordinatedopening-marker-${scenario.name}")
                .resolve("database.sqlite3")
                .toString()
            val connection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
            try {
                connection.execSQL("CREATE TABLE business_seed(value INTEGER)")
                connection.execSQL(scenario.markerSql)
            } finally {
                connection.close()
            }
            var preInitCalled = false
            var migrationCalled = false
            val database = SqliteNowDatabase(
                path,
                object : DatabaseMigrations {
                    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                        migrationCalled = true
                        error("migration must not run for ${scenario.name} marker")
                    }
                },
            )

            assertFailsWith<SqliteNowSyncOpenRequiredException>(scenario.name) {
                database.open { preInitCalled = true }
            }
            assertFalse(preInitCalled, scenario.name)
            assertFalse(migrationCalled, scenario.name)
            assertFalse(database.isOpen(), scenario.name)
        }
    }

    @Test
    fun canonicalLifecycleLeaseRejectsSecondObjectUntilPublishedOwnerCloses() = runTest {
        val directory = createTempDirectory("sqlitenow-coordinatedopening-lifecycle")
        val canonicalPath = directory.resolve("database.sqlite3").toString()
        val aliasPath = directory.resolve(".").resolve("database.sqlite3").toString()
        val first = SqliteNowDatabase(canonicalPath, TestMigrations)
        val second = SqliteNowDatabase(aliasPath, TestMigrations)
        val firstSession = first.reserveUnpublishedSession()
        val secondSession = second.reserveUnpublishedSession()

        firstSession.acquire()
        val busyBeforePublish = assertFailsWith<SqliteNowLifecycleBusyException> {
            secondSession.acquire()
        }
        assertEquals(File(canonicalPath).canonicalPath, busyBeforePublish.databaseIdentity)

        firstSession.prepare()
        firstSession.migrateAndValidate()
        firstSession.publish()
        assertFailsWith<SqliteNowLifecycleBusyException> { secondSession.acquire() }

        first.close()
        secondSession.acquire()
        secondSession.close()
        second.close()
    }

    @Test
    fun cleanupFailureRetainsLifecycleOwnershipUntilUnpublishedAndPublishedRetry() = runTest {
        listOf(false, true).forEach { published ->
            val path = createTempDirectory("sqlitenow-coordinatedopening-close-retry").resolve("database.sqlite3")
            val hook = FailOnceCloseHook()
            val provider = FaultingCoordinatedProvider(hook, inspection = !published)
            val owner = SqliteNowDatabase(path.toString(), TestMigrations, provider)
            val session = owner.reserveUnpublishedSession()
            session.acquire()
            if (published) {
                session.prepare()
                session.publish()
            }

            hook.failNextClose = true
            val failure = assertFailsWith<IllegalStateException> { owner.close() }
            assertEquals("COORDINATED_OPENING_CLOSE_FAILURE", failure.message)

            val contender = SqliteNowDatabase(path.toString(), TestMigrations, provider)
            val contenderSession = contender.reserveUnpublishedSession()
            assertFailsWith<SqliteNowLifecycleBusyException> { contenderSession.acquire() }

            owner.close()
            contenderSession.acquire()
            contenderSession.close()
            contender.close()
        }
    }

    @Test
    fun rawDriverCloseFailureIsRetriedBeforeLifecycleRelease() = runTest {
        val path = createTempDirectory("sqlitenow-coordinatedopening-raw-close-retry")
            .resolve("database.sqlite3")
            .toString()
        val database = SqliteNowDatabase(path, TestMigrations)
        val session = database.reserveUnpublishedSession()
        session.acquire()
        if (session.preparationRequired) session.prepare()
        val closeCalls = AtomicInteger()
        session.withConnection { connection ->
            connection.closeRawDriverForTest = {
                if (closeCalls.incrementAndGet() == 1) {
                    throw IllegalStateException("COORDINATED_OPENING_RAW_CLOSE_FAILURE")
                }
                connection.ref.close()
            }
        }

        val failure = assertFailsWith<IllegalStateException> { session.close() }
        assertEquals("COORDINATED_OPENING_RAW_CLOSE_FAILURE", failure.message)

        val contender = SqliteNowDatabase(path, TestMigrations)
        val contenderSession = contender.reserveUnpublishedSession()
        assertFailsWith<SqliteNowLifecycleBusyException> { contenderSession.acquire() }

        session.close()
        assertEquals(2, closeCalls.get())
        contenderSession.acquire()
        contenderSession.close()
    }

    @Test
    fun processContentionAndDeathReleaseTheSidecarLease() = runTest {
        val directory = createTempDirectory("sqlitenow-coordinatedopening-process-lifecycle")
        val path = directory.resolve("database.sqlite3").toString()
        val owner = SqliteNowDatabase(path, TestMigrations)
        val ownerSession = owner.reserveUnpublishedSession()
        ownerSession.acquire()

        val probe = lifecycleHelperProcess(path, "probe").start()
        assertTrue(probe.waitFor(10, TimeUnit.SECONDS))
        assertEquals(0, probe.exitValue(), probe.errorStream.bufferedReader().readText())
        assertEquals("BUSY", probe.inputStream.bufferedReader().readText().trim())
        ownerSession.close()

        val killedOwner = lifecycleHelperProcess(path, "hold").start()
        assertEquals("ACQUIRED", killedOwner.inputStream.bufferedReader().readLine())
        killedOwner.destroyForcibly()
        assertTrue(killedOwner.waitFor(10, TimeUnit.SECONDS))

        val reopened = SqliteNowDatabase(path, TestMigrations)
        val reopenedSession = reopened.reserveUnpublishedSession()
        reopenedSession.acquire()
        reopenedSession.close()
    }

    private fun lifecycleHelperProcess(path: String, mode: String): ProcessBuilder {
        val classpath = sequenceOf(
            SqliteNowLifecycleLeaseProcessHelper::class.java,
            SqliteNowDatabase::class.java,
            SqliteNowLifecycleLease::class.java,
            SQLiteConnection::class.java,
            BundledSQLiteDriver::class.java,
            kotlin.Unit::class.java,
            kotlinx.coroutines.Job::class.java,
        )
            .mapNotNull { type -> type.protectionDomain?.codeSource?.location }
            .mapNotNull { url -> runCatching { File(url.toURI()).path }.getOrNull() }
            .distinct()
            .joinToString(File.pathSeparator)
        check(classpath.isNotBlank()) { "Cannot resolve JVM test runtime classpath" }
        val java = File(System.getProperty("java.home"), "bin/java").path
        return ProcessBuilder(
            java,
            "-cp",
            classpath,
            SqliteNowLifecycleLeaseProcessHelper::class.java.name,
            path,
            mode,
        )
    }

    private fun testDatabase(suffix: String): SqliteNowDatabase {
        val path = createTempDirectory("sqlitenow-coordinatedopening-$suffix").resolve("database.sqlite3")
        return SqliteNowDatabase(
            dbName = path.toString(),
            migration = TestMigrations,
            connectionProvider = BundledSqliteConnectionProvider,
        )
    }

    private object TestMigrations : DatabaseMigrations {
        override val targetVersion: Int = 2

        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                conn.execSQL("CREATE TABLE notes (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
                return targetVersion
            }
            return currentVersion
        }
    }
}

@OptIn(SqliteNowInternalApi::class)
private class FaultingCoordinatedProvider(
    private val hook: FailOnceCloseHook,
    private val inspection: Boolean,
) : SqliteConnectionProvider {
    override suspend fun openConnection(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SafeSQLiteConnection = BundledSqliteConnectionProvider.openConnection(
        dbName,
        debug,
        config,
    )

    override suspend fun openCoordinatedSession(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SqliteNowCoordinatedProviderSession {
        val sessionConfig = config.copy(executionContextHook = hook)
        return DefaultSqliteNowCoordinatedProviderSession(
            inspectionConnection = if (inspection) {
                BundledSqliteConnectionProvider.openConnection(dbName, debug, sessionConfig)
            } else {
                null
            },
            preparationRequired = !inspection,
            prepare = {
                BundledSqliteConnectionProvider.openConnection(dbName, debug, sessionConfig)
            },
        )
    }
}

private class FailOnceCloseHook : SqliteNowContextHook {
    var failNextClose = false

    override fun capture(): Any = Unit

    override suspend fun <T> withCaptured(captured: Any?, block: suspend () -> T): T {
        if (failNextClose) {
            failNextClose = false
            throw IllegalStateException("COORDINATED_OPENING_CLOSE_FAILURE")
        }
        return block()
    }
}

private class BlockingFailOnceCloseHook : SqliteNowContextHook {
    val closeEntered = CompletableDeferred<Unit>()
    val releaseClose = CompletableDeferred<Unit>()
    private var failed = false

    override fun capture(): Any = Unit

    override suspend fun <T> withCaptured(captured: Any?, block: suspend () -> T): T {
        if (!failed) {
            failed = true
            closeEntered.complete(Unit)
            releaseClose.await()
            throw IllegalStateException("COORDINATED_OPENING_PROVIDER_CLOSE_FAILURE")
        }
        return block()
    }
}

@OptIn(SqliteNowInternalApi::class)
object SqliteNowLifecycleLeaseProcessHelper {
    @JvmStatic
    fun main(arguments: Array<String>) = runBlocking {
        require(arguments.size == 2)
        val database = SqliteNowDatabase(
            arguments[0],
            object : DatabaseMigrations {
                override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int =
                    currentVersion
            },
        )
        val session = database.reserveUnpublishedSession()
        try {
            session.acquire()
        } catch (_: SqliteNowLifecycleBusyException) {
            println("BUSY")
            return@runBlocking
        }
        println("ACQUIRED")
        System.out.flush()
        if (arguments[1] == "hold") {
            System.`in`.read()
        }
        session.close()
    }
}

private suspend fun SafeSQLiteConnection.readVersionForTest(): Int =
    prepare("PRAGMA user_version").use { statement ->
        check(statement.step())
        statement.getLong(0).toInt()
    }

private suspend fun SafeSQLiteConnection.hasTableForTest(name: String): Boolean =
    prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").use { statement ->
        statement.bindText(1, name)
        statement.step()
    }
