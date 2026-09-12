package dev.goquick.sqlitenow.core

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail
import platform.posix.fflush
import platform.posix.getenv
import platform.posix.getpid

/**
 * External process-death helper for the native SQLiteNow lifecycle contract.
 *
 * The normal native test suites leave this test as a no-op. The coordinated opening controller starts the
 * compiled test binary with an explicit role, kills the durable owner process, and starts a
 * distinct observer process against the same database file.
 */
@OptIn(SqliteNowInternalApi::class, ExperimentalForeignApi::class)
class SqliteNowNativeProcessDeathTest {
    @Test
    fun externallyControlledOwnerProbeAndFreshObserver() = runTest {
        val role = environment("COORDINATED_OPENING_NATIVE_PROCESS_ROLE") ?: return@runTest
        val path = requireNotNull(environment("COORDINATED_OPENING_NATIVE_PROCESS_DB")) {
            "COORDINATED_OPENING_NATIVE_PROCESS_DB is required when COORDINATED_OPENING_NATIVE_PROCESS_ROLE is set"
        }
        val payload = environment("COORDINATED_OPENING_NATIVE_PROCESS_PAYLOAD") ?: "coordinatedopening-native-process-payload"
        val database = SqliteNowDatabase(path, NativeProcessMigrations)
        val session = database.reserveUnpublishedSession()

        if (role == "probe") {
            try {
                session.acquire()
            } catch (_: SqliteNowLifecycleBusyException) {
                marker("COORDINATED_OPENING_NATIVE_PROCESS_BUSY", role, path, payload)
                session.close()
                return@runTest
            }
            session.close()
            fail("probe acquired the lifecycle lease while the owner process was alive")
        }

        require(role == "owner" || role == "observer") { "unsupported native process role: $role" }
        session.acquire()
        session.prepare()
        session.migrateAndValidate()
        session.publish()

        if (role == "owner") {
            database.connection().prepare(
                "INSERT OR REPLACE INTO coordinatedopening_process_witness(id, payload) VALUES (1, ?)",
            ).let { statement ->
                try {
                    statement.bindText(1, payload)
                    check(!statement.step())
                } finally {
                    statement.close()
                }
            }
            marker("COORDINATED_OPENING_NATIVE_PROCESS_OWNER_READY", role, path, payload)
            awaitCancellation()
        }

        val retained = database.connection().prepare(
            "SELECT payload FROM coordinatedopening_process_witness WHERE id = 1",
        ).let { statement ->
            try {
                check(statement.step()) { "fresh observer did not find the retained witness" }
                statement.getText(0)
            } finally {
                statement.close()
            }
        }
        assertEquals(payload, retained)
        marker("COORDINATED_OPENING_NATIVE_PROCESS_OBSERVER_PASS", role, path, retained)
        database.close()
    }

    private fun environment(name: String): String? = getenv(name)?.toKString()

    private fun marker(kind: String, role: String, path: String, payload: String) {
        println(
            "{\"kind\":\"$kind\",\"pid\":${getpid()},\"role\":\"$role\"," +
                "\"database\":\"$path\",\"payload\":\"$payload\"}",
        )
        fflush(null)
    }

    private object NativeProcessMigrations : DatabaseMigrations {
        override val targetVersion: Int = 1

        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            conn.execSQL(
                "CREATE TABLE IF NOT EXISTS coordinatedopening_process_witness (" +
                    "id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            )
            return if (currentVersion < targetVersion) targetVersion else currentVersion
        }
    }
}
