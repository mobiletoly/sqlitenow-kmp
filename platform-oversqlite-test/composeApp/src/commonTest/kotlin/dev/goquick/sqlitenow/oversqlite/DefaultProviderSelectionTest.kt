package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.use
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class DefaultProviderSelectionTest {
    @Test
    fun ordinaryDefaultProviderSelectsTheRuntimeAndCannotSelfSkip() = runTest {
        val ownedStorage = Phase6OwnedStorage()
        val dbName = ownedStorage.newDatabaseName()
        try {
            val provider = oversqliteTestConnectionProvider()
            val connection = provider.openConnection(dbName, debug = true)
            try {
                connection.execSQL(
                    "CREATE TABLE phase7_default_probe(id INTEGER PRIMARY KEY NOT NULL)",
                )
                val failure = assertFailsWith<SqliteException> {
                    connection.execSQL(
                        "CREATE TABLE phase7_default_probe(id INTEGER PRIMARY KEY NOT NULL)",
                    )
                }
                if (webRuntimeKind() in setOf("js-node", "js-browser", "wasm-browser")) {
                    assertTrue(
                        failure.cause.toString().contains("SQLite worker"),
                        "The web default requires a worker-specific runtime failure cause.",
                    )
                } else {
                    assertFalse(failure.cause.toString().contains("SQLite worker"))
                }
            } finally {
                connection.close()
            }

            if (webRuntimeKind() in setOf("js-node", "js-browser", "wasm-browser")) {
                val reopened = oversqliteTestConnectionProvider().openConnection(
                    dbName,
                    debug = true,
                )
                try {
                    val statement = reopened.prepare(
                        """
                        SELECT count(*)
                        FROM sqlite_master
                        WHERE type = 'table' AND name = 'phase7_default_probe'
                        """.trimIndent(),
                    )
                    val tableCount = statement.use {
                        assertTrue(it.step())
                        it.getLong(0)
                    }
                    when (webRuntimeKind()) {
                        "js-node" -> assertEquals(0L, tableCount, "Node worker opens must be transient.")
                        "js-browser", "wasm-browser" ->
                            assertEquals(1L, tableCount, "Browser worker opens must reuse direct OPFS.")
                        else -> error("Unexpected worker runtime: ${webRuntimeKind()}")
                    }
                } finally {
                    reopened.close()
                }
            }

            println(
                "phase7_oversqlite_default provider=BundledSqliteConnectionProvider " +
                    "runtime=${webRuntimeKind()} db=$dbName",
            )
        } finally {
            ownedStorage.cleanup()
        }
    }
}
