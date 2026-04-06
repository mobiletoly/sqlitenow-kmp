package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.promise
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.js.ExperimentalWasmJsInterop

class SqliteWasmSmokeTest {

    @OptIn(DelicateCoroutinesApi::class, ExperimentalWasmJsInterop::class)
    @Test
    fun openQueryAndReadRow() = GlobalScope.promise {
        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = ":memory:",
            debug = false,
        )
        try {
            connection.execSQL("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            connection.execSQL("INSERT INTO test (id, name) VALUES (1, 'Ada Lovelace');")

            val statement = connection.prepare("SELECT id, name FROM test WHERE id = 1")
            try {
                assertTrue(statement.step(), "Expected a row to be returned")
                assertEquals(1L, statement.getLong(0))
                assertEquals("Ada Lovelace", statement.getText(1))
                assertFalse(statement.step(), "Only one row should be present")
            } finally {
                statement.close()
            }
        } finally {
            connection.close()
        }
    }
}
