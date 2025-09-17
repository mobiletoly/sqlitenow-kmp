package dev.goquick.sqlitenow.core

import androidx.sqlite.execSQL
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import java.io.File

class TransactionSafetyTest {
    private lateinit var database: SqliteNowDatabase
    private lateinit var testDbName: String
    private lateinit var testDbFile: File

    private class TestMigrations : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            // No-op migration, initialize fresh DB to version 0
            return if (currentVersion == -1) 0 else currentVersion
        }
    }

    @BeforeTest
    fun setup() {
        testDbName = "tx-safety-${System.currentTimeMillis()}.db"
        testDbFile = File(testDbName)
        database = SqliteNowDatabase(testDbName, TestMigrations())
    }

    @AfterTest
    fun teardown() {
        try {
            runBlocking { database.close() }
        } finally {
            if (this::testDbFile.isInitialized && testDbFile.exists()) {
                testDbFile.delete()
            }
        }
    }

    @Test
    fun nestedTransactionCommitsSafely() = runTest {
        database.open()

        database.connection().ref.execSQL(
            """
            CREATE TABLE t (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            )
            """.trimIndent()
        )

        database.transaction(TransactionMode.IMMEDIATE) {
            database.connection().ref.execSQL("INSERT INTO t(id, name) VALUES (1, 'outer')")
            val innerResult = database.transaction(TransactionMode.EXCLUSIVE) {
                database.connection().ref.execSQL("INSERT INTO t(id, name) VALUES (2, 'inner')")
                "ok"
            }
            assertEquals("ok", innerResult)
        }

        val st = database.connection().ref.prepare("SELECT COUNT(*) FROM t")
        try {
            st.step()
            assertEquals(2, st.getInt(0))
        } finally {
            st.close()
        }
    }

    @Test
    fun nestedTransactionInnerFailureRollsBackAll() = runTest {
        database.open()

        database.connection().ref.execSQL(
            """
            CREATE TABLE t2 (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            )
            """.trimIndent()
        )

        try {
            database.transaction(TransactionMode.IMMEDIATE) {
                database.connection().ref.execSQL("INSERT INTO t2(id, name) VALUES (1, 'outer')")
                database.transaction(TransactionMode.EXCLUSIVE) {
                    database.connection().ref.execSQL("INSERT INTO t2(id, name) VALUES (2, 'inner')")
                    error("boom")
                }
            }
        } catch (_: Throwable) {
            // expected
        }

        val st = database.connection().ref.prepare("SELECT COUNT(*) FROM t2")
        try {
            st.step()
            // Since we do not use savepoints, the outer transaction is rolled back as well
            assertEquals(0, st.getInt(0))
        } finally {
            st.close()
        }
    }
}
