/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.core

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

        database.connection().execSQL(
            """
            CREATE TABLE t (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            )
            """.trimIndent()
        )

        database.transaction(TransactionMode.IMMEDIATE) {
            database.connection().execSQL("INSERT INTO t(id, name) VALUES (1, 'outer')")
            val innerResult = database.transaction(TransactionMode.EXCLUSIVE) {
                database.connection().execSQL("INSERT INTO t(id, name) VALUES (2, 'inner')")
                "ok"
            }
            assertEquals("ok", innerResult)
        }

        val st = database.connection().prepare("SELECT COUNT(*) FROM t")
        try {
            st.step()
            assertEquals(2, st.getLong(0).toInt())
        } finally {
            st.close()
        }
    }

    @Test
    fun nestedTransactionInnerFailureRollsBackAll() = runTest {
        database.open()

        database.connection().execSQL(
            """
            CREATE TABLE t2 (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            )
            """.trimIndent()
        )

        try {
            database.transaction(TransactionMode.IMMEDIATE) {
                database.connection().execSQL("INSERT INTO t2(id, name) VALUES (1, 'outer')")
                database.transaction(TransactionMode.EXCLUSIVE) {
                    database.connection().execSQL("INSERT INTO t2(id, name) VALUES (2, 'inner')")
                    error("boom")
                }
            }
        } catch (_: Throwable) {
            // expected
        }

        val st = database.connection().prepare("SELECT COUNT(*) FROM t2")
        try {
            st.step()
            // Since we do not use savepoints, the outer transaction is rolled back as well
            assertEquals(0, st.getLong(0).toInt())
        } finally {
            st.close()
        }
    }
}
