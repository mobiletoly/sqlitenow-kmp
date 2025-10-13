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
package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class SqliteJsIntegrationTest {
    @Test
    fun selectAllWithLargeLimitReturnsRows() = runTest {
        val connection = BundledSqliteConnectionProvider.openConnection(":memory:", debug = true)
        try {
            connection.execSQL(
                """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL
                );
                """.trimIndent(),
            )
            connection.execSQL("INSERT INTO person (id, first_name, last_name) VALUES (1, 'Ada', 'Lovelace')")
            connection.execSQL("INSERT INTO person (id, first_name, last_name) VALUES (2, 'Alan', 'Turing')")
            connection.execSQL("INSERT INTO person (id, first_name, last_name) VALUES (3, 'Grace', 'Hopper')")

            val statement = connection.prepare(
                "SELECT id FROM person ORDER BY id DESC LIMIT ? OFFSET ?",
            )
            try {
                statement.bindLong(index = 1, value = Int.MAX_VALUE.toLong())
                statement.bindInt(index = 2, value = 0)

                val ids = mutableListOf<Long>()
                while (statement.step()) {
                    ids += statement.getLong(0)
                }

                assertEquals(listOf(3L, 2L, 1L), ids)
            } finally {
                statement.close()
            }
        } finally {
            connection.close()
        }
    }

    @Test
    fun bindingOutOfRangeLongsUsesStringFallback() = runTest {
        val connection = BundledSqliteConnectionProvider.openConnection(":memory:", debug = true)
        try {
            connection.execSQL(
                """
                CREATE TABLE comment (
                    id INTEGER PRIMARY KEY NOT NULL,
                    person_id INTEGER NOT NULL,
                    body TEXT NOT NULL
                );
                """.trimIndent(),
            )

            val largePersonId = 5_000_000_000_123_456_789L
            val largeCommentId = Long.MAX_VALUE - 256
            connection.execSQL(
                """
                INSERT INTO comment (id, person_id, body)
                VALUES ($largeCommentId, $largePersonId, 'integration payload');
                """.trimIndent(),
            )

            val statement = connection.prepare("SELECT id, person_id FROM comment WHERE person_id = ?")
            try {
                statement.bindLong(index = 1, value = largePersonId)

                assertTrue(statement.step(), "Expected a row for the large identifier")
                val fetchedId = statement.getLong(0)
                val fetchedPersonId = statement.getLong(1)

                assertEquals(largeCommentId, fetchedId)
                assertEquals(largePersonId, fetchedPersonId)
                assertFalse(statement.step(), "Only one row should match the large identifier")
            } finally {
                statement.close()
            }
        } finally {
            connection.close()
        }
    }
}
