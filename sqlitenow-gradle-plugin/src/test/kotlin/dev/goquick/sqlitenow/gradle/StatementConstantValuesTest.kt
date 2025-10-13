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
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.update.Update
import org.junit.jupiter.api.Test

class StatementConstantValuesTest {

    @Test
    fun `update statement handles mixed constant assignments`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY,
                        age INTEGER NOT NULL,
                        weight REAL NOT NULL,
                        first_name TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 0,
                        updated_at TEXT,
                        notes TEXT
                    )
                    """.trimIndent()
                )
            }

            val sql = """
                UPDATE person
                SET age = :age,
                    weight = :weight,
                    first_name = 'Static',
                    active = 1,
                    updated_at = CURRENT_TIMESTAMP,
                    notes = NULL
                WHERE id = :id;
            """.trimIndent()

            val update = CCJSqlParserUtil.parse(sql) as Update
            val parsed = UpdateStatement.parse(update, conn)

            assertEquals(setOf("age", "weight", "id"), parsed.namedParameters.toSet())
            assertEquals(
                mapOf(
                    "age" to "age",
                    "weight" to "weight"
                ),
                parsed.namedParametersToColumnNames
            )
        }
    }

    @Test
    fun `insert statement handles constant values and conflict updates`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY,
                        age INTEGER NOT NULL,
                        weight REAL NOT NULL,
                        notes TEXT,
                        active INTEGER NOT NULL DEFAULT 0
                    )
                    """.trimIndent()
                )
            }

            val sql = """
                INSERT INTO person (id, age, weight, notes, active)
                VALUES (:id, 42, 2.5, NULL, 1)
                ON CONFLICT(id) DO UPDATE SET
                    age = excluded.age,
                    weight = excluded.weight,
                    notes = :notes,
                    active = 0;
            """.trimIndent()

            val insert = CCJSqlParserUtil.parse(sql) as Insert
            val parsed = InsertStatement.parse(insert, conn)

            assertEquals(setOf("id", "notes"), parsed.namedParameters.toSet())
            assertEquals(
                mapOf(
                    "id" to "id",
                    "notes" to "notes"
                ),
                parsed.columnNamesAssociatedWithNamedParameters
            )
        }
    }

    @Test
    fun `delete statement handles constants in where clause`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY,
                        active INTEGER NOT NULL DEFAULT 0,
                        email TEXT NOT NULL
                    )
                    """.trimIndent()
                )
            }

            val sql = """
                DELETE FROM person
                WHERE active = 0
                  AND email = :email;
            """.trimIndent()

            val delete = CCJSqlParserUtil.parse(sql) as Delete
            val parsed = DeleteStatement.parse(delete, conn)

            assertEquals(listOf("email"), parsed.namedParameters)
            assertTrue(parsed.namedParametersToColumns.containsKey("email"))
        }
    }

    @Test
    fun `select statement handles constants in projections and filters`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY,
                        age INTEGER NOT NULL,
                        first_name TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 0
                    )
                    """.trimIndent()
                )
            }

            val sql = """
                SELECT id,
                       first_name,
                       1 AS const_int,
                       3.14 AS const_real,
                       CURRENT_TIMESTAMP AS const_timestamp,
                       NULL AS const_null
                FROM person
                WHERE active = 1
                  AND first_name = 'Static'
                  AND age > :minAge;
            """.trimIndent()

            val select = CCJSqlParserUtil.parse(sql) as Select
            val plainSelect = select.selectBody as? PlainSelect
                ?: error("Expected PlainSelect")

            val parsed = SelectStatement.parse(conn, plainSelect)

            assertEquals(listOf("minAge"), parsed.namedParameters)
            assertTrue(parsed.fields.any { it.fieldName == "const_int" })
            assertTrue(parsed.fields.any { it.fieldName == "const_real" })
            assertTrue(parsed.fields.any { it.fieldName == "const_timestamp" })
            assertTrue(parsed.fields.any { it.fieldName == "const_null" })
        }
    }
}
