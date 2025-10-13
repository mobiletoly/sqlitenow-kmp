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

import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SelectStatementInParameterTest {
    @Test
    fun `IN clause with named parameter expands to json_each`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE person(doc_id TEXT NOT NULL, last_name TEXT NOT NULL)")
            }

            val sql = "SELECT p.* FROM person p WHERE p.last_name IN :names"
            val parsed = CCJSqlParserUtil.parse(sql)
            val plainSelect = (parsed as net.sf.jsqlparser.statement.select.Select).selectBody as PlainSelect

            val selectStatement = SelectStatement.parse(connection, plainSelect)

            assertEquals(
                "SELECT p.* FROM person p WHERE p.last_name IN (SELECT value FROM json_each(?))",
                selectStatement.sql,
            )
            assertEquals(listOf("names"), selectStatement.namedParameters)
            val associated = selectStatement.namedParametersToColumns["names"]
            assertTrue(
                associated is AssociatedColumn.Collection,
                "Expected IN parameter to be treated as collection",
            )
            assertEquals("last_name", (associated as AssociatedColumn.Collection).columnName)
        }
    }
}
