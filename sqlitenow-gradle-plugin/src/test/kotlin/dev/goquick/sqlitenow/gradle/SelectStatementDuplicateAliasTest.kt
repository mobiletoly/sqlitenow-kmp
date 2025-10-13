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

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.DriverManager
import kotlin.test.assertTrue

class SelectStatementDuplicateAliasTest {

    @Test
    fun `parse throws when duplicate column aliases present`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT)")
            }

            val sql = "SELECT id, id FROM person"
            val parsed = CCJSqlParserUtil.parse(sql)
            val plainSelect = (parsed as net.sf.jsqlparser.statement.select.Select).selectBody as PlainSelect

            val exception = assertThrows<IllegalArgumentException> {
                SelectStatement.parse(connection, plainSelect)
            }
            assertTrue(exception.message?.contains("Duplicate column aliases") == true)
            assertTrue(exception.message?.contains("id") == true)
        }
    }
}
