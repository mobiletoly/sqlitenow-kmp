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
package dev.goquick.sqlitenow.gradle.sqlite

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SqlStatementParserTest {

    @Test
    @DisplayName("Test parsing SQL statements with comments between statements")
    fun testParseStatementsWithCommentsBetween() {
        val sql = """
            INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
            VALUES (18, 18, 'HOME', '1515 Hawthorn Rd', 'Boston', 'MA', '02101', 'USA', 1);

            -- Work addresses for some people
            INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
            VALUES (19, 1, 'WORK', '100 Business Plaza', 'New York', 'NY', '10002', 'USA', 0);
        """.trimIndent()

        val statements = parseSqlStatements(sql)
        // Should have 2 statements
        assertEquals(2, statements.size, "Should have 2 SQL statements")

        // First statement should not have the comment
        assertEquals(0, statements[0].topComments.size, "First statement should not have top comments")
        assertEquals(0, statements[0].innerComments.size, "First statement should not have inner comments")
        assertTrue(statements[0].sql.contains("VALUES (18, 18, 'HOME'"), "First statement should contain the first INSERT")

        // Second statement should have the comment as a top comment
        assertEquals(1, statements[1].topComments.size, "Second statement should have 1 top comment")
        assertEquals("-- Work addresses for some people", statements[1].topComments[0], "Second statement should have the correct top comment")
        assertTrue(statements[1].sql.contains("VALUES (19, 1, 'WORK'"), "Second statement should contain the second INSERT")
    }

    @Test
    @DisplayName("Test parsing SQL statements with multiple comments between statements")
    fun testParseStatementsWithMultipleCommentsBetween() {
        val sql = """
            INSERT INTO Person (id, name) VALUES (1, 'John');

            -- First comment
            -- Second comment
            INSERT INTO Person (id, name) VALUES (2, 'Jane');
        """.trimIndent()

        val statements = parseSqlStatements(sql)

        // Should have 2 statements
        assertEquals(2, statements.size, "Should have 2 SQL statements")

        // First statement should not have the comments
        assertEquals(0, statements[0].topComments.size, "First statement should not have top comments")

        // Second statement should have both comments as top comments
        assertEquals(2, statements[1].topComments.size, "Second statement should have 2 top comments")
        assertEquals("-- First comment", statements[1].topComments[0], "Second statement should have the correct first top comment")
        assertEquals("-- Second comment", statements[1].topComments[1], "Second statement should have the correct second top comment")
    }

    @Test
    @DisplayName("Test parsing SQL statements with comments before any statement")
    fun testParseStatementsWithCommentsBeforeAnyStatement() {
        val sql = """
            -- Initial comment
            -- Another initial comment
            INSERT INTO Person (id, name) VALUES (1, 'John');

            -- Comment for second statement
            INSERT INTO Person (id, name) VALUES (2, 'Jane');
        """.trimIndent()

        val statements = parseSqlStatements(sql)

        // Should have 2 statements
        assertEquals(2, statements.size, "Should have 2 SQL statements")

        // First statement should have the initial comments
        assertEquals(2, statements[0].topComments.size, "First statement should have 2 top comments")
        assertEquals("-- Initial comment", statements[0].topComments[0], "First statement should have the correct first top comment")
        assertEquals("-- Another initial comment", statements[0].topComments[1], "First statement should have the correct second top comment")

        // Second statement should have its comment
        assertEquals(1, statements[1].topComments.size, "Second statement should have 1 top comment")
        assertEquals("-- Comment for second statement", statements[1].topComments[0], "Second statement should have the correct top comment")
    }
}
