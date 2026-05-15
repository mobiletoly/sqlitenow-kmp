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

        val statements = assertParsedTopComments(
            sql = sql,
            expectedTopComments = listOf(
                emptyList(),
                listOf("-- Work addresses for some people"),
            ),
        )

        // First statement should not have the comment
        assertEquals(0, statements[0].innerComments.size, "First statement should not have inner comments")
        assertTrue(statements[0].sql.contains("VALUES (18, 18, 'HOME'"), "First statement should contain the first INSERT")

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

        assertParsedTopComments(
            sql = sql,
            expectedTopComments = listOf(
                emptyList(),
                listOf("-- First comment", "-- Second comment"),
            ),
        )
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

        assertParsedTopComments(
            sql = sql,
            expectedTopComments = listOf(
                listOf("-- Initial comment", "-- Another initial comment"),
                listOf("-- Comment for second statement"),
            ),
        )
    }

    private fun assertParsedTopComments(
        sql: String,
        expectedTopComments: List<List<String>>,
    ): List<SqlSingleStatement> {
        val statements = parseSqlStatements(sql)

        assertEquals(expectedTopComments.size, statements.size, "Should have ${expectedTopComments.size} SQL statements")
        expectedTopComments.forEachIndexed { index, comments ->
            assertEquals(comments, statements[index].topComments, "Statement $index should have expected top comments")
        }
        return statements
    }
}
