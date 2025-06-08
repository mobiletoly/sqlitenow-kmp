package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Tests for SQL parameter extraction in SqlStatementIntrospector.
 * These tests focus on the prepareSql method and its ability to correctly
 * identify and extract named parameters while ignoring parameters in
 * comments and string literals.
 */
class SqlParameterExtractionTest {

    /**
     * Helper method to get the prepared SQL and parameters from a SQL statement.
     * Note: This method will extract leading comments from the SQL statement.
     */
    private fun getPreparedSqlInfo(sql: String): SqlStatementInfo.PreparedSql {
        val introspector = SqlStatementIntrospector(sql)
        return introspector.introspect().preparedSql
    }

    /**
     * Helper method to directly test the SQL parameter extraction without
     * extracting leading comments.
     */
    private fun testSqlParameterExtraction(sql: String, expectedSql: String, expectedParamCount: Int, expectedParamNames: List<String>) {
        val preparedSql = getPreparedSqlInfo(sql)

        // Check that the parameters were extracted correctly
        assertEquals(expectedParamCount, preparedSql.parameters.size)
        for (i in 0 until expectedParamCount) {
            assertEquals(expectedParamNames[i], preparedSql.parameters[i].name)
        }

        // Check that the SQL was rewritten correctly, ignoring leading comments
        // that might have been removed by extractLeadingComments
        val actualSqlNoLeadingComments = preparedSql.sql.trim()

        // For block comments, we need to handle them differently
        // The SqlStatementIntrospector.extractLeadingComments method only extracts line comments (--),
        // not block comments (/* */), so we need to check if the expected SQL starts with a block comment
        val expectedSqlNoLeadingComments = if (expectedSql.trim().startsWith("/*")) {
            // If the expected SQL starts with a block comment, check if it's a leading comment
            // that might have been removed by the SqlStatementIntrospector
            val blockCommentEndIndex = expectedSql.indexOf("*/") + 2
            val afterBlockComment = expectedSql.substring(blockCommentEndIndex).trim()

            // If there's content after the block comment, and that content matches the actual SQL,
            // then the block comment was a leading comment that was removed
            if (afterBlockComment.isNotEmpty() && actualSqlNoLeadingComments.startsWith(afterBlockComment.lines().first())) {
                afterBlockComment
            } else {
                expectedSql.trim()
            }
        } else {
            // If the expected SQL doesn't start with a block comment, handle line comments as before
            expectedSql.lines()
                .dropWhile { it.trim().startsWith("--") }
                .joinToString("\n")
                .trim()
        }

        assertEquals(expectedSqlNoLeadingComments, actualSqlNoLeadingComments)
    }

    @Test
    @DisplayName("Test basic parameter extraction")
    fun testBasicParameterExtraction() {
        val sql = "SELECT * FROM users WHERE id = :userId AND name = :userName"
        val expectedSql = "SELECT * FROM users WHERE id = ? AND name = ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 2,
            expectedParamNames = listOf("userId", "userName")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with different parameter prefixes")
    fun testParameterPrefixes() {
        val sql = "SELECT * FROM users WHERE id = :userId AND name = @userName AND age > ${'$'}minAge"
        val expectedSql = "SELECT * FROM users WHERE id = ? AND name = ? AND age > ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 3,
            expectedParamNames = listOf("userId", "userName", "minAge")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with duplicate parameters")
    fun testDuplicateParameters() {
        val sql = "SELECT * FROM users WHERE name LIKE :pattern OR email LIKE :pattern"
        val expectedSql = "SELECT * FROM users WHERE name LIKE ? OR email LIKE ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 2,
            expectedParamNames = listOf("pattern", "pattern")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with parameters in single quotes")
    fun testParametersInSingleQuotes() {
        val sql = "SELECT * FROM users WHERE name = ':userId' AND id = :userId"
        val expectedSql = "SELECT * FROM users WHERE name = ':userId' AND id = ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userId")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with parameters in double quotes")
    fun testParametersInDoubleQuotes() {
        val sql = "SELECT * FROM users WHERE name = \":userId\" AND id = :userId"
        val expectedSql = "SELECT * FROM users WHERE name = \":userId\" AND id = ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userId")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with parameters in line comments")
    fun testParametersInLineComments() {
        val sql = """
            -- This is a comment with :userId
            SELECT * FROM users
            WHERE id = :userId -- Another comment with :commentParam
        """.trimIndent()

        val expectedSql = """
            -- This is a comment with :userId
            SELECT * FROM users
            WHERE id = ? -- Another comment with :commentParam
        """.trimIndent()

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userId")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with parameters in block comments")
    fun testParametersInBlockComments() {
        val sql = """
            /* This is a block comment with :commentParam */
            SELECT * FROM users
            WHERE id = :userId /* Another block comment with :anotherParam */
        """.trimIndent()

        val expectedSql = """
            /* This is a block comment with :commentParam */
            SELECT * FROM users
            WHERE id = ? /* Another block comment with :anotherParam */
        """.trimIndent()

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userId")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with escaped quotes")
    fun testParametersWithEscapedQuotes() {
        val sql = "SELECT * FROM users WHERE name = 'It''s me' AND id = :userId"
        val expectedSql = "SELECT * FROM users WHERE name = 'It''s me' AND id = ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userId")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with complex parameter names")
    fun testComplexParameterNames() {
        val sql = "SELECT * FROM users WHERE id = :user_id AND name = :user.name AND age > :user123"
        val expectedSql = "SELECT * FROM users WHERE id = ? AND name = ? AND age > ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 3,
            expectedParamNames = listOf("user_id", "user.name", "user123")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with mixed scenarios")
    fun testMixedScenarios() {
        val sql = """
            -- Comment with :commentParam
            SELECT * FROM users
            WHERE id = :userId /* Block comment with :blockParam */
            AND name = ':notAParam' -- Line comment with :lineParam
            AND email = ":alsoNotAParam"
            AND age > :age
        """.trimIndent()

        val expectedSql = """
            -- Comment with :commentParam
            SELECT * FROM users
            WHERE id = ? /* Block comment with :blockParam */
            AND name = ':notAParam' -- Line comment with :lineParam
            AND email = ":alsoNotAParam"
            AND age > ?
        """.trimIndent()

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 2,
            expectedParamNames = listOf("userId", "age")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with incomplete parameters")
    fun testIncompleteParameters() {
        val sql = "SELECT * FROM users WHERE id = : AND name = :userName"
        val expectedSql = "SELECT * FROM users WHERE id = : AND name = ?"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userName")
        )
    }

    @Test
    @DisplayName("Test parameter extraction with special characters")
    fun testParametersWithSpecialChars() {
        val sql = "SELECT * FROM users WHERE id = :userId AND name LIKE '%:notAParam%'"
        val expectedSql = "SELECT * FROM users WHERE id = ? AND name LIKE '%:notAParam%'"

        testSqlParameterExtraction(
            sql = sql,
            expectedSql = expectedSql,
            expectedParamCount = 1,
            expectedParamNames = listOf("userId")
        )
    }
}
