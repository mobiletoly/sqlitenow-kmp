package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.InsertStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
// PropertyNameGeneratorType is used in StatementAnnotationOverrides constructor

class ReturningClauseTest {

    @Test
    @DisplayName("Test InsertStatement detects RETURNING clause correctly")
    fun testInsertStatementReturningClauseDetection() {
        // Create an in-memory SQLite connection
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test table
        conn.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """.trimIndent())

        // Test INSERT with RETURNING *
        val insertWithReturningAll = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING *"
        val parsedInsertAll = CCJSqlParserUtil.parse(insertWithReturningAll) as Insert
        val insertStatementAll = InsertStatement.parse(parsedInsertAll, conn)

        assertTrue(insertStatementAll.hasReturningClause, "Should detect RETURNING * clause")
        assertEquals(listOf("*"), insertStatementAll.returningColumns, "Should return ['*'] for RETURNING *")

        // Test INSERT with RETURNING specific columns
        val insertWithReturningColumns = "INSERT INTO users (name, email) VALUES ('Jane', 'jane@example.com') RETURNING id, name, created_at"
        val parsedInsertColumns = CCJSqlParserUtil.parse(insertWithReturningColumns) as Insert
        val insertStatementColumns = InsertStatement.parse(parsedInsertColumns, conn)

        assertTrue(insertStatementColumns.hasReturningClause, "Should detect RETURNING with specific columns")
        assertEquals(listOf("id", "name", "created_at"), insertStatementColumns.returningColumns,
                    "Should return specific column names")

        // Test INSERT without RETURNING
        val insertWithoutReturning = "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')"
        val parsedInsertNoReturning = CCJSqlParserUtil.parse(insertWithoutReturning) as Insert
        val insertStatementNoReturning = InsertStatement.parse(parsedInsertNoReturning, conn)

        assertFalse(insertStatementNoReturning.hasReturningClause, "Should not detect RETURNING clause when none exists")
        assertTrue(insertStatementNoReturning.returningColumns.isEmpty(), "Should return empty list when no RETURNING clause")

        conn.close()
    }

    @Test
    @DisplayName("Test AnnotatedExecuteStatement RETURNING methods")
    fun testAnnotatedExecuteStatementReturningMethods() {
        // Create an in-memory SQLite connection
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test table
        conn.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Test with RETURNING clause
        val insertWithReturning = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, name"
        val parsedInsert = CCJSqlParserUtil.parse(insertWithReturning) as Insert
        val insertStatement = InsertStatement.parse(parsedInsert, conn)

        val annotatedStatement = AnnotatedExecuteStatement(
            name = "AddUser",
            src = insertStatement,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            )
        )

        assertTrue(annotatedStatement.hasReturningClause(), "AnnotatedExecuteStatement should detect RETURNING clause")
        assertEquals(listOf("id", "name"), annotatedStatement.getReturningColumns(),
                    "AnnotatedExecuteStatement should return correct column names")

        // Test without RETURNING clause
        val insertWithoutReturning = "INSERT INTO users (name, email) VALUES ('Jane', 'jane@example.com')"
        val parsedInsertNoReturning = CCJSqlParserUtil.parse(insertWithoutReturning) as Insert
        val insertStatementNoReturning = InsertStatement.parse(parsedInsertNoReturning, conn)

        val annotatedStatementNoReturning = AnnotatedExecuteStatement(
            name = "AddUserNoReturning",
            src = insertStatementNoReturning,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            )
        )

        assertFalse(annotatedStatementNoReturning.hasReturningClause(),
                   "AnnotatedExecuteStatement should not detect RETURNING clause when none exists")
        assertTrue(annotatedStatementNoReturning.getReturningColumns().isEmpty(),
                  "AnnotatedExecuteStatement should return empty list when no RETURNING clause")

        conn.close()
    }

    @Test
    @DisplayName("Test RETURNING clause with simple column names works correctly")
    fun testReturningClauseWithSimpleColumns() {
        // Create an in-memory SQLite connection
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test table
        conn.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """.trimIndent())

        // Test RETURNING with simple column names (this should work)
        val insertWithSimpleColumns = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, name, created_at"
        val parsedInsert = CCJSqlParserUtil.parse(insertWithSimpleColumns) as Insert
        val insertStatement = InsertStatement.parse(parsedInsert, conn)

        assertTrue(insertStatement.hasReturningClause, "Should detect RETURNING clause with simple columns")
        assertEquals(listOf("id", "name", "created_at"), insertStatement.returningColumns,
                    "Should return correct simple column names")

        conn.close()
    }

    @Test
    @DisplayName("Test RETURNING clause with aliases throws exception")
    fun testReturningClauseWithAliasThrowsException() {
        // Create an in-memory SQLite connection
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test table
        conn.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Test INSERT with RETURNING alias - should throw exception
        val insertWithAlias = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, name AS user_name"
        val parsedInsert = CCJSqlParserUtil.parse(insertWithAlias) as Insert

        try {
            InsertStatement.parse(parsedInsert, conn)
            assertTrue(false, "Expected IllegalArgumentException for RETURNING with alias")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message?.contains("RETURNING clause with aliases is currently not supported") == true,
                      "Exception message should mention aliases not supported: ${e.message}")
        }

        conn.close()
    }

    @Test
    @DisplayName("Test RETURNING clause with expressions throws exception")
    fun testReturningClauseWithExpressionThrowsException() {
        // Create an in-memory SQLite connection
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test table
        conn.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Test INSERT with RETURNING expression - should throw exception
        val insertWithExpression = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, upper(name)"
        val parsedInsert = CCJSqlParserUtil.parse(insertWithExpression) as Insert

        try {
            InsertStatement.parse(parsedInsert, conn)
            assertTrue(false, "Expected IllegalArgumentException for RETURNING with expression")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message?.contains("RETURNING clause with expressions is currently not supported") == true,
                      "Exception message should mention expressions not supported: ${e.message}")
        }

        conn.close()
    }

    @Test
    @DisplayName("Test RETURNING clause with both alias and expression throws exception")
    fun testReturningClauseWithAliasAndExpressionThrowsException() {
        // Create an in-memory SQLite connection
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test table
        conn.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Test INSERT with RETURNING expression with alias - should throw exception
        val insertWithExpressionAndAlias = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, upper(name) AS upper_name"
        val parsedInsert = CCJSqlParserUtil.parse(insertWithExpressionAndAlias) as Insert

        try {
            InsertStatement.parse(parsedInsert, conn)
            assertTrue(false, "Expected IllegalArgumentException for RETURNING with expression and alias")
        } catch (e: IllegalArgumentException) {
            // Should catch alias error first since that's checked first in the code
            assertTrue(e.message?.contains("RETURNING clause with aliases is currently not supported") == true,
                      "Exception message should mention aliases not supported: ${e.message}")
        }

        conn.close()
    }
}
