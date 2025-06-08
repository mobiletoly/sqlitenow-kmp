package dev.goquick.sqlitenow.gradle.introsqlite

import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import dev.goquick.sqlitenow.gradle.sqlite.parseSqlStatements
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqlFieldCommentsTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create a test table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT DEFAULT current_timestamp
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test that field-level comments are preserved in their original position")
    fun testFieldLevelCommentsPreserved() {
        // SQL with top-level and field-level comments
        val sql = """
            -- @@className=EntityWithCount
            SELECT
                -- @@field=total_person_count @@propertyName=myTotalPersonCount @@propertyType=Int @@nonNull
                COUNT(id) AS total_person_count,
                *
            FROM Person
            WHERE birth_date < :birth_date
        """.trimIndent()

        // Parse the SQL statement
        val statements = parseSqlStatements(sql)
        assertEquals(1, statements.size, "Should have 1 statement")

        val statement = statements[0]

        // Print debug information
        println("\nOriginal SQL:")
        println(sql)

        println("\nParsed SQL:")
        println(statement.sql)

        println("\nTop comments:")
        statement.topComments.forEach { comment ->
            println("Comment: $comment")
        }

        // Verify top comments
        assertEquals(1, statement.topComments.size, "Should have 1 top comment")
        assertEquals("-- @@className=EntityWithCount", statement.topComments[0], "Top comment should be the class name annotation")

        // Verify that the field-level comment is preserved in the SQL
        assertTrue(statement.sql.contains("-- @@field=total_person_count"),
            "SQL should contain the field-level comment")

        // Verify that the SQL statement is correctly parsed
        val introspector = SqlStatementIntrospector(statement.sql, connection)
        val statementInfo = introspector.introspect()

        // Verify that the field-level comment is in the inner comments
        assertEquals(1, statementInfo.innerComments.size, "Should have 1 inner comment")
        assertTrue(statementInfo.innerComments[0].contains("@@field=total_person_count"),
            "Inner comment should contain the field annotation")
    }

    @Test
    @DisplayName("Test that multiple field-level comments are preserved")
    fun testMultipleFieldLevelComments() {
        // SQL with multiple field-level comments
        val sql = """
            -- @@className=EntityWithCount
            SELECT
                -- @@field=total_person_count @@propertyName=myTotalPersonCount @@propertyType=Int @@nonNull
                COUNT(id) AS total_person_count,

                -- @@field=first_name @@propertyName=firstName
                first_name,

                -- @@field=last_name @@propertyName=lastName
                last_name,

                *
            FROM Person
            WHERE birth_date < :birth_date
        """.trimIndent()

        // Parse the SQL statement
        val statements = parseSqlStatements(sql)
        assertEquals(1, statements.size, "Should have 1 statement")

        val statement = statements[0]

        // Verify top comments
        assertEquals(1, statement.topComments.size, "Should have 1 top comment")
        assertEquals("-- @@className=EntityWithCount", statement.topComments[0], "Top comment should be the class name annotation")

        // Verify that all field-level comments are preserved in the SQL
        assertTrue(statement.sql.contains("-- @@field=total_person_count"),
            "SQL should contain the total_person_count field-level comment")
        assertTrue(statement.sql.contains("-- @@field=first_name"),
            "SQL should contain the first_name field-level comment")
        assertTrue(statement.sql.contains("-- @@field=last_name"),
            "SQL should contain the last_name field-level comment")

        // Verify that the SQL statement is correctly parsed
        val introspector = SqlStatementIntrospector(statement.sql, connection)
        val statementInfo = introspector.introspect()

        // Verify that all field-level comments are in the inner comments
        assertEquals(3, statementInfo.innerComments.size, "Should have 3 inner comments")

        // Check that all field comments are in innerComments
        val hasCountComment = statementInfo.innerComments.any { it.contains("@@field=total_person_count") }
        val hasFirstNameComment = statementInfo.innerComments.any { it.contains("@@field=first_name") }
        val hasLastNameComment = statementInfo.innerComments.any { it.contains("@@field=last_name") }

        assertTrue(hasCountComment, "Inner comments should include the total_person_count field annotation")
        assertTrue(hasFirstNameComment, "Inner comments should include the first_name field annotation")
        assertTrue(hasLastNameComment, "Inner comments should include the last_name field annotation")
    }

    @Test
    @DisplayName("Test that mixed comments are correctly preserved")
    fun testMixedComments() {
        // SQL with mixed comments
        val sql = """
            -- @@className=EntityWithCount
            SELECT
                -- @@field=total_person_count @@propertyName=myTotalPersonCount @@propertyType=Int @@nonNull
                COUNT(id) AS total_person_count,
                -- some other comment we want to preserve #1
                *
            -- some other comment we want to preserve #2
            FROM Person
            /*
            some other comment we want to preserve #3
            some other comment we want to preserve #4
             */
            WHERE birth_date < :birth_date
        """.trimIndent()

        // Parse the SQL statement
        val statements = parseSqlStatements(sql)
        assertEquals(1, statements.size, "Should have 1 statement")

        val statement = statements[0]

        // Print debug information
        println("\nOriginal SQL:")
        println(sql)

        println("\nParsed SQL:")
        println(statement.sql)

        println("\nTop comments:")
        statement.topComments.forEach { comment ->
            println("Comment: $comment")
        }

        // Verify top comments
        assertEquals(1, statement.topComments.size, "Should have 1 top comment")
        assertEquals("-- @@className=EntityWithCount", statement.topComments[0], "Top comment should be the class name annotation")

        // Verify that all comments are preserved in the SQL
        assertTrue(statement.sql.contains("-- @@field=total_person_count"),
            "SQL should contain the field-level comment")
        assertTrue(statement.sql.contains("-- some other comment we want to preserve #1"),
            "SQL should contain regular comment #1")
        assertTrue(statement.sql.contains("-- some other comment we want to preserve #2"),
            "SQL should contain regular comment #2")
        assertTrue(statement.sql.contains("/*"),
            "SQL should contain block comment start")
        assertTrue(statement.sql.contains("some other comment we want to preserve #3"),
            "SQL should contain regular comment #3")
        assertTrue(statement.sql.contains("some other comment we want to preserve #4"),
            "SQL should contain regular comment #4")
        assertTrue(statement.sql.contains("*/"),
            "SQL should contain block comment end")

        // Verify that the SQL statement is correctly parsed
        val introspector = SqlStatementIntrospector(statement.sql, connection)
        val statementInfo = introspector.introspect()

        // Verify that all comments are correctly categorized
        assertTrue(statementInfo.innerComments.any { it.contains("@@field=total_person_count") },
            "Inner comments should include the field annotation")
        assertTrue(statementInfo.innerComments.any { it.contains("some other comment we want to preserve #1") },
            "Inner comments should include regular comment #1")
        assertTrue(statementInfo.innerComments.any { it.contains("some other comment we want to preserve #2") },
            "Inner comments should include regular comment #2")
    }

    @Test
    @DisplayName("Test that comments at the beginning of SQL are correctly categorized")
    fun testCommentsAtBeginning() {
        // SQL with field comments at the beginning
        val sql = """
            -- @@field=total_person_count @@propertyName=myTotalPersonCount @@propertyType=Int @@nonNull
            SELECT
                COUNT(id) AS total_person_count,
                *
            FROM Person
            WHERE birth_date < :birth_date
        """.trimIndent()

        // Parse the SQL statement
        val statements = parseSqlStatements(sql)
        assertEquals(1, statements.size, "Should have 1 statement")

        val statement = statements[0]

        // Print debug information
        println("\nOriginal SQL:")
        println(sql)

        println("\nParsed SQL:")
        println(statement.sql)

        println("\nTop comments:")
        statement.topComments.forEach { comment ->
            println("Comment: $comment")
        }

        // Verify top comments
        assertEquals(1, statement.topComments.size, "Should have 1 top comment")
        assertTrue(statement.topComments[0].contains("-- @@field=total_person_count"),
            "Top comment should contain the field annotation")

        // Verify that the SQL statement is correctly parsed
        val introspector = SqlStatementIntrospector(statement.sql, connection)
        val statementInfo = introspector.introspect()

        // Verify that there are no inner comments
        assertEquals(0, statementInfo.innerComments.size, "Should have 0 inner comments")
    }
}
