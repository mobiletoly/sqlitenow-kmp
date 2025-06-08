package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SelectFieldsIntrospectionTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory SQLite database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create a test table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    age INTEGER,
                    created_at TEXT
                )
            """)

            // Insert some test data
            stmt.executeUpdate("""
                INSERT INTO users (id, first_name, last_name, email, age, created_at)
                VALUES
                (1, 'John', 'Doe', 'john@example.com', 30, '2023-01-01'),
                (2, 'Jane', 'Smith', 'jane@example.com', 25, '2023-01-02')
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting field names from a simple SELECT statement")
    fun testSimpleSelectFieldNames() {
        val sql = "SELECT id, first_name, last_name FROM users"

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        assertEquals("users", statementInfo.tableName)

        // Verify field names
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(3, fieldNames.size)
        assertEquals("id", fieldNames[0])
        assertEquals("first_name", fieldNames[1])
        assertEquals("last_name", fieldNames[2])
    }

    @Test
    @DisplayName("Test extracting field names with aliases")
    fun testSelectWithAliases() {
        val sql = "SELECT id, first_name AS firstName, last_name AS lastName FROM users"

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        assertEquals("users", statementInfo.tableName)

        // Verify field names with aliases
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(3, fieldNames.size)
        assertEquals("id", fieldNames[0])
        assertEquals("firstName", fieldNames[1])
        assertEquals("lastName", fieldNames[2])
    }

    @Test
    @DisplayName("Test extracting field names with expressions")
    fun testSelectWithExpressions() {
        val sql = "SELECT id, first_name || ' ' || last_name AS full_name, age, age + 10 AS future_age FROM users"

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        assertEquals("users", statementInfo.tableName)

        // Verify field names with expressions
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(4, fieldNames.size)
        assertEquals("id", fieldNames[0])
        assertEquals("full_name", fieldNames[1])
        assertEquals("age", fieldNames[2])
        assertEquals("future_age", fieldNames[3])
    }

    @Test
    @DisplayName("Test extracting field names with functions")
    fun testSelectWithFunctions() {
        val sql = "SELECT id, upper(first_name) AS upper_name, count(*) AS count, max(age) AS max_age FROM users GROUP BY id, first_name"

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        assertEquals("users", statementInfo.tableName)

        // Verify field names with functions
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(4, fieldNames.size)
        assertEquals("id", fieldNames[0])
        assertEquals("upper_name", fieldNames[1])
        assertEquals("count", fieldNames[2])
        assertEquals("max_age", fieldNames[3])
    }

    @Test
    @DisplayName("Test extracting field names with table qualifiers")
    fun testSelectWithTableQualifiers() {
        val sql = "SELECT u.id, u.first_name, u.last_name FROM users u"

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)

        // Verify field names with table qualifiers
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(3, fieldNames.size)
        assertEquals("id", fieldNames[0])
        assertEquals("first_name", fieldNames[1])
        assertEquals("last_name", fieldNames[2])
    }

    @Test
    @DisplayName("Test extracting field names with star notation")
    fun testSelectWithStar() {
        val sql = "SELECT * FROM users"

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        assertEquals("users", statementInfo.tableName)

        // Verify all fields are extracted
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(6, fieldNames.size)
        assertTrue(fieldNames.contains("id"))
        assertTrue(fieldNames.contains("first_name"))
        assertTrue(fieldNames.contains("last_name"))
        assertTrue(fieldNames.contains("email"))
        assertTrue(fieldNames.contains("age"))
        assertTrue(fieldNames.contains("created_at"))
    }
}
