package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TableValidationTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory SQLite database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create a test table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    street TEXT,
                    city TEXT,
                    FOREIGN KEY (person_id) REFERENCES Person(id)
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test that introspection succeeds with valid tables")
    fun testValidTables() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name, a.street, a.city
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(5, fieldNames.size)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent table")
    fun testNonExistentTable() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name, a.street, a.city
            FROM Person2 p
            JOIN PersonAddress a ON p.id = a.person_id
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such table", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent joined table")
    fun testNonExistentJoinedTable() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name, a.street, a.city
            FROM Person p
            JOIN PersonAddress2 a ON p.id = a.person_id
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such table", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection throws exception with multiple non-existent tables")
    fun testMultipleNonExistentTables() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name, a.street, a.city, c.name
            FROM Person2 p
            JOIN PersonAddress2 a ON p.id = a.person_id
            JOIN Company c ON p.company_id = c.id
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such table", ignoreCase = true) == true)
    }
}
