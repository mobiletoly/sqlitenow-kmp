package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SqlValidationByExecutionTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory SQLite database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test tables
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address_type TEXT,
                    street TEXT,
                    city TEXT,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT,
                    is_primary INTEGER,
                    created_at TEXT,
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
    @DisplayName("Test that introspection succeeds with valid SQL")
    fun testValidSql() {
        val sql = """
            SELECT p.id AS person_id,
            p.first_name, p.last_name, p.email, p.phone, p.birth_date,
            p.created_at AS person_created_at,
            a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
            a.is_primary, a.created_at AS address_created_at,
            p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
            AND p.last_name LIKE :lastName
            ORDER BY person_created_at
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
    }

    @Test
    @DisplayName("Test that introspection fails with non-existent table")
    fun testNonExistentTable() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name
            FROM Person2 p
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such table", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection fails with non-existent column")
    fun testNonExistentColumn() {
        val sql = """
            SELECT p.id, p.first_name2, p.last_name
            FROM Person p
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection fails with invalid JOIN condition")
    fun testInvalidJoinCondition() {
        val sql = """
            SELECT p.id, p.first_name, a.street
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id222
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection fails with syntax error")
    fun testSyntaxError() {
        val sql = """
            SELEC p.id, p.first_name, p.last_name
            FROM Person p
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("syntax error", ignoreCase = true) == true)
    }
}
