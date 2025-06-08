package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ColumnValidationTest {
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
    @DisplayName("Test that introspection succeeds with valid columns")
    fun testValidColumns() {
        val sql = """
            SELECT p.id AS person_id,
            p.first_name, p.last_name, p.email, p.phone, p.birth_date,
            p.created_at AS person_created_at,
            a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
            a.is_primary, a.created_at AS address_created_at
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :namePattern
            AND p.last_name LIKE :namePattern
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(16, fieldNames.size)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent column in SELECT")
    fun testNonExistentColumnInSelect() {
        val sql = """
            SELECT p.id AS person_id,
            p.first_name2 AS first_name_1,
            p.last_name, p.email, p.phone, p.birth_date,
            p.created_at AS person_created_at
            FROM Person p
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent column in WHERE")
    fun testNonExistentColumnInWhere() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name
            FROM Person p
            WHERE p.age > 30
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent column in ORDER BY")
    fun testNonExistentColumnInOrderBy() {
        val sql = """
            SELECT p.id, p.first_name, p.last_name
            FROM Person p
            ORDER BY p.registration_date DESC
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent column in GROUP BY")
    fun testNonExistentColumnInGroupBy() {
        val sql = """
            SELECT p.id, COUNT(*) as count
            FROM Person p
            GROUP BY p.department
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection throws exception with non-existent column in HAVING")
    fun testNonExistentColumnInHaving() {
        val sql = """
            SELECT p.id, COUNT(*) as count
            FROM Person p
            GROUP BY p.id
            HAVING p.salary > 50000
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            SqlStatementIntrospector(sql, connection).introspect()
        }

        assert(exception.message?.contains("no such column", ignoreCase = true) == true)
    }

    @Test
    @DisplayName("Test that introspection handles expressions correctly")
    fun testExpressions() {
        val sql = """
            SELECT p.id,
                   p.first_name || ' ' || p.last_name AS full_name,
                   UPPER(p.email) AS upper_email,
                   CASE WHEN p.birth_date IS NULL THEN 'Unknown' ELSE p.birth_date END AS birth_date_display
            FROM Person p
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(4, fieldNames.size)
    }

    @Test
    @DisplayName("Test that introspection handles aliases in ORDER BY")
    fun testAliasesInOrderBy() {
        val sql = """
            SELECT p.id AS person_id,
                   p.first_name AS first_name_1,
                   p.first_name AS first_name_2,
                   p.last_name, p.email, p.phone, p.birth_date,
                   p.created_at AS person_created_at,
                   a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                   a.is_primary, a.created_at AS address_created_at,
                   p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
            AND p.last_name LIKE :lastName
            ORDER BY person_created_at;
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(18, fieldNames.size)
    }
}
