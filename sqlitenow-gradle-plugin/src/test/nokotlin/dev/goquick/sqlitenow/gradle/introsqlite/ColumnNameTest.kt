package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ColumnNameTest {
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
    @DisplayName("Test that column names are correctly extracted for aliased columns")
    fun testAliasedColumnNames() {
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
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        // Check that column names are correctly extracted for aliased columns
        val personIdField = statementInfo.fieldSources.find { it.fieldName == "person_id" }
        assertNotNull(personIdField)
        assertEquals("id", personIdField.columnName)

        val personCreatedAtField = statementInfo.fieldSources.find { it.fieldName == "person_created_at" }
        assertNotNull(personCreatedAtField)
        assertEquals("created_at", personCreatedAtField.columnName)

        val addressIdField = statementInfo.fieldSources.find { it.fieldName == "address_id" }
        assertNotNull(addressIdField)
        assertEquals("id", addressIdField.columnName)
    }
}
