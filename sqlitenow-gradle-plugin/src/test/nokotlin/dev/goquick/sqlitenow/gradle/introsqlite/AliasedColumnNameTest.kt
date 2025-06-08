package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class AliasedColumnNameTest {
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
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test column name extraction for aliased columns")
    fun testAliasedColumnNames() {
        val sql = """
            SELECT p.id AS person_id, p.created_at AS person_created_at
            FROM Person p
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        // Print all field sources for debugging
        println("All field sources:")
        for (fieldSource in statementInfo.fieldSources) {
            println("  $fieldSource")
        }

        // Check person_id field source
        val personIdField = statementInfo.fieldSources.find { it.fieldName == "person_id" }
        assertNotNull(personIdField)
        assertEquals("Person", personIdField.tableName)
        assertEquals("id", personIdField.columnName)

        // Check person_created_at field source
        val personCreatedAtField = statementInfo.fieldSources.find { it.fieldName == "person_created_at" }
        assertNotNull(personCreatedAtField)
        assertEquals("Person", personCreatedAtField.tableName)
        assertEquals("created_at", personCreatedAtField.columnName)
    }
}
