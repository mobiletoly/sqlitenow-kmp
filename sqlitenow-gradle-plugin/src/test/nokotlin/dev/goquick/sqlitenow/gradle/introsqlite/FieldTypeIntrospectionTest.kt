package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class FieldTypeIntrospectionTest {
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
                    age INTEGER,
                    height REAL,
                    is_active BOOLEAN,
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
    @DisplayName("Test extracting field types from a simple SELECT query")
    fun testSimpleSelectFieldTypes() {
        val sql = """
            SELECT id, first_name, last_name, email, age, height, is_active, birth_date
            FROM Person
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(8, fieldNames.size)

        // Check field types
        val idField = statementInfo.fieldSources.find { it.fieldName == "id" }
        assertNotNull(idField)
        assertEquals("INTEGER", idField.dataType)
        // kotlinType assertions removed

        val firstNameField = statementInfo.fieldSources.find { it.fieldName == "first_name" }
        assertNotNull(firstNameField)
        assertEquals("TEXT", firstNameField.dataType)
        // kotlinType assertions removed

        val ageField = statementInfo.fieldSources.find { it.fieldName == "age" }
        assertNotNull(ageField)
        assertEquals("INTEGER", ageField.dataType)
        // kotlinType assertions removed

        val heightField = statementInfo.fieldSources.find { it.fieldName == "height" }
        assertNotNull(heightField)
        assertEquals("REAL", heightField.dataType)
        // kotlinType assertions removed

        val isActiveField = statementInfo.fieldSources.find { it.fieldName == "is_active" }
        assertNotNull(isActiveField)
        assertTrue(isActiveField.dataType.contains("BOOLEAN") || isActiveField.dataType.contains("INTEGER"))
        // kotlinType assertions removed
    }

    @Test
    @DisplayName("Test extracting field types from a query with expressions")
    fun testExpressionFieldTypes() {
        val sql = """
            SELECT
                id,
                first_name || ' ' || last_name AS full_name,
                UPPER(email) AS upper_email,
                age + 10 AS future_age,
                height * 100 AS height_cm,
                CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END AS status
            FROM Person
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(6, fieldNames.size)

        // Check expression field types
        val fullNameField = statementInfo.fieldSources.find { it.fieldName == "full_name" }
        assertNotNull(fullNameField)
        assertTrue(fullNameField.dataType == "TEXT" || fullNameField.dataType == "NUMERIC")
        // kotlinType assertions removed

        val upperEmailField = statementInfo.fieldSources.find { it.fieldName == "upper_email" }
        assertNotNull(upperEmailField)
        assertTrue(upperEmailField.dataType == "TEXT" || upperEmailField.dataType == "NUMERIC")
        // kotlinType assertions removed

        val futureAgeField = statementInfo.fieldSources.find { it.fieldName == "future_age" }
        assertNotNull(futureAgeField)
        assertTrue(futureAgeField.dataType == "INTEGER" || futureAgeField.dataType == "NUMERIC")
        // kotlinType assertions removed

        val heightCmField = statementInfo.fieldSources.find { it.fieldName == "height_cm" }
        assertNotNull(heightCmField)
        assertTrue(heightCmField.dataType == "INTEGER" || heightCmField.dataType == "NUMERIC" || heightCmField.dataType == "REAL")
        // kotlinType assertions removed
    }

    @Test
    @DisplayName("Test extracting field types from a query with aggregate functions")
    fun testAggregateFunctionFieldTypes() {
        val sql = """
            SELECT
                COUNT(*) AS count,
                SUM(age) AS total_age,
                AVG(height) AS avg_height,
                MIN(age) AS min_age,
                MAX(age) AS max_age
            FROM Person
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(5, fieldNames.size)

        // Check aggregate function field types
        val countField = statementInfo.fieldSources.find { it.fieldName == "count" }
        assertNotNull(countField)
        assertTrue(countField.dataType == "INTEGER" || countField.dataType == "NUMERIC")
        // kotlinType assertions removed

        val totalAgeField = statementInfo.fieldSources.find { it.fieldName == "total_age" }
        assertNotNull(totalAgeField)
        assertTrue(totalAgeField.dataType == "INTEGER" || totalAgeField.dataType == "NUMERIC")
        // kotlinType assertions removed

        val avgHeightField = statementInfo.fieldSources.find { it.fieldName == "avg_height" }
        assertNotNull(avgHeightField)
        assertTrue(avgHeightField.dataType.contains("REAL") || avgHeightField.dataType.contains("INTEGER") || avgHeightField.dataType.contains("NUMERIC"))
        // kotlinType assertions removed
    }
}
