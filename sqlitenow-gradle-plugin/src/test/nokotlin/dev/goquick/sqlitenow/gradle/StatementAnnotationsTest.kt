package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import dev.goquick.sqlitenow.gradle.introsqlite.SqlStatementIntrospector

/**
 * Tests for the annotations extraction functionality in StatementKotlinCodeGenerator.
 */
class StatementAnnotationsTest {
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

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address_type TEXT NOT NULL,
                    street TEXT NOT NULL,
                    city TEXT NOT NULL,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT NOT NULL,
                    is_primary INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT current_timestamp,
                    FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting annotations from SQL statement with inline comments")
    fun testExtractAnnotationsFromSqlInlineComments() {
        // SQL with inline comment annotations
        val sqlWithInlineComments = """
            -- @@className=SelectPersonWithAddress
            -- @@extractable

            SELECT p.id AS person_id, p.first_name, p.last_name, p.email, p.phone, p.birth_date,
                   p.created_at AS person_created_at,
                   a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                   a.is_primary, a.created_at AS address_created_at,
                   p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
            AND p.last_name LIKE :lastName;
        """.trimIndent()

        // Introspect the statement to get comments
        val statementInfo = SqlStatementIntrospector(sqlWithInlineComments, connection).introspect()

        // Extract annotations using StatementKotlinCodeGenerator
        val codeGenerator = StatementKotlinCodeGenerator(sqlWithInlineComments, connection, emptyList())
        val annotations = codeGenerator.annotatedStatement.topLevelAnnotations

        // Verify the annotations were extracted
        assertNotNull(annotations, "Annotations should not be null")
        assertEquals(2, annotations.size, "Should have extracted 2 annotations")
        assertEquals("SelectPersonWithAddress", annotations["className"], "className annotation should be extracted")
        assertEquals(null, annotations["extractable"], "extractable annotation should be extracted with null value")

        // Verify the top comments were preserved
        assertTrue(statementInfo.topComments.isNotEmpty(), "Top comments should not be empty")
        val topCommentsText = statementInfo.topComments.joinToString("\n")
        assertTrue(topCommentsText.contains("@@className=SelectPersonWithAddress"), "Top comments should contain the className annotation")
        assertTrue(topCommentsText.contains("@@extractable"), "Top comments should contain the extractable annotation")
    }

    @Test
    @DisplayName("Test extracting annotations from SQL statement with multiple inline comments")
    fun testExtractAnnotationsFromSqlMultipleInlineComments() {
        // SQL with multiple inline comment annotations
        val sqlWithMultipleInlineComments = """
            -- @@className=SelectPersonWithAddress
            -- @@extended
            -- @extractable

            SELECT p.id AS person_id, (SELECT count(*) FROM Person) AS count_here,
                   p.first_name, p.last_name, p.email, p.phone, p.birth_date,
                   p.created_at AS person_created_at,
                   a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                   a.is_primary, a.created_at AS address_created_at,
                   p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
            AND p.last_name LIKE :lastName;
        """.trimIndent()

        // Introspect the statement to get comments
        val statementInfo = SqlStatementIntrospector(sqlWithMultipleInlineComments, connection).introspect()

        // Extract annotations using StatementKotlinCodeGenerator
        val codeGenerator = StatementKotlinCodeGenerator(sqlWithMultipleInlineComments, connection, emptyList())
        val annotations = codeGenerator.annotatedStatement.topLevelAnnotations

        // Verify the annotations were extracted
        assertNotNull(annotations, "Annotations should not be null")
        assertEquals(2, annotations.size, "Should have extracted 2 annotations")
        assertEquals("SelectPersonWithAddress", annotations["className"], "className annotation should be extracted")
        assertEquals(null, annotations["extended"], "extended annotation should be extracted with null value")

        // Verify the top comments were preserved
        assertTrue(statementInfo.topComments.isNotEmpty(), "Top comments should not be empty")
        val topCommentsText = statementInfo.topComments.joinToString("\n")
        assertTrue(topCommentsText.contains("@@className=SelectPersonWithAddress"), "Top comments should contain the className annotation")
        assertTrue(topCommentsText.contains("@@extended"), "Top comments should contain the extended annotation")
    }

    @Test
    @DisplayName("Test extracting annotations from SQL statement with multiple inline comments with different content")
    fun testExtractAnnotationsFromSqlDifferentInlineComments() {
        // SQL with different inline comment annotations
        val sqlWithDifferentInlineComments = """
            -- @@className=SelectPersonWithAddress
            -- @@extended
            -- This is a regular comment
            -- @@propertyNameGenerator=lowerCamelCase

            SELECT p.id AS person_id, (SELECT count(*) FROM Person) AS count_here,
                   p.first_name, p.last_name, p.email, p.phone, p.birth_date,
                   p.created_at AS person_created_at,
                   a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                   a.is_primary, a.created_at AS address_created_at,
                   p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
            AND p.last_name LIKE :lastName;
        """.trimIndent()

        // Introspect the statement to get comments
        val statementInfo = SqlStatementIntrospector(sqlWithDifferentInlineComments, connection).introspect()

        // Extract annotations using StatementKotlinCodeGenerator
        val codeGenerator = StatementKotlinCodeGenerator(sqlWithDifferentInlineComments, connection, emptyList())
        val annotations = codeGenerator.annotatedStatement.topLevelAnnotations

        // Verify the annotations were extracted
        assertNotNull(annotations, "Annotations should not be null")
        assertEquals(3, annotations.size, "Should have extracted 3 annotations")
        assertEquals("SelectPersonWithAddress", annotations["className"], "className annotation should be extracted")
        assertEquals(null, annotations["extended"], "extended annotation should be extracted with null value")
        assertEquals("lowerCamelCase", annotations["propertyNameGenerator"], "propertyNameGenerator annotation should be extracted")

        // Verify the top comments were preserved
        assertTrue(statementInfo.topComments.isNotEmpty(), "Top comments should not be empty")
        val topCommentsText = statementInfo.topComments.joinToString("\n")
        assertTrue(topCommentsText.contains("@@className=SelectPersonWithAddress"), "Top comments should contain the className annotation")
        assertTrue(topCommentsText.contains("@@extended"), "Top comments should contain the extended annotation")
        assertTrue(topCommentsText.contains("This is a regular comment"), "Top comments should contain the regular comment")
    }
}
