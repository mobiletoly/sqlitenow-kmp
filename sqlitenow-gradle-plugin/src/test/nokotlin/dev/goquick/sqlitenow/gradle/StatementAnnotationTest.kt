package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.AnnotatedStatementInfo
import dev.goquick.sqlitenow.gradle.introsqlite.DatabaseColumn
import java.sql.Connection
import java.sql.DriverManager
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StatementAnnotationTest {

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
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    birth_date TEXT
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test Column class with annotations")
    fun testColumnWithAnnotations() {
        // Create a DatabaseColumn with comments containing annotations
        val columnComment = "-- @@propertyType=kotlinx.datetime.LocalDate @@propertyName=birthDate"
        val dbColumn = DatabaseColumn(
            name = "birth_date",
            dataType = "TEXT",
            notNull = false,
            defaultValue = null,
            isPrimaryKey = false,
            isAutoincrement = false,
            isUnique = false,
            collationName = null,
            position = 0,
            comments = listOf(columnComment)
        )

        val column = StatementKotlinCodeGenerator.Column(dbColumn)

        assertEquals(2, column.annotations.size, "Column should have 2 annotations")
        assertEquals("kotlinx.datetime.LocalDate", column.annotations["propertyType"], "Column should have propertyType annotation")
        assertEquals("birthDate", column.annotations["propertyName"], "Column should have propertyName annotation")
    }

    @Test
    @DisplayName("Test parsing annotations from comments")
    fun testParseAnnotations() {
        // Test with a simple annotation
        val simpleComment = "-- @@className=PersonEntity"
        val simpleAnnotations = AnnotatedStatementInfo.parseAnnotations(simpleComment)
        assertEquals(1, simpleAnnotations.size)
        assertEquals("PersonEntity", simpleAnnotations["className"])

        // Test with multiple annotations
        val multipleComment = "-- @@propertyType=kotlinx.datetime.LocalDate @@propertyName=birthDate"
        val multipleAnnotations = AnnotatedStatementInfo.parseAnnotations(multipleComment)
        assertEquals(2, multipleAnnotations.size)
        assertEquals("kotlinx.datetime.LocalDate", multipleAnnotations["propertyType"])
        assertEquals("birthDate", multipleAnnotations["propertyName"])

        // Test with a comment that has no annotations
        val noAnnotationComment = "-- This is a regular comment"
        val noAnnotations = AnnotatedStatementInfo.parseAnnotations(noAnnotationComment)
        assertTrue(noAnnotations.isEmpty())

        // Test with a null comment
        val nullAnnotations = emptyMap<String, String?>()
        assertTrue(nullAnnotations.isEmpty())

        // Test with an empty comment
        val emptyAnnotations = AnnotatedStatementInfo.parseAnnotations("")
        assertTrue(emptyAnnotations.isEmpty())

        // Test with a multi-line comment
        val multiLineComment = """
            /*
             * @@className=PersonEntity
             * @@tableName=person
             */
        """.trimIndent()
        val multiLineAnnotations = AnnotatedStatementInfo.parseAnnotations(multiLineComment)
        assertEquals(2, multiLineAnnotations.size)
        assertEquals("PersonEntity", multiLineAnnotations["className"])
        assertEquals("person", multiLineAnnotations["tableName"])
    }

    @Test
    @DisplayName("Test parsing annotations without values")
    fun testParseAnnotationsWithoutValues() {
        // Test with an annotation without a value
        val noValueComment = "-- @@nullable"
        val noValueAnnotations = AnnotatedStatementInfo.parseAnnotations(noValueComment)
        assertEquals(1, noValueAnnotations.size)
        assertNull(noValueAnnotations["nullable"])

        // Test with mixed annotations (with and without values)
        val mixedComment = "-- @@propertyType=kotlinx.datetime.LocalDate @@nullable @@propertyName=birthDate"
        val mixedAnnotations = AnnotatedStatementInfo.parseAnnotations(mixedComment)
        assertEquals(3, mixedAnnotations.size)
        assertEquals("kotlinx.datetime.LocalDate", mixedAnnotations["propertyType"])
        assertNull(mixedAnnotations["nullable"])
        assertEquals("birthDate", mixedAnnotations["propertyName"])

        // Test with multiple annotations without values
        val multipleNoValueComment = "-- @@nullable @@readonly @@deprecated"
        val multipleNoValueAnnotations = AnnotatedStatementInfo.parseAnnotations(multipleNoValueComment)
        assertEquals(3, multipleNoValueAnnotations.size)
        assertNull(multipleNoValueAnnotations["nullable"])
        assertNull(multipleNoValueAnnotations["readonly"])
        assertNull(multipleNoValueAnnotations["deprecated"])
    }

    @Test
    @DisplayName("Test parsing annotations with complex values")
    fun testParseAnnotationsWithComplexValues() {
        // Test with a fully qualified class name
        val complexComment = "-- @@propertyType=kotlinx.datetime.LocalDate"
        val complexAnnotations = AnnotatedStatementInfo.parseAnnotations(complexComment)
        assertEquals(1, complexAnnotations.size)
        assertEquals("kotlinx.datetime.LocalDate", complexAnnotations["propertyType"])

        // Test with a boolean value
        val booleanComment = "-- @@isNullable=true"
        val booleanAnnotations = AnnotatedStatementInfo.parseAnnotations(booleanComment)
        assertEquals(1, booleanAnnotations.size)
        assertEquals("true", booleanAnnotations["isNullable"])

        // Test with a numeric value
        val numericComment = "-- @@maxLength=255"
        val numericAnnotations = AnnotatedStatementInfo.parseAnnotations(numericComment)
        assertEquals(1, numericAnnotations.size)
        assertEquals("255", numericAnnotations["maxLength"])
    }
}
