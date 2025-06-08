package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue

class StatementKotlinCodeGeneratorAnnotationTest {
    private lateinit var connection: Connection
    private lateinit var codeGenerator: StatementKotlinCodeGenerator
    private lateinit var tables: List<dev.goquick.sqlitenow.gradle.introsqlite.DatabaseTable>

    @BeforeEach
    fun setUp() {
        // Create an in-memory database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test tables
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
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

        // Introspect the database to get table information
        val introspector = dev.goquick.sqlitenow.gradle.introsqlite.DatabaseIntrospector(connection)
        tables = introspector.getTables()
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test generating a data class with className annotation")
    fun testGenerateDataClassWithClassNameAnnotation() {
        val sql = """
            -- @@className=SelectPersonWithAddress

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

        val sqlCodeGenerator = StatementKotlinCodeGenerator(sql, connection, tables)
        val fileSpecBuilder = sqlCodeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        // Print the generated code for debugging
        println("Generated code with className annotation:\n$generatedCode")

        // Verify the class name is taken from the annotation
        assertTrue(generatedCode.contains("class SelectPersonWithAddress"), "Class name should be taken from annotation")
    }

    @Test
    @DisplayName("Test generating a data class with multiple inline comment annotations")
    fun testGenerateDataClassWithMultipleInlineCommentAnnotations() {
        val sql = """
            -- @@className=PersonWithAddressData
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

        val sqlCodeGenerator = StatementKotlinCodeGenerator(sql, connection, tables)
        val fileSpecBuilder = sqlCodeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        // Print the generated code for debugging
        println("Generated code with block comment annotations:\n$generatedCode")

        // Verify the class name is taken from the annotation
        assertTrue(generatedCode.contains("class PersonWithAddressData"), "Class name should be taken from annotation")
    }

    @Test
    @DisplayName("Test generating a data class with multiple different inline comment annotations")
    fun testGenerateDataClassWithMultipleDifferentInlineCommentAnnotations() {
        val sql = """
            -- @@className=MixedCommentClass
            -- @@extractable
            -- @extended
            -- This is a regular comment

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

        val sqlCodeGenerator = StatementKotlinCodeGenerator(sql, connection, tables)
        val fileSpecBuilder = sqlCodeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        // Print the generated code for debugging
        println("Generated code with mixed comment annotations:\n$generatedCode")

        // Verify the class name is taken from the annotation
        assertTrue(generatedCode.contains("class MixedCommentClass"), "Class name should be taken from annotation")
    }
}
