package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue

class StatementMergedAnnotationsTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create a test table with annotations in comments
        val createTableSql = """
            CREATE TABLE IF NOT EXISTS Person (
                id INTEGER PRIMARY KEY NOT NULL,

                -- @@propertyName=myFirstName
                first_name VARCHAR NOT NULL,

                -- @@propertyName=myLastName
                last_name TEXT NOT NULL,

                email TEXT NOT NULL UNIQUE,
                phone TEXT,

                birth_date TEXT,

                -- @@propertyType=java.time.Instant @@nonNull
                created_at TEXT NOT NULL DEFAULT current_timestamp
            );

            CREATE TABLE IF NOT EXISTS PersonAddress (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,

                -- @@propertyType=dev.goquick.sqlitenow.samplekmp.model.AddressType
                address_type TEXT NOT NULL,

                street TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT,
                postal_code TEXT,
                country TEXT NOT NULL,

                -- @@propertyType=Boolean
                is_primary INTEGER NOT NULL DEFAULT 0,

                -- @@propertyType=kotlinx.datetime.LocalDate
                created_at TEXT NOT NULL DEFAULT current_timestamp,

                FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
            );
        """.trimIndent()

        connection.createStatement().use { stmt ->
            stmt.executeUpdate(createTableSql)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test merging annotations from CREATE TABLE and SELECT query")
    fun testMergeAnnotations() {
        // Define a SELECT query with annotations that should override some of the CREATE TABLE annotations
        val selectSql = """
            -- @@className=SelectPersonWithAddress
            SELECT
                p.id AS person_id,

                -- @@field=total_person_count @@nonNull
                (SELECT count(*) FROM Person) AS total_person_count,

                -- @@field=first_name @@propertyName=firstName @@nullable
                p.first_name,

                p.last_name, p.email, p.phone, p.birth_date,
                p.created_at AS person_created_at,
                a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                a.is_primary,

                -- @@field=address_created_at @@propertyType=kotlinx.datetime.LocalDate
                a.created_at AS address_created_at,

                -- @@field=full_name @@nonNull @@propertyName=fullName @@propertyType=String
                p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
                JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
                AND p.last_name LIKE :lastName;
        """.trimIndent()

        // Generate code using the SELECT query
        val introspector = dev.goquick.sqlitenow.gradle.introsqlite.DatabaseIntrospector(connection)
        val tables = introspector.getTables()
        val codeGenerator = StatementKotlinCodeGenerator(selectSql, connection, tables)
        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("Generated code with merged annotations:")
        println(generatedCode)

        // Verify class name from SELECT query annotation
        assertTrue(generatedCode.contains("class SelectPersonWithAddress"),
            "Generated code should use the class name from the SELECT query annotation")

        // Verify property name from SELECT query annotation (overrides CREATE TABLE annotation)
        assertTrue(generatedCode.contains("val firstName"),
            "Generated code should use the property name from the SELECT query annotation (firstName)")

        // Verify property name from CREATE TABLE annotation (not overridden in SELECT query)
        assertTrue(generatedCode.contains("val myLastName"),
            "Generated code should use the property name from the CREATE TABLE annotation (myLastName)")

        // Verify property type from SELECT query annotation
        assertTrue(generatedCode.contains("val fullName: String"),
            "Generated code should use the property type from the SELECT query annotation for fullName")

        // Verify property type from CREATE TABLE annotation (not overridden in SELECT query)
        assertTrue(generatedCode.contains("val isPrimary"),
            "Generated code should use the property type from the CREATE TABLE annotation")

        // Verify property type from SELECT query annotation (overrides CREATE TABLE annotation)
        assertTrue(generatedCode.contains("val addressCreatedAt: LocalDate"),
            "Generated code should use the property type from the SELECT query annotation")
    }
}
