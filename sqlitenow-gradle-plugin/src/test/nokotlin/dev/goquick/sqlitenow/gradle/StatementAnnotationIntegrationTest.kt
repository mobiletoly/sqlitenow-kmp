package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.introsqlite.DatabaseIntrospector
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StatementAnnotationIntegrationTest {
    private lateinit var connection: Connection
    private lateinit var tables: List<dev.goquick.sqlitenow.gradle.introsqlite.DatabaseTable>

    @BeforeEach
    fun setUp() {
        // Create an in-memory database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create a test table with annotations in comments
        val createTableSql = """
            -- @@className=PersonEntity

            CREATE TABLE Person (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE,
                phone TEXT,

                -- @@propertyType=kotlinx.datetime.LocalDate @@propertyName=birthDate
                birth_date TEXT,

                created_at TEXT DEFAULT current_timestamp
            )
        """.trimIndent()

        println("SQL statement:\n$createTableSql")

        connection.createStatement().use { stmt ->
            stmt.executeUpdate(createTableSql)
        }

        // Initialize the code generator
        val introspector = DatabaseIntrospector(connection)

        // Get the SQL for the Person table
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("""
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'Person'
            """)

            if (rs.next()) {
                val tableSql = rs.getString("sql")
                println("Table SQL from sqlite_master:\n$tableSql")
            }
        }

        tables = introspector.getTables()
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test that annotations are correctly extracted from SQL comments")
    fun testAnnotationsExtraction() {
        // Create a simple SQL query to test with
        val sql = """
            -- @@className=PersonData
            SELECT id, first_name, last_name, email, phone, birth_date, created_at
            FROM Person
        """.trimIndent()

        // Create a code generator with the SQL query
        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, tables)

        // Verify the annotations were extracted correctly
        assertEquals("PersonData", codeGenerator.annotatedStatement.topLevelAnnotations["className"], "className annotation should be extracted")

        // Generate code and verify it uses the class name from the annotation
        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()
        assertTrue(generatedCode.contains("class PersonData"), "Generated code should use the class name from the annotation")
    }
}
