package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import java.sql.DriverManager

/**
 * This test demonstrates how to use the generateInputParametersCode function.
 */
class GenerateInputParamsTest {

    @Test
    fun testGenerateInputParams() {
        // Create an in-memory SQLite database
        val connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the Person table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT
                )
            """)
        }

        // The SQL query
        val sql = """
            -- @@className=SelectPersonWithAddress
            SELECT
                p.id AS person_id,
                p.first_name,
                p.last_name,
                p.email
            FROM Person p
            WHERE p.first_name LIKE :firstName
                AND p.last_name LIKE :lastName;
        """.trimIndent()

        // Generate the input parameters class
        val codeGenerator = StatementKotlinCodeGenerator(
            sql = sql,
            connection = connection,
            tables = emptyList()
        )

        val fileSpecBuilder = codeGenerator.generateInputParametersCode("SelectPersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        // Print the generated code
        println("\nGenerated input parameters class:")
        println(generatedCode)

        // Close the connection
        connection.close()
    }
}
