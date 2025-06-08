package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue

class QueriesCodeGeneratorTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun `test generate namespace code with empty Queries object`() {
        // Create a temporary directory for SQL files
        val sqlDir = File(tempDir, "sql").apply { mkdirs() }
        val personDir = File(sqlDir, "person").apply { mkdirs() }

        // Create a SQL file
        val sqlFile = File(personDir, "get_person.sql")
        sqlFile.writeText("""
            -- @@className=GetPerson
            SELECT id, name, email
            FROM person
            WHERE id = :id;
        """.trimIndent())

        // Create an in-memory SQLite database
        val connection = DriverManager.getConnection("jdbc:sqlite::memory:")
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT
                )
            """)
        }

        // Create the output directory
        val outputDir = File(tempDir, "output").apply { mkdirs() }

        // Create the code generator
        val codeGenerator = QueriesCodeGenerator(
            queriesDir = sqlDir,
            tables = emptyList(),
            connection = connection
        )

        // Generate the code
        codeGenerator.generateAllCode(
            packageName = "dev.goquick.sqlitenow.test",
            outputDir = outputDir
        )

        // Verify the generated file exists
        val packagePath = "dev/goquick/sqlitenow/test"
        val generatedFile = File(outputDir, "$packagePath/Person.kt")
        assertTrue(generatedFile.exists(), "Generated file should exist")

        // Read the generated code
        val generatedCode = generatedFile.readText()
        println("Generated code:\n$generatedCode")

        // Verify the generated code contains the expected content
        assertTrue(generatedCode.contains("public object Person"), "Generated code should contain the object declaration")
        assertTrue(generatedCode.contains("public object Result"), "Generated code should contain the Result object")
        assertTrue(generatedCode.contains("public object Queries"), "Generated code should contain the empty Queries object")
        assertTrue(generatedCode.contains("data class GetPerson"), "Generated code should contain the data class")
    }
}
