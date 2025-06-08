package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MigratorCodeGeneratorTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun `test generate migration code`() {
        // Create a temporary migration directory with a SQL file
        val migrationsDir = File(tempDir, "migrations").apply { mkdirs() }
        val sqlFile = File(migrationsDir, "0001.sql")
        sqlFile.writeText("""
            -- Create a test table
            CREATE TABLE IF NOT EXISTS TestTable (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            );

            -- Insert some test data
            INSERT INTO TestTable (id, name, value) VALUES (1, 'Test 1', 100);
            INSERT INTO TestTable (id, name, value) VALUES (2, 'Test 2', 200);
        """.trimIndent())

        // Create a migrator
        val migrator = Migrator(
            migrationDir = migrationsDir,
            storage = MigratorTempStorage.Memory,
            logIntrospectionResults = false
        )

        // Create the output directory
        val outputDir = File(tempDir, "output").apply { mkdirs() }

        // Create the code generator
        val codeGenerator = MigratorCodeGenerator(
            migrator = migrator,
            packageName = "dev.goquick.sqlitenow.test",
            outputDir = outputDir
        )

        // Generate the code
        val fileSpec = codeGenerator.generateCode("TestVersionBasedDatabaseMigrations")

        // Verify the generated file exists
        // KotlinPoet creates the directory structure based on the package name
        val packagePath = "dev/goquick/sqlitenow/test"
        val generatedFile = File(outputDir, "$packagePath/TestVersionBasedDatabaseMigrations.kt")
        println("Looking for file: ${generatedFile.absolutePath}")
        println("Output directory exists: ${outputDir.exists()}")
        println("Output directory contents: ${outputDir.listFiles()?.joinToString(", ") { it.name } ?: "empty"}")
        assertTrue(generatedFile.exists(), "Generated file should exist")

        // Read the generated code
        val generatedCode = generatedFile.readText()
        println("Generated code:\n$generatedCode")

        // Verify the generated code contains the expected content
        assertTrue(generatedCode.contains("public class TestVersionBasedDatabaseMigrations"), "Generated code should contain the class declaration")
        assertTrue(generatedCode.contains("import dev.goquick.sqlitenow.core.DatabaseMigrations"), "Generated code should import DatabaseMigrations interface")
        assertTrue(generatedCode.contains("DatabaseMigrations"), "Generated code should implement DatabaseMigrations interface")
        assertFalse(generatedCode.contains("internal fun applyMigrations"), "Generated code should NOT contain the applyMigrations function")
        assertTrue(generatedCode.contains("override fun applyMigration"), "Generated code should contain the applyMigration function")
        assertTrue(generatedCode.contains("private fun applyMigrationVersion0001"), "Generated code should contain the version function")
        assertTrue(generatedCode.contains("CREATE TABLE IF NOT EXISTS TestTable"), "Generated code should contain the SQL statement")
        assertTrue(generatedCode.contains("INSERT INTO TestTable"), "Generated code should contain the INSERT statements")
        assertTrue(generatedCode.contains("import androidx.sqlite.SQLiteConnection"), "Generated code should import SQLiteConnection")
        assertTrue(generatedCode.contains("import androidx.sqlite.execSQL"), "Generated code should import execSQL extension function")
        assertTrue(generatedCode.contains("conn.execSQL"), "Generated code should use conn.execSQL")
        assertTrue(generatedCode.contains("currentVersion"), "Generated code should use currentVersion parameter")
        assertTrue(generatedCode.contains("if (currentVersion <"), "Generated code should use if statements for version checking")
    }
}
