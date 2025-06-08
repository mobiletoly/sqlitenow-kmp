package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertTrue

class DatabaseCodeGeneratorTest {

    @TempDir
    lateinit var outputDir: File

    @Test
    fun `test database class generation`() {
        // Arrange
        val dbName = "now_sample_database"
        val packageName = "dev.goquick.sqlitenow.test"
        val tables = emptyList<dev.goquick.sqlitenow.gradle.introsqlite.DatabaseTable>()

        // Act
        val generator = DatabaseCodeGenerator(dbName, packageName, outputDir, tables)
        generator.generateCode()

        // Assert
        val packagePath = "dev/goquick/sqlitenow/test"
        val generatedFile = File(outputDir, "$packagePath/NowSampleDatabase.kt")
        assertTrue(generatedFile.exists(), "Generated file should exist")

        val generatedCode = generatedFile.readText()
        println("Generated code:\n$generatedCode")

        assertTrue(generatedCode.contains("public class NowSampleDatabase"), "Generated code should contain the class declaration")
        assertTrue(generatedCode.contains("private val conn: SQLiteConnection"), "Generated code should contain the conn property")
        assertTrue(generatedCode.contains("public constructor("), "Generated code should contain the constructor")
        assertTrue(generatedCode.contains("this.conn = conn"), "Generated code should initialize the conn property")
        assertTrue(generatedCode.contains("import androidx.sqlite.SQLiteConnection"), "Generated code should import SQLiteConnection")
    }
}
