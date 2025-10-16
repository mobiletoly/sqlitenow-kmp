package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertTrue

class UpdateStatementTest {

    @Test
    @DisplayName("Test UPDATE statement basic parsing")
    fun testUpdateStatementBasicParsing() {
        // Create a temporary directory for SQL files
        val tempDir = Files.createTempDirectory("test-sql")
        val queriesDir = tempDir.resolve("queries").resolve("person").toFile()
        queriesDir.mkdirs()

        // Create a simple UPDATE SQL file
        val updateSqlFile = File(queriesDir, "updateAge.sql")
        updateSqlFile.writeText("UPDATE Person SET age = :newAge WHERE id = :personId;")

        // Create an in-memory SQLite database for testing
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the Person table
        conn.createStatement().execute("""
            CREATE TABLE person (
                id INTEGER PRIMARY KEY,
                age INTEGER
            )
        """.trimIndent())

        // Test that the UPDATE statement can be parsed without errors
        val stmtProcessingHelper = StatementProcessingHelper(conn)
        val nsWithStatements = stmtProcessingHelper.processQueriesDirectory(queriesDir.parentFile)

        // Verify that the UPDATE statement was processed
        assertTrue(nsWithStatements.containsKey("person"), "Should contain person namespace")
        val personStatements = nsWithStatements["person"]!!
        assertTrue(personStatements.isNotEmpty(), "Should have at least one statement")

        val updateStatement = personStatements.find { it.name == "updateAge" }
        assertTrue(updateStatement != null, "Should find updateAge statement")
        assertTrue(updateStatement is AnnotatedExecuteStatement, "Should be an AnnotatedExecuteStatement")

        val executeStatement = updateStatement
        assertTrue(executeStatement.src is UpdateStatement,
            "Should be an UpdateStatement")

        // Clean up
        tempDir.toFile().deleteRecursively()
    }


}
