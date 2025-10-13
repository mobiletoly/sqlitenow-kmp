/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        val executeStatement = updateStatement as AnnotatedExecuteStatement
        assertTrue(executeStatement.src is UpdateStatement,
            "Should be an UpdateStatement")

        // Clean up
        tempDir.toFile().deleteRecursively()
    }


}
