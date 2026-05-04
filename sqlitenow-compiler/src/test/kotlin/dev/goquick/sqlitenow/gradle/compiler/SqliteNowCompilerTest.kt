package dev.goquick.sqlitenow.gradle.compiler

import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqliteNowCompilerTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun compilerReturnsGeneratedFilesWarningsAndDiagnostics() {
        val sqlDir = tempDir.resolve("sql/TestDatabase")
        sqlDir.resolve("schema/docs.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    -- @@{enableSync=true}
                    CREATE TABLE docs (
                        id TEXT PRIMARY KEY NOT NULL,
                        title TEXT NOT NULL
                    );
                """.trimIndent()
            )
        }
        sqlDir.resolve("queries/docs/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    SELECT *
                    FROM docs
                    ORDER BY id;
                """.trimIndent()
            )
        }

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "TestDatabase",
                sqlDirectory = sqlDir,
                packageName = "com.test.db",
                outputDirectory = tempDir.resolve("generated"),
                oversqlite = false,
            )
        )

        assertTrue(
            result.generatedFiles.any { it.name == "TestDatabase.kt" },
            "Compiler result should report generated database source files"
        )
        assertTrue(result.generatedFiles.all { it.isFile })
        assertEquals(1, result.warnings.size)
        assertTrue(result.warnings.single().contains("oversqlite=false"))
        assertEquals(1, result.diagnostics.size)
        assertEquals(SqliteNowCompilerDiagnosticSeverity.WARNING, result.diagnostics.single().severity)
        assertEquals(result.warnings.single(), result.diagnostics.single().message)
    }
}
