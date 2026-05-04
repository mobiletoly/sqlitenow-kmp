package dev.goquick.sqlitenow.gradle

import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqlFileProcessorTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("findSqlFiles discovers SQL files case-insensitively and sorts them by filename")
    fun findSqlFilesDiscoversSqlFilesCaseInsensitively() {
        val directory = tempDir.resolve("sql").toFile().apply { mkdirs() }
        File(directory, "b.SQL").writeText("INSERT INTO person VALUES (2);")
        File(directory, "a.sql").writeText("CREATE TABLE person (id INTEGER);")
        File(directory, "ignore.txt").writeText("ignore")

        val sqlFiles = SqlFileProcessor.findSqlFiles(directory)

        assertEquals(listOf("a.sql", "b.SQL"), sqlFiles.map { it.name })
    }

    @Test
    @DisplayName("parseAllSqlFilesInDirectory preserves file ordering across mixed-case SQL extensions")
    fun parseAllSqlFilesInDirectoryPreservesFileOrder() {
        val directory = tempDir.resolve("sql-order").toFile().apply { mkdirs() }
        File(directory, "02.SQL").writeText("INSERT INTO person VALUES (2);")
        File(directory, "01.sql").writeText("CREATE TABLE person (id INTEGER);")

        val statements = SqlFileProcessor.parseAllSqlFilesInDirectory(directory)

        assertEquals(2, statements.size)
        assertTrue(statements[0].sql.contains("CREATE TABLE person"))
        assertTrue(statements[1].sql.contains("INSERT INTO person"))
    }
}
