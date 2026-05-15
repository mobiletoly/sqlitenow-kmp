package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.database.MigrationInspector
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MigrationInspectorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var migrationDir: File

    @BeforeEach
    fun setup() {
        migrationDir = File(tempDir.toFile(), "migrations")
        migrationDir.mkdir()
    }

    @TestFactory
    @DisplayName("Valid migration filenames are inspected")
    fun validMigrationFilenamesAreInspected(): List<DynamicTest> = listOf(
        ValidMigrationCase(
            displayName = "numbered migration files",
            files = mapOf(
                "0001.sql" to """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """.trimIndent(),
                "0002.sql" to "ALTER TABLE users ADD COLUMN email TEXT;",
                "0005.sql" to "CREATE INDEX idx_users_email ON users(email);",
            ),
            expectedVersions = listOf(1, 2, 5),
            expectedLatestVersion = 5,
            expectedStatementSnippets = mapOf(
                1 to "CREATE TABLE users",
                2 to "ALTER TABLE users",
                5 to "CREATE INDEX",
            ),
        ),
        ValidMigrationCase(
            displayName = "zero-padded version numbers",
            files = mapOf(
                "0001.sql" to "CREATE TABLE test1 (id INTEGER);",
                "0010.sql" to "CREATE TABLE test2 (id INTEGER);",
                "0100.sql" to "CREATE TABLE test3 (id INTEGER);",
            ),
            expectedVersions = listOf(1, 10, 100),
            expectedLatestVersion = 100,
        ),
        ValidMigrationCase(
            displayName = "single migration file",
            files = mapOf("0042.sql" to "CREATE TABLE answer (id INTEGER);"),
            expectedVersions = listOf(42),
            expectedLatestVersion = 42,
        ),
        ValidMigrationCase(
            displayName = "non-sequential version numbers",
            files = mapOf(
                "0001.sql" to "CREATE TABLE users (id INTEGER);",
                "0005.sql" to "CREATE TABLE posts (id INTEGER);",
                "0003.sql" to "CREATE TABLE comments (id INTEGER);",
                "0010.sql" to "CREATE TABLE tags (id INTEGER);",
            ),
            expectedVersions = listOf(1, 3, 5, 10),
            expectedLatestVersion = 10,
        ),
        ValidMigrationCase(
            displayName = "empty directory",
            files = emptyMap(),
            expectedVersions = emptyList(),
            expectedLatestVersion = 0,
        ),
        ValidMigrationCase(
            displayName = "valid 4-digit numeric filenames",
            files = mapOf(
                "0001.sql" to "CREATE TABLE test1 (id INTEGER);",
                "0002.sql" to "CREATE TABLE test2 (id INTEGER);",
                "0010.sql" to "CREATE TABLE test3 (id INTEGER);",
                "0099.sql" to "CREATE TABLE test4 (id INTEGER);",
                "1000.sql" to "CREATE TABLE test5 (id INTEGER);",
            ),
            expectedVersions = listOf(1, 2, 10, 99, 1000),
            expectedLatestVersion = 1000,
        ),
        ValidMigrationCase(
            displayName = "descriptive filenames",
            files = mapOf(
                "0001_create_users.sql" to """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """.trimIndent(),
                "0002_add_email_column.sql" to "ALTER TABLE users ADD COLUMN email TEXT;",
                "0005_create_email_index.sql" to "CREATE INDEX idx_users_email ON users(email);",
            ),
            expectedVersions = listOf(1, 2, 5),
            expectedLatestVersion = 5,
            expectedStatementSnippets = mapOf(
                1 to "CREATE TABLE users",
                2 to "ALTER TABLE users",
                5 to "CREATE INDEX",
            ),
        ),
        ValidMigrationCase(
            displayName = "mixed descriptive and plain filenames",
            files = mapOf(
                "0001.sql" to "CREATE TABLE users (id INTEGER);",
                "0002_add_posts.sql" to "CREATE TABLE posts (id INTEGER);",
                "0003.sql" to "CREATE TABLE comments (id INTEGER);",
                "0010_create_indexes.sql" to "CREATE INDEX idx_posts ON posts(id);",
            ),
            expectedVersions = listOf(1, 2, 3, 10),
            expectedLatestVersion = 10,
        ),
        ValidMigrationCase(
            displayName = "4-digit versions only",
            files = mapOf(
                "0001.sql" to "CREATE TABLE users (id INTEGER);",
                "0010.sql" to "CREATE TABLE posts (id INTEGER);",
                "0100.sql" to "CREATE TABLE comments (id INTEGER);",
                "1000.sql" to "CREATE TABLE tags (id INTEGER);",
            ),
            expectedVersions = listOf(1, 10, 100, 1000),
            expectedLatestVersion = 1000,
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val inspector = inspectMigrations(case.displayName, case.files)

            assertEquals(case.expectedVersions.size, inspector.sqlStatements.size, "Should have ${case.expectedVersions.size} versions")
            assertEquals(case.expectedLatestVersion, inspector.latestVersion, "Latest version should be ${case.expectedLatestVersion}")
            assertEquals(case.expectedVersions, inspector.sqlStatements.keys.sorted(), "Versions should be parsed in numeric order")
            assertStatementSnippets(inspector, case.expectedStatementSnippets)
        }
    }

    @TestFactory
    @DisplayName("Invalid migration filenames fail")
    fun invalidMigrationFilenamesFail(): List<DynamicTest> = listOf(
        InvalidMigrationCase(
            displayName = "invalid filename mixed with valid migrations",
            files = mapOf(
                "0001.sql" to "CREATE TABLE users (id INTEGER);",
                "0003.sql" to "CREATE TABLE posts (id INTEGER);",
                "init.sql" to "INSERT INTO users VALUES (1);",
            ),
        ),
        InvalidMigrationCase(
            displayName = "no numbered files",
            files = mapOf(
                "init.sql" to "CREATE TABLE users (id INTEGER);",
                "seed.sql" to "INSERT INTO users VALUES (1);",
            ),
        ),
        InvalidMigrationCase(
            displayName = "non-padded versions",
            files = mapOf(
                "1.sql" to "CREATE TABLE users (id INTEGER);",
                "5.sql" to "CREATE TABLE posts (id INTEGER);",
                "15.sql" to "CREATE TABLE comments (id INTEGER);",
            ),
        ),
        InvalidMigrationCase(
            displayName = "non-numeric filename",
            files = mapOf(
                "0001.sql" to "CREATE TABLE test1 (id INTEGER);",
                "v001.sql" to "CREATE TABLE ignored1 (id INTEGER);",
            ),
        ),
        InvalidMigrationCase(
            displayName = "duplicate version numbers",
            files = mapOf(
                "0001.sql" to "CREATE TABLE test1 (id INTEGER);",
                "0001_duplicate.sql" to "CREATE TABLE test2 (id INTEGER);",
            ),
        ),
        InvalidMigrationCase(
            displayName = "invalid descriptive filename",
            files = mapOf(
                "0001_create_users.sql" to "CREATE TABLE users (id INTEGER);",
                "001_invalid.sql" to "CREATE TABLE ignored (id INTEGER);",
            ),
        ),
        InvalidMigrationCase(
            displayName = "non-4-digit versions",
            files = mapOf(
                "1.sql" to "CREATE TABLE users (id INTEGER);",
                "12.sql" to "CREATE TABLE posts (id INTEGER);",
                "123.sql" to "CREATE TABLE comments (id INTEGER);",
            ),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            assertFailsWith<IllegalArgumentException> {
                inspectMigrations(case.displayName, case.files)
            }
        }
    }

    private fun inspectMigrations(displayName: String, files: Map<String, String>): MigrationInspector {
        val caseDir = caseDirectory(displayName)
        files.forEach { (name, sql) -> File(caseDir, name).writeText(sql) }
        return MigrationInspector(caseDir)
    }

    private fun caseDirectory(displayName: String): File =
        File(migrationDir, displayName.lowercase().replace(Regex("[^a-z0-9]+"), "_").trim('_')).apply {
            mkdirs()
        }

    private fun assertStatementSnippets(inspector: MigrationInspector, expectedSnippets: Map<Int, String>) {
        expectedSnippets.forEach { (version, snippet) ->
            assertTrue(
                inspector.sqlStatements[version]?.firstOrNull()?.sql?.contains(snippet) == true,
                "Version $version should contain $snippet",
            )
        }
    }

    private data class ValidMigrationCase(
        val displayName: String,
        val files: Map<String, String>,
        val expectedVersions: List<Int>,
        val expectedLatestVersion: Int,
        val expectedStatementSnippets: Map<Int, String> = emptyMap(),
    )

    private data class InvalidMigrationCase(
        val displayName: String,
        val files: Map<String, String>,
    )
}
