package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MigrationInspectorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var migrationDir: File

    @BeforeEach
    fun setup() {
        migrationDir = File(tempDir.toFile(), "migrations")
        migrationDir.mkdir()
    }

    @Test
    @DisplayName("Test MigrationInspector with numbered migration files")
    fun testInspectWithNumberedMigrations() {
        // Create migration files with version numbers
        val migration1 = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
        """.trimIndent()

        val migration2 = """
            ALTER TABLE users ADD COLUMN email TEXT;
        """.trimIndent()

        val migration3 = """
            CREATE INDEX idx_users_email ON users(email);
        """.trimIndent()

        // Create files with version numbers
        File(migrationDir, "0001.sql").writeText(migration1)
        File(migrationDir, "0002.sql").writeText(migration2)
        File(migrationDir, "0005.sql").writeText(migration3)

        // Create MigrationInspector
        val inspector = MigrationInspector(migrationDir)

        // Verify that statements are partitioned by version
        assertEquals(3, inspector.sqlStatements.size, "Should have 3 versions")
        assertTrue(inspector.sqlStatements.containsKey(1), "Should contain version 1")
        assertTrue(inspector.sqlStatements.containsKey(2), "Should contain version 2")
        assertTrue(inspector.sqlStatements.containsKey(5), "Should contain version 5")

        // Verify that the latest version is correctly identified
        assertEquals(5, inspector.latestVersion, "Latest version should be 5")

        // Verify that each version has the correct statements
        assertEquals(1, inspector.sqlStatements[1]?.size, "Version 1 should have 1 statement")
        assertEquals(1, inspector.sqlStatements[2]?.size, "Version 2 should have 1 statement")
        assertEquals(1, inspector.sqlStatements[5]?.size, "Version 5 should have 1 statement")

        // Verify statement content
        assertTrue(inspector.sqlStatements[1]?.get(0)?.sql?.contains("CREATE TABLE users") == true, "Version 1 should contain users table creation")
        assertTrue(inspector.sqlStatements[2]?.get(0)?.sql?.contains("ALTER TABLE users") == true, "Version 2 should contain alter table statement")
        assertTrue(inspector.sqlStatements[5]?.get(0)?.sql?.contains("CREATE INDEX") == true, "Version 5 should contain index creation")
    }

    @Test
    @DisplayName("Test MigrationInspector with zero-padded version numbers")
    fun testInspectWithZeroPaddedVersions() {
        // Create files with different zero-padding
        File(migrationDir, "0001.sql").writeText("CREATE TABLE test1 (id INTEGER);")
        File(migrationDir, "0010.sql").writeText("CREATE TABLE test2 (id INTEGER);")
        File(migrationDir, "0100.sql").writeText("CREATE TABLE test3 (id INTEGER);")

        val inspector = MigrationInspector(migrationDir)

        assertEquals(3, inspector.sqlStatements.size, "Should have 3 versions")
        assertEquals(100, inspector.latestVersion, "Latest version should be 100")
    }

    @Test
    @DisplayName("Test MigrationInspector with invalid filename formats")
    fun testInspectWithInvalidFilenameFormats() {
        // Create files with valid version numbers
        File(migrationDir, "0001.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "0003.sql").writeText("CREATE TABLE posts (id INTEGER);")

        // Create files with invalid version formats (should cause exception)
        File(migrationDir, "init.sql").writeText("INSERT INTO users VALUES (1);")

        // Should throw exception due to invalid filename
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
    }

    @Test
    @DisplayName("Test MigrationInspector with no numbered files")
    fun testInspectWithNoNumberedFiles() {
        // Create files without version numbers (should cause exception)
        File(migrationDir, "init.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "seed.sql").writeText("INSERT INTO users VALUES (1);")

        // Should throw exception due to non-numeric filenames
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
    }

    @Test
    @DisplayName("Test MigrationInspector with single migration file")
    fun testInspectWithSingleMigration() {
        File(migrationDir, "0042.sql").writeText("CREATE TABLE answer (id INTEGER);")

        val inspector = MigrationInspector(migrationDir)

        assertEquals(1, inspector.sqlStatements.size, "Should have 1 version")
        assertEquals(42, inspector.latestVersion, "Latest version should be 42")
    }

    @Test
    @DisplayName("Test MigrationInspector with non-sequential version numbers")
    fun testInspectWithNonSequentialVersions() {
        // Create files with non-sequential version numbers
        File(migrationDir, "0001.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "0005.sql").writeText("CREATE TABLE posts (id INTEGER);")
        File(migrationDir, "0003.sql").writeText("CREATE TABLE comments (id INTEGER);")
        File(migrationDir, "0010.sql").writeText("CREATE TABLE tags (id INTEGER);")

        val inspector = MigrationInspector(migrationDir)

        assertEquals(4, inspector.sqlStatements.size, "Should have 4 versions")
        assertEquals(10, inspector.latestVersion, "Latest version should be 10 (highest number)")
    }

    @Test
    @DisplayName("Test MigrationInspector with version numbers without zero-padding")
    fun testInspectWithNonPaddedVersions() {
        // Create files with version numbers without zero-padding
        File(migrationDir, "1.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "5.sql").writeText("CREATE TABLE posts (id INTEGER);")
        File(migrationDir, "15.sql").writeText("CREATE TABLE comments (id INTEGER);")

        val inspector = MigrationInspector(migrationDir)

        assertEquals(3, inspector.sqlStatements.size, "Should have 3 versions")
        assertEquals(15, inspector.latestVersion, "Latest version should be 15")
    }

    @Test
    @DisplayName("Test MigrationInspector with empty directory")
    fun testInspectWithEmptyDirectory() {
        // Empty directory is now valid (no SQL files is OK)
        val inspector = MigrationInspector(migrationDir)

        assertEquals(0, inspector.sqlStatements.size, "Should have no versions")
        assertEquals(0, inspector.latestVersion, "Latest version should be 0 when no files exist")
    }

    @Test
    @DisplayName("Test MigrationInspector with valid numeric filenames only")
    fun testValidNumericFilenamesOnly() {
        // Test various valid numeric filename formats
        File(migrationDir, "0001.sql").writeText("CREATE TABLE test1 (id INTEGER);")
        File(migrationDir, "0002.sql").writeText("CREATE TABLE test2 (id INTEGER);")
        File(migrationDir, "0010.sql").writeText("CREATE TABLE test3 (id INTEGER);")
        File(migrationDir, "0099.sql").writeText("CREATE TABLE test4 (id INTEGER);")
        File(migrationDir, "1000.sql").writeText("CREATE TABLE test5 (id INTEGER);")

        val inspector = MigrationInspector(migrationDir)

        // All files should be collected as versions
        assertEquals(5, inspector.sqlStatements.size, "Should have 5 versions")

        // Latest version should be the highest number
        assertEquals(1000, inspector.latestVersion, "Latest version should be 1000")
    }

    @Test
    @DisplayName("Test MigrationInspector with invalid non-numeric filenames")
    fun testInvalidNonNumericFilenames() {
        // Create valid files
        File(migrationDir, "0001.sql").writeText("CREATE TABLE test1 (id INTEGER);")

        // Create invalid files (should cause exception)
        File(migrationDir, "v001.sql").writeText("CREATE TABLE ignored1 (id INTEGER);")

        // Should throw exception due to invalid filename
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
    }

    @Test
    @DisplayName("Test MigrationInspector with duplicate version numbers")
    fun testDuplicateVersionNumbers() {
        // Create files with the same version number (different padding)
        File(migrationDir, "001.sql").writeText("CREATE TABLE test1 (id INTEGER);")
        File(migrationDir, "0001.sql").writeText("CREATE TABLE test2 (id INTEGER);")

        // Should throw exception due to duplicate version
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
    }
}
