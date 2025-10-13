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

import dev.goquick.sqlitenow.gradle.database.MigrationInspector
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
        // Create files with version numbers without zero-padding (should fail with new 4-digit requirement)
        File(migrationDir, "1.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "5.sql").writeText("CREATE TABLE posts (id INTEGER);")
        File(migrationDir, "15.sql").writeText("CREATE TABLE comments (id INTEGER);")

        // Should throw exception due to non-4-digit version numbers
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
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
    @DisplayName("Test MigrationInspector with valid 4-digit numeric filenames only")
    fun testValid4DigitNumericFilenamesOnly() {
        // Test various valid 4-digit numeric filename formats
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

    @Test
    @DisplayName("Test MigrationInspector with descriptive filenames")
    fun testInspectWithDescriptiveFilenames() {
        // Create migration files with descriptive names
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

        // Create files with descriptive names
        File(migrationDir, "0001_create_users.sql").writeText(migration1)
        File(migrationDir, "0002_add_email_column.sql").writeText(migration2)
        File(migrationDir, "0005_create_email_index.sql").writeText(migration3)

        // Create MigrationInspector
        val inspector = MigrationInspector(migrationDir)

        // Verify that statements are partitioned by version (ignoring descriptions)
        assertEquals(3, inspector.sqlStatements.size, "Should have 3 versions")
        assertTrue(inspector.sqlStatements.containsKey(1), "Should contain version 1")
        assertTrue(inspector.sqlStatements.containsKey(2), "Should contain version 2")
        assertTrue(inspector.sqlStatements.containsKey(5), "Should contain version 5")

        // Verify that the latest version is correctly identified
        assertEquals(5, inspector.latestVersion, "Latest version should be 5")

        // Verify statement content
        assertTrue(inspector.sqlStatements[1]?.get(0)?.sql?.contains("CREATE TABLE users") == true, "Version 1 should contain users table creation")
        assertTrue(inspector.sqlStatements[2]?.get(0)?.sql?.contains("ALTER TABLE users") == true, "Version 2 should contain alter table statement")
        assertTrue(inspector.sqlStatements[5]?.get(0)?.sql?.contains("CREATE INDEX") == true, "Version 5 should contain index creation")
    }

    @Test
    @DisplayName("Test MigrationInspector with mixed filename formats")
    fun testInspectWithMixedFilenameFormats() {
        // Mix of descriptive and non-descriptive filenames
        File(migrationDir, "0001.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "0002_add_posts.sql").writeText("CREATE TABLE posts (id INTEGER);")
        File(migrationDir, "0003.sql").writeText("CREATE TABLE comments (id INTEGER);")
        File(migrationDir, "0010_create_indexes.sql").writeText("CREATE INDEX idx_posts ON posts(id);")

        val inspector = MigrationInspector(migrationDir)

        assertEquals(4, inspector.sqlStatements.size, "Should have 4 versions")
        assertEquals(10, inspector.latestVersion, "Latest version should be 10")

        // All versions should be parsed correctly
        assertTrue(inspector.sqlStatements.containsKey(1), "Should contain version 1")
        assertTrue(inspector.sqlStatements.containsKey(2), "Should contain version 2")
        assertTrue(inspector.sqlStatements.containsKey(3), "Should contain version 3")
        assertTrue(inspector.sqlStatements.containsKey(10), "Should contain version 10")
    }

    @Test
    @DisplayName("Test MigrationInspector with invalid descriptive filenames")
    fun testInspectWithInvalidDescriptiveFilenames() {
        // Create valid files
        File(migrationDir, "0001_create_users.sql").writeText("CREATE TABLE users (id INTEGER);")

        // Create invalid files (wrong number of digits)
        File(migrationDir, "001_invalid.sql").writeText("CREATE TABLE ignored (id INTEGER);")

        // Should throw exception due to invalid filename (not 4 digits)
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
    }

    @Test
    @DisplayName("Test MigrationInspector with non-4-digit version numbers")
    fun testInspectWithNon4DigitVersions() {
        // Create files with non-4-digit version numbers (should fail with new format)
        File(migrationDir, "1.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "12.sql").writeText("CREATE TABLE posts (id INTEGER);")
        File(migrationDir, "123.sql").writeText("CREATE TABLE comments (id INTEGER);")

        // Should throw exception due to non-4-digit version numbers
        assertFailsWith<IllegalArgumentException> {
            MigrationInspector(migrationDir)
        }
    }

    @Test
    @DisplayName("Test MigrationInspector with valid 4-digit versions only")
    fun testInspectWith4DigitVersionsOnly() {
        // Create files with exactly 4-digit version numbers
        File(migrationDir, "0001.sql").writeText("CREATE TABLE users (id INTEGER);")
        File(migrationDir, "0010.sql").writeText("CREATE TABLE posts (id INTEGER);")
        File(migrationDir, "0100.sql").writeText("CREATE TABLE comments (id INTEGER);")
        File(migrationDir, "1000.sql").writeText("CREATE TABLE tags (id INTEGER);")

        val inspector = MigrationInspector(migrationDir)

        assertEquals(4, inspector.sqlStatements.size, "Should have 4 versions")
        assertEquals(1000, inspector.latestVersion, "Latest version should be 1000")
    }
}
