package dev.goquick.sqlitenow.gradle

import java.io.File
import java.sql.DriverManager
import kotlin.io.path.createTempDirectory
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class GenerateDatabaseFilesDeferrableForeignKeyTest {

    @Test
    fun generateDatabaseFilesSupportsDeferrableForeignKeys() {
        val root = createTempDirectory(prefix = "deferrable-fk-").toFile()
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queryDir = File(root, "queries/posts").apply { mkdirs() }
        val outDir = File(root, "out").apply { mkdirs() }
        val schemaDatabaseFile = File(root, "schema.db")

        File(schemaDir, "users.sql").writeText(
            """
            CREATE TABLE users (
                id TEXT PRIMARY KEY NOT NULL
            );
            """.trimIndent()
        )
        File(schemaDir, "posts.sql").writeText(
            """
            CREATE TABLE posts (
                id TEXT PRIMARY KEY NOT NULL,
                author_id TEXT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
            );
            """.trimIndent()
        )
        File(queryDir, "selectAll.sql").writeText(
            """
            SELECT id, author_id FROM posts;
            """.trimIndent()
        )

        generateDatabaseFiles(
            dbName = "TestDbDeferrable",
            sqlDir = root,
            packageName = "dev.test.deferrable",
            outDir = outDir,
            schemaDatabaseFile = schemaDatabaseFile,
            debug = false,
        )

        val generatedSource = outDir.walkTopDown().firstOrNull { it.extension == "kt" }
        assertNotNull(generatedSource, "Expected generateDatabaseFiles to emit Kotlin sources")

        DriverManager.getConnection("jdbc:sqlite:${schemaDatabaseFile.absolutePath}").use { conn ->
            val createdSql = conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT sql FROM sqlite_master WHERE type='table' AND name='posts'").use { rs ->
                    assertTrue(rs.next(), "posts table should exist in the generated schema database")
                    rs.getString("sql")
                }
            }
            assertTrue(createdSql.contains("DEFERRABLE INITIALLY DEFERRED"))
        }
    }
}
