package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import java.sql.Connection
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SchemaInspectorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var schemaDir: File
    private lateinit var conn: Connection

    @BeforeEach
    fun setup() {
        schemaDir = File(tempDir.toFile(), "schema")
        schemaDir.mkdir()
        conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
    }

    @Test
    @DisplayName("Test SchemaInspector with multiple SQL files")
    fun testSchemaInspectorWithMultipleFiles() {
        // Create multiple SQL files
        val usersTableSql = """
            -- Users table
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """.trimIndent()

        val postsTableSql = """
            -- Posts table
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY NOT NULL,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """.trimIndent()

        val commentsTableSql = """
            -- Comments table
            CREATE TABLE comments (
                id INTEGER PRIMARY KEY NOT NULL,
                post_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """.trimIndent()

        // Write SQL files
        File(schemaDir, "01_users.sql").writeText(usersTableSql)
        File(schemaDir, "02_posts.sql").writeText(postsTableSql)
        File(schemaDir, "03_comments.sql").writeText(commentsTableSql)

        // Create SchemaInspector
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)

        // Create a connection for testing
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Verify that all SQL statements were parsed
        assertEquals(3, schemaInspector.sqlStatements.size, "Should have 3 SQL statements")
        assertEquals(3, schemaInspector.getCreateTableStatements(conn).size, "Should have 3 CREATE TABLE statements")

        // Verify table names
        val tableNames = schemaInspector.getCreateTableStatements(conn).map { it.name }.toSet()
        assertTrue(tableNames.contains("users"), "Should contain users table")
        assertTrue(tableNames.contains("posts"), "Should contain posts table")
        assertTrue(tableNames.contains("comments"), "Should contain comments table")

        // Verify that SQL statements contain the expected content
        val allSql = schemaInspector.sqlStatements.joinToString(" ") { it.sql }
        assertTrue(allSql.contains("CREATE TABLE users"), "Should contain users table creation")
        assertTrue(allSql.contains("CREATE TABLE posts"), "Should contain posts table creation")
        assertTrue(allSql.contains("CREATE TABLE comments"), "Should contain comments table creation")
    }

    @Test
    @DisplayName("Test SchemaInspector with non-existent directory")
    fun testSchemaInspectorWithNonExistentDirectory() {
        val nonExistentDir = File(tempDir.toFile(), "non-existent")
        
        assertFailsWith<IllegalArgumentException> {
            SchemaInspector(schemaDirectory = nonExistentDir)
        }
    }

    @Test
    @DisplayName("Test SchemaInspector with file instead of directory")
    fun testSchemaInspectorWithFile() {
        val file = File(tempDir.toFile(), "test.sql")
        file.writeText("CREATE TABLE test (id INTEGER);")
        
        assertFailsWith<IllegalArgumentException> {
            SchemaInspector(schemaDirectory = file)
        }
    }

    @Test
    @DisplayName("Test SchemaInspector with empty directory")
    fun testSchemaInspectorWithEmptyDirectory() {
        assertFailsWith<IllegalArgumentException> {
            SchemaInspector(schemaDirectory = schemaDir)
        }
    }

    @Test
    @DisplayName("Test SchemaInspector with non-SQL files")
    fun testSchemaInspectorWithNonSqlFiles() {
        // Create non-SQL files
        File(schemaDir, "readme.txt").writeText("This is a readme file")
        File(schemaDir, "config.json").writeText("{}")
        
        assertFailsWith<IllegalArgumentException> {
            SchemaInspector(schemaDirectory = schemaDir)
        }
    }

    @Test
    @DisplayName("Test SchemaInspector with mixed file types")
    fun testSchemaInspectorWithMixedFileTypes() {
        // Create SQL and non-SQL files
        val tableSql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
        """.trimIndent()

        File(schemaDir, "table.sql").writeText(tableSql)
        File(schemaDir, "readme.txt").writeText("This is a readme file")
        File(schemaDir, "config.json").writeText("{}")

        // SchemaInspector should only process .sql files
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)

        // Create a connection for testing
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        assertEquals(1, schemaInspector.sqlStatements.size, "Should have 1 SQL statement")
        assertEquals(1, schemaInspector.getCreateTableStatements(conn).size, "Should have 1 CREATE TABLE statement")
        assertEquals("test_table", schemaInspector.getCreateTableStatements(conn)[0].name, "Should have test_table")
    }

    @Test
    @DisplayName("Test CREATE VIEW execution is ordered by dependencies")
    fun testCreateViewDependencyOrder() {
        // Base table
        val tableSql = """
            CREATE TABLE base_table (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
        """.trimIndent()

        // Dependent view (references other view)
        val viewA = """
            CREATE VIEW view_a AS
            SELECT id FROM activity_category_for_join;
        """.trimIndent()

        // Referenced view
        val viewB = """
            CREATE VIEW activity_category_for_join AS
            SELECT id, name FROM base_table;
        """.trimIndent()

        // Write files in order that would fail without topological sorting
        File(schemaDir, "01_base.sql").writeText(tableSql)
        File(schemaDir, "02_view_a.sql").writeText(viewA)
        File(schemaDir, "03_view_b.sql").writeText(viewB)

        val inspector = SchemaInspector(schemaDirectory = schemaDir)

        // Execute CREATE TABLE statements
        val createdTables = inspector.getCreateTableStatements(conn)
        kotlin.test.assertEquals(1, createdTables.size)

        // Execute CREATE VIEW statements; should not throw despite file order
        val createdViews = inspector.getCreateViewStatements(conn)
        kotlin.test.assertEquals(2, createdViews.size)

        // Verify both views exist in the database
        conn.createStatement().use { st ->
            st.executeQuery("SELECT name FROM sqlite_master WHERE type='view' AND name='activity_category_for_join'").use { rs ->
                kotlin.test.assertTrue(rs.next(), "activity_category_for_join should be created")
            }
            st.executeQuery("SELECT name FROM sqlite_master WHERE type='view' AND name='view_a'").use { rs ->
                kotlin.test.assertTrue(rs.next(), "view_a should be created")
            }
        }
    }
}
