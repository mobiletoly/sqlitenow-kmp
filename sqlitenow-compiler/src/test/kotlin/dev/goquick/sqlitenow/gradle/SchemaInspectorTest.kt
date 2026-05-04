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
import kotlin.test.assertNotNull
import kotlin.test.assertFalse

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
        assertEquals(1, createdTables.size)

        // Execute CREATE VIEW statements; should not throw despite file order
        val createdViews = inspector.getCreateViewStatements(conn)
        assertEquals(2, createdViews.size)

        // Verify both views exist in the database
        conn.createStatement().use { st ->
            st.executeQuery("SELECT name FROM sqlite_master WHERE type='view' AND name='activity_category_for_join'").use { rs ->
                assertTrue(rs.next(), "activity_category_for_join should be created")
            }
            st.executeQuery("SELECT name FROM sqlite_master WHERE type='view' AND name='view_a'").use { rs ->
                assertTrue(rs.next(), "view_a should be created")
            }
        }
    }

    @Test
    @DisplayName("Test SchemaInspector supports deferrable foreign key clauses")
    fun testSchemaInspectorSupportsDeferrableForeignKeys() {
        File(schemaDir, "01_users.sql").writeText(
            """
            CREATE TABLE users (
                id TEXT PRIMARY KEY NOT NULL
            );
            """.trimIndent()
        )
        File(schemaDir, "02_posts.sql").writeText(
            """
            CREATE TABLE posts (
                id TEXT PRIMARY KEY NOT NULL,
                author_id TEXT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
            );
            """.trimIndent()
        )
        File(schemaDir, "03_comments.sql").writeText(
            """
            CREATE TABLE comments (
                id TEXT PRIMARY KEY NOT NULL,
                post_id TEXT NOT NULL,
                FOREIGN KEY (post_id) REFERENCES posts(id) NOT DEFERRABLE
            );
            """.trimIndent()
        )
        File(schemaDir, "04_likes.sql").writeText(
            """
            CREATE TABLE likes (
                id TEXT PRIMARY KEY NOT NULL,
                post_id TEXT NOT NULL,
                FOREIGN KEY (post_id) REFERENCES posts(id) DEFERRABLE INITIALLY IMMEDIATE
            );
            """.trimIndent()
        )

        val inspector = SchemaInspector(schemaDirectory = schemaDir)
        val createdTables = inspector.getCreateTableStatements(conn)

        assertEquals(4, createdTables.size, "Should execute all CREATE TABLE statements with deferrable clauses")
        assertTrue(inspector.sqlStatements.any { it.sql.contains("DEFERRABLE INITIALLY DEFERRED") })
        assertTrue(inspector.sqlStatements.any { it.sql.contains("NOT DEFERRABLE") })
        assertTrue(inspector.sqlStatements.any { it.sql.contains("DEFERRABLE INITIALLY IMMEDIATE") })

        fun tableSql(name: String): String {
            return conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT sql FROM sqlite_master WHERE type='table' AND name='$name'").use { rs ->
                    assertTrue(rs.next(), "$name table should exist")
                    rs.getString("sql")
                }
            }
        }

        val postsSql = tableSql("posts")
        val commentsSql = tableSql("comments")
        val likesSql = tableSql("likes")

        assertNotNull(postsSql)
        assertNotNull(commentsSql)
        assertNotNull(likesSql)
        assertTrue(postsSql.contains("DEFERRABLE INITIALLY DEFERRED"))
        assertTrue(commentsSql.contains("NOT DEFERRABLE"))
        assertTrue(likesSql.contains("DEFERRABLE INITIALLY IMMEDIATE"))
    }

    @Test
    @DisplayName("Test SchemaInspector supports WITHOUT ROWID tables while normalizing parser input")
    fun testSchemaInspectorSupportsWithoutRowIdTables() {
        File(schemaDir, "01_lookup.sql").writeText(
            """
            CREATE TABLE lookup (
                code TEXT PRIMARY KEY NOT NULL,
                label TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )

        val inspector = SchemaInspector(schemaDirectory = schemaDir)
        val createdTables = inspector.getCreateTableStatements(conn)

        assertEquals(1, createdTables.size)
        val normalized = normalizeForParser(
            inspector,
            inspector.sqlStatements.single().sql,
        )
        assertFalse(normalized.contains("WITHOUT ROWID"))

        val storedSql = conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT sql FROM sqlite_master WHERE type='table' AND name='lookup'").use { rs ->
                assertTrue(rs.next(), "lookup table should exist")
                rs.getString("sql")
            }
        }
        assertNotNull(storedSql)
        assertTrue(storedSql.contains("WITHOUT ROWID"))
    }

    @Test
    @DisplayName("Test SchemaInspector normalization strips unsupported FK clauses without touching comments or strings")
    fun testSchemaInspectorNormalizationPreservesCommentsAndStrings() {
        File(schemaDir, "01_base.sql").writeText(
            """
            CREATE TABLE base_table (
                id INTEGER PRIMARY KEY NOT NULL
            );
            """.trimIndent()
        )
        val sampleSql = """
            -- DEFERRABLE INITIALLY DEFERRED should stay in comments
            CREATE TABLE child (
                id TEXT PRIMARY KEY NOT NULL,
                /* NOT DEFERRABLE should stay in block comments */
                note TEXT DEFAULT 'NOT DEFERRABLE inside string literal',
                parent_id TEXT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
            );
            """.trimIndent()

        val inspector = SchemaInspector(schemaDirectory = schemaDir)
        val normalized = normalizeForParser(inspector, sampleSql)

        assertTrue(normalized.contains("-- DEFERRABLE INITIALLY DEFERRED should stay in comments"))
        assertTrue(normalized.contains("'NOT DEFERRABLE inside string literal'"))
        assertTrue(normalized.contains("/* NOT DEFERRABLE should stay in block comments */"))
        assertFalse(normalized.contains("REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED"))
        assertTrue(normalized.contains("REFERENCES parent(id)"))

        val createdTables = inspector.getCreateTableStatements(conn)
        assertEquals(1, createdTables.size)
    }

    private fun normalizeForParser(inspector: SchemaInspector, sql: String): String {
        val method = SchemaInspector::class.java.getDeclaredMethod("normalizeForParser", String::class.java)
        method.isAccessible = true
        return method.invoke(inspector, sql) as String
    }
}
