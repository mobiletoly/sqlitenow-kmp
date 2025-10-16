package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SQLBatchInspectorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var sqlDir: File

    @BeforeEach
    fun setup() {
        sqlDir = File(tempDir.toFile(), "sql")
        sqlDir.mkdir()
    }

    @Test
    @DisplayName("Test SQLBatchInspector with multiple SQL files")
    fun testInspectMultipleSqlFiles() {
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

        val insertDataSql = """
            INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');
            INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane@example.com');
            
            INSERT INTO posts (id, user_id, title, content) VALUES (1, 1, 'First Post', 'This is the first post');
            INSERT INTO posts (id, user_id, title, content) VALUES (2, 2, 'Second Post', 'This is the second post');
        """.trimIndent()

        // Write SQL files
        File(sqlDir, "01_users.sql").writeText(usersTableSql)
        File(sqlDir, "02_posts.sql").writeText(postsTableSql)
        File(sqlDir, "03_data.sql").writeText(insertDataSql)

        // Create SQLBatchInspector
        val inspector = SQLBatchInspector(sqlDir, mandatory = false)

        // Verify that all SQL statements were collected
        assertEquals(6, inspector.sqlStatements.size, "Should have collected 6 SQL statements")

        // Verify that the SQL statements contain the expected content
        val allSql = inspector.sqlStatements.joinToString(" ") { it.sql }
        assertTrue(allSql.contains("CREATE TABLE users"), "Should contain users table creation")
        assertTrue(allSql.contains("CREATE TABLE posts"), "Should contain posts table creation")
        assertTrue(allSql.contains("INSERT INTO users"), "Should contain user inserts")
        assertTrue(allSql.contains("INSERT INTO posts"), "Should contain post inserts")
    }

    @Test
    @DisplayName("Test SQLBatchInspector with file instead of directory")
    fun testInspectWithFile() {
        val file = File(tempDir.toFile(), "test.sql")
        file.writeText("CREATE TABLE test (id INTEGER);")
        
        assertFailsWith<IllegalArgumentException> {
            SQLBatchInspector(file, mandatory = false)
        }
    }

    @Test
    @DisplayName("Test SQLBatchInspector with mixed file types")
    fun testInspectWithMixedFileTypes() {
        // Create SQL and non-SQL files
        val tableSql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
        """.trimIndent()

        File(sqlDir, "table.sql").writeText(tableSql)
        File(sqlDir, "readme.txt").writeText("This is a readme file")
        File(sqlDir, "config.json").writeText("{}")

        // SQLBatchInspector should only process .sql files
        val inspector = SQLBatchInspector(sqlDir, mandatory = false)

        assertEquals(1, inspector.sqlStatements.size, "Should have collected 1 SQL statement")
        
        // Verify that the SQL contains the expected table creation
        val sql = inspector.sqlStatements[0].sql
        assertTrue(sql.contains("CREATE TABLE test_table"), "Should contain test_table creation")
    }

    @Test
    @DisplayName("Test SQLBatchInspector with complex SQL statements")
    fun testInspectWithComplexSqlStatements() {
        // Create a file with various SQL statement types
        val complexSql = """
            -- Create tables
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
            
            -- Create indexes
            CREATE INDEX idx_users_name ON users(name);
            
            -- Insert data
            INSERT INTO users (id, name) VALUES (1, 'Test User');
            
            -- Update data
            UPDATE users SET name = 'Updated User' WHERE id = 1;
            
            -- Create view
            CREATE VIEW user_view AS SELECT id, name FROM users WHERE id > 0;
        """.trimIndent()

        File(sqlDir, "complex.sql").writeText(complexSql)

        // Create SQLBatchInspector
        val inspector = SQLBatchInspector(sqlDir, mandatory = false)

        // Verify that all statements were collected
        assertEquals(5, inspector.sqlStatements.size, "Should have collected 5 SQL statements")

        // Verify statement types
        val allSql = inspector.sqlStatements.joinToString(" ") { it.sql }
        assertTrue(allSql.contains("CREATE TABLE"), "Should contain CREATE TABLE")
        assertTrue(allSql.contains("CREATE INDEX"), "Should contain CREATE INDEX")
        assertTrue(allSql.contains("INSERT INTO"), "Should contain INSERT")
        assertTrue(allSql.contains("UPDATE"), "Should contain UPDATE")
        assertTrue(allSql.contains("CREATE VIEW"), "Should contain CREATE VIEW")
    }

    @Test
    @DisplayName("Test SQLBatchInspector with files in alphabetical order")
    fun testInspectWithAlphabeticalOrder() {
        // Create files that should be processed in alphabetical order
        File(sqlDir, "c_third.sql").writeText("INSERT INTO test VALUES (3);")
        File(sqlDir, "a_first.sql").writeText("CREATE TABLE test (id INTEGER);")
        File(sqlDir, "b_second.sql").writeText("INSERT INTO test VALUES (1);")

        val inspector = SQLBatchInspector(sqlDir, mandatory = false)

        assertEquals(3, inspector.sqlStatements.size, "Should have collected 3 SQL statements")

        // Verify that statements are in alphabetical order by filename
        val statements = inspector.sqlStatements.map { it.sql.trim() }
        assertEquals("CREATE TABLE test (id INTEGER);", statements[0], "First statement should be from a_first.sql")
        assertEquals("INSERT INTO test VALUES (1);", statements[1], "Second statement should be from b_second.sql")
        assertEquals("INSERT INTO test VALUES (3);", statements[2], "Third statement should be from c_third.sql")
    }
}
