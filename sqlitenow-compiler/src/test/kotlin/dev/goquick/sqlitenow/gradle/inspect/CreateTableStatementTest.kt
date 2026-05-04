package dev.goquick.sqlitenow.gradle.inspect

import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for CreateTableStatement class, focusing on SQLite dialect.
 *
 * SQLite has several specific features and syntax elements that differ from other SQL dialects:
 * - AUTOINCREMENT keyword (only valid for INTEGER PRIMARY KEY)
 * - Specific data types (TEXT, INTEGER, REAL, BLOB, NUMERIC)
 * - WITHOUT ROWID tables
 * - ON CONFLICT clauses
 * - STRICT tables (SQLite 3.37.0+)
 */
class CreateTableStatementTest {

    @Test
    @DisplayName("Test parsing basic CREATE TABLE statement")
    fun testBasicCreateTable() {
        // A simple CREATE TABLE statement
        val createTableSql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify table name
        assertEquals("users", createTableStatement.tableName)

        // Verify columns
        assertEquals(4, createTableStatement.columns.size)

        // Verify first column (id)
        val idColumn = createTableStatement.columns[0]
        assertEquals("id", idColumn.name)
        assertEquals("INTEGER", idColumn.dataType)
        assertTrue(idColumn.primaryKey)
        assertFalse(idColumn.notNull) // PRIMARY KEY implies NOT NULL, but it's not explicitly specified
        assertFalse(idColumn.autoIncrement)
        assertFalse(idColumn.unique) // PRIMARY KEY implies UNIQUE, but it's not explicitly specified

        // Verify second column (username)
        val usernameColumn = createTableStatement.columns[1]
        assertEquals("username", usernameColumn.name)
        assertEquals("TEXT", usernameColumn.dataType)
        assertTrue(usernameColumn.notNull)
        assertFalse(usernameColumn.primaryKey)
        assertFalse(usernameColumn.autoIncrement)
        assertFalse(usernameColumn.unique)

        // Verify third column (email)
        val emailColumn = createTableStatement.columns[2]
        assertEquals("email", emailColumn.name)
        assertEquals("TEXT", emailColumn.dataType)
        assertTrue(emailColumn.notNull)
        assertFalse(emailColumn.primaryKey)
        assertFalse(emailColumn.autoIncrement)
        assertTrue(emailColumn.unique)

        // Verify fourth column (created_at)
        val createdAtColumn = createTableStatement.columns[3]
        assertEquals("created_at", createdAtColumn.name)
        assertEquals("TIMESTAMP", createdAtColumn.dataType)
        assertFalse(createdAtColumn.notNull)
        assertFalse(createdAtColumn.primaryKey)
        assertFalse(createdAtColumn.autoIncrement)
        assertFalse(createdAtColumn.unique)
    }

    @Test
    @DisplayName("Test parsing CREATE TABLE with table-level constraints")
    fun testCreateTableWithTableLevelConstraints() {
        // CREATE TABLE with table-level constraints
        val createTableSql = """
            CREATE TABLE orders (
                id INTEGER,
                user_id INTEGER,
                product_id INTEGER,
                quantity INTEGER NOT NULL,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE (user_id, product_id)
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify table name
        assertEquals("orders", createTableStatement.tableName)

        // Verify columns
        assertEquals(5, createTableStatement.columns.size)

        // Verify id column (should be primary key from table-level constraint)
        val idColumn = createTableStatement.columns[0]
        assertEquals("id", idColumn.name)
        assertEquals("INTEGER", idColumn.dataType)
        assertTrue(idColumn.primaryKey)

        // Verify user_id and product_id columns (should be unique from table-level constraint)
        val userIdColumn = createTableStatement.columns[1]
        assertEquals("user_id", userIdColumn.name)
        assertTrue(userIdColumn.unique)

        val productIdColumn = createTableStatement.columns[2]
        assertEquals("product_id", productIdColumn.name)
        assertTrue(productIdColumn.unique)
    }

    @Test
    @DisplayName("Test parsing CREATE TABLE with autoincrement")
    fun testCreateTableWithAutoincrement() {
        // CREATE TABLE with autoincrement
        val createTableSql = """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify id column with autoincrement
        val idColumn = createTableStatement.columns[0]
        assertEquals("id", idColumn.name)
        assertTrue(idColumn.primaryKey)
        assertTrue(idColumn.autoIncrement)
    }

    @Test
    @DisplayName("Test parsing CREATE TABLE with SQLite AUTOINCREMENT")
    fun testCreateTableWithSqliteAutoincrement() {
        // CREATE TABLE with SQLite AUTOINCREMENT (note: in SQLite, AUTOINCREMENT is only valid for INTEGER PRIMARY KEY)
        val createTableSql = """
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify id column with AUTOINCREMENT
        val idColumn = createTableStatement.columns[0]
        assertEquals("id", idColumn.name)
        assertEquals("INTEGER", idColumn.dataType)
        assertTrue(idColumn.primaryKey)
        assertTrue(idColumn.autoIncrement)
    }

    @Test
    @DisplayName("Test parsing CREATE TABLE with empty column list")
    fun testCreateTableWithEmptyColumnList() {
        // CREATE TABLE with no columns (edge case)
        val createTableSql = """
            CREATE TABLE empty_table (
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify table name
        assertEquals("empty_table", createTableStatement.tableName)

        // Verify empty column list
        assertEquals(0, createTableStatement.columns.size)
    }

    @Test
    @DisplayName("Test parsing CREATE TABLE with SQLite data types")
    fun testCreateTableWithSqliteDataTypes() {
        // CREATE TABLE with SQLite-specific data types
        val createTableSql = """
            CREATE TABLE measurements (
                id INTEGER PRIMARY KEY,
                text_value TEXT NOT NULL,
                int_value INTEGER,
                real_value REAL,
                blob_value BLOB,
                numeric_value NUMERIC
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify columns with SQLite data types
        assertEquals(6, createTableStatement.columns.size)

        // Verify TEXT column
        val textColumn = createTableStatement.columns[1]
        assertEquals("text_value", textColumn.name)
        assertEquals("TEXT", textColumn.dataType)
        assertTrue(textColumn.notNull)

        // Verify INTEGER column
        val intColumn = createTableStatement.columns[2]
        assertEquals("int_value", intColumn.name)
        assertEquals("INTEGER", intColumn.dataType)

        // Verify REAL column
        val realColumn = createTableStatement.columns[3]
        assertEquals("real_value", realColumn.name)
        assertEquals("REAL", realColumn.dataType)

        // Verify BLOB column
        val blobColumn = createTableStatement.columns[4]
        assertEquals("blob_value", blobColumn.name)
        assertEquals("BLOB", blobColumn.dataType)

        // Verify NUMERIC column
        val numericColumn = createTableStatement.columns[5]
        assertEquals("numeric_value", numericColumn.name)
        assertEquals("NUMERIC", numericColumn.dataType)
    }

    // Note: JSqlParser doesn't support the WITHOUT ROWID syntax directly
    // This test focuses on the common pattern used with WITHOUT ROWID tables
    // (non-INTEGER PRIMARY KEY)
    @Test
    @DisplayName("Test parsing CREATE TABLE with non-INTEGER PRIMARY KEY")
    fun testCreateTableWithNonIntegerPrimaryKey() {
        // CREATE TABLE with TEXT PRIMARY KEY (common in WITHOUT ROWID tables)
        val createTableSql = """
            CREATE TABLE optimized_table (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify table name
        assertEquals("optimized_table", createTableStatement.tableName)

        // Verify columns
        assertEquals(3, createTableStatement.columns.size)

        // Verify PRIMARY KEY on non-INTEGER column (common with WITHOUT ROWID tables)
        val codeColumn = createTableStatement.columns[0]
        assertEquals("code", codeColumn.name)
        assertEquals("TEXT", codeColumn.dataType)
        assertTrue(codeColumn.primaryKey)
    }

    // Note: JSqlParser doesn't support the STRICT mode directly
    // This test is commented out as it would fail with the current JSqlParser version
    /*
    @Test
    @DisplayName("Test parsing CREATE TABLE with STRICT mode")
    fun testCreateTableWithStrictMode() {
        // CREATE TABLE with STRICT mode (SQLite 3.37.0+)
        val createTableSql = """
            CREATE TABLE strict_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            ) STRICT;
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(parsedStatement)

        // Verify table name
        assertEquals("strict_table", createTableStatement.tableName)

        // Verify columns
        assertEquals(3, createTableStatement.columns.size)
    }
    */

    // Instead, test a regular table that would be used with STRICT mode
    @Test
    @DisplayName("Test parsing CREATE TABLE with explicit types (for STRICT mode)")
    fun testCreateTableWithExplicitTypes() {
        // CREATE TABLE with explicit types (good practice for STRICT mode)
        val createTableSql = """
            CREATE TABLE strict_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify table name
        assertEquals("strict_table", createTableStatement.tableName)

        // Verify columns
        assertEquals(3, createTableStatement.columns.size)

        // Verify all columns have explicit types
        createTableStatement.columns.forEach { column ->
            assertTrue(column.dataType.isNotEmpty(), "Column ${column.name} should have an explicit type")
        }
    }

    // Note: JSqlParser doesn't support the ON CONFLICT clause directly
    // This test focuses on the UNIQUE constraint without the ON CONFLICT part
    @Test
    @DisplayName("Test parsing CREATE TABLE with UNIQUE constraints")
    fun testCreateTableWithUniqueConstraints() {
        // CREATE TABLE with UNIQUE constraints (simplified from ON CONFLICT version)
        val createTableSql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL UNIQUE
            );
        """.trimIndent()

        // Parse the SQL statement
        val parsedStatement = CCJSqlParserUtil.parse(createTableSql) as CreateTable
        val createTableStatement = CreateTableStatement.parse(sql = createTableSql, create = parsedStatement)

        // Verify table name
        assertEquals("users", createTableStatement.tableName)

        // Verify columns
        assertEquals(3, createTableStatement.columns.size)

        // Verify email column with UNIQUE constraint
        val emailColumn = createTableStatement.columns[1]
        assertEquals("email", emailColumn.name)
        assertEquals("TEXT", emailColumn.dataType)
        assertTrue(emailColumn.notNull)
        assertTrue(emailColumn.unique)

        // Verify username column with UNIQUE constraint
        val usernameColumn = createTableStatement.columns[2]
        assertEquals("username", usernameColumn.name)
        assertEquals("TEXT", usernameColumn.dataType)
        assertTrue(usernameColumn.notNull)
        assertTrue(usernameColumn.unique)
    }
}
