package dev.goquick.sqlitenow.gradle.inspect

import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals
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

    @TestFactory
    fun parseCreateTableScenarios(): List<DynamicTest> {
        return listOf(
            CreateTableCase(
                name = "parsing basic CREATE TABLE statement",
                sql = """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        username TEXT NOT NULL,
                        email TEXT NOT NULL UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """.trimIndent(),
                tableName = "users",
                columns = listOf(
                    ExpectedColumn(
                        name = "id",
                        dataType = "INTEGER",
                        primaryKey = true,
                        notNull = false, // PRIMARY KEY implies NOT NULL, but it's not explicitly specified
                        autoIncrement = false,
                        unique = false, // PRIMARY KEY implies UNIQUE, but it's not explicitly specified
                    ),
                    ExpectedColumn(
                        name = "username",
                        dataType = "TEXT",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                    ExpectedColumn(
                        name = "email",
                        dataType = "TEXT",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = true,
                    ),
                    ExpectedColumn(
                        name = "created_at",
                        dataType = "TIMESTAMP",
                        notNull = false,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                ),
            ),
            CreateTableCase(
                name = "parsing CREATE TABLE with table-level constraints",
                sql = """
                    CREATE TABLE orders (
                        id INTEGER,
                        user_id INTEGER,
                        product_id INTEGER,
                        quantity INTEGER NOT NULL,
                        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE (user_id, product_id)
                    );
                """.trimIndent(),
                tableName = "orders",
                columns = listOf(
                    ExpectedColumn(name = "id", dataType = "INTEGER", primaryKey = true),
                    ExpectedColumn(name = "user_id", unique = true),
                    ExpectedColumn(name = "product_id", unique = true),
                    ExpectedColumn(name = "quantity"),
                    ExpectedColumn(name = "order_date"),
                ),
            ),
            CreateTableCase(
                name = "parsing CREATE TABLE with autoincrement",
                sql = """
                    CREATE TABLE products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        price DECIMAL(10,2) NOT NULL
                    );
                """.trimIndent(),
                tableName = "products",
                columns = listOf(
                    ExpectedColumn(name = "id", primaryKey = true, autoIncrement = true),
                    ExpectedColumn(name = "name"),
                    ExpectedColumn(name = "price"),
                ),
            ),
            CreateTableCase(
                name = "parsing CREATE TABLE with SQLite AUTOINCREMENT",
                sql = """
                    CREATE TABLE categories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE
                    );
                """.trimIndent(),
                tableName = "categories",
                columns = listOf(
                    ExpectedColumn(name = "id", dataType = "INTEGER", primaryKey = true, autoIncrement = true),
                    ExpectedColumn(name = "name"),
                ),
            ),
            CreateTableCase(
                name = "parsing CREATE TABLE with empty column list",
                sql = """
                    CREATE TABLE empty_table (
                    );
                """.trimIndent(),
                tableName = "empty_table",
                columns = emptyList(),
            ),
            CreateTableCase(
                name = "parsing CREATE TABLE with SQLite data types",
                sql = """
                    CREATE TABLE measurements (
                        id INTEGER PRIMARY KEY,
                        text_value TEXT NOT NULL,
                        int_value INTEGER,
                        real_value REAL,
                        blob_value BLOB,
                        numeric_value NUMERIC
                    );
                """.trimIndent(),
                tableName = "measurements",
                columns = listOf(
                    ExpectedColumn(name = "id"),
                    ExpectedColumn(name = "text_value", dataType = "TEXT", notNull = true),
                    ExpectedColumn(name = "int_value", dataType = "INTEGER"),
                    ExpectedColumn(name = "real_value", dataType = "REAL"),
                    ExpectedColumn(name = "blob_value", dataType = "BLOB"),
                    ExpectedColumn(name = "numeric_value", dataType = "NUMERIC"),
                ),
            ),
            // JSqlParser doesn't support WITHOUT ROWID directly; this covers the common non-INTEGER PK pattern.
            CreateTableCase(
                name = "parsing CREATE TABLE with non-INTEGER PRIMARY KEY",
                sql = """
                    CREATE TABLE optimized_table (
                        code TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        value INTEGER
                    );
                """.trimIndent(),
                tableName = "optimized_table",
                columns = listOf(
                    ExpectedColumn(name = "code", dataType = "TEXT", primaryKey = true),
                    ExpectedColumn(name = "name"),
                    ExpectedColumn(name = "value"),
                ),
            ),
            // JSqlParser doesn't support STRICT mode directly; this covers a table suitable for STRICT mode.
            CreateTableCase(
                name = "parsing CREATE TABLE with explicit types for STRICT mode",
                sql = """
                    CREATE TABLE strict_table (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        value INTEGER
                    );
                """.trimIndent(),
                tableName = "strict_table",
                columns = listOf(
                    ExpectedColumn(name = "id"),
                    ExpectedColumn(name = "name"),
                    ExpectedColumn(name = "value"),
                ),
                afterParse = { createTableStatement ->
                    createTableStatement.columns.forEach { column ->
                        assertTrue(column.dataType.isNotEmpty(), "Column ${column.name} should have an explicit type")
                    }
                },
            ),
            // JSqlParser doesn't support ON CONFLICT directly; this covers UNIQUE constraints without ON CONFLICT.
            CreateTableCase(
                name = "parsing CREATE TABLE with UNIQUE constraints",
                sql = """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        email TEXT NOT NULL UNIQUE,
                        username TEXT NOT NULL UNIQUE
                    );
                """.trimIndent(),
                tableName = "users",
                columns = listOf(
                    ExpectedColumn(name = "id"),
                    ExpectedColumn(name = "email", dataType = "TEXT", notNull = true, unique = true),
                    ExpectedColumn(name = "username", dataType = "TEXT", notNull = true, unique = true),
                ),
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.name) {
                val createTableStatement = assertCreateTable(case.sql, case.tableName, case.columns)
                case.afterParse?.invoke(createTableStatement)
            }
        }
    }

    private fun assertCreateTable(
        sql: String,
        tableName: String,
        columns: List<ExpectedColumn>,
    ): CreateTableStatement {
        val createTableStatement = parseCreateTableStatement(sql)
        assertEquals(tableName, createTableStatement.tableName)
        assertEquals(columns.size, createTableStatement.columns.size)
        columns.forEachIndexed { index, expected ->
            assertColumn(createTableStatement.columns[index], expected)
        }
        return createTableStatement
    }

    private fun parseCreateTableStatement(sql: String): CreateTableStatement {
        val parsedStatement = CCJSqlParserUtil.parse(sql) as CreateTable
        return CreateTableStatement.parse(sql = sql, create = parsedStatement)
    }

    private fun assertColumn(
        column: CreateTableStatement.Column,
        expected: ExpectedColumn,
    ) {
        assertEquals(expected.name, column.name)
        expected.dataType?.let { assertEquals(it, column.dataType) }
        expected.notNull?.let { assertEquals(it, column.notNull) }
        expected.primaryKey?.let { assertEquals(it, column.primaryKey) }
        expected.autoIncrement?.let { assertEquals(it, column.autoIncrement) }
        expected.unique?.let { assertEquals(it, column.unique) }
    }

    private data class CreateTableCase(
        val name: String,
        val sql: String,
        val tableName: String,
        val columns: List<ExpectedColumn>,
        val afterParse: ((CreateTableStatement) -> Unit)? = null,
    )

    private data class ExpectedColumn(
        val name: String,
        val dataType: String? = null,
        val notNull: Boolean? = null,
        val primaryKey: Boolean? = null,
        val autoIncrement: Boolean? = null,
        val unique: Boolean? = null,
    )
}
