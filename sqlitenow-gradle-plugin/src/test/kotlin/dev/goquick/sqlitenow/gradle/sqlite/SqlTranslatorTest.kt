package dev.goquick.sqlitenow.gradle.sqlite

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SqlTranslatorTest {

    @Test
    @DisplayName("Test translating SQL without dollar signs")
    fun testTranslateSqlWithoutDollarSigns() {
        val sql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """.trimIndent()

        val result = translateSqliteStatementToKotlin(sql)

        // The result should not have the $$ prefix
        assertEquals(
            "|CREATE TABLE test_table (\n" +
            "|id INTEGER PRIMARY KEY,\n" +
            "|name TEXT NOT NULL,\n" +
            "|created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
            "|);",
            result
        )
    }

    @Test
    @DisplayName("Test translating SQL with dollar signs")
    fun testTranslateSqlWithDollarSigns() {
        val sql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                price REAL DEFAULT 0.0,
                currency TEXT DEFAULT '$',
                description TEXT
            );
        """.trimIndent()

        val result = translateSqliteStatementToKotlin(sql)

        // The result should not have the $$ prefix since we've rolled back that implementation
        assertEquals(
            "|CREATE TABLE test_table (\n" +
            "|id INTEGER PRIMARY KEY,\n" +
            "|price REAL DEFAULT 0.0,\n" +
            "|currency TEXT DEFAULT '$',\n" +
            "|description TEXT\n" +
            "|);",
            result
        )
    }

    @Test
    @DisplayName("Test translating SQL with multiple dollar signs")
    fun testTranslateSqlWithMultipleDollarSigns() {
        val sql = """
            INSERT INTO products (name, price, currency, discount_code)
            VALUES ('Product 1', 19.99, '$', 'SAVE$10');
        """.trimIndent()

        val result = translateSqliteStatementToKotlin(sql)

        // The result should not have the $$ prefix since we've rolled back that implementation
        assertEquals(
            "|INSERT INTO products (name, price, currency, discount_code)\n" +
            "|VALUES ('Product 1', 19.99, '$', 'SAVE$10');",
            result
        )
    }
}
