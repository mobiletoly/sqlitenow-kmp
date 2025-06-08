package dev.goquick.sqlitenow.gradle.sqlite

import kotlin.test.Test
import kotlin.test.assertEquals

class SqlTranslatorTest {

    @Test
    fun `test basic SQL translation`() {
        val sql = """
            CREATE VIEW activityBundleView AS
            SELECT bndl.*, prov.title AS providerTitle FROM activityBundleEntity bndl
            JOIN providerEntity AS prov ON bndl.providerDocId = prov.docId
        """.trimIndent()

        val expected = """
            |CREATE VIEW activityBundleView AS
            |SELECT bndl.*, prov.title AS providerTitle FROM activityBundleEntity bndl
            |JOIN providerEntity AS prov ON bndl.providerDocId = prov.docId
        """.trimIndent()

        assertEquals(expected, translateSqliteStatementToKotlin(sql))
    }

    @Test
    fun `test SQL with special characters`() {
        val sql = """
            SELECT * FROM users WHERE name LIKE '%$%' AND cost > 100.00
        """.trimIndent()

        val expected = "|SELECT * FROM users WHERE name LIKE '%$%' AND cost > 100.00"

        assertEquals(expected, translateSqliteStatementToKotlin(sql))
    }

    @Test
    fun `test SQL with multiple statements`() {
        val sql = """
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            
            INSERT INTO users (name) VALUES ('John');
            INSERT INTO users (name) VALUES ('Jane');
        """.trimIndent()

        val expected = """
            |CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            |INSERT INTO users (name) VALUES ('John');
            |INSERT INTO users (name) VALUES ('Jane');
        """.trimIndent()

        assertEquals(expected, translateSqliteStatementToKotlin(sql))
    }

    @Test
    fun `test SQL with extra whitespace`() {
        val sql = """
            
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
            
        """.trimIndent()

        val expected = """
            |CREATE TABLE users (
            |id INTEGER PRIMARY KEY,
            |name TEXT
            |);
        """.trimIndent()

        assertEquals(expected, translateSqliteStatementToKotlin(sql))
    }

    @Test
    fun `test SQL with different line endings`() {
        val sql = "CREATE TABLE users (id INTEGER PRIMARY KEY);\r\nINSERT INTO users VALUES (1);\rSELECT * FROM users;"

        val expected = """
            |CREATE TABLE users (id INTEGER PRIMARY KEY);
            |INSERT INTO users VALUES (1);
            |SELECT * FROM users;
        """.trimIndent()

        assertEquals(expected, translateSqliteStatementToKotlin(sql))
    }
}
