package dev.goquick.sqlitenow.gradle.sqlite

import kotlin.test.*

typealias S = SqlSingleStatement

class SqlParserTests {

    @Test
    fun testEmptyInput() {
        val result = parseSqlStatements("")
        assertTrue(result.isEmpty(), "Expected no statements for empty input")
    }

    @Test
    fun testWhitespaceOnly() {
        val input = "   \n  \t"
        val result = parseSqlStatements(input)
        assertTrue(result.isEmpty(), "Expected no statements for whitespace-only input")
    }

    @Test
    fun testSingleStatementNoComments() {
        val sql = "SELECT * FROM users;"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertTrue(stmt.topComments.isEmpty(), "No top comments expected")
        assertTrue(stmt.innerComments.isEmpty(), "No inner comments expected")
        assertEquals(sql, stmt.sql)
    }

    @Test
    fun testMultipleStatements() {
        val sql = "SELECT 1;\nSELECT 2;"
        val result = parseSqlStatements(sql)
        assertEquals(2, result.size)
        assertEquals("SELECT 1;", result[0].sql)
        assertEquals("SELECT 2;", result[1].sql)
    }

    @Test
    fun testTopComments() {
        val sql = "-- top comment\n/* block top */\nSELECT 1;"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertEquals(listOf("-- top comment", "/* block top */"), stmt.topComments)
        assertTrue(stmt.innerComments.isEmpty())
        assertTrue(stmt.sql.startsWith("-- top comment"))
    }

    @Test
    fun testInnerLineComments() {
        val sql = "SELECT 1 -- inline comment\n;"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertTrue(stmt.topComments.isEmpty())
        assertEquals(listOf("-- inline comment"), stmt.innerComments)
        assertTrue(stmt.sql.contains("-- inline comment"))
    }

    @Test
    fun testInnerBlockComments() {
        val sql = "SELECT /* inner block */ 1;"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertTrue(stmt.topComments.isEmpty())
        assertEquals(listOf("/* inner block */"), stmt.innerComments)
        assertTrue(stmt.sql.contains("/* inner block */"))
    }

    @Test
    fun testSemicolonInStringLiteral() {
        val sql = "INSERT INTO t VALUES ('semi;colon'); SELECT 2;"
        val result = parseSqlStatements(sql)
        assertEquals(2, result.size)
        assertEquals("INSERT INTO t VALUES ('semi;colon');", result[0].sql)
        assertEquals("SELECT 2;", result[1].sql)
    }

    @Test
    fun testDoubleQuotedIdentifierWithSemicolon() {
        val sql = "CREATE TABLE \"weird;name\" (id INT);"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        assertEquals(sql, result[0].sql)
    }

    @Test
    fun testUnterminatedLineCommentAtEOF() {
        val sql = "SELECT 1 -- comment without newline"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertEquals(emptyList<String>(), stmt.topComments)
        assertEquals(listOf("-- comment without newline"), stmt.innerComments)
        assertTrue(stmt.sql.endsWith("-- comment without newline"))
    }

    @Test
    fun testNoSemicolonAtEnd() {
        val sql = "SELECT 3"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        assertEquals("SELECT 3", result[0].sql)
    }

    @Test
    fun testSemicolonInBlockCommentDoesNotEndStatement() {
        val sql = "/* comment; still comment */ SELECT 1;"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertEquals(listOf("/* comment; still comment */"), stmt.topComments)
        assertTrue(stmt.innerComments.isEmpty())
        assertTrue(stmt.sql.startsWith("/* comment; still comment */ SELECT 1;"))
    }

    @Test
    fun testCommentMarkersInSingleQuotedString() {
        val sql = "SELECT '- - not a comment', '/* nope */';"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertTrue(stmt.topComments.isEmpty())
        assertTrue(stmt.innerComments.isEmpty(), "Should not detect comment markers inside literals")
        assertEquals(sql, stmt.sql)
    }

    @Test
    fun testCommentMarkersInDoubleQuotedIdentifier() {
        val sql = "SELECT \"--not_comment\" as col;"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        assertTrue(stmt.innerComments.isEmpty())
        assertEquals(sql, stmt.sql)
    }

    @Test
    fun testUnterminatedBlockCommentAtEOF() {
        val sql = "SELECT 1 /* unterminated block comment"
        val result = parseSqlStatements(sql)
        assertEquals(1, result.size)
        val stmt = result[0]
        // Unterminated block comments are preserved in the sql but not classified
        assertTrue(stmt.innerComments.isEmpty())
        assertTrue(stmt.sql.endsWith("/* unterminated block comment"))
    }

    @Test
    fun testMultipleSemicolonsEmptyStatements() {
        val sql = ";;SELECT 1;;"
        val result = parseSqlStatements(sql)
        // Expect empty statements for each semicolon with no content
        assertEquals(4, result.size)
        // The third statement should be SELECT 1;
        assertEquals("SELECT 1;", result[2].sql.trim())
    }

    @Test
    fun testComplexScript() {
        val sql = """
            -- top1
            /* top2 */
            CREATE TABLE t (
              id INT /* id comment */,
              name TEXT -- name comment
            );
            INSERT INTO t VALUES ('foo;bar'); -- insert comment
        """.trimIndent()

        val result = parseSqlStatements(sql)
        assertEquals(2, result.size)

        val create = result[0]
        assertEquals(listOf("-- top1", "/* top2 */"), create.topComments)
        assertEquals(listOf("/* id comment */", "-- name comment"), create.innerComments)
        assertTrue(create.sql.contains("CREATE TABLE t"))

        val insert = result[1]
        assertTrue(insert.topComments.isEmpty())
        assertEquals(listOf("-- insert comment"), insert.innerComments)
        assertTrue(insert.sql.contains("INSERT INTO t VALUES ('foo;bar');"))
    }

    @Test
    fun testTwoTableScript() {
        val sql = """
            -- Script to generate 15-20 Person and PersonAddress records
            -- This script creates tables and inserts sample data

            CREATE TABLE Person (
                id INTEGER PRIMARY KEY NOT NULL,

                -- @@propertyName=myFirstName
                first_name VARCHAR NOT NULL,

                -- @@propertyName=myLastName @@nullable
                last_name TEXT NOT NULL,

                email TEXT NOT NULL UNIQUE,
                phone TEXT,

                birth_date TEXT,

                age INTEGER,

                -- @@propertyType=kotlinx.datetime.LocalDateTime @@nonNull
                created_at TEXT NOT NULL DEFAULT current_timestamp
            );

            CREATE TABLE PersonAddress (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,

                -- @@propertyType=dev.goquick.sqlitenow.samplekmp.model.AddressType
                address_type TEXT NOT NULL,

                street TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT,

                postal_code TEXT,

                country TEXT NOT NULL,

                -- @@propertyType=Boolean
                is_primary INTEGER NOT NULL DEFAULT 0,

                -- @@propertyType=kotlinx.datetime.LocalDate
                created_at TEXT NOT NULL DEFAULT current_timestamp,

                FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
            );
        """.trimIndent()

        val result = parseSqlStatements(sql)
        assertEquals(2, result.size, "Should parse two CREATE TABLE statements")

        val person = result[0]
        assertTrue(person.sql.contains("CREATE TABLE Person"), "First statement should be Person table")
        assertEquals(
            listOf(
                "-- Script to generate 15-20 Person and PersonAddress records",
                "-- This script creates tables and inserts sample data"
            ),
            person.topComments
        )
        assertTrue(
            person.innerComments.any { it.contains("@@propertyName=myFirstName") },
            "Should capture first_name annotation"
        )
        assertTrue(
            person.innerComments.any { it.contains("@@propertyType=kotlinx.datetime.LocalDateTime") },
            "Should capture created_at annotation"
        )

        val address = result[1]
        assertTrue(address.sql.contains("CREATE TABLE PersonAddress"), "Second statement should be PersonAddress table")
        assertTrue(
            address.innerComments.any { it.contains("@@propertyType=dev.goquick.sqlitenow.samplekmp.model.AddressType") },
            "Should capture address_type annotation"
        )
        assertTrue(
            address.innerComments.any { it.contains("@@propertyType=kotlinx.datetime.LocalDate") },
            "Should capture created_at annotation in PersonAddress"
        )
    }
}
