package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqlCommentsTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create a test table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT DEFAULT current_timestamp
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address_type TEXT NOT NULL,
                    street TEXT NOT NULL,
                    city TEXT NOT NULL,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT NOT NULL,
                    is_primary INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT current_timestamp,
                    FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting top comments and all comments from SQL statement")
    fun testExtractComments() {
        // SQL with various comments
        val sqlWithComments = """
            -- @@className=SelectPersonWithAddress
            SELECT
                p.id AS person_id,

                -- @@field=count_here @@propertyName=totalPersonCount @@nonNull
                (SELECT count(*) FROM Person) AS count_here,

                -- @@field=first_name @@propertyName=firstName
                p.first_name,

                p.last_name, p.email, p.phone, p.birth_date,
                p.created_at AS person_created_at,
                a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                a.is_primary, a.created_at AS address_created_at,
                p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
                JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
                AND p.last_name LIKE :lastName;
        """.trimIndent()

        // Introspect the statement
        val statementInfo = SqlStatementIntrospector(sqlWithComments, connection).introspect()

        // Print debug information
        println("\nTop comments:")
        statementInfo.topComments.forEach { comment ->
            println("Comment: $comment")
        }

        println("\nInner comments:")
        statementInfo.innerComments.forEach { comment ->
            println("Comment: $comment")
        }

        // Verify top comments
        assertEquals(1, statementInfo.topComments.size, "Should have 1 top comment")
        assertEquals("@@className=SelectPersonWithAddress", statementInfo.topComments[0], "Top comment should be cleaned up")

        // Verify inner comments
        assertEquals(2, statementInfo.innerComments.size, "Should have 2 comments in total (excluding top comments)")

        // Check for specific comments
        val hasTopComment = statementInfo.innerComments.any { it == "@@className=SelectPersonWithAddress" }
        val hasFieldComments = statementInfo.innerComments.any { it.contains("@@field=count_here") && it.contains("@@propertyName=totalPersonCount") && it.contains("@@nonNull") }
        val hasFirstNameComment = statementInfo.innerComments.any { it.contains("@@field=first_name") && it.contains("@@propertyName=firstName") }

        assertFalse(hasTopComment, "Inner comments should NOT include the top comment")
        assertTrue(hasFieldComments, "Inner comments should include the field comments")
        assertTrue(hasFirstNameComment, "Inner comments should include the first_name comment")
    }

    @Test
    @DisplayName("Test extracting comments with multiple inline comments")
    fun testExtractMultipleInlineComments() {
        // SQL with multiple inline comments
        val sqlWithMultipleInlineComments = """
            -- This is an inline comment at the top
            -- This is another inline comment at the top
            SELECT
                p.id AS person_id,

                -- This is an inline comment
                (SELECT count(*) FROM Person) AS count_here,

                p.first_name, p.last_name
            FROM Person p
            WHERE p.id = :personId;
        """.trimIndent()

        // Introspect the statement
        val statementInfo = SqlStatementIntrospector(sqlWithMultipleInlineComments, connection).introspect()

        // Verify top comments
        assertEquals(2, statementInfo.topComments.size, "Should have 2 top comments")
        assertTrue(statementInfo.topComments[0].contains("This is an inline comment at the top"),
            "Top comment should contain 'This is an inline comment at the top'")
        assertTrue(statementInfo.topComments[1].contains("This is another inline comment at the top"),
            "Top comment should contain 'This is another inline comment at the top'")

        // Verify inner comments
        assertEquals(1, statementInfo.innerComments.size, "Should have 1 comment in total (excluding top comments)")

        // Check for specific comments
        val hasTopComment = statementInfo.innerComments.any { it.contains("This is an inline comment at the top") }
        val hasInlineComment = statementInfo.innerComments.any { it.contains("This is an inline comment") }

        assertFalse(hasTopComment, "Inner comments should NOT include the top comment")
        assertTrue(hasInlineComment, "Inner comments should include the inline comment")
    }

    @Test
    @DisplayName("Test extracting multiple consecutive inline comments")
    fun testExtractMultipleConsecutiveInlineComments() {
        // SQL with multiple consecutive inline comments
        val sqlWithMultipleConsecutiveComments = """
            -- First top comment
            -- Second top comment
            SELECT
                -- Comment for person_id
                p.id AS person_id,

                -- Comment for count_here
                (SELECT count(*) FROM Person) AS count_here,

                -- Comment for first_name
                p.first_name,

                -- Comment for last_name
                p.last_name
            FROM Person p
            WHERE p.id = :personId;
        """.trimIndent()

        // Introspect the statement
        val statementInfo = SqlStatementIntrospector(sqlWithMultipleConsecutiveComments, connection).introspect()

        // Verify top comments
        assertEquals(2, statementInfo.topComments.size, "Should have 2 top comments")
        assertTrue(statementInfo.topComments.contains("First top comment"),
            "Top comments should contain 'First top comment'")
        assertTrue(statementInfo.topComments.contains("Second top comment"),
            "Top comments should contain 'Second top comment'")

        // Verify inner comments
        assertEquals(4, statementInfo.innerComments.size, "Should have 4 comments in total (excluding top comments)")

        // Check for specific comments
        val hasFirstTopComment = statementInfo.innerComments.contains("First top comment")
        val hasSecondTopComment = statementInfo.innerComments.contains("Second top comment")
        val hasPersonIdComment = statementInfo.innerComments.contains("Comment for person_id")
        val hasCountHereComment = statementInfo.innerComments.contains("Comment for count_here")
        val hasFirstNameComment = statementInfo.innerComments.contains("Comment for first_name")
        val hasLastNameComment = statementInfo.innerComments.contains("Comment for last_name")

        assertFalse(hasFirstTopComment, "Inner comments should NOT include the first top comment")
        assertFalse(hasSecondTopComment, "Inner comments should NOT include the second top comment")
        assertTrue(hasPersonIdComment, "Inner comments should include the person_id comment")
        assertTrue(hasCountHereComment, "Inner comments should include the count_here comment")
        assertTrue(hasFirstNameComment, "Inner comments should include the first_name comment")
        assertTrue(hasLastNameComment, "Inner comments should include the last_name comment")
    }
}
