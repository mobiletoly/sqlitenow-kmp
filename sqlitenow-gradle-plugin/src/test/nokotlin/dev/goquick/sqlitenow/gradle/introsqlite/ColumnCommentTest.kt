package dev.goquick.sqlitenow.gradle.introsqlite

import dev.goquick.sqlitenow.gradle.introsqlite.DatabaseIntrospector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ColumnCommentTest {
    private lateinit var connection: Connection
    private lateinit var introspector: DatabaseIntrospector

    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")
        introspector = DatabaseIntrospector(connection)
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting comments on a line before column definition")
    fun testCommentsBeforeColumn() {
        val createTableSql = """
        CREATE TABLE Person (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,

            -- some comment for birth_date
            birth_date TEXT,

            created_at TEXT DEFAULT current_timestamp
        )
        """.trimIndent()

        // Create the table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("DROP TABLE IF EXISTS Person;")
            stmt.executeUpdate(createTableSql)
        }

        // Extract comments for birth_date column
        val birthDateComments = introspector.extractColumnCommentsFromSql(createTableSql, "birth_date")

        assertEquals(1, birthDateComments.size)
        assertEquals("some comment for birth_date", birthDateComments[0])

        // Extract comments for a column without a comment
        val firstNameComments = introspector.extractColumnCommentsFromSql(createTableSql, "first_name")
        assertTrue(firstNameComments.isEmpty())
    }

    @Test
    @DisplayName("Test extracting comments on the same line as column definition")
    fun testCommentsOnSameLine() {
        val createTableSql = """
        CREATE TABLE Person (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL, -- First name of the person
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,
            birth_date TEXT, -- Date of birth in ISO format
            created_at TEXT DEFAULT current_timestamp
        )
        """.trimIndent()

        // Create the table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("DROP TABLE IF EXISTS Person;")
            stmt.executeUpdate(createTableSql)
        }

        // Extract comments
        val firstNameComments = introspector.extractColumnCommentsFromSql(createTableSql, "first_name")
        val birthDateComments = introspector.extractColumnCommentsFromSql(createTableSql, "birth_date")

        assertEquals(1, firstNameComments.size)
        assertEquals("First name of the person", firstNameComments[0])
        assertEquals(1, birthDateComments.size)
        assertEquals("Date of birth in ISO format", birthDateComments[0])
    }

    @Test
    @DisplayName("Test extracting multiple inline comments before column definition")
    fun testMultipleInlineComments() {
        val createTableSql = """
        CREATE TABLE Person (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,

            -- some comment
            -- for birth_date
            birth_date TEXT,

            created_at TEXT DEFAULT current_timestamp
        )
        """.trimIndent()

        // Create the table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("DROP TABLE IF EXISTS Person;")
            stmt.executeUpdate(createTableSql)
        }

        // Extract comments for birth_date column
        val birthDateComments = introspector.extractColumnCommentsFromSql(createTableSql, "birth_date")

        assertEquals(2, birthDateComments.size)
        assertEquals("some comment", birthDateComments[0])
        assertEquals("for birth_date", birthDateComments[1])
    }

    @Test
    @DisplayName("Test extracting inline comments before column definition")
    fun testInlineCommentsBeforeColumnDefinition() {
        val createTableSql = """
        CREATE TABLE Person (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,

            -- some comment for birth_date
            birth_date TEXT,

            created_at TEXT DEFAULT current_timestamp
        )
        """.trimIndent()

        // Create the table
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("DROP TABLE IF EXISTS Person;")
            stmt.executeUpdate(createTableSql)
        }

        // Extract comments for birth_date column
        val birthDateComments = introspector.extractColumnCommentsFromSql(createTableSql, "birth_date")

        assertEquals(1, birthDateComments.size)
        assertEquals("some comment for birth_date", birthDateComments[0])
    }

    @Test
    @DisplayName("Test extracting comments from real database")
    fun testCommentsFromDatabase() {
        // Create a table with various comment styles
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
            DROP TABLE IF EXISTS Person;

            CREATE TABLE Person (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL, -- First name of the person
                last_name TEXT NOT NULL,
                email TEXT UNIQUE,
                phone TEXT,

                -- Date of birth in ISO format
                birth_date TEXT,

                -- Multi-line comment
                -- for registration date
                registration_date TEXT,

                -- Comment at beginning
                last_login TEXT,

                created_at TEXT DEFAULT current_timestamp
            );
            """)
        }

        // Get the table from the database
        val tables = introspector.getTables()
        val personTable = tables.find { it.name == "Person" }
        assertNotNull(personTable)

        // Get the columns
        val firstNameColumn = personTable.columns["first_name"]
        val birthDateColumn = personTable.columns["birth_date"]
        val registrationDateColumn = personTable.columns["registration_date"]
        val lastLoginColumn = personTable.columns["last_login"]

        assertNotNull(firstNameColumn)
        assertNotNull(birthDateColumn)
        assertNotNull(registrationDateColumn)
        assertNotNull(lastLoginColumn)

        assertEquals(1, firstNameColumn.comments.size)
        assertEquals("First name of the person", firstNameColumn.comments[0])
        assertEquals(1, birthDateColumn.comments.size)
        assertEquals("Date of birth in ISO format", birthDateColumn.comments[0])
        assertEquals(2, registrationDateColumn.comments.size)
        assertEquals("Multi-line comment", registrationDateColumn.comments[0])
        assertEquals("for registration date", registrationDateColumn.comments[1])
        assertEquals(1, lastLoginColumn.comments.size)
        assertEquals("Comment at beginning", lastLoginColumn.comments[0])
    }
}
