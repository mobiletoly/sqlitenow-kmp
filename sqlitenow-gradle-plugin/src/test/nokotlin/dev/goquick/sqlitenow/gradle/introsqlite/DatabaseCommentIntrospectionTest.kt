package dev.goquick.sqlitenow.gradle.introsqlite

import dev.goquick.sqlitenow.gradle.introsqlite.DatabaseIntrospector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DatabaseCommentIntrospectionTest {
    private lateinit var connection: Connection
    private lateinit var introspector: DatabaseIntrospector

    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")
        introspector = DatabaseIntrospector(connection)

        connection.createStatement().use { stmt ->
            // Create a table with comments
            stmt.executeUpdate(
                """
                -- This is a test users table
                CREATE TABLE test_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,       -- User's full name
                    age INTEGER,   /* User's age in years */
                    email TEXT,
                    is_active INTEGER,  -- Whether the user is active (1) or not (0)
                    profile_data BLOB
                )
                """
            )

            // Create a table with block comment
            stmt.executeUpdate(
                """
                /*
                 * Products table for storing product information
                 * This is a multi-line comment
                 */
                CREATE TABLE products (
                    product_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,  /* Product name */
                    price REAL NOT NULL, -- Product price in USD
                    description TEXT,
                    category TEXT,
                    in_stock INTEGER DEFAULT 1
                )
                """
            )
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting column comments")
    fun testColumnComments() {
        // Since SQLite doesn't store comments in its system tables,
        // we need to provide the original SQL with comments to extract them

        val testUsersSql = """
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name TEXT,       -- User's full name
            age INTEGER,   -- User's age in years
            email TEXT,
            is_active INTEGER,  -- Whether the user is active (1) or not (0)
            profile_data BLOB
        );
        """.trimIndent()

        val productsSql = """
        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,  -- Product name
            price REAL NOT NULL, -- Product price in USD
            description TEXT,
            category TEXT,
            in_stock INTEGER DEFAULT 1
        );
        """.trimIndent()

        // Create the tables
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("DROP TABLE IF EXISTS test_users;")
            stmt.executeUpdate(testUsersSql)

            stmt.executeUpdate("DROP TABLE IF EXISTS products;")
            stmt.executeUpdate(productsSql)
        }

        // Extract comments directly from the SQL
        val nameComments = introspector.extractColumnCommentsFromSql(testUsersSql, "name")
        val ageComments = introspector.extractColumnCommentsFromSql(testUsersSql, "age")
        val isActiveComments = introspector.extractColumnCommentsFromSql(testUsersSql, "is_active")
        val emailComments = introspector.extractColumnCommentsFromSql(testUsersSql, "email")

        val productNameComments = introspector.extractColumnCommentsFromSql(productsSql, "name")
        val priceComments = introspector.extractColumnCommentsFromSql(productsSql, "price")

        assertEquals(1, nameComments.size)
        assertEquals("User's full name", nameComments[0])
        assertEquals(1, ageComments.size)
        assertEquals("User's age in years", ageComments[0])
        assertEquals(1, isActiveComments.size)
        assertEquals("Whether the user is active (1) or not (0)", isActiveComments[0])
        assertTrue(emailComments.isEmpty())

        assertEquals(1, productNameComments.size)
        assertEquals("Product name", productNameComments[0])
        assertEquals(1, priceComments.size)
        assertEquals("Product price in USD", priceComments[0])
    }
}
