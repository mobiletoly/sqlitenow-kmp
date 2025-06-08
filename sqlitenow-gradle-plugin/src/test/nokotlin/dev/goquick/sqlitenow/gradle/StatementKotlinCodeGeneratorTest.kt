package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue

class StatementKotlinCodeGeneratorTest {
    private lateinit var connection: Connection
    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL,
                    email TEXT,
                    created_at TEXT,
                    is_active INTEGER
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test generating a data class from a simple SELECT statement")
    fun testGenerateDataClassFromSimpleSelect() {
        val sql = """
            SELECT id, username, email, created_at, is_active
            FROM users
            WHERE email LIKE :emailPattern
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateDataStructureCode(className = "UserResult")
        val generatedCode = fileSpecBuilder.build().toString()

        assertTrue(generatedCode.contains("data class UserResult"))
        assertTrue(generatedCode.contains("val id: Int"))
        assertTrue(generatedCode.contains("val username: String"))
        assertTrue(generatedCode.contains("val email: String"))
        assertTrue(generatedCode.contains("val createdAt: String"))
        assertTrue(generatedCode.contains("val isActive: Int"))

        println("Generated code:\n$generatedCode")
    }

    @Test
    @DisplayName("Test generating a data class from a SELECT statement with aliases")
    fun testGenerateDataClassFromSelectWithAliases() {
        val sql = """
            SELECT
                id AS user_id,
                username,
                email,
                created_at,
                is_active AS active
            FROM users
            WHERE email LIKE :emailPattern
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateDataStructureCode(className = "UserWithAliases")
        val generatedCode = fileSpecBuilder.build().toString()

        assertTrue(generatedCode.contains("data class UserWithAliases"))
        assertTrue(generatedCode.contains("val userId: Int"))
        assertTrue(generatedCode.contains("val username: String"))
        assertTrue(generatedCode.contains("val email: String"))
        assertTrue(generatedCode.contains("val createdAt: String"))
        assertTrue(generatedCode.contains("val active: Int"))

        println("Generated code with aliases:\n$generatedCode")
    }

    @Test
    @DisplayName("Test generating a data class from a SELECT statement with expressions")
    fun testGenerateDataClassFromSelectWithExpressions() {
        val sql = """
            SELECT
                id,
                username,
                UPPER(email) AS email_upper,
                SUBSTR(created_at, 1, 10) AS created_date,
                CASE WHEN is_active = 1 THEN 'Yes' ELSE 'No' END AS active_text,
                COUNT(*) OVER() AS total_count
            FROM users
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateDataStructureCode(className = "UserWithExpressions")
        val generatedCode = fileSpecBuilder.build().toString()

        println("Generated code with expressions:\n$generatedCode")

        assertTrue(generatedCode.contains("data class UserWithExpressions"))
        assertTrue(generatedCode.contains("val id: Int"))
        assertTrue(generatedCode.contains("val username: String"))
        assertTrue(generatedCode.contains("val emailUpper: String"))
        assertTrue(generatedCode.contains("val createdDate: String"))
        assertTrue(generatedCode.contains("val activeText: String"))
        assertTrue(generatedCode.contains("val totalCount: String"))
    }

    @Test
    @DisplayName("Test error when using SELECT * with tables that have columns with the same name")
    fun testErrorWhenUsingSelectStarWithDuplicateColumns() {
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address TEXT NOT NULL
                )
            """)
        }

        val sql = """
            SELECT p.*, a.*
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
        """.trimIndent()

        val exception = assertThrows<IllegalArgumentException> {
            StatementKotlinCodeGenerator(sql, connection, emptyList())
        }

        assertTrue(exception.message?.contains("Duplicate field names") == true)
    }

    @Test
    @DisplayName("Test error when using duplicate aliases")
    fun testErrorWhenUsingDuplicateAliases() {
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address_type TEXT,
                    street TEXT
                )
            """)
        }

        val sql = """
            SELECT
                p.id AS some_id,
                p.first_name,
                p.last_name,
                a.id AS some_id,
                a.address_type,
                a.street
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
        """.trimIndent()

        val exception = assertThrows<IllegalArgumentException> {
            StatementKotlinCodeGenerator(sql, connection, emptyList())
        }

        assertTrue(exception.message?.contains("Duplicate field names") == true)
    }
}
