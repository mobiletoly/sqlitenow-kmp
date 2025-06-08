package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertTrue

class StatementInputParametersTest {
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
    @DisplayName("Test generating a parameters class from a simple SELECT statement")
    fun testGenerateParametersClassFromSimpleSelect() {
        val sql = """
            SELECT id, username, email, created_at, is_active
            FROM users
            WHERE email LIKE :emailPattern AND username = :username
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateInputParametersCode(className = "UserQuery")
        val generatedCode = fileSpecBuilder.build().toString()

        println("Generated parameters class:\n$generatedCode")

        // Verify the generated code contains a data class with the expected name
        assertTrue(generatedCode.contains("data class UserQueryParams"))

        // Verify the generated code contains properties for each parameter
        assertTrue(generatedCode.contains("val emailPattern: String"))
        assertTrue(generatedCode.contains("val username: String"))
    }

    @Test
    @DisplayName("Test generating a parameters class from a SELECT statement with snake_case parameters")
    fun testGenerateParametersClassWithSnakeCaseParameters() {
        val sql = """
            SELECT id, username, email, created_at, is_active
            FROM users
            WHERE created_at > :created_after AND is_active = :is_active
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateInputParametersCode(className = "UserFilter")
        val generatedCode = fileSpecBuilder.build().toString()

        println("Generated parameters class with snake_case parameters:\n$generatedCode")

        // Verify the generated code contains a data class with the expected name
        assertTrue(generatedCode.contains("data class UserFilterParams"))

        // Verify the generated code contains properties with camelCase names (converted from snake_case)
        assertTrue(generatedCode.contains("val createdAfter: String"))
        assertTrue(generatedCode.contains("val isActive: String"))
    }

    @Test
    @DisplayName("Test generating a parameters class with propertyNameGenerator=plain annotation")
    fun testGenerateParametersClassWithPlainPropertyNames() {
        val sql = """
            -- @@propertyNameGenerator=plain
            SELECT id, username, email, created_at, is_active
            FROM users
            WHERE created_at > :created_after AND is_active = :is_active
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sql, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateInputParametersCode(className = "UserFilter")
        val generatedCode = fileSpecBuilder.build().toString()

        println("Generated parameters class with plain property names:\n$generatedCode")

        // Verify the generated code contains a data class with the expected name
        assertTrue(generatedCode.contains("data class UserFilterParams"))

        // Verify the generated code contains properties with the original snake_case names
        assertTrue(generatedCode.contains("val created_after: String"))
        assertTrue(generatedCode.contains("val is_active: String"))
    }
}
