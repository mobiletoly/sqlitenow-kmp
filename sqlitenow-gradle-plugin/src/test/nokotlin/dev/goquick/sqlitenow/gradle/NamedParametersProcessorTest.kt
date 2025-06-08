package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class NamedParametersProcessorTest {

    @Test
    fun `test simple query with named parameters`() {
        val sql = "SELECT * FROM users WHERE id = :userId AND name = :userName"
        val processor = NamedParametersProcessor(sql)

        // Check preprocessed SQL
        assertEquals("SELECT * FROM users WHERE id = ? AND name = ?", processor.preprocessedSql)

        // Check parameters
        assertEquals(2, processor.parameters.size)
        assertEquals("userId", processor.parameters[0])
        assertEquals("userName", processor.parameters[1])
    }

    @Test
    fun `test query with duplicate parameters`() {
        val sql = "SELECT * FROM users WHERE id = :userId OR created_by = :userId"
        val processor = NamedParametersProcessor(sql)

        // Check preprocessed SQL
        assertEquals("SELECT * FROM users WHERE id = ? OR created_by = ?", processor.preprocessedSql)

        // Check parameters - should include duplicates
        assertEquals(2, processor.parameters.size)
        assertEquals("userId", processor.parameters[0])
        assertEquals("userId", processor.parameters[1])
    }

    @Test
    fun `test complex query with multiple parameters`() {
        val sql = """
            SELECT u.id, u.name, a.street
            FROM users u
            JOIN addresses a ON a.user_id = u.id
            WHERE u.id = :userId
            AND u.status = :status
            AND a.city = :city
            HAVING COUNT(*) > :minCount
        """.trimIndent()

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(4, processor.parameters.size)
        assertTrue(processor.parameters.contains("userId"))
        assertTrue(processor.parameters.contains("status"))
        assertTrue(processor.parameters.contains("city"))
        assertTrue(processor.parameters.contains("minCount"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("WHERE u.id = ?"))
        assertTrue(processor.preprocessedSql.contains("AND u.status = ?"))
        assertTrue(processor.preprocessedSql.contains("AND a.city = ?"))
        assertTrue(processor.preprocessedSql.contains("HAVING COUNT(*) > ?"))
    }

    @Test
    fun `test parameters in JOIN conditions`() {
        val sql = """
            SELECT u.*, o.*
            FROM users u
            JOIN orders o ON o.user_id = u.id AND o.created_at > :startDate
            WHERE u.status = :status
        """.trimIndent()

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(2, processor.parameters.size)
        assertTrue(processor.parameters.contains("startDate"))
        assertTrue(processor.parameters.contains("status"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("o.created_at > ?"))
        assertTrue(processor.preprocessedSql.contains("WHERE u.status = ?"))
    }

    @Test
    fun `test parameters in HAVING clause`() {
        val sql = """
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
            HAVING COUNT(*) > :minOrders AND MAX(amount) > :minAmount
        """.trimIndent()

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(2, processor.parameters.size)
        assertTrue(processor.parameters.contains("minOrders"))
        assertTrue(processor.parameters.contains("minAmount"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("HAVING COUNT(*) > ? AND MAX(amount) > ?"))
    }

    @Test
    fun `test parameters in subqueries`() {
        val sql = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE amount > :minAmount)
            AND status = :status
        """.trimIndent()

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(2, processor.parameters.size)
        assertTrue(processor.parameters.contains("minAmount"))
        assertTrue(processor.parameters.contains("status"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("WHERE amount > ?"))
        assertTrue(processor.preprocessedSql.contains("AND status = ?"))
    }

    @Test
    fun `test parameters in INSERT statement`() {
        val sql = "INSERT INTO users (name, email, created_at) VALUES (:name, :email, :createdAt)"

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(3, processor.parameters.size)
        assertTrue(processor.parameters.contains("name"))
        assertTrue(processor.parameters.contains("email"))
        assertTrue(processor.parameters.contains("createdAt"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("VALUES (?, ?, ?)"))
    }

    @Test
    fun `test parameters in UPDATE statement`() {
        val sql = "UPDATE users SET name = :name, email = :email WHERE id = :userId"

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(3, processor.parameters.size)
        assertTrue(processor.parameters.contains("name"))
        assertTrue(processor.parameters.contains("email"))
        assertTrue(processor.parameters.contains("userId"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("SET name = ?, email = ?"))
        assertTrue(processor.preprocessedSql.contains("WHERE id = ?"))
    }

    @Test
    fun `test parameters in DELETE statement`() {
        val sql = "DELETE FROM users WHERE id = :userId"

        val processor = NamedParametersProcessor(sql)

        // Check parameters
        assertEquals(1, processor.parameters.size)
        assertTrue(processor.parameters.contains("userId"))

        // Check that all named parameters are replaced with ?
        assertTrue(processor.preprocessedSql.contains("WHERE id = ?"))
    }


}
