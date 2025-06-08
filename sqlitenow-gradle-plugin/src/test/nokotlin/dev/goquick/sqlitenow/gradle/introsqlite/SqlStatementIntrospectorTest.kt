package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.sql.Connection

class SqlStatementIntrospectorTest {

    @Test
    fun `test extractLeadingComments with line comments`() {
        val sql = """
            -- Comment 1
            -- Comment 2
            SELECT * FROM users
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val (comments, sqlWithoutComments) = introspector.extractLeadingComments()

        assertEquals(2, comments.size)
        assertEquals("Comment 1", comments[0])
        assertEquals("Comment 2", comments[1])
        assertEquals("SELECT * FROM users", sqlWithoutComments)
    }



    @Test
    fun `test extractLeadingComments with multiple line comments`() {
        val sql = """
            -- Comment 1
            -- Comment 2
            -- Comment 3
            SELECT * FROM users
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val (comments, sqlWithoutComments) = introspector.extractLeadingComments()

        assertEquals(3, comments.size)
        assertEquals("Comment 1", comments[0])
        assertEquals("Comment 2", comments[1])
        assertEquals("Comment 3", comments[2])
        assertEquals("SELECT * FROM users", sqlWithoutComments)
    }

    @Test
    fun `test extractInnerComments`() {
        val sql = """
            -- Comment 1
            SELECT * FROM users -- Comment 2
            WHERE id > 0 -- Comment 3
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val comments = introspector.extractInnerComments(sql)

        assertEquals(3, comments.size)
        assertEquals("Comment 1", comments[0])
        assertEquals("Comment 2", comments[1])
        assertEquals("Comment 3", comments[2])
    }

    @Test
    fun `test introspect SELECT statement`() {
        val sql = """
            -- Top comment
            SELECT id, name, email FROM users WHERE id > :userId
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val info = introspector.introspect()

        assertEquals("SELECT", info.statementType)
        assertEquals("users", info.tableName)
        assertEquals(1, info.preparedSql.parameters.size)
        assertEquals("userId", info.preparedSql.parameters[0].name)
        assertEquals(1, info.topComments.size)
        assertEquals("Top comment", info.topComments[0])
    }

    @Test
    fun `test introspect INSERT statement`() {
        val sql = """
            -- Insert comment
            INSERT INTO users (name, email) VALUES (:name, :email)
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val info = introspector.introspect()

        assertEquals("INSERT", info.statementType)
        assertEquals("users", info.tableName)
        assertEquals(2, info.preparedSql.parameters.size)
        assertEquals("name", info.preparedSql.parameters[0].name)
        assertEquals("email", info.preparedSql.parameters[1].name)
        assertEquals(1, info.topComments.size)
        assertEquals("Insert comment", info.topComments[0])
    }

    @Test
    fun `test introspect UPDATE statement`() {
        val sql = """
            -- Update comment
            UPDATE users SET name = :name WHERE id = :id
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val info = introspector.introspect()

        assertEquals("UPDATE", info.statementType)
        assertEquals("users", info.tableName)
        assertEquals(2, info.preparedSql.parameters.size)
        assertEquals("name", info.preparedSql.parameters[0].name)
        assertEquals("id", info.preparedSql.parameters[1].name)
        assertEquals(1, info.topComments.size)
        assertEquals("Update comment", info.topComments[0])
    }

    @Test
    fun `test introspect DELETE statement`() {
        val sql = """
            -- Delete comment
            DELETE FROM users WHERE id = :id
        """.trimIndent()

        val introspector = SqlStatementIntrospector(sql)
        val info = introspector.introspect()

        assertEquals("DELETE", info.statementType)
        assertEquals("users", info.tableName)
        assertEquals(1, info.preparedSql.parameters.size)
        assertEquals("id", info.preparedSql.parameters[0].name)
        assertEquals(1, info.topComments.size)
        assertEquals("Delete comment", info.topComments[0])
    }
}
