package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateParametersProcessor
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import java.sql.DriverManager
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.update.Update
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ExecuteParameterProcessorTest {

    @Test
    fun `update parameter processor rewrites IN parameters and captures cast types`() {
        val sql = """
            UPDATE person
            SET status = :status
            WHERE id IN :ids
              AND score >= CAST(:minScore AS INTEGER)
        """.trimIndent()

        val processor = UpdateParametersProcessor(CCJSqlParserUtil.parse(sql) as Update)

        assertEquals(listOf("status", "ids", "minScore"), processor.parameters)
        assertTrue(processor.processedSql.contains("id IN (SELECT value FROM json_each(?))"))
        assertEquals("INTEGER", processor.parameterCastTypes["minScore"])
    }

    @Test
    fun `update statement exposes collection parameters through associated columns`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, status TEXT, group_id INTEGER)")
            }

            val sql = """
                UPDATE person
                SET status = :status
                WHERE group_id IN :groupIds
            """.trimIndent()

            val updateStatement = UpdateStatement.parse(CCJSqlParserUtil.parse(sql) as Update, connection)

            val associated = updateStatement.namedParametersToColumns["groupIds"]
            assertTrue(associated is AssociatedColumn.Collection)
            assertEquals("group_id", (associated as AssociatedColumn.Collection).columnName)
        }
    }

    @Test
    fun `delete statement exposes WITH clause parameters collection parameters and cast types`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, age INTEGER, group_id INTEGER, owner_id INTEGER)")
            }

            val sql = """
                WITH target_ids AS (
                    SELECT id
                    FROM person
                    WHERE owner_id = :ownerId
                )
                DELETE FROM person
                WHERE id IN (SELECT id FROM target_ids)
                  AND group_id IN :groupIds
                  AND age >= CAST(:minAge AS INTEGER)
            """.trimIndent()

            val deleteStatement = DeleteStatement.parse(CCJSqlParserUtil.parse(sql) as Delete, connection)

            assertEquals(listOf("ownerId", "groupIds", "minAge"), deleteStatement.namedParameters)
            assertEquals(1, deleteStatement.withSelectStatements.size)
            assertTrue(deleteStatement.sql.contains("group_id IN (SELECT value FROM json_each(?))"))
            assertEquals("INTEGER", deleteStatement.parameterCastTypes["minAge"])

            val associated = deleteStatement.namedParametersToColumns["groupIds"]
            assertTrue(associated is AssociatedColumn.Collection)
            assertEquals("group_id", (associated as AssociatedColumn.Collection).columnName)
        }
    }
}
