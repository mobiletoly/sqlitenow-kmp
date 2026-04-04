package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import java.sql.DriverManager
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.Select
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SelectStatementParseTest {

    @Test
    fun `parse captures table aliases and join conditions from real SQL`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT, address_id INTEGER)")
                statement.execute("CREATE TABLE address(id INTEGER PRIMARY KEY, city TEXT)")
            }

            val sql = """
                SELECT p.id, a.city
                FROM person p
                LEFT JOIN address a ON p.address_id = a.id
                WHERE p.id = :personId
            """.trimIndent()

            val selectStatement = SelectStatement.parse(connection, parsePlainSelect(sql))

            assertEquals("person", selectStatement.fromTable)
            assertEquals(listOf("address"), selectStatement.joinTables)
            assertEquals(mapOf("p" to "person", "a" to "address"), selectStatement.tableAliases)
            assertEquals(1, selectStatement.joinConditions.size)
            assertEquals("p", selectStatement.joinConditions.single().leftTable)
            assertEquals("address_id", selectStatement.joinConditions.single().leftColumn)
            assertEquals("a", selectStatement.joinConditions.single().rightTable)
            assertEquals("id", selectStatement.joinConditions.single().rightColumn)
            assertEquals(listOf("personId"), selectStatement.namedParameters)
        }
    }

    @Test
    fun `parse preserves aliased expressions for generated field lookup`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)")
            }

            val sql = """
                SELECT
                    p.id,
                    upper(p.first_name || ' ' || p.last_name) AS display_name
                FROM person p
            """.trimIndent()

            val selectStatement = SelectStatement.parse(connection, parsePlainSelect(sql))
            val displayName = selectStatement.fields.firstOrNull { it.fieldName == "display_name" }

            assertNotNull(displayName)
            assertNotNull(displayName.expression)
            assertTrue(displayName.expression.toString().contains("upper"))
            assertEquals("display_name", displayName.originalColumnName)
        }
    }

    @Test
    fun `parse records limit and offset named parameters`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT)")
            }

            val sql = """
                SELECT p.id, p.name
                FROM person p
                WHERE p.name = :name
                ORDER BY p.id
                LIMIT :limitValue OFFSET :offsetValue
            """.trimIndent()

            val selectStatement = SelectStatement.parse(connection, parsePlainSelect(sql))

            assertEquals(listOf("name", "limitValue", "offsetValue"), selectStatement.namedParameters)
            assertEquals("limitValue", selectStatement.limitNamedParam)
            assertEquals("offsetValue", selectStatement.offsetNamedParam)
        }
    }

    @Test
    fun `parse reports sqlite auto-disambiguated duplicate labels clearly`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, address_id INTEGER)")
                statement.execute("CREATE TABLE address(id INTEGER PRIMARY KEY)")
            }

            val sql = """
                SELECT
                    p.id AS duplicate_alias,
                    a.id AS duplicate_alias
                FROM person p
                JOIN address a ON a.id = p.address_id
            """.trimIndent()

            val error = assertThrows<IllegalArgumentException> {
                SelectStatement.parse(connection, parsePlainSelect(sql))
            }

            assertTrue(error.message!!.contains("Duplicate column aliases"))
            assertTrue(error.message!!.contains("duplicate_alias"))
            assertTrue(error.message!!.contains("SQLite emitted auto-disambiguated names"))
        }
    }

    private fun parsePlainSelect(sql: String) =
        (CCJSqlParserUtil.parse(sql) as Select).plainSelect
}
