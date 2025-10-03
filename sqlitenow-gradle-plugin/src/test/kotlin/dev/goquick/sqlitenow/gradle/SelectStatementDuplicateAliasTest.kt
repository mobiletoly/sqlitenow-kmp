package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.DriverManager
import kotlin.test.assertTrue

class SelectStatementDuplicateAliasTest {

    @Test
    fun `parse throws when duplicate column aliases present`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT)")
            }

            val sql = "SELECT id, id FROM person"
            val parsed = CCJSqlParserUtil.parse(sql)
            val plainSelect = (parsed as net.sf.jsqlparser.statement.select.Select).selectBody as PlainSelect

            val exception = assertThrows<IllegalArgumentException> {
                SelectStatement.parse(connection, plainSelect)
            }
            assertTrue(exception.message?.contains("Duplicate column aliases") == true)
            assertTrue(exception.message?.contains("id") == true)
        }
    }
}
