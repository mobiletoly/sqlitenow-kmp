package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class DebugColumnNameTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory SQLite database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test tables
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Debug column name extraction for aliased columns")
    fun debugColumnNameExtraction() {
        val sql = """
            SELECT p.id AS person_id, p.created_at AS person_created_at
            FROM Person p
        """.trimIndent()

        // Execute the query and print the metadata
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery(sql)
            val metaData = rs.metaData
            val columnCount = metaData.columnCount

            println("Column metadata:")
            for (i in 1..columnCount) {
                val columnLabel = metaData.getColumnLabel(i)
                val columnName = metaData.getColumnName(i)
                val tableName = try { metaData.getTableName(i) } catch (e: Exception) { "N/A" }

                println("Column $i:")
                println("  Label: $columnLabel")
                println("  Name: $columnName")
                println("  Table: $tableName")
            }
        }

        // Now use our introspector
        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        println("\nField sources:")
        for (fieldSource in statementInfo.fieldSources) {
            println("  ${fieldSource.fieldName}:")
            println("    tableName: ${fieldSource.tableName}")
            println("    columnName: ${fieldSource.columnName}")
            println("    expression: ${fieldSource.expression}")
            println("    dataType: ${fieldSource.dataType}")
        }
    }
}
