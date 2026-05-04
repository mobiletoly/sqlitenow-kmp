package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.StatementProcessor
import kotlin.test.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class StatementProcessorTest {

    @Test
    @DisplayName("StatementProcessor filters select and execute statements while ignoring schema statements")
    fun statementProcessorFiltersStatementsBySubtype() {
        val selectOne = annotatedSelectStatement(
            name = "SelectOne",
            sources = listOf(fieldSource("id", "person", dataType = "INTEGER")),
        )
        val execute = annotatedInsertStatement(
            name = "InsertOne",
            table = "person",
        )
        val createTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(annotatedTableColumn("id", "INTEGER")),
        )
        val selectTwo = annotatedSelectStatement(
            name = "SelectTwo",
            sources = listOf(fieldSource("name", "person")),
        )

        val processor = StatementProcessor(listOf(selectOne, execute, createTable, selectTwo))

        assertEquals(listOf("SelectOne", "SelectTwo"), processor.selectStatements.map { it.name })
        assertEquals(listOf("InsertOne"), processor.executeStatements.map { it.name })
    }

    @Test
    @DisplayName("StatementProcessor processes selects before executes using cached filtered order")
    fun statementProcessorProcessesStatementsInFilteredOrder() {
        val select = annotatedSelectStatement(
            name = "SelectAlpha",
            sources = listOf(fieldSource("id", "person", dataType = "INTEGER")),
        )
        val executeOne = annotatedInsertStatement(name = "InsertAlpha", table = "person")
        val executeTwo = annotatedDeleteStatement(name = "DeleteAlpha", table = "person")
        val processor = StatementProcessor(listOf(executeOne, select, executeTwo))
        val calls = mutableListOf<String>()

        processor.processStatements(
            onSelectStatement = { calls += "select:${it.name}" },
            onExecuteStatement = { calls += "execute:${it.name}" },
        )

        assertEquals(
            listOf("select:SelectAlpha", "execute:InsertAlpha", "execute:DeleteAlpha"),
            calls,
        )
    }
}
