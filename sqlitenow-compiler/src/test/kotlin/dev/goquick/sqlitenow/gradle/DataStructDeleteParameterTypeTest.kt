package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DataStructDeleteParameterTypeTest : DataStructParameterTestSupport() {
    @Test
    @DisplayName("Test DELETE statement with WITH clause parameter handling")
    fun testDeleteWithClauseParameterHandling() {
        val withSelectStatement = SelectStatement(
            sql = "SELECT id FROM Person WHERE age = :myAge",
            fromTable = "person",
            joinTables = emptyList(),
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "id",
                    tableName = "person",
                    originalColumnName = "id",
                    dataType = "INTEGER"
                )
            ),
            namedParameters = listOf("myAge"),
            namedParametersToColumns = mapOf(
                "myAge" to AssociatedColumn.Default("age")
            ),
            offsetNamedParam = null,
            limitNamedParam = null
        )
        val fixture = createDeleteFixture(
            queryFileName = "deleteOlderThan.sql",
            querySql = "WITH IDsToDelete AS (SELECT id FROM Person WHERE age = :myAge) DELETE FROM Person WHERE id IN (SELECT id FROM IDsToDelete) AND score = :myScore;",
            createTableSql = """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY,
                    age INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    birth_date TEXT
                )
            """,
            tableColumns = deleteWithClauseColumns(ageNotNull = true, includeScore = true),
            deleteSql = "WITH IDsToDelete AS (SELECT id FROM Person WHERE age = :myAge) DELETE FROM Person WHERE id IN (SELECT id FROM IDsToDelete) AND score = :myScore",
            namedParameters = listOf("myScore"),
            namedParametersToColumns = mapOf("myScore" to AssociatedColumn.Default("score")),
            withSelectStatements = listOf(withSelectStatement),
        )
        try {
            val myAgeType = fixture.generator.inferParameterType("myAge", fixture.statement)
            assertEquals(
                "kotlin.Long", myAgeType.toString(),
                "myAge parameter from WITH clause should map to Long type"
            )

            val myScoreType = fixture.generator.inferParameterType("myScore", fixture.statement)
            assertEquals(
                "kotlin.Long", myScoreType.toString(),
                "myScore parameter from DELETE clause should map to Long type"
            )

            val generatedCode = fixture.generator
                .generateNamespaceDataStructuresCode("person", "com.example.db")
                .build()
                .toString()
            assertTrue(generatedCode.contains("object DeleteOlderThan"), "Should generate DeleteOlderThan query object")
            assertTrue(generatedCode.contains("data class Params"), "Should generate Params data class")
            assertTrue(generatedCode.contains("val myAge: Long"), "Should include myAge parameter from WITH clause")
            assertTrue(generatedCode.contains("val myScore: Long"), "Should include myScore parameter from DELETE clause")
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test DELETE WITH clause nullable parameter binding")
    fun testDeleteWithClauseNullableParameterBinding() {
        val withSelectStatement = SelectStatement(
            sql = "SELECT id FROM Person WHERE age = :myAge",
            fromTable = "person",
            joinTables = emptyList(),
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "id",
                    tableName = "person",
                    originalColumnName = "id",
                    dataType = "INTEGER"
                )
            ),
            namedParameters = listOf("myAge"),
            namedParametersToColumns = mapOf(
                "myAge" to AssociatedColumn.Default("age")
            ),
            offsetNamedParam = null,
            limitNamedParam = null
        )
        val fixture = createDeleteFixture(
            queryFileName = "deleteByAge.sql",
            querySql = "WITH IDsToDelete AS (SELECT id FROM Person WHERE age = :myAge) DELETE FROM Person WHERE id IN (SELECT id FROM IDsToDelete);",
            createTableSql = """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY,
                    age INTEGER
                )
            """,
            tableColumns = deleteWithClauseColumns(ageNotNull = false, includeScore = false),
            deleteSql = "WITH IDsToDelete AS (SELECT id FROM Person WHERE age = :myAge) DELETE FROM Person WHERE id IN (SELECT id FROM IDsToDelete)",
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            withSelectStatements = listOf(withSelectStatement),
        )
        try {
            val columnLookup = ColumnLookup(fixture.createTableStatements, createViewStatements = emptyList())
            val isNullable = columnLookup.isParameterNullable(fixture.statement, "myAge")
            assertTrue(isNullable, "myAge parameter from WITH clause should be detected as nullable")

            val myAgeType = fixture.generator.inferParameterType("myAge", fixture.statement)
            assertEquals(
                "kotlin.Long?", myAgeType.toString(),
                "myAge parameter should be nullable Long due to nullable age column"
            )
        } finally {
            fixture.cleanup()
        }
    }

    private fun deleteWithClauseColumns(
        ageNotNull: Boolean,
        includeScore: Boolean,
    ): List<ParameterTableColumn> = buildList {
        add(ParameterTableColumn(name = "id", dataType = "INTEGER", notNull = true))
        add(ParameterTableColumn(name = "age", dataType = "INTEGER", notNull = ageNotNull))
        if (includeScore) {
            add(ParameterTableColumn(name = "score", dataType = "INTEGER", notNull = true))
        }
    }

}
