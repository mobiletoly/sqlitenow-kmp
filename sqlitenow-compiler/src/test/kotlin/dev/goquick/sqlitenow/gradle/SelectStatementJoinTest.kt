package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SelectStatementJoinTest {

    @TestFactory
    fun `joined table fields are identified correctly`(): List<DynamicTest> = listOf(
        JoinStatementCase(
            displayName = "simple LEFT JOIN",
            name = "SelectWithLeftJoin",
            sql = "SELECT p.id, p.name, a.street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
            joinTables = listOf("Address"),
            sources = personSources() + fieldSource("street", "a"),
            expectedFieldCountsByAlias = mapOf("p" to 2, "a" to 1),
        ),
        JoinStatementCase(
            displayName = "INNER JOIN",
            name = "SelectWithInnerJoin",
            sql = "SELECT p.id, p.name, a.street FROM Person p INNER JOIN Address a ON p.id = a.person_id",
            joinTables = listOf("Address"),
            sources = personSources() + fieldSource("street", "a"),
            expectedFieldCountsByAlias = mapOf("p" to 2, "a" to 1),
        ),
        JoinStatementCase(
            displayName = "multiple LEFT JOINs",
            name = "SelectWithMultipleJoins",
            sql = "SELECT p.id, p.name, a.street, c.comment FROM person p LEFT JOIN address a ON p.id = a.person_id LEFT JOIN comment c ON p.id = c.person_id",
            joinTables = listOf("address", "comment"),
            sources = personSources() + listOf(fieldSource("street", "a"), fieldSource("comment", "c")),
            expectedFieldCountsByAlias = mapOf("p" to 2, "a" to 1, "c" to 1),
        ),
        JoinStatementCase(
            displayName = "joined table field counts",
            name = "SelectWithJoinedFields",
            sql = "SELECT p.id, p.name, a.street, a.city, c.comment FROM person p LEFT JOIN address a ON p.id = a.person_id LEFT JOIN comment c ON p.id = c.person_id",
            joinTables = listOf("address", "comment"),
            sources = personSources() + listOf(
                fieldSource("street", "a"),
                fieldSource("city", "a"),
                fieldSource("comment", "c"),
            ),
            expectedFieldCountsByAlias = mapOf("p" to 2, "a" to 2, "c" to 1),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val statement = case.statement()

            assertEquals(case.joinTables, statement.src.joinTables)
            case.expectedFieldCountsByAlias.forEach { (alias, count) ->
                assertEquals(count, statement.fields.count { it.src.tableName == alias }, "field count for alias $alias")
            }
        }
    }

    @Test
    fun `LEFT JOIN fields should be nullable by default`() {
        val statement = joinStatement(
            name = "SelectWithNullableLeftJoin",
            sql = "SELECT p.id, p.name, a.street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
            joinTables = listOf("Address"),
            sources = personSources() + fieldSource("street", "a"),
        )

        assertNotNull(statement)

        val streetField = statement.fields.single { it.src.originalColumnName == "street" }
        assertEquals(null, streetField.annotations.notNull)
    }

    @Test
    fun `LEFT JOIN with notNull annotation overrides nullability`() {
        val statement = joinStatement(
            name = "SelectWithNotNullLeftJoin",
            sql = "SELECT p.id, p.name, a.street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
            joinTables = listOf("Address"),
            sources = personSources() + fieldSource("street", "a"),
            fieldAnnotations = mapOf("street" to mapOf(AnnotationConstants.NOT_NULL to true)),
        )

        assertNotNull(statement)

        val streetField = statement.fields.find { it.src.originalColumnName == "street" }
        assertNotNull(streetField)
        assertEquals(true, streetField.annotations.notNull)
    }

    private data class JoinStatementCase(
        val displayName: String,
        val name: String,
        val sql: String,
        val joinTables: List<String>,
        val sources: List<SelectStatement.FieldSource>,
        val expectedFieldCountsByAlias: Map<String, Int>,
    ) {
        fun statement() = joinStatement(
            name = name,
            sql = sql,
            joinTables = joinTables,
            sources = sources,
        )
    }

    private companion object {
        fun personSources(): List<SelectStatement.FieldSource> = listOf(
            fieldSource("id", "p", dataType = "INTEGER"),
            fieldSource("name", "p"),
        )

        fun joinStatement(
            name: String,
            sql: String,
            joinTables: List<String>,
            sources: List<SelectStatement.FieldSource>,
            fieldAnnotations: Map<String, Map<String, Any?>> = emptyMap(),
        ) = annotatedSelectStatementWithFieldAnnotations(
            name = name,
            sql = sql,
            fromTable = "person",
            joinTables = joinTables,
            sources = sources,
            fieldAnnotations = fieldAnnotations,
        )
    }
}
