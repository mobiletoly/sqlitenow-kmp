package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.FunSpec
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class AdapterParameterEmitterTest {

    @Test
    @DisplayName("parameterBindingAdapterNames deduplicates repeated column adapters")
    fun parameterBindingAdapterNamesDeduplicateRepeatedAdapters() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("id", "INTEGER"),
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val statement = annotatedSelectStatement(
            name = "SelectByBirthRange",
            src = selectStatementWithParameters(
                fields = listOf(fieldSource("birth_date", "p", originalColumnName = "birth_date")),
                namedParameters = listOf("birthDateStart", "birthDateEnd"),
                namedParametersToColumns = linkedMapOf(
                    "birthDateStart" to AssociatedColumn.Default("birth_date"),
                    "birthDateEnd" to AssociatedColumn.Default("birth_date"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(regularField("birth_date", "p", originalColumnName = "birth_date")),
        )
        val emitter = AdapterParameterEmitter(
            generatorContext(createTableStatements = listOf(personTable), selectStatements = listOf(statement)),
        )

        assertEquals(
            listOf("birthDateToSqlValue"),
            emitter.parameterBindingAdapterNames("fixture", statement),
        )
    }

    @Test
    @DisplayName("resultConversionAdapterNames keep stable field then mapTo order")
    fun resultConversionAdapterNamesKeepStableOrder() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
                annotatedTableColumn("created_at", "TEXT", notNull = false, adapter = true),
            ),
        )
        val statement = annotatedSelectStatement(
            name = "SelectSummary",
            src = selectStatement(
                fields = listOf(
                    fieldSource("birth_date", "p", originalColumnName = "birth_date"),
                    fieldSource("created_at", "p", originalColumnName = "created_at"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(
                regularField("birth_date", "p", originalColumnName = "birth_date"),
                regularField("created_at", "p", originalColumnName = "created_at"),
            ),
            queryResult = "PersonSummaryRow",
            mapTo = "fixture.model.PersonSummary",
        )
        val emitter = AdapterParameterEmitter(
            generatorContext(createTableStatements = listOf(personTable), selectStatements = listOf(statement)),
        )

        assertEquals(
            listOf("sqlValueToBirthDate", "sqlValueToCreatedAt", "personSummaryRowMapper"),
            emitter.resultConversionAdapterNames("fixture", statement),
        )

        val fn = FunSpec.builder("read")
        emitter.addResultConversionAdapters(fn, "fixture", statement)
        val rendered = fn.build().toString()
        assertTrue(rendered.contains("sqlValueToBirthDate: (kotlin.String?) -> kotlin.String?"))
        assertTrue(rendered.contains("sqlValueToCreatedAt: (kotlin.String?) -> kotlin.String?"))
        assertTrue(rendered.contains("personSummaryRowMapper: (fixture.db.PersonSummaryRow) -> fixture.model.PersonSummary"))
    }
}
