package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateViewStatement
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class AdapterConfigTest {

    @Test
    @DisplayName("hasAdapterAnnotation resolves annotations from field, table, and view sources")
    fun hasAdapterAnnotationResolvesAcrossFieldTableAndView() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("id", "INTEGER"),
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val view = AnnotatedCreateViewStatement(
            name = "PersonView",
            src = CreateViewStatement(
                sql = "CREATE VIEW person_view AS SELECT p.birth_date AS birth_date FROM person p",
                viewName = "person_view",
                selectStatement = selectStatement(
                    fields = listOf(fieldSource("birth_date", "p", originalColumnName = "birth_date")),
                    fromTable = "person_view",
                    tableAliases = mapOf("p" to "person"),
                ),
            ),
            annotations = statementAnnotations(),
            fields = listOf(
                AnnotatedCreateViewStatement.Field(
                    src = fieldSource("birth_date", "person", originalColumnName = "birth_date"),
                    annotations = FieldAnnotationOverrides(
                        propertyName = null,
                        propertyType = null,
                        notNull = null,
                        adapter = null,
                    ),
                ),
            ),
            dynamicFields = emptyList(),
        )
        val adapterConfig = AdapterConfig(
            columnLookup = ColumnLookup(listOf(personTable), listOf(view)),
            createTableStatements = listOf(personTable),
            createViewStatements = listOf(view),
            packageName = "fixture.db",
        )

        val directField = regularField(
            fieldName = "birth_date",
            tableName = "person",
            adapter = true,
        )
        val tableField = regularField(
            fieldName = "birth_date",
            tableName = "person",
        )
        val viewField = regularField(
            fieldName = "birth_date",
            tableName = "person_view",
        )

        assertTrue(adapterConfig.hasAdapterAnnotation(directField))
        assertTrue(adapterConfig.hasAdapterAnnotation(tableField))
        assertTrue(adapterConfig.hasAdapterAnnotation(viewField))
    }

    @Test
    @DisplayName("collectAllParamConfigs exposes field, table, and mapTo adapter configs")
    fun collectAllParamConfigsIncludesFieldTableAndMapToAdapters() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("id", "INTEGER"),
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val statement = annotatedSelectStatement(
            name = "SelectSummary",
            src = selectStatementWithParameters(
                fields = listOf(fieldSource("birth_date", "p", originalColumnName = "birth_date")),
                namedParameters = listOf("birthDateStart"),
                namedParametersToColumns = linkedMapOf(
                    "birthDateStart" to dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn.Default("birth_date"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(
                regularField("birth_date", "p", originalColumnName = "birth_date"),
            ),
            queryResult = "PersonSummaryRow",
            mapTo = "fixture.model.PersonSummary",
        )
        val adapterConfig = AdapterConfig(
            columnLookup = ColumnLookup(listOf(personTable), emptyList()),
            createTableStatements = listOf(personTable),
            packageName = "fixture.db",
        )

        val configs = adapterConfig.collectAllParamConfigs(statement, namespace = "person")

        assertTrue(configs.any { it.kind == AdapterConfig.AdapterKind.INPUT && it.adapterFunctionName == "birthDateToSqlValue" })
        assertTrue(configs.any { it.kind == AdapterConfig.AdapterKind.RESULT_FIELD && it.adapterFunctionName == "sqlValueToBirthDate" })
        assertTrue(configs.any {
            it.kind == AdapterConfig.AdapterKind.MAP_RESULT &&
                it.adapterFunctionName == "personSummaryRowMapper" &&
                it.providerNamespace == "person"
        })
        assertTrue(configs.none { it.adapterFunctionName == AnnotationConstants.ADAPTER_CUSTOM })
    }
}
