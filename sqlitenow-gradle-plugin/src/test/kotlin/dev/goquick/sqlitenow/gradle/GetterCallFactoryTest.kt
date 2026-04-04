package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterNameResolver
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.context.TypeMapping
import dev.goquick.sqlitenow.gradle.generator.query.GetterCallFactory
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import kotlin.test.Test
import kotlin.test.assertEquals

class GetterCallFactoryTest {

    @Test
    fun `buildGetterCall emits plain getter for non-null main-table fields`() {
        val tables = listOf(
            annotatedCreateTable(
                tableName = "person",
                columns = listOf(
                    annotatedTableColumn(
                        name = "age",
                        dataType = "INTEGER",
                        notNull = true,
                    ),
                ),
            ),
        )
        val field = regularField("age", "person", "age", "INTEGER", isNullable = false)
        val statement = simpleStatement(field)

        val getter = newFactory(tables).buildGetterCall(
            statement = statement,
            field = field,
            columnIndex = 0,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = false,
            tableAliases = mapOf("person" to "person"),
            aliasPrefixes = emptyList(),
        )

        assertEquals("statement.getLong(0)", getter)
    }

    @Test
    fun `buildGetterCall emits adapter-backed getter for custom result types`() {
        val tables = listOf(
            annotatedCreateTable(
                tableName = "person",
                columns = listOf(
                    annotatedTableColumn(
                        name = "birth_date",
                        dataType = "TEXT",
                        notNull = false,
                        propertyType = "kotlinx.datetime.LocalDate",
                        adapter = true,
                    ),
                ),
            ),
        )
        val field = regularField("birth_date", "person", "birth_date", "TEXT", isNullable = true)
        val statement = simpleStatement(field)

        val getter = newFactory(tables).buildGetterCall(
            statement = statement,
            field = field,
            columnIndex = 1,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = false,
            tableAliases = mapOf("person" to "person"),
            aliasPrefixes = emptyList(),
        )

        assertEquals(
            "if (statement.isNull(1)) null else sqlValueToBirthDate(statement.getText(1))",
            getter,
        )
    }

    @Test
    fun `buildGetterCall guards joined-table reads even for non-null kotlin types`() {
        val field = regularField("nickname", "profile", "nickname", "TEXT", isNullable = false)
        val statement = simpleStatement(field)

        val getter = newFactory().buildGetterCall(
            statement = statement,
            field = field,
            columnIndex = 2,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = true,
            tableAliases = mapOf("profile" to "profile"),
            aliasPrefixes = emptyList(),
        )

        assertEquals("if (statement.isNull(2)) null else statement.getText(2)", getter)
    }

    @Test
    fun `buildGetterCall guards nullable main-table fields`() {
        val field = regularField("nickname", "person", "nickname", "TEXT", isNullable = true)
        val statement = simpleStatement(field)

        val getter = newFactory().buildGetterCall(
            statement = statement,
            field = field,
            columnIndex = 3,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = false,
            tableAliases = mapOf("person" to "person"),
            aliasPrefixes = emptyList(),
        )

        assertEquals("if (statement.isNull(3)) null else statement.getText(3)", getter)
    }

    private fun simpleStatement(field: dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement.Field) =
        annotatedSelectStatement(
            src = selectStatement(
                fields = listOf(field.src),
                tableAliases = mapOf(field.src.tableName to field.src.tableName),
            ),
            fields = listOf(field),
        )

    private fun newFactory(
        tables: List<dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement> = emptyList(),
    ): GetterCallFactory {
        val columnLookup = ColumnLookup(tables, emptyList())
        return GetterCallFactory(
            adapterConfig = AdapterConfig(
                columnLookup = columnLookup,
                createTableStatements = tables,
                createViewStatements = emptyList(),
                packageName = "com.example.db",
            ),
            adapterNameResolver = AdapterParameterNameResolver(),
            selectFieldGenerator = SelectFieldCodeGenerator(
                createTableStatements = tables,
                createViewStatements = emptyList(),
                packageName = "com.example.db",
            ),
            typeMapping = TypeMapping(),
        )
    }
}
