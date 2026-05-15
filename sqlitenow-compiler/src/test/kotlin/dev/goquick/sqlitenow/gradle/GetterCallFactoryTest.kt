package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
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
    fun `buildGetterCall emits main-table getters`() {
        val cases = listOf(
            MainTableGetterCase(
                description = "non-null INTEGER field uses plain getter",
                columnName = "age",
                dataType = "INTEGER",
                notNull = true,
                fieldNullable = false,
                columnIndex = 0,
                expectedGetter = "statement.getLong(0)",
            ),
            MainTableGetterCase(
                description = "custom nullable TEXT field uses adapter-backed getter",
                columnName = "birth_date",
                dataType = "TEXT",
                notNull = false,
                propertyType = "kotlinx.datetime.LocalDate",
                adapter = true,
                fieldNullable = true,
                columnIndex = 1,
                expectedGetter = "if (statement.isNull(1)) null else sqlValueToBirthDate(statement.getText(1))",
            )
        )

        cases.forEach { case ->
            val tables = listOf(
                annotatedCreateTable(
                    tableName = "person",
                    columns = listOf(
                        annotatedTableColumn(
                            name = case.columnName,
                            dataType = case.dataType,
                            notNull = case.notNull,
                            propertyType = case.propertyType,
                            adapter = case.adapter,
                        ),
                    ),
                ),
            )
            val field = regularField(
                fieldName = case.columnName,
                tableName = "person",
                originalColumnName = case.columnName,
                dataType = case.dataType,
                isNullable = case.fieldNullable,
            )

            val getter = buildGetterCall(field = field, tables = tables, columnIndex = case.columnIndex)

            assertEquals(case.expectedGetter, getter, case.description)
        }
    }

    @Test
    fun `buildGetterCall guards nullable reads`() {
        val cases = listOf(
            NullableGuardGetterCase(
                description = "joined table read uses null guard even for non-null Kotlin type",
                tableName = "profile",
                fieldNullable = false,
                columnIndex = 2,
                isFromJoinedTable = true,
                expectedGetter = "if (statement.isNull(2)) null else statement.getText(2)",
            ),
            NullableGuardGetterCase(
                description = "nullable main-table field uses null guard",
                tableName = "person",
                fieldNullable = true,
                columnIndex = 3,
                expectedGetter = "if (statement.isNull(3)) null else statement.getText(3)",
            )
        )

        cases.forEach { case ->
            val field = regularField("nickname", case.tableName, "nickname", "TEXT", isNullable = case.fieldNullable)
            val getter = buildGetterCall(
                field = field,
                columnIndex = case.columnIndex,
                isFromJoinedTable = case.isFromJoinedTable,
            )

            assertEquals(case.expectedGetter, getter, case.description)
        }
    }

    @Test
    fun `buildExecuteReturningGetter keeps nullable adapter invocation`() {
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
        val field = regularField(
            fieldName = "birth_date",
            tableName = "person",
            originalColumnName = "birth_date",
            dataType = "TEXT",
            isNullable = true,
        )
        val getter = newFactory(tables).buildExecuteReturningGetter(
            field = field,
            desiredType = ClassName("kotlinx.datetime", "LocalDate").copy(nullable = true),
            columnIndex = 4,
        )

        assertEquals(
            "if (statement.isNull(4)) sqlValueToBirthDate(null) else sqlValueToBirthDate(statement.getText(4))",
            getter,
        )
    }

    private data class MainTableGetterCase(
        val description: String,
        val columnName: String,
        val dataType: String,
        val notNull: Boolean,
        val propertyType: String? = null,
        val adapter: Boolean = false,
        val fieldNullable: Boolean,
        val columnIndex: Int,
        val expectedGetter: String,
    )

    private data class NullableGuardGetterCase(
        val description: String,
        val tableName: String,
        val fieldNullable: Boolean,
        val columnIndex: Int,
        val isFromJoinedTable: Boolean = false,
        val expectedGetter: String,
    )

    private fun buildGetterCall(
        field: dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement.Field,
        tables: List<dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement> = emptyList(),
        columnIndex: Int,
        isFromJoinedTable: Boolean = false,
    ): String = newFactory(tables).buildGetterCall(
        statement = simpleStatement(field),
        field = field,
        columnIndex = columnIndex,
        propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
        isFromJoinedTable = isFromJoinedTable,
        tableAliases = mapOf(field.src.tableName to field.src.tableName),
        aliasPrefixes = emptyList(),
    )

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
