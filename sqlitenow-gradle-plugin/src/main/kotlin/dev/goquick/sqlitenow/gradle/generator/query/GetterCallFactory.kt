package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterNameResolver
import dev.goquick.sqlitenow.gradle.context.TypeMapping
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter

/**
 * Builds Kotlin snippets that read a column from a statement (optionally piping through adapters).
 */
internal class GetterCallFactory(
    private val adapterConfig: AdapterConfig,
    private val adapterNameResolver: AdapterParameterNameResolver,
    private val selectFieldGenerator: SelectFieldCodeGenerator,
    private val typeMapping: TypeMapping,
) {
    fun buildGetterCall(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        isFromJoinedTable: Boolean,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>,
    ): String {
        val desiredType = selectFieldGenerator.generateProperty(field, propertyNameGenerator).type
        val needsAdapter = isCustomKotlinType(desiredType) || adapterConfig.hasAdapterAnnotation(field, aliasPrefixes)

        return if (needsAdapter) {
            val adapterParamName = adapterNameResolver.resolveOutputAdapterParamNameForField(
                statement = statement,
                field = field,
                tableAliases = tableAliases,
                aliasPrefixes = aliasPrefixes,
                adapterConfig = adapterConfig,
            )

            val baseGetterCall = typeMapping.getGetterCall(
                SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(field.src.dataType),
                columnIndex,
                receiver = "statement",
            )
            val guardedCall = "$adapterParamName($baseGetterCall)"
            if (isFromJoinedTable || desiredType.isNullable) {
                "if (statement.isNull($columnIndex)) null else $guardedCall"
            } else {
                guardedCall
            }
        } else {
            val baseGetterCall = typeMapping.getGetterCall(
                desiredType.copy(nullable = false),
                columnIndex,
                receiver = "statement",
            )
            if (isFromJoinedTable || desiredType.isNullable) {
                "if (statement.isNull($columnIndex)) null else $baseGetterCall"
            } else {
                baseGetterCall
            }
        }
    }

    private fun isCustomKotlinType(type: TypeName): Boolean {
        return !typeMapping.isStandardKotlinType(type.toString())
    }
}
