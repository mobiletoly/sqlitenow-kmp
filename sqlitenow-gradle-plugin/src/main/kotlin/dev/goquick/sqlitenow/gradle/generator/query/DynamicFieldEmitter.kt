package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder

/**
 * Emits constructor invocations for dynamic fields that map from joined result rows.
 */
internal class DynamicFieldEmitter(
    private val helper: ResultMappingHelper,
) {
    fun emitFromJoined(
        dynamicField: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        sourceVar: String,
        rowsVar: String?,
        baseIndentLevel: Int,
    ): String {
        val notNull = dynamicField.annotations.notNull == true
        val originatesFromPrimaryAlias = dynamicField.aliasPath.size <= 1
        val skipNullGuard = notNull && originatesFromPrimaryAlias
        val needsNullGuard = !skipNullGuard

        val useRowVariable = needsNullGuard && rowsVar != null
        val effectiveSourceVar = if (useRowVariable) "row" else sourceVar

        val constructorContext = ResultMappingHelper.ConstructorRenderContext(
            statement = statement,
            sourceVariableName = effectiveSourceVar,
            additionalIndent = 6,
            enforceNonNull = notNull,
            rowsVar = rowsVar,
            baseIndentLevel = baseIndentLevel,
            aliasPath = dynamicField.aliasPath,
            dynamicFieldMapper = { field, nestedStmt, nestedSrc, nestedRows, nestedIndent ->
                helper.generateDynamicFieldMappingCodeFromJoined(
                    dynamicField = field,
                    statement = nestedStmt,
                    sourceVar = nestedSrc,
                    rowsVar = nestedRows,
                    baseIndentLevel = nestedIndent,
                )
            },
        )
        val constructorArgs = helper.generateConstructorArgumentsFromMapping(mapping, constructorContext)
        val constructorExpression = renderConstructorExpression(
            dynamicField.annotations.propertyType,
            constructorArgs,
        )

        if (!needsNullGuard) {
            return constructorExpression
        }

        val effectiveRowsVar = if (useRowVariable) {
            rowsVar ?: error("rowsVar must be provided when useRowVariable is true")
        } else {
            null
        }

        val guardConfig = NullGuardBuilder.GuardConfig(
            statement = statement,
            mapping = mapping,
            aliasPath = dynamicField.aliasPath,
            columns = mapping.columns,
            notNull = notNull,
            sourceVar = sourceVar,
            rowsVar = effectiveRowsVar,
            baseIndentLevel = baseIndentLevel,
            constructorExpression = constructorExpression,
            fieldName = dynamicField.src.fieldName,
        )
        return helper.buildNullGuard(guardConfig)
    }

    fun renderConstructorExpression(
        typeName: String?,
        constructorArgs: String,
    ): String {
        val nonNullType = typeName ?: "null"
        if (constructorArgs.isBlank()) {
            return "$nonNullType()"
        }

        val builder = IndentedCodeBuilder()
        builder.line("$nonNullType(")
        builder.indent {
            constructorArgs.trim().lines().forEach { raw ->
                val trimmed = raw.trim()
                if (trimmed.isNotEmpty()) builder.line(trimmed)
            }
        }
        builder.line(")")
        return builder.build().trimEnd()
    }
}
