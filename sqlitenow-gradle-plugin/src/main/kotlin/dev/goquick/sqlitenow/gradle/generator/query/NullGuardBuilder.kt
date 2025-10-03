package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder

/**
 * Builds null-guard expressions for dynamic field construction.
 */
internal class NullGuardBuilder(
    private val helper: ResultMappingHelper,
) {
    data class GuardConfig(
        val statement: AnnotatedSelectStatement,
        val mapping: DynamicFieldMapper.DynamicFieldMapping,
        val aliasPath: List<String>?,
        val columns: List<SelectStatement.FieldSource>,
        val notNull: Boolean,
        val sourceVar: String,
        val rowsVar: String?,
        val baseIndentLevel: Int,
        val constructorExpression: String,
        val fieldName: String,
    )

    fun build(config: GuardConfig): String {
        val relevantColumns = config.columns
            .filterNot { DynamicFieldUtils.isNestedAlias(it.fieldName, config.mapping.aliasPrefix) }
        if (relevantColumns.isEmpty()) {
            return config.constructorExpression
        }

        val joinedNameMap = helper.computeJoinedNameMap(config.statement)

        fun nullCheckCondition(variable: String): String {
            return relevantColumns.joinToString(" && ") { column ->
                val joinedPropertyName = helper.resolveJoinedPropertyName(
                    column = column,
                    mapping = config.mapping,
                    statement = config.statement,
                    aliasPath = config.aliasPath,
                    joinedNameMap = joinedNameMap,
                )
                "$variable.$joinedPropertyName == null"
            }
        }

        val builder = IndentedCodeBuilder()
        if (config.rowsVar != null) {
            val selectorCondition = nullCheckCondition("row")
            val selector = if (selectorCondition == "false") {
                "${config.rowsVar}.firstOrNull()"
            } else {
                "${config.rowsVar}.firstOrNull { row -> !($selectorCondition) }"
            }
            builder.line("$selector?.let { row ->")
            builder.indent(config.baseIndentLevel * 2 + 2) {
                appendTrimmedBlock(this, config.constructorExpression)
            }
            if (config.notNull) {
                builder.line("} ?: error(\"Required dynamic field '${config.fieldName}' is null\")")
            } else {
                builder.line("}")
            }
        } else {
            val nullCondition = nullCheckCondition(config.sourceVar)
            builder.line("if ($nullCondition) {")
            builder.indent(config.baseIndentLevel * 2 + 2) {
                if (config.notNull) {
                    line("error(\"Required dynamic field '${config.fieldName}' is null\")")
                } else {
                    line("null")
                }
            }
            builder.line("} else {")
            builder.indent(config.baseIndentLevel * 2 + 2) {
                appendTrimmedBlock(this, config.constructorExpression)
            }
            builder.line("}")
        }
        return builder.build().trimEnd()
    }

    private fun appendTrimmedBlock(builder: IndentedCodeBuilder, text: String) {
        text.trimEnd().lines().forEach { raw ->
            val trimmed = raw.trim()
            if (trimmed.isEmpty()) {
                builder.line("")
            } else {
                builder.line(trimmed)
            }
        }
    }
}
