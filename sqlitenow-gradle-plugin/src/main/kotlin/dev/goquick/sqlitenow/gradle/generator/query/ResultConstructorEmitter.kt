package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver

/**
 * Emits the property assignment block for constructing a mapped result object.
 */
internal class ResultConstructorEmitter(
    private val helper: ResultMappingHelper,
) {
    fun emitConstructorArguments(
        targetStatement: AnnotatedSelectStatement,
        parentStatement: AnnotatedSelectStatement,
        sourceVar: String,
        rowsVar: String?,
        additionalIndent: Int,
        baseIndentLevel: Int,
        enforceNonNull: Boolean,
        dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
    ): String {
        val indent = "  ".repeat(additionalIndent + 3)
        val propertyIndent = indent
        val propertyNameGenerator = targetStatement.annotations.propertyNameGenerator
        val mappingPlan = targetStatement.mappingPlan
        val joinedNameMap = helper.computeJoinedNameMap(parentStatement)
        val properties = mutableListOf<String>()

        mappingPlan.regularFields.forEach { field ->
            val propName = helper.getPropertyName(field, propertyNameGenerator)
            val tableAlias = field.src.tableName.orEmpty()
            val candidateKeys = buildList {
                add(JoinedPropertyNameResolver.JoinedFieldKey(tableAlias, field.src.fieldName))
                field.src.originalColumnName.takeIf { it.isNotBlank() }?.let { original ->
                    add(JoinedPropertyNameResolver.JoinedFieldKey(tableAlias, original))
                }
            }
            val joinedPropertyName =
                candidateKeys.firstNotNullOfOrNull { key -> joinedNameMap[key] }
                    ?: parentStatement.annotations.propertyNameGenerator
                        .convertToPropertyName(field.src.fieldName)
            val isNullable = helper.isTargetPropertyNullable(field.src)
            val expression = when {
                enforceNonNull && !isNullable -> "$sourceVar.$joinedPropertyName!!"
                isNullable -> "$sourceVar.$joinedPropertyName"
                else -> "$sourceVar.$joinedPropertyName!!"
            }
            properties += "$propName = $expression"
        }

        mappingPlan.includedDynamicFields.forEach { field ->
            val propName = helper.getPropertyName(field, propertyNameGenerator)
            val fieldExpression = dynamicFieldMapper(
                field,
                targetStatement,
                sourceVar,
                rowsVar,
                baseIndentLevel + additionalIndent + 2,
            )
            properties += "$propName = $fieldExpression"
        }

        return if (properties.isEmpty()) {
            ""
        } else {
            properties.joinToString(",\n$propertyIndent")
        }
    }
}
