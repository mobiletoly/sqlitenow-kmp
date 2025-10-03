package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import dev.goquick.sqlitenow.gradle.generator.query.ResultMappingHelper.ConstructorRenderContext

/**
 * Emits ``.filter``/``.map``/``.distinctBy`` chains for collection-mapped dynamic fields.
 */
internal class CollectionMappingEmitter(
    private val resultMappingHelper: ResultMappingHelper,
) {
    fun emitCollectionMapping(
        builder: IndentedCodeBuilder,
        dynamicField: AnnotatedSelectStatement.Field,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        rowsVar: String,
        baseIndentLevel: Int,
        constructorArgumentsProvider: (
            DynamicFieldMapper.DynamicFieldMapping,
            ConstructorRenderContext,
        ) -> String,
        dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
    ) {
        val propertyType = dynamicField.annotations.propertyType ?: "kotlin.collections.List<Any>"
        val elementType = resultMappingHelper.extractFirstTypeArgumentOrSelf(propertyType)
        val elementSimpleName = elementType.substringAfterLast('.')
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val relevantColumns = mapping.columns.filterNot {
            DynamicFieldUtils.isNestedAlias(it.fieldName, mapping.aliasPrefix)
        }
        val nullCondition = relevantColumns.joinToString(" && ") { column ->
            val propName = propertyNameGenerator.convertToPropertyName(column.fieldName)
            "row.$propName == null"
        }.ifBlank { "false" }

        val requiresNested = resultMappingHelper.requiresNestedConstruction(elementType)
        val rawGroupBy = mapping.groupByColumn?.takeIf { it.isNotBlank() }
            ?: dynamicField.annotations.collectionKey?.takeIf { it.isNotBlank() }
        val groupByProperty = rawGroupBy?.let { propertyNameGenerator.convertToPropertyName(it) }

        val chainBuilder = CollectionMappingBuilder(builder)
        chainBuilder.emit(rowsVar) {
            filter(nullCondition)

            if (requiresNested && groupByProperty != null) {
                groupedMap(
                    groupExpression = "row.$groupByProperty",
                    rowsVarName = "rowsForNested",
                    firstRowVar = "firstNestedRow",
                    elementSimpleName = elementSimpleName,
                ) {
                    val nestedContext = ConstructorRenderContext(
                        statement = statement,
                        sourceVariableName = "firstNestedRow",
                        additionalIndent = 4,
                        enforceNonNull = dynamicField.annotations.notNull == true,
                        rowsVar = "rowsForNested",
                        baseIndentLevel = baseIndentLevel + 2,
                        aliasPath = dynamicField.aliasPath,
                        dynamicFieldMapper = dynamicFieldMapper,
                    )
                    val nestedArgs = resultMappingHelper.generateNestedResultConstructor(
                        targetPropertyType = elementType,
                        mapping = mapping,
                        parentStatement = statement,
                        context = nestedContext,
                    )
                    emitMultiline(nestedArgs)
                }
            } else {
                mapRows(
                    elementSimpleName = elementSimpleName,
                ) {
                    val elementContext = ConstructorRenderContext(
                        statement = statement,
                        sourceVariableName = "row",
                        additionalIndent = 4,
                        enforceNonNull = dynamicField.annotations.notNull == true,
                        rowsVar = rowsVar,
                        baseIndentLevel = baseIndentLevel + 2,
                        aliasPath = dynamicField.aliasPath,
                        dynamicFieldMapper = dynamicFieldMapper,
                    )
                    val elementArgs = constructorArgumentsProvider(mapping, elementContext)
                    emitMultiline(elementArgs)
                }
            }

            val distinctProperty =
                resultMappingHelper.findUniqueFieldForCollection(dynamicField, statement)
            if (distinctProperty != null) {
                val distinctPath = if (requiresNested) {
                    resultMappingHelper.findDistinctByPathForNestedConstruction(
                        elementType,
                        distinctProperty,
                        statement
                    )
                } else {
                    distinctProperty
                }
                distinctBy(distinctPath)
            }
        }
    }
}
