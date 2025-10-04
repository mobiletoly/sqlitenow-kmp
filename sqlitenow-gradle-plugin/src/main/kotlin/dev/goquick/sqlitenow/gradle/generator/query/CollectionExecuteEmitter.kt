package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import dev.goquick.sqlitenow.gradle.util.pascalize

/**
 * Emits the `executeAsList` body for collection-mapped queries. Handles validation, grouping key
 * resolution, and defers constructor emission to `ResultMappingHelper`.
 */
internal class CollectionExecuteEmitter(
    private val resultMappingHelper: ResultMappingHelper,
    private val selectFieldGenerator: SelectFieldCodeGenerator,
    private val queryNamespaceName: (String) -> String,
) {
    fun emitExecuteAsListImplementation(
        builder: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        paramsString: String,
        mapAdapterName: String?,
    ) {
        val collectionFields = statement.mappingPlan.includedCollectionFields
        if (collectionFields.isEmpty()) {
            builder.line("emptyList()")
            return
        }

        val collectionKey = statement.annotations.collectionKey?.takeIf { it.isNotBlank() }
            ?: throw IllegalArgumentException(
                "Statement-level annotation '${AnnotationConstants.COLLECTION_KEY}' is required when there are " +
                        "fields with '${AnnotationConstants.MAPPING_TYPE}=collection'. Found collection " +
                        "fields: ${collectionFields.map { it.annotations.propertyName ?: it.src.fieldName }}"
            )

        val groupingField = resultMappingHelper.findFieldByCollectionKey(statement, collectionKey)
            ?: throw IllegalArgumentException("Statement-level collectionKey '$collectionKey' not found in SELECT statement")

        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val groupingKey = resultMappingHelper.getPropertyName(groupingField, propertyNameGenerator)
        val groupingKeyType = selectFieldGenerator
            .generateProperty(groupingField, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            .type
            .copy(nullable = false)
            .toString()

        val joinedClassFullName = if (statement.annotations.queryResult != null) {
            "${statement.annotations.queryResult}_Joined"
        } else {
            val resultClassName = "${pascalize(namespace)}${className}Result"
            "${resultClassName}_Joined"
        }

        val capitalizedNamespace = queryNamespaceName(namespace)
        val baseResultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)

        builder.line("// Read all joined rows first")
        builder.line("val joinedRows = mutableListOf<$joinedClassFullName>()")
        builder.line("while (statement.step()) {")
        builder.indent(by = 2) {
            line("joinedRows.add($capitalizedNamespace.$className.readJoinedStatementResult($paramsString))")
        }
        builder.line("}")
        builder.line("")
        builder.line("// Group joined rows by $groupingKey")
        builder.line("val groupedRows: Map<$groupingKeyType, List<$joinedClassFullName>> = joinedRows.groupBy { it.$groupingKey }")
        builder.line("")
        builder.line("// Create mapped objects with collections")
        builder.line("groupedRows.map { (_, rowsForEntity: List<$joinedClassFullName>) ->")
        builder.indent(by = 2) {
            line("val firstRow = rowsForEntity.first()")
            if (mapAdapterName != null) {
                line("val rawResult = $baseResultType(")
                indent(by = 2) {
                    resultMappingHelper.emitCollectionConstructorBlocks(
                        builder = this,
                        statement = statement,
                        firstRowVar = "firstRow",
                        rowsVar = "rowsForEntity",
                    )
                }
                line(")")
                line("$mapAdapterName(rawResult)")
            } else {
                line("$baseResultType(")
                indent(by = 2) {
                    resultMappingHelper.emitCollectionConstructorBlocks(
                        builder = this,
                        statement = statement,
                        firstRowVar = "firstRow",
                        rowsVar = "rowsForEntity",
                    )
                }
                line(")")
            }
        }
        builder.line("}")
    }
}
