package dev.goquick.sqlitenow.gradle.processing

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement

/**
 * Utility class for handling dynamic field mapping with table aliases.
 * Supports mapping columns from joined tables to dynamic field objects.
 */
class DynamicFieldMapper {

    /**
     * Information about columns that should be mapped to a dynamic field.
     */
    data class DynamicFieldMapping(
        val fieldName: String,
        val sourceTableAlias: String,
        val aliasPrefix: String?,
        val mappingType: AnnotationConstants.MappingType,
        val propertyType: String,
        val columns: List<SelectStatement.FieldSource>,
        val aliasPath: List<String>,
        val groupByColumn: String? = null  // For collection mapping
    )

    companion object {

        /**
         * Validates that when mappingType is used, all SELECT columns follow the alias.column format.
         * This is required for the dynamic field mapping to work correctly.
         */
        fun validateAliasColumnFormat(
            selectStatement: SelectStatement,
            dynamicFields: List<AnnotatedSelectStatement.Field>
        ) {
            // Only validate if there are dynamic fields with mappingType
            val fieldsWithMappingType = dynamicFields.filter {
                it.annotations.mappingType != null
            }

            if (fieldsWithMappingType.isEmpty()) {
                return // No validation needed
            }

            // Get all table aliases from SelectStatement
            val tableAliases = selectStatement.tableAliases
            // Collect any aliasPrefix hints from dynamic fields
            val allowedPrefixes = fieldsWithMappingType.mapNotNull { it.annotations.aliasPrefix }

            // Determine relevant aliases for dynamic mapping (source tables from dynamic fields)
            val relevantAliases =
                fieldsWithMappingType.mapNotNull { it.annotations.sourceTable }.toSet()

            // Check only the fields that are relevant for dynamic mapping
            selectStatement.fields.forEach { fieldSource ->
                val tableName = fieldSource.tableName

                // Only validate fields that belong to relevant aliases or match aliasPrefix patterns
                val shouldValidate =
                    relevantAliases.contains(tableName) || allowedPrefixes.any { prefix ->
                        prefix.isNotBlank() && fieldSource.fieldName.startsWith(prefix)
                    }
                if (!shouldValidate) return@forEach

                // Check if the table name corresponds to a known alias mapping
                val isValidTable =
                    tableAliases.containsValue(tableName) || tableAliases.containsKey(tableName)

                // VIEW/star-friendly fallback: accept fields that match declared aliasPrefix
                val matchesPrefix = allowedPrefixes.any { prefix ->
                    prefix.isNotBlank() && fieldSource.fieldName.startsWith(prefix)
                }

                if (!isValidTable && tableName.isNotEmpty() && !matchesPrefix) {
                    throw IllegalArgumentException(
                        "When using mappingType annotation, relevant SELECT columns should use alias.column or declared prefix. " +
                                "Found column '${fieldSource.fieldName}' from table '${tableName}' which doesn't correspond to a recognized table alias. " +
                                "Available aliases: ${tableAliases.keys.joinToString(", ")} -> ${
                                    tableAliases.values.joinToString(
                                        ", "
                                    )
                                }"
                    )
                }
            }
        }

        /**
         * Extracts the grouping column from JOIN relationships for collection mapping.
         * Returns the field name from SELECT that corresponds to the JOIN condition.
         */
        fun extractGroupingColumn(
            selectStatement: SelectStatement,
            sourceTableAlias: String
        ): String? {
            // Find JOIN condition where the right side matches our source table alias
            selectStatement.joinConditions.forEach { joinCondition ->
                if (joinCondition.rightTable == sourceTableAlias) {
                    // Find the field in SELECT that matches the left side of the JOIN
                    // e.g., for JOIN condition "p.id = a.person_id", find field with table "p" and column "id"
                    val leftTable = joinCondition.leftTable.lowercase()
                    val leftColumn = joinCondition.leftColumn.lowercase()
                    val matchingField = selectStatement.fields.find { field ->
                        field.tableName.lowercase() == leftTable &&
                                field.originalColumnName.lowercase() == leftColumn
                    }

                    // Return the field name (which might be aliased, e.g., "person_id" from "p.id AS person_id")
                    return matchingField?.fieldName
                }
            }
            return null
        }

        /**
         * Creates dynamic field mappings for fields with mappingType annotation.
         */
        fun createDynamicFieldMappings(
            selectStatement: SelectStatement,
            dynamicFields: List<AnnotatedSelectStatement.Field>
        ): List<DynamicFieldMapping> {
            val mappings = mutableListOf<DynamicFieldMapping>()
            val consumedFieldNames = mutableSetOf<String>()

            dynamicFields.filter { it.annotations.mappingType != null }.forEach { dynamicField ->
                val sourceTableAlias = dynamicField.annotations.sourceTable ?: return@forEach
                val aliasPrefix = dynamicField.annotations.aliasPrefix
                val mappingType = AnnotationConstants.MappingType.fromString(dynamicField.annotations.mappingType!!)
                val propertyType = dynamicField.annotations.propertyType!!

                val columns = resolveColumnsForDynamicField(
                    selectStatement = selectStatement,
                    sourceTableAlias = sourceTableAlias,
                    aliasPrefix = aliasPrefix,
                    consumedFieldNames = consumedFieldNames
                )

                if (columns.isNotEmpty()) {
                    consumedFieldNames.addAll(columns.map { it.fieldName })

                    val groupByColumn = when (mappingType) {
                        AnnotationConstants.MappingType.COLLECTION -> extractGroupingColumn(
                            selectStatement,
                            sourceTableAlias
                        )

                        AnnotationConstants.MappingType.PER_ROW,
                        AnnotationConstants.MappingType.ENTITY -> null
                    }

                    mappings.add(
                        DynamicFieldMapping(
                            fieldName = dynamicField.src.fieldName,
                            sourceTableAlias = sourceTableAlias,
                            aliasPrefix = aliasPrefix,
                            mappingType = mappingType,
                            propertyType = propertyType,
                            columns = columns,
                            aliasPath = dynamicField.aliasPath,
                            groupByColumn = groupByColumn
                        )
                    )
                }
            }

            return mappings
        }

        private fun resolveColumnsForDynamicField(
            selectStatement: SelectStatement,
            sourceTableAlias: String,
            aliasPrefix: String?,
            consumedFieldNames: MutableSet<String>
        ): List<SelectStatement.FieldSource> {
            val byAlias = selectStatement.fields.filter { fieldSource ->
                matchesSourceTable(fieldSource, selectStatement, sourceTableAlias) &&
                    !consumedFieldNames.contains(fieldSource.fieldName)
            }

            if (byAlias.isNotEmpty()) {
                return byAlias
            }

            if (!aliasPrefix.isNullOrBlank()) {
                val prefixedCandidates = selectStatement.fields.filter { fieldSource ->
                    fieldSource.fieldName.startsWith(aliasPrefix, ignoreCase = true) &&
                        !consumedFieldNames.contains(fieldSource.fieldName)
                }
                if (prefixedCandidates.isNotEmpty()) {
                    val withoutDisambiguator = prefixedCandidates.filter { fieldSource ->
                        val suffix = fieldSource.fieldName.substring(aliasPrefix.length)
                        !suffix.contains(':')
                    }
                    if (withoutDisambiguator.isNotEmpty()) {
                        return withoutDisambiguator
                    }
                    return prefixedCandidates
                }
            }

            return emptyList()
        }

        private fun matchesSourceTable(
            fieldSource: SelectStatement.FieldSource,
            selectStatement: SelectStatement,
            sourceTableAlias: String
        ): Boolean {
            val sourceAliasLower = sourceTableAlias.lowercase()
            val resolvedTable = selectStatement.tableAliases[sourceTableAlias] ?: sourceTableAlias
            val fieldTableLower = fieldSource.tableName.lowercase()
            if (fieldTableLower == resolvedTable.lowercase()) {
                return true
            }

            return selectStatement.tableAliases.any { (alias, table) ->
                alias.lowercase() == sourceAliasLower &&
                        fieldTableLower == table.lowercase()
            }
        }

        /**
         * Gets the set of column names that are mapped to dynamic fields.
         * These columns should be excluded from the data class.
         */
        fun getMappedColumns(
            fields: List<AnnotatedSelectStatement.Field>,
            tableAliases: Map<String, String>
        ): Set<String> {
            val mappedColumns = mutableSetOf<String>()

            fields.filter { it.annotations.isDynamicField && it.annotations.mappingType != null }
                .forEach { dynamicField ->
                    val sourceTableAlias = dynamicField.annotations.sourceTable ?: return@forEach
                    val aliasPrefix = dynamicField.annotations.aliasPrefix
                    val targetTableName = tableAliases[sourceTableAlias] ?: sourceTableAlias

                    fields.filter { !it.annotations.isDynamicField }
                        .filter { field -> field.src.tableName.lowercase() == targetTableName.lowercase() }
                        .forEach { field -> mappedColumns.add(field.src.fieldName) }

                    if (!aliasPrefix.isNullOrBlank()) {
                        val prefixLower = aliasPrefix.lowercase()
                        fields.filter { !it.annotations.isDynamicField }
                            .filter { field -> field.src.fieldName.lowercase().startsWith(prefixLower) }
                            .forEach { field -> mappedColumns.add(field.src.fieldName) }
                    }
                }

            return mappedColumns
        }
    }
}
