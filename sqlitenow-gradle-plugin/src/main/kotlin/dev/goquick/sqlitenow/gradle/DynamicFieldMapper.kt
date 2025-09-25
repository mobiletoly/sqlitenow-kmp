package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.schema.Table

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
        val removeAliasPrefix: String?,
        val mappingType: String,
        val propertyType: String,
        val columns: List<SelectStatement.FieldSource>,
        val groupByColumn: String? = null,  // For collection mapping
        val isCollection: Boolean = mappingType == AnnotationConstants.MAPPING_TYPE_COLLECTION
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
            // Collect any removeAliasPrefix hints from dynamic fields
            val allowedPrefixes = fieldsWithMappingType.mapNotNull { it.annotations.removeAliasPrefix }

            // Determine relevant aliases for dynamic mapping (source tables from dynamic fields)
            val relevantAliases = fieldsWithMappingType.mapNotNull { it.annotations.sourceTable }.toSet()

            // Check only the fields that are relevant for dynamic mapping
            selectStatement.fields.forEach { fieldSource ->
                val tableName = fieldSource.tableName

                // Only validate fields that belong to relevant aliases or match removeAliasPrefix patterns
                val shouldValidate = relevantAliases.contains(tableName) || allowedPrefixes.any { prefix ->
                    prefix.isNotBlank() && fieldSource.fieldName.startsWith(prefix)
                }
                if (!shouldValidate) return@forEach

                // Check if the table name corresponds to a known alias mapping
                val isValidTable = tableAliases.containsValue(tableName) || tableAliases.containsKey(tableName)

                // VIEW/star-friendly fallback: accept fields that match declared removeAliasPrefix
                val matchesPrefix = allowedPrefixes.any { prefix ->
                    prefix.isNotBlank() && fieldSource.fieldName.startsWith(prefix)
                }

                if (!isValidTable && tableName.isNotEmpty() && !matchesPrefix) {
                    throw IllegalArgumentException(
                        "When using mappingType annotation, relevant SELECT columns should use alias.column or declared prefix. " +
                                "Found column '${fieldSource.fieldName}' from table '${tableName}' which doesn't correspond to a recognized table alias. " +
                                "Available aliases: ${tableAliases.keys.joinToString(", ")} -> ${tableAliases.values.joinToString(", ")}"
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
                    val matchingField = selectStatement.fields.find { field ->
                        field.tableName.equals(joinCondition.leftTable, ignoreCase = true) &&
                        field.originalColumnName.equals(joinCondition.leftColumn, ignoreCase = true)
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

            // Find all dynamic fields with mappingType
            val fieldsWithMappingType = dynamicFields.filter {
                it.annotations.isDynamicField && it.annotations.mappingType != null
            }

            fieldsWithMappingType.forEach { dynamicField ->
                val sourceTableAlias = dynamicField.annotations.sourceTable
                val removeAliasPrefix = dynamicField.annotations.removeAliasPrefix
                val mappingType = dynamicField.annotations.mappingType!!
                val propertyType = dynamicField.annotations.propertyType!!

                if (sourceTableAlias != null) {
                    // Try 1: by underlying table name
                    val targetTableName = selectStatement.tableAliases[sourceTableAlias]
                    var columns: List<SelectStatement.FieldSource> = emptyList()
                    if (targetTableName != null) {
                        columns = selectStatement.fields.filter { f ->
                            f.tableName.equals(targetTableName, ignoreCase = true)
                        }
                    } else {
                        // Try 2: assume alias equals actual table name
                        columns = selectStatement.fields.filter { f ->
                            f.tableName.equals(sourceTableAlias, ignoreCase = true)
                        }
                    }
                    // Try 3: VIEW/star-friendly fallback using removeAliasPrefix
                    if (columns.isEmpty() && !removeAliasPrefix.isNullOrBlank()) {
                        columns = selectStatement.fields.filter { f ->
                            f.fieldName.startsWith(removeAliasPrefix)
                        }
                    }

                    if (columns.isNotEmpty()) {
                        val groupByColumn = if (mappingType == AnnotationConstants.MAPPING_TYPE_COLLECTION) {
                            extractGroupingColumn(selectStatement, sourceTableAlias)
                        } else null

                        mappings.add(
                            DynamicFieldMapping(
                                fieldName = dynamicField.src.fieldName,
                                sourceTableAlias = sourceTableAlias,
                                removeAliasPrefix = removeAliasPrefix,
                                mappingType = mappingType,
                                propertyType = propertyType,
                                columns = columns,
                                groupByColumn = groupByColumn
                            )
                        )
                    }
                }
            }

            return mappings
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

            // Find all dynamic fields with mappingType
            val dynamicFieldsWithMapping = fields.filter {
                it.annotations.isDynamicField && it.annotations.mappingType != null
            }

            dynamicFieldsWithMapping.forEach { dynamicField ->
                val sourceTableAlias = dynamicField.annotations.sourceTable
                val removeAliasPrefix = dynamicField.annotations.removeAliasPrefix
                if (sourceTableAlias != null) {
                    var matched = false
                    // Method 1: alias -> underlying table name mapping
                    val targetTableName = tableAliases[sourceTableAlias]
                    if (targetTableName != null) {
                        fields.forEach { field ->
                            if (!field.annotations.isDynamicField &&
                                field.src.tableName.equals(targetTableName, ignoreCase = true)) {
                                mappedColumns.add(field.src.fieldName)
                                matched = true
                            }
                        }
                    } else {
                        // Method 2: assume sourceTableAlias is the actual table name
                        fields.forEach { field ->
                            if (!field.annotations.isDynamicField &&
                                field.src.tableName.equals(sourceTableAlias, ignoreCase = true)) {
                                mappedColumns.add(field.src.fieldName)
                                matched = true
                            }
                        }
                    }
                    // Method 3: VIEW/star-friendly fallback using removeAliasPrefix
                    if (!matched && !removeAliasPrefix.isNullOrBlank()) {
                        fields.forEach { field ->
                            if (!field.annotations.isDynamicField &&
                                field.src.fieldName.startsWith(removeAliasPrefix)) {
                                mappedColumns.add(field.src.fieldName)
                            }
                        }
                    }
                }
            }

            return mappedColumns
        }
    }
}
