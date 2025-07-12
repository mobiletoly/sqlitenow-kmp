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

            // Check each field to ensure it has a valid table name that corresponds to an alias
            selectStatement.fields.forEach { fieldSource ->
                val tableName = fieldSource.tableName

                // Check if the table name corresponds to a known table (either alias target or alias itself)
                val isValidTable = tableAliases.containsValue(tableName) || tableAliases.containsKey(tableName)

                if (!isValidTable && tableName.isNotEmpty()) {
                    throw IllegalArgumentException(
                        "When using mappingType annotation, all SELECT columns must use alias.column format. " +
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
                    // Get the table name for this alias
                    val targetTableName = selectStatement.tableAliases[sourceTableAlias]

                    if (targetTableName != null) {
                        // Find all columns that belong to the target table
                        val columns = selectStatement.fields.filter { fieldSource ->
                            fieldSource.tableName.equals(targetTableName, ignoreCase = true)
                        }

                        if (columns.isNotEmpty()) {
                            val groupByColumn = if (mappingType == AnnotationConstants.MAPPING_TYPE_COLLECTION) {
                                extractGroupingColumn(selectStatement, sourceTableAlias)
                            } else {
                                null
                            }

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
                if (sourceTableAlias != null) {
                    // Method 1: Try to get table name from alias mapping
                    val targetTableName = tableAliases[sourceTableAlias]

                    if (targetTableName != null) {
                        // Find all columns that belong to the target table
                        fields.forEach { field ->
                            if (!field.annotations.isDynamicField &&
                                field.src.tableName.equals(targetTableName, ignoreCase = true)) {
                                mappedColumns.add(field.src.fieldName)
                            }
                        }
                    } else {
                        // Method 2: Fallback - assume sourceTableAlias is the actual table name
                        fields.forEach { field ->
                            if (!field.annotations.isDynamicField &&
                                field.src.tableName.equals(sourceTableAlias, ignoreCase = true)) {
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
