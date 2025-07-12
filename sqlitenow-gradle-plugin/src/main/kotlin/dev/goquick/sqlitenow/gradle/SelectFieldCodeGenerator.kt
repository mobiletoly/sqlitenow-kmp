package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName

/**
 * Generates KotlinPoet property specifications for data class properties based on
 * AnnotatedSelectStatement.Field parameters.
 *
 * This generator is specifically tailored for SQLite dialect.
 *
 * @param createTableStatements List of AnnotatedCreateTableStatement objects to assist with type and nullability inference
 */
class SelectFieldCodeGenerator(
    createTableStatements: List<AnnotatedCreateTableStatement> = emptyList(),
    private val packageName: String? = null
) {
    // Convert the list to a map with table name as the key for more efficient lookups
    private val tableMap: Map<String, AnnotatedCreateTableStatement> = createTableStatements.associateBy {
        it.src.tableName.lowercase()
    }

    /**
     * Generates field information for a property or parameter based on the given field.
     *
     * @param field The AnnotatedSelectStatement.Field to generate information for
     * @param propertyNameGeneratorType The type of property name generator to use
     * @return A Pair of property name and property type
     */
    private fun generateFieldInfo(
        field: AnnotatedSelectStatement.Field,
        propertyNameGeneratorType: PropertyNameGeneratorType
    ): Pair<String, TypeName> {
        // For dynamic fields, use the field name directly and get type from propertyType annotation
        if (field.annotations.isDynamicField) {
            val propertyName = generatePropertyName(field.src.fieldName, field.annotations, propertyNameGeneratorType)

            // Determine nullability for dynamic fields
            val isNullable = when (field.annotations.notNull) {
                true -> false   // Explicitly marked as notNull
                false -> true   // Explicitly marked as nullable
                null -> {
                    // Default behavior: mapped dynamic fields (with mappingType) are nullable by default
                    // since they typically come from JOINs which can be null
                    field.annotations.mappingType != null
                }
            }

            // For dynamic fields, we must have a propertyType annotation
            val propertyType = field.annotations.propertyType?.let {
                // Use determinePropertyType with a dummy base type since we only care about the annotation
                SqliteTypeToKotlinCodeConverter.determinePropertyType(
                    baseType = ClassName("kotlin", "String"),
                    propertyType = it,
                    isNullable = isNullable,
                    packageName = packageName
                )
            } ?: throw IllegalStateException("Dynamic field must have propertyType annotation")

            return Pair(propertyName, propertyType)
        }

        // Regular field processing
        val fieldName = field.src.fieldName
        val propertyName = generatePropertyName(fieldName, field.annotations, propertyNameGeneratorType)
        val baseType = mapSqlTypeToKotlinType(field.src.dataType, field)
        val isNullable = determineNullability(field)

        // Determine the final property type based on annotations and nullability
        // The SELECT statement annotations have higher priority than CREATE TABLE annotations
        val propertyType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
            baseType,
            field.annotations.propertyType,
            isNullable,
            packageName
        )

        return Pair(propertyName, propertyType)
    }

    /**
     * Generates a PropertySpec for a data class property based on the given field.
     *
     * @param field The AnnotatedSelectStatement.Field to generate a property for
     * @param propertyNameGeneratorType The type of property name generator to use
     * @return A PropertySpec representing the property
     */
    fun generateProperty(
        field: AnnotatedSelectStatement.Field,
        propertyNameGeneratorType: PropertyNameGeneratorType = PropertyNameGeneratorType.LOWER_CAMEL_CASE
    ): PropertySpec {
        val (propertyName, propertyType) = generateFieldInfo(field, propertyNameGeneratorType)

        // Build and return the property spec
        return PropertySpec.builder(propertyName, propertyType)
            .initializer(propertyName)
            .build()
    }

    /**
     * Generates a ParameterSpec for a constructor parameter based on the given field.
     *
     * @param field The AnnotatedSelectStatement.Field to generate a parameter for
     * @param propertyNameGeneratorType The type of property name generator to use
     * @return A ParameterSpec representing the constructor parameter
     */
    fun generateParameter(
        field: AnnotatedSelectStatement.Field,
        propertyNameGeneratorType: PropertyNameGeneratorType = PropertyNameGeneratorType.LOWER_CAMEL_CASE
    ): ParameterSpec {
        val (propertyName, propertyType) = generateFieldInfo(field, propertyNameGeneratorType)

        // Build the parameter spec
        val parameterBuilder = ParameterSpec.builder(propertyName, propertyType)

        // Add default value for dynamic fields if specified
        if (field.annotations.isDynamicField && field.annotations.defaultValue != null) {
            parameterBuilder.defaultValue(field.annotations.defaultValue)
        }

        return parameterBuilder.build()
    }

    /**
     * Finds a column in the schema based on field information.
     * This method encapsulates the common pattern of looking up columns by table name first,
     * then searching all tables if not found.
     *
     * @param field The field to find the column for
     * @return The column if found, null otherwise
     */
    private fun findColumnForField(field: AnnotatedSelectStatement.Field): AnnotatedCreateTableStatement.Column? {
        val tableName = field.src.tableName
        val fieldName = field.src.fieldName
        val originalColumnName = field.src.originalColumnName

        // First try to find the column in the specified table
        if (tableName.isNotBlank()) {
            val table = tableMap[tableName.lowercase()]
            if (table != null) {
                val column = findColumnInTable(table, originalColumnName, fieldName)
                if (column != null) {
                    return column
                }
            }
        }

        // If we didn't find a match or no specific table is provided, check all tables
        // This is important for fields that come from joined tables
        for (table in tableMap.values) {
            val column = findColumnInTable(table, originalColumnName, fieldName)
            if (column != null) {
                return column
            }
        }

        return null
    }

    /**
     * Maps a SQLite type to a Kotlin type.
     *
     * @param sqliteType The SQLite type to map
     * @param field Optional field information for more accurate type mapping
     * @return The corresponding Kotlin type
     */
    private fun mapSqlTypeToKotlinType(sqliteType: String, field: AnnotatedSelectStatement.Field? = null): TypeName {
        // Extract the base type without size or precision information
        val baseType = sqliteType.split("(")[0].uppercase()

        // First check if we have a type from the SQL type mapping
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(baseType)

        // If we have field information, try to get a more accurate type from the table schema
        if (field != null) {
            val column = findColumnForField(field)
            if (column != null) {
                val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
                if (propertyType != null) {
                    return SqliteTypeToKotlinCodeConverter.determinePropertyType(kotlinType, propertyType, false, packageName)
                }

                // If no annotation, use the column's data type
                val columnType = column.src.dataType.uppercase()
                return SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(columnType)
            }
        }

        return kotlinType
    }

    /**
     * Generates a property name based on the field name, annotations, and property name generator type.
     *
     * @param fieldName The field name
     * @param annotations The field annotations
     * @param propertyNameGeneratorType The property name generator type
     * @return The generated property name
     */
    private fun generatePropertyName(
        fieldName: String,
        annotations: FieldAnnotationOverrides,
        propertyNameGeneratorType: PropertyNameGeneratorType
    ): String {
        // Use the property name from the annotation if present
        annotations.propertyName?.let { return it }

        // If no explicit property name is provided, apply the property name generator
        return propertyNameGeneratorType.convertToPropertyName(fieldName)
    }

    /**
     * Determines if a property should be nullable based on field annotations and table schema.
     *
     * @param field The field to check
     * @return True if the property should be nullable, false otherwise
     */
    fun determineNullability(field: AnnotatedSelectStatement.Field): Boolean {
        // Check for explicit notNull annotation first (highest priority)
        when (field.annotations.notNull) {
            true -> return false   // notNull=true means not nullable
            false -> return true   // notNull=false means nullable
            null -> {
                // notNull not specified - check table schema
                val column = findColumnForField(field)
                if (column != null) {
                    return column.isNullable()
                }

                // Default to nullable if no explicit annotation or schema constraint is found
                return true
            }
        }
    }

    /**
     * Finds a column in a table by its original column name or field name.
     *
     * @param table The table to search in
     * @param originalColumnName The original column name
     * @param fieldName The field name (used as fallback)
     * @return The column if found, null otherwise
     */
    private fun findColumnInTable(
        table: AnnotatedCreateTableStatement,
        originalColumnName: String,
        fieldName: String
    ): AnnotatedCreateTableStatement.Column? {
        // Try to find the column by original column name first (for aliased columns)
        var column = table.findColumnByName(originalColumnName)

        // If not found by original column name, try the field name
        if (column == null) {
            column = table.findColumnByName(fieldName)
        }

        return column
    }
}
