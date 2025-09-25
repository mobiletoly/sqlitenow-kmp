package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.TypeName

/**
 * Service responsible for collecting and configuring adapter parameters for SQL statements.
 */
class AdapterConfig(
    private val columnLookup: ColumnLookup,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    private val createViewStatements: List<AnnotatedCreateViewStatement> = emptyList(),
    private val packageName: String? = null
) {
    // Backward-compatible constructor used by some tests
    constructor(
        columnLookup: ColumnLookup,
        createTableStatements: List<AnnotatedCreateTableStatement>,
        packageName: String?
    ) : this(columnLookup, createTableStatements, emptyList(), packageName)
    /** Resolve original base column name for a field, considering VIEW mappings. */
    fun baseOriginalNameForField(field: AnnotatedSelectStatement.Field): String {
        val fs = field.src
        // Try view mapping by tableName first
        if (fs.tableName.isNotEmpty()) {
            columnLookup.findViewByName(fs.tableName)?.let { view ->
                view.fields.find { it.src.fieldName.equals(fs.fieldName, ignoreCase = true) }?.let { vf ->
                    return vf.src.originalColumnName
                }
            }
        }
        // Fallback to direct original name or alias with heuristic prefix strip
        if (fs.originalColumnName.isNotEmpty()) {
            val mo = Regex("^joined_[^_]+_(.*)$").matchEntire(fs.originalColumnName)
            if (mo != null && mo.groupValues.size > 1) return mo.groupValues[1]
            return fs.originalColumnName
        }
        // Heuristic on alias
        val m = Regex("^joined_[^_]+_(.*)$").matchEntire(fs.fieldName)
        if (m != null && m.groupValues.size > 1) return m.groupValues[1]
        return fs.fieldName
    }
    data class ParamConfig(
        val paramName: String,
        val adapterFunctionName: String,
        val inputType: TypeName,
        val outputType: TypeName,
        val isNullable: Boolean
    )

    /** Collects all adapter configurations needed for a statement. */
    fun collectAllParamConfigs(statement: AnnotatedStatement): List<ParamConfig> {
        val configs = mutableListOf<ParamConfig>()
        val processedAdapters = mutableSetOf<String>()

        when (statement) {
            is AnnotatedSelectStatement -> {
                configs.addAll(collectInputParamConfigs(statement, processedAdapters))
                configs.addAll(collectOutputParamConfigs(statement, processedAdapters))
            }

            is AnnotatedExecuteStatement -> {
                configs.addAll(collectInputParamConfigs(statement, processedAdapters))
            }

            is AnnotatedCreateTableStatement -> {
                // CREATE TABLE statements don't need adapter parameters
            }

            is AnnotatedCreateViewStatement -> {
                // CREATE VIEW statements don't need adapter parameters
            }
        }
        return configs
    }

    private fun collectInputParamConfigs(
        statement: AnnotatedStatement,
        processedAdapters: MutableSet<String>
    ): List<ParamConfig> {
        val configs = mutableListOf<ParamConfig>()

        // Group parameters by the column they reference
        val uniqueParameters = StatementUtils.getNamedParameters(statement).distinct()
        val columnToParameters = mutableMapOf<String, MutableList<String>>()

        uniqueParameters.forEach { paramName ->
            val column = columnLookup.findColumnForParameter(statement, paramName)
            if (column != null && column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                val columnKey = "${column.src.name}_${column.src.dataType}" // Unique key for column
                columnToParameters.getOrPut(columnKey) { mutableListOf() }.add(paramName)
            }
        }

        // Generate one adapter per unique column (not per parameter)
        columnToParameters.forEach { (_, paramNames) ->
            val firstParamName = paramNames.first()
            val column = columnLookup.findColumnForParameter(statement, firstParamName)!!

            // Use actual column name for adapter function name (ignore property name customizations)
            val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(column.src.name)
            val adapterFunctionName = getInputAdapterFunctionName(columnName)

            // Skip if already processed
            if (adapterFunctionName in processedAdapters) {
                return@forEach
            }
            processedAdapters.add(adapterFunctionName)

            // Create adapter configuration for input (parameter binding)
            // Use the first parameter name for type inference, but name after column
            val config = createInputParamConfig(
                parameterName = firstParamName,
                adapterFunctionName = adapterFunctionName,
                column = column
            )
            configs.add(config)
        }

        return configs
    }

    private fun collectOutputParamConfigs(
        statement: AnnotatedSelectStatement,
        processedAdapters: MutableSet<String>
    ): List<ParamConfig> {
        val configs = mutableListOf<ParamConfig>()
        val selectFieldGenerator = SelectFieldCodeGenerator(createTableStatements, createViewStatements, packageName)

        statement.fields.forEach { field: AnnotatedSelectStatement.Field ->
            // Skip dynamic fields - they don't need adapters since they're not read from database
            if (field.annotations.isDynamicField) {
                return@forEach
            }

            if (hasAdapterAnnotation(field)) {
                // Use base original column name (resolve via VIEW if needed)
                val baseName = baseOriginalNameForField(field)
                val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(baseName)
                val adapterFunctionName = getOutputAdapterFunctionName(columnName)

                // Skip if already processed
                if (adapterFunctionName in processedAdapters) {
                    return@forEach
                }
                processedAdapters.add(adapterFunctionName)

                // Create adapter configuration for output (result conversion)
                val config = createOutputParamConfig(
                    field = field,
                    adapterFunctionName = adapterFunctionName,
                    propertyNameGenerator = statement.annotations.propertyNameGenerator,
                    selectFieldGenerator = selectFieldGenerator
                )
                configs.add(config)
            }
        }

        return configs
    }

    /** Creates an input adapter configuration for parameter binding. */
    private fun createInputParamConfig(
        parameterName: String,
        adapterFunctionName: String,
        column: AnnotatedCreateTableStatement.Column
    ): ParamConfig {
        val baseType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
        val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
        val isNullable = column.isNullable()

        val targetType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable, packageName)

        val inputType = targetType.copy(nullable = isNullable)
        val outputType = baseType.copy(nullable = isNullable)

        return ParamConfig(
            paramName = parameterName,
            adapterFunctionName = adapterFunctionName,
            inputType = inputType,
            outputType = outputType,
            isNullable = isNullable
        )
    }

    /** Creates an output adapter configuration for result field conversion. */
    private fun createOutputParamConfig(
        field: AnnotatedSelectStatement.Field,
        adapterFunctionName: String,
        propertyNameGenerator: PropertyNameGeneratorType,
        selectFieldGenerator: SelectFieldCodeGenerator
    ): ParamConfig {
        // Get the property type from the field
        val property = selectFieldGenerator.generateProperty(field, propertyNameGenerator)
        val targetType = property.type

        val inputNullable = property.type.isNullable
        val underlyingType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(field.src.dataType)
        val inputType = underlyingType.copy(nullable = inputNullable)

        return ParamConfig(
            paramName = field.src.fieldName,
            adapterFunctionName = adapterFunctionName,
            inputType = inputType,
            outputType = targetType,
            isNullable = inputNullable
        )
    }

    /**
     * Helper function to check if a field has an adapter annotation.
     * Checks both SELECT field annotations and CREATE TABLE column annotations.
     */
    fun hasAdapterAnnotation(field: AnnotatedSelectStatement.Field): Boolean {
        // First check if the field has a direct adapter annotation in the SELECT statement
        if (field.annotations.adapter == true) {
            return true
        }

        // If not, check if the underlying column in CREATE TABLE has an adapter annotation
        val fieldSource = field.src
        if (fieldSource.tableName.isNotEmpty() && fieldSource.originalColumnName.isNotEmpty()) {
            // Try as a direct table first
            createTableStatements.find { it.src.tableName == fieldSource.tableName }?.let { table ->
                table.findColumnByName(fieldSource.originalColumnName)?.let { col ->
                    return col.annotations.containsKey(AnnotationConstants.ADAPTER)
                }
            }
            // If not a table, try resolving through a VIEW: tableName may be a view name
            val view = columnLookup.findViewByName(fieldSource.tableName)
            if (view != null) {
                val viewField = view.fields.find { it.src.fieldName.equals(fieldSource.fieldName, ignoreCase = true) }
                if (viewField != null) {
                    createTableStatements.find { it.src.tableName.equals(viewField.src.tableName, ignoreCase = true) }?.let { table ->
                        table.findColumnByName(viewField.src.originalColumnName)?.let { col ->
                            return col.annotations.containsKey(AnnotationConstants.ADAPTER)
                        }
                    }
                }
            }
        }

        // Fallback: try to resolve by base original column name across all tables (best-effort)
        val baseName = baseOriginalNameForField(field)
        createTableStatements.forEach { table ->
            table.findColumnByName(baseName)?.let { col ->
                if (col.annotations.containsKey(AnnotationConstants.ADAPTER)) return true
            }
        }
        return false
    }

    /** Helper function to get the property name for a field. */
    fun getPropertyName(
        field: AnnotatedSelectStatement.Field,
        propertyNameGenerator: PropertyNameGeneratorType,
        selectFieldGenerator: SelectFieldCodeGenerator
    ): String {
        val property = selectFieldGenerator.generateProperty(field, propertyNameGenerator)
        return property.name
    }

    /**
     * Generates an adapter function name for input parameters (parameter -> SQL column).
     * Example: "birthDate" -> "birthDateToSqlValue"
     */
    private fun getInputAdapterFunctionName(propertyName: String): String {
        return "${propertyName}ToSqlValue"
    }

    /**
     * Generates an adapter function name for output fields (SQL column -> property).
     * Example: "birthDate" -> "sqlValueToBirthDate"
     */
    fun getOutputAdapterFunctionName(propertyName: String): String {
        return "sqlValueTo${propertyName.capitalized()}"
    }
}
