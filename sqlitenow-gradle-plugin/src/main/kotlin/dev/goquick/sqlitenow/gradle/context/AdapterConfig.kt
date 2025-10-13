/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.context

import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementUtils
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.util.capitalized

/**
 * Service responsible for collecting and configuring adapter parameters for SQL statements.
 */
class AdapterConfig(
    private val columnLookup: ColumnLookup,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    private val createViewStatements: List<AnnotatedCreateViewStatement> = emptyList(),
    private val packageName: String? = null
) {
    private val tableLookup: Map<String, AnnotatedCreateTableStatement> =
        createTableStatements.associateBy { it.src.tableName.lowercase() }
    private val viewLookup: Map<String, AnnotatedCreateViewStatement> =
        createViewStatements.associateBy { it.src.viewName.lowercase() }
    private val viewFieldLookup: Map<AnnotatedCreateViewStatement, CaseInsensitiveMap<AnnotatedCreateViewStatement.Field>> =
        createViewStatements.associateWith { view ->
            CaseInsensitiveMap(view.fields.map { it.src.fieldName to it })
        }
    // Backward-compatible constructor used by some tests
    constructor(
        columnLookup: ColumnLookup,
        createTableStatements: List<AnnotatedCreateTableStatement>,
        packageName: String?
    ) : this(columnLookup, createTableStatements, emptyList(), packageName)
    /** Resolve original base column name for a field, considering VIEW mappings and dynamic alias prefixes. */
    fun baseOriginalNameForField(
        field: AnnotatedSelectStatement.Field,
        aliasPrefixes: List<String> = emptyList(),
        statement: AnnotatedSelectStatement? = null
    ): String {
        val fs = field.src
        if (statement != null) {
            columnLookup.findColumnForSelectField(statement, field, aliasPrefixes)?.let { column ->
                return column.src.name
            }
        }
        // 1) Try view mapping by tableName first
        if (fs.tableName.isNotEmpty()) {
            columnLookup.findViewByName(fs.tableName)?.let { view ->
                val fields = viewFieldLookup[view]
                fields?.get(fs.fieldName)?.let { vf ->
                    return vf.src.originalColumnName
                }
            }
        }
        // 2) Use provided aliasPrefixes (from dynamic field mappings) to strip prefixes deterministically
        if (fs.originalColumnName.isNotEmpty()) {
            // Prefer original column name, but if the alias has a declared prefix, strip from alias to derive base
            aliasPrefixes.firstOrNull { prefix -> field.src.fieldName.startsWith(prefix) }?.let { prefix ->
                return field.src.fieldName.removePrefix(prefix)
            }
            return fs.originalColumnName
        }
        // No originalColumnName; try stripping from fieldName
        aliasPrefixes.firstOrNull { prefix -> fs.fieldName.startsWith(prefix) }?.let { prefix ->
            return fs.fieldName.removePrefix(prefix)
        }
        // 3) No dynamic prefix to strip; return as-is
        return fs.fieldName
    }
    enum class AdapterKind {
        INPUT,
        RESULT_FIELD,
        MAP_RESULT,
    }

    data class ParamConfig(
        val paramName: String,
        val adapterFunctionName: String,
        val inputType: TypeName,
        val outputType: TypeName,
        val isNullable: Boolean,
        // Optional hint for which namespace should provide this adapter (e.g., table behind aliasPrefix)
        val providerNamespace: String? = null,
        val kind: AdapterKind,
    )

    /** Collects all adapter configurations needed for a statement. */
    fun collectAllParamConfigs(statement: AnnotatedStatement, namespace: String? = null): List<ParamConfig> {
        val configs = mutableListOf<ParamConfig>()
        val processedAdapters = mutableSetOf<String>()
        var mapToNames: MutableMap<String, String>? = null

        when (statement) {
            is AnnotatedSelectStatement -> {
                if (namespace != null) {
                    mapToNames = mutableMapOf()
                }
                configs.addAll(collectInputParamConfigs(statement, processedAdapters))
                configs.addAll(collectOutputParamConfigs(statement, processedAdapters))
                if (namespace != null) {
                    createMapToParamConfig(statement, namespace, mapToNames!!)?.let { configs.add(it) }
                }
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


    /**
     * Collect alias prefixes applicable for a given SELECT statement.
     * - Includes dynamic mapping prefixes on the statement
     * - Includes prefixes defined on referenced views
     * - Includes all prefixes from any view as a conservative fallback (handles nested/expanded views)
     */
    fun collectAliasPrefixesForSelect(
        statement: AnnotatedSelectStatement,
    ): List<String> {
        return buildSet {
            // Prefixes declared directly on dynamic fields of this statement
            statement.fields.forEach { f ->
                if (f.annotations.isDynamicField) {
                    f.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { add(it) }
                }
            }
            // Prefixes declared on referenced views
            statement.src.tableAliases.values.forEach { tableOrViewName ->
                viewLookup[tableOrViewName.lowercase()]?.let { view ->
                    view.dynamicFields.forEach { df ->
                        df.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { add(it) }
                    }
                }
            }
            // Conservative fallback: prefixes from all views
            createViewStatements.forEach { view ->
                view.dynamicFields.forEach { df ->
                    df.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { add(it) }
                }
            }
        }.toList()
    }

    private fun collectOutputParamConfigs(
        statement: AnnotatedSelectStatement,
        processedAdapters: MutableSet<String>
    ): List<ParamConfig> {
        val configs = mutableListOf<ParamConfig>()
        val selectFieldGenerator =
            SelectFieldCodeGenerator(createTableStatements, createViewStatements, packageName)

        // Collect aliasPrefixes from dynamic field mappings for this statement
        val dynamicFields = statement.fields.filter { it.annotations.isDynamicField }
        val dynamicMappings = DynamicFieldMapper.Companion.createDynamicFieldMappings(statement.src, dynamicFields)
        val aliasPrefixes = buildSet {
            addAll(dynamicMappings.mapNotNull { it.aliasPrefix }.filter { it.isNotBlank() })
            // Also collect aliasPrefixes declared on referenced views (for SELECT * FROM view)
            statement.src.tableAliases.values.forEach { tableOrViewName ->
                viewLookup[tableOrViewName.lowercase()]?.let { view ->
                    view.dynamicFields.forEach { df ->
                        df.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { add(it) }
                    }
                }
            }
            // As a conservative fallback, include all aliasPrefixes declared on any view (to support nested views)
            createViewStatements.forEach { view ->
                view.dynamicFields.forEach { df ->
                    df.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { add(it) }
                }
            }
        }.toList()

        statement.fields.forEach { field: AnnotatedSelectStatement.Field ->
            // Skip dynamic fields - they don't need adapters since they're not read from database
            if (field.annotations.isDynamicField) {
                return@forEach
            }

            if (hasAdapterAnnotation(field, aliasPrefixes)) {
                // Use base column name to generate adapter function name (strips alias prefixes)
                val baseColumnName = baseOriginalNameForField(field, aliasPrefixes, statement)
                val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(baseColumnName)
                val adapterFunctionName = getOutputAdapterFunctionName(columnName)

                // Skip if already processed
                if (adapterFunctionName in processedAdapters) {
                    return@forEach
                }
                processedAdapters.add(adapterFunctionName)

                // Create adapter configuration for output (result conversion)
                val config = createOutputParamConfig(
                    statement = statement,
                    field = field,
                    adapterFunctionName = adapterFunctionName,
                    propertyNameGenerator = statement.annotations.propertyNameGenerator,
                    selectFieldGenerator = selectFieldGenerator,
                    baseColumnName = baseColumnName,
                    aliasPrefixes = aliasPrefixes
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
        val baseType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(column.src.dataType)
        val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
        val propertyNullable = column.isNullable()
        val sqlNullable = column.isSqlNullable()

        val targetType = SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
            baseType,
            propertyType,
            propertyNullable,
            packageName
        )

        val inputType = targetType.copy(nullable = propertyNullable)
        val outputType = baseType.copy(nullable = sqlNullable)

        return ParamConfig(
            paramName = parameterName,
            adapterFunctionName = adapterFunctionName,
            inputType = inputType,
            outputType = outputType,
            isNullable = propertyNullable,
            providerNamespace = null, // inputs don't need cross-namespace routing for now
            kind = AdapterKind.INPUT,
        )
    }

    /** Creates an output adapter configuration for result field conversion. */
    private fun createOutputParamConfig(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        adapterFunctionName: String,
        propertyNameGenerator: PropertyNameGeneratorType,
        selectFieldGenerator: SelectFieldCodeGenerator,
        baseColumnName: String,
        aliasPrefixes: List<String>
    ): ParamConfig {
        // Determine preferred provider namespace by resolving the field's source table alias -> table name
        val providerNs: String? = run {
            val alias = field.src.tableName
            if (alias.isNotBlank()) {
                statement.src.tableAliases[alias] ?: alias
            } else {
                // Try resolving via VIEW if table name is actually a view
                val from = statement.src.fromTable
                val view = from?.let { columnLookup.findViewByName(it) }
                if (view != null) {
                    val vf = view.fields.find { it.src.fieldName.equals(field.src.fieldName, ignoreCase = true) }
                    vf?.let { vfield ->
                        val vAlias = vfield.src.tableName
                        view.src.selectStatement.tableAliases[vAlias] ?: vAlias
                    }
                } else null
            }
        }

        // Get the property type from the field
        val property = selectFieldGenerator.generateProperty(field, propertyNameGenerator)
        val targetType = property.type

        val underlyingType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(field.src.dataType)
        val propertyNullable = targetType.isNullable
        val sqlNullable = columnLookup.findColumnForSelectField(
            statement = statement,
            field = field,
            aliasPrefixes = aliasPrefixes,
        )?.isSqlNullable() ?: field.src.isNullable
        val inputType = underlyingType.copy(nullable = sqlNullable)

        return ParamConfig(
            paramName = field.src.fieldName,
            adapterFunctionName = adapterFunctionName,
            inputType = inputType,
            outputType = targetType,
            isNullable = propertyNullable,
            providerNamespace = providerNs,
            kind = AdapterKind.RESULT_FIELD,
        )
    }

    private fun createMapToParamConfig(
        statement: AnnotatedSelectStatement,
        namespace: String,
        seenMapToNames: MutableMap<String, String>,
    ): ParamConfig? {
        val targetTypeNameRaw = statement.annotations.mapTo ?: return null
        val baseType = SharedResultTypeUtils.createResultTypeName(
            packageName = packageName ?: "",
            namespace = namespace,
            statement = statement,
        )
        val targetType = SqliteTypeToKotlinCodeConverter.parseCustomType(targetTypeNameRaw, packageName)
        val adapterFunctionName = buildMapToAdapterFunctionName(statement, namespace)
        val existing = seenMapToNames[adapterFunctionName]
        if (existing != null && existing != statement.name) {
            throw IllegalStateException(
                "MapTo adapter name '$adapterFunctionName' already defined in namespace '$namespace' by statement '$existing'. " +
                    "Only one mapTo per queryResult name is allowed."
            )
        }
        seenMapToNames[adapterFunctionName] = statement.name
        return ParamConfig(
            paramName = adapterFunctionName,
            adapterFunctionName = adapterFunctionName,
            inputType = baseType,
            outputType = targetType,
            isNullable = targetType.isNullable,
            providerNamespace = namespace,
            kind = AdapterKind.MAP_RESULT,
        )
    }

    private fun buildMapToAdapterFunctionName(
        statement: AnnotatedSelectStatement,
        namespace: String,
    ): String {
        val rawName = statement.annotations.queryResult?.takeIf { it.isNotBlank() }
            ?: run {
                val packageHint = packageName ?: ""
                val className = SharedResultTypeUtils.createResultTypeName(packageHint, namespace, statement)
                className.simpleNames.lastOrNull() ?: statement.getDataClassName()
            }
        val sanitized = rawName.replace(Regex("[^A-Za-z0-9]"), "")
        val base = sanitized.replaceFirstChar { if (it.isLowerCase()) it else it.lowercaseChar() }
        return "${base.ifEmpty { "result" }}Mapper"
    }

    /**
     * Helper function to check if a field has an adapter annotation.
     * Checks both SELECT field annotations and CREATE TABLE column annotations.
     */
    fun hasAdapterAnnotation(field: AnnotatedSelectStatement.Field, aliasPrefixes: List<String> = emptyList()): Boolean {
        // First check if the field has a direct adapter annotation in the SELECT statement
        if (field.annotations.adapter == true) {
            return true
        }

        // If not, check if the underlying column in CREATE TABLE has an adapter annotation
        val fieldSource = field.src
        if (fieldSource.tableName.isNotEmpty() && fieldSource.originalColumnName.isNotEmpty()) {
            // Try as a direct table first
            tableLookup[fieldSource.tableName.lowercase()]?.let { table ->
                table.findColumnByName(fieldSource.originalColumnName)?.let { col ->
                    return col.annotations.containsKey(AnnotationConstants.ADAPTER)
                }
            }
            // If not a table, try resolving through a VIEW: tableName may be a view name
            val view = columnLookup.findViewByName(fieldSource.tableName)
            if (view != null) {
                val viewField = viewFieldLookup[view]?.get(fieldSource.fieldName)
                if (viewField != null) {
                    tableLookup[viewField.src.tableName.lowercase()]?.let { table ->
                        table.findColumnByName(viewField.src.originalColumnName)?.let { col ->
                            return col.annotations.containsKey(AnnotationConstants.ADAPTER)
                        }
                    }
                }
            }


        }

        // Fallback: try to resolve by base original column name across all tables (best-effort)
        val baseName = baseOriginalNameForField(field, aliasPrefixes)
        tableLookup.values.forEach { table ->
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
