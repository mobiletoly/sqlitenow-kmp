package dev.goquick.sqlitenow.gradle.context

import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveSet
import kotlin.collections.forEach

/**
 * Handles adding adapter parameters to generated query functions and exposes helper lookups
 * for adapter parameter names derived from statement annotations.
 */
internal class AdapterParameterEmitter(
    private val generatorContext: GeneratorContext,
) {
    private val packageName: String = generatorContext.packageName
    private val adapterConfig: AdapterConfig = generatorContext.adapterConfig
    private val adapterNameResolver: AdapterParameterNameResolver =
        generatorContext.adapterNameResolver

    /**
     * Re-exposed so callers (and tests) can re-use the adapter resolution logic.
     */
    fun chooseAdapterParamNames(
        configs: List<AdapterConfig.ParamConfig>
    ): Map<AdapterConfig.ParamConfig, String> {
        return adapterNameResolver.chooseAdapterParamNames(configs)
    }

    fun addParameterBindingAdapters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
    ) {
        addAdapterParameters(fnBld, statement) { config ->
            config.adapterFunctionName.endsWith("ToSqlValue")
        }
    }

    fun addResultConversionAdapters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement
    ) {
        addAdapterParameters(fnBld, statement) { config ->
            config.adapterFunctionName.startsWith("sqlValueTo")
        }
    }

    fun addResultConversionAdaptersForExecute(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement
    ) {
        addExecuteReturningAdapters(fnBld, statement)
    }

    fun parameterBindingAdapterNames(statement: AnnotatedStatement): List<String> {
        return getFilteredAdapterNames(statement) { config ->
            config.adapterFunctionName.endsWith("ToSqlValue")
        }
    }

    fun resultConversionAdapterNames(statement: AnnotatedStatement): List<String> {
        return getFilteredAdapterNames(statement) { config ->
            config.adapterFunctionName.startsWith("sqlValueTo")
        }
    }

    fun buildJoinedReadParamsList(statement: AnnotatedSelectStatement): List<String> {
        val params = mutableListOf("statement")
        params += resultConversionAdapterNames(statement)
        return params
    }

    fun buildExecuteReadParamsList(statement: AnnotatedExecuteStatement): List<String> {
        val params = mutableListOf("statement")
        params += resultConversionAdapterNamesForExecute(statement)
        return params
    }

    private fun addAdapterParameters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        filter: (AdapterConfig.ParamConfig) -> Boolean,
    ) {
        val adapterConfigs = adapterConfig.collectAllParamConfigs(statement)
        val filteredConfigs = adapterConfigs.filter(filter)
        val chosenNames = chooseAdapterParamNames(filteredConfigs)
        val byName: MutableMap<String, AdapterConfig.ParamConfig> = linkedMapOf()
        filteredConfigs.forEach { cfg ->
            val name = chosenNames[cfg]!!
            byName.putIfAbsent(name, cfg)
        }
        byName.forEach { (name, cfg) ->
            val adapterType = LambdaTypeName.get(
                parameters = arrayOf(cfg.inputType),
                returnType = cfg.outputType
            )
            val adapterParam = ParameterSpec.builder(name, adapterType).build()
            fnBld.addParameter(adapterParam)
        }
    }

    private fun getFilteredAdapterNames(
        statement: AnnotatedStatement,
        filter: (AdapterConfig.ParamConfig) -> Boolean,
    ): List<String> {
        val adapterConfigs = adapterConfig.collectAllParamConfigs(statement)
        val filtered = adapterConfigs.filter(filter)
        val chosen = chooseAdapterParamNames(filtered)
        val seen = LinkedHashSet<String>()
        filtered.forEach { config ->
            seen.add(chosen[config]!!)
        }
        return seen.toList()
    }

    private fun addExecuteReturningAdapters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement,
    ) {
        val tableLookup =
            generatorContext.createTableStatements.associateBy { it.src.tableName.lowercase() }
        val tableStatement = statement.tableDefinition(tableLookup) ?: return
        val columnsToInclude = statement.returningColumns(tableStatement)
        val processedAdapters = mutableSetOf<String>()
        columnsToInclude.forEach { column ->
            if (column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                val propertyName =
                    statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
                val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)
                if (!processedAdapters.add(adapterFunctionName)) return@forEach

                val baseType =
                    SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
                val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
                val isNullable = column.isNullable()
                val targetType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
                    baseType,
                    propertyType,
                    isNullable,
                    packageName
                )
                val inputType = baseType.copy(nullable = isNullable)
                val outputType = targetType.copy(nullable = isNullable)
                val adapterType =
                    LambdaTypeName.get(parameters = arrayOf(inputType), returnType = outputType)
                val parameterSpec = ParameterSpec.builder(adapterFunctionName, adapterType).build()
                fnBld.addParameter(parameterSpec)
            }
        }
    }

    private fun resultConversionAdapterNamesForExecute(
        statement: AnnotatedExecuteStatement
    ): List<String> {
        val tableLookup =
            generatorContext.createTableStatements.associateBy { it.src.tableName.lowercase() }
        val tableStatement = statement.tableDefinition(tableLookup) ?: return emptyList()
        val columnsToInclude = statement.returningColumns(tableStatement)
        val adapterNames = mutableListOf<String>()
        val processedAdapters = mutableSetOf<String>()
        columnsToInclude.forEach { column ->
            if (column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                val propertyName =
                    statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
                val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)
                if (processedAdapters.add(adapterFunctionName)) {
                    adapterNames.add(adapterFunctionName)
                }
            }
        }
        return adapterNames
    }

    private fun AnnotatedExecuteStatement.tableDefinition(
        tableLookup: Map<String, AnnotatedCreateTableStatement>
    ): AnnotatedCreateTableStatement? {
        val tableName = when (val src = src) {
            is InsertStatement -> src.table
            is UpdateStatement -> src.table
            is DeleteStatement -> src.table
            else -> return null
        }
        return tableLookup[tableName.lowercase()]
    }

    private fun AnnotatedExecuteStatement.returningColumns(
        tableStatement: AnnotatedCreateTableStatement
    ): List<AnnotatedCreateTableStatement.Column> {
        val returningColumns = when (val src = src) {
            is InsertStatement -> src.returningColumns
            is UpdateStatement -> src.returningColumns
            is DeleteStatement -> src.returningColumns
            else -> emptyList<String>()
        }
        if (returningColumns.contains("*")) {
            return tableStatement.columns
        }
        val returningSet = CaseInsensitiveSet().apply { addAll(returningColumns) }
        return tableStatement.columns.filter { column -> returningSet.containsIgnoreCase(column.src.name) }
    }
}
