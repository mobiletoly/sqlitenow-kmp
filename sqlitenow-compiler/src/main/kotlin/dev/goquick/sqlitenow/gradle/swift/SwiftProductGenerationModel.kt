/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.gradle.swift

import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.model.ResultMappingPlan
import dev.goquick.sqlitenow.gradle.oversqlite.ResolvedOversqliteSyncTable
import dev.goquick.sqlitenow.gradle.processing.AffectedTablesResolver
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.util.Locale

internal class SwiftProductGenerationModel(
    private val context: GeneratorContext,
    private val plan: SwiftGenerationPlan,
    private val affectedTablesResolver: AffectedTablesResolver,
) {
    private val resultFieldsBySelectStatement: Map<Int, List<SwiftProductField>> by lazy {
        selectStatements.associate { (namespace, statement) ->
            System.identityHashCode(statement) to buildResultFields(namespace, statement)
        }
    }

    private val selectColumnFieldsByStatement: Map<Int, List<SwiftProductField>> by lazy {
        selectStatements.associate { (namespace, statement) ->
            System.identityHashCode(statement) to buildSelectColumnFields(namespace, statement)
        }
    }

    private val returningFieldsByStatement: Map<Int, List<SwiftProductField>> by lazy {
        executeReturningStatements.associate { (namespace, statement) ->
            System.identityHashCode(statement) to buildReturningFields(namespace, statement)
        }
    }

    private val parameterDescriptorsByStatement: Map<Int, List<SwiftProductParameter>> by lazy {
        statements.associate { (namespace, statement) ->
            System.identityHashCode(statement) to buildParameterDescriptors(namespace, statement)
        }
    }

    private val resultStatements: List<SwiftProductResult> by lazy {
        buildResultStatements()
    }

    private val namespaceResultsByName: Map<String, List<SwiftProductResult>> by lazy {
        namespaces.associateWith { namespace -> buildNamespaceResults(namespace) }
    }

    private val resultNamesByStatement: Map<Int, String> by lazy {
        selectStatements
            .map { (namespace, statement) -> System.identityHashCode(statement) to plan.resultClassName(namespace, statement) }
            .plus(
                executeReturningStatements.map { (namespace, statement) ->
                    System.identityHashCode(statement) to plan.resultClassName(namespace, statement)
                }
            )
            .toMap()
    }

    val adapterDescriptors: List<SwiftAdapterDescriptor> by lazy {
        plan.adapterDescriptors
    }

    val namespaces: List<String> by lazy {
        plan.namespaces
    }

    private val statements: List<Pair<String, AnnotatedStatement>> by lazy {
        plan.statements.map { it.namespace to it.statement }
    }

    private val selectStatements: List<Pair<String, AnnotatedSelectStatement>> by lazy {
        plan.selectStatements.map { it.namespace to it.statement }
    }

    private val executeReturningStatements: List<Pair<String, AnnotatedExecuteStatement>> by lazy {
        plan.executeReturningStatements.map { it.namespace to it.statement }
    }

    fun validateSupportedSurface(syncEnabled: Boolean, syncTables: List<ResolvedOversqliteSyncTable>) {
        selectStatements.forEach { (namespace, statement) ->
            require(statement.annotations.mapTo == null) {
                "Product Swift source export does not support mapTo result adapters until a later phase: ${statement.name}."
            }
            if (statement.mappingPlan.includedCollectionEntries.isNotEmpty()) {
                require(!statement.annotations.collectionKey.isNullOrBlank()) {
                    "Product Swift SELECT '${statement.name}' with collection dynamic fields requires statement-level collectionKey."
                }
            }
            statement.mappingPlan.includedDynamicEntries.forEach { entry ->
                val dynamicFieldIdentity = "${resultClassName(namespace, statement)}.${entry.field.swiftPropertyName(statement)}"
                val mappingType = entry.mappingType
                require(
                    mappingType == AnnotationConstants.MappingType.COLLECTION ||
                        mappingType == AnnotationConstants.MappingType.PER_ROW
                ) {
                    "Product Swift source export supports only collection and perRow dynamic fields in this phase: $dynamicFieldIdentity."
                }
                require(!entry.field.annotations.aliasPrefix.isNullOrBlank()) {
                    "Product Swift dynamic field '$dynamicFieldIdentity' requires aliasPrefix."
                }
                if (mappingType == AnnotationConstants.MappingType.COLLECTION) {
                    require(!entry.field.annotations.collectionKey.isNullOrBlank()) {
                        "Product Swift collection dynamic field '$dynamicFieldIdentity' requires collectionKey."
                    }
                }
                val propertyType = entry.field.annotations.propertyType.orEmpty()
                val targetResultName = dynamicFieldElementType(entry.field)
                require(propertyType.isNotBlank() && targetResultName.isNotBlank()) {
                    "Product Swift dynamic field '$dynamicFieldIdentity' requires propertyType."
                }
                val targetStatement = context.findSelectStatementByResultName(targetResultName)
                require(targetStatement != null) {
                    "Product Swift dynamic field '$dynamicFieldIdentity' targets unknown result type '$targetResultName'."
                }
                require(targetStatement.mappingPlan.includedDynamicEntries.isEmpty()) {
                    "Product Swift dynamic field '$dynamicFieldIdentity' targets result " +
                        "'$targetResultName' with dynamic fields; nested dynamic result targets are not supported in this phase."
                }
            }
        }
        if (syncEnabled) {
            require(syncTables.isNotEmpty()) {
                "Product Swift runtime=sync source export requires at least one table annotated with enableSync=true."
            }
        } else {
            require(context.createTableStatements.none { it.annotations.enableSync }) {
                "Product Swift core source export does not support sync-enabled tables. Set runtime=sync for sync product export."
            }
        }
    }

    fun resultFields(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): List<SwiftProductField> =
        resultFieldsBySelectStatement[System.identityHashCode(statement)]
            ?: buildResultFields(namespace, statement)

    private fun buildResultFields(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): List<SwiftProductField> =
        statement.mappingPlan.regularFields.mapIndexed { index, field ->
            selectField(namespace, statement, field, physicalIndex(statement, field, index))
        } + dynamicResultFields(statement)

    fun selectColumnFields(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): List<SwiftProductField> =
        selectColumnFieldsByStatement[System.identityHashCode(statement)]
            ?: buildSelectColumnFields(namespace, statement)

    private fun buildSelectColumnFields(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): List<SwiftProductField> =
        statement.fields
            .filterNot { it.annotations.isDynamicField }
            .mapIndexed { index, field ->
                selectField(namespace, statement, field, physicalIndex(statement, field, index))
            }

    fun dynamicResultFields(statement: AnnotatedSelectStatement): List<SwiftProductField> =
        statement.mappingPlan.includedDynamicEntries.map { entry ->
            SwiftProductField(
                propertyName = entry.field.swiftPropertyName(statement),
                swiftType = dynamicFieldSwiftType(entry.field),
                columnType = "",
                columnName = entry.field.src.fieldName,
                index = -1,
                adapter = null,
                dynamicFieldName = entry.field.src.fieldName,
            )
        }

    fun returningFields(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ): List<SwiftProductField> =
        returningFieldsByStatement[System.identityHashCode(statement)]
            ?: buildReturningFields(namespace, statement)

    private fun buildReturningFields(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ): List<SwiftProductField> =
        ReturningColumnsResolver.resolveColumns(context, statement).mapIndexed { index, column ->
            returningField(namespace, statement, column, index)
        }

    fun selectField(
        namespace: String,
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        index: Int,
    ): SwiftProductField {
        val aliasPrefixes = context.adapterConfig.collectAliasPrefixesForSelect(statement)
        val adapter = outputAdapter(namespace, statement, field, aliasPrefixes)
        val swiftType = adapter?.outputSwiftType ?: field.swiftType(statement)
        requireKnownReadableType(swiftType, adapter, statement.name, field.src.fieldName)
        return SwiftProductField(
            propertyName = field.swiftPropertyName(statement),
            swiftType = swiftType,
            columnType = adapter?.inputSwiftType?.runtimeColumnType()
                ?: runtimeColumnType(field.src.dataType, swiftType),
            columnName = field.src.fieldName,
            index = index,
            adapter = adapter,
            dynamicFieldName = null,
        )
    }

    fun returningField(
        namespace: String,
        statement: AnnotatedExecuteStatement,
        column: AnnotatedCreateTableStatement.Column,
        index: Int,
    ): SwiftProductField {
        val propertyName = ReturningColumnsResolver.propertyNameForColumn(statement, column)
        val adapter = outputAdapter(namespace, column, propertyName)
        val swiftType = adapter?.outputSwiftType ?: column.swiftType()
        requireKnownReadableType(swiftType, adapter, statement.name, column.src.name)
        return SwiftProductField(
            propertyName = propertyName,
            swiftType = swiftType,
            columnType = adapter?.inputSwiftType?.runtimeColumnType()
                ?: runtimeColumnType(column.src.dataType, swiftType),
            columnName = column.src.name,
            index = index,
            adapter = adapter,
            dynamicFieldName = null,
        )
    }

    fun dynamicMappingTargetFields(
        parentStatement: AnnotatedSelectStatement,
        targetStatement: AnnotatedSelectStatement,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
    ): List<SwiftProductField> {
        val targetNamespace = plan.namespaceFor(targetStatement)
        val aliasPrefixes = context.adapterConfig.collectAliasPrefixesForSelect(targetStatement)
        return targetStatement.mappingPlan.regularFields.map { targetField ->
            val mappingColumn = dynamicMappingColumn(mapping, targetField)
            val index = parentStatement.src.fields.indexOfFirst { it.fieldName == mappingColumn.fieldName }
                .takeIf { it >= 0 }
                ?: error("Product Swift dynamic mapping column '${mappingColumn.fieldName}' is not selected by '${parentStatement.name}'.")
            val adapter = outputAdapter(targetNamespace, targetStatement, targetField, aliasPrefixes)
            val swiftType = adapter?.outputSwiftType ?: targetField.swiftType(targetStatement)
            requireKnownReadableType(swiftType, adapter, parentStatement.name, mappingColumn.fieldName)
            SwiftProductField(
                propertyName = targetField.swiftPropertyName(targetStatement),
                swiftType = swiftType,
                columnType = adapter?.inputSwiftType?.runtimeColumnType()
                    ?: runtimeColumnType(mappingColumn.dataType, swiftType),
                columnName = mappingColumn.fieldName,
                index = index,
                adapter = adapter,
                dynamicFieldName = null,
            )
        }
    }

    fun dynamicMappingColumn(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        targetField: AnnotatedSelectStatement.Field,
    ): SelectStatement.FieldSource {
        val targetNames = listOf(
            targetField.src.fieldName,
            targetField.src.originalColumnName,
        )
            .filter { it.isNotBlank() }
            .map { normalizeDynamicColumnName(it, null) }
            .toSet()

        return mapping.columns.firstOrNull { column ->
            val normalizedFieldName = normalizeDynamicColumnName(column.fieldName, mapping.aliasPrefix)
            val normalizedOriginalName = normalizeDynamicColumnName(column.originalColumnName, null)
            normalizedFieldName in targetNames || normalizedOriginalName in targetNames
        } ?: error(
            "Product Swift dynamic field '${mapping.fieldName}' cannot map target field '${targetField.src.fieldName}'."
        )
    }

    fun normalizeDynamicColumnName(value: String, aliasPrefix: String?): String {
        val withoutDisambiguator = value.substringBefore(':')
        val withoutPrefix = aliasPrefix
            ?.takeIf { it.isNotBlank() && withoutDisambiguator.startsWith(it, ignoreCase = true) }
            ?.let { withoutDisambiguator.substring(it.length) }
            ?: withoutDisambiguator
        return withoutPrefix.lowercase(Locale.ROOT)
    }

    fun dynamicFieldSwiftType(field: AnnotatedSelectStatement.Field): String {
        val propertyType = field.annotations.propertyType.orEmpty().toSwiftTypeName()
        val mappingType = field.annotations.mappingType?.let { AnnotationConstants.MappingType.fromString(it) }
        return if (
            mappingType == AnnotationConstants.MappingType.PER_ROW &&
            field.annotations.notNull != true &&
            !propertyType.endsWith("?")
        ) {
            "$propertyType?"
        } else {
            propertyType
        }
    }

    fun dynamicFieldElementType(field: AnnotatedSelectStatement.Field): String {
        val propertyType = field.annotations.propertyType.orEmpty()
            .removeSuffix("?")
            .trim()
        val rawElement = if (propertyType.contains("<") && propertyType.endsWith(">")) {
            propertyType.substringAfter("<").substringBeforeLast(">")
        } else {
            propertyType
        }
        return rawElement.removeSuffix("?").substringAfterLast('.')
    }

    fun dynamicFieldHelperName(statementId: String, propertyName: String): String =
        "${statementId}Map${pascalize(propertyName)}"

    fun parameterDescriptors(
        namespace: String,
        statement: AnnotatedStatement,
    ): List<SwiftProductParameter> =
        parameterDescriptorsByStatement[System.identityHashCode(statement)]
            ?: buildParameterDescriptors(namespace, statement)

    private fun buildParameterDescriptors(
        namespace: String,
        statement: AnnotatedStatement,
    ): List<SwiftProductParameter> =
        plan.baseParameters(statement).map { base ->
            val adapter = inputAdapter(namespace, statement, base.sqlName)
            val swiftType = when {
                adapter != null && base.collection -> "[${adapter.inputSwiftType}]"
                adapter != null -> adapter.inputSwiftType
                else -> base.swiftType
            }
            val bindSwiftType = adapter?.outputSwiftType ?: swiftType
            SwiftProductParameter(
                sqlName = base.sqlName,
                propertyName = base.propertyName,
                swiftType = swiftType,
                collection = base.collection,
                bindKind = bindSwiftType.swiftProductBindKind(),
                adapter = adapter,
            )
        }

    fun inputAdapter(
        namespace: String,
        statement: AnnotatedStatement,
        paramName: String,
    ): SwiftAdapterDescriptor? {
        val column = context.columnLookup.findColumnForParameter(statement, paramName) ?: return null
        if (!column.annotations.containsKey(AnnotationConstants.ADAPTER)) return null
        val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(column.src.name)
        val expectedName = "${columnName}ToSqlValue"
        val config = context.adapterConfig.collectAllParamConfigs(statement, namespace)
            .firstOrNull {
                it.kind == AdapterConfig.AdapterKind.INPUT &&
                    plan.adapterBaseName(it.adapterFunctionName) == expectedName
            }
            ?: error("Adapter '$expectedName' was not collected for Swift input parameter '$paramName'.")
        return plan.adapterBySignature(
            namespace = namespace,
            expectedName = expectedName,
            expectedInputType = config.inputType,
            expectedOutputType = config.outputType,
        )
    }

    fun outputAdapter(
        namespace: String,
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        aliasPrefixes: List<String>,
    ): SwiftAdapterDescriptor? {
        if (!context.adapterConfig.hasAdapterAnnotation(field, aliasPrefixes)) return null
        val baseColumnName = context.adapterConfig.baseOriginalNameForField(field, aliasPrefixes, statement)
        val propertyName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(baseColumnName)
        val expectedName = context.adapterConfig.getOutputAdapterFunctionName(propertyName)
        val config = context.adapterConfig.collectAllParamConfigs(statement, namespace)
            .firstOrNull {
                it.kind == AdapterConfig.AdapterKind.RESULT_FIELD &&
                    plan.adapterBaseName(it.adapterFunctionName) == expectedName
            }
            ?: error("Adapter '$expectedName' was not collected for Swift output field '${field.src.fieldName}'.")
        return plan.adapterBySignature(
            namespace = namespace,
            expectedName = expectedName,
            expectedInputType = config.inputType,
            expectedOutputType = config.outputType,
        )
    }

    fun outputAdapter(
        namespace: String,
        column: AnnotatedCreateTableStatement.Column,
        propertyName: String,
    ): SwiftAdapterDescriptor? {
        if (!column.annotations.containsKey(AnnotationConstants.ADAPTER)) return null
        val adapterName = context.adapterConfig.getOutputAdapterFunctionName(propertyName)
        val config = plan.executeReturningStatements
            .filter { it.namespace == namespace }
            .flatMap { context.adapterConfig.collectExecuteReturningOutputParamConfigs(it.statement) }
            .firstOrNull {
                it.kind == AdapterConfig.AdapterKind.RESULT_FIELD &&
                    plan.adapterBaseName(it.adapterFunctionName) == adapterName
            }
            ?: error("Adapter '$adapterName' was not collected for Swift RETURNING column '${column.src.name}'.")
        return plan.adapterBySignature(
            namespace = namespace,
            expectedName = adapterName,
            expectedInputType = config.inputType,
            expectedOutputType = config.outputType,
        )
    }

    fun resultStatements(): List<SwiftProductResult> = resultStatements

    private fun buildResultStatements(): List<SwiftProductResult> {
        val results = linkedMapOf<String, SwiftProductResult>()
        selectStatements.forEach { (namespace, statement) ->
            val name = resultClassName(namespace, statement)
            results.putIfAbsent(name, SwiftProductResult(name = name, fields = resultFields(namespace, statement)))
        }
        executeReturningStatements.forEach { (namespace, statement) ->
            val name = resultClassName(namespace, statement)
            results.putIfAbsent(name, SwiftProductResult(name = name, fields = returningFields(namespace, statement)))
        }
        return results.values.toList()
    }

    fun namespaceResults(namespace: String): List<SwiftProductResult> =
        namespaceResultsByName[namespace].orEmpty()

    private fun buildNamespaceResults(namespace: String): List<SwiftProductResult> {
        val results = linkedMapOf<String, SwiftProductResult>()
        context.nsWithStatements[namespace].orEmpty().forEach { statement ->
            when (statement) {
                is AnnotatedSelectStatement -> {
                    val name = resultClassName(namespace, statement)
                    results.putIfAbsent(name, SwiftProductResult(name = name, fields = resultFields(namespace, statement)))
                }
                is AnnotatedExecuteStatement -> {
                    if (statement.hasReturningClause()) {
                        val name = resultClassName(namespace, statement)
                        results.putIfAbsent(name, SwiftProductResult(name = name, fields = returningFields(namespace, statement)))
                    }
                }
                else -> Unit
            }
        }
        return results.values.toList()
    }

    fun resultClassName(namespace: String, statement: AnnotatedStatement): String =
        resultNamesByStatement[System.identityHashCode(statement)]
            ?: plan.resultClassName(namespace, statement)

    fun statementSql(statement: AnnotatedStatement): String =
        when (statement) {
            is AnnotatedSelectStatement -> statement.src.sql
            is AnnotatedExecuteStatement -> statement.src.sql
            else -> error("Unsupported Swift product statement: ${statement.name}")
        }

    fun physicalIndex(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        fallback: Int,
    ): Int =
        statement.src.fields.indexOfFirst { it.fieldName == field.src.fieldName }
            .takeIf { it >= 0 }
            ?: fallback

    fun statementIdentifier(statement: AnnotatedStatement): String = statement.swiftFunctionName()

    fun bindValuesArguments(params: List<SwiftProductParameter>): String =
        if (params.isEmpty()) "adapters: adapters" else "params, adapters: adapters"

    fun streamArguments(params: List<SwiftProductParameter>): String =
        if (params.isEmpty()) {
            "runtime: runtime, adapters: adapters"
        } else {
            "params, runtime: runtime, adapters: adapters"
        }

    fun AnnotatedCreateTableStatement.Column.swiftType(): String {
        val explicit = annotations[AnnotationConstants.PROPERTY_TYPE] as? String
        return src.dataType.sqliteSwiftType(
            nullable = isNullable(),
            explicitType = explicit,
        )
    }

    fun runtimeColumnType(sqlType: String, swiftType: String): String {
        val rawSwift = swiftType.dropSwiftOptional()
        return when (rawSwift) {
            "Bool" -> "bool"
            "Data" -> "blob"
            "Double" -> "double"
            "Int64" -> "int64"
            "String" -> "text"
            else -> {
                val normalized = sqlType.uppercase(Locale.ROOT)
                when {
                    "INT" in normalized -> "int64"
                    "REAL" in normalized || "FLOA" in normalized || "DOUB" in normalized -> "double"
                    "BLOB" in normalized -> "blob"
                    else -> "text"
                }
            }
        }
    }

    fun String.runtimeColumnType(): String =
        when (dropSwiftOptional()) {
            "Bool" -> "bool"
            "Data" -> "blob"
            "Double" -> "double"
            "Int64" -> "int64"
            "String" -> "text"
            else -> "text"
        }

    fun requireKnownReadableType(
        swiftType: String,
        adapter: SwiftAdapterDescriptor?,
        statementName: String,
        fieldName: String,
    ) {
        if (adapter != null) return
        require(swiftType.dropSwiftOptional() in setOf("Bool", "Data", "Double", "Int64", "String")) {
            "Product Swift source export needs an adapter for '$statementName.$fieldName' with Swift type '$swiftType'."
        }
    }

    fun affectedTables(statement: AnnotatedStatement): List<String> =
        affectedTablesResolver.tablesFor(statement).toList()

}

internal data class SwiftProductResult(
    val name: String,
    val fields: List<SwiftProductField>,
)

internal data class SwiftProductField(
    val propertyName: String,
    val swiftType: String,
    val columnType: String,
    val columnName: String,
    val index: Int,
    val adapter: SwiftAdapterDescriptor?,
    val dynamicFieldName: String?,
) {
    fun readExpression(cellExpression: String): String {
        val rawExpression = when (adapter?.inputSwiftType ?: swiftType) {
            "Int64" -> "try sqliteNowReadInt64($cellExpression, column: ${columnName.toSwiftStringLiteral()})"
            "Int64?" -> "sqliteNowReadOptionalInt64($cellExpression)"
            "Double" -> "try sqliteNowReadDouble($cellExpression, column: ${columnName.toSwiftStringLiteral()})"
            "Double?" -> "sqliteNowReadOptionalDouble($cellExpression)"
            "Bool" -> "try sqliteNowReadBool($cellExpression, column: ${columnName.toSwiftStringLiteral()})"
            "Bool?" -> "sqliteNowReadOptionalBool($cellExpression)"
            "Data" -> "try sqliteNowReadData($cellExpression, column: ${columnName.toSwiftStringLiteral()})"
            "Data?" -> "sqliteNowReadOptionalData($cellExpression)"
            "String" -> "try sqliteNowReadString($cellExpression, column: ${columnName.toSwiftStringLiteral()})"
            "String?" -> "sqliteNowReadOptionalString($cellExpression)"
            else -> "try sqliteNowReadString($cellExpression, column: ${columnName.toSwiftStringLiteral()})"
        }
        return adapter?.let { "try adapters.${it.name}($rawExpression)" } ?: rawExpression
    }
}
