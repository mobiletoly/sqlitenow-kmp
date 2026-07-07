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

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.database.UniqueAdapter
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.util.pascalize

internal class SwiftKmpExportModel(
    private val context: GeneratorContext,
    private val plan: SwiftGenerationPlan,
) {
    val generatedAdapterProviders: Map<String, List<UniqueAdapter>>
        get() = plan.generatedAdapterProviders

    fun parameterDescriptors(statement: AnnotatedStatement): List<SwiftKmpParameter> =
        plan.baseParameters(statement).map { base ->
            SwiftKmpParameter(
                propertyName = base.propertyName,
                swiftType = base.swiftType,
                bridgeKotlinType = base.bridgeKotlinType,
            )
        }

    fun adapterDescriptors(): List<SwiftAdapterDescriptor> = plan.adapterDescriptors

    fun namespaces(): List<String> = plan.namespaces

    fun resultStatements(): List<SwiftKmpResult> {
        val results = linkedMapOf<String, SwiftKmpResult>()
        selectStatements().forEach { (_, statement) ->
            val name = resultClassName(statement)
            results.putIfAbsent(name, SwiftKmpResult(name = name, fields = resultFields(statement)))
        }
        executeReturningStatements().forEach { (_, statement) ->
            val name = resultClassName(statement)
            results.putIfAbsent(name, SwiftKmpResult(name = name, fields = returningFields(statement)))
        }
        return results.values.toList()
    }

    fun statements(): List<Pair<String, AnnotatedStatement>> =
        plan.statements.map { it.namespace to it.statement }

    fun selectStatements(): List<Pair<String, AnnotatedSelectStatement>> =
        plan.selectStatements.map { it.namespace to it.statement }

    fun executeStatements(): List<Pair<String, AnnotatedExecuteStatement>> =
        plan.executeStatements.map { it.namespace to it.statement }

    fun executeReturningStatements(): List<Pair<String, AnnotatedExecuteStatement>> =
        plan.executeReturningStatements.map { it.namespace to it.statement }

    fun resultClasses(): List<String> =
        resultStatements().map { it.name }.sorted()

    fun resultClassName(statement: AnnotatedStatement): String =
        plan.resultClassName(statement)

    fun namespaceFor(target: AnnotatedStatement): String =
        plan.namespaceFor(target)

    fun queryObjectName(namespace: String): String = plan.queryObjectName(namespace)

    fun bridgeSelectMethodName(namespace: String, statement: AnnotatedSelectStatement): String =
        "${statement.swiftFunctionName()}${pascalize(namespace)}Query"

    fun bridgeExecuteMethodName(namespace: String, statement: AnnotatedExecuteStatement): String =
        "${statement.swiftFunctionName()}${pascalize(namespace)}"

    fun bridgeSelectQueryClassName(statement: AnnotatedSelectStatement): String =
        "App${pascalize(namespaceFor(statement))}${statement.getDataClassName()}Query"

    fun bridgeExecuteReturningQueryClassName(statement: AnnotatedExecuteStatement): String =
        "App${pascalize(namespaceFor(statement))}${statement.getDataClassName()}Query"

    private fun resultFields(statement: AnnotatedSelectStatement): List<SwiftKmpField> =
        (statement.mappingPlan.regularFields + statement.mappingPlan.includedDynamicFields).map { field ->
            val (propertyName, kotlinType) = context.selectFieldGenerator.generateFieldInfo(
                field,
                statement.annotations.propertyNameGenerator
            )
            swiftField(propertyName = propertyName, kotlinType = kotlinType.toString())
        }

    private fun returningFields(statement: AnnotatedExecuteStatement): List<SwiftKmpField> =
        ReturningColumnsResolver.resolveColumns(context, statement).map { column ->
            val baseType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val kotlinType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
                baseType = baseType,
                propertyType = propertyType,
                isNullable = column.isNullable(),
                packageName = context.packageName,
            )
            val propertyName = ReturningColumnsResolver.propertyNameForColumn(statement, column)
            swiftField(propertyName = propertyName, kotlinType = kotlinType.toString())
        }

    private fun swiftField(propertyName: String, kotlinType: String): SwiftKmpField {
        val nestedResult = nestedResultReference(kotlinType)
        return SwiftKmpField(
            propertyName = propertyName,
            swiftType = kotlinType.toSwiftTypeName(),
            bridgeKotlinType = nestedResult?.bridgeKotlinType ?: kotlinType.toBridgeKotlinType(),
            nestedResult = nestedResult,
        )
    }

    private fun nestedResultReference(kotlinType: String): SwiftKmpNestedResult? {
        val type = kotlinType.removeSuffix("?")
        val nullable = kotlinType.endsWith("?")
        val collectionInner = listOf(
            "kotlin.collections.Collection<",
            "Collection<",
            "kotlin.collections.List<",
            "List<",
        ).firstNotNullOfOrNull { prefix ->
            type.takeIf { it.startsWith(prefix) }
                ?.substringAfter("<")
                ?.substringBeforeLast(">")
        }
        val rawResultType = collectionInner ?: type
        val resultName = rawResultType.removeSuffix("?").substringAfterLast('.')
        if (resultName !in resultModelNames()) return null
        val bridgeResultName = resultName.swiftBridgeResultName()
        val bridgeKotlinType = if (collectionInner != null) {
            "List<$bridgeResultName>".withKotlinNullable(nullable)
        } else {
            bridgeResultName.withKotlinNullable(nullable)
        }
        return SwiftKmpNestedResult(
            swiftType = resultName,
            bridgeType = bridgeResultName,
            bridgeKotlinType = bridgeKotlinType,
            isCollection = collectionInner != null,
            isNullable = nullable,
        )
    }

    private fun resultModelNames(): Set<String> =
        plan.resultModelNames()
}

internal data class SwiftKmpResult(
    val name: String,
    val fields: List<SwiftKmpField>,
)

internal data class SwiftKmpNestedResult(
    val swiftType: String,
    val bridgeType: String,
    val bridgeKotlinType: String,
    val isCollection: Boolean,
    val isNullable: Boolean,
) {
    fun bridgeResultExpression(source: String): String =
        when {
            isCollection && isNullable -> "$source?.map { it.to$bridgeType() }"
            isCollection -> "$source.map { it.to$bridgeType() }"
            isNullable -> "$source?.to$bridgeType()"
            else -> "$source.to$bridgeType()"
        }

    fun bridgeToSwiftExpression(source: String): String =
        when {
            isCollection && isNullable -> "$source?.map($swiftType.init)"
            isCollection -> "$source.map($swiftType.init)"
            isNullable -> "$source.map($swiftType.init)"
            else -> "$swiftType($source)"
        }
}

internal data class SwiftKmpField(
    val propertyName: String,
    val swiftType: String,
    val bridgeKotlinType: String,
    val nestedResult: SwiftKmpNestedResult? = null,
) {
    fun bridgeResultExpression(source: String): String =
        nestedResult?.bridgeResultExpression(source) ?: source

    fun bridgeToSwiftExpression(source: String): String =
        nestedResult?.bridgeToSwiftExpression(source) ?: when {
            swiftType.dropSwiftOptional().isSwiftData() && bridgeKotlinType.endsWith("?") -> "$source.map { sqliteNowData(from: \$0) }"
            swiftType.isNonOptionalSwiftData() && bridgeKotlinType.isNonOptionalBridgeByteArray() -> "sqliteNowData(from: $source)"
            swiftType == "Int64?" && bridgeKotlinType == "Long?" -> "sqliteNowInt64(from: $source)"
            swiftType == "Double?" && bridgeKotlinType == "Double?" -> "sqliteNowDouble(from: $source)"
            swiftType == "Bool?" && bridgeKotlinType == "Boolean?" -> "sqliteNowBool(from: $source)"
            else -> source
        }

    fun swiftToBridgeExpression(source: String): String =
        when {
            swiftType.dropSwiftOptional().isSwiftData() && bridgeKotlinType.endsWith("?") -> "$source.map { sqliteNowByteArray(from: \$0) }"
            swiftType.isNonOptionalSwiftData() && bridgeKotlinType.isNonOptionalBridgeByteArray() -> "sqliteNowByteArray(from: $source)"
            swiftType == "Int64?" && bridgeKotlinType == "Long?" -> "sqliteNowKotlinLong(from: $source)"
            swiftType == "Double?" && bridgeKotlinType == "Double?" -> "sqliteNowKotlinDouble(from: $source)"
            swiftType == "Bool?" && bridgeKotlinType == "Boolean?" -> "sqliteNowKotlinBoolean(from: $source)"
            else -> source
        }

    fun supportsSwiftEquatable(): Boolean = swiftType.supportsSwiftEquatable()

    fun supportsSwiftSendable(): Boolean = swiftType.supportsSwiftSendable()
}

internal data class SwiftKmpParameter(
    val propertyName: String,
    val swiftType: String,
    val bridgeKotlinType: String,
) {
    fun swiftToBridgeExpression(source: String): String =
        when {
            swiftType.dropSwiftOptional().isSwiftData() && bridgeKotlinType.endsWith("?") -> "$source.map { sqliteNowByteArray(from: \$0) }"
            swiftType.isNonOptionalSwiftData() && bridgeKotlinType.isNonOptionalBridgeByteArray() -> "sqliteNowByteArray(from: $source)"
            swiftType == "Int64?" && bridgeKotlinType == "Long?" -> "sqliteNowKotlinLong(from: $source)"
            swiftType == "Double?" && bridgeKotlinType == "Double?" -> "sqliteNowKotlinDouble(from: $source)"
            swiftType == "Bool?" && bridgeKotlinType == "Boolean?" -> "sqliteNowKotlinBoolean(from: $source)"
            else -> source
        }
}
