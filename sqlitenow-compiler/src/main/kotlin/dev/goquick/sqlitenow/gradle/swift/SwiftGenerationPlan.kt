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

import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.database.UniqueAdapter
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.util.pascalize

internal data class SwiftStatementRef<out T : AnnotatedStatement>(
    val namespace: String,
    val statement: T,
)

internal data class SwiftBaseParameter(
    val sqlName: String,
    val propertyName: String,
    val inferredType: TypeName,
    val swiftType: String,
    val bridgeKotlinType: String,
    val collection: Boolean,
)

internal data class SwiftAdapterDescriptor(
    val name: String,
    val inputKotlinType: String,
    val outputKotlinType: String,
    val inputSwiftType: String,
    val outputSwiftType: String,
) {
    fun resultClassName(): String = "App${pascalize(name)}AdapterResult"
}

internal class SwiftGenerationPlan(
    val context: GeneratorContext,
    private val dataStructCodeGenerator: DataStructCodeGenerator,
) {
    private val adapterPlanning = SwiftAdapterPlanning(context)

    val adaptersByNamespace: Map<String, List<UniqueAdapter>> by lazy {
        adapterPlanning.adaptersByNamespace
    }

    val generatedAdapterProviders: Map<String, List<UniqueAdapter>> by lazy {
        adapterPlanning.generatedAdapterProviders
    }

    val adapterDescriptors: List<SwiftAdapterDescriptor> by lazy {
        selectedAdapters
            .map { adapter -> adapter.toSwiftAdapterDescriptor(adapterName(adapter)) }
            .sortedBy { it.name }
    }

    private val selectedAdapters: List<UniqueAdapter> by lazy {
        generatedAdapterProviders.values
            .flatten()
            .sortedWith(compareBy({ it.functionName }, { it.inputType.toString() }, { it.outputType.toString() }))
    }

    private val adapterNamesBySignature: Map<SwiftAdapterSignatureKey, String> by lazy {
        selectedAdapters
            .groupBy { it.functionName }
            .flatMap { (_, adapters) ->
                adapters.map { adapter ->
                    val name = if (adapters.size == 1) {
                        adapter.functionName
                    } else {
                        adapter.signatureDisambiguatedFunctionName(
                            adapterPlanning.planner.baseFunctionKey(adapter.functionName)
                        )
                    }
                    adapter.swiftSignatureKey() to name
                }
            }
            .toMap()
    }

    val namespaces: List<String> by lazy {
        context.nsWithStatements.keys.sorted()
    }

    val statements: List<SwiftStatementRef<AnnotatedStatement>> by lazy {
        context.nsWithStatements.flatMap { (namespace, statements) ->
            statements.mapNotNull { statement ->
                when (statement) {
                    is AnnotatedSelectStatement,
                    is AnnotatedExecuteStatement -> SwiftStatementRef(namespace, statement)

                    else -> null
                }
            }
        }
    }

    val selectStatements: List<SwiftStatementRef<AnnotatedSelectStatement>> by lazy {
        context.nsWithStatements.flatMap { (namespace, statements) ->
            statements.filterIsInstance<AnnotatedSelectStatement>()
                .map { SwiftStatementRef(namespace, it) }
        }
    }

    val executeStatements: List<SwiftStatementRef<AnnotatedExecuteStatement>> by lazy {
        context.nsWithStatements.flatMap { (namespace, statements) ->
            statements.filterIsInstance<AnnotatedExecuteStatement>()
                .map { SwiftStatementRef(namespace, it) }
        }
    }

    val executeReturningStatements: List<SwiftStatementRef<AnnotatedExecuteStatement>> by lazy {
        executeStatements.filter { it.statement.hasReturningClause() }
    }

    private val namespaceByStatementIdentity: Map<Int, String> by lazy {
        statements.associate { System.identityHashCode(it.statement) to it.namespace }
    }

    fun namespaceFor(statement: AnnotatedStatement): String =
        namespaceByStatementIdentity[System.identityHashCode(statement)]
            ?: context.nsWithStatements.entries.firstNotNullOf { (namespace, statements) ->
                namespace.takeIf { statements.any { it === statement } }
            }

    fun resultClassName(statement: AnnotatedStatement): String =
        resultClassName(namespaceFor(statement), statement)

    fun resultClassName(namespace: String, statement: AnnotatedStatement): String =
        SharedResultTypeUtils.createResultTypeString(
            namespace = namespace,
            statement = statement,
        )

    fun resultModelNames(): Set<String> =
        selectStatements.map { resultClassName(it.namespace, it.statement) }.toSet() +
            executeReturningStatements.map { resultClassName(it.namespace, it.statement) }.toSet()

    fun queryObjectName(namespace: String): String = "${pascalize(namespace)}Query"

    fun adapterClassNameFor(namespace: String): String =
        adapterPlanning.planner.adapterClassNameFor(namespace)

    fun adapterPropertyNameFor(namespace: String): String =
        adapterPlanning.planner.adapterPropertyNameFor(namespace)

    fun adapterBaseName(functionName: String): String =
        adapterPlanning.planner.baseFunctionKey(functionName)

    fun adapterBySignature(
        namespace: String,
        expectedName: String,
        expectedInputType: TypeName,
        expectedOutputType: TypeName,
    ): SwiftAdapterDescriptor {
        val expectedBaseName = adapterPlanning.planner.baseFunctionKey(expectedName)
        val uniqueAdapter = selectedAdapters.firstOrNull { adapter ->
            adapterPlanning.planner.baseFunctionKey(adapter.functionName) == expectedBaseName &&
                adapter.inputType == expectedInputType &&
                adapter.outputType == expectedOutputType
        } ?: error(
            "Adapter '$expectedName' with signature ($expectedInputType) -> $expectedOutputType " +
                "was not collected for Swift export namespace '$namespace'."
        )
        return uniqueAdapter.toSwiftAdapterDescriptor(adapterName(uniqueAdapter))
    }

    fun baseParameters(statement: AnnotatedStatement): List<SwiftBaseParameter> =
        statement.uniqueNamedParameters().map { name ->
            val inferred = dataStructCodeGenerator.inferParameterType(name, statement)
            SwiftBaseParameter(
                sqlName = name,
                propertyName = statement.annotations.propertyNameGenerator.swiftParamPropertyName(name),
                inferredType = inferred,
                swiftType = inferred.toSwiftTypeName(),
                bridgeKotlinType = inferred.toBridgeKotlinType(),
                collection = inferred.isSwiftCollectionType(),
            )
        }

    private fun adapterName(adapter: UniqueAdapter): String =
        adapterNamesBySignature.getValue(adapter.swiftSignatureKey())
}

private data class SwiftAdapterSignatureKey(
    val baseFunctionName: String,
    val inputType: String,
    val outputType: String,
)

private fun UniqueAdapter.swiftSignatureKey(): SwiftAdapterSignatureKey =
    SwiftAdapterSignatureKey(
        baseFunctionName = functionName.substringBefore("For"),
        inputType = inputType.toString(),
        outputType = outputType.toString(),
    )

internal fun TypeName.isSwiftCollectionType(): Boolean =
    toString().startsWith("kotlin.collections.Collection<") ||
        toString().startsWith("kotlin.collections.List<") ||
        toString().startsWith("Collection<") ||
        toString().startsWith("List<")

private fun UniqueAdapter.toSwiftAdapterDescriptor(name: String): SwiftAdapterDescriptor =
    SwiftAdapterDescriptor(
        name = name,
        inputKotlinType = inputType.toString(),
        outputKotlinType = outputType.toString(),
        inputSwiftType = inputType.toSwiftTypeName(),
        outputSwiftType = outputType.toSwiftTypeName(),
    )
