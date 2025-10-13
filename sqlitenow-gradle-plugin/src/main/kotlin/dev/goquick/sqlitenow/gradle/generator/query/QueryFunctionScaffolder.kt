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
package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.ParameterSpec
import dev.goquick.sqlitenow.gradle.processing.StatementUtils.getNamedParameters
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement

/**
 * Provides the shared receiver/parameter boilerplate for generated query extension functions.
 * Keeping this in one place ensures the bind/read/execute emitters remain focused on their
 * behaviour instead of repeating parameter plumbing logic.
 */
internal class QueryFunctionScaffolder(
    private val packageName: String,
    private val namespaceFormatter: (String) -> String,
    private val adapterParameterEmitter: AdapterParameterEmitter,
) {
    enum class AdapterType {
        NONE,
        PARAMETER_BINDING,
        RESULT_CONVERSION,
    }

    fun queryNamespaceName(namespace: String): String = namespaceFormatter(namespace)

    fun createParamsTypeName(namespace: String, className: String): ClassName {
        val capitalizedNamespace = queryNamespaceName(namespace)
        return ClassName(packageName, capitalizedNamespace)
            .nestedClass(className)
            .nestedClass("Params")
    }

    fun setupExecuteFunctionStructure(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
    ) {
        val receiverType = receiverType(namespace, className)
        fnBld.receiver(receiverType)
        val connectionParam = ParameterSpec.builder(
            name = "conn",
            ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"),
        ).build()
        fnBld.addParameter(connectionParam)
        if (getNamedParameters(statement).isNotEmpty()) {
            val paramsType = createParamsTypeName(namespace, className)
            val paramsParam = ParameterSpec.builder("params", paramsType).build()
            fnBld.addParameter(paramsParam)
        }
        adapterParameterEmitter.addParameterBindingAdapters(fnBld, namespace, statement)
        when (statement) {
            is AnnotatedSelectStatement ->
                adapterParameterEmitter.addResultConversionAdapters(fnBld, namespace, statement)

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    adapterParameterEmitter.addResultConversionAdaptersForExecute(fnBld, statement)
                }
            }

            else -> Unit
        }
    }

    fun setupStatementFunctionStructure(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        includeParamsParameter: Boolean,
        adapterType: AdapterType,
    ) {
        val receiverType = receiverType(namespace, className)
        fnBld.receiver(receiverType)
        val statementParam = ParameterSpec.builder(
            name = "statement",
            ClassName("dev.goquick.sqlitenow.core.sqlite", "SqliteStatement"),
        ).build()
        fnBld.addParameter(statementParam)
        if (includeParamsParameter && getNamedParameters(statement).isNotEmpty()) {
            val paramsType = createParamsTypeName(namespace, className)
            val paramsParam = ParameterSpec.builder("params", paramsType).build()
            fnBld.addParameter(paramsParam)
        }
        when (adapterType) {
            AdapterType.PARAMETER_BINDING ->
                adapterParameterEmitter.addParameterBindingAdapters(fnBld, namespace, statement)

            AdapterType.RESULT_CONVERSION -> when (statement) {
                is AnnotatedSelectStatement ->
                    adapterParameterEmitter.addResultConversionAdapters(fnBld, namespace, statement)

                is AnnotatedExecuteStatement -> {
                    if (statement.hasReturningClause()) {
                        adapterParameterEmitter.addResultConversionAdaptersForExecute(fnBld, statement)
                    }
                }

                else -> Unit
            }

            AdapterType.NONE -> Unit
        }
    }

    private fun receiverType(namespace: String, className: String): ClassName {
        val capitalizedNamespace = queryNamespaceName(namespace)
        return ClassName(packageName, capitalizedNamespace).nestedClass(className)
    }
}
