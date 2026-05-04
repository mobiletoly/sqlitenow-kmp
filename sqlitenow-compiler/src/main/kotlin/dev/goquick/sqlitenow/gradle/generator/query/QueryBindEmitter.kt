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
package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.FunSpec
import dev.goquick.sqlitenow.gradle.processing.StatementUtils.getNamedParameters
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement

/**
 * Emits the `bindStatementParams` helper for any query statement, sharing parameter binding
 * behaviour across SELECT / INSERT / UPDATE / DELETE generators.
 */
internal class QueryBindEmitter(
    private val parameterBinding: ParameterBinding,
    private val scaffolder: QueryFunctionScaffolder,
    private val debug: Boolean,
) {
    fun generateBindStatementParamsFunction(
        namespace: String,
        statement: AnnotatedStatement,
    ): FunSpec {
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("bindStatementParams")
            .addKdoc("Binds parameters to an already prepared SQLiteStatement for the ${statement.name} query.")
        scaffolder.setupStatementFunctionStructure(
            fnBld = fnBld,
            statement = statement,
            namespace = namespace,
            className = className,
            includeParamsParameter = true,
            adapterType = QueryFunctionScaffolder.AdapterType.PARAMETER_BINDING,
        )
        fnBld.returns(Unit::class)
        addBindStatementParamsProcessing(fnBld, statement, namespace, className)
        return fnBld.build()
    }

    private fun addBindStatementParamsProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
    ) {
        val namedParameters = getNamedParameters(statement)
        if (namedParameters.isEmpty()) {
            return
        }
        if (debug) {
            fnBld.addStatement("val __paramsLog = mutableListOf<String>()")
            fnBld.addStatement("val __seenParams = mutableSetOf<String>()")
        }
        val processedAdapterVars = mutableMapOf<String, String>()
        namedParameters.forEachIndexed { index, paramName ->
            val propertyName =
                statement.annotations.propertyNameGenerator.convertToPropertyName(paramName)
            parameterBinding.generateParameterBinding(
                fnBld = fnBld,
                paramName = paramName,
                paramIndex = index,
                propertyName = propertyName,
                statement = statement,
                namespace = namespace,
                className = className,
                processedAdapterVars = processedAdapterVars,
            )
        }
        if (debug) {
            fnBld.addStatement(
                "sqliteNowLogger.d { %S + __paramsLog.joinToString(%S) }",
                "bind ${scaffolder.queryNamespaceName(namespace)}.$className params: ",
                ", ",
            )
        }
    }
}
