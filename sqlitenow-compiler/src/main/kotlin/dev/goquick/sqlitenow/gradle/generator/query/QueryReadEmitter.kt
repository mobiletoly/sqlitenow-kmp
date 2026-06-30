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
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder

/**
 * Emits the read* helper functions that materialise statement results into DTOs.
 * Heavy lifting (null-guard logic, dynamic field handling) stays in QueryCodeGenerator for now; the
 * emitter simply coordinates the scaffolding and delegates to the existing helpers.
 */
internal class QueryReadEmitter(
    private val packageName: String,
    private val queryNamespaceName: (String) -> String,
    private val scaffolder: QueryFunctionScaffolder,
    private val adapterParameterEmitter: AdapterParameterEmitter,
    private val adapterConfig: AdapterConfig,
    private val selectFieldGenerator: SelectFieldCodeGenerator,
    private val resultMappingHelper: ResultMappingHelper,
    private val generateGetterCallWithPrefixes: (
        AnnotatedSelectStatement,
        AnnotatedSelectStatement.Field,
        Int,
        PropertyNameGeneratorType,
        Boolean,
        Map<String, String>,
        List<String>,
    ) -> String,
    private val generateExecuteReturningGetter: (AnnotatedSelectStatement.Field, TypeName, Int, String) -> String,
    private val generateDynamicFieldMappingFromJoined: DynamicFieldExpression,
    private val resolveExecuteReturningFields: (AnnotatedExecuteStatement) -> List<ReturningColumnsResolver.ResolvedColumn>,
    private val findMainTableAlias: (List<AnnotatedSelectStatement.Field>) -> String?,
) {
    fun generateReadStatementResultFunction(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): FunSpec {
        val fnBld = createReadStatementResultFunctionBuilder(
            namespace = namespace,
            statement = statement,
            resultType = SharedResultTypeUtils.createPublicResultTypeName(packageName, namespace, statement),
        )
        addReadStatementResultProcessing(fnBld, statement, namespace)
        return fnBld.build()
    }

    fun generateReadJoinedStatementResultFunction(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): FunSpec {
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readJoinedStatementResult")
            .addKdoc("Read statement and convert it to joined data class with all fields from the SQL query")
        scaffolder.setupFunctionStructure(
            fnBld = fnBld,
            statement = statement,
            namespace = namespace,
            className = className,
            primaryParameter = QueryFunctionScaffolder.PrimaryParameter.STATEMENT,
            includeParamsParameter = false,
            adapterType = QueryFunctionScaffolder.AdapterType.NONE,
        )
        adapterParameterEmitter.addResultConversionAdapters(
            fnBld = fnBld,
            namespace = namespace,
            statement = statement,
            includeMapAdapters = false,
        )
        val resultType = SharedResultTypeUtils.createJoinedResultTypeName(packageName, namespace, statement)
        fnBld.returns(resultType)
        addReadJoinedStatementResultProcessing(fnBld, statement, resultType.simpleName)
        return fnBld.build()
    }

    fun generateReadStatementResultFunctionForExecute(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ): FunSpec {
        val fnBld = createReadStatementResultFunctionBuilder(
            namespace = namespace,
            statement = statement,
            resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement),
        )
        addReadStatementResultProcessingForExecute(fnBld, statement, namespace)
        return fnBld.build()
    }

    private fun createReadStatementResultFunctionBuilder(
        namespace: String,
        statement: AnnotatedStatement,
        resultType: TypeName,
    ): FunSpec.Builder {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")
        scaffolder.setupFunctionStructure(
            fnBld = fnBld,
            statement = statement,
            namespace = namespace,
            className = className,
            primaryParameter = QueryFunctionScaffolder.PrimaryParameter.STATEMENT,
            includeParamsParameter = false,
            adapterType = QueryFunctionScaffolder.AdapterType.RESULT_CONVERSION,
        )
        fnBld.returns(resultType)
        return fnBld
    }

    private fun addReadStatementResultProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String,
    ) {
        if (statement.hasDynamicFieldMapping()) {
            addReadStatementResultProcessingUsingJoined(fnBld, statement, namespace)
        } else {
            addReadStatementResultProcessingDirect(fnBld, statement, namespace)
        }
    }

    private fun addReadStatementResultProcessingUsingJoined(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String,
    ) {
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val paramsString = adapterParameterEmitter.buildSelectReadParamsList(
            namespace,
            statement,
            includeMapAdapters = false,
        ).joinToString(", ")
        val mapAdapterName = adapterParameterEmitter.mapToAdapterParameterName(namespace, statement)
        val transformationCall = buildString {
            append("    val joinedData = $capitalizedNamespace.$className.readJoinedStatementResult($paramsString)\n")
            if (mapAdapterName != null) {
                append("    val rawResult = $resultType(\n")
            } else {
                append("return $resultType(\n")
            }
            resultMappingHelper.addJoinedToMappedTransformation(
                builder = this,
                statement = statement,
                dynamicFieldMapper = { request, rowsVar ->
                    generateDynamicFieldMappingFromJoined(request, rowsVar)
                },
            )
            append(")")
            if (mapAdapterName != null) {
                append("\n    return $mapAdapterName(rawResult)")
            }
        }
        fnBld.addStatement(transformationCall)
    }

    private fun addReadJoinedStatementResultProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        resultTypeSimpleName: String,
    ) {
        val regularFields = statement.fields.filter { !it.annotations.isDynamicField }
        val joinedNameMap = resultMappingHelper.computeJoinedNameMap(statement)
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val aliasPrefixes = adapterConfig.collectAliasPrefixesForSelect(statement)
        val builder = IndentedCodeBuilder()
        builder.line("return $resultTypeSimpleName(")
        builder.indent {
            regularFields.forEachIndexed { index, field ->
                val key = JoinedPropertyNameResolver.JoinedFieldKey(
                    field.src.tableName,
                    field.src.fieldName,
                )
                val targetPropertyName = joinedNameMap[key]
                    ?: resultMappingHelper.getPropertyName(field, propertyNameGenerator)
                val getterCall = generateJoinedGetterCall(
                    statement = statement,
                    field = field,
                    columnIndex = index,
                    allFields = regularFields,
                    aliasPrefixes = aliasPrefixes,
                )
                val comment = resultMappingHelper.buildFieldDebugComment(
                    field = field,
                    selectStatement = statement.src,
                    propertyNameGenerator = propertyNameGenerator,
                    includeType = true,
                )
                if (comment.isNotEmpty()) {
                    line("// $comment")
                }
                val suffix = if (index == regularFields.lastIndex) "" else ","
                line("$targetPropertyName = $getterCall$suffix")
            }
        }
        builder.line(")")
        fnBld.addStatement(builder.build())
    }

    private fun generateJoinedGetterCall(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        allFields: List<AnnotatedSelectStatement.Field>,
        aliasPrefixes: List<String>,
    ): String {
        val fieldTableAlias = field.src.tableName
        val mainTableAlias = findMainTableAlias(allFields)
        val explicitNotNull = field.annotations.notNull == true
        val joinedNullable = if (fieldTableAlias.isNotBlank()) {
            mainTableAlias != null && fieldTableAlias != mainTableAlias
        } else {
            val dynAliasPrefixes = allFields
                .filter { it.annotations.isDynamicField }
                .mapNotNull { it.annotations.aliasPrefix }
                .filter { it.isNotBlank() }
            val visibleName = field.src.fieldName
            dynAliasPrefixes.any { prefix -> visibleName.startsWith(prefix) }
        }

        val baseDesiredType = selectFieldGenerator
            .generateProperty(field, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            .type

        val isFromJoinedForGetter = if (explicitNotNull) {
            false
        } else {
            joinedNullable && !baseDesiredType.isNullable
        }

        return generateGetterCallWithPrefixes(
            statement,
            field,
            columnIndex,
            PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedForGetter,
            statement.src.tableAliases,
            aliasPrefixes,
        )
    }

    private fun addReadStatementResultProcessingDirect(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String,
    ) {
        val baseResultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val mapAdapterName = adapterParameterEmitter.mapToAdapterParameterName(namespace, statement)
        val aliasPrefixes = adapterConfig.collectAliasPrefixesForSelect(statement)
        val lastIndex = lastNonDynamicIndex(statement)
        val builder = IndentedCodeBuilder()
        val rawVarName = "rawResult"
        if (mapAdapterName != null) {
            builder.line("val $rawVarName = $baseResultType(")
        } else {
            builder.line("return $baseResultType(")
        }
        builder.indent {
            statement.fields.forEachIndexed { index, field ->
                if (!field.annotations.isDynamicField) {
                    val propertyName = resultMappingHelper.getPropertyName(
                        field = field,
                        propertyNameGenerator = statement.annotations.propertyNameGenerator,
                    )
                    val getterCall = generateGetterCallWithPrefixes(
                        statement,
                        field,
                        index,
                        statement.annotations.propertyNameGenerator,
                        false,
                        statement.src.tableAliases,
                        aliasPrefixes,
                    )
                    val comment = resultMappingHelper.buildFieldDebugComment(
                        field = field,
                        selectStatement = statement.src,
                        propertyNameGenerator = statement.annotations.propertyNameGenerator,
                        includeType = true,
                    )
                    if (comment.isNotEmpty()) {
                        line("// $comment")
                    }
                    val suffix = if (index == lastIndex) "" else ","
                    line("$propertyName = $getterCall$suffix")
                }
            }
        }
        builder.line(")")
        if (mapAdapterName != null) {
            builder.line("return $mapAdapterName($rawVarName)")
        }
        fnBld.addStatement(builder.build())
    }

    private fun lastNonDynamicIndex(statement: AnnotatedSelectStatement): Int {
        for (i in statement.fields.indices.reversed()) {
            if (!statement.fields[i].annotations.isDynamicField) {
                return i
            }
        }
        return -1
    }

    private fun addReadStatementResultProcessingForExecute(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement,
        namespace: String,
    ) {
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val returningFields = resolveExecuteReturningFields(statement)
        val builder = IndentedCodeBuilder()
        builder.line("return $resultType(")
        val lastIndex = returningFields.lastIndex
        builder.indent {
            returningFields.forEachIndexed { index, returningField ->
                val field = returningField.field
                val propertyName = returningField.propertyName
                val desiredType = selectFieldGenerator
                    .generateProperty(field, statement.annotations.propertyNameGenerator)
                    .type
                val getterCall = generateExecuteReturningGetter(field, desiredType, index, propertyName)
                val suffix = if (index == lastIndex) "" else ","
                line("$propertyName = $getterCall$suffix")
            }
        }
        builder.line(")")
        fnBld.addStatement(builder.build())
    }

}
