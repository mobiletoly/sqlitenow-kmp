package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.context.TypeMapping
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils

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
    private val typeMapping: TypeMapping,
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
    private val generateDynamicFieldMappingFromJoined: (
        AnnotatedSelectStatement.Field,
        AnnotatedSelectStatement,
        String,
        String?,
        Int,
    ) -> String,
    private val addReadJoinedStatementResultProcessing: (FunSpec.Builder, AnnotatedSelectStatement, String) -> Unit,
    private val createSelectLikeFieldsFromExecuteReturning: (AnnotatedExecuteStatement) -> List<AnnotatedSelectStatement.Field>,
    private val createJoinedResultTypeName: (String, AnnotatedSelectStatement) -> ClassName,
) {
    fun generateReadStatementResultFunction(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): FunSpec {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")
        scaffolder.setupStatementFunctionStructure(
            fnBld = fnBld,
            statement = statement,
            namespace = namespace,
            className = className,
            includeParamsParameter = false,
            adapterType = QueryFunctionScaffolder.AdapterType.RESULT_CONVERSION,
        )
        val resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        fnBld.returns(resultType)
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
        scaffolder.setupStatementFunctionStructure(
            fnBld = fnBld,
            statement = statement,
            namespace = namespace,
            className = className,
            includeParamsParameter = false,
            adapterType = QueryFunctionScaffolder.AdapterType.RESULT_CONVERSION,
        )
        val resultType = createJoinedResultTypeName(namespace, statement)
        fnBld.returns(resultType)
        addReadJoinedStatementResultProcessing(fnBld, statement, namespace)
        return fnBld.build()
    }

    fun generateReadStatementResultFunctionForExecute(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ): FunSpec {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")
        scaffolder.setupStatementFunctionStructure(
            fnBld = fnBld,
            statement = statement,
            namespace = namespace,
            className = className,
            includeParamsParameter = false,
            adapterType = QueryFunctionScaffolder.AdapterType.RESULT_CONVERSION,
        )

        val resultClassName = if (statement.annotations.queryResult != null) {
            statement.annotations.queryResult!!
        } else {
            "${pascalize(namespace)}${className}Result"
        }
        val resultType = ClassName(packageName, resultClassName)
        fnBld.returns(resultType)

        addReadStatementResultProcessingForExecute(fnBld, statement, namespace)
        return fnBld.build()
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
        val paramsString = buildJoinedReadParams(statement)
        val transformationCall = buildString {
            append("    val joinedData = $capitalizedNamespace.$className.readJoinedStatementResult($paramsString)\n")
            append("return $resultType(\n")
            resultMappingHelper.addJoinedToMappedTransformation(
                builder = this,
                statement = statement,
                dynamicFieldMapper = { field, stmt, sourceVar, rowsVar, baseIndent ->
                    generateDynamicFieldMappingFromJoined(field, stmt, sourceVar, rowsVar, baseIndent)
                },
            )
            append(")")
        }
        fnBld.addStatement(transformationCall)
    }

    private fun buildJoinedReadParams(statement: AnnotatedSelectStatement): String {
        val params = mutableListOf("statement")
        params += adapterParameterEmitter.resultConversionAdapterNames(statement)
        return params.joinToString(", ")
    }

    private fun addReadStatementResultProcessingDirect(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String,
    ) {
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val aliasPrefixes = adapterConfig.collectAliasPrefixesForSelect(statement)
        val constructorCall = buildString {
            append("return $resultType(\n")
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
                        append("  // $comment\n")
                    }
                    append("  $propertyName = $getterCall,\n")
                }
            }
            append(")")
        }
        fnBld.addStatement(constructorCall)
    }

    private fun addReadStatementResultProcessingForExecute(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement,
        namespace: String,
    ) {
        val resultType = SharedResultTypeUtils.createResultTypeStringForExecute(namespace, statement)
        val selectLikeFields = createSelectLikeFieldsFromExecuteReturning(statement)
        val constructorCall = buildString {
            append("return $resultType(\n")
            selectLikeFields.forEachIndexed { index, field ->
                val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(field.src.fieldName)
                val desiredType = selectFieldGenerator
                    .generateProperty(field, statement.annotations.propertyNameGenerator)
                    .type
                val getterCall = if (isCustomKotlinType(desiredType) || adapterConfig.hasAdapterAnnotation(field)) {
                    val visibleName = field.src.fieldName
                    val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(visibleName)
                    val adapterParamName = adapterConfig.getOutputAdapterFunctionName(columnName)
                    val inputGetter = typeMapping
                        .getGetterCall(
                            SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(field.src.dataType),
                            index,
                        )
                        .replace("stmt", "statement")
                    if (desiredType.isNullable) {
                        "if (statement.isNull($index)) $adapterParamName(null) else $adapterParamName($inputGetter)"
                    } else {
                        "$adapterParamName($inputGetter)"
                    }
                } else {
                    val baseGetterCall = typeMapping
                        .getGetterCall(desiredType.copy(nullable = false), index)
                        .replace("stmt", "statement")
                    if (desiredType.isNullable) {
                        "if (statement.isNull($index)) null else $baseGetterCall"
                    } else {
                        baseGetterCall
                    }
                }
                append("  $propertyName = $getterCall")
                if (index < selectLikeFields.size - 1) {
                    append(",")
                }
                append("\n")
            }
            append(")")
        }
        fnBld.addStatement(constructorCall)
    }

    private fun isCustomKotlinType(type: TypeName): Boolean {
        return !typeMapping.isStandardKotlinType(type.toString())
    }
}
