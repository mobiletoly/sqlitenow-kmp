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
