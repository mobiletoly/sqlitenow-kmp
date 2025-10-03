package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementUtils

/**
 * Emits the suspend execute helpers (SELECT, INSERT/UPDATE/DELETE) that sit on generated query classes.
 * Handles the shared statement preparation, execution and result-routing logic while delegating
 * collection-heavy cases back to the supplied collection mapping builder.
 */
internal class QueryExecuteEmitter(
    private val packageName: String,
    private val debug: Boolean,
    private val scaffolder: QueryFunctionScaffolder,
    private val adapterParameterEmitter: AdapterParameterEmitter,
    private val queryNamespaceName: (String) -> String,
    private val collectionMappingBuilder: (
        IndentedCodeBuilder,
        AnnotatedSelectStatement,
        String,
        String,
        String,
    ) -> Unit,
) {
    fun generateSelectQueryFunction(
        namespace: String,
        statement: AnnotatedSelectStatement,
        functionName: String,
    ): FunSpec {
        val className = statement.getDataClassName()
        val kdoc = when (functionName) {
            "executeAsList" -> "Executes the ${statement.name} SELECT query and returns results as a list."
            "executeAsOne" -> "Executes the ${statement.name} SELECT query and returns exactly one result. Throws an exception if no results are found."
            "executeAsOneOrNull" -> "Executes the ${statement.name} SELECT query and returns one result or null if no results are found."
            else -> "Executes the ${statement.name} SELECT query."
        }
        val fnBld = FunSpec.builder(functionName)
            .addModifiers(KModifier.SUSPEND)
            .addKdoc(kdoc)

        scaffolder.setupExecuteFunctionStructure(fnBld, statement, namespace, className)

        val resultType =
            SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        val returnType = when (functionName) {
            "executeAsList" -> ClassName("kotlin.collections", "List").parameterizedBy(resultType)
            "executeAsOne" -> resultType
            "executeAsOneOrNull" -> resultType.copy(nullable = true)
            else -> resultType
        }
        fnBld.returns(returnType)

        addSqlStatementProcessing(fnBld, statement, namespace, className, functionName)
        return fnBld.build()
    }

    fun generateExecuteQueryFunction(
        namespace: String,
        statement: AnnotatedExecuteStatement,
        functionName: String = if (statement.hasReturningClause()) "executeReturningList" else "execute",
    ): FunSpec {
        val className = statement.getDataClassName()
        val hasReturning = statement.hasReturningClause()
        val fnBld = FunSpec.builder(functionName)
            .addModifiers(KModifier.SUSPEND)
            .addKdoc("${if (hasReturning) "Executes and returns results from" else "Executes"} the ${statement.name} query.")

        scaffolder.setupExecuteFunctionStructure(fnBld, statement, namespace, className)

        if (hasReturning) {
            val resultClassName = if (statement.annotations.queryResult != null) {
                statement.annotations.queryResult!!
            } else {
                "${pascalize(namespace)}${className}Result"
            }
            val resultType = ClassName(packageName, resultClassName)
            when (functionName) {
                "executeReturningList" ->
                    fnBld.returns(ClassName("kotlin.collections", "List").parameterizedBy(resultType))
                "executeReturningOne" -> fnBld.returns(resultType)
                "executeReturningOneOrNull" -> fnBld.returns(resultType.copy(nullable = true))
                else -> fnBld.returns(resultType)
            }
        } else {
            fnBld.returns(Unit::class)
        }

        addSqlStatementProcessing(fnBld, statement, namespace, className, functionName)
        return fnBld.build()
    }

    private fun addSqlStatementProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val builder = IndentedCodeBuilder()
        builder.line(withContextHeader())
        builder.indent(by = 2) {
            prepareAndMaybeBindParamsLines(statement, capitalizedNamespace, className).forEach { line(it) }
            addSqlExecutionImplementationToBuilder(this, statement, namespace, className, functionName)
        }
        builder.line("}")
        fnBld.addStatement(builder.build())
    }

    private fun withContextHeader(): String =
        if (debug) "return conn.withContextAndTrace {" else "return withContext(conn.dispatcher) {"

    private fun prepareAndMaybeBindParamsLines(
        statement: AnnotatedStatement,
        capitalizedNamespace: String,
        className: String,
    ): List<String> {
        val lines = mutableListOf<String>()
        lines += "val sql = $capitalizedNamespace.$className.SQL"
        lines += "val statement = conn.prepare(sql)"
        val namedParameters = StatementUtils.getNamedParameters(statement)
        if (namedParameters.isNotEmpty()) {
            val params = mutableListOf("statement", "params")
            params += adapterParameterEmitter.parameterBindingAdapterNames(statement)
            lines += "$capitalizedNamespace.$className.bindStatementParams(${params.joinToString(", ")})"
        }
        return lines
    }

    private fun addSqlExecutionImplementationToBuilder(
        builder: IndentedCodeBuilder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        when (statement) {
            is AnnotatedSelectStatement -> addSelectExecutionImplementationToBuilder(
                builder,
                statement,
                namespace,
                className,
                functionName,
            )

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    addExecuteReturningStatementImplementationToBuilder(
                        builder,
                        statement,
                        namespace,
                        className,
                        functionName,
                    )
                } else {
                    addExecuteStatementImplementationToBuilder(builder)
                }
            }

            is AnnotatedCreateTableStatement, is AnnotatedCreateViewStatement -> {
                builder.line("TODO(\"Unimplemented\")")
            }
        }
    }

    private fun addSelectExecutionImplementationToBuilder(
        builder: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val paramsString = adapterParameterEmitter.buildJoinedReadParamsList(statement).joinToString(", ")
        builder.line("statement.use { statement ->")
        builder.indent(by = 2) {
            when (functionName) {
                "executeAsList" -> {
                    if (statement.hasCollectionMapping()) {
                        collectionMappingBuilder(this, statement, namespace, className, paramsString)
                    } else {
                        line("val results = mutableListOf<$resultType>()")
                        line("while (statement.step()) {")
                        indent { line("results.add($capitalizedNamespace.$className.readStatementResult($paramsString))") }
                        line("}")
                        line("results")
                    }
                }

                "executeAsOne" -> {
                    line("if (statement.step()) {")
                    indent { line("$capitalizedNamespace.$className.readStatementResult($paramsString)") }
                    line("} else {")
                    indent { line("throw IllegalStateException(\"Query returned no results, but exactly one result was expected\")") }
                    line("}")
                }

                "executeAsOneOrNull" -> {
                    line("if (statement.step()) {")
                    indent { line("$capitalizedNamespace.$className.readStatementResult($paramsString)") }
                    line("} else {")
                    indent { line("null") }
                    line("}")
                }
            }
        }
        builder.line("}")
    }

    private fun addExecuteStatementImplementationToBuilder(builder: IndentedCodeBuilder) {
        builder.line("statement.use { statement ->")
        builder.indent(by = 2) { line("statement.step()") }
        builder.line("}")
    }

    private fun addExecuteReturningStatementImplementationToBuilder(
        builder: IndentedCodeBuilder,
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val paramsString = adapterParameterEmitter.buildExecuteReadParamsList(statement).joinToString(", ")
        val resultTypeString = SharedResultTypeUtils.createResultTypeStringForExecute(namespace, statement)
        when (functionName) {
            "executeReturningList" -> {
                builder.line("statement.use { statement ->")
                builder.indent(by = 2) {
                    line("val results = mutableListOf<$resultTypeString>()")
                    line("while (statement.step()) {")
                    indent(by = 2) {
                        line("results.add($capitalizedNamespace.$className.readStatementResult($paramsString))")
                    }
                    line("}")
                    line("results")
                }
                builder.line("}")
            }

            "executeReturningOne" -> {
                builder.line("statement.use { statement ->")
                builder.indent(by = 2) {
                    line("if (statement.step()) {")
                    indent(by = 2) {
                        line("$capitalizedNamespace.$className.readStatementResult($paramsString)")
                    }
                    line("} else {")
                    indent(by = 2) {
                        line("throw IllegalStateException(\"Statement with RETURNING returned no results, but exactly one result was expected\")")
                    }
                    line("}")
                }
                builder.line("}")
            }

            "executeReturningOneOrNull" -> {
                builder.line("statement.use { statement ->")
                builder.indent(by = 2) {
                    line("if (statement.step()) {")
                    indent(by = 2) {
                        line("$capitalizedNamespace.$className.readStatementResult($paramsString)")
                    }
                    line("} else {")
                    indent(by = 2) {
                        line("null")
                    }
                    line("}")
                }
                builder.line("}")
            }

            else -> {
                builder.line("statement.use { statement ->")
                builder.indent(by = 2) {
                    line("if (statement.step()) {")
                    indent(by = 2) {
                        line("$capitalizedNamespace.$className.readStatementResult($paramsString)")
                    }
                    line("} else {")
                    indent(by = 2) {
                        line("throw IllegalStateException(\"Statement with RETURNING returned no results\")")
                    }
                    line("}")
                }
                builder.line("}")
            }
        }
    }
}
