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
package dev.goquick.sqlitenow.gradle.database

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterNameResolver
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.util.lowercaseFirst
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import dev.goquick.sqlitenow.gradle.util.queryNamespaceClassName
import dev.goquick.sqlitenow.gradle.util.queryParamsTypeName
import dev.goquick.sqlitenow.gradle.processing.SharedResultManager
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementUtils
import java.io.File

/**
 * Generates a high-level database class that simplifies usage of generated data structures
 * and queries.
 */
class DatabaseCodeGenerator(
    private val nsWithStatements: Map<String, List<AnnotatedStatement>>,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    createViewStatements: List<AnnotatedCreateViewStatement>,
    private val packageName: String,
    private val outputDir: File,
    private val databaseClassName: String,
    private val debug: Boolean = false,
    private val oversqlite: Boolean = false,
) {
    private val columnLookup = ColumnLookup(createTableStatements, createViewStatements)
    private val adapterConfig = AdapterConfig(
        columnLookup = columnLookup,
        createTableStatements = createTableStatements,
        packageName = packageName
    )
    private val tableLookup = CaseInsensitiveMap(createTableStatements.map { it.src.tableName to it })
    private val sharedResultManager = SharedResultManager()
    private val adapterNameResolver = AdapterParameterNameResolver()
    private val adapterPlanner = DatabaseAdapterPlanner(
        nsWithStatements = nsWithStatements,
        adapterConfig = adapterConfig,
        tableLookup = tableLookup,
        sharedResultManager = sharedResultManager,
    )
    private val oversqliteEmitter = DatabaseOversqliteEmitter(
        databaseClassName = databaseClassName,
        createTableStatements = createTableStatements,
        oversqlite = oversqlite,
    )

    private fun adapterClassNameFor(namespace: String): String =
        adapterPlanner.adapterClassNameFor(namespace)

    private fun adapterPropertyNameFor(namespace: String): String =
        adapterPlanner.adapterPropertyNameFor(namespace)

    private fun queryNamespaceName(namespace: String): String = queryNamespaceClassName(namespace)

    private fun routerClassNameFor(namespace: String): String =
        adapterPlanner.baseNameForNamespace(namespace) + "Router"

    private fun routerPropertyNameFor(namespace: String): String =
        adapterPlanner.baseNameForNamespace(namespace).lowercaseFirst()

    /**
     * Generates the main database class file.
     */
    fun generateDatabaseClass() {
        val fileBuilder = FileSpec.builder(packageName, databaseClassName)
            .addFileComment("Generated database class with unified adapter management")
            .addFileComment("Do not modify this file manually")
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )

        fileBuilder.addImport("dev.goquick.sqlitenow.core", "DatabaseMigrations")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "SqliteNowDatabase")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "SelectRunners")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "ExecuteStatement")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "ExecuteReturningStatement")
        fileBuilder.addImport("kotlinx.coroutines.flow", "Flow")
        if (debug) {
            fileBuilder.addImport("dev.goquick.sqlitenow.common", "sqliteNowLogger")
        }

        val databaseClass = generateMainDatabaseClass()
        fileBuilder.addType(databaseClass)
        fileBuilder.build().writeTo(outputDir)
    }

    /** Generates the main database class with constructor and router properties. */
    private fun generateMainDatabaseClass(): TypeSpec {
        val classBuilder = TypeSpec.classBuilder(databaseClassName)
            .addModifiers(KModifier.PUBLIC)
            .superclass(ClassName("dev.goquick.sqlitenow.core", "SqliteNowDatabase"))

        val adaptersByNamespace = adapterPlanner.collectAdaptersByNamespace()

        // Compute best provider per adapter function and filter adapters accordingly
        val bestProviderForFunction: Map<String, String> = adapterPlanner.computeBestProviders(adaptersByNamespace)
        val namespacesWithFilteredAdapters = adaptersByNamespace
            .mapValues { (ns, adapters) ->
                adapters.filter {
                    bestProviderForFunction[adapterPlanner.baseFunctionKey(
                        it.functionName
                    )] == ns
                }
            }
            .filterValues { it.isNotEmpty() }

        // Build constructor and private adapter properties
        val ctor = buildConstructorWithAdapters(namespacesWithFilteredAdapters)
        classBuilder.primaryConstructor(ctor)
        // Superclass constructor call
        classBuilder.addSuperclassConstructorParameter("dbName = dbName")
        classBuilder.addSuperclassConstructorParameter("migration = migration")
        classBuilder.addSuperclassConstructorParameter("debug = debug")

        // Add private adapter properties matching constructor params
        addAdapterPrivateProperties(classBuilder, namespacesWithFilteredAdapters.keys)

        // Add router properties (one per namespace)
        addRouterProperties(classBuilder)

        // Generate adapter wrapper classes (best providers only)
        generateAdapterWrapperClasses(classBuilder, namespacesWithFilteredAdapters)

        // Generate router classes
        nsWithStatements.forEach { (namespace, statements) ->
            val routerClass = generateRouterClass(namespace, statements, adaptersByNamespace)
            classBuilder.addType(routerClass)
        }

        oversqliteEmitter.addOversqliteSupport(classBuilder)

        return classBuilder.build()
    }

    private fun buildConstructorWithAdapters(
        namespacesWithAdapters: Map<String, List<UniqueAdapter>>
    ): FunSpec {
        val b = FunSpec.constructorBuilder()
            .addParameter("dbName", String::class)
            .addParameter(
                "migration",
                ClassName("dev.goquick.sqlitenow.core", "DatabaseMigrations")
            )
            .addParameter(
                ParameterSpec.builder("debug", Boolean::class).defaultValue("%L", debug).build()
            )
        namespacesWithAdapters.keys.forEach { ns ->
            val adapterClassName = adapterClassNameFor(ns)
            val adapterPropName = adapterPropertyNameFor(ns)
            b.addParameter(adapterPropName, ClassName("", adapterClassName))
        }
        return b.build()
    }

    private fun addAdapterPrivateProperties(
        classBuilder: TypeSpec.Builder,
        namespaces: Set<String>
    ) {
        namespaces.forEach { ns ->
            val adapterClassName = adapterClassNameFor(ns)
            val adapterPropName = adapterPropertyNameFor(ns)
            classBuilder.addProperty(
                PropertySpec.builder(adapterPropName, ClassName("", adapterClassName))
                    .addModifiers(KModifier.PRIVATE)
                    .initializer(adapterPropName)
                    .build()
            )
        }
    }

    private fun addRouterProperties(classBuilder: TypeSpec.Builder) {
        nsWithStatements.keys.forEach { namespace ->
            val routerClassName = routerClassNameFor(namespace)
            val routerPropName = routerPropertyNameFor(namespace)
            classBuilder.addProperty(
                PropertySpec.builder(routerPropName, ClassName("", routerClassName))
                    .initializer("$routerClassName(ref = this)")
                    .build()
            )
        }
    }

    private fun generateAdapterWrapperClasses(
        classBuilder: TypeSpec.Builder,
        namespacesWithAdapters: Map<String, List<UniqueAdapter>>
    ) {
        namespacesWithAdapters.forEach { (namespace, adapters) ->
            val adapterClass = generateAdapterClass(namespace, adapters)
            classBuilder.addType(adapterClass)
        }
    }

    // ---------- Object expression helpers (Select/Execute runners) ----------

    private fun resolveAdapterProvider(
        config: AdapterConfig.ParamConfig,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): Pair<String, String> {
        val providerNs =
            adapterPlanner.findBestProviderByName(config.adapterFunctionName, adaptersByNamespace)?.first
                ?: namespace
        val providerProp = adapterPropertyNameFor(providerNs)
        val actualAdapterName =
            adapterPlanner.findAdapterName(providerNs, config.adapterFunctionName, config.inputType, adaptersByNamespace)
        return providerProp to actualAdapterName
    }

    private fun buildCommonParamsLines(
        hasParams: Boolean,
        statementAdapters: List<AdapterConfig.ParamConfig>,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): List<String> {
        val lines = mutableListOf<String>()
        lines += "conn = ref.connection()"
        if (hasParams) lines += "params = params"

        // Use the same canonicalized parameter names that query execution functions expect
        val chosenParamNames = adapterNameResolver.chooseAdapterParamNames(statementAdapters)

        statementAdapters.forEach { config ->
            val (providerProp, adapterName) = resolveAdapterProvider(
                config,
                namespace,
                adaptersByNamespace
            )
            val canonicalParamName = chosenParamNames[config] ?: config.adapterFunctionName
            lines += "$canonicalParamName = ref.$providerProp.$adapterName"
        }
        return lines
    }

    /**
     * Collects output adapter configurations for EXECUTE statements with RETURNING clause.
     * This is similar to how SELECT statements collect output adapters.
     */
    private fun collectOutputAdaptersForExecuteReturning(statement: AnnotatedExecuteStatement): List<AdapterConfig.ParamConfig> {
        return adapterConfig.collectExecuteReturningOutputParamConfigs(statement)
    }

    /** Generates an adapter wrapper class for a specific namespace. */
    private fun generateAdapterClass(namespace: String, adapters: List<UniqueAdapter>): TypeSpec {
        val adapterClassName = adapterClassNameFor(namespace)
        val classBuilder = TypeSpec.classBuilder(adapterClassName)
            .addModifiers(KModifier.PUBLIC, KModifier.DATA)

        // Add constructor with adapter parameters
        val constructorBuilder = FunSpec.constructorBuilder()

        adapters.forEach { adapter ->
            val paramSpec = adapter.toParameterSpec()
            constructorBuilder.addParameter(paramSpec)

            // Add as property
            val propertySpec =
                PropertySpec.builder(adapter.functionName, adapter.toParameterSpec().type)
                    .initializer(adapter.functionName)
                    .build()
            classBuilder.addProperty(propertySpec)
        }

        classBuilder.primaryConstructor(constructorBuilder.build())
        return classBuilder.build()
    }

    /** Generates a router class for a specific namespace. */
    private fun generateRouterClass(
        namespace: String,
        statements: List<AnnotatedStatement>,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): TypeSpec {
        val routerClassName = routerClassNameFor(namespace)
        val classBuilder = TypeSpec.classBuilder(routerClassName)
            .addModifiers(KModifier.PUBLIC)

        // Add constructor with database reference
        val constructorBuilder = FunSpec.constructorBuilder()
            .addParameter("ref", ClassName("", databaseClassName))
        classBuilder.primaryConstructor(constructorBuilder.build())

        // Add ref property
        val refProperty = PropertySpec.builder("ref", ClassName("", databaseClassName))
            .initializer("ref")
            .addModifiers(KModifier.PRIVATE)
            .build()
        classBuilder.addProperty(refProperty)

        // Generate methods for each statement
        statements.forEach { statement ->
            when (statement) {
                is AnnotatedSelectStatement -> {
                    // Generate SelectRunners object for SELECT statements
                    classBuilder.addProperty(
                        generateSelectRunnersProperty(
                            statement,
                            namespace,
                            adaptersByNamespace
                        )
                    )
                }

                is AnnotatedExecuteStatement -> {
                    val hasParams = StatementUtils.getNamedParameters(statement).isNotEmpty()
                    if (statement.hasReturningClause()) {
                        if (hasParams) {
                            classBuilder.addProperty(
                                generateExecuteReturningStatementProperty(
                                    statement,
                                    namespace,
                                    adaptersByNamespace
                                )
                            )
                        } else {
                            generateExecuteReturningFunctionsWithoutParams(
                                statement = statement,
                                namespace = namespace,
                                adaptersByNamespace = adaptersByNamespace
                            ).forEach { functionSpec ->
                                classBuilder.addFunction(functionSpec)
                            }
                        }
                    } else {
                        if (hasParams) {
                            classBuilder.addProperty(
                                generateExecuteStatementProperty(
                                    statement,
                                    namespace,
                                    adaptersByNamespace
                                )
                            )
                        } else {
                            classBuilder.addFunction(
                                generateExecuteFunctionWithoutParams(
                                    statement = statement,
                                    namespace = namespace,
                                    adaptersByNamespace = adaptersByNamespace
                                )
                            )
                        }
                    }
                }

                else -> {
                    // CREATE TABLE/VIEW statements don't need router methods
                }
            }
        }

        return classBuilder.build()
    }

    /** Generates a SelectRunners property for SELECT statements. */
    private fun generateSelectRunnersProperty(
        statement: AnnotatedSelectStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.lowercaseFirst()

        // Determine result type (handles shared results)
        val resultType = SharedResultTypeUtils.createPublicResultTypeName(packageName, namespace, statement)

        // Check if statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        val hasParams = namedParameters.isNotEmpty()

        // Create the property type
        val propertyType = if (hasParams) {
            // Function type: (Params) -> SelectRunners<ResultType>
            val paramsType = ClassName(packageName, queryNamespaceName(namespace))
                .nestedClass(className)
                .nestedClass("Params")
            val selectRunnersType = ClassName("dev.goquick.sqlitenow.core", "SelectRunners")
                .parameterizedBy(resultType)
            LambdaTypeName.get(parameters = arrayOf(paramsType), returnType = selectRunnersType)
        } else {
            // Direct SelectRunners<ResultType> type
            ClassName("dev.goquick.sqlitenow.core", "SelectRunners")
                .parameterizedBy(resultType)
        }

        // Generate the object expression implementing SelectRunners
        val objectExpression = generateSelectRunnersObjectExpression(
            statement,
            namespace,
            className,
            hasParams,
            adaptersByNamespace
        )

        return PropertySpec.builder(propertyName, propertyType)
            .initializer(objectExpression)
            .build()
    }

    /** Generates the object expression that implements SelectRunners interface. */
    private fun generateSelectRunnersObjectExpression(
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        hasParams: Boolean,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val paramLines = buildCommonParamsLines(
            hasParams = hasParams,
            statementAdapters = statementAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        val b = IndentedCodeBuilder()
        val resultTypeName = SharedResultTypeUtils.createPublicResultTypeString(namespace, statement)
        if (hasParams) {
            b.line("{ params ->")
        }
        b.line("object : SelectRunners<$resultTypeName> {")
        // asList
        b.indent(by = 2) {
            b.line("override suspend fun asList() = $capitalizedNamespace.$className.executeAsList(")
            b.indent(by = 2) {
                paramLines.forEachIndexed { idx, line ->
                    val suffix = if (idx < paramLines.lastIndex) "," else ""
                    b.line("$line$suffix")
                }
            }
            b.line(")")
            b.line("")

            b.line("override suspend fun asOne() = $capitalizedNamespace.$className.executeAsOne(")
            b.indent(by = 2) {
                paramLines.forEachIndexed { idx, line ->
                    val suffix = if (idx < paramLines.lastIndex) "," else ""
                    b.line("$line$suffix")
                }
            }
            b.line(")")
            b.line("")

            b.line("override suspend fun asOneOrNull() = $capitalizedNamespace.$className.executeAsOneOrNull(")
            b.indent(by = 2) {
                paramLines.forEachIndexed { idx, line ->
                    val suffix = if (idx < paramLines.lastIndex) "," else ""
                    b.line("$line$suffix")
                }
            }
            b.line(")")
            b.line("")

            // asFlow
            b.line("override fun asFlow() = ref.createReactiveQueryFlow(")
            b.indent(by = 2) {
                b.line("affectedTables = $capitalizedNamespace.$className.affectedTables,")
                b.line("queryExecutor = {")
                b.indent(by = 2) {
                    b.line("$capitalizedNamespace.$className.executeAsList(")
                    b.indent(by = 2) {
                        paramLines.forEachIndexed { idx, line ->
                            val suffix = if (idx < paramLines.lastIndex) "," else ""
                            b.line("$line$suffix")
                        }
                    }
                    b.line(")")
                }
                b.line("}")
            }
            b.line(")")
        }
        b.line("}")

        if (hasParams) {
            b.line("}")
        }

        return b.build()
    }

    private fun generateExecuteStatementProperty(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.lowercaseFirst()
        val propertyType = ClassName("dev.goquick.sqlitenow.core", "ExecuteStatement")
            .parameterizedBy(queryParamsTypeName(packageName, queryNamespaceName(namespace), className))

        val initializer = buildExecuteStatementInitializer(
            statement = statement,
            namespace = namespace,
            className = className,
            adaptersByNamespace = adaptersByNamespace,
        )

        return buildExecuteProperty(propertyName, propertyType, initializer)
    }

    private fun buildExecuteStatementInitializer(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): String {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val paramLines = buildCommonParamsLines(
            hasParams = true,
            statementAdapters = statementAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        return buildExecuteInitializerExpression(
            constructorName = "ExecuteStatement",
            paramLines = paramLines,
            affectedTablesOwner = "$capitalizedNamespace.$className",
            blocks = listOf(
                ExecuteInitializerBlock(
                    name = "executeBlock",
                    invocationStart = "$capitalizedNamespace.$className.execute",
                )
            ),
        )
    }

    private fun generateExecuteFunctionWithoutParams(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): FunSpec {
        val className = statement.getDataClassName()
        val functionName = className.lowercaseFirst()
        val capitalizedNamespace = queryNamespaceName(namespace)
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val paramLines = buildCommonParamsLines(
            hasParams = false,
            statementAdapters = statementAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        val body = IndentedCodeBuilder()
        body.addInvocationAndNotify(
            invocationStart = "$capitalizedNamespace.$className.execute",
            paramLines = paramLines,
            affectedTablesOwner = "$capitalizedNamespace.$className",
        )

        return FunSpec.builder(functionName)
            .addModifiers(KModifier.PUBLIC, KModifier.SUSPEND)
            .addKdoc("Executes the ${statement.name} statement.")
            .addCode(body.build())
            .build()
    }

    private fun generateExecuteReturningStatementProperty(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.lowercaseFirst()

        val paramsType = queryParamsTypeName(packageName, queryNamespaceName(namespace), className)
        val resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        val propertyType = ClassName("dev.goquick.sqlitenow.core", "ExecuteReturningStatement")
            .parameterizedBy(paramsType, resultType)

        val initializer = buildExecuteReturningInitializer(
            statement = statement,
            namespace = namespace,
            className = className,
            adaptersByNamespace = adaptersByNamespace,
        )

        return buildExecuteProperty(propertyName, propertyType, initializer)
    }

    private fun buildExecuteProperty(
        propertyName: String,
        propertyType: TypeName,
        initializer: String,
    ): PropertySpec {
        return PropertySpec.builder(propertyName, propertyType)
            .initializer(initializer)
            .build()
    }

    private fun buildExecuteReturningInitializer(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): String {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val inputAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val outputAdapters = collectOutputAdaptersForExecuteReturning(statement)
        val allAdapters = inputAdapters + outputAdapters
        val paramLines = buildCommonParamsLines(
            hasParams = true,
            statementAdapters = allAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )
        val resultTypeString = SharedResultTypeUtils.createResultTypeString(namespace, statement)

        return buildExecuteInitializerExpression(
            constructorName = "ExecuteReturningStatement",
            paramLines = paramLines,
            affectedTablesOwner = "$capitalizedNamespace.$className",
            blocks = listOf(
                ExecuteInitializerBlock(
                    name = "listBlock",
                    invocationStart = "val result = $capitalizedNamespace.$className.executeReturningList",
                    resultLine = "result",
                ),
                ExecuteInitializerBlock(
                    name = "oneBlock",
                    invocationStart = "val result = $capitalizedNamespace.$className.executeReturningOne",
                    resultLine = "result",
                ),
                ExecuteInitializerBlock(
                    name = "oneOrNullBlock",
                    invocationStart = "val result = $capitalizedNamespace.$className.executeReturningOneOrNull",
                    resultLine = "result",
                ),
            ),
        )
    }

    private fun generateExecuteReturningFunctionsWithoutParams(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): List<FunSpec> {
        val className = statement.getDataClassName()
        val baseName = className.lowercaseFirst()
        val capitalizedNamespace = queryNamespaceName(namespace)

        val inputAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val outputAdapters = collectOutputAdaptersForExecuteReturning(statement)
        val allAdapters = inputAdapters + outputAdapters
        val paramLines = buildCommonParamsLines(
            hasParams = false,
            statementAdapters = allAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        val resultTypeName = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        val listTypeName = ClassName("kotlin.collections", "List").parameterizedBy(resultTypeName)
        val resultTypeNullable = resultTypeName.copy(nullable = true)

        fun createBody(invocation: String): String {
            val bodyBuilder = IndentedCodeBuilder()
            bodyBuilder.addInvocationAndNotify(
                invocationStart = "val result = $capitalizedNamespace.$className.$invocation",
                paramLines = paramLines,
                affectedTablesOwner = "$capitalizedNamespace.$className",
                resultLine = "return result",
            )
            return bodyBuilder.build()
        }

        val listFunction = FunSpec.builder(baseName)
            .addModifiers(KModifier.PUBLIC, KModifier.SUSPEND)
            .returns(listTypeName)
            .addKdoc("Executes the ${statement.name} statement and returns all rows.")
            .addCode(createBody("executeReturningList"))
            .build()

        val oneFunction = FunSpec.builder("${baseName}One")
            .addModifiers(KModifier.PUBLIC, KModifier.SUSPEND)
            .returns(resultTypeName)
            .addKdoc("Executes the ${statement.name} statement and returns exactly one row.")
            .addCode(createBody("executeReturningOne"))
            .build()

        val oneOrNullFunction = FunSpec.builder("${baseName}OneOrNull")
            .addModifiers(KModifier.PUBLIC, KModifier.SUSPEND)
            .returns(resultTypeNullable)
            .addKdoc("Executes the ${statement.name} statement and returns one row or null when none match.")
            .addCode(createBody("executeReturningOneOrNull"))
            .build()

        return listOf(listFunction, oneFunction, oneOrNullFunction)
    }

    private data class ExecuteInitializerBlock(
        val name: String,
        val invocationStart: String,
        val resultLine: String? = null,
    )

    private fun buildExecuteInitializerExpression(
        constructorName: String,
        paramLines: List<String>,
        affectedTablesOwner: String,
        blocks: List<ExecuteInitializerBlock>,
    ): String {
        val b = IndentedCodeBuilder()
        b.line("$constructorName(")
        b.indent {
            blocks.forEachIndexed { idx, block ->
                b.line("${block.name} = { params ->")
                b.indent {
                    b.addInvocationAndNotify(
                        invocationStart = block.invocationStart,
                        paramLines = paramLines,
                        affectedTablesOwner = affectedTablesOwner,
                        resultLine = block.resultLine,
                    )
                }
                val suffix = if (idx < blocks.lastIndex) "," else ""
                b.line("}$suffix")
            }
        }
        b.line(")")
        return b.build()
    }

    private fun IndentedCodeBuilder.addInvocationAndNotify(
        invocationStart: String,
        paramLines: List<String>,
        affectedTablesOwner: String,
        resultLine: String? = null,
    ) {
        line("$invocationStart(")
        indent {
            addParamLines(paramLines)
        }
        line(")")
        addNotifyTablesChanged(affectedTablesOwner)
        if (resultLine != null) {
            line(resultLine)
        }
    }

    private fun IndentedCodeBuilder.addParamLines(paramLines: List<String>) {
        paramLines.forEachIndexed { idx, paramLine ->
            val suffix = if (idx < paramLines.lastIndex) "," else ""
            line("$paramLine$suffix")
        }
    }

    private fun IndentedCodeBuilder.addNotifyTablesChanged(affectedTablesOwner: String) {
        line("// Notify listeners that tables have changed")
        if (debug) {
            line("sqliteNowLogger.d { \"notifyTablesChanged -> \" + $affectedTablesOwner.affectedTables.joinToString(\", \") }")
        }
        line("ref.notifyTablesChanged($affectedTablesOwner.affectedTables)")
    }

}
