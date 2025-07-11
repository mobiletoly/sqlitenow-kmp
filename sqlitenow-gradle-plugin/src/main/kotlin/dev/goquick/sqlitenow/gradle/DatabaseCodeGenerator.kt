package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import org.gradle.internal.extensions.stdlib.capitalized
import java.io.File

/**
 * Generates a high-level database class that simplifies usage of generated data structures and queries.
 */
class DatabaseCodeGenerator(
    private val nsWithStatements: Map<String, List<AnnotatedStatement>>,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    createViewStatements: List<AnnotatedCreateViewStatement>,
    private val packageName: String,
    private val outputDir: File,
    private val databaseClassName: String,
) {
    private val adapterConfig = AdapterConfig(
        columnLookup = ColumnLookup(createTableStatements, createViewStatements),
        createTableStatements = createTableStatements,
        packageName = packageName
    )
    private val sharedResultManager = SharedResultManager()

    /** Data class representing a unique adapter with its function signature. */
    data class UniqueAdapter(
        val functionName: String,
        val inputType: TypeName,
        val outputType: TypeName,
        val isNullable: Boolean
    ) {
        fun toParameterSpec(): ParameterSpec {
            val lambdaType = LambdaTypeName.get(
                parameters = arrayOf(inputType),
                returnType = outputType
            )
            return ParameterSpec.builder(functionName, lambdaType).build()
        }

        /** Create a signature key for deduplication that considers the actual TypeName structure */
        fun signatureKey(): String {
            return "${inputType}__$outputType"
        }
    }

    /**
     * Collects and deduplicates adapters per namespace.
     */
    private fun collectAdaptersByNamespace(): Map<String, List<UniqueAdapter>> {
        val adaptersByNamespace = mutableMapOf<String, MutableList<UniqueAdapter>>()

        // Collect adapters for each namespace separately
        nsWithStatements.forEach { (namespace, statements) ->
            val namespaceAdapters = mutableListOf<UniqueAdapter>()
            val processedSharedResults = mutableSetOf<String>()

            // First, register all shared results for this namespace
            statements.filterIsInstance<AnnotatedSelectStatement>().forEach { statement ->
                sharedResultManager.registerSharedResult(statement, namespace)
            }

            statements.forEach { statement ->
                // For SELECT statements with shared results, only collect adapters once per shared result
                if (statement is AnnotatedSelectStatement && statement.annotations.sharedResult != null) {
                    val sharedResultKey = "${namespace}.${statement.annotations.sharedResult}"
                    if (sharedResultKey in processedSharedResults) {
                        // Skip - we already processed adapters for this shared result
                        return@forEach
                    }
                    processedSharedResults.add(sharedResultKey)
                }

                // Collect adapters for this statement
                val statementAdapters = adapterConfig.collectAllParamConfigs(statement)
                statementAdapters.forEach { config ->
                    namespaceAdapters.add(
                        UniqueAdapter(
                            functionName = config.adapterFunctionName,
                            inputType = config.inputType,
                            outputType = config.outputType,
                            isNullable = config.isNullable
                        )
                    )
                }
            }

            // Deduplicate adapters within this namespace
            val deduplicatedAdapters = deduplicateAdaptersForNamespace(namespaceAdapters)
            adaptersByNamespace[namespace] = deduplicatedAdapters.toMutableList()
        }

        return adaptersByNamespace
    }

    /**
     * Deduplicates adapters within a single namespace.
     */
    private fun deduplicateAdaptersForNamespace(adapters: List<UniqueAdapter>): List<UniqueAdapter> {
        val adaptersByName = adapters.groupBy { it.functionName }
        val result = mutableListOf<UniqueAdapter>()

        adaptersByName.forEach { (_, adapterList) ->
            val uniqueSignatures = adapterList.distinctBy { it.signatureKey() }

            if (uniqueSignatures.size == 1) {
                // No conflict - add single adapter
                result.add(uniqueSignatures.first())
            } else {
                // Conflict detected - generate unique parameter names by appending type suffix
                uniqueSignatures.forEach { adapter ->
                    val inputTypeName = adapter.inputType.toString().substringAfterLast('.')
                    val uniqueName = "${adapter.functionName}For${inputTypeName.replaceFirstChar { it.uppercase() }}"
                    result.add(adapter.copy(functionName = uniqueName))
                }
            }
        }

        return result
    }

    /**
     * Simple helper function to find the correct adapter name.
     * Uses direct lookup by expected function name pattern.
     */
    private fun findAdapterName(
        namespace: String,
        expectedFunctionName: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val deduplicatedAdapters = adaptersByNamespace[namespace] ?: return expectedFunctionName

        // Look for exact match first
        val exactMatch = deduplicatedAdapters.find { it.functionName == expectedFunctionName }
        if (exactMatch != null) {
            return exactMatch.functionName
        }

        // Look for renamed version (e.g., phoneToSqlValueForString)
        val renamedMatch = deduplicatedAdapters.find {
            it.functionName.startsWith(expectedFunctionName + "For")
        }

        return renamedMatch?.functionName ?: expectedFunctionName
    }

    /**
     * Generates the main database class file.
     */
    fun generateDatabaseClass() {
        val fileBuilder = FileSpec.builder(packageName, databaseClassName)
            .addFileComment("Generated database class with unified adapter management")
            .addFileComment("Do not modify this file manually")

        fileBuilder.addImport("dev.goquick.sqlitenow.core", "DatabaseMigrations")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "SqliteNowDatabase")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "SelectRunners")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "ExecuteRunners")
        fileBuilder.addImport("kotlinx.coroutines.flow", "Flow")

        val databaseClass = generateMainDatabaseClass()
        fileBuilder.addType(databaseClass)
        fileBuilder.build().writeTo(outputDir)
    }

    /** Generates the main database class with constructor and router properties. */
    private fun generateMainDatabaseClass(): TypeSpec {
        val classBuilder = TypeSpec.classBuilder(databaseClassName)
            .addModifiers(KModifier.PUBLIC)
            .superclass(ClassName("dev.goquick.sqlitenow.core", "SqliteNowDatabase"))

        val adaptersByNamespace = collectAdaptersByNamespace()

        // Filter out namespaces with no adapters
        val namespacesWithAdapters = adaptersByNamespace
            .filter { (_, adapters) -> adapters.isNotEmpty() }

        // Add constructor with adapter wrapper parameters
        val constructorBuilder = FunSpec.constructorBuilder()
            .addParameter("dbName", String::class)
            .addParameter("migration", ClassName("dev.goquick.sqlitenow.core", "DatabaseMigrations"))

        // Add adapter wrapper parameters to constructor only for namespaces that have adapters
        namespacesWithAdapters.forEach { (namespace, _) ->
            val adapterClassName = "${namespace.capitalized()}Adapters"
            constructorBuilder.addParameter("${namespace}Adapters", ClassName("", adapterClassName))

            // Add as private property
            val propertySpec = PropertySpec.builder("${namespace}Adapters", ClassName("", adapterClassName))
                .addModifiers(KModifier.PRIVATE)
                .initializer("${namespace}Adapters")
                .build()
            classBuilder.addProperty(propertySpec)
        }

        classBuilder.primaryConstructor(constructorBuilder.build())

        // Add superclass constructor call
        classBuilder.addSuperclassConstructorParameter("dbName = dbName")
        classBuilder.addSuperclassConstructorParameter("migration = migration")

        // Add router properties for each namespace
        nsWithStatements.keys.forEach { namespace ->
            val routerClassName = "${namespace.capitalized()}Router"
            val routerProperty = PropertySpec.builder(namespace, ClassName("", routerClassName))
                .initializer("$routerClassName(ref = this)")
                .build()
            classBuilder.addProperty(routerProperty)
        }

        // Generate adapter wrapper classes only for namespaces that have adapters
        namespacesWithAdapters.forEach { (namespace, adapters) ->
            val adapterClass = generateAdapterClass(namespace, adapters)
            classBuilder.addType(adapterClass)
        }

        // Generate router classes
        nsWithStatements.forEach { (namespace, statements) ->
            val routerClass = generateRouterClass(namespace, statements, adaptersByNamespace)
            classBuilder.addType(routerClass)
        }

        return classBuilder.build()
    }

    /** Generates an adapter wrapper class for a specific namespace. */
    private fun generateAdapterClass(namespace: String, adapters: List<UniqueAdapter>): TypeSpec {
        val adapterClassName = "${namespace.capitalized()}Adapters"
        val classBuilder = TypeSpec.classBuilder(adapterClassName)
            .addModifiers(KModifier.PUBLIC, KModifier.DATA)

        // Add constructor with adapter parameters
        val constructorBuilder = FunSpec.constructorBuilder()

        adapters.forEach { adapter ->
            val paramSpec = adapter.toParameterSpec()
            constructorBuilder.addParameter(paramSpec)

            // Add as property
            val propertySpec = PropertySpec.builder(adapter.functionName, adapter.toParameterSpec().type)
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
        val routerClassName = "${namespace.capitalized()}Router"
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
                    classBuilder.addProperty(generateSelectRunnersProperty(statement, namespace, adaptersByNamespace))
                }

                is AnnotatedExecuteStatement -> {
                    // Generate ExecuteRunners object for INSERT/UPDATE/DELETE statements
                    classBuilder.addProperty(generateExecuteRunnersProperty(statement, namespace, adaptersByNamespace))
                }

                else -> {
                    // CREATE TABLE/VIEW statements don't need router methods
                }
            }
        }

        return classBuilder.build()
    }

    /** Generates an ExecuteRunners property for INSERT/UPDATE/DELETE statements. */
    private fun generateExecuteRunnersProperty(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.replaceFirstChar { it.lowercase() }

        // Check if statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        val hasParams = namedParameters.isNotEmpty()

        // Create the property type
        val propertyType = if (hasParams) {
            // Function type: (Params) -> ExecuteRunners
            val paramsType = ClassName(packageName, "${namespace.capitalized()}Query")
                .nestedClass(className)
                .nestedClass("Params")
            val executeRunnersType = ClassName("dev.goquick.sqlitenow.core", "ExecuteRunners")
            LambdaTypeName.get(parameters = arrayOf(paramsType), returnType = executeRunnersType)
        } else {
            // Direct ExecuteRunners type
            ClassName("dev.goquick.sqlitenow.core", "ExecuteRunners")
        }

        // Generate the object expression implementing ExecuteRunners
        val objectExpression = generateExecuteRunnersObjectExpression(statement, namespace, className,
            hasParams, adaptersByNamespace)

        return PropertySpec.builder(propertyName, propertyType)
            .initializer(objectExpression)
            .build()
    }

    /** Generates a SelectRunners property for SELECT statements. */
    private fun generateSelectRunnersProperty(
        statement: AnnotatedSelectStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.replaceFirstChar { it.lowercase() }

        // Determine result type (handles shared results)
        val resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)

        // Check if statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        val hasParams = namedParameters.isNotEmpty()

        // Create the property type
        val propertyType = if (hasParams) {
            // Function type: (Params) -> SelectRunners<ResultType>
            val paramsType = ClassName(packageName, "${namespace.capitalized()}Query")
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
        val objectExpression = generateSelectRunnersObjectExpression(statement, namespace, className, hasParams, adaptersByNamespace)

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
        val capitalizedNamespace = "${namespace.capitalized()}Query"
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement)
        val hasAdapters = statementAdapters.isNotEmpty()

        // Build the common method call parameters
        val commonParams = buildString {
            append("conn = ref.connection()")
            if (hasParams) {
                append(",\n            params = params")
            }
            if (hasAdapters) {
                statementAdapters.forEach { config ->
                    val actualAdapterName = findAdapterName(namespace, config.adapterFunctionName, adaptersByNamespace)
                    append(",\n            ${config.adapterFunctionName} = ref.${namespace}Adapters.${actualAdapterName}")
                }
            }
        }

        // Build the object expression
        return buildString {
            if (hasParams) {
                append("{ params ->\n")
            }
            append("    object : SelectRunners<${statement.getResultTypeName(namespace)}> {\n")

            append("        override suspend fun asList() = $capitalizedNamespace.$className.executeAsList(\n")
            append("            $commonParams\n")
            append("        )\n\n")

            append("        override suspend fun asOne() = $capitalizedNamespace.$className.executeAsOne(\n")
            append("            $commonParams\n")
            append("        )\n\n")

            append("        override suspend fun asOneOrNull() = $capitalizedNamespace.$className.executeAsOneOrNull(\n")
            append("            $commonParams\n")
            append("        )\n\n")

            append("        override fun asFlow() = ref.createReactiveQueryFlow(\n")
            append("            affectedTables = $capitalizedNamespace.$className.affectedTables,\n")
            append("            queryExecutor = {\n")
            append("                $capitalizedNamespace.$className.executeAsList(\n")
            append("                    ${commonParams.lines().joinToString("\n        ")}\n")
            append("                )\n")
            append("            }\n")
            append("        )\n")
            append("    }")

            if (hasParams) {
                append("\n}")
            }
        }
    }

    /** Generates the object expression that implements ExecuteRunners interface. */
    private fun generateExecuteRunnersObjectExpression(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        hasParams: Boolean,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val capitalizedNamespace = "${namespace.capitalized()}Query"
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement)
        val hasAdapters = statementAdapters.isNotEmpty()

        // Build the common method call parameters
        val commonParams = buildString {
            append("conn = ref.connection()")
            if (hasParams) {
                append(",\n                params = params")
            }
            if (hasAdapters) {
                statementAdapters.forEach { config ->
                    val actualAdapterName = findAdapterName(namespace, config.adapterFunctionName, adaptersByNamespace)
                    append(",\n                ${config.adapterFunctionName} = ref.${namespace}Adapters.${actualAdapterName}")
                }
            }
        }

        // Build the object expression
        return buildString {
            if (hasParams) {
                append("\n{ params ->\n")
            }
            append("    object : ExecuteRunners {\n")
            append("        override suspend fun execute() {\n")
            append("            $capitalizedNamespace.$className.execute(\n")
            append("                $commonParams\n")
            append("            )\n")
            append("            // Notify listeners that tables have changed\n")
            append("            ref.notifyTablesChanged($capitalizedNamespace.$className.affectedTables)\n")
            append("        }\n")
            append("    }")

            if (hasParams) {
                append("\n}")
            }
        }
    }

    /** Helper extension to get the result type name for a statement. */
    private fun AnnotatedSelectStatement.getResultTypeName(namespace: String): String {
        val resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, this)
        return resultType.toString()
    }


}
