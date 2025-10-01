package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.StatementUtils.getNamedParameters
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
import dev.goquick.sqlitenow.gradle.inspect.InsertStatement
import dev.goquick.sqlitenow.gradle.inspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.inspect.DeleteStatement
import java.io.File

/**
 * Generates Kotlin extension functions for query operations.
 * Works with DataStructCodeGenerator to create query extension functions.
 */
internal class QueryCodeGenerator(
    private val dataStructCodeGenerator: DataStructCodeGenerator,
    private val packageName: String,
    private val outputDir: File,
    private val debug: Boolean = false,
) {
    private fun queryNamespaceName(namespace: String): String = pascalize(namespace) + "Query"
    private val columnLookup = ColumnLookup(
        dataStructCodeGenerator.createTableStatements,
        dataStructCodeGenerator.createViewStatements
    )
    private val typeMapping = TypeMapping()
    private val parameterBinding =
        ParameterBinding(columnLookup, typeMapping, dataStructCodeGenerator, debug)
    private val adapterConfig = AdapterConfig(
        columnLookup,
        dataStructCodeGenerator.createTableStatements,
        dataStructCodeGenerator.createViewStatements,
        packageName
    )

    private data class ColumnAssignment(
        val rendered: String,
        val isSuffixed: Boolean,
        val order: Int,
        val disambiguationIndex: Int?
    )
    private val selectFieldGenerator = SelectFieldCodeGenerator(
        dataStructCodeGenerator.createTableStatements,
        dataStructCodeGenerator.createViewStatements,
        packageName
    )
    private val adapterNameResolver = AdapterParameterNameResolver()

    // --- Helper: choose final adapter parameter names for a statement, canonicalizing and de-duplicating by signature ---
    internal fun chooseAdapterParamNames(
        configs: List<AdapterConfig.ParamConfig>
    ): Map<AdapterConfig.ParamConfig, String> {
        return adapterNameResolver.chooseAdapterParamNames(configs)
    }

    // --- Helper: resolve chosen adapter param name for a given output field ---
    private fun resolveOutputAdapterParamNameForField(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>
    ): String? {
        return adapterNameResolver.resolveOutputAdapterParamNameForField(
            statement, field, tableAliases, aliasPrefixes, adapterConfig
        )
    }

    /**
     * Generates query extension function files for all namespaces.
     * Creates separate files per query like Person_SelectWeird.kt, Person_AddUser.kt, etc.
     */
    fun generateCode() {
        dataStructCodeGenerator.nsWithStatements.forEach { (namespace, statements) ->
            val statementProcessor = StatementProcessor(statements)
            statementProcessor.processStatements(
                onSelectStatement = { statement ->
                    generateQueryFile(namespace, statement, packageName)
                },
                onExecuteStatement = { statement ->
                    generateQueryFile(namespace, statement, packageName)
                }
            )
        }
    }

    /**
     * Generates a separate file for a specific query.
     * Example: PersonQuery_SelectWeird.kt
     */
    private fun generateQueryFile(
        namespace: String,
        statement: AnnotatedStatement,
        packageName: String
    ) {
        val className = statement.getDataClassName()
        val fileName = "${queryNamespaceName(namespace)}_$className"
        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated query extension functions for ${namespace}.${className}")
            .addFileComment("\nDO NOT MODIFY THIS FILE MANUALLY!")
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )
            .addImport("dev.goquick.sqlitenow.core.util", "jsonEncodeToSqlite")
        if (!debug) {
            fileSpecBuilder.addImport("kotlinx.coroutines", "withContext")
        } else {
            fileSpecBuilder.addImport("dev.goquick.sqlitenow.common", "sqliteNowLogger")
            fileSpecBuilder.addImport("dev.goquick.sqlitenow.core.util", "sqliteNowPreview")
        }
        // Generate bindStatementParams function first
        val bindFunction = generateBindStatementParamsFunction(namespace, statement)
        fileSpecBuilder.addFunction(bindFunction)
        // For SELECT statements, also generate readStatementResult function
        if (statement is AnnotatedSelectStatement) {
            // Only generate readStatementResult for non-collection queries
            if (!statement.hasCollectionMapping()) {
                val readStatementResultFunction =
                    generateReadStatementResultFunction(namespace, statement)
                fileSpecBuilder.addFunction(readStatementResultFunction)
            }
            // Also generate readJoinedStatementResult function for queries with dynamic field mapping
            if (statement.hasDynamicFieldMapping()) {
                val readJoinedStatementResultFunction =
                    generateReadJoinedStatementResultFunction(namespace, statement)
                fileSpecBuilder.addFunction(readJoinedStatementResultFunction)
            }
        }

        // For EXECUTE statements with RETURNING clause, also generate readStatementResult function
        if (statement is AnnotatedExecuteStatement && statement.hasReturningClause()) {
            val readStatementResultFunction =
                generateReadStatementResultFunctionForExecute(namespace, statement)
            fileSpecBuilder.addFunction(readStatementResultFunction)
        }
        // Then generate execute function(s) that use bindStatementParams and readStatementResult
        when (statement) {
            is AnnotatedSelectStatement -> {
                val executeAsListFunction =
                    generateSelectQueryFunction(namespace, statement, "executeAsList")
                fileSpecBuilder.addFunction(executeAsListFunction)
                // Only generate single-row functions for non-collection queries
                // (Collection mapping requires multiple rows to build collections)
                if (!statement.hasCollectionMapping()) {
                    val executeAsOneFunction =
                        generateSelectQueryFunction(namespace, statement, "executeAsOne")
                    val executeAsOneOrNullFunction =
                        generateSelectQueryFunction(namespace, statement, "executeAsOneOrNull")
                    fileSpecBuilder.addFunction(executeAsOneFunction)
                    fileSpecBuilder.addFunction(executeAsOneOrNullFunction)
                }
            }

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    // Generate multiple functions for RETURNING queries (like SELECT queries)
                    val executeReturningListFunction =
                        generateExecuteQueryFunction(namespace, statement, "executeReturningList")
                    val executeReturningOneFunction =
                        generateExecuteQueryFunction(namespace, statement, "executeReturningOne")
                    val executeReturningOneOrNullFunction = generateExecuteQueryFunction(
                        namespace,
                        statement,
                        "executeReturningOneOrNull"
                    )

                    fileSpecBuilder.addFunction(executeReturningListFunction)
                    fileSpecBuilder.addFunction(executeReturningOneFunction)
                    fileSpecBuilder.addFunction(executeReturningOneOrNullFunction)
                } else {
                    // Generate single execute function for non-RETURNING queries
                    val executeFunction =
                        generateExecuteQueryFunction(namespace, statement, "execute")
                    fileSpecBuilder.addFunction(executeFunction)
                }
            }

            is AnnotatedCreateTableStatement, is AnnotatedCreateViewStatement -> return
        }
        val fileSpec = fileSpecBuilder.build()
        fileSpec.writeTo(outputDir)
    }

    /**
     * Generates an extension function for SELECT statements.
     * Example: fun Person.SelectWeird.executeAsList(conn: Connection, params: Person.SelectWeird.Params): List<Person.SelectWeird.Result>
     */
    private fun generateSelectQueryFunction(
        namespace: String,
        statement: AnnotatedSelectStatement,
        functionName: String
    ): FunSpec {
        val className = statement.getDataClassName()
        // Create the extension function with appropriate documentation
        val kdoc = when (functionName) {
            "executeAsList" -> "Executes the ${statement.name} SELECT query and returns results as a list."
            "executeAsOne" -> "Executes the ${statement.name} SELECT query and returns exactly one result. Throws an exception if no results are found."
            "executeAsOneOrNull" -> "Executes the ${statement.name} SELECT query and returns one result or null if no results are found."
            else -> "Executes the ${statement.name} SELECT query."
        }
        val fnBld = FunSpec.builder(functionName)
            .addModifiers(com.squareup.kotlinpoet.KModifier.SUSPEND)
            .addKdoc(kdoc)
        // Set up common function structure (receiver, connection, params)
        setupExecuteFunctionStructure(fnBld, statement, namespace, className)
        // Set return type based on function name (handles shared results)
        val resultType =
            SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        val returnType = when (functionName) {
            "executeAsList" ->
                ClassName("kotlin.collections", "List")
                    .parameterizedBy(resultType)

            "executeAsOne" -> resultType
            "executeAsOneOrNull" -> resultType.copy(nullable = true)
            else -> resultType
        }
        fnBld.returns(returnType)
        addSqlStatementProcessing(fnBld, statement, namespace, className, functionName)
        return fnBld.build()
    }

    /**
     * Generates a bindStatementParams extension function for any statement type.
     * Example: fun Person.SelectWeird.bindStatementParams(statement: SQLiteStatement, params: Person.SelectWeird.Params, adapters...)
     */
    private fun generateBindStatementParamsFunction(
        namespace: String,
        statement: AnnotatedStatement
    ): FunSpec {
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("bindStatementParams")
            .addKdoc("Binds parameters to an already prepared SQLiteStatement for the ${statement.name} query.")
        setupStatementFunctionStructure(
            fnBld, statement, namespace, className,
            includeParamsParameter = true,
            adapterType = AdapterType.PARAMETER_BINDING
        )
        fnBld.returns(Unit::class)
        addBindStatementParamsProcessing(fnBld, statement, namespace, className)
        return fnBld.build()
    }

    /**
     * Generates a readStatementResult extension function for SELECT statements.
     * Example: fun Person.SelectWeird.readStatementResult(statement: SQLiteStatement, adapters...): Person.SelectWeird.Result
     */
    private fun generateReadStatementResultFunction(
        namespace: String,
        statement: AnnotatedSelectStatement
    ): FunSpec {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")
        setupReadStatementResultStructure(fnBld, statement, namespace, className)
        val resultType =
            SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        fnBld.returns(resultType)
        addReadStatementResultProcessing(fnBld, statement, namespace)
        return fnBld.build()
    }

    /**
     * Generates a readJoinedStatementResult extension function for SELECT statements with dynamic field mapping.
     * Example: fun Person.SelectWeird.readJoinedStatementResult(statement: SQLiteStatement, adapters...): Person.SelectWeird.Result_Joined
     */
    private fun generateReadJoinedStatementResultFunction(
        namespace: String,
        statement: AnnotatedSelectStatement
    ): FunSpec {
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readJoinedStatementResult")
            .addKdoc("Read statement and convert it to joined data class with all fields from the SQL query")
        setupReadStatementResultStructure(fnBld, statement, namespace, className)
        val resultType = createJoinedResultTypeName(namespace, statement)
        fnBld.returns(resultType)
        addReadJoinedStatementResultProcessing(fnBld, statement, namespace)
        return fnBld.build()
    }

    /**
     * Generates a readStatementResult extension function for EXECUTE statements with RETURNING clause.
     * Example: fun Activity.Add.readStatementResult(statement: SQLiteStatement, adapters...): Activity.Add.Result
     */
    private fun generateReadStatementResultFunctionForExecute(
        namespace: String,
        statement: AnnotatedExecuteStatement
    ): FunSpec {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")

        // Reuse the same structure setup as SELECT statements
        setupStatementFunctionStructure(
            fnBld, statement, namespace, className,
            includeParamsParameter = false,
            adapterType = AdapterType.RESULT_CONVERSION
        )

        // Set return type - now using separate result class file
        val resultClassName = if (statement.annotations.queryResult != null) {
            statement.annotations.queryResult!!
        } else {
            "${pascalize(namespace)}${className}Result"
        }
        val resultType = ClassName(packageName, resultClassName)
        fnBld.returns(resultType)

        // Add processing logic - reuse SELECT logic by converting EXECUTE to SELECT-like structure
        addReadStatementResultProcessingForExecute(fnBld, statement, namespace)
        return fnBld.build()
    }

    /**
     * Generates an extension function for execute (INSERT/DELETE/UPDATE) statements.
     * Example: fun Person.AddUser.execute(conn: Connection, params: Person.AddUser.Params)
     * For RETURNING statements: fun Person.AddUser.executeReturningList(conn: Connection, params: Person.AddUser.Params): List<Person.AddUser.Result>
     */
    private fun generateExecuteQueryFunction(
        namespace: String,
        statement: AnnotatedExecuteStatement,
        functionName: String = if (statement.hasReturningClause()) "executeReturningList" else "execute"
    ): FunSpec {
        val className = statement.getDataClassName()
        val hasReturning = statement.hasReturningClause()

        val fnBld = FunSpec.builder(functionName)
            .addModifiers(com.squareup.kotlinpoet.KModifier.SUSPEND)
            .addKdoc("${if (hasReturning) "Executes and returns results from" else "Executes"} the ${statement.name} query.")

        setupExecuteFunctionStructure(fnBld, statement, namespace, className)

        // Set return type based on function name
        if (hasReturning) {
            val resultClassName = if (statement.annotations.queryResult != null) {
                statement.annotations.queryResult!!
            } else {
                "${pascalize(namespace)}${className}Result"
            }
            val resultType = ClassName(packageName, resultClassName)

            when (functionName) {
                "executeReturningList" -> {
                    val listType =
                        ClassName("kotlin.collections", "List").parameterizedBy(resultType)
                    fnBld.returns(listType)
                }

                "executeReturningOne" -> {
                    fnBld.returns(resultType)
                }

                "executeReturningOneOrNull" -> {
                    fnBld.returns(resultType.copy(nullable = true))
                }

                else -> {
                    // Fallback for backward compatibility
                    fnBld.returns(resultType)
                }
            }
        } else {
            fnBld.returns(Unit::class)
        }

        addSqlStatementProcessing(
            fnBld,
            statement,
            namespace,
            className,
            functionName = functionName
        )
        return fnBld.build()
    }

    /**
     * Helper function to set up common function structure.
     * This includes receiver type, connection parameter, and params parameter.
     */
    private fun setupExecuteFunctionStructure(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val receiverType = ClassName(packageName, capitalizedNamespace).nestedClass(className)
        fnBld.receiver(receiverType)
        // Add connection parameter
        val connectionParam = ParameterSpec.builder(
            name = "conn",
            ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection")
        ).build()
        fnBld.addParameter(connectionParam)
        if (getNamedParameters(statement).isNotEmpty()) {
            val paramsType = createParamsTypeName(namespace, className)
            val paramsParam = ParameterSpec.builder("params", paramsType).build()
            fnBld.addParameter(paramsParam)
        }

        // Add input adapters (for parameter binding)
        addAdapterParameters(fnBld, statement)

        // For EXECUTE statements with RETURNING clause, also add output adapters (for result conversion)
        if (statement is AnnotatedExecuteStatement && statement.hasReturningClause()) {
            addResultConversionAdapterParametersForExecute(fnBld, statement)
        }
    }

    /**
     * Unified helper function to set up common function structure for statement-based functions.
     * This includes receiver type, statement parameter, and optional params parameter and adapters.
     */
    private fun setupStatementFunctionStructure(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        includeParamsParameter: Boolean = false,
        adapterType: AdapterType = AdapterType.NONE
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val receiverType = ClassName(packageName, capitalizedNamespace).nestedClass(className)
        fnBld.receiver(receiverType)
        val statementParam = ParameterSpec.builder(
            name = "statement",
            ClassName("androidx.sqlite", "SQLiteStatement")
        ).build()
        fnBld.addParameter(statementParam)
        // Add params parameter if requested and the statement has named parameters
        if (includeParamsParameter && getNamedParameters(statement).isNotEmpty()) {
            val paramsType = createParamsTypeName(namespace, className)
            val paramsParam = ParameterSpec.builder("params", paramsType).build()
            fnBld.addParameter(paramsParam)
        }
        // Add adapters based on type
        when (adapterType) {
            AdapterType.PARAMETER_BINDING -> addParameterBindingAdapterParameters(fnBld, statement)
            AdapterType.RESULT_CONVERSION -> {
                when (statement) {
                    is AnnotatedSelectStatement -> addResultConversionAdapterParameters(
                        fnBld,
                        statement
                    )

                    is AnnotatedExecuteStatement -> addResultConversionAdapterParametersForExecute(
                        fnBld,
                        statement
                    )

                    else -> throw IllegalArgumentException("Unsupported statement type for result conversion adapters")
                }
            }

            AdapterType.NONE -> {} /* No adapters */
        }
    }

    /**
     * Helper function to set up function structure for readStatementResult.
     * This includes receiver type, statement parameter, and result conversion adapters.
     */
    private fun setupReadStatementResultStructure(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String
    ) {
        setupStatementFunctionStructure(
            fnBld, statement, namespace, className,
            includeParamsParameter = false,
            adapterType = AdapterType.RESULT_CONVERSION
        )
    }

    /**
     * Enum to specify which type of adapters to add to a function.
     */
    private enum class AdapterType {
        PARAMETER_BINDING,  // xxxToSqlColumn adapters
        RESULT_CONVERSION,  // sqlColumnToXxx adapters
        NONE               // No adapters
    }

    /**
     * Method to add only result conversion adapter parameters (sqlColumnToXxx).
     * Used for readStatementResult functions.
     */
    private fun addResultConversionAdapterParameters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement
    ) {
        addAdapterParameters(fnBld, statement) { config ->
            config.adapterFunctionName.startsWith("sqlValueTo")
        }
    }

    /**
     * Method to add result conversion adapter parameters for EXECUTE statements with RETURNING clause.
     * Similar to SELECT statements but works with table columns instead of query fields.
     */
    private fun addResultConversionAdapterParametersForExecute(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement
    ) {
        // Get the table name from the execute statement
        val tableName = when (val src = statement.src) {
            is InsertStatement -> src.table
            is UpdateStatement -> src.table
            is DeleteStatement -> src.table
            else -> return // Only INSERT, UPDATE, and DELETE statements supported
        }

        // Find the table definition
        val tableStatement = dataStructCodeGenerator.createTableStatements.find {
            it.src.tableName.equals(tableName, ignoreCase = true)
        } ?: return

        // Get RETURNING columns
        val returningColumns = when (val src = statement.src) {
            is InsertStatement -> src.returningColumns
            is UpdateStatement -> src.returningColumns
            is DeleteStatement -> src.returningColumns
            else -> emptyList<String>()
        }

        // Determine which columns to include
        val columnsToInclude = if (returningColumns.contains("*")) {
            // RETURNING * - include all table columns
            tableStatement.columns
        } else {
            // RETURNING specific columns - include only those columns
            tableStatement.columns.filter { column ->
                returningColumns.any { returningCol ->
                    returningCol.equals(column.src.name, ignoreCase = true)
                }
            }
        }

        // Add adapter parameters for columns that need them
        val processedAdapters = mutableSetOf<String>()
        columnsToInclude.forEach { column ->
            if (column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                val propertyName =
                    statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
                val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)

                // Skip if already processed
                if (adapterFunctionName in processedAdapters) {
                    return@forEach
                }
                processedAdapters.add(adapterFunctionName)

                // Create adapter parameter
                val baseType =
                    SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
                val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
                val isNullable = column.isNullable()
                val targetType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
                    baseType,
                    propertyType,
                    isNullable,
                    packageName
                )

                val inputType = baseType.copy(nullable = isNullable)
                val outputType = targetType.copy(nullable = isNullable)

                val adapterType = LambdaTypeName.get(
                    parameters = arrayOf(inputType),
                    returnType = outputType
                )

                val parameterSpec = ParameterSpec.builder(adapterFunctionName, adapterType).build()
                fnBld.addParameter(parameterSpec)
            }
        }
    }

    /**
     * Unified method to add adapter parameters based on filter criteria.
     * Consolidates duplicate logic for adding adapter parameters to functions.
     */
    private fun addAdapterParameters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        filter: (AdapterConfig.ParamConfig) -> Boolean = { true }
    ) {
        val adapterConfigs = adapterConfig.collectAllParamConfigs(statement)
        val filteredConfigs = adapterConfigs.filter(filter)
        val chosenNames = chooseAdapterParamNames(filteredConfigs)
        // Add only one parameter per unique chosen name; signatures are identical within each name
        val byName: MutableMap<String, AdapterConfig.ParamConfig> = linkedMapOf()
        filteredConfigs.forEach { cfg ->
            val name = chosenNames[cfg]!!
            byName.putIfAbsent(name, cfg)
        }
        byName.forEach { (name, cfg) ->
            val adapterType = LambdaTypeName.get(
                parameters = arrayOf(cfg.inputType),
                returnType = cfg.outputType
            )
            val adapterParam = ParameterSpec.builder(name, adapterType).build()
            fnBld.addParameter(adapterParam)
        }
    }

    /**
     * Method to add only parameter binding adapter parameters (xxxToSqlColumn).
     * Used for prepareStatement functions.
     */
    private fun addParameterBindingAdapterParameters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement
    ) {
        addAdapterParameters(fnBld, statement) { config ->
            config.adapterFunctionName.endsWith("ToSqlValue")
        }
    }

    /**
     * Helper function to get filtered adapter function names.
     */
    private fun getFilteredAdapterNames(
        statement: AnnotatedStatement,
        filter: (AdapterConfig.ParamConfig) -> Boolean
    ): List<String> {
        val adapterConfigs = adapterConfig.collectAllParamConfigs(statement)
        val filtered = adapterConfigs.filter(filter)
        val chosen = chooseAdapterParamNames(filtered)
        val seen = LinkedHashSet<String>()
        filtered.forEach { config ->
            seen.add(chosen[config]!!)
        }
        return seen.toList()
    }

    // ----- Small helpers to reduce duplication -----

    private fun resultConversionAdapterNames(statement: AnnotatedStatement): List<String> =
        getFilteredAdapterNames(statement) { config ->
            config.adapterFunctionName.startsWith("sqlValueTo")
        }

    private fun parameterBindingAdapterNames(statement: AnnotatedStatement): List<String> =
        getFilteredAdapterNames(statement) { config ->
            config.adapterFunctionName.endsWith("ToSqlValue")
        }

    private fun buildJoinedReadParamsList(statement: AnnotatedSelectStatement): List<String> {
        val params = mutableListOf("statement")
        params += resultConversionAdapterNames(statement)
        return params
    }

    private fun buildExecuteReadParamsList(statement: AnnotatedExecuteStatement): List<String> {
        val params = mutableListOf("statement")
        params += resultConversionAdapterNamesForExecute(statement)
        return params
    }

    private fun resultConversionAdapterNamesForExecute(statement: AnnotatedExecuteStatement): List<String> {
        // Get the table name from the execute statement
        val tableName = when (val src = statement.src) {
            is InsertStatement -> src.table
            is UpdateStatement -> src.table
            is DeleteStatement -> src.table
            else -> return emptyList()
        }

        // Find the table definition
        val tableStatement = dataStructCodeGenerator.createTableStatements.find {
            it.src.tableName.equals(tableName, ignoreCase = true)
        } ?: return emptyList()

        // Get RETURNING columns
        val returningColumns = when (val src = statement.src) {
            is InsertStatement -> src.returningColumns
            is UpdateStatement -> src.returningColumns
            is DeleteStatement -> src.returningColumns
            else -> emptyList<String>()
        }

        // Determine which columns to include
        val columnsToInclude = if (returningColumns.contains("*")) {
            // RETURNING * - include all table columns
            tableStatement.columns
        } else {
            // RETURNING specific columns - include only those columns
            tableStatement.columns.filter { column ->
                returningColumns.any { returningCol ->
                    returningCol.equals(column.src.name, ignoreCase = true)
                }
            }
        }

        // Get adapter function names for columns that need them
        val adapterNames = mutableListOf<String>()
        val processedAdapters = mutableSetOf<String>()
        columnsToInclude.forEach { column ->
            if (column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                val propertyName =
                    statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
                val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)

                // Skip if already processed
                if (adapterFunctionName in processedAdapters) {
                    return@forEach
                }
                processedAdapters.add(adapterFunctionName)
                adapterNames.add(adapterFunctionName)
            }
        }

        return adapterNames
    }

    private fun withContextHeader(): String =
        if (debug) "return conn.withContextAndTrace {" else "return withContext(conn.dispatcher) {"

    private fun prepareAndMaybeBindParamsLines(
        statement: AnnotatedStatement,
        capitalizedNamespace: String,
        className: String
    ): List<String> {
        val lines = mutableListOf<String>()
        lines += "val sql = $capitalizedNamespace.$className.SQL"
        lines += "val statement = conn.prepare(sql)"
        val namedParameters = getNamedParameters(statement)
        if (namedParameters.isNotEmpty()) {
            val params = mutableListOf("statement", "params")
            params += parameterBindingAdapterNames(statement)
            lines += "$capitalizedNamespace.$className.bindStatementParams(${params.joinToString(", ")})"
        }
        return lines
    }

    /**
     * Helper function to create Joined Result type name for SELECT statements.
     * Uses SharedResult_Joined if the statement has sharedResult annotation, otherwise uses regular Result_Joined.
     */
    private fun createJoinedResultTypeName(
        namespace: String,
        statement: AnnotatedSelectStatement
    ): ClassName {
        val capitalizedNamespace = queryNamespaceName(namespace)
        return if (statement.annotations.queryResult != null) {
            // For queryResult: PersonWithAddressRow_Joined (separate file)
            ClassName(packageName, "${statement.annotations.queryResult}_Joined")
        } else {
            // For regular results: PersonSelectWeirdResult_Joined (separate file)
            val className = statement.getDataClassName()
            val resultClassName = "${pascalize(namespace)}${className}Result"
            ClassName(packageName, "${resultClassName}_Joined")
        }
    }

    /**
     * Helper function to create Params type name.
     * Consolidates duplicate ClassName construction for Params types.
     */
    private fun createParamsTypeName(namespace: String, className: String): ClassName {
        val capitalizedNamespace = queryNamespaceName(namespace)
        return ClassName(packageName, capitalizedNamespace)
            .nestedClass(className)
            .nestedClass("Params")
    }

    /**
     * Helper function to get the underlying SQLite type and appropriate getter for a field.
     * Returns a pair of (KotlinType, GetterCall) based on the original SQLite column type.
     */
    private fun getUnderlyingTypeAndGetter(
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int
    ): Pair<TypeName, String> {
        val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(field.src.dataType)
        val baseGetterCall = typeMapping.getGetterCall(kotlinType, columnIndex)
        return Pair(kotlinType, baseGetterCall)
    }

    /**
     * Helper function to add common SQL statement processing logic.
     * This calls prepareStatement() and then executes the statement.
     */
    private fun addSqlStatementProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val b = IndentedCodeBuilder()
        b.line(withContextHeader())
        b.indent(by = 2) {
            // Prepare SQL and optionally bind params
            prepareAndMaybeBindParamsLines(
                statement,
                capitalizedNamespace,
                className
            ).forEach { line ->
                b.line(line)
            }
            // Add execution logic
            addSqlExecutionImplementationToBuilder(
                b,
                statement,
                namespace,
                className,
                functionName
            )
        }
        b.line("}")
        fnBld.addStatement(b.build())
    }

    /**
     * Helper function to add bind statement params processing logic.
     * This includes only parameter binding logic for an already prepared statement.
     */
    private fun addBindStatementParamsProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String
    ) {
        val namedParameters = getNamedParameters(statement)
        if (namedParameters.isNotEmpty()) {
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
                    processedAdapterVars = processedAdapterVars
                )
            }
            if (debug) {
                fnBld.addStatement(
                    "sqliteNowLogger.d { %S + __paramsLog.joinToString(%S) }",
                    "bind ${queryNamespaceName(namespace)}.$className params: ", ", "
                )
            }
        }
    }

    /**
     * Helper function to add readStatementResult processing logic.
     * This generates code to read a single row from the statement and convert it to a Result object.
     * If dynamic field mapping is present, it reuses readJoinedStatementResult to avoid code duplication.
     */
    private fun addReadStatementResultProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String
    ) {
        if (statement.hasDynamicFieldMapping()) {
            // Use the joined function approach to avoid code duplication
            addReadStatementResultProcessingUsingJoined(fnBld, statement, namespace)
        } else {
            // Use the original direct approach for queries without dynamic field mapping
            addReadStatementResultProcessingDirect(fnBld, statement, namespace)
        }
    }

    /**
     * Generates readStatementResult logic by reusing readJoinedStatementResult.
     * This approach is used when dynamic field mapping is present.
     */
    private fun addReadStatementResultProcessingUsingJoined(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String
    ) {
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        // Build parameter list for readJoinedStatementResult (same adapters as this function)
        val paramsList = mutableListOf("statement")
        // Add result conversion adapter function names (same as current function parameters)
        paramsList.addAll(resultConversionAdapterNames(statement))
        val paramsString = paramsList.joinToString(", ")
        // Call readJoinedStatementResult and transform the result
        val transformationCall = buildString {
            append("    val joinedData = $capitalizedNamespace.$className.readJoinedStatementResult($paramsString)\n")
            append("return $resultType(\n")
            addJoinedToMappedTransformation(this, statement)
            append(")")
        }
        fnBld.addStatement(transformationCall)
    }

    /**
     * Generates readStatementResult logic using direct statement access.
     * This approach is used when no dynamic field mapping is present.
     */
    private fun addReadStatementResultProcessingDirect(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String
    ) {
        // Use the correct result type (handles shared results)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        // Build the constructor call with all properties
        val aliasPrefixes = adapterConfig.collectAliasPrefixesForSelect(statement)
        val constructorCall = buildString {
            append("return $resultType(\n")
            // No dynamic field mapping, so process all fields directly
            statement.fields.forEachIndexed { index, field ->
                if (!field.annotations.isDynamicField) {
                    val propertyName =
                        getPropertyName(field, statement.annotations.propertyNameGenerator)
                    val getterCall = generateGetterCallInternalWithPrefixes(
                        statement = statement,
                        field = field,
                        columnIndex = index,
                        propertyNameGenerator = statement.annotations.propertyNameGenerator,
                        isFromJoinedTable = false,
                        tableAliases = statement.src.tableAliases,
                        aliasPrefixes = aliasPrefixes,
                    )
                    val comment = buildFieldDebugComment(
                        field,
                        statement.src,
                        statement.annotations.propertyNameGenerator,
                        includeType = true
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

    /**
     * Adds transformation logic from joined data to mapped data structure.
     * This generates the property assignments for the mapped result constructor.
     */
    private fun addJoinedToMappedTransformation(
        codeBuilder: StringBuilder,
        statement: AnnotatedSelectStatement,
    ) {
        val mappingPlan = statement.mappingPlan
        val regularFields = mappingPlan.regularFields
        val joinedNameMap = computeJoinedNameMap(statement)
        regularFields.forEachIndexed { index, field ->
            val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
            val key = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName.orEmpty(), field.src.fieldName)
            val joinedProp = joinedNameMap[key] ?: propertyName
            val comment = buildFieldDebugComment(
                field,
                statement.src,
                statement.annotations.propertyNameGenerator,
                includeType = true
            )
            if (comment.isNotEmpty()) {
                codeBuilder.append("  // $comment\n")
            }
            codeBuilder.append("  $propertyName = joinedData.$joinedProp,\n")
        }
        mappingPlan.includedDynamicEntries.forEach { dynamicEntry ->
            val field = dynamicEntry.field
            val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
            val comment = buildFieldDebugComment(
                field,
                statement.src,
                statement.annotations.propertyNameGenerator
            )
            if (comment.isNotEmpty()) {
                codeBuilder.append("  // $comment\n")
            }
            if (dynamicEntry.mappingType != null) {
                val mappingCode = generateDynamicFieldMappingCodeFromJoined(field, statement)
                codeBuilder.append("  $propertyName = $mappingCode,\n")
            } else {
                val defaultValue = field.annotations.defaultValue ?: "null"
                codeBuilder.append("  $propertyName = $defaultValue,\n")
            }
        }
    }

    /**
     * Generates mapping code for a dynamic field using joined data instead of statement.
     * Handles NULL checks for LEFT JOINs and type conversions for non-nullable target properties.
     */
    private fun generateDynamicFieldMappingCodeFromJoined(
        dynamicField: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement,
        sourceVar: String = "joinedData",
        rowsVar: String? = null,
        baseIndentLevel: Int = 2
    ): String {
        val selectStatement = statement.src
        val mappings = DynamicFieldMapper.createDynamicFieldMappings(
            selectStatement,
            listOf(dynamicField)
        )
        val mapping = mappings.firstOrNull()
        if (mapping == null || mapping.columns.isEmpty()) {
            return "null // No columns found for mapping"
        }

        val mappingType = AnnotationConstants.MappingType.fromString(dynamicField.annotations.mappingType)
        if (mappingType == AnnotationConstants.MappingType.COLLECTION) {
            val rowsVariableName = rowsVar ?: "rowsForEntity"
            return generateCollectionMappingCode(
                dynamicField,
                mapping,
                statement,
                sourceVar,
                rowsVariableName,
                baseIndentLevel
            )
        }

        val notNull = dynamicField.annotations.notNull == true
        val originatesFromPrimaryAlias = dynamicField.aliasPath.size <= 1
        val skipNullGuard = notNull && originatesFromPrimaryAlias
        val needsNullGuard = !skipNullGuard

        val joinedNameMap = computeJoinedNameMap(statement)
        val useRowVariable = needsNullGuard && rowsVar != null
        val effectiveSourceVar = if (useRowVariable) "row" else sourceVar

        val constructorArgs = generateConstructorArgumentsFromMapping(
            mapping,
            statement,
            effectiveSourceVar,
            additionalIndent = 6,
            enforceNonNull = notNull,
            rowsVar = rowsVar,
            baseIndentLevel = baseIndentLevel,
            aliasPath = dynamicField.aliasPath
        )
        val constructorExpression = buildString {
            append("${dynamicField.annotations.propertyType}(")
            append("\n      $constructorArgs\n")
            append("    )")
        }

        if (!needsNullGuard) {
            return constructorExpression
        }

        val relevantColumns = mapping.columns
            .filterNot { DynamicFieldUtils.isNestedAlias(it.fieldName, mapping.aliasPrefix) }

        fun nullCheckCondition(variable: String): String {
            if (relevantColumns.isEmpty()) {
                return "false"
            }
            return relevantColumns.joinToString(separator = " && ") { column ->
                val joinedPropertyName = resolveJoinedPropertyName(
                    column = column,
                    mapping = mapping,
                    statement = statement,
                    aliasPath = dynamicField.aliasPath,
                    joinedNameMap = joinedNameMap
                )
                "$variable.$joinedPropertyName == null"
            }
        }

        val nullCheckIndent = " ".repeat(baseIndentLevel * 2)

        if (useRowVariable) {
            val rowsSource = rowsVar ?: error("rowsVar must be provided when useRowVariable is true")
            val selectorCondition = nullCheckCondition("row")
            val selector = if (selectorCondition == "false") {
                "$rowsSource.firstOrNull()"
            } else {
                "$rowsSource.firstOrNull { row -> !($selectorCondition) }"
            }
            return if (notNull) {
                buildString {
                    append("$selector?.let { row ->\n")
                    append("$nullCheckIndent  $constructorExpression\n")
                    append("$nullCheckIndent} ?: error(\"Required dynamic field '${dynamicField.src.fieldName}' is null\")")
                }
            } else {
                buildString {
                    append("$selector?.let { row ->\n")
                    append("$nullCheckIndent  $constructorExpression\n")
                    append("$nullCheckIndent}")
                }
            }
        }

        val nullCheckCondition = nullCheckCondition(sourceVar)
        return if (notNull) {
            buildString {
                append("if ($nullCheckCondition) {\n")
                append("$nullCheckIndent  error(\"Required dynamic field '${dynamicField.src.fieldName}' is null\")\n")
                append("$nullCheckIndent} else {\n")
                append("$nullCheckIndent  $constructorExpression\n")
                append("$nullCheckIndent}")
            }
        } else {
            buildString {
                append("if ($nullCheckCondition) {\n")
                append("$nullCheckIndent  null\n")
                append("$nullCheckIndent} else {\n")
                append("$nullCheckIndent  $constructorExpression\n")
                append("$nullCheckIndent}")
            }
        }
    }

    /**
     * Determines if the target property is nullable based on the original column definition from the schema.
     * Uses the proper schema lookup tools to determine nullability.
     */
    private fun isTargetPropertyNullable(
        column: SelectStatement.FieldSource,
    ): Boolean {
        // Create a mock AnnotatedSelectStatement.Field to use with SelectFieldCodeGenerator
        val mockFieldAnnotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            notNull = null,
            adapter = false
        )
        val mockField = AnnotatedSelectStatement.Field(
            src = column,
            annotations = mockFieldAnnotations
        )
        // Use the existing SelectFieldCodeGenerator to determine nullability from schema
        return selectFieldGenerator.determineNullability(mockField)
    }

    /**
     * Helper function to add SQL execution implementation to a StringBuilder.
     * Generates different code for SELECT vs INSERT/DELETE statements.
     */
    private fun addSqlExecutionImplementationToBuilder(
        b: IndentedCodeBuilder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        when (statement) {
            is AnnotatedSelectStatement -> {
                addSelectExecutionImplementationToBuilder(
                    b,
                    statement,
                    namespace,
                    className,
                    functionName
                )
            }

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    addExecuteReturningStatementImplementationToBuilder(
                        b,
                        statement,
                        namespace,
                        className,
                        functionName
                    )
                } else {
                    addExecuteStatementImplementationToBuilder(b)
                }
            }

            is AnnotatedCreateTableStatement -> {
                b.line("TODO(\"Unimplemented\")")
            }

            is AnnotatedCreateViewStatement -> {
                b.line("TODO(\"Unimplemented\")")
            }
        }
    }

    /**
     * Helper function to add SELECT statement execution implementation to StringBuilder.
     * Generates code to execute query and convert results to data classes using readStatementResult.
     */
    private fun addSelectExecutionImplementationToBuilder(
        bOuter: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        functionName: String,
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val paramsString = buildJoinedReadParamsList(statement).joinToString(", ")
        bOuter.line("statement.use { statement ->")
        bOuter.indent(by = 2) {
            when (functionName) {
                "executeAsList" -> {
                    if (statement.hasCollectionMapping()) {
                        addCollectionMappingExecuteAsListImplementation(
                            this,
                            statement,
                            namespace,
                            className,
                            paramsString
                        )
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
        bOuter.line("}")
    }

    /**
     * Helper function to add INSERT/DELETE statement execution implementation to StringBuilder.
     * Generates code to execute statement without returning a value.
     */
    private fun addExecuteStatementImplementationToBuilder(b: IndentedCodeBuilder) {
        b.line("statement.use { statement ->")
        b.indent(by = 2) { b.line("statement.step()") }
        b.line("}")
    }

    /**
     * Helper function to add INSERT statement with RETURNING clause execution implementation.
     * Generates code to execute statement and return the result(s) based on function type.
     */
    private fun addExecuteReturningStatementImplementationToBuilder(
        b: IndentedCodeBuilder,
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        functionName: String
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val paramsString = buildExecuteReadParamsList(statement).joinToString(", ")
        val resultTypeString = SharedResultTypeUtils.createResultTypeStringForExecute(namespace, statement)

        when (functionName) {
            "executeReturningList" -> {
                // Generate list collection logic (similar to SELECT executeAsList)
                b.line("statement.use { statement ->")
                b.indent(by = 2) {
                    b.line("val results = mutableListOf<$resultTypeString>()")
                    b.line("while (statement.step()) {")
                    b.indent(by = 2) {
                        b.line("results.add($capitalizedNamespace.$className.readStatementResult($paramsString))")
                    }
                    b.line("}")
                    b.line("results")
                }
                b.line("}")
            }

            "executeReturningOne" -> {
                // Generate single result logic (similar to SELECT executeAsOne)
                b.line("statement.use { statement ->")
                b.indent(by = 2) {
                    b.line("if (statement.step()) {")
                    b.indent(by = 2) {
                        b.line("$capitalizedNamespace.$className.readStatementResult($paramsString)")
                    }
                    b.line("} else {")
                    b.indent(by = 2) {
                        b.line("throw IllegalStateException(\"Statement with RETURNING returned no results, but exactly one result was expected\")")
                    }
                    b.line("}")
                }
                b.line("}")
            }

            "executeReturningOneOrNull" -> {
                // Generate nullable single result logic (similar to SELECT executeAsOneOrNull)
                b.line("statement.use { statement ->")
                b.indent(by = 2) {
                    b.line("if (statement.step()) {")
                    b.indent(by = 2) {
                        b.line("$capitalizedNamespace.$className.readStatementResult($paramsString)")
                    }
                    b.line("} else {")
                    b.indent(by = 2) {
                        b.line("null")
                    }
                    b.line("}")
                }
                b.line("}")
            }

            else -> {
                // Fallback for backward compatibility
                b.line("statement.use { statement ->")
                b.indent(by = 2) {
                    b.line("if (statement.step()) {")
                    b.indent(by = 2) {
                        b.line("$capitalizedNamespace.$className.readStatementResult(statement)")
                    }
                    b.line("} else {")
                    b.indent(by = 2) {
                        b.line("throw IllegalStateException(\"Statement with RETURNING returned no results\")")
                    }
                    b.line("}")
                }
                b.line("}")
            }
        }
    }

    /**
     * Helper function to add readStatementResult processing logic for EXECUTE statements with RETURNING clause.
     * This generates code to read a single row from the statement and convert it to a Result object.
     * Reuses SELECT statement logic by converting EXECUTE RETURNING columns to SELECT-like fields.
     */
    private fun addReadStatementResultProcessingForExecute(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement,
        namespace: String
    ) {
        // Use the correct result type (handles separate result files)
        val resultType = SharedResultTypeUtils.createResultTypeStringForExecute(namespace, statement)

        // Convert EXECUTE statement to SELECT-like fields for reusing existing logic
        val selectLikeFields = createSelectLikeFieldsFromExecuteReturning(statement)

        // Build the constructor call with all properties - reuse SELECT logic
        val constructorCall = buildString {
            append("return $resultType(\n")
            selectLikeFields.forEachIndexed { index, field ->
                val propertyName =
                    statement.annotations.propertyNameGenerator.convertToPropertyName(field.src.fieldName)
                // Inline getter logic for EXECUTE RETURNING (single-table)
                val desiredType = selectFieldGenerator.generateProperty(field, statement.annotations.propertyNameGenerator).type
                val hasAdapter = isCustomKotlinType(desiredType) || adapterConfig.hasAdapterAnnotation(field)
                val getterCall = if (hasAdapter) {
                    val visibleName = field.src.fieldName
                    val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(visibleName)
                    val adapterParamName = adapterConfig.getOutputAdapterFunctionName(columnName)
                    val inputGetter = typeMapping
                        .getGetterCall(
                            SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(field.src.dataType),
                            index
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

    /**
     * Converts EXECUTE statement RETURNING columns to SELECT-like fields so we can reuse existing SELECT logic.
     * This creates fake AnnotatedSelectStatement.Field objects that represent the RETURNING columns.
     */
    private fun createSelectLikeFieldsFromExecuteReturning(statement: AnnotatedExecuteStatement): List<AnnotatedSelectStatement.Field> {
        // Get the table name from the execute statement
        val tableName = when (val src = statement.src) {
            is InsertStatement -> src.table
            is UpdateStatement -> src.table
            is DeleteStatement -> src.table
            else -> throw IllegalArgumentException("Only INSERT, UPDATE, and DELETE statements with RETURNING are supported")
        }

        // Find the table definition
        val tableStatement = dataStructCodeGenerator.createTableStatements.find {
            it.src.tableName.equals(tableName, ignoreCase = true)
        } ?: throw IllegalArgumentException("Table '$tableName' not found")

        // Get RETURNING columns
        val returningColumns = when (val src = statement.src) {
            is InsertStatement -> src.returningColumns
            is UpdateStatement -> src.returningColumns
            is DeleteStatement -> src.returningColumns
            else -> emptyList<String>()
        }

        // Determine which columns to include
        val columnsToInclude = if (returningColumns.contains("*")) {
            // RETURNING * - include all table columns
            tableStatement.columns
        } else {
            // RETURNING specific columns - include only those columns
            tableStatement.columns.filter { column ->
                returningColumns.any { returningCol ->
                    returningCol.equals(column.src.name, ignoreCase = true)
                }
            }
        }

        // Convert table columns to SELECT-like fields
        return columnsToInclude.map { column ->
            // Create a fake SelectStatement.FieldSource that represents this column
            val fieldSrc = SelectStatement.FieldSource(
                fieldName = column.src.name,
                tableName = tableName,
                originalColumnName = column.src.name,
                dataType = column.src.dataType
            )

            // Create field annotations - copy from table column annotations
            val annotationMap = mutableMapOf<String, Any?>()
            if (column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                annotationMap[AnnotationConstants.ADAPTER] = AnnotationConstants.ADAPTER_CUSTOM
            }
            column.annotations[AnnotationConstants.PROPERTY_TYPE]?.let { propertyType ->
                annotationMap[AnnotationConstants.PROPERTY_TYPE] = propertyType
            }

            val fieldAnnotations = FieldAnnotationOverrides.parse(annotationMap)

            AnnotatedSelectStatement.Field(fieldSrc, fieldAnnotations)
        }
    }

    /**
     * Get the desired property type for an EXECUTE statement column.
     * This should match the type used in the generated Result data class.
     */
    private fun getDesiredPropertyTypeForExecuteColumn(
        column: AnnotatedCreateTableStatement.Column,
        propertyNameGenerator: PropertyNameGeneratorType
    ): TypeName {
        // For now, use the same logic as the data class generator
        // This should match what DataStructCodeGenerator generates for the Result class
        return SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
    }


    /**
     * Helper function to get the property name for a field.
     * Delegates to AdapterConfigurationService.
     */
    private fun getPropertyName(
        field: AnnotatedSelectStatement.Field,
        propertyNameGenerator: PropertyNameGeneratorType
    ): String {
        return adapterConfig.getPropertyName(field, propertyNameGenerator, selectFieldGenerator)
    }


    /**
     * Computes the (tableAlias, fieldName) -> unique joined property name mapping for a statement.
     */
    private fun computeJoinedNameMap(statement: AnnotatedSelectStatement): Map<JoinedPropertyNameResolver.JoinedFieldKey, String> {
        return JoinedPropertyNameResolver.computeNameMap(
            fields = statement.fields,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            selectFieldGenerator = selectFieldGenerator
        )
    }

    private fun buildTableCandidates(
        column: SelectStatement.FieldSource,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement
    ): List<String> {
        val candidates = mutableListOf<String>()
        if (column.tableName.isNotBlank()) {
            candidates += column.tableName
        }
        mapping.sourceTableAlias?.takeIf { it.isNotBlank() }?.let { alias ->
            candidates += alias
            statement.src.tableAliases[alias]?.let { resolved ->
                if (resolved.isNotBlank()) {
                    candidates += resolved
                }
            }
        }
        if (candidates.isEmpty() && column.tableName.isNotBlank()) {
            candidates += column.tableName
        }
        return candidates.filter { it.isNotBlank() }.distinct()
    }

    private fun candidateFieldNames(
        column: SelectStatement.FieldSource,
        aliasPrefix: String?
    ): List<String> {
        val names = mutableListOf<String>()
        val fieldName = column.fieldName
        if (fieldName.isNotBlank()) {
            names += fieldName
        }
        val original = column.originalColumnName
        if (original.isNotBlank()) {
            names += original
        }
        aliasPrefix?.takeIf { it.isNotBlank() }?.let { prefix ->
            if (fieldName.startsWith(prefix) && fieldName.length > prefix.length) {
                names += fieldName.removePrefix(prefix)
            }
            if (original.isNotBlank() && original.startsWith(prefix) && original.length > prefix.length) {
                names += original.removePrefix(prefix)
            }
        }
        return names.filter { it.isNotBlank() }.distinct()
    }

    private fun resolveJoinedPropertyName(
        column: SelectStatement.FieldSource,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        aliasPath: List<String>?,
        joinedNameMap: Map<JoinedPropertyNameResolver.JoinedFieldKey, String>
    ): String {
        val aliasPrefix = mapping.aliasPrefix?.takeIf { it.isNotBlank() }
        val tableCandidates = buildTableCandidates(column, mapping, statement)
        val candidateNames = candidateFieldNames(column, aliasPrefix)
        val rootAliases = collectRootAliases(statement)
        val aliasPathMatchesRoot = aliasPath?.any { rootAliases.contains(it) } == true
        val sourceAliasMatchesRoot = !mapping.sourceTableAlias.isNullOrBlank() && rootAliases.contains(mapping.sourceTableAlias)
        val preferSuffixed = aliasPath != null && aliasPath.isNotEmpty() && !(aliasPathMatchesRoot || sourceAliasMatchesRoot)
        tableCandidates.forEach { table ->
            val expandedCandidates = expandCandidateNamesForTable(table, candidateNames, joinedNameMap, preferSuffixed)
            expandedCandidates.forEach { candidate ->
                val key = JoinedPropertyNameResolver.JoinedFieldKey(table, candidate)
                joinedNameMap[key]?.let { return it }
            }
        }
        val fallbackName = candidateNames.firstOrNull().orEmpty().ifBlank { column.fieldName }
        return statement.annotations.propertyNameGenerator.convertToPropertyName(fallbackName)
    }

    private fun expandCandidateNamesForTable(
        tableAlias: String,
        baseNames: List<String>,
        joinedNameMap: Map<JoinedPropertyNameResolver.JoinedFieldKey, String>,
        preferSuffixed: Boolean
    ): List<String> {
        val expanded = mutableListOf<String>()
        baseNames.forEach { base ->
            if (!base.contains(':')) {
                val suffixed = joinedNameMap.keys
                    .filter { key -> key.tableAlias == tableAlias && key.fieldName.startsWith("$base:") }
                    .map { it.fieldName }
                    .sorted()
                if (preferSuffixed) {
                    suffixed.forEach { candidate ->
                        if (!expanded.contains(candidate)) {
                            expanded.add(candidate)
                        }
                    }
                }
                if (!expanded.contains(base)) {
                    expanded.add(base)
                }
                if (!preferSuffixed) {
                    suffixed.forEach { candidate ->
                        if (!expanded.contains(candidate)) {
                            expanded.add(candidate)
                        }
                    }
                }
            } else if (!expanded.contains(base)) {
                expanded.add(base)
            }
        }
        return expanded
    }

    private fun collectRootAliases(statement: AnnotatedSelectStatement): Set<String> {
        val aliases = mutableSetOf<String>()
        statement.src.fromTable?.let { aliases.add(it) }
        aliases.addAll(statement.src.tableAliases.keys)
        statement.mappingPlan.includedEntityEntries.forEach { entry ->
            entry.field.aliasPath.firstOrNull()?.let { aliases.add(it) }
            entry.field.aliasPath.getOrNull(1)?.let { aliases.add(it) }
            entry.field.annotations.sourceTable?.let { aliases.add(it) }
        }
        return aliases.filter { it.isNotBlank() }.toSet()
    }

    /**
     * Variant of getter call generation that uses known aliasPrefixes for dynamic fields.
     */
    private fun generateGetterCallInternalWithPrefixes(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        isFromJoinedTable: Boolean,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>,
    ): String {
        val desiredType = selectFieldGenerator.generateProperty(field, propertyNameGenerator).type
        val isCustomDesiredType = isCustomKotlinType(desiredType)
        if (isCustomDesiredType || adapterConfig.hasAdapterAnnotation(field, aliasPrefixes)) {
            // Name the adapter param by the visible (aliased) column name, then canonicalize to provider namespace to reduce noise
            val visibleName = field.src.fieldName
            val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(visibleName)
            val rawAdapterName = adapterConfig.getOutputAdapterFunctionName(columnName)
            val providerNs = if (field.src.tableName.isNotBlank()) {
                tableAliases[field.src.tableName] ?: field.src.tableName
            } else null
            val adapterParamName = resolveOutputAdapterParamNameForField(
                statement = statement,
                field = field,
                tableAliases = tableAliases,
                aliasPrefixes = aliasPrefixes
            ) ?: (providerNs?.let { adapterNameResolver.canonicalizeAdapterNameForNamespace(it, rawAdapterName) } ?: rawAdapterName)

            val inputNullable = desiredType.isNullable
            val baseGetterCall = getUnderlyingTypeAndGetter(field, columnIndex).second.replace("stmt", "statement")

            return if (isFromJoinedTable || inputNullable) {
                // For joined-table fields, the Joined property must be nullable; return null if DB value is NULL
                //   or
                // If the receiver property is nullable, propagate DB NULL as null (do not call adapter with null)
                "if (statement.isNull($columnIndex)) null else $adapterParamName($baseGetterCall)"
            } else {
                // Non-nullable receiver: read directly and let adapter handle the concrete value
                "$adapterParamName($baseGetterCall)"
            }
        } else {
            val kotlinType = desiredType
            val baseGetterCall = typeMapping
                .getGetterCall(kotlinType.copy(nullable = false), columnIndex)
                .replace("stmt", "statement")
            return if (isFromJoinedTable || desiredType.isNullable) {
                "if (statement.isNull($columnIndex)) null else $baseGetterCall"
            } else {
                baseGetterCall
            }
        }
    }

    private fun isCustomKotlinType(type: com.squareup.kotlinpoet.TypeName): Boolean {
        val t = type.toString()
        return !typeMapping.isStandardKotlinType(t)
    }

    /**
     * Helper function to add readJoinedStatementResult processing logic.
     * This generates code to read a single row from the statement and convert it to a Joined Result object.
     * Unlike the regular version, this includes ALL columns without any dynamic field mapping or exclusions.
     */
    private fun addReadJoinedStatementResultProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String
    ) {
        // Create the joined result type name
        val resultType = if (statement.annotations.queryResult != null) {
            "${statement.annotations.queryResult}_Joined"
        } else {
            val className = statement.getDataClassName()
            "${pascalize(namespace)}${className}Result_Joined"
        }
        // Build the constructor call with ALL properties (no exclusions)
        val constructorCall = buildString {
            append("return $resultType(\n")
            // Include ALL fields from the SELECT statement (no dynamic field mapping or exclusions)
            val allFields = statement.fields.filter { !it.annotations.isDynamicField }
            val joinedNameMap = computeJoinedNameMap(statement)
            allFields.forEachIndexed { index, field ->
                val key = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName.orEmpty(), field.src.fieldName)
                val propertyName = joinedNameMap[key]
                    ?: getPropertyName(field, statement.annotations.propertyNameGenerator)
                val getterCall = generateJoinedGetterCall(statement, field, index, allFields)
                val comment = buildFieldDebugComment(
                    field,
                    statement.src,
                    statement.annotations.propertyNameGenerator,
                    includeType = true
                )
                append("  $propertyName = $getterCall, // $comment\n")
            }
            append(")")
        }
        fnBld.addStatement(constructorCall)
    }

    /**
     * Generates executeAsList implementation for collection mapping queries.
     * This reads all joined rows, groups them by the main entity, and creates mapped objects
     * with collections.
     */
    private fun addCollectionMappingExecuteAsListImplementation(
        b: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        paramsString: String
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val mappingPlan = statement.mappingPlan
        val collectionFields = mappingPlan.includedCollectionFields
        if (collectionFields.isEmpty()) return
        // Validate that statement-level collectionKey is provided when there are collection fields
        if (statement.annotations.collectionKey == null || statement.annotations.collectionKey.isBlank()) {
            throw IllegalArgumentException(
                "Statement-level annotation '${AnnotationConstants.COLLECTION_KEY}' is required when there are " +
                        "fields with '${AnnotationConstants.MAPPING_TYPE}=collection'. Found collection " +
                        "fields: ${collectionFields.map { it.annotations.propertyName ?: it.src.fieldName }}"
            )
        }
        // Determine the grouping key from statement-level collectionKey annotation
        // (validation that collectionKey exists is done above)
        val collectionKey = statement.annotations.collectionKey
        val groupingKey = if (collectionKey.contains(".")) {
            // Format: "p.id" -> find the field and convert to property name
            val (tableAlias, columnName) = collectionKey.split(".", limit = 2)
            val matchingField = statement.fields.find { field ->
                field.src.tableName == tableAlias && field.src.originalColumnName == columnName
            }
            if (matchingField != null) {
                // Use the actual property name from the field (considering propertyName annotation)
                getPropertyName(matchingField, statement.annotations.propertyNameGenerator)
            } else {
                throw IllegalArgumentException("Statement-level collectionKey '$collectionKey' not found in SELECT statement")
            }
        } else {
            // Format: "person_id" -> find the field with this alias
            val matchingField = statement.fields.find { field ->
                field.src.fieldName == collectionKey
            }
            if (matchingField != null) {
                // Use the actual property name from the field (considering propertyName annotation)
                getPropertyName(matchingField, statement.annotations.propertyNameGenerator)
            } else {
                throw IllegalArgumentException("Statement-level collectionKey '$collectionKey' not found in SELECT statement")
            }
        }
        // Get the joined class name with correct scope
        val joinedClassFullName = if (statement.annotations.queryResult != null) {
            "${statement.annotations.queryResult}_Joined"
        } else {
            val resultClassName = "${pascalize(namespace)}${className}Result"
            "${resultClassName}_Joined"
        }
        b.line("// Read all joined rows first")
        b.line("val joinedRows = mutableListOf<$joinedClassFullName>()")
        b.line("while (statement.step()) {")
        b.indent(by = 2) { b.line("joinedRows.add($capitalizedNamespace.$className.readJoinedStatementResult($paramsString))") }
        b.line("}")
        b.line("")
        // Determine the type of the grouping key field using the helper function
        val groupingField = findFieldByCollectionKey(collectionKey, statement.fields)
        val groupingKeyType = if (groupingField != null) {
            val property =
                selectFieldGenerator.generateProperty(
                    groupingField,
                    PropertyNameGeneratorType.LOWER_CAMEL_CASE
                )
            property.type.copy(nullable = false)
                .toString() // Remove nullability for the Map key type
        } else {
            "Any" // Fallback, though this should not happen due to validation above
        }
        b.line("// Group joined rows by $groupingKey")
        b.line("val groupedRows: Map<$groupingKeyType, List<$joinedClassFullName>> = joinedRows.groupBy { it.$groupingKey }")
        b.line("")
        b.line("// Create mapped objects with collections")
        b.line("groupedRows.map { (_, rowsForEntity: List<$joinedClassFullName>) ->")
        b.indent(by = 2) {
            b.line("val firstRow = rowsForEntity.first()")
            b.line("$resultType(")
            b.indent(by = 2) {
                val ctorBlocks = mutableListOf<List<String>>()
                // Regular fields
                val regularFields = mappingPlan.regularFields
                val joinedNameMap = computeJoinedNameMap(statement)
                regularFields.forEach { field ->
                    val prop = getPropertyName(field, statement.annotations.propertyNameGenerator)
                    val key = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName.orEmpty(), field.src.fieldName)
                    val joinedProp = joinedNameMap[key] ?: prop
                    val comment = buildFieldDebugComment(
                        field,
                        statement.src,
                        statement.annotations.propertyNameGenerator,
                        includeType = true
                    )
                    val block = mutableListOf<String>()
                    if (comment.isNotEmpty()) {
                        block += "// $comment"
                    }
                    block += "$prop = firstRow.$joinedProp"
                    ctorBlocks += block
                }
                // Per-row dynamic fields (excluding those that belong to collection elements)
                val collectionAliasPaths = collectionFields.mapNotNull { field ->
                    field.aliasPath.takeIf { it.isNotEmpty() }?.map { alias -> alias.lowercase() }
                }

                val perRowDynamicFields = mappingPlan.includedPerRowEntries.filter { entry ->
                    val aliasPath = entry.field.aliasPathLowercased()
                    aliasPath.isEmpty() || !collectionAliasPaths.any { prefix ->
                        aliasPath.startsWithAliasPath(prefix) && aliasPath.size > prefix.size
                    }
                }.map { it.field }
                perRowDynamicFields.forEach { dynamicField ->
                    val prop =
                        getPropertyName(dynamicField, statement.annotations.propertyNameGenerator)
                    val mappingCode = generateDynamicFieldMappingCodeFromJoined(
                        dynamicField,
                        statement,
                        sourceVar = "firstRow",
                        rowsVar = "rowsForEntity",
                        baseIndentLevel = 4
                    )
                    val comment = buildFieldDebugComment(
                        dynamicField,
                        statement.src,
                        statement.annotations.propertyNameGenerator
                    )
                    val block = mutableListOf<String>()
                    if (comment.isNotEmpty()) {
                        block += "// $comment"
                    }
                    block += "$prop = $mappingCode"
                    ctorBlocks += block
                }
                // Entity dynamic fields (excluding those that belong to collection elements)
                val entityDynamicFields = mappingPlan.includedEntityEntries.filter { entry ->
                    val aliasPath = entry.field.aliasPathLowercased()
                    aliasPath.isEmpty() || !collectionAliasPaths.any { prefix ->
                        aliasPath.startsWithAliasPath(prefix) && aliasPath.size > prefix.size
                    }
                }.map { it.field }
                entityDynamicFields.forEach { dynamicField ->
                    val prop =
                        getPropertyName(dynamicField, statement.annotations.propertyNameGenerator)
                    val mappingCode = generateDynamicFieldMappingCodeFromJoined(
                        dynamicField,
                        statement,
                        sourceVar = "firstRow",
                        rowsVar = "rowsForEntity",
                        baseIndentLevel = 4
                    )
                    val comment = buildFieldDebugComment(
                        dynamicField,
                        statement.src,
                        statement.annotations.propertyNameGenerator
                    )
                    val block = mutableListOf<String>()
                    if (comment.isNotEmpty()) {
                        block += "// $comment"
                    }
                    block += "$prop = $mappingCode"
                    ctorBlocks += block
                }
                // Collection fields as multi-line blocks (reuse the collectionFields from above)
                collectionFields.forEach { collectionField ->
                    val prop = getPropertyName(
                        collectionField,
                        statement.annotations.propertyNameGenerator
                    )
                    val selectStatement = statement.src
                    val mapping = DynamicFieldMapper.createDynamicFieldMappings(
                        selectStatement,
                        listOf(collectionField)
                    ).firstOrNull()
                    val block = mutableListOf<String>()
                    val comment = buildFieldDebugComment(
                        collectionField,
                        selectStatement,
                        statement.annotations.propertyNameGenerator
                    )
                    if (comment.isNotEmpty()) {
                        block += "// $comment"
                    }
                    if (mapping != null && mapping.columns.isNotEmpty()) {
                        val collectionExpr = generateCollectionMappingCode(
                            collectionField,
                            mapping,
                            statement,
                            sourceVar = "firstRow",
                            rowsVar = "rowsForEntity",
                            baseIndentLevel = 3
                        )
                        val exprLines = collectionExpr.split("\n")
                        if (exprLines.isNotEmpty()) {
                            block += "$prop = ${exprLines.first()}"
                            exprLines.drop(1).forEach { line -> block += line }
                        } else {
                            block += "$prop = emptyList()"
                        }
                    } else {
                        block += "$prop = emptyList()"
                    }
                    ctorBlocks += block
                }

                // Emit all constructor blocks, comma after each block except the last
                ctorBlocks.forEachIndexed { idx, lines ->
                    val isLastBlock = idx == ctorBlocks.lastIndex
                    lines.forEachIndexed { li, ln ->
                        val isLastLine = li == lines.lastIndex
                        val suffix = if (isLastLine && !isLastBlock) "," else ""
                        b.line(ln + suffix)
                    }
                }
            }
            b.line(")")
        }
        b.line("}")
    }

    private fun buildFieldDebugComment(
        field: AnnotatedSelectStatement.Field,
        selectStatement: SelectStatement,
        propertyNameGenerator: PropertyNameGeneratorType,
        includeType: Boolean = false
    ): String {
        val parts = mutableListOf<String>()
        if (includeType) {
            val sqlType = field.src.dataType
            val kotlinType = selectFieldGenerator
                .generateProperty(field, propertyNameGenerator)
                .type
                .toString()
            parts += "type=$sqlType -> $kotlinType"
        }
        field.src.fieldName.takeIf { it.isNotBlank() }?.let { label ->
            parts += "select=$label"
        }
        val sourceAlias = when {
            !field.annotations.sourceTable.isNullOrBlank() -> field.annotations.sourceTable
            !field.src.tableName.isNullOrBlank() -> field.src.tableName
            else -> null
        }
        sourceAlias?.let { alias ->
            val target = selectStatement.tableAliases[alias] ?: alias
            val descriptor = if (!alias.equals(target, ignoreCase = true)) "$alias->$target" else alias
            parts += "source=$descriptor"
        }
        field.src.originalColumnName.takeIf { it.isNotBlank() && it != field.src.fieldName }?.let { column ->
            parts += "column=$column"
        }
        if (field.annotations.aliasPrefix != null) {
            val prefix = field.annotations.aliasPrefix
            if (!prefix.isNullOrBlank()) {
                parts += "prefix=$prefix"
            }
        }
        if (field.aliasPath.isNotEmpty()) {
            parts += "aliasPath=${field.aliasPath.joinToString("->")}"
        }
        field.annotations.mappingType?.let { mappingType ->
            parts += "mapping=${mappingType.lowercase()}"
            field.annotations.collectionKey?.takeIf { it.isNotBlank() }?.let { key ->
                parts += "collectionKey=$key"
            }
        }
        field.annotations.notNull?.let { parts += "notNull=$it" }
        return parts.joinToString(", ")
    }

    /**
     * Generates getter call for joined data classes with proper JOIN nullability handling.
     * For fields from joined tables, always adds NULL checks regardless of schema nullability.
     */
    private fun generateJoinedGetterCall(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        allFields: List<AnnotatedSelectStatement.Field>
    ): String {
        // Determine if this property's nullability in the Joined data class comes from JOINs
        val fieldTableAlias = field.src.tableName
        val mainTableAlias = dataStructCodeGenerator.findMainTableAlias(allFields)
        val joinedNullable = if (fieldTableAlias.isNotBlank()) {
            // If the field is not from the main table, Joined property must be nullable
            fieldTableAlias != mainTableAlias
        } else {
            // Fallback for cases without explicit alias metadata: rely on aliasPrefix from dynamic fields
            val dynAliasPrefixes = allFields
                .filter { it.annotations.isDynamicField }
                .mapNotNull { it.annotations.aliasPrefix }
                .filter { it.isNotBlank() }
            val visibleName = field.src.fieldName
            dynAliasPrefixes.any { prefix -> visibleName.startsWith(prefix) }
        }

        // Base (schema) nullability for the column itself
        val baseDesiredType = selectFieldGenerator
            .generateProperty(field, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            .type

        // Only use the "joined" null guard when nullability is introduced by JOINs.
        // If the base type is nullable, generateGetterCallInternalWithPrefixes will handle that path.
        val isFromJoinedForGetter = joinedNullable && !baseDesiredType.isNullable

        val aliasPrefixes = adapterConfig.collectAliasPrefixesForSelect(statement)
        return generateGetterCallInternalWithPrefixes(
            statement = statement,
            field = field,
            columnIndex = columnIndex,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = isFromJoinedForGetter,
            tableAliases = statement.src.tableAliases,
            aliasPrefixes = aliasPrefixes,
        )
    }

    /**
     * Helper function to find a field by collectionKey.
     * Handles both "alias.column" and "aliased_column" formats.
     */
    private fun findFieldByCollectionKey(
        collectionKey: String,
        fields: List<AnnotatedSelectStatement.Field>
    ): AnnotatedSelectStatement.Field? {
        return if (collectionKey.contains(".")) {
            // Format: "a.id" -> find the field with this table alias and column
            val (tableAlias, columnName) = collectionKey.split(".", limit = 2)
            fields.find { field ->
                field.src.tableName == tableAlias && field.src.originalColumnName == columnName
            }
        } else {
            // Format: "address_id" -> find the field with this alias in SELECT
            fields.find { field ->
                field.src.fieldName == collectionKey
            }
        }
    }

    /**
     * Finds the correct path to access a unique field in a nested result class that requires nested construction.
     * For example, if we have ActivityDetailedDoc with nested 'main' object containing 'id',
     * this would return "main.id" for the unique field "id".
     */
    private fun findDistinctByPathForNestedConstruction(
        targetPropertyType: String,
        uniqueField: String,
        statement: AnnotatedSelectStatement
    ): String {
        val actualType = GenericTypeParser.extractFirstTypeArgument(targetPropertyType)
        val resultClassName = actualType.substringAfterLast('.')

        // For known result classes with nested construction, map the unique field to the correct nested path
        return when (resultClassName) {
            "ActivityDetailedDoc" -> {
                // For ActivityDetailedDoc, the unique field (like "id") should be accessed via "main.id"
                // since the main ActivityDoc object contains the primary key
                when (uniqueField) {
                    "id" -> "main.id"
                    else -> "main.$uniqueField" // Default to main object for other fields
                }
            }
            else -> {
                // For other result classes, try to infer the correct nested path
                // Look for dynamic fields in the current statement to find which nested object should contain the unique field
                val dynamicFields = statement.fields.filter {
                    it.annotations.isDynamicField && it.annotations.mappingType != null
                }

                // Find the entity mapping (most likely to contain the primary key)
                val entityField = dynamicFields.find { it.annotations.mappingType == "entity" }
                if (entityField != null) {
                    val entityFieldName = entityField.annotations.propertyName
                        ?: statement.annotations.propertyNameGenerator.convertToPropertyName(entityField.src.fieldName)
                    return "$entityFieldName.$uniqueField"
                }

                // Fallback: use the unique field directly
                uniqueField
            }
        }
    }

    private fun AnnotatedSelectStatement.Field.aliasPathLowercased(): List<String> =
        aliasPath.map { it.lowercase() }

    private fun List<String>.startsWithAliasPath(prefix: List<String>): Boolean {
        if (prefix.isEmpty() || prefix.size > size) return false
        prefix.indices.forEach { idx ->
            if (this[idx] != prefix[idx]) return false
        }
        return true
    }

    /**
     * Finds the unique identifier field for a collection based on the collectionKey annotation.
     * This is used for deduplication to remove duplicates caused by Cartesian products in multiple JOINs.
     * Validates that the collectionKey refers to an actual field in the SELECT statement.
     */
    private fun findUniqueFieldForCollection(
        collectionField: AnnotatedSelectStatement.Field,
        selectStatement: SelectStatement?,
        annotatedStatement: AnnotatedSelectStatement
    ): String? {
        val collectionKey = collectionField.annotations.collectionKey
        if (collectionKey == null || collectionKey.isBlank()) {
            return null
        }
        // Parse collectionKey using the helper function to eliminate code duplication
        val matchingField = findFieldByCollectionKey(collectionKey, annotatedStatement.fields)
        if (matchingField == null) {
            // Create a more helpful error message showing both table names and aliases
            val availableFields = annotatedStatement.fields.map { field ->
                val tableInfo = if (field.src.tableName.isNotBlank()) {
                    "${field.src.tableName}.${field.src.originalColumnName}"
                } else {
                    field.src.originalColumnName
                }
                "$tableInfo AS ${field.src.fieldName}"
            }
            throw IllegalArgumentException(
                "collectionKey '$collectionKey' not found in SELECT statement. Available fields: $availableFields"
            )
        }
        // Convert to the target property name, considering aliasPrefix
        val mapping = if (selectStatement != null) {
            DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(collectionField))
                .firstOrNull()
        } else {
            null
        }
        if (mapping != null) {
            // Find the corresponding column in the mapping
            val mappingColumn = mapping.columns.find { it.fieldName == matchingField.src.fieldName }
            if (mappingColumn != null) {
                // Use the target property name from the mapping (which handles aliasPrefix)
                val basePropertyName = if (mapping.aliasPrefix != null &&
                    mappingColumn.fieldName.startsWith(mapping.aliasPrefix)
                ) {
                    // Remove the prefix for the target property name
                    mappingColumn.fieldName.removePrefix(mapping.aliasPrefix)
                } else {
                    // Use the original column name
                    mappingColumn.originalColumnName
                }
                return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(
                    basePropertyName
                )
            }
        }
        // Fallback: convert the field name directly
        return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(
            matchingField.src.fieldName
        )
    }

    /**
     * Generates constructor arguments from a dynamic field mapping.
     */
    private fun generateConstructorArgumentsFromMapping(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        additionalIndent: Int = 0,
        enforceNonNull: Boolean = false,
        rowsVar: String? = null,
        baseIndentLevel: Int = 2,
        aliasPath: List<String>? = null
    ): String {
        // Check if the target type requires nested construction based on known result classes with dynamic fields
        val targetPropertyType = mapping.propertyType
        if (requiresNestedConstruction(targetPropertyType)) {
            return generateNestedResultConstructor(
                targetPropertyType,
                mapping,
                statement,
                sourceVariableName,
                additionalIndent,
                enforceNonNull,
                rowsVar,
                baseIndentLevel
            )
        }

        val indent = " ".repeat(6 + additionalIndent)
        val joinedNameMap = computeJoinedNameMap(statement)
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val aliasPrefix = mapping.aliasPrefix?.takeIf { it.isNotBlank() }
        val preferSuffixed = shouldPreferSuffixed(aliasPath, mapping, statement)
        val assignments = linkedMapOf<String, ColumnAssignment>()

        mapping.columns.forEachIndexed { index, column ->
            if (DynamicFieldUtils.isNestedAlias(column.fieldName, mapping.aliasPrefix)) {
                return@forEachIndexed
            }
            val effectiveJoinedPropertyName = resolveJoinedPropertyName(
                column = column,
                mapping = mapping,
                statement = statement,
                aliasPath = aliasPath,
                joinedNameMap = joinedNameMap
            )

            val normalizedFieldName = column.fieldName.substringBefore(':')
            val baseColumnName = column.originalColumnName.ifBlank { normalizedFieldName }
            val strippedBaseName = if (aliasPrefix != null && baseColumnName.startsWith(aliasPrefix)) {
                baseColumnName.removePrefix(aliasPrefix)
            } else {
                baseColumnName
            }

            val resolvedNameFromSource = mapping.sourceTableAlias
                ?.takeIf { it.isNotBlank() }
                ?.let { alias -> findOriginalColumnPropertyName(strippedBaseName, alias, statement) }

            val parameterName = resolvedNameFromSource
                ?: propertyNameGenerator.convertToPropertyName(strippedBaseName)

            val expectedJoinedName = propertyNameGenerator.convertToPropertyName(normalizedFieldName)
            val isSuffixed = effectiveJoinedPropertyName != expectedJoinedName

            val isTargetNullable = isTargetPropertyNullable(column)
            val valueExpression = when {
                enforceNonNull && !isTargetNullable -> "$sourceVariableName.$effectiveJoinedPropertyName!!"
                isTargetNullable -> "$sourceVariableName.$effectiveJoinedPropertyName"
                else -> "$sourceVariableName.$effectiveJoinedPropertyName!!"
            }

            val disambiguationIndex = column.fieldName.substringAfter(':', "").toIntOrNull()

            val candidate = ColumnAssignment(
                rendered = "$parameterName = $valueExpression",
                isSuffixed = isSuffixed,
                order = index,
                disambiguationIndex = disambiguationIndex
            )

            val existing = assignments[parameterName]
            if (existing == null) {
                assignments[parameterName] = candidate
            } else if (shouldReplaceAssignment(existing, candidate, preferSuffixed)) {
                assignments[parameterName] = candidate
            }
        }

        return assignments.values
            .sortedBy { it.order }
            .joinToString(",\n$indent") { it.rendered }
    }

    private fun shouldPreferSuffixed(
        aliasPath: List<String>?,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement
    ): Boolean {
        if (aliasPath.isNullOrEmpty()) return false
        val rootAliases = collectRootAliases(statement)
        val aliasPathMatchesRoot = aliasPath.any { rootAliases.contains(it) }
        val sourceAliasMatchesRoot = !mapping.sourceTableAlias.isNullOrBlank() &&
            rootAliases.contains(mapping.sourceTableAlias)
        return !(aliasPathMatchesRoot || sourceAliasMatchesRoot)
    }

    private fun shouldReplaceAssignment(
        existing: ColumnAssignment,
        candidate: ColumnAssignment,
        preferSuffixed: Boolean
    ): Boolean {
        return when {
            preferSuffixed && candidate.isSuffixed && !existing.isSuffixed -> true
            !preferSuffixed && !candidate.isSuffixed && existing.isSuffixed -> true
            preferSuffixed && candidate.isSuffixed && existing.isSuffixed -> {
                val existingIndex = existing.disambiguationIndex ?: -1
                val candidateIndex = candidate.disambiguationIndex ?: -1
                candidateIndex > existingIndex
            }
            else -> false
        }
    }

    /**
     * Checks if the given property type requires nested construction.
     * This is true for result classes that have dynamic field mappings.
     */
    private fun requiresNestedConstruction(propertyType: String): Boolean {
        val actualType = GenericTypeParser.extractFirstTypeArgument(propertyType)
        val nestedStatement = dataStructCodeGenerator.findSelectStatementByResultName(actualType)
            ?: dataStructCodeGenerator.findSelectStatementByResultName(actualType.substringAfterLast('.'))
        if (nestedStatement != null) {
            val nestedPlan = nestedStatement.mappingPlan
            if (nestedPlan.includedDynamicEntries.isNotEmpty()) {
                return true
            }
        }

        val resultClassName = actualType.substringAfterLast('.')
        return resultClassName.contains("Detailed") ||
               resultClassName.contains("WithActivities") ||
               resultClassName.contains("WithProgramItems") ||
               resultClassName.contains("WithAddresses")
    }

    /**
     * Generates constructor arguments for a result type that requires nested construction.
     */
    private fun generateNestedResultConstructor(
        targetPropertyType: String,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        additionalIndent: Int = 0,
        enforceNonNull: Boolean,
        rowsVar: String? = null,
        baseIndentLevel: Int = 2
    ): String {
        val indent = "  ".repeat(additionalIndent + 3)
        val actualType = GenericTypeParser.extractFirstTypeArgument(targetPropertyType)
        val resultClassName = actualType.substringAfterLast('.')

        val nestedStatement = dataStructCodeGenerator.findSelectStatementByResultName(actualType)
            ?: dataStructCodeGenerator.findSelectStatementByResultName(resultClassName)

        if (nestedStatement != null) {
            return generateResultConstructorFromStatement(
                nestedStatement,
                statement,
                sourceVariableName,
                rowsVar,
                additionalIndent,
                enforceNonNull,
                baseIndentLevel
            )
        }

        val aliasPrefix = mapping.aliasPrefix
        return if (aliasPrefix != null) {
            generateNestedObjectConstruction(actualType, aliasPrefix, statement, sourceVariableName, indent, enforceNonNull)
        } else {
            generateFlatFieldMapping(mapping, statement, sourceVariableName, additionalIndent, enforceNonNull)
        }
    }

    /**
     * Generates nested object construction for entity/perRow mappings.
     */
    private fun generateNestedObjectConstruction(
        targetType: String,
        aliasPrefix: String?,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        indent: String,
        enforceNonNull: Boolean
    ): String {
        if (aliasPrefix == null) {
            return "null"
        }

        // Find all fields that match this alias prefix by converting the prefix to the joined field naming pattern
        // For example: "act__" becomes "act", "category__" becomes "category", "schedule__" becomes "schedule"
        val joinedFieldPrefix = aliasPrefix.removeSuffix("__").removeSuffix("_")

        val prefixedFields = statement.fields.filter { field ->
            field.src.fieldName.startsWith(aliasPrefix)
        }

        // Generate constructor arguments for the nested object
        val nestedArgs = prefixedFields.map { field ->
            val baseFieldName = field.src.fieldName.removePrefix(aliasPrefix)
            val parameterName = statement.annotations.propertyNameGenerator.convertToPropertyName(baseFieldName)

            // Convert the aliased field name to the joined class field name
            // For example: "act__id" becomes "actId", "category__doc_id" becomes "categoryDocId"
            val joinedFieldName = joinedFieldPrefix + statement.annotations.propertyNameGenerator.convertToPropertyName(baseFieldName).replaceFirstChar { it.uppercase() }

            val fieldValue = if (enforceNonNull) {
                "$sourceVariableName.$joinedFieldName!!"
            } else {
                val isNullable = isTargetPropertyNullable(field.src)
                if (isNullable) {
                    "$sourceVariableName.$joinedFieldName"
                } else {
                    "$sourceVariableName.$joinedFieldName!!"
                }
            }
            "$parameterName = $fieldValue"
        }

        val nestedIndent = indent + "  "
        val simpleTypeName = targetType.substringAfterLast('.')
        return "$simpleTypeName(\n$nestedIndent${nestedArgs.joinToString(",\n$nestedIndent")}\n$indent)"
    }

    private fun generateResultConstructorFromStatement(
        targetStatement: AnnotatedSelectStatement,
        parentStatement: AnnotatedSelectStatement,
        sourceVar: String,
        rowsVar: String?,
        additionalIndent: Int,
        enforceNonNull: Boolean,
        baseIndentLevel: Int
    ): String {
        val indent = "  ".repeat(additionalIndent + 3)
        val propertyIndent = indent
        val propertyNameGenerator = targetStatement.annotations.propertyNameGenerator
        val mappingPlan = targetStatement.mappingPlan
        val joinedNameMap = computeJoinedNameMap(parentStatement)
        val properties = mutableListOf<String>()

        mappingPlan.regularFields.forEach { field ->
                val propName = getPropertyName(field, propertyNameGenerator)
                val tableAlias = field.src.tableName.orEmpty()
                val candidateKeys = buildList {
                    add(JoinedPropertyNameResolver.JoinedFieldKey(tableAlias, field.src.fieldName))
                    field.src.originalColumnName.takeIf { it.isNotBlank() }?.let { original ->
                        add(JoinedPropertyNameResolver.JoinedFieldKey(tableAlias, original))
                    }
                }
                val joinedPropertyName = candidateKeys.firstNotNullOfOrNull { key -> joinedNameMap[key] }
                    ?: parentStatement.annotations.propertyNameGenerator
                        .convertToPropertyName(field.src.fieldName)
                val isNullable = isTargetPropertyNullable(field.src)
                val expression = when {
                    enforceNonNull && !isNullable -> "$sourceVar.$joinedPropertyName!!"
                    isNullable -> "$sourceVar.$joinedPropertyName"
                    else -> "$sourceVar.$joinedPropertyName!!"
                }
                properties += "$propName = $expression"
            }

        mappingPlan.includedDynamicFields.forEach { field ->
                val propName = getPropertyName(field, propertyNameGenerator)
            val fieldExpression = generateDynamicFieldMappingCodeFromJoined(
                field,
                parentStatement,
                sourceVar,
                rowsVar = rowsVar,
                baseIndentLevel = baseIndentLevel + additionalIndent + 2
            )
                properties += "$propName = $fieldExpression"
            }

        return if (properties.isEmpty()) {
            ""
        } else {
            properties.joinToString(",\n$propertyIndent")
        }
    }

    private fun generateCollectionMappingCode(
        dynamicField: AnnotatedSelectStatement.Field,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVar: String,
        rowsVar: String,
        baseIndentLevel: Int
    ): String {
        val propertyType = dynamicField.annotations.propertyType ?: "kotlin.collections.List<Any>"
        val elementType = GenericTypeParser.extractFirstTypeArgument(propertyType)
        val elementSimpleName = elementType.substringAfterLast('.')
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val relevantColumns = mapping.columns.filterNot {
            DynamicFieldUtils.isNestedAlias(it.fieldName, mapping.aliasPrefix)
        }
        val nullCondition = relevantColumns.joinToString(" && ") { column ->
            val propName = propertyNameGenerator.convertToPropertyName(column.fieldName)
            "row.$propName == null"
        }.ifBlank { "false" }
        val indent = " ".repeat(baseIndentLevel * 2)
        val lines = mutableListOf<String>()
        lines += rowsVar
        lines += "$indent  .filter { row -> !($nullCondition) }"

        val requiresNested = requiresNestedConstruction(elementType)
        val rawGroupByColumn = mapping.groupByColumn?.takeIf { it.isNotBlank() }
            ?: dynamicField.annotations.collectionKey?.takeIf { it.isNotBlank() }
        val groupByProperty = rawGroupByColumn?.let { groupColumn ->
            propertyNameGenerator.convertToPropertyName(groupColumn)
        }

        if (requiresNested && groupByProperty != null) {
            lines += "$indent  .groupBy { row -> row.$groupByProperty }"
            lines += "$indent  .map { (_, rowsForNested) ->"
            lines += "$indent    val firstNestedRow = rowsForNested.first()"
            val nestedArgs = generateNestedResultConstructor(
                elementType,
                mapping,
                statement,
                "firstNestedRow",
                additionalIndent = 4,
                enforceNonNull = dynamicField.annotations.notNull == true,
                rowsVar = "rowsForNested",
                baseIndentLevel = baseIndentLevel + 2
            )
            lines += "$indent    $elementSimpleName("
            if (nestedArgs.isNotBlank()) {
                lines += "$indent      $nestedArgs"
            }
            lines += "$indent    )"
            lines += "$indent  }"
        } else {
            lines += "$indent  .map { row ->"
            val elementArgs = generateConstructorArgumentsFromMapping(
                mapping,
                statement,
                "row",
                additionalIndent = 4,
                enforceNonNull = dynamicField.annotations.notNull == true,
                rowsVar = rowsVar,
                baseIndentLevel = baseIndentLevel + 2,
                aliasPath = dynamicField.aliasPath
            )
            lines += "$indent    $elementSimpleName("
            if (elementArgs.isNotBlank()) {
                lines += "$indent      $elementArgs"
            }
            lines += "$indent    )"
            lines += "$indent  }"
        }

        val unique = findUniqueFieldForCollection(dynamicField, statement.src, statement)
        if (unique != null) {
            val distinctPath = if (requiresNested) {
                findDistinctByPathForNestedConstruction(elementType, unique, statement)
            } else {
                unique
            }
            lines += "$indent  .distinctBy { it.$distinctPath }"
        }

        return lines.joinToString("\n")
    }

    /**
     * Generates flat field mapping as a fallback for nested construction.
     */
    private fun generateFlatFieldMapping(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        additionalIndent: Int,
        enforceNonNull: Boolean
    ): String {
        val indent = "  ".repeat(additionalIndent + 3)

        val constructorArgs = mapping.columns
            .filterNot { DynamicFieldUtils.isNestedAlias(it.fieldName, mapping.aliasPrefix) }
            .map { column ->
            val parameterName = findOriginalColumnPropertyName(
                column.originalColumnName.ifBlank { column.fieldName },
                mapping.sourceTableAlias ?: "",
                statement
            ) ?: statement.annotations.propertyNameGenerator.convertToPropertyName(
                column.originalColumnName.ifBlank { column.fieldName }
            )

            val joinedPropertyName = if (column.originalColumnName.isNotBlank()) {
                val basePropertyName = column.originalColumnName
                statement.annotations.propertyNameGenerator.convertToPropertyName(basePropertyName)
            } else {
                statement.annotations.propertyNameGenerator.convertToPropertyName(column.fieldName)
            }

            val isTargetNullable = isTargetPropertyNullable(column)
            val valueExpression = if (enforceNonNull) {
                "$sourceVariableName.$joinedPropertyName!!"
            } else if (isTargetNullable) {
                "$sourceVariableName.$joinedPropertyName"
            } else {
                "$sourceVariableName.$joinedPropertyName!!"
            }
            "$parameterName = $valueExpression"
        }

        return constructorArgs.joinToString(",\n$indent")
    }

    /**
     * Finds the original column's propertyName annotation for entity mapping parameter generation.
     * This ensures that entity mapping uses the correct parameter names that match the target class constructor.
     */
    private fun findOriginalColumnPropertyName(
        baseColumnName: String,
        sourceTableAlias: String,
        statement: AnnotatedSelectStatement
    ): String? {
        // Find the table name from the alias
        val tableName = statement.src.tableAliases[sourceTableAlias] ?: sourceTableAlias

        // Find the table definition
        val table = dataStructCodeGenerator.createTableStatements.find {
            it.src.tableName.equals(tableName, ignoreCase = true)
        } ?: return null

        // Find the column by name
        val column = table.columns.find {
            it.src.name.equals(baseColumnName, ignoreCase = true)
        } ?: return null

        // Check if the column has a propertyName annotation
        val propertyName = column.annotations[AnnotationConstants.PROPERTY_NAME] as? String
        if (propertyName != null) {
            return propertyName
        }

        // If no propertyName annotation, use the default property name generation
        return statement.annotations.propertyNameGenerator.convertToPropertyName(baseColumnName)
    }

    /**
     * Generates constructor arguments for a nested shared result type by recursively constructing its nested objects.
     */
    private fun generateNestedSharedResultConstructor(
        targetPropertyType: String,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        additionalIndent: Int = 0,
        enforceNonNull: Boolean = false
    ): String {
        val indent = " ".repeat(6 + additionalIndent)

        // Handle generic types like List<ActivityQuery.SharedResult.ActivityDetailedDoc>
        val actualType = GenericTypeParser.extractFirstTypeArgument(targetPropertyType)

        // Find the shared result definition by matching the exact type name
        val allSharedResults = dataStructCodeGenerator.getAllSharedResults()
        val sharedResult = allSharedResults.find { sharedResult ->
            // Convert snake_case namespace to PascalCase
            val pascalCaseNamespace = pascalize(sharedResult.namespace)
            val expectedTypeName = "${pascalCaseNamespace}Query.SharedResult.${sharedResult.name}"
            expectedTypeName == actualType
        } ?: return "null // Shared result type '$actualType' not found"

        // Check if this shared result type has dynamic field mappings (nested structure)
        if (sharedResult.hasDynamicFieldMapping()) {
            // This is a nested shared result type with its own dynamic fields (like ActivityDetailedDoc)
            // We need to construct each dynamic field recursively
            return generateNestedDynamicFieldConstructor(sharedResult, statement, sourceVariableName, indent, enforceNonNull)
        } else {
            // This is a simple shared result type without dynamic fields
            // Map individual fields directly to constructor parameters
            return generateSimpleSharedResultConstructor(sharedResult, mapping, statement, sourceVariableName, indent, enforceNonNull)
        }
    }

    /**
     * Generates constructor arguments for a shared result type with dynamic field mappings.
     * This handles cases like ActivityDetailedDoc which has nested objects (main, category, schedule).
     */
    private fun generateNestedDynamicFieldConstructor(
        sharedResult: SharedResultManager.SharedResult,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        indent: String,
        enforceNonNull: Boolean
    ): String {
        // Get the dynamic fields from the shared result
        val dynamicFields = sharedResult.fields.filter { it.annotations.isDynamicField && it.annotations.mappingType != null }

        return dynamicFields.joinToString(",\n$indent") { dynamicField ->
            val parameterName = dynamicField.annotations.propertyName
                ?: sharedResult.propertyNameGenerator.convertToPropertyName(dynamicField.src.fieldName)

            // Recursively construct the nested object
            val nestedConstructor = generateDynamicFieldMappingCodeFromJoined(
                dynamicField,
                statement,
                sourceVariableName,
                rowsVar = null,
                baseIndentLevel = 4
            )

            "$parameterName = $nestedConstructor"
        }
    }

    /**
     * Generates constructor arguments for a simple shared result type without dynamic fields.
     */
    private fun generateSimpleSharedResultConstructor(
        sharedResult: SharedResultManager.SharedResult,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        indent: String,
        enforceNonNull: Boolean
    ): String {
        // Generate constructor arguments for each parameter of the shared result type
        return sharedResult.fields
            .filter { !it.annotations.isDynamicField } // Only non-dynamic fields become constructor parameters
            .joinToString(",\n$indent") { field ->
                val parameterName = field.annotations.propertyName
                    ?: sharedResult.propertyNameGenerator.convertToPropertyName(field.src.fieldName)

                // Create a synthetic mapping for this field that uses the original mapping's alias prefix
                // but maps to the shared result field
                val fieldMapping = DynamicFieldMapper.DynamicFieldMapping(
                    fieldName = parameterName,
                    sourceTableAlias = mapping.sourceTableAlias,
                    aliasPrefix = mapping.aliasPrefix,
                    mappingType = AnnotationConstants.MappingType.PER_ROW,
                    propertyType = "String", // Not used for field lookup
                    columns = listOf(
                        SelectStatement.FieldSource(
                            fieldName = field.src.fieldName,
                            tableName = mapping.sourceTableAlias,
                            originalColumnName = field.src.originalColumnName,
                            dataType = "TEXT"
                        )
                    ),
                    aliasPath = mapping.aliasPath,
                    groupByColumn = null
                )

                // Find the corresponding joined field using the synthetic mapping
                val joinedFieldName = findJoinedFieldName(field, fieldMapping, statement)

                val valueExpression = if (enforceNonNull) {
                    "$sourceVariableName.$joinedFieldName!!"
                } else {
                    val isTargetNullable = isTargetPropertyNullable(field.src)
                    if (isTargetNullable) {
                        "$sourceVariableName.$joinedFieldName"
                    } else {
                        "$sourceVariableName.$joinedFieldName!!"
                    }
                }

                "$parameterName = $valueExpression"
            }
    }

    /**
     * Finds the joined field name for a given field in the context of a dynamic field mapping.
     */
    private fun findJoinedFieldName(
        field: AnnotatedSelectStatement.Field,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement
    ): String {
        val joinedNameMap = computeJoinedNameMap(statement)

        // Try to find the field using the mapping's alias prefix
        val aliasPrefix = mapping.aliasPrefix ?: ""

        if (aliasPrefix.isNotEmpty()) {
            // For nested shared result construction, we need to find the joined field name
            // by combining the alias prefix with the field's property name
            val propertyName = field.annotations.propertyName
                ?: statement.annotations.propertyNameGenerator.convertToPropertyName(field.src.fieldName)

            // Try with the original column name first (this matches the SQL query structure)
            val expectedFieldNameWithColumn = aliasPrefix + field.src.originalColumnName
            val keyWithColumn = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName, expectedFieldNameWithColumn)
            val resultWithColumn = joinedNameMap[keyWithColumn]
            if (resultWithColumn != null) {
                return resultWithColumn
            }

            // Try with the property name (in case the SQL alias uses property name)
            val expectedFieldNameWithProperty = aliasPrefix + propertyName
            val keyWithProperty = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName, expectedFieldNameWithProperty)
            val resultWithProperty = joinedNameMap[keyWithProperty]
            if (resultWithProperty != null) {
                return resultWithProperty
            }

            // If still not found, try without the prefix (in case the SQL query doesn't use the alias)
            val keyWithoutPrefix = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName, field.src.fieldName)
            val resultWithoutPrefix = joinedNameMap[keyWithoutPrefix]
            if (resultWithoutPrefix != null) {
                return resultWithoutPrefix
            }

            // If still not found, fall back to generating the name
            return statement.annotations.propertyNameGenerator.convertToPropertyName(expectedFieldNameWithProperty)
        } else {
            // No alias prefix, use the field name directly
            val key = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName, field.src.fieldName)
            return joinedNameMap[key]
                ?: statement.annotations.propertyNameGenerator.convertToPropertyName(field.src.fieldName)
        }
    }
}
