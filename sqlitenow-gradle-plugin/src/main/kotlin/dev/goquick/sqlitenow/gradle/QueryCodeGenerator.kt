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
    private val selectFieldGenerator = SelectFieldCodeGenerator(
        dataStructCodeGenerator.createTableStatements,
        dataStructCodeGenerator.createViewStatements,
        packageName
    )

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
                    val executeReturningListFunction = generateExecuteQueryFunction(namespace, statement, "executeReturningList")
                    val executeReturningOneFunction = generateExecuteQueryFunction(namespace, statement, "executeReturningOne")
                    val executeReturningOneOrNullFunction = generateExecuteQueryFunction(namespace, statement, "executeReturningOneOrNull")

                    fileSpecBuilder.addFunction(executeReturningListFunction)
                    fileSpecBuilder.addFunction(executeReturningOneFunction)
                    fileSpecBuilder.addFunction(executeReturningOneOrNullFunction)
                } else {
                    // Generate single execute function for non-RETURNING queries
                    val executeFunction = generateExecuteQueryFunction(namespace, statement, "execute")
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

        // Set return type
        val resultType = ClassName(packageName, capitalizedNamespace).nestedClass(className).nestedClass("Result")
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
            val capitalizedNamespace = queryNamespaceName(namespace)
            val resultType = ClassName(packageName, capitalizedNamespace).nestedClass(className).nestedClass("Result")

            when (functionName) {
                "executeReturningList" -> {
                    val listType = ClassName("kotlin.collections", "List").parameterizedBy(resultType)
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

        addSqlStatementProcessing(fnBld, statement, namespace, className, functionName = functionName)
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
                    is AnnotatedSelectStatement -> addResultConversionAdapterParameters(fnBld, statement)
                    is AnnotatedExecuteStatement -> addResultConversionAdapterParametersForExecute(fnBld, statement)
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
                val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
                val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)

                // Skip if already processed
                if (adapterFunctionName in processedAdapters) {
                    return@forEach
                }
                processedAdapters.add(adapterFunctionName)

                // Create adapter parameter
                val baseType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
                val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
                val isNullable = column.isNullable()
                val targetType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable, packageName)

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
        filteredConfigs.forEach { config ->
            val adapterType = LambdaTypeName.get(
                parameters = arrayOf(config.inputType),
                returnType = config.outputType
            )
            val adapterParam =
                ParameterSpec.builder(config.adapterFunctionName, adapterType).build()
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
        return adapterConfigs.filter(filter).map { it.adapterFunctionName }
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
                val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
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

    // IndentedCodeBuilder moved to its own file for reuse

    /**
     * Helper function to create Joined Result type name for SELECT statements.
     * Uses SharedResult_Joined if the statement has sharedResult annotation, otherwise uses regular Result_Joined.
     */
    private fun createJoinedResultTypeName(
        namespace: String,
        statement: AnnotatedSelectStatement
    ): ClassName {
        val capitalizedNamespace = queryNamespaceName(namespace)
        return if (statement.annotations.sharedResult != null) {
            // For shared results: PersonQuery.SharedResult.PersonWithAddressRow_Joined
            ClassName(packageName, capitalizedNamespace)
                .nestedClass("SharedResult")
                .nestedClass("${statement.annotations.sharedResult}_Joined")
        } else {
            // For regular results: PersonQuery.SelectWeird.Result_Joined
            val className = statement.getDataClassName()
            ClassName(packageName, capitalizedNamespace)
                .nestedClass(className)
                .nestedClass("Result_Joined")
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
            prepareAndMaybeBindParamsLines(statement, capitalizedNamespace, className).forEach { line ->
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
        val constructorCall = buildString {
            append("return $resultType(\n")
            // No dynamic field mapping, so process all fields directly
            statement.fields.forEachIndexed { index, field ->
                if (!field.annotations.isDynamicField) {
                    val propertyName =
                        getPropertyName(field, statement.annotations.propertyNameGenerator)
                    val getterCall = generateGetterCallInternal(
                        field = field,
                        columnIndex = index,
                        propertyNameGenerator = statement.annotations.propertyNameGenerator,
                        isFromJoinedTable = false,
                    )
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
        val mappedColumns =
            DynamicFieldMapper.getMappedColumns(statement.fields, statement.src.tableAliases)
        val regularFields = statement.fields.filter {
            !it.annotations.isDynamicField && !mappedColumns.contains(it.src.fieldName)
        }
        regularFields.forEachIndexed { index, field ->
            val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
            codeBuilder.append("  $propertyName = joinedData.$propertyName,\n")
        }
        statement.fields
            .filter { it.annotations.isDynamicField }
            .forEachIndexed { index, field ->
                val propertyName =
                    getPropertyName(field, statement.annotations.propertyNameGenerator)
                if (field.annotations.mappingType != null) {
                    // Generate mapping logic for mapped dynamic fields using joined data
                    val mappingCode = generateDynamicFieldMappingCodeFromJoined(field, statement)
                    codeBuilder.append("  $propertyName = $mappingCode,\n")
                } else {
                    // Use default values for regular dynamic fields
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
        sourceVar: String = "joinedData"
    ): String {
        val selectStatement = statement.src
        // Use DynamicFieldMapper to get the mapping information
        val mappings = DynamicFieldMapper.createDynamicFieldMappings(
            selectStatement, listOf(dynamicField))
        val mapping = mappings.firstOrNull()
        if (mapping == null || mapping.columns.isEmpty()) {
            return "null // No columns found for mapping"
        }
        // Generate NULL check for all joined fields
        val nullCheckConditions = mapping.columns.map { column ->
            val fieldName = column.fieldName
            val joinedPropertyName =
                statement.annotations.propertyNameGenerator.convertToPropertyName(fieldName)
            "$sourceVar.$joinedPropertyName == null"
        }
        val nullCheckCondition = nullCheckConditions.joinToString(" && ")
        // Generate the constructor arguments with proper type conversions
        val constructorArgs = generateConstructorArgumentsFromMapping(
            mapping,
            statement,
            sourceVar,
            additionalIndent = 6,
            enforceNonNull = (dynamicField.annotations.notNull == true)
        )
        // Generate the complete mapping code with NULL check
        val notNull = dynamicField.annotations.notNull == true
        return if (notNull) {
            buildString {
                append("if ($nullCheckCondition) {\n")
                append("          error(\"Required dynamic field '${dynamicField.src.fieldName}' is null\")\n")
                append("        } else {\n")
                append("          ${dynamicField.annotations.propertyType}(\n")
                append("            $constructorArgs\n")
                append("          )\n")
                append("        }")
            }
        } else {
            buildString {
                append("if ($nullCheckCondition) {\n")
                append("    null\n")
                append("  } else {\n")
                append("    ${dynamicField.annotations.propertyType}(\n")
                append("      $constructorArgs\n")
                append("    )\n")
                append("  }")
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
        functionName: String = "execute"
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
                    addExecuteReturningStatementImplementationToBuilder(b, statement, namespace, className, functionName)
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
        functionName: String = "executeAsList"
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

        when (functionName) {
            "executeReturningList" -> {
                // Generate list collection logic (similar to SELECT executeAsList)
                b.line("statement.use { statement ->")
                b.indent(by = 2) {
                    b.line("val results = mutableListOf<$capitalizedNamespace.$className.Result>()")
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
        val capitalizedNamespace = queryNamespaceName(namespace)
        val className = statement.getDataClassName()
        val resultType = "$capitalizedNamespace.$className.Result"

        // Convert EXECUTE statement to SELECT-like fields for reusing existing logic
        val selectLikeFields = createSelectLikeFieldsFromExecuteReturning(statement)

        // Build the constructor call with all properties - reuse SELECT logic
        val constructorCall = buildString {
            append("return $resultType(\n")
            selectLikeFields.forEachIndexed { index, field ->
                val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(field.src.fieldName)
                // Reuse the existing SELECT getter call logic
                val getterCall = generateGetterCallInternal(
                    field = field,
                    columnIndex = index,
                    propertyNameGenerator = statement.annotations.propertyNameGenerator,
                    isFromJoinedTable = false
                )
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
     * Generate getter call for an EXECUTE statement column with proper adapter handling.
     * Similar to generateGetterCallInternal but works with table columns instead of query fields.
     */
    private fun generateGetterCallForExecuteColumn(
        column: AnnotatedCreateTableStatement.Column,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType
    ): String {
        val propertyName = propertyNameGenerator.convertToPropertyName(column.src.name)
        val hasAdapter = column.annotations.containsKey(AnnotationConstants.ADAPTER)

        if (hasAdapter) {
            // Use adapter function
            val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)
            val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
            val baseGetterCall = typeMapping.getGetterCall(kotlinType, columnIndex).replace("stmt", "statement")

            return if (column.isNullable()) {
                "if (statement.isNull($columnIndex)) $adapterFunctionName(null) else $adapterFunctionName($baseGetterCall)"
            } else {
                "$adapterFunctionName($baseGetterCall)"
            }
        } else {
            // No adapter: use direct getter based on the desired property type
            // We need to determine the desired property type from the Result data class
            val desiredType = getDesiredPropertyTypeForExecuteColumn(column, propertyNameGenerator)
            val baseGetterCall = typeMapping.getGetterCall(desiredType.copy(nullable = false), columnIndex).replace("stmt", "statement")

            return if (column.isNullable()) {
                "if (statement.isNull($columnIndex)) null else $baseGetterCall"
            } else {
                baseGetterCall
            }
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
     * Generate getter call for a field with proper nullability handling.
     * Only applies converters when the field or its underlying column has an adapter annotation
     * (custom types or explicitly annotated), not for every primitive/standard type.
     */
    private fun generateGetterCallInternal(
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        isFromJoinedTable: Boolean,
    ): String {
        val desiredType = selectFieldGenerator.generateProperty(field, propertyNameGenerator).type
        val isCustomDesiredType = isCustomKotlinType(desiredType)
        if (isCustomDesiredType || adapterConfig.hasAdapterAnnotation(field)) {
            // Use base original column name to derive adapter function name (view-aware)
            val baseOriginal = adapterConfig.baseOriginalNameForField(field)
            val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(baseOriginal)
            val adapterParamName = adapterConfig.getOutputAdapterFunctionName(columnName)

            val inputNullable = desiredType.isNullable
            val baseGetterCall = getUnderlyingTypeAndGetter(field, columnIndex).second.replace("stmt", "statement")

            return if (isFromJoinedTable || inputNullable) {
                if (isFromJoinedTable) {
                    // For joined table fields, return null if DB value is NULL to keep joined-row nullability
                    "if (statement.isNull($columnIndex)) null else $adapterParamName($baseGetterCall)"
                } else {
                    // For nullable fields, propagate null to the adapter
                    "if (statement.isNull($columnIndex)) $adapterParamName(null) else $adapterParamName($baseGetterCall)"
                }
            } else {
                "$adapterParamName($baseGetterCall)"
            }
        } else {
            // No adapter: use direct getter based on desired property type
            val kotlinType = desiredType
            val baseGetterCall = typeMapping
                .getGetterCall(kotlinType.copy(nullable = false), columnIndex)
                .replace("stmt", "statement")

            return if (isFromJoinedTable || kotlinType.isNullable) {
                "if (statement.isNull($columnIndex)) null else $baseGetterCall"
            } else {
                baseGetterCall
            }
        }
    }

    private fun isCustomKotlinType(type: com.squareup.kotlinpoet.TypeName): Boolean {
        val t = type.toString()
        return !(t.startsWith("kotlin.") || t.startsWith("kotlinx."))
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
        val resultType = if (statement.annotations.sharedResult != null) {
            "${queryNamespaceName(namespace)}.SharedResult.${statement.annotations.sharedResult}_Joined"
        } else {
            "${queryNamespaceName(namespace)}.${statement.getDataClassName()}.Result_Joined"
        }
        // Build the constructor call with ALL properties (no exclusions)
        val constructorCall = buildString {
            append("return $resultType(\n")
            // Include ALL fields from the SELECT statement (no dynamic field mapping or exclusions)
            val allFields = statement.fields.filter { !it.annotations.isDynamicField }
            allFields.forEachIndexed { index, field ->
                val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
                val getterCall = generateJoinedGetterCall(field, index, allFields)
                val typeComment = buildTypeComment(field)
                append("  $propertyName = $getterCall, // $typeComment\n")
            }
            append(")")
        }
        fnBld.addStatement(constructorCall)
    }

    /**
     * Builds a concise end-of-line comment describing the SQL -> Kotlin type mapping for a field.
     */
    private fun buildTypeComment(field: AnnotatedSelectStatement.Field): String {
        val sqlType = field.src.dataType
        val kotlinType = selectFieldGenerator
            .generateProperty(field, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            .type
            .toString()
        return "$sqlType -> $kotlinType"
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
        // Find ALL collection fields
        val collectionFields = statement.fields.filter {
            it.annotations.isDynamicField && it.annotations.mappingType == AnnotationConstants.MAPPING_TYPE_COLLECTION
        }
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
        val joinedClassFullName = if (statement.annotations.sharedResult != null) {
            "${capitalizedNamespace}.SharedResult.${statement.annotations.sharedResult}_Joined"
        } else {
            "${capitalizedNamespace}.$className.Result_Joined"
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
                val mappedColumns = getMappedColumnsForStatement(statement)
                val regularFields = statement.fields.filter { !it.annotations.isDynamicField && !mappedColumns.contains(it.src.fieldName) }
                regularFields.forEach { field ->
                    val prop = getPropertyName(field, statement.annotations.propertyNameGenerator)
                    ctorBlocks += listOf("$prop = firstRow.$prop")
                }
                // Per-row dynamic fields
                val perRowDynamicFields = statement.fields.filter { it.annotations.isDynamicField && it.annotations.mappingType == AnnotationConstants.MAPPING_TYPE_PER_ROW }
                perRowDynamicFields.forEach { dynamicField ->
                    val prop = getPropertyName(dynamicField, statement.annotations.propertyNameGenerator)
                    val mappingCode = generateDynamicFieldMappingCodeFromJoined(dynamicField, statement, sourceVar = "firstRow")
                    ctorBlocks += listOf("$prop = $mappingCode")
                }
                // Collection fields as multi-line blocks
                val collectionFields = statement.fields.filter { it.annotations.isDynamicField && it.annotations.mappingType == AnnotationConstants.MAPPING_TYPE_COLLECTION }
                collectionFields.forEach { collectionField ->
                    val prop = getPropertyName(collectionField, statement.annotations.propertyNameGenerator)
                    val selectStatement = statement.src
                    val mapping = DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(collectionField)).firstOrNull()
                    val block = mutableListOf<String>()
                    if (mapping != null && mapping.columns.isNotEmpty()) {
                        val nullChecks = mapping.columns.map { col ->
                            val jp = statement.annotations.propertyNameGenerator.convertToPropertyName(col.fieldName)
                            "row.$jp == null"
                        }
                        val cond = nullChecks.joinToString(" && ")
                        block += "$prop = rowsForEntity"
                        block += ".filter { row -> !($cond) }"
                        block += ".map { row: $joinedClassFullName ->"
                        val elementType = collectionField.annotations.propertyType?.let { pt -> if (pt.contains("<") && pt.contains(">")) pt.substringAfter("<").substringBeforeLast(">") else pt } ?: "Unknown"
                        block += "  $elementType("
                        // Add item constructor args (indented at +2 in emission)
                        val args = generateConstructorArgumentsFromMapping(mapping, statement, "row").split(",\n").map { it.trim() }
                        args.forEachIndexed { i, arg ->
                            val comma = if (i < args.lastIndex) "," else ""
                            block += "    $arg$comma"
                        }
                        block += "  )"
                        block += "}"
                    } else {
                        block += "$prop = rowsForEntity.map { row: $joinedClassFullName ->"
                        val elementType = collectionField.annotations.propertyType?.let { pt -> if (pt.contains("<") && pt.contains(">")) pt.substringAfter("<").substringBeforeLast(">") else pt } ?: "Unknown"
                        block += "  $elementType("
                        val args = DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(collectionField)).firstOrNull()?.let { generateConstructorArgumentsFromMapping(it, statement, "row").split(",\n").map { s -> s.trim() } } ?: emptyList()
                        args.forEachIndexed { i, arg ->
                            val comma = if (i < args.lastIndex) "," else ""
                            block += "    $arg$comma"
                        }
                        block += "  )"
                        block += "}"
                    }
                    // Append distinctBy if available as part of the block's last line
                    val unique = findUniqueFieldForCollection(collectionField, selectStatement, statement)
                    if (unique != null) {
                        // Attach distinctBy on its own line; comma added during emission if needed
                        block += ".distinctBy { it.$unique }"
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

    /**
     * Helper method to get mapped columns for a statement.
     */
    private fun getMappedColumnsForStatement(statement: AnnotatedSelectStatement): Set<String> {
        val hasMappedDynamicFields = statement.fields.any {
            it.annotations.isDynamicField && it.annotations.mappingType != null
        }
        return if (hasMappedDynamicFields) {
            val selectStatement = statement.src
            DynamicFieldMapper.getMappedColumns(statement.fields, selectStatement.tableAliases)
        } else {
            emptySet()
        }
    }

    /**
     * Generates getter call for joined data classes with proper JOIN nullability handling.
     * For fields from joined tables, always adds NULL checks regardless of schema nullability.
     */
    private fun generateJoinedGetterCall(
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        allFields: List<AnnotatedSelectStatement.Field>
    ): String {
        // Check if this field comes from a joined table
        val fieldTableAlias = field.src.tableName
        val isFromJoinedTable = if (fieldTableAlias.isNotBlank()) {
            // Use the existing findMainTableAlias function to eliminate code duplication
            val mainTableAlias = dataStructCodeGenerator.findMainTableAlias(allFields)
            // This field is from a joined table if it's not from the main table
            fieldTableAlias != mainTableAlias
        } else {
            false
        }
        return generateGetterCallInternal(
            field = field,
            columnIndex = columnIndex,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = isFromJoinedTable,
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
        enforceNonNull: Boolean = false
    ): String {
        val indent = " ".repeat(6 + additionalIndent)
        val sourceIsView = run {
            val alias = mapping.sourceTableAlias
            val tableName = statement.src.tableAliases[alias] ?: alias
            dataStructCodeGenerator.createViewStatements.any { it.src.viewName.equals(tableName, ignoreCase = true) }
        }
        return mapping.columns.joinToString(",\n$indent") { column ->
            val fieldName = column.fieldName
            // The joined data class uses the raw field names (e.g., addressId, addressPersonId)
            val joinedPropertyName =
                statement.annotations.propertyNameGenerator.convertToPropertyName(fieldName)
            // For the constructor parameter name, we need the original column name (with prefix removed if applicable)
            val isAliased = fieldName != column.originalColumnName
            val basePropertyName = when {
                // Aliased columns with prefix: remove prefix
                isAliased && mapping.aliasPrefix != null && fieldName.startsWith(mapping.aliasPrefix) ->
                    fieldName.removePrefix(mapping.aliasPrefix)
                // View-sourced synthetic names often include the prefix verbatim; strip for parameter names
                !isAliased && sourceIsView && mapping.aliasPrefix != null && fieldName.startsWith(mapping.aliasPrefix) ->
                    fieldName.removePrefix(mapping.aliasPrefix)
                else ->
                    // Use original column name to match target shared result parameter naming
                    column.originalColumnName.ifBlank { fieldName }
            }
            val parameterName =
                statement.annotations.propertyNameGenerator.convertToPropertyName(basePropertyName)
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
    }
}
