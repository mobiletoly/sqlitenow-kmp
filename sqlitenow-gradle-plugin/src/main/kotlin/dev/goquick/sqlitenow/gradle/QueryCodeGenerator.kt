package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.StatementUtils.getNamedParameters
import org.gradle.internal.extensions.stdlib.capitalized
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
    private val columnLookup = ColumnLookup(dataStructCodeGenerator.createTableStatements, dataStructCodeGenerator.createViewStatements)
    private val typeMapping = TypeMapping()
    private val parameterBinding = ParameterBinding(columnLookup, typeMapping, dataStructCodeGenerator)
    private val adapterConfig = AdapterConfig(columnLookup, dataStructCodeGenerator.createTableStatements, packageName)
    private val selectFieldGenerator = SelectFieldCodeGenerator(dataStructCodeGenerator.createTableStatements, packageName)

    /**
     * Generates query extension function files for all namespaces.
     * Creates separate files per query like Person_SelectWeird.kt, Person_AddUser.kt, etc.
     */
    fun generateCode() {
        // Generate separate files for each query
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
        val fileName = "${namespace.capitalized()}Query_$className"

        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated query extension functions for ${namespace}.${className}")
            .addFileComment("\nDo not modify this file manually")
            .addImport("dev.goquick.sqlitenow.core.util", "jsonEncodeToSqlite")

        if (debug) {
            // No additional import needed for conn.withContextAndTrace
        } else {
            fileSpecBuilder.addImport("kotlinx.coroutines", "withContext")
        }

        // Generate bindStatementParams function first
        val bindFunction = generateBindStatementParamsFunction(namespace, statement)
        fileSpecBuilder.addFunction(bindFunction)

        // For SELECT statements, also generate readStatementResult function
        if (statement is AnnotatedSelectStatement) {
            val readStatementResultFunction = generateReadStatementResultFunction(namespace, statement)
            fileSpecBuilder.addFunction(readStatementResultFunction)
        }

        // Then generate execute function(s) that use bindStatementParams and readStatementResult
        when (statement) {
            is AnnotatedSelectStatement -> {
                // Generate all three SELECT functions: executeAsList, executeAsOne, executeAsOneOrNull
                val executeAsListFunction = generateSelectQueryFunction(namespace, statement, "executeAsList")
                val executeAsOneFunction = generateSelectQueryFunction(namespace, statement, "executeAsOne")
                val executeAsOneOrNullFunction = generateSelectQueryFunction(namespace, statement, "executeAsOneOrNull")

                fileSpecBuilder.addFunction(executeAsListFunction)
                fileSpecBuilder.addFunction(executeAsOneFunction)
                fileSpecBuilder.addFunction(executeAsOneOrNullFunction)
            }

            is AnnotatedExecuteStatement -> {
                val queryFunction = generateExecuteQueryFunction(namespace, statement)
                fileSpecBuilder.addFunction(queryFunction)
            }

            is AnnotatedCreateTableStatement -> return
            is AnnotatedCreateViewStatement -> return
        }

        // Write the file
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
        val resultType = createSelectResultTypeName(namespace, statement)

        val returnType = when (functionName) {
            "executeAsList" -> ClassName("kotlin.collections", "List").parameterizedBy(resultType)
            "executeAsOne" -> resultType
            "executeAsOneOrNull" -> resultType.copy(nullable = true)
            else -> resultType
        }
        fnBld.returns(returnType)

        // Add SQL statement processing logic with function-specific execution
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

        // Create the extension function
        val fnBld = FunSpec.builder("bindStatementParams")
            .addKdoc("Binds parameters to an already prepared SQLiteStatement for the ${statement.name} query.")

        // Set up function structure for bindStatementParams
        setupBindStatementParamsStructure(fnBld, statement, namespace, className)

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
        val capitalizedNamespace = "${namespace.capitalized()}Query"
        val className = statement.getDataClassName()

        // Create the extension function
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")

        // Set up function structure for readStatementResult
        setupReadStatementResultStructure(fnBld, statement, namespace, className)

        // Set return type (handles shared results)
        val resultType = createSelectResultTypeName(namespace, statement)
        fnBld.returns(resultType)

        // Add result reading logic
        addReadStatementResultProcessing(fnBld, statement, namespace)

        return fnBld.build()
    }

    /**
     * Generates an extension function for execute (INSERT/DELETE/UPDATE) statements.
     * Example: fun Person.AddUser.execute(conn: Connection, params: Person.AddUser.Params)
     */
    private fun generateExecuteQueryFunction(
        namespace: String,
        statement: AnnotatedExecuteStatement
    ): FunSpec {
        val className = statement.getDataClassName()

        // Create the extension function
        val fnBld = FunSpec.builder("execute")
            .addModifiers(com.squareup.kotlinpoet.KModifier.SUSPEND)
            .addKdoc("Executes the ${statement.name} query.")

        // Set up common function structure (receiver, connection, params)
        setupExecuteFunctionStructure(fnBld, statement, namespace, className)

        // Set return type: Unit (no return value needed for INSERT/DELETE/UPDATE)
        fnBld.returns(Unit::class)

        // Add common SQL statement processing logic
        addSqlStatementProcessing(fnBld, statement, namespace, className, functionName = "execute")

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
        val capitalizedNamespace = "${namespace.capitalized()}Query"

        // Add receiver type: PersonQuery.SelectWeird
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

        addAllAdapterParameters(fnBld, statement)
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
        val capitalizedNamespace = "${namespace.capitalized()}Query"

        // Add receiver type: PersonQuery.SelectWeird
        val receiverType = ClassName(packageName, capitalizedNamespace).nestedClass(className)
        fnBld.receiver(receiverType)

        // Add statement parameter (the prepared SQLiteStatement)
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
            AdapterType.RESULT_CONVERSION -> addResultConversionAdapterParameters(fnBld, statement as AnnotatedSelectStatement)
            AdapterType.NONE -> { /* No adapters */ }
        }
    }

    /**
     * Helper function to set up function structure for bindStatementParams.
     * This includes receiver type, statement parameter, params parameter, and binding adapters.
     */
    private fun setupBindStatementParamsStructure(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String
    ) {
        setupStatementFunctionStructure(
            fnBld, statement, namespace, className,
            includeParamsParameter = true,
            adapterType = AdapterType.PARAMETER_BINDING
        )
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
            val adapterParam = ParameterSpec.builder(config.adapterFunctionName, adapterType).build()
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
     * Unified method to add all adapter parameters for any statement type.
     * Handles both input adapters (for parameters) and output adapters (for SELECT fields).
     */
    private fun addAllAdapterParameters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedStatement
    ) {
        addAdapterParameters(fnBld, statement) // No filter = all adapters
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

    /**
     * Helper function to create Result type name for SELECT statements.
     * Uses SharedResult if the statement has sharedResult annotation, otherwise uses regular Result.
     */
    private fun createSelectResultTypeName(namespace: String, statement: AnnotatedSelectStatement): ClassName {
        return SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
    }

    /**
     * Helper function to create Params type name.
     * Consolidates duplicate ClassName construction for Params types.
     */
    private fun createParamsTypeName(namespace: String, className: String): ClassName {
        val capitalizedNamespace = "${namespace.capitalized()}Query"
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
        val baseGetterCall = getGetterCallForKotlinType(kotlinType, columnIndex)

        return Pair(kotlinType, baseGetterCall)
    }

    /**
     * Helper function to generate the appropriate getter call for a Kotlin type.
     */
    private fun getGetterCallForKotlinType(kotlinType: TypeName, columnIndex: Int): String {
        return typeMapping.getGetterCall(kotlinType, columnIndex)
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
        val capitalizedNamespace = "${namespace.capitalized()}Query"

        // Build the complete withContext block as a single statement
        val codeBuilder = StringBuilder()
        if (debug) {
            codeBuilder.append("return conn.withContextAndTrace {\n")
        } else {
            codeBuilder.append("return withContext(conn.dispatcher) {\n")
        }

        // Prepare the statement and bind parameters
        codeBuilder.append("  val sql = $capitalizedNamespace.$className.SQL\n")
        codeBuilder.append("  val statement = conn.ref.prepare(sql)\n")

        val namedParameters = getNamedParameters(statement)
        if (namedParameters.isNotEmpty()) {
            // Build parameter list for bindStatementParams: statement, params, and only xxxToSqlColumn adapters
            val paramsList = mutableListOf("statement", "params")

            // Add parameter binding adapter function names
            val parameterBindingAdapters = getFilteredAdapterNames(statement) { config ->
                config.adapterFunctionName.endsWith("ToSqlValue")
            }
            paramsList.addAll(parameterBindingAdapters)

            val paramsString = paramsList.joinToString(", ")
            codeBuilder.append("  $capitalizedNamespace.$className.bindStatementParams($paramsString)\n")
        }

        // Add the execution implementation
        addSqlExecutionImplementationToCodeBuilder(codeBuilder, statement, namespace, className, functionName)

        codeBuilder.append("}")

        fnBld.addStatement(codeBuilder.toString())
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
            val processedAdapterVars = mutableMapOf<String, String>()

            namedParameters.forEachIndexed { index, paramName ->
                val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(paramName)

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
        }
        // No return statement needed - this function returns Unit
    }

    /**
     * Helper function to add readStatementResult processing logic.
     * This generates code to read a single row from the statement and convert it to a Result object.
     */
    private fun addReadStatementResultProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String
    ) {
        // Use the correct result type (handles shared results)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)

        // Build the constructor call with all properties
        val constructorCall = buildString {
            append("return $resultType(\n")

            // Separate dynamic fields from regular fields
            val regularFields = statement.fields.filter { !it.annotations.isDynamicField }
            val dynamicFields = statement.fields.filter { it.annotations.isDynamicField }

            // Process regular fields (read from database)
            regularFields.forEachIndexed { index, field ->
                val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
                val getterCall = generateGetterCall(field, index, statement.annotations.propertyNameGenerator)
                val isLast = index == regularFields.size - 1 && dynamicFields.isEmpty()
                val comma = if (isLast) "" else ","

                append("  $propertyName = $getterCall$comma\n")
            }

            // Process dynamic fields (use default values)
            dynamicFields.forEachIndexed { index, field ->
                val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
                val defaultValue = field.annotations.defaultValue ?: "null"
                val isLast = index == dynamicFields.size - 1
                val comma = if (isLast) "" else ","

                append("  $propertyName = $defaultValue$comma\n")
            }

            append(")")
        }

        fnBld.addStatement(constructorCall)
    }

    /**
     * Helper function to add SQL execution implementation to a StringBuilder.
     * Generates different code for SELECT vs INSERT/DELETE statements.
     */
    private fun addSqlExecutionImplementationToCodeBuilder(
        codeBuilder: StringBuilder,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        functionName: String = "execute"
    ) {
        when (statement) {
            is AnnotatedSelectStatement -> {
                addSelectExecutionImplementationToCodeBuilder(
                    codeBuilder,
                    statement,
                    namespace,
                    className,
                    functionName
                )
            }

            is AnnotatedExecuteStatement -> {
                addExecuteStatementImplementationToCodeBuilder(codeBuilder)
            }

            is AnnotatedCreateTableStatement -> {
                codeBuilder.append("  TODO(\"Unimplemented\")\n")
            }

            is AnnotatedCreateViewStatement -> {
                codeBuilder.append("  TODO(\"Unimplemented\")\n")
            }
        }
    }

    /**
     * Helper function to add SELECT statement execution implementation to StringBuilder.
     * Generates code to execute query and convert results to data classes using readStatementResult.
     */
    private fun addSelectExecutionImplementationToCodeBuilder(
        codeBuilder: StringBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        functionName: String = "executeAsList"
    ) {
        val capitalizedNamespace = "${namespace.capitalized()}Query"

        // Use the correct result type (handles shared results)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)

        // Build parameter list for readStatementResult: statement and only sqlColumnToXxx adapters
        val paramsList = mutableListOf("statement")

        // Add result conversion adapter function names
        val resultConversionAdapters = getFilteredAdapterNames(statement) { config ->
            config.adapterFunctionName.startsWith("sqlValueTo")
        }
        paramsList.addAll(resultConversionAdapters)

        val paramsString = paramsList.joinToString(", ")

        codeBuilder.append("  statement.use { statement ->\n")

        when (functionName) {
            "executeAsList" -> {
                codeBuilder.append("  val results = mutableListOf<$resultType>()\n")
                codeBuilder.append("  while (statement.step()) {\n")
                codeBuilder.append("    results.add($capitalizedNamespace.$className.readStatementResult($paramsString))\n")
                codeBuilder.append("  }\n")
                codeBuilder.append("  results\n")
            }

            "executeAsOne" -> {
                codeBuilder.append("  if (statement.step()) {\n")
                codeBuilder.append("    $capitalizedNamespace.$className.readStatementResult($paramsString)\n")
                codeBuilder.append("  } else {\n")
                codeBuilder.append("    throw IllegalStateException(\"Query returned no results, but exactly one result was expected\")\n")
                codeBuilder.append("  }\n")
            }

            "executeAsOneOrNull" -> {
                codeBuilder.append("  if (statement.step()) {\n")
                codeBuilder.append("    $capitalizedNamespace.$className.readStatementResult($paramsString)\n")
                codeBuilder.append("  } else {\n")
                codeBuilder.append("    null\n")
                codeBuilder.append("  }\n")
            }
        }

        codeBuilder.append("  }\n")
    }

    /**
     * Helper function to add INSERT/DELETE statement execution implementation to StringBuilder.
     * Generates code to execute statement without returning a value.
     */
    private fun addExecuteStatementImplementationToCodeBuilder(
        codeBuilder: StringBuilder,
    ) {
        codeBuilder.append("  statement.use { statement ->\n")
        codeBuilder.append("    statement.step()\n")
        codeBuilder.append("  }\n")
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
     * Helper function to generate the appropriate getter call for a field.
     * Uses type-specific getters like getLong(), getText(), etc.
     * If the field has an 'adapter' annotation, uses the adapter function.
     */
    private fun generateGetterCall(
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType
    ): String {
        if (adapterConfig.hasAdapterAnnotation(field)) {
            // Use actual column name for adapter function name (ignore property name customizations)
            val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(field.src.originalColumnName)
            val adapterParamName = adapterConfig.getOutputAdapterFunctionName(columnName)

            val property = selectFieldGenerator.generateProperty(field, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            val inputNullable = property.type.isNullable

            val baseGetterCall = getUnderlyingTypeAndGetter(field, columnIndex).second.replace("stmt", "statement")

            return if (inputNullable) {
                "if (statement.isNull($columnIndex)) $adapterParamName(null) else $adapterParamName($baseGetterCall)"
            } else {
                "$adapterParamName($baseGetterCall)"
            }
        }

        val property = selectFieldGenerator.generateProperty(field, propertyNameGenerator)
        val kotlinType = property.type
        val baseGetterCall =
            getGetterCallForKotlinType(kotlinType.copy(nullable = false), columnIndex).replace("stmt", "statement")

        return if (kotlinType.isNullable) {
            "if (statement.isNull($columnIndex)) null else $baseGetterCall"
        } else {
            baseGetterCall
        }
    }
}
