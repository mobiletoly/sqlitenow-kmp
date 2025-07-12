package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.Dynamic
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.StatementUtils.getNamedParameters
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
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
    private val columnLookup = ColumnLookup(
        dataStructCodeGenerator.createTableStatements,
        dataStructCodeGenerator.createViewStatements
    )
    private val typeMapping = TypeMapping()
    private val parameterBinding = ParameterBinding(columnLookup, typeMapping, dataStructCodeGenerator)
    private val adapterConfig = AdapterConfig(columnLookup, dataStructCodeGenerator.createTableStatements, packageName)
    private val selectFieldGenerator = SelectFieldCodeGenerator(
        dataStructCodeGenerator.createTableStatements,
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
        val fileName = "${namespace.capitalized()}Query_$className"

        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated query extension functions for ${namespace}.${className}")
            .addFileComment("\nDO NOT MODIFY THIS FILE MANUALLY!")
            .addImport("dev.goquick.sqlitenow.core.util", "jsonEncodeToSqlite")

        if (!debug) {
            fileSpecBuilder.addImport("kotlinx.coroutines", "withContext")
        }

        // Generate bindStatementParams function first
        val bindFunction = generateBindStatementParamsFunction(namespace, statement)
        fileSpecBuilder.addFunction(bindFunction)

        // For SELECT statements, also generate readStatementResult function
        if (statement is AnnotatedSelectStatement) {
            // Only generate readStatementResult for non-collection queries
            if (!statement.hasCollectionMapping()) {
                val readStatementResultFunction = generateReadStatementResultFunction(namespace, statement)
                fileSpecBuilder.addFunction(readStatementResultFunction)
            }

            // Also generate readJoinedStatementResult function for queries with dynamic field mapping
            if (statement.hasDynamicFieldMapping()) {
                val readJoinedStatementResultFunction = generateReadJoinedStatementResultFunction(namespace, statement)
                fileSpecBuilder.addFunction(readJoinedStatementResultFunction)
            }
        }

        // Then generate execute function(s) that use bindStatementParams and readStatementResult
        when (statement) {
            is AnnotatedSelectStatement -> {
                val executeAsListFunction = generateSelectQueryFunction(namespace, statement, "executeAsList")
                fileSpecBuilder.addFunction(executeAsListFunction)

                // Only generate single-row functions for non-collection queries
                // (Collection mapping requires multiple rows to build collections)
                if (!statement.hasCollectionMapping()) {
                    val executeAsOneFunction = generateSelectQueryFunction(namespace, statement, "executeAsOne")
                    val executeAsOneOrNullFunction =
                        generateSelectQueryFunction(namespace, statement, "executeAsOneOrNull")

                    fileSpecBuilder.addFunction(executeAsOneFunction)
                    fileSpecBuilder.addFunction(executeAsOneOrNullFunction)
                }
            }

            is AnnotatedExecuteStatement -> {
                val queryFunction = generateExecuteQueryFunction(namespace, statement)
                fileSpecBuilder.addFunction(queryFunction)
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
        val resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)

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
        val capitalizedNamespace = "${namespace.capitalized()}Query"
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("readStatementResult")
            .addKdoc("Read statement and convert it to ${capitalizedNamespace}.${className}.Result entity")
        setupReadStatementResultStructure(fnBld, statement, namespace, className)
        val resultType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
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
     * Generates an extension function for execute (INSERT/DELETE/UPDATE) statements.
     * Example: fun Person.AddUser.execute(conn: Connection, params: Person.AddUser.Params)
     */
    private fun generateExecuteQueryFunction(
        namespace: String,
        statement: AnnotatedExecuteStatement
    ): FunSpec {
        val className = statement.getDataClassName()
        val fnBld = FunSpec.builder("execute")
            .addModifiers(com.squareup.kotlinpoet.KModifier.SUSPEND)
            .addKdoc("Executes the ${statement.name} query.")
        setupExecuteFunctionStructure(fnBld, statement, namespace, className)
        // Set return type: Unit (no return value needed for INSERT/DELETE/UPDATE)
        fnBld.returns(Unit::class)
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
        addAdapterParameters(fnBld, statement)
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
            AdapterType.RESULT_CONVERSION -> addResultConversionAdapterParameters(
                fnBld,
                statement as AnnotatedSelectStatement
            )

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
     * Helper function to create Joined Result type name for SELECT statements.
     * Uses SharedResult_Joined if the statement has sharedResult annotation, otherwise uses regular Result_Joined.
     */
    private fun createJoinedResultTypeName(namespace: String, statement: AnnotatedSelectStatement): ClassName {
        val capitalizedNamespace = "${namespace.capitalized()}Query"
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
            val parameterBindingAdapters = getFilteredAdapterNames(statement) { config ->
                config.adapterFunctionName.endsWith("ToSqlValue")
            }
            paramsList.addAll(parameterBindingAdapters)
            val paramsString = paramsList.joinToString(", ")
            codeBuilder.append("  $capitalizedNamespace.$className.bindStatementParams($paramsString)\n")
        }

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
        val capitalizedNamespace = "${namespace.capitalized()}Query"
        val className = statement.getDataClassName()

        // Build parameter list for readJoinedStatementResult (same adapters as this function)
        val paramsList = mutableListOf("statement")

        // Add result conversion adapter function names (same as current function parameters)
        val resultConversionAdapters = getFilteredAdapterNames(statement) { config ->
            config.adapterFunctionName.startsWith("sqlValueTo")
        }
        paramsList.addAll(resultConversionAdapters)
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
                    val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
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
        val mappedColumns = DynamicFieldMapper.getMappedColumns(statement.fields, statement.src.tableAliases)
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
                val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
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
        statement: AnnotatedSelectStatement
    ): String {
        val selectStatement = statement.src

        // Use DynamicFieldMapper to get the mapping information
        val mappings = DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(dynamicField))
        val mapping = mappings.firstOrNull()

        if (mapping == null || mapping.columns.isEmpty()) {
            return "null // No columns found for mapping"
        }

        // Generate NULL check for all joined fields
        val nullCheckConditions = mapping.columns.map { column ->
            val fieldName = column.fieldName
            val joinedPropertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(fieldName)
            "joinedData.$joinedPropertyName == null"
        }
        val nullCheckCondition = nullCheckConditions.joinToString(" && ")

        // Generate the constructor arguments with proper type conversions
        val constructorArgs = generateConstructorArgumentsFromMapping(mapping, statement, "joinedData")

        // Generate the complete mapping code with NULL check
        return buildString {
            append("if ($nullCheckCondition) {\n")
            append("    null\n")
            append("  } else {\n")
            append("    ${dynamicField.annotations.propertyType}(\n")
            append("      $constructorArgs\n")
            append("    )\n")
            append("  }")
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
                if (statement.hasCollectionMapping()) {
                    // For collection mapping: read all joined rows, group them, then create mapped objects
                    addCollectionMappingExecuteAsListImplementation(
                        codeBuilder,
                        statement,
                        namespace,
                        className,
                        paramsString
                    )
                } else {
                    // For regular queries: use readStatementResult directly
                    codeBuilder.append("    val results = mutableListOf<$resultType>()\n")
                    codeBuilder.append("    while (statement.step()) {\n")
                    codeBuilder.append("      results.add($capitalizedNamespace.$className.readStatementResult($paramsString))\n")
                    codeBuilder.append("    }\n")
                    codeBuilder.append("    results\n")
                }
            }

            "executeAsOne" -> {
                codeBuilder.append("    if (statement.step()) {\n")
                codeBuilder.append("      $capitalizedNamespace.$className.readStatementResult($paramsString)\n")
                codeBuilder.append("    } else {\n")
                codeBuilder.append("      throw IllegalStateException(\"Query returned no results, but exactly one result was expected\")\n")
                codeBuilder.append("    }\n")
            }

            "executeAsOneOrNull" -> {
                codeBuilder.append("    if (statement.step()) {\n")
                codeBuilder.append("      $capitalizedNamespace.$className.readStatementResult($paramsString)\n")
                codeBuilder.append("    } else {\n")
                codeBuilder.append("      null\n")
                codeBuilder.append("    }\n")
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
     * Generate getter call for a field with proper nullability handling.
     */
    private fun generateGetterCallInternal(
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        isFromJoinedTable: Boolean,
    ): String {
        if (adapterConfig.hasAdapterAnnotation(field)) {
            // Use actual column name for adapter function name (ignore property name customizations)
            val columnName =
                PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(field.src.originalColumnName)
            val adapterParamName = adapterConfig.getOutputAdapterFunctionName(columnName)

            val property = selectFieldGenerator.generateProperty(field, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            val inputNullable = property.type.isNullable

            val baseGetterCall = getUnderlyingTypeAndGetter(field, columnIndex).second.replace("stmt", "statement")

            // Handle NULL checks based on whether this is a joined table field
            return if (isFromJoinedTable || inputNullable) {
                if (isFromJoinedTable) {
                    // For joined table fields, always return null directly when database value is NULL
                    // This prevents adapters from returning non-null defaults (like emptyList())
                    // which would make it impossible to detect missing joined records
                    "if (statement.isNull($columnIndex)) null else $adapterParamName($baseGetterCall)"
                } else {
                    // For non-joined nullable fields, use the original logic
                    "if (statement.isNull($columnIndex)) $adapterParamName(null) else $adapterParamName($baseGetterCall)"
                }
            } else {
                "$adapterParamName($baseGetterCall)"
            }
        } else {
            // No adapter, use direct getter with proper type mapping
            val property = selectFieldGenerator.generateProperty(field, propertyNameGenerator)
            val kotlinType = property.type

            // For joined data classes, use TypeMapping for better type handling
            val baseGetterCall = if (isFromJoinedTable) {
                typeMapping.getGetterCall(kotlinType.copy(nullable = false), columnIndex).replace("stmt", "statement")
            } else {
                typeMapping.getGetterCall(kotlinType.copy(nullable = false), columnIndex)
                    .replace("stmt", "statement")
            }

            // Handle NULL checks - for joined tables, always add NULL check regardless of schema nullability
            return if (isFromJoinedTable || kotlinType.isNullable) {
                "if (statement.isNull($columnIndex)) null else $baseGetterCall"
            } else {
                baseGetterCall
            }
        }
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
            "${namespace.capitalized()}Query.SharedResult.${statement.annotations.sharedResult}_Joined"
        } else {
            "${namespace.capitalized()}Query.${statement.getDataClassName()}.Result_Joined"
        }

        // Build the constructor call with ALL properties (no exclusions)
        val constructorCall = buildString {
            append("return $resultType(\n")

            // Include ALL fields from the SELECT statement (no dynamic field mapping or exclusions)
            val allFields = statement.fields.filter { !it.annotations.isDynamicField }

            allFields.forEachIndexed { index, field ->
                val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
                // For joined data classes, override nullability for fields from joined tables
                val getterCall = generateJoinedGetterCall(field, index, allFields)
                append("  $propertyName = $getterCall,\n")
            }
            append(")")
        }
        fnBld.addStatement(constructorCall)
    }

    /**
     * Generates executeAsList implementation for collection mapping queries.
     * This reads all joined rows, groups them by the main entity, and creates mapped objects with collections.
     */
    private fun addCollectionMappingExecuteAsListImplementation(
        codeBuilder: StringBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        paramsString: String
    ) {
        val capitalizedNamespace = "${namespace.capitalized()}Query"
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

        codeBuilder.append("    // Read all joined rows first\n")
        codeBuilder.append("    val joinedRows = mutableListOf<$joinedClassFullName>()\n")
        codeBuilder.append("    while (statement.step()) {\n")
        codeBuilder.append("      joinedRows.add($capitalizedNamespace.$className.readJoinedStatementResult($paramsString))\n")
        codeBuilder.append("    }\n")
        codeBuilder.append("\n")

        // Determine the type of the grouping key field using the helper function
        val groupingField = findFieldByCollectionKey(collectionKey, statement.fields)
        val groupingKeyType = if (groupingField != null) {
            val property =
                selectFieldGenerator.generateProperty(groupingField, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            property.type.copy(nullable = false).toString() // Remove nullability for the Map key type
        } else {
            "Any" // Fallback, though this should not happen due to validation above
        }

        codeBuilder.append("    // Group joined rows by $groupingKey\n")
        codeBuilder.append("    val groupedRows: Map<$groupingKeyType, List<$joinedClassFullName>> = joinedRows.groupBy { it.$groupingKey }\n")
        codeBuilder.append("\n")

        codeBuilder.append("    // Create mapped objects with collections\n")
        codeBuilder.append("    groupedRows.map { (_, rowsForEntity: List<$joinedClassFullName>) ->\n")
        codeBuilder.append("      val firstRow = rowsForEntity.first()\n")
        codeBuilder.append("      $resultType(\n")

        // Add regular fields (copy from first row) - exclude mapped columns
        val mappedColumns = getMappedColumnsForStatement(statement)
        val regularFields = statement.fields.filter {
            !it.annotations.isDynamicField && !mappedColumns.contains(it.src.fieldName)
        }

        regularFields.forEach { field ->
            val propertyName = getPropertyName(field, statement.annotations.propertyNameGenerator)
            codeBuilder.append("        $propertyName = firstRow.$propertyName,\n")
        }

        // Add ALL collection fields with filtering for NULL joined records
        collectionFields.forEachIndexed { index, collectionField ->
            val collectionPropertyName = getPropertyName(collectionField, statement.annotations.propertyNameGenerator)

            // Generate NULL check conditions for all joined table columns
            val selectStatement = statement.src
            val mappings = DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(collectionField))
            val mapping = mappings.firstOrNull()

            if (mapping != null && mapping.columns.isNotEmpty()) {
                val nullCheckConditions = mapping.columns.map { column ->
                    val fieldName = column.fieldName
                    val joinedPropertyName =
                        statement.annotations.propertyNameGenerator.convertToPropertyName(fieldName)
                    "row.$joinedPropertyName == null"
                }
                val nullCheckCondition = nullCheckConditions.joinToString(" && ")

                codeBuilder.append("        $collectionPropertyName = rowsForEntity\n")
                codeBuilder.append("          .filter { row -> !($nullCheckCondition) }\n")
                codeBuilder.append("          .map { row: $joinedClassFullName ->\n")
            } else {
                // Fallback if mapping is not available
                codeBuilder.append("        $collectionPropertyName = rowsForEntity.map { row: $joinedClassFullName ->\n")
            }

            // Extract the element type from the collection type (e.g., List<PersonAddressQuery.SharedResult.Row> -> PersonAddressQuery.SharedResult.Row)
            val elementType = collectionField.annotations.propertyType?.let { propertyType ->
                if (propertyType.contains("<") && propertyType.contains(">")) {
                    propertyType.substringAfter("<").substringBeforeLast(">")
                } else {
                    propertyType
                }
            } ?: "Unknown"

            codeBuilder.append("            $elementType(\n")

            // Add collection item properties using the same logic as the joined-to-mapped transformation
            addCollectionItemPropertiesFromJoined(codeBuilder, collectionField, statement)

            codeBuilder.append("            )\n")
            codeBuilder.append("          }\n")

            // Find the unique identifier field for deduplication from JOIN conditions
            val uniqueField = findUniqueFieldForCollection(collectionField, selectStatement, statement)
            if (uniqueField != null) {
                codeBuilder.append("          .distinctBy { it.$uniqueField },")
            }
            codeBuilder.append("\n")
        }

        codeBuilder.append("      )\n    }\n")
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
     * Helper method to add collection item properties from joined data.
     */
    private fun addCollectionItemPropertiesFromJoined(
        codeBuilder: StringBuilder,
        collectionField: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement
    ) {
        val selectStatement = statement.src
        val mappings = DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(collectionField))
        val mapping = mappings.firstOrNull()
        if (mapping == null || mapping.columns.isEmpty()) {
            codeBuilder.append("          // No columns found for mapping\n")
            return
        }
        // Generate the constructor parameters using joined data properties with named parameters
        val constructorArgs = generateConstructorArgumentsFromMapping(mapping, statement, "row")
        constructorArgs.split(",\n").forEach { arg ->
            codeBuilder.append("              ${arg.trim()},\n")
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

        // Convert to the target property name, considering removeAliasPrefix
        val mapping = if (selectStatement != null) {
            DynamicFieldMapper.createDynamicFieldMappings(selectStatement, listOf(collectionField)).firstOrNull()
        } else {
            null
        }
        if (mapping != null) {
            // Find the corresponding column in the mapping
            val mappingColumn = mapping.columns.find { it.fieldName == matchingField.src.fieldName }
            if (mappingColumn != null) {
                // Use the target property name from the mapping (which handles removeAliasPrefix)
                val basePropertyName = if (mapping.removeAliasPrefix != null &&
                    mappingColumn.fieldName.startsWith(mapping.removeAliasPrefix) &&
                    mappingColumn.fieldName != mappingColumn.originalColumnName
                ) {
                    // Remove the prefix for the target property name
                    mappingColumn.fieldName.removePrefix(mapping.removeAliasPrefix)
                } else {
                    // Use the original column name
                    mappingColumn.originalColumnName
                }
                return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(basePropertyName)
            }
        }

        // Fallback: convert the field name directly
        return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(matchingField.src.fieldName)
    }

    /**
     * Generates constructor arguments from a dynamic field mapping.
     */
    private fun generateConstructorArgumentsFromMapping(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String
    ): String {
        return mapping.columns.joinToString(",\n      ") { column ->
            val fieldName = column.fieldName
            // The joined data class uses the raw field names (e.g., addressId, addressPersonId)
            val joinedPropertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(fieldName)

            // For the constructor parameter name, we need the original column name (with prefix removed if applicable)
            val basePropertyName = if (mapping.removeAliasPrefix != null &&
                fieldName.startsWith(mapping.removeAliasPrefix) &&
                fieldName != column.originalColumnName
            ) {
                // This is an aliased column, remove the prefix for the parameter name
                fieldName.removePrefix(mapping.removeAliasPrefix)
            } else {
                fieldName
            }
            val parameterName = statement.annotations.propertyNameGenerator.convertToPropertyName(basePropertyName)

            val isTargetNullable = isTargetPropertyNullable(column)
            val valueExpression = if (isTargetNullable) {
                "$sourceVariableName.$joinedPropertyName"  // No conversion needed for nullable targets
            } else {
                "$sourceVariableName.$joinedPropertyName!!"  // Use !! for non-nullable targets
            }
            "$parameterName = $valueExpression"
        }
    }
}
