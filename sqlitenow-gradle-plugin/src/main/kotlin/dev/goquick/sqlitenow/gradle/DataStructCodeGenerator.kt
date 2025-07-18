package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.inspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.inspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.inspect.InsertStatement
import dev.goquick.sqlitenow.gradle.inspect.UpdateStatement
import java.io.File
import java.sql.Connection
import org.gradle.internal.extensions.stdlib.capitalized

open class DataStructCodeGenerator(
    conn: Connection,
    queriesDir: File,
    packageName: String,
    outputDir: File,
    statementExecutors: MutableList<DeferredStatementExecutor>,
    providedCreateTableStatements: List<AnnotatedCreateTableStatement>? = null
) {
    open val createTableStatements = providedCreateTableStatements ?: statementExecutors
        .filterIsInstance<CreateTableStatementExecutor>()
        .map {
            try {
                it.execute(conn) as AnnotatedCreateTableStatement
            } catch (e: Exception) {
                System.err.println(
                    """
                    |Failed to execute CREATE TABLE statement:
                    |${it.reportContext()}
                    """.trimMargin()
                )
                throw e
            }
        }
    val createViewStatements = statementExecutors
        .filterIsInstance<CreateViewStatementExecutor>()
        .map {
            try {
                it.execute(conn) as AnnotatedCreateViewStatement
            } catch (e: Exception) {
                System.err.println(
                    """
                    |Failed to execute CREATE VIEW statement:
                    |${it.reportContext()}
                    """.trimIndent()
                )
                throw e
            }
        }

    private val annotationResolver = FieldAnnotationResolver(createTableStatements, createViewStatements)
    private val columnLookup = ColumnLookup(createTableStatements, createViewStatements)
    private val fileGenerationHelper = FileGenerationHelper(packageName, outputDir)
    private val stmtProcessingHelper = StatementProcessingHelper(conn, annotationResolver)
    private val sharedResultManager = SharedResultManager()
    val nsWithStatements = stmtProcessingHelper.processQueriesDirectory(queriesDir)

    fun generateNamespaceDataStructuresCode(
        namespace: String,
        packageName: String,
    ): FileSpec.Builder {
        val fileName = "${namespace.capitalized()}Query"
        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated code for $namespace namespace queries")
            .addFileComment("\nDo not modify this file manually")

        val capitalizedNamespace = "${namespace.capitalized()}Query"
        val namespaceObject = TypeSpec.objectBuilder(capitalizedNamespace)
            .addKdoc("Contains queries for the $namespace namespace")

        val allStatements = nsWithStatements[namespace] ?: emptyList()
        val statementProcessor = StatementProcessor(allStatements)

        // Track shared results and their source statements for table alias access
        val sharedResultsWithContext = mutableMapOf<String, AnnotatedSelectStatement>()

        // Generate query-specific objects and register shared results
        statementProcessor.processStatements(
            onSelectStatement = { statement: AnnotatedSelectStatement ->
                // Register shared result and track context
                sharedResultManager.registerSharedResult(statement, namespace)
                if (statement.annotations.sharedResult != null) {
                    sharedResultsWithContext[statement.annotations.sharedResult] = statement
                }
                val queryObject = generateQueryObject(statement, namespace)
                namespaceObject.addType(queryObject)
            },
            onExecuteStatement = { statement: AnnotatedExecuteStatement ->
                val queryObject = generateQueryObject(statement, namespace)
                namespaceObject.addType(queryObject)
            }
        )

        // Generate SharedResult object if there are any shared results
        val sharedResults = sharedResultManager.getSharedResultsByNamespace()[namespace]
        if (!sharedResults.isNullOrEmpty()) {
            val sharedResultObject = generateSharedResultObject(sharedResults, sharedResultsWithContext)
            namespaceObject.addType(sharedResultObject)
        }

        fileSpecBuilder.addType(namespaceObject.build())

        return fileSpecBuilder
    }

    /** Generates a query-specific object (e.g., Person.SelectWeird) containing SQL, Params, and Result. */
    private fun generateQueryObject(statement: AnnotatedStatement, namespace: String): TypeSpec {
        val className = statement.getDataClassName()

        val queryObjectBuilder = TypeSpec.objectBuilder(className)
            .addKdoc("Contains SQL, parameters, and result types for the ${statement.name} query.")

        // Add SQL constant
        val sql = getSqlFromStatement(statement)

        val sqlProperty = PropertySpec.builder("SQL", String::class)
            .addModifiers(KModifier.CONST)
            .initializer("%S", sql)
            .addKdoc("The SQL query string for ${statement.name}.")
            .build()
        queryObjectBuilder.addProperty(sqlProperty)

        // Add affectedTables constant
        val affectedTables = getAffectedTablesFromStatement(statement)
        val affectedTablesType = ClassName("kotlin.collections", "Set").parameterizedBy(ClassName("kotlin", "String"))
        val affectedTablesProperty = PropertySpec.builder("affectedTables", affectedTablesType)
            .initializer("setOf(%L)", affectedTables.joinToString(", ") { "\"$it\"" })
            .addKdoc("Set of table names that are affected by the ${statement.name} query.")
            .build()
        queryObjectBuilder.addProperty(affectedTablesProperty)

        // Add Params data class if the statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        if (namedParameters.isNotEmpty()) {
            val paramsDataClass = generateParameterDataClass(statement)
            queryObjectBuilder.addType(paramsDataClass)
        }

        // Add Result data class for SELECT statements (unless they use shared results)
        if (statement is AnnotatedSelectStatement && !sharedResultManager.isSharedResult(statement)) {
            val resultDataClass = generateResultDataClassForSelectStatement(statement, namespace)
            queryObjectBuilder.addType(resultDataClass)
        }

        // Add Joined data class for SELECT statements with dynamic field mapping (unless they use shared results)
        if (statement is AnnotatedSelectStatement && statement.annotations.sharedResult == null) {
            if (statement.hasDynamicFieldMapping()) {
                val joinedDataClass = generateJoinedDataClass(statement)
                queryObjectBuilder.addType(joinedDataClass)
            }
        }

        return queryObjectBuilder.build()
    }

    /** Generates the SharedResult object containing all shared result classes for a namespace. */
    private fun generateSharedResultObject(
        sharedResults: List<SharedResultManager.SharedResult>,
        sharedResultsWithContext: Map<String, AnnotatedSelectStatement>
    ): TypeSpec {
        val sharedResultObjectBuilder = TypeSpec.objectBuilder(SharedResultTypeUtils.SHARED_RESULT_OBJECT_NAME)
            .addKdoc("Contains shared result classes that can be used across multiple queries.")

        sharedResults.forEach { sharedResult ->
            val sourceStatement = sharedResultsWithContext[sharedResult.name]
            val resultDataClass = generateSharedResultDataClass(sharedResult, sourceStatement)
            sharedResultObjectBuilder.addType(resultDataClass)

            // Add Joined data class for shared results with dynamic field mapping
            if (sourceStatement != null) {
                if (sharedResult.hasDynamicFieldMapping()) {
                    val joinedDataClass = generateJoinedDataClassInternal(
                        joinedClassName = "${sharedResult.name}_Joined",
                        fields = sharedResult.fields,
                        propertyNameGenerator = sharedResult.propertyNameGenerator
                    )
                    sharedResultObjectBuilder.addType(joinedDataClass)
                }
            }
        }

        return sharedResultObjectBuilder.build()
    }

    /** Generates a shared result data class. */
    private fun generateSharedResultDataClass(
        sharedResult: SharedResultManager.SharedResult,
        sourceStatement: AnnotatedSelectStatement?
    ): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder(sharedResult.name)
            .addModifiers(KModifier.DATA)
            .addKdoc("Shared result data class for queries using sharedResult=${sharedResult.name}")

        // Add interface implementation if specified
        if (sharedResult.implements != null) {
            val interfaceType = if (sharedResult.implements.contains('.')) {
                ClassName.bestGuess(sharedResult.implements)
            } else {
                // Create a ClassName without package name to avoid imports for same-package classes
                ClassName("", sharedResult.implements)
            }
            dataClassBuilder.addSuperinterface(interfaceType)
        }

        val constructorBuilder = FunSpec.constructorBuilder()
        val fieldCodeGenerator = SelectFieldCodeGenerator(createTableStatements, fileGenerationHelper.packageName)

        val mappedColumns = if (sourceStatement != null) {
            DynamicFieldMapper.getMappedColumns(sharedResult.fields, sourceStatement.src.tableAliases)
        } else {
            emptySet()
        }

        generatePropertiesWithInterfaceSupport(
            fields = sharedResult.fields,
            mappedColumns = mappedColumns,
            propertyNameGenerator = sharedResult.propertyNameGenerator,
            implementsInterface = sharedResult.implements,
            excludeOverrideFields = sharedResult.excludeOverrideFields,
            fieldCodeGenerator = fieldCodeGenerator,
            constructorBuilder = constructorBuilder
        ) { property ->
            dataClassBuilder.addProperty(property)
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        return dataClassBuilder.build()
    }

    private fun generateResultDataClassForSelectStatement(
        statement: AnnotatedSelectStatement,
        namespace: String
    ): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder("Result")
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query results.")

        // Add interface implementation if specified
        if (statement.annotations.implements != null) {
            val interfaceType = if (statement.annotations.implements.contains('.')) {
                ClassName.bestGuess(statement.annotations.implements)
            } else {
                // Create a ClassName without package name to avoid imports for same-package classes
                ClassName("", statement.annotations.implements)
            }
            dataClassBuilder.addSuperinterface(interfaceType)
        }

        val constructorBuilder = FunSpec.constructorBuilder()

        // Pass all createTableStatements to SelectFieldCodeGenerator so it can properly
        // map view columns back to original table columns. The SelectFieldCodeGenerator
        // already handles searching across all tables when needed.
        val fieldCodeGenerator = SelectFieldCodeGenerator(createTableStatements, fileGenerationHelper.packageName)
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator
        val mappedColumns = DynamicFieldMapper.getMappedColumns(statement.fields, statement.src.tableAliases)
        val effectiveExcludeOverrideFields = sharedResultManager.getEffectiveExcludeOverrideFields(statement, namespace)
        generatePropertiesWithInterfaceSupport(
            fields = statement.fields,
            mappedColumns = mappedColumns,
            propertyNameGenerator = propertyNameGeneratorType,
            implementsInterface = statement.annotations.implements,
            excludeOverrideFields = effectiveExcludeOverrideFields,
            fieldCodeGenerator = fieldCodeGenerator,
            constructorBuilder = constructorBuilder
        ) { property ->
            dataClassBuilder.addProperty(property)
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        return dataClassBuilder.build()
    }

    /**
     * Generates all Kotlin code files for queries.
     */
    fun generateCode() {
        fileGenerationHelper.generateFiles(
            namespaces = nsWithStatements.keys,
            fileGenerator = { namespace, packageName ->
                generateNamespaceDataStructuresCode(namespace, packageName)
            }
        )
    }

    /**
     * Generates a parameter data class for statements with named parameters.
     */
    private fun generateParameterDataClass(statement: AnnotatedStatement): TypeSpec {
        val paramClassBuilder = TypeSpec.classBuilder("Params")
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query parameters.")
        val paramConstructorBuilder = FunSpec.constructorBuilder()
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator

        val uniqueParams = StatementUtils.getAllNamedParameters(statement)
        uniqueParams.forEach { paramName ->
            val propertyName = propertyNameGeneratorType.convertToPropertyName(paramName)
            val propertyType = inferParameterType(paramName, statement)

            val parameter = ParameterSpec.builder(propertyName, propertyType).build()
            paramConstructorBuilder.addParameter(parameter)

            val property = PropertySpec.builder(propertyName, propertyType)
                .initializer(propertyName)
                .build()
            paramClassBuilder.addProperty(property)
        }

        paramClassBuilder.primaryConstructor(paramConstructorBuilder.build())
        return paramClassBuilder.build()
    }

    /**
     * Helper function to determine parameter type based on database schema.
     * Uses the same approach as Results generation - looks up column types from schema.
     * Returns a TypeName that respects the nullability from the database schema.
     */
    fun inferParameterType(paramName: String, statement: AnnotatedStatement): TypeName {
        if (statement is AnnotatedExecuteStatement) {
            val execStmt = statement.src
            for (withSelectStatement in execStmt.withSelectStatements) {
                if (paramName in withSelectStatement.namedParameters) {
                    val annotatedWithSelectStatement = AnnotatedSelectStatement(
                        name = "withClause",
                        src = withSelectStatement,
                        annotations = StatementAnnotationOverrides(
                            name = null,
                            propertyNameGenerator = statement.annotations.propertyNameGenerator,
                            sharedResult = null,
                            implements = statement.annotations.implements,
                            excludeOverrideFields = statement.annotations.excludeOverrideFields,
                            collectionKey = null
                        ),
                        fields = withSelectStatement.fields.map { field ->
                            AnnotatedSelectStatement.Field(
                                src = field,
                                annotations = FieldAnnotationOverrides.parse(emptyMap())
                            )
                        }
                    )
                    return inferParameterType(paramName, annotatedWithSelectStatement)
                }
            }
        }

        // Check for CAST expressions first - this takes precedence over other type inference
        val castType = getCastTypeForParameter(paramName, statement)
        if (castType != null) {
            return SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(castType)
        }

        // Check for collection parameters (IN clauses) across all statement types
        val associatedColumn = getAssociatedColumn(statement, paramName)
        if (associatedColumn is AssociatedColumn.Collection) {
            val column = columnLookup.findColumnForParameter(statement, paramName)
            return handleCollectionParameterType(column)
        }

        // Special handling for LIMIT and OFFSET parameters in SELECT statements
        if (statement is AnnotatedSelectStatement) {
            val selectStmt = statement.src
            if (paramName == selectStmt.limitNamedParam || paramName == selectStmt.offsetNamedParam) {
                return ClassName("kotlin", "Long")
            }
        }

        val column = columnLookup.findColumnForParameter(statement, paramName)
        if (column != null) {
            val baseType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val isNullable = column.isNullable()

            return SqliteTypeToKotlinCodeConverter.determinePropertyType(
                baseType,
                propertyType,
                isNullable,
                fileGenerationHelper.packageName
            )
        }

        return ClassName("kotlin", "String")
    }

    /**
     * Helper function to get the CAST target type for a parameter if it's used within a CAST expression.
     * Returns null if the parameter is not used in a CAST expression.
     */
    private fun getCastTypeForParameter(paramName: String, statement: AnnotatedStatement): String? {
        return when (statement) {
            is AnnotatedSelectStatement -> {
                statement.src.parameterCastTypes[paramName]
            }

            is AnnotatedExecuteStatement -> {
                statement.src.parameterCastTypes[paramName]
            }

            else -> null
        }
    }

    /**
     * Helper function to extract SQL string from any statement type.
     */
    private fun getSqlFromStatement(statement: AnnotatedStatement): String {
        return when (statement) {
            is AnnotatedSelectStatement -> statement.src.sql
            is AnnotatedExecuteStatement -> statement.src.sql
            is AnnotatedCreateTableStatement -> statement.src.sql
            is AnnotatedCreateViewStatement -> statement.src.sql
        }
    }

    /**
     * Helper function to extract affected tables from any statement type.
     * Returns a set of table names that are used in the statement (main table and joined tables).
     */
    private fun getAffectedTablesFromStatement(statement: AnnotatedStatement): Set<String> {
        return when (statement) {
            is AnnotatedSelectStatement -> {
                val tables = mutableSetOf<String>()
                // Add main table if present
                statement.src.fromTable?.let { tables.add(it) }
                // Add joined tables
                tables.addAll(statement.src.joinTables)
                tables
            }

            is AnnotatedExecuteStatement -> {
                // For INSERT/UPDATE/DELETE, return the main table
                setOf(statement.src.table)
            }

            is AnnotatedCreateTableStatement -> {
                // For CREATE TABLE, return the table being created
                setOf(statement.src.tableName)
            }

            is AnnotatedCreateViewStatement -> {
                // For CREATE VIEW, we could potentially extract tables from the view definition
                // but for now, return empty set as views don't directly affect tables
                emptySet()
            }
        }
    }

    /**
     * Helper function to get the associated column for a parameter from any statement type.
     */
    private fun getAssociatedColumn(statement: AnnotatedStatement, paramName: String): AssociatedColumn? {
        return when (statement) {
            is AnnotatedExecuteStatement -> {
                when (val src = statement.src) {
                    is DeleteStatement -> src.namedParametersToColumns[paramName]
                    is UpdateStatement -> src.namedParametersToColumns[paramName]
                    is InsertStatement -> {
                        // InsertStatement doesn't have namedParametersToColumns
                        // We need to check if this parameter is a collection type differently
                        // For now, return null as InsertStatement typically doesn't have IN clauses
                        null
                    }

                    else -> null
                }
            }

            is AnnotatedSelectStatement -> statement.src.namedParametersToColumns[paramName]
            else -> null
        }
    }

    /**
     * Helper function to handle Collection parameter types for IN clauses.
     */
    private fun handleCollectionParameterType(
        column: AnnotatedCreateTableStatement.Column?,
    ): TypeName {
        if (column != null) {
            val baseType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(column.src.dataType)
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val elementType = SqliteTypeToKotlinCodeConverter.determinePropertyType(
                baseType, propertyType, column.isNullable(),
            )
            return ClassName("kotlin.collections", "Collection").parameterizedBy(elementType)
        }
        return ClassName("kotlin.collections", "Collection").parameterizedBy(ClassName("kotlin", "String"))
    }

    /**
     * Generates a Joined data class for dynamic field mapping that includes ALL columns from the SELECT statement.
     * This class contains all fields without any dynamic field mapping or column exclusions.
     * The removeAliasPrefix annotation is ignored - joined column names are used.
     * Used for both mappingType=collection and mappingType=perRow.
     */
    private fun generateJoinedDataClass(
        statement: AnnotatedSelectStatement,
    ): TypeSpec {
        // Determine the class name based on whether this is a shared result or regular result
        val baseClassName = statement.annotations.sharedResult ?: "Result"
        return generateJoinedDataClassInternal(
            joinedClassName = "${baseClassName}_Joined",
            fields = statement.fields,
            propertyNameGenerator = statement.annotations.propertyNameGenerator
        )
    }

    /**
     * Internal method that contains the common logic for generating joined data classes.
     * This eliminates code duplication between generateJoinedDataClass and generateJoinedDataClassForSharedResult.
     */
    private fun generateJoinedDataClassInternal(
        joinedClassName: String,
        fields: List<AnnotatedSelectStatement.Field>,
        propertyNameGenerator: PropertyNameGeneratorType
    ): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder(joinedClassName)
            .addModifiers(KModifier.DATA)
            .addKdoc("Joined row data containing all fields from the SQL query without any dynamic field mapping")

        val constructorBuilder = FunSpec.constructorBuilder()
        val fieldCodeGenerator = SelectFieldCodeGenerator(createTableStatements, fileGenerationHelper.packageName)

        // Add ALL fields from the SELECT statement (including those that would normally be mapped to dynamic fields)
        fields.forEach { field ->
            // Skip dynamic fields themselves, but include all regular database columns
            if (!field.annotations.isDynamicField) {
                // Generate the property using the public method which correctly applies custom property name annotations
                val generatedProperty = fieldCodeGenerator.generateProperty(field, propertyNameGenerator)

                // For joined data classes, make all fields from joined tables nullable
                // This is critical because LEFT JOINs can result in NULL values when no matching record exists
                val adjustedType = adjustTypeForJoinNullability(generatedProperty.type, field, fields)
                val property = PropertySpec.builder(generatedProperty.name, adjustedType)
                    .initializer(generatedProperty.name)
                    .build()

                dataClassBuilder.addProperty(property)
                constructorBuilder.addParameter(generatedProperty.name, adjustedType)
            }
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        return dataClassBuilder.build()
    }

    /**
     * Adjusts the type for JOIN nullability in joined data classes.
     * For LEFT JOINs, all fields from joined tables must be nullable even if they're non-nullable in the schema.
     *
     * @param originalType The original type from the field code generator
     * @param field The field being processed
     * @param allFields All fields in the statement (to determine main table vs joined tables)
     * @return The adjusted type (nullable if from joined table)
     */
    private fun adjustTypeForJoinNullability(
        originalType: TypeName,
        field: AnnotatedSelectStatement.Field,
        allFields: List<AnnotatedSelectStatement.Field>
    ): TypeName {
        val fieldTableAlias = field.src.tableName
        if (fieldTableAlias.isBlank()) {
            return originalType
        }
        // Find the main table alias (the one that appears first in the FROM clause)
        // For queries with dynamic field mapping, we can identify this by finding the table alias
        // that appears in non-dynamic fields first, or by checking if it's the sourceTable of any dynamic field
        val mainTableAlias = findMainTableAlias(allFields)
        // If this field comes from a joined table (not the main table), make it nullable
        if (fieldTableAlias != mainTableAlias) {
            return originalType.copy(nullable = true)
        }
        // Field is from the main table, keep original nullability
        return originalType
    }

    /**
     * Finds the main table alias (FROM table) by analyzing the fields.
     * The main table is typically the one that doesn't have a sourceTable annotation in dynamic fields.
     */
    fun findMainTableAlias(allFields: List<AnnotatedSelectStatement.Field>): String? {
        // Find dynamic fields with sourceTable annotation
        val sourceTableAliases = allFields
            .filter { it.annotations.isDynamicField && it.annotations.sourceTable != null }
            .map { it.annotations.sourceTable!! }
            .toSet()

        // Find the first table alias that is NOT a sourceTable (this should be the main table)
        val allTableAliases = allFields
            .map { it.src.tableName }
            .filter { it.isNotBlank() }
            .distinct()

        return allTableAliases.firstOrNull { it !in sourceTableAliases }
    }

    /**
     * Generates properties for a data class with interface implementation support.
     */
    fun generatePropertiesWithInterfaceSupport(
        fields: List<AnnotatedSelectStatement.Field>,
        mappedColumns: Set<String>,
        propertyNameGenerator: PropertyNameGeneratorType,
        implementsInterface: String?,
        excludeOverrideFields: Set<String>?,
        fieldCodeGenerator: SelectFieldCodeGenerator,
        constructorBuilder: FunSpec.Builder,
        onPropertyGenerated: (PropertySpec) -> Unit
    ) {
        fields.forEach { field ->
            // Skip columns that are mapped to dynamic fields
            if (!mappedColumns.contains(field.src.fieldName)) {
                val parameter = fieldCodeGenerator.generateParameter(field, propertyNameGenerator)
                constructorBuilder.addParameter(parameter)

                val property = fieldCodeGenerator.generateProperty(field, propertyNameGenerator)

                // Add override modifier if implementing an interface and field is not excluded
                val finalProperty = if (implementsInterface != null) {
                    val fieldName = property.name
                    val isExcluded = excludeOverrideFields?.contains(fieldName) == true

                    if (!isExcluded) {
                        property.toBuilder().addModifiers(KModifier.OVERRIDE).build()
                    } else {
                        property
                    }
                } else {
                    property
                }

                onPropertyGenerated(finalProperty)
            }
        }
    }
}
