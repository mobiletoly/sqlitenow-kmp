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
import kotlin.collections.filterIsInstance

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
            it.execute(conn) as AnnotatedCreateTableStatement
        }
    val createViewStatements = statementExecutors
        .filterIsInstance<CreateViewStatementExecutor>()
        .map {
            it.execute(conn) as AnnotatedCreateViewStatement
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
        val fileName = namespace.capitalized()
        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated code for $namespace namespace queries")
            .addFileComment("\nDo not modify this file manually")

        val capitalizedNamespace = namespace.capitalized()
        val namespaceObject = TypeSpec.objectBuilder(capitalizedNamespace)
            .addKdoc("Contains queries for the $namespace namespace")

        val allStatements = nsWithStatements[namespace] ?: emptyList()
        val statementProcessor = StatementProcessor(allStatements)

        // Register shared results for SELECT statements
        statementProcessor.processStatements(
            onSelectStatement = { statement ->
                sharedResultManager.registerSharedResult(statement, namespace)
            },
            onExecuteStatement = { /* No shared results for execute statements */ }
        )

        // Generate SharedResult object if there are any shared results
        val sharedResults = sharedResultManager.getSharedResultsByNamespace()[namespace]
        if (!sharedResults.isNullOrEmpty()) {
            val sharedResultObject = generateSharedResultObject(sharedResults)
            namespaceObject.addType(sharedResultObject)
        }

        // Generate query-specific objects
        statementProcessor.processStatements(
            onSelectStatement = { statement ->
                val queryObject = generateQueryObject(statement, namespace)
                namespaceObject.addType(queryObject)
            },
            onExecuteStatement = { statement ->
                val queryObject = generateQueryObject(statement, namespace)
                namespaceObject.addType(queryObject)
            }
        )

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
            val resultDataClass = generateResultDataClass(statement, namespace)
            queryObjectBuilder.addType(resultDataClass)
        }

        return queryObjectBuilder.build()
    }

    /** Generates the SharedResult object containing all shared result classes for a namespace. */
    private fun generateSharedResultObject(sharedResults: List<SharedResultManager.SharedResult>): TypeSpec {
        val sharedResultObjectBuilder = TypeSpec.objectBuilder(SharedResultTypeUtils.SHARED_RESULT_OBJECT_NAME)
            .addKdoc("Contains shared result classes that can be used across multiple queries.")

        sharedResults.forEach { sharedResult ->
            val resultDataClass = generateSharedResultDataClass(sharedResult)
            sharedResultObjectBuilder.addType(resultDataClass)
        }

        return sharedResultObjectBuilder.build()
    }

    /** Generates a shared result data class. */
    private fun generateSharedResultDataClass(sharedResult: SharedResultManager.SharedResult): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder(sharedResult.name)
            .addModifiers(KModifier.DATA)
            .addKdoc("Shared result data class for queries using @@sharedResult=${sharedResult.name}.")

        // Add interface implementation if specified
        if (sharedResult.implements != null) {
            val interfaceType = ClassName.bestGuess(sharedResult.implements)
            dataClassBuilder.addSuperinterface(interfaceType)
        }

        val constructorBuilder = FunSpec.constructorBuilder()
        val fieldCodeGenerator = SelectFieldCodeGenerator(createTableStatements)

        sharedResult.fields.forEach { field ->
            val parameter = fieldCodeGenerator.generateParameter(field, sharedResult.propertyNameGenerator)
            constructorBuilder.addParameter(parameter)

            val property = fieldCodeGenerator.generateProperty(field, sharedResult.propertyNameGenerator)

            // Add override modifier if implementing an interface and field is not excluded
            val propertyBuilder = property.toBuilder()
            if (sharedResult.implements != null) {
                val fieldName = property.name
                val isExcluded = sharedResult.excludeOverrideFields?.contains(fieldName) == true

                if (!isExcluded) {
                    propertyBuilder.addModifiers(KModifier.OVERRIDE)
                }
            }

            dataClassBuilder.addProperty(propertyBuilder.build())
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        return dataClassBuilder.build()
    }

    /** Generates a result data class for a SELECT statement. */
    private fun generateResultDataClass(statement: AnnotatedSelectStatement, namespace: String): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder("Result")
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query results.")

        // Add interface implementation if specified
        if (statement.annotations.implements != null) {
            val interfaceType = ClassName.bestGuess(statement.annotations.implements)
            dataClassBuilder.addSuperinterface(interfaceType)
        }

        val constructorBuilder = FunSpec.constructorBuilder()

        // Pass all createTableStatements to SelectFieldCodeGenerator so it can properly
        // map view columns back to original table columns. The SelectFieldCodeGenerator
        // already handles searching across all tables when needed.
        val fieldCodeGenerator = SelectFieldCodeGenerator(createTableStatements)
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator

        statement.fields.forEach { field ->
            val parameter = fieldCodeGenerator.generateParameter(field, propertyNameGeneratorType)
            constructorBuilder.addParameter(parameter)

            val property = fieldCodeGenerator.generateProperty(field, propertyNameGeneratorType)

            // Add override modifier if implementing an interface and field is not excluded
            val propertyBuilder = property.toBuilder()
            if (statement.annotations.implements != null) {
                val fieldName = property.name
                val effectiveExcludeOverrideFields = sharedResultManager.getEffectiveExcludeOverrideFields(statement, namespace)
                val isExcluded = effectiveExcludeOverrideFields?.contains(fieldName) == true
                if (!isExcluded) {
                    propertyBuilder.addModifiers(KModifier.OVERRIDE)
                }
            }

            dataClassBuilder.addProperty(propertyBuilder.build())
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        return dataClassBuilder.build()
    }

    /** Generates all Kotlin code files for queries. */
    fun generateCode() {
        fileGenerationHelper.generateFiles(
            namespaces = nsWithStatements.keys,
            fileGenerator = { namespace, packageName ->
                generateNamespaceDataStructuresCode(namespace, packageName)
            }
        )
    }

    /** Generates a parameter data class for statements with named parameters. */
    private fun generateParameterDataClass(statement: AnnotatedStatement): TypeSpec {
        val uniqueParams = StatementUtils.getAllNamedParameters(statement)

        val paramClassBuilder = TypeSpec.classBuilder("Params")
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query parameters.")

        val paramConstructorBuilder = FunSpec.constructorBuilder()
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator

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
                            excludeOverrideFields = statement.annotations.excludeOverrideFields
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
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE]
            val isNullable = column.isNullable()

            return SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)
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
     * Consolidates duplicate SQL extraction logic.
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
                tables // Set automatically handles duplicates
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
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE]
            val isNullable = column.isNullable()

            val elementType = SqliteTypeToKotlinCodeConverter.determinePropertyType(baseType, propertyType, isNullable)

            return ClassName("kotlin.collections", "Collection").parameterizedBy(elementType)
        }

        return ClassName("kotlin.collections", "Collection").parameterizedBy(ClassName("kotlin", "String"))
    }
}
