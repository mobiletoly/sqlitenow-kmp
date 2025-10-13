/*
 * Copyright 2025 Anatoliy Pochkin
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
package dev.goquick.sqlitenow.gradle.generator.data

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatementExecutor
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateViewStatementExecutor
import dev.goquick.sqlitenow.gradle.sqlinspect.DeferredStatementExecutor
import dev.goquick.sqlitenow.gradle.util.FileGenerationHelper
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.logger
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.SharedResultManager
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.processing.StatementProcessor
import dev.goquick.sqlitenow.gradle.processing.StatementUtils
import java.io.File
import java.sql.Connection

open class DataStructCodeGenerator(
    conn: Connection,
    queriesDir: File,
    packageName: String,
    private val outputDir: File,
    statementExecutors: MutableList<DeferredStatementExecutor>,
    providedCreateTableStatements: List<AnnotatedCreateTableStatement>? = null
) {
    open val createTableStatements = providedCreateTableStatements ?: statementExecutors
        .filterIsInstance<CreateTableStatementExecutor>()
        .map {
            try {
                it.execute(conn) as AnnotatedCreateTableStatement
            } catch (e: Exception) {
                logger.error(
                    """|
                    |Failed to execute CREATE TABLE statement:
                    |${it.reportContext()}
                    """.trimMargin()
                )
                throw e
            }
        }
    val createViewStatements = run {
        val viewExecs = statementExecutors.filterIsInstance<CreateViewStatementExecutor>()
        val sorted = sortViewsByDependencies(viewExecs)
        sorted.map {
            try {
                it.execute(conn) as AnnotatedCreateViewStatement
            } catch (e: Exception) {
                logger.error(
                    """
                    |Failed to execute CREATE VIEW statement:
                    |${it.reportContext()}
                    """.trimIndent()
                )
                throw e
            }
        }
    }

    private val createViewStatementsByName: Map<String, AnnotatedCreateViewStatement> =
        createViewStatements.associateBy { it.src.viewName.lowercase() }

    private val viewTableDependencies: Map<String, Set<String>> by lazy {
        createViewStatements.associate { view ->
            val resolved = resolveViewDependencies(view.src.viewName.lowercase(), mutableSetOf())
            view.src.viewName.lowercase() to resolved
        }
    }

    private val annotationResolver =
        FieldAnnotationResolver(createTableStatements, createViewStatements)
    private val fileGenerationHelper = FileGenerationHelper(packageName, outputDir)
    private val stmtProcessingHelper = StatementProcessingHelper(conn, annotationResolver)
    private val sharedResultManager = SharedResultManager()
    val nsWithStatements = stmtProcessingHelper.processQueriesDirectory(queriesDir)
    internal val generatorContext = GeneratorContext(
        packageName = packageName,
        outputDir = outputDir,
        createTableStatements = createTableStatements,
        createViewStatements = createViewStatements,
        nsWithStatements = nsWithStatements
    )
    private val propertyEmitter = DataStructPropertyEmitter()
    private val joinedEmitter = DataStructJoinedEmitter(generatorContext)
    private val resultEmitter = DataStructResultEmitter(generatorContext, propertyEmitter)
    private val resultFileEmitter = DataStructResultFileEmitter(
        generatorContext = generatorContext,
        joinedEmitter = joinedEmitter,
        resultEmitter = resultEmitter,
        outputDir = outputDir
    )

    private fun sortViewsByDependencies(viewExecutors: List<CreateViewStatementExecutor>): List<CreateViewStatementExecutor> {
        if (viewExecutors.size <= 1) return viewExecutors
        val nameToExec = viewExecutors.associateBy { it.viewName() }
        val viewNames = nameToExec.keys.toSet()

        val adj = mutableMapOf<String, MutableList<String>>()
        val indeg = mutableMapOf<String, Int>().apply { viewNames.forEach { this[it] = 0 } }

        viewExecutors.forEach { exec ->
            val v = exec.viewName()
            val deps = exec.referencedTableOrViewNames().filter { it in viewNames }
            deps.forEach { dep ->
                adj.getOrPut(dep) { mutableListOf() }.add(v)
                indeg[v] = (indeg[v] ?: 0) + 1
            }
        }

        val queue = ArrayDeque(indeg.filter { it.value == 0 }.keys)
        val orderedNames = mutableListOf<String>()
        while (queue.isNotEmpty()) {
            val u = queue.removeFirst()
            orderedNames.add(u)
            adj[u]?.forEach { w ->
                indeg[w] = (indeg[w] ?: 0) - 1
                if ((indeg[w] ?: 0) == 0) queue.add(w)
            }
        }

        if (orderedNames.size < viewExecutors.size) {
            val remaining = viewExecutors.map { it.viewName() }.filter { it !in orderedNames }
            orderedNames.addAll(remaining)
        }

        return orderedNames.mapNotNull { nameToExec[it] }
    }

    fun generateNamespaceDataStructuresCode(
        namespace: String,
        packageName: String,
    ): FileSpec.Builder {
        val fileName = "${pascalize(namespace)}Query"
        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated code for $namespace namespace queries")
            .addFileComment("\nDo not modify this file manually")
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "Suppress"))
                    .addMember("%S", "UNNECESSARY_NOT_NULL_ASSERTION")
                    .build()
            )
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )

        val capitalizedNamespace = "${pascalize(namespace)}Query"
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
                if (statement.annotations.queryResult != null) {
                    sharedResultsWithContext[statement.annotations.queryResult] = statement
                }
                val queryObject = generateQueryObject(namespace, packageName, statement)
                namespaceObject.addType(queryObject)
            },
            onExecuteStatement = { statement: AnnotatedExecuteStatement ->
                val queryObject = generateQueryObject(namespace, packageName, statement)
                namespaceObject.addType(queryObject)
            }
        )

        // Generate separate result class files for SELECT statements
        statementProcessor.processStatements(
            onSelectStatement = { statement: AnnotatedSelectStatement ->
                resultFileEmitter.writeSelectResultFile(statement, namespace, packageName)

                // Generate separate Joined class file if needed
                if (statement.hasDynamicFieldMapping()) {
                    resultFileEmitter.writeJoinedClassFile(statement, namespace, packageName)
                }
            },
            onExecuteStatement = { statement: AnnotatedExecuteStatement ->
                if (statement.hasReturningClause()) {
                    resultFileEmitter.writeExecuteResultFile(statement, namespace, packageName)
                }
            }
        )

        fileSpecBuilder.addType(namespaceObject.build())

        return fileSpecBuilder
    }

    /** Generates a query-specific object (e.g., Person.SelectWeird) containing SQL, Params, and Result. */
    private fun generateQueryObject(
        namespace: String,
        packageName: String,
        statement: AnnotatedStatement,
    ): TypeSpec {
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
        generateParameterDefinitions(statement)?.let { paramsType ->
            queryObjectBuilder.addType(paramsType)
        }

        return queryObjectBuilder.build()
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

    private data class ParameterDescriptor(
        val propertyName: String,
        val type: TypeName,
        val isNullable: Boolean,
    )

    /**
     * Generates parameter data structures (Params + Builder) for statements with named parameters.
     */
    private fun generateParameterDefinitions(
        statement: AnnotatedStatement,
    ): TypeSpec? {
        val uniqueParams = StatementUtils.getAllNamedParameters(statement)
        if (uniqueParams.isEmpty()) return null

        val paramClassBuilder = TypeSpec.classBuilder("Params")
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query parameters.")
        val paramConstructorBuilder = FunSpec.constructorBuilder()
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator

        val paramCollectedProps = mutableListOf<PropertySpec>()
        val descriptors = mutableListOf<ParameterDescriptor>()
        uniqueParams.forEach { paramName ->
            val propertyName = propertyNameGeneratorType.convertToPropertyName(paramName)
            val propertyType = inferParameterType(paramName, statement)

            val parameter = ParameterSpec.builder(propertyName, propertyType).build()
            paramConstructorBuilder.addParameter(parameter)

            val property = PropertySpec.builder(propertyName, propertyType)
                .initializer(propertyName)
                .build()
            paramCollectedProps.add(property)
            paramClassBuilder.addProperty(property)

            descriptors += ParameterDescriptor(
                propertyName = propertyName,
                type = propertyType,
                isNullable = propertyType.isNullable,
            )
        }

        paramClassBuilder.primaryConstructor(paramConstructorBuilder.build())
        DataStructUtils.addArraySafeEqualsAndHashCodeIfNeeded(
            classBuilder = paramClassBuilder,
            className = "Params",
            properties = paramCollectedProps
        )

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
                            queryResult = null,
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
            return SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(castType)
        }

        // Check for collection parameters (IN clauses) across all statement types
        val associatedColumn = getAssociatedColumn(statement, paramName)
        if (associatedColumn is AssociatedColumn.Collection) {
            val column = generatorContext.columnLookup.findColumnForParameter(statement, paramName)
            return handleCollectionParameterType(column)
        }

        // Special handling for LIMIT and OFFSET parameters in SELECT statements
        if (statement is AnnotatedSelectStatement) {
            val selectStmt = statement.src
            if (paramName == selectStmt.limitNamedParam || paramName == selectStmt.offsetNamedParam) {
                return ClassName("kotlin", "Long")
            }
        }

        val column = generatorContext.columnLookup.findColumnForParameter(statement, paramName)
        if (column != null) {
            val baseType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(column.src.dataType)
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val isNullable = column.isNullable()

            return SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
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
        fun expandTables(tables: Set<String>): Set<String> {
            val expanded = mutableSetOf<String>()
            tables.forEach { table ->
                val key = table.lowercase()
                expanded += key
                val viewDeps = viewTableDependencies[key]
                if (viewDeps != null) {
                    expanded += viewDeps
                }
            }
            return expanded
        }

        return when (statement) {
            is AnnotatedSelectStatement -> {
                val tables = mutableSetOf<String>()
                // Add main table if present
                statement.src.fromTable?.let { tables.add(it) }
                // Add joined tables
                tables.addAll(statement.src.joinTables)
                expandTables(tables)
            }

            is AnnotatedExecuteStatement -> {
                // For INSERT/UPDATE/DELETE, return the main table
                expandTables(setOf(statement.src.table))
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

    private fun resolveViewDependencies(
        viewName: String,
        seen: MutableSet<String>,
    ): Set<String> {
        if (!seen.add(viewName)) return emptySet()
        val view = createViewStatementsByName[viewName] ?: return emptySet()
        val dependencies = mutableSetOf<String>()
        val select = view.src.selectStatement
        val referencedTables = buildList {
            select.fromTable?.let { add(it) }
            addAll(select.joinTables)
        }
        referencedTables.forEach { table ->
            val key = table.lowercase()
            dependencies += key
            dependencies += resolveViewDependencies(key, seen)
        }
        return dependencies
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
            val baseType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(column.src.dataType)
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val elementType = SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
                baseType, propertyType, column.isNullable(),
            )
            return ClassName("kotlin.collections", "Collection").parameterizedBy(elementType)
        }
        return ClassName("kotlin.collections", "Collection").parameterizedBy(ClassName("kotlin", "String"))
    }
}
