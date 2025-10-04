package dev.goquick.sqlitenow.gradle.processing

import dev.goquick.sqlitenow.gradle.SqlFileProcessor
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.logger
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import java.io.File
import java.sql.Connection
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.update.Update

/**
 * Helper class for processing SQL statements from files.
 * Provides shared functionality for parsing SQL files and creating annotated statements.
 */
class StatementProcessingHelper(
    private val conn: Connection,
    private val annotationResolver: FieldAnnotationResolver? = null
) {

    /**
     * Processes SQL files from a queries directory and groups them by namespace.
     * This centralizes the common pattern of scanning directories and parsing SQL files.
     *
     * @param queriesDir The directory containing SQL query files organized by namespace
     * @return Map of namespace to list of annotated statements
     */
    fun processQueriesDirectory(queriesDir: File): Map<String, List<AnnotatedStatement>> {
        val nsWithFiles = scanQueriesByNamespace(queriesDir)

        return nsWithFiles.map { (namespace, files) ->
            namespace to files.map { file ->
                try {
                    processQueryFile(file)
                } catch (e: Exception) {
                    logger.error("*** FAILED TO PROCESS SQL FILE ***")
                    logger.error("File: ${file.absolutePath}")
                    logger.error("Namespace: $namespace")
                    logger.error("Error: ${e.message}")
                    if (e.cause != null) {
                        logger.error("Caused by: ${e.cause?.message}")
                    }
                    throw RuntimeException("Failed to process SQL file '${file.name}' in namespace '$namespace'", e)
                }
            }
        }.toMap()
    }

    /**
     * Processes a single SQL query file and returns an annotated statement.
     *
     * @param file The SQL file to process
     * @return The annotated statement created from the file
     */
    fun processQueryFile(file: File): AnnotatedStatement {
        val stmtName = file.nameWithoutExtension
        val sqlStatements = SqlFileProcessor.parseAllSqlFiles(listOf(file))
        validateSqlStatements(sqlStatements, file.name)

        val sqlStatement = sqlStatements.first()
        val parsedStatement = CCJSqlParserUtil.parse(normalizeSelectForParser(sqlStatement.sql))

        return try {
            when (parsedStatement) {
                is PlainSelect -> {
                    createAnnotatedSelectStatement(stmtName, parsedStatement, sqlStatement)
                }

                is Insert, is Delete, is Update -> {
                    createAnnotatedExecuteStatement(stmtName, parsedStatement, sqlStatement)
                }

                else -> {
                    throw RuntimeException("Unsupported statement type in ${file.name}")
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to process SQL statement in file: ${file.absolutePath}")
            logger.error("Statement name: $stmtName")
            logger.error("SQL content:")
            logger.error(sqlStatement.topComments.joinToString("\n"))
            logger.error(sqlStatement.sql)
            logger.error("Parse error: ${e.message}")
            throw RuntimeException("Failed to parse SQL statement '$stmtName' in file '${file.name}'", e)
        }
    }

    /**
     * Scans a queries directory and groups files by namespace (subdirectory).
     *
     * @param queriesDir The root queries directory
     * @return Map of namespace to list of SQL files
     */
    private fun scanQueriesByNamespace(queriesDir: File): Map<String, List<File>> {
        if (!queriesDir.exists() || !queriesDir.isDirectory) {
            return emptyMap()
        }

        return queriesDir.listFiles()
            ?.filter { it.isDirectory }
            ?.associate { namespaceDir ->
                val namespace = namespaceDir.name
                val sqlFiles = namespaceDir.listFiles()
                    ?.filter { it.isFile && it.extension == "sql" }
                    ?: emptyList()
                namespace to sqlFiles
            } ?: emptyMap()
    }

    /**
     * Validates that SQL statements are valid for processing.
     *
     * @param sqlStatements List of SQL statements to validate
     * @param fileName Name of the file being processed (for error messages)
     */
    private fun validateSqlStatements(sqlStatements: List<SqlSingleStatement>, fileName: String) {
        if (sqlStatements.isEmpty()) {
            throw RuntimeException("No SQL statements found in file: $fileName")
        }
        if (sqlStatements.size > 1) {
            throw RuntimeException("Only one SQL statement per file is supported: $fileName")
        }
    }

    /**
     * Creates an annotated SELECT statement from parsed components.
     */
    private fun createAnnotatedSelectStatement(
        stmtName: String,
        parsedStatement: PlainSelect,
        sqlStatement: SqlSingleStatement
    ): AnnotatedSelectStatement {
        val stmt = SelectStatement.parse(
            conn = conn,
            select = parsedStatement,
        )

        // Extract annotations from comments
        var statementAnnotations = StatementAnnotationOverrides.parse(
            extractAnnotations(sqlStatement.topComments)
        )
        val fieldAnnotations = extractFieldAssociatedAnnotations(sqlStatement.innerComments)

        // Build fields with merged annotations
        val fields = mutableListOf<AnnotatedSelectStatement.Field>()

        // Add regular database fields
        stmt.fields.forEach { column ->
            val enrichedColumn = enrichFieldSourceWithViewExpression(column, stmt)
            val annotations = mergeFieldAnnotations(
                enrichedColumn,
                fieldAnnotations,
                stmt,
                annotationResolver
            )
            val aliasPath = column.tableName.takeIf { it.isNotBlank() }?.let { alias ->
                computeAliasPathForAlias(stmt, alias)
            } ?: emptyList()
            fields.add(
                AnnotatedSelectStatement.Field(
                    src = enrichedColumn,
                    annotations = annotations,
                    aliasPath = aliasPath
                )
            )
        }

        // Add dynamic fields from SELECT-level annotations
        val dynamicFields = mutableListOf<AnnotatedSelectStatement.Field>()
        fieldAnnotations.forEach { (fieldName, annotations) ->
            if (annotations[AnnotationConstants.IS_DYNAMIC_FIELD] == true) {
                // Create a dummy FieldSource for dynamic fields
                val dummyFieldSource = SelectStatement.FieldSource(
                    fieldName = fieldName,
                    tableName = "", // Dynamic fields don't belong to any table
                    originalColumnName = fieldName,
                    dataType = "DYNAMIC", // Special type for dynamic fields
                    expression = null
                )
                val fieldAnnotationOverrides = FieldAnnotationOverrides.parse(annotations)
                val aliasPath = computeAliasPathForSelectField(stmt, fieldAnnotationOverrides.sourceTable)
                val dynamicField = AnnotatedSelectStatement.Field(
                    src = dummyFieldSource,
                    annotations = fieldAnnotationOverrides,
                    aliasPath = aliasPath
                )
                dynamicFields.add(dynamicField)
                fields.add(dynamicField)
            }
        }

        // Inherit dynamic field mappings from referenced VIEWS (unless overridden by SELECT)
        if (annotationResolver != null) {
            // Build a set of already-declared dynamic field names to enforce precedence
            val declaredDynamicNames = dynamicFields.map { it.src.fieldName }.toMutableSet()

            // Walk through FROM + JOIN aliases and find views
            stmt.tableAliases.forEach { (alias, tableOrViewName) ->
                val view = annotationResolver.findView(tableOrViewName)
                if (view != null) {
                    // Merge statement-level collectionKey from view if not specified on SELECT
                    if (statementAnnotations.collectionKey == null && view.annotations.collectionKey != null) {
                        statementAnnotations = statementAnnotations.copy(
                            collectionKey = view.annotations.collectionKey
                        )
                    }
                    // Collect dynamic fields declared on this view and any underlying views it references
                    val inheritedDynamicFields = collectTransitiveDynamicFields(view, annotationResolver, emptyList())
                    // For each dynamic field, inject into this SELECT unless overridden
                    inheritedDynamicFields.forEach { df ->
                        if (!declaredDynamicNames.contains(df.name)) {
                            // Adapt view-level mapping to this SELECT context: set sourceTable to the view alias
                            val adapted = df.annotations.copy(
                                sourceTable = alias
                            )
                            val dummyFieldSource = SelectStatement.FieldSource(
                                fieldName = df.name,
                                tableName = "",
                                originalColumnName = df.name,
                                dataType = "DYNAMIC",
                                expression = null
                            )
                            val aliasPrefix = computeAliasPathForAlias(stmt, alias)
                            val dynField = AnnotatedSelectStatement.Field(
                                src = dummyFieldSource,
                                annotations = adapted,
                                aliasPath = mergeAliasPaths(aliasPrefix, df.aliasPath)
                            )
                            dynamicFields.add(dynField)
                            fields.add(dynField)
                            declaredDynamicNames.add(df.name)
                        }
                    }
                }
            }
        }

        // Enrich regular fields with alias path information inferred from dynamic mappings
        val aliasPathHints = DynamicFieldMapper.createDynamicFieldMappings(stmt, fields)
            .flatMap { mapping ->
                mapping.columns.map { column -> column.fieldName to mapping.aliasPath }
            }
            .groupBy({ it.first }, { it.second })
            .mapValues { entry -> entry.value.toMutableList() }

        fields.forEachIndexed { index, field ->
            if (!field.annotations.isDynamicField && field.aliasPath.isEmpty()) {
                val queue = aliasPathHints[field.src.fieldName]
                val hint = queue?.firstOrNull()
                if (hint != null && hint.isNotEmpty()) {
                    fields[index] = field.copy(aliasPath = hint)
                    queue.removeAt(0)
                }
            }
        }

        // Validate alias.column format if mappingType is used
        if (dynamicFields.any { it.annotations.mappingType != null }) {
            DynamicFieldMapper.validateAliasColumnFormat(stmt, dynamicFields)
        }

        return AnnotatedSelectStatement(
            name = stmtName,
            src = stmt,
            annotations = statementAnnotations,
            fields = fields
        )
    }

    private data class DynamicFieldDescriptor(
        val name: String,
        val annotations: FieldAnnotationOverrides,
        val aliasPath: List<String>
    )

    private fun computeAliasPathForSelectField(
        selectStatement: SelectStatement,
        sourceAlias: String?
    ): List<String> {
        if (sourceAlias.isNullOrBlank()) {
            val primaryAlias = selectStatement.tableAliases.keys.firstOrNull()
                ?: selectStatement.fromTable
            return primaryAlias?.let { listOf(it) } ?: emptyList()
        }

        val aliasPath = computeAliasPathForAlias(selectStatement, sourceAlias)
        if (aliasPath.isNotEmpty()) {
            return aliasPath
        }

        // Fallback for source aliases not present on the select statement (e.g. nested view aliases)
        val primaryAlias = selectStatement.tableAliases.keys.firstOrNull()
            ?: selectStatement.fromTable
        return buildList {
            if (!primaryAlias.isNullOrBlank()) {
                add(primaryAlias)
            }
            if (isEmpty() || last() != sourceAlias) {
                add(sourceAlias)
            }
        }
    }

    private fun computeAliasPathForAlias(
        selectStatement: SelectStatement,
        alias: String
    ): List<String> {
        if (alias.isBlank()) return emptyList()

        val tableAliases = selectStatement.tableAliases
        val primaryAlias = tableAliases.keys.firstOrNull()
            ?: selectStatement.fromTable

        val containsAlias = tableAliases.containsKey(alias)
        if (!containsAlias) {
            return buildList {
                if (!primaryAlias.isNullOrBlank()) {
                    add(primaryAlias)
                }
                if (isEmpty() || last() != alias) {
                    add(alias)
                }
            }
        }

        return buildList {
            if (!primaryAlias.isNullOrBlank()) {
                add(primaryAlias)
            }
            if (primaryAlias != alias) {
                add(alias)
            }
        }
    }

    private fun mergeAliasPaths(prefix: List<String>, suffix: List<String>): List<String> {
        if (prefix.isEmpty()) return suffix
        if (suffix.isEmpty()) return prefix

        val merged = prefix.toMutableList()
        suffix.forEach { alias ->
            if (merged.isEmpty() || merged.last() != alias) {
                merged += alias
            }
        }
        return merged
    }

    private fun enrichFieldSourceWithViewExpression(
        column: SelectStatement.FieldSource,
        stmt: SelectStatement
    ): SelectStatement.FieldSource {
        if (column.expression != null || annotationResolver == null) {
            return column
        }

        val candidateViewNames = linkedSetOf<String>().apply {
            stmt.fromTable?.takeIf { it.isNotBlank() }?.let { add(it) }
            stmt.tableAliases.values.forEach { value ->
                if (value.isNotBlank()) add(value)
            }
            stmt.joinTables.forEach { table ->
                if (table.isNotBlank()) add(table)
            }
        }

        candidateViewNames.forEach { viewName ->
            val view = annotationResolver.findView(viewName) ?: return@forEach
            val matchedField = view.fields.firstOrNull { viewField ->
                viewField.src.fieldName.equals(column.fieldName, ignoreCase = true)
            }
            if (matchedField?.src?.expression != null) {
                return column.copy(expression = matchedField.src.expression)
            }
        }

        return column
    }

    /** Recursively collect dynamic fields from a view and its referenced views. */
    private fun collectTransitiveDynamicFields(
        view: AnnotatedCreateViewStatement,
        resolver: FieldAnnotationResolver,
        aliasPath: List<String>
    ): List<DynamicFieldDescriptor> {
        val result = mutableListOf<DynamicFieldDescriptor>()
        // Add direct dynamic fields
        view.dynamicFields.forEach { df ->
            val path = if (!df.annotations.sourceTable.isNullOrBlank()) {
                aliasPath + df.annotations.sourceTable
            } else {
                aliasPath
            }
            result += DynamicFieldDescriptor(df.name, df.annotations, path)
        }
        // Recurse into any referenced views from this view's SELECT
        view.src.selectStatement.tableAliases.forEach { (childAlias, name) ->
            val child = resolver.findView(name)
            if (child != null) {
                result.addAll(collectTransitiveDynamicFields(child, resolver, aliasPath + childAlias))
            }
        }
        return result
    }

    /**
     * Creates an annotated EXECUTE statement (INSERT/DELETE/UPDATE) from parsed components.
     */
    private fun createAnnotatedExecuteStatement(
        stmtName: String,
        parsedStatement: Any,
        sqlStatement: SqlSingleStatement
    ): AnnotatedExecuteStatement {
        val stmt = when (parsedStatement) {
            is Insert -> {
                InsertStatement.parse(parsedStatement, conn)
            }

            is Delete -> {
                DeleteStatement.parse(parsedStatement, conn)
            }

            is Update -> {
                UpdateStatement.parse(parsedStatement, conn)
            }

            else -> {
                throw UnsupportedOperationException("Unsupported statement type")
            }
        }

        return AnnotatedExecuteStatement.parse(
            name = stmtName,
            execStatement = stmt,
            topComments = sqlStatement.topComments,
        )
    }

    /**
     * Merges field annotations from SELECT statement comments and resolved annotations.
     * SELECT statement field annotations take precedence over resolved annotations.
     */
    private fun mergeFieldAnnotations(
        column: SelectStatement.FieldSource,
        selectFieldAnnotations: Map<String, Map<String, Any?>>,
        selectStatement: SelectStatement,
        annotationResolver: FieldAnnotationResolver?
    ): FieldAnnotationOverrides {
        // Start with resolved annotations as base (from tables/views)
        val mergedAnnotations = mutableMapOf<String, Any?>()

        // If we have a resolver and this SELECT queries a table/view, get resolved annotations
        if (annotationResolver != null) {
            val candidateSources = linkedSetOf<String>()

            val tableAlias = column.tableName
            if (tableAlias.isNotBlank()) {
                candidateSources += tableAlias
                selectStatement.tableAliases[tableAlias]?.let { candidateSources += it }
            } else {
                candidateSources += selectStatement.tableAliases.values
            }

            selectStatement.fromTable?.let { candidateSources += it }

            candidateSources.forEach { sourceName ->
                val resolvedAnnotations = annotationResolver.getFieldAnnotations(sourceName, column.fieldName)
                    ?: annotationResolver.getFieldAnnotations(sourceName, column.originalColumnName)

                if (resolvedAnnotations != null) {
                    FieldAnnotationMerger.mergeFieldAnnotations(mergedAnnotations, resolvedAnnotations)
                    return@forEach
                }

                val view = annotationResolver.findView(sourceName)
                if (view != null) {
                    val viewField = view.fields.firstOrNull { field ->
                        field.src.fieldName.equals(column.fieldName, ignoreCase = true) ||
                                field.src.originalColumnName.equals(column.fieldName, ignoreCase = true)
                    } ?: view.fields.firstOrNull { field ->
                        field.src.fieldName.equals(column.originalColumnName, ignoreCase = true) ||
                                field.src.originalColumnName.equals(column.originalColumnName, ignoreCase = true)
                    }

                    if (viewField != null) {
                        FieldAnnotationMerger.mergeFieldAnnotations(mergedAnnotations, viewField.annotations)
                        return@forEach
                    }
                }
            }
        }

        // Override with SELECT statement annotations (they take precedence)
        val selectAnnotations = selectFieldAnnotations[column.fieldName] ?: emptyMap()
        mergedAnnotations.putAll(selectAnnotations)

        return FieldAnnotationOverrides.parse(mergedAnnotations)
    }

    /**
     * Normalizes SELECT statements for JSqlParser.
     *
     * JSqlParser 4.x can misinterpret extra blank lines between the SELECT list and
     * the FROM clause as the end of the statement. To avoid this, collapse multiple
     * consecutive blank lines into a single newline before parsing. This does not
     * change SQL semantics and keeps formatting largely intact.
     */
    private fun normalizeSelectForParser(sql: String): String {
        // Collapse 2+ consecutive blank lines (including lines with only spaces/tabs)
        val collapsed = sql.replace(Regex("(?m)(?:\\r?\\n[ \t]*){2,}"), "\n")
        return collapsed.trim()
    }
}
