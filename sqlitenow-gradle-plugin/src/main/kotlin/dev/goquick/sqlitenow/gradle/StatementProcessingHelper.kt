package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.inspect.InsertStatement
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
import dev.goquick.sqlitenow.gradle.inspect.UpdateStatement
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
                    System.err.println("*** Failed to process query file: ${file.absolutePath}")
                    e.printStackTrace()
                    throw e
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
        val parsedStatement = CCJSqlParserUtil.parse(sqlStatement.sql)

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
            System.err.println("""
Failed to process statement:

${sqlStatement.topComments.joinToString("\n")}
${sqlStatement.sql}
""")
            throw e
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
        val statementAnnotations = StatementAnnotationOverrides.parse(
            extractAnnotations(sqlStatement.topComments)
        )
        val fieldAnnotations = extractFieldAssociatedAnnotations(sqlStatement.innerComments)

        // Build fields with merged annotations
        val fields = stmt.fields.map { column ->
            val annotations = mergeFieldAnnotations(
                column,
                fieldAnnotations,
                stmt,
                annotationResolver
            )
            AnnotatedSelectStatement.Field(
                src = column,
                annotations = annotations
            )
        }

        return AnnotatedSelectStatement(
            name = stmtName,
            src = stmt,
            annotations = statementAnnotations,
            fields = fields
        )
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
        selectFieldAnnotations: Map<String, Map<String, String?>>,
        selectStatement: SelectStatement,
        annotationResolver: FieldAnnotationResolver?
    ): FieldAnnotationOverrides {
        // Start with resolved annotations as base (from tables/views)
        val mergedAnnotations = mutableMapOf<String, String?>()

        // If we have a resolver and this SELECT queries a table/view, get resolved annotations
        val fromTable = selectStatement.fromTable
        if (fromTable != null && annotationResolver != null) {
            val resolvedAnnotations = annotationResolver.getFieldAnnotations(fromTable, column.fieldName)
                ?: annotationResolver.getFieldAnnotations(fromTable, column.originalColumnName)

            if (resolvedAnnotations != null) {
                FieldAnnotationMerger.mergeFieldAnnotations(mergedAnnotations, resolvedAnnotations)
            }
        }

        // Override with SELECT statement annotations (they take precedence)
        val selectAnnotations = selectFieldAnnotations[column.fieldName] ?: emptyMap()
        mergedAnnotations.putAll(selectAnnotations)

        return FieldAnnotationOverrides.parse(mergedAnnotations)
    }
}
