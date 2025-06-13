package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.inspect.CreateViewStatement
import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import java.io.File
import java.sql.Connection
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.view.CreateView

internal class SchemaInspector(
    schemaDirectory: File
) {
    val statementExecutors = mutableListOf<DeferredStatementExecutor>()
    val sqlStatements: List<SqlSingleStatement>

    // Cache for executed statements to prevent duplicate execution
    private var cachedCreateTableStatements: List<AnnotatedCreateTableStatement>? = null
    private var cachedCreateViewStatements: List<AnnotatedCreateViewStatement>? = null

    // Helper properties for backward compatibility with tests
    fun getCreateTableStatements(conn: Connection): List<AnnotatedCreateTableStatement> {
        if (cachedCreateTableStatements == null) {
            cachedCreateTableStatements = statementExecutors
                .filterIsInstance<CreateTableStatementExecutor>()
                .map { it.execute(conn) as AnnotatedCreateTableStatement }
        }
        return cachedCreateTableStatements!!
    }

    fun getCreateViewStatements(conn: Connection): List<AnnotatedCreateViewStatement> {
        if (cachedCreateViewStatements == null) {
            cachedCreateViewStatements = statementExecutors
                .filterIsInstance<CreateViewStatementExecutor>()
                .map { it.execute(conn) as AnnotatedCreateViewStatement }
        }
        return cachedCreateViewStatements!!
    }

    init {
        SqlFileProcessor.validateDirectory(schemaDirectory, "Schema", mandatory = true)
        sqlStatements = SqlFileProcessor.parseAllSqlFilesInDirectory(schemaDirectory)

        // Only validate that we have SQL files for schema directory since it's required
        if (sqlStatements.isEmpty()) {
            throw IllegalArgumentException("No .sql files found in Schema directory: ${schemaDirectory.absolutePath}")
        }

        sqlStatements.forEach { sqlStatement ->
            inspect(sqlStatement)
        }
    }

    private fun inspect(sqlStatement: SqlSingleStatement) {
        try {
            val parsedStatement = CCJSqlParserUtil.parse(sqlStatement.sql)
            when (parsedStatement) {
                is CreateTable -> {
                    val executor = CreateTableStatementExecutor(
                        sqlStatement = sqlStatement,
                        createTable = parsedStatement
                    )
                    statementExecutors.add(executor)
                }
                is CreateView -> {
                    val executor = CreateViewStatementExecutor(
                        sqlStatement = sqlStatement,
                        createView = parsedStatement
                    )
                    statementExecutors.add(executor)
                }
            }
        } catch (e: Exception) {
            StandardErrorHandler.handleSqlParsingError(sqlStatement.sql, e, "SchemaInspector")
        }
    }
}

interface DeferredStatementExecutor {
    fun execute(conn: Connection): AnnotatedStatement
}

class CreateTableStatementExecutor(
    private val sqlStatement: SqlSingleStatement,
    private val createTable: CreateTable
) : DeferredStatementExecutor {
    override fun execute(conn: Connection): AnnotatedStatement {
        return conn.createStatement().use { stmt ->
            val createTableStmt = CreateTableStatement.parse(sql = sqlStatement.sql, create = createTable)
            val annotatedCreateTableStatement = AnnotatedCreateTableStatement.parse(
                name = createTableStmt.tableName,
                createTableStmt,
                sqlStatement.topComments,
                sqlStatement.innerComments
            )
            stmt.executeUpdate(annotatedCreateTableStatement.src.sql)
            annotatedCreateTableStatement
        }
    }
}

class CreateViewStatementExecutor(
    private val sqlStatement: SqlSingleStatement,
    private val createView: CreateView
) : DeferredStatementExecutor {
    override fun execute(conn: Connection): AnnotatedStatement {
        return conn.createStatement().use { stmt ->
            val createViewStmt = CreateViewStatement.parse(sql = sqlStatement.sql, createView = createView, conn = conn)
            val annotatedCreateViewStatement = AnnotatedCreateViewStatement.parse(
                name = createViewStmt.viewName,
                createViewStmt,
                sqlStatement.topComments,
                sqlStatement.innerComments
            )
            stmt.executeUpdate(annotatedCreateViewStatement.src.sql)
            annotatedCreateViewStatement
        }
    }
}
