/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.gradle.sqlinspect

import dev.goquick.sqlitenow.gradle.SqlFileProcessor
import dev.goquick.sqlitenow.gradle.StandardErrorHandler
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import java.io.File
import java.sql.Connection
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Table
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
            val viewExecutors = statementExecutors.filterIsInstance<CreateViewStatementExecutor>()
            // Topologically sort views based on dependencies between views
            val sorted = sortViewsByDependencies(viewExecutors)
            cachedCreateViewStatements = sorted.map { it.execute(conn) as AnnotatedCreateViewStatement }
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

    private fun sortViewsByDependencies(viewExecutors: List<CreateViewStatementExecutor>): List<CreateViewStatementExecutor> {
        if (viewExecutors.size <= 1) return viewExecutors

        val nameToExec = viewExecutors.associateBy { it.viewName() }
        val viewNames = nameToExec.keys.toSet()

        // Build graph: dep -> list of views depending on it
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

        // Kahn's algorithm
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

        // If cycle or unresolved, append remaining in original order as fallback
        if (orderedNames.size < viewExecutors.size) {
            val remaining = viewExecutors.map { it.viewName() }.filter { it !in orderedNames }
            orderedNames.addAll(remaining)
        }

        return orderedNames.mapNotNull { nameToExec[it] }
    }

    private fun inspect(sqlStatement: SqlSingleStatement) {
        try {
            val parsedStatement = CCJSqlParserUtil.parse(normalizeForParser(sqlStatement.sql))
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

    /**
     * JSqlParser doesn't understand some SQLite-specific tail modifiers like "WITHOUT ROWID".
     * We strip such suffixes for parsing only, but we keep the original SQL for execution
     * (CreateTableStatement stores the original sql passed in SchemaInspector).
     */
    private fun normalizeForParser(sql: String): String {
        // Remove trailing WITHOUT ROWID (case-insensitive), optionally before semicolon
        val regex = Regex("(?is)\\s+WITHOUT\\s+ROWID\\s*;?\\s*$")
        return sql.replace(regex, ";")
    }
}

interface DeferredStatementExecutor {
    fun execute(conn: Connection): AnnotatedStatement
    fun reportContext(): String
}

class CreateTableStatementExecutor(
    private val sqlStatement: SqlSingleStatement,
    private val createTable: CreateTable,
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

    override fun reportContext(): String {
        return sqlStatement.sql
    }
}

class CreateViewStatementExecutor(
    private val sqlStatement: SqlSingleStatement,
    private val createView: CreateView
) : DeferredStatementExecutor {
    fun viewName(): String = createView.view.name

    fun referencedTableOrViewNames(): Set<String> {
        val names = mutableSetOf<String>()
        val select = createView.select.plainSelect
        // FROM item
        (select.fromItem as? Table)?.let { names += it.nameParts[0] }
        // JOIN items
        select.joins?.forEach { join ->
            (join.fromItem as? Table)?.let { names += it.nameParts[0] }
        }
        return names
    }
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

    override fun reportContext(): String {
        return sqlStatement.sql
    }
}
