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
package dev.goquick.sqlitenow.gradle.sqlinspect

import net.sf.jsqlparser.statement.ReturningClause
import java.sql.Connection
import net.sf.jsqlparser.statement.select.WithItem

/**
 * Base interface for EXECUTE statements (INSERT/UPDATE/DELETE).
 */
sealed interface ExecuteStatement : SqlStatement {
    override val sql: String
    val table: String
    override val namedParameters: List<String>
    val withSelectStatements: List<SelectStatement>
    val parameterCastTypes: Map<String, String>
    val hasReturningClause: Boolean
    val returningColumns: List<String>

    companion object {
        fun buildSelectStatementFromWithItemsList(
            conn: Connection,
            withItemsList: List<WithItem>?,
        ): List<SelectStatement> {
            val withSelectStatements = withItemsList
                ?.map { withItem ->
                    val select = withItem.select.plainSelect
                    val stmt = SelectStatement.parse(
                        conn = conn,
                        select = select,
                    )
                    stmt
                }
                ?: emptyList()
            return withSelectStatements
        }

        fun buildReturningColumns(returningClause: ReturningClause?): List<String> {
            val hasReturningClause = returningClause != null
            val returningColumns = if (hasReturningClause) {
                returningClause.map { selectItem ->
                    if (selectItem.alias != null) {
                        throw IllegalArgumentException("RETURNING clause with aliases is currently not supported: $selectItem")
                    }
                    val node = selectItem.astNode
                    if (node.jjtGetFirstToken().toString() != node.jjtGetLastToken().toString()) {
                        throw IllegalArgumentException("RETURNING clause with expressions is currently not supported: $selectItem")
                    }
                    selectItem.toString()
                }
            } else {
                emptyList()
            }
            return returningColumns
        }
    }
}
