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

import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement.Companion.buildReturningColumns
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement.Companion.buildSelectStatementFromWithItemsList
import java.sql.Connection
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.statement.insert.Insert

class InsertStatement(
    override val sql: String,
    override val table: String,
    override val namedParameters: List<String>,
    val columnNamesAssociatedWithNamedParameters: Map<String, String>,
    override val withSelectStatements: List<SelectStatement>,
    override val parameterCastTypes: Map<String, String> = emptyMap(),
    override val hasReturningClause: Boolean,
    override val returningColumns: List<String>
) : ExecuteStatement {

    companion object {
        fun parse(insert: Insert, conn: Connection): InsertStatement {
            val table = insert.table.let {
                it.nameParts[0]
            }
            val withItemsList = insert.withItemsList
            val withSelectStatements = buildSelectStatementFromWithItemsList(conn, withItemsList)
            val allColumnNames = insert.columns?.map { it.columnName } ?: emptyList()
            val values = insert.select?.values?.expressions
            val requiredColumnNames = mutableListOf<String>()
            val columnNamesAssociatedWithNamedParameters = linkedMapOf<String, String>()

            // Process VALUES clause parameters
            values?.forEachIndexed { index, expr ->
                if (expr is JdbcNamedParameter) {
                    val columnName = allColumnNames[index]
                    requiredColumnNames.add(columnName)
                    columnNamesAssociatedWithNamedParameters[expr.name.removePrefix(":")] = columnName
                }
            }

            // Process ON CONFLICT DO UPDATE clause parameters
            insert.conflictAction?.updateSets?.forEach { updateSet ->
                val columnName: String? = updateSet.columns.firstOrNull()?.columnName
                val expr = updateSet.values.firstOrNull()
                if (columnName != null && expr is JdbcNamedParameter) {
                    columnNamesAssociatedWithNamedParameters[expr.name.removePrefix(":")] = columnName
                }
            }

            // Check for RETURNING clause
            val returningClause = insert.returningClause
            val hasReturningClause = returningClause != null
            val returningColumns = buildReturningColumns(returningClause)

            val processor = NamedParametersProcessor(stmt = insert)
            return InsertStatement(
                sql = processor.processedSql,
                table = table,
                namedParameters = processor.parameters,
                columnNamesAssociatedWithNamedParameters = columnNamesAssociatedWithNamedParameters,
                withSelectStatements = withSelectStatements,
                parameterCastTypes = processor.parameterCastTypes,
                hasReturningClause = hasReturningClause,
                returningColumns = returningColumns,
            )
        }
    }
}
