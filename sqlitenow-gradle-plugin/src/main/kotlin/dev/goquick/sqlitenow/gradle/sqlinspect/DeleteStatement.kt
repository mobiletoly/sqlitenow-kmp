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

import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement.Companion.buildReturningColumns
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement.Companion.buildSelectStatementFromWithItemsList
import java.sql.Connection
import net.sf.jsqlparser.statement.delete.Delete

class DeleteStatement(
    override val sql: String,
    override val table: String,
    override val namedParameters: List<String>,
    val namedParametersToColumns: Map<String, AssociatedColumn>,
    override val withSelectStatements: List<SelectStatement>,
    override val parameterCastTypes: Map<String, String> = emptyMap(),
    override val hasReturningClause: Boolean = false,
    override val returningColumns: List<String> = emptyList()
) : ExecuteStatement {

    companion object {
        fun parse(delete: Delete, conn: Connection): DeleteStatement {
            val namedParamsWithColumns = delete.where?.collectNamedParametersAssociatedWithColumns()
                ?: emptyMap()

            val table = delete.table.let {
                it.nameParts[0]
            }
            val withItemsList = delete.withItemsList
            val withSelectStatements = buildSelectStatementFromWithItemsList(conn, withItemsList)


            // Check for RETURNING clause
            val returningClause = delete.returningClause
            val hasReturningClause = returningClause != null
            val returningColumns = buildReturningColumns(returningClause)

            val processor = DeleteParametersProcessor(stmt = delete)
            return DeleteStatement(
                sql = processor.processedSql,
                table = table,
                namedParameters = processor.parameters,
                namedParametersToColumns = namedParamsWithColumns,
                withSelectStatements = withSelectStatements,
                parameterCastTypes = processor.parameterCastTypes,
                hasReturningClause = hasReturningClause,
                returningColumns = returningColumns,
            )
        }
    }
}
