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

import net.sf.jsqlparser.statement.create.view.CreateView
import java.sql.Connection

/**
 * Represents a CREATE VIEW statement.
 * Views are treated similarly to tables but represent stored queries rather than physical tables.
 */
data class CreateViewStatement(
    override val sql: String,
    val viewName: String,
    val selectStatement: SelectStatement,
    val columnNames: List<String>? = null,
) : SqlStatement {
    override val namedParameters: List<String>
        get() = selectStatement.namedParameters

    companion object {
        fun parse(sql: String, createView: CreateView, conn: Connection): CreateViewStatement {
            val selectStatement = SelectStatement.parse(
                conn = conn,
                select = createView.select.plainSelect
            )

            val columnNames = createView.columnNames?.map { it.columnName }

            return CreateViewStatement(
                sql = sql,
                viewName = createView.view.name,
                selectStatement = selectStatement,
                columnNames = columnNames
            )
        }
    }
}
