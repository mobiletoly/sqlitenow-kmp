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

import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.table.NamedConstraint

data class CreateTableStatement(
    override val sql: String,
    val tableName: String,
    val columns: List<Column>,
) : SqlStatement {
    override val namedParameters: List<String>
        get() = emptyList()

    data class Column(
        val name: String,
        val dataType: String,
        val notNull: Boolean,
        val primaryKey: Boolean,
        val autoIncrement: Boolean,
        val unique: Boolean,
    )

    companion object {
        fun parse(sql: String, create: CreateTable): CreateTableStatement {
            return CreateTableStatement(
                sql = sql,
                tableName = create.table.name,
                columns = parseColumns(create),
            )
        }

        private fun parseColumns(create: CreateTable): List<Column> {
            val pkCols = create.indexes
                ?.filterIsInstance<NamedConstraint>()
                ?.filter { it.type.equals("PRIMARY KEY", ignoreCase = true) }
                ?.flatMap { it.columnsNames }
                ?: emptyList()
            val uniqueCols = create.indexes
                ?.filterIsInstance<NamedConstraint>()
                ?.filter { it.type.equals("UNIQUE", ignoreCase = true) }
                ?.flatMap { it.columnsNames }
                ?: emptyList()

            return create.columnDefinitions
                ?.mapIndexed { idx, col ->
                    val specs = col.columnSpecs ?: emptyList()

                    Column(
                        name = col.columnName,
                        dataType = col.colDataType.toString(),
                        notNull = specs.any {
                            it.equals(
                                "NOT",
                                true
                            )
                        } && specs.any { it.equals("NULL", true) },
                        primaryKey = specs.any {
                            it.equals("PRIMARY", true) &&
                                    specs.any { it.equals("KEY", true) }
                        }
                                || pkCols.contains(col.columnName),
                        autoIncrement = specs.any {
                            it.equals("AUTOINCREMENT", true) || it.equals(
                                "AUTO_INCREMENT",
                                true
                            )
                        },
                        unique = specs.any { it.equals("UNIQUE", true) }
                                || uniqueCols.contains(col.columnName),
                    )
                }
                ?: emptyList()
        }
    }
}
