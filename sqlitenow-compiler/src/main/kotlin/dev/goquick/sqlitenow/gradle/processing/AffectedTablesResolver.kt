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
package dev.goquick.sqlitenow.gradle.processing

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.buildCreateViewTableDependencies
import java.util.Locale

internal class AffectedTablesResolver private constructor(
    private val viewTableDependencies: Map<String, Set<String>>,
    private val cascadeNotifyResolver: CascadeNotifyResolver,
    private val includeSchemaStatements: Boolean,
) {
    fun tablesFor(statement: AnnotatedStatement): Set<String> {
        return when (statement) {
            is AnnotatedSelectStatement -> {
                buildSet {
                    statement.src.fromTable?.let { add(it) }
                    addAll(statement.src.joinTables)
                }.let(::expandTables)
            }

            is AnnotatedExecuteStatement -> {
                val baseTable = statement.src.table
                val cascadeTables = when (statement.src) {
                    is InsertStatement -> cascadeNotifyResolver.tablesFor(
                        baseTable,
                        StatementAnnotationOverrides.CascadeAction.INSERT
                    )

                    is UpdateStatement -> cascadeNotifyResolver.tablesFor(
                        baseTable,
                        StatementAnnotationOverrides.CascadeAction.UPDATE
                    )

                    is DeleteStatement -> cascadeNotifyResolver.tablesFor(
                        baseTable,
                        StatementAnnotationOverrides.CascadeAction.DELETE
                    )
                }
                expandTables(setOf(baseTable) + cascadeTables)
            }

            is AnnotatedCreateTableStatement ->
                if (includeSchemaStatements) setOf(statement.src.tableName) else emptySet()

            is AnnotatedCreateViewStatement -> emptySet()
        }
    }

    private fun expandTables(tables: Set<String>): Set<String> {
        val expanded = mutableSetOf<String>()
        tables.forEach { table ->
            val key = table.lowercase(Locale.ROOT)
            expanded += key
            viewTableDependencies[key]?.let { expanded += it }
        }
        return expanded
    }

    companion object {
        fun fromStatements(
            createTableStatements: Iterable<AnnotatedCreateTableStatement>,
            createViewStatements: Iterable<AnnotatedCreateViewStatement>,
            includeSchemaStatements: Boolean = false,
        ): AffectedTablesResolver {
            return AffectedTablesResolver(
                viewTableDependencies = buildCreateViewTableDependencies(createViewStatements),
                cascadeNotifyResolver = CascadeNotifyResolver.fromCreateTableStatements(createTableStatements),
                includeSchemaStatements = includeSchemaStatements,
            )
        }
    }
}
