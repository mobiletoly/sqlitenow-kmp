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
package dev.goquick.sqlitenow.gradle.context

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import java.io.File

internal class GeneratorContext(
    val packageName: String,
    val outputDir: File,
    val createTableStatements: List<AnnotatedCreateTableStatement>,
    val createViewStatements: List<AnnotatedCreateViewStatement>,
    val nsWithStatements: Map<String, List<AnnotatedStatement>>
) {
    val columnLookup = ColumnLookup(createTableStatements, createViewStatements)
    val typeMapping = TypeMapping()
    val adapterConfig = AdapterConfig(
        columnLookup,
        createTableStatements,
        createViewStatements,
        packageName
    )
    val adapterNameResolver = AdapterParameterNameResolver()
    val selectFieldGenerator = SelectFieldCodeGenerator(
        createTableStatements,
        createViewStatements,
        packageName
    )

    fun findSelectStatementByResultName(resultName: String): AnnotatedSelectStatement? {
        if (resultName.isBlank()) return null
        val targetSimpleName = resultName.substringAfterLast('.')

        nsWithStatements.forEach { (_, statements) ->
            statements.filterIsInstance<AnnotatedSelectStatement>().firstOrNull { statement ->
                val queryResultName = statement.annotations.queryResult
                queryResultName != null && queryResultName.substringAfterLast('.') == targetSimpleName
            }?.let { return it }
        }
        return null
    }

    fun findMainTableAlias(allFields: List<AnnotatedSelectStatement.Field>): String? {
        val sourceTableAliases = allFields
            .filter { it.annotations.isDynamicField && it.annotations.sourceTable != null }
            .map { it.annotations.sourceTable!! }
            .toSet()

        val allTableAliases = allFields
            .map { it.src.tableName }
            .filter { it.isNotBlank() }
            .distinct()

        return allTableAliases.firstOrNull { it !in sourceTableAliases }
    }
}