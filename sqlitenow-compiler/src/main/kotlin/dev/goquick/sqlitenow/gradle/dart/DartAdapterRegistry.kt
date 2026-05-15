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
package dev.goquick.sqlitenow.gradle.dart

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.util.Locale

internal class DartAdapterRegistry {
    private val columns = linkedMapOf<String, DartAdapterColumn>()

    val values: Collection<DartAdapterColumn>
        get() = columns.values

    fun isEmpty(): Boolean = columns.isEmpty()

    fun collect(context: GeneratorContext) {
        context.createTableStatements.forEach { table ->
            table.columns.forEach { column ->
                if (!column.hasAdapter()) return@forEach
                adapterFor(table.src.tableName, column.src.name, column)
            }
        }
    }

    fun constructorParameter(databaseName: String): String =
        if (isEmpty()) {
            "${databaseName}Adapters adapters = const ${databaseName}Adapters(),"
        } else {
            "required ${databaseName}Adapters adapters,"
        }

    fun adapterFor(
        tableName: String,
        columnName: String,
        column: AnnotatedCreateTableStatement.Column,
    ): DartAdapterColumn? {
        if (!column.hasAdapter()) return null
        val key = "${tableName.lowercase(Locale.ROOT)}.${columnName.lowercase(Locale.ROOT)}"
        return columns.getOrPut(key) {
            val baseName = "${pascalize(tableName)}${pascalize(columnName)}"
            DartAdapterColumn(
                encodeName = "${baseName.replaceFirstChar { it.lowercase() }}ToSql",
                decodeName = "sqlValueTo$baseName",
            )
        }
    }
}
