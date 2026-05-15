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
import java.util.Locale

internal class CascadeNotifyResolver private constructor(
    private val cascadeNotifyByTable: Map<String, StatementAnnotationOverrides.CascadeNotify>
) {
    private val cascadeClosureCache =
        mutableMapOf<Pair<String, StatementAnnotationOverrides.CascadeAction>, Set<String>>()

    fun tablesFor(
        tableName: String,
        action: StatementAnnotationOverrides.CascadeAction,
    ): Set<String> {
        val tableKey = tableName.lowercase(Locale.ROOT)
        val cacheKey = tableKey to action
        cascadeClosureCache[cacheKey]?.let { return it }

        val closure = computeCascadeClosure(
            startTable = tableKey,
            action = action,
            visited = mutableSetOf()
        )
        cascadeClosureCache[cacheKey] = closure
        return closure
    }

    private fun computeCascadeClosure(
        startTable: String,
        action: StatementAnnotationOverrides.CascadeAction,
        visited: MutableSet<String>,
    ): Set<String> {
        if (!visited.add(startTable)) return emptySet()
        val direct = cascadeNotifyByTable[startTable]?.tablesFor(action) ?: return emptySet()
        if (direct.isEmpty()) return emptySet()

        val result = mutableSetOf<String>()
        direct.forEach { childRaw ->
            val child = childRaw.lowercase(Locale.ROOT)
            if (result.add(child)) {
                result += computeCascadeClosure(child, action, visited)
            }
        }
        return result
    }

    companion object {
        fun fromCreateTableStatements(
            createTableStatements: Iterable<AnnotatedCreateTableStatement>
        ): CascadeNotifyResolver {
            val cascadeNotifyByTable = createTableStatements.mapNotNull { table ->
                val cascade = table.annotations.cascadeNotify ?: return@mapNotNull null
                table.src.tableName.lowercase(Locale.ROOT) to cascade
            }.toMap()
            return CascadeNotifyResolver(cascadeNotifyByTable)
        }
    }
}
