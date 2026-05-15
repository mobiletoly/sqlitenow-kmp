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

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import java.util.Locale

internal fun sortCreateViewsByDependencies(
    viewExecutors: List<CreateViewStatementExecutor>
): List<CreateViewStatementExecutor> {
    if (viewExecutors.size <= 1) return viewExecutors

    val nameToExec = viewExecutors.associateBy { it.viewName() }
    val viewNames = nameToExec.keys.toSet()

    // Build graph: dependency view -> views depending on it.
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

    if (orderedNames.size < viewExecutors.size) {
        val remaining = viewExecutors.map { it.viewName() }.filter { it !in orderedNames }
        orderedNames.addAll(remaining)
    }

    return orderedNames.mapNotNull { nameToExec[it] }
}

internal fun buildCreateViewTableDependencies(
    createViewStatements: Iterable<AnnotatedCreateViewStatement>
): Map<String, Set<String>> {
    val createViewStatementsByName = createViewStatements.associateBy { it.src.viewName.lowercase(Locale.ROOT) }

    fun resolveViewDependencies(
        viewName: String,
        seen: MutableSet<String>,
    ): Set<String> {
        if (!seen.add(viewName)) return emptySet()
        val view = createViewStatementsByName[viewName] ?: return emptySet()
        val dependencies = mutableSetOf<String>()
        val select = view.src.selectStatement
        val referencedTables = buildList {
            select.fromTable?.let { add(it) }
            addAll(select.joinTables)
        }
        referencedTables.forEach { table ->
            val key = table.lowercase(Locale.ROOT)
            dependencies += key
            dependencies += resolveViewDependencies(key, seen)
        }
        return dependencies
    }

    return createViewStatements.associate { view ->
        val key = view.src.viewName.lowercase(Locale.ROOT)
        key to resolveViewDependencies(key, mutableSetOf())
    }
}
