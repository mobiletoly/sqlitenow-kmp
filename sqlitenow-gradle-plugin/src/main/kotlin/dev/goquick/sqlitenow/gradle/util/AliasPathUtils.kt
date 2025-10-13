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
package dev.goquick.sqlitenow.gradle.util

/** Utility helpers for manipulating alias path chains (e.g., p->a->b). */
object AliasPathUtils {

    /** Lowercase every alias segment for case-insensitive comparisons. */
    fun lowercase(path: List<String>): List<String> = path.map { it.lowercase() }

    /** Case-insensitive prefix check for alias path lists. */
    fun startsWith(path: List<String>, prefix: List<String>): Boolean {
        if (prefix.isEmpty() || path.size < prefix.size) return false
        prefix.indices.forEach { idx ->
            if (!path[idx].equals(prefix[idx], ignoreCase = true)) return false
        }
        return true
    }
}
