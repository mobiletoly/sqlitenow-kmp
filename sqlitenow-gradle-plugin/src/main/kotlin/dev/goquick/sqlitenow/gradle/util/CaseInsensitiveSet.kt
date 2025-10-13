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

/**
 * Simple helper for case-insensitive lookups while preserving the original casing of inserted values.
 */
internal class CaseInsensitiveSet {
    private val delegate = linkedSetOf<String>()
    private val lowercased = linkedSetOf<String>()

    fun add(value: String) {
        delegate += value
        lowercased += value.lowercase()
    }

    fun addAll(values: Iterable<String>) {
        values.forEach { add(it) }
    }

    fun containsIgnoreCase(value: String): Boolean = lowercased.contains(value.lowercase())
}
