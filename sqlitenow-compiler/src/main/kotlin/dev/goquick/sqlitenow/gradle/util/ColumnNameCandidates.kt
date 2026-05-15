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
package dev.goquick.sqlitenow.gradle.util

import java.util.LinkedHashSet

internal fun buildColumnNameCandidates(vararg names: String?): LinkedHashSet<String> {
    val candidates = LinkedHashSet<String>()
    names.forEach { name ->
        addColumnNameVariants(name, candidates)
    }
    return candidates
}

internal fun addColumnNameVariants(name: String?, sink: MutableSet<String>) {
    val trimmed = name?.trim() ?: return
    if (trimmed.isEmpty()) return
    sink += trimmed

    val withoutSuffix = trimmed.substringBefore(':')
    if (withoutSuffix.isNotEmpty()) sink += withoutSuffix

    val afterDot = withoutSuffix.substringAfterLast('.', withoutSuffix)
    if (afterDot.isNotEmpty()) sink += afterDot

    val segments = afterDot.split('_').filter { it.isNotEmpty() }
    if (segments.size > 1) {
        segments.indices.forEach { index ->
            sink += segments.drop(index).joinToString("_")
        }
    }
}
