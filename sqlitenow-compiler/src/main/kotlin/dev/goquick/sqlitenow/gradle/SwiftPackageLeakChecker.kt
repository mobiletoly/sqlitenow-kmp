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
package dev.goquick.sqlitenow.gradle

import java.io.File

data class SwiftPackageLeak(
    val relativePath: String,
    val value: String,
)

object SwiftPackageLeakChecker {
    val DEFAULT_FORBIDDEN_REGEX_PATTERNS = listOf(
        "Kotlin",
        "Ktor",
        "StateFlow",
        "Flow<",
        "Result<",
        "Throwable",
        "KotlinByteArray",
        "Coroutine",
        "\\bKt\\b",
    )

    fun findLeaks(
        files: Iterable<File>,
        rootDirectory: File,
        forbiddenLiteralValues: Iterable<String> = emptyList(),
        forbiddenRegexPatterns: Iterable<String> = emptyList(),
    ): List<SwiftPackageLeak> {
        val literals = forbiddenLiteralValues
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .distinct()
        val regexes = forbiddenRegexPatterns
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .distinct()
            .map { pattern -> pattern to Regex(pattern) }
        if (literals.isEmpty() && regexes.isEmpty()) {
            return emptyList()
        }

        val root = rootDirectory.canonicalFile
        return files
            .filter { it.isFile }
            .flatMap { file ->
                val text = file.readText()
                val relativePath = relativePath(file, root)
                val literalLeaks = literals
                    .filter { literal -> text.contains(literal) }
                    .map { literal -> SwiftPackageLeak(relativePath = relativePath, value = literal) }
                val regexLeaks = regexes.flatMap { (_, regex) ->
                    regex.findAll(text).map { match ->
                        SwiftPackageLeak(relativePath = relativePath, value = match.value)
                    }
                }
                literalLeaks + regexLeaks
            }
            .distinct()
    }

    fun requireNoLeaks(
        files: Iterable<File>,
        rootDirectory: File,
        forbiddenLiteralValues: Iterable<String> = emptyList(),
        forbiddenRegexPatterns: Iterable<String> = emptyList(),
        messagePrefix: String,
    ) {
        val leaks = findLeaks(
            files = files,
            rootDirectory = rootDirectory,
            forbiddenLiteralValues = forbiddenLiteralValues,
            forbiddenRegexPatterns = forbiddenRegexPatterns,
        )
        require(leaks.isEmpty()) {
            "$messagePrefix\n" + leaks.joinToString(separator = "\n") { leak ->
                "${leak.relativePath} contains '${leak.value}'"
            }
        }
    }

    private fun relativePath(file: File, rootDirectory: File): String =
        runCatching { file.canonicalFile.relativeTo(rootDirectory).invariantSeparatorsPath }
            .getOrElse { file.absolutePath }
}
