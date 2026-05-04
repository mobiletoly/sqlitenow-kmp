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

import kotlin.text.iterator

/**
 * Utility class for parsing generic type strings and extracting type information.
 * This provides a robust alternative to simple string manipulation for handling
 * generic types like List<Type>, Map<String, List<Type>>, etc.
 */
class GenericTypeParser {

    companion object {

        /**
         * Extracts the first type argument from a generic type string.
         *
         * Examples:
         * - "List<String>" -> "String"
         * - "Map<String, Int>" -> "String"
         * - "List<Map<String, Int>>" -> "Map<String, Int>"
         * - "String" -> "String" (non-generic type)
         *
         * @param typeString The type string to parse
         * @return The first type argument, or the original string if not generic
         */
        fun extractFirstTypeArgument(typeString: String): String {
            val trimmed = typeString.trim()

            if (!isGenericType(trimmed)) {
                return trimmed
            }

            val typeArguments = extractTypeArguments(trimmed)
            return typeArguments.firstOrNull() ?: trimmed
        }

        /**
         * Extracts all type arguments from a generic type string.
         *
         * Examples:
         * - "List<String>" -> ["String"]
         * - "Map<String, Int>" -> ["String", "Int"]
         * - "List<Map<String, Int>>" -> ["Map<String, Int>"]
         * - "String" -> [] (non-generic type)
         *
         * @param typeString The type string to parse
         * @return List of type arguments, empty if not generic
         */
        fun extractTypeArguments(typeString: String): List<String> {
            val trimmed = typeString.trim()

            if (!isGenericType(trimmed)) {
                return emptyList()
            }

            val openBracket = trimmed.indexOf('<')
            val closeBracket = trimmed.lastIndexOf('>')

            if (openBracket == -1 || closeBracket == -1 || openBracket >= closeBracket) {
                return emptyList()
            }

            val typeArgumentsString = trimmed.substring(openBracket + 1, closeBracket).trim()
            return parseTypeArguments(typeArgumentsString)
        }

        /**
         * Extracts the raw type name from a generic type string.
         *
         * Examples:
         * - "List<String>" -> "List"
         * - "Map<String, Int>" -> "Map"
         * - "com.example.CustomList<String>" -> "com.example.CustomList"
         * - "String" -> "String" (non-generic type)
         *
         * @param typeString The type string to parse
         * @return The raw type name without generic parameters
         */
        fun extractRawTypeName(typeString: String): String {
            val trimmed = typeString.trim()

            if (!isGenericType(trimmed)) {
                return trimmed
            }

            val openBracket = trimmed.indexOf('<')
            return if (openBracket > 0) {
                trimmed.substring(0, openBracket).trim()
            } else {
                trimmed
            }
        }

        /**
         * Checks if a type string represents a generic type.
         *
         * @param typeString The type string to check
         * @return true if the type string contains generic parameters
         */
        fun isGenericType(typeString: String): Boolean {
            val trimmed = typeString.trim()

            if (!trimmed.contains('<') || !trimmed.contains('>')) {
                return false
            }

            val openBracket = trimmed.indexOf('<')
            val closeBracket = trimmed.lastIndexOf('>')

            // Must have valid bracket positions: < comes before >
            return openBracket != -1 && closeBracket != -1 && openBracket < closeBracket
        }

        /**
         * Parses type arguments from a string, handling nested generics properly.
         *
         * Examples:
         * - "String, Int" -> ["String", "Int"]
         * - "String" -> ["String"]
         * - "Map<String, Int>, List<String>" -> ["Map<String, Int>", "List<String>"]
         *
         * @param typeArgumentsString The type arguments string to parse
         * @return List of individual type argument strings
         */
        fun parseTypeArguments(typeArgumentsString: String): List<String> {
            if (typeArgumentsString.isBlank()) {
                return emptyList()
            }

            val arguments = mutableListOf<String>()
            val currentArg = StringBuilder()
            var bracketDepth = 0

            for (char in typeArgumentsString) {
                when (char) {
                    '<' -> {
                        bracketDepth++
                        currentArg.append(char)
                    }
                    '>' -> {
                        bracketDepth--
                        currentArg.append(char)
                    }
                    ',' -> {
                        if (bracketDepth == 0) {
                            // We're at the top level, this comma separates type arguments
                            val argString = currentArg.toString().trim()
                            if (argString.isNotEmpty()) {
                                arguments.add(argString)
                            }
                            currentArg.clear()
                        } else {
                            currentArg.append(char)
                        }
                    }
                    else -> {
                        currentArg.append(char)
                    }
                }
            }

            // Add the last argument
            val lastArgString = currentArg.toString().trim()
            if (lastArgString.isNotEmpty()) {
                arguments.add(lastArgString)
            }

            return arguments
        }
    }
}