package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.TypeName

/**
 * Utility class for converting SQLite data types to Kotlin types.
 */
class SqliteTypeToKotlinCodeConverter {
    companion object Companion {
        // Map of SQLite data types to Kotlin types
        private val SQL_TO_KOTLIN_TYPE_MAP = mapOf(
            "INTEGER" to ClassName("kotlin", "Long"),
            "INT" to ClassName("kotlin", "Long"),
            "TINYINT" to ClassName("kotlin", "Byte"),
            "SMALLINT" to ClassName("kotlin", "Int"),
            "MEDIUMINT" to ClassName("kotlin", "Int"),
            "BIGINT" to ClassName("kotlin", "Long"),
            "UNSIGNED BIG INT" to ClassName("kotlin", "Long"),
            "TEXT" to ClassName("kotlin", "String"),
            "CHARACTER" to ClassName("kotlin", "String"),
            "VARCHAR" to ClassName("kotlin", "String"),
            "VARYING CHARACTER" to ClassName("kotlin", "String"),
            "NCHAR" to ClassName("kotlin", "String"),
            "NATIVE CHARACTER" to ClassName("kotlin", "String"),
            "NVARCHAR" to ClassName("kotlin", "String"),
            "CLOB" to ClassName("kotlin", "String"),
            "REAL" to ClassName("kotlin", "Double"),
            "DOUBLE" to ClassName("kotlin", "Double"),
            "DOUBLE PRECISION" to ClassName("kotlin", "Double"),
            "FLOAT" to ClassName("kotlin", "Float"),
            "NUMERIC" to ClassName("kotlin", "Long"),
            "DECIMAL" to ClassName("kotlin", "Long"),
            "BOOLEAN" to ClassName("kotlin", "Boolean"),
            "DATE" to ClassName("kotlin", "String"),
            "DATETIME" to ClassName("kotlin", "String"),
            "BLOB" to ClassName("kotlin", "ByteArray")
        )

        // Default type to use when the SQLite type is unknown
        val DEFAULT_TYPE = ClassName("kotlin", "String")

        // List of Kotlin standard library types
        val KOTLIN_STDLIB_TYPES = listOf(
            "String", "Int", "Long", "Double", "Float", "Boolean", "Byte", "Short", "Char", "Any", "Unit", "Nothing"
        )

        // List of Kotlin collection types that should be mapped to kotlin.collections
        val KOTLIN_COLLECTION_TYPES = listOf(
            "List", "MutableList", "Set", "MutableSet", "Map", "MutableMap", "Collection", "MutableCollection",
            "Iterable", "MutableIterable", "Iterator", "MutableIterator", "ListIterator", "MutableListIterator"
        )

        /**
         * Maps a SQLite data type to a Kotlin type.
         *
         * @param sqlType The SQLite data type
         * @return The corresponding Kotlin type
         */
        fun mapSqlTypeToKotlinType(sqlType: String): TypeName {
            val upperCaseSqlType = sqlType.uppercase()
            return SQL_TO_KOTLIN_TYPE_MAP[upperCaseSqlType] ?: DEFAULT_TYPE
        }

        /**
         * Determines the property type based on the base type, annotations, and nullability.
         *
         * @param baseType The base Kotlin type
         * @param propertyType The property type from annotation (if any)
         * @param isNullable Whether the property should be nullable
         * @return The final property type
         */
        fun determinePropertyType(
            baseType: TypeName,
            propertyType: String?,
            isNullable: Boolean
        ): TypeName {
            // Use the property type from the annotation if present
            val typeFromAnnotation = propertyType?.let {
                parseTypeString(it)
            }

            // Apply nullability to the type
            val finalType = typeFromAnnotation ?: baseType
            return if (isNullable) finalType.copy(nullable = true) else finalType
        }

        /**
         * Parses a type string that may contain generics (e.g., "List<String>", "Map<String, Int>")
         * and converts it to a proper KotlinPoet TypeName.
         *
         * @param typeString The type string to parse
         * @return The corresponding TypeName
         */
        private fun parseTypeString(typeString: String): TypeName {
            val trimmed = typeString.trim()

            // Check if it's a generic type
            if (trimmed.contains('<') && trimmed.contains('>')) {
                return parseGenericType(trimmed)
            }

            // Handle simple types
            return when (trimmed) {
                in KOTLIN_STDLIB_TYPES -> ClassName("kotlin", trimmed)
                in KOTLIN_COLLECTION_TYPES -> ClassName("kotlin.collections", trimmed)
                else -> ClassName.bestGuess(trimmed)
            }
        }

        /**
         * Parses a generic type string (e.g., "List<String>", "Map<String, Int>")
         * and converts it to a parameterized TypeName.
         *
         * @param typeString The generic type string to parse
         * @return The corresponding ParameterizedTypeName
         */
        private fun parseGenericType(typeString: String): TypeName {
            val openBracket = typeString.indexOf('<')
            val closeBracket = typeString.lastIndexOf('>')

            if (openBracket == -1 || closeBracket == -1 || openBracket >= closeBracket) {
                throw IllegalArgumentException("Invalid generic type format: $typeString")
            }

            val rawTypeName = typeString.substring(0, openBracket).trim()
            val typeArgumentsString = typeString.substring(openBracket + 1, closeBracket).trim()

            // Parse the raw type
            val rawType = when {
                rawTypeName in KOTLIN_STDLIB_TYPES -> ClassName("kotlin", rawTypeName)
                rawTypeName in KOTLIN_COLLECTION_TYPES -> ClassName("kotlin.collections", rawTypeName)
                else -> ClassName.bestGuess(rawTypeName)
            }

            // Parse type arguments
            val typeArguments = parseTypeArguments(typeArgumentsString)

            return rawType.parameterizedBy(typeArguments)
        }

        /**
         * Parses type arguments from a string (e.g., "String, Int" or "String")
         * and returns a list of TypeName objects.
         *
         * @param typeArgumentsString The type arguments string to parse
         * @return List of TypeName objects representing the type arguments
         */
        private fun parseTypeArguments(typeArgumentsString: String): List<TypeName> {
            if (typeArgumentsString.isBlank()) {
                return emptyList()
            }

            val arguments = mutableListOf<TypeName>()
            var currentArg = StringBuilder()
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
                                arguments.add(parseTypeString(argString))
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
                arguments.add(parseTypeString(lastArgString))
            }

            return arguments
        }
    }
}
