package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
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
                // Special handling for Kotlin standard library types to avoid import conflicts
                if (it in KOTLIN_STDLIB_TYPES) {
                    ClassName("kotlin", it)
                } else {
                    ClassName.bestGuess(it)
                }
            }

            // Apply nullability to the type
            val finalType = typeFromAnnotation ?: baseType
            return if (isNullable) finalType.copy(nullable = true) else finalType
        }
    }
}
