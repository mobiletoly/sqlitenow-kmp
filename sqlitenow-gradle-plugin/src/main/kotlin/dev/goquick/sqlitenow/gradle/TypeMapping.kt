package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.TypeName

/**
 * Service responsible for mapping Kotlin types to SQLite binding and getter operations.
 * Centralizes type-specific logic that was previously duplicated across QueryCodeGenerator.
 * 
 * This service eliminates duplication between:
 * - getBindingCallForKotlinTypeWithExpression() 
 * - getGetterCallForKotlinType()
 */
class TypeMapping {

    /**
     * Generates the appropriate SQLite binding call for a Kotlin type.
     */
    fun getBindingCall(kotlinType: TypeName, paramIndex: Int, expression: String): String {
        val sqliteIndex = paramIndex + 1
        val baseType = kotlinType.toString().removePrefix("kotlin.")
        
        return when (baseType) {
            "Int" -> "statement.bindInt($sqliteIndex, $expression)"
            "Long" -> "statement.bindLong($sqliteIndex, $expression)"
            "Double" -> "statement.bindDouble($sqliteIndex, $expression)"
            "Float" -> "statement.bindFloat($sqliteIndex, $expression)"
            "Boolean" -> "statement.bindInt($sqliteIndex, if ($expression) 1 else 0)"
            "ByteArray" -> "statement.bindBlob($sqliteIndex, $expression)"
            "Byte" -> "statement.bindInt($sqliteIndex, $expression.toInt())"
            else -> "statement.bindText($sqliteIndex, $expression)"
        }
    }

    /** Generates the appropriate SQLite getter call for a Kotlin type. */
    fun getGetterCall(kotlinType: TypeName, columnIndex: Int): String {
        val baseType = kotlinType.toString().removePrefix("kotlin.")

        return when (baseType) {
            "Long" -> "stmt.getLong($columnIndex)"
            "Int" -> "stmt.getInt($columnIndex)"
            "Double" -> "stmt.getDouble($columnIndex)"
            "Float" -> "stmt.getFloat($columnIndex)"
            "Boolean" -> "stmt.getInt($columnIndex) != 0"
            "String" -> "stmt.getText($columnIndex)"
            "ByteArray" -> "stmt.getBlob($columnIndex)"
            "Byte" -> "stmt.getInt($columnIndex).toByte()"
            else -> "stmt.getText($columnIndex)" // Default to text
        }
    }

    /** Generates a binding call for Collection types that need JSON encoding. */
    fun getCollectionBindingCall(paramIndex: Int, propertyName: String): String {
        val sqliteIndex = paramIndex + 1
        return "statement.bindText($sqliteIndex, params.$propertyName.jsonEncodeToSqlite())"
    }

    /** Checks if a type string represents a standard Kotlin type that we can handle directly. */
    fun isStandardKotlinType(typeString: String): Boolean {
        val baseType = typeString.removePrefix("kotlin.").removeSuffix("?")
        return baseType in SUPPORTED_TYPES
    }

    companion object Companion {
        /**
         * Set of Kotlin types that have direct SQLite binding/getter support.
         */
        private val SUPPORTED_TYPES = setOf(
            "Int", "Long", "Double", "Float", "Boolean", 
            "String", "ByteArray", "Byte"
        )
    }
}
