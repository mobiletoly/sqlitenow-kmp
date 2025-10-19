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
package dev.goquick.sqlitenow.gradle.context

import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter

/**
 * Service responsible for mapping Kotlin types to SQLite binding and getter operations.
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

    /**
     * Generates the appropriate SQLite getter call for a Kotlin type.
     * Caller may customise the receiver used to access statement APIs (defaults to `statement`).
     */
    fun getGetterCall(
        kotlinType: TypeName,
        columnIndex: Int,
        receiver: String = "statement",
    ): String {
        val baseType = kotlinType.toString().removePrefix("kotlin.")

        return when (baseType) {
            "Long" -> "$receiver.getLong($columnIndex)"
            "Int" -> "$receiver.getInt($columnIndex)"
            "Double" -> "$receiver.getDouble($columnIndex)"
            "Float" -> "$receiver.getFloat($columnIndex)"
            "Boolean" -> "$receiver.getInt($columnIndex) != 0"
            "String" -> "$receiver.getText($columnIndex)"
            "ByteArray" -> "$receiver.getBlob($columnIndex)"
            "Byte" -> "$receiver.getInt($columnIndex).toByte()"
            else -> "$receiver.getText($columnIndex)" // Default to text
        }
    }

    /** Generates a binding call for Collection types that need JSON encoding. */
    fun getCollectionBindingCall(paramIndex: Int, propertyName: String): String {
        val sqliteIndex = paramIndex + 1
        return "statement.bindText($sqliteIndex, Json.encodeToString(params.$propertyName))"
    }

    /** Checks if a type string represents a standard Kotlin type that we can handle directly. */
    fun isStandardKotlinType(typeString: String): Boolean {
        val baseType = typeString.removePrefix("kotlin.").removeSuffix("?")
        return baseType in SqliteTypeToKotlinCodeConverter.Companion.KOTLIN_STDLIB_TYPES
    }
}
