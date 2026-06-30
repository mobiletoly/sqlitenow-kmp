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
package dev.goquick.sqlitenow.gradle.swift

internal data class SwiftProductParameter(
    val sqlName: String,
    val propertyName: String,
    val swiftType: String,
    val collection: Boolean,
    val bindKind: SwiftProductBindKind,
    val adapter: SwiftAdapterDescriptor?,
) {
    fun bindExpression(sourceExpression: String): String {
        if (collection) {
            val elementBindKind = adapter?.outputSwiftType?.swiftProductBindKind()
                ?: swiftType.swiftCollectionElementType()?.swiftProductBindKind()
                ?: bindKind
            require(elementBindKind != SwiftProductBindKind.DATA) {
                "Swift product collection parameter '$sqlName' maps to Data/BLOB, which is not supported. " +
                    "Use scalar BLOB parameters or map the SQL value to a supported collection element type."
            }
            val valueExpression = adapter?.let {
                "try $sourceExpression.map { try adapters.${it.name}(${'$'}0) }"
            } ?: sourceExpression
            return "sqliteNowBindJsonArray($valueExpression)"
        }
        val valueExpression = adapter?.let { "try adapters.${it.name}($sourceExpression)" } ?: sourceExpression
        return when (bindKind) {
            SwiftProductBindKind.INT64,
            SwiftProductBindKind.DOUBLE,
            SwiftProductBindKind.TEXT,
            SwiftProductBindKind.BOOL,
            SwiftProductBindKind.DATA -> "sqliteNowBind($valueExpression)"
        }
    }
}

internal enum class SwiftProductBindKind {
    INT64,
    DOUBLE,
    TEXT,
    BOOL,
    DATA,
}

private fun String.swiftCollectionElementType(): String? =
    if (startsWith("[") && endsWith("]")) {
        removePrefix("[").removeSuffix("]")
    } else {
        null
    }

internal fun String.swiftProductBindKind(): SwiftProductBindKind =
    when (dropSwiftOptional()) {
        "Bool" -> SwiftProductBindKind.BOOL
        "Data" -> SwiftProductBindKind.DATA
        "Double" -> SwiftProductBindKind.DOUBLE
        "Int64" -> SwiftProductBindKind.INT64
        "String" -> SwiftProductBindKind.TEXT
        else -> SwiftProductBindKind.TEXT
    }
