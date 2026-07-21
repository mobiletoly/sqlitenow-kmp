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

import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.util.Locale

internal class SwiftWriter {
    private val builder = StringBuilder()
    private var indent = ""

    fun line(value: String = "") {
        if (value.isEmpty()) {
            builder.append('\n')
        } else {
            builder.append(indent).append(value).append('\n')
        }
    }

    fun indent(block: SwiftWriter.() -> Unit) {
        val previous = indent
        indent += "    "
        block()
        indent = previous
    }

    override fun toString(): String = builder.toString()
}

internal fun AnnotatedCreateTableStatement.Column.parsedSwiftFieldAnnotations(): FieldAnnotationOverrides =
    FieldAnnotationOverrides.parse(annotations)

internal fun String.toSwiftLowerCamel(): String {
    val pascal = pascalize(this)
    return pascal.replaceFirstChar { it.lowercase() }
}

internal fun String.swiftIdentifier(): String =
    when (this) {
        "class", "deinit", "enum", "extension", "func", "import", "init", "let", "private", "public",
        "static", "struct", "subscript", "typealias", "var", "break", "case", "catch", "continue",
        "default", "defer", "do", "else", "fallthrough", "for", "guard", "if", "in", "repeat",
        "return", "throw", "switch", "where", "while", "as", "Any", "false", "is", "nil", "rethrows",
        "self", "Self", "super", "throws", "true", "try" -> "`$this`"
        else -> this
    }

internal fun String.toSwiftStringLiteral(): String =
    buildString {
        append('"')
        this@toSwiftStringLiteral.forEach { char ->
            when (char) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> append(char)
            }
        }
        append('"')
    }

internal fun TypeName.toSwiftTypeName(): String {
    val nullable = toString().endsWith("?")
    val raw = toString().removeSuffix("?")
    val type = raw.removePrefix("kotlin.")
    val swiftType = when {
        type in setOf("Byte", "Short", "Int", "Long") -> "Int64"
        type in setOf("Float", "Double") -> "Double"
        type == "Boolean" -> "Bool"
        type == "String" -> "String"
        type == "ByteArray" -> "Data"
        raw.startsWith("kotlin.collections.Collection<") -> {
            "[${raw.substringAfter("<").substringBeforeLast(">").toSwiftTypeName()}]"
        }
        raw.startsWith("kotlin.collections.List<") -> {
            "[${raw.substringAfter("<").substringBeforeLast(">").toSwiftTypeName()}]"
        }
        else -> type.substringAfterLast('.')
    }
    return swiftType.withSwiftNullable(nullable)
}

internal fun String.toSwiftTypeName(): String {
    val nullable = endsWith("?")
    val raw = removeSuffix("?")
    val type = raw.removePrefix("kotlin.")
    val swiftType = when {
        raw.startsWith("kotlin.collections.Collection<") || raw.startsWith("Collection<") -> {
            "[${raw.substringAfter("<").substringBeforeLast(">").toSwiftTypeName()}]"
        }
        raw.startsWith("kotlin.collections.List<") || raw.startsWith("List<") -> {
            "[${raw.substringAfter("<").substringBeforeLast(">").toSwiftTypeName()}]"
        }
        type in setOf("Byte", "Short", "Int", "Long") -> "Int64"
        type in setOf("Float", "Double") -> "Double"
        type == "Boolean" -> "Bool"
        type == "String" -> "String"
        type == "ByteArray" -> "Data"
        else -> type.substringAfterLast('.')
    }
    return swiftType.withSwiftNullable(nullable)
}

internal fun TypeName.toBridgeKotlinType(): String = toString().toBridgeKotlinType()

internal fun String.toBridgeKotlinType(): String {
    val nullable = endsWith("?")
    val raw = removeSuffix("?")
    val type = raw.removePrefix("kotlin.")
    val kotlinType = when {
        raw.startsWith("kotlin.collections.Collection<") || raw.startsWith("Collection<") -> {
            "Collection<${raw.substringAfter("<").substringBeforeLast(">").toBridgeKotlinType()}>"
        }
        raw.startsWith("kotlin.collections.List<") || raw.startsWith("List<") -> {
            "List<${raw.substringAfter("<").substringBeforeLast(">").toBridgeKotlinType()}>"
        }
        type in setOf("Byte", "Short", "Int", "Long") -> "Long"
        type in setOf("Float", "Double") -> "Double"
        type == "Boolean" -> "Boolean"
        type == "String" -> "String"
        type == "ByteArray" -> "ByteArray"
        else -> raw
    }
    return kotlinType.withKotlinNullable(nullable)
}

internal fun String.sqliteSwiftType(nullable: Boolean, explicitType: String? = null): String {
    explicitType?.let { return it.toSwiftTypeName().withSwiftNullable(nullable) }
    val normalized = uppercase(Locale.ROOT)
    val swiftType = when {
        "BOOL" in normalized -> "Bool"
        "INT" in normalized -> "Int64"
        "REAL" in normalized || "FLOA" in normalized || "DOUB" in normalized -> "Double"
        "BLOB" in normalized -> "Data"
        else -> "String"
    }
    return swiftType.withSwiftNullable(nullable)
}

internal fun String.sqliteBridgeKotlinType(nullable: Boolean, explicitType: String? = null): String {
    explicitType?.let { return it.toBridgeKotlinType().withKotlinNullable(nullable) }
    val normalized = uppercase(Locale.ROOT)
    val rawType = when {
        "BOOL" in normalized -> "Boolean"
        "INT" in normalized -> "Long"
        "REAL" in normalized || "FLOA" in normalized || "DOUB" in normalized -> "Double"
        "BLOB" in normalized -> "ByteArray"
        else -> "String"
    }
    return rawType.withKotlinNullable(nullable)
}

internal fun String.withSwiftNullable(nullable: Boolean): String =
    if (nullable && !endsWith("?")) "$this?" else this

internal fun String.withKotlinNullable(nullable: Boolean): String =
    if (nullable && !endsWith("?")) "$this?" else this

internal fun String.asNullableKotlinType(): String = removeSuffix("?").withKotlinNullable(nullable = true)

internal fun AnnotatedSelectStatement.Field.swiftPropertyName(statement: AnnotatedSelectStatement): String =
    annotations.propertyName
        ?: statement.annotations.propertyNameGenerator.convertToPropertyName(src.fieldName)

internal fun AnnotatedSelectStatement.Field.isSwiftNullable(): Boolean {
    annotations.notNull?.let { return !it }
    return src.isNullable
}

internal fun AnnotatedSelectStatement.Field.swiftType(statement: AnnotatedSelectStatement): String =
    src.dataType.sqliteSwiftType(
        nullable = isSwiftNullable(),
        explicitType = annotations.propertyType
    )

internal fun AnnotatedSelectStatement.Field.bridgeKotlinType(): String =
    src.dataType.sqliteBridgeKotlinType(
        nullable = isSwiftNullable(),
        explicitType = annotations.propertyType
    )

internal fun AnnotatedStatement.srcNamedParameters(): List<String> =
    when (this) {
        is AnnotatedSelectStatement -> src.namedParameters
        is AnnotatedExecuteStatement -> src.namedParameters
        else -> emptyList()
    }

internal fun AnnotatedStatement.uniqueNamedParameters(): List<String> =
    srcNamedParameters().distinct()

internal fun AnnotatedStatement.kotlinQueryObjectName(): String = getDataClassName()

internal fun AnnotatedStatement.swiftParamsName(namespace: String): String =
    "${pascalize(namespace)}${getDataClassName()}Params"

internal fun AnnotatedStatement.swiftFunctionName(): String = name.toSwiftLowerCamel()

internal fun AnnotatedStatement.kotlinRouterPropertyName(): String = name.toSwiftLowerCamel()

internal fun String.swiftNamespacePropertyName(): String = toSwiftLowerCamel()

internal fun String.swiftNamespaceTypeName(suffix: String): String = "${pascalize(this)}$suffix"

internal fun String.swiftBridgeResultName(): String = "App$this"

internal fun String.swiftBridgeParamsName(): String = "App${this}Bridge"

internal fun AnnotatedExecuteStatement.tableNameForExecute(): String =
    when (val execute = src) {
        is InsertStatement -> execute.table
        is UpdateStatement -> execute.table
        is DeleteStatement -> execute.table
    }

internal fun PropertyNameGeneratorType.swiftParamPropertyName(paramName: String): String =
    convertToPropertyName(paramName)

internal fun Collection<String>.toKotlinSetLiteral(): String =
    if (isEmpty()) {
        "emptySet()"
    } else {
        joinToString(prefix = "setOf(", postfix = ")") { it.toSwiftStringLiteral() }
    }

internal fun String.dropSwiftOptional(): String = removeSuffix("?")

internal fun String.isSwiftData(): Boolean = dropSwiftOptional() == "Data"

internal fun String.isBridgeByteArray(): Boolean = removeSuffix("?") == "ByteArray"

internal fun String.isNonOptionalSwiftData(): Boolean = this == "Data"

internal fun String.isNonOptionalBridgeByteArray(): Boolean = this == "ByteArray"

internal fun String.supportsSwiftEquatable(): Boolean =
    supportsKnownSwiftValueConformance()

internal fun String.supportsSwiftSendable(): Boolean =
    supportsKnownSwiftValueConformance()

private fun String.supportsKnownSwiftValueConformance(): Boolean {
    val type = dropSwiftOptional()
    if (type.startsWith("[") && type.endsWith("]")) {
        return type.substring(1, type.lastIndex).supportsKnownSwiftValueConformance()
    }
    return type in setOf("Bool", "Data", "Double", "Int64", "String")
}
