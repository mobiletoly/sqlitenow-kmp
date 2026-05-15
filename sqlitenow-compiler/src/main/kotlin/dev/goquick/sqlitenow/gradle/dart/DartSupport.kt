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
package dev.goquick.sqlitenow.gradle.dart

import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.util.Locale

internal fun AnnotatedCreateTableStatement.Column.parsedFieldAnnotations(): FieldAnnotationOverrides =
    FieldAnnotationOverrides.parse(annotations)

internal fun AnnotatedCreateTableStatement.Column.hasAdapter(): Boolean =
    parsedFieldAnnotations().adapter == true

internal fun TypeName.toDartType(): String {
    val nullable = toString().endsWith("?")
    val rawType = toString().removeSuffix("?")
    val type = rawType.removePrefix("kotlin.")
    val dartType = when {
        type == "Int" || type == "Long" || type == "Short" || type == "Byte" -> "int"
        type == "Double" || type == "Float" -> "double"
        type == "Boolean" -> "bool"
        type == "String" -> "String"
        type == "ByteArray" -> "Uint8List"
        rawType.startsWith("kotlin.collections.Collection<") -> {
            "List<${rawType.substringAfter("<").substringBeforeLast(">").toDartTypeName()}>"
        }
        rawType.startsWith("kotlin.collections.List<") -> {
            "List<${rawType.substringAfter("<").substringBeforeLast(">").toDartTypeName()}>"
        }
        else -> type.toDartTypeName()
    }
    return dartType.withNullable(nullable)
}

internal fun String.sqliteDartType(): String {
    val normalized = uppercase(Locale.ROOT)
    return when {
        "INT" in normalized -> "int"
        "REAL" in normalized || "FLOA" in normalized || "DOUB" in normalized -> "double"
        "BLOB" in normalized -> "Uint8List"
        else -> "String"
    }
}

internal fun String.sqliteReadCall(nullable: Boolean): String {
    val prefix = if (nullable) "readNullable" else "read"
    val normalized = uppercase(Locale.ROOT)
    return when {
        "INT" in normalized -> "${prefix}Int"
        "REAL" in normalized || "FLOA" in normalized || "DOUB" in normalized -> "${prefix}Double"
        "BLOB" in normalized -> "${prefix}Blob"
        else -> "${prefix}String"
    }
}

internal fun String.toDartTypeName(): String {
    val trimmed = removePrefix("kotlin.").removePrefix("dart.").substringAfterLast('.')
    return when (trimmed) {
        "Int", "Long", "Short", "Byte" -> "int"
        "Double", "Float" -> "double"
        "Boolean" -> "bool"
        "String" -> "String"
        "ByteArray" -> "Uint8List"
        else -> trimmed
    }
}

internal fun String.collectionElementType(): String {
    val type = toDartTypeName().removeSuffix("?")
    require(type.startsWith("List<") && type.endsWith(">")) {
        "Expected Dart collection type, got '$this'."
    }
    return type.removePrefix("List<").removeSuffix(">")
}

internal fun String.withNullable(nullable: Boolean): String =
    if (nullable && !endsWith("?")) "$this?" else this

internal fun String.toLowerCamel(): String {
    val pascal = pascalize(this)
    return pascal.replaceFirstChar { it.lowercase() }
}

internal fun String.toSnakeCase(): String {
    val out = StringBuilder()
    forEachIndexed { index, char ->
        if (char.isUpperCase() && index > 0) {
            out.append('_')
        }
        out.append(char.lowercaseChar())
    }
    return out.toString()
}

internal fun String.toDartRawString(): String {
    require(!contains("'''")) {
        "Dart raw string emitter does not support SQL containing triple single quotes yet."
    }
    return "r'''$this'''"
}

internal fun Set<String>.toDartSetLiteral(): String =
    if (isEmpty()) {
        "const <String>{}"
    } else {
        "const <String>{${joinToString(", ") { "'$it'" }}}"
    }

internal fun List<DartParameter>.forwardingArgument(): String =
    if (isEmpty()) "" else "params"
