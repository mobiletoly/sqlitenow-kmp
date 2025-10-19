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
package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder

internal class CollectionMappingBuilder(
    private val builder: IndentedCodeBuilder,
) {
    fun emit(rowsVar: String, block: ChainBuilder.() -> Unit) {
        builder.line(rowsVar)
        builder.indent {
            ChainBuilder(builder).block()
        }
    }

    class ChainBuilder(private val builder: IndentedCodeBuilder) {
        fun filter(nonNullCondition: String) {
            builder.line(".filter { row -> !($nonNullCondition) }")
        }

        fun groupedMap(
            groupExpression: String,
            rowsVarName: String,
            firstRowVar: String,
            elementSimpleName: String,
            emitElement: ArgumentEmitter.() -> Unit,
        ) {
            builder.line(".groupBy { row -> $groupExpression }")
            builder.line(".map { (_, $rowsVarName) ->")
            builder.indent {
                builder.line("val $firstRowVar = $rowsVarName.first()")
                builder.line("$elementSimpleName(")
                builder.indent {
                    ArgumentEmitter(builder).emitElement()
                }
                builder.line(")")
            }
            builder.line("}")
        }

        fun mapRows(
            elementSimpleName: String,
            emitElement: ArgumentEmitter.() -> Unit,
        ) {
            builder.line(".map { row ->")
            builder.indent {
                builder.line("$elementSimpleName(")
                builder.indent {
                    ArgumentEmitter(builder).emitElement()
                }
                builder.line(")")
            }
            builder.line("}")
        }

        fun distinctBy(path: String) {
            builder.line(".distinctBy { it.$path }")
        }
    }

    class ArgumentEmitter(private val builder: IndentedCodeBuilder) {
        fun emitMultiline(text: String) {
            text.trimEnd().lines().forEach { raw ->
                val trimmed = raw.trim()
                if (trimmed.isEmpty()) {
                    builder.line("")
                } else {
                    builder.line(trimmed)
                }
            }
        }
    }
}
