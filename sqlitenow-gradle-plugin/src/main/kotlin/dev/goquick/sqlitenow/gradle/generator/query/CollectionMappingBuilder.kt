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
