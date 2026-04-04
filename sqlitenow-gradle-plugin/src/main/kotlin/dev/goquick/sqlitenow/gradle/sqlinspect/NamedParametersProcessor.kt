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
package dev.goquick.sqlitenow.gradle.sqlinspect

import net.sf.jsqlparser.expression.CastExpression
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.expression.operators.relational.InExpression
import net.sf.jsqlparser.parser.SimpleNode
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.util.deparser.ExpressionDeParser
import net.sf.jsqlparser.util.deparser.InsertDeParser
import net.sf.jsqlparser.util.deparser.SelectDeParser
import net.sf.jsqlparser.util.deparser.StatementDeParser

/**
 * Collect named parameters and rewrites them to '?'
 */
class NamedParametersProcessor(
    private val stmt: Statement,
) {
    /** The preprocessed SQL with all named parameters replaced by '?' */
    val processedSql: String

    /** The list of parameter names in the order they appear in the SQL (including duplicates) */
    val parameters: List<String>

    /** Map of parameter names to their CAST target types (if used within CAST expressions) */
    val parameterCastTypes: Map<String, String>

    init {
        val result = collectParamPairs()
        processedSql = result.first
        parameters = result.second
        parameterCastTypes = result.third
    }

    private fun collectParamPairs(): Triple<String, List<String>, Map<String, String>> {
        val seen = mutableListOf<String>()
        val buffer = StringBuilder()
        val castTypes = mutableMapOf<String, String>()
        val inParameters = mutableSetOf<String>()

        // Extract common parameter processing logic
        fun processParameter(param: JdbcNamedParameter) {
            seen += param.name
            val parent = param.astNode?.jjtGetParent() as? SimpleNode
            val value = parent?.jjtGetValue()
            // Check if this parameter is used within a CAST expression
            if (value is CastExpression && value.leftExpression == param) {
                castTypes[param.name] = value.colDataType.dataType
            }

            if (value is InExpression) {
                // We are using json_each() to expand the array into individual values
                inParameters += param.name
                buffer.append("(SELECT value FROM json_each(?))")
            } else {
                buffer.append('?')
            }
        }

        fun collectMetadataOnly(expression: net.sf.jsqlparser.expression.Expression?) {
            expression ?: return
            expression.accept(object : ExpressionDeParser() {
                override fun visit(param: JdbcNamedParameter) {
                    seen += param.name
                    val parent = param.astNode?.jjtGetParent() as? SimpleNode
                    val value = parent?.jjtGetValue()
                    if (value is CastExpression && value.leftExpression == param) {
                        castTypes[param.name] = value.colDataType.dataType
                    }
                    if (value is InExpression) {
                        inParameters += param.name
                    }
                }
            })
        }

        val exprDp = object : ExpressionDeParser() {
            override fun visit(param: JdbcNamedParameter) = processParameter(param)
        }

        val selectDp = SelectDeParser(exprDp, buffer)
        exprDp.selectVisitor = selectDp

        // Use custom StatementDeParser that handles INSERT ON CONFLICT properly
        val stmtDp = object : StatementDeParser(exprDp, selectDp, buffer) {
            override fun visit(insert: Insert) {
                val customExprDp = object : ExpressionDeParser(exprDp.selectVisitor, buffer) {
                    override fun visit(param: JdbcNamedParameter) = processParameter(param)
                }
                val customSelectDp = SelectDeParser(customExprDp, buffer)
                customExprDp.selectVisitor = customSelectDp
                InsertDeParser(customExprDp, customSelectDp, buffer).deParse(insert)
            }
        }

        stmt.accept(stmtDp)

        if (stmt is Insert) {
            stmt.conflictTarget?.indexExpression?.let(::collectMetadataOnly)
            stmt.conflictTarget?.whereExpression?.let(::collectMetadataOnly)
            stmt.conflictAction?.updateSets.orEmpty().forEach { updateSet ->
                updateSet.values.orEmpty().forEach { expr ->
                    collectMetadataOnly(expr)
                }
            }
            stmt.conflictAction?.whereExpression?.let(::collectMetadataOnly)
        }

        return Triple(rewriteRemainingNamedParameters(buffer.toString(), inParameters), seen, castTypes)
    }

    private fun rewriteRemainingNamedParameters(sql: String, inParameters: Set<String>): String {
        val out = StringBuilder()
        var index = 0
        var inSingleQuote = false
        var inDoubleQuote = false

        while (index < sql.length) {
            val current = sql[index]

            when (current) {
                '\'' -> {
                    out.append(current)
                    if (!inDoubleQuote) {
                        inSingleQuote = !inSingleQuote
                    }
                    index++
                }

                '"' -> {
                    out.append(current)
                    if (!inSingleQuote) {
                        inDoubleQuote = !inDoubleQuote
                    }
                    index++
                }

                ':' -> {
                    if (!inSingleQuote && !inDoubleQuote) {
                        val start = index + 1
                        var end = start
                        while (end < sql.length && (sql[end].isLetterOrDigit() || sql[end] == '_')) {
                            end++
                        }
                        if (end > start) {
                            val paramName = sql.substring(start, end)
                            if (paramName in inParameters) {
                                out.append("(SELECT value FROM json_each(?))")
                            } else {
                                out.append('?')
                            }
                            index = end
                            continue
                        }
                    }
                    out.append(current)
                    index++
                }

                else -> {
                    out.append(current)
                    index++
                }
            }
        }

        return out.toString()
    }
}
