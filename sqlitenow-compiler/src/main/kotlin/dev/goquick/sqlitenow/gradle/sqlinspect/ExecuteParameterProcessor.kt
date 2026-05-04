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
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.util.deparser.ExpressionDeParser
import net.sf.jsqlparser.util.deparser.SelectDeParser
import net.sf.jsqlparser.util.deparser.StatementDeParser

/**
 * Base class for parameter processors that handle EXECUTE statements (INSERT/UPDATE/DELETE).
 * Contains common logic shared between UpdateParametersProcessor and DeleteParametersProcessor.
 */
abstract class ExecuteParameterProcessor(
    protected val stmt: Statement
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

    /**
     * Abstract method for processing WITH clauses.
     * Different statement types (UPDATE vs DELETE) handle WITH clauses differently.
     */
    protected abstract fun processWithClauses(stmtDp: StatementDeParser, buffer: StringBuilder)

    private fun collectParamPairs(): Triple<String, List<String>, Map<String, String>> {
        val seen = mutableListOf<String>()
        val buffer = StringBuilder()
        val castTypes = mutableMapOf<String, String>()

        val exprDp = object : ExpressionDeParser() {
            override fun visit(param: JdbcNamedParameter) {
                seen += param.name
                val parent = param.astNode.jjtGetParent() as SimpleNode
                val value = parent.jjtGetValue()

                // Check if this parameter is used within a CAST expression
                if (value is CastExpression && value.leftExpression == param) {
                    castTypes[param.name] = value.colDataType.dataType
                }

                if (value is InExpression) {
                    // We are using json_each() to expand the array into individual values
                    buffer.append("(SELECT value FROM json_each(?))")
                } else {
                    val newParam = param.withName("?")
                    newParam.setParameterCharacter("")
                    super.visit(newParam)
                }
            }

            override fun visit(select: Select) {
                super.visit(select)
            }
        }

        val selectDp = SelectDeParser(exprDp, buffer)
        exprDp.selectVisitor = selectDp
        val stmtDp = StatementDeParser(exprDp, selectDp, buffer)

        // Process WITH clauses (different for each statement type)
        processWithClauses(stmtDp, buffer)

        stmt.accept(stmtDp)

        return Triple(buffer.toString(), seen, castTypes)
    }
}
