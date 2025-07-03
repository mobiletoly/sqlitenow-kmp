package dev.goquick.sqlitenow.gradle.inspect

import net.sf.jsqlparser.expression.CastExpression
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.expression.operators.relational.InExpression
import net.sf.jsqlparser.parser.SimpleNode
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.util.deparser.ExpressionDeParser
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
                    buffer.append("SELECT value from json_each(?)")
                } else {
                    buffer.append('?')
                }
            }
        }

        val selectDp = SelectDeParser(exprDp, buffer)
        exprDp.selectVisitor = selectDp
        val stmtDp = StatementDeParser(exprDp, selectDp, buffer)
        stmt.accept(stmtDp)

        return Triple(buffer.toString(), seen, castTypes)
    }
}
