package dev.goquick.sqlitenow.gradle.inspect

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

    init {
        val result = collectParamPairs()
        processedSql = result.first
        parameters = result.second
    }

    /**
     * Abstract method for processing WITH clauses.
     * Different statement types (UPDATE vs DELETE) handle WITH clauses differently.
     */
    protected abstract fun processWithClauses(stmtDp: StatementDeParser, buffer: StringBuilder)

    private fun collectParamPairs(): Pair<String, List<String>> {
        val seen = mutableListOf<String>()
        val buffer = StringBuilder()

        val exprDp = object : ExpressionDeParser() {
            override fun visit(param: JdbcNamedParameter) {
                seen += param.name
                val parent = param.astNode.jjtGetParent() as SimpleNode
                val value = parent.jjtGetValue()
                if (value is InExpression) {
                    // We are using json_each() to expand the array into individual values
                    buffer.append("SELECT value from json_each(?)")
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

        return Pair(buffer.toString(), seen)
    }
}
