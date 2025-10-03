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
                buffer.append("SELECT value from json_each(?)")
            } else {
                buffer.append('?')
            }
        }

        val exprDp = object : ExpressionDeParser() {
            override fun visit(param: JdbcNamedParameter) = processParameter(param)
        }

        val selectDp = SelectDeParser(exprDp, buffer)
        exprDp.selectVisitor = selectDp

        // Use custom StatementDeParser that handles INSERT ON CONFLICT properly
        val stmtDp = object : StatementDeParser(exprDp, selectDp, buffer) {
            override fun visit(insert: Insert) {
                // Use the default INSERT handling but with custom expression visitor
                val customExprDp = object : ExpressionDeParser(exprDp.selectVisitor, buffer) {
                    override fun visit(param: JdbcNamedParameter) = processParameter(param)
                }

                val customSelectDp = SelectDeParser(customExprDp, buffer)
                customExprDp.selectVisitor = customSelectDp

                // Create a custom INSERT deparser that handles conflict actions
                val insertDp = object : InsertDeParser(customExprDp, customSelectDp, buffer) {
                    override fun deParse(insert: Insert) {
                        // Manually handle the entire INSERT statement to avoid duplication
                        buffer.append("INSERT INTO ")
                        buffer.append(insert.table.name)

                        if (insert.columns != null) {
                            buffer.append(" (")
                            buffer.append(insert.columns.joinToString(", ") { it.columnName })
                            buffer.append(")")
                        }

                        if (insert.select != null) {
                            buffer.append(" VALUES (")
                            insert.select.values.expressions.forEachIndexed { index, expr ->
                                if (index > 0) buffer.append(", ")
                                expr.accept(customExprDp)
                            }
                            buffer.append(")")
                        }

                        // Handle ON CONFLICT clause manually
                        insert.conflictAction?.let { conflictAction ->
                            buffer.append(" ON CONFLICT")
                            buffer.append(" DO UPDATE SET ")
                            conflictAction.updateSets.forEachIndexed { index, updateSet ->
                                if (index > 0) buffer.append(", ")
                                buffer.append(updateSet.columns[0].columnName)
                                buffer.append(" = ")
                                updateSet.values[0].accept(customExprDp)
                            }
                        }

                        // Handle RETURNING clause manually
                        insert.returningClause?.let { returningClause ->
                            buffer.append(" RETURNING ")
                            returningClause.forEachIndexed { index, selectItem ->
                                if (index > 0) buffer.append(", ")
                                buffer.append(selectItem.toString())
                            }
                        }
                    }
                }

                insertDp.deParse(insert)
            }
        }

        stmt.accept(stmtDp)

        return Triple(buffer.toString(), seen, castTypes)
    }
}
