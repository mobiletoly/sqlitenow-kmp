package dev.goquick.sqlitenow.gradle.inspect

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.util.deparser.StatementDeParser

/**
 * Rewrites all named parameters in DELETE statements to '?' and collects named parameters.
 */
class DeleteParametersProcessor(
    stmt: Statement
) : ExecuteParameterProcessor(stmt) {

    override fun processWithClauses(stmtDp: StatementDeParser, buffer: StringBuilder) {
        (stmt as Delete).withItemsList?.forEach {
            it.select.accept(stmtDp)
        }
        buffer.clear()
    }
}
