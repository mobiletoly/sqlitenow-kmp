package dev.goquick.sqlitenow.gradle.sqlinspect

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.util.deparser.StatementDeParser

/**
 * Rewrites all named parameters in UPDATE statements to '?' and collects named parameters.
 */
class UpdateParametersProcessor(
    stmt: Statement
) : ExecuteParameterProcessor(stmt) {

    override fun processWithClauses(stmtDp: StatementDeParser, buffer: StringBuilder) {
        (stmt as Update).withItemsList?.forEach {
            it.select.accept(stmtDp)
        }
        buffer.clear()
    }
}
