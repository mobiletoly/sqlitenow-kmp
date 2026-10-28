package dev.goquick.sqlitenow.core

/** Reads execution policy at step/reset time, never at prepare time. No bind values retained. */
internal class SqliteNowStatementDiagnostics(
    private val owner: SafeSQLiteConnection,
    private val descriptor: DiagnosticSqlDescriptor,
) {
    private var captured = false
    private var execution: SqliteNowDiagnosticSpan? = null
    private var cacheScope: DiagnosticSpanBinding? = null
    private var family: String? = null

    fun bindCacheScope(span: SqliteNowDiagnosticSpan?, familyLabel: String?, hit: Boolean) {
        cacheScope = span?.binding; family = familyLabel
        span?.recordSql(descriptor, if (hit) "cache_hit" else "cache_miss", 0, familyLabel = familyLabel)
    }

    fun span(action: String): SqliteNowDiagnosticSpan? {
        cacheScope?.span?.let { if (!it.isFinished) return it }
        cacheScope = null
        family = null
        if (owner.diagnosticOperationActive) {
            family = owner.activeDiagnosticFamilyLabel
            return owner.activeDiagnosticSpan
        }
        if (!captured) {
            captured = true
            execution = owner.beginStatementDiagnostics()
        }
        return execution
    }

    fun complete(span: SqliteNowDiagnosticSpan, action: String, began: Long, row: Boolean, failed: Boolean) {
        span.recordSql(descriptor, action, span.nowNanos() - began, row, failed, family)
        if (action == "close" && !failed) owner.recordDiagnosticStatementResources(span, true)
        if (action == "reset" || action == "close" || action == "step" && (!row || failed)) finish(failed)
    }

    fun finish(failed: Boolean = false) {
        execution?.finish(if (failed) "failure" else "success")
        execution = null
        captured = false
    }

    fun disabledBoundary(action: String, row: Boolean) {
        if (action == "reset" || action == "close" || action == "step" && !row) finish()
    }
}

internal interface DiagnosticStatementBinding {
    fun bindDiagnosticScope(span: SqliteNowDiagnosticSpan?, familyLabel: String?, hit: Boolean)
}
