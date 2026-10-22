package dev.goquick.sqlitenow.core

import kotlin.time.TimeSource

/**
 * Explicit, connection-owned diagnostic session. No parameter values or identifiers are emitted.
 * [sink] runs synchronously on the connection dispatcher and may block it. A throwing sink is
 * disabled. There is deliberately no asynchronous queue. Limits include one terminal notice.
 */
data class SqlTracePolicy(
    val sink: (String) -> Unit,
    val maxEvents: Int = 1_000,
    val durationMillis: Long = 30_000,
    val maxEventChars: Int = 512,
    val maxInputChars: Int = 4_096,
) {
    init {
        require(maxEvents > 0)
        require(durationMillis > 0)
        require(maxEventChars in 64..512)
        require(maxInputChars in 1..4_096)
    }
}

/** Accessed only on its connection dispatcher. Formatter injection is internal test support. */
internal class SqlTraceSession(
    private val policy: SqlTracePolicy,
    private val nowMillis: () -> Long = run {
        val start = TimeSource.Monotonic.markNow()
        val elapsed: () -> Long = { start.elapsedNow().inWholeMilliseconds }
        elapsed
    },
    private val formatter: (String, Int, Int) -> String? = ::sqlStatementShape,
) {
    private var emitted = 0
    private var stopped = false

    fun record(operation: String, sql: String) {
        if (stopped) return
        try {
            // Gate before inspecting, copying or formatting the SQL input.
            if (emitted >= policy.maxEvents || nowMillis() >= policy.durationMillis) {
                stopped = true
                policy.sink("sql-trace truncated limit=${if (emitted >= policy.maxEvents) "events" else "duration"}")
                return
            }
            emitted++
            val prefix = "sql-trace $operation statement=$emitted "
            val shape = formatter(sql, policy.maxInputChars, policy.maxEventChars - prefix.length)
            policy.sink(prefix + (shape ?: "opaque"))
        } catch (_: Throwable) {
            // Diagnostics never participate in SQL, cancellation, or cleanup error precedence.
            stopped = true
        }
    }
}

private val sqlShapeKeywords = setOf(
    "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "INTO", "VALUES", "SET", "WHERE",
    "AND", "OR", "NOT", "NULL", "IS", "IN", "AS", "JOIN", "ON", "LEFT", "INNER",
    "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "DISTINCT", "RETURNING",
    "CREATE", "TABLE", "INDEX", "UNIQUE", "IF", "EXISTS", "DROP", "ALTER", "ADD",
    "BEGIN", "IMMEDIATE", "DEFERRED", "EXCLUSIVE", "COMMIT", "ROLLBACK", "SAVEPOINT",
    "RELEASE", "PRAGMA", "WITH", "RECURSIVE", "UNION", "ALL", "CASE", "WHEN", "THEN",
    "ELSE", "END", "ASC", "DESC", "REPLACE", "IGNORE", "CONFLICT", "DO", "NOTHING",
)

/** Conservative lexer: keywords and punctuation only; every other token becomes a placeholder. */
internal fun sqlStatementShape(sql: String, inputLimit: Int, outputLimit: Int): String? {
    if (sql.length > inputLimit || outputLimit < 1) return null
    var i = 0
    val out = StringBuilder(minOf(sql.length, outputLimit))
    fun emit(token: String): Boolean {
        if (out.length + token.length + 1 > outputLimit) return false
        if (out.isNotEmpty()) out.append(' ')
        out.append(token)
        return true
    }
    while (i < sql.length) {
        val c = sql[i]
        when {
            c.isWhitespace() -> i++
            c == '-' && sql.getOrNull(i + 1) == '-' -> {
                i += 2
                while (i < sql.length && sql[i] != '\n') i++
            }
            c == '/' && sql.getOrNull(i + 1) == '*' -> {
                i += 2
                while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/')) i++
                if (i + 1 >= sql.length) return null
                i += 2
            }
            c == '\'' || c == '"' || c == '`' || c == '[' -> {
                val end = if (c == '[') ']' else c
                i++
                var closed = false
                while (i < sql.length) {
                    if (sql[i++] == end) {
                        if (i < sql.length && sql[i] == end) i++ else { closed = true; break }
                    }
                }
                if (!closed || !emit("?")) return null
            }
            c in 'a'..'z' || c in 'A'..'Z' || c == '_' -> {
                val start = i++
                while (i < sql.length && (sql[i].isLetterOrDigit() || sql[i] == '_')) i++
                val word = sql.substring(start, i).uppercase()
                if (!emit(if (word in sqlShapeKeywords) word else "?")) return null
            }
            c.isDigit() || c == '?' || c == ':' || c == '@' || c == '$' -> {
                i++
                while (i < sql.length && (sql[i].isLetterOrDigit() || sql[i] in "._")) i++
                if (!emit("?")) return null
            }
            c in "(),;=<>!+-*/%|&~." -> { i++; if (!emit(c.toString())) return null }
            else -> return null
        }
    }
    return out.toString().takeIf { it.isNotEmpty() }
}
