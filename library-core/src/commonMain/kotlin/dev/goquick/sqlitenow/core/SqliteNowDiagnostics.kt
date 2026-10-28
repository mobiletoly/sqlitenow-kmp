@file:OptIn(kotlin.concurrent.atomics.ExperimentalAtomicApi::class)

package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.common.deliverSqliteNowDiagnostic
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.serialization.json.*
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.AtomicReference
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.io.encoding.Base64
import kotlin.random.Random
import kotlin.time.TimeSource

/**
 * Opt-in, synchronous diagnostics through the existing logger. Sharing this instance connects
 * Core and Oversqlite policy. Changing [isEnabled] affects the next outer operation. Enabled
 * logging has a cost and a slow logger can block its caller. No SQL values are recorded.
 */
class SqliteNowDiagnostics internal constructor(
    enabled: Boolean,
    val maxSqlFamilies: Int,
    val maxActiveSpans: Int,
    internal val runtime: SqliteNowDiagnosticRuntime,
) {
    constructor(enabled: Boolean = false, maxSqlFamilies: Int = 1_024, maxActiveSpans: Int = 128) :
        this(enabled, maxSqlFamilies, maxActiveSpans, DefaultSqliteNowDiagnosticRuntime)
    init { require(maxSqlFamilies > 0); require(maxActiveSpans > 0) }

    @Volatile var isEnabled: Boolean = enabled
        private set
    @Volatile private var failed = false

    fun setEnabled(enabled: Boolean) { if (enabled) failed = false; isEnabled = enabled }

    fun beginOperation(name: String, connectionId: String? = null): SqliteNowDiagnosticSpan? {
        if (!isEnabled || failed) return null
        return safely {
            SqliteNowDiagnosticSession(this, runtime.clock(), connectionId).start(name, null)
        }
    }

    /** Use fixed developer-authored labels; never pass user input, SQL, identifiers or URLs. */
    suspend fun <T> operation(name: String, connectionId: String? = null, block: suspend () -> T): T {
        val context = currentCoroutineContext()
        val inherited = SqliteNowDiagnosticContext.current(context)
        if (inherited != null) return sqliteNowDiagnosticPhase(name, block = block)
        if (context.hasDisabledOwnedConnectionDiagnostics()) return block()
        val span = beginOperation(name, connectionId)
        // The off marker preserves a running operation's policy across a runtime toggle.
        return withSqliteNowDiagnosticContext(span) { finishDiagnosticBlock(span, block) }
    }

    internal fun <T> safely(block: () -> T): T? {
        if (failed) return null
        return try { block() } catch (_: Throwable) { failed = true; null }
    }

    internal fun deliver(record: String): Boolean {
        if (failed) return false
        val delivered = runtime.emit(record)
        if (!delivered) failed = true
        return delivered
    }
}

/** Execution-scoped bridge for integrations. A null span is an explicit disabled snapshot. */
class SqliteNowDiagnosticContext internal constructor(
    val span: SqliteNowDiagnosticSpan?,
    val sqlFamilyLabel: String? = null,
    private val parent: SqliteNowDiagnosticContext? = null,
) : AbstractCoroutineContextElement(Key) {
    @Volatile internal var active = true
    private val isActive: Boolean get() = active && span?.isFinished != true
    companion object Key : CoroutineContext.Key<SqliteNowDiagnosticContext> {
        /** Expired metadata falls back to its live parent; completed operations retain no history. */
        fun current(context: CoroutineContext): SqliteNowDiagnosticContext? {
            var frame = context[this]
            while (frame != null && !frame.isActive) frame = frame.parent
            return frame
        }
    }
}

/** Installs metadata without introducing a Job, cancellation boundary or child-waiting scope. */
suspend fun <T> withSqliteNowDiagnosticContext(
    span: SqliteNowDiagnosticSpan?,
    sqlFamilyLabel: String? = null,
    block: suspend () -> T,
): T = suspendCoroutineUninterceptedOrReturn { completion ->
    val frame = SqliteNowDiagnosticContext(span, sqlFamilyLabel, SqliteNowDiagnosticContext.current(completion.context))
    try {
        val result = block.startCoroutineUninterceptedOrReturn(object : Continuation<T> {
            override val context = completion.context + frame
            override fun resumeWith(result: Result<T>) {
                frame.active = false
                completion.resumeWith(result)
            }
        })
        if (result !== COROUTINE_SUSPENDED) frame.active = false
        result
    } catch (failure: Throwable) {
        frame.active = false
        throw failure
    }
}

/** Child phases inherit the outer snapshot, including when that snapshot is disabled. */
suspend fun <T> sqliteNowDiagnosticPhase(
    name: String,
    counts: (() -> Map<String, Long>)? = null,
    block: suspend () -> T,
): T {
    val parent = SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span ?: return block()
    val child = parent.child(name, counts)
    if (child == null) return block()
    return withSqliteNowDiagnosticContext(child) { finishDiagnosticBlock(child, block) }
}

private suspend fun <T> finishDiagnosticBlock(span: SqliteNowDiagnosticSpan?, block: suspend () -> T): T {
    var outcome = "success"
    try { return block() }
    catch (failure: Throwable) { outcome = sqliteNowDiagnosticOutcome(failure); throw failure }
    finally { span?.finish(outcome) }
}

/** Fixed categories only; exception text is deliberately excluded. */
fun sqliteNowDiagnosticOutcome(failure: Throwable): String = when {
    failure is CancellationException -> "cancelled"
    failure::class.simpleName in setOf("SyncWorkLimitException", "ApplyProgressPending", "ApplyProgressPendingException") -> "work_yield"
    else -> "failure"
}

/** An enabled operation retains bounded live aggregates, never payloads or completed span history. */
class SqliteNowDiagnosticSpan internal constructor(
    private val session: SqliteNowDiagnosticSession,
    val id: Long,
    private val parentId: Long?,
    private val name: String,
    private val started: Long,
) {
    private val metrics = AtomicReference<Array<DiagnosticSqlFamilyMetrics?>>(emptyArray())
    private val ended = AtomicLong(0)
    internal val binding = DiagnosticSpanBinding(this)
    internal val isFinished: Boolean get() = ended.load() != 0L

    fun child(name: String, counts: (() -> Map<String, Long>)? = null): SqliteNowDiagnosticSpan? =
        session.start(name, this, counts)

    fun event(name: String, outcome: String = "observed", counts: (() -> Map<String, Long>)? = null,
              countBasis: String = "observed", schema: String? = null, table: String? = null) {
        session.emit("event", id, parentId, name, outcome, counts, countBasis, schema, table = table)
    }

    fun nowNanos(): Long = session.now()
    fun setAttempt(attempt: Long) { session.attempt.store(attempt) }
    fun setWorkSlice(slice: Long) { session.slice.store(slice) }
    internal fun bindConnection(identity: String?) { session.bindConnection(identity) }

    fun aggregateWork(name: String, elapsedNanos: Long, failed: Boolean = false) {
        sql("", "work", elapsedNanos, failed = failed, familyLabel = name)
    }

    /** Numeric counters only. Call count includes failures; row count is successful step(true). */
    fun sql(sql: String, action: String, elapsedNanos: Long, returnedRow: Boolean = false,
            failed: Boolean = false, familyLabel: String? = null) {
        recordSql(if (familyLabel == null) DiagnosticSqlDescriptor(sql) else semanticSqlDescriptor,
            action, elapsedNanos, returnedRow, failed, familyLabel)
    }

    internal fun recordSql(descriptor: DiagnosticSqlDescriptor, action: String, elapsedNanos: Long,
                           returnedRow: Boolean = false, failed: Boolean = false, familyLabel: String? = null) {
        session.safe {
            val family = session.family(descriptor, familyLabel)
            val slot = diagnosticSqlActions.indexOf(action).takeIf { it >= 0 } ?: diagnosticSqlActions.lastIndex
            addMetric(family, slot, elapsedNanos, returnedRow, failed)
            val root = session.root
            if (root !== this) root?.addMetric(family, slot, elapsedNanos, returnedRow, failed)
        }
    }

    private fun addMetric(family: DiagnosticSqlFamily, action: Int, elapsed: Long, row: Boolean, failed: Boolean) {
        while (true) {
            val old = metrics.load()
            val existing = old.getOrNull(family.index)
            if (existing != null) { existing.actions[action].record(elapsed, row, failed); return }
            val next = old.copyOf(maxOf(old.size, family.index + 1))
            val fresh = DiagnosticSqlFamilyMetrics(family.id)
            next[family.index] = fresh
            if (metrics.compareAndSet(old, next)) { fresh.actions[action].record(elapsed, row, failed); return }
        }
    }

    fun finish(outcome: String = "success", counts: (() -> Map<String, Long>)? = null,
               countBasis: String = "observed") {
        if (!ended.compareAndSet(0, 1)) return
        binding.span = null
        session.safe {
            val endedAt = session.now()
            metrics.exchange(emptyArray()).forEach { family ->
                family?.actions?.forEach { metric -> if (metric.count.load() != 0L) session.emitMetric(id, metric) }
            }
            session.emit("span_end", id, parentId, name, outcome, counts, countBasis,
                elapsed = (endedAt - started).coerceAtLeast(0), timestamp = endedAt)
            session.release()
        }
    }
}

/** Owned by one serialized statement; classification is lazy and retains no capture or values. */
internal class DiagnosticSqlDescriptor(sql: String) {
    private var unclassifiedSql: String? = if (sql.length <= 4_096) sql else ""
    private var classified = false
    private var classification = "opaque"
    fun shape(runtime: SqliteNowDiagnosticRuntime): String {
        if (!classified) {
            classification = runtime.shape(checkNotNull(unclassifiedSql)) ?: "opaque"
            classified = true
            unclassifiedSql = null
        }
        return classification
    }
}

internal data class DiagnosticSqlFamily(val index: Int, val id: String)
/** Cached statements retain this revocable link, never a completed span/session. */
internal class DiagnosticSpanBinding(@Volatile var span: SqliteNowDiagnosticSpan?)
internal class DiagnosticSqlFamilyMetrics(family: String) {
    val actions = diagnosticSqlActions.map { DiagnosticSqlMetric(family, it) }.toTypedArray()
}

internal class DiagnosticSqlMetric(val family: String, val action: String) {
    val count = AtomicLong(0); val total = AtomicLong(0); val min = AtomicLong(Long.MAX_VALUE)
    val max = AtomicLong(0); val rows = AtomicLong(0); val failures = AtomicLong(0)
    fun record(elapsed: Long, row: Boolean, failed: Boolean) {
        val duration = elapsed.coerceAtLeast(0)
        count.fetchAndAdd(1); total.fetchAndAdd(duration)
        var low = min.load()
        while (duration < low && !min.compareAndSet(low, duration)) low = min.load()
        var high = max.load()
        while (duration > high && !max.compareAndSet(high, duration)) high = max.load()
        if (row) rows.fetchAndAdd(1)
        if (failed) failures.fetchAndAdd(1)
    }
}

internal class SqliteNowDiagnosticSession(
    private val policy: SqliteNowDiagnostics,
    private val clock: () -> Long,
    @Volatile private var connection: String?,
) {
    @Volatile private var stopped = false
    @Volatile private var multipleConnections = false
    private val capture = Random.nextLong().toULong().toString(16)
    val attempt = AtomicLong(0)
    val slice = AtomicLong(0)
    private val sequence = AtomicLong(0)
    private val spanIds = AtomicLong(0)
    private val active = AtomicLong(0)
    private val lostSpans = AtomicLong(0)
    private val lostFamilies = AtomicLong(0)
    private val families = AtomicReference<Map<String, DiagnosticSqlFamily>>(emptyMap())
    private val overflowFamily = DiagnosticSqlFamily(policy.maxSqlFamilies, "overflow")
    @Volatile var root: SqliteNowDiagnosticSpan? = null
        private set

    fun bindConnection(identity: String?) {
        if (identity == null) return
        if (connection == null) connection = identity
        else if (connection != identity) multipleConnections = true
    }

    fun now(): Long {
        if (stopped) return 0
        return policy.safely(clock) ?: run { stopped = true; 0L }
    }
    fun safe(block: () -> Unit) {
        if (stopped) return
        policy.safely {
            try { block() } catch (failure: Throwable) { stopped = true; throw failure }
        }
    }

    fun start(name: String, parent: SqliteNowDiagnosticSpan?, counts: (() -> Map<String, Long>)? = null): SqliteNowDiagnosticSpan? =
        policy.safely {
            if (active.fetchAndAdd(1) >= policy.maxActiveSpans) {
                active.fetchAndAdd(-1); lostSpans.fetchAndAdd(1); null
            } else {
                val id = spanIds.fetchAndAdd(1) + 1
                val span = SqliteNowDiagnosticSpan(this, id, parent?.id, name, now())
                if (parent == null) root = span
                emit("span_start", id, parent?.id, name, "start", counts, "expected")
                span
            }
        }

    fun release() { active.fetchAndAdd(-1) }

    fun family(descriptor: DiagnosticSqlDescriptor, label: String?): DiagnosticSqlFamily {
        // No raw SQL hash, literals or identifiers. Semantic labels are fixed call-site names.
        val shape = label?.take(128) ?: descriptor.shape(policy.runtime)
        while (true) {
            val old = families.load()
            old[shape]?.let { return it }
            if (old.size >= policy.maxSqlFamilies) { lostFamilies.fetchAndAdd(1); return overflowFamily }
            val family = DiagnosticSqlFamily(old.size, "q${old.size + 1}")
            if (families.compareAndSet(old, old + (shape to family))) {
                emit("sql_family", root?.id ?: 1, null, shape, "defined", family = family.id)
                return family
            }
        }
    }

    fun emitMetric(span: Long, metric: DiagnosticSqlMetric) {
        emit(if (metric.action == "work") "work_metric" else "sql_metric", span, null,
            if (metric.action == "work") metric.family else metric.action, "observed", {
            mapOf("calls" to metric.count.load(), "total_ns" to metric.total.load(),
                "min_ns" to metric.min.load(), "max_ns" to metric.max.load(),
                "returned_rows" to metric.rows.load(), "failures" to metric.failures.load())
        }, "api_calls_returned_rows", family = metric.family)
    }

    fun emit(kind: String, span: Long, parent: Long?, name: String, outcome: String,
             counts: (() -> Map<String, Long>)? = null, countBasis: String = "observed",
             schema: String? = null, elapsed: Long? = null, family: String? = null, timestamp: Long? = null, table: String? = null) = safe {
        val seq = sequence.fetchAndAdd(1) + 1
        val json = buildJsonObject {
            put("v", 1); put("capture", capture); put("seq", seq); put("operation", 1)
            val aggregate = kind == "sql_metric" || kind == "work_metric"
            put("attempt", if (aggregate) 0 else attempt.load()); put("slice", if (aggregate) 0 else slice.load())
            if (aggregate) put("aggregation", "span")
            put("span", span); parent?.let { put("parent", it) }; connection?.let { put("connection", it) }
            put("kind", kind); put("name", name.take(512)); put("outcome", outcome)
            put("multiple_connections", multipleConnections)
            put("t_ns", timestamp ?: now()); elapsed?.let { put("elapsed_ns", it) }
            family?.let { put("family", it) }; schema?.let { put("schema", it.take(512)) }
            table?.let { put("table", it.take(512)) }
            put("count_basis", countBasis)
            put("counts", buildJsonObject { counts?.invoke()?.forEach { (key, value) -> put(key, value) } })
            put("lost_spans", lostSpans.load()); put("family_overflow_calls", lostFamilies.load())
        }.toString()
        if (json.encodeToByteArray().size <= 2_970) {
            if (!policy.deliver("SQLITENOW_DIAG $json")) stopped = true
        }
        else {
            val encoded = Base64.encode(json.encodeToByteArray())
            val chunks = encoded.chunked(2_400)
            chunks.forEachIndexed { index, data ->
                val fragment = buildJsonObject {
                    put("v", 1); put("capture", capture); put("seq", seq)
                    put("part", index); put("parts", chunks.size); put("encoding", "base64"); put("data", data)
                }
                if (!stopped && !policy.deliver("SQLITENOW_DIAG $fragment")) stopped = true
            }
        }
    }
}

/** Clock, formatter and logger dependencies; production and tests use the same capture path. */
internal interface SqliteNowDiagnosticRuntime {
    fun clock(): () -> Long
    fun emit(message: String): Boolean
    fun shape(sql: String): String?
}

private object DefaultSqliteNowDiagnosticRuntime : SqliteNowDiagnosticRuntime {
    override fun clock(): () -> Long {
        val origin = TimeSource.Monotonic.markNow()
        return { origin.elapsedNow().inWholeNanoseconds }
    }
    override fun emit(message: String): Boolean = deliverSqliteNowDiagnostic(message)
    override fun shape(sql: String): String? = sqlStatementShape(sql, 4_096, 512)
}

private val diagnosticSqlActions = listOf("prepare", "execute", "step", "reset", "clear_bindings", "close", "cache_hit", "cache_miss", "work", "other")
// Labelled work never classifies SQL; share this inert descriptor rather than allocate per wait.
private val semanticSqlDescriptor = DiagnosticSqlDescriptor("")
