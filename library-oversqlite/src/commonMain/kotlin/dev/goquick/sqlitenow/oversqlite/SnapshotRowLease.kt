package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.*

/** One row in one pass. The caller's original row admission covers its base representation. */
internal class SnapshotRowLease private constructor(
    val db: SafeSQLiteConnection,
    val row: ApplySurvivor,
    val version: Long,
    val charge: Long,
    private val unit: String,
    private val cache: StatementCache?,
    private val workspace: OversqliteWorkspace,
    private val json: Json,
    private var decoded: JsonObject?,
    private val slice: OversqliteWorkSlice?,
    private var span: SqliteNowDiagnosticSpan?,
    private var typedValues: SnapshotTypedValues?,
    initialTypedCharge: WorkspaceCharge,
    private val read: SnapshotReadWorkspace?,
) {
    private val parent = slice?.snapshotRowLease
    private var encoded: String? = row.payload
    var retainedCharge = initialTypedCharge; private set
    private var typedCharge = initialTypedCharge
    private var transferredTyped: SnapshotTypedValues? = null
    private var decodedCharge = WorkspaceCharge()
    private var closed = false
    val hasPayload: Boolean get() = decoded != null || encoded != null
    val source: PayloadSource get() = if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE

    suspend fun payloadObject(): JsonObject? {
        check(!closed)
        decoded?.let { return it }
        val text = encoded ?: return null
        val extra = WorkspaceCharge(4, workspace.scale(unit, utf8Size(text), 6))
        val retain = tryRetain(extra)
        count("row_processing.row_decode")
        val result = json.parseToJsonElement(text) as JsonObject
        if (retain) { decoded = result; decodedCharge = extra }
        return result
    }

    /** Transfer to the caller's already-admitted transient row before nested admission/DML. */
    fun takePayloadObject(): JsonObject? {
        check(!closed)
        decoded?.let { value ->
            decoded = null
            retainedCharge = WorkspaceCharge(retainedCharge.entries - decodedCharge.entries, retainedCharge.bytes - decodedCharge.bytes)
            decodedCharge = WorkspaceCharge()
            return value
        }
        val text = encoded ?: return null
        count("row_processing.row_decode")
        return json.parseToJsonElement(text).jsonObject
    }

    /** Compatibility boundary consumes the decoded form; it never retains a second copy. */
    fun legacyRow(): ApplySurvivor {
        check(!closed)
        if (encoded == null && decoded != null) {
            count("row_processing.row_encode")
            encoded = checkNotNull(decoded).toString()
            decoded = null
            typedValues?.close(); typedValues = null
            retainedCharge = WorkspaceCharge(); typedCharge = WorkspaceCharge(); decodedCharge = WorkspaceCharge()
        }
        return if (row.payload == encoded) row else row.copy(payload = encoded)
    }

    fun referenceRow(): ApplySurvivor {
        check(!closed && row.role == "snapshot_ref" && hasPayload)
        count("snapshot.reference_load")
        return row
    }

    suspend fun encodedPayloadSize(): Long {
        check(!closed)
        encoded?.let { return utf8Size(it) }
        val payload = decoded ?: return 0
        val context = currentCoroutineContext()
        return encodedJsonSize(payload) { context.ensureActive() }
    }

    suspend fun value(info: TableInfo, name: String): TypedValue {
        check(!closed)
        typedValues?.requireOwner(db, info)
        return typedValues?.value(name) ?: OversqliteValueCodec.decodePayloadValue(
            info.column(name), checkNotNull(payloadObject()).getValue(name.lowercase()), source)
    }

    suspend fun matches(info: TableInfo, live: String?): Boolean {
        if (!hasPayload || live == null) return !hasPayload && live == null
        count("row_processing.live_row_decode")
        val other = json.parseToJsonElement(live).jsonObject
        val typed = typed(info)
        return if (typed != null) typed.matches(other, PayloadSource.LOCAL_STATE)
        else OversqliteValueCodec.equivalentPayloadObjects(info, checkNotNull(payloadObject()), other, source, PayloadSource.LOCAL_STATE)
    }

    private suspend fun typed(info: TableInfo): SnapshotTypedValues? {
        typedValues?.let { it.requireOwner(db, info); count("row_processing.typed_row_reuse"); return it }
        val extra = SnapshotTypedValues.charge(workspace, unit, info, charge)
        if (!tryRetain(extra)) return null
        val payload = payloadObject() ?: return null
        val typed = SnapshotTypedValues.decode(db, info, source, payload, span)
        typedValues = typed; typedCharge = extra
        return typed
    }

    fun releaseOptionalTyped(connection: SafeSQLiteConnection): Boolean {
        if (connection !== db || typedValues == null) return false
        typedValues?.close(); typedValues = null
        retainedCharge = WorkspaceCharge(retainedCharge.entries - typedCharge.entries, retainedCharge.bytes - typedCharge.bytes)
        typedCharge = WorkspaceCharge()
        count("row_processing.typed_reuse_fallback")
        return true
    }

    fun takeTypedValues(): SnapshotTypedValues? {
        val typed = typedValues ?: return null
        typedValues = null; transferredTyped = typed
        retainedCharge = WorkspaceCharge(retainedCharge.entries - typedCharge.entries, retainedCharge.bytes - typedCharge.bytes)
        typedCharge = WorkspaceCharge()
        return typed
    }

    suspend fun upsert(local: OversqliteLocalStore, cache: StatementCache?, skipUniqueReplay: Boolean = false) {
        val typed = takeTypedValues()
        local.upsertRow(row.table, checkNotNull(takePayloadObject()), source, cache,
            skipUniqueReplay = skipUniqueReplay, preservedRowId = row.rowId, typedValues = typed)
    }

    /** Additional retained representations are optional, independently charged and writer-local. */
    suspend fun tryRetain(extra: WorkspaceCharge): Boolean {
        check(!closed)
        if (slice == null) { count("row_processing.row_reuse_fallback"); return false }
        try { workspace.check(unit, extra, statementCache = cache, read = read) }
        catch (capacity: SyncWorkspaceLimitException) {
            if (capacity.suppressedExceptions.isNotEmpty()) throw capacity
            count("row_processing.row_reuse_fallback")
            return false
        }
        retainedCharge = workspace.add(unit, retainedCharge, extra)
        return true
    }

    private fun count(name: String) { span?.aggregateWork(name, 0) }
    fun close() {
        if (closed) return
        typedValues?.close(); transferredTyped?.close(); typedValues=null; transferredTyped=null
        closed = true; decoded = null; encoded = null; span = null; retainedCharge = WorkspaceCharge(); decodedCharge = WorkspaceCharge()
        if (slice?.snapshotRowLease === this) slice.snapshotRowLease = parent
    }

    fun extraCharge(connection: SafeSQLiteConnection, unit: String): WorkspaceCharge {
        var current: SnapshotRowLease? = this
        var total = WorkspaceCharge()
        while (current != null) {
            if (current.db === connection) total = workspace.add(unit, total, current.retainedCharge)
            current = current.parent
        }
        return total
    }

    companion object {
        suspend fun <T> use(db: SafeSQLiteConnection, row: ApplySurvivor, version: Long, charge: Long,
                            unit: String, cache: StatementCache?, workspace: OversqliteWorkspace, json: Json,
                            decoded: JsonObject?, typedValues: SnapshotTypedValues? = null,
                            typedCharge: WorkspaceCharge = WorkspaceCharge(), read: SnapshotReadWorkspace? = null, block: suspend (SnapshotRowLease) -> T): T {
            val context = currentCoroutineContext()
            val slice = context[OversqliteWorkSlice]
            val span = SqliteNowDiagnosticContext.current(context)?.span
            // Raw adapters have no registered owner for an additional retained representation.
            if (slice == null) typedValues?.close()
            val retainedTyped = if (slice == null) null else typedValues
            val retainedTypedCharge = if (retainedTyped == null) WorkspaceCharge() else typedCharge
            val lease = SnapshotRowLease(db, row, version, charge, unit, cache, workspace, json, decoded, slice, span, retainedTyped, retainedTypedCharge, read)
            if (slice != null) slice.snapshotRowLease = lease
            span?.aggregateWork("row_processing.row_lease", 0)
            try { return block(lease) } finally { lease.close() }
        }
    }
}

/** Zero-duration numeric work counters stream with their containing diagnostic span. */
internal suspend fun snapshotRowWork(name: String) {
    SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span?.aggregateWork(name, 0)
}
