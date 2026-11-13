package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan
import dev.goquick.sqlitenow.core.TransactionMode
import androidx.sqlite.async.step
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.longOrNull

internal const val generalSnapshotEngine = "general_delta"
internal const val typedSnapshotEngine = "typed_bootstrap"
internal const val bootstrapRetainedFormat = "typed-bootstrap-v1"

/** A transport tree may cross SQL page commits, never its structured request lifetime. */
internal class BootstrapTransportScope private constructor(private val workspace: OversqliteWorkspace) :
    AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<BootstrapTransportScope> {
        suspend fun <T> use(workspace: OversqliteWorkspace, block: suspend (BootstrapTransportScope) -> T): T {
            check(currentCoroutineContext()[BootstrapTransportScope] == null) { "bootstrap transport owners cannot overlap" }
            val scope = BootstrapTransportScope(workspace)
            try {
                return withContext(scope) { scope.grow(65_536, 64); block(scope) }
            } finally { scope.active = false; scope.charge = WorkspaceCharge() }
        }
    }
    val db get() = workspace.connectionOwner
    var charge = WorkspaceCharge()
        private set
    private var active = true

    suspend fun requireCurrent() {
        check(active && currentCoroutineContext()[BootstrapTransportScope] === this) { "bootstrap transport owner expired or changed" }
    }

    suspend fun grow(bufferBytes: Long, entries: Long) {
        requireCurrent()
        val unit = "bootstrap-transport"
        val next = WorkspaceCharge(maxOf(charge.entries, bootstrapCheckedAdd(32, entries)),
            maxOf(charge.bytes, bootstrapCheckedAdd(4096, bootstrapCheckedAdd(workspace.scale(unit, bufferBytes, 16), workspace.scale(unit, entries, 64)))))
        // A fresh scalar base is used at each allocation boundary. It is never
        // retained across a network await or an application/transport callback.
        workspace.check(unit, WorkspaceCharge(next.entries - charge.entries, next.bytes - charge.bytes))
        bootstrapWork("snapshot.bootstrap.quota_reads")
        charge = next
    }
}

/** Admission only: hostile wide objects count before any JSON tree allocation. */
internal suspend fun bootstrapTransportAllocationShape(raw: String): Long {
    var quoted = false; var escaped = false; var depth = 0L; var maximum = 0L
    var containers = 0L; var separators = 0L
    for (index in raw.indices) {
        if (index % 256 == 0) currentCoroutineContext().ensureActive()
        val ch = raw[index]
        if (quoted) {
            if (escaped) escaped = false else if (ch == '\\') escaped = true else if (ch == '"') quoted = false
        } else when (ch) {
            '"' -> quoted = true
            '[', '{' -> { containers++; depth++; maximum = maxOf(maximum, depth) }
            ']', '}' -> depth--
            ',', ':' -> separators++
        }
    }
    return bootstrapCheckedAdd(64 + maximum, maxOf(bootstrapReceiptScale(containers, 32),
        bootstrapReceiptScale(bootstrapCheckedAdd(containers, separators), 2)))
}

internal data class BootstrapChunkProgress(val rows: Long, val logicalBytes: Long, val nextOrdinal: Long, val hasMore: Boolean)

/** Borrowed from the independently admitted transport/chunk owner. */
internal data class BootstrapWireRow(val tableIndex: Int, val key: JsonArray, val version: Long, val payload: JsonObject)

internal class BootstrapReceiptScope(val db: SafeSQLiteConnection, initialCharge: WorkspaceCharge,
    private val span: SqliteNowDiagnosticSpan?, private val config: OversqliteConfig) :
    AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<BootstrapReceiptScope>
    var charge = initialCharge
        private set
    var active = true
        private set
    suspend fun retain(workspace: OversqliteWorkspace, unit: String, extra: WorkspaceCharge) {
        check(active && workspace.connectionOwner === db && currentCoroutineContext()[BootstrapReceiptScope] === this)
        workspace.check(unit, extra)
        charge = workspace.add(unit, charge, extra)
    }
    private var peakEntries = 0L
    private var peakBytes = 0L
    private var durableEntries = 0L
    private var durableBytes = 0L
    fun observe(durable: WorkspaceCharge? = null, admitted: WorkspaceCharge? = null) {
        if (span == null || !active) return
        durable?.let { durableEntries = maxOf(durableEntries, it.entries); durableBytes = maxOf(durableBytes, it.bytes) }
        admitted?.let { peakEntries = maxOf(peakEntries, it.entries); peakBytes = maxOf(peakBytes, it.bytes) }
    }
    fun close() {
        if (!active) return
        active = false
        span?.event("snapshot.bootstrap.workspace", counts = { mapOf("admitted_peak_entries" to peakEntries,
            "admitted_peak_bytes" to peakBytes, "durable_peak_entries" to durableEntries, "durable_peak_bytes" to durableBytes,
            "entry_limit" to config.maxSyncWorkspaceEntries, "byte_limit" to config.maxSyncWorkspaceBytes) })
    }
}

internal suspend fun bootstrapReceiptScope(db: SafeSQLiteConnection, workspace: OversqliteWorkspace,
    unit: String, charge: WorkspaceCharge): BootstrapReceiptScope {
    val parent = currentCoroutineContext()[BootstrapReceiptScope]?.takeIf { it.active && it.db === db }?.charge ?: WorkspaceCharge()
    return BootstrapReceiptScope(db, workspace.add(unit, charge, parent), currentBootstrapSpan(), workspace.configurationOwner)
}

/**
 * One logical receiving lifetime. Only bounded provider state and immutable
 * expectations cross commits; every chunk owns a new transaction and statement cache, and every
 * page within it reads a fresh quota base.
 */
internal class TypedSnapshotStage private constructor(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    val descriptor: SnapshotDescriptor,
    val session: SnapshotSession,
    private val generation: SyncGeneration,
    val source: String,
    private val operations: OversqliteClientOperationController,
    private val scope: BootstrapReceiptScope,
    private var operation: OversqliteOperationState,
    private var chain: String,
    private var expectations: List<BootstrapTableExpectation>,
) {
    val identity = BootstrapProjectionIdentity(session.snapshotId, generation.id, generation.fingerprint,
        session.snapshotBundleSeq, descriptor.signature)
    val retainedOperation: OversqliteOperationState get() = operation
    private val workspace = OversqliteWorkspace(db, config)
    private var verifiedCache: StatementCache? = null
    private var projection: BootstrapProjectionHashWriter? = BootstrapProjectionHashWriter(scope, identity)
    private var sealed = operation.snapshotStageComplete

    private suspend fun requireOwner() {
        check(scope.active && currentCoroutineContext()[BootstrapReceiptScope] === scope && scope.db === db) {
            "bootstrap receipt belongs to a different or expired operation"
        }
        check(projection != null && !sealed) { "bootstrap receipt is sealed or poisoned; verify the committed prefix before retry" }
    }

    private fun poison() { projection?.close(); projection = null }

    /** Compact structure is checked once; the receipt performs the only scalar decode. */
    suspend fun receiveCompact(
        encoded: JsonElement, after: Long, next: Long, more: Boolean, declaredBytes: Long,
        maxRows: Int, admittedRowBytes: Long, transport: BootstrapTransportScope,
    ): BootstrapChunkProgress {
        requireOwner(); transport.requireCurrent(); require(transport.db === db)
        require(after == operation.snapshotStagedRowCount && maxRows > 0 && admittedRowBytes > 0)
        val groups = encoded as? JsonArray ?: error("snapshot compact groups must be an array")
        val rows = ArrayList<BootstrapWireRow>()
        var ordinal = bootstrapCheckedAdd(after, 1); var previous = -1; var logical = 0L
        fun integer(value: JsonElement): Long {
            val primitive = value as? JsonPrimitive ?: error("compact integer required")
            require(!primitive.isString)
            return requireNotNull(primitive.longOrNull).also { require(it >= 0) }
        }
        for (element in groups) {
            currentCoroutineContext().ensureActive()
            val group = element as? JsonArray ?: error("compact group must be an array")
            require(group.size == 3)
            val tableIndex = integer(group[0]); require(tableIndex in generation.tables.indices.first.toLong()..generation.tables.indices.last.toLong())
            require(tableIndex.toInt() != previous && integer(group[1]) == ordinal)
            val tuples = group[2] as? JsonArray ?: error("compact tuples must be an array")
            require(tuples.isNotEmpty())
            for (tupleElement in tuples) {
                currentCoroutineContext().ensureActive()
                require(rows.size < maxRows) { "snapshot chunk row limit exceeded" }
                val tuple = tupleElement as? JsonArray ?: error("compact row must be an array")
                require(tuple.size == 4 && tuple[0] == JsonPrimitive("i")) { "snapshot row must be a complete insert" }
                val key = tuple[1] as? JsonArray ?: error("compact key must be an array")
                require(key.size == 1)
                val version = integer(tuple[2]); require(version > 0 && version <= session.snapshotBundleSeq)
                val payload = tuple[3] as? JsonObject ?: error("snapshot after-image must be an object")
                val rowBytes = bootstrapCheckedAdd(encodedJsonSize(key), encodedJsonSize(payload))
                require(rowBytes <= admittedRowBytes) { "snapshot row exceeds admitted decoded bytes" }
                val numberBytes = utf8Size(canonicalizeJsonElement(JsonPrimitive(version)))
                logical = bootstrapCheckedAdd(logical, bootstrapCheckedAdd(9 + tableIndex.toString().length + numberBytes, rowBytes))
                rows += BootstrapWireRow(tableIndex.toInt(), key, version, payload)
                ordinal = bootstrapCheckedAdd(ordinal, 1)
            }
            previous = tableIndex.toInt()
        }
        val count = rows.size.toLong()
        require(next == bootstrapCheckedAdd(after, count) && logical == declaredBytes && (!more || count > 0)) { "snapshot chunk progress differs" }
        val totalBytes = bootstrapCheckedAdd(operation.snapshotStagedByteCount, logical)
        require(next <= session.rowCount && totalBytes <= session.logicalByteCount &&
            if (more) next < session.rowCount && totalBytes < session.logicalByteCount
            else next == session.rowCount && totalBytes == session.logicalByteCount) { "snapshot chunk totals differ from session" }
        receive(rows)
        return BootstrapChunkProgress(count, logical, next, more)
    }

    /** Independent last-chunk wire pass, then a fresh wire/typed seal pass. */
    suspend fun seal() {
        requireOwner()
        require(operation.snapshotStagedRowCount == session.rowCount && operation.snapshotStagedByteCount == session.logicalByteCount)
        try {
            operations.completeWorkPhase("typed snapshot last chunk") {
                db.transaction(TransactionMode.DEFERRED) {
                    db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                        requireBootstrapOperationMatches(db, operation, cache)
                        operations.reserveSyncWork("bootstrap-last-chunk", snapshotWorkAdd(512,
                            snapshotWorkAdd(snapshotWorkScale(session.rowCount, 256), snapshotWorkScale(session.logicalByteCount, 4))), readyAtomic = true)
                        syncPhase("verification_last_chunk", session.rowCount, session.logicalByteCount) {
                            verifyBootstrapWire(db, config, descriptor, operation, session, generation, source, cache)
                        }
                    }
                }
            }
            // Expected projection is finalized from received values, before any
            // persisted projection result can be observed.
            val expectedProjection = checkNotNull(projection).finish(scope, session.rowCount, session.logicalByteCount)
            operations.completeWorkPhase("typed snapshot seal") {
                db.transaction(TransactionMode.IMMEDIATE) {
                    db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                        requireBootstrapOperationMatches(db, operation, cache)
                        val expected = readBootstrapExpectedStage(db, descriptor, operation, identity, workspace, cache)
                        require(expected.phase == "receiving" && expected.chain == chain && expected.tables == expectations)
                        operations.reserveSyncWork("bootstrap-seal", snapshotWorkAdd(512,
                            snapshotWorkAdd(snapshotWorkScale(session.rowCount, 512), snapshotWorkScale(session.logicalByteCount, 16))), readyAtomic = true)
                        val observed = BootstrapProjectionHashWriter(scope, identity)
                        val value = syncPhase("verification_seal", session.rowCount, session.logicalByteCount) {
                            verifyBootstrapProjection(db, config, descriptor, operation, session, generation, source, expected, cache, observed, scope)
                        }
                        require(value == expectedProjection) { "received and stored bootstrap projection differ" }
                        val next = operation.copy(snapshotStageComplete = true)
                        val manifest = BootstrapSnapshotManifest(expectedProjection, chain, expectations, next.bootstrapPageCount, next.snapshotStagedRowCount, next.snapshotStagedByteCount).canonical(identity, next)
                        val bound = next.copy(bootstrapReference = bootstrapOperationReference(identity, next, "sealed", chain, manifest, expectations))
                        val base = workspace.readDurableBase("bootstrap-seal", cache)
                        val oldCharge = bootstrapMetadataCharge(descriptor, session.snapshotId, chain, "", expectations)
                        val nextCharge = bootstrapMetadataCharge(descriptor, session.snapshotId, chain, manifest, expectations)
                        // Admit durable manifest and independent host copies before writing.
                        workspace.checkCapturedReadBase("bootstrap-seal", base, WorkspaceCharge(bytes = maxOf(0, nextCharge.bytes - oldCharge.bytes)))
                        persistBootstrapExpectedOwners(db, descriptor, session.snapshotId, chain, "sealed", manifest, expectations, cache, expectations)
                        OversqliteOperationStateStore(db).persistState(bound, cache)
                        updateBootstrapMetadataQuota(db, workspace, "bootstrap-seal", base, WorkspaceCharge(), WorkspaceCharge(), oldCharge, nextCharge, cache)
                        operation = bound
                    }
                }
            }
            sealed = true
            poison()
        } catch (failure: Throwable) { poison(); throw failure }
    }

    /** Must run in the final writer before any business effect, even after complete resume. */
    suspend fun verifyForApply(cache: StatementCache, audit: BootstrapStageAudit? = null): BootstrapSnapshotManifest {
        check(scope.active && currentCoroutineContext()[BootstrapReceiptScope] === scope && db.inTransaction() && sealed)
        cache.requireConnectionOwner(db)
        verifiedCache = null
        requireBootstrapOperationMatches(db, operation, cache)
        val expected = readBootstrapExpectedStage(db, descriptor, operation, identity, workspace, cache)
        require(expected.phase == "sealed" && operation.bootstrapCleanupChain.isEmpty())
        val manifest = BootstrapSnapshotManifest.read(expected.manifest, identity, operation, descriptor.tables.size)
        require(manifest.chain == expected.chain && manifest.tables == expected.tables)
        val fresh = BootstrapProjectionHashWriter(scope, identity)
        val observed = syncPhase("verification_final_apply", session.rowCount, session.logicalByteCount) {
            verifyBootstrapProjection(db, config, descriptor, operation, session, generation, source, expected, cache, fresh, scope, audit = audit)
        }
        require(observed == manifest.projection) { "sealed bootstrap projection differs" }
        verifiedCache = cache
        return manifest
    }

    /** Bounded cleanup in the same final publication writer; no partial business commit. */
    suspend fun clearAfterInstallation(cache: StatementCache) {
        cache.requireConnectionOwner(db)
        check(verifiedCache === cache) { "bootstrap cleanup requires the independently verified final writer" }
        check(scope.active && currentCoroutineContext()[BootstrapReceiptScope] === scope && db.inTransaction() && sealed)
        requireBootstrapOperationMatches(db, operation, cache)
        var released = WorkspaceCharge()
        for (page in 1..operation.bootstrapPageCount) {
            currentCoroutineContext().ensureActive()
            val record = readBootstrapCleanupPage(db, session.snapshotId, page, cache)
            released = workspace.add("bootstrap-cleanup", released,
                deleteBootstrapPage(db, descriptor, session.snapshotId, operation.snapshotBundleSeq, record, page, cache))
        }
        require(released.entries == bootstrapCheckedAdd(operation.snapshotStagedRowCount, operation.bootstrapPageCount))
        finishBootstrapCleanup(db, descriptor, cache)
        sealed = false
        poison()
    }

    /**
     * Stages one downloaded chunk. Its pages are admitted, hashed and proved one at a time, and
     * share one transaction and one statement cache: they commit or roll back together, so durable
     * progress advances a whole chunk at a time. A failure anywhere poisons this lifetime; resume
     * reads the committed prefix from SQL again.
     */
    suspend fun receive(rows: List<BootstrapWireRow>) {
        requireOwner()
        if (rows.isEmpty()) return
        // A chunk is staged whole or not at all, so a chunk that cannot be staged beside its own
        // transport tree is refused before anything is written and before this lifetime is
        // touched. The caller answers a transport refusal by requesting the same ordinal with
        // fewer rows; a refusal in the middle of staging would instead roll the chunk back and
        // repeat on every later call. The reserve is the one request sizing leaves:
        // `bootstrapChunkRowsForQuota` in OversqliteDownloadWorkflow.kt.
        workspace.check("bootstrap-transport", WorkspaceCharge(entries = bootstrapChunkStagingEntries(
            rows.size, descriptor.tables.maxOfOrNull { it.columns.size } ?: 0)))
        try {
            syncPhase("snapshot.bootstrap.staging") { operations.completeWorkPhase("typed snapshot receipt") {
                db.transaction(TransactionMode.IMMEDIATE) {
                    db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                        var offset = 0
                        while (offset < rows.size) {
                            val used = receivePage(rows, offset, cache)
                            check(used > 0)
                            offset += used
                        }
                    }
                }
            } }
        } catch (failure: Throwable) { poison(); throw failure }
    }

    private suspend fun receivePage(rows: List<BootstrapWireRow>, offset: Int, cache: StatementCache): Int {
        requireOwner()
        requireBootstrapOperationMatches(db, operation, cache)
        require(!operation.snapshotStageComplete && operation.snapshotEngine == typedSnapshotEngine)
        val unit = "bootstrap-receipt"
        val base = workspace.readDurableBase(unit, cache)
        bootstrapWork("snapshot.bootstrap.quota_reads")
        val pageCharge = bootstrapPageCharge(session.snapshotId)
        // Compare the expected owners in SQLite; do not load a second manifest.
        requireBootstrapReference(db, identity, operation, "receiving", chain, "", expectations, cache)
        var admitted = pageCharge
        var rawBytes = 0L
        var end = offset
        val rowTarget = minOf(config.transferTargets.maxSnapshotChunkRows, config.snapshotApplyBatchRows)
        val byteTarget = minOf(config.transferTargets.maxSnapshotChunkDecodedBytes, config.snapshotApplyBatchBytes)
        while (end < rows.size && end - offset < rowTarget) {
            currentCoroutineContext().ensureActive()
            val raw = rows[end]
            val binding = descriptor.tables.firstOrNull { it.tableIndex == raw.tableIndex }
                ?: error("snapshot row has no admitted table")
            val bytes = bootstrapCheckedAdd(encodedJsonSize(raw.payload), encodedJsonSize(raw.key))
            val nextRaw = bootstrapCheckedAdd(rawBytes, bytes)
            require(bytes <= byteTarget) { "bootstrap receipt row exceeds admitted page target" }
            if (nextRaw > byteTarget) break
            val identityBytes = bootstrapCheckedAdd(utf8Size(session.snapshotId),
                bootstrapCheckedAdd(utf8Size(binding.schema), utf8Size(binding.table)))
            val bound = WorkspaceCharge(32 + 4L * binding.columns.size,
                bootstrapCheckedAdd(8192, bootstrapCheckedAdd(workspace.scale(unit, bytes, 48),
                    bootstrapCheckedAdd(workspace.scale(unit, identityBytes, 24), 512L * binding.columns.size))))
            val proposed = workspace.add(unit, admitted, bound)
            if (!workspace.admitOptionalFromFreshRow(unit, workspace.withCurrentTransientCharges(unit, base), WorkspaceCharge(), proposed)) {
                if (end > offset) break
                // The exact single-row path still admits host copies before decode,
                // then durable copies before INSERT. It has no oversized exception.
                workspace.checkCapturedReadBase(unit, base, admitted)
                end++
                rawBytes = nextRaw
                break
            }
            admitted = proposed
            rawBytes = nextRaw
            end++
        }
        require(end > offset)
        require(operation.snapshotStagedRowCount <= session.rowCount - (end - offset)) { "too many snapshot rows" }
        operations.reserveSyncWork(unit, snapshotWorkAdd(512,
            snapshotWorkAdd(snapshotWorkScale(descriptor.tables.size.toLong(), 64),
                snapshotWorkAdd(snapshotWorkScale(rawBytes, 16), snapshotWorkScale((end - offset).toLong(), 256)))), readyAtomic = true)
        val first = bootstrapCheckedAdd(operation.snapshotStagedRowCount, 1)
        val last = bootstrapCheckedAdd(operation.snapshotStagedRowCount, (end - offset).toLong())
        val range = first..last
        val pageProjection = BootstrapProjectionHashWriter(scope, identity, range)
        val pageWire = BootstrapWirePageHashWriter(scope, identity, range)
        var persistent = pageCharge
        var host = WorkspaceCharge()
        var logical = 0L
        val received = ArrayList<BootstrapReceivedRow>(end - offset)
        val tableCounts = LongArray(descriptor.tables.size)
        try {
            for (index in offset until end) {
                currentCoroutineContext().ensureActive()
                val raw = rows[index]
                val binding = descriptor.tables.single { it.tableIndex == raw.tableIndex }
                val row = BootstrapReceivedRow.decode(scope, binding, session.snapshotId,
                    first + index - offset, raw.version, session.snapshotBundleSeq, raw.key, raw.payload,
                    currentBootstrapSpan()) { charge ->
                    val next = workspace.add(unit, host, charge)
                    workspace.checkCapturedReadBase(unit, base, workspace.add(unit, persistent, next))
                    host = next
                }
                received += row
                persistent = workspace.add(unit, persistent, workspace.add(unit, row.wireCharge, row.typedCharge))
                workspace.checkCapturedReadBase(unit, base, workspace.add(unit, persistent, host))
                logical = bootstrapCheckedAdd(logical, row.logicalBytes)
                tableCounts[binding.slot]++
            }
            require(operation.snapshotStagedByteCount <= session.logicalByteCount - logical) { "snapshot logical bytes exceed metadata" }
            val oldMetadata = bootstrapMetadataCharge(descriptor, session.snapshotId, chain, "", expectations)
            // Projection hashes become fixed 64-byte strings on the first page
            // for a slot. Admit that exact metadata growth before creating the
            // new proof/inventory and before the first durable row effect.
            var growthBytes = 0L
            for (slot in tableCounts.indices) if (tableCounts[slot] != 0L && expectations[slot].projection.isEmpty())
                growthBytes = bootstrapCheckedAdd(growthBytes, 6L * 64)
            val nextMetadata = oldMetadata.copy(bytes = bootstrapCheckedAdd(oldMetadata.bytes, growthBytes))
            workspace.checkCapturedReadBase(unit, base, workspace.add(unit, WorkspaceCharge(bytes = growthBytes),
                workspace.add(unit, persistent, host)))
            for (row in received) {
                currentCoroutineContext().ensureActive()
                val values = row.values(scope)
                val frame = row.frame(scope)
                checkNotNull(projection).add(scope, row.ordinal, row.binding.tableIndex, row.rowVersion, row.keyJson, values, row.logicalBytes)
                pageProjection.add(scope, row.ordinal, row.binding.tableIndex, row.rowVersion, row.keyJson, values, row.logicalBytes)
                pageWire.add(scope, row.ordinal, frame, row.logicalBytes)
            }
            val count = (end - offset).toLong()
            val page = bootstrapCheckedAdd(operation.bootstrapPageCount, 1)
            val typedHash = pageProjection.finish(scope, count, logical)
            val wireHash = pageWire.finish(scope, count, logical)
            val proof = bootstrapPageProof(identity, page, first, last, count, logical, wireHash, typedHash)
            val nextChain = bootstrapPageChainExtend(chain, proof)
            val nextTables = expectations.mapIndexed { slot, old ->
                if (tableCounts[slot] == 0L) old else old.append(identity, descriptor.tables[slot], page, tableCounts[slot], typedHash)
            }
            val next = operation.copy(snapshotStagedRowCount = last,
                snapshotStagedByteCount = bootstrapCheckedAdd(operation.snapshotStagedByteCount, logical), bootstrapPageCount = page)
            val boundNext = next.copy(bootstrapReference = bootstrapOperationReference(identity, next, "receiving", nextChain, "", nextTables))
            require(bootstrapMetadataCharge(descriptor, session.snapshotId, nextChain, "", nextTables) == nextMetadata)
            // Decode/hash/proof construction and every charge are complete.
            // SQL binds borrow the already owned values without decoding again.
            val groupCharge = WorkspaceCharge(4L * descriptor.tables.size + received.size,
                1024L + 64L * descriptor.tables.size + 16L * received.size)
            host = workspace.add(unit, host, groupCharge)
            workspace.checkCapturedReadBase(unit, base, workspace.add(unit, WorkspaceCharge(bytes = growthBytes),
                workspace.add(unit, persistent, host)))
            val groups = Array(descriptor.tables.size) { slot -> ArrayList<BootstrapReceivedRow>(tableCounts[slot].toInt()) }
            for (row in received) groups[row.binding.slot].add(row)
            for ((index, row) in received.withIndex()) {
                currentCoroutineContext().ensureActive()
                // Sixteen variables fit the descriptor's proved minimum of18.
                // Both frames borrow this already admitted page; no row is decoded again.
                if (index % 2 == 0) {
                    val pair = received.getOrNull(index + 1)
                    db.withPreparedStatement(if (pair == null) bootstrapWireInsertSql else bootstrapWirePairInsertSql, cache) { st ->
                        fun bind(member: BootstrapReceivedRow, offset: Int) {
                            st.bindText(offset + 1, session.snapshotId); st.bindLong(offset + 2, member.ordinal)
                            st.bindLong(offset + 3, member.binding.tableIndex.toLong()); st.bindText(offset + 4, member.keyJson)
                            st.bindLong(offset + 5, member.rowVersion); st.bindBlob(offset + 6, member.frame(scope))
                            st.bindLong(offset + 7, member.logicalBytes); st.bindLong(offset + 8, member.wireCharge.bytes)
                        }
                        bind(row, 0); pair?.let { bind(it, 8) }; st.step()
                    }
                }
            }
            for (binding in descriptor.tables) {
                val group = groups[binding.slot]
                var index = 0
                while (index < group.size) {
                    currentCoroutineContext().ensureActive()
                    val row = group[index]
                    val pair = if (binding.insertPairSql != null) group.getOrNull(index + 1) else null
                    db.withPreparedStatement(if (pair == null) binding.insertSql else checkNotNull(binding.insertPairSql), cache) { st ->
                        fun bind(member: BootstrapReceivedRow, offset: Int) {
                            st.bindText(offset + 1, session.snapshotId); st.bindLong(offset + 2, member.ordinal)
                            st.bindText(offset + 3, member.keyJson); st.bindLong(offset + 4, member.rowVersion)
                            st.bindLong(offset + 5, member.typedCharge.bytes)
                            member.values(scope).forEachIndexed { n, value ->
                                OversqliteValueCodec.bindTypedValue(st, offset + n + 6, binding.columns[n], value)
                            }
                        }
                        bind(row, 0); pair?.let { bind(it, binding.columns.size + 5) }; st.step()
                    }
                    index += if (pair == null) 1 else 2
                }
            }
            db.withPreparedStatement(bootstrapPageInsertSql, cache) { st ->
                st.bindText(1, session.snapshotId); st.bindLong(2, page); st.bindLong(3, first); st.bindLong(4, last)
                st.bindLong(5, count); st.bindLong(6, logical); st.bindText(7, wireHash); st.bindText(8, typedHash)
                st.bindText(9, proof); st.bindText(10, nextChain); st.bindLong(11, pageCharge.bytes); st.step()
            }
            persistBootstrapExpectedOwners(db, descriptor, session.snapshotId, nextChain, "receiving", "", nextTables, cache, expectations)
            OversqliteOperationStateStore(db).persistState(boundNext, cache)
            db.withPreparedStatement("UPDATE _sync_operation_state SET snapshot_stage_entries=snapshot_stage_entries+?,snapshot_stage_bytes=snapshot_stage_bytes+? WHERE singleton_key=1", cache) {
                it.bindLong(1, persistent.entries); it.bindLong(2, persistent.bytes); it.step()
            }
            updateBootstrapMetadataQuota(db, workspace, unit, base, persistent, host,
                oldMetadata, nextMetadata, cache)
            // Assignment is provisional until the enclosing transaction/cache closes.
            // Any failure after this point poisons the lifetime; resume reads SQL again.
            operation = boundNext; chain = nextChain; expectations = nextTables
            bootstrapWork("snapshot.bootstrap.admitted_pages")
            return end - offset
        } finally {
            received.forEach { it.close() }; pageProjection.close(); pageWire.close()
        }
    }

    companion object {
        suspend fun <T> resume(
            db: SafeSQLiteConnection, config: OversqliteConfig, descriptor: SnapshotDescriptor,
            session: SnapshotSession, generation: SyncGeneration, source: String,
            operations: OversqliteClientOperationController, block: suspend (TypedSnapshotStage) -> T,
        ): T {
            validateSnapshotSession(session)
            val workspace = OversqliteWorkspace(db, config)
            val operation = db.transaction(TransactionMode.DEFERRED) {
                db.withStatementCache(cleanupTimeoutMillis = 5_000) { readBootstrapOperation(db, workspace, it).first }
            }
            require(operation.snapshotEngine == typedSnapshotEngine && operation.stagedSnapshotId == session.snapshotId &&
                operation.snapshotBundleSeq == session.snapshotBundleSeq && operation.snapshotRowCount == session.rowCount &&
                operation.snapshotByteCount == session.logicalByteCount)
            requireBootstrapTransfer(config, operation.snapshotTransfer, session, source)
            val fixed = workspace.add("bootstrap-resume", descriptor.charge, WorkspaceCharge(64 + 16L * descriptor.tables.size,
                bootstrapCheckedAdd(32768 + 4096L * descriptor.tables.size,
                    workspace.scale("bootstrap-resume", bootstrapCheckedAdd(utf8Size(operation.snapshotTransfer), utf8Size(source)), 16))))
            workspace.check("bootstrap-resume", fixed)
            val scope = bootstrapReceiptScope(db, workspace, "bootstrap-resume", fixed)
            var stage: TypedSnapshotStage? = null
            try {
                return withContext(scope) {
                    val identity = BootstrapProjectionIdentity(session.snapshotId, generation.id, generation.fingerprint, session.snapshotBundleSeq, descriptor.signature)
                    operations.completeWorkPhase("typed snapshot resume") {
                        db.transaction(TransactionMode.DEFERRED) {
                            db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                                verifyBootstrapDescriptorHeader(db, descriptor, cache)
                                verifyBootstrapFamilyObjects(db, descriptor, cache)
                                requireBootstrapOperationMatches(db, operation, cache)
                                val expected = readBootstrapExpectedStage(db, descriptor, operation, identity, workspace, cache)
                                require(expected.phase != "retiring") { "retiring bootstrap cannot be adopted" }
                                val resumed = TypedSnapshotStage(db, config, descriptor, session, generation, source, operations,
                                    scope, operation, expected.chain, expected.tables)
                                stage = resumed
                                operations.reserveSyncWork("bootstrap-resume", snapshotWorkAdd(512,
                                    snapshotWorkAdd(snapshotWorkScale(operation.snapshotStagedRowCount, 512),
                                        snapshotWorkScale(operation.snapshotStagedByteCount, 16))), readyAtomic = true)
                                val observed = verifyBootstrapProjection(db, config, descriptor, operation, session, generation,
                                    source, expected, cache, checkNotNull(resumed.projection), scope, finish = operation.snapshotStageComplete)
                                if (operation.snapshotStageComplete) {
                                    val manifest = BootstrapSnapshotManifest.read(expected.manifest, identity, operation, descriptor.tables.size)
                                    require(observed == manifest.projection && manifest.chain == expected.chain && manifest.tables == expected.tables)
                                    resumed.poison()
                                }
                            }
                        }
                    }
                    block(checkNotNull(stage))
                }
            } finally { stage?.poison(); scope.close() }
        }

        suspend fun <T> begin(
            db: SafeSQLiteConnection, config: OversqliteConfig, admission: BootstrapSchemaAdmission.Eligible,
            metadata: ValidatedConfig, session: SnapshotSession, generation: SyncGeneration, source: String,
            stageGuard: String, transfer: String, operations: OversqliteClientOperationController,
            beforeStaging: suspend () -> Unit = {},
            block: suspend (TypedSnapshotStage) -> T,
        ): T {
            validateSnapshotSession(session)
            require(source.isNotEmpty() && isCanonicalSha256(stageGuard))
            requireBootstrapTransfer(config, transfer, session, source)
            val descriptor = admission.descriptor
            require(descriptor.generationId == generation.id && descriptor.generationFingerprint == generation.fingerprint)
            val strings = bootstrapCheckedAdd(utf8Size(transfer), bootstrapCheckedAdd(utf8Size(source), utf8Size(session.snapshotId)))
            val workspace = OversqliteWorkspace(db, config)
            val fixed = workspace.add("bootstrap-receipt", descriptor.charge, WorkspaceCharge(64 + 16L * descriptor.tables.size,
                bootstrapCheckedAdd(32768 + 4096L * descriptor.tables.size, workspace.scale("bootstrap-receipt", strings, 16))))
            workspace.check("bootstrap-receipt", fixed)
            val scope = bootstrapReceiptScope(db, workspace, "bootstrap-receipt", fixed)
            var stage: TypedSnapshotStage? = null
            try {
                return withContext(scope) {
                    val identity = BootstrapProjectionIdentity(session.snapshotId, generation.id, generation.fingerprint,
                        session.snapshotBundleSeq, descriptor.signature)
                    val tables = descriptor.tables.map { BootstrapTableExpectation() }
                    val chain = bootstrapPageChainSeed(identity)
                    val operation = operations.completeWorkPhase("typed snapshot begin") {
                        db.transaction(TransactionMode.IMMEDIATE) {
                            beforeStaging()
                            admission.pin.requireCurrent(db, metadata)
                            requireCleanSnapshotWork(db)
                            db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                                require(bootstrapManagedStateEmpty(db, metadata, cache)) { "bootstrap requires empty managed state" }
                                verifyBootstrapDescriptorHeader(db, descriptor, cache)
                                requireEmptyBootstrapSnapshotOwners(db, descriptor, cache)
                                require(db.scalarLong("SELECT EXISTS(SELECT 1 FROM _sync_snapshot_stage)") == 0L) { "bootstrap cannot replace general staged payloads" }
                                val old = OversqliteOperationStateStore(db).loadState()
                                require(old.stagedSnapshotId.isEmpty() && old.snapshotEngine == generalSnapshotEngine)
                                val next = old.copy(stagedSnapshotId = session.snapshotId, snapshotBundleSeq = session.snapshotBundleSeq,
                                    snapshotRowCount = session.rowCount, snapshotByteCount = session.logicalByteCount,
                                    snapshotStageGuard = stageGuard, snapshotTransfer = transfer, snapshotEngine = typedSnapshotEngine)
                                val bound = next.copy(bootstrapReference = bootstrapOperationReference(identity, next, "receiving", chain, "", tables))
                                val base = workspace.readDurableBase("bootstrap-receipt", cache)
                                val oldMetadata = bootstrapMetadataCharge(descriptor, "", "", "", tables)
                                val nextMetadata = bootstrapMetadataCharge(descriptor, session.snapshotId, chain, "", tables)
                                workspace.checkCapturedReadBase("bootstrap-receipt", base,
                                    WorkspaceCharge(bytes = maxOf(0, nextMetadata.bytes - oldMetadata.bytes)))
                                persistBootstrapExpectedOwners(db, descriptor, session.snapshotId, chain, "receiving", "", tables, cache, tables)
                                OversqliteOperationStateStore(db).persistState(bound, cache)
                                updateBootstrapMetadataQuota(db, workspace, "bootstrap-receipt", base, WorkspaceCharge(), WorkspaceCharge(),
                                    oldMetadata, nextMetadata, cache)
                                bound
                            }
                        }
                    }
                    stage = TypedSnapshotStage(db, config, descriptor, session, generation, source, operations, scope, operation, chain, tables)
                    block(checkNotNull(stage))
                }
            } finally { stage?.poison(); scope.close() }
        }
    }
}

private fun requireBootstrapTransfer(config: OversqliteConfig, transfer: String, session: SnapshotSession, source: String) {
    val seal = kotlinx.serialization.json.Json.decodeFromString(RetainedSnapshotSeal.serializer(), transfer)
    require(seal.session == session && seal.source == source && seal.authority == config.serviceId && isCanonicalSha256(session.snapshotHash)) { "bootstrap transfer differs from authority" }
}

/** Equality stays inside SQLite, so a corrupt mutable pin cannot allocate a large copy. */
internal suspend fun requireBootstrapOperationMatches(db: SafeSQLiteConnection, expected: OversqliteOperationState, cache: StatementCache) {
    require(db.withPreparedStatement("""SELECT count(*) FROM _sync_operation_state WHERE singleton_key=1
        AND kind=? AND target_user_id=? AND staged_snapshot_id=? AND snapshot_bundle_seq=? AND snapshot_row_count=?
        AND snapshot_byte_count=? AND snapshot_stage_complete=? AND snapshot_staged_row_count=? AND snapshot_staged_byte_count=?
        AND snapshot_stage_guard=? AND snapshot_transfer=? AND snapshot_engine=? AND bootstrap_reference=?
        AND bootstrap_page_count=? AND reason=? AND replacement_source_id=?
        AND snapshot_cleanup_ordinal=? AND bootstrap_cleanup_chain=?""", cache) {
        it.bindText(1, expected.kind); it.bindText(2, expected.targetUserId); it.bindText(3, expected.stagedSnapshotId)
        it.bindLong(4, expected.snapshotBundleSeq); it.bindLong(5, expected.snapshotRowCount); it.bindLong(6, expected.snapshotByteCount)
        it.bindLong(7, if (expected.snapshotStageComplete) 1 else 0); it.bindLong(8, expected.snapshotStagedRowCount)
        it.bindLong(9, expected.snapshotStagedByteCount); it.bindText(10, expected.snapshotStageGuard); it.bindText(11, expected.snapshotTransfer)
        it.bindText(12, expected.snapshotEngine); it.bindText(13, expected.bootstrapReference); it.bindLong(14, expected.bootstrapPageCount)
        it.bindText(15, expected.reason); it.bindText(16, expected.replacementSourceId)
        it.bindLong(17, expected.bootstrapCleanupOrdinal); it.bindText(18, expected.bootstrapCleanupChain); require(it.step()); it.getLong(0)
    } == 1L) { "bootstrap operation changed" }
}

private const val bootstrapWireInsertSql = "INSERT INTO _sync_bootstrap_wire(snapshot_id,row_ordinal,table_index,key_json,row_version,frame,logical_bytes,charge) VALUES(?,?,?,?,?,?,?,?)"
private const val bootstrapWirePairInsertSql = "$bootstrapWireInsertSql,(?,?,?,?,?,?,?,?)"
private const val bootstrapPageInsertSql = "INSERT INTO _sync_bootstrap_pages(snapshot_id,page_ordinal,first_ordinal,last_ordinal,row_count,logical_bytes,wire_hash,typed_hash,page_proof,chain_root,charge) VALUES(?,?,?,?,?,?,?,?,?,?,?)"

internal fun bootstrapPageCharge(snapshotId: String) = WorkspaceCharge(1, bootstrapCheckedAdd(2048, bootstrapReceiptScale(utf8Size(snapshotId), 6)))

private suspend fun currentBootstrapSpan() = dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span
internal suspend fun bootstrapWork(label: String, count: Long = 1) {
    val span = currentBootstrapSpan() ?: return
    repeat(count.toInt()) { span.aggregateWork(label, 0) }
}

internal suspend fun requireBootstrapReference(
    db: SafeSQLiteConnection, identity: BootstrapProjectionIdentity, operation: OversqliteOperationState,
    phase: String, chain: String, manifest: String, tables: List<BootstrapTableExpectation>, cache: StatementCache,
) {
    require(operation.bootstrapReference == bootstrapOperationReference(identity, operation, phase, chain, manifest, tables)) {
        "bootstrap operation reference differs"
    }
    require(db.withPreparedStatement("SELECT count(*) FROM _sync_bootstrap_state WHERE singleton_key=1 AND phase=? AND snapshot_id=? AND descriptor_signature=? AND expected_manifest=? AND chain_root=?", cache) {
        it.bindText(1, phase); it.bindText(2, identity.snapshotId); it.bindText(3, identity.descriptorSignature)
        it.bindText(4, manifest); it.bindText(5, chain); check(it.step()); it.getLong(0)
    } == 1L) { "bootstrap expected owner changed" }
    val expected = bootstrapCanonicalStrings(tables.flatMap { listOf(it.rows.toString(), it.projection) })
    require(db.withPreparedStatement("""SELECT json_group_array(value) FROM (
        SELECT CAST(expected_row_count AS TEXT) value,slot,0 member FROM _sync_bootstrap_tables
        UNION ALL SELECT expected_projection value,slot,1 member FROM _sync_bootstrap_tables ORDER BY slot,member)""", cache) {
        check(it.step()); it.getText(0)
    } == expected) { "bootstrap table expectations changed" }
}

internal suspend fun persistBootstrapExpectedOwners(
    db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, snapshotId: String, chain: String, phase: String,
    manifest: String, tables: List<BootstrapTableExpectation>, cache: StatementCache,
    previous: List<BootstrapTableExpectation>? = null,
) {
    db.withPreparedStatement("UPDATE _sync_bootstrap_state SET phase=?,snapshot_id=?,expected_manifest=?,chain_root=? WHERE singleton_key=1", cache) {
        it.bindText(1, phase); it.bindText(2, snapshotId); it.bindText(3, manifest); it.bindText(4, chain); it.step()
    }
    for (binding in descriptor.tables) {
        if (previous?.get(binding.slot) == tables[binding.slot]) continue
        db.withPreparedStatement("UPDATE _sync_bootstrap_tables SET expected_row_count=?,expected_projection=? WHERE slot=?", cache) {
            it.bindLong(1, tables[binding.slot].rows); it.bindText(2, tables[binding.slot].projection)
            it.bindLong(3, binding.slot.toLong()); it.step()
        }
    }
}

internal fun bootstrapMetadataCharge(
    descriptor: SnapshotDescriptor, snapshotId: String, chain: String, manifest: String,
    tables: List<BootstrapTableExpectation>,
): WorkspaceCharge {
    var strings = bootstrapCheckedAdd(utf8Size(snapshotId), bootstrapCheckedAdd(utf8Size(chain), utf8Size(manifest)))
    strings = bootstrapCheckedAdd(strings, bootstrapCheckedAdd(utf8Size(descriptor.signature), descriptor.descriptorBytes))
    var bytes = bootstrapCheckedAdd(512, bootstrapReceiptScale(strings, 6))
    for (binding in descriptor.tables) {
        val names = bootstrapCheckedAdd(bootstrapCheckedAdd(utf8Size(binding.schema), utf8Size(binding.table)),
            bootstrapCheckedAdd(utf8Size(descriptor.signature), utf8Size(tables[binding.slot].projection)))
        bytes = bootstrapCheckedAdd(bytes, bootstrapCheckedAdd(128, bootstrapReceiptScale(names, 6)))
    }
    return WorkspaceCharge(1L + descriptor.tables.size, bytes)
}

internal suspend fun updateBootstrapMetadataQuota(
    db: SafeSQLiteConnection, workspace: OversqliteWorkspace, unit: String, base: WorkspaceCharge,
    persistent: WorkspaceCharge, host: WorkspaceCharge, old: WorkspaceCharge, next: WorkspaceCharge,
    cache: StatementCache,
) {
    val growth = WorkspaceCharge(maxOf(0, next.entries - old.entries), maxOf(0, next.bytes - old.bytes))
    workspace.checkCapturedReadBase(unit, base, workspace.add(unit, growth, workspace.add(unit, persistent, host)))
    db.withPreparedStatement("""UPDATE _sync_operation_state SET bootstrap_metadata_entries=?,bootstrap_metadata_bytes=?
        WHERE singleton_key=1 AND bootstrap_metadata_entries=? AND bootstrap_metadata_bytes=?""", cache) {
        it.bindLong(1, next.entries); it.bindLong(2, next.bytes); it.bindLong(3, old.entries); it.bindLong(4, old.bytes); it.step()
    }
    require(db.scalarLong("SELECT changes()") == 1L) { "bootstrap metadata quota owner changed" }
}

/** One admitted received row. No JSON payload is retained after receipt. */
internal class BootstrapReceivedRow private constructor(
    private val owner: Any,
    val binding: SnapshotTableBinding,
    val ordinal: Long,
    val keyJson: String,
    val rowVersion: Long,
    val logicalBytes: Long,
    val wireCharge: WorkspaceCharge,
    val typedCharge: WorkspaceCharge,
    val hostCharge: WorkspaceCharge,
    private var retainedValues: List<TypedValue>?,
    private var retainedFrame: ByteArray?,
) {
    fun requireOwner(candidate: Any, table: SnapshotTableBinding = binding) {
        check(owner === candidate && table === binding && retainedValues != null && retainedFrame != null) {
            "received row belongs to a different or expired owner"
        }
    }
    fun values(candidate: Any): List<TypedValue> { requireOwner(candidate); return checkNotNull(retainedValues) }
    fun frame(candidate: Any): ByteArray { requireOwner(candidate); return checkNotNull(retainedFrame) }
    fun close() { retainedValues = null; retainedFrame = null }

    companion object {
        /** Raw HTTP JSON is independently admitted by its request owner. */
        suspend fun decode(
            owner: Any,
            binding: SnapshotTableBinding,
            snapshotId: String,
            ordinal: Long,
            version: Long,
            boundary: Long,
            wireKey: JsonArray,
            payload: JsonObject,
            span: SqliteNowDiagnosticSpan? = null,
            admit: suspend (WorkspaceCharge) -> Unit,
        ): BootstrapReceivedRow {
            require(snapshotId.isNotEmpty() && ordinal > 0 && version > 0 && version <= boundary)
            require(wireKey.size == 1 && payload.size == binding.columns.size &&
                binding.columns.all { it.name in payload }) { "snapshot payload must be an exact full after-image" }
            val keyColumn = binding.columns[binding.keyIndex]
            val rawKey = wireKey.single() as? JsonPrimitive
            require(rawKey != null && rawKey.isString && payload.getValue(keyColumn.name) == rawKey) {
                "snapshot key differs from payload"
            }
            val context = currentCoroutineContext()
            val rawBytes = bootstrapCheckedAdd(encodedJsonSize(payload) { context.ensureActive() },
                encodedJsonSize(wireKey) { context.ensureActive() })
            // Existing typed-holder convention (6*input +128*C), plus independent
            // canonical strings/encoding/frame copies (6*input +4096).
            val host = WorkspaceCharge(16 + 2L * binding.columns.size,
                bootstrapCheckedAdd(4096, bootstrapCheckedAdd(bootstrapReceiptScale(rawBytes, 12),
                    bootstrapReceiptScale(binding.columns.size.toLong(), 128))))
            admit(host)
            context.ensureActive()
            val values = binding.columns.map { column ->
                context.ensureActive()
                decodeSnapshotScalar(span) {
                    OversqliteValueCodec.decodePayloadValue(column, payload.getValue(column.name), PayloadSource.AUTHORITATIVE_WIRE)
                }
            }
            val localKey = when (val key = values[binding.keyIndex]) {
                is TypedValue.Text -> key.value
                is TypedValue.Blob -> bytesToHexLower(key.bytes)
                else -> error("bootstrap key must be TEXT or UUID BLOB")
            }
            val keyJson = canonicalizeJsonElement(JsonObject(mapOf(keyColumn.name.lowercase() to JsonPrimitive(localKey))))
            val keyText = canonicalizeJsonElement(wireKey)
            val payloadBytes = canonicalizeJsonElement(payload).encodeToByteArray()
            // Numeric envelope is logical-wire accounting; quoted numbers are hash evidence.
            val numericVersion = canonicalizeJsonElement(JsonPrimitive(version))
            val numericPrefix = "[\"i\",${binding.tableIndex},$keyText,$numericVersion,"
            val wirePrefix = "[\"i\",\"${binding.tableIndex}\",$keyText,\"$version\","
            val logical = bootstrapCheckedAdd(bootstrapCheckedAdd(utf8Size(numericPrefix), payloadBytes.size.toLong()), 1)
            val prefixBytes = wirePrefix.encodeToByteArray()
            val bodySize = bootstrapCheckedAdd(bootstrapCheckedAdd(prefixBytes.size.toLong(), payloadBytes.size.toLong()), 1)
            require(bodySize <= Int.MAX_VALUE - 8L) { "snapshot frame exceeds driver BLOB limit" }
            val frame = ByteArray(bodySize.toInt() + 8)
            bootstrapBigEndian(bodySize, 8).copyInto(frame)
            prefixBytes.copyInto(frame, 8)
            payloadBytes.copyInto(frame, 8 + prefixBytes.size)
            frame[frame.lastIndex] = ']'.code.toByte()
            val identityBytes = bootstrapCheckedAdd(bootstrapCheckedAdd(utf8Size(snapshotId), utf8Size(binding.schema)),
                bootstrapCheckedAdd(utf8Size(binding.table), utf8Size(keyJson)))
            // One staged logical row owns one entry (on its typed row); its canonical frame
            // is a second representation and charges bytes only.
            val wire = WorkspaceCharge(0, bootstrapCheckedAdd(64,
                bootstrapCheckedAdd(bootstrapReceiptScale(identityBytes, 6), frame.size.toLong())))
            val typed = WorkspaceCharge(1, bootstrapCheckedAdd(96,
                bootstrapCheckedAdd(bootstrapReceiptScale(utf8Size(keyJson), 6),
                    bootstrapReceiptScale(bootstrapProjectionRowBytes(keyJson, values), 2))))
            span?.aggregateWork("snapshot.bootstrap.received_rows", 0)
            span?.aggregateWork("snapshot.bootstrap.received_canonicalizations", 0)
            return BootstrapReceivedRow(owner, binding, ordinal, keyJson, version, logical,
                wire, typed, host, values, frame)
        }
    }
}

private fun bootstrapReceiptScale(value: Long, scale: Long): Long {
    require(value >= 0 && scale > 0 && value <= Long.MAX_VALUE / scale) { "bootstrap receipt size overflow" }
    return value * scale
}

/** Fixed owners exist even when capabilities leave the descriptor inactive. */
internal val bootstrapRetainedTableDdl = linkedMapOf(
    "_sync_bootstrap_state" to """CREATE TABLE IF NOT EXISTS _sync_bootstrap_state (
 singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1),
 format_identity TEXT NOT NULL CHECK(format_identity='typed-bootstrap-v1'),
 phase TEXT NOT NULL DEFAULT 'inactive' CHECK(phase IN ('inactive','receiving','sealed','retiring')),
 snapshot_id TEXT NOT NULL DEFAULT '',
 descriptor_signature TEXT NOT NULL DEFAULT '',
 descriptor_json TEXT NOT NULL DEFAULT '',
 expected_manifest TEXT NOT NULL DEFAULT '',
 chain_root TEXT NOT NULL DEFAULT ''
)""",
    "_sync_bootstrap_tables" to """CREATE TABLE IF NOT EXISTS _sync_bootstrap_tables (
 slot INTEGER PRIMARY KEY CHECK(typeof(slot)='integer' AND slot>=0),
 table_index INTEGER NOT NULL UNIQUE CHECK(typeof(table_index)='integer' AND table_index>=0),
 schema_name TEXT NOT NULL, table_name TEXT NOT NULL,
 layout_signature TEXT NOT NULL,
 expected_row_count INTEGER NOT NULL DEFAULT 0 CHECK(typeof(expected_row_count)='integer' AND expected_row_count>=0),
 expected_projection TEXT NOT NULL DEFAULT '',
 UNIQUE(schema_name,table_name)
)""",
    "_sync_bootstrap_wire" to """CREATE TABLE IF NOT EXISTS _sync_bootstrap_wire (
 snapshot_id TEXT NOT NULL,
 row_ordinal INTEGER NOT NULL CHECK(typeof(row_ordinal)='integer' AND row_ordinal>0),
 table_index INTEGER NOT NULL CHECK(typeof(table_index)='integer' AND table_index>=0),
 key_json TEXT NOT NULL CHECK(json_valid(key_json) AND json_type(key_json)='object'),
 row_version INTEGER NOT NULL CHECK(typeof(row_version)='integer' AND row_version>0),
 frame BLOB NOT NULL CHECK(typeof(frame)='blob' AND length(frame)>=8),
 logical_bytes INTEGER NOT NULL CHECK(typeof(logical_bytes)='integer' AND logical_bytes>=0),
 charge INTEGER NOT NULL CHECK(typeof(charge)='integer' AND charge>=0),
 PRIMARY KEY(snapshot_id,row_ordinal)
) WITHOUT ROWID""",
    "_sync_bootstrap_pages" to """CREATE TABLE IF NOT EXISTS _sync_bootstrap_pages (
 snapshot_id TEXT NOT NULL,
 page_ordinal INTEGER NOT NULL CHECK(typeof(page_ordinal)='integer' AND page_ordinal>0),
 first_ordinal INTEGER NOT NULL CHECK(typeof(first_ordinal)='integer' AND first_ordinal>0),
 last_ordinal INTEGER NOT NULL CHECK(typeof(last_ordinal)='integer' AND last_ordinal>=first_ordinal),
 row_count INTEGER NOT NULL CHECK(typeof(row_count)='integer' AND row_count=last_ordinal-first_ordinal+1),
 logical_bytes INTEGER NOT NULL CHECK(typeof(logical_bytes)='integer' AND logical_bytes>=0),
 wire_hash TEXT NOT NULL, typed_hash TEXT NOT NULL, page_proof TEXT NOT NULL, chain_root TEXT NOT NULL,
 charge INTEGER NOT NULL CHECK(typeof(charge)='integer' AND charge>=0),
 PRIMARY KEY(snapshot_id,page_ordinal), UNIQUE(snapshot_id,first_ordinal)
) WITHOUT ROWID""",
)

internal suspend fun initializeBootstrapRetainedOwners(db: SafeSQLiteConnection) {
    for (ddl in bootstrapRetainedTableDdl.values) db.execSQL(ddl)
    db.execSQL("INSERT INTO _sync_bootstrap_state(singleton_key,format_identity) VALUES(1,'$bootstrapRetainedFormat') ON CONFLICT(singleton_key) DO NOTHING")
}

/** Independent scalar audit; never repairs an operation's recorded usage. */
internal suspend fun measureBootstrapMetadataCharge(db: SafeSQLiteConnection, cache: StatementCache? = null): WorkspaceCharge =
    db.withPreparedStatement("""SELECT 1+(SELECT count(*) FROM _sync_bootstrap_tables),
        512+6*(length(CAST(snapshot_id AS BLOB))+length(CAST(descriptor_signature AS BLOB))+
            length(CAST(descriptor_json AS BLOB))+length(CAST(expected_manifest AS BLOB))+length(CAST(chain_root AS BLOB)))+
        COALESCE((SELECT sum(128+6*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+
            length(CAST(layout_signature AS BLOB))+length(CAST(expected_projection AS BLOB)))) FROM _sync_bootstrap_tables),0)
        FROM _sync_bootstrap_state WHERE singleton_key=1""", cache) { st ->
        require(st.step()) { "missing bootstrap metadata owner" }
        WorkspaceCharge(st.getLong(0), st.getLong(1))
    }

/** Inactive development routing cannot adopt unverified typed-store contents. */
internal suspend fun validateInactiveBootstrapOwners(db: SafeSQLiteConnection) {
    require(db.scalarLong("""SELECT count(*) FROM _sync_bootstrap_state WHERE singleton_key=1
 AND format_identity='typed-bootstrap-v1' AND phase='inactive' AND snapshot_id=''
 AND descriptor_signature='' AND descriptor_json='' AND expected_manifest='' AND chain_root=''""") == 1L) {
        "bootstrap retained contents require descriptor and stage admission"
    }
    require(db.scalarLong("SELECT count(*) FROM _sync_bootstrap_tables") == 0L &&
        db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire") == 0L &&
        db.scalarLong("SELECT count(*) FROM _sync_bootstrap_pages") == 0L &&
        bootstrapFamilyObjectCount(db) == 0L) {
        "inactive bootstrap descriptor has unowned retained contents"
    }
    require(OversqliteOperationStateStore(db).loadState().snapshotEngine == generalSnapshotEngine) {
        "typed snapshot engine requires admitted retained verification"
    }
    require(db.scalarLong("""SELECT count(*) FROM _sync_operation_state WHERE singleton_key=1
        AND bootstrap_page_count=0 AND bootstrap_cleanup_chain='' AND bootstrap_metadata_entries=1 AND bootstrap_metadata_bytes=512""") == 1L) {
        "inactive bootstrap quota owner disagrees with retained metadata"
    }
}

/** Row mode retains the existing trigger body. Bulk mode has a separate guard only. */
internal fun expectedBootstrapBaselineGuardSql(operation: String): String {
    require(operation in setOf("insert", "update", "delete"))
    val validation = if (operation != "insert")
        "SELECT RAISE(ABORT,'bootstrap baseline UPDATE/DELETE is forbidden');"
    else """SELECT RAISE(ABORT,'bootstrap baseline INSERT has no owned range or target context')
 WHERE NOT EXISTS(SELECT 1 FROM _sync_apply_state a
 JOIN _sync_operation_state o ON o.singleton_key=1
 JOIN _sync_bootstrap_state b ON b.singleton_key=1
 WHERE a.singleton_key=1 AND a.apply_mode=1 AND a.baseline_mode='bootstrap'
 AND a.bootstrap_operation!='' AND a.bootstrap_operation=o.staged_snapshot_id
 AND o.snapshot_engine='typed_bootstrap' AND b.snapshot_id=a.bootstrap_operation
 AND b.phase='sealed' AND b.expected_manifest!=''
 AND a.bootstrap_revision_low>0 AND a.bootstrap_revision_high>=a.bootstrap_revision_low
 AND NEW.server_revision BETWEEN a.bootstrap_revision_low AND a.bootstrap_revision_high
 AND NEW.server_kind='live' AND NEW.row_version>0 AND NEW.relationships_known=1
 AND NEW.server_context_id IS NOT NULL
 AND a.bootstrap_target_context>0 AND NEW.server_context_id=a.bootstrap_target_context);"""
    return "CREATE TRIGGER IF NOT EXISTS _sync_bootstrap_baseline_$operation BEFORE $operation ON _sync_row_state " +
        "WHEN (SELECT baseline_mode FROM _sync_apply_state WHERE singleton_key=1)='bootstrap' BEGIN $validation END"
}

internal val bootstrapBaselineGuardDdl = listOf("insert", "update", "delete").associate {
    "_sync_bootstrap_baseline_$it" to expectedBootstrapBaselineGuardSql(it)
}

private data class BootstrapCleanupPage(val first: Long, val last: Long, val rows: Long, val logical: Long,
    val wire: String, val typed: String, val proof: String, val chain: String, val charge: Long)

private suspend fun readBootstrapCleanupPage(db: SafeSQLiteConnection, snapshot: String, page: Long, cache: StatementCache): BootstrapCleanupPage =
    db.withPreparedStatement("""SELECT first_ordinal,last_ordinal,row_count,logical_bytes,wire_hash,typed_hash,page_proof,chain_root,charge
        FROM _sync_bootstrap_pages WHERE snapshot_id=? AND page_ordinal=?""", cache) {
        it.bindText(1, snapshot); it.bindLong(2, page); require(it.step())
        BootstrapCleanupPage(it.getLong(0), it.getLong(1), it.getLong(2), it.getLong(3), it.getText(4),
            it.getText(5), it.getText(6), it.getText(7), it.getLong(8))
    }

/** Scalar-only page cleanup visits represented tables; no payload or frame copies. */
private suspend fun deleteBootstrapPage(db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, snapshot: String, boundary: Long,
    record: BootstrapCleanupPage, page: Long, cache: StatementCache): WorkspaceCharge {
    val wireGroups = mutableListOf<Triple<Int, Long, Long>>()
    var wireBytes = 0L; var logical = 0L
    db.withPreparedStatement("""SELECT w.table_index,count(*),sum(w.charge),sum(w.logical_bytes),
        sum(64+6*(length(CAST(w.snapshot_id AS BLOB))+length(CAST(t.schema_name AS BLOB))+
          length(CAST(t.table_name AS BLOB))+length(CAST(w.key_json AS BLOB)))+length(w.frame)),
        sum(CASE WHEN t.slot IS NULL OR typeof(w.charge)!='integer' OR typeof(w.logical_bytes)!='integer'
          OR typeof(w.frame)!='blob' OR typeof(w.row_version)!='integer' OR typeof(w.key_json)!='text' THEN 1 ELSE 0 END)
        FROM _sync_bootstrap_wire w LEFT JOIN _sync_bootstrap_tables t ON t.table_index=w.table_index
        WHERE w.snapshot_id=? AND w.row_ordinal>=? AND w.row_ordinal<=? GROUP BY w.table_index""", cache) {
        it.bindText(1, snapshot); it.bindLong(2, record.first); it.bindLong(3, record.last)
        while (it.step()) {
            require(it.getLong(2) == it.getLong(4) && it.getLong(5) == 0L) { "bootstrap cleanup wire charge differs" }
            val index = it.getLong(0); require(index in 0..Int.MAX_VALUE.toLong())
            wireGroups += Triple(index.toInt(), it.getLong(1), it.getLong(2))
            wireBytes = bootstrapCheckedAdd(wireBytes, it.getLong(2)); logical = bootstrapCheckedAdd(logical, it.getLong(3))
        }
    }
    require(wireGroups.fold(0L) { n, group -> bootstrapCheckedAdd(n, group.second) } == record.rows &&
        record.rows == record.last - record.first + 1 && logical == record.logical &&
        record.charge == bootstrapPageCharge(snapshot).bytes) { "bootstrap cleanup page differs" }
    var typedBytes = 0L
    for ((index, count, _) in wireGroups) {
        currentCoroutineContext().ensureActive()
        val binding = descriptor.tables.single { it.tableIndex == index }
        val bytes = db.withPreparedStatement(binding.auditSql + " WHERE snapshot_id=? AND row_ordinal>=? AND row_ordinal<=?", cache) {
            it.bindText(1, snapshot); it.bindLong(2, boundary); it.bindLong(3, record.first - 1); it.bindLong(4, record.last)
            it.bindText(5, snapshot); it.bindLong(6, record.first); it.bindLong(7, record.last)
            require(it.step() && it.getLong(0) == count && it.getLong(1) == it.getLong(2) && it.getLong(6) == 0L) { "bootstrap cleanup typed charge differs" }
            it.getLong(1)
        }
        require(db.withPreparedStatement("""SELECT count(*) FROM ${quoteIdent(binding.stageName)} s
            LEFT JOIN _sync_bootstrap_wire w ON w.snapshot_id=s.snapshot_id AND w.row_ordinal=s.row_ordinal
            WHERE s.snapshot_id=? AND s.row_ordinal>=? AND s.row_ordinal<=?
            AND (w.table_index IS NULL OR w.table_index!=? OR w.key_json!=s.key_json COLLATE BINARY OR w.row_version!=s.row_version)""", cache) {
            it.bindText(1, snapshot); it.bindLong(2, record.first); it.bindLong(3, record.last); it.bindLong(4, index.toLong())
            require(it.step()); it.getLong(0)
        } == 0L) { "bootstrap cleanup typed membership differs" }
        db.withPreparedStatement("DELETE FROM ${quoteIdent(binding.stageName)} WHERE snapshot_id=? AND row_ordinal>=? AND row_ordinal<=?", cache) {
            it.bindText(1, snapshot); it.bindLong(2, record.first); it.bindLong(3, record.last); it.step()
        }
        typedBytes = bootstrapCheckedAdd(typedBytes, bytes)
    }
    db.withPreparedStatement("DELETE FROM _sync_bootstrap_wire WHERE snapshot_id=? AND row_ordinal>=? AND row_ordinal<=?", cache) {
        it.bindText(1, snapshot); it.bindLong(2, record.first); it.bindLong(3, record.last); it.step()
    }
    db.withPreparedStatement("DELETE FROM _sync_bootstrap_pages WHERE snapshot_id=? AND page_ordinal=?", cache) {
        it.bindText(1, snapshot); it.bindLong(2, page); it.step()
    }
    val charge = WorkspaceCharge(bootstrapCheckedAdd(record.rows, 1),
        bootstrapCheckedAdd(record.charge, bootstrapCheckedAdd(wireBytes, typedBytes)))
    db.withPreparedStatement("""UPDATE _sync_operation_state SET snapshot_stage_entries=snapshot_stage_entries-?,
        snapshot_stage_bytes=snapshot_stage_bytes-? WHERE singleton_key=1 AND snapshot_stage_entries>=? AND snapshot_stage_bytes>=?""", cache) {
        it.bindLong(1, charge.entries); it.bindLong(2, charge.bytes); it.bindLong(3, charge.entries); it.bindLong(4, charge.bytes); it.step()
    }
    require(db.scalarLong("SELECT changes()") == 1L)
    bootstrapWork("snapshot.bootstrap.cleanup_rows", record.rows)
    return charge
}

private suspend fun finishBootstrapCleanup(db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, cache: StatementCache,
    pending: OversqliteOperationState? = null) {
    require(db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state WHERE singleton_key=1") == 0L)
    val empty = descriptor.tables.map { BootstrapTableExpectation() }
    persistBootstrapExpectedOwners(db, descriptor, "", "", "inactive", "", empty, cache)
    val metadata = bootstrapMetadataCharge(descriptor, "", "", "", empty)
    db.withPreparedStatement("UPDATE _sync_operation_state SET bootstrap_metadata_entries=?,bootstrap_metadata_bytes=? WHERE singleton_key=1", cache) {
        it.bindLong(1, metadata.entries); it.bindLong(2, metadata.bytes); it.step()
    }
    val cleared = pending?.copy(stagedSnapshotId = "", snapshotBundleSeq = 0, snapshotRowCount = 0, snapshotByteCount = 0,
        snapshotStageComplete = false, snapshotStagedRowCount = 0, snapshotStagedByteCount = 0, snapshotStageGuard = "",
        snapshotTransfer = "", snapshotEngine = generalSnapshotEngine, bootstrapReference = "", bootstrapPageCount = 0,
        bootstrapCleanupOrdinal = 0, bootstrapCleanupChain = "") ?: OversqliteOperationState()
    OversqliteOperationStateStore(db).persistState(cleared, cache)
    requireEmptyBootstrapSnapshotOwners(db, descriptor, cache)
    require(measureBootstrapMetadataCharge(db, cache) == metadata)
}

/** A retiring transfer has no adoption route. Each step ends every SQL owner before returning. */
internal class BootstrapSnapshotRetirement private constructor(
    private val db: SafeSQLiteConnection, private val config: OversqliteConfig,
    private val descriptor: SnapshotDescriptor, private val generation: SyncGeneration,
    private val operations: OversqliteClientOperationController, private val scope: BootstrapReceiptScope,
) {
    private var active = true
    private val workspace = OversqliteWorkspace(db, config)

    suspend fun step(): Boolean {
        check(active && scope.active && currentCoroutineContext()[BootstrapReceiptScope] === scope && scope.db === db) {
            "bootstrap retirement owner expired or changed"
        }
        try {
            return operations.completeWorkPhase("typed snapshot retirement") {
                db.transaction(TransactionMode.IMMEDIATE) {
                    db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                        operations.reserveSyncWork("bootstrap-retirement", 512, readyAtomic = true)
                        val (operation, _) = readBootstrapOperation(db, workspace, cache)
                        val identity = BootstrapProjectionIdentity(operation.stagedSnapshotId, generation.id, generation.fingerprint,
                            operation.snapshotBundleSeq, descriptor.signature)
                        val expected = readBootstrapExpectedStage(db, descriptor, operation, identity, workspace, cache)
                        require(expected.phase == "retiring")
                        val manifest = BootstrapSnapshotManifest.read(expected.manifest, identity, operation, descriptor.tables.size)
                        require(manifest.chain == expected.chain && manifest.tables == expected.tables)
                        if (operation.bootstrapPageCount == 0L) {
                            require(operation.snapshotStagedRowCount == 0L && operation.snapshotStagedByteCount == 0L &&
                                operation.bootstrapCleanupChain == expected.chain)
                            finishBootstrapCleanup(db, descriptor, cache, operation)
                            true
                        } else {
                            val page = manifest.pageCount - operation.bootstrapPageCount + 1
                            val record = readBootstrapCleanupPage(db, operation.stagedSnapshotId, page, cache)
                            require(record.first == operation.bootstrapCleanupOrdinal + 1 && record.last <= manifest.receivedRows)
                            require(record.proof == bootstrapPageProof(identity, page, record.first, record.last, record.rows,
                                record.logical, record.wire, record.typed) &&
                                record.chain == bootstrapPageChainExtend(operation.bootstrapCleanupChain, record.proof)) { "bootstrap retirement frontier differs" }
                            operations.reserveSyncWork("bootstrap-retirement", snapshotWorkAdd(snapshotWorkScale(record.rows, 256),
                                snapshotWorkScale(record.logical, 16)), readyAtomic = true)
                            deleteBootstrapPage(db, descriptor, operation.stagedSnapshotId, operation.snapshotBundleSeq, record, page, cache)
                            val next = operation.copy(snapshotStagedRowCount = operation.snapshotStagedRowCount - record.rows,
                                snapshotStagedByteCount = operation.snapshotStagedByteCount - record.logical,
                                bootstrapPageCount = operation.bootstrapPageCount - 1, bootstrapCleanupOrdinal = record.last,
                                bootstrapCleanupChain = record.chain)
                            val bound = next.copy(bootstrapReference = bootstrapOperationReference(identity, next, "retiring",
                                expected.chain, expected.manifest, expected.tables))
                            OversqliteOperationStateStore(db).persistState(bound, cache)
                            if (next.bootstrapPageCount == 0L) {
                                require(next.snapshotStagedRowCount == 0L && next.snapshotStagedByteCount == 0L && record.chain == expected.chain)
                                finishBootstrapCleanup(db, descriptor, cache, operation)
                                true
                            } else false
                        }
                    }
                }
            }.also { if (it) active = false }
        } catch (failure: Throwable) { active = false; throw failure }
    }

    companion object {
        suspend fun <T> use(db: SafeSQLiteConnection, config: OversqliteConfig, descriptor: SnapshotDescriptor,
            generation: SyncGeneration, operations: OversqliteClientOperationController,
            block: suspend (BootstrapSnapshotRetirement) -> T): T {
            val workspace = OversqliteWorkspace(db, config)
            val fixed = workspace.add("bootstrap-retirement", descriptor.charge,
                WorkspaceCharge(64 + 16L * descriptor.tables.size, 32768 + 4096L * descriptor.tables.size))
            workspace.check("bootstrap-retirement", fixed)
            val scope = bootstrapReceiptScope(db, workspace, "bootstrap-retirement", fixed)
            val retirement = BootstrapSnapshotRetirement(db, config, descriptor, generation, operations, scope)
            try { return withContext(scope) {
                operations.completeWorkPhase("typed snapshot retirement admission") {
                    db.transaction(TransactionMode.IMMEDIATE) {
                        db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                            val (operation, _) = readBootstrapOperation(db, workspace, cache)
                            require(operation.snapshotEngine == typedSnapshotEngine)
                            verifyBootstrapDescriptorHeader(db, descriptor, cache)
                            verifyBootstrapFamilyObjects(db, descriptor, cache)
                            val transfer = kotlinx.serialization.json.Json.decodeFromString(RetainedSnapshotSeal.serializer(), operation.snapshotTransfer)
                            requireBootstrapTransfer(config, operation.snapshotTransfer, transfer.session, transfer.source)
                            val identity = BootstrapProjectionIdentity(operation.stagedSnapshotId, generation.id, generation.fingerprint,
                                operation.snapshotBundleSeq, descriptor.signature)
                            val expected = readBootstrapExpectedStage(db, descriptor, operation, identity, workspace, cache)
                            operations.reserveSyncWork("bootstrap-retirement", snapshotWorkAdd(512,
                                snapshotWorkAdd(snapshotWorkScale(operation.snapshotStagedRowCount, 512),
                                    snapshotWorkScale(operation.snapshotStagedByteCount, 16))), readyAtomic = true)
                            val writer = BootstrapProjectionHashWriter(scope, identity)
                            val observed = verifyBootstrapProjection(db, config, descriptor, operation, transfer.session, generation,
                                transfer.source, expected, cache, writer, scope)
                            if (expected.phase == "sealed") require(observed == BootstrapSnapshotManifest.read(expected.manifest,
                                identity, operation, descriptor.tables.size).projection)
                            if (expected.phase != "retiring") {
                                val next = operation.copy(snapshotStageComplete = false, bootstrapCleanupOrdinal = 0,
                                    bootstrapCleanupChain = bootstrapPageChainSeed(identity))
                                val manifest = if (expected.phase == "sealed") expected.manifest else BootstrapSnapshotManifest("",
                                    expected.chain, expected.tables, operation.bootstrapPageCount, operation.snapshotStagedRowCount,
                                    operation.snapshotStagedByteCount).canonical(identity, next)
                                val bound = next.copy(bootstrapReference = bootstrapOperationReference(identity, next, "retiring", expected.chain, manifest, expected.tables))
                                val oldCharge = bootstrapMetadataCharge(descriptor, identity.snapshotId, expected.chain, expected.manifest, expected.tables)
                                val newCharge = bootstrapMetadataCharge(descriptor, identity.snapshotId, expected.chain, manifest, expected.tables)
                                val base = workspace.readDurableBase("bootstrap-retirement", cache)
                                workspace.checkCapturedReadBase("bootstrap-retirement", base, WorkspaceCharge(bytes =
                                    bootstrapCheckedAdd(maxOf(0, newCharge.bytes - oldCharge.bytes), 6L * 64)))
                                persistBootstrapExpectedOwners(db, descriptor, identity.snapshotId, expected.chain, "retiring", manifest, expected.tables, cache, expected.tables)
                                OversqliteOperationStateStore(db).persistState(bound, cache)
                                updateBootstrapMetadataQuota(db, workspace, "bootstrap-retirement", base, WorkspaceCharge(), WorkspaceCharge(bytes = 6L * 64), oldCharge, newCharge, cache)
                            }
                        }
                    }
                }
                block(retirement)
            } } finally { retirement.active = false; scope.close() }
        }
    }
}


/**
 * Entries that staging a chunk of [rows] rows needs beside the chunk's transport tree: one staged
 * row and at most one page record per row, two receipt pages of the widest table, and the fixed
 * shape reserve. This is the reserve `bootstrapChunkRowsForQuota` leaves when it sizes a request.
 */
internal fun bootstrapChunkStagingEntries(rows: Int, maxColumns: Int): Long {
    require(rows >= 0 && maxColumns >= 0)
    return 2L * rows + 2 * (32 + 4L * maxColumns) + 256
}
