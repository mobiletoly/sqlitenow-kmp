package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json

/** Mutable control text is measured before any copied operation/seal strings. */
internal suspend fun readBootstrapOperation(
    db: SafeSQLiteConnection, workspace: OversqliteWorkspace, cache: StatementCache,
): Pair<OversqliteOperationState, WorkspaceCharge> {
    val bytes = db.withPreparedStatement("""SELECT
        length(CAST(kind AS BLOB))+length(CAST(target_user_id AS BLOB))+length(CAST(staged_snapshot_id AS BLOB))+
        length(CAST(snapshot_stage_guard AS BLOB))+length(CAST(snapshot_transfer AS BLOB))+
        length(CAST(snapshot_engine AS BLOB))+length(CAST(bootstrap_reference AS BLOB))+
        length(CAST(reason AS BLOB))+length(CAST(replacement_source_id AS BLOB))+length(CAST(bootstrap_cleanup_chain AS BLOB)),
        typeof(snapshot_bundle_seq)='integer' AND typeof(snapshot_row_count)='integer'
        AND typeof(snapshot_byte_count)='integer' AND typeof(snapshot_stage_complete)='integer'
        AND typeof(snapshot_staged_row_count)='integer' AND typeof(snapshot_staged_byte_count)='integer'
        AND typeof(bootstrap_page_count)='integer' AND typeof(snapshot_stage_entries)='integer'
        AND typeof(snapshot_stage_bytes)='integer' AND typeof(bootstrap_metadata_entries)='integer'
        AND typeof(bootstrap_metadata_bytes)='integer' AND typeof(snapshot_cleanup_ordinal)='integer'
        AND typeof(bootstrap_cleanup_chain)='text' AND typeof(kind)='text' AND typeof(target_user_id)='text'
        AND typeof(staged_snapshot_id)='text' AND typeof(snapshot_stage_guard)='text' AND typeof(snapshot_transfer)='text'
        AND typeof(snapshot_engine)='text' AND typeof(bootstrap_reference)='text' AND typeof(reason)='text'
        AND typeof(replacement_source_id)='text'
        FROM _sync_operation_state WHERE singleton_key=1""", cache) {
        require(it.step() && it.getLong(1) == 1L) { "bootstrap operation counter storage differs" }; it.getLong(0)
    }
    val charge = WorkspaceCharge(32, bootstrapCheckedAdd(8192, workspace.scale("bootstrap-control", bytes, 16)))
    workspace.check("bootstrap-control", charge, statementCache = cache)
    return OversqliteOperationStateStore(db).loadState() to charge
}

internal data class BootstrapExpectedStage(
    val phase: String, val chain: String, val manifest: String,
    val tables: List<BootstrapTableExpectation>,
)

internal suspend fun readBootstrapExpectedStage(
    db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, operation: OversqliteOperationState,
    identity: BootstrapProjectionIdentity, workspace: OversqliteWorkspace, cache: StatementCache,
): BootstrapExpectedStage {
    val bytes = db.withPreparedStatement("SELECT length(CAST(expected_manifest AS BLOB))+length(CAST(chain_root AS BLOB)),typeof(phase)='text' AND typeof(snapshot_id)='text' AND typeof(expected_manifest)='text' AND typeof(chain_root)='text' FROM _sync_bootstrap_state WHERE singleton_key=1", cache) {
        require(it.step() && it.getLong(1) == 1L) { "bootstrap expected owner storage differs" }; it.getLong(0)
    }
    workspace.check("bootstrap-proof", WorkspaceCharge(16 + 8L * descriptor.tables.size,
        bootstrapCheckedAdd(8192 + 1024L * descriptor.tables.size, workspace.scale("bootstrap-proof", bytes, 16))), statementCache = cache)
    val state = db.withPreparedStatement("SELECT phase,snapshot_id,expected_manifest,chain_root FROM _sync_bootstrap_state WHERE singleton_key=1", cache) {
        require(it.step()); require(it.getText(1) == identity.snapshotId)
        Triple(it.getText(0), it.getText(2), it.getText(3))
    }
    require(state.first in setOf("receiving", "sealed", "retiring") && isCanonicalSha256(state.third))
    require((state.first == "sealed") == operation.snapshotStageComplete &&
        (state.first != "receiving") == state.second.isNotEmpty() &&
        (state.first == "retiring") == operation.bootstrapCleanupChain.isNotEmpty() &&
        (state.first == "retiring" || operation.bootstrapCleanupOrdinal == 0L)) { "bootstrap completion owners disagree" }
    val tables = mutableListOf<BootstrapTableExpectation>()
    db.withPreparedStatement("SELECT slot,expected_row_count,expected_projection,length(CAST(expected_projection AS BLOB)),typeof(expected_row_count),typeof(expected_projection) FROM _sync_bootstrap_tables ORDER BY slot", cache) {
        for (slot in descriptor.tables.indices) {
            require(it.step() && it.getLong(0) == slot.toLong() && it.getLong(3) in setOf(0L, 64L) && it.getText(4) == "integer" && it.getText(5) == "text")
            tables += BootstrapTableExpectation(it.getLong(1), it.getText(2))
        }
        require(!it.step())
    }
    requireBootstrapReference(db, identity, operation, state.first, state.third, state.second, tables, cache)
    require(bootstrapMetadataCharge(descriptor, identity.snapshotId, state.third, state.second, tables) == measureBootstrapMetadataCharge(db, cache)) { "bootstrap expected metadata differs" }
    return BootstrapExpectedStage(state.first, state.third, state.second, tables)
}

internal data class BootstrapStageMeasure(
    val rows: Long, val logicalBytes: Long, val frames: Long, val bodies: Long,
    val maxFrame: Long, val maxKey: Long, val maxBody: Long, val charge: WorkspaceCharge,
    val tables: List<Long>,
)

/** A scalar audit borrowed only by fresh proofs in the same read-only final writer. */
internal class BootstrapStageAudit private constructor(
    val measured: BootstrapStageMeasure, private val db: SafeSQLiteConnection, private val writer: StatementCache,
    private val descriptor: SnapshotDescriptor, private val operation: OversqliteOperationState,
) {
    suspend fun requireCurrent(connection: SafeSQLiteConnection, cache: StatementCache,
        expectedDescriptor: SnapshotDescriptor, expectedOperation: OversqliteOperationState): BootstrapStageMeasure {
        check(connection === db && writer === cache && db.inTransaction() && descriptor === expectedDescriptor && operation == expectedOperation) {
            "bootstrap audit writer or descriptor changed"
        }
        cache.requireConnectionOwner(db)
        return measured
    }
    companion object {
        suspend fun read(db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, operation: OversqliteOperationState,
            cache: StatementCache): BootstrapStageAudit {
            check(db.inTransaction())
            cache.requireConnectionOwner(db)
            return BootstrapStageAudit(measureBootstrapStage(db, descriptor, operation, cache), db, cache, descriptor, operation)
        }
    }
}

/** Scalar storage/charge audit precedes every payload copy. No saved JSON is decoded. */
internal suspend fun measureBootstrapStage(
    db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, operation: OversqliteOperationState,
    cache: StatementCache,
): BootstrapStageMeasure {
    var typedRows = 0L; var typedBytes = 0L; var bodies = 0L; var maxBody = 0L; var maxKey = 0L
    val tables = descriptor.tables.map { binding ->
        currentCoroutineContext().ensureActive()
        db.withPreparedStatement(binding.auditSql, cache) {
            it.bindText(1, operation.stagedSnapshotId); it.bindLong(2, operation.snapshotBundleSeq)
            it.bindLong(3, operation.bootstrapCleanupOrdinal); it.bindLong(4, bootstrapCheckedAdd(operation.bootstrapCleanupOrdinal, operation.snapshotStagedRowCount))
            require(it.step() && it.getLong(1) == it.getLong(2) && it.getLong(6) == 0L) { "typed bootstrap storage or charge differs" }
            val rows = it.getLong(0)
            typedRows = bootstrapCheckedAdd(typedRows, rows); typedBytes = bootstrapCheckedAdd(typedBytes, it.getLong(1))
            bodies = bootstrapCheckedAdd(bodies, it.getLong(3)); maxBody = maxOf(maxBody, it.getLong(4)); maxKey = maxOf(maxKey, it.getLong(5))
            rows
        }
    }
    val wire = db.withPreparedStatement("""SELECT count(*),COALESCE(sum(w.logical_bytes),0),COALESCE(sum(length(w.frame)),0),
        COALESCE(max(length(w.frame)),0),COALESCE(max(length(CAST(w.key_json AS BLOB))),0),COALESCE(sum(w.charge),0),
        COALESCE(sum(64+6*(length(CAST(w.snapshot_id AS BLOB))+length(CAST(t.schema_name AS BLOB))+
          length(CAST(t.table_name AS BLOB))+length(CAST(w.key_json AS BLOB)))+length(w.frame)),0),
        COALESCE(sum(CASE WHEN t.slot IS NULL OR w.snapshot_id!=? COLLATE BINARY OR w.row_version>?
          OR typeof(w.frame)!='blob' OR length(w.frame)<8 OR typeof(w.row_ordinal)!='integer'
          OR typeof(w.row_version)!='integer' OR typeof(w.table_index)!='integer'
          OR typeof(w.logical_bytes)!='integer' OR w.logical_bytes<0 OR typeof(w.charge)!='integer'
          OR typeof(w.key_json)!='text' OR typeof(w.snapshot_id)!='text' THEN 1 ELSE 0 END),0),
        COALESCE(min(w.row_ordinal),0),COALESCE(max(w.row_ordinal),0)
        FROM _sync_bootstrap_wire w LEFT JOIN _sync_bootstrap_tables t ON t.table_index=w.table_index""", cache) {
        it.bindText(1, operation.stagedSnapshotId); it.bindLong(2, operation.snapshotBundleSeq)
        require(it.step() && it.getLong(5) == it.getLong(6) && it.getLong(7) == 0L) { "bootstrap wire storage or charge differs" }
        require(it.getLong(0) == operation.snapshotStagedRowCount && it.getLong(1) == operation.snapshotStagedByteCount &&
            it.getLong(0) == typedRows && it.getLong(8) == (if (typedRows == 0L) 0L else operation.bootstrapCleanupOrdinal + 1) &&
            it.getLong(9) == (if (typedRows == 0L) 0L else bootstrapCheckedAdd(operation.bootstrapCleanupOrdinal, typedRows))) { "bootstrap wire/typed progress differs" }
        LongArray(6) { n -> it.getLong(n) }
    }
    val pages = db.withPreparedStatement("""SELECT count(*),COALESCE(sum(charge),0),
        COALESCE(sum(2048+6*length(CAST(snapshot_id AS BLOB))),0),
        COALESCE(sum(CASE WHEN snapshot_id!=? COLLATE BINARY OR length(wire_hash)!=64 OR length(typed_hash)!=64
          OR length(page_proof)!=64 OR length(chain_root)!=64 OR typeof(wire_hash)!='text'
          OR typeof(typed_hash)!='text' OR typeof(page_proof)!='text' OR typeof(chain_root)!='text'
          OR typeof(page_ordinal)!='integer' OR typeof(first_ordinal)!='integer' OR typeof(last_ordinal)!='integer'
          OR typeof(row_count)!='integer' OR typeof(logical_bytes)!='integer' OR typeof(charge)!='integer'
          OR page_ordinal<=0 OR first_ordinal<=0 OR last_ordinal<first_ordinal OR logical_bytes<0
          THEN 1 ELSE 0 END),0)
        FROM _sync_bootstrap_pages""", cache) {
        it.bindText(1, operation.stagedSnapshotId); require(it.step())
        require(it.getLong(0) == operation.bootstrapPageCount && it.getLong(1) == it.getLong(2) && it.getLong(3) == 0L) { "bootstrap page quota differs" }
        WorkspaceCharge(it.getLong(0), it.getLong(1))
    }
    val charge = WorkspaceCharge(bootstrapCheckedAdd(bootstrapCheckedAdd(typedRows, wire[0]), pages.entries),
        bootstrapCheckedAdd(bootstrapCheckedAdd(typedBytes, wire[5]), pages.bytes))
    require(db.withPreparedStatement("SELECT snapshot_stage_entries,snapshot_stage_bytes FROM _sync_operation_state WHERE singleton_key=1", cache) {
        require(it.step()); WorkspaceCharge(it.getLong(0), it.getLong(1))
    } == charge) { "bootstrap operation quota differs" }
    return BootstrapStageMeasure(typedRows, wire[1], wire[2], bodies, wire[3], maxOf(maxKey, wire[4]), maxBody, charge, tables)
}

/** The measured simultaneous header/value copies obey the byte target too. */
internal fun bootstrapProjectionPageRows(config: OversqliteConfig, measured: BootstrapStageMeasure): Int {
    if (measured.rows == 0L) return 1
    val rowBytes = bootstrapCheckedAdd(measured.maxFrame, bootstrapCheckedAdd(measured.maxKey, measured.maxBody))
    val byteTarget = minOf(config.snapshotApplyBatchBytes, config.transferTargets.maxSnapshotChunkDecodedBytes)
    require(rowBytes > 0 && rowBytes <= byteTarget) { "bootstrap projection row exceeds admitted page target" }
    return minOf(config.snapshotApplyBatchRows.toLong(), config.transferTargets.maxSnapshotChunkRows.toLong(),
        measured.rows, byteTarget / rowBytes).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
}

private data class BootstrapProofRow(val ordinal: Long, val tableIndex: Int, val key: String, val version: Long,
    val frame: ByteArray, val logical: Long, var values: List<TypedValue>? = null)

/** Fresh ordinary wire pass, also used independently at the last-chunk boundary. */
internal suspend fun verifyBootstrapWire(
    db: SafeSQLiteConnection, config: OversqliteConfig, descriptor: SnapshotDescriptor, operation: OversqliteOperationState,
    session: SnapshotSession, generation: SyncGeneration, source: String, cache: StatementCache,
): BootstrapStageMeasure {
    val measured = measureBootstrapStage(db, descriptor, operation, cache)
    val hash = SnapshotHashWriter(source, generation, session.snapshotId, session.snapshotBundleSeq, session.recovery)
    val workspace = OversqliteWorkspace(db, config)
    SnapshotReadWorkspace.use(workspace, "bootstrap-wire-proof", cache) { read ->
        val rowCharge = WorkspaceCharge(8, bootstrapCheckedAdd(4096, workspace.scale("bootstrap-wire-proof", measured.maxFrame, 4)))
        workspace.check("bootstrap-wire-proof", rowCharge, statementCache = cache, read = read)
        var ordinal = 0L
        db.withPreparedStatement("SELECT row_ordinal,frame,logical_bytes FROM _sync_bootstrap_wire WHERE snapshot_id=? ORDER BY row_ordinal", cache) {
            it.bindText(1, session.snapshotId)
            while (it.step()) {
                currentCoroutineContext().ensureActive(); ordinal++
                require(it.getLong(0) == ordinal)
                hash.addPersistedFrame(it.getBlob(1), it.getLong(2))
                bootstrapWork("snapshot.bootstrap.wire_rows_verified")
            }
        }
        require(ordinal == measured.rows)
    }
    require(hash.finish(session.rowCount, session.logicalByteCount) == session.snapshotHash) { "bootstrap ordinary wire hash differs" }
    return measured
}

/**
 * One header/frame page, then one typed range per represented slot. Exact global
 * owner counts and exact range membership exclude orphan and duplicate typed rows.
 */
internal suspend fun verifyBootstrapProjection(
    db: SafeSQLiteConnection, config: OversqliteConfig, descriptor: SnapshotDescriptor, operation: OversqliteOperationState,
    session: SnapshotSession, generation: SyncGeneration, source: String, expected: BootstrapExpectedStage,
    cache: StatementCache, projection: BootstrapProjectionHashWriter, owner: Any,
    finish: Boolean = true, audit: BootstrapStageAudit? = null,
): String? = syncPhase("snapshot.bootstrap.projection_verification", operation.snapshotStagedRowCount, operation.snapshotStagedByteCount) {
    val identity = BootstrapProjectionIdentity(session.snapshotId, generation.id, generation.fingerprint, session.snapshotBundleSeq, descriptor.signature)
    val measured = audit?.requireCurrent(db, cache, descriptor, operation) ?: measureBootstrapStage(db, descriptor, operation, cache)
    val retiring = expected.phase == "retiring"
    val retirement = if (retiring) BootstrapSnapshotManifest.read(expected.manifest, identity, operation, descriptor.tables.size).also {
        require(it.chain == expected.chain && it.tables == expected.tables)
    } else null
    require(retiring || measured.tables == expected.tables.map { it.rows }) { "bootstrap table counts differ" }
    val ordinary = SnapshotHashWriter(source, generation, session.snapshotId, session.snapshotBundleSeq, session.recovery)
    val workspace = OversqliteWorkspace(db, config)
    var chain = if (retiring) operation.bootstrapCleanupChain else bootstrapPageChainSeed(identity)
    var ordinal = operation.bootstrapCleanupOrdinal; var totalLogical = 0L
    var tables = descriptor.tables.map { BootstrapTableExpectation() }
    val unit = "bootstrap-projection-proof"
    try {
        SnapshotReadWorkspace.use(workspace, unit, cache) { read ->
            val scalarBytes = bootstrapCheckedAdd(measured.maxFrame, bootstrapCheckedAdd(measured.maxKey, measured.maxBody))
            val perRow = WorkspaceCharge(32 + 4L * (descriptor.tables.maxOfOrNull { it.columns.size } ?: 0),
                bootstrapCheckedAdd(8192, workspace.scale(unit, scalarBytes, 16)))
            var pageRows = bootstrapProjectionPageRows(config, measured)
            while (measured.rows > 0) {
                val charge = WorkspaceCharge(workspace.scale(unit, perRow.entries, pageRows.toLong()), workspace.scale(unit, perRow.bytes, pageRows.toLong()))
                try { workspace.check(unit, charge, statementCache = cache, read = read); break }
                catch (capacity: SyncWorkspaceLimitException) {
                    if (capacity.suppressedExceptions.isNotEmpty() || pageRows == 1) throw capacity
                    pageRows = maxOf(1, pageRows / 2)
                }
            }
            val firstPage = (retirement?.pageCount ?: operation.bootstrapPageCount) - operation.bootstrapPageCount + 1
            for (page in firstPage..(retirement?.pageCount ?: operation.bootstrapPageCount)) {
                currentCoroutineContext().ensureActive()
                val record = db.withPreparedStatement("""SELECT first_ordinal,last_ordinal,row_count,logical_bytes,
                    wire_hash,typed_hash,page_proof,chain_root FROM _sync_bootstrap_pages WHERE snapshot_id=? AND page_ordinal=?""", cache) {
                    it.bindText(1, session.snapshotId); it.bindLong(2, page); require(it.step())
                    BootstrapPageRecord(it.getLong(0), it.getLong(1), it.getLong(2), it.getLong(3), it.getText(4), it.getText(5), it.getText(6), it.getText(7))
                }
                require(record.first == ordinal + 1 && record.last <= bootstrapCheckedAdd(operation.bootstrapCleanupOrdinal, measured.rows) && record.rows == record.last - record.first + 1)
                val pageProjection = BootstrapProjectionHashWriter(owner, identity, record.first..record.last)
                val pageWire = BootstrapWirePageHashWriter(owner, identity, record.first..record.last)
                var pageLogical = 0L
                val counts = LongArray(descriptor.tables.size)
                try {
                    while (ordinal < record.last) {
                        val last = minOf(record.last, snapshotWorkAdd(ordinal, pageRows.toLong()))
                        val rows = ArrayList<BootstrapProofRow>((last - ordinal).toInt())
                        db.withPreparedStatement("""SELECT row_ordinal,table_index,key_json,row_version,frame,logical_bytes
                            FROM _sync_bootstrap_wire WHERE snapshot_id=? AND row_ordinal>? AND row_ordinal<=? ORDER BY row_ordinal""", cache) {
                            it.bindText(1, session.snapshotId); it.bindLong(2, ordinal); it.bindLong(3, last)
                            for (n in 1..(last - ordinal)) {
                                require(it.step() && it.getLong(0) == ordinal + n)
                                val index = it.getLong(1); require(index in generation.tables.indices.first.toLong()..generation.tables.indices.last.toLong())
                                rows += BootstrapProofRow(it.getLong(0), index.toInt(), it.getText(2), it.getLong(3), it.getBlob(4), it.getLong(5))
                            }
                        }
                        for (binding in descriptor.tables) {
                            val members = rows.filter { it.tableIndex == binding.tableIndex }
                            if (members.isEmpty()) continue
                            db.withPreparedStatement(binding.scanSql, cache) {
                                it.bindText(1, session.snapshotId); it.bindLong(2, ordinal); it.bindLong(3, last)
                                for (member in members) {
                                    currentCoroutineContext().ensureActive()
                                    require(it.step() && it.getLong(0) == member.ordinal && it.getText(1) == member.key && it.getLong(2) == member.version) { "bootstrap header/typed membership differs" }
                                    member.values = SnapshotTypedValues.readProjection(binding.projectionInfo, it, 4, canonicalizeReals = false)
                                }
                            }
                        }
                        for (row in rows) {
                            currentCoroutineContext().ensureActive()
                            val values = requireNotNull(row.values)
                            val slot = descriptor.tables.single { it.tableIndex == row.tableIndex }.slot
                            counts[slot]++
                            if (!retiring) ordinary.addPersistedFrame(row.frame, row.logical)
                            pageWire.add(owner, row.ordinal, row.frame, row.logical)
                            pageProjection.add(owner, row.ordinal, row.tableIndex, row.version, row.key, values, row.logical)
                            if (!retiring) projection.add(owner, row.ordinal, row.tableIndex, row.version, row.key, values, row.logical)
                            pageLogical = bootstrapCheckedAdd(pageLogical, row.logical)
                            if (!retiring) bootstrapWork("snapshot.bootstrap.wire_rows_verified")
                            bootstrapWork("snapshot.bootstrap.projection_rows_verified")
                        }
                        ordinal = last
                    }
                    require(pageLogical == record.logical)
                    require(pageWire.finish(owner, record.rows, record.logical) == record.wire &&
                        pageProjection.finish(owner, record.rows, record.logical) == record.typed) { "bootstrap received page differs" }
                    val proof = bootstrapPageProof(identity, page, record.first, record.last, record.rows, record.logical, record.wire, record.typed)
                    chain = bootstrapPageChainExtend(chain, proof)
                    require(proof == record.proof && chain == record.chain) { "bootstrap page chain differs" }
                    tables = tables.mapIndexed { slot, old -> if (counts[slot] == 0L) old else old.append(identity,
                        descriptor.tables[slot], page, counts[slot], record.typed) }
                    totalLogical = bootstrapCheckedAdd(totalLogical, record.logical)
                } finally { pageProjection.close(); pageWire.close() }
            }
        }
        require(ordinal == bootstrapCheckedAdd(operation.bootstrapCleanupOrdinal, measured.rows) && totalLogical == measured.logicalBytes &&
            chain == expected.chain && (retiring || tables == expected.tables)) { "bootstrap received prefix differs" }
        if (retiring) { projection.close(); return@syncPhase null }
        // Partial resume has no complete remote hash yet; page hashes are its independent evidence.
        val wire = ordinary.finish(measured.rows, measured.logicalBytes)
        if (operation.snapshotStagedRowCount == session.rowCount && operation.snapshotStagedByteCount == session.logicalByteCount)
            require(wire == session.snapshotHash) { "bootstrap ordinary wire hash differs" }
        return@syncPhase if (finish) projection.finish(owner, measured.rows, measured.logicalBytes) else null
    } catch (failure: Throwable) { projection.close(); throw failure }
}

private data class BootstrapPageRecord(val first: Long, val last: Long, val rows: Long, val logical: Long,
    val wire: String, val typed: String, val proof: String, val chain: String)

// This identifies a reserved namespace for rejection. Ownership additionally
// requires every exact name, type, table owner and generated definition below.
internal const val bootstrapFamilyPredicate = "lower(name) GLOB '_sync_bootstrap_values_t*' OR lower(tbl_name) GLOB '_sync_bootstrap_values_t*'"

internal suspend fun bootstrapFamilyObjectCount(db: SafeSQLiteConnection): Long = db.scalarLong(
    "SELECT count(*) FROM (SELECT name,tbl_name FROM sqlite_schema UNION ALL SELECT name,tbl_name FROM sqlite_temp_schema) WHERE $bootstrapFamilyPredicate")

internal suspend fun verifyBootstrapFamilyObjects(db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, cache: StatementCache) {
    var expected = 0L
    suspend fun definition(name: String, type: String, owner: String, sql: String) {
        currentCoroutineContext().ensureActive()
        // Generated SQL is compared in SQLite, without retrieving another copy.
        val matches = db.withPreparedStatement("SELECT count(*) FROM sqlite_schema WHERE name=? COLLATE BINARY AND type=? AND tbl_name=? COLLATE BINARY AND sql=? COLLATE BINARY", cache) { st ->
            st.bindText(1, name); st.bindText(2, type); st.bindText(3, owner); st.bindText(4, sql)
            check(st.step()); st.getLong(0)
        }
        require(matches == 1L) { "incompatible retained bootstrap object; recreate this development database explicitly" }
        expected = bootstrapCheckedAdd(expected, 1)
    }
    for (binding in descriptor.tables) {
        definition(binding.stageName, "table", binding.stageName, binding.createSql)
        var index = 0
        for (unique in binding.metadata.uniqueKeys.indices)
            definition("${binding.stageName}_unique_$unique", "index", binding.stageName, binding.indexSql[index++])
        for (foreign in binding.metadata.foreignKeys.indices)
            definition("${binding.stageName}_foreign_$foreign", "index", binding.stageName, binding.indexSql[index++])
        check(index == binding.indexSql.size)
    }
    require(bootstrapFamilyObjectCount(db) == expected) { "unexpected retained bootstrap object" }
}

internal suspend fun verifyBootstrapDescriptorHeader(db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, cache: StatementCache) {
    require(db.withPreparedStatement("SELECT count(*) FROM _sync_bootstrap_state WHERE singleton_key=1 AND format_identity=? AND descriptor_signature=? AND descriptor_json=?", cache) { st ->
        st.bindText(1, bootstrapRetainedFormat); st.bindText(2, descriptor.signature); st.bindText(3, descriptor.canonical)
        check(st.step()); st.getLong(0)
    } == 1L) { "retained bootstrap descriptor differs from admitted schema" }
    require(db.scalarLong("SELECT count(*) FROM _sync_bootstrap_tables") == descriptor.tables.size.toLong()) { "retained bootstrap slot inventory differs" }
    for (binding in descriptor.tables) {
        require(db.withPreparedStatement("""SELECT count(*) FROM _sync_bootstrap_tables WHERE slot=? AND table_index=?
            AND schema_name=? AND table_name=? AND layout_signature=?""", cache) { st ->
            st.bindLong(1, binding.slot.toLong()); st.bindLong(2, binding.tableIndex.toLong())
            st.bindText(3, binding.schema); st.bindText(4, binding.table); st.bindText(5, descriptor.signature)
            check(st.step()); st.getLong(0)
        } == 1L) { "retained bootstrap slot mapping differs" }
    }
}

internal suspend fun requireEmptyBootstrapSnapshotOwners(db: SafeSQLiteConnection, descriptor: SnapshotDescriptor, cache: StatementCache) {
    require(db.scalarLong("""SELECT count(*) FROM _sync_bootstrap_state WHERE singleton_key=1 AND phase='inactive'
        AND snapshot_id='' AND expected_manifest='' AND chain_root=''""") == 1L) { "bootstrap snapshot integrity admission is required" }
    require(db.scalarLong("SELECT EXISTS(SELECT 1 FROM _sync_bootstrap_wire) OR EXISTS(SELECT 1 FROM _sync_bootstrap_pages) OR EXISTS(SELECT 1 FROM _sync_bootstrap_tables WHERE expected_row_count!=0 OR expected_projection!='')") == 0L) { "inactive bootstrap descriptor owns snapshot evidence" }
    for (binding in descriptor.tables) {
        require(db.withPreparedStatement("SELECT EXISTS(SELECT 1 FROM ${quoteIdent(binding.stageName)} LIMIT 1)", cache) {
            check(it.step()); it.getLong(0)
        } == 0L) { "inactive bootstrap descriptor owns typed rows" }
    }
}

/** No migration: only first creation or a supplied, verified empty compatible descriptor. */
internal suspend fun bindBootstrapDescriptor(
    db: SafeSQLiteConnection, admission: BootstrapSchemaAdmission.Eligible, metadata: ValidatedConfig,
    workspace: OversqliteWorkspace, cache: StatementCache, previous: SnapshotDescriptor? = null,
    independentHolder: WorkspaceCharge = WorkspaceCharge(),
) {
    cache.requireConnectionOwner(db)
    admission.pin.requireCurrent(db, metadata)
    val descriptor = admission.descriptor
    val unit = "bootstrap-layout"
    require(db.withPreparedStatement("SELECT count(*) FROM _sync_contract_identity WHERE singleton_key=1 AND generation_id=? AND fingerprint=?", cache) { st ->
        st.bindLong(1, descriptor.generationId.toLong()); st.bindText(2, descriptor.generationFingerprint); check(st.step()); st.getLong(0)
    } == 1L) { "bootstrap descriptor differs from installed authority" }
    val operation = OversqliteOperationStateStore(db).loadState()
    require(operation.snapshotEngine == generalSnapshotEngine && operation.stagedSnapshotId.isEmpty() && operation.bootstrapPageCount == 0L) { "bootstrap descriptor has an active operation" }
    require(db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state") == 0L &&
        db.scalarLong("SELECT EXISTS(SELECT 1 FROM _sync_snapshot_stage)") == 0L) { "bootstrap descriptor cannot replace staged input" }
    val recorded = db.withPreparedStatement("SELECT bootstrap_metadata_entries,bootstrap_metadata_bytes FROM _sync_operation_state", cache) {
        check(it.step()); WorkspaceCharge(it.getLong(0), it.getLong(1))
    }
    require(recorded == measureBootstrapMetadataCharge(db, cache)) { "bootstrap metadata quota differs" }
    val fresh = db.scalarLong("SELECT descriptor_signature='' FROM _sync_bootstrap_state WHERE singleton_key=1") == 1L
    if (fresh) {
        validateInactiveBootstrapOwners(db)
        require(bootstrapFamilyObjectCount(db) == 0L) { "bootstrap stage-name collision" }
    } else {
        val old = previous ?: descriptor
        verifyBootstrapDescriptorHeader(db, old, cache)
        verifyBootstrapFamilyObjects(db, old, cache)
        requireEmptyBootstrapSnapshotOwners(db, old, cache)
        // A compatible rebind changes metadata; it never rebuilds physical tables.
        verifyBootstrapFamilyObjects(db, descriptor, cache)
        if (old.signature == descriptor.signature) return
    }
    var next = WorkspaceCharge(1, bootstrapCheckedAdd(512, workspace.scale(unit,
        bootstrapCheckedAdd(utf8Size(descriptor.signature), descriptor.descriptorBytes), 6)))
    for (binding in descriptor.tables) next = workspace.add(unit, next, WorkspaceCharge(1,
        bootstrapCheckedAdd(128, workspace.scale(unit, bootstrapCheckedAdd(bootstrapCheckedAdd(
            utf8Size(binding.schema), utf8Size(binding.table)), utf8Size(descriptor.signature)), 6))))
    val holders = workspace.add(unit, independentHolder, workspace.add(unit, descriptor.charge,
        if (previous != null && previous !== descriptor) previous.charge else WorkspaceCharge()))
    val extra = workspace.add(unit, holders, WorkspaceCharge(maxOf(0, next.entries - recorded.entries), maxOf(0, next.bytes - recorded.bytes)))
    workspace.check(unit, workspace.add(unit, extra, WorkspaceCharge(16 + 8L * descriptor.tables.size, 4096)), statementCache = cache)
    if (fresh) for (binding in descriptor.tables) {
        db.execSQL(binding.createSql)
        for (sql in binding.indexSql) db.execSQL(sql)
    }
    verifyBootstrapFamilyObjects(db, descriptor, cache)
    db.withPreparedStatement("""UPDATE _sync_bootstrap_state SET descriptor_signature=?,descriptor_json=?
        WHERE singleton_key=1 AND phase='inactive' AND snapshot_id='' AND expected_manifest='' AND chain_root=''
        AND descriptor_signature=? AND descriptor_json=?""", cache) { st ->
        st.bindText(1, descriptor.signature); st.bindText(2, descriptor.canonical)
        st.bindText(3, if (fresh) "" else checkNotNull(previous).signature)
        st.bindText(4, if (fresh) "" else checkNotNull(previous).canonical); st.step()
    }
    require(db.scalarLong("SELECT changes()") == 1L) { "bootstrap descriptor owner changed" }
    db.execSQL("DELETE FROM _sync_bootstrap_tables")
    for (binding in descriptor.tables) db.withPreparedStatement("INSERT INTO _sync_bootstrap_tables(slot,table_index,schema_name,table_name,layout_signature) VALUES(?,?,?,?,?)", cache) { st ->
        st.bindLong(1, binding.slot.toLong()); st.bindLong(2, binding.tableIndex.toLong()); st.bindText(3, binding.schema)
        st.bindText(4, binding.table); st.bindText(5, descriptor.signature); st.step()
    }
    db.withPreparedStatement("""UPDATE _sync_operation_state SET bootstrap_metadata_entries=?,bootstrap_metadata_bytes=?
        WHERE singleton_key=1 AND snapshot_engine='general_delta' AND staged_snapshot_id=''
        AND bootstrap_metadata_entries=? AND bootstrap_metadata_bytes=?""", cache) { st ->
        st.bindLong(1, next.entries); st.bindLong(2, next.bytes); st.bindLong(3, recorded.entries); st.bindLong(4, recorded.bytes); st.step()
    }
    require(db.scalarLong("SELECT changes()") == 1L && measureBootstrapMetadataCharge(db, cache) == next) { "bootstrap quota owner changed" }
    verifyBootstrapDescriptorHeader(db, descriptor, cache)
}

/** Configured reopen independently verifies every durable receipt owner before adoption. */
internal suspend fun validateBootstrapRetainedLayout(
    db: SafeSQLiteConnection, config: OversqliteConfig?, proof: RetainedCaptureProof,
    abandonedReservations: Boolean = false,
) {
    if (db.scalarLong("SELECT descriptor_signature='' FROM _sync_bootstrap_state WHERE singleton_key=1") == 1L) {
        validateInactiveBootstrapOwners(db)
        return
    }
    val configured = requireNotNull(config) { "retained bootstrap descriptor requires configured admission" }
    require(bootstrapFamilyObjectCount(db) > 0) { "retained bootstrap family is missing" }
    db.transaction(TransactionMode.DEFERRED) {
        val workspace = OversqliteWorkspace(db, configured, abandonedReservations)
        db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
            val measured = measureBootstrapMetadataCharge(db, cache)
            require(db.withPreparedStatement("SELECT bootstrap_metadata_entries,bootstrap_metadata_bytes FROM _sync_operation_state", cache) {
                check(it.step()); WorkspaceCharge(it.getLong(0), it.getLong(1))
            } == measured) { "retained bootstrap metadata quota differs" }
            val (generationId, bytes) = db.withPreparedStatement("""SELECT i.generation_id,length(CAST(g.descriptor AS BLOB)) FROM
                _sync_contract_identity i JOIN _sync_admitted_contracts g ON g.generation_id=i.generation_id WHERE i.singleton_key=1""", cache) {
                require(it.step()); it.getLong(0) to it.getLong(1)
            }
            val unit = "bootstrap-opening"
            val control = WorkspaceCharge(16 + bytes, bootstrapCheckedAdd(4096, workspace.scale(unit, bytes, 16)))
            workspace.check(unit, control, statementCache = cache)
            val generation = OversqliteContractStore(db).readGeneration(generationId)
            val installed = configured.copy(generationId = generationId,
                syncTables = generation.tables.map { SyncTable(it.table, it.syncKeyColumns.single()) })
            val physical = SyncRuntimeInitializer(installed, proof.metadata).validateConfig(db).withGeneration(generation)
            val descriptor = bootstrapDescriptorForAudit(db, physical, workspace, unit, cache, proof, control)
            verifyBootstrapDescriptorHeader(db, descriptor, cache)
            verifyBootstrapFamilyObjects(db, descriptor, cache)
            val (operation, operationCharge) = readBootstrapOperation(db, workspace, cache)
            if (operation.snapshotEngine == generalSnapshotEngine) {
                requireEmptyBootstrapSnapshotOwners(db, descriptor, cache)
            } else {
                require(db.scalarLong("SELECT EXISTS(SELECT 1 FROM _sync_snapshot_stage)") == 0L) { "typed bootstrap cannot own general staged payloads" }
                val fixed = workspace.add(unit, workspace.add(unit, control, descriptor.charge),
                    workspace.add(unit, operationCharge, WorkspaceCharge(32 + 16L * descriptor.tables.size, 32768 + 4096L * descriptor.tables.size)))
                workspace.check(unit, fixed, statementCache = cache)
                val receipt = bootstrapReceiptScope(db, workspace, unit, fixed)
                try { withContext(receipt) {
                    val transfer = Json.decodeFromString(RetainedSnapshotSeal.serializer(), operation.snapshotTransfer)
                    validateSnapshotSession(transfer.session)
                    require(transfer.authority == configured.serviceId && transfer.source.isNotEmpty() &&
                        operation.stagedSnapshotId == transfer.session.snapshotId && operation.snapshotBundleSeq == transfer.session.snapshotBundleSeq &&
                        operation.snapshotRowCount == transfer.session.rowCount && operation.snapshotByteCount == transfer.session.logicalByteCount &&
                        isCanonicalSha256(transfer.session.snapshotHash)) { "bootstrap retained transfer differs" }
                    val identity = BootstrapProjectionIdentity(transfer.session.snapshotId, generation.id, generation.fingerprint,
                        transfer.session.snapshotBundleSeq, descriptor.signature)
                    val expected = readBootstrapExpectedStage(db, descriptor, operation, identity, workspace, cache)
                    val writer = BootstrapProjectionHashWriter(receipt, identity)
                    val projection = verifyBootstrapProjection(db, configured, descriptor, operation, transfer.session, generation,
                        transfer.source, expected, cache, writer, receipt)
                    if (operation.snapshotStageComplete) {
                        val manifest = BootstrapSnapshotManifest.read(expected.manifest, identity, operation, descriptor.tables.size)
                        require(projection == manifest.projection && manifest.chain == expected.chain && manifest.tables == expected.tables) { "bootstrap retained sealed proof differs" }
                    }
                } } finally { receipt.close() }
            }
            workspace.check(unit, workspace.add(unit, control, descriptor.charge), statementCache = cache)
        }
    }
}

/** Current-generation admission owns provisioning; active transfers never change format. */
internal suspend fun prepareBootstrapDescriptorForAdmission(
    db: SafeSQLiteConnection, config: OversqliteConfig, metadata: ValidatedConfig, metadataVersion: Long,
    cache: StatementCache, previous: SnapshotDescriptor? = null,
): BootstrapSchemaAdmission? {
    cache.requireConnectionOwner(db)
    val workspace = OversqliteWorkspace(db, config)
    val (operation, control) = readBootstrapOperation(db, workspace, cache)
    if (operation.snapshotEngine == generalSnapshotEngine && operation.stagedSnapshotId.isNotEmpty()) {
        require(previous == null) { "pending snapshot must settle before generation change" }
        return null
    }
    val holder = workspace.add("bootstrap-admission", control, previous?.charge ?: WorkspaceCharge())
    val admission = admitBootstrapSchema(db, metadata, metadataVersion, workspace, "bootstrap-admission", cache, holder)
    when (admission) {
        is BootstrapSchemaAdmission.Unavailable -> require(previous == null && operation.snapshotEngine == generalSnapshotEngine) {
            "bootstrap physical family became unsupported; recreate this development database explicitly"
        }
        is BootstrapSchemaAdmission.Eligible -> {
            if (operation.snapshotEngine == typedSnapshotEngine) {
                require(previous == null) { "active bootstrap prevents generation rebind" }
                verifyBootstrapDescriptorHeader(db, admission.descriptor, cache)
                verifyBootstrapFamilyObjects(db, admission.descriptor, cache)
            } else if (operation.stagedSnapshotId.isEmpty()) {
                bindBootstrapDescriptor(db, admission, metadata, workspace, cache, previous, control)
            } else require(previous == null) { "staged general snapshot prevents generation rebind" }
        }
    }
    return admission
}

/** Capture the old empty family before the application's coordinated migration runs. */
internal suspend fun bootstrapDescriptorBeforeGenerationChange(
    db: SafeSQLiteConnection, config: OversqliteConfig, generation: SyncGeneration, cache: StatementCache,
): SnapshotDescriptor? {
    if (db.scalarLong("SELECT descriptor_signature='' FROM _sync_bootstrap_state WHERE singleton_key=1") == 1L) return null
    require(db.scalarLong("SELECT snapshot_engine='general_delta' AND staged_snapshot_id='' FROM _sync_operation_state WHERE singleton_key=1") == 1L) {
        "pending snapshot must settle before generation change"
    }
    val oldConfig = config.copy(generationId = generation.id.toLong(),
        syncTables = generation.tables.map { SyncTable(it.table, it.syncKeyColumns.single()) })
    val version = db.scalarLong("PRAGMA main.schema_version")
    val metadata = SyncRuntimeInitializer(oldConfig, TableInfoCache()).validateConfig(db).withGeneration(generation)
    val admission = prepareBootstrapDescriptorForAdmission(db, oldConfig, metadata, version, cache)
    require(admission is BootstrapSchemaAdmission.Eligible) { "bound bootstrap family is incompatible; recreate this development database explicitly" }
    requireEmptyBootstrapSnapshotOwners(db, admission.descriptor, cache)
    return admission.descriptor
}
