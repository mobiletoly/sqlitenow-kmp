package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

internal const val provenanceFormatIdentity = "complete-context-v1"

internal val provenanceContextDdl = """CREATE TABLE IF NOT EXISTS _sync_provenance_contexts (
 context_id INTEGER PRIMARY KEY AUTOINCREMENT,
 database_id TEXT NOT NULL CHECK(database_id<>''), service_id TEXT NOT NULL CHECK(service_id<>''),
 account_binding TEXT NOT NULL CHECK(account_binding IN ('anonymous','attached')), account_id TEXT NOT NULL,
 generation_id INTEGER NOT NULL CHECK(typeof(generation_id)='integer' AND generation_id>0),
 generation_fingerprint TEXT NOT NULL, admission TEXT NOT NULL CHECK(admission IN ('unadmitted','admitted')),
 local_version INTEGER NOT NULL CHECK(typeof(local_version)='integer' AND local_version>=0),
 physical_shape TEXT NOT NULL CHECK(physical_shape<>''), origin_source_id TEXT NOT NULL,
 CHECK((account_binding='anonymous' AND account_id='') OR (account_binding='attached' AND account_id<>'')),
 CHECK((admission='unadmitted' AND generation_fingerprint='') OR (admission='admitted' AND length(generation_fingerprint)=64)),
 UNIQUE(database_id,service_id,account_binding,account_id,generation_id,generation_fingerprint,admission,local_version,physical_shape,origin_source_id)
)""".trimIndent()

internal val provenanceStateDdl = """CREATE TABLE IF NOT EXISTS _sync_provenance_state (
 singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1),
 format_identity TEXT NOT NULL CHECK(format_identity='complete-context-v1'),
 active_context_id INTEGER REFERENCES _sync_provenance_contexts(context_id) ON DELETE RESTRICT,
 cleanup_cursor INTEGER NOT NULL DEFAULT 0 CHECK(typeof(cleanup_cursor)='integer' AND cleanup_cursor>=0)
)""".trimIndent()

internal val provenanceImmutableTriggerDdl = """CREATE TRIGGER IF NOT EXISTS _sync_provenance_context_immutable BEFORE UPDATE ON _sync_provenance_contexts BEGIN SELECT RAISE(ABORT,'SYNC_CONTEXT_IMMUTABLE'); END""".trimIndent()

internal fun provenanceObservationColumns(column: String, reference: String): String = """$column TEXT NOT NULL CHECK(COALESCE(json_valid($column) AND json_type($column)='object'
 AND json_type($column,'$.context_id')='integer' AND json_extract($column,'$.context_id')>0
 AND json_type($column,'$.origin')='text' AND json_extract($column,'$.origin') IN ('never_published','snapshot','pull','settlement','explicit_conflict')
 AND json_type($column,'$.bundle_sequence') IN ('integer','null')
 AND (json_type($column,'$.bundle_sequence')='null' OR json_extract($column,'$.bundle_sequence')>=0)
 AND json_remove($column,'$.context_id','$.origin','$.bundle_sequence')='{}',0)),
 ${reference} INTEGER GENERATED ALWAYS AS(json_extract($column,'$.context_id')) STORED NOT NULL REFERENCES _sync_provenance_contexts(context_id) ON DELETE RESTRICT""".trimIndent()

private val contextColumns = listOf("database_id", "service_id", "account_binding", "account_id", "generation_id",
    "generation_fingerprint", "admission", "local_version", "physical_shape", "origin_source_id")

/** Primary immutable data; complete contexts are never decoded into host row models. */
internal class OversqliteProvenanceContextStore(private val db: SafeSQLiteConnection) {
    suspend fun activeId(cache: StatementCache? = null): Long {
        val memo = currentCoroutineContext()[ProvenanceReadScope]?.takeIf { it.db === db }
        memo?.activeContext?.let { return it }
        val id = db.withPreparedStatement("SELECT active_context_id FROM _sync_provenance_state WHERE singleton_key=1", cache) { st ->
            check(st.step() && !st.isNull(0)) { "missing active provenance context" }; st.getLong(0)
        }
        memo?.activeContext = id
        return id
    }

    suspend fun target(state: RuntimeState, source: String): Long = db.transaction(TransactionMode.IMMEDIATE) {
        requireValidOversqliteSourceId(source)
        require(state.userId.isNotEmpty())
        db.withPreparedStatement("SELECT generation_id,fingerprint FROM _sync_contract_identity WHERE singleton_key=1") {
            check(it.step() && it.getLong(0).toString() == state.admittedContract.generation.id &&
                it.getText(1) == state.admittedContract.generation.fingerprint) { "snapshot target admission changed" }
        }
        intern("'attached'", "?", "?", listOf(state.userId, source))
    }

    // SQL owns its primary strings. No host physical-shape copies or JSON context parsing.
    private suspend fun intern(binding: String, account: String, source: String, values: List<String>): Long {
        val projection = "i.database_id,i.service_id,$binding,$account,i.generation_id,i.fingerprint," +
            "CASE WHEN i.fingerprint='' THEN 'unadmitted' ELSE 'admitted' END,i.local_version,i.physical_shape,$source"
        val from = " FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a WHERE i.singleton_key=1 AND a.singleton_key=1"
        suspend fun existing(): Long? = db.withPreparedStatement("SELECT c.context_id FROM _sync_provenance_contexts c WHERE (" +
            contextColumns.joinToString { "c.$it" } + ")=(SELECT $projection$from)") { st ->
            values.forEachIndexed { index, value -> st.bindText(index + 1, value) }
            if (st.step()) st.getLong(0) else null
        }
        // Look up first: an ignored INSERT OR IGNORE still consumes an AUTOINCREMENT value, so an
        // unconditional insert would make every ready reopen write sqlite_sequence.
        existing()?.let { return it }
        db.withPreparedStatement("INSERT INTO _sync_provenance_contexts(${contextColumns.joinToString()}) SELECT $projection$from") { st ->
            values.forEachIndexed { index, value -> st.bindText(index + 1, value) }; st.step()
        }
        return checkNotNull(existing()) { "complete provenance context is missing" }
    }

    suspend fun refreshActiveAndBind() = db.transaction(TransactionMode.IMMEDIATE) {
        val active = intern("a.binding_state", "a.attached_user_id", "a.current_source_id", emptyList())
        db.withPreparedStatement("UPDATE _sync_provenance_state SET active_context_id=? WHERE singleton_key=1") { it.bindLong(1, active); it.step() }
        currentCoroutineContext()[ProvenanceReadScope]?.takeIf { it.db === db }?.activeChanged(active)
        // Only A and dirty B participate in these established binding transitions. Frozen B is
        // independent, and each promoted identity retains its original observation source.
        val fields = contextColumns.map { name -> when (name) {
            "account_binding" -> "CASE WHEN c.account_binding='anonymous' AND a.binding_state='attached' THEN a.binding_state ELSE c.account_binding END"
            "account_id" -> "CASE WHEN c.account_binding='anonymous' AND a.binding_state='attached' THEN a.attached_user_id ELSE c.account_id END"
            "generation_fingerprint" -> "CASE WHEN c.admission='unadmitted' AND i.fingerprint<>'' THEN i.fingerprint ELSE c.generation_fingerprint END"
            "admission" -> "CASE WHEN c.admission='unadmitted' AND i.fingerprint<>'' THEN 'admitted' ELSE c.admission END"
            else -> "c.$name"
        } }
        val from = " FROM _sync_provenance_contexts c CROSS JOIN _sync_contract_identity i CROSS JOIN _sync_attachment_state a"
        val source = "$from WHERE c.context_id=? AND i.singleton_key=1 AND a.singleton_key=1"
        val candidate = "SELECT c.context_id$from WHERE c.context_id>? AND c.context_id<=? " +
            "AND i.singleton_key=1 AND a.singleton_key=1 " +
            "AND ((c.account_binding='anonymous' AND a.binding_state='attached') OR (c.admission='unadmitted' AND i.fingerprint<>'')) " +
            "AND (EXISTS(SELECT 1 FROM _sync_row_state r WHERE r.server_context_id=c.context_id) " +
            "OR EXISTS(SELECT 1 FROM _sync_dirty_rows d WHERE d.base_context_id=c.context_id)) ORDER BY c.context_id LIMIT 1"
        // A self-table INSERT SELECT spools complete contexts. Scalar VALUES subqueries keep
        // only the primary write's fixed registers, with no derived context/ID relation.
        val insert = "INSERT OR IGNORE INTO _sync_provenance_contexts(${contextColumns.joinToString()}) VALUES(" +
            fields.joinToString { "(SELECT $it$source)" } + ")"
        val mapped = "SELECT n.context_id FROM _sync_provenance_contexts n WHERE (" +
            contextColumns.joinToString { "n.$it" } + ")=(SELECT ${fields.joinToString()}$source)"
        db.withStatementCache { cache ->
            val highWater = db.withPreparedStatement("SELECT COALESCE(max(context_id),0) FROM _sync_provenance_contexts", cache) { st ->
                check(st.step()); st.getLong(0)
            }
            var after = 0L
            while (true) {
                currentCoroutineContext().ensureActive()
                val captured = db.withPreparedStatement(candidate, cache) { st ->
                    st.bindLong(1, after); st.bindLong(2, highWater)
                    if (st.step()) st.getLong(0) else null
                } ?: break
                db.withPreparedStatement(insert, cache) { st ->
                    contextColumns.indices.forEach { st.bindLong(it + 1, captured) }; st.step()
                }
                val promoted = db.withPreparedStatement(mapped, cache) { st ->
                    st.bindLong(1, captured); check(st.step()) { "promoted context is missing" }; st.getLong(0)
                }
                for ((table, column, reference) in listOf(Triple("_sync_row_state", "server_provenance", "server_context_id"),
                    Triple("_sync_dirty_rows", "base_provenance", "base_context_id"))) {
                    var rowAfter: Long? = null
                    while (true) {
                        currentCoroutineContext().ensureActive()
                        val row = db.withPreparedStatement("SELECT rowid FROM $table WHERE $reference=?" +
                            (if (rowAfter == null) "" else " AND rowid>?") + " ORDER BY rowid LIMIT 1", cache) { st ->
                            st.bindLong(1, captured); rowAfter?.let { st.bindLong(2, it) }
                            if (st.step()) st.getLong(0) else null
                        } ?: break
                        // Updating a scanned reference index can otherwise spool every owner
                        // rowid. A primary-row write keeps that derived work bounded to one.
                        db.withPreparedStatement("UPDATE $table SET $column=json_set($column,'$.context_id',?) WHERE rowid=? AND $reference=?", cache) { st ->
                            st.bindLong(1, promoted); st.bindLong(2, row); st.bindLong(3, captured); st.step()
                        }
                        rowAfter = row
                    }
                }
                after = captured
            }
        }
        // Three bounded slices settle the small transition set without a whole-history scan.
        repeat(3) { cleanup() }
    }

    /** At most one complete-context eligibility probe per memoized identity/active binding. */
    suspend fun validateHistorical(context: Long, requireSource: Boolean) {
        val active = activeId()
        val memo = currentCoroutineContext()[ProvenanceReadScope]?.takeIf { it.db === db }
        if (memo?.contains(context, active, requireSource) == true) return
        // A same-shape local-only upgrade keeps captured contexts immutable. Its
        // earlier representation version remains eligible under the exact current
        // generation/shape/account proof; a future version is never historical.
        val valid = db.withPreparedStatement("""SELECT c.database_id=i.database_id AND c.service_id=i.service_id
            AND c.account_binding=a.binding_state AND c.account_id=a.attached_user_id
            AND c.generation_id=i.generation_id AND c.generation_fingerprint=i.fingerprint
            AND c.admission=CASE WHEN i.fingerprint='' THEN 'unadmitted' ELSE 'admitted' END
            AND c.local_version<=i.local_version AND c.physical_shape=i.physical_shape,
            length(c.origin_source_id)>0
            FROM _sync_provenance_contexts c CROSS JOIN _sync_contract_identity i CROSS JOIN _sync_attachment_state a
            WHERE c.context_id=? AND i.singleton_key=1 AND a.singleton_key=1""") { st ->
            st.bindLong(1, context); st.step() && st.getLong(0) == 1L && (!requireSource || st.getLong(1) == 1L)
        }
        if (!valid) throw ResolutionBaseUnavailableException()
        memo?.remember(context, active, requireSource)
    }

    suspend fun validateRetained() {
        require(db.scalarLong("SELECT count(*) FROM _sync_provenance_state WHERE singleton_key=1 AND format_identity='complete-context-v1' AND active_context_id IS NOT NULL") == 1L) { "incompatible provenance format; recreate this development database explicitly" }
        require(db.scalarLong("""SELECT count(*) FROM _sync_provenance_state p
            JOIN _sync_provenance_contexts c ON c.context_id=p.active_context_id
            CROSS JOIN _sync_contract_identity i CROSS JOIN _sync_attachment_state a
            WHERE p.singleton_key=1 AND i.singleton_key=1 AND a.singleton_key=1
            AND c.database_id=i.database_id AND c.service_id=i.service_id
            AND c.generation_id=i.generation_id AND c.generation_fingerprint=i.fingerprint
            AND c.local_version=i.local_version AND c.physical_shape=i.physical_shape
            AND c.account_binding=a.binding_state AND c.account_id=a.attached_user_id
            AND c.origin_source_id=a.current_source_id
            AND c.admission=CASE WHEN i.fingerprint='' THEN 'unadmitted' ELSE 'admitted' END""") == 1L) { "active provenance context differs from retained authority" }
        for ((table, reference) in listOf("_sync_row_state" to "server_context_id", "_sync_dirty_rows" to "base_context_id", "_sync_outbox_rows" to "base_context_id", "_sync_provenance_state" to "active_context_id")) {
            require(db.scalarLong("SELECT EXISTS(SELECT 1 FROM $table r LEFT JOIN _sync_provenance_contexts c ON c.context_id=r.$reference WHERE c.context_id IS NULL)") == 0L) { "dangling retained provenance context" }
        }
    }

    suspend fun cleanup(maxCandidates: Int = 32): ProvenanceCleanupProgress = db.transaction(TransactionMode.IMMEDIATE) {
        require(maxCandidates in 1..32)
        val cursor = db.scalarLong("SELECT cleanup_cursor FROM _sync_provenance_state WHERE singleton_key=1")
        var after = cursor
        var examined = 0
        var probes = 0; var deleted = 0
        val roots = listOf("_sync_provenance_state" to "active_context_id", "_sync_row_state" to "server_context_id",
            "_sync_dirty_rows" to "base_context_id", "_sync_outbox_rows" to "base_context_id")
        db.withStatementCache { cache ->
            while (examined < maxCandidates) {
                currentCoroutineContext().ensureActive()
                val id = db.withPreparedStatement("SELECT context_id FROM _sync_provenance_contexts WHERE context_id>? ORDER BY context_id LIMIT 1", cache) { st ->
                    st.bindLong(1, after); if (st.step()) st.getLong(0) else null
                } ?: break
                after = id
                examined++
                var referenced = false
                for ((table, reference) in roots) {
                    probes++
                    val found = db.withPreparedStatement("SELECT 1 FROM $table WHERE $reference=?${if (table == "_sync_provenance_state") " AND singleton_key=1" else ""} LIMIT 1", cache) { st -> st.bindLong(1, id); st.step() }
                    if (found) { referenced = true; break }
                }
                if (!referenced) {
                    db.withPreparedStatement("DELETE FROM _sync_provenance_contexts WHERE context_id=?", cache) { it.bindLong(1, id); it.step() }
                    deleted++
                }
            }
        }
        val next = if (examined == 0) 0L else after
        db.withPreparedStatement("UPDATE _sync_provenance_state SET cleanup_cursor=? WHERE singleton_key=1") { it.bindLong(1, next); it.step() }
        ProvenanceCleanupProgress(next, examined, probes, deleted)
    }
}

internal data class ProvenanceCleanupProgress(val cursor: Long, val examined: Int, val referenceProbes: Int, val deleted: Int)

/** Only integer references and validation flags; no decoded complete contexts. One local work phase. */
internal class ProvenanceReadScope(val db: SafeSQLiteConnection, private val config: OversqliteConfig) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ProvenanceReadScope>
    private data class Binding(val context: Long, val active: Long, val sourceRequired: Boolean)
    private val validated = linkedSetOf<Binding>()
    var activeContext: Long? = null
    fun activeChanged(id: Long) { validated.clear(); activeContext = id }
    fun clear() { validated.clear(); activeContext = null }
    val charge get() = WorkspaceCharge(validated.size * 2L, validated.size * 128L)
    fun contains(context: Long, active: Long, source: Boolean) = Binding(context, active, source) in validated
    suspend fun remember(context: Long, active: Long, source: Boolean) {
        if (validated.size >= 8 || contains(context, active, source)) return
        try { OversqliteWorkspace(db, config).check("provenance validation", WorkspaceCharge(2, 128)) }
        catch (capacity: SyncWorkspaceLimitException) { if (capacity.suppressedExceptions.isNotEmpty()) throw capacity; return }
        validated += Binding(context, active, source)
    }
}
