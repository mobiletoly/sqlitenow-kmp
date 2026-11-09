package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json

internal const val uploadJournalDdl = """CREATE TABLE IF NOT EXISTS _sync_upload_journal (
 singleton_key INTEGER NOT NULL PRIMARY KEY CHECK(singleton_key=1),
 binding TEXT NOT NULL CHECK(json_valid(binding) AND json_type(binding)='object'),
 request_body BLOB NOT NULL CHECK(length(request_body)>0),
 request_digest TEXT NOT NULL CHECK(length(request_digest)=64),
 outcome_body BLOB NOT NULL DEFAULT X'',
 outcome_hash TEXT NOT NULL DEFAULT '',
 phase TEXT NOT NULL DEFAULT 'frozen' CHECK(phase IN ('frozen','ack_pending')),
 CHECK((length(outcome_body)=0 AND outcome_hash='' AND phase='frozen') OR
       (length(outcome_body)>0 AND length(outcome_hash)=64))
)"""

private val uploadJournalJson = Json { ignoreUnknownKeys = false; encodeDefaults = true; explicitNulls = true }

@Serializable
internal data class UploadBinding(
    @SerialName("scope_id") val scopeId: String,
    val authority: String,
    @SerialName("attachment_epoch") val attachmentEpoch: Long,
    val operation: PushOperationBinding,
)

internal data class UploadJournal(
    val binding: UploadBinding,
    val phase: String,
    val outcomeHash: String,
    val requestDigest: String,
    val requestBytes: Long,
    val outcomeBytes: Long,
)

internal suspend fun loadUploadJournal(db: SafeSQLiteConnection): UploadJournal? =
    db.queryList("SELECT binding,phase,outcome_hash,request_digest,length(request_body),length(outcome_body) FROM _sync_upload_journal WHERE singleton_key=1") { st ->
        val raw = st.getText(0)
        check(utf8Size(raw) <= 4096) { "invalid upload journal binding size" }
        requireUniqueSnapshotJsonObjectMembers(raw)
        val binding = uploadJournalJson.decodeFromString<UploadBinding>(raw)
        UploadJournal(binding, st.getText(1), st.getText(2), st.getText(3), st.getLong(4), st.getLong(5)).also { j ->
            val op = binding.operation
            requireValidOversqliteSourceId(op.sourceId)
            require(op.sourceBundleId > 0 && (op.generationId.toLongOrNull() ?: 0) in 1L..Int.MAX_VALUE.toLong())
            require(isCanonicalSha256(op.generationFingerprint) && isCanonicalSha256(op.canonicalRequestHash) && isCanonicalSha256(j.requestDigest))
            op.initializationId?.let(::requireValidOversqliteSessionToken)
            require(j.phase in setOf("frozen", "ack_pending") && j.requestBytes in 1..(64L shl 20) && j.outcomeBytes in 0..((64L shl 20)+8192))
            require(if(j.outcomeBytes==0L) j.outcomeHash.isEmpty() && j.phase=="frozen" else isCanonicalSha256(j.outcomeHash))
        }
    }.singleOrNull()

/** Same existing attachment/service identity; no new ownership epoch. */
internal class OversqliteUploadJournal(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
) {
    suspend fun guard(state: RuntimeState, j: UploadJournal) {
        val a = OversqliteAttachmentStateStore(db).loadState()
        val b = j.binding
        check(b.scopeId == state.userId && b.scopeId == a.attachedUserId &&
            b.operation.sourceId == state.sourceId && b.operation.sourceId == a.currentSourceId &&
            b.attachmentEpoch == a.attachmentEpoch && b.authority == config.serviceId) {
            "upload journal attachment or authority changed"
        }
    }

    suspend fun freeze(state: RuntimeState, snapshot: PushOutboundSnapshot, body: String) {
        val a = OversqliteAttachmentStateStore(db).loadState()
        val generation = checkNotNull(state.validated.generation)
        val binding = UploadBinding(state.userId, config.serviceId, a.attachmentEpoch,
            PushOperationBinding(state.sourceId, generation.id, generation.fingerprint, snapshot.sourceBundleId,
                snapshot.canonicalRequestHash, state.pendingInitializationId.takeIf { it.isNotEmpty() }))
        val raw = uploadJournalJson.encodeToString(binding)
        check(utf8Size(raw)<=4096)
        OversqliteWorkspace(db,config).check("upload-freeze", WorkspaceCharge(1,512+6*utf8Size(raw)+utf8Size(body)))
        db.withPreparedStatement("INSERT INTO _sync_upload_journal(singleton_key,binding,request_body,request_digest) VALUES(1,?,CAST(? AS BLOB),?)") { st ->
            st.bindText(1,raw);st.bindText(2,body);st.bindText(3,sha256Hex(body.encodeToByteArray()));st.step()
        }
    }

    suspend fun requestBody(state: RuntimeState, j: UploadJournal): String {
        guard(state,j)
        return db.queryRequiredSingle("SELECT CAST(request_body AS TEXT) FROM _sync_upload_journal", "missing frozen request") { it.getText(0) }.also {
            check(sha256Hex(it.encodeToByteArray())==j.requestDigest) { "frozen request bytes changed" }
        }
    }

    suspend fun outcomeBody(): String = db.queryRequiredSingle("SELECT CAST(outcome_body AS TEXT) FROM _sync_upload_journal", "missing terminal evidence") { it.getText(0) }

    suspend fun persistOutcome(state: RuntimeState, expected: UploadJournal, reply: TerminalUploadReply) = db.transaction(TransactionMode.IMMEDIATE) {
        val current = checkNotNull(loadUploadJournal(db))
        guard(state,current)
        check(current.binding==expected.binding && current.phase=="frozen" && current.outcomeBytes==0L) { "outcome owner changed" }
        OversqliteWorkspace(db,config).check("upload-outcome", WorkspaceCharge(bytes=utf8Size(reply.raw)))
        db.withPreparedStatement("UPDATE _sync_upload_journal SET outcome_body=CAST(? AS BLOB),outcome_hash=? WHERE singleton_key=1") { st ->
            st.bindText(1,reply.raw);st.bindText(2,reply.outcome.outcomeHash);st.step()
        }
    }

    /**
     * Lost-freeze recovery: the retained outcome belongs to this source and sequence but to an
     * earlier request. Replace the frozen request's hash and token with the discovered ones and
     * persist the complete outcome in one writer, so settlement and ack use the retained binding.
     */
    suspend fun rebindToRetainedOutcome(state: RuntimeState, expected: UploadJournal, reply: TerminalUploadReply) = db.transaction(TransactionMode.IMMEDIATE) {
        val current = checkNotNull(loadUploadJournal(db))
        guard(state,current)
        check(current.binding==expected.binding && current.phase=="frozen" && current.outcomeBytes==0L) { "outcome owner changed" }
        val frozen = current.binding.operation
        val outcome = reply.outcome
        check(outcome.sourceId==frozen.sourceId && outcome.generationId==frozen.generationId &&
            outcome.generationFingerprint==frozen.generationFingerprint && outcome.sourceBundleId==frozen.sourceBundleId) { "retained outcome binding mismatch" }
        outcome.initializationId?.let(::requireValidOversqliteSessionToken)
        val rebound = current.binding.copy(operation=frozen.copy(
            canonicalRequestHash=outcome.canonicalRequestHash,initializationId=outcome.initializationId))
        val raw = uploadJournalJson.encodeToString(rebound)
        check(utf8Size(raw)<=4096)
        OversqliteWorkspace(db,config).check("upload-outcome", WorkspaceCharge(bytes=utf8Size(reply.raw)))
        db.withPreparedStatement("UPDATE _sync_upload_journal SET binding=?,outcome_body=CAST(? AS BLOB),outcome_hash=? WHERE singleton_key=1") { st ->
            st.bindText(1,raw);st.bindText(2,reply.raw);st.bindText(3,outcome.outcomeHash);st.step()
        }
    }

    suspend fun acknowledge(state: RuntimeState, remote: OversqliteRemoteApi) {
        val j = loadUploadJournal(db) ?: return
        if(j.phase!="ack_pending") return
        guard(state,j)
        remote.acknowledgeOutcome(j.binding.operation,j.outcomeHash)
        db.transaction(TransactionMode.IMMEDIATE) {
            val current=checkNotNull(loadUploadJournal(db));guard(state,current)
            check(current.binding==j.binding && current.phase=="ack_pending" && current.outcomeHash==j.outcomeHash)
            val status=db.queryRequiredSingle("SELECT json_extract(CAST(outcome_body AS TEXT),'$.status') FROM _sync_upload_journal","missing outcome") {it.getText(0)}
            if(j.binding.operation.initializationId!=null && status!="committed")
                db.execSQL("UPDATE _sync_attachment_state SET initialization_reconnect_required=1 WHERE singleton_key=1")
            db.execSQL("DELETE FROM _sync_upload_journal WHERE singleton_key=1")
        }
    }

    suspend fun frozenMayResume(state: RuntimeState): Boolean {
        val journal=loadUploadJournal(db) ?: return false
        if(journal.phase!="frozen")return false
        guard(state,journal)
        check(OversqliteOutboxStateStore(db).loadBundleState().state==outboxStatePrepared)
        val operation=OversqliteOperationStateStore(db).loadState()
        // Once the replacement snapshot is staged the server has retired this source, so the frozen
        // request can no longer resume under it. The rotated rebuild carries its intent forward.
        if(operation.kind==operationKindSourceRecovery && operation.stagedSnapshotId.isNotEmpty()) return false
        check(operation.kind==operationKindNone || operation.kind==operationKindSourceRecovery) { "upload overlaps a destructive transition" }
        return true
    }

    suspend fun requireNoPending() {
        check(loadUploadJournal(db)==null && OversqliteOutboxStateStore(db).loadBundleState().state==outboxStateNone && db.scalarLong("SELECT count(*) FROM _sync_dirty_rows")==0L) { "pending upload must settle and acknowledge before source replacement or snapshot" }
    }
}

// Invoked only inside the existing atomic replay/conflict/requeue transaction.
internal suspend fun markUploadDisposition(db: SafeSQLiteConnection, cache: StatementCache? = null) {
    val j = checkNotNull(loadUploadJournal(db)) { "missing terminal disposition evidence" }
    check(j.outcomeBytes>0 && j.binding.operation.sourceBundleId<Long.MAX_VALUE) { "terminal evidence required for disposition" }
    OversqliteSourceStateStore(db).advanceAfterCommittedPush(j.binding.operation.sourceId,j.binding.operation.sourceBundleId)
    db.withPreparedStatement("UPDATE _sync_upload_journal SET phase='ack_pending' WHERE singleton_key=1",statementCache=cache) { it.step() }
}

internal suspend fun requireUploadRelease(db: SafeSQLiteConnection) {
    val j=loadUploadJournal(db)
    if(j==null) {check(OversqliteOutboxStateStore(db).loadBundleState().state!=outboxStatePrepared) { "missing upload journal" };return}
    check(j.phase=="ack_pending") { "cannot release an unsettled frozen upload" }
}

internal data class TerminalUploadReply(val raw: String, val outcome: TerminalPushOutcome, val rows: List<BundleRow>)
