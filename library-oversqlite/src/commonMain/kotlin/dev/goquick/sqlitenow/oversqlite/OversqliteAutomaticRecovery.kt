package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.*
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

@Serializable
enum class RecoveryMode {
    @SerialName("automatic_clean") AUTOMATIC_CLEAN,
    @SerialName("disabled") DISABLED,
}

/** One latest bounded status. Exact diagnostic identities never contain row payloads. */
@Serializable
data class RecoveryStatus(
    val phase: String = "idle",
    val reason: String = "",
    val recoveryId: String = "",
    val observedBoundary: Long = 0,
    val table: String = "",
    val index: String = "",
    val trigger: String = "",
    val dependency: String = "",
    val keys: List<String> = emptyList(),
    val truncated: Boolean = false,
    val nextAction: String = "sync",
)

class RecoveryBlockedException(val status: RecoveryStatus) : RuntimeException("automatic recovery blocked: ${status.reason}")
class RecoveryProgressIncompleteException(val reason: String) : RuntimeException("recovery incomplete: $reason")

@Serializable
internal data class AutomaticRecoveryRecord(
    val version: Int = 1,
    val mode: RecoveryMode = RecoveryMode.AUTOMATIC_CLEAN,
    val modeRevision: Long = 0,
    val capturedModeRevision: Long = 0,
    val status: RecoveryStatus = RecoveryStatus(),
    val sourceId: String = "",
    val generationFingerprint: String = "",
    val serviceId: String = "",
    val originalReason: String = "",
    val unitHash: String = "",
    val localGuard: String = "",
    val failureSignature: String = "",
    val armed: Boolean = false,
)
internal data class AutomaticRecoveryPin(val id: String, val modeRevision: Long)
internal class AutomaticRecoveryContext(val pin: AutomaticRecoveryPin) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<AutomaticRecoveryContext>
}
internal class HistoricalBaselineRequired(val boundary: Long, val hash: String, val reason: String) : RuntimeException("historical baseline required")

/** Metadata belongs to the existing operation row; transfer and DML stay with DownloadWorkflow. */
internal class OversqliteAutomaticRecovery(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val operations: OversqliteClientOperationController,
) {
    private val json = Json { encodeDefaults = true; ignoreUnknownKeys = false }
    private val workspace = OversqliteWorkspace(db,config)
    private val operationStore = OversqliteOperationStateStore(db)
    private val attachmentStore = OversqliteAttachmentStateStore(db)
    private val guardOwner=OversqliteRecoveryGuard(db,config,operations)
    private suspend fun relevantGuard(base:suspend()->String,table:String=""):String = sha256Hex((base()+guardOwner.dependencies(table)+":${config.transferTargets.maxSnapshotChunkRows}:${config.transferTargets.maxSnapshotChunkDecodedBytes}:${config.snapshotApplyBatchRows}:${config.snapshotApplyBatchBytes}").encodeToByteArray())

    suspend fun read(): AutomaticRecoveryRecord {
        val size = db.scalarLong("SELECT length(CAST(recovery_state AS BLOB)) FROM _sync_operation_state WHERE singleton_key=1")
        check(size in 0..65536) { "corrupt recovery metadata length" }
        workspace.check("recovery-control",WorkspaceCharge(16+size,4096+32*size))
        if (size==0L) return AutomaticRecoveryRecord(mode=config.recoveryMode)
        val encoded=db.withPreparedStatement("SELECT recovery_state FROM _sync_operation_state WHERE singleton_key=1") { st -> check(st.step());st.getText(0) }
        requireUniqueSnapshotJsonObjectMembers(encoded)
        val fields=json.parseToJsonElement(encoded).jsonObject
        check(fields.keys.containsAll(listOf("version","mode","modeRevision","capturedModeRevision","status","armed","sourceId","generationFingerprint","serviceId","originalReason","unitHash","localGuard","failureSignature"))) {"corrupt recovery fields"}
        val status=fields.getValue("status").jsonObject
        check(status.keys.containsAll(listOf("phase","reason","recoveryId","observedBoundary","table","index","trigger","dependency","keys","truncated","nextAction"))) {"corrupt recovery status fields"}
        val record=json.decodeFromString(AutomaticRecoveryRecord.serializer(),encoded)
        check(record.version==1 && record.modeRevision>=0 && record.capturedModeRevision in 0..record.modeRevision && record.status.observedBoundary>=0 && record.status.keys.size<=64 && size(record)<=16384) { "corrupt recovery identity" }
        check(record.status.phase in setOf("idle","captured","stage_baseline","prepare_transition","ready","completed","blocked")) { "corrupt recovery phase" }
        if(record.status.phase!="idle") {
            check(recoveryDigest(record.status.recoveryId) && recoveryDigest(record.localGuard) && record.sourceId.isNotEmpty() && record.serviceId.isNotEmpty() && recoveryDigest(record.generationFingerprint) && record.originalReason in setOf("history_pruned","checkpoint_ahead","historical_repair_required","conflicting_desired_state")) {"corrupt recovery authority or owner"}
            if(record.status.phase=="blocked")check(recoveryDigest(record.failureSignature)) {"corrupt recovery failure signature"}
        }
        return record
    }
    private fun size(record: AutomaticRecoveryRecord): Long = with(record) {
        val statusBytes=with(status) { utf8Size(phase)+utf8Size(reason)+utf8Size(recoveryId)+utf8Size(table)+utf8Size(index)+utf8Size(trigger)+utf8Size(dependency)+utf8Size(nextAction)+keys.sumOf(::utf8Size) }
        statusBytes+utf8Size(sourceId)+utf8Size(generationFingerprint)+utf8Size(serviceId)+utf8Size(originalReason)+utf8Size(unitHash)+utf8Size(localGuard)+utf8Size(failureSignature)
    }
    private suspend fun write(record: AutomaticRecoveryRecord) {
        check(record.status.keys.size<=64 && size(record)<=16384) { "recovery metadata exceeds bound" }
        workspace.check("recovery-control",WorkspaceCharge(32+record.status.keys.size.toLong(),8192+24*size(record)))
        val encoded=json.encodeToString(AutomaticRecoveryRecord.serializer(),record)
        db.withPreparedStatement("UPDATE _sync_operation_state SET recovery_state=? WHERE singleton_key=1") { st -> st.bindText(1,encoded);st.step() }
    }
    suspend fun status(): RecoveryStatus = db.transaction(TransactionMode.DEFERRED) {
        val record=read()
        if(record.mode==RecoveryMode.DISABLED)record.status.copy(reason="disabled",nextAction="enable_recovery_or_rebuild") else record.status
    }
    suspend fun setMode(mode: RecoveryMode) = db.transaction(TransactionMode.IMMEDIATE) {
        val current=read()
        if (current.mode!=mode) {
            check(current.modeRevision<Long.MAX_VALUE) {"recovery mode revision exhausted"}
            if(mode==RecoveryMode.DISABLED) releaseAutomaticRequirement(current)
            write(current.copy(mode=mode,modeRevision=current.modeRevision+1))
        }
    }
    suspend fun retry() = db.transaction(TransactionMode.IMMEDIATE) {
        val current=read()
        if (current.status.phase=="blocked" && !current.armed) write(current.copy(armed=true))
    }
    suspend fun enabled(): Boolean = read().mode==RecoveryMode.AUTOMATIC_CLEAN
    private suspend fun releaseAutomaticRequirement(record: AutomaticRecoveryRecord) {
        if(record.status.phase in setOf("idle","completed"))return
        val operation=operationStore.loadState();val attachment=attachmentStore.loadState()
        if(operation.kind==operationKindNone && operation.reason==record.originalReason && record.sourceId==attachment.currentSourceId && record.serviceId==config.serviceId) attachmentStore.setRebuildRequired(false)
    }
    suspend fun active(): Boolean = read().status.phase !in setOf("idle","completed")
    suspend fun capture(state: RuntimeState, reason: String, boundary: Long, hash: String, localGuard: suspend () -> String) {
        operations.completeWorkPhase("automatic recovery capture") { db.transaction(TransactionMode.IMMEDIATE) {
            requireCleanSnapshotWork(db)
            val current=read()
            if(current.mode==RecoveryMode.DISABLED)throw RecoveryBlockedException(current.status.copy(reason="disabled",nextAction="ordinary_sync_or_rebuild"))
            val guard=relevantGuard(localGuard)
            if (current.status.phase=="blocked" && !current.armed && current.localGuard==guard && current.unitHash==hash && current.capturedModeRevision==current.modeRevision) throw RecoveryBlockedException(current.status)
            operations.chargeRecoveryReplan()
            val observations=db.scalarLong("SELECT count(*) FROM _sync_row_state")
            operations.reserveSyncWork("snapshot-observation-floor",snapshotWorkAdd(32,snapshotWorkScale(observations,3)),readyAtomic=true)
            val floor=maxOf(boundary,snapshotObservationFloor(db))
            val id=sha256Hex("recovery-v1:$reason:$boundary:$hash:$guard".encodeToByteArray())
            val record=current.copy(status=RecoveryStatus("captured",reason,id,floor),sourceId=state.sourceId,
                generationFingerprint=state.admittedContract.generation.fingerprint,serviceId=config.serviceId,
                originalReason=reason,unitHash=hash,localGuard=guard,failureSignature="",armed=false,capturedModeRevision=current.modeRevision)
            val operation=operationStore.loadState()
            check(operation.kind==operationKindNone) { "another lifecycle owns recovery" }
            attachmentStore.setRebuildRequired(true)
            operationStore.persistState(operation.copy(reason=reason))
            write(record)
        } }
    }
    suspend fun validate(pin: AutomaticRecoveryPin) {
        val current=read()
        if (current.mode!=RecoveryMode.AUTOMATIC_CLEAN || current.modeRevision!=pin.modeRevision || current.status.recoveryId!=pin.id)
            throw RecoveryBlockedException(current.status.copy(reason="mode_or_owner_changed",nextAction="retry_recovery"))
    }
    suspend fun phase(phase: String) {
        val pin=currentCoroutineContext()[AutomaticRecoveryContext]?.pin ?: return
        operations.nextWorkSliceIfNeeded()
        db.transaction(TransactionMode.IMMEDIATE) { validate(pin); val current=read();write(current.copy(status=current.status.copy(phase=phase))) }
    }
    suspend fun ready() {
        val pin=currentCoroutineContext()[AutomaticRecoveryContext]?.pin ?: return
        validate(pin)
        val current=read();write(current.copy(status=current.status.copy(phase="ready")))
    }
    suspend fun completed(boundary: Long) {
        val pin=currentCoroutineContext()[AutomaticRecoveryContext]?.pin ?: return
        validate(pin)
        val current=read();write(current.copy(armed=false,status=current.status.copy(phase="completed",observedBoundary=boundary,keys=emptyList(),truncated=false,nextAction="sync")))
    }
    suspend fun <T> run(state: RuntimeState, localGuard: suspend () -> String, block: suspend () -> T): T {
        var current=read()
        if (current.mode==RecoveryMode.DISABLED) throw RecoveryBlockedException(current.status.copy(reason="disabled",nextAction="enable_recovery_or_rebuild"))
        if (current.status.phase in setOf("idle","completed")) throw RebuildRequiredException()
        check(current.sourceId==state.sourceId && current.generationFingerprint==state.admittedContract.generation.fingerprint && current.serviceId==config.serviceId) { "recovery authority changed" }
        requireCleanSnapshotWork(db)
        operations.markRecoveryPlan()
        if (current.status.phase=="blocked") {
            operations.nextWorkSliceIfNeeded()
            val guard=operations.completeWorkPhase("automatic recovery eligibility") { db.transaction(TransactionMode.DEFERRED) { relevantGuard(localGuard,current.status.table) } }
            if (!current.armed && current.localGuard==guard) throw RecoveryBlockedException(current.status)
            capture(state,current.originalReason,current.status.observedBoundary,current.unitHash,localGuard)
            current=read()
        }
        return withContext(AutomaticRecoveryContext(AutomaticRecoveryPin(current.status.recoveryId,current.modeRevision))) {
            phase("stage_baseline")
            try { block() } catch (failure: Throwable) {
                currentCoroutineContext().ensureActive()
                park(failure,localGuard)
                throw failure
            }
        }
    }
    private suspend fun park(failure: Throwable, localGuard: suspend () -> String) {
        val diagnostic=recoveryFailureDiagnostic(failure) ?: return
        operations.completeWorkPhase("automatic recovery parking") { db.transaction(TransactionMode.IMMEDIATE) {
            val current=read()
            val bytes=recoveryDiagnosticByteBound(diagnostic)
            workspace.check("recovery-status",WorkspaceCharge(32+minOf(diagnostic.keys.size,64).toLong(),8192+24*(size(current)+bytes)))
            val status=boundedRecoveryDiagnostic(diagnostic).copy(phase="blocked",recoveryId=current.status.recoveryId,observedBoundary=current.status.observedBoundary)
            val guard=relevantGuard(localGuard,status.table)
            val signature=sha256Hex(buildJsonArray {add(current.version);add(current.unitHash);add(status.reason);add(status.table);add(status.index);add(status.trigger);add(status.dependency);add(guard)}.toString().encodeToByteArray())
            write(current.copy(status=status,armed=false,localGuard=guard,failureSignature=signature))
        } }
    }

}

internal fun historicalRecoveryReason(error: Throwable): String? = when(error) {
    is HistoryPrunedException -> "history_pruned"
    is CheckpointAheadException -> "checkpoint_ahead"
    is HistoricalBaselineRequired -> error.reason
    is FKReconciliationRequiredException -> error.detail.takeIf { it=="historical_repair_required" }
    is FinalStateApplyBlockedException -> error.reason.takeIf { it=="conflicting_desired_state" && error.operation=="pull" }
    else -> null
}

internal fun recoveryFailureDiagnostic(failure: Throwable):RecoveryStatus? {
    val action="correct_blocker_or_retry_recovery"
    return when(failure){
        is FinalStateApplyBlockedException -> RecoveryStatus(reason=failure.reason,table=failure.table,index=failure.index,trigger=failure.trigger,keys=failure.keys,truncated=failure.hasMore,nextAction=action)
        is FKReconciliationRequiredException -> RecoveryStatus(reason=failure.detail,dependency=failure.detail,nextAction=action)
        is SnapshotFinalApplyGateException -> RecoveryStatus(reason="local_context_changed",nextAction=action)
        is SyncWorkspaceLimitException -> RecoveryStatus(reason="capacity_${failure.dimension}",nextAction="correct_capacity")
        is RecoveryBlockedException -> RecoveryStatus(reason="mode_or_owner_changed",nextAction=action)
        is SnapshotResponseBodyTooLargeException -> RecoveryStatus(reason="snapshot_body_too_large",nextAction="correct_snapshot_limits")
        is SnapshotTransferLimitException -> RecoveryStatus(reason="snapshot_transfer_limit",nextAction="correct_snapshot_limits")
        is SnapshotApplyRowTooLargeException -> RecoveryStatus(reason="snapshot_apply_row_too_large",nextAction="correct_capacity")
        is SnapshotSessionLimitExceededException -> RecoveryStatus(reason="snapshot_limit_${failure.dimension}",nextAction="correct_capacity")
        is SnapshotChunkTooSmallException -> RecoveryStatus(reason="snapshot_chunk_too_small",nextAction="correct_snapshot_limits")
        is SnapshotHttpException -> if(failure.errorCode in setOf("recovery_floor_ahead","push_outcome_pending"))RecoveryStatus(reason=failure.errorCode,nextAction="correct_authority_or_retry_recovery") else null
        is SnapshotSemanticException, is SnapshotResponseDecodeException, is SnapshotUnsupportedContentEncodingException -> RecoveryStatus(reason="snapshot_integrity",nextAction="inspect_authority_or_retry_recovery")
        else -> null
    }
}
internal fun boundedRecoveryDiagnostic(status: RecoveryStatus):RecoveryStatus {
    var bytes=0L;var truncated=status.truncated
    fun exact(value:String):String {return if(bytes+utf8Size(value)>8192){truncated=true;""}else{bytes+=utf8Size(value);value}}
    val table=exact(status.table);val index=exact(status.index);val trigger=exact(status.trigger);val dependency=exact(status.dependency)
    val keys=mutableListOf<String>()
    for(key in status.keys){if(keys.size==64 || bytes+utf8Size(key)>8192){truncated=true;break};keys+=key;bytes+=utf8Size(key)}
    return status.copy(table=table,index=index,trigger=trigger,dependency=dependency,keys=keys,truncated=truncated)
}

/** Scalar measurement precedes every diagnostic list/JSON copy. */
internal fun recoveryDiagnosticByteBound(status:RecoveryStatus):Long {
    var bytes=with(status){utf8Size(table)+utf8Size(index)+utf8Size(trigger)+utf8Size(dependency)}
    var count=0
    for(key in status.keys){if(count++==64)break;bytes+=utf8Size(key)}
    return minOf(8192L,bytes)
}

private fun recoveryDigest(value:String):Boolean = value.length==64 && value.all {it in '0'..'9' || it in 'a'..'f'}
