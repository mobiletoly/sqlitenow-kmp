package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.currentCoroutineContext
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/** A scheduling stall leaves the original local intent and continuation intact. */
class SyncWorkNoProgressException(val phase: String) :
    RuntimeException("sync_work_no_progress: phase=$phase"), OversqliteCategorizedException {
    override val category = OversqliteErrorCategory.STATE
}

internal class OversqliteWorkSlice : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<OversqliteWorkSlice>
    var checkpointed = false
    var durable = false
}

internal class PushPlanningWorkspaceSnapshot(val db: SafeSQLiteConnection) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<PushPlanningWorkspaceSnapshot>
    var base: WorkspaceCharge? = null
    var guard: PushPlanningGuard? = null
    var plan: PushPlan? = null
    var planLoaded = false
    var outboxNone = false
}

internal data class PushPlanningGuard(val value: Triple<String, Long, String>, val entries: Long, val bytes: Long)
internal val pushPlanningGuardCharge = WorkspaceCharge(4, 8192)

internal val pushPlanningWorkspaceCharge = WorkspaceCharge(2, 256)

internal suspend fun checkpointWorkStep() {
    currentCoroutineContext()[OversqliteWorkSlice]?.checkpointed = true
}

internal suspend fun commitWorkStep() {
    currentCoroutineContext()[OversqliteWorkSlice]?.let { if (it.checkpointed) it.durable = true }
}

// Scheduling observations are separate from the established replay accounting oracle.
internal data class WorkSliceDiagnostics(
    var workSlices: Long = 0,
    var peakSliceWork: Long = 0,
    var pushMetadataMeasurements: Long = 0,
    var totalWork: Long = 0,
    var seedVisits: Long = 0,
    var pushPlanningTransactions: Long = 0,
    var pushPlanningSteps: Long = 0,
    var pushCleanupCommits: Long = 0,
    var reservationBindingCacheBuilds: Long = 0,
    var reservationBindingCacheHits: Long = 0,
    var reservationBindingCacheFallbacks: Long = 0,
    var replayDecisionBuilds: Long = 0,
    var replayDecisionReuses: Long = 0,
    var replayDecisionInvalidations: Long = 0,
)
