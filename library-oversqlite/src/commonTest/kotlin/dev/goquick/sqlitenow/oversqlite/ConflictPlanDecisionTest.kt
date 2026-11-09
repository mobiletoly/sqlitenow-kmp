package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ConflictPlanDecisionTest {
    private val localRow = Json.parseToJsonElement("""{"id":"u1","name":"local"}""").jsonObject

    private fun context(localOp: String, serverRowVersion: Long, serverRowDeleted: Boolean, serverRow: JsonElement?) =
        ConflictContext(
            schema = "main", table = "users", key = mapOf("id" to "u1"), localOp = localOp, localPayload = localRow,
            baseRowVersion = 1, serverRowVersion = serverRowVersion, serverRowDeleted = serverRowDeleted, serverRow = serverRow,
        )

    @Test
    fun keepLocalUpdateOverRemoteDeleteReinsertsWithTombstoneBase() {
        val decision = buildConflictPlanDecision(context("UPDATE", 7, true, null), MergeResult.KeepLocal, localRow, emptySet())
        val plan = assertIs<ConflictPlanDecision.Apply>(decision).plan
        assertEquals(LocalRowAction.Upsert(localRow, PayloadSource.LOCAL_STATE), plan.localRowAction)
        assertEquals(RowStateAction.Upsert(rowVersion = 7, deleted = true), plan.rowStateAction)
        assertEquals(ResolvedDirtyIntent("INSERT", 7, RetryPayloadSource.LocalRow), plan.requeueIntent)
    }

    @Test
    fun keepMergedUpdateOverRemoteDeleteReinsertsMergedPayload() {
        val merged = Json.parseToJsonElement("""{"id":"u1","name":"merged"}""")
        val decision = buildConflictPlanDecision(
            context("UPDATE", 7, true, null), MergeResult.KeepMerged(merged), localRow, setOf("id", "name"),
        )
        val plan = assertIs<ConflictPlanDecision.Apply>(decision).plan
        assertEquals(LocalRowAction.Upsert(merged.jsonObject, PayloadSource.AUTHORITATIVE_WIRE), plan.localRowAction)
        assertEquals(RowStateAction.Upsert(rowVersion = 7, deleted = true), plan.rowStateAction)
        assertEquals(ResolvedDirtyIntent("INSERT", 7, RetryPayloadSource.LocalRow), plan.requeueIntent)
    }

    @Test
    fun keepLocalUpdateOverMissingServerRowInsertsFromAbsentBaseline() {
        val decision = buildConflictPlanDecision(context("UPDATE", 0, false, null), MergeResult.KeepLocal, localRow, emptySet())
        val plan = assertIs<ConflictPlanDecision.Apply>(decision).plan
        assertEquals(LocalRowAction.Upsert(localRow, PayloadSource.LOCAL_STATE), plan.localRowAction)
        assertEquals(RowStateAction.Delete, plan.rowStateAction)
        assertEquals(ResolvedDirtyIntent("INSERT", 0, RetryPayloadSource.LocalRow), plan.requeueIntent)
    }

    @Test
    fun keepLocalUpdateOverLiveRowStillRetriesAsUpdate() {
        val serverRow = Json.parseToJsonElement("""{"id":"u1","name":"server"}""")
        val decision = buildConflictPlanDecision(context("UPDATE", 5, false, serverRow), MergeResult.KeepLocal, localRow, emptySet())
        val plan = assertIs<ConflictPlanDecision.Apply>(decision).plan
        assertEquals(RowStateAction.Upsert(rowVersion = 5, deleted = false, payload = serverRow.jsonObject), plan.rowStateAction)
        assertEquals(ResolvedDirtyIntent("UPDATE", 5, RetryPayloadSource.LocalRow), plan.requeueIntent)
    }

    @Test
    fun keepMergedDeleteRemainsInvalid() {
        val decision = buildConflictPlanDecision(
            context("DELETE", 7, false, Json.parseToJsonElement("""{"id":"u1","name":"server"}""")),
            MergeResult.KeepMerged(localRow), null, setOf("id", "name"),
        )
        assertIs<ConflictPlanDecision.Invalid>(decision)
    }
}
