package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class OversqliteReplayPlannerTest {
    private data class EquivalentPayloadScenario(
        val name: String,
        val tableInfo: TableInfo,
        val uploadedPayload: String,
        val pendingPayload: String,
        val livePayload: String,
    )

    private data class PreserveLocalScenario(
        val name: String,
        val uploadedOp: String,
        val uploadedPayload: String?,
        val livePayload: String,
        val expectedRequeueOp: String,
        val expectedRequeuePayload: String,
    )

    private val planner = OversqliteReplayPlanner(Json { ignoreUnknownKeys = true })

    @Test
    fun deletedUploadPreservesOnlyCapturedNewerTombstone() {
        for (captured in listOf(false, true)) {
            val plan = planner.plan(
                tableInfo = usersTableInfo,
                uploaded = uploadedChange(op = "DELETE", payload = null),
                pending = DirtyUploadState(exists = captured, op = "DELETE", payload = null),
                livePayload = null,
            )
            if (captured) {
                assertEquals(ReplayRowAction.PreserveLocal("DELETE", null), plan.action)
            } else {
                assertEquals(ReplayRowAction.AcceptAuthoritative, plan.action)
            }
        }
    }
    private val usersTableInfo = TableInfo(
        table = "users",
        columns = listOf(
            ColumnInfo(name = "id", wireType = "text", declaredType = "TEXT", isPrimaryKey = true, notNull = true, defaultValue = null),
            ColumnInfo(name = "name", wireType = "text", declaredType = "TEXT", isPrimaryKey = false, notNull = true, defaultValue = null),
        ),
    )
    private val scoredUsersTableInfo = TableInfo(
        table = "users",
        columns = listOf(
            ColumnInfo(name = "id", wireType = "text", declaredType = "TEXT", isPrimaryKey = true, notNull = true, defaultValue = null),
            ColumnInfo(name = "score", wireType = "float64", declaredType = "DOUBLE PRECISION", isPrimaryKey = false, notNull = false, defaultValue = null),
        ),
    )

    @Test
    fun plan_preservesLaterWritesEvenWhenPayloadsMatch() {
        val scenarios = listOf(
            EquivalentPayloadScenario(
                name = "json object key order differs",
                tableInfo = usersTableInfo,
                uploadedPayload = """{"id":{"storage":"text","value":"1"},"name":{"storage":"text","value":"Ada"}}""",
                pendingPayload = """{"name":{"storage":"text","value":"Ada"},"id":{"storage":"text","value":"1"}}""",
                livePayload = """{"name":{"storage":"text","value":"Ada"},"id":{"storage":"text","value":"1"}}""",
            ),
            EquivalentPayloadScenario(
                name = "real payloads match after canonical number normalization",
                tableInfo = scoredUsersTableInfo,
                uploadedPayload = """{"id":{"storage":"text","value":"1"},"score":{"storage":"real","value":"1.0"}}""",
                pendingPayload = """{"id":{"storage":"text","value":"1"},"score":{"storage":"real","value":"1.00e+0"}}""",
                livePayload = """{"id":{"storage":"text","value":"1"},"score":{"storage":"real","value":"1.000"}}""",
            ),
        )

        for (scenario in scenarios) {
            val uploaded = uploadedChange(
                op = "UPDATE",
                payload = scenario.uploadedPayload,
            )

            val plan = planner.plan(
                tableInfo = scenario.tableInfo,
                uploaded = uploaded,
                pending = DirtyUploadState(exists = true, op = "UPDATE", payload = scenario.pendingPayload),
                livePayload = scenario.livePayload,
            )

            assertIs<ReplayRowAction.PreserveLocal>(plan.action, scenario.name)
            assertTrue(plan.diagnostics.pendingMatches, scenario.name)
            assertTrue(plan.diagnostics.liveMatches, scenario.name)
        }
    }

    @Test
    fun plan_preservesLocalPayloadsThatStillNeedReplay() {
        val scenarios = listOf(
            PreserveLocalScenario(
                name = "latest live payload changed after upload",
                uploadedOp = "UPDATE",
                uploadedPayload = """{"id":{"storage":"text","value":"1"},"name":{"storage":"text","value":"Ada"}}""",
                livePayload = """{"id":{"storage":"text","value":"1"},"name":{"storage":"text","value":"Grace"}}""",
                expectedRequeueOp = "UPDATE",
                expectedRequeuePayload = """{"id":{"storage":"text","value":"1"},"name":{"storage":"text","value":"Grace"}}""",
            ),
            PreserveLocalScenario(
                name = "delete upload was locally undone",
                uploadedOp = "DELETE",
                uploadedPayload = null,
                livePayload = """{"id":{"storage":"text","value":"1"},"name":{"storage":"text","value":"Ada"}}""",
                expectedRequeueOp = "INSERT",
                expectedRequeuePayload = """{"id":{"storage":"text","value":"1"},"name":{"storage":"text","value":"Ada"}}""",
            ),
        )

        for (scenario in scenarios) {
            val uploaded = uploadedChange(
                op = scenario.uploadedOp,
                payload = scenario.uploadedPayload,
            )

            val plan = planner.plan(
                tableInfo = usersTableInfo,
                uploaded = uploaded,
                pending = DirtyUploadState(),
                livePayload = scenario.livePayload,
            )

            val action = assertIs<ReplayRowAction.PreserveLocal>(plan.action, scenario.name)
            assertEquals(scenario.expectedRequeueOp, action.requeueOp, scenario.name)
            assertEquals(scenario.expectedRequeuePayload, action.requeuePayload, scenario.name)
        }
    }

    @Test
    fun replayDiagnosticsDoNotDiscloseKeysPayloadsOrHostileIdentifiers() {
        val sentinel = "customer-secret\nwire-value"
        val uploaded = uploadedChange(
            op = "UPDATE",
            payload = sentinel,
        ).copy(
            schemaName = sentinel,
            tableName = sentinel,
            keyJson = sentinel,
            localPk = sentinel,
            wireKey = mapOf(sentinel to sentinel),
        )
        val plan = UploadedReplayPlan(
            rowRef = uploaded.toRowRef(),
            action = ReplayRowAction.PreserveLocal(
                requeueOp = sentinel,
                requeuePayload = sentinel,
            ),
            diagnostics = ReplayPlanDiagnostics(
                pendingExists = true,
                pendingMatches = false,
                livePayload = sentinel,
                liveMatches = false,
                currentDirtyPayload = sentinel,
            ),
        )

        val diagnostic = replayDecisionLog(uploaded, plan)

        assertFalse(sentinel in diagnostic)
        assertFalse(" key=" in diagnostic)
        assertFalse("Payload=" in diagnostic)
        assertTrue("table=<redacted>" in diagnostic)
        assertTrue("requeueOp=<invalid>" in diagnostic)
        assertEquals("missing replay plan for uploaded row", replayMissingPlanMessage)
        assertEquals(
            "successful push replay left an unexpected dirty row",
            replayUnexpectedDirtyMessage,
        )
        assertEquals(
            "successful push replay lost a preserved dirty row",
            replayMissingPreservedDirtyMessage,
        )
    }

    private fun uploadedChange(
        op: String,
        payload: String?,
    ): DirtyRowCapture {
        return DirtyRowCapture(
            schemaName = "main",
            tableName = "users",
            keyJson = """{"id":"1"}""",
            localPk = "1",
            wireKey = mapOf("id" to "1"),
            op = op,
            baseRowVersion = 0,
            localPayload = payload,
            dirtyOrdinal = 1,
            base = HistoricalBase("absent", null, "{\"origin\":\"never_published\"}"),
        )
    }
}
