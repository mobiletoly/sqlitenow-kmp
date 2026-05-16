package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
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
    private val usersTableInfo = TableInfo(
        table = "users",
        columns = listOf(
            ColumnInfo(name = "id", declaredType = "TEXT", isPrimaryKey = true, notNull = true, defaultValue = null),
            ColumnInfo(name = "name", declaredType = "TEXT", isPrimaryKey = false, notNull = true, defaultValue = null),
        ),
    )
    private val scoredUsersTableInfo = TableInfo(
        table = "users",
        columns = listOf(
            ColumnInfo(name = "id", declaredType = "TEXT", isPrimaryKey = true, notNull = true, defaultValue = null),
            ColumnInfo(name = "score", declaredType = "DOUBLE PRECISION", isPrimaryKey = false, notNull = false, defaultValue = null),
        ),
    )

    @Test
    fun plan_treatsEquivalentPayloadsAsMatching() {
        val scenarios = listOf(
            EquivalentPayloadScenario(
                name = "json object key order differs",
                tableInfo = usersTableInfo,
                uploadedPayload = """{"id":"1","name":"Ada"}""",
                pendingPayload = """{"name":"Ada","id":"1"}""",
                livePayload = """{"name":"Ada","id":"1"}""",
            ),
            EquivalentPayloadScenario(
                name = "real payloads match after canonical number normalization",
                tableInfo = scoredUsersTableInfo,
                uploadedPayload = """{"id":"1","score":1.0}""",
                pendingPayload = """{"id":"1","score":1.00e+0}""",
                livePayload = """{"id":"1","score":1.000}""",
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

            assertIs<ReplayRowAction.AcceptAuthoritative>(plan.action, scenario.name)
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
                uploadedPayload = """{"id":"1","name":"Ada"}""",
                livePayload = """{"id":"1","name":"Grace"}""",
                expectedRequeueOp = "UPDATE",
                expectedRequeuePayload = """{"id":"1","name":"Grace"}""",
            ),
            PreserveLocalScenario(
                name = "delete upload was locally undone",
                uploadedOp = "DELETE",
                uploadedPayload = null,
                livePayload = """{"id":"1","name":"Ada"}""",
                expectedRequeueOp = "INSERT",
                expectedRequeuePayload = """{"id":"1","name":"Ada"}""",
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
        )
    }
}
