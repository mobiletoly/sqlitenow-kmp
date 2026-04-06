package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class OversqliteReplayPlannerTest {
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
    fun plan_treatsEquivalentJsonObjectsAsMatchingEvenWhenKeyOrderDiffers() {
        val uploaded = uploadedChange(
            op = "UPDATE",
            payload = """{"id":"1","name":"Ada"}""",
        )

        val plan = planner.plan(
            tableInfo = usersTableInfo,
            uploaded = uploaded,
            pending = DirtyUploadState(exists = true, op = "UPDATE", payload = """{"name":"Ada","id":"1"}"""),
            livePayload = """{"name":"Ada","id":"1"}""",
        )

        assertIs<ReplayRowAction.AcceptAuthoritative>(plan.action)
        assertTrue(plan.diagnostics.pendingMatches)
        assertTrue(plan.diagnostics.liveMatches)
    }

    @Test
    fun plan_requeuesLatestLivePayloadWhenLocalRowChangedAfterUpload() {
        val uploaded = uploadedChange(
            op = "UPDATE",
            payload = """{"id":"1","name":"Ada"}""",
        )

        val plan = planner.plan(
            tableInfo = usersTableInfo,
            uploaded = uploaded,
            pending = DirtyUploadState(),
            livePayload = """{"id":"1","name":"Grace"}""",
        )

        val action = assertIs<ReplayRowAction.PreserveLocal>(plan.action)
        assertEquals("UPDATE", action.requeueOp)
        assertEquals("""{"id":"1","name":"Grace"}""", action.requeuePayload)
    }

    @Test
    fun plan_requeuesInsertWhenDeleteUploadWasLocallyUndone() {
        val uploaded = uploadedChange(
            op = "DELETE",
            payload = null,
        )

        val plan = planner.plan(
            tableInfo = usersTableInfo,
            uploaded = uploaded,
            pending = DirtyUploadState(),
            livePayload = """{"id":"1","name":"Ada"}""",
        )

        val action = assertIs<ReplayRowAction.PreserveLocal>(plan.action)
        assertEquals("INSERT", action.requeueOp)
        assertEquals("""{"id":"1","name":"Ada"}""", action.requeuePayload)
    }

    @Test
    fun plan_treatsEquivalentRealPayloadsAsMatchingAfterCanonicalNumberNormalization() {
        val uploaded = uploadedChange(
            op = "UPDATE",
            payload = """{"id":"1","score":1.0}""",
        )

        val plan = planner.plan(
            tableInfo = scoredUsersTableInfo,
            uploaded = uploaded,
            pending = DirtyUploadState(
                exists = true,
                op = "UPDATE",
                payload = """{"id":"1","score":1.00e+0}""",
            ),
            livePayload = """{"id":"1","score":1.000}""",
        )

        assertIs<ReplayRowAction.AcceptAuthoritative>(plan.action)
        assertTrue(plan.diagnostics.pendingMatches)
        assertTrue(plan.diagnostics.liveMatches)
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
