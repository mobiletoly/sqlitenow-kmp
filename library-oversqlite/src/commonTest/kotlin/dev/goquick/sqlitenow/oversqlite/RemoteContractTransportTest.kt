package dev.goquick.sqlitenow.oversqlite

import io.ktor.http.HttpStatusCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RemoteContractTransportTest {
    @Test
    fun pushReplayHistoryPrunedUsesTheDirectPushRecoverySignal() {
        assertIs<PushReplayHistoryPrunedException>(
            decodePushReplayHistoryPrunedExceptionOrNull(
                HttpStatusCode.Conflict,
                """{"error":"history_pruned","message":"replay history unavailable"}""",
            ),
        )
    }

    @Test
    fun terminalDirectProtocolErrorsKeepTheirExactCodesAndNeverBecomeRetries() {
        val scenarios = listOf(
            Triple(409, "push_replay_mismatch", "push"),
            Triple(422, "bundle_projection_too_large", "push"),
            Triple(500, "committed_bundle_unrepresentable", "pull"),
            Triple(422, "deployment_rejected_row", "push"),
        )
        scenarios.forEach { (status, code, operation) ->
            val error = assertIs<OversqliteRemoteContractException>(
                decodeRemoteOperationErrorOrNull(
                    status,
                    """{"error":"$code","message":"safe"}""",
                    operation,
                ),
            )
            assertEquals(code, error.code)
            assertFalse(error.retryable)
            assertTrue(error.isTerminalSyncCompatibilityFailure())
        }
    }

    @Test
    fun decodesEveryGenerationErrorWithExactStatusRemediationAndRetryability() {
        data class Scenario(val status: Int, val code: String, val remediation: String, val retryable: Boolean = false)
        val scenarios = listOf(
            Scenario(400, "missing_schema_generation", "provide_schema_generation"),
            Scenario(400, "invalid_schema_generation_id", "correct_schema_generation_id"),
            Scenario(400, "missing_schema_generation_fingerprint", "provide_schema_generation_fingerprint"),
            Scenario(400, "malformed_schema_generation_fingerprint", "correct_schema_generation_fingerprint"),
            Scenario(409, "unknown_schema_generation", "upgrade_or_reconfigure_client"),
            Scenario(409, "schema_generation_fingerprint_mismatch", "upgrade_or_reconfigure_client"),
            Scenario(410, "unsupported_schema_generation", "upgrade_or_reconfigure_client"),
            Scenario(426, "retired_schema_generation", "upgrade_application"),
            Scenario(409, "source_generation_unbound", "connect_source"),
            Scenario(409, "source_generation_conflict", "rotate_source"),
            Scenario(409, "schema_generation_downgrade_forbidden", "keep_or_upgrade_generation"),
            Scenario(500, "corrupt_source_generation_state", "contact_operator"),
            Scenario(409, "source_binding_limit_exceeded", "contact_operator"),
            Scenario(409, "session_generation_mismatch", "restart_session_with_matching_generation"),
            Scenario(500, "corrupt_session_generation_state", "contact_operator"),
            Scenario(500, "corrupt_committed_push_state", "contact_operator"),
            Scenario(409, "generation_history_unavailable", "rebuild_from_snapshot"),
            Scenario(500, "generation_projection_integrity_error", "contact_operator"),
            Scenario(413, "bundle_limit_exceeded", "reduce_request"),
            Scenario(503, "history_projection_capacity_exceeded", "retry", retryable = true),
            Scenario(422, "push_effect_coverage_incomplete", "correct_server_write_logic"),
        )
        for (scenario in scenarios) {
            val raw = """{"error":"${scenario.code}","message":"safe message","retryable":${scenario.retryable},"remediation":"${scenario.remediation}"}"""
            val error = assertIs<OversqliteRemoteContractException>(
                decodeRemoteContractErrorOrNull(scenario.status, raw, "test operation"),
                scenario.code,
            )
            assertEquals(scenario.code, error.code)
            assertEquals(scenario.status, error.httpStatus)
            assertEquals("test operation", error.operation)
            assertEquals(scenario.remediation, error.remediation)
            assertEquals(scenario.retryable, error.retryable)
            assertEquals(null, error.generationId)
            assertEquals(!scenario.retryable, error.isTerminalSyncCompatibilityFailure())
        }
    }

    @Test
    fun rejectsMalformedKnownGenerationErrorsAndLeavesOtherErrorFamiliesToTheirDecoders() {
        val valid = """{"error":"retired_schema_generation","message":"upgrade","retryable":false,"remediation":"upgrade_application"}"""
        val corruptions = linkedMapOf(
            "wrong status" to (409 to valid),
            "wrong remediation" to (426 to valid.replace("upgrade_application", "retry")),
            "wrong retryability" to (426 to valid.replace("false", "true")),
            "string retryability" to (426 to valid.replace("false", "\"false\"")),
            "missing message" to (426 to valid.replace("\"message\":\"upgrade\",", "")),
            "empty message" to (426 to valid.replace("upgrade", "")),
            "extra field" to (426 to valid.dropLast(1) + ",\"extra\":true}"),
            "duplicate field" to (426 to valid.replace("\"error\":", "\"error\":\"retired_schema_generation\",\"error\":")),
        )
        for ((name, input) in corruptions) {
            assertFails(name) { decodeRemoteContractErrorOrNull(input.first, input.second, "test operation") }
        }
        assertNull(
            decodeRemoteContractErrorOrNull(
                503,
                """{"error":"service_not_ready","message":"retry"}""",
                "test operation",
            ),
        )
    }

    @Test
    fun usesFixedHardInboundCeilingsIndependentOfNegotiatedAdvertisement() {
        assertEquals(
            oversqlitePullResponseBodyHardLimit,
            oversqliteResponseBodyHardLimit("/sync/pull?after_bundle_seq=0", 200),
        )
        assertEquals(
            oversqlitePushResponseBodyHardLimit,
            oversqliteResponseBodyHardLimit("/sync/push", 200),
        )
        assertEquals(
            oversqlitePushResponseBodyHardLimit,
            oversqliteResponseBodyHardLimit("/sync/push", 409),
        )
        assertEquals(generationControlBodyLimit, oversqliteResponseBodyHardLimit("/sync/pull", 409))
        assertEquals(generationControlBodyLimit, oversqliteResponseBodyHardLimit("/sync/connect", 200))
        assertTrue(oversqlitePullResponseBodyHardLimit < Int.MAX_VALUE.toLong())
        assertTrue(oversqlitePushResponseBodyHardLimit < Int.MAX_VALUE.toLong())
        assertFalse(oversqlitePullResponseBodyHardLimit == oversqlitePushResponseBodyHardLimit)
    }
}
