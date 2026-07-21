package dev.goquick.sqlitenow.oversqlite

import io.ktor.http.HttpStatusCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

internal class OversqliteErrorCategoryTest {
    @Test
    fun exceptionsDeclareStableSwiftFacingCategories() {
        val scenarios = listOf(
            Scenario(
                name = "protocol mismatch is non-network protocol failure",
                error = ProtocolVersionMismatchException(actual = "v1"),
                expected = OversqliteErrorCategory.PROTOCOL,
            ),
            Scenario(
                name = "table contract mismatch is a protocol failure",
                error = SyncTableContractMismatchException(
                    serverOnlyTables = listOf("business.monitoring_focus"),
                    clientOnlyTables = emptyList(),
                    syncKeyMismatches = emptyList(),
                ),
                expected = OversqliteErrorCategory.PROTOCOL,
            ),
            Scenario(
                name = "state precondition",
                error = OpenRequiredException("sync"),
                expected = OversqliteErrorCategory.STATE,
            ),
            Scenario(
                name = "binding conflict is lifecycle state",
                error = ConnectBindingConflictException(
                    attachedUserId = "old-user",
                    requestedUserId = "new-user",
                ),
                expected = OversqliteErrorCategory.STATE,
            ),
            Scenario(
                name = "push retry exhaustion is conflict",
                error = PushConflictRetryExhaustedException(
                    retryCount = 3,
                    remainingDirtyCount = 1,
                ),
                expected = OversqliteErrorCategory.CONFLICT,
            ),
            Scenario(
                name = "upload server failure is network",
                error = UploadHttpException(
                    status = HttpStatusCode.InternalServerError,
                    rawBody = "{}",
                ),
                expected = OversqliteErrorCategory.NETWORK,
            ),
            Scenario(
                name = "upload unauthorized is auth",
                error = UploadHttpException(
                    status = HttpStatusCode.Unauthorized,
                    rawBody = "{}",
                ),
                expected = OversqliteErrorCategory.AUTH,
            ),
            Scenario(
                name = "download forbidden is auth",
                error = DownloadHttpException(
                    status = HttpStatusCode.Forbidden,
                    rawBody = "{}",
                ),
                expected = OversqliteErrorCategory.AUTH,
            ),
            Scenario(
                name = "source replacement invalid unauthorized is auth",
                error = SourceReplacementInvalidHttpException(
                    status = HttpStatusCode.Unauthorized,
                ),
                expected = OversqliteErrorCategory.AUTH,
            ),
            Scenario(
                name = "committed bundle not found is network",
                error = CommittedBundleNotFoundException("{}"),
                expected = OversqliteErrorCategory.NETWORK,
            ),
            Scenario(
                name = "history pruned is network",
                error = HistoryPrunedException("history pruned"),
                expected = OversqliteErrorCategory.NETWORK,
            ),
            Scenario(
                name = "dirty state rejection is state",
                error = DirtyStateRejectedException(dirtyCount = 2),
                expected = OversqliteErrorCategory.STATE,
            ),
        )

        scenarios.forEach { scenario ->
            assertEquals(
                expected = scenario.expected,
                actual = scenario.error.category,
                message = scenario.name,
            )
            assertEquals(
                expected = scenario.expected.name.lowercase(),
                actual = scenario.error.category.payloadCategory,
                message = scenario.name,
            )
        }
    }

    @Test
    fun protocolGateRejectsV0EmptyAndUnknownVersions() {
        val hostileProtocolVersion = "HOSTILE_PROTOCOL_VERSION_SECRET"
        listOf("v0", "", hostileProtocolVersion).forEach { actual ->
            val error = assertFailsWith<ProtocolVersionMismatchException>(actual) {
                CapabilitiesResponse(
                    protocolVersion = actual,
                    schemaVersion = 1,
                    registeredTableSpecs = testRegisteredTableSpecs("users"),
                    features = emptyMap(),
                    bundleLimits = testBundleCapabilitiesLimits(),
                ).requireSupportedProtocol()
            }
            assertEquals("v1", error.expected)
            assertEquals("", error.actual)
            assertEquals("oversqlite protocol version mismatch", error.message)
            assertFalse(error.message.orEmpty().contains(hostileProtocolVersion))
        }
    }

    @Test
    fun sourceReplacementInvalidExceptionsCannotRetainRemoteMessages() {
        val sentinel = "customer-secret-remote-message"
        val decoded = assertNotNull(
            decodeSourceReplacementInvalidExceptionOrNull(
                status = HttpStatusCode.Conflict,
                rawBody = """{"error":"source_replacement_invalid","message":"$sentinel"}""",
            ),
        )
        assertEquals(
            "snapshot session request failed: HTTP 409; server rejected the source replacement request",
            decoded.message,
        )
        assertFalse(decoded.toString().contains(sentinel))

        val publicError = SourceReplacementInvalidException()
        assertEquals("server rejected the source replacement request", publicError.message)
        assertFalse(publicError.toString().contains(sentinel))
    }

    private data class Scenario(
        val name: String,
        val error: OversqliteCategorizedException,
        val expected: OversqliteErrorCategory,
    )
}
