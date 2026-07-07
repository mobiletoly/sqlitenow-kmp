package dev.goquick.sqlitenow.oversqlite

import io.ktor.http.HttpStatusCode
import kotlin.test.Test
import kotlin.test.assertEquals

internal class OversqliteErrorCategoryTest {
    @Test
    fun exceptionsDeclareStableSwiftFacingCategories() {
        val scenarios = listOf(
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
                    rawBody = "{}",
                    errorMessage = "unauthorized",
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

    private data class Scenario(
        val name: String,
        val error: OversqliteCategorizedException,
        val expected: OversqliteErrorCategory,
    )
}
