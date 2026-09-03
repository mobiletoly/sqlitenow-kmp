package dev.goquick.sqlitenow.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class SqliteNowMigrationOwnerAccessTest {
    @Test
    fun operationFailureAfterExpirationFailsTheDrain() = runTest {
        val access = MigrationOwnerAccess(Any())
        val operationStarted = CompletableDeferred<Unit>()
        val releaseOperation = CompletableDeferred<Unit>()
        val expectedFailure = IllegalStateException("late operation failure")

        supervisorScope {
            val operation = async {
                access.withOperation {
                    operationStarted.complete(Unit)
                    releaseOperation.await()
                    throw expectedFailure
                }
            }

            operationStarted.await()
            access.expire()
            releaseOperation.complete(Unit)

            val drainFailure = assertFailsWith<IllegalStateException> {
                access.expireAndDrain(
                    cancelOperations = false,
                    propagateOperationFailure = true,
                )
            }
            assertSame(expectedFailure, drainFailure)
            val operationFailure = assertFailsWith<IllegalStateException> { operation.await() }
            assertEquals(expectedFailure.message, operationFailure.message)
        }
    }

    @Test
    fun handledOperationFailureBeforeExpirationIsNotReplayed() = runTest {
        val access = MigrationOwnerAccess(Any())
        val expectedFailure = IllegalStateException("handled operation failure")

        assertSame(
            expectedFailure,
            assertFailsWith {
                access.withOperation { throw expectedFailure }
            },
        )

        access.expire()
        access.expireAndDrain(
            cancelOperations = false,
            propagateOperationFailure = true,
        )
    }
}
