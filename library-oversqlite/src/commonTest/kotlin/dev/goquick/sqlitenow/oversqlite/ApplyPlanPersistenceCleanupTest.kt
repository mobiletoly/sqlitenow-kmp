package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.*

// Composed uncached-helper evidence; these are not raw finalize faults in private save.
internal class ApplyPlanPersistenceCleanupTest : CrossTargetSyncTestSupport() {
    @Test fun bodyFailureRemainsPrimaryWhenStatementCloseAlsoFails() = runOwnedWebDatabaseTest {
        helperFailure(bodyFails = true)
    }

    @Test fun closeOnlyFailureRollsBackAndLeavesConnectionReusable() = runOwnedWebDatabaseTest {
        helperFailure(bodyFails = false)
    }

    @Test fun callerCancellationKeepsCloseFailureAtHelperExitAndRollsBack() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE helper_rows(id INTEGER PRIMARY KEY)")
            val entered = CompletableDeferred<Unit>()
            val closeFailure = IllegalStateException("helper-close-fault")
            val cancellation = CancellationException("caller-cancel")
            var observed: Throwable? = null
            var helperFailure: Throwable? = null
            var closeCalls = 0
            var laterWork = false
            val job = launch(start = CoroutineStart.UNDISPATCHED) {
                try {
                    db.transaction(TransactionMode.IMMEDIATE) {
                        try {
                            db.withPreparedStatement("INSERT INTO helper_rows VALUES(1)", closeStatement = {
                                it.close(); closeCalls++; throw closeFailure
                            }) { st ->
                                st.step()
                                entered.complete(Unit)
                                CompletableDeferred<Unit>().await()
                                laterWork = true
                            }
                        } catch (failure: Throwable) { helperFailure = failure; throw failure }
                        laterWork = true
                    }
                } catch (failure: Throwable) { observed = failure; throw failure }
            }
            entered.await()
            job.cancel(cancellation)
            job.join()
            assertTrue(job.isCancelled)
            assertEquals(1, closeCalls, "the real statement closes before the injected close failure")
            assertSame(cancellation, originalFailure(observed!!))
            assertSame(cancellation, originalFailure(helperFailure!!))
            val chain = generateSequence(helperFailure) { it.cause }.toList()
            assertEquals(listOf(closeFailure), chain.flatMap { it.suppressedExceptions }.map(::originalFailure),
                "close failure occurs once across the coroutine recovery chain")
            assertEquals(1, closeCalls)
            assertFalse(laterWork)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM helper_rows"))
            db.execSQL("INSERT INTO helper_rows VALUES(2)")
            assertEquals(2L, scalarLong(db, "SELECT id FROM helper_rows"))
        } finally { db.close() }
    }

    private suspend fun helperFailure(bodyFails: Boolean) {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE helper_rows(id INTEGER PRIMARY KEY)")
            val bodyFailure = IllegalStateException("helper-body-fault")
            val closeFailure = IllegalStateException("helper-close-fault")
            var closeCalls = 0
            var laterWork = false
            val observed = assertFailsWith<IllegalStateException> {
                db.transaction(TransactionMode.IMMEDIATE) {
                    db.withPreparedStatement("INSERT INTO helper_rows VALUES(1)", closeStatement = {
                        it.close(); closeCalls++; throw closeFailure
                    }) { st -> st.step(); if (bodyFails) throw bodyFailure }
                    laterWork = true
                }
            }
            assertSame(if (bodyFails) bodyFailure else closeFailure, originalFailure(observed))
            assertEquals(if (bodyFails) listOf(closeFailure) else emptyList(), originalFailure(observed).suppressedExceptions.map(::originalFailure))
            assertEquals(1, closeCalls)
            assertFalse(laterWork)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM helper_rows"))
            db.execSQL("INSERT INTO helper_rows VALUES(2)")
            assertEquals(2L, scalarLong(db, "SELECT id FROM helper_rows"))
        } finally { db.close() }
    }

    // Coroutine stack-trace recovery may copy the throwable, retaining its original as cause.
    private fun originalFailure(error: Throwable): Throwable = generateSequence(error) { it.cause }.last()
}
