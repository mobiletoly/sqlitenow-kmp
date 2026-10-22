package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Verifies subscription ordering, retained invalidations and collection-owned cleanup.
 * Idle database close intentionally retains its existing caller-owned cancellation behavior.
 * All writes use real SQLite. A separate subscriber acknowledges the asynchronous notification.
 */
class SqliteNowReactiveObservationTest {
    @Test
    fun externalMeasurementChangeDuringInitialPublicationIsObservedWithoutAnotherWrite() = observationTest {
        assertPublicationGap(WriteNotification.EXTERNAL, "measurements")
    }

    @Test
    fun generatedStyleChangeDuringInitialPublicationIsObservedWithoutAnotherWrite() = observationTest {
        assertPublicationGap(WriteNotification.GENERATED_STYLE, "measurements")
    }

    @Test
    fun externalOwnershipChangeDuringInitialPublicationIsObservedWithoutAnotherWrite() = observationTest {
        assertPublicationGap(WriteNotification.EXTERNAL, "installations")
    }

    @Test
    fun changeAfterInitialSnapshotBeforeQueryReturnsIsObservedWithoutAnotherWrite() = observationTest {
        val changes = watchChanges()
        val snapshotRead = CompletableDeferred<Unit>()
        val releaseQuery = CompletableDeferred<Unit>()
        val reads = AtomicInteger()
        val observer = observe(database.valueFlow { value ->
            reads.incrementAndGet()
            if (value == 0L) {
                snapshotRead.complete(Unit)
                releaseQuery.await()
            }
        })
        snapshotRead.await()
        database.write("measurements", 1)
        changes.next() // The notification reached an already registered subscriber.
        assertEquals(1L, database.read("measurements"))
        releaseQuery.complete(Unit)
        assertEquals(0L, observer.next())
        assertEquals(1L, observer.next())
        assertEquals(2, reads.get())
        assertTrue(observer.job.isActive)
        assertEquals(1L, database.valueFlow().first())
        database.write("measurements", 2)
        changes.next()
        assertEquals(2L, observer.next())
    }

    @Test
    fun relevantChangeSurvivesLaterUnrelatedTableNotification() = observationTest {
        assertRelevantChangeSurvivesUnrelatedEvent(combined = false)
    }

    @Test
    fun combinedTableChangeSurvivesLaterUnrelatedTableNotification() = observationTest {
        assertRelevantChangeSurvivesUnrelatedEvent(combined = true)
    }

    @Test
    fun invalidationDuringSubscribedSlowQueryIsRetainedForFollowUpRead() = observationTest {
        assertSlowObserverRetainsChange(holdInQuery = true)
    }

    @Test
    fun invalidationDuringSubscribedSlowDownstreamIsRetainedForFollowUpRead() = observationTest {
        assertSlowObserverRetainsChange(holdInQuery = false)
    }

    @Test
    fun rapidInvalidationsDuringSlowQueryEventuallyPublishLatestCommittedState() = observationTest {
        val changes = watchChanges()
        val queryHeld = CompletableDeferred<Unit>()
        val releaseQuery = CompletableDeferred<Unit>()
        val activeQueries = AtomicInteger()
        val observer = observe(database.valueFlow { value ->
            assertEquals(1, activeQueries.incrementAndGet(), "Queries for one observer must not overlap")
            try {
                if (value == 1L) {
                    queryHeld.complete(Unit)
                    releaseQuery.await()
                }
            } finally {
                activeQueries.decrementAndGet()
            }
        })
        assertEquals(0L, observer.next())
        settleObservers() // Initial publication returned; the change subscription is registered.
        database.write("measurements", 1)
        changes.next()
        queryHeld.await() // Query read 1 and released SQLite before the test pauses it.
        for (value in 2L..12L) database.write("measurements", value)
        repeat(11) { changes.next() } // The independent listener can drain the entire burst.
        assertEquals(12L, database.read("measurements"))
        releaseQuery.complete(Unit)
        assertEquals(1L, observer.next())
        var latest = observer.next()
        while (latest != 12L) latest = observer.next()
        assertTrue(observer.job.isActive)
    }

    @Test
    fun emptyAffectedTablesEmitsOnceAndCompletes() = observationTest {
        val reads = AtomicInteger()
        assertEquals(
            listOf(0L),
            database.valueFlow(affectedTables = emptySet()) { reads.incrementAndGet() }.toList(),
        )
        assertEquals(1, reads.get())
    }

    @Test
    fun firstAndTakeCompleteWithoutLeavingTheirListenerRunning() = observationTest {
        assertEquals(0L, database.valueFlow().first())
        val changes = watchChanges()
        val observer = observe(database.valueFlow().take(2))
        assertEquals(0L, observer.next())
        database.write("measurements", 1)
        changes.next()
        assertEquals(1L, observer.next())
        observer.job.join() // A leaked structured listener would prevent completion.
        assertTrue(observer.job.isCompleted)
        assertEquals(1L, database.valueFlow().first())
    }

    @Test
    fun initialQueryFailureReleasesItsListenerAndAllowsNewObservation() = observationTest {
        assertQueryFailureReleasesListener(failOnFollowUp = false)
    }

    @Test
    fun followUpQueryFailureReleasesItsListenerAndAllowsNewObservation() = observationTest {
        assertQueryFailureReleasesListener(failOnFollowUp = true)
    }

    @Test
    fun downstreamFailureReleasesItsListenerAndAllowsNewObservation() = observationTest {
        val failure = IllegalStateException("downstream failure")
        val failures = Channel<Throwable>(Channel.UNLIMITED)
        val observer = observe(database.valueFlow(), failures) { throw failure }
        assertEquals(0L, observer.next())
        assertPropagatedFailure(failure, failures.receive())
        observer.job.join()
        assertTrue(observer.job.isCompleted)
        assertReplacementObservesNextWrite()
    }

    @Test
    fun multipleAffectedTablesAndIndependentObserversReceiveMatchingChanges() = observationTest {
        val changes = watchChanges()
        val measurements = observe(database.valueFlow())
        val installations = observe(database.valueFlow("installations"))
        val combined = observe(database.combinedFlow())
        assertEquals(0L, measurements.next())
        assertEquals(0L, installations.next())
        assertEquals(0L to 0L, combined.next())
        settleObservers()

        database.write("measurements", 1, reportedTables = setOf("MEASUREMENTS"))
        changes.next()
        assertEquals(1L, measurements.next())
        assertEquals(1L to 0L, combined.next())
        settleObservers()
        assertTrue(installations.values.tryReceive().isFailure)

        database.write("installations", 2)
        changes.next()
        assertEquals(2L, installations.next())
        assertEquals(1L to 2L, combined.next())
        settleObservers()
        assertTrue(measurements.values.tryReceive().isFailure)

        database.transaction {
            database.connection().execSQL("UPDATE measurements SET value = 3 WHERE id = 1")
            database.connection().execSQL("UPDATE installations SET value = 4 WHERE id = 1")
        }
        database.reportExternalTableChanges(setOf("measurements", "INSTALLATIONS"))
        changes.next()
        assertEquals(3L, measurements.next())
        assertEquals(4L, installations.next())
        assertEquals(3L to 4L, combined.next())
    }

    @Test
    fun databaseCloseLeavesIdleObservationActiveUntilCollectorCancellation() = observationTest {
        val observer = observe(database.valueFlow())
        assertEquals(0L, observer.next())
        settleObservers()
        database.close()
        settleObservers()
        assertTrue(observer.job.isActive, "Closing the producer scope does not complete SharedFlow")
        assertTrue(observer.values.tryReceive().isFailure)
        assertFailsWith<IllegalStateException> { database.valueFlow().first() }
        assertFailsWith<IllegalStateException> { database.reportExternalTableChanges(setOf("measurements")) }
        observer.job.cancelAndJoin()
        assertTrue(observer.job.isCancelled)
    }

    @Test
    fun cancellingHeldInitialCollectorTerminatesItAndAllowsNewObservation() = observationTest {
        val releaseInitial = CompletableDeferred<Unit>()
        val observer = observe(database.valueFlow()) { releaseInitial.await() }
        assertEquals(0L, observer.next())
        observer.job.cancelAndJoin()
        assertTrue(observer.job.isCancelled)
        val changes = watchChanges()
        val replacement = observe(database.valueFlow())
        assertEquals(0L, replacement.next())
        settleObservers()
        database.write("measurements", 1)
        changes.next()
        assertEquals(1L, replacement.next())
    }

    @Test
    fun cancellingHeldFollowUpQueryTerminatesItsListenerAndAllowsNewObservation() = observationTest {
        val changes = watchChanges()
        val queryHeld = CompletableDeferred<Unit>()
        val releaseQuery = CompletableDeferred<Unit>()
        val observer = observe(database.valueFlow { value ->
            if (value == 1L) {
                queryHeld.complete(Unit)
                releaseQuery.await()
            }
        })
        assertEquals(0L, observer.next())
        database.write("measurements", 1)
        changes.next()
        queryHeld.await()
        observer.job.cancelAndJoin()
        assertTrue(observer.job.isCancelled)
        assertReplacementObservesNextWrite()
    }

    private suspend fun Fixture.assertQueryFailureReleasesListener(failOnFollowUp: Boolean) {
        val changes = watchChanges()
        val failure = IllegalStateException("query failure")
        val failures = Channel<Throwable>(Channel.UNLIMITED)
        val observer = observe(database.valueFlow { value ->
            if (!failOnFollowUp || value == 1L) throw failure
        }, failures)
        if (failOnFollowUp) {
            assertEquals(0L, observer.next())
            database.write("measurements", 1)
            changes.next()
        }
        assertPropagatedFailure(failure, failures.receive())
        observer.job.join()
        assertTrue(observer.job.isCompleted)
        assertReplacementObservesNextWrite()
    }

    private fun assertPropagatedFailure(expected: Throwable, actual: Throwable) {
        assertEquals(expected::class, actual::class)
        assertEquals(expected.message, actual.message)
        // Coroutine stack recovery can copy the exception while retaining its original cause.
        assertSame(expected, generateSequence(actual) { it.cause }.last())
    }

    private suspend fun Fixture.assertReplacementObservesNextWrite() {
        val changes = watchChanges()
        val observer = observe(database.valueFlow())
        assertEquals(database.read("measurements"), observer.next())
        database.write("measurements", 2)
        changes.next()
        assertEquals(2L, observer.next())
    }

    private suspend fun Fixture.assertRelevantChangeSurvivesUnrelatedEvent(combined: Boolean) {
        val changes = watchChanges()
        val releaseInitial = CompletableDeferred<Unit>()
        val flow = if (combined) database.combinedFlow() else database.valueFlow().map { it to 0L }
        val observer = observe(flow) { if (it == (0L to 0L)) releaseInitial.await() }
        assertEquals(0L to 0L, observer.next())
        database.write("measurements", 1)
        changes.next()
        database.write("unrelated", 1)
        changes.next()
        releaseInitial.complete(Unit)
        assertEquals(1L to 0L, observer.next())
    }

    private suspend fun Fixture.assertPublicationGap(route: WriteNotification, table: String) {
        val changes = watchChanges()
        val releaseInitial = CompletableDeferred<Unit>()
        val reads = AtomicInteger()
        val observer = observe(database.valueFlow(table) { reads.incrementAndGet() }) { value ->
            if (value == 0L) releaseInitial.await()
        }
        assertEquals(0L, observer.next()) // SQLite snapshot obtained; initial downstream still held.
        database.write(table, 1, route)
        changes.next() // Real notification processed while initial publication is still held.
        assertEquals(1L, database.read(table))
        assertEquals(1, reads.get())
        releaseInitial.complete(Unit)
        assertEquals(1L, observer.next(), "The acknowledged change must survive initial publication")
        assertEquals(2, reads.get())
        assertTrue(observer.job.isActive)
        assertEquals(1L, database.valueFlow(table).first(), "Restart reads the committed state")
        database.write(table, 2, route)
        changes.next()
        assertEquals(2L, observer.next(), "A subsequent write proves the original observer is alive")
        assertEquals(3, reads.get())
    }

    private suspend fun Fixture.assertSlowObserverRetainsChange(holdInQuery: Boolean) {
        val changes = watchChanges()
        val held = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val reads = AtomicInteger()
        val pause: suspend (Long) -> Unit = { value ->
            if (value == 1L) {
                held.complete(Unit)
                release.await()
            }
        }
        val observer = observe(database.valueFlow { value ->
            reads.incrementAndGet()
            if (holdInQuery) pause(value)
        }) { value -> if (!holdInQuery) pause(value) }
        assertEquals(0L, observer.next())
        settleObservers()
        database.write("measurements", 1)
        changes.next()
        held.await()
        database.write("measurements", 2)
        changes.next() // The next invalidation is in the buffer while the existing consumer is held.
        release.complete(Unit)
        assertEquals(1L, observer.next())
        assertEquals(2L, observer.next())
        assertEquals(3, reads.get())
    }

    private fun observationTest(block: suspend Fixture.() -> Unit) = runBlocking {
        coroutineScope {
            val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
            val database = ObservationDatabase()
            val fixture = Fixture(database, this, dispatcher)
            try {
                withTimeout(10_000) {
                    database.open()
                    fixture.block()
                }
            } finally {
                withContext(NonCancellable) {
                    fixture.collectors.forEach { it.cancel() }
                    fixture.collectors.forEach { it.join() }
                    if (database.isOpen()) database.close()
                }
                dispatcher.close()
            }
        }
    }

    private class Fixture(
        val database: ObservationDatabase,
        private val scope: CoroutineScope,
        private val dispatcher: ExecutorCoroutineDispatcher,
    ) {
        val collectors = mutableListOf<Job>()

        fun <T> observe(
            flow: Flow<T>,
            failures: Channel<Throwable>? = null,
            onValue: suspend (T) -> Unit = {},
        ): Observation<T> {
            val values = Channel<T>(Channel.UNLIMITED)
            val job = scope.launch(dispatcher) {
                try {
                    flow.collect { value ->
                        values.send(value)
                        onValue(value)
                    }
                } catch (failure: Throwable) {
                    if (failure is CancellationException || failures == null) throw failure
                    failures.send(failure)
                }
            }
            collectors += job
            return Observation(values, job)
        }

        suspend fun watchChanges(): Observation<Unit> {
            val observer = observe(database.changes())
            settleObservers() // No SQLite query precedes this collect: first suspension is subscribed.
            return observer
        }

        // A FIFO serial-executor marker runs after resumed collectors reach their next suspension.
        // Raw change-flow collection has no suspension before SharedFlow registration. Reactive
        // query flows register their child listener before reading the initial SQLite state.
        suspend fun settleObservers() = withContext(dispatcher) {}
    }

    private class Observation<T>(val values: Channel<T>, val job: Job) {
        suspend fun next(): T = values.receive()
    }

    private enum class WriteNotification { EXTERNAL, GENERATED_STYLE }

    private class ObservationDatabase : SqliteNowDatabase(":memory:", ObservationMigration()) {
        fun valueFlow(
            table: String = "measurements",
            affectedTables: Set<String> = setOf(table),
            afterSnapshot: suspend (Long) -> Unit = {},
        ): Flow<Long> = createReactiveQueryFlow(affectedTables) {
            read(table).also { afterSnapshot(it) }
        }

        fun combinedFlow(): Flow<Pair<Long, Long>> = createReactiveQueryFlow(setOf("measurements", "installations")) {
            connection().withExclusiveAccess { read("measurements") to read("installations") }
        }

        suspend fun changes(): Flow<Unit> = createTableChangeFlow(TABLES)

        suspend fun read(table: String): Long {
            require(table in TABLES)
            return connection().prepare("SELECT value FROM $table WHERE id = 1").use {
                check(it.step())
                it.getLong(0)
            }
        }

        suspend fun write(
            table: String,
            value: Long,
            route: WriteNotification = WriteNotification.EXTERNAL,
            reportedTables: Set<String> = setOf(table),
        ) {
            require(table in TABLES)
            transaction { connection().execSQL("UPDATE $table SET value = $value WHERE id = 1") }
            check(!connection().inTransaction())
            when (route) {
                WriteNotification.EXTERNAL -> reportExternalTableChanges(reportedTables)
                // Mirrors the compiler's execute-then-notify wrapper, without generating/editing code.
                WriteNotification.GENERATED_STYLE -> notifyTablesChanged(reportedTables)
            }
        }
    }

    private class ObservationMigration : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                for (table in TABLES) {
                    conn.execSQL("CREATE TABLE $table(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
                    conn.execSQL("INSERT INTO $table(id, value) VALUES(1, 0)")
                }
                return 0
            }
            return currentVersion
        }
    }

    private companion object {
        val TABLES = setOf("measurements", "installations", "unrelated")
    }
}
