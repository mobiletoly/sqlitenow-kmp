package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlin.test.*

class ForeignKeyCascadeNotificationTest {

    private lateinit var database: LibraryTestDatabase

    private fun runDatabaseTest(block: suspend () -> Unit) = runPlatformTest {
        val lease = TestDatabaseHelper.createDatabaseLease()
        database = lease.database
        try {
            block()
        } finally {
            lease.closeAndCleanup()
        }
    }

    @Test
    fun cascadingUpdateAndDeleteNotifyChildTable() = runDatabaseTest {
            database.open()
            database.connection().execSQL("PRAGMA foreign_keys = ON")

            val person = database.person.add.one(
                PersonQuery.Add.Params(
                    email = "cascade@example.com",
                    firstName = "Cascade",
                    lastName = "Parent",
                    phone = "555-9999",
                    birthDate = LocalDate(1990, 1, 1),
                ),
            )

            database.personAddress.add(
                PersonAddressQuery.Add.Params(
                    personId = person.id,
                    addressType = AddressType.HOME,
                    street = "123 Cascade St",
                    city = "Cascade City",
                    state = "CA",
                    postalCode = "90001",
                    country = "USA",
                    isPrimary = true,
                ),
            )

            val initialRows = database.personAddress.selectAll.asList()
            assertEquals(1, initialRows.size, "The inserted address must be visible")
            database.enableTableChangeNotifications()

            // This is the first notified write in a fresh lease. Equal child rows
            // must still be published for the declared parent-update dependency.
            database.personAddress.selectAll.asFlow().observeCascadePhase(initialRows, initialRows) {
                database.person.updateById(
                    PersonQuery.UpdateById.Params(
                        id = person.id,
                        firstName = "UpdatedCascade",
                        lastName = "Parent",
                        email = person.email,
                        phone = person.phone,
                        birthDate = person.birthDate,
                    ),
                )
            }
            assertEquals(initialRows, database.personAddress.selectAll.asList())

            database.personAddress.selectAll.asFlow().observeCascadePhase(initialRows, emptyList()) {
                database.person.deleteByIdReturning.one(PersonQuery.DeleteByIdReturning.Params(id = person.id))
            }
            assertTrue(database.personAddress.selectAll.asList().isEmpty(), "Cascade should remove addresses")
    }
}

/** A fresh subscription acknowledges its initial query before the mutation. */
internal suspend fun <T> Flow<List<T>>.observeCascadePhase(
    before: List<T>,
    after: List<T>,
    changeTimeoutMs: Long = 5_000,
    mutate: suspend () -> Unit,
) = coroutineScope {
    val observations = Channel<List<T>>(Channel.UNLIMITED)
    val releaseInitial = CompletableDeferred<Unit>()
    val collector = launch(start = CoroutineStart.UNDISPATCHED) {
        var initial = true
        collect { rows ->
            observations.send(rows)
            if (initial) {
                initial = false
                releaseInitial.await()
            }
        }
    }
    try {
        assertEquals(before, withRealTimeout(5_000) { observations.receive() })
        mutate()
        releaseInitial.complete(Unit)
        assertEquals(after, awaitCascadeRows(observations, changeTimeoutMs) { it == after })
    } finally {
        releaseInitial.complete(Unit)
        collector.cancelAndJoin()
        observations.cancel()
    }
}

internal suspend fun <T> awaitCascadeRows(
    observations: ReceiveChannel<List<T>>,
    timeoutMs: Long = 5_000,
    accept: (List<T>) -> Boolean,
): List<T> = withRealTimeout(timeoutMs) {
    var rows = observations.receive()
    while (!accept(rows)) rows = observations.receive()
    rows
}

/** Duplicated old values cannot satisfy deletion; a missing transition fails closed. */
internal suspend fun verifyCascadeObservationControls() {
    val duplicate = Channel<List<Int>>(Channel.UNLIMITED)
    duplicate.trySend(listOf(1))
    duplicate.trySend(listOf(1))
    duplicate.trySend(emptyList())
    duplicate.close()
    assertEquals(emptyList(), awaitCascadeRows(duplicate) { it.isEmpty() })
    val lost = Channel<List<Int>>(Channel.UNLIMITED)
    lost.trySend(listOf(1))
    lost.trySend(listOf(1))
    lost.close()
    assertFailsWith<ClosedReceiveChannelException> {
        awaitCascadeRows(lost) { it.isEmpty() }
    }
}
