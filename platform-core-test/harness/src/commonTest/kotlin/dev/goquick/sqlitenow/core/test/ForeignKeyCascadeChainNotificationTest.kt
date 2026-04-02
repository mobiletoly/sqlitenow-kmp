package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressNoteQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddressNoteRow
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlin.test.*

/**
 * Verifies that multi-level foreign-key cascades propagate through reactive notifications.
 * Deleting a person should update both person_address and person_address_note flows.
 */
class ForeignKeyCascadeChainNotificationTest {

    private lateinit var database: LibraryTestDatabase

    private fun runDatabaseTest(block: suspend () -> Unit) = runPlatformTest {
        database = TestDatabaseHelper.createDatabase()
        try {
            block()
        } finally {
            database.close()
        }
    }

    @Test
    fun cascadingDeleteNotifiesGrandchildTable() = runDatabaseTest {
            database.prepareCascadeScenario {
                awaitEmission(target = 1)
                assertEquals(1, emissions.first().size, "Initial note should be visible")

                db.person.deleteByIdReturning.one(
                    PersonQuery.DeleteByIdReturning.Params(id = personId),
                )

                awaitEmission(target = 2, timeoutMs = 1_000)
                val secondEmission = emissions[1]

                assertTrue(db.personAddressNote.selectAll.asList().isEmpty(), "Cascade should remove notes from the table")
                assertEquals(0, secondEmission.size, "Reactive flow should emit empty list after cascade")
            }
    }

    @Test
    fun cascadingUpdateNotifiesGrandchildTable() = runDatabaseTest {
            database.prepareCascadeScenario {
                awaitEmission(target = 1)
                assertEquals(1, emissions.first().size, "Initial note should be visible")

                db.person.updateById(
                    PersonQuery.UpdateById.Params(
                        id = personId,
                        firstName = "Updated",
                        lastName = "Cascade",
                        email = "multi@example.com",
                        phone = "555-1000",
                        birthDate = LocalDate(1992, 6, 15),
                    ),
                )

                awaitEmission(target = 2, timeoutMs = 1_000)
                val secondEmission = emissions[1]

                assertEquals(1, secondEmission.size, "Reactive flow should emit the existing note after update")
                assertEquals("first note", secondEmission.first().note)
            }
    }

    private data class CascadeContext(
        val db: LibraryTestDatabase,
        val personId: Long,
        val addressId: Long,
        val emissions: MutableList<List<PersonAddressNoteRow>>,
        private val awaitEmissionFn: suspend (Int, Long) -> Unit,
    ) {
        suspend fun awaitEmission(target: Int, timeoutMs: Long = 5_000) {
            awaitEmissionFn(target, timeoutMs)
        }
    }

    private suspend fun LibraryTestDatabase.prepareCascadeScenario(
        block: suspend CascadeContext.() -> Unit,
    ) = coroutineScope {
        open()
        enableTableChangeNotifications()
        connection().execSQL("PRAGMA foreign_keys = ON")

        val person = person.add.one(
            PersonQuery.Add.Params(
                email = "multi@example.com",
                firstName = "Multi",
                lastName = "Cascade",
                phone = "555-1000",
                birthDate = LocalDate(1992, 6, 15),
            ),
        )

        val address = personAddress.addReturning.one(
            PersonAddressQuery.AddReturning.Params(
                personId = person.id,
                addressType = AddressType.HOME,
                street = "1 Cascade Way",
                city = "Chainville",
                state = "CA",
                postalCode = "90210",
                country = "USA",
                isPrimary = true,
            ),
        )

        personAddressNote.add(
            PersonAddressNoteQuery.Add.Params(
                addressId = address.id,
                note = "first note",
            ),
        )

        val flow = personAddressNote.selectAll.asFlow()
        val emissions = mutableListOf<List<PersonAddressNoteRow>>()
        val collector = launch {
            flow.collect { rows ->
                emissions += rows
            }
        }

        suspend fun awaitEmission(target: Int, timeoutMs: Long) {
            val success = awaitConditionWithRealTimeout(timeoutMs = timeoutMs) {
                emissions.size >= target
            }
            assertTrue(success == true, "Expected at least $target emissions")
        }

        val context = CascadeContext(
            db = this@prepareCascadeScenario,
            personId = person.id,
            addressId = address.id,
            emissions = emissions,
            awaitEmissionFn = ::awaitEmission,
        )

        try {
            context.block()
        } finally {
            collector.cancelAndJoin()
        }
    }
}
