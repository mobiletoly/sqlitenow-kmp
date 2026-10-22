package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressNoteQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddressNoteRow
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.datetime.LocalDate
import kotlin.test.*

/**
 * Verifies that multi-level foreign-key cascades propagate through reactive notifications.
 * Deleting a person should update both person_address and person_address_note flows.
 */
class ForeignKeyCascadeChainNotificationTest {

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
    fun cascadingDeleteNotifiesGrandchildTable() = runDatabaseTest {
            verifyCascadeObservationControls()
            database.prepareCascadeScenario {
                db.personAddressNote.selectAll.asFlow().observeCascadePhase(initialNotes, emptyList(), changeTimeoutMs = 1_000) {
                    db.person.deleteByIdReturning.one(PersonQuery.DeleteByIdReturning.Params(id = personId))
                }
                assertTrue(db.personAddressNote.selectAll.asList().isEmpty(), "Cascade should remove notes")
            }
    }

    @Test
    fun cascadingUpdateNotifiesGrandchildTable() = runDatabaseTest {
            database.prepareCascadeScenario {
                db.personAddressNote.selectAll.asFlow().observeCascadePhase(initialNotes, initialNotes, changeTimeoutMs = 1_000) {
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

                }
                assertEquals(initialNotes, db.personAddressNote.selectAll.asList())
                assertEquals("first note", initialNotes.single().note)
            }
    }

    private data class CascadeContext(
        val db: LibraryTestDatabase,
        val personId: Long,
        val addressId: Long,
        val initialNotes: List<PersonAddressNoteRow>,
    )

    private suspend fun LibraryTestDatabase.prepareCascadeScenario(
        block: suspend CascadeContext.() -> Unit,
    ) {
        open()
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

        val initialNotes = personAddressNote.selectAll.asList()
        assertEquals(1, initialNotes.size, "Initial note should be visible")
        enableTableChangeNotifications()
        CascadeContext(this, person.id, address.id, initialNotes).block()
    }
}
