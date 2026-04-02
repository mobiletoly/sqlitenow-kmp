package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddressRow
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlin.test.*

class ForeignKeyCascadeNotificationTest {

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
    fun cascadingUpdateAndDeleteNotifyChildTable() = runDatabaseTest {
            database.open()
            database.enableTableChangeNotifications()
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

            coroutineScope {
                val flow = database.personAddress.selectAll.asFlow()
                val emissions = mutableListOf<List<PersonAddressRow>>()
                val collector = launch {
                    flow.collect { rows ->
                        emissions += rows
                    }
                }

                suspend fun awaitEmissions(target: Int) {
                    assertTrue(awaitConditionWithRealTimeout(timeoutMs = 5_000) { emissions.size >= target }, "Expected at least $target emissions")
                }

                awaitEmissions(1)
                assertEquals(1, emissions.size, "Expected one initial emission")
                assertEquals(1, emissions.first().size, "Initial emission should contain the inserted address")

                val initialAddressId = emissions.first().first().id

                database.person.updateById(
                    PersonQuery.UpdateById.Params(
                        id = person.id,
                        firstName = "UpdatedCascade",
                        lastName = "Parent",
                        email = person.email,
                        phone = person.phone,
                        birthDate = person.birthDate
                    )
                )

                awaitEmissions(2)
                val afterUpdate = emissions[1]
                assertEquals(1, afterUpdate.size, "Cascade update should preserve address rows")
                assertEquals(initialAddressId, afterUpdate.first().id, "Cascade update should not alter address identity")

                database.person.deleteByIdReturning.one(PersonQuery.DeleteByIdReturning.Params(id = person.id))

                awaitEmissions(3)
                val afterDelete = emissions[2]
                collector.cancelAndJoin()

                assertTrue(database.personAddress.selectAll.asList().isEmpty(), "Cascade should remove addresses from the table")
                assertEquals(0, afterDelete.size, "Cascade path should eventually clear child rows")
            }
    }
}
