package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddressRow
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class ForeignKeyCascadeNotificationTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setup() {
        database = TestDatabaseHelper.createDatabase()
    }

    @After
    fun teardown() {
        runBlocking {
            database.close()
        }
    }

    @Test
    fun cascadingUpdateAndDeleteNotifyChildTable() {
        runBlocking {
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

            val flow = database.personAddress.selectAll.asFlow()
            val emissions = mutableListOf<List<PersonAddressRow>>()
            val collector = launch {
                flow.collect { rows ->
                    emissions += rows
                }
            }

            suspend fun awaitEmissions(target: Int) {
                withTimeout(5_000) {
                    while (emissions.size < target) {
                        delay(25)
                    }
                }
            }

            awaitEmissions(1)
            assertEquals("Expected one initial emission", 1, emissions.size)
            assertEquals("Initial emission should contain the inserted address", 1, emissions.first().size)

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
            assertEquals("Cascade update should preserve address rows", 1, afterUpdate.size)
            assertEquals("Cascade update should not alter address identity", initialAddressId, afterUpdate.first().id)

            database.person.deleteByIdReturning.one(PersonQuery.DeleteByIdReturning.Params(id = person.id))

            awaitEmissions(3)
            val afterDelete = emissions[2]
            collector.cancelAndJoin()

            assertTrue("Cascade should remove addresses from the table", database.personAddress.selectAll.asList().isEmpty())
            assertEquals("Cascade path should eventually clear child rows", 0, afterDelete.size)
        }
    }
}
