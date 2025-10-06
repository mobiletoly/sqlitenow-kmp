package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Basic collection mapping tests to validate core functionality.
 */
@RunWith(AndroidJUnit4::class)
class BasicCollectionTest {

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
    fun testBasicCollectionMapping() {
        runBlocking {
            database.open()

            // Setup: Create a person with multiple addresses
            val person = database.person.add(PersonQuery.Add.Params(
                firstName = "John",
                lastName = "Doe",
                email = "john.doe@example.com",
                phone = "555-0123",
                birthDate = LocalDate(1990, 5, 15)
            )).executeReturningOne()

            database.personAddress.add(PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.HOME,
                street = "123 Main St",
                city = "Springfield",
                state = "IL",
                postalCode = "62701",
                country = "USA",
                isPrimary = true
            )).execute()

            database.personAddress.add(PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.WORK,
                street = "456 Business Ave",
                city = "Springfield",
                state = "IL",
                postalCode = "62702",
                country = "USA",
                isPrimary = false
            )).execute()

            // Test: Query person with addresses collection
            val results = database.person.selectAllWithAddresses(PersonQuery.SelectAllWithAddresses.Params(
                limit = 10,
                offset = 0
            )).asList()

            // Verify: Collection mapping works correctly
            assertEquals(1, results.size)
            val personWithAddresses = results[0]
            assertEquals(person.id, personWithAddresses.personId)
            assertEquals("John", personWithAddresses.myFirstName)
            assertEquals("Doe", personWithAddresses.myLastName)

            // Verify addresses collection
            assertEquals(2, personWithAddresses.addresses.size)
            val homeAddress = personWithAddresses.addresses.find { it.addressType == AddressType.HOME }
            val workAddress = personWithAddresses.addresses.find { it.addressType == AddressType.WORK }

            assertNotNull(homeAddress)
            assertNotNull(workAddress)
            assertEquals("123 Main St", homeAddress!!.street)
            assertEquals("456 Business Ave", workAddress!!.street)
            assertTrue(homeAddress.isPrimary)
            assertFalse(workAddress.isPrimary)
        }
    }

    @Test
    fun testEmptyCollectionMapping() {
        runBlocking {
            database.open()

            // Setup: Create a person with no addresses
            val person = database.person.add(PersonQuery.Add.Params(
                firstName = "Jane",
                lastName = "Smith",
                email = "jane.smith@example.com",
                phone = null,
                birthDate = null
            )).executeReturningOne()

            // Test: Query person with addresses collection
            val results = database.person.selectAllWithAddresses(PersonQuery.SelectAllWithAddresses.Params(
                limit = 10,
                offset = 0
            )).asList()

            // Verify: Empty collection is handled correctly
            assertEquals(1, results.size)
            val personWithAddresses = results[0]
            assertEquals(person.id, personWithAddresses.personId)
            assertEquals("Jane", personWithAddresses.myFirstName)

            // Verify empty addresses collection
            assertTrue(personWithAddresses.addresses.isEmpty())
            assertTrue(personWithAddresses.comments.isEmpty())
        }
    }

    @Test
    fun testCollectionMappingQueryRestrictions() {
        runBlocking {
            database.open()

            // Setup: Create person with addresses
            val person = database.person.add(PersonQuery.Add.Params(
                firstName = "Query",
                lastName = "Restrictions",
                email = "restrictions@example.com",
                phone = "555-RESTRICT",
                birthDate = LocalDate(1992, 8, 25)
            )).executeReturningOne()

            database.personAddress.add(PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.HOME,
                street = "456 Restrict Ave",
                city = "Limitation",
                state = "TX",
                postalCode = "75001",
                country = "USA",
                isPrimary = true
            )).execute()

            // Test: Collection queries don't support asOne() and asOneOrNull()
            val selectRunners = database.person.selectAllWithAddresses(PersonQuery.SelectAllWithAddresses.Params(
                limit = 10,
                offset = 0
            ))

            // Verify: asOne() throws UnsupportedOperationException
            try {
                selectRunners.asOne()
                fail("Expected UnsupportedOperationException for asOne() on collection query")
            } catch (e: UnsupportedOperationException) {
                assertTrue(e.message?.contains("asOne() is not supported for collection mapping queries") == true)
            }

            // Verify: asOneOrNull() throws UnsupportedOperationException
            try {
                selectRunners.asOneOrNull()
                fail("Expected UnsupportedOperationException for asOneOrNull() on collection query")
            } catch (e: UnsupportedOperationException) {
                assertTrue(e.message?.contains("asOneOrNull() is not supported for collection mapping queries") == true)
            }

            // Verify: asList() works correctly
            val results = selectRunners.asList()
            assertEquals(1, results.size)
            assertEquals(person.id, results[0].personId)
        }
    }

    @Test
    fun selectAllByLastNamesUsesCollectionParameter() {
        runBlocking {
            database.open()

            val doe = database.person.add(
                PersonQuery.Add.Params(
                    firstName = "Alice",
                    lastName = "Doe",
                    email = "alice.doe@example.com",
                    phone = null,
                    birthDate = null,
                ),
            ).executeReturningOne()

            val smith = database.person.add(
                PersonQuery.Add.Params(
                    firstName = "Bob",
                    lastName = "Smith",
                    email = "bob.smith@example.com",
                    phone = null,
                    birthDate = null,
                ),
            ).executeReturningOne()

            database.person.add(
                PersonQuery.Add.Params(
                    firstName = "Charlie",
                    lastName = "Excluded",
                    email = "charlie@example.com",
                    phone = null,
                    birthDate = null,
                ),
            ).executeReturningOne()

            val params = PersonQuery.SelectAllByLastNames.Params(
                lastNames = listOf("Doe", "Smith"),
            )

            val results = database.person.selectAllByLastNames(params).asList()
            assertEquals(2, results.size)
            val resultIds = results.map { it.id }.toSet()
            assertTrue(resultIds.containsAll(listOf(doe.id, smith.id)))
            assertTrue(results.all { it.myLastName in listOf("Doe", "Smith") })

            val none = database.person.selectAllByLastNames(
                PersonQuery.SelectAllByLastNames.Params(lastNames = emptyList()),
            ).asList()
            assertTrue(none.isEmpty())
        }
    }
}
