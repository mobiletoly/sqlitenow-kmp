package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.datetime.LocalDate
import kotlin.test.*

/**
 * Integration coverage ensuring constant literals across INSERT/UPDATE/DELETE/SELECT
 * statements are accepted and mapped correctly by the generated runtime.
 */
class ConstantValueStatementsTest {

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
    fun testConstantAssignments() = runDatabaseTest {
            database.open()

            val person = database.person.add.one(
                PersonQuery.Add.Params(
                    email = "constant@example.com",
                    firstName = "Constant",
                    lastName = "Assignments",
                    phone = "+1000000000",
                    birthDate = LocalDate(1995, 1, 1)
                )
            )

            val insertedAddress = database.personAddress.addWithConstants.one(
                PersonAddressQuery.AddWithConstants.Params(
                    personId = person.id,
                    addressType = AddressType.HOME,
                    street = "123 Constant Ave",
                    city = "Literal City"
                )
            )

            assertEquals(person.id, insertedAddress.personId)
            assertEquals(AddressType.HOME, insertedAddress.addressType)
            assertEquals("US", insertedAddress.country)
            assertTrue(insertedAddress.isPrimary)
            assertNull(insertedAddress.state, "State should be null on insert")
            assertNotNull(insertedAddress.createdAt, "Created at should be populated from CURRENT_TIMESTAMP")

            val updatedAddress = database.personAddress.updateConstantFlags.one(
                PersonAddressQuery.UpdateConstantFlags.Params(
                    state = "CA",
                    addressId = insertedAddress.id
                )
            )

            assertEquals("CA", updatedAddress.state)
            assertEquals("CA", updatedAddress.country)
            assertFalse(updatedAddress.isPrimary)
            assertNotNull(updatedAddress.createdAt, "Created at should be refreshed on update")

            val selectResults = database.personAddress.selectWithConstantColumns(
                PersonAddressQuery.SelectWithConstantColumns.Params(
                    personId = person.id
                )
            ).asList()

            assertEquals(1, selectResults.size)
            val selectRow = selectResults.first()
            assertEquals(updatedAddress.id, selectRow.id)
            assertFalse(selectRow.isPrimary)
            val constantInt = selectRow.constantInt
            assertNotNull(constantInt)
            assertEquals(42, constantInt)
            val constantReal = selectRow.constantReal
            assertNotNull(constantReal)
            assertEquals(3.14, constantReal, 0.0001)
            assertNotNull(selectRow.constantTimestamp)
            assertNull(selectRow.constantNull)

            database.personAddress.deleteSecondaryByPerson(
                PersonAddressQuery.DeleteSecondaryByPerson.Params(
                    personId = person.id
                )
            )

            val remaining = database.personAddress.selectWithConstantColumns(
                PersonAddressQuery.SelectWithConstantColumns.Params(
                    personId = person.id
                )
            ).asList()
            assertTrue(remaining.isEmpty())
    }
}
