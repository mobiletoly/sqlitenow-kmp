package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddResult
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.datetime.LocalDate
import kotlin.test.*

/**
 * Comprehensive integration tests for SQLiteNow UPDATE operations.
 * Ensures ExecuteStatement wrappers cover UPDATE scenarios.
 */
class UpdateOperationsTest {

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
    fun testBasicUpdateOperation() = runDatabaseTest {
            database.open()
            
            // First insert a person to update
            val originalPerson = database.person.add.one(PersonQuery.Add.Params(
                email = "basic-update@example.com",
                firstName = "Original",
                lastName = "Name",
                phone = "+1111111111",
                birthDate = LocalDate(1990, 1, 1)
            ))
            
            // Update the person
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "Updated",
                lastName = "NewName",
                email = "updated-basic@example.com",
                phone = "+2222222222",
                birthDate = LocalDate(1991, 2, 2),
                id = originalPerson.id
            )
            
            database.person.updateById(updateParams)
            
            // Verify the update
            val updatedPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == originalPerson.id }
            
            assertNotNull(updatedPerson, "Updated person should be found")
            assertEquals(originalPerson.id, updatedPerson.id, "ID should remain the same")
            assertEquals("Updated", updatedPerson.myFirstName, "First name should be updated")
            assertEquals("NewName", updatedPerson.myLastName, "Last name should be updated")
            assertEquals("updated-basic@example.com", updatedPerson.email, "Email should be updated")
            assertEquals("+2222222222", updatedPerson.phone, "Phone should be updated")
            assertEquals(LocalDate(1991, 2, 2), updatedPerson.birthDate, "Birth date should be updated")
            assertEquals(originalPerson.createdAt, updatedPerson.createdAt, "Created at should remain unchanged")
    }

    @Test
    fun testUpdateWithNullValues() = runDatabaseTest {
            database.open()
            
            // Insert person with all fields populated
            val originalPerson = database.person.add.one(PersonQuery.Add.Params(
                email = "null-update@example.com",
                firstName = "Original",
                lastName = "WithValues",
                phone = "+3333333333",
                birthDate = LocalDate(1985, 5, 15)
            ))
            
            // Update to null values
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "Updated",
                lastName = "ToNull",
                email = "updated-to-null@example.com",
                phone = null, // Set to null
                birthDate = null, // Set to null
                id = originalPerson.id
            )
            
            database.person.updateById(updateParams)
            
            // Verify null update
            val updatedPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == originalPerson.id }
            
            assertNotNull(updatedPerson, "Updated person should be found")
            assertEquals("Updated", updatedPerson.myFirstName, "First name should be updated")
            assertEquals("ToNull", updatedPerson.myLastName, "Last name should be updated")
            assertEquals("updated-to-null@example.com", updatedPerson.email, "Email should be updated")
            assertNull(updatedPerson.phone, "Phone should be null")
            assertNull(updatedPerson.birthDate, "Birth date should be null")
    }

    @Test
    fun testUpdateFromNullToValues() = runDatabaseTest {
            database.open()
            
            // Insert person with null values
            val originalPerson = database.person.add.one(PersonQuery.Add.Params(
                email = "from-null-update@example.com",
                firstName = "Original",
                lastName = "WithNulls",
                phone = null,
                birthDate = null
            ))
            
            // Update from null to actual values
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "Updated",
                lastName = "FromNull",
                email = "updated-from-null@example.com",
                phone = "+4444444444", // From null to value
                birthDate = LocalDate(1992, 8, 20), // From null to value
                id = originalPerson.id
            )
            
            database.person.updateById(updateParams)
            
            // Verify update from null
            val updatedPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == originalPerson.id }
            
            assertNotNull(updatedPerson, "Updated person should be found")
            assertEquals("Updated", updatedPerson.myFirstName, "First name should be updated")
            assertEquals("FromNull", updatedPerson.myLastName, "Last name should be updated")
            assertEquals("updated-from-null@example.com", updatedPerson.email, "Email should be updated")
            assertEquals("+4444444444", updatedPerson.phone, "Phone should be updated from null")
            assertEquals(LocalDate(1992, 8, 20), updatedPerson.birthDate, "Birth date should be updated from null")
    }

    @Test
    fun testPartialFieldUpdates() = runDatabaseTest {
            database.open()
            
            // Insert person with all fields
            val originalPerson = database.person.add.one(PersonQuery.Add.Params(
                email = "partial-update@example.com",
                firstName = "Original",
                lastName = "Partial",
                phone = "+5555555555",
                birthDate = LocalDate(1988, 3, 12)
            ))
            
            // Update only some fields (SQLiteNow requires all fields in UpdateById, but we can test the effect)
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "PartiallyUpdated", // Changed
                lastName = "Partial", // Same as original
                email = "partial-update@example.com", // Same as original
                phone = "+5555555555", // Same as original
                birthDate = LocalDate(1989, 4, 13), // Changed
                id = originalPerson.id
            )
            
            database.person.updateById(updateParams)
            
            // Verify partial update
            val updatedPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == originalPerson.id }
            
            assertNotNull(updatedPerson, "Updated person should be found")
            assertEquals("PartiallyUpdated", updatedPerson.myFirstName, "First name should be updated")
            assertEquals("Partial", updatedPerson.myLastName, "Last name should remain same")
            assertEquals("partial-update@example.com", updatedPerson.email, "Email should remain same")
            assertEquals("+5555555555", updatedPerson.phone, "Phone should remain same")
            assertEquals(LocalDate(1989, 4, 13), updatedPerson.birthDate, "Birth date should be updated")
    }

    @Test
    fun testUpdateNonExistentRecord() = runDatabaseTest {
            database.open()
            
            // Try to update a record that doesn't exist
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "NonExistent",
                lastName = "Update",
                email = "non-existent@example.com",
                phone = "+9999999999",
                birthDate = LocalDate(2000, 1, 1),
                id = 99999L // Non-existent ID
            )
            
            // Execute update (should not throw exception, but affect 0 rows)
            database.person.updateById(updateParams)
            
            // Verify no records were affected
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(0, allPersons.size, "Should have 0 persons")
    }

    @Test
    fun testBatchUpdateOperations() = runDatabaseTest {
            database.open()
            
            // Insert multiple persons
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..5) {
                val person = database.person.add.one(PersonQuery.Add.Params(
                    email = "batch-update-$i@example.com",
                    firstName = "Batch$i",
                    lastName = "Original",
                    phone = "+${i.toString().padStart(10, '0')}",
                    birthDate = LocalDate(1990 + i, i, i)
                ))
                insertedPersons.add(person)
            }
            
            // Update each person individually
            insertedPersons.forEachIndexed { index, person ->
                val updateParams = PersonQuery.UpdateById.Params(
                    firstName = "UpdatedBatch${index + 1}",
                    lastName = "Updated",
                    email = "updated-batch-${index + 1}@example.com",
                    phone = "+${(index + 1).toString().padStart(10, '9')}",
                    birthDate = LocalDate(2000 + index + 1, index + 1, index + 1),
                    id = person.id
                )
                
                database.person.updateById(updateParams)
            }
            
            // Verify all updates
            val updatedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(5, updatedPersons.size, "Should have 5 updated persons")
            
            updatedPersons.sortedBy { it.id }.forEachIndexed { index, person ->
                assertEquals("UpdatedBatch${index + 1}", person.myFirstName, "First name should be updated for person ${index + 1}")
                assertEquals("Updated", person.myLastName, "Last name should be updated for person ${index + 1}")
                assertEquals("updated-batch-${index + 1}@example.com", person.email, "Email should be updated for person ${index + 1}")
                assertEquals(LocalDate(2000 + index + 1, index + 1, index + 1), person.birthDate, "Birth date should be updated for person ${index + 1}")
            }
    }

    @Test
    fun testUpdateWithComplexTypes() = runDatabaseTest {
            database.open()
            
            // Insert person and address
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "complex-update@example.com",
                firstName = "Complex",
                lastName = "Update",
                phone = "+6666666666",
                birthDate = LocalDate(1993, 6, 18)
            ))
            
            database.personAddress.add(PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.HOME,
                street = "123 Original St",
                city = "Original City",
                state = "OC",
                postalCode = "12345",
                country = "Original Country",
                isPrimary = false
            ))
            
            // Update address with different enum value and boolean
            val addresses = database.personAddress.selectAll.asList()
            val addressToUpdate = addresses[0]
            
            val updateParams = PersonAddressQuery.UpdateById.Params(
                personId = person.id,
                addressType = AddressType.WORK, // Changed enum
                street = "456 Updated Ave",
                city = "Updated City",
                state = "UC",
                postalCode = "54321",
                country = "Updated Country",
                isPrimary = true, // Changed boolean
                id = addressToUpdate.id
            )
            
            database.personAddress.updateById(updateParams)
            
            // Verify complex type updates
            val updatedAddresses = database.personAddress.selectAll.asList()
            assertEquals(1, updatedAddresses.size, "Should have 1 address")
            
            val updatedAddress = updatedAddresses[0]
            assertEquals(AddressType.WORK, updatedAddress.addressType, "Address type should be updated to WORK")
            assertEquals("456 Updated Ave", updatedAddress.street, "Street should be updated")
            assertEquals("Updated City", updatedAddress.city, "City should be updated")
            assertEquals("UC", updatedAddress.state, "State should be updated")
            assertEquals("54321", updatedAddress.postalCode, "Postal code should be updated")
            assertEquals("Updated Country", updatedAddress.country, "Country should be updated")
            assertTrue(updatedAddress.isPrimary, "Should be primary address")
    }

    @Test
    fun testUpdateWithDateTimeEdgeCases() = runDatabaseTest {
            database.open()
            
            // Insert person with normal date
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "datetime-edge-update@example.com",
                firstName = "DateTime",
                lastName = "Edge",
                phone = "+7777777777",
                birthDate = LocalDate(1990, 6, 15)
            ))
            
            // Test updating to edge case dates
            val edgeCaseDates = listOf(
                LocalDate(1900, 1, 1), // Very old date
                LocalDate(2099, 12, 31), // Future date
                LocalDate(2000, 2, 29), // Leap year
                LocalDate(1999, 2, 28), // Non-leap year
                null // Null date
            )
            
            edgeCaseDates.forEachIndexed { index, edgeDate ->
                val updateParams = PersonQuery.UpdateById.Params(
                    firstName = "EdgeCase$index",
                    lastName = "DateTime",
                    email = "edge-case-$index@example.com",
                    phone = "+${(7000000000L + index).toString()}",
                    birthDate = edgeDate,
                    id = person.id
                )
                
                database.person.updateById(updateParams)
                
                // Verify edge case date update
                val updatedPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                    .asList()
                    .find { it.id == person.id }
                
                assertNotNull(updatedPerson, "Updated person should be found for edge case $index")
                assertEquals(edgeDate, updatedPerson.birthDate, "Birth date should match edge case $index")
                assertEquals("EdgeCase$index", updatedPerson.myFirstName, "First name should be updated for edge case $index")
            }
    }

    @Test
    fun testUpdateWithLongStrings() = runDatabaseTest {
            database.open()
            
            // Insert person with short strings
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "short@example.com",
                firstName = "Short",
                lastName = "Name",
                phone = "+1234567890",
                birthDate = LocalDate(1985, 4, 10)
            ))
            
            // Update with very long strings
            val longEmail = "very-long-updated-email-address-that-exceeds-normal-length-limits@very-long-updated-domain.example.com"
            val longFirstName = "VeryLongUpdatedFirstNameThatExceedsNormalLengthLimitsForTestingUpdateOperations"
            val longLastName = "VeryLongUpdatedLastNameThatExceedsNormalLengthLimitsForTestingUpdateOperationsToo"
            val longPhone = "+1-800-555-9999-ext-8888-updated-department-customer-service-international"
            
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = longFirstName,
                lastName = longLastName,
                email = longEmail,
                phone = longPhone,
                birthDate = LocalDate(1986, 5, 11),
                id = person.id
            )
            
            database.person.updateById(updateParams)
            
            // Verify long string updates
            val updatedPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == person.id }
            
            assertNotNull(updatedPerson, "Updated person should be found")
            assertEquals(longEmail, updatedPerson.email, "Long email should be updated")
            assertEquals(longFirstName, updatedPerson.myFirstName, "Long first name should be updated")
            assertEquals(longLastName, updatedPerson.myLastName, "Long last name should be updated")
            assertEquals(longPhone, updatedPerson.phone, "Long phone should be updated")
    }

    @Test
    fun testUpdateConstraintValidation() = runDatabaseTest {
            database.open()
            
            // Insert two persons
            val person1 = database.person.add.one(PersonQuery.Add.Params(
                email = "constraint1@example.com",
                firstName = "Constraint1",
                lastName = "Test",
                phone = "+8888888888",
                birthDate = LocalDate(1987, 7, 25)
            ))
            
            val person2 = database.person.add.one(PersonQuery.Add.Params(
                email = "constraint2@example.com",
                firstName = "Constraint2",
                lastName = "Test",
                phone = "+9999999999",
                birthDate = LocalDate(1988, 8, 26)
            ))
            
            // Update person1 with valid data
            val validUpdateParams = PersonQuery.UpdateById.Params(
                firstName = "ValidUpdate",
                lastName = "Constraint",
                email = "valid-update@example.com",
                phone = "+1111111111",
                birthDate = LocalDate(1989, 9, 27),
                id = person1.id
            )
            
            database.person.updateById(validUpdateParams)
            
            // Verify valid update succeeded
            val updatedPerson1 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == person1.id }
            
            assertNotNull(updatedPerson1, "Updated person1 should be found")
            assertEquals("valid-update@example.com", updatedPerson1.email, "Email should be updated")
            assertEquals("ValidUpdate", updatedPerson1.myFirstName, "First name should be updated")
            
            // Verify person2 was not affected
            val unchangedPerson2 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == person2.id }
            
            assertNotNull(unchangedPerson2, "Person2 should still exist")
            assertEquals("constraint2@example.com", unchangedPerson2.email, "Person2 email should be unchanged")
            assertEquals("Constraint2", unchangedPerson2.myFirstName, "Person2 first name should be unchanged")
    }
}
