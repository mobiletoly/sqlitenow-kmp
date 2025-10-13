/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddResult
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Comprehensive integration tests for SQLiteNow UPDATE operations.
 * Ensures ExecuteStatement wrappers cover UPDATE scenarios.
 */
@RunWith(AndroidJUnit4::class)
class UpdateOperationsTest {

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
    fun testBasicUpdateOperation() {
        runBlocking {
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
            
            assertNotNull("Updated person should be found", updatedPerson)
            assertEquals("ID should remain the same", originalPerson.id, updatedPerson!!.id)
            assertEquals("First name should be updated", "Updated", updatedPerson.myFirstName)
            assertEquals("Last name should be updated", "NewName", updatedPerson.myLastName)
            assertEquals("Email should be updated", "updated-basic@example.com", updatedPerson.email)
            assertEquals("Phone should be updated", "+2222222222", updatedPerson.phone)
            assertEquals("Birth date should be updated", LocalDate(1991, 2, 2), updatedPerson.birthDate)
            assertEquals("Created at should remain unchanged", originalPerson.createdAt, updatedPerson.createdAt)
        }
    }

    @Test
    fun testUpdateWithNullValues() {
        runBlocking {
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
            
            assertNotNull("Updated person should be found", updatedPerson)
            assertEquals("First name should be updated", "Updated", updatedPerson!!.myFirstName)
            assertEquals("Last name should be updated", "ToNull", updatedPerson.myLastName)
            assertEquals("Email should be updated", "updated-to-null@example.com", updatedPerson.email)
            assertNull("Phone should be null", updatedPerson.phone)
            assertNull("Birth date should be null", updatedPerson.birthDate)
        }
    }

    @Test
    fun testUpdateFromNullToValues() {
        runBlocking {
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
            
            assertNotNull("Updated person should be found", updatedPerson)
            assertEquals("First name should be updated", "Updated", updatedPerson!!.myFirstName)
            assertEquals("Last name should be updated", "FromNull", updatedPerson.myLastName)
            assertEquals("Email should be updated", "updated-from-null@example.com", updatedPerson.email)
            assertEquals("Phone should be updated from null", "+4444444444", updatedPerson.phone)
            assertEquals("Birth date should be updated from null", LocalDate(1992, 8, 20), updatedPerson.birthDate)
        }
    }

    @Test
    fun testPartialFieldUpdates() {
        runBlocking {
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
            
            assertNotNull("Updated person should be found", updatedPerson)
            assertEquals("First name should be updated", "PartiallyUpdated", updatedPerson!!.myFirstName)
            assertEquals("Last name should remain same", "Partial", updatedPerson.myLastName)
            assertEquals("Email should remain same", "partial-update@example.com", updatedPerson.email)
            assertEquals("Phone should remain same", "+5555555555", updatedPerson.phone)
            assertEquals("Birth date should be updated", LocalDate(1989, 4, 13), updatedPerson.birthDate)
        }
    }

    @Test
    fun testUpdateNonExistentRecord() {
        runBlocking {
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
            assertEquals("Should have 0 persons", 0, allPersons.size)
        }
    }

    @Test
    fun testBatchUpdateOperations() {
        runBlocking {
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
            assertEquals("Should have 5 updated persons", 5, updatedPersons.size)
            
            updatedPersons.sortedBy { it.id }.forEachIndexed { index, person ->
                assertEquals("First name should be updated for person ${index + 1}", 
                    "UpdatedBatch${index + 1}", person.myFirstName)
                assertEquals("Last name should be updated for person ${index + 1}", 
                    "Updated", person.myLastName)
                assertEquals("Email should be updated for person ${index + 1}", 
                    "updated-batch-${index + 1}@example.com", person.email)
                assertEquals("Birth date should be updated for person ${index + 1}", 
                    LocalDate(2000 + index + 1, index + 1, index + 1), person.birthDate)
            }
        }
    }

    @Test
    fun testUpdateWithComplexTypes() {
        runBlocking {
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
            assertEquals("Should have 1 address", 1, updatedAddresses.size)
            
            val updatedAddress = updatedAddresses[0]
            assertEquals("Address type should be updated to WORK", AddressType.WORK, updatedAddress.addressType)
            assertEquals("Street should be updated", "456 Updated Ave", updatedAddress.street)
            assertEquals("City should be updated", "Updated City", updatedAddress.city)
            assertEquals("State should be updated", "UC", updatedAddress.state)
            assertEquals("Postal code should be updated", "54321", updatedAddress.postalCode)
            assertEquals("Country should be updated", "Updated Country", updatedAddress.country)
            assertTrue("Should be primary address", updatedAddress.isPrimary)
        }
    }

    @Test
    fun testUpdateWithDateTimeEdgeCases() {
        runBlocking {
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
                
                assertNotNull("Updated person should be found for edge case $index", updatedPerson)
                assertEquals("Birth date should match edge case $index", edgeDate, updatedPerson!!.birthDate)
                assertEquals("First name should be updated for edge case $index", "EdgeCase$index", updatedPerson.myFirstName)
            }
        }
    }

    @Test
    fun testUpdateWithLongStrings() {
        runBlocking {
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
            
            assertNotNull("Updated person should be found", updatedPerson)
            assertEquals("Long email should be updated", longEmail, updatedPerson!!.email)
            assertEquals("Long first name should be updated", longFirstName, updatedPerson.myFirstName)
            assertEquals("Long last name should be updated", longLastName, updatedPerson.myLastName)
            assertEquals("Long phone should be updated", longPhone, updatedPerson.phone)
        }
    }

    @Test
    fun testUpdateConstraintValidation() {
        runBlocking {
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
            
            assertNotNull("Updated person1 should be found", updatedPerson1)
            assertEquals("Email should be updated", "valid-update@example.com", updatedPerson1!!.email)
            assertEquals("First name should be updated", "ValidUpdate", updatedPerson1.myFirstName)
            
            // Verify person2 was not affected
            val unchangedPerson2 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .find { it.id == person2.id }
            
            assertNotNull("Person2 should still exist", unchangedPerson2)
            assertEquals("Person2 email should be unchanged", "constraint2@example.com", unchangedPerson2!!.email)
            assertEquals("Person2 first name should be unchanged", "Constraint2", unchangedPerson2.myFirstName)
        }
    }
}
