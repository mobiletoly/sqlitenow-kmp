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
 * Integration tests specifically for SQLiteNow RETURNING clause functionality.
 * Exercises ExecuteReturningStatement helpers (list/one/oneOrNull variants).
 */
@RunWith(AndroidJUnit4::class)
class ReturningClauseTest {

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
    fun testExecuteReturningOne() {
        runBlocking {
            database.open()
            
            // Test .one helper with INSERT RETURNING
            val testPerson = PersonQuery.Add.Params(
                email = "returning-one@example.com",
                firstName = "ReturningOne",
                lastName = "Test",
                phone = "+1111111111",
                birthDate = LocalDate(1990, 1, 15)
            )
            
            val insertedPerson = database.person.add.one(testPerson)
            
            assertNotNull("Inserted person should not be null", insertedPerson)
            assertTrue("ID should be positive", insertedPerson.id > 0)
            assertEquals("First name should match", "ReturningOne", insertedPerson.firstName)
            assertEquals("Last name should match", "Test", insertedPerson.lastName)
            assertEquals("Email should match", "returning-one@example.com", insertedPerson.email)
            assertEquals("Phone should match", "+1111111111", insertedPerson.phone)
            assertEquals("Birth date should match", LocalDate(1990, 1, 15), insertedPerson.birthDate)
            assertNotNull("Created at should not be null", insertedPerson.createdAt)
            assertTrue("Created at should be recent", insertedPerson.createdAt.year >= 2024)
        }
    }

    @Test
    fun testExecuteReturningList() {
        runBlocking {
            database.open()
            
            // Test .list helper with INSERT RETURNING (single insert returns list with one item)
            val testPerson = PersonQuery.Add.Params(
                email = "returning-list@example.com",
                firstName = "ReturningList",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = LocalDate(1985, 5, 20)
            )
            
            val insertedList = database.person.add.list(testPerson)
            
            assertEquals("Should return list with one item", 1, insertedList.size)
            
            val insertedPerson = insertedList[0]
            assertTrue("ID should be positive", insertedPerson.id > 0)
            assertEquals("First name should match", "ReturningList", insertedPerson.firstName)
            assertEquals("Last name should match", "Test", insertedPerson.lastName)
            assertEquals("Email should match", "returning-list@example.com", insertedPerson.email)
            assertEquals("Phone should match", "+2222222222", insertedPerson.phone)
            assertEquals("Birth date should match", LocalDate(1985, 5, 20), insertedPerson.birthDate)
            assertNotNull("Created at should not be null", insertedPerson.createdAt)
        }
    }

    @Test
    fun testExecuteReturningOneOrNull() {
        runBlocking {
            database.open()
            
            // Test .oneOrNull helper with successful INSERT RETURNING
            val testPerson = PersonQuery.Add.Params(
                email = "returning-or-null@example.com",
                firstName = "ReturningOrNull",
                lastName = "Test",
                phone = "+3333333333",
                birthDate = LocalDate(1992, 8, 10)
            )
            
            val insertedPerson = database.person.add.oneOrNull(testPerson)
            
            assertNotNull("Should return person", insertedPerson)
            assertEquals("First name should match", "ReturningOrNull", insertedPerson!!.firstName)
            assertEquals("Last name should match", "Test", insertedPerson.lastName)
            assertEquals("Email should match", "returning-or-null@example.com", insertedPerson.email)
            assertEquals("Phone should match", "+3333333333", insertedPerson.phone)
            assertEquals("Birth date should match", LocalDate(1992, 8, 10), insertedPerson.birthDate)
        }
    }

    @Test
    fun testReturningWithNullValues() {
        runBlocking {
            database.open()
            
            // Test RETURNING clause with null parameters
            val testPerson = PersonQuery.Add.Params(
                email = "returning-nulls@example.com",
                firstName = "ReturningNulls",
                lastName = "Test",
                phone = null, // Test null parameter
                birthDate = null // Test null date parameter
            )
            
            val insertedPerson = database.person.add.one(testPerson)
            
            // Verify null parameters were handled correctly in RETURNING
            assertNull("Phone should be null", insertedPerson.phone)
            assertNull("Birth date should be null", insertedPerson.birthDate)
            
            // Non-null parameters should be preserved
            assertEquals("Email should be preserved", "returning-nulls@example.com", insertedPerson.email)
            assertEquals("First name should be preserved", "ReturningNulls", insertedPerson.firstName)
            assertEquals("Last name should be preserved", "Test", insertedPerson.lastName)
            assertTrue("ID should be positive", insertedPerson.id > 0)
            assertNotNull("Created at should not be null", insertedPerson.createdAt)
        }
    }

    @Test
    fun testReturningWithTypeAdapters() {
        runBlocking {
            database.open()
            
            // Test RETURNING clause with custom type adapters (LocalDate, LocalDateTime)
            val testDate = LocalDate(1988, 12, 25)
            
            val testPerson = PersonQuery.Add.Params(
                email = "returning-adapters@example.com",
                firstName = "ReturningAdapters",
                lastName = "Test",
                phone = "+4444444444",
                birthDate = testDate
            )
            
            val insertedPerson = database.person.add.one(testPerson)
            
            // Verify type adapters worked correctly in RETURNING
            assertEquals("Birth date should be preserved through adapters", testDate, insertedPerson.birthDate)
            
            // Verify LocalDateTime adapter for createdAt
            assertNotNull("Created at should not be null", insertedPerson.createdAt)
            assertTrue("Created at should be recent", insertedPerson.createdAt.year >= 2024)
            
            // Verify other fields
            assertEquals("Email should match", "returning-adapters@example.com", insertedPerson.email)
            assertEquals("First name should match", "ReturningAdapters", insertedPerson.firstName)
            assertEquals("Phone should match", "+4444444444", insertedPerson.phone)
        }
    }

    @Test
    fun testReturningClauseConsistencyWithSelect() {
        runBlocking {
            database.open()
            
            // Test that RETURNING clause returns the same data as a subsequent SELECT
            val testPerson = PersonQuery.Add.Params(
                email = "consistency@example.com",
                firstName = "Consistency",
                lastName = "Test",
                phone = "+5555555555",
                birthDate = LocalDate(1995, 3, 15)
            )
            
            // Insert with RETURNING
            val insertedPerson = database.person.add.one(testPerson)
            
            // Select the same person
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            
            assertEquals("Should find exactly one person", 1, selectedPersons.size)
            val selectedPerson = selectedPersons[0]
            
            // Compare RETURNING result with SELECT result
            assertEquals("ID should match", insertedPerson.id, selectedPerson.id)
            assertEquals("First name should match", insertedPerson.firstName, selectedPerson.myFirstName)
            assertEquals("Last name should match", insertedPerson.lastName, selectedPerson.myLastName)
            assertEquals("Email should match", insertedPerson.email, selectedPerson.email)
            assertEquals("Phone should match", insertedPerson.phone, selectedPerson.phone)
            assertEquals("Birth date should match", insertedPerson.birthDate, selectedPerson.birthDate)
            assertEquals("Created at should match", insertedPerson.createdAt, selectedPerson.createdAt)
        }
    }

    @Test
    fun testReturningClauseWithUpsertBehavior() {
        runBlocking {
            database.open()
            
            // Test RETURNING clause with ON CONFLICT DO UPDATE (upsert behavior)
            val originalPerson = PersonQuery.Add.Params(
                email = "upsert@example.com",
                firstName = "Original",
                lastName = "Name",
                phone = "+6666666666",
                birthDate = LocalDate(1990, 6, 20)
            )
            
            // First insert
            val firstInsert = database.person.add.one(originalPerson)
            assertTrue("First insert ID should be positive", firstInsert.id > 0)
            assertEquals("First insert first name should match", "Original", firstInsert.firstName)
            
            // Second insert with same email (should trigger ON CONFLICT DO UPDATE)
            val updatedPerson = PersonQuery.Add.Params(
                email = "upsert@example.com", // Same email
                firstName = "Updated",
                lastName = "NewName",
                phone = "+7777777777",
                birthDate = LocalDate(1991, 7, 21)
            )
            
            val secondInsert = database.person.add.one(updatedPerson)
            
            // Should return the updated row
            assertEquals("Should be same ID (updated, not new)", firstInsert.id, secondInsert.id)
            assertEquals("First name should be updated", "Updated", secondInsert.firstName)
            assertEquals("Last name should be updated", "NewName", secondInsert.lastName)
            assertEquals("Phone should be updated", "+7777777777", secondInsert.phone)
            assertEquals("Birth date should be updated", LocalDate(1991, 7, 21), secondInsert.birthDate)
            
            // Verify only one person exists with this email
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            val upsertPersons = allPersons.filter { it.email == "upsert@example.com" }
            assertEquals("Should have only one person with this email", 1, upsertPersons.size)
        }
    }

    @Test
    fun testUpdateReturningClause() {
        runBlocking {
            database.open()

            // First insert a person to update
            val originalPerson = PersonQuery.Add.Params(
                email = "update-returning@example.com",
                firstName = "Original",
                lastName = "Name",
                phone = "+1111111111",
                birthDate = LocalDate(1990, 1, 1)
            )

            val insertedPerson = database.person.add.one(originalPerson)
            assertTrue("Inserted person ID should be positive", insertedPerson.id > 0)

            // Now update with RETURNING
            val updatedData = PersonQuery.UpdateByIdReturning.Params(
                firstName = "Updated",
                lastName = "NewName",
                email = "updated-returning@example.com",
                phone = "+2222222222",
                birthDate = LocalDate(1995, 5, 15),
                id = insertedPerson.id
            )

            val updatedPerson = database.person.updateByIdReturning.one(updatedData)

            // Verify the returned data matches the update
            assertEquals("ID should remain the same", insertedPerson.id, updatedPerson.id)
            assertEquals("First name should be updated", "Updated", updatedPerson.firstName)
            assertEquals("Last name should be updated", "NewName", updatedPerson.lastName)
            assertEquals("Email should be updated", "updated-returning@example.com", updatedPerson.email)
            assertEquals("Phone should be updated", "+2222222222", updatedPerson.phone)
            assertEquals("Birth date should be updated", LocalDate(1995, 5, 15), updatedPerson.birthDate)

            // Verify the database was actually updated
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }

            assertEquals("Should find exactly one person", 1, selectedPersons.size)
            val selectedPerson = selectedPersons.first()
            assertEquals("Selected person should match updated data", "Updated", selectedPerson.myFirstName)
            assertEquals("Selected person should match updated data", "NewName", selectedPerson.myLastName)
        }
    }

    @Test
    fun testDeleteReturningClause() {
        runBlocking {
            database.open()

            // First insert a person to delete
            val originalPerson = PersonQuery.Add.Params(
                email = "delete-returning@example.com",
                firstName = "ToDelete",
                lastName = "Person",
                phone = "+3333333333",
                birthDate = LocalDate(1985, 12, 25)
            )

            val insertedPerson = database.person.add.one(originalPerson)
            assertTrue("Inserted person ID should be positive", insertedPerson.id > 0)

            // Verify person exists before deletion
            val beforeDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            assertEquals("Person should exist before deletion", 1, beforeDelete.size)

            // Now delete with RETURNING
            val deleteParams = PersonQuery.DeleteByIdReturning.Params(id = insertedPerson.id)
            val deletedPerson = database.person.deleteByIdReturning.one(deleteParams)

            // Verify the returned data matches the deleted person
            assertEquals("ID should match", insertedPerson.id, deletedPerson.id)
            assertEquals("First name should match", "ToDelete", deletedPerson.firstName)
            assertEquals("Last name should match", "Person", deletedPerson.lastName)
            assertEquals("Email should match", "delete-returning@example.com", deletedPerson.email)
            assertEquals("Phone should match", "+3333333333", deletedPerson.phone)
            assertEquals("Birth date should match", LocalDate(1985, 12, 25), deletedPerson.birthDate)

            // Verify the person was actually deleted from the database
            val afterDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            assertEquals("Person should be deleted from database", 0, afterDelete.size)
        }
    }

    @Test
    fun testUpdateReturningWithTypeAdapters() {
        runBlocking {
            database.open()

            // Insert a person_address to update
            val originalAddress = PersonAddressQuery.AddReturning.Params(
                personId = 1,
                addressType = AddressType.HOME,
                street = "123 Original St",
                city = "Original City",
                state = "OS",
                postalCode = "12345",
                country = "Original Country",
                isPrimary = true
            )

            val insertedAddress = database.personAddress.addReturning.one(originalAddress)
            assertTrue("Inserted address ID should be positive", insertedAddress.id > 0)

            // Update with RETURNING
            val updatedData = PersonAddressQuery.UpdateByIdReturning.Params(
                personId = 2,
                addressType = AddressType.WORK,
                street = "456 Updated Ave",
                city = "Updated City",
                state = "US",
                postalCode = "67890",
                country = "Updated Country",
                isPrimary = false,
                id = insertedAddress.id
            )

            val updatedAddress = database.personAddress.updateByIdReturning.one(updatedData)

            // Verify type adapters worked correctly
            assertEquals("Address type should be converted correctly", AddressType.WORK, updatedAddress.addressType)
            assertEquals("Street should be updated", "456 Updated Ave", updatedAddress.street)
            assertEquals("City should be updated", "Updated City", updatedAddress.city)
            assertEquals("Is primary should be updated", false, updatedAddress.isPrimary)
        }
    }

    @Test
    fun testDeleteReturningWithTypeAdapters() {
        runBlocking {
            database.open()

            // Insert a person_address to delete
            val originalAddress = PersonAddressQuery.AddReturning.Params(
                personId = 1,
                addressType = AddressType.WORK,
                street = "789 Delete St",
                city = "Delete City",
                state = "DS",
                postalCode = "99999",
                country = "Delete Country",
                isPrimary = false
            )

            val insertedAddress = database.personAddress.addReturning.one(originalAddress)
            assertTrue("Inserted address ID should be positive", insertedAddress.id > 0)

            // Delete with RETURNING
            val deleteParams = PersonAddressQuery.DeleteByIdReturning.Params(id = insertedAddress.id)
            val deletedAddress = database.personAddress.deleteByIdReturning.one(deleteParams)

            // Verify type adapters worked correctly in RETURNING
            assertEquals("Address type should be converted correctly", AddressType.WORK, deletedAddress.addressType)
            assertEquals("Street should match", "789 Delete St", deletedAddress.street)
            assertEquals("City should match", "Delete City", deletedAddress.city)
            assertEquals("Is primary should match", false, deletedAddress.isPrimary)

            // Verify the address was actually deleted
            val afterDelete = database.personAddress.selectAll.asList()
                .filter { it.id == insertedAddress.id }
            assertEquals("Address should be deleted from database", 0, afterDelete.size)
        }
    }

    @Test
    fun testUpdateReturningMultipleRows() {
        runBlocking {
            database.open()

            // Insert multiple persons to update
            val person1 = PersonQuery.Add.Params(
                email = "multi-update1@example.com",
                firstName = "Multi1",
                lastName = "MultiUpdate",
                phone = "+1111111111",
                birthDate = LocalDate(1990, 1, 1)
            )
            val person2 = PersonQuery.Add.Params(
                email = "multi-update2@example.com",
                firstName = "Multi2",
                lastName = "MultiUpdate",
                phone = "+2222222222",
                birthDate = LocalDate(1991, 2, 2)
            )
            val person3 = PersonQuery.Add.Params(
                email = "multi-update3@example.com",
                firstName = "Multi3",
                lastName = "MultiUpdate",
                phone = "+3333333333",
                birthDate = LocalDate(1992, 3, 3)
            )

            val insertedPerson1 = database.person.add.one(person1)
            val insertedPerson2 = database.person.add.one(person2)
            val insertedPerson3 = database.person.add.one(person3)

            // Use the generated updateByLastNameReturning method to update multiple rows
            val updateParams = PersonQuery.UpdateByLastNameReturning.Params(
                firstName = "UpdatedMulti",
                lastName = "MultiUpdate"
            )

            // Execute the multi-row update with RETURNING
            val updatedPersons = database.person.updateByLastNameReturning.list(updateParams)

            // Verify we got 3 updated records back
            assertEquals("Should return 3 updated records", 3, updatedPersons.size)

            // Verify all returned records have updated first_name
            updatedPersons.forEach { person ->
                assertEquals("First name should be updated", "UpdatedMulti", person.firstName)
                assertEquals("Last name should remain", "MultiUpdate", person.lastName)
            }

            // Verify the IDs match our inserted persons
            val returnedIds = updatedPersons.map { it.id }.toSet()
            val expectedIds = setOf(insertedPerson1.id, insertedPerson2.id, insertedPerson3.id)
            assertEquals("Returned IDs should match inserted IDs", expectedIds, returnedIds)

            // Verify the database was actually updated
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.myLastName == "MultiUpdate" }

            assertEquals("Should find 3 updated persons in database", 3, allPersons.size)
            allPersons.forEach { person ->
                assertEquals("Database should reflect the update", "UpdatedMulti", person.myFirstName)
            }
        }
    }

    @Test
    fun testDeleteReturningMultipleRows() {
        runBlocking {
            database.open()

            // Insert multiple persons to delete
            val person1 = PersonQuery.Add.Params(
                email = "multi-delete1@example.com",
                firstName = "Delete1",
                lastName = "MultiDelete",
                phone = "+4444444444",
                birthDate = LocalDate(1985, 4, 4)
            )
            val person2 = PersonQuery.Add.Params(
                email = "multi-delete2@example.com",
                firstName = "Delete2",
                lastName = "MultiDelete",
                phone = "+5555555555",
                birthDate = LocalDate(1986, 5, 5)
            )
            val person3 = PersonQuery.Add.Params(
                email = "multi-delete3@example.com",
                firstName = "Delete3",
                lastName = "MultiDelete",
                phone = "+6666666666",
                birthDate = LocalDate(1987, 6, 6)
            )

            val insertedPerson1 = database.person.add.one(person1)
            val insertedPerson2 = database.person.add.one(person2)
            val insertedPerson3 = database.person.add.one(person3)

            // Verify persons exist before deletion
            val beforeDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.myLastName == "MultiDelete" }
            assertEquals("Should find 3 persons before deletion", 3, beforeDelete.size)

            // Use the generated deleteByLastNameReturning method to delete multiple rows
            val deleteParams = PersonQuery.DeleteByLastNameReturning.Params(
                lastName = "MultiDelete"
            )

            // Execute the multi-row delete with RETURNING
            val deletedPersons = database.person.deleteByLastNameReturning.list(deleteParams)

            // Verify we got 3 deleted records back
            assertEquals("Should return 3 deleted records", 3, deletedPersons.size)

            // Verify the returned data matches what we inserted
            val deletedByFirstName = deletedPersons.associateBy { it.firstName }

            assertEquals("Delete1 should be returned", "Delete1", deletedByFirstName["Delete1"]?.firstName)
            assertEquals("Delete2 should be returned", "Delete2", deletedByFirstName["Delete2"]?.firstName)
            assertEquals("Delete3 should be returned", "Delete3", deletedByFirstName["Delete3"]?.firstName)

            deletedPersons.forEach { person ->
                assertEquals("Last name should match", "MultiDelete", person.lastName)
            }

            // Verify the IDs match our inserted persons
            val returnedIds = deletedPersons.map { it.id }.toSet()
            val expectedIds = setOf(insertedPerson1.id, insertedPerson2.id, insertedPerson3.id)
            assertEquals("Returned IDs should match inserted IDs", expectedIds, returnedIds)

            // Verify the persons were actually deleted from the database
            val afterDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.myLastName == "MultiDelete" }
            assertEquals("Should find no persons after deletion", 0, afterDelete.size)
        }
    }

    @Test
    fun testUpdateReturningMultipleRowsWithTypeAdapters() {
        runBlocking {
            database.open()

            // Insert multiple person_addresses to update
            val address1 = PersonAddressQuery.AddReturning.Params(
                personId = 1,
                addressType = AddressType.HOME,
                street = "123 Multi St",
                city = "MultiCity",
                state = "MC",
                postalCode = "11111",
                country = "Multi Country",
                isPrimary = false
            )
            val address2 = PersonAddressQuery.AddReturning.Params(
                personId = 1,
                addressType = AddressType.WORK,
                street = "456 Multi Ave",
                city = "MultiCity",
                state = "MC",
                postalCode = "22222",
                country = "Multi Country",
                isPrimary = false
            )
            val address3 = PersonAddressQuery.AddReturning.Params(
                personId = 1,
                addressType = AddressType.HOME,
                street = "789 Multi Blvd",
                city = "MultiCity",
                state = "MC",
                postalCode = "33333",
                country = "Multi Country",
                isPrimary = false
            )

            val insertedAddress1 = database.personAddress.addReturning.one(address1)
            val insertedAddress2 = database.personAddress.addReturning.one(address2)
            val insertedAddress3 = database.personAddress.addReturning.one(address3)

            // Use the generated updateByCityReturning method to update multiple rows
            val updateParams = PersonAddressQuery.UpdateByCityReturning.Params(
                isPrimary = true,
                newCity = "UpdatedMultiCity",
                oldCity = "MultiCity"
            )

            // Execute the multi-row update with RETURNING and type adapters
            val updatedAddresses = database.personAddress.updateByCityReturning.list(updateParams)

            // Verify we got 3 updated records back
            assertEquals("Should return 3 updated records", 3, updatedAddresses.size)

            // Verify all returned records have updated values and correct type conversions
            updatedAddresses.forEach { address ->
                assertEquals("City should be updated", "UpdatedMultiCity", address.city)
                assertEquals("Is primary should be updated", true, address.isPrimary)
                assertTrue("Address type should be properly converted",
                    address.addressType in listOf(AddressType.HOME, AddressType.WORK))
            }

            // Verify the IDs match our inserted addresses
            val returnedIds = updatedAddresses.map { it.id }.toSet()
            val expectedIds = setOf(insertedAddress1.id, insertedAddress2.id, insertedAddress3.id)
            assertEquals("Returned IDs should match inserted IDs", expectedIds, returnedIds)

            // Verify the database was actually updated
            val allAddresses = database.personAddress.selectAll.asList()
                .filter { it.city == "UpdatedMultiCity" }

            assertEquals("Should find 3 updated addresses in database", 3, allAddresses.size)
            allAddresses.forEach { address ->
                assertEquals("Database should reflect the update", true, address.isPrimary)
                assertEquals("Database should reflect the city update", "UpdatedMultiCity", address.city)
            }
        }
    }
}
