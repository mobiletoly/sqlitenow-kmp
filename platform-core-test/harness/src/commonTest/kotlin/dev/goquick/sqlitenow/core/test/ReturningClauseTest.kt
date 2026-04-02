package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.datetime.LocalDate
import kotlin.test.*

/**
 * Integration tests specifically for SQLiteNow RETURNING clause functionality.
 * Exercises ExecuteReturningStatement helpers (list/one/oneOrNull variants).
 */
class ReturningClauseTest {

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
    fun testExecuteReturningOne() = runDatabaseTest {
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
            
            assertNotNull(insertedPerson, "Inserted person should not be null")
            assertTrue(insertedPerson.id > 0, "ID should be positive")
            assertEquals("ReturningOne", insertedPerson.firstName, "First name should match")
            assertEquals("Test", insertedPerson.lastName, "Last name should match")
            assertEquals("returning-one@example.com", insertedPerson.email, "Email should match")
            assertEquals("+1111111111", insertedPerson.phone, "Phone should match")
            assertEquals(LocalDate(1990, 1, 15), insertedPerson.birthDate, "Birth date should match")
            assertNotNull(insertedPerson.createdAt, "Created at should not be null")
            assertTrue(insertedPerson.createdAt.year >= 2024, "Created at should be recent")
    }

    @Test
    fun testExecuteReturningList() = runDatabaseTest {
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
            
            assertEquals(1, insertedList.size, "Should return list with one item")
            
            val insertedPerson = insertedList[0]
            assertTrue(insertedPerson.id > 0, "ID should be positive")
            assertEquals("ReturningList", insertedPerson.firstName, "First name should match")
            assertEquals("Test", insertedPerson.lastName, "Last name should match")
            assertEquals("returning-list@example.com", insertedPerson.email, "Email should match")
            assertEquals("+2222222222", insertedPerson.phone, "Phone should match")
            assertEquals(LocalDate(1985, 5, 20), insertedPerson.birthDate, "Birth date should match")
            assertNotNull(insertedPerson.createdAt, "Created at should not be null")
    }

    @Test
    fun testExecuteReturningOneOrNull() = runDatabaseTest {
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
            
            assertNotNull(insertedPerson, "Should return person")
            assertEquals("ReturningOrNull", insertedPerson.firstName, "First name should match")
            assertEquals("Test", insertedPerson.lastName, "Last name should match")
            assertEquals("returning-or-null@example.com", insertedPerson.email, "Email should match")
            assertEquals("+3333333333", insertedPerson.phone, "Phone should match")
            assertEquals(LocalDate(1992, 8, 10), insertedPerson.birthDate, "Birth date should match")
    }

    @Test
    fun testReturningWithNullValues() = runDatabaseTest {
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
            assertNull(insertedPerson.phone, "Phone should be null")
            assertNull(insertedPerson.birthDate, "Birth date should be null")
            
            // Non-null parameters should be preserved
            assertEquals("returning-nulls@example.com", insertedPerson.email, "Email should be preserved")
            assertEquals("ReturningNulls", insertedPerson.firstName, "First name should be preserved")
            assertEquals("Test", insertedPerson.lastName, "Last name should be preserved")
            assertTrue(insertedPerson.id > 0, "ID should be positive")
            assertNotNull(insertedPerson.createdAt, "Created at should not be null")
    }

    @Test
    fun testReturningWithTypeAdapters() = runDatabaseTest {
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
            assertEquals(testDate, insertedPerson.birthDate, "Birth date should be preserved through adapters")
            
            // Verify LocalDateTime adapter for createdAt
            assertNotNull(insertedPerson.createdAt, "Created at should not be null")
            assertTrue(insertedPerson.createdAt.year >= 2024, "Created at should be recent")
            
            // Verify other fields
            assertEquals("returning-adapters@example.com", insertedPerson.email, "Email should match")
            assertEquals("ReturningAdapters", insertedPerson.firstName, "First name should match")
            assertEquals("+4444444444", insertedPerson.phone, "Phone should match")
    }

    @Test
    fun testReturningClauseConsistencyWithSelect() = runDatabaseTest {
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
            
            assertEquals(1, selectedPersons.size, "Should find exactly one person")
            val selectedPerson = selectedPersons[0]
            
            // Compare RETURNING result with SELECT result
            assertEquals(insertedPerson.id, selectedPerson.id, "ID should match")
            assertEquals(insertedPerson.firstName, selectedPerson.myFirstName, "First name should match")
            assertEquals(insertedPerson.lastName, selectedPerson.myLastName, "Last name should match")
            assertEquals(insertedPerson.email, selectedPerson.email, "Email should match")
            assertEquals(insertedPerson.phone, selectedPerson.phone, "Phone should match")
            assertEquals(insertedPerson.birthDate, selectedPerson.birthDate, "Birth date should match")
            assertEquals(insertedPerson.createdAt, selectedPerson.createdAt, "Created at should match")
    }

    @Test
    fun testReturningClauseWithUpsertBehavior() = runDatabaseTest {
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
            assertTrue(firstInsert.id > 0, "First insert ID should be positive")
            assertEquals("Original", firstInsert.firstName, "First insert first name should match")
            
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
            assertEquals(firstInsert.id, secondInsert.id, "Should be same ID (updated, not new)")
            assertEquals("Updated", secondInsert.firstName, "First name should be updated")
            assertEquals("NewName", secondInsert.lastName, "Last name should be updated")
            assertEquals("+7777777777", secondInsert.phone, "Phone should be updated")
            assertEquals(LocalDate(1991, 7, 21), secondInsert.birthDate, "Birth date should be updated")
            
            // Verify only one person exists with this email
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            val upsertPersons = allPersons.filter { it.email == "upsert@example.com" }
            assertEquals(1, upsertPersons.size, "Should have only one person with this email")
    }

    @Test
    fun testUpdateReturningClause() = runDatabaseTest {
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
            assertTrue(insertedPerson.id > 0, "Inserted person ID should be positive")

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
            assertEquals(insertedPerson.id, updatedPerson.id, "ID should remain the same")
            assertEquals("Updated", updatedPerson.firstName, "First name should be updated")
            assertEquals("NewName", updatedPerson.lastName, "Last name should be updated")
            assertEquals("updated-returning@example.com", updatedPerson.email, "Email should be updated")
            assertEquals("+2222222222", updatedPerson.phone, "Phone should be updated")
            assertEquals(LocalDate(1995, 5, 15), updatedPerson.birthDate, "Birth date should be updated")

            // Verify the database was actually updated
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }

            assertEquals(1, selectedPersons.size, "Should find exactly one person")
            val selectedPerson = selectedPersons.first()
            assertEquals("Updated", selectedPerson.myFirstName, "Selected person should match updated data")
            assertEquals("NewName", selectedPerson.myLastName, "Selected person should match updated data")
    }

    @Test
    fun testDeleteReturningClause() = runDatabaseTest {
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
            assertTrue(insertedPerson.id > 0, "Inserted person ID should be positive")

            // Verify person exists before deletion
            val beforeDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            assertEquals(1, beforeDelete.size, "Person should exist before deletion")

            // Now delete with RETURNING
            val deleteParams = PersonQuery.DeleteByIdReturning.Params(id = insertedPerson.id)
            val deletedPerson = database.person.deleteByIdReturning.one(deleteParams)

            // Verify the returned data matches the deleted person
            assertEquals(insertedPerson.id, deletedPerson.id, "ID should match")
            assertEquals("ToDelete", deletedPerson.firstName, "First name should match")
            assertEquals("Person", deletedPerson.lastName, "Last name should match")
            assertEquals("delete-returning@example.com", deletedPerson.email, "Email should match")
            assertEquals("+3333333333", deletedPerson.phone, "Phone should match")
            assertEquals(LocalDate(1985, 12, 25), deletedPerson.birthDate, "Birth date should match")

            // Verify the person was actually deleted from the database
            val afterDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            assertEquals(0, afterDelete.size, "Person should be deleted from database")
    }

    @Test
    fun testUpdateReturningWithTypeAdapters() = runDatabaseTest {
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
            assertTrue(insertedAddress.id > 0, "Inserted address ID should be positive")

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
            assertEquals(AddressType.WORK, updatedAddress.addressType, "Address type should be converted correctly")
            assertEquals("456 Updated Ave", updatedAddress.street, "Street should be updated")
            assertEquals("Updated City", updatedAddress.city, "City should be updated")
            assertEquals(false, updatedAddress.isPrimary, "Is primary should be updated")
    }

    @Test
    fun testDeleteReturningWithTypeAdapters() = runDatabaseTest {
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
            assertTrue(insertedAddress.id > 0, "Inserted address ID should be positive")

            // Delete with RETURNING
            val deleteParams = PersonAddressQuery.DeleteByIdReturning.Params(id = insertedAddress.id)
            val deletedAddress = database.personAddress.deleteByIdReturning.one(deleteParams)

            // Verify type adapters worked correctly in RETURNING
            assertEquals(AddressType.WORK, deletedAddress.addressType, "Address type should be converted correctly")
            assertEquals("789 Delete St", deletedAddress.street, "Street should match")
            assertEquals("Delete City", deletedAddress.city, "City should match")
            assertEquals(false, deletedAddress.isPrimary, "Is primary should match")

            // Verify the address was actually deleted
            val afterDelete = database.personAddress.selectAll.asList()
                .filter { it.id == insertedAddress.id }
            assertEquals(0, afterDelete.size, "Address should be deleted from database")
    }

    @Test
    fun testUpdateReturningMultipleRows() = runDatabaseTest {
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
            assertEquals(3, updatedPersons.size, "Should return 3 updated records")

            // Verify all returned records have updated first_name
            updatedPersons.forEach { person ->
                assertEquals("UpdatedMulti", person.firstName, "First name should be updated")
                assertEquals("MultiUpdate", person.lastName, "Last name should remain")
            }

            // Verify the IDs match our inserted persons
            val returnedIds = updatedPersons.map { it.id }.toSet()
            val expectedIds = setOf(insertedPerson1.id, insertedPerson2.id, insertedPerson3.id)
            assertEquals(expectedIds, returnedIds, "Returned IDs should match inserted IDs")

            // Verify the database was actually updated
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.myLastName == "MultiUpdate" }

            assertEquals(3, allPersons.size, "Should find 3 updated persons in database")
            allPersons.forEach { person ->
                assertEquals("UpdatedMulti", person.myFirstName, "Database should reflect the update")
            }
    }

    @Test
    fun testDeleteReturningMultipleRows() = runDatabaseTest {
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
            assertEquals(3, beforeDelete.size, "Should find 3 persons before deletion")

            // Use the generated deleteByLastNameReturning method to delete multiple rows
            val deleteParams = PersonQuery.DeleteByLastNameReturning.Params(
                lastName = "MultiDelete"
            )

            // Execute the multi-row delete with RETURNING
            val deletedPersons = database.person.deleteByLastNameReturning.list(deleteParams)

            // Verify we got 3 deleted records back
            assertEquals(3, deletedPersons.size, "Should return 3 deleted records")

            // Verify the returned data matches what we inserted
            val deletedByFirstName = deletedPersons.associateBy { it.firstName }

            assertEquals("Delete1", deletedByFirstName["Delete1"]?.firstName, "Delete1 should be returned")
            assertEquals("Delete2", deletedByFirstName["Delete2"]?.firstName, "Delete2 should be returned")
            assertEquals("Delete3", deletedByFirstName["Delete3"]?.firstName, "Delete3 should be returned")

            deletedPersons.forEach { person ->
                assertEquals("MultiDelete", person.lastName, "Last name should match")
            }

            // Verify the IDs match our inserted persons
            val returnedIds = deletedPersons.map { it.id }.toSet()
            val expectedIds = setOf(insertedPerson1.id, insertedPerson2.id, insertedPerson3.id)
            assertEquals(expectedIds, returnedIds, "Returned IDs should match inserted IDs")

            // Verify the persons were actually deleted from the database
            val afterDelete = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.myLastName == "MultiDelete" }
            assertEquals(0, afterDelete.size, "Should find no persons after deletion")
    }

    @Test
    fun testUpdateReturningMultipleRowsWithTypeAdapters() = runDatabaseTest {
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
            assertEquals(3, updatedAddresses.size, "Should return 3 updated records")

            // Verify all returned records have updated values and correct type conversions
            updatedAddresses.forEach { address ->
                assertEquals("UpdatedMultiCity", address.city, "City should be updated")
                assertEquals(true, address.isPrimary, "Is primary should be updated")
                assertTrue(address.addressType in listOf(AddressType.HOME, AddressType.WORK), "Address type should be properly converted")
            }

            // Verify the IDs match our inserted addresses
            val returnedIds = updatedAddresses.map { it.id }.toSet()
            val expectedIds = setOf(insertedAddress1.id, insertedAddress2.id, insertedAddress3.id)
            assertEquals(expectedIds, returnedIds, "Returned IDs should match inserted IDs")

            // Verify the database was actually updated
            val allAddresses = database.personAddress.selectAll.asList()
                .filter { it.city == "UpdatedMultiCity" }

            assertEquals(3, allAddresses.size, "Should find 3 updated addresses in database")
            allAddresses.forEach { address ->
                assertEquals(true, address.isPrimary, "Database should reflect the update")
                assertEquals("UpdatedMultiCity", address.city, "Database should reflect the city update")
            }
    }
}
