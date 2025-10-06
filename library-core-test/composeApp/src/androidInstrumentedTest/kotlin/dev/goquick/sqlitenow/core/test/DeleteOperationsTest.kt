package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.CommentQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddResult
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Comprehensive integration tests for SQLiteNow DELETE operations.
 * Tests all aspects of DELETE operations using ExecuteRunners.
 */
@RunWith(AndroidJUnit4::class)
class DeleteOperationsTest {

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
    fun testBasicDeleteOperation() {
        runBlocking {
            database.open()
            
            // Insert persons to delete
            val person1 = database.person.add(PersonQuery.Add.Params(
                email = "delete1@example.com",
                firstName = "Delete1",
                lastName = "Test",
                phone = "+1111111111",
                birthDate = LocalDate(1990, 1, 1)
            )).executeReturningOne()
            
            val person2 = database.person.add(PersonQuery.Add.Params(
                email = "delete2@example.com",
                firstName = "Delete2",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = LocalDate(1991, 2, 2)
            )).executeReturningOne()
            
            val person3 = database.person.add(PersonQuery.Add.Params(
                email = "keep@example.com",
                firstName = "Keep",
                lastName = "Test",
                phone = "+3333333333",
                birthDate = LocalDate(1992, 3, 3)
            )).executeReturningOne()
            
            // Verify all persons exist
            val allPersonsBefore = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 3 persons before delete", 3, allPersonsBefore.size)
            
            // Delete specific persons by IDs
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person1.id, person2.id)
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify deletion
            val allPersonsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 1 person after delete", 1, allPersonsAfter.size)
            assertEquals("Remaining person should be 'Keep'", "Keep", allPersonsAfter[0].myFirstName)
            assertEquals("Remaining person ID should match", person3.id, allPersonsAfter[0].id)
        }
    }

    @Test
    fun testDeleteSingleRecord() {
        runBlocking {
            database.open()
            
            // Insert single person
            val person = database.person.add(PersonQuery.Add.Params(
                email = "single-delete@example.com",
                firstName = "Single",
                lastName = "Delete",
                phone = "+4444444444",
                birthDate = LocalDate(1985, 5, 15)
            )).executeReturningOne()
            
            // Verify person exists
            val personsBefore = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 1 person before delete", 1, personsBefore.size)
            
            // Delete single person
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person.id)
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify deletion
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 0 persons after delete", 0, personsAfter.size)
        }
    }

    @Test
    fun testDeleteNonExistentRecords() {
        runBlocking {
            database.open()
            
            // Insert one person
            val person = database.person.add(PersonQuery.Add.Params(
                email = "existing@example.com",
                firstName = "Existing",
                lastName = "Person",
                phone = "+5555555555",
                birthDate = LocalDate(1988, 8, 20)
            )).executeReturningOne()
            
            // Try to delete non-existent IDs
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(99999L, 88888L, 77777L) // Non-existent IDs
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify existing person was not affected
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should still have 1 person", 1, personsAfter.size)
            assertEquals("Existing person should remain", person.id, personsAfter[0].id)
            assertEquals("Existing person name should remain", "Existing", personsAfter[0].myFirstName)
        }
    }

    @Test
    fun testDeleteMixedExistentAndNonExistentRecords() {
        runBlocking {
            database.open()
            
            // Insert multiple persons
            val person1 = database.person.add(PersonQuery.Add.Params(
                email = "mixed1@example.com",
                firstName = "Mixed1",
                lastName = "Test",
                phone = "+6666666666",
                birthDate = LocalDate(1989, 9, 25)
            )).executeReturningOne()
            
            val person2 = database.person.add(PersonQuery.Add.Params(
                email = "mixed2@example.com",
                firstName = "Mixed2",
                lastName = "Test",
                phone = "+7777777777",
                birthDate = LocalDate(1990, 10, 26)
            )).executeReturningOne()
            
            val person3 = database.person.add(PersonQuery.Add.Params(
                email = "mixed3@example.com",
                firstName = "Mixed3",
                lastName = "Test",
                phone = "+8888888888",
                birthDate = LocalDate(1991, 11, 27)
            )).executeReturningOne()
            
            // Delete mix of existent and non-existent IDs
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person1.id, 99999L, person3.id, 88888L) // Mix of real and fake IDs
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify only existing records were deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 1 person remaining", 1, personsAfter.size)
            assertEquals("Remaining person should be Mixed2", person2.id, personsAfter[0].id)
            assertEquals("Remaining person name should be Mixed2", "Mixed2", personsAfter[0].myFirstName)
        }
    }

    @Test
    fun testDeleteEmptyIdsList() {
        runBlocking {
            database.open()
            
            // Insert person
            val person = database.person.add(PersonQuery.Add.Params(
                email = "empty-list@example.com",
                firstName = "EmptyList",
                lastName = "Test",
                phone = "+9999999999",
                birthDate = LocalDate(1987, 7, 18)
            )).executeReturningOne()
            
            // Try to delete with empty IDs list
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = emptyList()
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify person was not affected
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should still have 1 person", 1, personsAfter.size)
            assertEquals("Person should remain unchanged", person.id, personsAfter[0].id)
        }
    }

    @Test
    fun testDeleteWithDuplicateIds() {
        runBlocking {
            database.open()
            
            // Insert persons
            val person1 = database.person.add(PersonQuery.Add.Params(
                email = "duplicate1@example.com",
                firstName = "Duplicate1",
                lastName = "Test",
                phone = "+1010101010",
                birthDate = LocalDate(1986, 6, 12)
            )).executeReturningOne()
            
            val person2 = database.person.add(PersonQuery.Add.Params(
                email = "duplicate2@example.com",
                firstName = "Duplicate2",
                lastName = "Test",
                phone = "+2020202020",
                birthDate = LocalDate(1987, 7, 13)
            )).executeReturningOne()
            
            // Delete with duplicate IDs in the list
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person1.id, person1.id, person2.id, person1.id) // Duplicates
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify both persons were deleted (duplicates should not cause issues)
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 0 persons after delete", 0, personsAfter.size)
        }
    }

    @Test
    fun testDeleteLargeNumberOfRecords() {
        runBlocking {
            database.open()
            
            // Insert many persons
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..20) {
                val person = database.person.add(PersonQuery.Add.Params(
                    email = "bulk-delete-$i@example.com",
                    firstName = "BulkDelete$i",
                    lastName = "Test",
                    phone = "+${i.toString().padStart(10, '0')}",
                    birthDate = LocalDate(1980 + i, (i % 12) + 1, (i % 28) + 1)
                )).executeReturningOne()
                insertedPersons.add(person)
            }
            
            // Verify all persons exist
            val allPersonsBefore = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 25, offset = 0)).asList()
            assertEquals("Should have 20 persons before delete", 20, allPersonsBefore.size)
            
            // Delete first 15 persons
            val idsToDelete = insertedPersons.take(15).map { it.id }
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = idsToDelete)
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify deletion
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 25, offset = 0)).asList()
            assertEquals("Should have 5 persons remaining", 5, personsAfter.size)
            
            // Verify remaining persons are the last 5
            val remainingIds = personsAfter.map { it.id }.toSet()
            val expectedRemainingIds = insertedPersons.takeLast(5).map { it.id }.toSet()
            assertEquals("Remaining IDs should match last 5 inserted", expectedRemainingIds, remainingIds)
        }
    }

    @Test
    fun testDeleteWithForeignKeyConstraints() {
        runBlocking {
            database.open()
            
            // Insert person with related data
            val person = database.person.add(PersonQuery.Add.Params(
                email = "foreign-key@example.com",
                firstName = "ForeignKey",
                lastName = "Test",
                phone = "+1212121212",
                birthDate = LocalDate(1993, 4, 8)
            )).executeReturningOne()
            
            // Insert related address
            database.personAddress.add(PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.HOME,
                street = "123 Foreign Key St",
                city = "FK City",
                state = "FK",
                postalCode = "12345",
                country = "FK Country",
                isPrimary = true
            )).execute()
            
            // Insert related comment
            database.comment.add(CommentQuery.Add.Params(
                personId = person.id,
                comment = "Comment with foreign key reference",
                createdAt = LocalDateTime(2024, 4, 8, 10, 30, 45),
                tags = listOf("foreign-key", "constraint", "test")
            )).execute()
            
            // Verify related data exists
            val addressesBefore = database.personAddress.selectAll.asList()
            val commentsBefore = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals("Should have 1 address", 1, addressesBefore.size)
            assertEquals("Should have 1 comment", 1, commentsBefore.size)
            
            // Delete the person (this should cascade or handle foreign key constraints appropriately)
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = listOf(person.id))
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify person was deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 0 persons after delete", 0, personsAfter.size)
            
            // Note: The behavior of related data depends on foreign key constraint configuration
            // This test verifies the delete operation completes without errors
        }
    }

    @Test
    fun testDeleteOrderIndependence() {
        runBlocking {
            database.open()
            
            // Insert persons in specific order
            val person1 = database.person.add(PersonQuery.Add.Params(
                email = "order1@example.com",
                firstName = "Order1",
                lastName = "Test",
                phone = "+3030303030",
                birthDate = LocalDate(1984, 2, 14)
            )).executeReturningOne()
            
            val person2 = database.person.add(PersonQuery.Add.Params(
                email = "order2@example.com",
                firstName = "Order2",
                lastName = "Test",
                phone = "+4040404040",
                birthDate = LocalDate(1985, 3, 15)
            )).executeReturningOne()
            
            val person3 = database.person.add(PersonQuery.Add.Params(
                email = "order3@example.com",
                firstName = "Order3",
                lastName = "Test",
                phone = "+5050505050",
                birthDate = LocalDate(1986, 4, 16)
            )).executeReturningOne()
            
            // Delete in different order than insertion
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person3.id, person1.id) // Delete 3rd and 1st, keep 2nd
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify correct person remains
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 1 person remaining", 1, personsAfter.size)
            assertEquals("Remaining person should be Order2", person2.id, personsAfter[0].id)
            assertEquals("Remaining person name should be Order2", "Order2", personsAfter[0].myFirstName)
        }
    }

    @Test
    fun testDeleteAllRecords() {
        runBlocking {
            database.open()
            
            // Insert multiple persons
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..10) {
                val person = database.person.add(PersonQuery.Add.Params(
                    email = "delete-all-$i@example.com",
                    firstName = "DeleteAll$i",
                    lastName = "Test",
                    phone = "+${(6000000000L + i).toString()}",
                    birthDate = LocalDate(1975 + i, (i % 12) + 1, (i % 28) + 1)
                )).executeReturningOne()
                insertedPersons.add(person)
            }
            
            // Verify all persons exist
            val allPersonsBefore = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 15, offset = 0)).asList()
            assertEquals("Should have 10 persons before delete", 10, allPersonsBefore.size)
            
            // Delete all persons
            val allIds = insertedPersons.map { it.id }
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = allIds)
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify all persons were deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 15, offset = 0)).asList()
            assertEquals("Should have 0 persons after delete all", 0, personsAfter.size)
        }
    }

    @Test
    fun testDeleteWithComplexIdPatterns() {
        runBlocking {
            database.open()
            
            // Insert persons and collect their IDs
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..12) {
                val person = database.person.add(PersonQuery.Add.Params(
                    email = "complex-pattern-$i@example.com",
                    firstName = "ComplexPattern$i",
                    lastName = "Test",
                    phone = "+${(7000000000L + i).toString()}",
                    birthDate = LocalDate(1970 + i, (i % 12) + 1, (i % 28) + 1)
                )).executeReturningOne()
                insertedPersons.add(person)
            }
            
            // Delete every 3rd person (complex pattern)
            val idsToDelete = insertedPersons.filterIndexed { index, _ -> (index + 1) % 3 == 0 }.map { it.id }
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = idsToDelete)
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify correct persons were deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 15, offset = 0)).asList()
            assertEquals("Should have 8 persons remaining (12 - 4)", 8, personsAfter.size)
            
            // Verify remaining persons are not the deleted ones
            val remainingIds = personsAfter.map { it.id }.toSet()
            val deletedIds = idsToDelete.toSet()
            
            assertTrue("No remaining ID should be in deleted IDs", remainingIds.intersect(deletedIds).isEmpty())
            
            // Verify we have the expected remaining persons (1st, 2nd, 4th, 5th, 7th, 8th, 10th, 11th)
            val expectedRemainingIndices = listOf(0, 1, 3, 4, 6, 7, 9, 10) // 0-based indices
            val expectedRemainingIds = expectedRemainingIndices.map { insertedPersons[it].id }.toSet()
            assertEquals("Remaining IDs should match expected pattern", expectedRemainingIds, remainingIds)
        }
    }
}
