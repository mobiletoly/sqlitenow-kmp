package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.CommentQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddResult
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlin.test.*

/**
 * Comprehensive integration tests for SQLiteNow DELETE operations.
 * Ensures ExecuteStatement wrappers handle DELETE cases.
 */
class DeleteOperationsTest {

    private lateinit var database: LibraryTestDatabase

    private fun runDatabaseTest(block: suspend () -> Unit) = runPlatformTest {
        database = TestDatabaseHelper.createDatabase()
        try {
            block()
        } finally {
            database.close()
        }
    }

    private data class DeleteByIdsScenario(
        val people: List<String>,
        val idsToDelete: (List<PersonAddResult>) -> List<Long>,
        val expectedRemaining: List<String>,
        val selectLimit: Int = 10,
    )

    private suspend fun runDeleteByIdsScenario(scenario: DeleteByIdsScenario) {
        database.open()

        val insertedPersons = scenario.people.mapIndexed { index, firstName ->
            insertPerson(firstName = firstName, index = index)
        }

        val personsBefore = selectAllPersons(scenario.selectLimit)
        assertEquals(scenario.people.size, personsBefore.size, "Should have ${scenario.people.size} persons before delete")

        database.person.deleteByIds(
            PersonQuery.DeleteByIds.Params(ids = scenario.idsToDelete(insertedPersons))
        )

        val personsAfter = selectAllPersons(scenario.selectLimit)
        assertEquals(scenario.expectedRemaining.size, personsAfter.size, "Unexpected remaining person count")

        val expectedIds = scenario.expectedRemaining.map { expectedName ->
            insertedPersons[scenario.people.indexOf(expectedName)].id
        }.toSet()
        assertEquals(expectedIds, personsAfter.map { it.id }.toSet(), "Remaining IDs should match")
        assertEquals(scenario.expectedRemaining.toSet(), personsAfter.map { it.myFirstName }.toSet(), "Remaining names should match")
    }

    private suspend fun insertPerson(firstName: String, index: Int): PersonAddResult =
        database.person.add.one(PersonQuery.Add.Params(
            email = "${firstName.lowercase()}-$index@example.com",
            firstName = firstName,
            lastName = "Test",
            phone = "+${(1000000000L + index).toString()}",
            birthDate = LocalDate(1980 + index, (index % 12) + 1, (index % 28) + 1)
        ))

    private suspend fun selectAllPersons(limit: Int) =
        database.person.selectAll(PersonQuery.SelectAll.Params(limit = limit.toLong(), offset = 0)).asList()

    @Test
    fun testBasicDeleteOperation() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("Delete1", "Delete2", "Keep"),
                idsToDelete = { people -> listOf(people[0].id, people[1].id) },
                expectedRemaining = listOf("Keep"),
            ))
    }

    @Test
    fun testDeleteSingleRecord() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("Single"),
                idsToDelete = { people -> listOf(people[0].id) },
                expectedRemaining = emptyList(),
            ))
    }

    @Test
    fun testDeleteNonExistentRecords() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("Existing"),
                idsToDelete = { listOf(99999L, 88888L, 77777L) },
                expectedRemaining = listOf("Existing"),
            ))
    }

    @Test
    fun testDeleteMixedExistentAndNonExistentRecords() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("Mixed1", "Mixed2", "Mixed3"),
                idsToDelete = { people -> listOf(people[0].id, 99999L, people[2].id, 88888L) },
                expectedRemaining = listOf("Mixed2"),
            ))
    }

    @Test
    fun testDeleteEmptyIdsList() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("EmptyList"),
                idsToDelete = { emptyList() },
                expectedRemaining = listOf("EmptyList"),
            ))
    }

    @Test
    fun testDeleteWithDuplicateIds() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("Duplicate1", "Duplicate2"),
                idsToDelete = { people -> listOf(people[0].id, people[0].id, people[1].id, people[0].id) },
                expectedRemaining = emptyList(),
            ))
    }

    @Test
    fun testDeleteLargeNumberOfRecords() = runDatabaseTest {
            database.open()
            
            // Insert many persons
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..20) {
                val person = database.person.add.one(PersonQuery.Add.Params(
                    email = "bulk-delete-$i@example.com",
                    firstName = "BulkDelete$i",
                    lastName = "Test",
                    phone = "+${i.toString().padStart(10, '0')}",
                    birthDate = LocalDate(1980 + i, (i % 12) + 1, (i % 28) + 1)
                ))
                insertedPersons.add(person)
            }
            
            // Verify all persons exist
            val allPersonsBefore = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 25, offset = 0)).asList()
            assertEquals(20, allPersonsBefore.size, "Should have 20 persons before delete")
            
            // Delete first 15 persons
            val idsToDelete = insertedPersons.take(15).map { it.id }
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = idsToDelete)
            
            database.person.deleteByIds(deleteParams)
            
            // Verify deletion
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 25, offset = 0)).asList()
            assertEquals(5, personsAfter.size, "Should have 5 persons remaining")
            
            // Verify remaining persons are the last 5
            val remainingIds = personsAfter.map { it.id }.toSet()
            val expectedRemainingIds = insertedPersons.takeLast(5).map { it.id }.toSet()
            assertEquals(expectedRemainingIds, remainingIds, "Remaining IDs should match last 5 inserted")
    }

    @Test
    fun testDeleteWithForeignKeyConstraints() = runDatabaseTest {
            database.open()
            
            // Insert person with related data
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "foreign-key@example.com",
                firstName = "ForeignKey",
                lastName = "Test",
                phone = "+1212121212",
                birthDate = LocalDate(1993, 4, 8)
            ))
            
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
            ))
            
            // Insert related comment
            database.comment.add(CommentQuery.Add.Params(
                personId = person.id,
                comment = "Comment with foreign key reference",
                createdAt = LocalDateTime(2024, 4, 8, 10, 30, 45),
                tags = listOf("foreign-key", "constraint", "test")
            ))
            
            // Verify related data exists
            val addressesBefore = database.personAddress.selectAll.asList()
            val commentsBefore = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals(1, addressesBefore.size, "Should have 1 address")
            assertEquals(1, commentsBefore.size, "Should have 1 comment")
            
            // Delete the person (this should cascade or handle foreign key constraints appropriately)
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = listOf(person.id))
            database.person.deleteByIds(deleteParams)
            
            // Verify person was deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(0, personsAfter.size, "Should have 0 persons after delete")
            
            // Note: The behavior of related data depends on foreign key constraint configuration
            // This test verifies the delete operation completes without errors
    }

    @Test
    fun testDeleteOrderIndependence() = runDatabaseTest {
            runDeleteByIdsScenario(DeleteByIdsScenario(
                people = listOf("Order1", "Order2", "Order3"),
                idsToDelete = { people -> listOf(people[2].id, people[0].id) },
                expectedRemaining = listOf("Order2"),
            ))
    }

    @Test
    fun testDeleteAllRecords() = runDatabaseTest {
            database.open()
            
            // Insert multiple persons
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..10) {
                val person = database.person.add.one(PersonQuery.Add.Params(
                    email = "delete-all-$i@example.com",
                    firstName = "DeleteAll$i",
                    lastName = "Test",
                    phone = "+${(6000000000L + i).toString()}",
                    birthDate = LocalDate(1975 + i, (i % 12) + 1, (i % 28) + 1)
                ))
                insertedPersons.add(person)
            }
            
            // Verify all persons exist
            val allPersonsBefore = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 15, offset = 0)).asList()
            assertEquals(10, allPersonsBefore.size, "Should have 10 persons before delete")
            
            // Delete all persons
            val allIds = insertedPersons.map { it.id }
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = allIds)
            
            database.person.deleteByIds(deleteParams)
            
            // Verify all persons were deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 15, offset = 0)).asList()
            assertEquals(0, personsAfter.size, "Should have 0 persons after delete all")
    }

    @Test
    fun testDeleteWithComplexIdPatterns() = runDatabaseTest {
            database.open()
            
            // Insert persons and collect their IDs
            val insertedPersons = mutableListOf<PersonAddResult>()
            for (i in 1..12) {
                val person = database.person.add.one(PersonQuery.Add.Params(
                    email = "complex-pattern-$i@example.com",
                    firstName = "ComplexPattern$i",
                    lastName = "Test",
                    phone = "+${(7000000000L + i).toString()}",
                    birthDate = LocalDate(1970 + i, (i % 12) + 1, (i % 28) + 1)
                ))
                insertedPersons.add(person)
            }
            
            // Delete every 3rd person (complex pattern)
            val idsToDelete = insertedPersons.filterIndexed { index, _ -> (index + 1) % 3 == 0 }.map { it.id }
            val deleteParams = PersonQuery.DeleteByIds.Params(ids = idsToDelete)
            
            database.person.deleteByIds(deleteParams)
            
            // Verify correct persons were deleted
            val personsAfter = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 15, offset = 0)).asList()
            assertEquals(8, personsAfter.size, "Should have 8 persons remaining (12 - 4)")
            
            // Verify remaining persons are not the deleted ones
            val remainingIds = personsAfter.map { it.id }.toSet()
            val deletedIds = idsToDelete.toSet()
            
            assertTrue(remainingIds.intersect(deletedIds).isEmpty(), "No remaining ID should be in deleted IDs")
            
            // Verify we have the expected remaining persons (1st, 2nd, 4th, 5th, 7th, 8th, 10th, 11th)
            val expectedRemainingIndices = listOf(0, 1, 3, 4, 6, 7, 9, 10) // 0-based indices
            val expectedRemainingIds = expectedRemainingIndices.map { insertedPersons[it].id }.toSet()
            assertEquals(expectedRemainingIds, remainingIds, "Remaining IDs should match expected pattern")
    }
}
