package dev.goquick.sqlitenow.librarytest

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.librarytest.db.AddressType
import dev.goquick.sqlitenow.librarytest.db.LibraryTestDatabase
import dev.goquick.sqlitenow.librarytest.db.PersonQuery
import dev.goquick.sqlitenow.librarytest.db.VersionBasedDatabaseMigrations
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Integration tests specifically for SQLiteNow RETURNING clause functionality.
 * Tests ExecuteReturningRunners with executeReturningList(), executeReturningOne(), executeReturningOneOrNull().
 */
@RunWith(AndroidJUnit4::class)
class ReturningClauseTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setup() {
        // Create database with all required adapters
        database = LibraryTestDatabase(
            dbName = ":memory:",
            migration = VersionBasedDatabaseMigrations(),
            debug = true,
            categoryAdapters = LibraryTestDatabase.CategoryAdapters(
                sqlValueToCreatedAt = { LocalDateTime.fromSqliteTimestamp(it) },
                sqlValueToBirthDate = { it?.let { LocalDate.fromSqliteDate(it) } }
            ),
            personAdapters = LibraryTestDatabase.PersonAdapters(
                birthDateToSqlValue = { it?.toSqliteDate() },
                sqlValueToAddressType = { AddressType.from(it) },
                sqlValueToTags = { it?.let { Json.decodeFromString<List<String>>(it) } }
            ),
            commentAdapters = LibraryTestDatabase.CommentAdapters(
                createdAtToSqlValue = { it.toSqliteTimestamp() },
                tagsToSqlValue = { it?.let { Json.encodeToString(it) } }
            ),
            personCategoryAdapters = LibraryTestDatabase.PersonCategoryAdapters(
                sqlValueToAssignedAt = { LocalDateTime.fromSqliteTimestamp(it) }
            ),
            personAddressAdapters = LibraryTestDatabase.PersonAddressAdapters(
                addressTypeToSqlValue = { it.value }
            )
        )
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
            
            // Test executeReturningOne with INSERT RETURNING
            val testPerson = PersonQuery.Add.Params(
                email = "returning-one@example.com",
                firstName = "ReturningOne",
                lastName = "Test",
                phone = "+1111111111",
                birthDate = LocalDate(1990, 1, 15)
            )
            
            val insertedPerson = database.person.add(testPerson).executeReturningOne()
            
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
            
            // Test executeReturningList with INSERT RETURNING (single insert returns list with one item)
            val testPerson = PersonQuery.Add.Params(
                email = "returning-list@example.com",
                firstName = "ReturningList",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = LocalDate(1985, 5, 20)
            )
            
            val insertedList = database.person.add(testPerson).executeReturningList()
            
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
            
            // Test executeReturningOneOrNull with successful INSERT RETURNING
            val testPerson = PersonQuery.Add.Params(
                email = "returning-or-null@example.com",
                firstName = "ReturningOrNull",
                lastName = "Test",
                phone = "+3333333333",
                birthDate = LocalDate(1992, 8, 10)
            )
            
            val insertedPerson = database.person.add(testPerson).executeReturningOneOrNull()
            
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
            
            val insertedPerson = database.person.add(testPerson).executeReturningOne()
            
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
            
            val insertedPerson = database.person.add(testPerson).executeReturningOne()
            
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
            val insertedPerson = database.person.add(testPerson).executeReturningOne()
            
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
            val firstInsert = database.person.add(originalPerson).executeReturningOne()
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
            
            val secondInsert = database.person.add(updatedPerson).executeReturningOne()
            
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
}
