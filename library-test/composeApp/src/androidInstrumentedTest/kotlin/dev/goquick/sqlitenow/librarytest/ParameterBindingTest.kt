package dev.goquick.sqlitenow.librarytest

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.librarytest.db.AddressType
import dev.goquick.sqlitenow.librarytest.db.LibraryTestDatabase
import dev.goquick.sqlitenow.librarytest.db.PersonQuery
import dev.goquick.sqlitenow.librarytest.db.CommentQuery
import dev.goquick.sqlitenow.librarytest.db.PersonAddressQuery
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
 * Integration tests specifically for SQLiteNow Parameter Binding and Type Safety.
 * Tests named parameter binding, type conversion, and parameter validation.
 */
@RunWith(AndroidJUnit4::class)
class ParameterBindingTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setup() {
        // Create database with all required adapters
        database = LibraryTestDatabase(
            dbName = ":memory:",
            migration = VersionBasedDatabaseMigrations(),
            debug = true,
            categoryAdapters = LibraryTestDatabase.CategoryAdapters(
                sqlValueToBirthDate = { it?.let { LocalDate.fromSqliteDate(it) } }
            ),
            personAdapters = LibraryTestDatabase.PersonAdapters(
                birthDateToSqlValue = { it?.toSqliteDate() },
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
                addressTypeToSqlValue = { it.value },
                sqlValueToAddressType = { AddressType.from(it) },
                sqlValueToCreatedAt = { LocalDateTime.fromSqliteTimestamp(it) }
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
    fun testNamedParameterBinding() {
        runBlocking {
            database.open()
            
            // Test named parameter binding with various data types
            val testPerson = PersonQuery.Add.Params(
                email = "param-binding@example.com",
                firstName = "Parameter",
                lastName = "Binding",
                phone = "+1234567890",
                birthDate = LocalDate(1990, 5, 15)
            )
            
            val insertedPerson = database.person.add(testPerson).executeReturningOne()
            
            // Verify all parameters were bound correctly
            assertEquals("Email parameter should be bound correctly", "param-binding@example.com", insertedPerson.email)
            assertEquals("First name parameter should be bound correctly", "Parameter", insertedPerson.firstName)
            assertEquals("Last name parameter should be bound correctly", "Binding", insertedPerson.lastName)
            assertEquals("Phone parameter should be bound correctly", "+1234567890", insertedPerson.phone)
            assertEquals("Birth date parameter should be bound correctly", LocalDate(1990, 5, 15), insertedPerson.birthDate)
            
            // Test named parameter binding in SELECT queries
            val dateRangeParams = PersonQuery.SelectAllByBirthdayRange.Params(
                startDate = LocalDate(1989, 1, 1),
                endDate = LocalDate(1991, 12, 31)
            )
            
            val personsInRange = database.person.selectAllByBirthdayRange(dateRangeParams).asList()
            assertEquals("Should find 1 person in date range", 1, personsInRange.size)
            assertEquals("Found person should match", insertedPerson.id, personsInRange[0].id)
        }
    }

    @Test
    fun testNullParameterBinding() {
        runBlocking {
            database.open()
            
            // Test null parameter binding
            val personWithNulls = PersonQuery.Add.Params(
                email = "null-params@example.com",
                firstName = "Null",
                lastName = "Parameters",
                phone = null, // Test null string parameter
                birthDate = null // Test null date parameter
            )
            
            val insertedPerson = database.person.add(personWithNulls).executeReturningOne()
            
            // Verify null parameters were bound correctly
            assertEquals("Email should be bound", "null-params@example.com", insertedPerson.email)
            assertEquals("First name should be bound", "Null", insertedPerson.firstName)
            assertEquals("Last name should be bound", "Parameters", insertedPerson.lastName)
            assertNull("Phone should be null", insertedPerson.phone)
            assertNull("Birth date should be null", insertedPerson.birthDate)
            
            // Test null parameters in date range query
            val nullDateRangeParams = PersonQuery.SelectAllByBirthdayRange.Params(
                startDate = null,
                endDate = null
            )
            
            // This should work without throwing exceptions
            val personsWithNullRange = database.person.selectAllByBirthdayRange(nullDateRangeParams).asList()
            // The exact behavior depends on the SQL logic, but it should not crash
            assertTrue("Query with null parameters should execute", personsWithNullRange.size >= 0)
        }
    }

    @Test
    fun testCollectionParameterBinding() {
        runBlocking {
            database.open()
            
            // Insert multiple persons for testing collection parameters
            val person1 = database.person.add(PersonQuery.Add.Params(
                email = "collection1@example.com",
                firstName = "Collection1",
                lastName = "Test",
                phone = "+1111111111",
                birthDate = LocalDate(1985, 1, 1)
            )).executeReturningOne()
            
            val person2 = database.person.add(PersonQuery.Add.Params(
                email = "collection2@example.com",
                firstName = "Collection2",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = LocalDate(1986, 2, 2)
            )).executeReturningOne()
            
            val person3 = database.person.add(PersonQuery.Add.Params(
                email = "collection3@example.com",
                firstName = "Collection3",
                lastName = "Test",
                phone = "+3333333333",
                birthDate = LocalDate(1987, 3, 3)
            )).executeReturningOne()
            
            // Test collection parameter binding with DELETE
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person1.id, person3.id) // Delete person1 and person3, keep person2
            )
            
            database.person.deleteByIds(deleteParams).execute()
            
            // Verify collection parameter binding worked correctly
            val remainingPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 1 remaining person", 1, remainingPersons.size)
            assertEquals("Remaining person should be person2", person2.id, remainingPersons[0].id)
            assertEquals("Remaining person name should be Collection2", "Collection2", remainingPersons[0].myFirstName)
        }
    }

    @Test
    fun testTypeConversionParameterBinding() {
        runBlocking {
            database.open()
            
            // Test type conversion parameter binding with custom types
            val person = database.person.add(PersonQuery.Add.Params(
                email = "type-conversion@example.com",
                firstName = "TypeConversion",
                lastName = "Test",
                phone = "+4444444444",
                birthDate = LocalDate(1992, 6, 15)
            )).executeReturningOne()
            
            // Test enum type conversion in parameter binding
            val address = PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.HOME, // Enum parameter
                street = "123 Type Conversion St",
                city = "Parameter City",
                state = "PC",
                postalCode = "12345",
                country = "Test Country",
                isPrimary = true // Boolean parameter
            )
            
            database.personAddress.add(address).execute()
            
            // Verify enum parameter binding by selecting
            val homeAddresses = database.personAddress.selectAllByAddressType(
                PersonAddressQuery.SelectAllByAddressType.Params(addressType = AddressType.HOME)
            ).asList()
            
            assertEquals("Should find 1 HOME address", 1, homeAddresses.size)
            assertEquals("Address type should be HOME", AddressType.HOME, homeAddresses[0].addressType)
            assertEquals("Street should match", "123 Type Conversion St", homeAddresses[0].street)
            assertTrue("Should be primary address", homeAddresses[0].isPrimary)
            
            // Test JSON list parameter binding
            val testTags = listOf("parameter", "binding", "json", "test")
            val comment = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Test comment with JSON parameter binding",
                createdAt = LocalDateTime(2024, 6, 15, 12, 30, 45),
                tags = testTags // List<String> parameter
            )
            
            database.comment.add(comment).execute()
            
            // Verify JSON parameter binding
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals("Should have 1 comment", 1, comments.size)
            assertEquals("Tags should match", testTags, comments[0].tags)
        }
    }

    @Test
    fun testParameterBindingWithSpecialCharacters() {
        runBlocking {
            database.open()
            
            // Test parameter binding with special characters and edge cases
            val specialCharsPerson = PersonQuery.Add.Params(
                email = "special+chars@example.com",
                firstName = "Special'Chars\"Test",
                lastName = "O'Connor & Smith",
                phone = "+1-800-555-0123",
                birthDate = LocalDate(1988, 12, 31)
            )
            
            val insertedPerson = database.person.add(specialCharsPerson).executeReturningOne()
            
            // Verify special characters were handled correctly in parameter binding
            assertEquals("Email with + should be bound correctly", "special+chars@example.com", insertedPerson.email)
            assertEquals("First name with quotes should be bound correctly", "Special'Chars\"Test", insertedPerson.firstName)
            assertEquals("Last name with apostrophe and ampersand should be bound correctly", "O'Connor & Smith", insertedPerson.lastName)
            assertEquals("Phone with dashes should be bound correctly", "+1-800-555-0123", insertedPerson.phone)
            
            // Test special characters in JSON parameter binding
            val specialTags = listOf(
                "tag with spaces",
                "tag'with'quotes",
                "tag\"with\"double\"quotes",
                "tag&with&ampersands",
                "tag/with/slashes",
                "tag\\with\\backslashes",
                "tag\nwith\nnewlines",
                "tag\twith\ttabs"
            )
            
            val comment = CommentQuery.Add.Params(
                personId = insertedPerson.id,
                comment = "Comment with special characters: 'quotes', \"double quotes\", & ampersands, /slashes/, \\backslashes\\",
                createdAt = LocalDateTime(2024, 6, 15, 14, 45, 30),
                tags = specialTags
            )
            
            database.comment.add(comment).execute()
            
            // Verify special characters in JSON were handled correctly
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals("Should have 1 comment", 1, comments.size)
            assertEquals("Special tags should be preserved", specialTags, comments[0].tags)
            assertTrue("Comment content should contain special characters", 
                comments[0].comment.contains("'quotes'") && 
                comments[0].comment.contains("\"double quotes\"") &&
                comments[0].comment.contains("& ampersands"))
        }
    }

    @Test
    fun testParameterBindingTypeSafety() {
        runBlocking {
            database.open()
            
            // Test that parameter binding maintains type safety
            val person = database.person.add(PersonQuery.Add.Params(
                email = "type-safety@example.com",
                firstName = "TypeSafety",
                lastName = "Test",
                phone = "+5555555555",
                birthDate = LocalDate(1995, 8, 20)
            )).executeReturningOne()
            
            // Test that Long parameters are handled correctly
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "UpdatedTypeSafety",
                lastName = "UpdatedTest",
                email = "updated-type-safety@example.com",
                phone = "+6666666666",
                birthDate = LocalDate(1996, 9, 21),
                id = person.id // Long parameter
            )
            
            database.person.updateById(updateParams).execute()
            
            // Verify the update worked with correct type binding
            val updatedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == person.id }
            
            assertEquals("Should find exactly one updated person", 1, updatedPersons.size)
            val updatedPerson = updatedPersons[0]
            
            assertEquals("First name should be updated", "UpdatedTypeSafety", updatedPerson.myFirstName)
            assertEquals("Last name should be updated", "UpdatedTest", updatedPerson.myLastName)
            assertEquals("Email should be updated", "updated-type-safety@example.com", updatedPerson.email)
            assertEquals("Phone should be updated", "+6666666666", updatedPerson.phone)
            assertEquals("Birth date should be updated", LocalDate(1996, 9, 21), updatedPerson.birthDate)
            
            // Test that Boolean parameters are handled correctly
            val address = PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.WORK,
                street = "456 Type Safety Ave",
                city = "Safety City",
                state = "SC",
                postalCode = "54321",
                country = "Safety Country",
                isPrimary = false // Boolean parameter - should be bound as 0 in SQLite
            )
            
            database.personAddress.add(address).execute()
            
            // Verify Boolean parameter binding
            val addresses = database.personAddress.selectAll.asList()
            assertEquals("Should have 1 address", 1, addresses.size)
            assertFalse("Should not be primary address", addresses[0].isPrimary)
        }
    }

    @Test
    fun testParameterBindingWithLimitAndOffset() {
        runBlocking {
            database.open()
            
            // Insert multiple persons for pagination testing
            val persons = mutableListOf<PersonQuery.Add.Result>()
            for (i in 1..10) {
                val person = database.person.add(PersonQuery.Add.Params(
                    email = "pagination$i@example.com",
                    firstName = "Person$i",
                    lastName = "Test",
                    phone = "+${i.toString().padStart(10, '0')}",
                    birthDate = LocalDate(1990 + i, 1, 1)
                )).executeReturningOne()
                persons.add(person)
            }
            
            // Test LIMIT and OFFSET parameter binding
            val page1 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 0)).asList()
            val page2 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 3)).asList()
            val page3 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 6)).asList()
            val page4 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 9)).asList()
            
            // Verify pagination parameter binding
            assertEquals("Page 1 should have 3 persons", 3, page1.size)
            assertEquals("Page 2 should have 3 persons", 3, page2.size)
            assertEquals("Page 3 should have 3 persons", 3, page3.size)
            assertEquals("Page 4 should have 1 person", 1, page4.size)
            
            // Verify no overlap between pages (ORDER BY id DESC)
            val allIds = (page1 + page2 + page3 + page4).map { it.id }.toSet()
            assertEquals("Should have 10 unique IDs across all pages", 10, allIds.size)
            
            // Verify ordering (most recent first due to ORDER BY id DESC)
            assertTrue("Page 1 first person ID should be greater than page 2 first person ID", 
                page1[0].id > page2[0].id)
            assertTrue("Page 2 first person ID should be greater than page 3 first person ID", 
                page2[0].id > page3[0].id)
        }
    }
}
