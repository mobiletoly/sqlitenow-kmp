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
 * Integration tests specifically for SQLiteNow Parameter Binding and Type Safety.
 * Tests named parameter binding, type conversion, and parameter validation.
 */
class ParameterBindingTest {

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
    fun testNamedParameterBinding() = runDatabaseTest {
            database.open()
            
            // Test named parameter binding with various data types
            val testPerson = PersonQuery.Add.Params(
                email = "param-binding@example.com",
                firstName = "Parameter",
                lastName = "Binding",
                phone = "+1234567890",
                birthDate = LocalDate(1990, 5, 15)
            )
            
            val insertedPerson = database.person.add.one(testPerson)
            
            // Verify all parameters were bound correctly
            assertEquals("param-binding@example.com", insertedPerson.email, "Email parameter should be bound correctly")
            assertEquals("Parameter", insertedPerson.firstName, "First name parameter should be bound correctly")
            assertEquals("Binding", insertedPerson.lastName, "Last name parameter should be bound correctly")
            assertEquals("+1234567890", insertedPerson.phone, "Phone parameter should be bound correctly")
            assertEquals(LocalDate(1990, 5, 15), insertedPerson.birthDate, "Birth date parameter should be bound correctly")
            
            // Test named parameter binding in SELECT queries
            val dateRangeParams = PersonQuery.SelectAllByBirthdayRange.Params(
                startDate = LocalDate(1989, 1, 1),
                endDate = LocalDate(1991, 12, 31)
            )
            
            val personsInRange = database.person.selectAllByBirthdayRange(dateRangeParams).asList()
            assertEquals(1, personsInRange.size, "Should find 1 person in date range")
            assertEquals(insertedPerson.id, personsInRange[0].id, "Found person should match")
    }

    @Test
    fun testNullParameterBinding() = runDatabaseTest {
            database.open()
            
            // Test null parameter binding
            val personWithNulls = PersonQuery.Add.Params(
                email = "null-params@example.com",
                firstName = "Null",
                lastName = "Parameters",
                phone = null, // Test null string parameter
                birthDate = null // Test null date parameter
            )
            
            val insertedPerson = database.person.add.one(personWithNulls)
            
            // Verify null parameters were bound correctly
            assertEquals("null-params@example.com", insertedPerson.email, "Email should be bound")
            assertEquals("Null", insertedPerson.firstName, "First name should be bound")
            assertEquals("Parameters", insertedPerson.lastName, "Last name should be bound")
            assertNull(insertedPerson.phone, "Phone should be null")
            assertNull(insertedPerson.birthDate, "Birth date should be null")
            
            // Test null parameters in date range query
            val nullDateRangeParams = PersonQuery.SelectAllByBirthdayRange.Params(
                startDate = null,
                endDate = null
            )
            
            // This should work without throwing exceptions
            database.person.selectAllByBirthdayRange(nullDateRangeParams).asList()
    }

    @Test
    fun testCollectionParameterBinding() = runDatabaseTest {
            database.open()
            
            // Insert multiple persons for testing collection parameters
            val person1 = database.person.add.one(PersonQuery.Add.Params(
                email = "collection1@example.com",
                firstName = "Collection1",
                lastName = "Test",
                phone = "+1111111111",
                birthDate = LocalDate(1985, 1, 1)
            ))
            
            val person2 = database.person.add.one(PersonQuery.Add.Params(
                email = "collection2@example.com",
                firstName = "Collection2",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = LocalDate(1986, 2, 2)
            ))
            
            val person3 = database.person.add.one(PersonQuery.Add.Params(
                email = "collection3@example.com",
                firstName = "Collection3",
                lastName = "Test",
                phone = "+3333333333",
                birthDate = LocalDate(1987, 3, 3)
            ))
            
            // Test collection parameter binding with DELETE
            val deleteParams = PersonQuery.DeleteByIds.Params(
                ids = listOf(person1.id, person3.id) // Delete person1 and person3, keep person2
            )
            
            database.person.deleteByIds(deleteParams)
            
            // Verify collection parameter binding worked correctly
            val remainingPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(1, remainingPersons.size, "Should have 1 remaining person")
            assertEquals(person2.id, remainingPersons[0].id, "Remaining person should be person2")
            assertEquals("Collection2", remainingPersons[0].myFirstName, "Remaining person name should be Collection2")
    }

    @Test
    fun testTypeConversionParameterBinding() = runDatabaseTest {
            database.open()
            
            // Test type conversion parameter binding with custom types
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "type-conversion@example.com",
                firstName = "TypeConversion",
                lastName = "Test",
                phone = "+4444444444",
                birthDate = LocalDate(1992, 6, 15)
            ))
            
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
            
            database.personAddress.add(address)
            
            // Verify enum parameter binding by selecting
            val homeAddresses = database.personAddress.selectAllByAddressType(
                PersonAddressQuery.SelectAllByAddressType.Params(addressType = AddressType.HOME)
            ).asList()
            
            assertEquals(1, homeAddresses.size, "Should find 1 HOME address")
            assertEquals(AddressType.HOME, homeAddresses[0].addressType, "Address type should be HOME")
            assertEquals("123 Type Conversion St", homeAddresses[0].street, "Street should match")
            assertTrue(homeAddresses[0].isPrimary, "Should be primary address")
            
            // Test JSON list parameter binding
            val testTags = listOf("parameter", "binding", "json", "test")
            val comment = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Test comment with JSON parameter binding",
                createdAt = LocalDateTime(2024, 6, 15, 12, 30, 45),
                tags = testTags // List<String> parameter
            )
            
            database.comment.add(comment)
            
            // Verify JSON parameter binding
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals(1, comments.size, "Should have 1 comment")
            assertEquals(testTags, comments[0].tags, "Tags should match")
    }

    @Test
    fun testParameterBindingWithSpecialCharacters() = runDatabaseTest {
            database.open()
            
            // Test parameter binding with special characters and edge cases
            val specialCharsPerson = PersonQuery.Add.Params(
                email = "special+chars@example.com",
                firstName = "Special'Chars\"Test",
                lastName = "O'Connor & Smith",
                phone = "+1-800-555-0123",
                birthDate = LocalDate(1988, 12, 31)
            )
            
            val insertedPerson = database.person.add.one(specialCharsPerson)
            
            // Verify special characters were handled correctly in parameter binding
            assertEquals("special+chars@example.com", insertedPerson.email, "Email with + should be bound correctly")
            assertEquals("Special'Chars\"Test", insertedPerson.firstName, "First name with quotes should be bound correctly")
            assertEquals("O'Connor & Smith", insertedPerson.lastName, "Last name with apostrophe and ampersand should be bound correctly")
            assertEquals("+1-800-555-0123", insertedPerson.phone, "Phone with dashes should be bound correctly")
            
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
            
            database.comment.add(comment)
            
            // Verify special characters in JSON were handled correctly
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals(1, comments.size, "Should have 1 comment")
            assertEquals(specialTags, comments[0].tags, "Special tags should be preserved")
            assertTrue(comments[0].comment.contains("'quotes'") && 
                comments[0].comment.contains("\"double quotes\"") &&
                comments[0].comment.contains("& ampersands"), "Comment content should contain special characters")
    }

    @Test
    fun testParameterBindingTypeSafety() = runDatabaseTest {
            database.open()
            
            // Test that parameter binding maintains type safety
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "type-safety@example.com",
                firstName = "TypeSafety",
                lastName = "Test",
                phone = "+5555555555",
                birthDate = LocalDate(1995, 8, 20)
            ))
            
            // Test that Long parameters are handled correctly
            val updateParams = PersonQuery.UpdateById.Params(
                firstName = "UpdatedTypeSafety",
                lastName = "UpdatedTest",
                email = "updated-type-safety@example.com",
                phone = "+6666666666",
                birthDate = LocalDate(1996, 9, 21),
                id = person.id // Long parameter
            )
            
            database.person.updateById(updateParams)
            
            // Verify the update worked with correct type binding
            val updatedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == person.id }
            
            assertEquals(1, updatedPersons.size, "Should find exactly one updated person")
            val updatedPerson = updatedPersons[0]
            
            assertEquals("UpdatedTypeSafety", updatedPerson.myFirstName, "First name should be updated")
            assertEquals("UpdatedTest", updatedPerson.myLastName, "Last name should be updated")
            assertEquals("updated-type-safety@example.com", updatedPerson.email, "Email should be updated")
            assertEquals("+6666666666", updatedPerson.phone, "Phone should be updated")
            assertEquals(LocalDate(1996, 9, 21), updatedPerson.birthDate, "Birth date should be updated")
            
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
            
            database.personAddress.add(address)
            
            // Verify Boolean parameter binding
            val addresses = database.personAddress.selectAll.asList()
            assertEquals(1, addresses.size, "Should have 1 address")
            assertFalse(addresses[0].isPrimary, "Should not be primary address")
    }

    @Test
    fun testParameterBindingWithLimitAndOffset() = runDatabaseTest {
            database.open()
            
            // Insert multiple persons for pagination testing
            val persons = mutableListOf<PersonAddResult>()
            for (i in 1..10) {
                val person = database.person.add.one(PersonQuery.Add.Params(
                    email = "pagination$i@example.com",
                    firstName = "Person$i",
                    lastName = "Test",
                    phone = "+${i.toString().padStart(10, '0')}",
                    birthDate = LocalDate(1990 + i, 1, 1)
                ))
                persons.add(person)
            }
            
            // Test LIMIT and OFFSET parameter binding
            val page1 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 0)).asList()
            val page2 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 3)).asList()
            val page3 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 6)).asList()
            val page4 = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 3, offset = 9)).asList()
            
            // Verify pagination parameter binding
            assertEquals(3, page1.size, "Page 1 should have 3 persons")
            assertEquals(3, page2.size, "Page 2 should have 3 persons")
            assertEquals(3, page3.size, "Page 3 should have 3 persons")
            assertEquals(1, page4.size, "Page 4 should have 1 person")
            
            // Verify no overlap between pages (ORDER BY id DESC)
            val allIds = (page1 + page2 + page3 + page4).map { it.id }.toSet()
            assertEquals(10, allIds.size, "Should have 10 unique IDs across all pages")
            
            // Verify ordering (most recent first due to ORDER BY id DESC)
            assertTrue(page1[0].id > page2[0].id, "Page 1 first person ID should be greater than page 2 first person ID")
            assertTrue(page2[0].id > page3[0].id, "Page 2 first person ID should be greater than page 3 first person ID")
    }
}
