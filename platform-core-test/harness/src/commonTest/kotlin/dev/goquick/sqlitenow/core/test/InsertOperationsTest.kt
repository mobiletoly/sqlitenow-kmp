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
 * Comprehensive integration tests for SQLiteNow INSERT operations.
 * Validates ExecuteStatement and ExecuteReturningStatement wrappers end-to-end.
 */
class InsertOperationsTest {

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
    fun testBasicInsertWithReturning() = runDatabaseTest {
            database.open()
            
            val insertParams = PersonQuery.Add.Params(
                email = "basic-insert@example.com",
                firstName = "Basic",
                lastName = "Insert",
                phone = "+1234567890",
                birthDate = LocalDate(1990, 1, 1)
            )
            
            val insertedPerson = database.person.add.one(insertParams)
            
            // Verify all fields were inserted correctly
            assertTrue(insertedPerson.id > 0, "ID should be positive")
            assertEquals("basic-insert@example.com", insertedPerson.email, "Email should match")
            assertEquals("Basic", insertedPerson.firstName, "First name should match")
            assertEquals("Insert", insertedPerson.lastName, "Last name should match")
            assertEquals("+1234567890", insertedPerson.phone, "Phone should match")
            assertEquals(LocalDate(1990, 1, 1), insertedPerson.birthDate, "Birth date should match")
            assertNotNull(insertedPerson.createdAt, "Created at should be set")
    }

    @Test
    fun testInsertWithNullValues() = runDatabaseTest {
            database.open()
            
            val insertParams = PersonQuery.Add.Params(
                email = "null-insert@example.com",
                firstName = "Null",
                lastName = "Insert",
                phone = null, // Test null phone
                birthDate = null // Test null birth date
            )
            
            val insertedPerson = database.person.add.one(insertParams)
            
            // Verify null values were handled correctly
            assertTrue(insertedPerson.id > 0, "ID should be positive")
            assertEquals("null-insert@example.com", insertedPerson.email, "Email should match")
            assertEquals("Null", insertedPerson.firstName, "First name should match")
            assertEquals("Insert", insertedPerson.lastName, "Last name should match")
            assertNull(insertedPerson.phone, "Phone should be null")
            assertNull(insertedPerson.birthDate, "Birth date should be null")
            assertNotNull(insertedPerson.createdAt, "Created at should be set")
    }

    @Test
    fun testInsertWithUpsertBehavior() = runDatabaseTest {
            database.open()
            
            // First insert
            val originalParams = PersonQuery.Add.Params(
                email = "upsert@example.com",
                firstName = "Original",
                lastName = "Name",
                phone = "+1111111111",
                birthDate = LocalDate(1985, 5, 15)
            )
            
            val firstInsert = database.person.add.one(originalParams)
            
            // Second insert with same email (should trigger ON CONFLICT DO UPDATE)
            val upsertParams = PersonQuery.Add.Params(
                email = "upsert@example.com", // Same email
                firstName = "Updated",
                lastName = "Upserted",
                phone = "+2222222222",
                birthDate = LocalDate(1986, 6, 16)
            )
            
            val upsertedPerson = database.person.add.one(upsertParams)
            
            // Verify upsert behavior
            assertEquals(firstInsert.id, upsertedPerson.id, "Should be same ID (updated, not new record)")
            assertEquals("upsert@example.com", upsertedPerson.email, "Email should remain same")
            assertEquals("Updated", upsertedPerson.firstName, "First name should be updated")
            assertEquals("Upserted", upsertedPerson.lastName, "Last name should be updated")
            assertEquals("+2222222222", upsertedPerson.phone, "Phone should be updated")
            assertEquals(LocalDate(1986, 6, 16), upsertedPerson.birthDate, "Birth date should be updated")
            
            // Verify only one record exists
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(1, allPersons.size, "Should have only 1 person (upserted)")
    }

    @Test
    fun testBatchInsertOperations() = runDatabaseTest {
            database.open()
            
            // Insert multiple persons in sequence
            val insertedPersons = mutableListOf<PersonAddResult>()
            
            for (i in 1..5) {
                val params = PersonQuery.Add.Params(
                    email = "batch$i@example.com",
                    firstName = "Batch$i",
                    lastName = "Insert",
                    phone = "+${i.toString().padStart(10, '0')}",
                    birthDate = LocalDate(1990 + i, i, i)
                )
                
                val insertedPerson = database.person.add.one(params)
                insertedPersons.add(insertedPerson)
                
                // Verify each insert
                assertTrue(insertedPerson.id > 0, "ID should be positive for person $i")
                assertEquals("batch$i@example.com", insertedPerson.email, "Email should match for person $i")
                assertEquals("Batch$i", insertedPerson.firstName, "First name should match for person $i")
            }
            
            // Verify all persons were inserted
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(5, allPersons.size, "Should have 5 persons")
            
            // Verify IDs are unique and sequential
            val ids = insertedPersons.map { it.id }.sorted()
            assertEquals(5, ids.toSet().size, "Should have 5 unique IDs")
            
            // Verify IDs are sequential (assuming auto-increment)
            for (i in 1 until ids.size) {
                assertTrue(ids[i] > ids[i-1], "IDs should be sequential")
            }
    }

    @Test
    fun testInsertWithComplexTypes() = runDatabaseTest {
            database.open()
            
            // Insert person first
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "complex-types@example.com",
                firstName = "Complex",
                lastName = "Types",
                phone = "+3333333333",
                birthDate = LocalDate(1992, 8, 25)
            ))
            
            // Insert comment with JSON array (List<String>)
            val complexTags = listOf(
                "complex", "types", "json", "array", 
                "special chars: 'quotes'", "unicode: 🚀", 
                "numbers: 123", "symbols: @#$%"
            )
            
            val commentParams = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Comment with complex JSON array and special characters: 'test', \"quotes\", & symbols",
                createdAt = LocalDateTime(2024, 8, 25, 14, 30, 45),
                tags = complexTags
            )
            
            database.comment.add(commentParams)
            
            // Insert address with enum type
            val addressParams = PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.HOME,
                street = "123 Complex Types St",
                city = "Type City",
                state = "TC",
                postalCode = "12345",
                country = "Type Country",
                isPrimary = true
            )
            
            database.personAddress.add(addressParams)
            
            // Verify complex types were inserted correctly
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals(1, comments.size, "Should have 1 comment")
            assertEquals(complexTags, comments[0].tags, "Tags should match exactly")
            assertTrue(comments[0].comment.contains("'test'") && 
                comments[0].comment.contains("\"quotes\"") &&
                comments[0].comment.contains("& symbols"), "Comment should contain special characters")
            
            val addresses = database.personAddress.selectAll.asList()
            assertEquals(1, addresses.size, "Should have 1 address")
            assertEquals(AddressType.HOME, addresses[0].addressType, "Address type should be HOME")
            assertTrue(addresses[0].isPrimary, "Should be primary address")
    }

    @Test
    fun testInsertWithDateTimeEdgeCases() = runDatabaseTest {
            database.open()
            
            // Test edge case dates
            val edgeCaseDates = listOf(
                LocalDate(1900, 1, 1), // Very old date
                LocalDate(2099, 12, 31), // Future date
                LocalDate(2000, 2, 29), // Leap year
                LocalDate(1999, 2, 28), // Non-leap year
                null // Null date
            )
            
            val insertedPersons = mutableListOf<PersonAddResult>()
            
            edgeCaseDates.forEachIndexed { index, date ->
                val params = PersonQuery.Add.Params(
                    email = "edge-date-$index@example.com",
                    firstName = "EdgeDate$index",
                    lastName = "Test",
                    phone = "+${(4000000000L + index).toString()}",
                    birthDate = date
                )
                
                val insertedPerson = database.person.add.one(params)
                insertedPersons.add(insertedPerson)
                
                assertEquals(date, insertedPerson.birthDate, "Birth date should match for person $index")
            }
            
            // Test edge case DateTimes
            val person = insertedPersons[0]
            val edgeDateTime = LocalDateTime(1970, 1, 1, 0, 0, 0) // Unix epoch
            
            val commentParams = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Edge case datetime test",
                createdAt = edgeDateTime,
                tags = listOf("edge", "datetime", "unix-epoch")
            )
            
            database.comment.add(commentParams)
            
            // Verify edge case datetime
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals(1, comments.size, "Should have 1 comment")
            assertEquals(edgeDateTime, comments[0].createdAt, "DateTime should match Unix epoch")
    }

    @Test
    fun testInsertWithLongStrings() = runDatabaseTest {
            database.open()
            
            // Test with very long strings
            val longEmail = "very-long-email-address-that-exceeds-normal-length-limits-for-testing-purposes@very-long-domain-name-that-also-exceeds-normal-limits.example.com"
            val longFirstName = "VeryLongFirstNameThatExceedsNormalLengthLimitsForTestingPurposesAndShouldStillWorkCorrectly"
            val longLastName = "VeryLongLastNameThatExceedsNormalLengthLimitsForTestingPurposesAndShouldStillWorkCorrectlyToo"
            val longPhone = "+1-800-555-0123-ext-9999-department-customer-service-international"
            
            val params = PersonQuery.Add.Params(
                email = longEmail,
                firstName = longFirstName,
                lastName = longLastName,
                phone = longPhone,
                birthDate = LocalDate(1988, 7, 14)
            )
            
            val insertedPerson = database.person.add.one(params)
            
            // Verify long strings were inserted correctly
            assertEquals(longEmail, insertedPerson.email, "Long email should match")
            assertEquals(longFirstName, insertedPerson.firstName, "Long first name should match")
            assertEquals(longLastName, insertedPerson.lastName, "Long last name should match")
            assertEquals(longPhone, insertedPerson.phone, "Long phone should match")
            
            // Test very long comment
            val longComment = "This is a very long comment that contains multiple sentences and should test the limits of string storage in SQLite. ".repeat(10)
            val longTags = (1..50).map { "very-long-tag-name-$it-with-extra-content-to-make-it-longer" }
            
            val commentParams = CommentQuery.Add.Params(
                personId = insertedPerson.id,
                comment = longComment,
                createdAt = LocalDateTime(2024, 7, 14, 16, 45, 30),
                tags = longTags
            )
            
            database.comment.add(commentParams)
            
            // Verify long comment and tags
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals(1, comments.size, "Should have 1 comment")
            assertEquals(longComment, comments[0].comment, "Long comment should match")
            assertEquals(longTags, comments[0].tags, "Long tags should match")
    }

    @Test
    fun testInsertConstraintValidation() = runDatabaseTest {
            database.open()
            
            // Insert person with valid data
            val validParams = PersonQuery.Add.Params(
                email = "constraint-test@example.com",
                firstName = "Constraint",
                lastName = "Test",
                phone = "+5555555555",
                birthDate = LocalDate(1995, 3, 10)
            )
            
            val insertedPerson = database.person.add.one(validParams)
            assertTrue(insertedPerson.id > 0, "Valid insert should succeed")
            
            // Test foreign key constraint with address
            val validAddressParams = PersonAddressQuery.Add.Params(
                personId = insertedPerson.id, // Valid foreign key
                addressType = AddressType.WORK,
                street = "456 Constraint Ave",
                city = "Constraint City",
                state = "CC",
                postalCode = "54321",
                country = "Constraint Country",
                isPrimary = false
            )
            
            database.personAddress.add(validAddressParams)
            
            // Verify address was inserted with correct foreign key
            val addresses = database.personAddress.selectAll.asList()
            assertEquals(1, addresses.size, "Should have 1 address")
            assertEquals(insertedPerson.id, addresses[0].personId, "Foreign key should match")
            
            // Test comment foreign key constraint
            val validCommentParams = CommentQuery.Add.Params(
                personId = insertedPerson.id, // Valid foreign key
                comment = "Valid foreign key comment",
                createdAt = LocalDateTime(2024, 3, 10, 10, 15, 20),
                tags = listOf("constraint", "foreign-key", "valid")
            )
            
            database.comment.add(validCommentParams)
            
            // Verify comment was inserted with correct foreign key
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals(1, comments.size, "Should have 1 comment")
            assertEquals(insertedPerson.id, comments[0].personId, "Foreign key should match")
    }
}
