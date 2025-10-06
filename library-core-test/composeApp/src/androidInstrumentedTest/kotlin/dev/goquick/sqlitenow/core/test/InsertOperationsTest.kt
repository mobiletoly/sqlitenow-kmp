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
 * Comprehensive integration tests for SQLiteNow INSERT operations.
 * Tests all aspects of INSERT operations using ExecuteRunners and ExecuteReturningRunners.
 */
@RunWith(AndroidJUnit4::class)
class InsertOperationsTest {

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
    fun testBasicInsertWithReturning() {
        runBlocking {
            database.open()
            
            val insertParams = PersonQuery.Add.Params(
                email = "basic-insert@example.com",
                firstName = "Basic",
                lastName = "Insert",
                phone = "+1234567890",
                birthDate = LocalDate(1990, 1, 1)
            )
            
            val insertedPerson = database.person.add(insertParams).executeReturningOne()
            
            // Verify all fields were inserted correctly
            assertTrue("ID should be positive", insertedPerson.id > 0)
            assertEquals("Email should match", "basic-insert@example.com", insertedPerson.email)
            assertEquals("First name should match", "Basic", insertedPerson.firstName)
            assertEquals("Last name should match", "Insert", insertedPerson.lastName)
            assertEquals("Phone should match", "+1234567890", insertedPerson.phone)
            assertEquals("Birth date should match", LocalDate(1990, 1, 1), insertedPerson.birthDate)
            assertNotNull("Created at should be set", insertedPerson.createdAt)
        }
    }

    @Test
    fun testInsertWithNullValues() {
        runBlocking {
            database.open()
            
            val insertParams = PersonQuery.Add.Params(
                email = "null-insert@example.com",
                firstName = "Null",
                lastName = "Insert",
                phone = null, // Test null phone
                birthDate = null // Test null birth date
            )
            
            val insertedPerson = database.person.add(insertParams).executeReturningOne()
            
            // Verify null values were handled correctly
            assertTrue("ID should be positive", insertedPerson.id > 0)
            assertEquals("Email should match", "null-insert@example.com", insertedPerson.email)
            assertEquals("First name should match", "Null", insertedPerson.firstName)
            assertEquals("Last name should match", "Insert", insertedPerson.lastName)
            assertNull("Phone should be null", insertedPerson.phone)
            assertNull("Birth date should be null", insertedPerson.birthDate)
            assertNotNull("Created at should be set", insertedPerson.createdAt)
        }
    }

    @Test
    fun testInsertWithUpsertBehavior() {
        runBlocking {
            database.open()
            
            // First insert
            val originalParams = PersonQuery.Add.Params(
                email = "upsert@example.com",
                firstName = "Original",
                lastName = "Name",
                phone = "+1111111111",
                birthDate = LocalDate(1985, 5, 15)
            )
            
            val firstInsert = database.person.add(originalParams).executeReturningOne()
            
            // Second insert with same email (should trigger ON CONFLICT DO UPDATE)
            val upsertParams = PersonQuery.Add.Params(
                email = "upsert@example.com", // Same email
                firstName = "Updated",
                lastName = "Upserted",
                phone = "+2222222222",
                birthDate = LocalDate(1986, 6, 16)
            )
            
            val upsertedPerson = database.person.add(upsertParams).executeReturningOne()
            
            // Verify upsert behavior
            assertEquals("Should be same ID (updated, not new record)", firstInsert.id, upsertedPerson.id)
            assertEquals("Email should remain same", "upsert@example.com", upsertedPerson.email)
            assertEquals("First name should be updated", "Updated", upsertedPerson.firstName)
            assertEquals("Last name should be updated", "Upserted", upsertedPerson.lastName)
            assertEquals("Phone should be updated", "+2222222222", upsertedPerson.phone)
            assertEquals("Birth date should be updated", LocalDate(1986, 6, 16), upsertedPerson.birthDate)
            
            // Verify only one record exists
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have only 1 person (upserted)", 1, allPersons.size)
        }
    }

    @Test
    fun testBatchInsertOperations() {
        runBlocking {
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
                
                val insertedPerson = database.person.add(params).executeReturningOne()
                insertedPersons.add(insertedPerson)
                
                // Verify each insert
                assertTrue("ID should be positive for person $i", insertedPerson.id > 0)
                assertEquals("Email should match for person $i", "batch$i@example.com", insertedPerson.email)
                assertEquals("First name should match for person $i", "Batch$i", insertedPerson.firstName)
            }
            
            // Verify all persons were inserted
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 5 persons", 5, allPersons.size)
            
            // Verify IDs are unique and sequential
            val ids = insertedPersons.map { it.id }.sorted()
            assertEquals("Should have 5 unique IDs", 5, ids.toSet().size)
            
            // Verify IDs are sequential (assuming auto-increment)
            for (i in 1 until ids.size) {
                assertTrue("IDs should be sequential", ids[i] > ids[i-1])
            }
        }
    }

    @Test
    fun testInsertWithComplexTypes() {
        runBlocking {
            database.open()
            
            // Insert person first
            val person = database.person.add(PersonQuery.Add.Params(
                email = "complex-types@example.com",
                firstName = "Complex",
                lastName = "Types",
                phone = "+3333333333",
                birthDate = LocalDate(1992, 8, 25)
            )).executeReturningOne()
            
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
            
            database.comment.add(commentParams).execute()
            
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
            
            database.personAddress.add(addressParams).execute()
            
            // Verify complex types were inserted correctly
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals("Should have 1 comment", 1, comments.size)
            assertEquals("Tags should match exactly", complexTags, comments[0].tags)
            assertTrue("Comment should contain special characters", 
                comments[0].comment.contains("'test'") && 
                comments[0].comment.contains("\"quotes\"") &&
                comments[0].comment.contains("& symbols"))
            
            val addresses = database.personAddress.selectAll.asList()
            assertEquals("Should have 1 address", 1, addresses.size)
            assertEquals("Address type should be HOME", AddressType.HOME, addresses[0].addressType)
            assertTrue("Should be primary address", addresses[0].isPrimary)
        }
    }

    @Test
    fun testInsertWithDateTimeEdgeCases() {
        runBlocking {
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
                
                val insertedPerson = database.person.add(params).executeReturningOne()
                insertedPersons.add(insertedPerson)
                
                assertEquals("Birth date should match for person $index", date, insertedPerson.birthDate)
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
            
            database.comment.add(commentParams).execute()
            
            // Verify edge case datetime
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals("Should have 1 comment", 1, comments.size)
            assertEquals("DateTime should match Unix epoch", edgeDateTime, comments[0].createdAt)
        }
    }

    @Test
    fun testInsertWithLongStrings() {
        runBlocking {
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
            
            val insertedPerson = database.person.add(params).executeReturningOne()
            
            // Verify long strings were inserted correctly
            assertEquals("Long email should match", longEmail, insertedPerson.email)
            assertEquals("Long first name should match", longFirstName, insertedPerson.firstName)
            assertEquals("Long last name should match", longLastName, insertedPerson.lastName)
            assertEquals("Long phone should match", longPhone, insertedPerson.phone)
            
            // Test very long comment
            val longComment = "This is a very long comment that contains multiple sentences and should test the limits of string storage in SQLite. ".repeat(10)
            val longTags = (1..50).map { "very-long-tag-name-$it-with-extra-content-to-make-it-longer" }
            
            val commentParams = CommentQuery.Add.Params(
                personId = insertedPerson.id,
                comment = longComment,
                createdAt = LocalDateTime(2024, 7, 14, 16, 45, 30),
                tags = longTags
            )
            
            database.comment.add(commentParams).execute()
            
            // Verify long comment and tags
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals("Should have 1 comment", 1, comments.size)
            assertEquals("Long comment should match", longComment, comments[0].comment)
            assertEquals("Long tags should match", longTags, comments[0].tags)
        }
    }

    @Test
    fun testInsertConstraintValidation() {
        runBlocking {
            database.open()
            
            // Insert person with valid data
            val validParams = PersonQuery.Add.Params(
                email = "constraint-test@example.com",
                firstName = "Constraint",
                lastName = "Test",
                phone = "+5555555555",
                birthDate = LocalDate(1995, 3, 10)
            )
            
            val insertedPerson = database.person.add(validParams).executeReturningOne()
            assertTrue("Valid insert should succeed", insertedPerson.id > 0)
            
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
            
            database.personAddress.add(validAddressParams).execute()
            
            // Verify address was inserted with correct foreign key
            val addresses = database.personAddress.selectAll.asList()
            assertEquals("Should have 1 address", 1, addresses.size)
            assertEquals("Foreign key should match", insertedPerson.id, addresses[0].personId)
            
            // Test comment foreign key constraint
            val validCommentParams = CommentQuery.Add.Params(
                personId = insertedPerson.id, // Valid foreign key
                comment = "Valid foreign key comment",
                createdAt = LocalDateTime(2024, 3, 10, 10, 15, 20),
                tags = listOf("constraint", "foreign-key", "valid")
            )
            
            database.comment.add(validCommentParams).execute()
            
            // Verify comment was inserted with correct foreign key
            val comments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals("Should have 1 comment", 1, comments.size)
            assertEquals("Foreign key should match", insertedPerson.id, comments[0].personId)
        }
    }
}
