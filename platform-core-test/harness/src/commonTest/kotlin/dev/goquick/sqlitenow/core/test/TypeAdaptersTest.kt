package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.CommentQuery
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlin.test.*

/**
 * Integration tests specifically for SQLiteNow Type Adapters functionality.
 * Tests custom type adapters (LocalDate, LocalDateTime, custom objects, JSON serialization).
 */
class TypeAdaptersTest {

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
    fun testLocalDateTypeAdapter() = runDatabaseTest {
            database.open()

            // Test LocalDate adapter with a simple case first
            val testDate = LocalDate(1990, 1, 1)
            val person = PersonQuery.Add.Params(
                email = "date-test@example.com",
                firstName = "DateTest",
                lastName = "Test",
                phone = "+1111111111",
                birthDate = testDate
            )

            val insertedPerson = database.person.add.one(person)

            // Verify LocalDate adapter worked correctly in INSERT RETURNING
            assertEquals(testDate, insertedPerson.birthDate, "Birth date should match in RETURNING")

            // Verify by selecting the person
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(1, selectedPersons.size, "Should have 1 person")

            val selectedPerson = selectedPersons[0]
            assertEquals(testDate, selectedPerson.birthDate, "Selected birth date should match")
            assertEquals(insertedPerson.id, selectedPerson.id, "Selected person ID should match")

            // Test with null date
            val personWithNullDate = PersonQuery.Add.Params(
                email = "null-date-test@example.com",
                firstName = "NullDate",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = null
            )

            val insertedPersonWithNull = database.person.add.one(personWithNullDate)
            assertNull(insertedPersonWithNull.birthDate, "Birth date should be null in RETURNING")

            // Verify null date in SELECT
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals(2, allPersons.size, "Should have 2 persons")

            val nullDatePerson = allPersons.find { it.id == insertedPersonWithNull.id }
            assertNotNull(nullDatePerson, "Should find null date person")
            assertNull(nullDatePerson.birthDate, "Selected null birth date should be null")
    }

    @Test
    fun testLocalDateTimeTypeAdapter() = runDatabaseTest {
            database.open()
            
            // Insert a person to get a createdAt timestamp
            val person = PersonQuery.Add.Params(
                email = "datetime-test@example.com",
                firstName = "DateTime",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = LocalDate(1995, 3, 15)
            )
            
            val insertedPerson = database.person.add.one(person)
            
            // Verify LocalDateTime adapter worked correctly
            assertNotNull(insertedPerson.createdAt, "Created at should not be null")
            assertTrue(insertedPerson.createdAt.year >= 2024, "Created at should be recent")
            
            // Test with Comment table which has createdAt as input parameter
            val testDateTime = LocalDateTime(2024, 6, 15, 14, 30, 45)
            val comment = CommentQuery.Add.Params(
                personId = insertedPerson.id,
                comment = "Test comment with custom datetime",
                createdAt = testDateTime,
                tags = listOf("test", "datetime")
            )

            val insertedComment = database.comment.add(comment)
            
            // Verify by selecting the comment
            val selectedComments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = insertedPerson.id)).asList()
            assertEquals(1, selectedComments.size, "Should have 1 comment")
            assertEquals(testDateTime, selectedComments[0].createdAt, "Created at should match exactly")
    }

    @Test
    fun testCustomEnumTypeAdapter() = runDatabaseTest {
            database.open()
            
            // Test AddressType enum adapter
            val addressTypes = listOf(
                AddressType.HOME,
                AddressType.WORK
            )
            
            // First insert a person
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "enum-test@example.com",
                firstName = "Enum",
                lastName = "Test",
                phone = "+3333333333",
                birthDate = LocalDate(1988, 8, 20)
            ))
            
            addressTypes.forEachIndexed { index, addressType ->
                val address = PersonAddressQuery.Add.Params(
                    personId = person.id,
                    addressType = addressType,
                    street = "Test Street $index",
                    city = "Test City $index",
                    state = "Test State $index",
                    postalCode = "1234$index",
                    country = "Test Country $index",
                    isPrimary = index == 0
                )

                database.personAddress.add(address)
            }
            
            // Test selecting by address type
            val homeAddresses = database.personAddress.selectAllByAddressType(
                PersonAddressQuery.SelectAllByAddressType.Params(addressType = AddressType.HOME)
            ).asList()
            
            assertEquals(1, homeAddresses.size, "Should find 1 HOME address")
            assertEquals(AddressType.HOME, homeAddresses[0].addressType, "Address type should be HOME")
    }

    @Test
    fun testJsonListTypeAdapter() = runDatabaseTest {
            database.open()
            
            // Test JSON serialization/deserialization for List<String>
            val testTags = listOf("kotlin", "multiplatform", "sqlite", "testing")
            
            // First insert a person
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "json-test@example.com",
                firstName = "Json",
                lastName = "Test",
                phone = "+4444444444",
                birthDate = LocalDate(1992, 12, 5)
            ))
            
            // Insert comment with tags
            val comment = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Test comment with JSON tags",
                createdAt = LocalDateTime(2024, 6, 15, 10, 30, 0),
                tags = testTags
            )

            database.comment.add(comment)
            
            // Verify JSON adapter worked correctly by selecting the comment
            val selectedComments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals(1, selectedComments.size, "Should have 1 comment")
            val insertedComment = selectedComments[0]

            assertEquals(testTags, insertedComment.tags, "Tags should match exactly")
            assertEquals(testTags.size, insertedComment.tags?.size, "Should have ${testTags.size} tags")
            assertTrue(insertedComment.tags?.contains("kotlin") == true, "Should contain 'kotlin' tag")
            assertTrue(insertedComment.tags?.contains("multiplatform") == true, "Should contain 'multiplatform' tag")
            
            // Test with null tags
            val commentWithNullTags = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Comment without tags",
                createdAt = LocalDateTime(2024, 6, 15, 11, 0, 0),
                tags = null
            )

            database.comment.add(commentWithNullTags)

            // Test with empty tags list
            val commentWithEmptyTags = CommentQuery.Add.Params(
                personId = person.id,
                comment = "Comment with empty tags",
                createdAt = LocalDateTime(2024, 6, 15, 11, 30, 0),
                tags = emptyList()
            )

            database.comment.add(commentWithEmptyTags)

            // Verify by selecting all comments
            val allComments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            assertEquals(3, allComments.size, "Should have 3 comments total")

            val nullTagsComment = allComments.find { it.comment == "Comment without tags" }
            assertNotNull(nullTagsComment, "Should find null tags comment")
            assertNull(nullTagsComment.tags, "Tags should be null")

            val emptyTagsComment = allComments.find { it.comment == "Comment with empty tags" }
            assertNotNull(emptyTagsComment, "Should find empty tags comment")
            assertEquals(emptyList<String>(), emptyTagsComment.tags, "Tags should be empty list")
    }

    @Test
    fun testTypeAdapterNullHandling() = runDatabaseTest {
            database.open()
            
            // Test that all type adapters handle null values correctly
            val person = PersonQuery.Add.Params(
                email = "null-adapters@example.com",
                firstName = "NullAdapters",
                lastName = "Test",
                phone = null, // String? - should be null
                birthDate = null // LocalDate? - should be null through adapter
            )
            
            val insertedPerson = database.person.add.one(person)
            
            // Verify null handling
            assertNull(insertedPerson.phone, "Phone should be null")
            assertNull(insertedPerson.birthDate, "Birth date should be null")
            assertNotNull(insertedPerson.createdAt, "Created at should not be null (required field)")
            
            // Test selecting the person to ensure null values persist
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            
            assertEquals(1, selectedPersons.size, "Should find exactly one person")
            val selectedPerson = selectedPersons[0]
            
            assertNull(selectedPerson.phone, "Selected phone should be null")
            assertNull(selectedPerson.birthDate, "Selected birth date should be null")
            assertNotNull(selectedPerson.createdAt, "Selected created at should not be null")
    }

    @Test
    fun testTypeAdapterConsistencyBetweenInsertAndSelect() = runDatabaseTest {
            database.open()
            
            // Test that type adapters work consistently between INSERT and SELECT operations
            val testDate = LocalDate(1987, 4, 22)
            val testDateTime = LocalDateTime(2024, 6, 15, 16, 45, 30)
            val testTags = listOf("consistency", "test", "adapters")
            
            // Insert person
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "consistency@example.com",
                firstName = "Consistency",
                lastName = "Test",
                phone = "+5555555555",
                birthDate = testDate
            ))
            
            // Insert comment
            database.comment.add(CommentQuery.Add.Params(
                personId = person.id,
                comment = "Consistency test comment",
                createdAt = testDateTime,
                tags = testTags
            ))

            // Insert address
            database.personAddress.add(PersonAddressQuery.Add.Params(
                personId = person.id,
                addressType = AddressType.WORK,
                street = "Consistency Street",
                city = "Consistency City",
                state = "Test State",
                postalCode = "12345",
                country = "Test Country",
                isPrimary = true
            ))
            
            // Now select all data and verify consistency
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            val selectedComments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            val selectedAddresses = database.personAddress.selectAll.asList()
            
            // Verify person data consistency
            val selectedPerson = selectedPersons.find { it.id == person.id }
            assertNotNull(selectedPerson, "Should find selected person")
            assertEquals(testDate, selectedPerson.birthDate, "Birth date should be consistent")
            assertEquals(person.createdAt, selectedPerson.createdAt, "Created at should be consistent")
            
            // Verify comment data consistency
            val selectedComment = selectedComments.find { it.comment == "Consistency test comment" }
            assertNotNull(selectedComment, "Should find selected comment")
            assertEquals(testDateTime, selectedComment.createdAt, "Comment created at should be consistent")
            assertEquals(testTags, selectedComment.tags, "Comment tags should be consistent")

            // Verify address data consistency
            val selectedAddress = selectedAddresses.find { it.street == "Consistency Street" }
            assertNotNull(selectedAddress, "Should find selected address")
            assertEquals(AddressType.WORK, selectedAddress.addressType, "Address type should be consistent")
    }

    @Test
    fun testTypeAdapterErrorHandling() = runDatabaseTest {
            database.open()
            
            // Test edge cases and boundary values for type adapters
            
            // Test extreme dates
            val extremeDates = listOf(
                LocalDate(1900, 1, 1), // Very old date
                LocalDate(2100, 12, 31), // Future date
                LocalDate(2000, 2, 29) // Leap year date
            )
            
            extremeDates.forEachIndexed { index, date ->
                val person = database.person.add.one(PersonQuery.Add.Params(
                    email = "extreme-date-$index@example.com",
                    firstName = "ExtremeDate$index",
                    lastName = "Test",
                    phone = "+666666666$index",
                    birthDate = date
                ))
                
                assertEquals(date, person.birthDate, "Extreme date $index should be preserved")
            }
            
            // Test extreme datetime values
            val extremeDateTime = LocalDateTime(1970, 1, 1, 0, 0, 1) // Near Unix epoch
            
            val person = database.person.add.one(PersonQuery.Add.Params(
                email = "extreme-datetime@example.com",
                firstName = "ExtremeDateTime",
                lastName = "Test",
                phone = "+7777777777",
                birthDate = LocalDate(1990, 1, 1)
            ))
            
            database.comment.add(CommentQuery.Add.Params(
                personId = person.id,
                comment = "Extreme datetime test",
                createdAt = extremeDateTime,
                tags = listOf("extreme", "datetime")
            ))

            val selectedComments = database.comment.selectAll(CommentQuery.SelectAll.Params(personId = person.id)).asList()
            val comment = selectedComments.find { it.comment == "Extreme datetime test" }
            assertNotNull(comment, "Should find extreme datetime comment")
            
            assertEquals(extremeDateTime, comment.createdAt, "Extreme datetime should be preserved")
    }
}
