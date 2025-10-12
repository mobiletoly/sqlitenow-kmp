package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.CommentQuery
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
 * Integration tests specifically for SQLiteNow Type Adapters functionality.
 * Tests custom type adapters (LocalDate, LocalDateTime, custom objects, JSON serialization).
 */
@RunWith(AndroidJUnit4::class)
class TypeAdaptersTest {

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
    fun testLocalDateTypeAdapter() {
        runBlocking {
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
            assertEquals("Birth date should match in RETURNING", testDate, insertedPerson.birthDate)

            // Verify by selecting the person
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 1 person", 1, selectedPersons.size)

            val selectedPerson = selectedPersons[0]
            assertEquals("Selected birth date should match", testDate, selectedPerson.birthDate)
            assertEquals("Selected person ID should match", insertedPerson.id, selectedPerson.id)

            // Test with null date
            val personWithNullDate = PersonQuery.Add.Params(
                email = "null-date-test@example.com",
                firstName = "NullDate",
                lastName = "Test",
                phone = "+2222222222",
                birthDate = null
            )

            val insertedPersonWithNull = database.person.add.one(personWithNullDate)
            assertNull("Birth date should be null in RETURNING", insertedPersonWithNull.birthDate)

            // Verify null date in SELECT
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            assertEquals("Should have 2 persons", 2, allPersons.size)

            val nullDatePerson = allPersons.find { it.id == insertedPersonWithNull.id }
            assertNotNull("Should find null date person", nullDatePerson)
            assertNull("Selected null birth date should be null", nullDatePerson!!.birthDate)
        }
    }

    @Test
    fun testLocalDateTimeTypeAdapter() {
        runBlocking {
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
            assertNotNull("Created at should not be null", insertedPerson.createdAt)
            assertTrue("Created at should be recent", insertedPerson.createdAt.year >= 2024)
            
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
            assertEquals("Should have 1 comment", 1, selectedComments.size)
            assertEquals("Created at should match exactly", testDateTime, selectedComments[0].createdAt)
        }
    }

    @Test
    fun testCustomEnumTypeAdapter() {
        runBlocking {
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
            
            assertEquals("Should find 1 HOME address", 1, homeAddresses.size)
            assertEquals("Address type should be HOME", AddressType.HOME, homeAddresses[0].addressType)
        }
    }

    @Test
    fun testJsonListTypeAdapter() {
        runBlocking {
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
            assertEquals("Should have 1 comment", 1, selectedComments.size)
            val insertedComment = selectedComments[0]

            assertEquals("Tags should match exactly", testTags, insertedComment.tags)
            assertEquals("Should have ${testTags.size} tags", testTags.size, insertedComment.tags?.size)
            assertTrue("Should contain 'kotlin' tag", insertedComment.tags?.contains("kotlin") == true)
            assertTrue("Should contain 'multiplatform' tag", insertedComment.tags?.contains("multiplatform") == true)
            
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
            assertEquals("Should have 3 comments total", 3, allComments.size)

            val nullTagsComment = allComments.find { it.comment == "Comment without tags" }
            assertNotNull("Should find null tags comment", nullTagsComment)
            assertNull("Tags should be null", nullTagsComment!!.tags)

            val emptyTagsComment = allComments.find { it.comment == "Comment with empty tags" }
            assertNotNull("Should find empty tags comment", emptyTagsComment)
            assertEquals("Tags should be empty list", emptyList<String>(), emptyTagsComment!!.tags)
        }
    }

    @Test
    fun testTypeAdapterNullHandling() {
        runBlocking {
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
            assertNull("Phone should be null", insertedPerson.phone)
            assertNull("Birth date should be null", insertedPerson.birthDate)
            assertNotNull("Created at should not be null (required field)", insertedPerson.createdAt)
            
            // Test selecting the person to ensure null values persist
            val selectedPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0))
                .asList()
                .filter { it.id == insertedPerson.id }
            
            assertEquals("Should find exactly one person", 1, selectedPersons.size)
            val selectedPerson = selectedPersons[0]
            
            assertNull("Selected phone should be null", selectedPerson.phone)
            assertNull("Selected birth date should be null", selectedPerson.birthDate)
            assertNotNull("Selected created at should not be null", selectedPerson.createdAt)
        }
    }

    @Test
    fun testTypeAdapterConsistencyBetweenInsertAndSelect() {
        runBlocking {
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
            assertNotNull("Should find selected person", selectedPerson)
            assertEquals("Birth date should be consistent", testDate, selectedPerson!!.birthDate)
            assertEquals("Created at should be consistent", person.createdAt, selectedPerson.createdAt)
            
            // Verify comment data consistency
            val selectedComment = selectedComments.find { it.comment == "Consistency test comment" }
            assertNotNull("Should find selected comment", selectedComment)
            assertEquals("Comment created at should be consistent", testDateTime, selectedComment!!.createdAt)
            assertEquals("Comment tags should be consistent", testTags, selectedComment.tags)

            // Verify address data consistency
            val selectedAddress = selectedAddresses.find { it.street == "Consistency Street" }
            assertNotNull("Should find selected address", selectedAddress)
            assertEquals("Address type should be consistent", AddressType.WORK, selectedAddress!!.addressType)
        }
    }

    @Test
    fun testTypeAdapterErrorHandling() {
        runBlocking {
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
                
                assertEquals("Extreme date $index should be preserved", date, person.birthDate)
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
            assertNotNull("Should find extreme datetime comment", comment)
            
            assertEquals("Extreme datetime should be preserved", extremeDateTime, comment!!.createdAt)
        }
    }
}
