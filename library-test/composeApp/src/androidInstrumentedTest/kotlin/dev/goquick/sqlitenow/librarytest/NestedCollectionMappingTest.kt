package dev.goquick.sqlitenow.librarytest

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.librarytest.db.AddressType
import dev.goquick.sqlitenow.librarytest.db.CategoryQuery
import dev.goquick.sqlitenow.librarytest.db.CommentQuery
import dev.goquick.sqlitenow.librarytest.db.LibraryTestDatabase
import dev.goquick.sqlitenow.librarytest.db.PersonAddressQuery
import dev.goquick.sqlitenow.librarytest.db.PersonCategoryQuery
import dev.goquick.sqlitenow.librarytest.db.PersonQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Comprehensive instrumentation tests for nested objects in dynamic fields functionality.
 * Tests the complex collection mapping with multiple nested collections in a single query.
 */
@RunWith(AndroidJUnit4::class)
class NestedCollectionMappingTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setup() {
        runBlocking {
            database = TestDatabaseHelper.createDatabase()
            database.open()
        }
    }

    @After
    fun teardown() {
        runBlocking {
            database.close()
        }
    }

    @Test
    fun testNestedCollectionsWithFullData() = runBlocking {
        // Create a person with comprehensive nested data
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "John",
            lastName = "Doe",
            email = "john.doe@example.com",
            phone = "555-1234",
            birthDate = LocalDate(1990, 5, 15)
        )).executeReturningOne()

        // Create multiple addresses
        val homeAddress = database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.HOME,
            street = "123 Main St",
            city = "Springfield",
            state = "IL",
            postalCode = "62701",
            country = "USA",
            isPrimary = true
        )).executeReturningOne()

        val workAddress = database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.WORK,
            street = "456 Business Ave",
            city = "Springfield",
            state = "IL",
            postalCode = "62702",
            country = "USA",
            isPrimary = false
        )).executeReturningOne()

        // Create multiple comments
        val comment1 = database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "First comment about John",
            createdAt = LocalDateTime(2024, 1, 15, 10, 30, 0),
            tags = listOf("personal", "introduction")
        )).execute()

        val comment2 = database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "Second comment with more details",
            createdAt = LocalDateTime(2024, 2, 20, 14, 45, 30),
            tags = listOf("detailed", "follow-up", "important")
        )).execute()

        // Create multiple categories
        val techCategory = database.category.add(CategoryQuery.Add.Params(
            name = "Technology",
            description = "Tech-related category"
        )).executeReturningOne()

        val businessCategory = database.category.add(CategoryQuery.Add.Params(
            name = "Business",
            description = "Business-related category"
        )).executeReturningOne()

        // Link person to categories
        database.personCategory.add(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = techCategory.id,
            isPrimary = true
        )).executeReturningOne()

        database.personCategory.add(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = businessCategory.id,
            isPrimary = false
        )).executeReturningOne()

        // Test the nested collections query
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        // Verify main result structure
        assertEquals(1, result.size)
        val personWithCollections = result.first()

        // Verify person data
        assertEquals(person.id, personWithCollections.personId)
        assertEquals("John", personWithCollections.myFirstName)
        assertEquals("Doe", personWithCollections.myLastName)
        assertEquals("john.doe@example.com", personWithCollections.email)
        assertEquals("555-1234", personWithCollections.phone)
        assertEquals(LocalDate(1990, 5, 15), personWithCollections.birthDate)
        assertNotNull(personWithCollections.createdAt)

        // Verify addresses collection
        assertEquals(2, personWithCollections.addresses.size)
        val homeAddr = personWithCollections.addresses.find { it.addressType == AddressType.HOME }
        val workAddr = personWithCollections.addresses.find { it.addressType == AddressType.WORK }

        assertNotNull(homeAddr)
        assertNotNull(workAddr)
        assertEquals("123 Main St", homeAddr!!.street)
        assertEquals("456 Business Ave", workAddr!!.street)
        assertTrue(homeAddr.isPrimary)
        assertFalse(workAddr.isPrimary)

        // Verify comments collection
        assertEquals(2, personWithCollections.comments.size)
        val firstComment = personWithCollections.comments.find { it.comment.contains("First comment") }
        val secondComment = personWithCollections.comments.find { it.comment.contains("Second comment") }

        assertNotNull(firstComment)
        assertNotNull(secondComment)
        assertEquals(listOf("personal", "introduction"), firstComment!!.tags)
        assertEquals(listOf("detailed", "follow-up", "important"), secondComment!!.tags)

        // Verify categories collection
        assertEquals(2, personWithCollections.categories.size)
        val techCat = personWithCollections.categories.find { it.name == "Technology" }
        val businessCat = personWithCollections.categories.find { it.name == "Business" }

        assertNotNull(techCat)
        assertNotNull(businessCat)
        assertEquals("Tech-related category", techCat!!.description)
        assertEquals("Business-related category", businessCat!!.description)
    }

    @Test
    fun testNestedCollectionsWithPartialData() = runBlocking {
        // Create a person with only some nested data
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Jane",
            lastName = "Smith",
            email = "jane.smith@example.com",
            phone = null,
            birthDate = null
        )).executeReturningOne()

        // Add only one address and one comment, no categories
        database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.HOME,
            street = "789 Oak St",
            city = "Springfield",
            state = "IL",
            postalCode = "62703",
            country = "USA",
            isPrimary = true
        )).executeReturningOne()

        database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "Single comment for Jane",
            createdAt = LocalDateTime(2024, 3, 10, 9, 15, 0),
            tags = listOf("single")
        )).execute()

        // Test the nested collections query
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithCollections = result.first()

        // Verify person data
        assertEquals("Jane", personWithCollections.myFirstName)
        assertEquals("Smith", personWithCollections.myLastName)

        // Verify partial collections
        assertEquals(1, personWithCollections.addresses.size)
        assertEquals("789 Oak St", personWithCollections.addresses.first().street)

        assertEquals(1, personWithCollections.comments.size)
        assertEquals("Single comment for Jane", personWithCollections.comments.first().comment)

        // Verify empty categories collection
        assertEquals(0, personWithCollections.categories.size)
        assertTrue(personWithCollections.categories.isEmpty())
    }

    @Test
    fun testNestedCollectionsWithEmptyCollections() = runBlocking {
        // Create a person with no nested data
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Bob",
            lastName = "Johnson",
            email = "bob.johnson@example.com",
            phone = "555-9999",
            birthDate = LocalDate(1985, 12, 25)
        )).executeReturningOne()

        // Test the nested collections query with no related data
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithCollections = result.first()

        // Verify person data
        assertEquals("Bob", personWithCollections.myFirstName)
        assertEquals("Johnson", personWithCollections.myLastName)

        // Verify all collections are empty
        assertEquals(0, personWithCollections.addresses.size)
        assertEquals(0, personWithCollections.comments.size)
        assertEquals(0, personWithCollections.categories.size)
        assertTrue(personWithCollections.addresses.isEmpty())
        assertTrue(personWithCollections.comments.isEmpty())
        assertTrue(personWithCollections.categories.isEmpty())
    }

    @Test
    fun testNestedCollectionsWithNonExistentPerson() = runBlocking {
        // Test query with non-existent person ID
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = 99999L)
        ).asList()

        // Should return empty list for non-existent person
        assertEquals(0, result.size)
        assertTrue(result.isEmpty())
    }

    @Test
    fun testNestedCollectionsDistinctBy() = runBlocking {
        // Test that distinctBy works correctly to avoid duplicate entries
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Alice",
            lastName = "Wilson",
            email = "alice.wilson@example.com",
            phone = "555-7777",
            birthDate = LocalDate(1992, 8, 10)
        )).executeReturningOne()

        // Create address
        val address = database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.HOME,
            street = "321 Pine St",
            city = "Springfield",
            state = "IL",
            postalCode = "62704",
            country = "USA",
            isPrimary = true
        )).executeReturningOne()

        // Create multiple categories that could cause duplicate address entries
        val category1 = database.category.add(CategoryQuery.Add.Params(
            name = "Category1",
            description = "First category"
        )).executeReturningOne()

        val category2 = database.category.add(CategoryQuery.Add.Params(
            name = "Category2",
            description = "Second category"
        )).executeReturningOne()

        // Link person to both categories
        database.personCategory.add(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = category1.id,
            isPrimary = true
        )).executeReturningOne()

        database.personCategory.add(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = category2.id,
            isPrimary = false
        )).executeReturningOne()

        // Test the nested collections query
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithCollections = result.first()

        // Verify that address appears only once despite multiple categories
        assertEquals(1, personWithCollections.addresses.size)
        assertEquals("321 Pine St", personWithCollections.addresses.first().street)

        // Verify both categories are present
        assertEquals(2, personWithCollections.categories.size)
        val categoryNames = personWithCollections.categories.map { it.name }.sorted()
        assertEquals(listOf("Category1", "Category2"), categoryNames)
    }

    @Test
    fun testNestedCollectionsWithComplexJsonData() = runBlocking {
        // Test nested collections with complex JSON data in tags
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Charlie",
            lastName = "Brown",
            email = "charlie.brown@example.com",
            phone = "555-8888",
            birthDate = LocalDate(1988, 4, 1)
        )).executeReturningOne()

        // Create comments with complex JSON tags
        val complexTags1 = listOf(
            "tag with spaces",
            "tag'with'quotes",
            "tag\"with\"double\"quotes",
            "tag&with&ampersands",
            "unicode: 🚀🎉💻",
            "numbers: 123456",
            "special: @#$%^&*()"
        )

        val complexTags2 = listOf(
            "nested/path/structure",
            "json{\"key\":\"value\"}",
            "xml<tag>content</tag>",
            "sql'injection\"attempt",
            "newline\ncharacter",
            "tab\tcharacter"
        )

        database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "Comment with complex tags and special characters: 'test', \"quotes\", & symbols",
            createdAt = LocalDateTime(2024, 4, 1, 12, 0, 0),
            tags = complexTags1
        )).execute()

        database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "Another comment with different complex tags",
            createdAt = LocalDateTime(2024, 4, 2, 15, 30, 0),
            tags = complexTags2
        )).execute()

        // Test the nested collections query
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithCollections = result.first()

        // Verify complex JSON data is preserved correctly
        assertEquals(2, personWithCollections.comments.size)

        val comment1 = personWithCollections.comments.find { it.comment.contains("complex tags") }
        val comment2 = personWithCollections.comments.find { it.comment.contains("different complex") }

        assertNotNull(comment1)
        assertNotNull(comment2)

        // Verify complex tags are preserved
        assertEquals(complexTags1, comment1!!.tags)
        assertEquals(complexTags2, comment2!!.tags)

        // Verify specific complex tag values
        assertTrue(comment1.tags!!.contains("unicode: 🚀🎉💻"))
        assertTrue(comment1.tags!!.contains("tag'with'quotes"))
        assertTrue(comment2.tags!!.contains("json{\"key\":\"value\"}"))
        assertTrue(comment2.tags!!.contains("newline\ncharacter"))
    }

    @Test
    fun testNestedCollectionsPerformanceWithLargeDataset() = runBlocking {
        // Test performance and correctness with larger dataset (2 addresses, 5 comments, 4 categories)
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "David",
            lastName = "Performance",
            email = "david.performance@example.com",
            phone = "555-0000",
            birthDate = LocalDate(1980, 1, 1)
        )).executeReturningOne()

        // Create multiple addresses
        val addressTypes = listOf(AddressType.HOME, AddressType.WORK)
        addressTypes.forEachIndexed { index, addressType ->
            database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
                personId = person.id,
                addressType = addressType,
                street = "Street $index",
                city = "City $index",
                state = "State $index",
                postalCode = "ZIP$index",
                country = "Country $index",
                isPrimary = index == 0
            )).executeReturningOne()
        }

        // Create multiple comments
        repeat(5) { index ->
            database.comment.add(CommentQuery.Add.Params(
                personId = person.id,
                comment = "Performance test comment $index with detailed content",
                createdAt = LocalDateTime(2024, 1, index + 1, 10, 0, 0),
                tags = listOf("performance", "test", "comment$index", "large-dataset")
            )).execute()
        }

        // Create multiple categories
        repeat(4) { index ->
            val category = database.category.add(CategoryQuery.Add.Params(
                name = "Performance Category $index",
                description = "Category $index for performance testing"
            )).executeReturningOne()

            database.personCategory.add(PersonCategoryQuery.Add.Params(
                personId = person.id,
                categoryId = category.id,
                isPrimary = index == 0
            )).executeReturningOne()
        }

        // Test the nested collections query
        val startTime = System.currentTimeMillis()
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()
        val endTime = System.currentTimeMillis()

        // Verify correctness
        assertEquals(1, result.size)
        val personWithCollections = result.first()

        assertEquals(2, personWithCollections.addresses.size)
        assertEquals(5, personWithCollections.comments.size)
        assertEquals(4, personWithCollections.categories.size)

        // Verify data integrity
        val homeAddress = personWithCollections.addresses.find { it.addressType == AddressType.HOME }
        assertNotNull(homeAddress)
        assertTrue(homeAddress!!.isPrimary)

        val firstComment = personWithCollections.comments.find { it.comment.contains("comment 0") }
        assertNotNull(firstComment)
        assertTrue(firstComment!!.tags!!.contains("performance"))

        val firstCategory = personWithCollections.categories.find { it.name.contains("Category 0") }
        assertNotNull(firstCategory)
        assertEquals("Category 0 for performance testing", firstCategory!!.description)

        // Log performance (optional - for debugging)
        val executionTime = endTime - startTime
        println("Nested collections query execution time: ${executionTime}ms")

        // Performance should be reasonable (adjust threshold as needed)
        assertTrue("Query took too long: ${executionTime}ms", executionTime < 5000)
    }

    @Test
    fun testNestedCollectionsWithNullValues() = runBlocking {
        // Test nested collections with various null values
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Emma",
            lastName = "Null",
            email = "emma.null@example.com",
            phone = null, // Null phone
            birthDate = null // Null birth date
        )).executeReturningOne()

        // Create address with some null fields
        database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.HOME,
            street = "123 Null St",
            city = "Null City",
            state = null, // Null state
            postalCode = null, // Null postal code
            country = "USA",
            isPrimary = true
        )).executeReturningOne()

        // Create comment with null tags
        database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "Comment with null tags",
            createdAt = LocalDateTime(2024, 5, 1, 10, 0, 0),
            tags = null // Null tags
        )).execute()

        // Create category with null description
        val category = database.category.add(CategoryQuery.Add.Params(
            name = "Null Category",
            description = null // Null description
        )).executeReturningOne()

        database.personCategory.add(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = category.id,
            isPrimary = true
        )).executeReturningOne()

        // Test the nested collections query
        val result = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithCollections = result.first()

        // Verify null values are handled correctly
        assertEquals("Emma", personWithCollections.myFirstName)
        assertEquals("Null", personWithCollections.myLastName)
        assertEquals(null, personWithCollections.phone)
        assertEquals(null, personWithCollections.birthDate)

        // Verify address with null fields
        assertEquals(1, personWithCollections.addresses.size)
        val address = personWithCollections.addresses.first()
        assertEquals("123 Null St", address.street)
        assertEquals(null, address.state)
        assertEquals(null, address.postalCode)

        // Verify comment with null tags
        assertEquals(1, personWithCollections.comments.size)
        val comment = personWithCollections.comments.first()
        assertEquals("Comment with null tags", comment.comment)
        assertEquals(null, comment.tags)

        // Verify category with null description
        assertEquals(1, personWithCollections.categories.size)
        val cat = personWithCollections.categories.first()
        assertEquals("Null Category", cat.name)
        assertEquals(null, cat.description)
    }

    @Test
    fun testNestedCollectionsDataConsistency() = runBlocking {
        // Test data consistency across multiple queries
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Frank",
            lastName = "Consistent",
            email = "frank.consistent@example.com",
            phone = "555-1111",
            birthDate = LocalDate(1995, 6, 20)
        )).executeReturningOne()

        // Create test data
        val address = database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.WORK,
            street = "999 Consistency Blvd",
            city = "Consistent City",
            state = "CC",
            postalCode = "99999",
            country = "Consistent Country",
            isPrimary = true
        )).executeReturningOne()

        val comment = database.comment.add(CommentQuery.Add.Params(
            personId = person.id,
            comment = "Consistency test comment",
            createdAt = LocalDateTime(2024, 6, 20, 16, 45, 0),
            tags = listOf("consistency", "test", "verification")
        )).execute()

        // Query using nested collections
        val nestedResult = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList().first()

        // Verify data consistency by checking the nested result structure
        assertEquals("Frank", nestedResult.myFirstName)
        assertEquals("Consistent", nestedResult.myLastName)
        assertEquals("frank.consistent@example.com", nestedResult.email)
        assertEquals("555-1111", nestedResult.phone)
        assertEquals(LocalDate(1995, 6, 20), nestedResult.birthDate)

        // Verify address data
        assertEquals(1, nestedResult.addresses.size)
        val nestedAddress = nestedResult.addresses.first()
        assertEquals("999 Consistency Blvd", nestedAddress.street)
        assertEquals(AddressType.WORK, nestedAddress.addressType)
        assertEquals(true, nestedAddress.isPrimary)

        // Verify comment data
        assertEquals(1, nestedResult.comments.size)
        val nestedComment = nestedResult.comments.first()
        assertEquals("Consistency test comment", nestedComment.comment)
        assertEquals(listOf("consistency", "test", "verification"), nestedComment.tags)
        assertEquals(LocalDateTime(2024, 6, 20, 16, 45, 0), nestedComment.createdAt)
    }

    @Test
    fun testNestedCollectionsQueryRestrictions() = runBlocking {
        // Test that nested collection queries have proper restrictions
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Grace",
            lastName = "Restricted",
            email = "grace.restricted@example.com",
            phone = "555-2222",
            birthDate = LocalDate(1987, 9, 12)
        )).executeReturningOne()

        // Add some test data
        database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.HOME,
            street = "777 Restriction St",
            city = "Restricted City",
            state = "RC",
            postalCode = "77777",
            country = "Restricted Country",
            isPrimary = true
        )).executeReturningOne()

        // Test that asList() works correctly
        val listResult = database.person.selectWithNestedCollections(
            PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
        ).asList()

        assertEquals(1, listResult.size)
        assertEquals("Grace", listResult.first().myFirstName)

        // Test that asOne() throws UnsupportedOperationException for collection queries
        try {
            database.person.selectWithNestedCollections(
                PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
            ).asOne()
            assertTrue("Expected UnsupportedOperationException for asOne() on collection query", false)
        } catch (e: UnsupportedOperationException) {
            // Expected behavior
            assertTrue("Correctly threw UnsupportedOperationException", true)
        }

        // Test that asOneOrNull() throws UnsupportedOperationException for collection queries
        try {
            database.person.selectWithNestedCollections(
                PersonQuery.SelectWithNestedCollections.Params(personId = person.id)
            ).asOneOrNull()
            assertTrue("Expected UnsupportedOperationException for asOneOrNull() on collection query", false)
        } catch (e: UnsupportedOperationException) {
            // Expected behavior
            assertTrue("Correctly threw UnsupportedOperationException", true)
        }
    }

    @Test
    fun testPerRowMappingBasic() = runBlocking {
        // Test mappingType=perRow - maps data from JOIN tables to nested objects
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "PerRowBasic",
            lastName = "Test",
            email = "perrowbasic@example.com",
            phone = "555-6666",
            birthDate = LocalDate(1987, 9, 25)
        )).executeReturningOne()

        val address = database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.HOME,
            street = "789 PerRow Lane",
            city = "PerRow City",
            state = "PerRow State",
            postalCode = "67890",
            country = "PerRow Country",
            isPrimary = true
        )).executeReturningOne()

        // Test perRow mapping - this uses JOIN tables to create nested objects
        val result = database.person.selectWithPerRowMapping(
            PersonQuery.SelectWithPerRowMapping.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithPerRowMapping = result.first()

        // Verify main person data
        assertEquals("PerRowBasic", personWithPerRowMapping.myFirstName)
        assertEquals("Test", personWithPerRowMapping.myLastName)

        // Verify perRow mapped address (data comes from JOIN table person_address)
        assertNotNull(personWithPerRowMapping.address)
        assertEquals("789 PerRow Lane", personWithPerRowMapping.address!!.street)
        assertEquals("PerRow City", personWithPerRowMapping.address!!.city)
        assertEquals(AddressType.HOME, personWithPerRowMapping.address!!.addressType)
        assertTrue(personWithPerRowMapping.address!!.isPrimary)

        // Key insight: perRow mapping uses data from JOIN tables to create nested objects
    }

    @Test
    fun testEntityMappingFromSingleTable() = runBlocking {
        // Test mappingType=entity - maps columns from main FROM table (no JOINs required)
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Entity",
            lastName = "Test",
            email = "entity@example.com",
            phone = "555-4444",
            birthDate = LocalDate(1988, 7, 12)
        )).executeReturningOne()

        // Test entity mapping - this works without any JOINs
        // The entity mapping takes columns from the main person table and maps them to a nested object
        val result = database.person.selectWithEntityMapping(
            PersonQuery.SelectWithEntityMapping.Params(personId = person.id)
        ).asList()

        assertEquals(1, result.size)
        val personWithEntityMapping = result.first()

        // Verify that entity mapping created a nested object from the same table's columns
        assertNotNull(personWithEntityMapping.personDetails)
        assertEquals("Entity", personWithEntityMapping.personDetails.myFirstName)
        assertEquals("Test", personWithEntityMapping.personDetails.myLastName)
        assertEquals("entity@example.com", personWithEntityMapping.personDetails.email)
        assertEquals("555-4444", personWithEntityMapping.personDetails.phone)
        assertEquals(LocalDate(1988, 7, 12), personWithEntityMapping.personDetails.birthDate)

        // Key insight: entity mapping restructures data from the same table
        // No JOINs required - it's about decomposing wide tables into structured objects
    }

    @Test
    fun testPerRowVsEntityMappingDifference() = runBlocking {
        // Test the key difference between perRow and entity mapping
        val person = database.person.add(PersonQuery.Add.Params(
            firstName = "Comparison",
            lastName = "Test",
            email = "comparison@example.com",
            phone = "555-5555",
            birthDate = LocalDate(1991, 11, 5)
        )).executeReturningOne()

        val address = database.personAddress.addReturning(PersonAddressQuery.AddReturning.Params(
            personId = person.id,
            addressType = AddressType.WORK,
            street = "456 Comparison Ave",
            city = "Comparison City",
            state = "Comparison State",
            postalCode = "54321",
            country = "Comparison Country",
            isPrimary = false
        )).executeReturningOne()

        // Test perRow mapping - this uses JOIN tables to create nested objects
        val perRowResult = database.person.selectWithPerRowMapping(
            PersonQuery.SelectWithPerRowMapping.Params(personId = person.id)
        ).asList()

        assertEquals(1, perRowResult.size)
        val personWithPerRowMapping = perRowResult.first()

        // perRow mapping: address data comes from JOIN table (person_address)
        assertNotNull(personWithPerRowMapping.address)
        assertEquals("456 Comparison Ave", personWithPerRowMapping.address!!.street)
        assertEquals(AddressType.WORK, personWithPerRowMapping.address!!.addressType)

        // Now test entity mapping for comparison:
        // Entity mapping takes columns from the main FROM table (person) and maps them to nested objects
        val entityResult = database.person.selectWithEntityMapping(
            PersonQuery.SelectWithEntityMapping.Params(personId = person.id)
        ).asList()

        assertEquals(1, entityResult.size)
        val personWithEntityMapping = entityResult.first()

        // Entity mapping: personDetails data comes from the main FROM table (person)
        // No JOINs required - it restructures the same table's data into nested objects
        assertNotNull(personWithEntityMapping.personDetails)
        assertEquals("Comparison", personWithEntityMapping.personDetails.myFirstName)
        assertEquals("Test", personWithEntityMapping.personDetails.myLastName)
        assertEquals("comparison@example.com", personWithEntityMapping.personDetails.email)

        // Key difference demonstrated:
        // - perRow: Uses JOIN tables to create nested objects (address from person_address table)
        // - entity: Uses main FROM table columns to create nested objects (personDetails from person table)
        assertTrue("Both perRow and entity mapping work, but use different data sources", true)
    }
}
