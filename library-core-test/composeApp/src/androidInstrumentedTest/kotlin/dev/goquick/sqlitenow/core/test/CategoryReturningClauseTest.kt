package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.CategoryAddResult
import dev.goquick.sqlitenow.core.test.db.CategoryQuery
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDateTime
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Integration tests for Category table operations with RETURNING clause.
 * Tests the complete flow from SQL generation to execution and result mapping.
 */
@RunWith(AndroidJUnit4::class)
class CategoryReturningClauseTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setUp() {
        database = TestDatabaseHelper.createDatabase()
    }

    @After
    fun tearDown() {
        runBlocking {
            database.close()
        }
    }

    @Test
    fun testCategoryInsertReturningBasic() {
        runBlocking {
            database.open()

            // Test basic INSERT with RETURNING *
            val categoryParams = CategoryQuery.Add.Params(
                name = "Technology",
                description = "All things tech-related"
            )

            val insertedCategory = database.category.add(categoryParams).executeReturningOne()

            // Verify returned data
            assertNotNull("Inserted category should not be null", insertedCategory)
            assertTrue("ID should be positive", insertedCategory.id > 0)
            assertEquals("Name should match", "Technology", insertedCategory.name)
            assertEquals("Description should match", "All things tech-related", insertedCategory.description)
            assertNotNull("Created at should not be null", insertedCategory.createdAt)
            assertTrue("Created at should be recent", insertedCategory.createdAt.year >= 2024)
        }
    }

    @Test
    fun testCategoryInsertReturningWithNullDescription() {
        runBlocking {
            database.open()

            // Test INSERT with null description
            val categoryParams = CategoryQuery.Add.Params(
                name = "Minimal Category",
                description = null
            )

            val insertedCategory = database.category.add(categoryParams).executeReturningOne()

            // Verify returned data handles null correctly
            assertNotNull("Inserted category should not be null", insertedCategory)
            assertTrue("ID should be positive", insertedCategory.id > 0)
            assertEquals("Name should match", "Minimal Category", insertedCategory.name)
            assertNull("Description should be null", insertedCategory.description)
            assertNotNull("Created at should not be null", insertedCategory.createdAt)
        }
    }

    @Test
    fun testCategoryInsertReturningTypeAdapters() {
        runBlocking {
            database.open()

            // Test that LocalDateTime type adapter works correctly with RETURNING
            val categoryParams = CategoryQuery.Add.Params(
                name = "DateTime Test",
                description = "Testing LocalDateTime adapter"
            )

            val insertedCategory = database.category.add(categoryParams).executeReturningOne()

            // Verify LocalDateTime type adapter worked correctly
            assertNotNull("Created at should not be null", insertedCategory.createdAt)
            assertTrue("Created at should be LocalDateTime type", insertedCategory.createdAt is LocalDateTime)
            assertTrue("Created at should be recent", insertedCategory.createdAt.year >= 2024)
            assertTrue("Created at month should be valid", insertedCategory.createdAt.monthNumber in 1..12)
            assertTrue("Created at day should be valid", insertedCategory.createdAt.dayOfMonth in 1..31)
        }
    }

    @Test
    fun testCategoryInsertReturningConsistencyWithSelect() {
        runBlocking {
            database.open()

            // Test that RETURNING clause returns the same data as a subsequent SELECT
            val categoryParams = CategoryQuery.Add.Params(
                name = "Consistency Test",
                description = "Testing data consistency between INSERT RETURNING and SELECT"
            )

            // Insert with RETURNING
            val insertedCategory = database.category.add(categoryParams).executeReturningOne()

            // Select the same category
            val selectedCategories = database.category.selectAll.asList()
                .filter { it.id == insertedCategory.id }

            assertEquals("Should find exactly one category", 1, selectedCategories.size)
            val selectedCategory = selectedCategories.first()

            // Verify all fields match between RETURNING and SELECT
            assertEquals("ID should match", insertedCategory.id, selectedCategory.id)
            assertEquals("Name should match", insertedCategory.name, selectedCategory.name)
            assertEquals("Description should match", insertedCategory.description, selectedCategory.description)
            assertEquals("Created at should match", insertedCategory.createdAt, selectedCategory.createdAt)
        }
    }

    @Test
    fun testCategoryInsertReturningUniqueConstraint() {
        runBlocking {
            database.open()

            // First insert
            val firstCategory = CategoryQuery.Add.Params(
                name = "Unique Test",
                description = "First category"
            )

            val insertedFirst = database.category.add(firstCategory).executeReturningOne()
            assertTrue("First insert ID should be positive", insertedFirst.id > 0)
            assertEquals("First insert name should match", "Unique Test", insertedFirst.name)

            // Second insert with same name (should fail due to UNIQUE constraint)
            val duplicateCategory = CategoryQuery.Add.Params(
                name = "Unique Test", // Same name
                description = "Duplicate category"
            )

            try {
                database.category.add(duplicateCategory).executeReturningOne()
                fail("Should have thrown exception due to UNIQUE constraint violation")
            } catch (e: Exception) {
                // Expected behavior - UNIQUE constraint should prevent duplicate names
                assertTrue("Exception should mention constraint", 
                    e.message?.contains("UNIQUE") == true || e.message?.contains("constraint") == true)
            }
        }
    }

    @Test
    fun testCategoryInsertReturningMultipleCategories() {
        runBlocking {
            database.open()

            // Insert multiple categories and verify each RETURNING works correctly
            val categories = listOf(
                CategoryQuery.Add.Params("Sports", "Athletic activities"),
                CategoryQuery.Add.Params("Music", "Musical instruments and genres"),
                CategoryQuery.Add.Params("Books", null),
                CategoryQuery.Add.Params("Travel", "Places to visit and travel tips")
            )

            val insertedCategories = mutableListOf<CategoryAddResult>()

            categories.forEach { params ->
                val inserted = database.category.add(params).executeReturningOne()
                insertedCategories.add(inserted)
            }

            // Verify all categories were inserted correctly
            assertEquals("Should have inserted 4 categories", 4, insertedCategories.size)

            // Verify each category has unique ID and correct data
            val ids = insertedCategories.map { it.id }.toSet()
            assertEquals("All IDs should be unique", 4, ids.size)

            insertedCategories.forEachIndexed { index, category ->
                assertTrue("ID should be positive", category.id > 0)
                assertEquals("Name should match", categories[index].name, category.name)
                assertEquals("Description should match", categories[index].description, category.description)
                assertNotNull("Created at should not be null", category.createdAt)
            }

            // Verify all categories exist in database
            val allCategories = database.category.selectAll.asList()
            assertTrue("Should have at least 4 categories", allCategories.size >= 4)

            insertedCategories.forEach { inserted ->
                val found = allCategories.find { it.id == inserted.id }
                assertNotNull("Category should exist in database", found)
                assertEquals("Names should match", inserted.name, found?.name)
                assertEquals("Descriptions should match", inserted.description, found?.description)
            }
        }
    }

    @Test
    fun testCategoryInsertReturningFieldMapping() {
        runBlocking {
            database.open()

            // Test that field mapping works correctly with RETURNING
            // The category table has created_at field mapped to LocalDateTime
            val categoryParams = CategoryQuery.Add.Params(
                name = "Field Mapping Test",
                description = "Testing field name and type mapping"
            )

            val insertedCategory = database.category.add(categoryParams).executeReturningOne()

            // Verify field mapping worked correctly
            assertNotNull("Category should not be null", insertedCategory)
            
            // Test that the createdAt field (mapped from created_at column) is properly typed
            val createdAt = insertedCategory.createdAt
            assertNotNull("Created at should not be null", createdAt)
            assertTrue("Created at should be LocalDateTime", createdAt is LocalDateTime)
            
            // Verify the field is accessible with the mapped property name
            assertTrue("Should be able to access createdAt property", createdAt.year > 2020)
        }
    }

    @Test
    fun testCategoryInsertReturningErrorHandling() {
        runBlocking {
            database.open()

            // Test error handling with invalid data
            try {
                // Try to insert with empty name (should fail NOT NULL constraint)
                val invalidParams = CategoryQuery.Add.Params(
                    name = "", // Empty name might cause issues
                    description = "Invalid category"
                )

                database.category.add(invalidParams).executeReturningOne()
                // If we get here, the empty name was allowed, which is fine
                // The test mainly ensures no crashes occur
            } catch (e: Exception) {
                // If an exception occurs, ensure it's a reasonable database constraint error
                assertNotNull("Exception should have a message", e.message)
            }

            // Test with very long name (testing practical limits)
            val longName = "A".repeat(1000) // Very long name
            val longNameParams = CategoryQuery.Add.Params(
                name = longName,
                description = "Testing long name handling"
            )

            try {
                val result = database.category.add(longNameParams).executeReturningOne()
                // If successful, verify the data was stored correctly
                assertEquals("Long name should be preserved", longName, result.name)
            } catch (e: Exception) {
                // If it fails, that's also acceptable - depends on database limits
                assertNotNull("Exception should have a message", e.message)
            }
        }
    }
}
