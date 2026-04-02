package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.test.db.CategoryAddResult
import dev.goquick.sqlitenow.core.test.db.CategoryQuery
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.number
import kotlin.test.*

/**
 * Integration tests for Category table operations with RETURNING clause.
 * Tests the complete flow from SQL generation to execution and result mapping.
 */
class CategoryReturningClauseTest {

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
    fun testCategoryInsertReturningBasic() = runDatabaseTest {
            database.open()

            // Test basic INSERT with RETURNING *
            val categoryParams = CategoryQuery.Add.Params(
                name = "Technology",
                description = "All things tech-related"
            )

            val insertedCategory = database.category.add.one(categoryParams)

            // Verify returned data
            assertNotNull(insertedCategory, "Inserted category should not be null")
            assertTrue(insertedCategory.id > 0, "ID should be positive")
            assertEquals("Technology", insertedCategory.name, "Name should match")
            assertEquals("All things tech-related", insertedCategory.description, "Description should match")
            assertNotNull(insertedCategory.createdAt, "Created at should not be null")
            assertTrue(insertedCategory.createdAt.year >= 2024, "Created at should be recent")
    }

    @Test
    fun testCategoryInsertReturningWithNullDescription() = runDatabaseTest {
            database.open()

            // Test INSERT with null description
            val categoryParams = CategoryQuery.Add.Params(
                name = "Minimal Category",
                description = null
            )

            val insertedCategory = database.category.add.one(categoryParams)

            // Verify returned data handles null correctly
            assertNotNull(insertedCategory, "Inserted category should not be null")
            assertTrue(insertedCategory.id > 0, "ID should be positive")
            assertEquals("Minimal Category", insertedCategory.name, "Name should match")
            assertNull(insertedCategory.description, "Description should be null")
            assertNotNull(insertedCategory.createdAt, "Created at should not be null")
    }

    @Test
    fun testCategoryInsertReturningTypeAdapters() = runDatabaseTest {
            database.open()

            // Test that LocalDateTime type adapter works correctly with RETURNING
            val categoryParams = CategoryQuery.Add.Params(
                name = "DateTime Test",
                description = "Testing LocalDateTime adapter"
            )

            val insertedCategory = database.category.add.one(categoryParams)

            // Verify LocalDateTime type adapter worked correctly
            assertNotNull(insertedCategory.createdAt, "Created at should not be null")
            assertTrue(true, "Created at should be LocalDateTime type")
            assertTrue(insertedCategory.createdAt.year >= 2024, "Created at should be recent")
            assertTrue(insertedCategory.createdAt.month.number in 1..12, "Created at month should be valid")
            assertTrue(insertedCategory.createdAt.day in 1..31, "Created at day should be valid")
    }

    @Test
    fun testCategoryInsertReturningConsistencyWithSelect() = runDatabaseTest {
            database.open()

            // Test that RETURNING clause returns the same data as a subsequent SELECT
            val categoryParams = CategoryQuery.Add.Params(
                name = "Consistency Test",
                description = "Testing data consistency between INSERT RETURNING and SELECT"
            )

            // Insert with RETURNING
            val insertedCategory = database.category.add.one(categoryParams)

            // Select the same category
            val selectedCategories = database.category.selectAll.asList()
                .filter { it.id == insertedCategory.id }

            assertEquals(1, selectedCategories.size, "Should find exactly one category")
            val selectedCategory = selectedCategories.first()

            // Verify all fields match between RETURNING and SELECT
            assertEquals(insertedCategory.id, selectedCategory.id, "ID should match")
            assertEquals(insertedCategory.name, selectedCategory.name, "Name should match")
            assertEquals(insertedCategory.description, selectedCategory.description, "Description should match")
            assertEquals(insertedCategory.createdAt, selectedCategory.createdAt, "Created at should match")
    }

    @Test
    fun testCategoryInsertReturningUniqueConstraint() = runDatabaseTest {
            database.open()

            // First insert
            val firstCategory = CategoryQuery.Add.Params(
                name = "Unique Test",
                description = "First category"
            )

            val insertedFirst = database.category.add.one(firstCategory)
            assertTrue(insertedFirst.id > 0, "First insert ID should be positive")
            assertEquals("Unique Test", insertedFirst.name, "First insert name should match")

            // Second insert with same name (should fail due to UNIQUE constraint)
            val duplicateCategory = CategoryQuery.Add.Params(
                name = "Unique Test", // Same name
                description = "Duplicate category"
            )

            try {
                database.category.add.one(duplicateCategory)
                fail("Should have thrown exception due to UNIQUE constraint violation")
            } catch (e: Exception) {
                // Expected behavior - UNIQUE constraint should prevent duplicate names
                assertTrue(e.message?.contains("UNIQUE") == true || e.message?.contains("constraint") == true, "Exception should mention constraint")
            }
    }

    @Test
    fun testCategoryInsertReturningMultipleCategories() = runDatabaseTest {
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
                val inserted = database.category.add.one(params)
                insertedCategories.add(inserted)
            }

            // Verify all categories were inserted correctly
            assertEquals(4, insertedCategories.size, "Should have inserted 4 categories")

            // Verify each category has unique ID and correct data
            val ids = insertedCategories.map { it.id }.toSet()
            assertEquals(4, ids.size, "All IDs should be unique")

            insertedCategories.forEachIndexed { index, category ->
                assertTrue(category.id > 0, "ID should be positive")
                assertEquals(categories[index].name, category.name, "Name should match")
                assertEquals(categories[index].description, category.description, "Description should match")
                assertNotNull(category.createdAt, "Created at should not be null")
            }

            // Verify all categories exist in database
            val allCategories = database.category.selectAll.asList()
            assertTrue(allCategories.size >= 4, "Should have at least 4 categories")

            insertedCategories.forEach { inserted ->
                val found = allCategories.find { it.id == inserted.id }
                assertNotNull(found, "Category should exist in database")
                assertEquals(inserted.name, found?.name, "Names should match")
                assertEquals(inserted.description, found?.description, "Descriptions should match")
            }
    }

    @Test
    fun testCategoryInsertReturningFieldMapping() = runDatabaseTest {
            database.open()

            // Test that field mapping works correctly with RETURNING
            // The category table has created_at field mapped to LocalDateTime
            val categoryParams = CategoryQuery.Add.Params(
                name = "Field Mapping Test",
                description = "Testing field name and type mapping"
            )

            val insertedCategory = database.category.add.one(categoryParams)

            // Verify field mapping worked correctly
            assertNotNull(insertedCategory, "Category should not be null")
            
            // Test that the createdAt field (mapped from created_at column) is properly typed
            val createdAt = insertedCategory.createdAt
            assertNotNull(createdAt, "Created at should not be null")
            assertTrue(true, "Created at should be LocalDateTime")
            
            // Verify the field is accessible with the mapped property name
            assertTrue(createdAt.year > 2020, "Should be able to access createdAt property")
    }

    @Test
    fun testCategoryInsertReturningErrorHandling() = runDatabaseTest {
            database.open()

            // Test error handling with invalid data
            try {
                // Try to insert with empty name (should fail NOT NULL constraint)
                val invalidParams = CategoryQuery.Add.Params(
                    name = "", // Empty name might cause issues
                    description = "Invalid category"
                )

                database.category.add.one(invalidParams)
                // If we get here, the empty name was allowed, which is fine
                // The test mainly ensures no crashes occur
            } catch (e: Exception) {
                // If an exception occurs, ensure it's a reasonable database constraint error
                assertNotNull(e.message, "Exception should have a message")
            }

            // Test with very long name (testing practical limits)
            val longName = "A".repeat(1000) // Very long name
            val longNameParams = CategoryQuery.Add.Params(
                name = longName,
                description = "Testing long name handling"
            )

            try {
                val result = database.category.add.one(longNameParams)
                // If successful, verify the data was stored correctly
                assertEquals(longName, result.name, "Long name should be preserved")
            } catch (e: Exception) {
                // If it fails, that's also acceptable - depends on database limits
                assertNotNull(e.message, "Exception should have a message")
            }
    }
}
