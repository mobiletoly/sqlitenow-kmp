package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.CategoryQuery
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonCategoryQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class CollectionMappingTest {

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
    fun testPerRowMapping() = runBlocking {
        // Create test data
        val person = database.person.add.one(PersonQuery.Add.Params(
            firstName = "John",
            lastName = "Doe",
            email = "john.doe@example.com",
            phone = "555-1234",
            birthDate = LocalDate(1990, 1, 15)
        ))

        val category = database.category.add.one(CategoryQuery.Add.Params(
            name = "Technology",
            description = "Tech-related category"
        ))

        // Create person-category relationship
        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = category.id,
            isPrimary = true
        ))

        // Test perRow mapping - should map joined data to a single nested object
        val result = database.category.selectWithPersonPerRow(
            CategoryQuery.SelectWithPersonPerRow.Params(categoryId = category.id)
        ).asList()

        assertEquals(1, result.size)
        val categoryWithPerson = result.first()
        assertEquals("Technology", categoryWithPerson.name)
        assertEquals("Tech-related category", categoryWithPerson.description)

        // Verify perRow mapping creates single nested object
        assertNotNull(categoryWithPerson.primaryPerson)
        assertEquals("John", categoryWithPerson.primaryPerson!!.myFirstName)
        assertEquals("Doe", categoryWithPerson.primaryPerson!!.myLastName)
        assertEquals("john.doe@example.com", categoryWithPerson.primaryPerson!!.email)
    }

    @Test
    fun testCollectionMapping() = runBlocking {
        // Create test data
        val person1 = database.person.add.one(PersonQuery.Add.Params(
            firstName = "Alice",
            lastName = "Smith",
            email = "alice@example.com",
            phone = "555-1111",
            birthDate = LocalDate(1985, 3, 20)
        ))

        val person2 = database.person.add.one(PersonQuery.Add.Params(
            firstName = "Bob",
            lastName = "Johnson",
            email = "bob@example.com",
            phone = "555-2222",
            birthDate = LocalDate(1992, 7, 10)
        ))

        val category = database.category.add.one(CategoryQuery.Add.Params(
            name = "Engineering",
            description = "Engineering team category"
        ))

        // Create person-category relationships
        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = person1.id,
            categoryId = category.id,
            isPrimary = true
        ))

        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = person2.id,
            categoryId = category.id,
            isPrimary = false
        ))

        // Test collection mapping - should group multiple joined records into List<T>
        val result = database.category.selectWithPersonsCollection(
            CategoryQuery.SelectWithPersonsCollection.Params(categoryId = category.id)
        ).asList()

        assertEquals(1, result.size)
        val categoryWithPersons = result.first()
        assertEquals("Engineering", categoryWithPersons.name)
        assertEquals("Engineering team category", categoryWithPersons.description)

        // Verify collection mapping creates List<T> of related objects
        assertEquals(2, categoryWithPersons.persons.size)

        val alice = categoryWithPersons.persons.find { it.myFirstName == "Alice" }
        val bob = categoryWithPersons.persons.find { it.myFirstName == "Bob" }

        assertNotNull(alice)
        assertNotNull(bob)
        assertEquals("Smith", alice!!.myLastName)
        assertEquals("Johnson", bob!!.myLastName)
        assertEquals("alice@example.com", alice.email)
        assertEquals("bob@example.com", bob.email)
    }

    @Test
    fun testEmptyCollectionMapping() = runBlocking {
        // Create category without any associated persons
        val category = database.category.add.one(CategoryQuery.Add.Params(
            name = "Empty Category",
            description = "Category with no persons"
        ))

        // Test collection mapping with no related records
        val result = database.category.selectWithPersonsCollection(
            CategoryQuery.SelectWithPersonsCollection.Params(categoryId = category.id)
        ).asList()

        assertEquals(1, result.size)
        val categoryWithPersons = result.first()
        assertEquals("Empty Category", categoryWithPersons.name)

        // Verify empty collection is handled correctly
        assertEquals(0, categoryWithPersons.persons.size)
        assertTrue(categoryWithPersons.persons.isEmpty())
    }

    @Test
    fun testCollectionQueryRestrictions() = runBlocking {
        // Create test data
        val person = database.person.add.one(PersonQuery.Add.Params(
            firstName = "Test",
            lastName = "User",
            email = "test@example.com",
            phone = "555-0000",
            birthDate = LocalDate(1990, 1, 1)
        ))

        val category = database.category.add.one(CategoryQuery.Add.Params(
            name = "Test Category",
            description = "For testing restrictions"
        ))

        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = person.id,
            categoryId = category.id,
            isPrimary = true
        ))

        val selectRunners = database.category.selectWithPersonsCollection(
            CategoryQuery.SelectWithPersonsCollection.Params(categoryId = category.id)
        )

        // Test that collection queries support asList()
        val listResult = selectRunners.asList()
        assertEquals(1, listResult.size)
        assertEquals(1, listResult.first().persons.size)

        // Verify asOne() hydrates the same row
        val asOneResult = selectRunners.asOne()
        assertEquals(category.id, asOneResult.categoryId)
        assertEquals(listResult.first().persons.size, asOneResult.persons.size)

        // Verify asOneOrNull() mirrors asOne()
        val asOneOrNullResult = selectRunners.asOneOrNull()
        assertNotNull(asOneOrNullResult)
        assertEquals(category.id, asOneOrNullResult?.categoryId)
    }

    @Test
    fun testManyToManyRelationships() = runBlocking {
        // Create multiple persons and categories to test many-to-many relationships
        val dev1 = database.person.add.one(PersonQuery.Add.Params(
            firstName = "Developer",
            lastName = "One",
            email = "dev1@example.com",
            phone = "555-1001",
            birthDate = LocalDate(1988, 5, 15)
        ))

        val dev2 = database.person.add.one(PersonQuery.Add.Params(
            firstName = "Developer",
            lastName = "Two",
            email = "dev2@example.com",
            phone = "555-1002",
            birthDate = LocalDate(1991, 8, 22)
        ))

        val frontendCategory = database.category.add.one(CategoryQuery.Add.Params(
            name = "Frontend",
            description = "Frontend development"
        ))

        val backendCategory = database.category.add.one(CategoryQuery.Add.Params(
            name = "Backend",
            description = "Backend development"
        ))

        // Create many-to-many relationships
        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = dev1.id,
            categoryId = frontendCategory.id,
            isPrimary = true
        ))

        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = dev1.id,
            categoryId = backendCategory.id,
            isPrimary = false
        ))

        database.personCategory.add.one(PersonCategoryQuery.Add.Params(
            personId = dev2.id,
            categoryId = frontendCategory.id,
            isPrimary = true
        ))

        // Test frontend category has both developers
        val frontendResult = database.category.selectWithPersonsCollection(
            CategoryQuery.SelectWithPersonsCollection.Params(categoryId = frontendCategory.id)
        ).asList()

        assertEquals(1, frontendResult.size)
        assertEquals(2, frontendResult.first().persons.size)

        // Test backend category has only dev1
        val backendResult = database.category.selectWithPersonsCollection(
            CategoryQuery.SelectWithPersonsCollection.Params(categoryId = backendCategory.id)
        ).asList()

        assertEquals(1, backendResult.size)
        assertEquals(1, backendResult.first().persons.size)
        assertEquals("Developer", backendResult.first().persons.first().myFirstName)
        assertEquals("One", backendResult.first().persons.first().myLastName)
    }
}
