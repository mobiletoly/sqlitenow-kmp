/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.PersonRow
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Integration tests for SQLiteNow generated code functionality.
 * Tests the actual generated database classes, query classes, and type adapters.
 */
@RunWith(AndroidJUnit4::class)
class SqliteNowGeneratedCodeTest {

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
    fun testGeneratedDatabaseInitialization() {
        runBlocking {
            // Test that generated database initializes correctly
            database.open()
            
            // Should have valid connection
            val connection = database.connection()
            assertNotNull("Connection should not be null", connection)
            
            // Should have router objects
            assertNotNull("Person router should not be null", database.person)
            assertNotNull("Comment router should not be null", database.comment)
            assertNotNull("PersonAddress router should not be null", database.personAddress)
            
            // Test that schema was created by migrations
            connection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='person'").use { statement ->
                assertTrue("Should find person table", statement.step())
                assertEquals("Table name should be person", "person", statement.getText(0))
            }
        }
    }

    @Test
    fun testGeneratedQueryClassStructure() {
        // Test that generated query classes have expected structure
        
        // Test SQL constants
        assertNotNull("SelectAll SQL should not be null", PersonQuery.SelectAll.SQL)
        assertTrue("SelectAll SQL should contain SELECT", PersonQuery.SelectAll.SQL.contains("SELECT"))
        assertTrue("SelectAll SQL should contain FROM person", PersonQuery.SelectAll.SQL.contains("FROM person"))
        assertEquals("SelectAll SQL should match expected", "SELECT * FROM person ORDER BY id DESC LIMIT ? OFFSET ?", PersonQuery.SelectAll.SQL)
        
        // Test affected tables
        assertNotNull("SelectAll affectedTables should not be null", PersonQuery.SelectAll.affectedTables)
        assertTrue("SelectAll should affect person table", PersonQuery.SelectAll.affectedTables.contains("person"))
        assertEquals("SelectAll should affect only person table", 1, PersonQuery.SelectAll.affectedTables.size)
        
        // Test parameter data class
        val params = PersonQuery.SelectAll.Params(limit = 10, offset = 0)
        assertEquals("Limit should be 10", 10, params.limit)
        assertEquals("Offset should be 0", 0, params.offset)
    }

    @Test
    fun testGeneratedQueryExecution() {
        runBlocking {
            database.open()

            // Create test person data using generated code
            val testPerson = PersonQuery.Add.Params(
                email = "test@example.com",
                firstName = "John",
                lastName = "Doe",
                phone = "+1234567890",
                birthDate = LocalDate(1990, 1, 15)
            )

            // Test basic insert operation (without focusing on RETURNING details)
            val insertedPerson = database.person.add.one(testPerson)

            assertNotNull("Inserted person should not be null", insertedPerson)
            assertTrue("ID should be positive", insertedPerson.id > 0)
            assertEquals("First name should match", "John", insertedPerson.firstName)
            assertEquals("Email should match", "test@example.com", insertedPerson.email)
        }
    }

    @Test
    fun testGeneratedSelectOperations() {
        runBlocking {
            database.open()
            
            // Insert test data first using generated code
            val testPerson1 = PersonQuery.Add.Params(
                email = "person1@example.com",
                firstName = "Alice",
                lastName = "Smith",
                phone = null,
                birthDate = LocalDate(1985, 5, 20)
            )

            val testPerson2 = PersonQuery.Add.Params(
                email = "person2@example.com",
                firstName = "Bob",
                lastName = "Johnson",
                phone = "+9876543210",
                birthDate = LocalDate(1992, 8, 10)
            )
            
            database.person.add.one(testPerson1)
            database.person.add.one(testPerson2)
            
            // Test asList using generated SelectRunners
            val allPersons = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()

            assertEquals("Should have 2 persons", 2, allPersons.size)

            // Results should be ordered by id DESC (most recent first)
            val firstPerson = allPersons[0]
            val secondPerson = allPersons[1]

            assertTrue("First person ID should be greater than second", firstPerson.id > secondPerson.id)
            assertEquals("First person should be Bob", "Bob", firstPerson.myFirstName)
            assertEquals("Second person should be Alice", "Alice", secondPerson.myFirstName)

            // Test asOne
            val onePerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 1, offset = 0)).asOne()
            assertNotNull("Should get one person", onePerson)
            assertEquals("Should be the most recent person (Bob)", "Bob", onePerson.myFirstName)

            // Test asOneOrNull with no results
            val noPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 1, offset = 10)).asOneOrNull()
            assertNull("Should get null when no results", noPerson)
        }
    }

    @Test
    fun testAsFlowEmitsOnTableChanges() {
        runBlocking {
            database.open()
            database.enableTableChangeNotifications()

            val selectParams = PersonQuery.SelectAll.Params(limit = 10, offset = 0)
            val flow = database.person.selectAll(selectParams).asFlow()

            val emissions = mutableListOf<List<PersonRow>>()
            val firstEmission = CompletableDeferred<Unit>()

            val collector = launch {
                flow.take(2).collect { rows ->
                    emissions += rows
                    if (!firstEmission.isCompleted) {
                        firstEmission.complete(Unit)
                    }
                }
            }

            withTimeout(5_000) {
                firstEmission.await()
                database.person.add.one(
                    PersonQuery.Add.Params(
                        email = "flow@example.com",
                        firstName = "Flowy",
                        lastName = "Listener",
                        phone = null,
                        birthDate = LocalDate(1995, 12, 25),
                    ),
                )
                collector.join()
            }

            assertEquals("Flow should emit initial and updated results", 2, emissions.size)
            assertTrue("Initial emission should be empty", emissions.first().isEmpty())

            val updatedRows = emissions.last()
            assertEquals("Updated emission should contain one row", 1, updatedRows.size)
            val insertedRow = updatedRows.first()
            assertEquals("Flowy", insertedRow.myFirstName)
            assertEquals("Listener", insertedRow.myLastName)
        }
    }

    @Test
    fun testGeneratedTypeAdapters() {
        runBlocking {
            database.open()
            
            // Test with date parameters using generated type adapters
            val testDate = LocalDate(1988, 12, 25)
            
            val testPerson = PersonQuery.Add.Params(
                email = "date-test@example.com",
                firstName = "Date",
                lastName = "Test",
                phone = "+34-123-456-789",
                birthDate = testDate
            )
            
            val insertedPerson = database.person.add.one(testPerson)
            
            // Verify type adapters worked correctly
            assertEquals("Birth date should be preserved through adapters", testDate, insertedPerson.birthDate)
            
            // Verify LocalDateTime adapter
            assertNotNull("Created at should not be null", insertedPerson.createdAt)
            assertTrue("Created at should be recent", insertedPerson.createdAt.year >= 2024)
        }
    }

    @Test
    fun testGeneratedParameterBinding() {
        runBlocking {
            database.open()
            
            // Test INSERT with null parameters using generated code
            val testPerson = PersonQuery.Add.Params(
                email = "null-test@example.com",
                firstName = "Null",
                lastName = "Test",
                phone = null, // Test null parameter
                birthDate = null // Test null date parameter
            )

            val insertedPerson = database.person.add.one(testPerson)

            // Verify null parameters were handled correctly by generated code
            assertNull("Phone should be null", insertedPerson.phone)
            assertNull("Birth date should be null", insertedPerson.birthDate)
            
            // Non-null parameters should be preserved
            assertEquals("Email should be preserved", "null-test@example.com", insertedPerson.email)
            assertEquals("First name should be preserved", "Null", insertedPerson.firstName)
            assertEquals("Last name should be preserved", "Test", insertedPerson.lastName)
        }
    }

    @Test
    fun testGeneratedQueryWithDateRangeParameters() {
        runBlocking {
            database.open()
            
            // Insert persons with different birth dates using generated code
            val person1 = PersonQuery.Add.Params(
                email = "early@example.com",
                firstName = "Early",
                lastName = "Bird",
                phone = null,
                birthDate = LocalDate(1980, 1, 1)
            )

            val person2 = PersonQuery.Add.Params(
                email = "middle@example.com",
                firstName = "Middle",
                lastName = "Ground",
                phone = null,
                birthDate = LocalDate(1990, 6, 15)
            )

            val person3 = PersonQuery.Add.Params(
                email = "late@example.com",
                firstName = "Late",
                lastName = "Bloomer",
                phone = null,
                birthDate = LocalDate(2000, 12, 31)
            )
            
            database.person.add.one(person1)
            database.person.add.one(person2)
            database.person.add.one(person3)
            
            // Test date range query using generated code
            val rangeParams = PersonQuery.SelectAllByBirthdayRange.Params(
                startDate = LocalDate(1985, 1, 1),
                endDate = LocalDate(1995, 12, 31)
            )
            
            val personsInRange = database.person.selectAllByBirthdayRange(rangeParams).asList()

            assertEquals("Should find 1 person in range", 1, personsInRange.size)
            assertEquals("Should be Middle Ground", "Middle", personsInRange[0].myFirstName)
            assertEquals("Birth date should match", LocalDate(1990, 6, 15), personsInRange[0].birthDate)
        }
    }
}
