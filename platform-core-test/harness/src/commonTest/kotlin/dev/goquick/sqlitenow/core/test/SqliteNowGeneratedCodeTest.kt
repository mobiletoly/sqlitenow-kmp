package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import dev.goquick.sqlitenow.core.test.db.PersonRow
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlin.test.*

/**
 * Integration tests for SQLiteNow generated code functionality.
 * Tests the actual generated database classes, query classes, and type adapters.
 */
class SqliteNowGeneratedCodeTest {

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
    fun testGeneratedDatabaseInitialization() = runDatabaseTest {
            // Test that generated database initializes correctly
            database.open()
            
            // Should have valid connection
            val connection = database.connection()
            assertNotNull(connection, "Connection should not be null")
            
            // Should have router objects
            assertNotNull(database.person, "Person router should not be null")
            assertNotNull(database.comment, "Comment router should not be null")
            assertNotNull(database.personAddress, "PersonAddress router should not be null")
            
            // Test that schema was created by migrations
            connection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='person'").use { statement ->
                assertTrue(statement.step(), "Should find person table")
                assertEquals("person", statement.getText(0), "Table name should be person")
            }
    }

    @Test
    fun testGeneratedQueryClassStructure() {
        // Test that generated query classes have expected structure
        
        // Test SQL constants
        assertNotNull(PersonQuery.SelectAll.SQL, "SelectAll SQL should not be null")
        assertTrue(PersonQuery.SelectAll.SQL.contains("SELECT"), "SelectAll SQL should contain SELECT")
        assertTrue(PersonQuery.SelectAll.SQL.contains("FROM person"), "SelectAll SQL should contain FROM person")
        assertEquals("SELECT * FROM person ORDER BY id DESC LIMIT ? OFFSET ?", PersonQuery.SelectAll.SQL, "SelectAll SQL should match expected")
        
        // Test affected tables
        assertNotNull(PersonQuery.SelectAll.affectedTables, "SelectAll affectedTables should not be null")
        assertTrue(PersonQuery.SelectAll.affectedTables.contains("person"), "SelectAll should affect person table")
        assertEquals(1, PersonQuery.SelectAll.affectedTables.size, "SelectAll should affect only person table")
        
        // Test parameter data class
        val params = PersonQuery.SelectAll.Params(limit = 10, offset = 0)
        assertEquals(10, params.limit, "Limit should be 10")
        assertEquals(0, params.offset, "Offset should be 0")
    }

    @Test
    fun testGeneratedQueryExecution() = runDatabaseTest {
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

            assertNotNull(insertedPerson, "Inserted person should not be null")
            assertTrue(insertedPerson.id > 0, "ID should be positive")
            assertEquals("John", insertedPerson.firstName, "First name should match")
            assertEquals("test@example.com", insertedPerson.email, "Email should match")
    }

    @Test
    fun testGeneratedSelectOperations() = runDatabaseTest {
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

            assertEquals(2, allPersons.size, "Should have 2 persons")

            // Results should be ordered by id DESC (most recent first)
            val firstPerson = allPersons[0]
            val secondPerson = allPersons[1]

            assertTrue(firstPerson.id > secondPerson.id, "First person ID should be greater than second")
            assertEquals("Bob", firstPerson.myFirstName, "First person should be Bob")
            assertEquals("Alice", secondPerson.myFirstName, "Second person should be Alice")

            // Test asOne
            val onePerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 1, offset = 0)).asOne()
            assertNotNull(onePerson, "Should get one person")
            assertEquals("Bob", onePerson.myFirstName, "Should be the most recent person (Bob)")

            // Test asOneOrNull with no results
            val noPerson = database.person.selectAll(PersonQuery.SelectAll.Params(limit = 1, offset = 10)).asOneOrNull()
            assertNull(noPerson, "Should get null when no results")
    }

    @Test
    fun testAsFlowEmitsOnTableChanges() = runDatabaseTest {
            database.open()
            database.enableTableChangeNotifications()

            val selectParams = PersonQuery.SelectAll.Params(limit = 10, offset = 0)
            val flow = database.person.selectAll(selectParams).asFlow()

            val emissions = mutableListOf<List<PersonRow>>()
            coroutineScope {
                val collector = launch(Dispatchers.Default) {
                    flow.take(2).collect { rows ->
                        emissions += rows
                    }
                }

                assertTrue(awaitConditionWithRealTimeout(timeoutMs = 500) { emissions.size >= 1 }, "Flow should emit the initial result promptly")

                database.person.add.one(
                    PersonQuery.Add.Params(
                        email = "flow@example.com",
                        firstName = "Flowy",
                        lastName = "Listener",
                        phone = null,
                        birthDate = LocalDate(1995, 12, 25),
                    ),
                )

                assertTrue(awaitConditionWithRealTimeout(timeoutMs = 1_000) { emissions.size >= 2 }, "Flow should emit the updated result promptly")
                collector.cancelAndJoin()
            }

            assertEquals(2, emissions.size, "Flow should emit initial and updated results")
            assertTrue(emissions.first().isEmpty(), "Initial emission should be empty")

            val updatedRows = emissions.last()
            assertEquals(1, updatedRows.size, "Updated emission should contain one row")
            val insertedRow = updatedRows.first()
            assertEquals("Flowy", insertedRow.myFirstName)
            assertEquals("Listener", insertedRow.myLastName)
    }

    @Test
    fun testGeneratedTypeAdapters() = runDatabaseTest {
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
            assertEquals(testDate, insertedPerson.birthDate, "Birth date should be preserved through adapters")
            
            // Verify LocalDateTime adapter
            assertNotNull(insertedPerson.createdAt, "Created at should not be null")
            assertTrue(insertedPerson.createdAt.year >= 2024, "Created at should be recent")
    }

    @Test
    fun testGeneratedParameterBinding() = runDatabaseTest {
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
            assertNull(insertedPerson.phone, "Phone should be null")
            assertNull(insertedPerson.birthDate, "Birth date should be null")
            
            // Non-null parameters should be preserved
            assertEquals("null-test@example.com", insertedPerson.email, "Email should be preserved")
            assertEquals("Null", insertedPerson.firstName, "First name should be preserved")
            assertEquals("Test", insertedPerson.lastName, "Last name should be preserved")
    }

    @Test
    fun testGeneratedQueryWithDateRangeParameters() = runDatabaseTest {
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

            assertEquals(1, personsInRange.size, "Should find 1 person in range")
            assertEquals("Middle", personsInRange[0].myFirstName, "Should be Middle Ground")
            assertEquals(LocalDate(1990, 6, 15), personsInRange[0].birthDate, "Birth date should match")
    }
}
