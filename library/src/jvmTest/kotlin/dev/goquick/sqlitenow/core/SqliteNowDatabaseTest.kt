package dev.goquick.sqlitenow.core

import androidx.sqlite.execSQL
import kotlinx.coroutines.test.runTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail
import java.io.File
import kotlinx.coroutines.runBlocking

class SqliteNowDatabaseTest {
    private lateinit var database: SqliteNowDatabase
    private val testDbNameBase = "test_database"
    private lateinit var testDbName: String
    private lateinit var testDbFile: File

    // Mock implementation of DatabaseMigrations
    private class TestMigrations : DatabaseMigrations {
        var lastAppliedVersion = -1

        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            lastAppliedVersion = currentVersion
            // For fresh databases (currentVersion == -1), return 0
            // For existing databases, increment by 1
            return if (currentVersion == -1) 0 else currentVersion + 1
        }
    }

    private val testMigrations = TestMigrations()

    @BeforeTest
    fun setup() {
        // Create a unique database name for each test
        testDbName = "$testDbNameBase-${System.currentTimeMillis()}.db"
        testDbFile = File(testDbName)
        database = SqliteNowDatabase(testDbName, testMigrations)
    }

    @AfterTest
    fun teardown() {
        try {
            // Close the database connection first
            if (this::database.isInitialized) {
                runBlocking { database.close() }
            }
        } finally {
            // Clean up the database file
            if (this::testDbFile.isInitialized && testDbFile.exists()) {
                val deleted = testDbFile.delete()
                if (!deleted) {
                    println("Warning: Failed to delete test database file: ${testDbFile.absolutePath}")
                }
            }
        }
    }

    @Test
    fun testUserVersionOperations() = runTest {
        // Open the database
        database.open()

        // After opening, user_version should be 0 (fresh database with -1 -> 0 migration)
        assertEquals(0, database.getUserVersion())

        // Set user_version to 5
        database.setUserVersion(5)

        // Test suspended versions
        assertEquals(5, database.getUserVersion())
    }

    @Test
    fun testMigrationReceivesCorrectVersion() = runTest {
        // Open the database
        database.open()

        // Verify that the migration was called with version -1 (fresh database)
        assertEquals(-1, testMigrations.lastAppliedVersion)

        // Set user_version to 3
        database.setUserVersion(3)

        // Close and reopen the database
        database.close()
        database = SqliteNowDatabase(testDbName, testMigrations)
        database.open()

        // Verify that the migration was called with version 3
        assertEquals(3, testMigrations.lastAppliedVersion)
    }

    @Test
    fun testTransactionCommit() = runTest {
        // Open the database
        database.open()

        // Create a test table
        database.connection().ref.execSQL("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        """)

        // Execute a transaction that should commit
        val result = database.transaction {
            // Insert a row
            database.connection().ref.execSQL("""
                INSERT INTO test_table (id, name) VALUES (1, 'Test Name');
            """)

            // Return a value from the transaction
            "Transaction completed"
        }

        // Verify the result
        assertEquals("Transaction completed", result)

        // Verify that the row was inserted
        val statement = database.connection().ref.prepare("SELECT COUNT(*) FROM test_table;")
        try {
            statement.step()
            val count = statement.getInt(0)
            assertEquals(1, count, "Row should be inserted after successful transaction")
        } finally {
            statement.close()
        }
    }

    @Test
    fun testTransactionRollback() = runTest {
        // Open the database
        database.open()

        // Create a test table
        database.connection().ref.execSQL("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        """)

        // Execute a transaction that should roll back
        try {
            database.transaction {
                // Insert a row
                database.connection().ref.execSQL("""
                    INSERT INTO test_table (id, name) VALUES (1, 'Test Name');
                """)

                // Throw an exception to trigger rollback
                throw RuntimeException("Test exception to trigger rollback")
            }

            // Should not reach here
            fail("Transaction should have thrown an exception")
        } catch (e: RuntimeException) {
            // Expected exception
            assertEquals("Test exception to trigger rollback", e.message)
        }

        // Verify that the row was not inserted (rolled back)
        val statement = database.connection().ref.prepare("SELECT COUNT(*) FROM test_table;")
        try {
            statement.step()
            val count = statement.getInt(0)
            assertEquals(0, count, "Row should not be inserted after transaction rollback")
        } finally {
            statement.close()
        }
    }

    @Test
    fun testDatabaseFileCleanupOnException() = runTest {
        // Open the database
        database.open()

        // Verify the database file exists
        assertTrue(testDbFile.exists(), "Database file should exist after opening")

        // The teardown method should clean up the file even if this test were to fail
        // This test just verifies that the file exists during the test
        assertTrue(testDbFile.exists(), "Database file should still exist during test execution")
    }
}
