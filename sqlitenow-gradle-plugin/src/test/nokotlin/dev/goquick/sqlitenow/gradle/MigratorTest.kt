package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.sql.Connection
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class MigratorTest {

    private lateinit var connection: Connection

    @TempDir
    lateinit var tempDir: File

    @BeforeEach
    fun setUp() {
        // Create a migrations directory
        val migrationsDir = File(tempDir, "migrations")
        migrationsDir.mkdirs()

        // Create a test migration file
        val migrationFile = File(migrationsDir, "01_create_tables.sql")
        migrationFile.writeText("""
            CREATE TABLE IF NOT EXISTS Person (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS PersonAddress (
                id INTEGER PRIMARY KEY,
                person_id INTEGER NOT NULL,
                street TEXT NOT NULL,
                city TEXT NOT NULL,
                FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
            );
        """.trimIndent())
    }

    @AfterEach
    fun tearDown() {
        if (::connection.isInitialized && !connection.isClosed) {
            connection.close()
        }
    }

    @Test
    @DisplayName("Test creating an in-memory database")
    fun testCreateInMemoryDatabase() {
        // Create a migrator with in-memory storage
        val migrator = Migrator(
            migrationDir = File(tempDir, "migrations"),
            storage = MigratorTempStorage.Memory
        )

        // Get the connection
        connection = migrator.connection

        // Verify the connection is valid
        assertNotNull(connection)
        assertTrue(!connection.isClosed)

        // Verify the tables were created using the existing connection
        val introspector = Migrator(File(tempDir, "migrations"), MigratorTempStorage.Memory, logIntrospectionResults = false)
        val tables = introspector.tables
        assertEquals(2, tables.size)

        // Verify the Person table
        val personTable = tables.find { it.name == "Person" }
        assertNotNull(personTable)
        assertEquals(4, personTable.columns.size)

        // Verify the PersonAddress table
        val addressTable = tables.find { it.name == "PersonAddress" }
        assertNotNull(addressTable)
        assertEquals(4, addressTable.columns.size)

        // Verify the foreign key
        assertEquals(1, addressTable.foreignKeys.size)
    }

    @Test
    @DisplayName("Test creating a file database")
    fun testCreateFileDatabase() {
        // Create a database file
        val dbFile = File(tempDir, "test.db")

        // Create a migrator with file storage
        val migrator = Migrator(
            migrationDir = File(tempDir, "migrations"),
            storage = MigratorTempStorage.File(dbFile)
        )

        // Get the connection
        connection = migrator.connection

        // Verify the connection is valid
        assertNotNull(connection)
        assertTrue(!connection.isClosed)

        // Verify the database file was created
        assertTrue(dbFile.exists())

        // Verify the tables were created using the existing connection
        val introspector = Migrator(File(tempDir, "migrations"), MigratorTempStorage.Memory, logIntrospectionResults = false)
        val tables = introspector.tables
        assertEquals(2, tables.size)

        // Verify the Person table
        val personTable = tables.find { it.name == "Person" }
        assertNotNull(personTable)
        assertEquals(4, personTable.columns.size)
    }
}
