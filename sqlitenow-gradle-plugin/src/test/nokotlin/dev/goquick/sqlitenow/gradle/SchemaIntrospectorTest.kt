package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaIntrospectorTest {
    @TempDir
    lateinit var tempDir: File

    private lateinit var sqlFile: File

    @BeforeEach
    fun setUp() {
        // Create a temporary SQL file with CREATE TABLE statements
        sqlFile = File(tempDir, "schema.sql")
        sqlFile.writeText("""
            -- Create Person table
            CREATE TABLE Person (
                id INTEGER PRIMARY KEY NOT NULL,

                -- @@field=first_name @@propertyName=myFirstName
                first_name TEXT NOT NULL,

                -- @@field=last_name @@propertyName=myLastName @@nullable
                last_name TEXT NOT NULL,

                email TEXT UNIQUE,
                phone TEXT,
                birth_date TEXT,

                -- @@field=created_at @@propertyType=kotlinx.datetime.LocalDateTime @@nonNull
                created_at TEXT NOT NULL DEFAULT current_timestamp
            );

            -- Create PersonAddress table
            CREATE TABLE PersonAddress (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,

                -- @@field=address_type @@propertyType=dev.goquick.sqlitenow.samplekmp.model.AddressType
                address_type TEXT NOT NULL,

                street TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT,
                postal_code TEXT,
                country TEXT NOT NULL,

                -- @@field=is_primary @@propertyType=Boolean
                is_primary INTEGER NOT NULL DEFAULT 0,

                -- @@field=created_at @@propertyType=kotlinx.datetime.LocalDate
                created_at TEXT NOT NULL DEFAULT current_timestamp,

                FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
            );

            -- Create indexes
            CREATE INDEX idx_person_name ON Person(last_name, first_name);
            CREATE INDEX idx_person_email ON Person(email);
            CREATE INDEX idx_address_person_id ON PersonAddress(person_id);
            CREATE INDEX idx_address_primary ON PersonAddress(person_id, is_primary);
        """.trimIndent())
    }

    @Test
    @DisplayName("Test schema introspection with CREATE TABLE statements")
    fun testSchemaIntrospection() {
        // Create a SchemaIntrospector with the test SQL file
        val introspector = SchemaIntrospector(sqlFile)

        // Verify that the tables were correctly parsed
        assertEquals(2, introspector.tables.size, "Should have parsed 2 tables")

        // Verify the Person table
        val personTable = introspector.tables["Person"]
        assertNotNull(personTable, "Person table should be present")
        assertEquals("Person", personTable.name, "Table name should be 'Person'")
        assertEquals(7, personTable.columns.size, "Person table should have 7 columns")

        // Verify the id column
        val idColumn = personTable.columns.find { it.name == "id" }
        assertNotNull(idColumn, "id column should be present")
        assertEquals("INTEGER", idColumn.dataType, "id column should be INTEGER")
        assertTrue(idColumn.isPrimaryKey, "id column should be a primary key")
        assertTrue(idColumn.notNull, "id column should be NOT NULL")

        // Verify the first_name column with comments
        val firstNameColumn = personTable.columns.find { it.name == "first_name" }
        assertNotNull(firstNameColumn, "first_name column should be present")
        assertEquals("TEXT", firstNameColumn.dataType, "first_name column should be TEXT")
        assertTrue(firstNameColumn.notNull, "first_name column should be NOT NULL")
        assertEquals("myFirstName", firstNameColumn.annotations["propertyName"],
            "first_name column should have a propertyName annotation")

        // Verify the last_name column with comments
        val lastNameColumn = personTable.columns.find { it.name == "last_name" }
        assertNotNull(lastNameColumn, "last_name column should be present")
        assertEquals("TEXT", lastNameColumn.dataType, "last_name column should be TEXT")
        assertTrue(lastNameColumn.notNull, "last_name column should be NOT NULL")
        assertEquals("myLastName", lastNameColumn.annotations["propertyName"],
            "last_name column should have a propertyName annotation")
        assertTrue(lastNameColumn.annotations.containsKey("nullable"),
            "last_name column should have a nullable annotation")

        // Verify the email column
        val emailColumn = personTable.columns.find { it.name == "email" }
        assertNotNull(emailColumn, "email column should be present")
        assertEquals("TEXT", emailColumn.dataType, "email column should be TEXT")
        assertTrue(emailColumn.isUnique, "email column should be UNIQUE")

        // Verify the created_at column with comments
        val createdAtColumn = personTable.columns.find { it.name == "created_at" }
        assertNotNull(createdAtColumn, "created_at column should be present")
        assertEquals("TEXT", createdAtColumn.dataType, "created_at column should be TEXT")
        assertTrue(createdAtColumn.notNull, "created_at column should be NOT NULL")
        assertEquals("kotlinx.datetime.LocalDateTime", createdAtColumn.annotations["propertyType"],
            "created_at column should have a propertyType annotation")
        assertTrue(createdAtColumn.annotations.containsKey("nonNull"),
            "created_at column should have a nonNull annotation")

        // Verify the PersonAddress table
        val addressTable = introspector.tables["PersonAddress"]
        assertNotNull(addressTable, "PersonAddress table should be present")
        assertEquals("PersonAddress", addressTable.name, "Table name should be 'PersonAddress'")
        assertEquals(10, addressTable.columns.size, "PersonAddress table should have 10 columns")

        // Verify the person_id column
        val personIdColumn = addressTable.columns.find { it.name == "person_id" }
        assertNotNull(personIdColumn, "person_id column should be present")
        assertEquals("INTEGER", personIdColumn.dataType, "person_id column should be INTEGER")
        assertTrue(personIdColumn.notNull, "person_id column should be NOT NULL")

        // Verify the address_type column with comments
        val addressTypeColumn = addressTable.columns.find { it.name == "address_type" }
        assertNotNull(addressTypeColumn, "address_type column should be present")
        assertEquals("TEXT", addressTypeColumn.dataType, "address_type column should be TEXT")
        assertTrue(addressTypeColumn.notNull, "address_type column should be NOT NULL")
        assertEquals("dev.goquick.sqlitenow.samplekmp.model.AddressType", addressTypeColumn.annotations["propertyType"],
            "address_type column should have a propertyType annotation")

        // Verify the is_primary column with comments
        val isPrimaryColumn = addressTable.columns.find { it.name == "is_primary" }
        assertNotNull(isPrimaryColumn, "is_primary column should be present")
        assertEquals("INTEGER", isPrimaryColumn.dataType, "is_primary column should be INTEGER")
        assertTrue(isPrimaryColumn.notNull, "is_primary column should be NOT NULL")
        assertEquals("Boolean", isPrimaryColumn.annotations["propertyType"],
            "is_primary column should have a propertyType annotation")
    }

    @Test
    @DisplayName("Test schema introspection with empty file")
    fun testEmptyFile() {
        // Create an empty SQL file
        val emptyFile = File(tempDir, "empty.sql")
        emptyFile.writeText("")

        // Create a SchemaIntrospector with the empty SQL file
        val introspector = SchemaIntrospector(emptyFile)

        // Verify that no tables were parsed
        assertEquals(0, introspector.tables.size, "Should have parsed 0 tables")
    }

    @Test
    @DisplayName("Test schema introspection with non-CREATE TABLE statements")
    fun testNonCreateTableStatements() {
        // Create a SQL file with non-CREATE TABLE statements
        val nonCreateFile = File(tempDir, "non-create.sql")
        nonCreateFile.writeText("""
            -- Insert some data
            INSERT INTO Person (id, first_name, last_name, email) VALUES (1, 'John', 'Doe', 'john.doe@example.com');

            -- Select some data
            SELECT * FROM Person WHERE id = 1;

            -- Create a view
            CREATE VIEW PersonView AS SELECT * FROM Person;
        """.trimIndent())

        // Create a SchemaIntrospector with the non-CREATE TABLE SQL file
        val introspector = SchemaIntrospector(nonCreateFile)

        // Verify that no tables were parsed
        assertEquals(0, introspector.tables.size, "Should have parsed 0 tables")
    }
}
