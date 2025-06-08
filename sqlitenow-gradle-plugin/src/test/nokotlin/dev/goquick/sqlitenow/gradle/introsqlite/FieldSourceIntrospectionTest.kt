package dev.goquick.sqlitenow.gradle.introsqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class FieldSourceIntrospectionTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        // Create an in-memory SQLite database for testing
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create test tables
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address_type TEXT,
                    street TEXT,
                    city TEXT,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT,
                    is_primary INTEGER,
                    created_at TEXT,
                    FOREIGN KEY (person_id) REFERENCES Person(id)
                )
            """)

            // Insert some test data
            stmt.executeUpdate("""
                INSERT INTO Person (id, first_name, last_name, email, phone, birth_date, created_at)
                VALUES
                (1, 'John', 'Doe', 'john@example.com', '555-1234', '1980-01-01', '2023-01-01'),
                (2, 'Jane', 'Smith', 'jane@example.com', '555-5678', '1985-02-15', '2023-01-02')
            """)

            stmt.executeUpdate("""
                INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary, created_at)
                VALUES
                (1, 1, 'HOME', '123 Main St', 'Anytown', 'CA', '12345', 'USA', 1, '2023-01-01'),
                (2, 1, 'WORK', '456 Office Blvd', 'Workville', 'CA', '67890', 'USA', 0, '2023-01-02'),
                (3, 2, 'HOME', '789 Elm St', 'Othertown', 'NY', '54321', 'USA', 1, '2023-01-03')
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting field sources from a complex JOIN query with aliases")
    fun testComplexJoinWithAliases() {
        val sql = """
            SELECT p.id AS person_id,
            p.first_name, p.last_name, p.email, p.phone, p.birth_date,
            p.created_at AS person_created_at,
            a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
            a.is_primary, a.created_at AS address_created_at,
            p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :namePattern
            AND p.last_name LIKE :namePattern
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)

        // Verify field names
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(17, fieldNames.size)
        assertEquals("person_id", fieldNames[0])
        assertEquals("first_name", fieldNames[1])
        assertEquals("last_name", fieldNames[2])
        assertEquals("person_created_at", fieldNames[6])
        assertEquals("address_id", fieldNames[7])
        assertEquals("address_created_at", fieldNames[15])
        assertEquals("full_name", fieldNames[16])

        // Verify field sources
        assertEquals(17, statementInfo.fieldSources.size)

        // Check person_id field source
        val personIdSource = statementInfo.fieldSources.find { it.fieldName == "person_id" }
        assertNotNull(personIdSource)
        assertEquals("Person", personIdSource.tableName)
        assertEquals("id", personIdSource.columnName)  // The actual column name is 'id', not 'person_id' which is the alias

        // Check first_name field source
        val firstNameSource = statementInfo.fieldSources.find { it.fieldName == "first_name" }
        assertNotNull(firstNameSource)
        assertEquals("Person", firstNameSource.tableName)
        assertEquals("first_name", firstNameSource.columnName)

        // Check person_created_at field source
        val personCreatedAtSource = statementInfo.fieldSources.find { it.fieldName == "person_created_at" }
        assertNotNull(personCreatedAtSource)
        assertEquals("Person", personCreatedAtSource.tableName)
        assertEquals("created_at", personCreatedAtSource.columnName)  // The actual column name is 'created_at'

        // Check address_id field source
        val addressIdSource = statementInfo.fieldSources.find { it.fieldName == "address_id" }
        assertNotNull(addressIdSource)
        assertEquals("PersonAddress", addressIdSource.tableName)
        assertEquals("id", addressIdSource.columnName)  // The actual column name is 'id'

        // Check address_created_at field source
        val addressCreatedAtSource = statementInfo.fieldSources.find { it.fieldName == "address_created_at" }
        assertNotNull(addressCreatedAtSource)
        assertEquals("PersonAddress", addressCreatedAtSource.tableName)
        assertEquals("created_at", addressCreatedAtSource.columnName)  // The actual column name is 'created_at'

        // Check full_name field source (expression)
        val fullNameSource = statementInfo.fieldSources.find { it.fieldName == "full_name" }
        assertNotNull(fullNameSource)
        // For expressions, the tableName and columnName might be null or derived from the expression
        assertTrue(fullNameSource.expression.contains("p.first_name") && fullNameSource.expression.contains("p.last_name"))

        // Verify parameters
        assertEquals(2, statementInfo.preparedSql.parameters.size)
        assertEquals("namePattern", statementInfo.preparedSql.parameters[0].name)
        assertEquals("namePattern", statementInfo.preparedSql.parameters[1].name)
    }

    @Test
    @DisplayName("Test extracting field sources from a query with expressions")
    fun testQueryWithExpressions() {
        val sql = """
            SELECT
                p.id,
                p.first_name || ' ' || p.last_name AS full_name,
                UPPER(p.email) AS upper_email,
                CASE WHEN p.birth_date IS NULL THEN 'Unknown' ELSE p.birth_date END AS birth_date_display,
                COUNT(a.id) AS address_count
            FROM Person p
            LEFT JOIN PersonAddress a ON p.id = a.person_id
            GROUP BY p.id, p.first_name, p.last_name, p.email, p.birth_date
        """.trimIndent()

        val statementInfo = SqlStatementIntrospector(sql, connection).introspect()

        assertEquals("SELECT", statementInfo.statementType)

        // Verify field names
        val fieldNames = statementInfo.fieldSources.map { it.fieldName }
        assertEquals(5, fieldNames.size)
        assertEquals("id", fieldNames[0])
        assertEquals("full_name", fieldNames[1])
        assertEquals("upper_email", fieldNames[2])
        assertEquals("birth_date_display", fieldNames[3])
        assertEquals("address_count", fieldNames[4])

        // Verify field sources
        assertEquals(5, statementInfo.fieldSources.size)

        // Check id field source
        val idSource = statementInfo.fieldSources.find { it.fieldName == "id" }
        assertNotNull(idSource)
        assertEquals("Person", idSource.tableName)
        assertEquals("id", idSource.columnName)

        // Check full_name field source (expression)
        val fullNameSource = statementInfo.fieldSources.find { it.fieldName == "full_name" }
        assertNotNull(fullNameSource)
        assertTrue(fullNameSource.expression.contains("p.first_name") && fullNameSource.expression.contains("p.last_name"))

        // Check upper_email field source (function)
        val upperEmailSource = statementInfo.fieldSources.find { it.fieldName == "upper_email" }
        assertNotNull(upperEmailSource)
        assertTrue(upperEmailSource.expression.contains("UPPER") && upperEmailSource.expression.contains("p.email"))

        // Check address_count field source (aggregate function)
        val addressCountSource = statementInfo.fieldSources.find { it.fieldName == "address_count" }
        assertNotNull(addressCountSource)
        assertTrue(addressCountSource.expression.contains("COUNT") && addressCountSource.expression.contains("a.id"))
    }
}
