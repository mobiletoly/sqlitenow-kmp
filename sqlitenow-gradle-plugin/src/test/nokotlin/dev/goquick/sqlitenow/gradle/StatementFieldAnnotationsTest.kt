package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class StatementFieldAnnotationsTest {
    private lateinit var connection: Connection

    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:")
        connection.createStatement().use { stmt ->
            stmt.executeUpdate("""
                CREATE TABLE Person (
                    id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT DEFAULT current_timestamp
                )
            """)

            stmt.executeUpdate("""
                CREATE TABLE PersonAddress (
                    id INTEGER PRIMARY KEY,
                    person_id INTEGER,
                    address_type TEXT NOT NULL,
                    street TEXT NOT NULL,
                    city TEXT NOT NULL,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT NOT NULL,
                    is_primary INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT current_timestamp,
                    FOREIGN KEY (person_id) REFERENCES Person(id) ON DELETE CASCADE
                )
            """)
        }
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    @Test
    @DisplayName("Test extracting field annotations from SQL statement")
    fun testExtractFieldAnnotations() {
        val sqlWithFieldAnnotations = """
            -- @@className=SelectPersonWithAddress
            SELECT
                p.id AS person_id,

                -- @@field=count_here @@propertyName=totalPersonCount @@nonNull
                (SELECT count(*) FROM Person) AS count_here,

                -- @@field=first_name @@propertyName=firstName
                p.first_name,

                p.last_name, p.email, p.phone, p.birth_date,
                p.created_at AS person_created_at,
                a.id AS address_id, a.address_type, a.street, a.city, a.state, a.postal_code, a.country,
                a.is_primary, a.created_at AS address_created_at,
                p.first_name || ' ' || p.last_name AS full_name
            FROM Person p
                JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.first_name LIKE :firstName
                AND p.last_name LIKE :lastName;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithFieldAnnotations, connection, emptyList())
        val fieldAnnotations = codeGenerator.annotatedStatement.fieldAnnotations

        println("\nField annotations:")
        fieldAnnotations.forEach { (fieldName, annotations) ->
            println("Field: $fieldName, Annotations: $annotations")
        }

        assertNotNull(fieldAnnotations, "Field annotations should not be null")
        assertEquals(2, fieldAnnotations.size, "Should have extracted annotations for 2 fields")
        val countHereAnnotations = fieldAnnotations["count_here"]
        assertNotNull(countHereAnnotations, "count_here annotations should not be null")
        assertEquals("totalPersonCount", countHereAnnotations["propertyName"], "propertyName annotation should be extracted")
        assertTrue(countHereAnnotations.containsKey("nonNull"), "nonNull annotation should be extracted")

        val firstNameAnnotations = fieldAnnotations["first_name"]
        assertNotNull(firstNameAnnotations, "first_name annotations should not be null")
        assertEquals("firstName", firstNameAnnotations["propertyName"], "propertyName annotation should be extracted")
    }

    @Test
    @DisplayName("Test generating code with field annotations")
    fun testGenerateCodeWithFieldAnnotations() {
        val sqlWithFieldAnnotations = """
            -- @@className=PersonWithCount
            SELECT
                p.id AS person_id,

                -- @@field=count_here @@propertyName=totalPersonCount @@nonNull
                (SELECT count(*) FROM Person) AS count_here,

                -- @@field=first_name @@propertyName=firstName
                p.first_name,

                -- @@field=last_name @@nullable
                p.last_name
            FROM Person p
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithFieldAnnotations, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code:")
        println(generatedCode)

        assertTrue(generatedCode.contains("totalPersonCount"), "Generated code should use the custom property name 'totalPersonCount'")
        assertTrue(generatedCode.contains("firstName"), "Generated code should use the custom property name 'firstName'")
        assertTrue(generatedCode.contains("totalPersonCount: Int"), "totalPersonCount should be non-nullable")
        assertTrue(generatedCode.contains("lastName: String?"), "lastName should be nullable")
    }

    @Test
    @DisplayName("Test field annotations with multiple inline comments")
    fun testFieldAnnotationsWithMultipleInlineComments() {
        val sqlWithMultipleInlineComments = """
            SELECT
                p.id AS person_id,

                -- @@field=count_here
                -- @@propertyName=totalPersonCount
                -- @@nonNull
                (SELECT count(*) FROM Person) AS count_here,

                -- @@field=first_name @@nullable
                p.first_name, p.last_name
            FROM Person p
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithMultipleInlineComments, connection, emptyList())
        val fieldAnnotations = codeGenerator.annotatedStatement.fieldAnnotations

        println("\nInner comments:")
        codeGenerator.annotatedStatement.statementInfo.innerComments.forEach { println("  $it") }

        println("\nField annotations:")
        fieldAnnotations.forEach { (fieldName, annotations) ->
            println("  Field: $fieldName, Annotations: $annotations")
        }

        assertNotNull(fieldAnnotations, "Field annotations should not be null")
        assertEquals(2, fieldAnnotations.size, "Should have extracted annotations for 2 fields")

        val countHereAnnotations = fieldAnnotations["count_here"]
        assertNotNull(countHereAnnotations, "count_here annotations should not be null")
        assertEquals("totalPersonCount", countHereAnnotations["propertyName"], "propertyName annotation should be extracted")
        assertTrue(countHereAnnotations.containsKey("nonNull"), "nonNull annotation should be extracted")

        val firstNameAnnotations = fieldAnnotations["first_name"]
        assertNotNull(firstNameAnnotations, "first_name annotations should not be null")
        assertTrue(firstNameAnnotations.containsKey("nullable"), "nullable annotation should be extracted")
    }

    @Test
    @DisplayName("Test propertyNameGenerator annotation")
    fun testPropertyNameGenerator() {
        val sqlWithPropertyNameGenerator = """
            -- @@className=PersonWithAddress @@propertyNameGenerator=lowerCamelCase
            SELECT
                p.id AS person_id,
                p.first_name, p.last_name, p.email,
                a.id AS address_id,
                a.street AS address_street,
                a.city AS address_city,
                a.postal_code AS address_postal_code
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithPropertyNameGenerator, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code with propertyNameGenerator:")
        println(generatedCode)

        assertTrue(generatedCode.contains("personId"), "Generated code should use camel case for 'person_id'")
        assertTrue(generatedCode.contains("firstName"), "Generated code should use camel case for 'first_name'")
        assertTrue(generatedCode.contains("lastName"), "Generated code should use camel case for 'last_name'")
        assertTrue(generatedCode.contains("addressId"), "Generated code should use camel case for 'address_id'")
        assertTrue(generatedCode.contains("addressStreet"), "Generated code should use camel case for 'address_street'")
        assertTrue(generatedCode.contains("addressCity"), "Generated code should use camel case for 'address_city'")
        assertTrue(generatedCode.contains("addressPostalCode"), "Generated code should use camel case for 'address_postal_code'")
    }

    @Test
    @DisplayName("Test propertyNameGenerator with field-specific overrides")
    fun testPropertyNameGeneratorWithOverrides() {
        val sqlWithOverrides = """
            -- @@className=PersonWithAddress @@propertyNameGenerator=lowerCamelCase
            SELECT
                p.id AS person_id,

                -- @@field=first_name @@propertyName=givenName
                p.first_name,

                p.last_name, p.email,
                a.id AS address_id,
                a.street AS address_street
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithOverrides, connection, emptyList())
        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code with propertyNameGenerator and overrides:")
        println(generatedCode)

        assertTrue(generatedCode.contains("personId"), "Generated code should use camel case for 'person_id'")
        assertTrue(generatedCode.contains("givenName"), "Generated code should use the override 'givenName' for 'first_name'")
        assertTrue(generatedCode.contains("lastName"), "Generated code should use camel case for 'last_name'")
        assertTrue(generatedCode.contains("addressId"), "Generated code should use camel case for 'address_id'")
        assertTrue(generatedCode.contains("addressStreet"), "Generated code should use camel case for 'address_street'")
    }

    @Test
    @DisplayName("Test default property name generator (lowerCamelCase)")
    fun testDefaultPropertyNameGenerator() {
        val sqlWithoutGenerator = """
            -- @@className=PersonWithAddress
            SELECT
                p.id AS person_id,
                p.first_name, p.last_name, p.email,
                a.id AS address_id,
                a.street AS address_street
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithoutGenerator, connection, emptyList())

        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code with default property name generator:")
        println(generatedCode)

        assertTrue(generatedCode.contains("personId"), "Generated code should use camel case for 'person_id' by default")
        assertTrue(generatedCode.contains("firstName"), "Generated code should use camel case for 'first_name' by default")
        assertTrue(generatedCode.contains("lastName"), "Generated code should use camel case for 'last_name' by default")
        assertTrue(generatedCode.contains("addressId"), "Generated code should use camel case for 'address_id' by default")
        assertTrue(generatedCode.contains("addressStreet"), "Generated code should use camel case for 'address_street' by default")
    }

    @Test
    @DisplayName("Test plain property name generator")
    fun testPlainPropertyNameGenerator() {
        val sqlWithPlainGenerator = """
            -- @@className=PersonWithAddress @@propertyNameGenerator=plain
            SELECT
                p.id AS person_id,
                p.first_name, p.last_name, p.email,
                a.id AS address_id,
                a.street AS address_street
            FROM Person p
            JOIN PersonAddress a ON p.id = a.person_id
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithPlainGenerator, connection, emptyList())

        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code with plain property name generator:")
        println(generatedCode)

        assertTrue(generatedCode.contains("person_id"), "Generated code should use original name 'person_id'")
        assertTrue(generatedCode.contains("first_name"), "Generated code should use original name 'first_name'")
        assertTrue(generatedCode.contains("last_name"), "Generated code should use original name 'last_name'")
        assertTrue(generatedCode.contains("address_id"), "Generated code should use original name 'address_id'")
        assertTrue(generatedCode.contains("address_street"), "Generated code should use original name 'address_street'")
    }

    @Test
    @DisplayName("Test propertyType annotation")
    fun testPropertyTypeAnnotation() {
        val sqlWithPropertyTypes = """
            -- @@className=PersonWithCustomTypes
            SELECT
                p.id AS person_id,

                -- @@field=first_name @@propertyType=String
                p.first_name,

                -- @@field=last_name @@propertyType=String?
                p.last_name,

                -- @@field=email @@propertyType=EmailAddress
                p.email,

                -- @@field=birth_date @@propertyType=java.time.LocalDate
                p.birth_date,

                -- @@field=created_at @@propertyType=java.time.Instant @@nonNull
                p.created_at
            FROM Person p
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithPropertyTypes, connection, emptyList())

        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code with propertyType annotations:")
        println(generatedCode)

        assertTrue(generatedCode.contains("val firstName: String?"), "Generated code should use String type for firstName")
        assertTrue(generatedCode.contains("val lastName: String") || generatedCode.contains("val lastName: `String?`"), "Generated code should use String type for lastName")
        assertTrue(generatedCode.contains("val email: EmailAddress?"), "Generated code should use EmailAddress type for email")
        assertTrue(generatedCode.contains("val birthDate: LocalDate?"), "Generated code should use LocalDate type for birthDate")
        assertTrue(generatedCode.contains("val createdAt: Instant"), "Generated code should use non-nullable Instant type for createdAt")
    }

    @Test
    @DisplayName("Test propertyType with nullable annotation")
    fun testPropertyTypeWithNullableAnnotation() {
        val sqlWithPropertyTypesAndNullable = """
            SELECT
                p.id AS person_id,

                -- @@field=first_name @@propertyType=String @@nullable
                p.first_name,

                -- @@field=email @@propertyType=EmailAddress @@nonNull
                p.email
            FROM Person p
            WHERE p.id = 1;
        """.trimIndent()

        val codeGenerator = StatementKotlinCodeGenerator(sqlWithPropertyTypesAndNullable, connection, emptyList())

        val fileSpecBuilder = codeGenerator.generateDataStructureCode("PersonWithAddress")
        val generatedCode = fileSpecBuilder.build().toString()

        println("\nGenerated code with propertyType and nullable annotations:")
        println(generatedCode)

        assertTrue(generatedCode.contains("val firstName: String?"), "Generated code should use nullable String type for firstName")
        assertTrue(generatedCode.contains("val email: EmailAddress"), "Generated code should use non-nullable EmailAddress type for email")
    }

    @Test
    @DisplayName("Test unsupported property name generator")
    fun testUnsupportedPropertyNameGenerator() {
        val sqlWithUnsupportedGenerator = """
            -- @@className=PersonWithAddress @@propertyNameGenerator=upperCamelCase
            SELECT
                p.id AS person_id,
                p.first_name, p.last_name, p.email
            FROM Person p
            WHERE p.id = 1;
        """.trimIndent()

        val exception = assertFailsWith<IllegalArgumentException> {
            StatementKotlinCodeGenerator(sqlWithUnsupportedGenerator, connection, emptyList())
        }

        println("\nException message:")
        println(exception.message)

        assertTrue(exception.message!!.contains("Unsupported propertyNameGenerator value: 'upperCamelCase'"),
                   "Exception should mention the unsupported value")
        assertTrue(exception.message!!.contains("Supported values are 'plain' and 'lowerCamelCase'"),
                   "Exception should list the supported values")
    }
}
