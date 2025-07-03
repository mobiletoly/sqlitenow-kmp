package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SelectFieldCodeGeneratorTest {

    @Test
    @DisplayName("Test generating property for a field with SQLite INTEGER type")
    fun testGeneratePropertyForIntegerField() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("user_id")
        `when`(mockField.dataType).thenReturn("INTEGER")
        `when`(mockField.tableName).thenReturn("")
        `when`(mockField.originalColumnName).thenReturn("user_id")

        // Create a FieldAnnotationOverrides
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a SelectFieldCodeGenerator
        val generator = SelectFieldCodeGenerator()

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("userId", property.name)
        assertEquals("kotlin.Long?", property.type.toString())
        assertTrue(property.type.isNullable)
    }

    @Test
    @DisplayName("Test generating property for a field with SQLite TEXT type")
    fun testGeneratePropertyForTextField() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("username")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("")
        `when`(mockField.originalColumnName).thenReturn("username")

        // Create a FieldAnnotationOverrides
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a SelectFieldCodeGenerator
        val generator = SelectFieldCodeGenerator()

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("username", property.name)
        assertEquals("kotlin.String?", property.type.toString())
        assertTrue(property.type.isNullable)
    }

    @Test
    @DisplayName("Test generating property for a field with non-null annotation")
    fun testGeneratePropertyForNonNullField() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("email")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("")
        `when`(mockField.originalColumnName).thenReturn("email")

        // Create a FieldAnnotationOverrides with nonNull=true
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = true,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a SelectFieldCodeGenerator
        val generator = SelectFieldCodeGenerator()

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("email", property.name)
        assertEquals("kotlin.String", property.type.toString())
        assertFalse(property.type.isNullable)
    }

    @Test
    @DisplayName("Test generating property for a field with NOT NULL constraint from schema")
    fun testGeneratePropertyForNotNullConstraint() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("username")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("users")
        `when`(mockField.originalColumnName).thenReturn("username")

        // Create a FieldAnnotationOverrides with no explicit nullability
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a mock CreateTableStatement.Column
        val mockColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockColumn.name).thenReturn("username")
        `when`(mockColumn.dataType).thenReturn("TEXT")
        `when`(mockColumn.notNull).thenReturn(true)
        `when`(mockColumn.primaryKey).thenReturn(false)
        `when`(mockColumn.autoIncrement).thenReturn(false)
        `when`(mockColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement
        val mockCreateTable = mock(CreateTableStatement::class.java)
        `when`(mockCreateTable.tableName).thenReturn("users")
        `when`(mockCreateTable.columns).thenReturn(listOf(mockColumn))

        // Create a mock AnnotatedCreateTableStatement
        val mockAnnotatedColumn = AnnotatedCreateTableStatement.Column(
            src = mockColumn,
            annotations = emptyMap()
        )

        val mockAnnotatedCreateTable = AnnotatedCreateTableStatement(
            name = "users",
            src = mockCreateTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedColumn)
        )

        // Create a SelectFieldCodeGenerator with the mock schema
        val generator = SelectFieldCodeGenerator(listOf(mockAnnotatedCreateTable))

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("username", property.name)
        assertEquals("kotlin.String", property.type.toString())
        assertFalse(property.type.isNullable, "Property should be non-nullable due to NOT NULL constraint in schema")
    }

    @Test
    @DisplayName("Test generating property for a field from a joined table")
    fun testGeneratePropertyForJoinedTable() {
        // Create a mock SelectStatement.FieldSource for a field from a joined table
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("address")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("") // No tableName specified, simulating a field from a JOIN without explicit table name
        `when`(mockField.originalColumnName).thenReturn("address")

        // Create a FieldAnnotationOverrides with no explicit nullability
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create mock columns for the users table
        val mockUserColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockUserColumn.name).thenReturn("username")
        `when`(mockUserColumn.dataType).thenReturn("TEXT")
        `when`(mockUserColumn.notNull).thenReturn(true)
        `when`(mockUserColumn.primaryKey).thenReturn(false)
        `when`(mockUserColumn.autoIncrement).thenReturn(false)
        `when`(mockUserColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement for users
        val mockUserTable = mock(CreateTableStatement::class.java)
        `when`(mockUserTable.tableName).thenReturn("users")
        `when`(mockUserTable.columns).thenReturn(listOf(mockUserColumn))

        // Create mock columns for the addresses table
        val mockAddressColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockAddressColumn.name).thenReturn("address")
        `when`(mockAddressColumn.dataType).thenReturn("TEXT")
        `when`(mockAddressColumn.notNull).thenReturn(true)
        `when`(mockAddressColumn.primaryKey).thenReturn(false)
        `when`(mockAddressColumn.autoIncrement).thenReturn(false)
        `when`(mockAddressColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement for addresses
        val mockAddressTable = mock(CreateTableStatement::class.java)
        `when`(mockAddressTable.tableName).thenReturn("addresses")
        `when`(mockAddressTable.columns).thenReturn(listOf(mockAddressColumn))

        // Create mock AnnotatedCreateTableStatement objects
        val mockAnnotatedUserColumn = AnnotatedCreateTableStatement.Column(
            src = mockUserColumn,
            annotations = emptyMap()
        )

        val mockAnnotatedUserTable = AnnotatedCreateTableStatement(
            name = "users",
            src = mockUserTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedUserColumn)
        )

        val mockAnnotatedAddressColumn = AnnotatedCreateTableStatement.Column(
            src = mockAddressColumn,
            annotations = emptyMap()
        )

        val mockAnnotatedAddressTable = AnnotatedCreateTableStatement(
            name = "addresses",
            src = mockAddressTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedAddressColumn)
        )

        // Create a SelectFieldCodeGenerator with both tables in the schema
        val generator = SelectFieldCodeGenerator(listOf(mockAnnotatedUserTable, mockAnnotatedAddressTable))

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("address", property.name)
        assertEquals("kotlin.String", property.type.toString())
        assertFalse(property.type.isNullable, "Property should be non-nullable due to NOT NULL constraint in joined table schema")
    }

    @Test
    @DisplayName("Test generating property for an aliased column in a JOIN query")
    fun testGeneratePropertyForAliasedColumn() {
        // Create a mock SelectStatement.FieldSource for an aliased column
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("address_id") // Using the alias name
        `when`(mockField.dataType).thenReturn("INTEGER")
        `when`(mockField.tableName).thenReturn("a") // Using the table alias
        `when`(mockField.originalColumnName).thenReturn("id") // The original column name

        // Create a FieldAnnotationOverrides with a custom property name
        val annotations = FieldAnnotationOverrides(
            propertyName = "myAddressId",
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a mock column for the PersonAddress table
        val mockIdColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockIdColumn.name).thenReturn("id")
        `when`(mockIdColumn.dataType).thenReturn("INTEGER")
        `when`(mockIdColumn.notNull).thenReturn(true)
        `when`(mockIdColumn.primaryKey).thenReturn(false)
        `when`(mockIdColumn.autoIncrement).thenReturn(false)
        `when`(mockIdColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement for PersonAddress
        val mockPersonAddressTable = mock(CreateTableStatement::class.java)
        `when`(mockPersonAddressTable.tableName).thenReturn("PersonAddress")
        `when`(mockPersonAddressTable.columns).thenReturn(listOf(mockIdColumn))

        // Create a mock AnnotatedCreateTableStatement
        val mockAnnotatedIdColumn = AnnotatedCreateTableStatement.Column(
            src = mockIdColumn,
            annotations = emptyMap()
        )

        val mockAnnotatedPersonAddressTable = AnnotatedCreateTableStatement(
            name = "PersonAddress",
            src = mockPersonAddressTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedIdColumn)
        )

        // Create a SelectFieldCodeGenerator with the mock schema
        val generator = SelectFieldCodeGenerator(listOf(mockAnnotatedPersonAddressTable))

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("myAddressId", property.name)
        assertEquals("kotlin.Long", property.type.toString())
        assertFalse(property.type.isNullable, "Property should be non-nullable due to NOT NULL constraint in original column")
    }

    @Test
    @DisplayName("Test generating property for a field with custom property name")
    fun testGeneratePropertyWithCustomName() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("user_full_name")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("")
        `when`(mockField.originalColumnName).thenReturn("user_full_name")

        // Create a FieldAnnotationOverrides with custom property name
        val annotations = FieldAnnotationOverrides(
            propertyName = "fullName",
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a SelectFieldCodeGenerator
        val generator = SelectFieldCodeGenerator()

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("fullName", property.name)
        assertEquals("kotlin.String?", property.type.toString())
        assertTrue(property.type.isNullable)
    }

    @Test
    @DisplayName("Test generating property for a field with custom property type")
    fun testGeneratePropertyWithCustomType() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("created_at")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("")
        `when`(mockField.originalColumnName).thenReturn("created_at")

        // Create a FieldAnnotationOverrides with custom property type
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = "java.time.LocalDateTime",
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a SelectFieldCodeGenerator
        val generator = SelectFieldCodeGenerator()

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("createdAt", property.name)
        assertEquals("java.time.LocalDateTime?", property.type.toString())
        assertTrue(property.type.isNullable)
    }

    @Test
    @DisplayName("Test that CREATE TABLE annotations override schema constraints")
    fun testCreateTableAnnotationsOverrideSchemaConstraints() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("id")
        `when`(mockField.dataType).thenReturn("INTEGER")
        `when`(mockField.tableName).thenReturn("person")
        `when`(mockField.originalColumnName).thenReturn("id")

        // Create a FieldAnnotationOverrides with no explicit nullability
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a mock column for the Person table with NOT NULL constraint but @@nullable annotation
        val mockColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockColumn.name).thenReturn("id")
        `when`(mockColumn.dataType).thenReturn("INTEGER")
        `when`(mockColumn.notNull).thenReturn(true) // NOT NULL constraint
        `when`(mockColumn.primaryKey).thenReturn(true)
        `when`(mockColumn.autoIncrement).thenReturn(false)
        `when`(mockColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement
        val mockCreateTable = mock(CreateTableStatement::class.java)
        `when`(mockCreateTable.tableName).thenReturn("person")
        `when`(mockCreateTable.columns).thenReturn(listOf(mockColumn))

        // Create a mock AnnotatedCreateTableStatement with @@nullable annotation
        val mockAnnotatedColumn = AnnotatedCreateTableStatement.Column(
            src = mockColumn,
            annotations = mapOf(AnnotationConstants.NULLABLE to null) // @@nullable annotation
        )

        val mockAnnotatedCreateTable = AnnotatedCreateTableStatement(
            name = "person",
            src = mockCreateTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedColumn)
        )

        // Create a SelectFieldCodeGenerator with the mock schema
        val generator = SelectFieldCodeGenerator(listOf(mockAnnotatedCreateTable))

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("id", property.name)
        assertEquals("kotlin.Long?", property.type.toString())
        assertTrue(property.type.isNullable, "Property should be nullable due to @@nullable annotation in CREATE TABLE despite NOT NULL constraint")
    }

    @Test
    @DisplayName("Test that CREATE TABLE propertyType annotation is respected")
    fun testCreateTablePropertyTypeAnnotation() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("created_at")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("person")
        `when`(mockField.originalColumnName).thenReturn("created_at")

        // Create a FieldAnnotationOverrides with no explicit property type
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a mock column for the Person table with @@propertyType annotation
        val mockColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockColumn.name).thenReturn("created_at")
        `when`(mockColumn.dataType).thenReturn("TEXT")
        `when`(mockColumn.notNull).thenReturn(false)
        `when`(mockColumn.primaryKey).thenReturn(false)
        `when`(mockColumn.autoIncrement).thenReturn(false)
        `when`(mockColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement
        val mockCreateTable = mock(CreateTableStatement::class.java)
        `when`(mockCreateTable.tableName).thenReturn("person")
        `when`(mockCreateTable.columns).thenReturn(listOf(mockColumn))

        // Create a mock AnnotatedCreateTableStatement with @@propertyType annotation
        val mockAnnotatedColumn = AnnotatedCreateTableStatement.Column(
            src = mockColumn,
            annotations = mapOf(AnnotationConstants.PROPERTY_TYPE to "java.time.LocalDateTime") // @@propertyType=java.time.LocalDateTime annotation
        )

        val mockAnnotatedCreateTable = AnnotatedCreateTableStatement(
            name = "person",
            src = mockCreateTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedColumn)
        )

        // Create a SelectFieldCodeGenerator with the mock schema
        val generator = SelectFieldCodeGenerator(listOf(mockAnnotatedCreateTable))

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("createdAt", property.name)
        assertEquals("java.time.LocalDateTime?", property.type.toString())
        assertTrue(property.type.isNullable, "Property should be nullable by default")
    }

    @Test
    @DisplayName("Test that SELECT statement propertyType annotation overrides CREATE TABLE propertyType annotation")
    fun testSelectStatementPropertyTypeOverrideCreateTablePropertyType() {
        // Create a mock SelectStatement.FieldSource
        val mockField = mock(SelectStatement.FieldSource::class.java)
        `when`(mockField.fieldName).thenReturn("created_at")
        `when`(mockField.dataType).thenReturn("TEXT")
        `when`(mockField.tableName).thenReturn("person")
        `when`(mockField.originalColumnName).thenReturn("created_at")

        // Create a FieldAnnotationOverrides with explicit property type
        val annotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = "java.time.ZonedDateTime", // This should override the CREATE TABLE annotation
            nonNull = null,
            nullable = null,
            adapter = false
        )

        // Create an AnnotatedSelectStatement.Field
        val field = AnnotatedSelectStatement.Field(
            src = mockField,
            annotations = annotations
        )

        // Create a mock column for the Person table with @@propertyType annotation
        val mockColumn = mock(CreateTableStatement.Column::class.java)
        `when`(mockColumn.name).thenReturn("created_at")
        `when`(mockColumn.dataType).thenReturn("TEXT")
        `when`(mockColumn.notNull).thenReturn(false)
        `when`(mockColumn.primaryKey).thenReturn(false)
        `when`(mockColumn.autoIncrement).thenReturn(false)
        `when`(mockColumn.unique).thenReturn(false)

        // Create a mock CreateTableStatement
        val mockCreateTable = mock(CreateTableStatement::class.java)
        `when`(mockCreateTable.tableName).thenReturn("person")
        `when`(mockCreateTable.columns).thenReturn(listOf(mockColumn))

        // Create a mock AnnotatedCreateTableStatement with @@propertyType annotation
        val mockAnnotatedColumn = AnnotatedCreateTableStatement.Column(
            src = mockColumn,
            annotations = mapOf(AnnotationConstants.PROPERTY_TYPE to "java.time.LocalDateTime") // @@propertyType=java.time.LocalDateTime annotation
        )

        val mockAnnotatedCreateTable = AnnotatedCreateTableStatement(
            name = "person",
            src = mockCreateTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            columns = listOf(mockAnnotatedColumn)
        )

        // Create a SelectFieldCodeGenerator with the mock schema
        val generator = SelectFieldCodeGenerator(listOf(mockAnnotatedCreateTable))

        // Generate a property for the field
        val property = generator.generateProperty(field)

        // Verify the property name and type
        assertEquals("createdAt", property.name)
        assertEquals("java.time.ZonedDateTime?", property.type.toString())
        assertTrue(property.type.isNullable, "Property should be nullable by default")
    }
}
