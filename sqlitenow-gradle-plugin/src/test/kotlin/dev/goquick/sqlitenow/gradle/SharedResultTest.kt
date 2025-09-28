package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

class SharedResultTest {

    @Test
    @DisplayName("Test SharedResultManager registers and validates shared results")
    fun testSharedResultManagerBasicFunctionality() {
        val sharedResultManager = SharedResultManager()

        // Create a SELECT statement with queryResult=All
        val statement1 = AnnotatedSelectStatement(
            name = "SelectAllPaginated",
            src = SelectStatement(
                sql = "SELECT * FROM Person LIMIT :limit OFFSET :offset",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = listOf("limit", "offset"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = "offset",
                limitNamedParam = "limit",
                fields = listOf(
                    SelectStatement.FieldSource(
                        fieldName = "id",
                        tableName = "person",
                        originalColumnName = "id",
                        dataType = "INTEGER"
                    ),
                    SelectStatement.FieldSource(
                        fieldName = "name",
                        tableName = "person",
                        originalColumnName = "name",
                        dataType = "TEXT"
                    )
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "id",
                        tableName = "person",
                        originalColumnName = "id",
                        dataType = "INTEGER"
                    ),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "name",
                        tableName = "person",
                        originalColumnName = "name",
                        dataType = "TEXT"
                    ),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )

        // Register the shared result
        val sharedResult = sharedResultManager.registerSharedResult(statement1, "person")

        // Verify the shared result was created
        assertNotNull(sharedResult)
        assertEquals("All", sharedResult!!.name)
        assertEquals("person", sharedResult.namespace)
        assertEquals(2, sharedResult.fields.size)

        // Verify isSharedResult works
        assertTrue(sharedResultManager.isSharedResult(statement1))

        // Verify getSharedResult works
        val retrievedSharedResult = sharedResultManager.getSharedResult(statement1, "person")
        assertNotNull(retrievedSharedResult)
        assertEquals(sharedResult, retrievedSharedResult)
    }

    @Test
    @DisplayName("Test SharedResultManager validates field structure consistency")
    fun testSharedResultStructureValidation() {
        val sharedResultManager = SharedResultManager()

        // Create first statement with queryResult=All
        val statement1 = AnnotatedSelectStatement(
            name = "SelectAllPaginated",
            src = SelectStatement(
                sql = "SELECT id, name FROM Person LIMIT :limit",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = listOf("limit"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = "limit",
                fields = listOf(
                    SelectStatement.FieldSource("id", "person", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "person", "name", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "person", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "person", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )

        // Create second statement with same queryResult=All but different structure
        val statement2 = AnnotatedSelectStatement(
            name = "SelectAllFiltered",
            src = SelectStatement(
                sql = "SELECT id, email FROM Person WHERE active = 1",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "person", "id", "INTEGER"),
                    SelectStatement.FieldSource("email", "person", "email", "TEXT") // Different field!
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "person", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("email", "person", "email", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )

        // Register first statement - should succeed
        val sharedResult1 = sharedResultManager.registerSharedResult(statement1, "person")
        assertNotNull(sharedResult1)

        // Register second statement with different structure - should throw exception
        assertThrows(IllegalArgumentException::class.java) {
            sharedResultManager.registerSharedResult(statement2, "person")
        }
    }

    @Test
    @DisplayName("Test SharedResultManager groups results by namespace")
    fun testSharedResultsByNamespace() {
        val sharedResultManager = SharedResultManager()

        // Create statements in different namespaces
        val personStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = SelectStatement(
                sql = "SELECT * FROM Person",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        val orderStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = SelectStatement(
                sql = "SELECT * FROM Order",
                fromTable = "Order",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "All",
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Register shared results
        sharedResultManager.registerSharedResult(personStatement, "person")
        sharedResultManager.registerSharedResult(orderStatement, "order")

        // Get results by namespace
        val resultsByNamespace = sharedResultManager.getSharedResultsByNamespace()

        assertEquals(2, resultsByNamespace.size)
        assertTrue(resultsByNamespace.containsKey("person"))
        assertTrue(resultsByNamespace.containsKey("order"))
        assertEquals(1, resultsByNamespace["person"]!!.size)
        assertEquals(1, resultsByNamespace["order"]!!.size)
    }

    @Test
    @DisplayName("Test SharedResult with dynamic fields generates correctly")
    fun testSharedResultWithDynamicFields() {
        val sharedResultManager = SharedResultManager()

        // Create a statement with both regular fields and dynamic fields
        val statement = AnnotatedSelectStatement(
            name = "SelectWithDynamicField",
            src = SelectStatement(
                sql = "SELECT id, name FROM Person",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "person", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "person", "name", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "PersonWithExtras",
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = listOf(
                // Regular database field
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "Person", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                // Regular database field
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "person", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                // Dynamic field
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("addresses", "", "addresses", "DYNAMIC"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_TYPE to "List<String>",
                            AnnotationConstants.DEFAULT_VALUE to "listOf()"
                        )
                    )
                )
            )
        )

        // Register the shared result
        val sharedResult = sharedResultManager.registerSharedResult(statement, "person")

        // Verify the shared result was created and includes all fields (regular + dynamic)
        assertNotNull(sharedResult)
        assertEquals("PersonWithExtras", sharedResult!!.name)
        assertEquals("person", sharedResult.namespace)
        assertEquals(3, sharedResult.fields.size, "Should include 2 regular fields + 1 dynamic field")

        // Verify that dynamic field is included in the fields list
        val dynamicField = sharedResult.fields.find { it.annotations.isDynamicField }
        assertNotNull(dynamicField, "Dynamic field should be included in shared result")
        assertEquals("addresses", dynamicField!!.src.fieldName)
        assertEquals("List<String>", dynamicField.annotations.propertyType)
        assertEquals("listOf()", dynamicField.annotations.defaultValue)
        assertTrue(dynamicField.annotations.isDynamicField)
    }
}
