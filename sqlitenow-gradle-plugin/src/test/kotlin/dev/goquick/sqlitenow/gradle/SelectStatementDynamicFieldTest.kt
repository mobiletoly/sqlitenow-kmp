package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SelectStatementDynamicFieldTest {

    @Test
    fun `mappingType perRow creates dynamic field`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithPerRowMapping",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.id AS address_id FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("address_id", "a", "id", "INTEGER")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_TYPE to "Address",
                            AnnotationConstants.MAPPING_TYPE to "perRow",
                            AnnotationConstants.SOURCE_TABLE to "a"
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val dynamicField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(dynamicField)
        assertEquals("perRow", dynamicField.annotations.mappingType)
        assertEquals("Address", dynamicField.annotations.propertyType)
        assertEquals("a", dynamicField.annotations.sourceTable)
        assertTrue(dynamicField.annotations.isDynamicField)
    }

    @Test
    fun `mappingType collection creates dynamic field`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithCollectionMapping",
            src = SelectStatement(
                sql = "SELECT p.id AS person_id, p.name, a.id AS address_id FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("address_id", "a", "id", "INTEGER")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = "person_id"
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_TYPE to "List<Address>",
                            AnnotationConstants.MAPPING_TYPE to "collection",
                            AnnotationConstants.SOURCE_TABLE to "a",
                            AnnotationConstants.COLLECTION_KEY to "address_id"
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val dynamicField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(dynamicField)
        assertEquals("collection", dynamicField.annotations.mappingType)
        assertEquals("List<Address>", dynamicField.annotations.propertyType)
        assertEquals("a", dynamicField.annotations.sourceTable)
        assertTrue(dynamicField.annotations.isDynamicField)
    }

    @Test
    fun `sourceTable is required for mappingType validation`() {
        // This test documents that sourceTable validation should happen during annotation parsing
        val exception = assertThrows<IllegalArgumentException> {
            FieldAnnotationOverrides.parse(mapOf(
                AnnotationConstants.IS_DYNAMIC_FIELD to true,
                AnnotationConstants.MAPPING_TYPE to "perRow",
                AnnotationConstants.PROPERTY_TYPE to "Address"
                // Missing sourceTable
            ))
        }
        
        assert(exception.message?.contains("sourceTable") == true)
        assert(exception.message?.contains("required") == true)
    }

    @Test
    fun `invalid mappingType throws exception`() {
        val exception = assertThrows<IllegalArgumentException> {
            FieldAnnotationOverrides.parse(mapOf(
                AnnotationConstants.IS_DYNAMIC_FIELD to true,
                AnnotationConstants.MAPPING_TYPE to "invalid",
                AnnotationConstants.PROPERTY_TYPE to "Address",
                AnnotationConstants.SOURCE_TABLE to "a"
            ))
        }
        
        assert(exception.message?.contains("Invalid") == true)
        assert(exception.message?.contains("mappingType") == true)
    }

    @Test
    fun `mappingType requires dynamicField validation`() {
        val exception = assertThrows<IllegalArgumentException> {
            FieldAnnotationOverrides.parse(mapOf(
                AnnotationConstants.MAPPING_TYPE to "perRow",
                AnnotationConstants.PROPERTY_TYPE to "Address",
                AnnotationConstants.SOURCE_TABLE to "a"
                // Missing IS_DYNAMIC_FIELD
            ))
        }
        
        assert(exception.message?.contains("dynamic fields") == true)
    }

    @Test
    fun `aliasPrefix works with dynamic fields`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithAliasPrefix",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.id AS address_id, a.street AS address_street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    SelectStatement.FieldSource("address_street", "a", "street", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_TYPE to "Address",
                            AnnotationConstants.MAPPING_TYPE to "perRow",
                            AnnotationConstants.SOURCE_TABLE to "a",
                            AnnotationConstants.ALIAS_PREFIX to "address_"
                        )
                    )
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("address_street", "a", "street", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.ALIAS_PREFIX to "address_"
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val dynamicField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(dynamicField)
        assertEquals("address_", dynamicField.annotations.aliasPrefix)
    }

    @Test
    fun `multiple dynamic fields with different mappingTypes`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithMultipleDynamicFields",
            src = SelectStatement(
                sql = "SELECT p.id AS person_id, p.name, a.id AS address_id, c.id AS comment_id FROM person p LEFT JOIN address a ON p.id = a.person_id LEFT JOIN comment c ON p.id = c.person_id",
                fromTable = "person",
                joinTables = listOf("address", "comment"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    SelectStatement.FieldSource("comment_id", "c", "id", "INTEGER")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = "person_id"
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_NAME to "primaryAddress",
                            AnnotationConstants.PROPERTY_TYPE to "Address",
                            AnnotationConstants.MAPPING_TYPE to "perRow",
                            AnnotationConstants.SOURCE_TABLE to "a"
                        )
                    )
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("comment_id", "c", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_NAME to "comments",
                            AnnotationConstants.PROPERTY_TYPE to "List<Comment>",
                            AnnotationConstants.MAPPING_TYPE to "collection",
                            AnnotationConstants.SOURCE_TABLE to "c",
                            AnnotationConstants.COLLECTION_KEY to "comment_id"
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val dynamicFields = statement.fields.filter { it.annotations.isDynamicField }
        assertEquals(2, dynamicFields.size)
        
        val addressField = dynamicFields.find { it.annotations.propertyName == "primaryAddress" }
        assertNotNull(addressField)
        assertEquals("perRow", addressField.annotations.mappingType)
        
        val commentField = dynamicFields.find { it.annotations.propertyName == "comments" }
        assertNotNull(commentField)
        assertEquals("collection", commentField.annotations.mappingType)
    }

    @Test
    fun `dynamic field with notNull annotation`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithNotNullDynamicField",
            src = SelectStatement(
                sql = "SELECT p.id, p.name, a.id AS address_id FROM Person p INNER JOIN Address a ON p.id = a.person_id",
                fromTable = "person",
                joinTables = listOf("Address"),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    SelectStatement.FieldSource("address_id", "a", "id", "INTEGER")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("address_id", "a", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(
                        mapOf(
                            AnnotationConstants.IS_DYNAMIC_FIELD to true,
                            AnnotationConstants.PROPERTY_TYPE to "Address",
                            AnnotationConstants.MAPPING_TYPE to "perRow",
                            AnnotationConstants.SOURCE_TABLE to "a",
                            AnnotationConstants.NOT_NULL to true
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val dynamicField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(dynamicField)
        assertEquals(true, dynamicField.annotations.notNull)
    }
}
