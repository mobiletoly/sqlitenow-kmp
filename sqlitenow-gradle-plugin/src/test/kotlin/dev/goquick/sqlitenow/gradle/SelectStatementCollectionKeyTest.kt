package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SelectStatementCollectionKeyTest {

    @Test
    fun `statement-level collectionKey is parsed correctly`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithCollectionKey",
            src = SelectStatement(
                sql = "SELECT p.id AS person_id, p.name FROM Person p",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
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
                )
            )
        )
        
        assertNotNull(statement)
        assertEquals("person_id", statement.annotations.collectionKey)
    }

    @Test
    fun `statement-level collectionKey with alias dot column format`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithAliasCollectionKey",
            src = SelectStatement(
                sql = "SELECT p.id AS person_id, p.name FROM Person p",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    SelectStatement.FieldSource("name", "p", "name", "TEXT")
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = "p.id"
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("person_id", "p", "id", "INTEGER"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource("name", "p", "name", "TEXT"),
                    annotations = FieldAnnotationOverrides.parse(emptyMap())
                )
            )
        )
        
        assertNotNull(statement)
        assertEquals("p.id", statement.annotations.collectionKey)
    }

    @Test
    fun `field-level collectionKey for collection mapping`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithCollectionField",
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
                implements = null,
                excludeOverrideFields = null,
                collectionKey = "id"
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
        val addressField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(addressField)
        assertEquals("collection", addressField.annotations.mappingType)
        assertEquals("address_id", addressField.annotations.collectionKey)
    }

    @Test
    fun `field-level collectionKey with alias dot column format`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithAliasCollectionField",
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
                implements = null,
                excludeOverrideFields = null,
                collectionKey = "id"
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
                            AnnotationConstants.PROPERTY_TYPE to "List<Address>",
                            AnnotationConstants.MAPPING_TYPE to "collection",
                            AnnotationConstants.SOURCE_TABLE to "a",
                            AnnotationConstants.COLLECTION_KEY to "a.id"
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val addressField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(addressField)
        assertEquals("a.id", addressField.annotations.collectionKey)
    }

    @Test
    fun `collectionKey works with aliasPrefix`() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWithPrefixRemoval",
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
                implements = null,
                excludeOverrideFields = null,
                collectionKey = "id"
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
                            AnnotationConstants.PROPERTY_TYPE to "List<Address>",
                            AnnotationConstants.MAPPING_TYPE to "collection",
                            AnnotationConstants.SOURCE_TABLE to "a",
                            AnnotationConstants.COLLECTION_KEY to "address_id",
                            AnnotationConstants.ALIAS_PREFIX to "address_"
                        )
                    )
                )
            )
        )
        
        assertNotNull(statement)
        val addressField = statement.fields.find { it.annotations.isDynamicField }
        assertNotNull(addressField)
        assertEquals("address_id", addressField.annotations.collectionKey)
        assertEquals("address_", addressField.annotations.aliasPrefix)
    }
}
