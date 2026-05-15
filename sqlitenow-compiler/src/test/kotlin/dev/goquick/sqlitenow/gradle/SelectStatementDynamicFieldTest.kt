package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SelectStatementDynamicFieldTest {

    @Test
    fun `mappingType creates dynamic field`() {
        listOf(
            DynamicMappingCase(
                name = "SelectWithPerRowMapping",
                mappingType = "perRow",
                propertyType = "Address",
            ),
            DynamicMappingCase(
                name = "SelectWithCollectionMapping",
                sql = PERSON_ADDRESS_WITH_PERSON_ID_SQL,
                personIdFieldName = "person_id",
                statementCollectionKey = "person_id",
                mappingType = "collection",
                propertyType = "List<Address>",
                fieldCollectionKey = "address_id",
            ),
        ).forEach { case ->
            val statement = annotatedSelectStatementWithFieldAnnotations(
                name = case.name,
                sql = case.sql,
                fromTable = "person",
                joinTables = listOf("Address"),
                sources = personAddressSources(personIdFieldName = case.personIdFieldName),
                collectionKey = case.statementCollectionKey,
                fieldAnnotations = mapOf(
                    "address_id" to addressDynamicAnnotations(
                        mappingType = case.mappingType,
                        propertyType = case.propertyType,
                        collectionKey = case.fieldCollectionKey,
                    )
                )
            )

            assertNotNull(statement)
            val dynamicField = statement.fields.find { it.annotations.isDynamicField }
            assertNotNull(dynamicField)
            assertEquals(case.mappingType, dynamicField.annotations.mappingType)
            assertEquals(case.propertyType, dynamicField.annotations.propertyType)
            assertEquals("a", dynamicField.annotations.sourceTable)
            assertTrue(dynamicField.annotations.isDynamicField)
        }
    }

    @Test
    fun `optional dynamic field annotations are parsed`() {
        listOf(
            OptionalDynamicFieldCase(
                name = "SelectWithAliasPrefix",
                sql = "SELECT p.id, p.name, a.id AS address_id, a.street AS address_street FROM Person p LEFT JOIN Address a ON p.id = a.person_id",
                aliasPrefix = "address_",
                includeStreetField = true,
            ),
            OptionalDynamicFieldCase(
                name = "SelectWithNotNullDynamicField",
                sql = "SELECT p.id, p.name, a.id AS address_id FROM Person p INNER JOIN Address a ON p.id = a.person_id",
                notNull = true,
            ),
        ).forEach { case ->
            val fieldAnnotations = buildMap {
                put(
                    "address_id",
                    addressDynamicAnnotations(
                        mappingType = "perRow",
                        propertyType = "Address",
                        aliasPrefix = case.aliasPrefix,
                        notNull = case.notNull,
                    )
                )
                if (case.includeStreetField) {
                    put(
                        "address_street",
                        mapOf(AnnotationConstants.ALIAS_PREFIX to case.aliasPrefix)
                    )
                }
            }

            val statement = annotatedSelectStatementWithFieldAnnotations(
                name = case.name,
                sql = case.sql,
                fromTable = "person",
                joinTables = listOf("Address"),
                sources = personAddressSources() + if (case.includeStreetField) {
                    listOf(fieldSource("address_street", "a", "street"))
                } else {
                    emptyList()
                },
                fieldAnnotations = fieldAnnotations,
            )

            assertNotNull(statement)
            val dynamicField = statement.fields.find { it.annotations.isDynamicField }
            assertNotNull(dynamicField)
            assertEquals(case.aliasPrefix, dynamicField.annotations.aliasPrefix)
            assertEquals(case.notNull, dynamicField.annotations.notNull)
        }
    }

    @Test
    fun `multiple dynamic fields with different mappingTypes`() {
        val statement = annotatedSelectStatementWithFieldAnnotations(
            name = "SelectWithMultipleDynamicFields",
            sql = "SELECT p.id AS person_id, p.name, a.id AS address_id, c.id AS comment_id FROM person p LEFT JOIN address a ON p.id = a.person_id LEFT JOIN comment c ON p.id = c.person_id",
            fromTable = "person",
            joinTables = listOf("address", "comment"),
            sources = personAddressSources(personIdFieldName = "person_id") +
                    fieldSource("comment_id", "c", "id", "INTEGER"),
            collectionKey = "person_id",
            fieldAnnotations = mapOf(
                "address_id" to addressDynamicAnnotations(
                    mappingType = "perRow",
                    propertyType = "Address",
                    propertyName = "primaryAddress",
                ),
                "comment_id" to buildMap {
                    put(AnnotationConstants.IS_DYNAMIC_FIELD, true)
                    put(AnnotationConstants.PROPERTY_NAME, "comments")
                    put(AnnotationConstants.PROPERTY_TYPE, "List<Comment>")
                    put(AnnotationConstants.MAPPING_TYPE, "collection")
                    put(AnnotationConstants.SOURCE_TABLE, "c")
                    put(AnnotationConstants.COLLECTION_KEY, "comment_id")
                }
            ),
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

    private fun personAddressSources(personIdFieldName: String = "id") = listOf(
        fieldSource(personIdFieldName, "p", "id", "INTEGER"),
        fieldSource("name", "p"),
        fieldSource("address_id", "a", "id", "INTEGER"),
    )

    private fun addressDynamicAnnotations(
        mappingType: String,
        propertyType: String,
        propertyName: String? = null,
        collectionKey: String? = null,
        aliasPrefix: String? = null,
        notNull: Boolean? = null,
    ) = buildMap {
        put(AnnotationConstants.IS_DYNAMIC_FIELD, true)
        propertyName?.let { put(AnnotationConstants.PROPERTY_NAME, it) }
        put(AnnotationConstants.PROPERTY_TYPE, propertyType)
        put(AnnotationConstants.MAPPING_TYPE, mappingType)
        put(AnnotationConstants.SOURCE_TABLE, "a")
        collectionKey?.let { put(AnnotationConstants.COLLECTION_KEY, it) }
        aliasPrefix?.let { put(AnnotationConstants.ALIAS_PREFIX, it) }
        notNull?.let { put(AnnotationConstants.NOT_NULL, it) }
    }

    private data class DynamicMappingCase(
        val name: String,
        val sql: String = PERSON_ADDRESS_SQL,
        val personIdFieldName: String = "id",
        val statementCollectionKey: String? = null,
        val mappingType: String,
        val propertyType: String,
        val fieldCollectionKey: String? = null,
    )

    private data class OptionalDynamicFieldCase(
        val name: String,
        val sql: String,
        val aliasPrefix: String? = null,
        val notNull: Boolean? = null,
        val includeStreetField: Boolean = false,
    )

    private companion object {
        const val PERSON_ADDRESS_SQL =
            "SELECT p.id, p.name, a.id AS address_id FROM Person p LEFT JOIN Address a ON p.id = a.person_id"
        const val PERSON_ADDRESS_WITH_PERSON_ID_SQL =
            "SELECT p.id AS person_id, p.name, a.id AS address_id FROM Person p LEFT JOIN Address a ON p.id = a.person_id"
    }
}
