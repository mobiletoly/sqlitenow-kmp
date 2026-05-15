package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SelectStatementCollectionKeyTest {

    @Test
    fun `statement-level collectionKey is parsed correctly`() {
        listOf("person_id", "p.id").forEach { collectionKey ->
            val statement = annotatedSelectStatementWithFieldAnnotations(
                name = "SelectWithCollectionKey",
                sql = "SELECT p.id AS person_id, p.name FROM Person p",
                fromTable = "person",
                sources = listOf(
                    fieldSource("person_id", "p", "id", "INTEGER"),
                    fieldSource("name", "p")
                ),
                collectionKey = collectionKey,
            )

            assertNotNull(statement)
            assertEquals(collectionKey, statement.annotations.collectionKey)
        }
    }

    @Test
    fun `field-level collectionKey for collection mapping`() {
        listOf(
            CollectionKeyCase(collectionKey = "address_id"),
            CollectionKeyCase(collectionKey = "a.id"),
            CollectionKeyCase(collectionKey = "address_id", aliasPrefix = "address_"),
        ).forEach { case ->
            val statement = annotatedSelectStatementWithFieldAnnotations(
                name = "SelectWithCollectionField",
                sql = PERSON_ADDRESS_SQL,
                fromTable = "person",
                joinTables = listOf("Address"),
                sources = personAddressSources(),
                collectionKey = "id",
                fieldAnnotations = mapOf(
                    "address_id" to addressCollectionAnnotations(
                        collectionKey = case.collectionKey,
                        aliasPrefix = case.aliasPrefix,
                    )
                ),
            )

            assertNotNull(statement)
            val addressField = statement.fields.find { it.annotations.isDynamicField }
            assertNotNull(addressField)
            assertEquals("collection", addressField.annotations.mappingType)
            assertEquals(case.collectionKey, addressField.annotations.collectionKey)
            assertEquals(case.aliasPrefix, addressField.annotations.aliasPrefix)
        }
    }

    private fun personAddressSources() = listOf(
        fieldSource("id", "p", "id", "INTEGER"),
        fieldSource("name", "p"),
        fieldSource("address_id", "a", "id", "INTEGER"),
    )

    private fun addressCollectionAnnotations(
        collectionKey: String,
        aliasPrefix: String? = null,
    ) = buildMap {
        put(AnnotationConstants.IS_DYNAMIC_FIELD, true)
        put(AnnotationConstants.PROPERTY_TYPE, "List<Address>")
        put(AnnotationConstants.MAPPING_TYPE, "collection")
        put(AnnotationConstants.SOURCE_TABLE, "a")
        put(AnnotationConstants.COLLECTION_KEY, collectionKey)
        aliasPrefix?.let { put(AnnotationConstants.ALIAS_PREFIX, it) }
    }

    private data class CollectionKeyCase(
        val collectionKey: String,
        val aliasPrefix: String? = null,
    )

    private companion object {
        const val PERSON_ADDRESS_SQL =
            "SELECT p.id, p.name, a.id AS address_id FROM Person p LEFT JOIN Address a ON p.id = a.person_id"
    }
}
