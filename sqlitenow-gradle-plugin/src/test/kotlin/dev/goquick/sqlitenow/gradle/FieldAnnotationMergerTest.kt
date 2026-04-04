package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationMerger
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class FieldAnnotationMergerTest {

    @Test
    fun `merge field annotations copies collection key and mapping metadata`() {
        val target = mutableMapOf<String, Any?>()

        FieldAnnotationMerger.mergeFieldAnnotations(
            target,
            FieldAnnotationOverrides(
                propertyName = "children",
                propertyType = "kotlin.collections.List<ChildDoc>",
                notNull = true,
                adapter = true,
                isDynamicField = true,
                aliasPrefix = "child__",
                mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
                sourceTable = "cdv",
                collectionKey = "child__id",
                sqlTypeHint = "TEXT",
            )
        )

        assertEquals("children", target[AnnotationConstants.PROPERTY_NAME])
        assertEquals("kotlin.collections.List<ChildDoc>", target[AnnotationConstants.PROPERTY_TYPE])
        assertEquals(AnnotationConstants.ADAPTER_CUSTOM, target[AnnotationConstants.ADAPTER])
        assertEquals(true, target[AnnotationConstants.NOT_NULL])
        assertEquals(true, target[AnnotationConstants.IS_DYNAMIC_FIELD])
        assertEquals("child__", target[AnnotationConstants.ALIAS_PREFIX])
        assertEquals(AnnotationConstants.MAPPING_TYPE_COLLECTION, target[AnnotationConstants.MAPPING_TYPE])
        assertEquals("cdv", target[AnnotationConstants.SOURCE_TABLE])
        assertEquals("child__id", target[AnnotationConstants.COLLECTION_KEY])
        assertEquals("TEXT", target[AnnotationConstants.SQL_TYPE_HINT])
    }

    @Test
    fun `merge field annotations leaves target unchanged for empty overrides`() {
        val target = mutableMapOf<String, Any?>(
            AnnotationConstants.PROPERTY_NAME to "existingName"
        )

        FieldAnnotationMerger.mergeFieldAnnotations(
            target,
            FieldAnnotationOverrides(
                propertyName = null,
                propertyType = null,
                notNull = null,
                adapter = null,
            )
        )

        assertEquals("existingName", target[AnnotationConstants.PROPERTY_NAME])
        assertTrue(AnnotationConstants.COLLECTION_KEY !in target)
    }
}
