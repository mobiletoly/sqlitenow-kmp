package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DynamicFieldUtilsTest {

    @Test
    fun `computeSkipSet skips dynamic descendants beneath collection aliases`() {
        val fields = listOf(
            dynamicField(
                fieldName = "packages",
                mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
                propertyType = "kotlin.collections.List<PackageDoc>",
                sourceTable = "pkg",
                aliasPath = listOf("pkg"),
            ),
            dynamicField(
                fieldName = "packageCategory",
                mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                propertyType = "CategoryDoc",
                sourceTable = "cat",
                aliasPath = listOf("pkg", "cat"),
            ),
            dynamicField(
                fieldName = "categoryOwner",
                mappingType = AnnotationConstants.MAPPING_TYPE_ENTITY,
                propertyType = "OwnerDoc",
                sourceTable = "owner",
                aliasPath = listOf("pkg", "cat", "owner"),
            ),
            dynamicField(
                fieldName = "auditInfo",
                mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                propertyType = "AuditDoc",
                sourceTable = "audit",
                aliasPath = listOf("audit"),
            ),
        )

        assertEquals(setOf("packageCategory", "categoryOwner"), DynamicFieldUtils.computeSkipSet(fields))
    }

    @Test
    fun `isNestedAlias only matches later underscore bounded prefixes`() {
        assertTrue(
            DynamicFieldUtils.isNestedAlias(
                fieldName = "joined__bundle__package__category__title",
                aliasPrefix = "package__category__",
            ),
        )
        assertFalse(
            DynamicFieldUtils.isNestedAlias(
                fieldName = "package__category__title",
                aliasPrefix = "package__category__",
            ),
        )
        assertFalse(
            DynamicFieldUtils.isNestedAlias(
                fieldName = "joinedPackageCategoryTitle",
                aliasPrefix = "package__category__",
            ),
        )
        assertFalse(
            DynamicFieldUtils.isNestedAlias(
                fieldName = "joined_pkg_title",
                aliasPrefix = "joined_pkg_",
            ),
        )
    }
}
