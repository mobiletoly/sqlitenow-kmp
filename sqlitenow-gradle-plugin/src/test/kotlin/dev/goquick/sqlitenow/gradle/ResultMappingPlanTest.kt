package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.ResultMappingPlanner
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import kotlin.test.Test
import kotlin.test.assertEquals

class ResultMappingPlanTest {

    @Test
    fun `planner suppresses mapped columns and duplicate sqlite labels`() {
        val sources = listOf(
            fieldSource("person_id", "person", "id", "INTEGER"),
            fieldSource("pkg_doc_id", "package", "doc_id"),
            fieldSource("pkg_title", "package", "title"),
            fieldSource("pkg_doc_id:1", "package", "doc_id"),
        )
        val statement = annotatedSelectStatement(
            name = "PackageRows",
            sources = sources,
            regularFields = listOf(
                regularField("person_id", "person", "id", "INTEGER"),
                regularField("pkg_doc_id", "package", "doc_id"),
                regularField("pkg_title", "package", "title"),
                regularField("pkg_doc_id:1", "package", "doc_id"),
            ),
            dynamicFields = listOf(
                dynamicField(
                    fieldName = "packages",
                    mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
                    propertyType = "kotlin.collections.List<PackageDoc>",
                    sourceTable = "pkg",
                    aliasPrefix = "pkg_",
                    collectionKey = "pkg_doc_id",
                    aliasPath = listOf("pkg"),
                ),
            ),
            tableAliases = mapOf("p" to "person", "pkg" to "package"),
        )

        val plan = ResultMappingPlanner.create(statement.src, statement.fields)

        assertEquals(
            setOf("pkg_doc_id", "pkg_title", "pkg_doc_id:1"),
            plan.mappedColumns,
        )
        assertEquals(listOf("person_id"), plan.regularFields.map { it.src.fieldName })
        assertEquals(listOf("packages"), plan.includedDynamicEntries.map { it.field.src.fieldName })
        assertEquals(emptySet(), plan.skippedDynamicFieldNames)
    }

    @Test
    fun `planner pins regular and dynamic slices for nested aliases and suppressed fields`() {
        val sources = listOf(
            fieldSource("person_id", "person", "id", "INTEGER"),
            fieldSource("pkg_doc_id", "package", "doc_id"),
            fieldSource("pkg_title", "package", "title"),
            fieldSource("cat_id", "category", "id"),
            fieldSource("cat_title", "category", "title"),
            fieldSource("audit_code", "audit", "code"),
            fieldSource("legacy_code", "legacy", "code"),
            fieldSource("joined__cat__shadow", "", "shadow"),
        )
        val statement = annotatedSelectStatement(
            name = "NestedPackageRows",
            sources = sources,
            regularFields = listOf(
                regularField("person_id", "person", "id", "INTEGER"),
                regularField("pkg_doc_id", "package", "doc_id"),
                regularField("pkg_title", "package", "title"),
                regularField("cat_id", "category", "id"),
                regularField("cat_title", "category", "title"),
                regularField("audit_code", "audit", "code"),
                regularField("legacy_code", "legacy", "code"),
                regularField("joined__cat__shadow", "", "shadow"),
            ),
            dynamicFields = listOf(
                dynamicField(
                    fieldName = "packages",
                    mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
                    propertyType = "kotlin.collections.List<PackageDoc>",
                    sourceTable = "pkg",
                    aliasPrefix = "pkg_",
                    collectionKey = "pkg_doc_id",
                    aliasPath = listOf("pkg"),
                ),
                dynamicField(
                    fieldName = "packageCategory",
                    mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                    propertyType = "CategoryDoc",
                    sourceTable = "cat",
                    aliasPrefix = "cat_",
                    aliasPath = listOf("pkg", "cat"),
                ),
                dynamicField(
                    fieldName = "auditInfo",
                    mappingType = AnnotationConstants.MAPPING_TYPE_ENTITY,
                    propertyType = "AuditDoc",
                    sourceTable = "audit",
                    aliasPrefix = "audit_",
                    aliasPath = listOf("audit"),
                ),
                dynamicField(
                    fieldName = "legacyInfo",
                    mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                    propertyType = "LegacyDoc",
                    sourceTable = "legacy",
                    aliasPrefix = "legacy_",
                    suppressProperty = true,
                    aliasPath = listOf("legacy"),
                ),
            ),
            tableAliases = mapOf(
                "p" to "person",
                "pkg" to "package",
                "cat" to "category",
                "audit" to "audit",
                "legacy" to "legacy",
            ),
        )

        val plan = ResultMappingPlanner.create(statement.src, statement.fields)

        assertEquals(
            setOf("pkg_doc_id", "pkg_title", "cat_id", "cat_title", "audit_code", "legacy_code"),
            plan.mappedColumns,
        )
        assertEquals(listOf("person_id"), plan.regularFields.map { it.src.fieldName })
        assertEquals(
            listOf("packages", "auditInfo"),
            plan.includedDynamicEntries.map { it.field.src.fieldName },
        )
        assertEquals(setOf("packageCategory", "legacyInfo"), plan.skippedDynamicFieldNames)
    }
}
