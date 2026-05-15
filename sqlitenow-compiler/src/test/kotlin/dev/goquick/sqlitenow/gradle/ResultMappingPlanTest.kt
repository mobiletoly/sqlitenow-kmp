package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.ResultMappingPlan
import dev.goquick.sqlitenow.gradle.model.ResultMappingPlanner
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import kotlin.test.Test
import kotlin.test.assertEquals

class ResultMappingPlanTest {

    @Test
    fun `planner suppresses mapped columns and duplicate sqlite labels`() {
        val sources = listOf(
            personIdSource(),
            packageDocIdSource(),
            packageTitleSource(),
            fieldSource("pkg_doc_id:1", "package", "doc_id"),
        )
        val statement = annotatedSelectStatement(
            name = "PackageRows",
            sources = sources,
            dynamicFields = listOf(
                packagesDynamicField(),
            ),
            tableAliases = mapOf("p" to "person", "pkg" to "package"),
        )

        val plan = ResultMappingPlanner.create(statement.src, statement.fields)

        assertPlan(
            plan = plan,
            mappedColumns = setOf("pkg_doc_id", "pkg_title", "pkg_doc_id:1"),
            regularFields = listOf("person_id"),
            includedDynamicFields = listOf("packages"),
            skippedDynamicFields = emptySet(),
        )
    }

    @Test
    fun `planner pins regular and dynamic slices for nested aliases and suppressed fields`() {
        val sources = listOf(
            personIdSource(),
            packageDocIdSource(),
            packageTitleSource(),
            fieldSource("cat_id", "category", "id"),
            fieldSource("cat_title", "category", "title"),
            fieldSource("audit_code", "audit", "code"),
            fieldSource("legacy_code", "legacy", "code"),
            fieldSource("joined__cat__shadow", "", "shadow"),
        )
        val statement = annotatedSelectStatement(
            name = "NestedPackageRows",
            sources = sources,
            dynamicFields = listOf(
                packagesDynamicField(),
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

        assertPlan(
            plan = plan,
            mappedColumns = setOf("pkg_doc_id", "pkg_title", "cat_id", "cat_title", "audit_code", "legacy_code"),
            regularFields = listOf("person_id"),
            includedDynamicFields = listOf("packages", "auditInfo"),
            skippedDynamicFields = setOf("packageCategory", "legacyInfo"),
        )
    }

    private fun personIdSource() = fieldSource("person_id", "person", "id", "INTEGER")

    private fun packageDocIdSource() = fieldSource("pkg_doc_id", "package", "doc_id")

    private fun packageTitleSource() = fieldSource("pkg_title", "package", "title")

    private fun packagesDynamicField() = dynamicField(
        fieldName = "packages",
        mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
        propertyType = "kotlin.collections.List<PackageDoc>",
        sourceTable = "pkg",
        aliasPrefix = "pkg_",
        collectionKey = "pkg_doc_id",
        aliasPath = listOf("pkg"),
    )

    private fun assertPlan(
        plan: ResultMappingPlan,
        mappedColumns: Set<String>,
        regularFields: List<String>,
        includedDynamicFields: List<String>,
        skippedDynamicFields: Set<String>,
    ) {
        assertEquals(mappedColumns, plan.mappedColumns)
        assertEquals(regularFields, plan.regularFields.map { it.src.fieldName })
        assertEquals(includedDynamicFields, plan.includedDynamicEntries.map { it.field.src.fieldName })
        assertEquals(skippedDynamicFields, plan.skippedDynamicFieldNames)
    }
}
