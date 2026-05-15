package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.test.Test
import kotlin.test.assertEquals

class DynamicFieldMapperTest {

    @TestFactory
    fun `extract grouping column handles join predicate direction`(): List<DynamicTest> = listOf(
        GroupingColumnCase(
            displayName = "aliased join columns",
            selectName = "AddressRows",
            joinCondition = SelectStatement.JoinCondition("p", "id", "a", "person_id"),
        ),
        GroupingColumnCase(
            displayName = "reversed join predicates",
            selectName = "AddressRowsReversed",
            joinCondition = SelectStatement.JoinCondition("a", "person_id", "p", "id"),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val select = annotatedSelectStatement(
                name = case.selectName,
                sources = addressRowSources(),
                tableAliases = mapOf("p" to "person", "a" to "address"),
                joinConditions = listOf(case.joinCondition),
            ).src

            val groupingColumn = DynamicFieldMapper.extractGroupingColumn(select, "a")

            assertEquals("person_id", groupingColumn)
        }
    }

    @TestFactory
    fun `dynamic mappings choose alias columns before prefixed fallbacks`(): List<DynamicTest> = listOf(
        DynamicMappingCase(
            displayName = "prefer alias matched columns",
            selectName = "AliasPreferred",
            sources = listOf(
                fieldSource("doc_id", "package", "doc_id"),
                fieldSource("title", "package", "title"),
                fieldSource("joined_pkg_doc_id", "", "doc_id"),
                fieldSource("joined_pkg_title", "", "title"),
            ),
            expectedColumns = listOf("doc_id", "title"),
        ),
        DynamicMappingCase(
            displayName = "fall back to prefixed columns and skip sqlite duplicate suffixes",
            selectName = "PrefixFallback",
            sources = listOf(
                fieldSource("joined_pkg_doc_id", "", "doc_id"),
                fieldSource("joined_pkg_title", "", "title"),
                fieldSource("joined_pkg_doc_id:1", "", "doc_id"),
            ),
            expectedColumns = listOf("joined_pkg_doc_id", "joined_pkg_title"),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val select = annotatedSelectStatement(
                name = case.selectName,
                sources = case.sources,
                tableAliases = mapOf("pkg" to "package"),
            ).src

            val mappings = DynamicFieldMapper.createDynamicFieldMappings(
                select,
                listOf(
                    dynamicField(
                        fieldName = "packageDoc",
                        mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                        propertyType = "PackageDoc",
                        sourceTable = "pkg",
                        aliasPrefix = "joined_pkg_",
                    ),
                ),
            )

            assertEquals(case.expectedColumns, mappings.single().columns.map { it.fieldName })
        }
    }

    private fun addressRowSources(): List<SelectStatement.FieldSource> = listOf(
        fieldSource("person_id", "p", "id", "INTEGER"),
        fieldSource("address_id", "a", "id", "INTEGER"),
    )

    private data class GroupingColumnCase(
        val displayName: String,
        val selectName: String,
        val joinCondition: SelectStatement.JoinCondition,
    )

    private data class DynamicMappingCase(
        val displayName: String,
        val selectName: String,
        val sources: List<SelectStatement.FieldSource>,
        val expectedColumns: List<String>,
    )
}
