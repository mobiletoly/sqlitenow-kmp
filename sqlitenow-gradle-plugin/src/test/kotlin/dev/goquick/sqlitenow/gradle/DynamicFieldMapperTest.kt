package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import kotlin.test.Test
import kotlin.test.assertEquals

class DynamicFieldMapperTest {

    @Test
    fun `extract grouping column resolves aliased join columns`() {
        val select = annotatedSelectStatement(
            name = "AddressRows",
            sources = listOf(
                fieldSource("person_id", "p", "id", "INTEGER"),
                fieldSource("address_id", "a", "id", "INTEGER"),
            ),
            tableAliases = mapOf("p" to "person", "a" to "address"),
            joinConditions = listOf(
                SelectStatement.JoinCondition("p", "id", "a", "person_id"),
            ),
        ).src

        val groupingColumn = DynamicFieldMapper.extractGroupingColumn(select, "a")

        assertEquals("person_id", groupingColumn)
    }

    @Test
    fun `extract grouping column supports reversed join predicates`() {
        val select = annotatedSelectStatement(
            name = "AddressRowsReversed",
            sources = listOf(
                fieldSource("person_id", "p", "id", "INTEGER"),
                fieldSource("address_id", "a", "id", "INTEGER"),
            ),
            tableAliases = mapOf("p" to "person", "a" to "address"),
            joinConditions = listOf(
                SelectStatement.JoinCondition("a", "person_id", "p", "id"),
            ),
        ).src

        val groupingColumn = DynamicFieldMapper.extractGroupingColumn(select, "a")

        assertEquals("person_id", groupingColumn)
    }

    @Test
    fun `dynamic mappings prefer alias matched columns over prefixed fallbacks`() {
        val select = annotatedSelectStatement(
            name = "AliasPreferred",
            sources = listOf(
                fieldSource("doc_id", "package", "doc_id"),
                fieldSource("title", "package", "title"),
                fieldSource("joined_pkg_doc_id", "", "doc_id"),
                fieldSource("joined_pkg_title", "", "title"),
            ),
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

        assertEquals(listOf("doc_id", "title"), mappings.single().columns.map { it.fieldName })
    }

    @Test
    fun `dynamic mappings fall back to prefixed columns and skip sqlite duplicate suffixes`() {
        val select = annotatedSelectStatement(
            name = "PrefixFallback",
            sources = listOf(
                fieldSource("joined_pkg_doc_id", "", "doc_id"),
                fieldSource("joined_pkg_title", "", "title"),
                fieldSource("joined_pkg_doc_id:1", "", "doc_id"),
            ),
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

        assertEquals(
            listOf("joined_pkg_doc_id", "joined_pkg_title"),
            mappings.single().columns.map { it.fieldName },
        )
    }
}
