/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.util.AliasPathUtils

/**
 * Helper functions for resolving alias-prefixed column names when mapping joined data.
 */
internal object AliasResolutionHelper {

    /**
     * Determine which table aliases should be examined when trying to resolve a column reference.
     * Includes the column's own table, the mapping's declared alias, and any alias the select
     * resolves to.
     */
    fun buildTableCandidates(
        column: SelectStatement.FieldSource,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
    ): List<String> {
        val candidates = mutableListOf<String>()
        if (column.tableName.isNotBlank()) {
            candidates += column.tableName
        }
        mapping.sourceTableAlias?.takeIf { it.isNotBlank() }?.let { alias ->
            candidates += alias
            statement.src.tableAliases[alias]?.let { resolved ->
                if (resolved.isNotBlank()) {
                    candidates += resolved
                }
            }
        }
        if (candidates.isEmpty() && column.tableName.isNotBlank()) {
            candidates += column.tableName
        }
        return candidates.filter { it.isNotBlank() }.distinct()
    }

    /**
     * Produce possible field-name variants that may appear in the joined result,
     * accounting for alias prefixes.
     */
    fun candidateFieldNames(
        column: SelectStatement.FieldSource,
        aliasPrefix: String?,
    ): List<String> {
        val names = mutableListOf<String>()
        val fieldName = column.fieldName
        if (fieldName.isNotBlank()) {
            names += fieldName
        }
        val original = column.originalColumnName
        if (original.isNotBlank()) {
            names += original
        }
        aliasPrefix?.takeIf { it.isNotBlank() }?.let { prefix ->
            if (fieldName.startsWith(prefix) && fieldName.length > prefix.length) {
                names += fieldName.removePrefix(prefix)
            }
            if (original.isNotBlank() && original.startsWith(prefix) && original.length > prefix.length) {
                names += original.removePrefix(prefix)
            }
        }
        return names.filter { it.isNotBlank() }.distinct()
    }

    /**
     * Expand base field names with suffixed variants that might have been generated for disambiguation.
     */
    fun expandCandidateNamesForTable(
        tableAlias: String,
        baseNames: List<String>,
        joinedNameMap: Map<JoinedPropertyNameResolver.JoinedFieldKey, String>,
        preferSuffixed: Boolean,
    ): List<String> {
        val expanded = mutableListOf<String>()
        baseNames.forEach { base ->
            if (!base.contains(':')) {
                val suffixed = joinedNameMap.keys
                    .filter { key -> key.tableAlias == tableAlias && key.fieldName.startsWith("$base:") }
                    .map { it.fieldName }
                    .sorted()
                if (preferSuffixed) {
                    suffixed.forEach { candidate ->
                        if (!expanded.contains(candidate)) {
                            expanded.add(candidate)
                        }
                    }
                }
                if (!expanded.contains(base)) {
                    expanded.add(base)
                }
                if (!preferSuffixed) {
                    suffixed.forEach { candidate ->
                        if (!expanded.contains(candidate)) {
                            expanded.add(candidate)
                        }
                    }
                }
            } else if (!expanded.contains(base)) {
                expanded.add(base)
            }
        }
        return expanded
    }

    /**
     * Decide whether we should prefer suffixed column names when resolving joined properties.
     */
    fun shouldPreferSuffixed(
        aliasPath: List<String>?,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
    ): Boolean {
        if (aliasPath.isNullOrEmpty()) return false
        val rootAliases = collectRootAliases(statement)
        val aliasPathMatchesRoot = aliasPath.any { rootAliases.contains(it) }
        val sourceAliasMatchesRoot = mapping.sourceTableAlias.isNotBlank() &&
                rootAliases.contains(mapping.sourceTableAlias)
        return !(aliasPathMatchesRoot || sourceAliasMatchesRoot)
    }

    /**
     * Check whether a dynamic field should be emitted for the current collection context.
     * Filters out descendants that belong to deeper collection aliases to avoid duplication.
     */
    fun shouldIncludeDynamicForCollection(
        aliasPath: List<String>,
        collectionAliasPaths: List<List<String>>,
    ): Boolean {
        if (aliasPath.isEmpty()) return true
        val lowered = AliasPathUtils.lowercase(aliasPath)
        return collectionAliasPaths.none { prefix ->
            AliasPathUtils.startsWith(lowered, prefix) && lowered.size > prefix.size
        }
    }

    private fun collectRootAliases(statement: AnnotatedSelectStatement): Set<String> {
        val aliases = mutableSetOf<String>()
        statement.src.fromTable?.let { aliases.add(it) }
        aliases.addAll(statement.src.tableAliases.keys)
        statement.mappingPlan.includedEntityEntries.forEach { entry ->
            entry.field.aliasPath.firstOrNull()?.let { aliases.add(it) }
            entry.field.aliasPath.getOrNull(1)?.let { aliases.add(it) }
            entry.field.annotations.sourceTable?.let { aliases.add(it) }
        }
        return aliases.filter { it.isNotBlank() }.toSet()
    }
}
