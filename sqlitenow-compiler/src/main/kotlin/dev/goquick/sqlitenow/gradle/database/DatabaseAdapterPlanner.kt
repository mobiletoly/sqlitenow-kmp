/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.gradle.database

import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.SharedResultManager
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import dev.goquick.sqlitenow.gradle.util.lowercaseFirst
import dev.goquick.sqlitenow.gradle.util.pascalize

/** Data class representing a unique adapter with its function signature. */
internal data class UniqueAdapter(
    val functionName: String,
    val inputType: TypeName,
    val outputType: TypeName,
    val isNullable: Boolean,
    val providerNamespace: String?
) {
    fun toParameterSpec(): ParameterSpec {
        val lambdaType = LambdaTypeName.get(
            parameters = arrayOf(inputType),
            returnType = outputType
        )
        return ParameterSpec.builder(functionName, lambdaType).build()
    }

    /** Create a signature key for deduplication that considers the actual TypeName structure */
    fun signatureKey(): String {
        return "${inputType}__$outputType"
    }
}

internal class DatabaseAdapterPlanner(
    private val nsWithStatements: Map<String, List<AnnotatedStatement>>,
    private val adapterConfig: AdapterConfig,
    private val tableLookup: CaseInsensitiveMap<AnnotatedCreateTableStatement>,
    private val sharedResultManager: SharedResultManager,
) {
    private fun isCustomType(t: TypeName): Boolean {
        val s = t.toString()
        return !(s.startsWith("kotlin.") || s.startsWith("kotlinx."))
    }

    private fun normalizedTypeString(t: TypeName): String = t.toString().removeSuffix("?")

    private fun adapterScore(u: UniqueAdapter): Int {
        val customScore = if (isCustomType(u.outputType)) 2 else 0
        val nnInput = if (!u.inputType.isNullable) 1 else 0
        val nonIdentity =
            if (normalizedTypeString(u.inputType) != normalizedTypeString(u.outputType)) 1 else 0
        return customScore + nnInput + nonIdentity
    }

    // Give precedence to adapters emitted from the namespace that owns the source column/result
    private fun providerPreferenceScore(namespace: String, adapter: UniqueAdapter): Int {
        return if (adapter.providerNamespace?.equals(namespace, ignoreCase = true) == true) 1 else 0
    }

    fun baseFunctionKey(name: String): String = name.substringBefore("For")

    fun computeBestProviders(
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): Map<String, String> {
        val winners = mutableMapOf<String, Pair<String, UniqueAdapter>>()
        adaptersByNamespace.toSortedMap().forEach { (ns, adapters) ->
            // Iterate deterministically so ties break in a stable manner across platforms
            adapters.sortedBy { it.functionName }.forEach { ua ->
                val key = baseFunctionKey(ua.functionName)
                val current = winners[key]
                if (current == null) {
                    winners[key] = ns to ua
                } else {
                    val currentPref = providerPreferenceScore(current.first, current.second)
                    val candidatePref = providerPreferenceScore(ns, ua)
                    when {
                        candidatePref > currentPref -> winners[key] = ns to ua
                        candidatePref < currentPref -> { /* keep current */ }
                        else -> {
                            val currentScore = adapterScore(current.second)
                            val candidateScore = adapterScore(ua)
                            if (candidateScore > currentScore || (candidateScore == currentScore && ns < current.first)) {
                                winners[key] = ns to ua
                            }
                        }
                    }
                }
            }
        }
        return winners.mapValues { it.value.first }
    }

    fun baseNameForNamespace(namespace: String): String {
        val table = tableLookup[namespace]
        return table?.annotations?.name ?: pascalize(namespace)
    }

    fun adapterClassNameFor(namespace: String): String =
        baseNameForNamespace(namespace) + "Adapters"

    fun adapterPropertyNameFor(namespace: String): String {
        val cls = adapterClassNameFor(namespace)
        return cls.lowercaseFirst()
    }

    /** Collects and deduplicates adapters per namespace. */
    fun collectAdaptersByNamespace(): Map<String, List<UniqueAdapter>> {
        val adaptersByNamespace = mutableMapOf<String, MutableList<UniqueAdapter>>()

        // Collect adapters for each namespace separately
        nsWithStatements.forEach { (namespace, statements) ->
            val namespaceAdapters = mutableListOf<UniqueAdapter>()
            val processedSharedResults = mutableMapOf<String, Boolean>()

            // First, register all shared results for this namespace
            statements.filterIsInstance<AnnotatedSelectStatement>().forEach { statement ->
                sharedResultManager.registerSharedResult(statement, namespace)
            }

            statements.forEach { statement ->
                val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace).toMutableList()
                if (statement is AnnotatedSelectStatement && statement.annotations.queryResult != null) {
                    val sharedResultKey = "${namespace}.${statement.annotations.queryResult}"
                    val alreadyProcessed = processedSharedResults[sharedResultKey] ?: false
                    if (alreadyProcessed) {
                        statementAdapters.removeIf { it.kind == AdapterConfig.AdapterKind.MAP_RESULT }
                    }
                    val mapCollected = statement.annotations.mapTo != null
                    processedSharedResults[sharedResultKey] = alreadyProcessed || mapCollected
                }
                statementAdapters.forEach { config ->
                    namespaceAdapters.add(
                        UniqueAdapter(
                            functionName = config.adapterFunctionName,
                            inputType = config.inputType,
                            outputType = config.outputType,
                            isNullable = config.isNullable,
                            providerNamespace = config.providerNamespace
                        )
                    )
                }
            }

            val deduplicatedAdapters = deduplicateAdaptersForNamespace(namespaceAdapters)
            adaptersByNamespace[namespace] = deduplicatedAdapters.toMutableList()
        }

        return adaptersByNamespace
    }

    /** Deduplicates adapters within a single namespace. */
    private fun deduplicateAdaptersForNamespace(adapters: List<UniqueAdapter>): List<UniqueAdapter> {
        val adaptersByName = adapters.groupBy { it.functionName }
        val result = mutableListOf<UniqueAdapter>()

        adaptersByName.forEach { (baseName, adapterList) ->
            // Remove exact duplicates first (same signature)
            val uniqueBySignature = adapterList.distinctBy { it.signatureKey() }

            val best = uniqueBySignature.maxByOrNull { adapterScore(it) }
            val bestScore = best?.let { adapterScore(it) } ?: 0
            val top = uniqueBySignature.filter { adapterScore(it) == bestScore }

            if (top.size == 1) {
                result.add(top.first().copy(functionName = baseName))
            } else {
                // Tie: keep all, but disambiguate names deterministically
                top.forEach { adapter ->
                    val inputLabel = sanitizeTypeLabel(adapter.inputType)
                    val outputLabel = sanitizeTypeLabel(adapter.outputType)
                    val uniqueName = "${baseName}For${inputLabel}To${outputLabel}"
                    result.add(adapter.copy(functionName = uniqueName))
                }
            }
        }

        return result
    }

    private fun sanitizeTypeLabel(type: TypeName): String {
        // Build a short, identifier-safe type label (e.g. StringNullable, ByteArray, ListString)
        var s = type.toString()
        // Keep only leaf class names for qualified types
        s = s.split('.').last()
        // Replace nullability with a readable token
        s = s.replace("?", "Nullable")
        // Remove generic punctuation and non-alphanumerics
        s = s.replace(Regex("[<>.,\\s]"), "")
        // Capitalize first char for consistency
        return s.replaceFirstChar { it.uppercase() }
    }

    /**
     * Simple helper function to find the correct adapter name.
     * Uses direct lookup by expected function name pattern.
     */
    fun findAdapterName(
        namespace: String,
        expectedFunctionName: String,
        expectedInputType: TypeName,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val deduplicatedAdapters = adaptersByNamespace[namespace] ?: return expectedFunctionName

        // Look for exact match first
        val exactMatch = deduplicatedAdapters.find { it.functionName == expectedFunctionName }
        if (exactMatch != null) {
            return exactMatch.functionName
        }

        // Look for renamed version (e.g., phoneToSqlValueForString)
        val renamedMatch = deduplicatedAdapters.find {
            it.functionName.startsWith(expectedFunctionName + "For") && it.inputType == expectedInputType
        }

        return renamedMatch?.functionName ?: expectedFunctionName
    }

    fun findBestProviderByName(
        expectedFunctionName: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): Pair<String, UniqueAdapter>? {
        var best: Pair<String, UniqueAdapter>? = null
        adaptersByNamespace.forEach { (ns, adapters) ->
            adapters.filter {
                it.functionName == expectedFunctionName || it.functionName.startsWith(
                    expectedFunctionName + "For"
                )
            }.forEach { cand ->
                val cur = best
                if (cur == null) {
                    best = ns to cand
                } else {
                    val currentPref = providerPreferenceScore(cur.first, cur.second)
                    val candidatePref = providerPreferenceScore(ns, cand)
                    // Prefer adapters from the namespace that declared the column/result; when that
                    // still ties, fall back to quality score and finally namespace name for stability.
                    when {
                        candidatePref > currentPref -> best = ns to cand
                        candidatePref < currentPref -> {
                            // keep current
                        }
                        else -> {
                            val currentScore = adapterScore(cur.second)
                            val candidateScore = adapterScore(cand)
                            if (candidateScore > currentScore || (candidateScore == currentScore && ns < cur.first)) {
                                best = ns to cand
                            }
                        }
                    }
                }
            }
        }
        return best
    }
}
