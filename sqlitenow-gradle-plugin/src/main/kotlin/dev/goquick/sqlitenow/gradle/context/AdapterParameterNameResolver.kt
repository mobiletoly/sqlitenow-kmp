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
package dev.goquick.sqlitenow.gradle.context

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.util.pascalize

/**
 * Utility class for resolving and canonicalizing adapter parameter names.
 * This centralizes the logic for handling adapter name normalization and deduplication.
 */
internal class AdapterParameterNameResolver {

    /**
     * Remove namespace/table prefix from adapter function name when it belongs to that namespace.
     * Example: "sqlValueToPersonBirthDate" in "person" namespace -> "sqlValueToBirthDate"
     */
    fun canonicalizeAdapterNameForNamespace(namespace: String, functionName: String): String {
        val pascalNs = pascalize(namespace)
        val base = functionName.substringBefore("For")
        return when {
            base.startsWith("sqlValueTo") -> {
                val prop = base.removePrefix("sqlValueTo")
                val canonicalProp = if (prop.startsWith(pascalNs)) prop.removePrefix(pascalNs) else prop
                "sqlValueTo$canonicalProp"
            }
            base.endsWith("ToSqlValue") -> {
                val prop = base.removeSuffix("ToSqlValue")
                val canonicalProp = if (prop.startsWith(pascalNs)) prop.removePrefix(pascalNs) else prop
                "${canonicalProp}ToSqlValue"
            }
            else -> base
        }
    }

    /**
     * Normalize alias noise by removing duplicate tokens.
     * Example: "sqlValueToAddressAddressType" -> "sqlValueToAddressType"
     */
    fun normalizeAliasNoiseForNamespace(functionName: String): String {
        val base = functionName.substringBefore("For")
        fun compress(prop: String): String {
            val tokens = prop.split(Regex("(?=[A-Z])")).filter { it.isNotEmpty() }
            return if (tokens.size >= 2 && tokens[0] == tokens[1]) {
                (listOf(tokens[0]) + tokens.drop(2)).joinToString("")
            } else {
                prop
            }
        }
        return when {
            base.startsWith("sqlValueTo") -> {
                val prop = base.removePrefix("sqlValueTo")
                "sqlValueTo" + compress(prop)
            }
            base.endsWith("ToSqlValue") -> {
                val prop = base.removeSuffix("ToSqlValue")
                compress(prop) + "ToSqlValue"
            }
            else -> base
        }
    }

    /**
     * Choose final adapter parameter names for a statement, canonicalizing and de-duplicating by signature.
     * This is the main entry point for resolving adapter parameter names.
     */
    fun chooseAdapterParamNames(
        configs: List<AdapterConfig.ParamConfig>
    ): Map<AdapterConfig.ParamConfig, String> {
        // Step 1: canonicalize + normalize alias noise
        val canonical = configs.associateWith { config ->
            val base = config.providerNamespace?.let { ns ->
                canonicalizeAdapterNameForNamespace(ns, config.adapterFunctionName)
            } ?: config.adapterFunctionName
            normalizeAliasNoiseForNamespace(base)
        }
        
        // Step 2: group by (name + signature); assign one shared param name per group
        data class Sig(val inT: String, val outT: String)
        val groups = mutableMapOf<String, MutableMap<Sig, MutableList<AdapterConfig.ParamConfig>>>()
        configs.forEach { cfg ->
            val name = canonical[cfg]!!
            val sig = Sig(cfg.inputType.toString(), cfg.outputType.toString())
            val bySig = groups.getOrPut(name) { linkedMapOf() }
            bySig.getOrPut(sig) { mutableListOf() }.add(cfg)
        }
        
        // Step 3: build chosen names: if a name maps to multiple signatures, disambiguate by signature
        val result = mutableMapOf<AdapterConfig.ParamConfig, String>()
        groups.forEach { (name, bySig) ->
            if (bySig.size == 1) {
                // Single signature: use the canonical name
                bySig.values.first().forEach { cfg -> result[cfg] = name }
            } else {
                // Multiple signatures: disambiguate
                bySig.forEach { (sig, cfgs) ->
                    val disambiguated = "${name}For${sig.inT.substringAfterLast('.')}To${sig.outT.substringAfterLast('.')}"
                    cfgs.forEach { cfg -> result[cfg] = disambiguated }
                }
            }
        }
        return result
    }

    /**
     * Resolve chosen adapter param name for a given output field.
     * This is used by query generation to find the correct parameter name to use in function calls.
     */
    fun resolveOutputAdapterParamNameForField(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>,
        adapterConfig: AdapterConfig
    ): String {
        val all = adapterConfig.collectAllParamConfigs(statement)
        val outputConfigs = all.filter { it.adapterFunctionName.startsWith("sqlValueTo") }
        if (outputConfigs.isEmpty()) {
            return fallbackAdapterName(statement, field, tableAliases, aliasPrefixes, adapterConfig)
        }
        val chosen = chooseAdapterParamNames(outputConfigs)

        // Build raw function name as AdapterConfig does (using base column name, not visible name)
        val rawAdapterName = computeRawAdapterName(statement, field, aliasPrefixes, adapterConfig)
        val providerNs = providerNamespaceForField(field, tableAliases)

        // Try exact match (by raw name and provider namespace)
        val exact = outputConfigs.firstOrNull { it.adapterFunctionName == rawAdapterName && it.providerNamespace == providerNs }
        if (exact != null) {
            chosen[exact]?.let { return it }
        }

        // Fallback: match by raw name only
        val any = outputConfigs.firstOrNull { it.adapterFunctionName == rawAdapterName }
        chosen[any]?.let { return it }

        return fallbackAdapterName(statement, field, tableAliases, aliasPrefixes, adapterConfig)
    }

    private fun computeRawAdapterName(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        aliasPrefixes: List<String>,
        adapterConfig: AdapterConfig,
    ): String {
        val baseColumnName = adapterConfig.baseOriginalNameForField(field, aliasPrefixes, statement)
        val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(baseColumnName)
        return adapterConfig.getOutputAdapterFunctionName(columnName)
    }

    private fun providerNamespaceForField(
        field: AnnotatedSelectStatement.Field,
        tableAliases: Map<String, String>,
    ): String? {
        return if (field.src.tableName.isNotBlank()) {
            tableAliases[field.src.tableName] ?: field.src.tableName
        } else {
            null
        }
    }

    private fun fallbackAdapterName(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>,
        adapterConfig: AdapterConfig,
    ): String {
        val rawAdapterName = computeRawAdapterName(statement, field, aliasPrefixes, adapterConfig)
        val providerNs = providerNamespaceForField(field, tableAliases)
        val canonical = providerNs?.let { ns ->
            canonicalizeAdapterNameForNamespace(ns, rawAdapterName)
        } ?: rawAdapterName
        return normalizeAliasNoiseForNamespace(canonical)
    }


}
