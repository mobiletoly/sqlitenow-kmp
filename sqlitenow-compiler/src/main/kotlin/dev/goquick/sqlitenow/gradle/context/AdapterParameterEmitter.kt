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
package dev.goquick.sqlitenow.gradle.context

import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils

/**
 * Handles adding adapter parameters to generated query functions and exposes helper lookups
 * for adapter parameter names derived from statement annotations.
 */
internal class AdapterParameterEmitter(
    private val generatorContext: GeneratorContext,
) {
    private val packageName: String = generatorContext.packageName
    private val adapterConfig: AdapterConfig = generatorContext.adapterConfig
    private val adapterNameResolver: AdapterParameterNameResolver =
        generatorContext.adapterNameResolver

    /**
     * Re-exposed so callers (and tests) can re-use the adapter resolution logic.
     */
    fun chooseAdapterParamNames(
        configs: List<AdapterConfig.ParamConfig>
    ): Map<AdapterConfig.ParamConfig, String> {
        return adapterNameResolver.chooseAdapterParamNames(configs)
    }

    fun addParameterBindingAdapters(
        fnBld: FunSpec.Builder,
        namespace: String,
        statement: AnnotatedStatement,
    ) {
        addAdapterParameters(
            fnBld = fnBld,
            namespace = namespace,
            statement = statement,
            includeMapAdapters = true,
        ) { config ->
            config.kind == AdapterConfig.AdapterKind.INPUT
        }
    }

    fun addResultConversionAdapters(
        fnBld: FunSpec.Builder,
        namespace: String,
        statement: AnnotatedSelectStatement,
        includeMapAdapters: Boolean = true,
    ) {
        addAdapterParameters(
            fnBld = fnBld,
            namespace = namespace,
            statement = statement,
            includeMapAdapters = includeMapAdapters,
        ) { config ->
            when (config.kind) {
                AdapterConfig.AdapterKind.RESULT_FIELD -> true
                AdapterConfig.AdapterKind.MAP_RESULT -> true
                AdapterConfig.AdapterKind.INPUT -> false
            }
        }
    }

    fun addResultConversionAdaptersForExecute(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement
    ) {
        addExecuteReturningAdapters(fnBld, statement)
    }

    fun parameterBindingAdapterNames(namespace: String, statement: AnnotatedStatement): List<String> {
        return getFilteredAdapterNames(
            namespace = namespace,
            statement = statement,
            includeMapAdapters = true,
        ) { config ->
            config.kind == AdapterConfig.AdapterKind.INPUT
        }
    }

    fun resultConversionAdapterNames(
        namespace: String,
        statement: AnnotatedStatement,
        includeMapAdapters: Boolean = true,
    ): List<String> {
        return getFilteredAdapterNames(
            namespace = namespace,
            statement = statement,
            includeMapAdapters = includeMapAdapters,
        ) { config ->
            config.kind == AdapterConfig.AdapterKind.RESULT_FIELD ||
                config.kind == AdapterConfig.AdapterKind.MAP_RESULT
        }
    }

    fun buildSelectReadParamsList(
        namespace: String,
        statement: AnnotatedSelectStatement,
        includeMapAdapters: Boolean = true,
    ): List<String> {
        val params = mutableListOf("statement")
        params += resultConversionAdapterNames(namespace, statement, includeMapAdapters)
        return params
    }

    fun buildExecuteReadParamsList(statement: AnnotatedExecuteStatement): List<String> {
        val params = mutableListOf("statement")
        params += resultConversionAdapterNamesForExecute(statement)
        return params
    }

    fun mapToAdapterParameterName(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): String? {
        val configs = adapterConfig.collectAllParamConfigs(statement, namespace)
        val mapConfigs = configs.filter { it.kind == AdapterConfig.AdapterKind.MAP_RESULT }
        if (mapConfigs.isEmpty()) return null
        val targetInputType = SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
        val matchingConfig = mapConfigs.firstOrNull { it.inputType == targetInputType } ?: return null
        val chosen = chooseAdapterParamNames(configs)
        return chosen[matchingConfig]
    }

    private fun addAdapterParameters(
        fnBld: FunSpec.Builder,
        namespace: String,
        statement: AnnotatedStatement,
        includeMapAdapters: Boolean,
        filter: (AdapterConfig.ParamConfig) -> Boolean,
    ) {
        val adapterConfigs = adapterConfig.collectAllParamConfigs(statement, namespace)
        val filteredConfigs = adapterConfigs.filter(filter).filter { config ->
            includeMapAdapters || config.kind != AdapterConfig.AdapterKind.MAP_RESULT
        }
        val chosenNames = chooseAdapterParamNames(filteredConfigs)
        val byName: MutableMap<String, AdapterConfig.ParamConfig> = linkedMapOf()
        filteredConfigs.forEach { cfg ->
            val name = chosenNames[cfg]!!
            byName.putIfAbsent(name, cfg)
        }
        byName.forEach { (name, cfg) ->
            val adapterType = LambdaTypeName.get(
                parameters = arrayOf(cfg.inputType),
                returnType = cfg.outputType
            )
            val adapterParam = ParameterSpec.builder(name, adapterType).build()
            fnBld.addParameter(adapterParam)
        }
    }

    private fun getFilteredAdapterNames(
        namespace: String,
        statement: AnnotatedStatement,
        includeMapAdapters: Boolean,
        filter: (AdapterConfig.ParamConfig) -> Boolean,
    ): List<String> {
        val adapterConfigs = adapterConfig.collectAllParamConfigs(statement, namespace)
        val filtered = adapterConfigs.filter(filter).filter { config ->
            includeMapAdapters || config.kind != AdapterConfig.AdapterKind.MAP_RESULT
        }
        val chosen = chooseAdapterParamNames(filtered)
        val seen = LinkedHashSet<String>()
        filtered.forEach { config ->
            seen.add(chosen[config]!!)
        }
        return seen.toList()
    }

    private fun addExecuteReturningAdapters(
        fnBld: FunSpec.Builder,
        statement: AnnotatedExecuteStatement,
    ) {
        adapterConfig.collectExecuteReturningOutputParamConfigs(statement).forEach { config ->
            val adapterType = LambdaTypeName.get(
                parameters = arrayOf(config.inputType),
                returnType = config.outputType,
            )
            val parameterSpec = ParameterSpec.builder(config.adapterFunctionName, adapterType).build()
            fnBld.addParameter(parameterSpec)
        }
    }

    private fun resultConversionAdapterNamesForExecute(
        statement: AnnotatedExecuteStatement
    ): List<String> {
        return adapterConfig.collectExecuteReturningOutputParamConfigs(statement)
            .map { it.adapterFunctionName }
    }
}
