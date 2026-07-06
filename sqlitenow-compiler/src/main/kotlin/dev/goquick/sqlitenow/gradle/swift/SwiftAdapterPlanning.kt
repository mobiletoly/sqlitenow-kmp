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
package dev.goquick.sqlitenow.gradle.swift

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.database.DatabaseAdapterPlanner
import dev.goquick.sqlitenow.gradle.database.UniqueAdapter
import dev.goquick.sqlitenow.gradle.processing.SharedResultManager
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap

internal class SwiftAdapterPlanning(
    context: GeneratorContext,
) {
    val planner = DatabaseAdapterPlanner(
        nsWithStatements = context.nsWithStatements,
        adapterConfig = context.adapterConfig,
        tableLookup = CaseInsensitiveMap(context.createTableStatements.map { it.src.tableName to it }),
        sharedResultManager = SharedResultManager(),
    )

    val adaptersByNamespace: Map<String, List<UniqueAdapter>> by lazy {
        planner.collectAdaptersByNamespace(preserveSignatureCollisions = true)
    }

    val generatedAdapterProviders: Map<String, List<UniqueAdapter>> by lazy {
        val bestProviderForSignature = planner.computeBestProvidersBySignature(adaptersByNamespace)
        adaptersByNamespace
            .mapValues { (namespace, adapters) ->
                adapters.filter { adapter ->
                    bestProviderForSignature[planner.providerSignatureKey(adapter)] == namespace
                }
            }
            .filterValues { it.isNotEmpty() }
    }
}
