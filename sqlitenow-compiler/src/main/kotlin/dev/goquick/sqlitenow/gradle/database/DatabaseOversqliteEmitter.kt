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

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.oversqlite.ResolvedOversqliteSyncTable
import dev.goquick.sqlitenow.gradle.oversqlite.resolveOversqliteSyncTables

internal class DatabaseOversqliteEmitter(
    private val databaseClassName: String,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    private val oversqlite: Boolean,
) {
    fun addOversqliteSupport(classBuilder: TypeSpec.Builder) {
        val syncTables = resolveOversqliteSyncTables(
            databaseClassName = databaseClassName,
            createTableStatements = createTableStatements,
            oversqlite = oversqlite,
        )

        if (!oversqlite) return

        classBuilder.addType(buildSyncTablesCompanion(syncTables))
        addOpeningState(classBuilder)
        classBuilder.addFunction(buildOversqliteConfigFunction())
        classBuilder.addFunction(buildOversqliteAutomaticDownloadConfigFunction())
        classBuilder.addFunction(buildOpenForSyncFunction())
        classBuilder.addFunction(buildNewOversqliteClientFunction())
    }

    private fun addOpeningState(classBuilder: TypeSpec.Builder) {
        val databaseType = ClassName("", databaseClassName)
        val openingType = ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteOpening")
            .parameterizedBy(databaseType)
        listOf(
            PropertySpec.builder("_oversqliteOpening", openingType.copy(nullable = true))
                .addModifiers(KModifier.PRIVATE)
                .mutable(true)
                .initializer("null")
                .build(),
            PropertySpec.builder(
                "_oversqliteOpeningConfig",
                ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig").copy(nullable = true),
            ).addModifiers(KModifier.PRIVATE)
                .mutable(true)
                .initializer("null")
                .build(),
            PropertySpec.builder(
                "_oversqliteOpeningHttpClient",
                ClassName("io.ktor.client", "HttpClient").copy(nullable = true),
            ).addModifiers(KModifier.PRIVATE)
                .mutable(true)
                .initializer("null")
                .build(),
            PropertySpec.builder(
                "_oversqliteOpeningResolver",
                ClassName("dev.goquick.sqlitenow.oversqlite", "Resolver").copy(nullable = true),
            ).addModifiers(KModifier.PRIVATE)
                .mutable(true)
                .initializer("null")
                .build(),
        ).forEach(classBuilder::addProperty)
    }

    private fun buildSyncTablesCompanion(syncTables: List<ResolvedOversqliteSyncTable>): TypeSpec {
        val syncTableInitializers = syncTables.map { syncTable ->
            // Always materialize the resolved sync key into generated code so runtime bootstrap
            // does not depend on any implicit SyncTable default.
            val syncKeyParam = ", syncKeyColumnName = \"${syncTable.syncKeyColumnName}\""
            "%T(tableName = \"${syncTable.table.name}\"$syncKeyParam)"
        }

        val syncTableType = ClassName("dev.goquick.sqlitenow.oversqlite", "SyncTable")
        val listInitializer = syncTableInitializers.joinToString(", ")

        return TypeSpec.companionObjectBuilder()
            .addProperty(
                PropertySpec.builder(
                    "syncTables",
                    ClassName("kotlin.collections", "List").parameterizedBy(syncTableType)
                ).initializer(
                    "listOf($listInitializer)",
                    *Array(syncTables.size) { syncTableType })
                    .addKdoc("Tables annotated with enableSync in schema; used to configure oversqlite.")
                    .build()
            )
            .build()
    }

    private fun buildOversqliteConfigFunction(): FunSpec {
        return FunSpec.builder("buildOversqliteConfig")
            .addKdoc("Builds oversqlite config using enableSync tables.")
            .addParameter("generationId", Long::class)
            .addParameter("databaseId", String::class)
            .addParameter("serviceId", String::class)
            .addParameter(transferTargetsParameter())
            .addParameter(
                ParameterSpec.builder("snapshotApplyBatchRows", Int::class).defaultValue("256").build()
            )
            .addParameter(
                ParameterSpec.builder("snapshotApplyBatchBytes", Long::class)
                    .defaultValue("4L * 1024L * 1024L")
                    .build()
            )
            .addParameter(
                ParameterSpec.builder("verboseLogs", Boolean::class).defaultValue("false")
                    .build()
            )
            .addParameter(transientRetryPolicyParameter())
            .addParameter(snapshotCapacityRetryPolicyParameter())
            .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig"))
            .addStatement(
                "return %T(generationId = generationId, databaseId = databaseId, serviceId = serviceId, syncTables = Companion.syncTables, transferTargets = transferTargets, snapshotApplyBatchRows = snapshotApplyBatchRows, snapshotApplyBatchBytes = snapshotApplyBatchBytes, verboseLogs = verboseLogs, transientRetryPolicy = transientRetryPolicy, snapshotCapacityRetryPolicy = snapshotCapacityRetryPolicy)",
                ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig")
            )
            .build()
    }

    private fun buildOversqliteAutomaticDownloadConfigFunction(): FunSpec {
        return FunSpec.builder("buildOversqliteAutomaticDownloadConfig")
            .addKdoc("Builds optional automatic download config for the generated oversqlite client.")
            .addParameter(
                ParameterSpec.builder("automaticDownloadIntervalMillis", Long::class)
                    .defaultValue("60_000")
                    .build()
            )
            .addParameter(
                ParameterSpec.builder(
                    "bundleChangeWatchMode",
                    ClassName("dev.goquick.sqlitenow.oversqlite", "BundleChangeWatchMode")
                ).defaultValue(
                    "%T.OFF",
                    ClassName("dev.goquick.sqlitenow.oversqlite", "BundleChangeWatchMode")
                ).build()
            )
            .addParameter(
                ParameterSpec.builder("bundleChangeWatchReconnectMinMillis", Long::class)
                    .defaultValue("1_000")
                    .build()
            )
            .addParameter(
                ParameterSpec.builder("bundleChangeWatchReconnectMaxMillis", Long::class)
                    .defaultValue("60_000")
                    .build()
            )
            .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteAutomaticDownloadConfig"))
            .addStatement(
                "return %T(automaticDownloadIntervalMillis = automaticDownloadIntervalMillis, bundleChangeWatchMode = bundleChangeWatchMode, bundleChangeWatchReconnectMinMillis = bundleChangeWatchReconnectMinMillis, bundleChangeWatchReconnectMaxMillis = bundleChangeWatchReconnectMaxMillis)",
                ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteAutomaticDownloadConfig")
            )
            .build()
    }

    private fun buildOpenForSyncFunction(): FunSpec {
        val openingType = ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteOpening")
            .parameterizedBy(ClassName("", databaseClassName))
        val resolverType = ClassName("dev.goquick.sqlitenow.oversqlite", "Resolver")
        val upgradePlanType = ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteUpgradePlan")
        return FunSpec.builder("openForSync")
            .addModifiers(KModifier.SUSPEND)
            .addKdoc("Reserves this generated database and inspects it before any target DDL is executed.")
            .addParameter("config", ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig"))
            .addParameter("httpClient", ClassName("io.ktor.client", "HttpClient"))
            .addParameter(
                ParameterSpec.builder("resolver", resolverType)
                    .defaultValue("%T", ClassName("dev.goquick.sqlitenow.oversqlite", "DeferConflictResolver"))
                    .build(),
            )
            .addParameter(
                ParameterSpec.builder("upgradePlan", upgradePlanType.copy(nullable = true))
                    .defaultValue("null")
                    .build(),
            )
            .returns(openingType)
            .addStatement(
                "check(_oversqliteOpening == null) { %S }",
                "openForSync has already reserved this generated database instance",
            )
            .addStatement(
                "val frozenConfig = config.copy(syncTables = config.syncTables.map { it.copy(syncKeyColumns = it.syncKeyColumns.toList()) }, transientRetryPolicy = config.transientRetryPolicy.copy(), snapshotCapacityRetryPolicy = config.snapshotCapacityRetryPolicy.copy())",
            )
            .addStatement(
                "check(frozenConfig.syncTables == Companion.syncTables) { %S }",
                "openForSync config syncTables and keys must exactly match generated metadata",
            )
            .addStatement(
                "val opening = %T(database = this, config = frozenConfig, httpClient = httpClient, resolver = resolver, upgradePlan = upgradePlan)",
                ClassName("dev.goquick.sqlitenow.oversqlite", "openSqliteNowDatabaseForSync"),
            )
            .addStatement("_oversqliteOpeningConfig = frozenConfig")
            .addStatement("_oversqliteOpeningHttpClient = httpClient")
            .addStatement("_oversqliteOpeningResolver = resolver")
            .addStatement("_oversqliteOpening = opening")
            .addStatement("return opening")
            .build()
    }

    private fun buildNewOversqliteClientFunction(): FunSpec {
        return FunSpec.builder("newOversqliteClient")
            .addKdoc("Returns the client published by openForSync for this exact configuration and binding.")
            .addParameter("generationId", Long::class)
            .addParameter("databaseId", String::class)
            .addParameter("serviceId", String::class)
            .addParameter("httpClient", ClassName("io.ktor.client", "HttpClient"))
            .addParameter(
                ParameterSpec.builder(
                    "resolver",
                    ClassName("dev.goquick.sqlitenow.oversqlite", "Resolver")
                ).defaultValue(
                    "%T",
                    ClassName("dev.goquick.sqlitenow.oversqlite", "DeferConflictResolver")
                ).build()
            )
            .addParameter(transferTargetsParameter())
            .addParameter(
                ParameterSpec.builder("snapshotApplyBatchRows", Int::class).defaultValue("256").build()
            )
            .addParameter(
                ParameterSpec.builder("snapshotApplyBatchBytes", Long::class)
                    .defaultValue("4L * 1024L * 1024L")
                    .build()
            )
            .addParameter(
                ParameterSpec.builder("verboseLogs", Boolean::class).defaultValue("false")
                    .build()
            )
            .addParameter(transientRetryPolicyParameter())
            .addParameter(snapshotCapacityRetryPolicyParameter())
            .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteClient"))
            .addStatement(
                "val cfg = buildOversqliteConfig(generationId = generationId, databaseId = databaseId, serviceId = serviceId, transferTargets = transferTargets, snapshotApplyBatchRows = snapshotApplyBatchRows, snapshotApplyBatchBytes = snapshotApplyBatchBytes, verboseLogs = verboseLogs, transientRetryPolicy = transientRetryPolicy, snapshotCapacityRetryPolicy = snapshotCapacityRetryPolicy)"
            )
            .addStatement(
                "check(_oversqliteOpeningConfig == cfg && _oversqliteOpeningHttpClient === httpClient && _oversqliteOpeningResolver === resolver) { %S }",
                "newOversqliteClient must use the exact config, HttpClient, and Resolver supplied to openForSync",
            )
            .addStatement(
                "return checkNotNull(_oversqliteOpening?.readyOrNull()) { %S }.client",
                "openForSync().advance() must reach ready before requesting the generated client",
            )
            .build()
    }

    private fun transientRetryPolicyParameter(): ParameterSpec {
        val type = ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteTransientRetryPolicy")
        return ParameterSpec.builder("transientRetryPolicy", type)
            .defaultValue("%T()", type)
            .build()
    }

    private fun transferTargetsParameter(): ParameterSpec {
        val type = ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteTransferTargets")
        return ParameterSpec.builder("transferTargets", type)
            .defaultValue("%T()", type)
            .build()
    }

    private fun snapshotCapacityRetryPolicyParameter(): ParameterSpec {
        val type = ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteSnapshotCapacityRetryPolicy")
        return ParameterSpec.builder("snapshotCapacityRetryPolicy", type)
            .defaultValue("%T()", type)
            .build()
    }
}
