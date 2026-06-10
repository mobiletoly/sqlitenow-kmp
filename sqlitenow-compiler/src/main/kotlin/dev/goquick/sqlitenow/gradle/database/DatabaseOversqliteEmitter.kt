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
        classBuilder.addFunction(buildOversqliteConfigFunction())
        classBuilder.addFunction(buildOversqliteAutomaticDownloadConfigFunction())
        classBuilder.addFunction(buildNewOversqliteClientFunction())
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
            .addParameter("schema", String::class)
            .addParameter(
                ParameterSpec.builder("uploadLimit", Int::class).defaultValue("200").build()
            )
            .addParameter(
                ParameterSpec.builder("downloadLimit", Int::class).defaultValue("1000")
                    .build()
            )
            .addParameter(
                ParameterSpec.builder("verboseLogs", Boolean::class).defaultValue("false")
                    .build()
            )
            .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig"))
            .addStatement(
                "return %T(schema, syncTables, uploadLimit, downloadLimit, verboseLogs = verboseLogs)",
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

    private fun buildNewOversqliteClientFunction(): FunSpec {
        return FunSpec.builder("newOversqliteClient")
            .addKdoc("Creates a DefaultOversqliteClient bound to this DB using a pre-configured HttpClient with authentication and base URL.")
            .addParameter("schema", String::class)
            .addParameter("httpClient", ClassName("io.ktor.client", "HttpClient"))
            .addParameter(
                ParameterSpec.builder(
                    "resolver",
                    ClassName("dev.goquick.sqlitenow.oversqlite", "Resolver")
                ).defaultValue(
                    "%T",
                    ClassName("dev.goquick.sqlitenow.oversqlite", "ServerWinsResolver")
                ).build()
            )
            .addParameter(
                ParameterSpec.builder("uploadLimit", Int::class).defaultValue("200").build()
            )
            .addParameter(
                ParameterSpec.builder("downloadLimit", Int::class).defaultValue("1000")
                    .build()
            )
            .addParameter(
                ParameterSpec.builder("verboseLogs", Boolean::class).defaultValue("false")
                    .build()
            )
            .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteClient"))
            .addStatement("val cfg = buildOversqliteConfig(schema, uploadLimit, downloadLimit, verboseLogs)")
            .addStatement(
                "return %T(db = this.connection(), config = cfg, http = httpClient, resolver = resolver)",
                ClassName("dev.goquick.sqlitenow.oversqlite", "DefaultOversqliteClient"),
            )
            .build()
    }
}
