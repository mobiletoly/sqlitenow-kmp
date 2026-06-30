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
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement


internal class SwiftKotlinBridgeEmitter(
    private val databaseName: String,
    private val databasePackageName: String,
    private val config: SqliteNowSwiftExportConfig,
    private val oversqlite: Boolean,
    private val context: GeneratorContext,
    private val plan: SwiftGenerationPlan,
    private val model: SwiftLegacyExportModel,
) {
    private val swiftOversqliteEnabled: Boolean
        get() = oversqlite && context.createTableStatements.any { it.annotations.enableSync }

    fun emit(): String =
        SwiftWriter().apply {
            line("/*")
            line(" * Copyright 2025 Toly Pochkin")
            line(" *")
            line(" * Licensed under the Apache License, Version 2.0 (the \"License\");")
            line(" * you may not use this file except in compliance with the License.")
            line(" * You may obtain a copy of the License at")
            line(" *")
            line(" *     http://www.apache.org/licenses/LICENSE-2.0")
            line(" *")
            line(" * Unless required by applicable law or agreed to in writing, software")
            line(" * distributed under the License is distributed on an \"AS IS\" BASIS,")
            line(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.")
            line(" * See the License for the specific language governing permissions and")
            line(" * limitations under the License.")
            line(" */")
            line("package ${config.bridgePackageName}")
            line()
            line("import $databasePackageName.$databaseName as Generated$databaseName")
            line("import $databasePackageName.VersionBasedDatabaseMigrations")
            namespaces().forEach { namespace ->
                line("import $databasePackageName.${queryObjectName(namespace)}")
            }
            resultClasses().forEach { resultClass ->
                line("import $databasePackageName.$resultClass")
            }
            generatedAdapterProviders.keys.forEach { namespace ->
                line("import $databasePackageName.${plan.adapterClassNameFor(namespace)}")
            }
            if (swiftOversqliteEnabled) {
                line("import dev.goquick.sqlitenow.oversqlite.AttachResult")
                line("import dev.goquick.sqlitenow.oversqlite.BundleChangeWatchMode")
                line("import dev.goquick.sqlitenow.oversqlite.DetachOutcome")
                line("import dev.goquick.sqlitenow.oversqlite.OversqliteProgress")
                line("import dev.goquick.sqlitenow.oversqlite.PendingSyncStatus")
                line("import dev.goquick.sqlitenow.oversqlite.PushReport")
                line("import dev.goquick.sqlitenow.oversqlite.RemoteSyncReport")
                line("import dev.goquick.sqlitenow.oversqlite.RestoreSummary")
                line("import dev.goquick.sqlitenow.oversqlite.SourceInfo")
                line("import dev.goquick.sqlitenow.oversqlite.SyncReport")
                line("import dev.goquick.sqlitenow.oversqlite.SyncThenDetachResult")
                line("import dev.goquick.sqlitenow.oversqlite.SyncStatus")
                line("import dev.goquick.sqlitenow.oversqlite.runAutomaticDownloads")
                line("import io.ktor.client.HttpClient")
                line("import io.ktor.client.plugins.HttpTimeout")
                line("import io.ktor.client.plugins.auth.Auth")
                line("import io.ktor.client.plugins.auth.providers.BearerTokens")
                line("import io.ktor.client.plugins.auth.providers.bearer")
                line("import io.ktor.client.plugins.contentnegotiation.ContentNegotiation")
                line("import io.ktor.client.plugins.defaultRequest")
                line("import io.ktor.serialization.kotlinx.json.json")
                line("import kotlinx.serialization.json.Json")
            }
            line("import dev.goquick.sqlitenow.core.SelectRunners")
            line("import kotlin.Suppress")
            line("import kotlin.Throwable")
            if (selectStatements().isNotEmpty() || swiftOversqliteEnabled) {
                line("import kotlinx.coroutines.CancellationException")
            }
            line("import kotlinx.coroutines.CoroutineScope")
            line("import kotlinx.coroutines.Dispatchers")
            line("import kotlinx.coroutines.Job")
            line("import kotlinx.coroutines.SupervisorJob")
            line("import kotlinx.coroutines.cancelChildren")
            line("import kotlinx.coroutines.flow.collect")
            line("import kotlinx.coroutines.launch")
            line()
            emitBridgeErrorTypes()
            emitBridgeSyncTypes()
            emitBridgeAdapterResultTypes()
            emitBridgeResultTypes()
            emitBridgeParamTypes()
            emitBridgeSelectQueryTypes()
            emitBridgeExecuteReturningQueryTypes()
            emitBridgeMutationBatch()
            emitBridgeDatabase()
            emitBridgeSyncClient()
            emitBridgeMappingFunctions()
        }.toString()

    private fun SwiftWriter.emitBridgeErrorTypes() {
        line("public class ${databaseName}BridgeError(")
        indent {
            line("public val type: String,")
            line("public val message: String?,")
        }
        line(")")
        line()
        line("public class ${databaseName}Observation internal constructor(")
        indent {
            line("private val job: Job,")
        }
        line(") {")
        indent {
            line("public fun cancel() {")
            indent {
                line("job.cancel()")
            }
            line("}")
        }
        line("}")
        line()
        line("private fun Throwable.toBridgeError(): ${databaseName}BridgeError =")
        indent {
            line("${databaseName}BridgeError(")
            indent {
                line("type = this::class.simpleName ?: \"Throwable\",")
                line("message = message ?: toString(),")
            }
            line(")")
        }
        line()
    }

    private fun SwiftWriter.emitBridgeSyncTypes() {
        if (!swiftOversqliteEnabled) return

        line("public class ${databaseName}PendingSyncStatusBridge(")
        indent {
            line("public val hasPendingSyncData: Boolean,")
            line("public val pendingRowCount: Long,")
            line("public val blocksDetach: Boolean,")
        }
        line(")")
        line()
        line("public class ${databaseName}SyncStatusBridge(")
        indent {
            line("public val authority: String,")
            line("public val pending: ${databaseName}PendingSyncStatusBridge,")
            line("public val lastBundleSeqSeen: Long,")
        }
        line(")")
        line()
        line("public class ${databaseName}RestoreSummaryBridge(")
        indent {
            line("public val bundleSeq: Long,")
            line("public val rowCount: Long,")
        }
        line(")")
        line()
        line("public class ${databaseName}AttachResultBridge(")
        indent {
            line("public val kind: String,")
            line("public val outcome: String?,")
            line("public val status: ${databaseName}SyncStatusBridge?,")
            line("public val retryAfterSeconds: Long,")
            line("public val restore: ${databaseName}RestoreSummaryBridge?,")
        }
        line(")")
        line()
        line("public class ${databaseName}SourceInfoBridge(")
        indent {
            line("public val currentSourceId: String,")
            line("public val rebuildRequired: Boolean,")
            line("public val sourceRecoveryRequired: Boolean,")
            line("public val sourceRecoveryReason: String?,")
        }
        line(")")
        line()
        line("public class ${databaseName}DetachOutcomeBridge(")
        indent {
            line("public val outcome: String,")
        }
        line(")")
        line()
        line("public class ${databaseName}PushReportBridge(")
        indent {
            line("public val outcome: String,")
            line("public val status: ${databaseName}SyncStatusBridge,")
        }
        line(")")
        line()
        line("public class ${databaseName}RemoteSyncReportBridge(")
        indent {
            line("public val outcome: String,")
            line("public val status: ${databaseName}SyncStatusBridge,")
            line("public val restore: ${databaseName}RestoreSummaryBridge?,")
        }
        line(")")
        line()
        line("public class ${databaseName}SyncReportBridge(")
        indent {
            line("public val pushOutcome: String,")
            line("public val remoteOutcome: String,")
            line("public val status: ${databaseName}SyncStatusBridge,")
            line("public val restore: ${databaseName}RestoreSummaryBridge?,")
        }
        line(")")
        line()
        line("public class ${databaseName}SyncThenDetachResultBridge(")
        indent {
            line("public val lastSync: ${databaseName}SyncReportBridge,")
            line("public val detach: ${databaseName}DetachOutcomeBridge,")
            line("public val syncRounds: Int,")
            line("public val remainingPendingRowCount: Long,")
            line("public val success: Boolean,")
        }
        line(")")
        line()
        line("public class ${databaseName}ProgressBridge(")
        indent {
            line("public val kind: String,")
            line("public val operation: String?,")
            line("public val phase: String?,")
        }
        line(")")
        line()
        line("public class ${databaseName}AutomaticDownloadConfigBridge(")
        indent {
            line("public val automaticDownloadIntervalMillis: Long,")
            line("public val bundleChangeWatchMode: String,")
            line("public val bundleChangeWatchReconnectMinMillis: Long,")
            line("public val bundleChangeWatchReconnectMaxMillis: Long,")
        }
        line(")")
        line()
        line("private fun PendingSyncStatus.to${databaseName}PendingSyncStatusBridge(): ${databaseName}PendingSyncStatusBridge =")
        indent {
            line("${databaseName}PendingSyncStatusBridge(")
            indent {
                line("hasPendingSyncData = hasPendingSyncData,")
                line("pendingRowCount = pendingRowCount,")
                line("blocksDetach = blocksDetach,")
            }
            line(")")
        }
        line()
        line("private fun SyncStatus.to${databaseName}SyncStatusBridge(): ${databaseName}SyncStatusBridge =")
        indent {
            line("${databaseName}SyncStatusBridge(")
            indent {
                line("authority = authority.name,")
                line("pending = pending.to${databaseName}PendingSyncStatusBridge(),")
                line("lastBundleSeqSeen = lastBundleSeqSeen,")
            }
            line(")")
        }
        line()
        line("private fun RestoreSummary.to${databaseName}RestoreSummaryBridge(): ${databaseName}RestoreSummaryBridge =")
        indent {
            line("${databaseName}RestoreSummaryBridge(")
            indent {
                line("bundleSeq = bundleSeq,")
                line("rowCount = rowCount,")
            }
            line(")")
        }
        line()
        line("private fun AttachResult.to${databaseName}AttachResultBridge(): ${databaseName}AttachResultBridge =")
        indent {
            line("when (this) {")
            indent {
                line("is AttachResult.Connected -> ${databaseName}AttachResultBridge(")
                indent {
                    line("kind = \"connected\",")
                    line("outcome = outcome.name,")
                    line("status = status.to${databaseName}SyncStatusBridge(),")
                    line("retryAfterSeconds = 0,")
                    line("restore = restore?.to${databaseName}RestoreSummaryBridge(),")
                }
                line(")")
                line("is AttachResult.RetryLater -> ${databaseName}AttachResultBridge(")
                indent {
                    line("kind = \"retryLater\",")
                    line("outcome = null,")
                    line("status = null,")
                    line("retryAfterSeconds = retryAfterSeconds,")
                    line("restore = null,")
                }
                line(")")
            }
            line("}")
        }
        line()
        line("private fun SourceInfo.to${databaseName}SourceInfoBridge(): ${databaseName}SourceInfoBridge =")
        indent {
            line("${databaseName}SourceInfoBridge(")
            indent {
                line("currentSourceId = currentSourceId,")
                line("rebuildRequired = rebuildRequired,")
                line("sourceRecoveryRequired = sourceRecoveryRequired,")
                line("sourceRecoveryReason = sourceRecoveryReason?.name,")
            }
            line(")")
        }
        line()
        line("private fun DetachOutcome.to${databaseName}DetachOutcomeBridge(): ${databaseName}DetachOutcomeBridge =")
        indent {
            line("when (this) {")
            indent {
                line("DetachOutcome.DETACHED -> ${databaseName}DetachOutcomeBridge(outcome = \"DETACHED\")")
                line("DetachOutcome.BLOCKED_UNSYNCED_DATA -> ${databaseName}DetachOutcomeBridge(outcome = \"BLOCKED_UNSYNCED_DATA\")")
            }
            line("}")
        }
        line()
        line("private fun PushReport.to${databaseName}PushReportBridge(): ${databaseName}PushReportBridge =")
        indent {
            line("${databaseName}PushReportBridge(")
            indent {
                line("outcome = outcome.name,")
                line("status = status.to${databaseName}SyncStatusBridge(),")
            }
            line(")")
        }
        line()
        line("private fun RemoteSyncReport.to${databaseName}RemoteSyncReportBridge(): ${databaseName}RemoteSyncReportBridge =")
        indent {
            line("${databaseName}RemoteSyncReportBridge(")
            indent {
                line("outcome = outcome.name,")
                line("status = status.to${databaseName}SyncStatusBridge(),")
                line("restore = restore?.to${databaseName}RestoreSummaryBridge(),")
            }
            line(")")
        }
        line()
        line("private fun SyncReport.to${databaseName}SyncReportBridge(): ${databaseName}SyncReportBridge =")
        indent {
            line("${databaseName}SyncReportBridge(")
            indent {
                line("pushOutcome = pushOutcome.name,")
                line("remoteOutcome = remoteOutcome.name,")
                line("status = status.to${databaseName}SyncStatusBridge(),")
                line("restore = restore?.to${databaseName}RestoreSummaryBridge(),")
            }
            line(")")
        }
        line()
        line("private fun SyncThenDetachResult.to${databaseName}SyncThenDetachResultBridge(): ${databaseName}SyncThenDetachResultBridge =")
        indent {
            line("${databaseName}SyncThenDetachResultBridge(")
            indent {
                line("lastSync = lastSync.to${databaseName}SyncReportBridge(),")
                line("detach = detach.to${databaseName}DetachOutcomeBridge(),")
                line("syncRounds = syncRounds,")
                line("remainingPendingRowCount = remainingPendingRowCount,")
                line("success = isSuccess(),")
            }
            line(")")
        }
        line()
        line("private fun OversqliteProgress.to${databaseName}ProgressBridge(): ${databaseName}ProgressBridge =")
        indent {
            line("when (this) {")
            indent {
                line("OversqliteProgress.Idle -> ${databaseName}ProgressBridge(")
                indent {
                    line("kind = \"idle\",")
                    line("operation = null,")
                    line("phase = null,")
                }
                line(")")
                line("is OversqliteProgress.Active -> ${databaseName}ProgressBridge(")
                indent {
                    line("kind = \"active\",")
                    line("operation = operation.name,")
                    line("phase = phase.name,")
                }
                line(")")
            }
            line("}")
        }
        line()
        line("private fun ${databaseName}AutomaticDownloadConfigBridge.toBundleChangeWatchMode(): BundleChangeWatchMode =")
        indent {
            line("when (bundleChangeWatchMode) {")
            indent {
                line("\"AUTO\" -> BundleChangeWatchMode.AUTO")
                line("else -> BundleChangeWatchMode.OFF")
            }
            line("}")
        }
        line()
        line("private fun ${databaseName}AutomaticDownloadConfigBridge.toOversqliteAutomaticDownloadConfig(database: Generated$databaseName) =")
        indent {
            line("database.buildOversqliteAutomaticDownloadConfig(")
            indent {
                line("automaticDownloadIntervalMillis = automaticDownloadIntervalMillis,")
                line("bundleChangeWatchMode = toBundleChangeWatchMode(),")
                line("bundleChangeWatchReconnectMinMillis = bundleChangeWatchReconnectMinMillis,")
                line("bundleChangeWatchReconnectMaxMillis = bundleChangeWatchReconnectMaxMillis,")
            }
            line(")")
        }
        line()
    }

    private fun SwiftWriter.emitBridgeAdapterResultTypes() {
        adapterDescriptors().forEach { adapter ->
            line("public class ${adapter.resultClassName()} private constructor(")
            indent {
                line("private val value: ${adapter.outputKotlinType.asNullableKotlinType()},")
                line("private val failureMessage: String?,")
            }
            line(") {")
            indent {
                line("public companion object {")
                indent {
                    line("public fun success(value: ${adapter.outputKotlinType}): ${adapter.resultClassName()} =")
                    indent {
                        line("${adapter.resultClassName()}(value = value, failureMessage = null)")
                    }
                    line()
                    line("public fun failure(message: String?): ${adapter.resultClassName()} =")
                    indent {
                        line("${adapter.resultClassName()}(value = null, failureMessage = message)")
                    }
                }
                line("}")
                line()
                line("internal fun getOrThrow(): ${adapter.outputKotlinType} {")
                indent {
                    line("failureMessage?.let { message ->")
                    indent {
                        line("throw IllegalStateException(\"Adapter failed: \$message\")")
                    }
                    line("}")
                    line("@Suppress(\"UNCHECKED_CAST\")")
                    line("return value as ${adapter.outputKotlinType}")
                }
                line("}")
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitBridgeResultTypes() {
        resultStatements().forEach { result ->
            val resultName = result.name
            val bridgeName = resultName.swiftBridgeResultName()
            val fields = result.fields
            line("public class $bridgeName(")
            indent {
                fields.forEachIndexed { index, field ->
                    val suffix = if (index == fields.lastIndex) "" else ","
                    line("public val ${field.propertyName}: ${field.bridgeKotlinType}$suffix")
                }
            }
            line(")")
            line()
        }
    }

    private fun SwiftWriter.emitBridgeParamTypes() {
        statements().forEach { (namespace, statement) ->
            val params = parameterDescriptors(statement)
            if (params.isEmpty()) return@forEach
            val paramsName = statement.swiftParamsName(namespace)
            val bridgeName = paramsName.swiftBridgeParamsName()
            line("public class $bridgeName(")
            indent {
                params.forEachIndexed { index, param ->
                    val suffix = if (index == params.lastIndex) "" else ","
                    line("public val ${param.propertyName}: ${param.bridgeKotlinType}$suffix")
                }
            }
            line(")")
            line()
        }
    }

    private fun SwiftWriter.emitBridgeSelectQueryTypes() {
        selectStatements().forEach { (_, statement) ->
            val resultName = resultClassName(statement)
            val bridgeResultName = resultName.swiftBridgeResultName()
            val queryName = bridgeSelectQueryClassName(statement)
            line("public class $queryName internal constructor(")
            indent {
                line("private val runners: SelectRunners<$resultName>,")
                line("private val scope: CoroutineScope,")
            }
            line(") {")
            indent {
                line("@Throws(Exception::class)")
                line("public suspend fun list(): List<$bridgeResultName> =")
                indent {
                    line("runners.asList().map { it.to$bridgeResultName() }")
                }
                line()
                line("@Throws(Exception::class)")
                line("public suspend fun one(): $bridgeResultName =")
                indent {
                    line("runners.asOne().to$bridgeResultName()")
                }
                line()
                line("@Throws(Exception::class)")
                line("public suspend fun oneOrNull(): $bridgeResultName? =")
                indent {
                    line("runners.asOneOrNull()?.to$bridgeResultName()")
                }
                line()
                line("public fun observe(")
                indent {
                    line("onRows: (List<$bridgeResultName>) -> Unit,")
                    line("onError: (${databaseName}BridgeError) -> Unit,")
                }
                line("): ${databaseName}Observation {")
                indent {
                    line("val job = scope.launch {")
                    indent {
                        line("try {")
                        indent {
                            line("runners.asFlow().collect { rows ->")
                            indent {
                                line("onRows(rows.map { it.to$bridgeResultName() })")
                            }
                            line("}")
                        }
                        line("} catch (error: Throwable) {")
                        indent {
                            line("if (error is CancellationException) throw error")
                            line("onError(error.toBridgeError())")
                        }
                        line("}")
                    }
                    line("}")
                    line("return ${databaseName}Observation(job)")
                }
                line("}")
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitBridgeExecuteReturningQueryTypes() {
        executeReturningStatements().forEach { (_, statement) ->
            val resultName = resultClassName(statement)
            val bridgeResultName = resultName.swiftBridgeResultName()
            val queryName = bridgeExecuteReturningQueryClassName(statement)
            line("public class $queryName internal constructor(")
            indent {
                line("private val listBlock: suspend () -> List<$resultName>,")
                line("private val oneBlock: suspend () -> $resultName,")
                line("private val oneOrNullBlock: suspend () -> $resultName?,")
            }
            line(") {")
            indent {
                line("@Throws(Exception::class)")
                line("public suspend fun list(): List<$bridgeResultName> =")
                indent {
                    line("listBlock().map { it.to$bridgeResultName() }")
                }
                line()
                line("@Throws(Exception::class)")
                line("public suspend fun one(): $bridgeResultName =")
                indent {
                    line("oneBlock().to$bridgeResultName()")
                }
                line()
                line("@Throws(Exception::class)")
                line("public suspend fun oneOrNull(): $bridgeResultName? =")
                indent {
                    line("oneOrNullBlock()?.to$bridgeResultName()")
                }
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitBridgeMutationBatch() {
        line("public class ${databaseName}MutationBatch {")
        indent {
            line("internal val operations: MutableList<suspend (Generated$databaseName) -> Unit> = mutableListOf()")
            namespaces().forEach { namespace ->
                line("public val ${namespace.swiftNamespacePropertyName()}: ${namespace.swiftNamespaceTypeName("MutationBatch")} =")
                indent {
                    line("${namespace.swiftNamespaceTypeName("MutationBatch")}(this)")
                }
            }
            line()
            line("internal fun add(operation: suspend (Generated$databaseName) -> Unit) {")
            indent {
                line("operations += operation")
            }
            line("}")
        }
        line("}")
        line()
        namespaces().forEach { namespace ->
            val executeStatements = context.nsWithStatements[namespace].orEmpty()
                .filterIsInstance<AnnotatedExecuteStatement>()
                .filterNot { it.hasReturningClause() }
            line("public class ${namespace.swiftNamespaceTypeName("MutationBatch")} internal constructor(")
            indent {
                line("private val parent: ${databaseName}MutationBatch,")
            }
            line(") {")
            indent {
                executeStatements.forEach { statement ->
                    val params = parameterDescriptors(statement)
                    val functionName = statement.swiftFunctionName()
                    val paramsType = statement.swiftParamsName(namespace).swiftBridgeParamsName()
                    val argument = if (params.isEmpty()) "" else "params: $paramsType"
                    line("public fun $functionName($argument) {")
                    indent {
                        line("parent.add { database ->")
                        indent {
                            val routerCall = "database.${namespace.swiftNamespacePropertyName()}.${statement.kotlinRouterPropertyName()}"
                            if (params.isEmpty()) {
                                line("$routerCall()")
                            } else {
                                line("$routerCall(params.toGeneratedParams())")
                            }
                        }
                        line("}")
                    }
                    line("}")
                    line()
                }
            }
            line("}")
            line()
        }
    }

    private fun SwiftWriter.emitBridgeDatabase() {
        line("public class ${databaseName}Bridge(")
        indent {
            line("path: String,")
            adapterDescriptors().forEach { adapter ->
                line("${adapter.name}: (${adapter.inputKotlinType}) -> ${adapter.resultClassName()},")
            }
        }
        line(") {")
        indent {
            line("private val database = Generated$databaseName(")
            indent {
                line("dbName = path,")
                line("migration = VersionBasedDatabaseMigrations(),")
                generatedAdapterProviders.forEach { (namespace, adapters) ->
                    line("${plan.adapterPropertyNameFor(namespace)} = ${plan.adapterClassNameFor(namespace)}(")
                    indent {
                        adapters.forEach { adapter ->
                            line("${adapter.functionName} = { input -> ${adapter.functionName}(input).getOrThrow() },")
                        }
                    }
                    line("),")
                }
            }
            line(")")
            line("private val observationScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)")
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun open() {")
            indent {
                line("database.open()")
            }
            line("}")
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun close() {")
            indent {
                line("observationScope.coroutineContext.cancelChildren()")
                line("database.close()")
            }
            line("}")
            line()
            selectStatements().forEach { (namespace, statement) ->
                val params = parameterDescriptors(statement)
                val methodName = bridgeSelectMethodName(namespace, statement)
                val queryName = bridgeSelectQueryClassName(statement)
                val argument = if (params.isEmpty()) {
                    ""
                } else {
                    "params: ${statement.swiftParamsName(namespace).swiftBridgeParamsName()}"
                }
                line("public fun $methodName($argument): $queryName =")
                indent {
                    val runner = if (params.isEmpty()) {
                        "database.${namespace.swiftNamespacePropertyName()}.${statement.kotlinRouterPropertyName()}"
                    } else {
                        "database.${namespace.swiftNamespacePropertyName()}.${statement.kotlinRouterPropertyName()}(params.toGeneratedParams())"
                    }
                    line("$queryName($runner, observationScope)")
                }
                line()
            }
            executeReturningStatements().forEach { (namespace, statement) ->
                val params = parameterDescriptors(statement)
                val methodName = bridgeExecuteMethodName(namespace, statement)
                val queryName = bridgeExecuteReturningQueryClassName(statement)
                val argument = if (params.isEmpty()) {
                    ""
                } else {
                    "params: ${statement.swiftParamsName(namespace).swiftBridgeParamsName()}"
                }
                line("public fun $methodName($argument): $queryName =")
                indent {
                    val router = "database.${namespace.swiftNamespacePropertyName()}.${statement.kotlinRouterPropertyName()}"
                    line("$queryName(")
                    indent {
                        if (params.isEmpty()) {
                            line("listBlock = { $router() },")
                            line("oneBlock = { ${router}One() },")
                            line("oneOrNullBlock = { ${router}OneOrNull() },")
                        } else {
                            line("listBlock = { $router.list(params.toGeneratedParams()) },")
                            line("oneBlock = { $router.one(params.toGeneratedParams()) },")
                            line("oneOrNullBlock = { $router.oneOrNull(params.toGeneratedParams()) },")
                        }
                    }
                    line(")")
                }
                line()
            }
            executeStatements().filterNot { (_, statement) -> statement.hasReturningClause() }.forEach { (namespace, statement) ->
                val params = parameterDescriptors(statement)
                val methodName = bridgeExecuteMethodName(namespace, statement)
                val argument = if (params.isEmpty()) {
                    ""
                } else {
                    "params: ${statement.swiftParamsName(namespace).swiftBridgeParamsName()}"
                }
                line("@Throws(Exception::class)")
                line("public suspend fun $methodName($argument) {")
                indent {
                    val router = "database.${namespace.swiftNamespacePropertyName()}.${statement.kotlinRouterPropertyName()}"
                    if (params.isEmpty()) {
                        line("$router()")
                    } else {
                        line("$router(params.toGeneratedParams())")
                    }
                }
                line("}")
                line()
            }
            line("@Throws(Exception::class)")
            line("public suspend fun transaction(batch: ${databaseName}MutationBatch) {")
            indent {
                line("database.transaction {")
                indent {
                    line("batch.operations.forEach { operation ->")
                    indent {
                        line("operation(database)")
                    }
                    line("}")
                }
                line("}")
            }
            line("}")
            if (swiftOversqliteEnabled) {
                line()
                line("public fun makeSyncClient(")
                indent {
                    line("baseUrl: String,")
                    line("accessTokenProvider: () -> String,")
                    line("refreshedAccessTokenProvider: (() -> String?)?,")
                    line("schema: String,")
                    line("uploadLimit: Int,")
                    line("downloadLimit: Int,")
                    line("verboseLogs: Boolean,")
                }
                line("): ${databaseName}SyncBridge =")
                indent {
                    line("${databaseName}SyncBridge(")
                    indent {
                        line("database = database,")
                        line("baseUrl = baseUrl,")
                        line("accessTokenProvider = accessTokenProvider,")
                        line("refreshedAccessTokenProvider = refreshedAccessTokenProvider,")
                        line("schema = schema,")
                        line("uploadLimit = uploadLimit,")
                        line("downloadLimit = downloadLimit,")
                        line("verboseLogs = verboseLogs,")
                    }
                    line(")")
                }
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitBridgeSyncClient() {
        if (!swiftOversqliteEnabled) return

        line("private class ${databaseName}SyncAuth(")
        indent {
            line("private val accessTokenProvider: () -> String,")
            line("private val refreshedAccessTokenProvider: (() -> String?)?,")
        }
        line(") {")
        indent {
            line("fun bearerTokens(): BearerTokens = BearerTokens(accessTokenProvider(), null)")
            line()
            line("fun refreshTokens(): BearerTokens? {")
            indent {
                line("val token = refreshedAccessTokenProvider?.invoke() ?: accessTokenProvider()")
                line("return BearerTokens(token, null)")
            }
            line("}")
        }
        line("}")
        line()
        line("private fun ${databaseName}SyncHttpClient(")
        indent {
            line("baseUrl: String,")
            line("auth: ${databaseName}SyncAuth,")
        }
        line("): HttpClient =")
        indent {
            line("HttpClient {")
            indent {
                line("install(HttpTimeout) {")
                indent {
                    line("requestTimeoutMillis = 30_000L")
                    line("connectTimeoutMillis = 10_000L")
                    line("socketTimeoutMillis = 90_000L")
                }
                line("}")
                line("install(ContentNegotiation) {")
                indent {
                    line("json(Json { ignoreUnknownKeys = true })")
                }
                line("}")
                line("install(Auth) {")
                indent {
                    line("bearer {")
                    indent {
                        line("loadTokens {")
                        indent {
                            line("auth.bearerTokens()")
                        }
                        line("}")
                        line("refreshTokens {")
                        indent {
                            line("auth.refreshTokens()")
                        }
                        line("}")
                        line("sendWithoutRequest { true }")
                        line("cacheTokens = false")
                        line("nonCancellableRefresh = true")
                    }
                    line("}")
                }
                line("}")
                line("defaultRequest {")
                indent {
                    line("url(baseUrl)")
                }
                line("}")
            }
            line("}")
        }
        line()
        line("public class ${databaseName}SyncBridge internal constructor(")
        indent {
            line("private val database: Generated$databaseName,")
            line("baseUrl: String,")
            line("accessTokenProvider: () -> String,")
            line("refreshedAccessTokenProvider: (() -> String?)?,")
            line("schema: String,")
            line("uploadLimit: Int,")
            line("downloadLimit: Int,")
            line("verboseLogs: Boolean,")
        }
        line(") {")
        indent {
            line("private val auth = ${databaseName}SyncAuth(accessTokenProvider, refreshedAccessTokenProvider)")
            line("private val httpClient = ${databaseName}SyncHttpClient(baseUrl, auth)")
            line("private val client = database.newOversqliteClient(")
            indent {
                line("schema = schema,")
                line("httpClient = httpClient,")
                line("uploadLimit = uploadLimit,")
                line("downloadLimit = downloadLimit,")
                line("verboseLogs = verboseLogs,")
            }
            line(")")
            line("private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)")
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun open() {")
            indent {
                line("client.open().getOrThrow()")
            }
            line("}")
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun attach(userId: String): ${databaseName}AttachResultBridge =")
            indent {
                line("client.attach(userId).getOrThrow().to${databaseName}AttachResultBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun sourceInfo(): ${databaseName}SourceInfoBridge =")
            indent {
                line("client.sourceInfo().getOrThrow().to${databaseName}SourceInfoBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun syncStatus(): ${databaseName}SyncStatusBridge =")
            indent {
                line("client.syncStatus().getOrThrow().to${databaseName}SyncStatusBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun detach(): ${databaseName}DetachOutcomeBridge =")
            indent {
                line("client.detach().getOrThrow().to${databaseName}DetachOutcomeBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun pushPending(): ${databaseName}PushReportBridge =")
            indent {
                line("client.pushPending().getOrThrow().to${databaseName}PushReportBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun pullToStable(): ${databaseName}RemoteSyncReportBridge =")
            indent {
                line("client.pullToStable().getOrThrow().to${databaseName}RemoteSyncReportBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun sync(): ${databaseName}SyncReportBridge =")
            indent {
                line("client.sync().getOrThrow().to${databaseName}SyncReportBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun syncThenDetach(): ${databaseName}SyncThenDetachResultBridge =")
            indent {
                line("client.syncThenDetach().getOrThrow().to${databaseName}SyncThenDetachResultBridge()")
            }
            line()
            line("@Throws(Exception::class)")
            line("public suspend fun rebuild(): ${databaseName}RemoteSyncReportBridge =")
            indent {
                line("client.rebuild().getOrThrow().to${databaseName}RemoteSyncReportBridge()")
            }
            line()
            line("public fun observeProgress(")
            indent {
                line("onProgress: (${databaseName}ProgressBridge) -> Unit,")
                line("onError: (${databaseName}BridgeError) -> Unit,")
                line("onComplete: () -> Unit,")
            }
            line("): ${databaseName}Observation {")
            indent {
                line("val job = scope.launch {")
                indent {
                    line("try {")
                    indent {
                        line("client.progress.collect { progress ->")
                        indent {
                            line("onProgress(progress.to${databaseName}ProgressBridge())")
                        }
                        line("}")
                        line("onComplete()")
                    }
                    line("} catch (error: Throwable) {")
                    indent {
                        line("if (error is CancellationException) {")
                        indent {
                            line("onComplete()")
                            line("throw error")
                        }
                        line("}")
                        line("onError(error.toBridgeError())")
                    }
                    line("}")
                }
                line("}")
                line("return ${databaseName}Observation(job)")
            }
            line("}")
            line()
            line("public fun startAutomaticDownloads(")
            indent {
                line("config: ${databaseName}AutomaticDownloadConfigBridge,")
                line("onError: (${databaseName}BridgeError) -> Unit,")
            }
            line("): ${databaseName}Observation {")
            indent {
                line("val job = scope.launch {")
                indent {
                    line("try {")
                    indent {
                        line("client.runAutomaticDownloads(config.toOversqliteAutomaticDownloadConfig(database))")
                    }
                    line("} catch (error: Throwable) {")
                    indent {
                        line("if (error is CancellationException) throw error")
                        line("onError(error.toBridgeError())")
                    }
                    line("}")
                }
                line("}")
                line("return ${databaseName}Observation(job)")
            }
            line("}")
            line()
            line("public fun close() {")
            indent {
                line("scope.coroutineContext.cancelChildren()")
                line("client.close()")
                line("httpClient.close()")
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitBridgeMappingFunctions() {
        statements().forEach { (namespace, statement) ->
            val params = parameterDescriptors(statement)
            if (params.isEmpty()) return@forEach
            val paramsName = statement.swiftParamsName(namespace)
            val bridgeName = paramsName.swiftBridgeParamsName()
            line("private fun $bridgeName.toGeneratedParams(): ${queryObjectName(namespace)}.${statement.kotlinQueryObjectName()}.Params =")
            indent {
                line("${queryObjectName(namespace)}.${statement.kotlinQueryObjectName()}.Params(")
                indent {
                    params.forEach { param ->
                        line("${param.propertyName} = ${param.propertyName},")
                    }
                }
                line(")")
            }
            line()
        }
        resultStatements().forEach { result ->
            val resultName = result.name
            val bridgeResultName = resultName.swiftBridgeResultName()
            val fields = result.fields
            line("private fun $resultName.to$bridgeResultName(): $bridgeResultName =")
            indent {
                line("$bridgeResultName(")
                indent {
                    fields.forEach { field ->
                        line("${field.propertyName} = ${field.bridgeResultExpression(field.propertyName)},")
                    }
                }
                line(")")
            }
            line()
        }
    }


    private val generatedAdapterProviders
        get() = model.generatedAdapterProviders

    private fun parameterDescriptors(statement: AnnotatedStatement): List<SwiftLegacyParameter> =
        model.parameterDescriptors(statement)

    private fun adapterDescriptors(): List<SwiftAdapterDescriptor> =
        model.adapterDescriptors()

    private fun namespaces(): List<String> = model.namespaces()

    private fun resultStatements(): List<SwiftLegacyResult> = model.resultStatements()

    private fun statements(): List<Pair<String, AnnotatedStatement>> = model.statements()

    private fun selectStatements(): List<Pair<String, AnnotatedSelectStatement>> = model.selectStatements()

    private fun executeStatements(): List<Pair<String, AnnotatedExecuteStatement>> = model.executeStatements()

    private fun executeReturningStatements(): List<Pair<String, AnnotatedExecuteStatement>> =
        model.executeReturningStatements()

    private fun resultClasses(): List<String> = model.resultClasses()

    private fun resultClassName(statement: AnnotatedStatement): String = model.resultClassName(statement)

    private fun queryObjectName(namespace: String): String = model.queryObjectName(namespace)

    private fun bridgeSelectMethodName(namespace: String, statement: AnnotatedSelectStatement): String =
        model.bridgeSelectMethodName(namespace, statement)

    private fun bridgeExecuteMethodName(namespace: String, statement: AnnotatedExecuteStatement): String =
        model.bridgeExecuteMethodName(namespace, statement)

    private fun bridgeSelectQueryClassName(statement: AnnotatedSelectStatement): String =
        model.bridgeSelectQueryClassName(statement)

    private fun bridgeExecuteReturningQueryClassName(statement: AnnotatedExecuteStatement): String =
        model.bridgeExecuteReturningQueryClassName(statement)

}
