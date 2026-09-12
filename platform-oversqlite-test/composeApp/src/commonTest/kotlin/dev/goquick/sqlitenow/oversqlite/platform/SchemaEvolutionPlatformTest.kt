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
package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlanMismatchException
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.platformSuiteEnabled
import dev.goquick.sqlitenow.oversqlite.platformsupport.openAndConnect
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpHeaders
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class SchemaEvolutionPlatformEvolutionTest : PlatformCrossTargetTestSupport() {
    @Test
    fun addedTableNamespaceCollisionRejectsBeforeU0AndOldContractReopens() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        // The Node worker is deliberately memory-only across coordinated close/reopen. Browser,
        // Wasm, JVM, and native targets provide the durable reopen required by this edge test.
        if (webRuntimeKind() == "js-node") return@runTest
        val old = testGenerationResponse(testRegisteredTableSpecs("users"))
        val target = Json.parseToJsonElement(
            """{"protocol_version":"v1","schema_generation":{"id":"2","fingerprint":"310292465a46d16aca4560836e87fc558cb2a8e425ea1738487cad02b4660e15","tables":[{"schema":"main","table":"tags","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"label","wire_type":"text","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"users","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"name","wire_type":"text","nullable":false}],"foreign_keys":[]}]}}""",
        ) as JsonObject
        val oldConfig = config(1, listOf("users"))
        val targetConfig = config(2, listOf("tags", "users"))
        val db = newCoordinatedDb()
        val initialServer = MockSyncServer(testRegisteredTableSpecs("users"))
        val initialHttp = initialServer.newHttpClient()
        var targetHttp: HttpClient? = null
        var recoveryHttp: HttpClient? = null
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            db.openClient(oldConfig, initialHttp, ServerWinsResolver)
                .openAndConnect("user-1")
                .getOrThrow()

            db.close()
            val inspection = db.reopenForInspection()
            try {
                inspection.execSQL("CREATE TABLE tags (id TEXT PRIMARY KEY NOT NULL, label TEXT NOT NULL)")
            } finally {
                inspection.close()
            }

            val upgradeHttp = generationHttp(old, target)
            targetHttp = upgradeHttp
            val failure = assertFailsWith<OversqliteUpgradePlanMismatchException> {
                db.openClient(
                    config = targetConfig,
                    http = upgradeHttp,
                    resolver = ServerWinsResolver,
                    upgradePlan = OversqliteUpgradePlan(
                        migrationId = "platform-schemaevolution-g1-g2",
                        fromGenerationId = 1,
                        toGenerationId = 2,
                        fromLocalVersion = 1,
                        toLocalVersion = 1,
                        preservedLocalTables = emptySet(),
                    ),
                )
            }
            assertEquals("schema_evolution_compatibility_new_table_namespace", failure.message)

            recoveryHttp = initialServer.newHttpClient()
            db.openClient(oldConfig, recoveryHttp, ServerWinsResolver)
        } finally {
            runCatching { db.close() }
            initialHttp.close()
            targetHttp?.close()
            recoveryHttp?.close()
            cleanupOwnedWorkerStorage()
        }
    }

    private fun config(generationId: Long, tables: List<String>): OversqliteConfig = OversqliteConfig(
        generationId = generationId,
        databaseId = "platform-schemaevolution",
        serviceId = "platform-mock",
        syncTables = tables.map { SyncTable(it, "id") },
    )

    private fun generationHttp(old: JsonObject, target: JsonObject): HttpClient {
        val json = Json { encodeDefaults = true }
        return HttpClient(MockEngine { request ->
            when (request.url.encodedPath) {
                "/sync/generations/2" -> respond(
                    testDiscoveryResponse(target).toString(),
                    headers = headersOf(HttpHeaders.ContentType, "application/json"),
                )
                else -> error("Unexpected schemaevolution platform request: ${request.url.encodedPath}")
            }
        }) {
            install(ContentNegotiation) { json(json) }
            defaultRequest { url("https://sync.test") }
        }
    }
}
