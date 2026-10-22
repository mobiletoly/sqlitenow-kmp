package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponseContainer
import io.ktor.client.statement.HttpResponsePipeline
import io.ktor.client.statement.bodyAsChannel
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.encodedPath
import io.ktor.serialization.kotlinx.json.json
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readBuffer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.io.readByteArray
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Actual browser Fetch and normal SQLite worker; all artifacts belong to the parent. */
internal class WireByteArtifactTest : RealServerSupport() {
    private class Capture(var workload: String = "setup", var stage: String = "prepare") {
        val exchanges = mutableListOf<JsonObject>()
    }

    @Test fun collectSevenTransportWorkloads() = runOwnedWebDatabaseTest(180.seconds) {
        withContext(Dispatchers.Default) {
            if (realServerEnv("OVERSQLITE_REALSERVER_TESTS") != "true") {
                println("BODY_NOT_EXECUTED: wire Fetch requires explicit isolated realserver activation")
                return@withContext
            }
            val base = checkNotNull(realServerEnv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL"))
            check(base.startsWith("http://127.0.0.1:"))
            val control = newRealServerHttpClient(base)
            try {
                val status = json.parseToJsonElement(control.get("/syncx/status").bodyAsText()).jsonObject
                assertEquals("oversqlite-qualification-wire", status.getValue("app_name").jsonPrimitive.content)
                val envelope = json.parseToJsonElement(control.get("/qualification/wire/manifest").bodyAsText()).jsonObject
                val nonce = envelope.getValue("nonce").jsonPrimitive.content
                val runtime = envelope.getValue("runtime").jsonPrimitive.content
                assertTrue(runtime in setOf("browser", "wasm"))
                val manifest = envelope.getValue("manifest").jsonObject
                val workloads = manifest.getValue("workloads").jsonArray
                assertEquals(7, workloads.size)
                val generation = manifest.getValue("generation").jsonObject
                val ddl = manifest.getValue("sqlite_ddl").jsonPrimitive.content
                val capture = Capture()
                for ((index, element) in workloads.withIndex()) {
                    val input = element.jsonObject
                    val id = input.getValue("id").jsonPrimitive.content
                    capture.workload = id; capture.stage = "prepare"
                    if (id == "deterministic-incompressible-above-threshold") {
                        val http = capturedClient(base, null, nonce, capture)
                        try {
                            capture.stage = "measure"
                            val response = http.get("/qualification/wire/incompressible")
                            assertEquals(200, response.status.value)
                            val bytes = response.bodyAsChannel().readBuffer(4097L).readByteArray()
                            assertEquals(4096, bytes.size)
                            assertEquals(manifest.getValue("opaque_sha256").jsonPrimitive.content, sha256Hex(bytes))
                        } finally { http.close() }
                    } else if (id == "large-snapshot-chunk") {
                        val seed = envelope.getValue("snapshot_seed").jsonObject
                        val user = "wire-$nonce-$index"
                        assertEquals(user, seed.getValue("user").jsonPrimitive.content)
                        assertEquals(nonce, seed.getValue("nonce").jsonPrimitive.content)
                        assertEquals(1000, seed.getValue("rows").jsonPrimitive.int)
                        assertEquals(input.getValue("rows_sha256"), seed.getValue("rows_sha256"))
                        assertEquals("ScopeManager.ExecWrite", seed.getValue("route").jsonPrimitive.content)
                        capture.stage = "measure"
                        val follower = openOwner(base, user, ddl, nonce, capture)
                        try {
                            follower.client.pullToStable().getOrThrow()
                            verifyState(follower.db, generation, input.getValue("rows").jsonArray)
                        } finally { follower.close() }
                    } else {
                        val user = "wire-$nonce-$index"
                        val writer = openOwner(base, user, ddl, nonce, capture,
                            pushRows = if (id == "mixed-table-push") 201 else 200)
                        var follower: Owner? = null
                        try {
                            if (id == "ordinary-multi-bundle-pull") {
                                follower = openOwner(base, user, ddl, nonce, capture)
                                follower.client.pullToStable().getOrThrow()
                            }
                            if (id == "small-single-row-delete") {
                                val initial = workloads.single { it.jsonObject.getValue("id").jsonPrimitive.content == "small-single-row-insert" }.jsonObject
                                applyRows(writer.db, initial.getValue("rows").jsonArray)
                                writer.client.pushPending().getOrThrow()
                            }
                            val expected = if (id == "ordinary-multi-bundle-pull") {
                                val batches = input.getValue("bundles").jsonArray
                                for (batch in batches) {
                                    applyRows(writer.db, batch.jsonArray)
                                    writer.client.pushPending().getOrThrow()
                                }
                                capture.stage = "measure"
                                checkNotNull(follower).client.pullToStable().getOrThrow()
                                JsonArray(batches.flatMap { it.jsonArray })
                            } else {
                                val rows = input.getValue("rows").jsonArray
                                applyRows(writer.db, rows); capture.stage = "measure"
                                writer.client.pushPending().getOrThrow()
                                capture.stage = "measure"
                                follower = openOwner(base, user, ddl, nonce, capture)
                                follower.client.pullToStable().getOrThrow()
                                if (id == "small-single-row-delete") JsonArray(emptyList()) else rows
                            }
                            verifyState(checkNotNull(follower).db, generation, expected)
                        } finally {
                            try { follower?.close() } finally { writer.close() }
                        }
                    }
                    capture.stage = "receipt"
                    val exchanges = capture.exchanges.filter { it.getValue("workload").jsonPrimitive.content == id }
                    assertTrue(exchanges.isNotEmpty())
                    val ordinals = exchanges.map { it.getValue("ordinal").jsonPrimitive.int }
                    assertEquals(ordinals.size, ordinals.toSet().size)
                    val evidence = buildJsonObject {
                        put("nonce", nonce); put("runtime", runtime); put("workload", id)
                        put("manifest_sha256", envelope.getValue("manifest_sha256"))
                        put("expected_rows", input.getValue("expected_rows"))
                        put("full_state_verified", true); put("exchanges", JsonArray(exchanges))
                    }
                    val receipt = buildJsonObject {
                        put("nonce", nonce); put("runtime", runtime); put("workload", id); put("evidence", evidence)
                    }
                    val response = control.post("/qualification/wire/receipt") {
                        header(HttpHeaders.ContentType, ContentType.Application.Json.toString()); setBody(receipt.toString())
                    }
                    assertEquals(204, response.status.value)
                    println("QUALIFICATION_WIRE_WORKLOAD_COMPLETE runtime=$runtime nonce=$nonce workload=$id rows=${input.getValue("expected_rows")}")
                }
            } finally { control.close() }
        }
    }

    private fun capturedClient(base: String, token: String?, nonce: String, capture: Capture): HttpClient = HttpClient {
        install(ContentNegotiation) { json(json) }
        defaultRequest {
            url(base)
            if (token != null) header(HttpHeaders.Authorization, "Bearer $token")
            header("X-Qualification-Workload", capture.workload)
            header("X-Qualification-Stage", capture.stage)
        }
        install("QualificationActualDecodedBytes") {
            responsePipeline.intercept(HttpResponsePipeline.Receive) {
                val route = context.request.url.encodedPath
                val channel = subject.response as? ByteReadChannel
                if (channel != null && (route.startsWith("/sync/") || route == "/qualification/wire/incompressible")) {
                    val bytes = channel.readBuffer((64L shl 20) + 1).readByteArray()
                    check(bytes.size <= (64 shl 20))
                    val ordinal = checkNotNull(context.response.headers["X-Qualification-Ordinal"]).toInt()
                    assertEquals(nonce, context.response.headers["X-Qualification-Nonce"])
                    capture.exchanges += buildJsonObject {
                        put("ordinal", ordinal); put("nonce", nonce); put("route", route)
                        put("workload", capture.workload); put("stage", capture.stage)
                        put("status", context.response.status.value)
                        put("response_decoded_sha256", sha256Hex(bytes)); put("decoded_body_bytes", bytes.size)
                    }
                    // Preserve the exact Fetch-decoded bytes for the actual SDK receiver.
                    proceedWith(HttpResponseContainer(subject.expectedType, ByteReadChannel(bytes)))
                } else proceed()
            }
        }
    }

    private inner class Owner(val db: SafeSQLiteConnection, val http: HttpClient, val client: DefaultOversqliteClient) {
        suspend fun close() { try { client.closeFromCoordinatedOwner() } finally { try { http.close() } finally { db.close() } } }
    }

    private suspend fun openOwner(base: String, user: String, ddl: String, nonce: String, capture: Capture, pushRows: Int = 200): Owner {
        val db = newDb()
        try {
            for (statement in ddl.split(';').filter { it.isNotBlank() }) db.execSQL(statement)
            db.execSQL("PRAGMA synchronous=FULL"); db.execSQL("PRAGMA foreign_keys=ON")
            val source = bootstrapManagedSourceId(db, base, syncTables = businessRichSyncTables)
            val token = issueDummySigninToken(base, user, source)
            val http = capturedClient(base, token, nonce, capture)
            try {
                val client = DefaultOversqliteClient.forTesting(db, OversqliteConfig(
                    generationId = 1, databaseId = "fixture", serviceId = "business", syncTables = businessRichSyncTables,
                    // The fixed mixed workload is one 201-row request; this is a soft caller target.
                    transferTargets = OversqliteTransferTargets(maxPushRows = pushRows),
                ), http)
                try { client.open().getOrThrow(); client.attach(user).getOrThrow(); return Owner(db, http, client) }
                catch (failure: Throwable) { try { client.closeFromCoordinatedOwner() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }; throw failure }
            } catch (failure: Throwable) { http.close(); throw failure }
        } catch (failure: Throwable) { try { db.close() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }; throw failure }
    }

    private fun sqlValue(value: JsonElement): String = if (value == JsonNull) "NULL" else "'${value.jsonPrimitive.content.replace("'", "''")}'"
    private suspend fun applyRows(db: SafeSQLiteConnection, rows: JsonArray) = db.transaction(TransactionMode.IMMEDIATE) {
        for (element in rows) {
            val row = element.jsonObject; val table = row.getValue("table").jsonPrimitive.content
            check(table in setOf("categories", "users", "posts"))
            if (row.getValue("op").jsonPrimitive.content == "DELETE") {
                db.execSQL("DELETE FROM \"$table\" WHERE id=${sqlValue(row.getValue("key").jsonObject.getValue("id"))}")
            } else {
                val payload = row.getValue("payload").jsonObject.entries.sortedBy { it.key }
                val columns = payload.joinToString(",") { "\"${it.key}\"" }
                db.execSQL("INSERT INTO \"$table\" ($columns) VALUES (${payload.joinToString(",") { sqlValue(it.value) }})")
            }
        }
    }

    private suspend fun verifyState(db: SafeSQLiteConnection, generation: JsonObject, expected: JsonArray) {
        val byTable = expected.map { it.jsonObject }.groupBy { it.getValue("table").jsonPrimitive.content }
        for (element in generation.getValue("tables").jsonArray) {
            val table = element.jsonObject; val name = table.getValue("table").jsonPrimitive.content
            val columns = table.getValue("columns").jsonArray.map { it.jsonObject.getValue("name").jsonPrimitive.content }
            val actual = db.queryList("SELECT ${columns.joinToString(",") { "\"$it\"" }} FROM \"$name\" ORDER BY id") { statement ->
                buildJsonObject { for ((index, column) in columns.withIndex()) put(column, if (statement.isNull(index)) JsonNull else JsonPrimitive(statement.getText(index))) }
            }
            val wanted = byTable[name].orEmpty().map { it.getValue("payload").jsonObject }.sortedBy { it.getValue("id").jsonPrimitive.content }
            assertEquals(wanted, actual, "complete typed business table $name")
        }
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
    }
}
