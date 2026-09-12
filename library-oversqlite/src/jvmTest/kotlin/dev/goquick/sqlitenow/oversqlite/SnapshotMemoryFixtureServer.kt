package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

private const val fixtureSnapshotId = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa"

fun main(args: Array<String>) {
    check(System.getenv("GITHUB_ACTIONS") != "true") { "snapshot memory fixture is local-only" }
    require(args.size in 2..3) { "usage: SnapshotMemoryFixtureServer <rows> <row-bytes> [snapshot|pull]" }
    SnapshotMemoryFixtureServer(
        rowCount = args[0].toInt(),
        targetRowBytes = args[1].toInt(),
        mode = args.getOrNull(2) ?: "snapshot",
    ).run()
}

private class SnapshotMemoryFixtureServer(
    private val rowCount: Int,
    private val targetRowBytes: Int,
    private val mode: String,
) {
    private val generation = testGeneration()
    private val shutdown = CountDownLatch(1)
    private val chunks = AtomicLong()
    private val maxChunkRows = AtomicLong()
    private val maxChunkBytes = AtomicLong()
    private val pullBodyBytes = AtomicLong()
    private val executor = Executors.newFixedThreadPool(2)
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        this.executor = this@SnapshotMemoryFixtureServer.executor
    }

    fun run() {
        require(rowCount > 0 && targetRowBytes >= 256)
        require(mode == "snapshot" || mode == "pull")
        try {
            installRoutes()
            server.start()
            println("snapshot_memory_server_ready port=${server.address.port} pid=${ProcessHandle.current().pid()}")
            System.out.flush()
            shutdown.await()
            println("snapshot_memory_server_final chunks=${chunks.get()} " +
                "server_max_chunk_rows=${maxChunkRows.get()} server_max_chunk_bytes=${maxChunkBytes.get()} " +
                "pull_body_bytes=${pullBodyBytes.get()}")
            System.out.flush()
        } finally {
            server.stop(0)
            executor.shutdownNow()
            check(executor.awaitTermination(5, TimeUnit.SECONDS)) { "fixture executor did not stop" }
        }
    }

    private fun installRoutes() {
        server.createContext("/sync/generations/1") { exchange ->
            respondJson(exchange, Json.encodeToString(
                GenerationDiscovery.serializer(),
                GenerationDiscovery("v1", generation, testWireLimits(), watchEnabled = false),
            ))
        }
        server.createContext("/sync/connect") { exchange ->
            respondJson(exchange, """{"resolution":"remote_authoritative"}""")
        }
        server.createContext("/sync/snapshot-sessions/$fixtureSnapshotId") { exchange ->
            when (exchange.requestMethod) {
                "GET" -> respondChunk(exchange)
                "DELETE" -> { exchange.sendResponseHeaders(204, -1); exchange.close() }
                else -> methodNotAllowed(exchange)
            }
        }
        server.createContext("/sync/snapshot-sessions") { exchange ->
            if (exchange.requestMethod != "POST") return@createContext methodNotAllowed(exchange)
            val sampleBytes = OversqliteCompactCodec.snapshotLogicalByteCount(generation, listOf(row(1)))
            respondJson(exchange, buildJsonObject {
                put("snapshot_id", JsonPrimitive(fixtureSnapshotId))
                put("snapshot_bundle_seq", JsonPrimitive(1))
                put("row_count", JsonPrimitive(rowCount))
                put("logical_byte_count", JsonPrimitive(Math.multiplyExact(sampleBytes, rowCount.toLong())))
                put("expires_at", JsonPrimitive("2099-01-01T00:00:00Z"))
            }.toString())
        }
        server.createContext("/sync/pull") { exchange ->
            if (mode != "pull") return@createContext respondJson(exchange,
                """{"stable_bundle_seq":0,"bundles":[],"has_more":false}""")
            val rows = (1..rowCount).map(::row)
            val body = buildJsonObject {
                put("stable_bundle_seq", JsonPrimitive(1))
                put("bundles", kotlinx.serialization.json.buildJsonArray {
                    add(buildJsonObject {
                        put("bundle_seq", JsonPrimitive(1))
                        put("source_id", JsonPrimitive("fixture-source"))
                        put("source_bundle_id", JsonPrimitive(1))
                        put("bundle_hash", JsonPrimitive(canonicalCommittedBundleHash(rows)))
                        put("rows", OversqliteCompactCodec.encodeBundleRows(generation, rows))
                    })
                })
                put("has_more", JsonPrimitive(false))
            }.toString()
            pullBodyBytes.set(body.encodeToByteArray().size.toLong())
            respondJson(exchange, body)
        }
        server.createContext("/__shutdown") { exchange ->
            if (exchange.requestMethod != "POST") return@createContext methodNotAllowed(exchange)
            exchange.sendResponseHeaders(204, -1)
            exchange.close()
            shutdown.countDown()
        }
    }

    private fun respondChunk(exchange: HttpExchange) {
        val query = exchange.requestURI.rawQuery.orEmpty().split('&').associate {
            it.substringBefore('=') to it.substringAfter('=')
        }
        val after = query.getValue("after_row_ordinal").toInt()
        val requestedRows = query.getValue("max_rows").toInt()
        val requestedBytes = query.getValue("max_bytes").toLong()
        val count = minOf(requestedRows, rowCount - after,
            (requestedBytes / (targetRowBytes + 64L)).toInt().coerceAtLeast(1))
        check(count > 0)
        val rows = ((after + 1)..(after + count)).map(::row)
        val body = buildJsonObject {
            put("rows", OversqliteCompactCodec.encodeBundleRows(
                generation, rows, firstOrdinal = after + 1L, snapshot = true))
            put("next_row_ordinal", JsonPrimitive(after + count))
            put("has_more", JsonPrimitive(after + count < rowCount))
            put("logical_byte_count", JsonPrimitive(OversqliteCompactCodec.snapshotLogicalByteCount(generation, rows)))
        }.toString()
        check(body.encodeToByteArray().size.toLong() <= requestedBytes)
        chunks.incrementAndGet()
        maxChunkRows.accumulateAndGet(count.toLong(), ::maxOf)
        maxChunkBytes.accumulateAndGet(body.encodeToByteArray().size.toLong(), ::maxOf)
        respondJson(exchange, body)
    }

    private fun row(index: Int): BundleRow {
        val id = "user-${index.toString().padStart(12, '0')}"
        return BundleRow("main", "users", mapOf("id" to id), "INSERT", 1,
            buildJsonObject {
                put("id", JsonPrimitive(id))
                put("name", JsonPrimitive("x".repeat(targetRowBytes - 96)))
            })
    }

    private fun methodNotAllowed(exchange: HttpExchange) {
        exchange.sendResponseHeaders(405, -1)
        exchange.close()
    }

    private fun respondJson(exchange: HttpExchange, body: String) {
        val bytes = body.encodeToByteArray()
        exchange.responseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
