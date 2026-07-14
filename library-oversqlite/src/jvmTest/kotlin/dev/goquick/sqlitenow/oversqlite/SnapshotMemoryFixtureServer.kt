package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private const val memorySnapshotId = "snapshot-memory-baseline"

fun main(args: Array<String>) {
    check(System.getenv("GITHUB_ACTIONS") != "true") {
        "snapshot memory fixture server is local-heavy and must not run with GITHUB_ACTIONS=true"
    }
    require(args.size == 2) { "usage: SnapshotMemoryFixtureServer <row-count> <target-row-bytes>" }
    val rowCount = args[0].toInt()
    val targetRowBytes = args[1].toInt()
    SnapshotMemoryFixtureServer(rowCount, targetRowBytes).run()
}

internal fun encodedMemorySnapshotRow(ordinal: Int, targetBytes: Int): String {
    val id = "user-${ordinal.toString().padStart(12, '0')}"
    val prefix =
        """{"schema":"main","table":"users","key":{"id":"$id"},"row_version":1,"payload":{"id":"$id","name":""""
    val suffix = "\"}}"
    val paddingBytes = targetBytes - prefix.encodeToByteArray().size - suffix.encodeToByteArray().size
    require(paddingBytes >= 0) { "target row byte size is below the canonical fixture minimum" }
    return prefix + "x".repeat(paddingBytes) + suffix
}

private class SnapshotMemoryFixtureServer(
    private val rowCount: Int,
    private val targetRowBytes: Int,
) {
    private val json = Json { ignoreUnknownKeys = true }
    private val shutdown = CountDownLatch(1)
    private val chunkCount = AtomicLong()
    private val maxChunkRows = AtomicLong()
    private val maxChunkBytes = AtomicLong()
    private val executor = Executors.newFixedThreadPool(2)
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        this.executor = this@SnapshotMemoryFixtureServer.executor
    }

    fun run() {
        var primaryFailure: Throwable? = null
        try {
            require(rowCount > 0)
            require(targetRowBytes > 0)
            require(encodedMemorySnapshotRow(1, targetRowBytes).encodeToByteArray().size == targetRowBytes)
            installRoutes()
            server.start()
            println("snapshot_memory_server_ready port=${server.address.port} pid=${ProcessHandle.current().pid()}")
            System.out.flush()
            shutdown.await()
            println(
                "snapshot_memory_server_final chunks=${chunkCount.get()} " +
                    "server_max_chunk_rows=${maxChunkRows.get()} " +
                    "server_max_chunk_bytes=${maxChunkBytes.get()}",
            )
            System.out.flush()
        } catch (error: Throwable) {
            primaryFailure = error
            throw error
        } finally {
            var cleanupFailure: Throwable? = null
            try {
                server.stop(0)
            } catch (error: Throwable) {
                cleanupFailure = appendFixtureCleanupFailure(cleanupFailure, error)
            }
            try {
                executor.shutdownNow()
            } catch (error: Throwable) {
                cleanupFailure = appendFixtureCleanupFailure(cleanupFailure, error)
            }
            try {
                check(executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    "snapshot memory fixture executor did not stop"
                }
            } catch (error: Throwable) {
                cleanupFailure = appendFixtureCleanupFailure(cleanupFailure, error)
            }
            val completedCleanupFailure = cleanupFailure
            if (completedCleanupFailure != null) {
                val primary = primaryFailure
                if (primary == null) {
                    throw completedCleanupFailure
                }
                primary.addSuppressed(completedCleanupFailure)
            }
        }
    }

    private fun installRoutes() {
        server.createContext("/sync/capabilities") { exchange ->
            requireMethod(exchange, "GET") ?: return@createContext
            respondJson(
                exchange,
                json.encodeToString(
                    CapabilitiesResponse(
                        protocolVersion = "v1",
                        schemaVersion = 1,
                        features = mapOf("connect_lifecycle" to true),
                        bundleLimits = testBundleCapabilitiesLimits(),
                    ),
                ),
            )
        }
        server.createContext("/sync/connect") { exchange ->
            requireMethod(exchange, "POST") ?: return@createContext
            respondJson(exchange, json.encodeToString(ConnectResponse(resolution = "initialize_empty")))
        }
        server.createContext("/sync/snapshot-sessions/$memorySnapshotId") { exchange ->
            when (exchange.requestMethod) {
                "GET" -> respondChunk(exchange)
                "DELETE" -> {
                    exchange.sendResponseHeaders(204, -1)
                    exchange.close()
                }
                else -> methodNotAllowed(exchange)
            }
        }
        server.createContext("/sync/snapshot-sessions") { exchange ->
            requireMethod(exchange, "POST") ?: return@createContext
            val declaredTotalBytes = Math.multiplyExact(rowCount.toLong(), targetRowBytes.toLong())
            respondJson(
                exchange,
                """{"snapshot_id":"$memorySnapshotId","snapshot_bundle_seq":1,"row_count":$rowCount,"byte_count":$declaredTotalBytes,"expires_at":"2099-01-01T00:00:00Z"}""",
            )
        }
        server.createContext("/__shutdown") { exchange ->
            requireMethod(exchange, "POST") ?: return@createContext
            exchange.sendResponseHeaders(204, -1)
            exchange.close()
            shutdown.countDown()
        }
    }

    private fun respondChunk(exchange: HttpExchange) {
        val after = queryParameter(exchange, "after_row_ordinal").toInt()
        val requestedRows = queryParameter(exchange, "max_rows").toInt()
        val requestedBytes = queryParameter(exchange, "max_bytes").toLong()
        val remaining = rowCount - after
        val rowsByBytes = min(requestedBytes / targetRowBytes, Int.MAX_VALUE.toLong()).toInt()
        val rowsInChunk = min(min(requestedRows, remaining), rowsByBytes)
        check(rowsInChunk > 0) { "memory fixture received a byte budget smaller than one row" }
        val declaredChunkBytes = Math.multiplyExact(rowsInChunk.toLong(), targetRowBytes.toLong())
        chunkCount.incrementAndGet()
        maxChunkRows.accumulateAndGet(rowsInChunk.toLong(), ::maxOf)
        maxChunkBytes.accumulateAndGet(declaredChunkBytes, ::maxOf)
        val next = after + rowsInChunk
        val body = buildString(rowsInChunk * (targetRowBytes + 1) + 192) {
            append("{\"snapshot_id\":\"$memorySnapshotId\",\"snapshot_bundle_seq\":1,\"rows\":[")
            repeat(rowsInChunk) { offset ->
                if (offset > 0) append(',')
                append(encodedMemorySnapshotRow(after + offset + 1, targetRowBytes))
            }
            append("],\"next_row_ordinal\":")
            append(next)
            append(",\"has_more\":")
            append(next < rowCount)
            append(",\"byte_count\":")
            append(declaredChunkBytes)
            append('}')
        }
        respondJson(exchange, body)
    }

    private fun requireMethod(exchange: HttpExchange, expected: String): Unit? {
        if (exchange.requestMethod == expected) return Unit
        methodNotAllowed(exchange)
        return null
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

    private fun queryParameter(exchange: HttpExchange, name: String): String {
        return exchange.requestURI.rawQuery
            ?.split('&')
            ?.asSequence()
            ?.map { it.split('=', limit = 2) }
            ?.firstOrNull { it.firstOrNull() == name }
            ?.getOrNull(1)
            ?: error("missing query parameter $name")
    }
}

private fun appendFixtureCleanupFailure(first: Throwable?, next: Throwable): Throwable {
    if (first == null) return next
    if (first !== next) first.addSuppressed(next)
    return first
}
