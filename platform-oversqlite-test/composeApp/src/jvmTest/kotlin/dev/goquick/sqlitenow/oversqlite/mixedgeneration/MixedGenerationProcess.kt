package dev.goquick.sqlitenow.oversqlite.mixedgeneration

import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.DeferConflictResolver
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.OversqliteTransientRetryPolicy
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationone.MixedGenerationOneDatabase
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationone.VersionBasedDatabaseMigrations as GenerationOneMigrations
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationtwo.MixedGenerationTwoDatabase
import dev.goquick.sqlitenow.oversqlite.mixedgeneration.generationtwo.VersionBasedDatabaseMigrations as GenerationTwoMigrations
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.serialization.kotlinx.json.json
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json

/** Guarded JVM entry point used only by the mixedgeneration mixed-generation launcher. */
fun main() = runBlocking {
    check(System.getenv("MIXED_GENERATION_KMP_HELPER") == "current-test-subprocess-v1") {
        "MixedGeneration KMP helper is launcher-only"
    }
    val action = requiredEnvironment("MIXED_GENERATION_KMP_ACTION")
    val fault = System.getenv("MIXED_GENERATION_KMP_FAULT").orEmpty()
    val generation = requiredEnvironment("MIXED_GENERATION_KMP_GENERATION").toInt()
    val path = requiredEnvironment("MIXED_GENERATION_KMP_FILE")
    val userId = requiredEnvironment("MIXED_GENERATION_KMP_USER")
    val maxPushRows = System.getenv("MIXED_GENERATION_KMP_MAX_PUSH_ROWS")?.toIntOrNull()
    val attempts = AtomicInteger()
    val proxy = GenerationFaultProxy(
        targetBaseUrl = requiredEnvironment("MIXED_GENERATION_KMP_URL"),
        fault = fault,
        attempts = attempts,
    )
    val http = mixedgenerationAuthorityHttp(
        baseUrl = proxy.baseUrl,
        token = requiredEnvironment("MIXED_GENERATION_KMP_TOKEN"),
    )
    val retryPolicy = OversqliteTransientRetryPolicy(
        maxAttempts = 2,
        initialBackoffMillis = 1,
        maxBackoffMillis = 1,
        jitterRatio = 0.0,
    )
    val (database, opening) = when (generation) {
        1 -> MixedGenerationOneDatabase(path, GenerationOneMigrations()).let { database ->
            val config = database.buildOversqliteConfig(1, DATABASE_ID, SERVICE_ID)
            database to database.openForSync(
                config.copy(
                    transferTargets = config.transferTargets.copy(
                        maxPushRows = maxPushRows ?: config.transferTargets.maxPushRows,
                        maxSnapshotChunkRows = 1,
                    ),
                    snapshotApplyBatchRows = 1,
                    transientRetryPolicy = retryPolicy,
                ),
                http,
                DeferConflictResolver,
            )
        }
        2 -> MixedGenerationTwoDatabase(path, GenerationTwoMigrations()).let { database ->
            val config = database.buildOversqliteConfig(2, DATABASE_ID, SERVICE_ID)
            database to database.openForSync(
                config.copy(
                    transferTargets = config.transferTargets.copy(
                        maxPushRows = maxPushRows ?: config.transferTargets.maxPushRows,
                        maxSnapshotChunkRows = 1,
                    ),
                    snapshotApplyBatchRows = 1,
                    transientRetryPolicy = retryPolicy,
                ),
                http,
                DeferConflictResolver,
            )
        }
        else -> error("unsupported KMP generation $generation")
    }
    try {
        check(opening.advance().outcome == OversqliteOpeningOutcome.READY)
        val client = checkNotNull(opening.readyOrNull()).client
        check(client.attach(userId).getOrThrow() is AttachResult.Connected)
        if (fault == "final_snapshot_stage") {
            installFinalSnapshotStageBarrier(client as DefaultOversqliteClient)
        }
        val result = when (action) {
            "exec" -> {
                database.connection().execSQL(requiredEnvironment("MIXED_GENERATION_KMP_SQL"))
                println("MIXED_GENERATION:action=exec")
                return@runBlocking
            }
            "push" -> client.pushPending()
            "pull" -> client.pullToStable()
            "sync" -> client.sync()
            "rebuild" -> client.rebuild()
            else -> error("unsupported KMP action $action")
        }
        val failure = result.exceptionOrNull()
        if (fault == "interrupt_pull" || fault == "block_upload") {
            check(failure?.javaClass?.simpleName == "TransientRetryExhaustedException") {
                "expected typed transient retry exhaustion, got ${failure?.stackTraceToString()}"
            }
            println("MIXED_GENERATION:typed_failure=${failure::class.qualifiedName};attempts=${attempts.get()}")
            System.out.flush()
        } else {
            result.getOrThrow()
            println("MIXED_GENERATION:action=$action")
        }
    } finally {
        opening.close()
        http.close()
        proxy.close()
    }
}

internal fun mixedgenerationAuthorityHttp(
    baseUrl: String,
    token: String,
): HttpClient = HttpClient(CIO) {
    install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
    defaultRequest {
        url(baseUrl)
        header(HttpHeaders.Authorization, "Bearer $token")
    }
}

private class GenerationFaultProxy(
    private val targetBaseUrl: String,
    private val fault: String,
    private val attempts: AtomicInteger,
) : AutoCloseable {
    private val client = java.net.http.HttpClient.newBuilder()
        .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
        .build()
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        executor = Executors.newCachedThreadPool { task -> Thread(task, "mixed-generation-proxy").apply { isDaemon = true } }
        createContext("/") { exchange -> handle(exchange) }
        start()
    }
    val baseUrl: String = "http://127.0.0.1:${server.address.port}"

    private fun handle(exchange: HttpExchange) {
        try {
            val path = exchange.requestURI.path
            val isPush = exchange.requestMethod == "POST" && path == "/sync/push"
            val isSnapshotPage = exchange.requestMethod == "GET" && path.startsWith("/sync/snapshot-sessions/")
            when {
                fault == "block_upload" && isPush -> {
                    attempts.incrementAndGet()
                    exchange.close()
                    return
                }
                fault == "before_push" && isPush -> blockAtBoundary("prepared_before_push")
                fault == "nonfinal_snapshot_stage" && isSnapshotPage &&
                    !exchange.requestURI.rawQuery.orEmpty().contains("after_row_ordinal=0") ->
                    blockAtBoundary("nonfinal_snapshot_stage")
            }

            val suffix = exchange.requestURI.rawPath +
                (exchange.requestURI.rawQuery?.let { "?$it" } ?: "")
            val request = HttpRequest.newBuilder(URI.create(targetBaseUrl + suffix)).apply {
                exchange.requestHeaders.forEach { (name, values) ->
                    if (name.lowercase() !in HOP_HEADERS) values.forEach { value -> header(name, value) }
                }
                method(exchange.requestMethod, HttpRequest.BodyPublishers.ofByteArray(exchange.requestBody.readAllBytes()))
            }.build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofByteArray())
            if (exchange.requestMethod == "POST" && path == "/sync/push") {
                println("MIXED_GENERATION:push_response=${response.body().decodeToString()}")
                System.out.flush()
            }
            when {
                fault == "after_push_reply" && isPush && response.statusCode() in 200..299 ->
                    blockAtBoundary("applied_before_reply")
                fault == "after_first_push_reply" && isPush && response.statusCode() in 200..299 ->
                    blockAtBoundary("first_push_applied")
                fault == "interrupt_pull" && path == "/sync/pull" && response.statusCode() in 200..299 -> {
                    attempts.incrementAndGet()
                    exchange.close()
                    return
                }
            }
            response.headers().map().forEach { (name, values) ->
                if (name.lowercase() !in HOP_HEADERS) values.forEach { value -> exchange.responseHeaders.add(name, value) }
            }
            exchange.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
            exchange.responseBody.use { it.write(response.body()) }
        } catch (failure: Throwable) {
            runCatching { exchange.close() }
            if (failure is InterruptedException) Thread.currentThread().interrupt()
        }
    }

    private fun blockAtBoundary(name: String): Nothing {
        println("MIXED_GENERATION:boundary=$name;pid=${ProcessHandle.current().pid()}")
        System.out.flush()
        CountDownLatch(1).await()
        error("unreachable boundary $name")
    }

    override fun close() = server.stop(0)

    private companion object {
        val HOP_HEADERS = setOf("connection", "content-length", "expect", "host", "transfer-encoding", "upgrade")
    }
}

private suspend fun stopAtBoundary(name: String): Nothing {
    println("MIXED_GENERATION:boundary=$name;pid=${ProcessHandle.current().pid()}")
    System.out.flush()
    awaitCancellation()
}

private fun installFinalSnapshotStageBarrier(client: DefaultOversqliteClient) {
    val injectorClass = Class.forName("dev.goquick.sqlitenow.oversqlite.SnapshotApplyFaultInjector")
    val constructor = injectorClass.declaredConstructors.single { it.parameterCount == 6 }.apply { isAccessible = true }
    val afterPageLoaded: suspend () -> Unit = { stopAtBoundary("final_snapshot_stage") }
    val injector = constructor.newInstance(null, afterPageLoaded, null, null, null, 5_000L)
    val setter = client.javaClass.declaredMethods.single {
        it.name.startsWith("setSnapshotApplyFaultInjectorForTest") && it.parameterCount == 1
    }.apply { isAccessible = true }
    setter.invoke(client, injector)
}

private fun requiredEnvironment(name: String): String =
    System.getenv(name)?.takeIf { it.isNotBlank() } ?: error("missing $name")

private const val DATABASE_ID = "coexistence-client"
private const val SERVICE_ID = "coexistence-service"
