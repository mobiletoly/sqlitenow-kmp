package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import java.io.BufferedOutputStream
import java.io.Closeable
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class BundleChangeWatchSocketTimeoutTest {
    @Test
    fun idleWatchSurvivesTwoHeartbeatIntervalsAndReceivesBundle() = runBlocking {
        val heartbeats = AtomicInteger()
        val events = mutableListOf<Long>()
        LoopbackSseServer { response ->
            repeat(2) {
                Thread.sleep(20_000)
                response.chunk(": heartbeat\n\n")
                heartbeats.incrementAndGet()
            }
            response.chunk("event: bundle\ndata: {\"bundle_seq\":9}\n\n")
            response.finish()
        }.use { server ->
            withWatchApi(server) { api, _, _, observations ->
                val started = System.nanoTime()
                withTimeout(60_000) {
                    api.watchBundleChanges("source-a", 8) { event -> events += event.bundleSeq }
                    server.finished.await()
                }
                val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                println("watch idle elapsed=${elapsedMillis}ms heartbeats=${heartbeats.get()} events=$events")
                assertEquals(2, heartbeats.get())
                assertEquals(listOf(9L), events, "heartbeat comments must not dispatch bundle events")
                assertEquals("/sync/watch?after_bundle_seq=8", server.requestTarget.await())
                assertEquals(90_000, observations.single().readMillis)
                assertEquals(10_000, observations.single().connectMillis)
            }
        }
    }

    @Test
    fun cancellingAdmittedIdleWatchClosesConnectionPromptly() = runBlocking {
        val peerClosed = CompletableDeferred<Unit>()
        LoopbackSseServer { response ->
            response.awaitPeerClose()
            peerClosed.complete(Unit)
        }.use { server ->
            withWatchApi(server) { api, _, responseReceived, observations ->
                val watcher = launch { api.watchBundleChanges("source-a", 8) { error("unexpected event") } }
                try {
                    withTimeout(5_000) { responseReceived.await() }
                    val started = System.nanoTime()
                    withTimeout(2_000) {
                        watcher.cancelAndJoin()
                        peerClosed.await()
                        server.finished.await()
                    }
                    println("watch cancellation elapsed=${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)}ms")
                    assertEquals(90_000, observations.single().readMillis)
                    assertEquals(10_000, observations.single().connectMillis)
                } finally {
                    withTimeout(2_000) { watcher.cancelAndJoin() }
                }
            }
        }
    }

    @Test
    fun watchTimeoutOverridePreservesCallerAndOrdinaryRequestTimeouts() = runBlocking {
        LoopbackSseServer { response ->
            response.chunk(": heartbeat\n\n")
            response.finish()
        }.use { watchServer ->
            withWatchApi(watchServer, socketMillis = 250, connectMillis = 1_234) { api, clients, _, observations ->
                withTimeout(5_000) { api.watchBundleChanges("source-a", 8) { error("unexpected event") } }
                assertEquals(90_000, observations.single().readMillis)
                assertEquals(1_234, observations.single().connectMillis)

                // Check both the SDK-derived client and its caller after the watch has completed.
                for (http in clients) {
                    LoopbackSseServer { response -> response.awaitPeerClose() }.use { ordinaryServer ->
                        val started = System.nanoTime()
                        val failure = withTimeout(3_000) {
                            runCatching { http.get(ordinaryServer.baseUrl + "ordinary").bodyAsText() }.exceptionOrNull()
                        }
                        assertNotNull(failure, "ordinary stalled responses must retain the caller's socket timeout")
                        assertTrue(
                            generateSequence(failure) { it.cause }.any { it is SocketTimeoutException },
                            "expected a socket timeout in the cause chain, got $failure",
                        )
                        println("ordinary timeout elapsed=${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)}ms")
                        withTimeout(2_000) { ordinaryServer.finished.await() }
                    }
                    assertEquals(250, observations.last().readMillis)
                    assertEquals(1_234, observations.last().connectMillis)
                }
            }
        }
    }

    private suspend fun withWatchApi(
        server: LoopbackSseServer,
        socketMillis: Long? = null,
        connectMillis: Long? = null,
        block: suspend (OversqliteRemoteApi, List<HttpClient>, CompletableDeferred<Unit>, List<SocketTimeoutObservation>) -> Unit,
    ) {
        val observations = CopyOnWriteArrayList<SocketTimeoutObservation>()
        val responseReceived = CompletableDeferred<Unit>()
        val caller = HttpClient(OkHttp) {
            engine {
                addInterceptor { chain ->
                    val observation = SocketTimeoutObservation(chain.readTimeoutMillis(), chain.connectTimeoutMillis())
                    observations += observation
                    println("OkHttp path=${chain.request().url.encodedPath} read=${observation.readMillis}ms connect=${observation.connectMillis}ms")
                    chain.proceed(chain.request())
                }
            }
            if (socketMillis != null || connectMillis != null) {
                install(HttpTimeout) {
                    socketTimeoutMillis = socketMillis
                    connectTimeoutMillis = connectMillis
                }
            }
            defaultRequest { url(server.baseUrl) }
        }
        val derived = oversqliteRedirectSafeHttpClient(caller)
        val api = OversqliteRemoteApi(derived, Json, generation = { testGeneration() }, log = { message ->
            val value = message()
            if (value.contains("op=bundle change watch") && value.contains("status=200")) {
                responseReceived.complete(Unit)
            }
        })
        try {
            block(api, listOf(derived, caller), responseReceived, observations)
        } finally {
            derived.close()
            caller.close()
            withTimeout(2_000) {
                derived.coroutineContext.job.join()
                caller.coroutineContext.job.join()
            }
        }
    }
}

private data class SocketTimeoutObservation(val readMillis: Int, val connectMillis: Int)

/** A single-request HTTP/1 SSE fixture, compiled separately for JVM and Android tests. */
private class LoopbackSseServer(serve: (SocketSseResponse) -> Unit) : Closeable {
    private val listener = ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
    private val activeSocket = AtomicReference<Socket?>()
    private val executor = Executors.newSingleThreadExecutor()
    val baseUrl = "http://127.0.0.1:${listener.localPort}/"
    val requestTarget = CompletableDeferred<String>()
    val finished = CompletableDeferred<Unit>()
    private val task = executor.submit {
        try {
            listener.accept().use { socket ->
                activeSocket.set(socket)
                socket.soTimeout = 5_000
                val reader = socket.getInputStream().bufferedReader(Charsets.US_ASCII)
                val requestLine = checkNotNull(reader.readLine())
                requestTarget.complete(requestLine.split(' ')[1])
                while (!checkNotNull(reader.readLine()).isEmpty()) { /* drain request headers */ }
                val response = SocketSseResponse(socket)
                response.begin()
                serve(response)
            }
            finished.complete(Unit)
        } catch (error: Throwable) {
            finished.completeExceptionally(error)
        }
    }

    override fun close() {
        listener.close()
        activeSocket.get()?.close()
        task.cancel(true)
        executor.shutdownNow()
        check(executor.awaitTermination(2, TimeUnit.SECONDS)) { "loopback SSE worker did not terminate" }
        requestTarget.cancel()
        finished.cancel()
    }
}

private class SocketSseResponse(private val socket: Socket) {
    private val output = BufferedOutputStream(socket.getOutputStream())

    fun begin() {
        output.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" +
            "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n").toByteArray(Charsets.US_ASCII))
        output.flush()
    }

    fun chunk(value: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        output.write("${bytes.size.toString(16)}\r\n".toByteArray(Charsets.US_ASCII))
        output.write(bytes)
        output.write("\r\n".toByteArray(Charsets.US_ASCII))
        output.flush()
    }

    fun finish() {
        output.write("0\r\n\r\n".toByteArray(Charsets.US_ASCII))
        output.flush()
    }

    fun awaitPeerClose() {
        assertEquals(-1, socket.getInputStream().read(), "expected the client to close its idle connection")
    }
}
