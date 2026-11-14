package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.initializePlatformTestContext
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import java.io.File
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

/**
 * End-to-end sync timing against an owned live authority. It does nothing unless
 * OVERSQLITE_SYNC_BENCHMARK=true, so ordinary suites do not execute it.
 *
 * Two phases share a state directory: "seed" publishes the population and restores it on a
 * second device; "run" reopens both devices in a fresh process and measures incremental push,
 * pull, conflict, restore and source recovery. Server-side time, round trips and bytes come
 * from the measurement authority's /bench/metrics endpoint when it is available.
 */
internal class SyncPerformanceBenchmarkLiveTest : RealServerSupport() {
    private val parser = Json { ignoreUnknownKeys = true }
    private fun env(name: String) = realServerEnv("OVERSQLITE_SYNC_BENCHMARK_$name")?.trim()?.takeUnless { it.isEmpty() }

    private class Device(
        val label: String, val path: String, val db: SafeSQLiteConnection,
        val http: HttpClient, val client: DefaultOversqliteClient,
    )

    private class Population(val prefix: String, var users: Int, var posts: Int, var typed: Int) {
        fun userId(i: Int) = id(1, i)
        fun postId(i: Int) = id(2, i)
        fun typedId(i: Int) = id(3, i)
        private fun id(kind: Int, i: Int) = "$prefix-000$kind-4000-8000-${i.toString(16).padStart(12, '0')}"
    }

    private lateinit var baseUrl: String
    private lateinit var resultFile: File
    private var journal = "delete"
    private var diagnostics: SqliteNowDiagnostics? = null
    private var phase = "seed"
    private var rows = 1000
    private var conflictRetryExhaustions = 0
    private var watchedPath: String? = null

    @Test
    fun measureSyncScenarios() = runOwnedWebDatabaseTest(timeout = 90.minutes) {
        withContext(Dispatchers.Default) {
            if (realServerEnv("OVERSQLITE_SYNC_BENCHMARK") != "true") return@withContext
            initializePlatformTestContext()
            baseUrl = checkNotNull(requireRealServerConfig()) { "benchmark requires an owned live authority" }.baseUrl
            val stateDir = File(checkNotNull(env("STATE_DIR")) { "OVERSQLITE_SYNC_BENCHMARK_STATE_DIR required" })
            stateDir.mkdirs()
            resultFile = File(checkNotNull(env("RESULT_FILE")) { "OVERSQLITE_SYNC_BENCHMARK_RESULT_FILE required" })
            journal = env("JOURNAL") ?: "delete"
            phase = env("PHASE") ?: "seed"
            rows = (env("ROWS") ?: "1000").toInt()
            val scenarios = (env("SCENARIOS") ?: "push_pull,idle,conflict,restore,recovery").split(',').map { it.trim() }.toSet()
            val diagnosticsFile = env("DIAGNOSTICS_FILE")?.let(::File)
            val recording = env("JFR_FILE")?.let { startRecording() }
            val previousLogger = sqliteNowLogger
            if (diagnosticsFile != null) {
                diagnostics = SqliteNowDiagnostics(true)
                sqliteNowLogger = DiagnosticFileLogger(diagnosticsFile)
            }
            try {
                if (phase == "seed") seed(stateDir) else run(stateDir, scenarios)
            } finally {
                sqliteNowLogger = previousLogger
                if (recording != null) {
                    recording.stop(); recording.dump(File(env("JFR_FILE")!!).toPath()); recording.close()
                }
            }
        }
    }

    // ---- phases ----------------------------------------------------------------------------

    private suspend fun seed(stateDir: File) {
        val user = randomRealServerId("sync-bench")
        val population = Population(randomRealServerUuid().take(8), 0, 0, 0)
        val writer = device("writer", File(stateDir, "writer.db").path, user, DeferConflictResolver)
        var reader: Device? = null
        try {
            val batch = 1000
            var published = 0
            val seedStarted = System.nanoTime()
            while (published < rows) {
                val count = minOf(batch, rows - published)
                val first = published
                insertPopulationRows(writer.db, population, first, count)
                if (first == 0) measure("seed_first_push_batch", count, 0) { pushAll(writer) } else pushAll(writer)
                published += count
            }
            val seedMs = (System.nanoTime() - seedStarted) / 1e6
            emit(buildJsonObject {
                put("scenario", "seed_total"); put("rows", rows); put("wall_ms", seedMs)
                put("rows_per_s", rows / (seedMs / 1000.0))
            })
            if (env("MEMORY") == "true") {
                // Memory lane: keep the published account reusable even if the seed's own restore is refused.
                File(stateDir, "state.json").writeText(buildJsonObject {
                    put("user", user); put("prefix", population.prefix)
                    put("users", population.users); put("posts", population.posts); put("typed", population.typed)
                }.toString())
            }
            watchedPath = File(stateDir, "reader.db").path
            reader = measure("bootstrap_restore", rows, 0) {
                device("reader", File(stateDir, "reader.db").path, user, ClientWinsResolver)
            }
            watchedPath = null
            assertPopulationsMatch(writer, reader)
            File(stateDir, "state.json").writeText(buildJsonObject {
                put("user", user); put("prefix", population.prefix)
                put("users", population.users); put("posts", population.posts); put("typed", population.typed)
            }.toString())
        } finally {
            reader?.let { close(it) }
            close(writer)
        }
    }

    private suspend fun run(stateDir: File, scenarios: Set<String>) {
        val state = parser.parseToJsonElement(File(stateDir, "state.json").readText()).jsonObject
        val user = state.getValue("user").jsonPrimitive.content
        val population = Population(
            state.getValue("prefix").jsonPrimitive.content, state.getValue("users").jsonPrimitive.long.toInt(),
            state.getValue("posts").jsonPrimitive.long.toInt(), state.getValue("typed").jsonPrimitive.long.toInt(),
        )
        val writer = measure("open_attach_resume", rows, 0) {
            device("writer", File(stateDir, "writer.db").path, user, DeferConflictResolver)
        }
        // Identifiers are dense per table, so the next index is the current row count.
        population.users = scalarLong(writer.db, "SELECT count(*) FROM users").toInt()
        population.posts = scalarLong(writer.db, "SELECT count(*) FROM posts").toInt()
        population.typed = scalarLong(writer.db, "SELECT count(*) FROM typed_rows").toInt()
        var reader: Device? = null
        val extra = mutableListOf<Device>()
        try {
            // NO_READER=true (memory lane, restore-only runs): the writer stands in for the reader, so an account whose
            // own reader restore was refused can still be measured.
            reader = if (env("NO_READER") == "true") writer else measure("open_attach_resume", rows, 1) {
                device("reader", File(stateDir, "reader.db").path, user, ClientWinsResolver)
            }
            var cursor = 0
            if ("push_pull" in scenarios) {
                // Timing lane: PUSH_PULL_SIZES (comma list) narrows the measured sizes, e.g. for a profile of one size.
                val sizes = env("PUSH_PULL_SIZES")?.split(',')?.map { it.trim().toInt() } ?: listOf(1, 100, 1000)
                for (changed in sizes) {
                    val samples = env("SAMPLES")?.toInt() ?: if (changed == 1) 6 else 3
                    repeat(samples) { sample ->
                        mutate(writer.db, population, cursor, changed, "w$changed-$sample"); cursor += changed
                        measure("push", changed, sample) { pushAll(writer) }
                        measure("pull", changed, sample) { reader.client.pullToStable().getOrThrow() }
                    }
                }
            }
            if ("bulk" in scenarios) {
                // One large offline backlog drained bundle by bundle, to expose cost that grows across the drain.
                val backlog = env("BULK_ROWS")?.toInt() ?: 3000
                mutate(writer.db, population, cursor, backlog, "bulk"); cursor += backlog
                var bundle = 0
                while (scalarLong(writer.db, "SELECT count(*) FROM _sync_dirty_rows") > 0L) {
                    measure("bulk_push_bundle", backlog, bundle++) { writer.client.pushPending().getOrThrow() }
                }
                measure("bulk_pull", backlog, 0) { reader.client.pullToStable().getOrThrow() }
            }
            if ("backlog_pull" in scenarios) {
                // Memory lane: the writer pushes BACKLOG_ROWS changes in 1,000-row steps (linear drain), then the
                // reader catches up in one measured pull call.
                val backlog = env("BACKLOG_ROWS")?.toInt() ?: 20_000
                var pushed = 0
                while (pushed < backlog) {
                    val step = minOf(1000, backlog - pushed)
                    mutate(writer.db, population, cursor, step, "backlog-$pushed"); cursor += step; pushed += step
                    pushAll(writer)
                }
                measure("backlog_pull", backlog, 0) { reader.client.pullToStable().getOrThrow() }
            }
            if ("fk_group" in scenarios) {
                // Memory lane: one offline transaction of a new parent and FK_GROUP_ROWS children forms one push FK group.
                val children = env("FK_GROUP_ROWS")?.toInt() ?: 5000
                writer.db.transaction {
                    insertUser(writer.db, population)
                    val parent = population.userId(population.users - 1)
                    repeat(children) {
                        val i = population.posts++
                        writer.db.withPreparedStatement("INSERT INTO posts(id,title,content,author_id,created_at,updated_at) VALUES(?,?,?,?,?,?)") {
                            it.bindText(1, population.postId(i)); it.bindText(2, "Benchmark post title fk-group number $i")
                            it.bindText(3, prose(1_000_000 + i, 256)); it.bindText(4, parent); it.bindText(5, stamp); it.bindText(6, stamp); it.step()
                        }
                    }
                }
                measure("push_fk_group", children + 1, 0) { pushAll(writer) }
            }
            if ("idle" in scenarios) repeat(4) { sample ->
                measure("idle_sync", 0, sample) { writer.client.sync().getOrThrow() }
            }
            if ("conflict" in scenarios) {
                for (changed in listOf(1, 100)) repeat(3) { sample ->
                    updateTyped(writer.db, population, cursor, changed, "remote-$changed-$sample")
                    updateTyped(reader.db, population, cursor, changed, "local-$changed-$sample")
                    cursor += changed
                    pushAll(writer)
                    measure("conflict_push_resolve_repush", changed, sample) { pushAll(reader) }
                    measure("pull_after_conflict", changed, sample) { writer.client.pullToStable().getOrThrow() }
                    reader.client.pullToStable().getOrThrow()
                }
            }
            if ("restore" in scenarios) repeat(env("RESTORE_SAMPLES")?.toInt() ?: 2) { sample ->
                val path = File(stateDir, "restore-$sample-${System.nanoTime()}.db").path
                // RESTORE_ATTEMPTS > 1 retries a failed restore on the same database (memory lane only).
                val attempts = env("RESTORE_ATTEMPTS")?.toInt() ?: 1
                for (attempt in 1..attempts) {
                    watchedPath = path
                    val outcome = runCatching {
                        measure("bootstrap_restore", rows, sample + 1) { device("restore-$sample", path, user, DeferConflictResolver) }
                    }
                    watchedPath = null
                    if (outcome.isSuccess) { extra += outcome.getOrThrow(); break }
                    if (attempt == attempts) throw outcome.exceptionOrNull()!!
                }
            }
            if ("recovery" in scenarios) {
                val pending = 100
                mutate(reader.db, population, cursor, pending, "carried"); cursor += pending
                markSourceRecoveryRequired(reader.db)
                measure("source_recovery_rebuild", pending, 0) { reader.client.rebuild().getOrThrow() }
                assertEquals(pending.toLong(), scalarLong(reader.db, "SELECT count(*) FROM _sync_dirty_rows"), "pending intent carried")
                measure("push_after_recovery", pending, 0) { pushAll(reader) }
                writer.client.pullToStable().getOrThrow()
            }
            reader.client.pullToStable().getOrThrow()
            writer.client.pullToStable().getOrThrow()
            assertPopulationsMatch(writer, reader)
            File(stateDir, "state.json").writeText(buildJsonObject {
                put("user", user); put("prefix", population.prefix)
                put("users", population.users); put("posts", population.posts); put("typed", population.typed)
            }.toString())
        } finally {
            extra.forEach { runCatching { close(it) }; deleteTempSqliteNowTestDbArtifacts(it.path) }
            reader?.takeIf { it !== writer }?.let { close(it) }
            close(writer)
        }
    }

    // ---- devices ---------------------------------------------------------------------------

    private suspend fun device(label: String, path: String, user: String, resolver: Resolver): Device {
        val db = BundledSqliteConnectionProvider.openConnection(
            dbName = path, debug = false, config = SqliteConnectionConfig(diagnostics = diagnostics),
        )
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        try {
            if (journal == "wal") db.execSQL("PRAGMA journal_mode=WAL")
            // Makes every commit wait for the storage device, as it does on phones; macOS fsync alone does not.
            if (env("FULLFSYNC") == "true") { db.execSQL("PRAGMA fullfsync=ON"); db.execSQL("PRAGMA checkpoint_fullfsync=ON") }
            if (scalarLong(db, "SELECT count(*) FROM sqlite_master WHERE name='typed_rows'") == 0L) {
                createBusinessRichSchemaTables(db)
                // The typed bootstrap restore route needs a leading index on every foreign-key column;
                // without them a new device falls back to the general snapshot installer.
                if (env("CHILD_INDEXES") == "true") for ((table, column) in listOf("posts" to "author_id",
                        "categories" to "parent_id", "teams" to "captain_member_id", "team_members" to "team_id",
                        "file_reviews" to "file_id")) {
                    db.execSQL("CREATE INDEX ${table}_${column}_idx ON $table($column)")
                }
            }
            val token = benchmarkToken(user)
            http = HttpClient(OkHttp) {
                // OkHttp's default read timeout is 10 s; a large snapshot session build can exceed it.
                env("HTTP_READ_TIMEOUT_S")?.toLong()?.let { seconds ->
                    engine { config { readTimeout(java.time.Duration.ofSeconds(seconds)) } }
                }
                install(ContentNegotiation) { json(parser) }
                defaultRequest { url(baseUrl); header(HttpHeaders.Authorization, "Bearer $token") }
            }
            client = DefaultOversqliteClient.forTesting(
                db = db,
                config = OversqliteConfig(
                    generationId = 1, syncTables = businessRichSyncTables, databaseId = "fixture", serviceId = "business",
                    diagnostics = diagnostics,
                ),
                http = http, resolver = resolver,
            )
            client.open().getOrThrow()
            client.attach(user).getOrThrow()
            return Device(label, path, db, http, client)
        } catch (failure: Throwable) {
            runCatching { client?.closeFromCoordinatedOwner() }; http?.close(); db.close()
            throw failure
        }
    }

    private suspend fun close(device: Device) {
        try { device.client.closeFromCoordinatedOwner() } finally { device.http.close(); device.db.close() }
    }

    private suspend fun benchmarkToken(user: String): String {
        val probe = HttpClient(OkHttp)
        try {
            val response = probe.get("$baseUrl/bench/token?user=$user")
            if (response.status == HttpStatusCode.OK) {
                return parser.parseToJsonElement(response.bodyAsText()).jsonObject.getValue("token").jsonPrimitive.content
            }
        } finally { probe.close() }
        return issueDummySigninToken(baseUrl, user, "benchmark")
    }

    private suspend fun pushAll(device: Device): Int {
        var bundles = 0
        var calls = 0
        while (true) {
            check(++calls < 10_000) { "push did not drain" }
            // One conflicting row is resolved per attempt; the client stops after two automatic
            // retries and keeps the remaining intent, so the caller continues the same push.
            val report = try { device.client.pushPending().getOrThrow() }
                catch (_: PushConflictRetryExhaustedException) { conflictRetryExhaustions++; continue }
            if (report.outcome == PushOutcome.COMMITTED) bundles++
            if (report.status.pending.pendingRowCount == 0L) return bundles
        }
    }

    // ---- data ------------------------------------------------------------------------------

    // Pseudo-words from a fixed vocabulary compress like prose; the blob is incompressible like a hash or image tile.
    private val vocabulary = kotlin.random.Random(42).let { random ->
        List(512) { String(CharArray(random.nextInt(2, 10)) { 'a' + random.nextInt(26) }) }
    }
    private fun prose(seed: Int, length: Int): String {
        val random = kotlin.random.Random(seed)
        val text = StringBuilder(length + 10)
        while (text.length < length) text.append(vocabulary[random.nextInt(vocabulary.size)]).append(' ')
        return text.substring(0, length)
    }
    private fun blob(seed: Int) = kotlin.random.Random(seed).nextBytes(64)
    private val stamp = "2026-11-11T00:00:00Z"
    // Control workload: NULL in both REAL columns, so no binary64 value is rendered canonically.
    private val withoutReals get() = env("WITHOUT_REALS") == "true"

    private suspend fun insertPopulationRows(db: SafeSQLiteConnection, population: Population, first: Int, count: Int) {
        db.transaction {
            for (index in first until first + count) when (index % 10) {
                0 -> insertUser(db, population)
                1, 2, 3 -> if (population.users == 0) insertUser(db, population) else insertPost(db, population, "seed")
                else -> insertTyped(db, population, "seed")
            }
        }
    }

    private suspend fun insertUser(db: SafeSQLiteConnection, population: Population) {
        val i = population.users++
        db.withPreparedStatement("INSERT INTO users(id,name,email,created_at,updated_at) VALUES(?,?,?,?,?)") {
            it.bindText(1, population.userId(i)); it.bindText(2, "Benchmark user number $i"); it.bindText(3, "user$i@example.com")
            it.bindText(4, stamp); it.bindText(5, stamp); it.step()
        }
    }

    private suspend fun insertPost(db: SafeSQLiteConnection, population: Population, tag: String) {
        val i = population.posts++
        db.withPreparedStatement("INSERT INTO posts(id,title,content,author_id,created_at,updated_at) VALUES(?,?,?,?,?,?)") {
            it.bindText(1, population.postId(i)); it.bindText(2, "Benchmark post title $tag number $i"); it.bindText(3, prose(1_000_000 + i, 256))
            it.bindText(4, population.userId(i % population.users)); it.bindText(5, stamp); it.bindText(6, stamp); it.step()
        }
    }

    private suspend fun insertTyped(db: SafeSQLiteConnection, population: Population, tag: String) {
        val i = population.typed++
        db.withPreparedStatement(
            "INSERT INTO typed_rows(id,name,note,count_value,small_count,medium_count,exact_amount,enabled_flag,rating,float4_value,data,created_at) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        ) {
            it.bindText(1, population.typedId(i)); it.bindText(2, "Benchmark typed row $tag $i"); it.bindText(3, prose(i, 160))
            it.bindLong(4, i.toLong()); it.bindLong(5, (i % 100).toLong()); it.bindLong(6, (i % 10_000).toLong())
            it.bindText(7, "1234.56"); it.bindLong(8, 1); if (withoutReals) { it.bindNull(9); it.bindNull(10) } else { it.bindDouble(9, 4.25); it.bindDouble(10, 1.5) }
            it.bindBlob(11, blob(i)); it.bindText(12, stamp); it.step()
        }
    }

    private suspend fun updateTyped(db: SafeSQLiteConnection, population: Population, cursor: Int, count: Int, tag: String) {
        db.transaction {
            repeat(count) { offset ->
                db.withPreparedStatement("UPDATE typed_rows SET name=?, count_value=count_value+1 WHERE id=?") {
                    it.bindText(1, "Benchmark typed row $tag $offset"); it.bindText(2, population.typedId((cursor + offset) % population.typed)); it.step()
                }
            }
        }
    }

    /** Three updates, one typed insert and one FK-bearing post insert per five changes. */
    private suspend fun mutate(db: SafeSQLiteConnection, population: Population, cursor: Int, count: Int, tag: String) {
        db.transaction {
            repeat(count) { offset ->
                when (offset % 5) {
                    3 -> insertTyped(db, population, tag)
                    4 -> insertPost(db, population, tag)
                    else -> db.withPreparedStatement("UPDATE typed_rows SET name=?, count_value=count_value+1 WHERE id=?") {
                        it.bindText(1, "Benchmark typed row $tag $offset"); it.bindText(2, population.typedId((cursor + offset) % population.typed)); it.step()
                    }
                }
            }
        }
    }

    private suspend fun assertPopulationsMatch(left: Device, right: Device) {
        for (sql in listOf(
            "SELECT count(*) FROM users", "SELECT count(*) FROM posts", "SELECT count(*) FROM typed_rows",
            "SELECT coalesce(sum(count_value),0)+coalesce(sum(length(name)),0) FROM typed_rows",
            "SELECT count(*) FROM _sync_dirty_rows",
        )) assertEquals(scalarLong(left.db, sql), scalarLong(right.db, sql), "${left.label} and ${right.label} differ: $sql")
    }

    // ---- measurement -----------------------------------------------------------------------

    private suspend fun <T> measure(scenario: String, changed: Int, sample: Int, block: suspend () -> T): T {
        serverMetrics(reset = true)
        val allocatedBefore = allocatedBytes()
        val exhaustionsBefore = conflictRetryExhaustions
        val probe = if (env("MEMORY") == "true") MemoryProbe(
            watchedPath, env("MEMORY_SAMPLE_MS")?.toLong() ?: 50L, env("MEMORY_LIVE_MS")?.toLong(),
            env("MEMORY_HISTOGRAM") == "true",
        ).also { it.start() } else null
        val startedEpochMs = System.currentTimeMillis()
        val started = System.nanoTime()
        val outcome = runCatching { block() }
        val endedEpochMs = System.currentTimeMillis()
        val wallMs = (System.nanoTime() - started) / 1e6
        val allocated = allocatedBytes() - allocatedBefore
        val failure = outcome.exceptionOrNull()
        // Timing lane: RECORD_FAILURES=true records a failed call (e.g. a capped restore attempt) without the memory probe.
        if (failure != null && probe == null && env("RECORD_FAILURES") != "true") throw failure
        val memory = probe?.stop()
        val server = serverMetrics(reset = true)
        emit(buildJsonObject {
            if (memory != null) put("memory", memory)
            if (failure != null) putJsonObject("failure") {
                put("type", failure::class.qualifiedName); put("message", failure.message?.take(2000))
                put("cause", failure.cause?.let { "${it::class.qualifiedName}: ${it.message?.take(500)}" })
            }
            put("scenario", scenario); put("phase", phase); put("base_rows", rows); put("changed_rows", changed)
            put("sample", sample); put("journal", journal); put("diagnostics", diagnostics != null)
            put("fullfsync", env("FULLFSYNC") == "true"); put("jfr", env("JFR_FILE") != null); put("without_reals", withoutReals)
            put("wall_ms", wallMs); put("allocated_bytes", allocated)
            put("started_epoch_ms", startedEpochMs); put("ended_epoch_ms", endedEpochMs)
            put("conflict_retry_exhaustions", conflictRetryExhaustions - exhaustionsBefore)
            if (server != null) {
                val routes = server.getValue("routes").jsonObject.filterKeys { "/sync/" in it }
                val serverMs = routes.values.sumOf { it.jsonObject.getValue("total_ms").jsonPrimitive.double }
                put("round_trips", routes.values.sumOf { it.jsonObject.getValue("count").jsonPrimitive.long })
                put("request_bytes", routes.values.sumOf { it.jsonObject.getValue("req_bytes").jsonPrimitive.long })
                put("response_bytes", routes.values.sumOf { it.jsonObject.getValue("resp_bytes").jsonPrimitive.long })
                put("server_ms", serverMs); put("client_and_transport_ms", wallMs - serverMs)
                putJsonObject("routes") {
                    for ((route, value) in routes) putJsonObject(route) {
                        val entry = value.jsonObject
                        put("count", entry.getValue("count")); put("total_ms", entry.getValue("total_ms"))
                        put("req_bytes", entry.getValue("req_bytes")); put("resp_bytes", entry.getValue("resp_bytes"))
                    }
                }
                put("stages", server.getValue("stages"))
            }
        })
        return outcome.getOrThrow()
    }

    private fun emit(record: JsonObject) {
        println("SYNC_BENCH $record")
        resultFile.appendText("$record\n")
    }

    private suspend fun serverMetrics(reset: Boolean): JsonObject? {
        val probe = HttpClient(OkHttp)
        return try {
            val response = probe.get("$baseUrl/bench/metrics${if (reset) "?reset=1" else ""}")
            if (response.status == HttpStatusCode.OK) parser.parseToJsonElement(response.bodyAsText()).jsonObject else null
        } finally { probe.close() }
    }

    private fun allocatedBytes(): Long {
        val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        return threads.getThreadAllocatedBytes(threads.allThreadIds).filter { it > 0 }.sum()
    }

    private fun startRecording(): jdk.jfr.Recording {
        val recording = jdk.jfr.Recording(jdk.jfr.Configuration.getConfiguration("profile"))
        recording.start()
        return recording
    }

    /**
     * Memory lane (OVERSQLITE_SYNC_BENCHMARK_MEMORY=true): settles the heap with System.gc() before the call, then samples
     * heap, direct buffers, RSS (ps) and the watched database's files; tracks after-GC heap from GC notifications; with
     * MEMORY_LIVE_MS forces a full GC at that period to read the live set (and a live class histogram at its maximum).
     */
    private class MemoryProbe(
        private val watched: String?, private val sampleMs: Long, private val liveMs: Long?, private val histogram: Boolean,
    ) {
        private val heap = java.lang.management.ManagementFactory.getMemoryMXBean()
        private val buffers = ManagementFactory.getPlatformMXBeans(java.lang.management.BufferPoolMXBean::class.java)
        private val pid = ProcessHandle.current().pid()
        private val server = ManagementFactory.getPlatformMBeanServer()
        private val dcmdName = javax.management.ObjectName("com.sun.management:type=DiagnosticCommand")
        @Volatile private var running = false
        private var startedNs = 0L
        private var baselineUsed = 0L; private var baselineRss = 0L; private var baselineFootprint = 0L
        @Volatile private var peakUsed = 0L; @Volatile private var peakCommitted = 0L
        @Volatile private var peakRss = 0L; @Volatile private var peakDirect = 0L
        @Volatile private var peakDb = 0L; @Volatile private var peakJournal = 0L; @Volatile private var peakWal = 0L
        @Volatile private var peakUnlinked = 0L
        @Volatile private var maxAfterGc = 0L; @Volatile private var maxAfterGcAtMs = 0L
        @Volatile private var gcCount = 0; @Volatile private var gcPauseMs = 0L; @Volatile private var gcFull = 0
        @Volatile private var maxLive = 0L; @Volatile private var maxLiveAtMs = 0L; @Volatile private var liveSamples = 0
        @Volatile private var histogramTop: String? = null; @Volatile private var nmtAtMaxLive: String? = null
        @Volatile private var rssAtMaxLive = 0L
        private val threads = mutableListOf<Thread>()
        private val listener = javax.management.NotificationListener { notification, _ ->
            if (!running || notification.type != com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION) {
                return@NotificationListener
            }
            val info = com.sun.management.GarbageCollectionNotificationInfo.from(
                notification.userData as javax.management.openmbean.CompositeData,
            )
            gcCount++; gcPauseMs += info.gcInfo.duration
            if (info.gcName.contains("Old") || info.gcName.contains("Full")) gcFull++
            if (info.gcCause == "System.gc()" || info.gcCause == "Heap Inspection Initiated GC") return@NotificationListener
            val pools = ManagementFactory.getMemoryPoolMXBeans().filter { it.type == java.lang.management.MemoryType.HEAP }.map { it.name }.toSet()
            val heapAfter = info.gcInfo.memoryUsageAfterGc.filterKeys { it in pools }.values.sumOf { it.used }
            if (heapAfter > maxAfterGc) { maxAfterGc = heapAfter; maxAfterGcAtMs = elapsedMs() }
        }

        private fun elapsedMs() = (System.nanoTime() - startedNs) / 1_000_000

        fun start() {
            System.gc(); System.gc()
            baselineUsed = heap.heapMemoryUsage.used
            baselineRss = rss(); baselineFootprint = footprint().first
            peakUsed = baselineUsed; peakRss = baselineRss
            ManagementFactory.getGarbageCollectorMXBeans().forEach {
                (it as javax.management.NotificationEmitter).addNotificationListener(listener, null, null)
            }
            startedNs = System.nanoTime()
            running = true
            threads += kotlin.concurrent.thread(isDaemon = true, name = "bench-memory-sampler") {
                var tick = 0L
                while (running) {
                    val usage = heap.heapMemoryUsage
                    peakUsed = maxOf(peakUsed, usage.used); peakCommitted = maxOf(peakCommitted, usage.committed)
                    peakDirect = maxOf(peakDirect, buffers.sumOf { it.memoryUsed })
                    if (tick % 2 == 0L) peakRss = maxOf(peakRss, rss())
                    if (watched != null) {
                        peakDb = maxOf(peakDb, File(watched).length())
                        peakJournal = maxOf(peakJournal, File("$watched-journal").length())
                        peakWal = maxOf(peakWal, File("$watched-wal").length())
                        if (tick % 20 == 0L) peakUnlinked = maxOf(peakUnlinked, unlinkedOpenFileBytes())
                    }
                    tick++
                    Thread.sleep(sampleMs)
                }
            }
            if (liveMs != null) threads += kotlin.concurrent.thread(isDaemon = true, name = "bench-memory-live") {
                while (running) {
                    Thread.sleep(liveMs)
                    if (!running) break
                    System.gc()
                    val live = heap.heapMemoryUsage.used
                    liveSamples++
                    if (live > maxLive) {
                        maxLive = live; maxLiveAtMs = elapsedMs(); rssAtMaxLive = rss()
                        if (histogram) {
                            histogramTop = dcmd("gcClassHistogram").lines().take(28).joinToString("\n")
                            nmtAtMaxLive = dcmd("vmNativeMemory", "summary").lines()
                                .filter { it.contains("committed=") && (it.startsWith("Total") || it.startsWith("-")) }
                                .take(30).joinToString("\n")
                        }
                    }
                }
            }
        }

        fun stop(): JsonObject {
            running = false
            threads.forEach { it.join(10_000) }
            ManagementFactory.getGarbageCollectorMXBeans().forEach {
                runCatching { (it as javax.management.NotificationEmitter).removeNotificationListener(listener) }
            }
            val endUsed = heap.heapMemoryUsage.used
            val (footprintNow, footprintPeak) = footprint()
            System.gc(); System.gc()
            val retained = heap.heapMemoryUsage.used
            val mib = { bytes: Long -> Math.round(bytes / 1048576.0 * 10) / 10.0 }
            return buildJsonObject {
                put("xmx_mib", mib(heap.heapMemoryUsage.max)); put("baseline_live_mib", mib(baselineUsed))
                put("peak_heap_used_mib", mib(maxOf(peakUsed, endUsed))); put("peak_heap_committed_mib", mib(peakCommitted))
                put("max_after_gc_mib", mib(maxAfterGc)); put("max_after_gc_at_ms", maxAfterGcAtMs)
                put("retained_after_mib", mib(retained))
                put("gc_count", gcCount); put("gc_pause_ms", gcPauseMs); put("gc_full_count", gcFull)
                put("peak_direct_mib", mib(peakDirect))
                put("baseline_rss_mib", mib(baselineRss)); put("peak_rss_mib", mib(peakRss))
                put("baseline_footprint_mib", mib(baselineFootprint)); put("end_footprint_mib", mib(footprintNow))
                put("process_peak_footprint_mib", mib(footprintPeak))
                if (liveMs != null) {
                    put("live_every_ms", liveMs); put("live_samples", liveSamples)
                    put("max_live_mib", mib(maxLive)); put("max_live_at_ms", maxLiveAtMs); put("rss_at_max_live_mib", mib(rssAtMaxLive))
                    histogramTop?.let { put("histogram_at_max_live", it) }
                    nmtAtMaxLive?.let { put("nmt_at_max_live", it) }
                }
                if (watched != null) {
                    put("db_end_mib", mib(File(watched).length())); put("db_peak_mib", mib(peakDb))
                    put("journal_peak_mib", mib(peakJournal)); put("wal_peak_mib", mib(peakWal))
                    put("unlinked_open_files_peak_mib", mib(peakUnlinked))
                }
            }
        }

        private fun dcmd(name: String, vararg args: String): String =
            server.invoke(dcmdName, name, arrayOf<Any>(arrayOf(*args)), arrayOf(Array<String>::class.java.name)) as String

        private fun run(vararg command: String): String {
            val process = ProcessBuilder(*command).redirectErrorStream(true).start()
            val text = process.inputStream.bufferedReader().use { it.readText() }
            process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)
            return text
        }

        private fun rss(): Long = run("/bin/ps", "-o", "rss=", "-p", pid.toString()).trim().toLongOrNull()?.times(1024) ?: 0L

        /** phys_footprint and its process-lifetime peak (macOS). */
        private fun footprint(): Pair<Long, Long> {
            val text = run("/usr/bin/footprint", "-p", pid.toString())
            fun field(name: String) = Regex("$name: (\\d+(?:\\.\\d+)?) (B|KB|MB|GB)").find(text)?.let {
                val unit = when (it.groupValues[2]) { "KB" -> 1024.0; "MB" -> 1048576.0; "GB" -> 1073741824.0; else -> 1.0 }
                (it.groupValues[1].toDouble() * unit).toLong()
            } ?: 0L
            return field("phys_footprint") to field("phys_footprint_peak")
        }

        /** Bytes in open, already unlinked regular files (SQLite temp files), from lsof. */
        private fun unlinkedOpenFileBytes(): Long = run("/usr/sbin/lsof", "-nP", "-a", "-p", pid.toString(), "+L1", "-Fts")
            .lines().fold(0L to "") { (sum, type), line ->
                when {
                    line.startsWith("t") -> sum to line.substring(1)
                    line.startsWith("s") && type == "REG" -> (sum + (line.substring(1).toLongOrNull() ?: 0L)) to type
                    else -> sum to type
                }
            }.first
    }

    private class DiagnosticFileLogger(private val file: File) : SqliteNowLogger {
        private fun record(message: () -> String) {
            val line = message()
            if (line.startsWith("SQLITENOW_DIAG ")) synchronized(file) { file.appendText("$line\n") }
        }
        override fun e(throwable: Throwable?, message: () -> String) = record(message)
        override fun w(throwable: Throwable?, message: () -> String) = record(message)
        override fun i(throwable: Throwable?, message: () -> String) = record(message)
        override fun d(throwable: Throwable?, message: () -> String) = record(message)
    }
}
