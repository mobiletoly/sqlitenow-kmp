package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import io.ktor.client.HttpClient
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

private const val phase5ProfileRowsEnvironment = "OVERSQLITE_PHASE5_REALSERVER_ROWS"
private const val phase5ProfileRowBytesEnvironment = "OVERSQLITE_PHASE5_REALSERVER_ROW_BYTES"
private const val phase5ProfileRunLabelEnvironment = "OVERSQLITE_PHASE5_REALSERVER_RUN_LABEL"

internal class RealServerSnapshotProfileJvmTest : RealServerSupport() {
    @Test
    fun preseededSnapshotProfile_restoresThroughRealServer() = runBlocking<Unit> {
        val rawRows = System.getenv(phase5ProfileRowsEnvironment) ?: return@runBlocking
        check(System.getenv("GITHUB_ACTIONS") != "true") {
            "Phase 5 realserver profiles are local-heavy and must not run with GITHUB_ACTIONS=true"
        }
        require(System.getenv("OVERSQLITE_REALSERVER_HEAVY") == "true") {
            "Phase 5 realserver profiles require OVERSQLITE_REALSERVER_HEAVY=true"
        }

        val rows = rawRows.toIntOrNull()
            ?: error("$phase5ProfileRowsEnvironment must be numeric")
        require(rows == 10_000 || rows == 100_000) {
            "$phase5ProfileRowsEnvironment must be 10000 or 100000"
        }
        val rowBytes = System.getenv(phase5ProfileRowBytesEnvironment)?.toIntOrNull()
            ?: error("$phase5ProfileRowBytesEnvironment must be 256 or 1024")
        require(rowBytes == 256 || rowBytes == 1024) {
            "$phase5ProfileRowBytesEnvironment must be 256 or 1024"
        }
        val runLabel = System.getenv(phase5ProfileRunLabelEnvironment)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?: "manual-${rows}x$rowBytes"
        val userId = "phase5-${rows}x$rowBytes"
        val realServer = requireNotNull(requireRealServerConfig()) {
            "Phase 5 realserver profile requires an available explicitly configured server"
        }

        val directory = Files.createTempDirectory("oversqlite-phase5-realserver-")
        val databasePath = directory.resolve("oversqlite.sqlite")
        val database = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        try {
            createBusinessSubsetTables(database)
            database.execSQL("PRAGMA journal_mode = WAL")
            val sourceId = bootstrapManagedSourceId(
                db = database,
                baseUrl = realServer.baseUrl,
            )
            val token = issueDummySigninToken(realServer.baseUrl, userId, sourceId)
            val profileHttp = newRealServerHttpClient(realServer.baseUrl, token)
            http = profileHttp
            val profileClient = newRealServerClient(
                db = database,
                http = profileHttp,
                snapshotChunkRows = 1_000,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client = profileClient

            profileClient.open().getOrThrow()
            val startedNanos = System.nanoTime()
            val connected = assertIs<AttachResult.Connected>(profileClient.attach(userId).getOrThrow())
            val elapsedNanos = System.nanoTime() - startedNanos
            assertEquals(AttachOutcome.USED_REMOTE_STATE, connected.outcome)
            assertEquals(rows.toLong(), connected.restore?.rowCount)
            assertEquals(rows.toLong(), scalarLong(database, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(database, "SELECT COUNT(*) FROM _sync_snapshot_stage"))

            val diagnostics = profileClient.snapshotRestoreDiagnosticsForTest()
            assertEquals(1L, diagnostics.sessionCount)
            assertEquals(rows.toLong(), diagnostics.finalStagedRows)
            assertEquals(rows.toLong(), diagnostics.finalAppliedRows)
            assertTrue(diagnostics.fetchedChunks > 1L)
            assertTrue(diagnostics.maxValidatedChunkRows <= 1_000)
            assertTrue(diagnostics.maxDeclaredChunkBytes <= 4L * 1024L * 1024L)
            assertTrue(diagnostics.maxCompletelyDecodedChunkBodyBytes <= 4L * 1024L * 1024L + 64L * 1024L)
            assertTrue(diagnostics.maxLiveApplyPageRows <= 256)
            assertTrue(diagnostics.maxLiveApplyPageStagedTextBytes <= 4L * 1024L * 1024L)
            assertEquals(0L, diagnostics.capacityResponses)
            assertEquals(0L, diagnostics.capacityRetries)

            val sqliteBytes = Files.size(databasePath)
            val walPath = databasePath.resolveSibling("${databasePath.fileName}-wal")
            val walBytes = if (Files.exists(walPath)) Files.size(walPath) else 0L
            val elapsedMillis = elapsedNanos / 1_000_000L
            val throughput = if (elapsedNanos == 0L) 0.0 else rows * 1_000_000_000.0 / elapsedNanos
            println(
                "phase5_realserver_profile runtime=kmp-jvm label=$runLabel rows=$rows " +
                    "approximate_row_bytes=$rowBytes elapsed_ms=$elapsedMillis " +
                    "throughput_rows_per_second=${"%.2f".format(throughput)} " +
                    "sessions=${diagnostics.sessionCount} chunks=${diagnostics.fetchedChunks} " +
                    "max_chunk_rows=${diagnostics.maxValidatedChunkRows} " +
                    "max_chunk_wire_bytes=${diagnostics.maxDeclaredChunkBytes} " +
                    "max_chunk_decoded_body_bytes=${diagnostics.maxCompletelyDecodedChunkBodyBytes} " +
                    "apply_pages=${diagnostics.applyPages} " +
                    "max_apply_page_rows=${diagnostics.maxLiveApplyPageRows} " +
                    "max_apply_page_staged_text_bytes=${diagnostics.maxLiveApplyPageStagedTextBytes} " +
                    "sqlite_bytes=$sqliteBytes sqlite_wal_bytes=$walBytes",
            )
        } finally {
            client?.close()
            http?.close()
            database.close()
            directory.toFile().deleteRecursively()
        }
    }
}
