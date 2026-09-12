package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.*

private fun deathDatabase(path: String) = SqliteNowDatabase(path, object : DatabaseMigrations {
    override val targetVersion = 0
    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int) = 0
}, syncManaged = true)

internal class ReservationOwnerDeathJvmTest : SurvivorTestSupport() {
    @Test fun killedOwnerReclaimsOnlyTransientReservations() = runOwnedWebDatabaseTest {
        val root = Files.createTempDirectory("a3-reservation-death-")
        val path = root.resolve("durable.sqlite").toString()
        try {
            withSurvivorFixture(dbPath = path) { server, db, http, seed ->
                db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                server.normalizePushEffects = { effects() }
                server.beforePushResponse = { throw CancellationException("lost committed response") }
                assertFailsWith<CancellationException> { seed.pushPending().getOrThrow() }
                db.execSQL("UPDATE nodes SET name='unrelated local intent' WHERE id='Q'")
                val retainedOwners = listOf("nodes", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
                val before = baselineQualificationVector(db, retainedOwners)
                val admission = root.resolve("admission.json")
                val session = seed.javaClass.getDeclaredField("sessionState").apply { isAccessible = true }.get(seed) as OversqliteClientSessionState
                Files.writeString(admission, Json.encodeToString(session.requireAdmittedContract()))
                seed.closeFromCoordinatedOwner(); db.close()
                val marker = root.resolve("child.ready")
                val log = root.resolve("child.log")
                val child = ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", checkNotNull(System.getProperty("coordinatedopening.jvmTest.classpath")),
                    ReservationDeathOwner::class.java.name, path, marker.toString(), admission.toString())
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start()
                try {
                    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
                    while (!Files.exists(marker) && child.isAlive && System.nanoTime() < deadline) Thread.sleep(10)
                    assertTrue(Files.exists(marker), Files.readString(log))
                    val observing = newFileBackedDb(path)
                    try {
                        assertEquals(4L, scalarLong(observing, "SELECT reserved_entries FROM _sync_operation_state"))
                        assertFailsWith<SqliteNowLifecycleBusyException> {
                            deathDatabase(path).openForSync(
                                OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables), http)
                        }
                        assertEquals(4L, scalarLong(observing, "SELECT reserved_entries FROM _sync_operation_state"))
                    } finally { observing.close() }
                    assertTrue(child.isAlive)
                    child.destroyForcibly()
                    assertTrue(child.waitFor(5, TimeUnit.SECONDS), "SIGKILL deadline")
                    assertEquals(137, child.exitValue(), "parent-directed SIGKILL, no orderly child close")
                    val ordinaryDb = newFileBackedDb(path)
                    try {
                        val ordinary = newClient(ordinaryDb, http, syncTables = nodeTables)
                        try {
                            ordinary.open().getOrThrow()
                            assertEquals(4L, scalarLong(ordinaryDb, "SELECT reserved_entries FROM _sync_operation_state"))
                        } finally { ordinary.closeFromCoordinatedOwner() }
                    } finally { ordinaryDb.close() }
                    val database = deathDatabase(path)
                    val opening = database.openForSync(
                        OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables), http)
                    try {
                        assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
                        val ready = assertNotNull(opening.readyOrNull())
                        val reopened = database.connection()
                        assertEquals(0L, scalarLong(reopened, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                        assertEquals(before, baselineQualificationVector(reopened, retainedOwners))
                        ready.client.openAndConnect("user-1").getOrThrow()
                        ready.client.pushPending().getOrThrow()
                        assertEquals(server.pushBodies[1], server.pushBodies[2])
                        requireSurvivors(reopened)
                        assertEquals(1L, scalarLong(reopened, "SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{\"id\":\"Q\"}'"))
                        assertEquals(3L, scalarLong(reopened, "SELECT next_source_bundle_id FROM _sync_source_state"))
                        assertEquals(2L, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_outbox_rows"))
                        assertEquals(0L, scalarLong(reopened, "SELECT snapshot_stage_entries+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                    } finally { opening.close() }
                    println("A3 reservation death: pid=${child.pid()} signal=SIGKILL exit=${child.exitValue()} liveEntries=4 reacquisition=PASS completeVector=PASS exactReplay=PASS")
                } finally {
                    if (child.isAlive) { child.destroyForcibly(); assertTrue(child.waitFor(5, TimeUnit.SECONDS)) }
                    println(Files.readString(log))
                    child.inputStream.close(); child.errorStream.close(); child.outputStream.close()
                }
            }
        } finally { root.toFile().deleteRecursively() }
    }
}

// Fresh JVM with a real public opening/lifetime lease and actual charged copy.
// There are deliberately no finally/close/release paths: the parent SIGKILLs it.
internal object ReservationDeathOwner : SurvivorTestSupport() {
    @JvmStatic fun main(args: Array<String>) = runBlocking {
        val http = HttpClient(MockEngine { error("death witness must not contact a remote authority") })
        val database = deathDatabase(args[0])
        val config = OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables)
        val opening = database.openForSync(config, http)
        check(opening.advance().outcome == OversqliteOpeningOutcome.READY)
        val client = checkNotNull(opening.readyOrNull()).client as DefaultOversqliteClient
        client.open().getOrThrow()
        val session = client.javaClass.getDeclaredField("sessionState").apply { isAccessible = true }.get(client) as OversqliteClientSessionState
        val durable = session.loadDurableLifecycleView()
        // Reuse the exact parent's admitted wire identity for this offline
        // reservation witness; lease acquisition remains the public opening.
        val admitted = Json.decodeFromString<AdmittedOversqliteContract>(Files.readString(Path.of(args[2])))
        val state = RuntimeState(session.requireValidatedConfig(), admitted, durable.attachedUserId, durable.sourceId)
        val token = OversqliteWorkspace(database.connection(), config).reserve(state, applyUnit("death-buffer", 0, ""), WorkspaceCharge(3, 100))
        check(token.isNotEmpty())
        val buffer = ByteArray(100)
        Files.writeString(Path.of(args[1]), "live reservation")
        while (true) {
            Thread.sleep(50)
            buffer[0] = (buffer[0] + 1).toByte()
            check(opening.readyOrNull() != null)
        }
    }
}
