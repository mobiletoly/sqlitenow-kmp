package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException
import java.nio.file.Files
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertNotNull
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.HttpClientPlugin
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.util.AttributeKey
import java.io.Closeable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.first
import kotlin.test.assertSame
import kotlinx.coroutines.runBlocking

class OversqliteHttpOwnershipTest {
    @Test
    fun productionFacadeRetainsLeaseUntilDerivedChildCleanupActuallyFinishes() = runBlocking {
        for (entry in listOf("opening", "database", "client")) {
            val plugin = HeldDerivedCleanupPlugin()
            val caller = HttpClient(MockEngine { respond("caller-alive") }) { install(plugin) }
            val path = Files.createTempDirectory("http-owner").resolve("db.sqlite3").toString()
            val migrations = object : DatabaseMigrations {
                override val targetVersion = 1
                override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                    if (currentVersion == -1) conn.execSQL("CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL)")
                    return 1
                }
            }
            var queryCalls = 0
            val database = object : SqliteNowDatabase(path, migrations, syncManaged = true) {
                suspend fun tableFlow() = createTableChangeFlow(setOf("items"))
                fun queryFlow() = createReactiveQueryFlow(setOf("items")) { queryCalls++; 1 }
            }
            val config = OversqliteConfig(generationId = 1, databaseId = "http-owner", serviceId = "main",
                syncTables = listOf(SyncTable("items", syncKeyColumnName = "id")))
            val opening = database.openForSync(config, caller)
            try {
                opening.advance()
                val client = assertNotNull(opening.readyOrNull()).client as DefaultOversqliteClient
                client.cleanupObservationTimeoutMillis = 50
                val captured = database.connection()
                val capturedChanges = database.tableFlow()
                val capturedQuery = database.queryFlow()
                val closing = async(start = CoroutineStart.UNDISPATCHED) {
                    runCatching {
                        when (entry) {
                            "database" -> database.close()
                            "client" -> { client.close(); opening.close() }
                            else -> opening.close()
                        }
                    }.exceptionOrNull()
                }
                withTimeout(5_000) { plugin.cleanupEntered.await() }
                assertNull(opening.readyOrNull(), entry)
                assertFalse(database.isOpen(), entry)
                assertFailsWith<IllegalStateException>(entry) { database.connection() }
                assertFailsWith<IllegalStateException>(entry) { captured.execSQL("INSERT INTO items VALUES('blocked')") }
                assertFailsWith<IllegalStateException>(entry) { captured.transaction { error("not admitted") } }
                assertFailsWith<IllegalStateException>(entry) { database.enableTableChangeNotifications() }
                assertFailsWith<IllegalStateException>(entry) { capturedChanges.first() }
                assertFailsWith<IllegalStateException>(entry) { capturedQuery.first() }
                assertEquals(0, queryCalls, entry)
                assertNotNull(withTimeout(5_000) { closing.await() }, entry)
                assertTrue(client.cleanupPending, entry)
                val contender = SqliteNowDatabase(path, migrations, syncManaged = true)
                assertFailsWith<SqliteNowLifecycleBusyException>(entry) { contender.openForSync(config, caller) }
                assertEquals("caller-alive", caller.get("https://example.test/alive").bodyAsText())
                assertEquals(1, plugin.closeCalls, entry)
                plugin.release.complete(Unit)
                opening.close()
                assertFalse(client.cleanupPending, entry)
                assertEquals(1, plugin.closeCalls, entry)
                contender.openForSync(config, caller).close()
            } finally {
                plugin.release.complete(Unit)
                opening.close()
                caller.close()
            }
        }
    }

    @Test
    fun throwingDerivedPluginStillTerminatesDerivedJobWithoutClosingCallerClient() = runBlocking {
        val closeFailure = IllegalStateException("DERIVED_HTTP_CLOSE_SENTINEL")
        val throwingPlugin = FailFirstClosePlugin(closeFailure)
        val caller = HttpClient(MockEngine { respond("caller-alive") }) {
            install(throwingPlugin)
        }
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val client = DefaultOversqliteClient.forTesting(
            db = db,
            config = OversqliteConfig(
                generationId = 1,
                databaseId = "http-ownership",
                serviceId = "main",
                syncTables = listOf(SyncTable("items", syncKeyColumnName = "id")),
            ),
            http = caller,
        )
        try {
            assertSame(
                closeFailure,
                assertFailsWith<IllegalStateException> { client.closeFromCoordinatedOwner() },
            )
            client.closeFromCoordinatedOwner()
            assertEquals(1, throwingPlugin.closeCalls)
            assertEquals("caller-alive", caller.get("https://example.test/alive").bodyAsText())
        } finally {
            caller.close()
            db.close()
        }
        assertEquals(2, throwingPlugin.closeCalls)
    }
}

private class FailFirstClosePlugin(
    private val failure: Throwable,
) : HttpClientPlugin<Unit, FailFirstClosePlugin>, Closeable {
    override val key: AttributeKey<FailFirstClosePlugin> = AttributeKey("FailFirstClosePlugin")
    var closeCalls = 0
        private set

    override fun prepare(block: Unit.() -> Unit): FailFirstClosePlugin = this

    override fun install(plugin: FailFirstClosePlugin, scope: HttpClient) = Unit

    override fun close() {
        closeCalls++
        if (closeCalls == 1) throw failure
    }
}

private class HeldDerivedCleanupPlugin : HttpClientPlugin<Unit, HeldDerivedCleanupPlugin>, Closeable {
    override val key = AttributeKey<HeldDerivedCleanupPlugin>("HeldDerivedCleanupPlugin")
    val cleanupEntered = CompletableDeferred<Unit>()
    val release = CompletableDeferred<Unit>()
    private var installations = 0
    var closeCalls = 0
        private set
    override fun prepare(block: Unit.() -> Unit) = this
    override fun install(plugin: HeldDerivedCleanupPlugin, scope: HttpClient) {
        installations++
        if (installations == 2) scope.launch(start = CoroutineStart.UNDISPATCHED) {
            try { awaitCancellation() } finally {
                withContext(NonCancellable) {
                    cleanupEntered.complete(Unit)
                    release.await()
                }
            }
        }
    }
    override fun close() { closeCalls++ }
}
