package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.platformsupport.canonicalizeJsonElement
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.oversqlite.queryList
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteNowCoordinatedProviderSession
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import kotlinx.serialization.json.*

// Fixture input is installed after the normal provider prepares its one handle,
// before that handle is handed to the public opening coordinator. The refusal
// census is observed before real provider cleanup, so a transient Node close
// cannot discard the very state whose preservation the test needs to prove.
// No import, persistence, startup-mode, close suppression, or alternate driver.
@OptIn(SqliteNowInternalApi::class)
internal class RetainedOpenerFixtureProvider(
    private val delegate: SqliteConnectionProvider,
    private val seed: suspend (SafeSQLiteConnection) -> Unit,
) : SqliteConnectionProvider {
    var before: Map<String, List<String>>? = null
        private set
    var after: Map<String, List<String>>? = null
        private set
    var preparedConnection: SafeSQLiteConnection? = null
        private set
    var observedConnection: SafeSQLiteConnection? = null
        private set
    var seedCalls = 0
        private set
    var closeCalls = 0
        private set
    var publications = 0
        private set

    suspend fun observeBeforeClose() {
        val connection = checkNotNull(preparedConnection)
        check(after == null) { "retained refusal census must be observed exactly once" }
        observedConnection = connection
        after = retainedDatabaseVector(connection)
    }

    override suspend fun openConnection(
        dbName: String, debug: Boolean, config: SqliteConnectionConfig,
    ): SafeSQLiteConnection = error("retained opener fixture forbids a separate ordinary handle")

    override suspend fun openCoordinatedSession(
        dbName: String, debug: Boolean, config: SqliteConnectionConfig,
    ): SqliteNowCoordinatedProviderSession {
        val session = delegate.openCoordinatedSession(dbName, debug, config)
        try {
            check(session.inspectionConnection?.queryList(
                "SELECT count(*) FROM sqlite_schema WHERE type='table'",
            ) { it.getLong(0) }?.single()?.let { it == 0L } != false) {
                "retained opener fixture requires a fresh default-provider database"
            }
        } catch (failure: Throwable) {
            runCatching { session.close() }.onFailure(failure::addSuppressed)
            throw failure
        }
        return object : SqliteNowCoordinatedProviderSession by session {
            override suspend fun prepareConnection(): SafeSQLiteConnection {
                val connection = session.prepareConnection()
                check(seedCalls == 0) { "retained fixture must be seeded exactly once" }
                seedCalls++
                preparedConnection = connection
                seed(connection)
                before = retainedDatabaseVector(connection)
                return connection
            }

            override fun publish(connection: SafeSQLiteConnection) {
                publications++
                session.publish(connection)
            }

            override suspend fun close() {
                session.close()
                closeCalls++
            }
        }
    }
}

// This test-only descriptor follows the server-owned wire shape and derives
// an action-specific fingerprint from the declared schema. SET DEFAULT is
// constructed only for the negative generation-admission test.
internal fun nodeGeneration(action: String): JsonObject {
    val wireAction = action.lowercase().replace(' ', '_')
    require(wireAction in setOf("no_action", "restrict", "cascade", "set_null", "set_default"))
    val columns = listOf(
        Triple("id", "text", false),
        Triple("name", "text", false),
        Triple("parent_id", "text", true),
    )
    val fingerprintInput = JsonArray(listOf(
        JsonPrimitive("oversync.schema-generation.v1"),
        JsonPrimitive("1"),
        JsonArray(listOf(JsonArray(listOf(
            JsonPrimitive("main"), JsonPrimitive("nodes"),
            JsonArray(listOf(JsonPrimitive("id"))),
            JsonArray(columns.sortedBy { it.first }.map { (name, wireType, nullable) ->
                JsonArray(listOf(JsonPrimitive(name), JsonPrimitive(wireType), JsonPrimitive(nullable)))
            }),
            JsonArray(listOf(JsonArray(listOf(
                JsonArray(listOf(JsonPrimitive("parent_id"))), JsonPrimitive("main"), JsonPrimitive("nodes"),
                JsonArray(listOf(JsonPrimitive("id"))), JsonPrimitive(wireAction), JsonPrimitive("no_action"),
            )))),
        )))),
    ))
    val fingerprint = sha256Hex(canonicalizeJsonElement(fingerprintInput).encodeToByteArray())
    val generation = buildJsonObject {
        put("id", "1")
        put("fingerprint", fingerprint)
        putJsonArray("tables") {
            add(buildJsonObject {
                put("schema", "main")
                put("table", "nodes")
                putJsonArray("sync_key_columns") { add("id") }
                putJsonArray("columns") {
                    columns.forEach { (name, wireType, nullable) ->
                        add(buildJsonObject {
                            put("name", name)
                            put("wire_type", wireType)
                            put("nullable", nullable)
                        })
                    }
                }
                putJsonArray("foreign_keys") {
                    add(buildJsonObject {
                        putJsonArray("columns") { add("parent_id") }
                        put("referenced_schema", "main")
                        put("referenced_table", "nodes")
                        putJsonArray("referenced_columns") { add("id") }
                        put("on_delete", wireAction)
                        put("on_update", "no_action")
                    })
                }
            })
        }
    }
    return buildJsonObject {
        put("protocol_version", "v1")
        put("schema_generation", generation)
        put("limits", testGenerationResponse(testRegisteredTableSpecs("users"))["limits"]!!)
        put("watch_enabled", false)
    }
}
