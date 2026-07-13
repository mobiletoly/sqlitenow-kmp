package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.getColumnNames
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

open class SharedRuntimeStateFixtureSupport : BundleClientContractTestSupport() {
    protected val contractJson = Json {
        prettyPrint = true
        prettyPrintIndent = "  "
        explicitNulls = false
        ignoreUnknownKeys = true
    }

    protected val updateRuntimeStateExpected =
        System.getProperty("oversqlite.runtimeStateContracts.update") == "true" ||
            System.getenv("OVERSQLITE_RUNTIME_STATE_CONTRACTS_UPDATE") == "true"

    protected suspend fun <T> withRuntimeStateUsersDatabase(
        block: suspend RuntimeStateUsersEnv.() -> T,
    ): T {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newRuntimeStateClient(db, http)
            return RuntimeStateUsersEnv(db, server, http, client).block()
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    protected fun newRuntimeStateClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
    ): DefaultOversqliteClient {
        return newClient(
            db,
            http,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
        )
    }

    protected suspend fun dumpRuntimeSchema(db: SafeSQLiteConnection): JsonElement {
        val normalizer = SourceIdNormalizer()
        val tableNames = runtimeTableNames(db)
        return buildJsonObject {
            put("formatVersion", JsonPrimitive(1))
            put("fixture", JsonPrimitive("users"))
            put(
                "pragmas",
                buildJsonObject {
                    put("foreignKeys", JsonPrimitive(db.scalarLong("PRAGMA foreign_keys")))
                },
            )
            put(
                "tables",
                JsonArray(
                    tableNames.map { tableName ->
                        buildJsonObject {
                            put("name", JsonPrimitive(tableName))
                            put("columns", dumpTableColumns(db, tableName))
                        }
                    },
                ),
            )
            put("indexes", dumpRuntimeIndexes(db, tableNames))
            put("triggers", dumpRuntimeTriggers(db))
            put("state", dumpRuntimeStateRows(db, normalizer))
        }
    }

    protected suspend fun dumpRuntimeState(db: SafeSQLiteConnection): JsonElement {
        return buildJsonObject {
            put("formatVersion", JsonPrimitive(1))
            put("state", dumpRuntimeStateRows(db, SourceIdNormalizer()))
        }
    }

    protected fun assertOrUpdateExpected(expectedPath: Path, actual: JsonElement) {
        val actualText = contractJson.encodeToString(actual) + "\n"
        if (updateRuntimeStateExpected) {
            expectedPath.parent.createDirectories()
            expectedPath.writeText(actualText)
            return
        }
        val expected = contractJson.parseToJsonElement(expectedPath.readText())
        assertEquals(expected, actual, "runtime-state contract mismatch: $expectedPath")
    }

    protected fun assertRuntimeStateExpectedException(
        caseName: String,
        expected: String,
        error: Throwable?,
    ) {
        when (expected) {
            "none" -> assertNull(error, "$caseName: expected success")
            "any_error" -> assertIs<Throwable>(error, "$caseName: expected failure")
            "http_error" -> assertIs<Throwable>(error, "$caseName: expected HTTP-style failure")
            "rebuild_required" -> assertIs<RebuildRequiredException>(error, "$caseName: expected rebuild required")
            "source_recovery_required" -> assertIs<SourceRecoveryRequiredException>(
                error,
                "$caseName: expected source recovery required",
            )
            "source_sequence_mismatch" -> assertIs<SourceSequenceMismatchException>(
                error,
                "$caseName: expected source sequence mismatch",
            )
            "source_replacement_diverged" -> assertIs<SourceReplacementDivergedException>(
                error,
                "$caseName: expected source replacement divergence",
            )
            "source_replacement_invalid" -> assertIs<SourceReplacementInvalidException>(
                error,
                "$caseName: expected source replacement invalid",
            )
            "protocol_version_mismatch" -> assertIs<ProtocolVersionMismatchException>(
                error,
                "$caseName: expected protocol version mismatch",
            )
            "invalid_source_recovery_reason" -> assertIs<IllegalStateException>(
                error,
                "$caseName: expected invalid source recovery reason",
            )
            else -> error("$caseName: unknown expected exception $expected")
        }
    }

    protected fun configureRuntimeStateServer(
        caseName: String,
        script: RuntimeStateServerScript,
        server: HttpServer,
    ): FakeChunkedSyncServer? {
        return when (script.kind) {
            "default" -> FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                install(server)
            }
            "precommit_retry" -> {
                var failed = false
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    commitError = { _, _ ->
                        if (!failed) {
                            failed = true
                            500 to """{"error":"temporary","message":"retry"}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "committed_replay_first_fetch_http_error" -> {
                var failed = false
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    committedBundleChunkError = { _, _ ->
                        if (!failed) {
                            failed = true
                            500 to """{"error":"temporary","message":"retry"}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "committed_bundle_seq_gap" -> FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                nextBundleSeq = script.bundleSeq ?: error("$caseName: missing bundleSeq")
                install(server)
            }
            "source_retired_on_push_create" -> FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                createError = { request ->
                    409 to """
                        {
                          "error": "source_retired",
                          "message": "source retired",
                          "source_id": "${request.sourceId}",
                          "replaced_by_source_id": "${script.replacementSourceId ?: "replacement-source"}"
                        }
                    """.trimIndent()
                }
                install(server)
            }
            "pull_incremental_users" -> {
                val response = script.response ?: error("$caseName: missing pull response")
                server.createContext("/sync/pull") { exchange ->
                    respondJson(exchange, 200, response.toString())
                }
                null
            }
            else -> error("$caseName: unknown server script ${script.kind}")
        }
    }

    private suspend fun runtimeTableNames(db: SafeSQLiteConnection): List<String> {
        return db.queryList(
            """
            SELECT name
            FROM sqlite_schema
            WHERE type = 'table' AND name LIKE '_sync_%'
            ORDER BY name
            """.trimIndent(),
        ) { st -> st.getText(0) }
    }

    private suspend fun dumpTableColumns(db: SafeSQLiteConnection, tableName: String): JsonArray {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA table_info(${quoteIdentifier(tableName)})").use { st ->
                val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                put("name", JsonPrimitive(st.getText(columns.getValue("name"))))
                                put("type", JsonPrimitive(st.getText(columns.getValue("type"))))
                                put("notNull", JsonPrimitive(st.getLong(columns.getValue("notnull")) == 1L))
                                put("defaultValue", nullableString(st, columns.getValue("dflt_value")))
                                put("primaryKeyPosition", JsonPrimitive(st.getLong(columns.getValue("pk"))))
                            },
                        )
                    }
                }
            }
        }
    }

    private suspend fun dumpRuntimeIndexes(
        db: SafeSQLiteConnection,
        tableNames: List<String>,
    ): JsonArray {
        val indexes = mutableListOf<JsonElement>()
        for (tableName in tableNames) {
            val summaries = db.withExclusiveAccess {
                db.prepare("PRAGMA index_list(${quoteIdentifier(tableName)})").use { st ->
                    val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                    buildList {
                        while (st.step()) {
                            val name = st.getText(columns.getValue("name"))
                            if (!name.startsWith("sqlite_")) {
                                add(
                                    RuntimeIndexSummary(
                                        tableName = tableName,
                                        name = name,
                                        unique = st.getLong(columns.getValue("unique")) == 1L,
                                    ),
                                )
                            }
                        }
                    }
                }
            }
            for (summary in summaries) {
                indexes += buildJsonObject {
                    put("name", JsonPrimitive(summary.name))
                    put("unique", JsonPrimitive(summary.unique))
                    put("table", JsonPrimitive(summary.tableName))
                    put("columns", dumpIndexColumns(db, summary.name))
                }
            }
        }
        return JsonArray(indexes.sortedBy { it.jsonObjectString("name") })
    }

    private suspend fun dumpIndexColumns(db: SafeSQLiteConnection, indexName: String): JsonArray {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA index_info(${quoteIdentifier(indexName)})").use { st ->
                val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                buildJsonArray {
                    while (st.step()) {
                        add(JsonPrimitive(st.getText(columns.getValue("name"))))
                    }
                }
            }
        }
    }

    private suspend fun dumpRuntimeTriggers(db: SafeSQLiteConnection): JsonArray {
        return db.queryList(
            """
            SELECT name, tbl_name, sql
            FROM sqlite_schema
            WHERE type = 'trigger' AND sql LIKE '%_sync_%'
            ORDER BY name
            """.trimIndent(),
        ) { st ->
            buildJsonObject {
                put("name", JsonPrimitive(st.getText(0)))
                put("table", JsonPrimitive(st.getText(1)))
                put("sql", JsonPrimitive(normalizeTriggerSql(st.getText(2))))
            }
        }.let(::JsonArray)
    }

    private suspend fun dumpRuntimeStateRows(
        db: SafeSQLiteConnection,
        normalizer: SourceIdNormalizer,
    ): JsonObject {
        return buildJsonObject {
            for (table in runtimeStateTables) {
                put(table.name.removePrefix("_sync_"), dumpRows(db, table, normalizer))
            }
        }
    }

    private suspend fun dumpRows(
        db: SafeSQLiteConnection,
        table: RuntimeStateTable,
        normalizer: SourceIdNormalizer,
    ): JsonArray {
        val columnTypes = columnTypes(db, table.name)
        return db.withExclusiveAccess {
            db.prepare("SELECT * FROM ${quoteIdentifier(table.name)} ORDER BY ${table.orderBy}").use { st ->
                val columnNames = st.getColumnNames()
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                for ((index, columnName) in columnNames.withIndex()) {
                                    put(columnName, normalizedValue(st, index, columnName, columnTypes[columnName].orEmpty(), normalizer))
                                }
                            },
                        )
                    }
                }
            }
        }
    }

    private suspend fun columnTypes(db: SafeSQLiteConnection, tableName: String): Map<String, String> {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA table_info(${quoteIdentifier(tableName)})").use { st ->
                buildMap {
                    while (st.step()) {
                        put(st.getText(1), st.getText(2).uppercase())
                    }
                }
            }
        }
    }

    private fun normalizedValue(
        st: SqliteStatement,
        index: Int,
        columnName: String,
        declaredType: String,
        normalizer: SourceIdNormalizer,
    ): JsonElement {
        if (st.isNull(index)) return JsonNull
        if (columnName == "created_at" || columnName == "updated_at") {
            return JsonPrimitive("<timestamp>")
        }
        if (columnName == "canonical_request_hash" || columnName == "remote_bundle_hash") {
            val value = st.getText(index)
            return JsonPrimitive(if (value.isBlank()) "" else "<hash>")
        }
        if (isSourceIdColumn(columnName)) {
            val value = st.getText(index)
            return JsonPrimitive(if (value.isBlank()) "" else normalizer.normalize(value))
        }
        return if ("INT" in declaredType) {
            JsonPrimitive(st.getLong(index))
        } else {
            JsonPrimitive(st.getText(index))
        }
    }

    private fun isSourceIdColumn(columnName: String): Boolean {
        return columnName == "source_id" ||
            columnName == "current_source_id" ||
            columnName == "replaced_by_source_id" ||
            columnName == "replacement_source_id"
    }

    private fun nullableString(st: SqliteStatement, index: Int): JsonElement {
        return if (st.isNull(index)) JsonNull else JsonPrimitive(st.getText(index))
    }

    private fun normalizeTriggerSql(sql: String): String {
        val collapsed = Regex("\\s+").replace(sql.trim(), " ")
        return collapsed
            .replace(
                Regex("^CREATE\\s+TRIGGER\\s+IF\\s+NOT\\s+EXISTS\\s+", RegexOption.IGNORE_CASE),
                "CREATE TRIGGER ",
            )
            .replace(Regex("^CREATE\\s+TRIGGER\\s+", RegexOption.IGNORE_CASE), "CREATE TRIGGER ")
    }

    private fun quoteIdentifier(identifier: String): String = "\"" + identifier.replace("\"", "\"\"") + "\""

    private fun JsonElement.jsonObjectString(key: String): String {
        return (this as JsonObject)[key].toString().trim('"')
    }

    protected data class RuntimeStateUsersEnv(
        val db: SafeSQLiteConnection,
        val server: HttpServer,
        val http: HttpClient,
        var client: DefaultOversqliteClient,
    )

    protected class SourceIdNormalizer {
        private val ids = linkedMapOf<String, String>()

        fun normalize(value: String): String {
            return ids.getOrPut(value) { "<source-${ids.size + 1}>" }
        }
    }

    private data class RuntimeIndexSummary(
        val tableName: String,
        val name: String,
        val unique: Boolean,
    )

    private data class RuntimeStateTable(
        val name: String,
        val orderBy: String,
    )

    @Serializable
    protected data class RuntimeStateTransitionSpec(
        val formatVersion: Int,
        val cases: List<RuntimeStateTransitionCase>,
    )

    @Serializable
    protected data class RuntimeStateTransitionCase(
        val name: String,
        val description: String = "",
        val serverScript: RuntimeStateServerScript = RuntimeStateServerScript(),
        val steps: List<RuntimeStateTransitionStep>,
    )

    @Serializable
    protected data class RuntimeStateServerScript(
        val kind: String = "default",
        val bundleSeq: Long? = null,
        val replacementSourceId: String? = null,
        val response: JsonObject? = null,
    )

    @Serializable
    protected data class RuntimeStateTransitionStep(
        val action: String,
        val sql: List<String> = emptyList(),
        val expectedException: String = "none",
        val expectedState: JsonObject? = null,
    )

    private companion object {
        val runtimeStateTables = listOf(
            RuntimeStateTable("_sync_apply_state", "singleton_key"),
            RuntimeStateTable("_sync_attachment_state", "singleton_key"),
            RuntimeStateTable("_sync_source_state", "source_id"),
            RuntimeStateTable("_sync_operation_state", "singleton_key"),
            RuntimeStateTable("_sync_outbox_bundle", "singleton_key"),
            RuntimeStateTable("_sync_outbox_rows", "source_bundle_id, row_ordinal"),
            RuntimeStateTable("_sync_dirty_rows", "dirty_ordinal, table_name, key_json"),
            RuntimeStateTable("_sync_row_state", "schema_name, table_name, key_json"),
            RuntimeStateTable("_sync_snapshot_stage", "snapshot_id, row_ordinal"),
            RuntimeStateTable("_sync_managed_tables", "schema_name, table_name"),
        )
    }
}
