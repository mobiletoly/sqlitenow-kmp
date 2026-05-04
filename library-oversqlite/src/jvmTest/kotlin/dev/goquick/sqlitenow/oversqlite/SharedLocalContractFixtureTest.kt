package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.getColumnNames
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals

class SharedLocalContractFixtureTest : BundleClientContractTestSupport() {
    private val contractJson = Json {
        prettyPrint = true
        prettyPrintIndent = "  "
        ignoreUnknownKeys = true
        explicitNulls = true
    }

    private val fixtureRoot = findRepoRoot().resolve("oversqlite-contracts/local-schema")
    private val updateExpected =
        System.getProperty("oversqlite.contracts.update") == "true" ||
            System.getenv("OVERSQLITE_CONTRACTS_UPDATE") == "true"

    @Test
    fun kmpSharedLocalSchemaSnapshotsMatchExpected() = runBlocking {
        for (fixture in loadFixtures()) {
            val actual = withOpenedFixture(fixture) { db ->
                dumpCatalogSnapshot(db, fixture.name)
            }
            assertOrUpdateExpected(
                expectedPath = fixture.path.resolve("schema.expected.json"),
                actual = actual,
            )
        }
    }

    @Test
    fun kmpSharedLocalWriteTransitionsMatchExpected() = runBlocking {
        for (fixture in loadFixtures()) {
            val actual = runWriteTransitions(fixture)
            assertOrUpdateExpected(
                expectedPath = fixture.path.resolve("write-transitions.expected.json"),
                actual = actual,
            )
        }
    }

    private suspend fun runWriteTransitions(fixture: LocalSchemaFixture): JsonElement {
        val spec = contractJson.decodeFromString<WriteTransitionsSpec>(
            fixture.path.resolve("write-transitions.json").readText(),
        )
        val caseResults = mutableListOf<JsonElement>()
        for (case in spec.cases) {
            val caseResult = withOpenedFixture(fixture) { db ->
                executeStatements(db, case.setupSql)
                executeStatements(db, case.actionSql)
                buildJsonObject {
                    put("name", JsonPrimitive(case.name))
                    put("dirtyRows", dumpDirtyRows(db))
                    put("applicationRowCounts", dumpApplicationRowCounts(db, spec.applicationTables))
                }
            }
            caseResults += caseResult
        }
        return buildJsonObject {
            put("formatVersion", JsonPrimitive(1))
            put("fixture", JsonPrimitive(fixture.name))
            put("cases", JsonArray(caseResults))
        }
    }

    private suspend fun <T> withOpenedFixture(
        fixture: LocalSchemaFixture,
        block: suspend (SafeSQLiteConnection) -> T,
    ): T {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            executeSqlScript(db, fixture.path.resolve("schema.sql").readText())
            val config = contractJson.decodeFromString<FixtureConfig>(
                fixture.path.resolve("config.json").readText(),
            )
            val client = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = config.schema,
                    syncTables = config.syncTables.map {
                        SyncTable(tableName = it.tableName, syncKeyColumnName = it.syncKeyColumnName)
                    },
                ),
                http = http,
                resolver = ServerWinsResolver,
            )
            client.open().getOrThrow()
            return block(db)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun loadFixtures(): List<LocalSchemaFixture> {
        require(fixtureRoot.exists()) { "missing Oversqlite contract fixture root: $fixtureRoot" }
        return Files.list(fixtureRoot).use { stream ->
            stream
                .filter { Files.isDirectory(it) }
                .sorted { left, right -> left.name.compareTo(right.name) }
                .map { LocalSchemaFixture(name = it.name, path = it) }
                .toList()
        }
    }

    private suspend fun executeSqlScript(db: SafeSQLiteConnection, sql: String) {
        executeStatements(
            db,
            sql
                .lineSequence()
                .filterNot { it.trimStart().startsWith("--") }
                .joinToString("\n")
                .split(";")
                .map { it.trim() }
                .filter { it.isNotEmpty() },
        )
    }

    private suspend fun executeStatements(db: SafeSQLiteConnection, statements: List<String>) {
        for (statement in statements) {
            db.execSQL(statement)
        }
    }

    private suspend fun dumpCatalogSnapshot(db: SafeSQLiteConnection, fixtureName: String): JsonElement {
        val tableNames = namesByType(db, "table")
        val viewNames = namesByType(db, "view")
        val triggerNames = namesByType(db, "trigger")
        val tables = mutableListOf<JsonElement>()
        for (tableName in tableNames) {
            tables += dumpTable(db, tableName)
        }
        val views = mutableListOf<JsonElement>()
        for (viewName in viewNames) {
            views += dumpView(db, viewName)
        }
        val triggers = mutableListOf<JsonElement>()
        for (triggerName in triggerNames) {
            triggers += dumpTrigger(db, triggerName)
        }
        return buildJsonObject {
            put("formatVersion", JsonPrimitive(1))
            put("fixture", JsonPrimitive(fixtureName))
            put("catalog", dumpSqliteCatalog(db))
            put("tables", JsonArray(tables))
            put("views", JsonArray(views))
            put("triggers", JsonArray(triggers))
        }
    }

    private suspend fun dumpSqliteCatalog(db: SafeSQLiteConnection): JsonArray {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name
                """.trimIndent(),
            ).use { st ->
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                put("type", JsonPrimitive(st.getText(0)))
                                put("name", JsonPrimitive(st.getText(1)))
                                put("tableName", JsonPrimitive(st.getText(2)))
                                put("sql", nullableString(st, 3, normalizeTriggers = st.getText(0) == "trigger"))
                            },
                        )
                    }
                }
            }
        }
    }

    private suspend fun namesByType(db: SafeSQLiteConnection, type: String): List<String> {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT name
                FROM sqlite_schema
                WHERE type = ? AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, type)
                buildList {
                    while (st.step()) {
                        add(st.getText(0))
                    }
                }
            }
        }
    }

    private suspend fun dumpTable(db: SafeSQLiteConnection, tableName: String): JsonObject {
        return buildJsonObject {
            put("name", JsonPrimitive(tableName))
            put("columns", dumpPragmaTableInfo(db, tableName))
            put("foreignKeys", dumpPragmaForeignKeys(db, tableName))
            put("indexes", dumpIndexes(db, tableName))
        }
    }

    private suspend fun dumpView(db: SafeSQLiteConnection, viewName: String): JsonObject {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT sql
                FROM sqlite_schema
                WHERE type = 'view' AND name = ?
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, viewName)
                check(st.step())
                buildJsonObject {
                    put("name", JsonPrimitive(viewName))
                    put("sql", nullableString(st, 0))
                }
            }
        }
    }

    private suspend fun dumpTrigger(db: SafeSQLiteConnection, triggerName: String): JsonObject {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT tbl_name, sql
                FROM sqlite_schema
                WHERE type = 'trigger' AND name = ?
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, triggerName)
                check(st.step())
                buildJsonObject {
                    put("name", JsonPrimitive(triggerName))
                    put("tableName", JsonPrimitive(st.getText(0)))
                    put("sql", nullableString(st, 1, normalizeTriggers = true))
                }
            }
        }
    }

    private suspend fun dumpPragmaTableInfo(db: SafeSQLiteConnection, tableName: String): JsonArray {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA table_info(${quoteIdentifier(tableName)})").use { st ->
                val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                put("cid", JsonPrimitive(st.getLong(columns.getValue("cid"))))
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

    private suspend fun dumpPragmaForeignKeys(db: SafeSQLiteConnection, tableName: String): JsonArray {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA foreign_key_list(${quoteIdentifier(tableName)})").use { st ->
                val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                put("id", JsonPrimitive(st.getLong(columns.getValue("id"))))
                                put("seq", JsonPrimitive(st.getLong(columns.getValue("seq"))))
                                put("table", JsonPrimitive(st.getText(columns.getValue("table"))))
                                put("from", JsonPrimitive(st.getText(columns.getValue("from"))))
                                put("to", JsonPrimitive(st.getText(columns.getValue("to"))))
                                put("onUpdate", JsonPrimitive(st.getText(columns.getValue("on_update"))))
                                put("onDelete", JsonPrimitive(st.getText(columns.getValue("on_delete"))))
                                put("match", JsonPrimitive(st.getText(columns.getValue("match"))))
                            },
                        )
                    }
                }
            }
        }
    }

    private suspend fun dumpIndexes(db: SafeSQLiteConnection, tableName: String): JsonArray {
        val indexes = db.withExclusiveAccess {
            db.prepare("PRAGMA index_list(${quoteIdentifier(tableName)})").use { st ->
                val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                buildList {
                    while (st.step()) {
                        add(
                            IndexSummary(
                                seq = st.getLong(columns.getValue("seq")),
                                name = st.getText(columns.getValue("name")),
                                unique = st.getLong(columns.getValue("unique")) == 1L,
                                origin = st.getText(columns.getValue("origin")),
                                partial = st.getLong(columns.getValue("partial")) == 1L,
                            ),
                        )
                    }
                }
            }
        }
        val result = mutableListOf<JsonElement>()
        for (index in indexes) {
            result += buildJsonObject {
                put("seq", JsonPrimitive(index.seq))
                put("name", JsonPrimitive(index.name))
                put("unique", JsonPrimitive(index.unique))
                put("origin", JsonPrimitive(index.origin))
                put("partial", JsonPrimitive(index.partial))
                put("columns", dumpIndexColumns(db, index.name))
            }
        }
        return JsonArray(result)
    }

    private suspend fun dumpIndexColumns(db: SafeSQLiteConnection, indexName: String): JsonArray {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA index_xinfo(${quoteIdentifier(indexName)})").use { st ->
                val columns = st.getColumnNames().mapIndexed { index, name -> name.lowercase() to index }.toMap()
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                put("seqno", JsonPrimitive(st.getLong(columns.getValue("seqno"))))
                                put("cid", JsonPrimitive(st.getLong(columns.getValue("cid"))))
                                put("name", nullableString(st, columns.getValue("name")))
                                put("desc", JsonPrimitive(st.getLong(columns.getValue("desc")) == 1L))
                                put("collation", nullableString(st, columns.getValue("coll")))
                                put("key", JsonPrimitive(st.getLong(columns.getValue("key")) == 1L))
                            },
                        )
                    }
                }
            }
        }
    }

    private suspend fun dumpDirtyRows(db: SafeSQLiteConnection): JsonArray {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal
                FROM _sync_dirty_rows
                ORDER BY dirty_ordinal, table_name, key_json
                """.trimIndent(),
            ).use { st ->
                buildJsonArray {
                    while (st.step()) {
                        add(
                            buildJsonObject {
                                put("schemaName", JsonPrimitive(st.getText(0)))
                                put("tableName", JsonPrimitive(st.getText(1)))
                                put("keyJson", JsonPrimitive(st.getText(2)))
                                put("op", JsonPrimitive(st.getText(3)))
                                put("baseRowVersion", JsonPrimitive(st.getLong(4)))
                                put("payload", nullableString(st, 5))
                                put("dirtyOrdinal", JsonPrimitive(st.getLong(6)))
                            },
                        )
                    }
                }
            }
        }
    }

    private suspend fun dumpApplicationRowCounts(
        db: SafeSQLiteConnection,
        tables: List<String>,
    ): JsonObject {
        return buildJsonObject {
            for (table in tables.sorted()) {
                val count = db.withExclusiveAccess {
                    db.prepare("SELECT COUNT(*) FROM ${quoteIdentifier(table)}").use { st ->
                        check(st.step())
                        st.getLong(0)
                    }
                }
                put(table, JsonPrimitive(count))
            }
        }
    }

    private fun nullableString(
        st: SqliteStatement,
        index: Int,
        normalizeTriggers: Boolean = false,
    ): JsonElement {
        if (st.isNull(index)) {
            return JsonNull
        }
        val value = st.getText(index)
        return JsonPrimitive(if (normalizeTriggers) normalizeTriggerSql(value) else value)
    }

    private fun normalizeTriggerSql(sql: String): String {
        val collapsed = Regex("\\s+").replace(sql.trim(), " ")
        return collapsed
            .replace(
                Regex("^CREATE\\s+TRIGGER\\s+IF\\s+NOT\\s+EXISTS\\s+", RegexOption.IGNORE_CASE),
                "CREATE TRIGGER ",
            )
            .replace(
                Regex("^CREATE\\s+TRIGGER\\s+", RegexOption.IGNORE_CASE),
                "CREATE TRIGGER ",
            )
    }

    private fun assertOrUpdateExpected(expectedPath: Path, actual: JsonElement) {
        val actualText = contractJson.encodeToString(actual) + "\n"
        if (updateExpected) {
            expectedPath.parent.createDirectories()
            expectedPath.writeText(actualText)
            return
        }
        val expected = contractJson.parseToJsonElement(expectedPath.readText())
        assertEquals(expected, actual, "contract fixture mismatch: $expectedPath")
    }

    private fun quoteIdentifier(identifier: String): String = "\"" + identifier.replace("\"", "\"\"") + "\""

    private fun findRepoRoot(): Path {
        var current = Paths.get("").toAbsolutePath()
        while (true) {
            if (current.resolve("settings.gradle.kts").exists()) {
                return current
            }
            current = current.parent ?: error("could not locate repository root from ${Paths.get("").toAbsolutePath()}")
        }
    }

    private data class LocalSchemaFixture(
        val name: String,
        val path: Path,
    )

    private data class IndexSummary(
        val seq: Long,
        val name: String,
        val unique: Boolean,
        val origin: String,
        val partial: Boolean,
    )

    @Serializable
    private data class FixtureConfig(
        val schema: String,
        val syncTables: List<FixtureSyncTable>,
    )

    @Serializable
    private data class FixtureSyncTable(
        val tableName: String,
        val syncKeyColumnName: String,
    )

    @Serializable
    private data class WriteTransitionsSpec(
        val formatVersion: Int,
        val applicationTables: List<String>,
        val cases: List<WriteTransitionCase>,
    )

    @Serializable
    private data class WriteTransitionCase(
        val name: String,
        @SerialName("setupSql") val setupSql: List<String> = emptyList(),
        @SerialName("actionSql") val actionSql: List<String>,
    )
}
