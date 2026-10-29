package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlTracePolicy
import dev.goquick.sqlitenow.common.SqliteNowLogger
import io.ktor.client.HttpClient
import kotlinx.serialization.json.*

/** Synthetic library fixture; children precede parents in canonical local order. */
internal open class SnapshotBootstrapTestSupport : CrossTargetSyncTestSupport() {
    protected class BootstrapDiagnosticCapture(private val printRecords: Boolean = false) : SqliteNowLogger {
        val lines = mutableListOf<String>()
        fun clear() = lines.clear()
        fun records() = lines.map { Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject }
        private fun record(message: () -> String) {
            val text = message()
            if (text.startsWith("SQLITENOW_DIAG ")) {
                check(lines.size < 20_000)
                lines += text
                if (printRecords) println(text)
            }
        }
        override fun e(throwable: Throwable?, message: () -> String) = record(message)
        override fun w(throwable: Throwable?, message: () -> String) = record(message)
        override fun i(throwable: Throwable?, message: () -> String) = record(message)
        override fun d(throwable: Throwable?, message: () -> String) = record(message)
    }

    protected fun bootstrapDiagnosticCounts(records: List<JsonObject>, rootName: String): Map<String, Long> {
        val root = records.single { it["kind"]?.jsonPrimitive?.content == "span_start" && it["name"]?.jsonPrimitive?.content == rootName }
        val sameCapture = records.filter { it["capture"] == root["capture"] }
        val families = sameCapture.filter { it["kind"]?.jsonPrimitive?.content == "sql_family" }
            .associate { it.getValue("family") to it.getValue("name").jsonPrimitive.content }
        val metrics = sameCapture.filter { it["span"] == root["span"] }
        val peaks = sameCapture.filter { it["name"]?.jsonPrimitive?.content == "snapshot.bootstrap.workspace" }
            .map { it.getValue("counts").jsonObject }
        fun calls(kind: String, name: String? = null, family: String? = null) = metrics.filter {
            it["kind"]?.jsonPrimitive?.content == kind && (name == null || it["name"]?.jsonPrimitive?.content == name) &&
                (family == null || families[it["family"]] == family)
        }.sumOf { it.getValue("counts").jsonObject.getValue("calls").jsonPrimitive.long }
        return linkedMapOf("driver_steps" to calls("sql_metric", "step"), "quota_reads" to calls("sql_metric", "step", "workspace.usage"),
            "received_rows" to calls("work_metric", family = "snapshot.bootstrap.received_rows"),
            "canonicalizations" to calls("work_metric", family = "snapshot.bootstrap.received_canonicalizations"),
            "scalar_decodes" to calls("work_metric", family = "row_processing.typed_scalar_decode"),
            "wire_rows" to calls("work_metric", family = "snapshot.bootstrap.wire_rows_verified"),
            "projection_rows" to calls("work_metric", family = "snapshot.bootstrap.projection_rows_verified"),
            "stage_pages" to calls("work_metric", family = "snapshot.bootstrap.admitted_pages"),
            "install_pages" to calls("work_metric", family = "snapshot.bootstrap.install_pages"),
            "installation_rows" to calls("work_metric", family = "snapshot.bootstrap.installation_rows"),
            "cleanup_rows" to calls("work_metric", family = "snapshot.bootstrap.cleanup_rows"),
            "legacy_wire_decodes" to calls("work_metric", family = "row_processing.wire_decode"),
            "legacy_wire_normalizations" to calls("work_metric", family = "row_processing.wire_normalize"),
            "peak_workspace_entries" to (peaks.maxOfOrNull { it.getValue("admitted_peak_entries").jsonPrimitive.long } ?: 0),
            "peak_workspace_bytes" to (peaks.maxOfOrNull { it.getValue("admitted_peak_bytes").jsonPrimitive.long } ?: 0),
            "peak_durable_entries" to (peaks.maxOfOrNull { it.getValue("durable_peak_entries").jsonPrimitive.long } ?: 0),
            "peak_durable_bytes" to (peaks.maxOfOrNull { it.getValue("durable_peak_bytes").jsonPrimitive.long } ?: 0),
            "table_checks" to calls("work_metric", family = "snapshot.bootstrap.table_checks"),
            "foreign_key_checks" to calls("work_metric", family = "snapshot.bootstrap.foreign_key_checks"))
    }

    protected fun bootstrapFixtureBundles(count: Int): List<BundleRow> {
        require(count > 0 && count % 2 == 0)
        val parents = (0 until count / 2).map { n -> BundleRow("main", "bootstrap_parents", mapOf("id" to "parent-$n"), "INSERT", 1,
            buildJsonObject { put("id", "parent-$n"); put("label", "Parent $n") }) }
        val children = (0 until count / 2).map { n -> BundleRow("main", "bootstrap_children", mapOf("id" to "child-$n"), "INSERT", 1,
            buildJsonObject { put("id", "child-$n"); put("parent_id", "parent-$n"); put("tag", JsonNull)
                put("amount", "9223372036854775807"); put("data", "AAEC") }) }
        return children + parents
    }
    protected data class BootstrapFixture(
        val db: SafeSQLiteConnection,
        val server: MockSyncServer,
        val http: HttpClient,
        val client: DefaultOversqliteClient,
        val config: OversqliteConfig,
        val generation: SyncGeneration,
    )

    protected data class BootstrapTransferFixture(
        val metadata: ValidatedConfig, val admission: BootstrapSchemaAdmission.Eligible,
        val session: SnapshotSession, val source: String, val transfer: String, val rows: List<BootstrapWireRow>,
    )

    protected fun bootstrapFixtureRows(): List<BootstrapWireRow> = listOf(
            BootstrapWireRow(0, JsonArray(listOf(JsonPrimitive("parent"))), 1,
                buildJsonObject { put("id", "parent"); put("label", "Parent") }),
            BootstrapWireRow(1, JsonArray(listOf(JsonPrimitive("child"))), 1,
                buildJsonObject { put("id", "child"); put("parent_id", "parent"); put("tag", JsonNull)
                    put("amount", "9223372036854775807"); put("data", JsonNull) }),
        )

    protected suspend fun prepareBootstrapTransfer(
        fixture: BootstrapFixture, empty: Boolean = false,
    ): BootstrapTransferFixture {
        bindFixtureDescriptor(fixture)
        val (metadata, admission) = withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
            metadata to kotlin.test.assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(fixture.db,
                metadata, version, workspace, "bootstrap-transfer-fixture", cache))
        }
        val rows = if (empty) emptyList() else bootstrapFixtureRows()
        val source = fixture.db.withPreparedStatement("SELECT current_source_id FROM _sync_attachment_state") { kotlin.test.assertTrue(it.step()); it.getText(0) }
        val hash = SnapshotHashWriter(source, fixture.generation, "durable-receipt", 2, null)
        var bytes = 0L
        for (row in rows) {
            hash.add(row.tableIndex, row.key, row.version, row.payload)
            bytes += utf8Size(canonicalizeJsonElement(JsonArray(listOf(JsonPrimitive("i"), JsonPrimitive(row.tableIndex),
                row.key, JsonPrimitive(row.version), row.payload))))
        }
        val session = SnapshotSession("durable-receipt", 2, rows.size.toLong(), bytes, "2099-01-01T00:00:00Z", hash.finish(rows.size.toLong(), bytes))
        return BootstrapTransferFixture(metadata, admission, session, source, Json.encodeToString(RetainedSnapshotSeal.serializer(),
            RetainedSnapshotSeal(session, source, fixture.config.serviceId)), rows)
    }

    protected suspend fun freshBootstrapTransfer(fixture: BootstrapFixture, previous: BootstrapTransferFixture): BootstrapTransferFixture =
        withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
            previous.copy(metadata = metadata, admission = kotlin.test.assertIs<BootstrapSchemaAdmission.Eligible>(
                admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-resume-fixture", cache)))
        }

    protected suspend fun <T> useBootstrapTransfer(
        fixture: BootstrapFixture, transfer: BootstrapTransferFixture, resume: Boolean = false,
        block: suspend (TypedSnapshotStage, OversqliteClientOperationController) -> T,
    ): T {
        val operations = OversqliteClientOperationController(fixture.config,
            OversqliteClientResourceOwner(fixture.db, fixture.http), SnapshotDiagnosticsState()) {}
        return operations.runExclusiveOperation {
            if (resume) TypedSnapshotStage.resume(fixture.db, fixture.config, transfer.admission.descriptor,
                transfer.session, fixture.generation, transfer.source, operations) { block(it, operations) }
            else TypedSnapshotStage.begin(fixture.db, fixture.config, transfer.admission, transfer.metadata,
                transfer.session, fixture.generation, transfer.source, "a".repeat(64), transfer.transfer, operations) { block(it, operations) }
        }.getOrThrow()
    }

    protected suspend fun withBootstrapFixture(dbPath: String? = null, parentCollation: String = "NOCASE", remoteSchema: String = "business", chunkRows: Int = 1000, chunkBytes: Long = 4L * 1024L * 1024L,
        sqlTrace: SqlTracePolicy? = null, block: suspend (BootstrapFixture) -> Unit) {
        require(parentCollation in setOf("BINARY", "NOCASE", "RTRIM"))
        require(remoteSchema in setOf("business", "main"))
        val parents = GenerationTable(remoteSchema, "bootstrap_parents", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("label", "text", false),
        ), emptyList())
        val children = GenerationTable(remoteSchema, "bootstrap_children", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("parent_id", "text", true),
            GenerationColumn("tag", "text", true), GenerationColumn("amount", "int64", false), GenerationColumn("data", "bytes", true),
        ), listOf(GenerationForeignKey(listOf("parent_id"), remoteSchema, "bootstrap_parents", listOf("id"), "restrict", "no_action")))
        val generation = SyncGeneration("1", "", listOf(parents, children)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val server = MockSyncServer().also { it.generationOverride = generation }
        val db = if (sqlTrace != null) {
            require(dbPath != null)
            val platformConfig = createSqliteNowTestConnectionConfig(dbPath) ?: SqliteConnectionConfig()
            BundledSqliteConnectionProvider.openConnection(dbPath, debug = true, config = platformConfig.copy(sqlTrace = sqlTrace))
        } else if (dbPath == null) newDb() else newFileBackedDb(dbPath)
        val http = server.newHttpClient()
        val tables = listOf(SyncTable("bootstrap_parents", "id"), SyncTable("bootstrap_children", "id"))
        val config = OversqliteConfig(1, tables, "fixture", "bootstrap",
            transferTargets = OversqliteTransferTargets(maxSnapshotChunkRows = chunkRows, maxSnapshotChunkDecodedBytes = chunkBytes))
        val client = newClient(db, http, syncTables = tables, schema = config.serviceId,
            workspaceEntries = config.maxSyncWorkspaceEntries, workspaceBytes = config.maxSyncWorkspaceBytes)
        try {
            db.execSQL("CREATE TABLE bootstrap_parents(id TEXT COLLATE $parentCollation PRIMARY KEY NOT NULL,label TEXT NOT NULL)")
            db.execSQL("CREATE UNIQUE INDEX bootstrap_parent_labels ON bootstrap_parents(label COLLATE RTRIM)")
            db.execSQL("""CREATE TABLE bootstrap_children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT REFERENCES bootstrap_parents(id)
                ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,tag TEXT,amount INTEGER NOT NULL,data BLOB,UNIQUE(parent_id,tag))""")
            db.execSQL("CREATE INDEX bootstrap_child_parent ON bootstrap_children(parent_id COLLATE $parentCollation)")
            db.execSQL("CREATE TABLE local_memos(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
            db.execSQL("INSERT INTO local_memos VALUES(1,'local-only-sentinel')")
            client.openAndConnect("fixture-owner").getOrThrow()
            block(BootstrapFixture(db, server, http, client, config, generation))
        } finally {
            client.closeFromCoordinatedOwner(); http.close(); db.close()
        }
    }

    /** Assemble normal internal owners in test support; no product force-fast switch. */
    protected suspend fun <T> withAutomaticBootstrapWorkflow(fixture: BootstrapFixture, userId: String = "fixture-owner",
        beforeTransfer: suspend (OversqliteRemoteApi, RuntimeState, SnapshotDiagnosticsRecorder) -> Unit = { _, _, _ -> },
        block: suspend (OversqliteDownloadWorkflow, RuntimeState) -> T): T {
        val metadata = SyncRuntimeInitializer(fixture.config, TableInfoCache()).validateConfig(fixture.db).withGeneration(fixture.generation)
        val source = fixture.db.withPreparedStatement("SELECT current_source_id FROM _sync_attachment_state") { require(it.step()); it.getText(0) }
        val resource = OversqliteClientResourceOwner(fixture.db, fixture.http)
        val snapshotState = SnapshotDiagnosticsState()
        val operations = OversqliteClientOperationController(fixture.config, resource, snapshotState) {}
        val local = OversqliteLocalStore(fixture.db, Json) { metadata }
        val sync = OversqliteSyncStateStore(fixture.db) { local }
        val outbox = OversqliteOutboxStateStore(fixture.db)
        val attachment = OversqliteAttachmentStateStore(fixture.db)
        val operation = OversqliteOperationStateStore(fixture.db)
        val automatic = OversqliteAutomaticRecovery(fixture.db, fixture.config, operations)
        val survivors = OversqliteApplySurvivors(fixture.db, local, fixture.config, operations, Json)
        val remote = OversqliteRemoteApi(resource.http, Json, generation = { fixture.generation }, log = {},
            diagnostics = fixture.config.diagnostics, beforeRecoveryExchange = operations::chargeRecoveryExchange, snapshotDiagnostics = snapshotState::current)
        val workflow = OversqliteDownloadWorkflow(automatic, operations, fixture.db, fixture.config, remote,
            OversqliteSourceStateStore(fixture.db), outbox, attachment, operation, sync, local,
            OversqliteBundleApplier(local, sync, survivors), OversqliteStageStore(fixture.db, local, sync, Json, fixture.config, operations),
            OversqliteApplyExecutor(fixture.db, OversqliteApplyStateStore(fixture.db), operations),
            OversqliteSnapshotGate(automatic, fixture.db, sync, outbox, attachment, operation), snapshotState::current, Json,
            notifyUpdatedTables = fixture.db::reportExternalTableChanges, log = {})
        val runtime = RuntimeState(metadata, checkNotNull(OversqliteContractStore(fixture.db).readAdmission(1)), userId, source)
        try { return operations.runExclusiveOperation { operations.withSnapshotDiagnosticsScope {
            beforeTransfer(remote, runtime, checkNotNull(snapshotState.current()))
            block(workflow, runtime)
        } }.getOrThrow() }
        finally { resource.closeFromCoordinatedOwner() }
    }

    protected suspend fun <T> withBootstrapMetadata(
        fixture: BootstrapFixture,
        operations: StatementCacheOperations = StatementCacheOperations(),
        block: suspend (ValidatedConfig, Long, OversqliteWorkspace, StatementCache) -> T,
    ): T = fixture.db.transaction(TransactionMode.IMMEDIATE) {
        val version = fixture.db.scalarLong("PRAGMA main.schema_version")
        val metadata = SyncRuntimeInitializer(fixture.config, TableInfoCache()).validateConfig(fixture.db)
            .withGeneration(fixture.generation)
        fixture.db.withStatementCache(operations, cleanupTimeoutMillis = 5_000) { cache ->
            block(metadata, version, OversqliteWorkspace(fixture.db, fixture.config), cache)
        }
    }

    protected suspend fun admitFixture(fixture: BootstrapFixture): BootstrapSchemaAdmission =
        withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
            admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-schema", cache)
        }

    protected suspend fun bindFixtureDescriptor(fixture: BootstrapFixture): SnapshotDescriptor =
        withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
            val admission = kotlin.test.assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(
                fixture.db, metadata, version, workspace, "bootstrap-layout", cache))
            bindBootstrapDescriptor(fixture.db, admission, metadata, workspace, cache)
            workspace.reconcileRetainedOwners()
            admission.descriptor
        }

    /** Metadata-only negative case; real preparation, stepping and cleanup remain owned. */
    protected fun customCollationMetadataOperations(resetFailure: Throwable? = null): StatementCacheOperations {
        var watched: SQLiteStatement? = null
        var pending = resetFailure
        return StatementCacheOperations(prepare = { db, sql ->
            val statement = db.prepare(sql)
            if (sql.startsWith("SELECT name,coll,") && "pragma_index_xinfo" in sql)
                object : SQLiteStatement by statement {
                    override fun getText(index: Int): String = if (index == 1) "DRIVER_PRIVATE" else statement.getText(index)
                }.also { watched = it }
            else statement
        }, reset = { statement ->
            statement.reset()
            if (statement === watched) pending?.let { failure -> pending = null; throw failure }
        })
    }
}
