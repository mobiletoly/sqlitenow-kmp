package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.json.*
import kotlin.test.*
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf

internal class TypedSnapshotStageTest : SnapshotBootstrapTestSupport() {
    @Test fun bulkAuthoritativeSqlMatchesCurrentCodecAtScalarEdges() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val binding = createStage(db)
            fixtureRows().forEachIndexed { index, row -> insert(db, binding, index + 1L, row) }
            executeRange(db, binding.businessSql)
            db.execSQL("""CREATE TABLE _sync_row_state(schema_name TEXT,table_name TEXT,key_json TEXT,row_version INTEGER,
                server_kind TEXT,server_payload TEXT,server_provenance TEXT,relationships_known INTEGER,server_revision INTEGER,
                server_context_id INTEGER GENERATED ALWAYS AS(json_extract(server_provenance,'$.context_id')) VIRTUAL)""")
            db.withPreparedStatement(binding.baselineSql) {
                it.bindLong(1, 1); it.bindLong(2, 5); it.bindLong(3, 0)
                it.bindText(4, "snapshot"); it.bindLong(5, 0); it.bindLong(6, 3); it.step()
            }
            val oracle = linkedMapOf<String, JsonObject>()
            db.withPreparedStatement("SELECT data,id,label,n,r FROM items ORDER BY id") {
                while (it.step()) oracle[it.getText(1)] = OversqliteValueCodec.encodeLocalPayloadObject(binding.projectionInfo, it)
            }
            db.withPreparedStatement("SELECT key_json,server_payload FROM _sync_row_state ORDER BY key_json") {
                var count = 0
                while (it.step()) {
                    val key = Json.parseToJsonElement(it.getText(0)).jsonObject.getValue("id").jsonPrimitive.content
                    val payload = Json.parseToJsonElement(it.getText(1)).jsonObject
                    assertTrue(OversqliteValueCodec.equivalentPayloadObjects(binding.projectionInfo, oracle.getValue(key), payload,
                        PayloadSource.LOCAL_STATE, PayloadSource.LOCAL_STATE), key)
                    count++
                }
                assertEquals(3, count)
            }
            suspend fun verify() = db.withPreparedStatement(binding.authoritativeVerificationSql) {
                it.bindText(1, "snapshot"); it.bindLong(2, 0); it.bindLong(3, 5); it.bindLong(4, 1); it.bindLong(5, 5)
                assertTrue(it.step()); it.getLong(0)
            }
            assertEquals(0L, verify())
            db.execSQL("UPDATE _sync_row_state SET server_payload=json_set(server_payload,'$.r.value','0') WHERE key_json='{\"id\":\"two\"}'")
            assertEquals(1L, verify(), "the new authoritative proof requires its exact SQLite REAL representation")
        } finally { db.close() }
    }

    @Test fun receiptAndProjectionByteTargetsHaveExactBoundaries() = runOwnedWebDatabaseTest {
        for (fits in listOf(true, false)) withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            val row = transfer.rows.first()
            val bytes = encodedJsonSize(row.key) + encodedJsonSize(row.payload)
            val bounded = fixture.copy(config = fixture.config.copy(snapshotApplyBatchBytes = bytes - if (fits) 0 else 1))
            useBootstrapTransfer(bounded, transfer) { stage, _ ->
                if (fits) {
                    stage.receive(listOf(row))
                    assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                } else {
                    val failure = assertFails { stage.receive(listOf(row)) }
                    assertTrue(failure.message.orEmpty().contains("receipt row exceeds admitted page target"))
                    assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                    assertEquals(0L, fixture.db.scalarLong("SELECT bootstrap_page_count+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
                }
            }
        }
        val measured = BootstrapStageMeasure(10, 100, 100, 100, 31, 7, 19, WorkspaceCharge(), listOf(10))
        val config = OversqliteConfig(1, listOf(SyncTable("items", "id")), "fixture", "main", snapshotApplyBatchBytes = 57)
        assertEquals(1, bootstrapProjectionPageRows(config, measured))
        assertEquals(2, bootstrapProjectionPageRows(config.copy(snapshotApplyBatchBytes = 114), measured))
        assertFails { bootstrapProjectionPageRows(config.copy(snapshotApplyBatchBytes = 56), measured) }
        assertEquals(1, bootstrapProjectionPageRows(config.copy(snapshotApplyBatchBytes = 1), measured.copy(rows = 0)))
    }

    @Test fun typedStageChargesFitExactlyAndRefuseOneUnitOrByteLess() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            val owner = Any()
            val incoming = transfer.rows.first()
            var host = WorkspaceCharge()
            val row = BootstrapReceivedRow.decode(owner, transfer.admission.descriptor.tables.single { it.tableIndex == incoming.tableIndex }, transfer.session.snapshotId,
                1, incoming.version, transfer.session.snapshotBundleSeq, incoming.key, incoming.payload) { host = it }
            try { withBootstrapMetadata(fixture) { _, _, workspace, cache ->
                val base = workspace.readDurableBase("bootstrap-exact-fit", cache)
                val extra = workspace.add("bootstrap-exact-fit", transfer.admission.descriptor.charge,
                    workspace.add("bootstrap-exact-fit", host, workspace.add("bootstrap-exact-fit", bootstrapPageCharge(transfer.session.snapshotId),
                        workspace.add("bootstrap-exact-fit", WorkspaceCharge(bytes = 6 * 64), workspace.add("bootstrap-exact-fit", row.wireCharge, row.typedCharge)))))
                val exact = fixture.config.copy(maxSyncWorkspaceEntries = base.entries + extra.entries,
                    maxSyncWorkspaceBytes = base.bytes + extra.bytes)
                OversqliteWorkspace(fixture.db, exact).checkCapturedReadBase("bootstrap-exact-fit", base, extra)
                val entries = assertFailsWith<SyncWorkspaceLimitException> {
                    OversqliteWorkspace(fixture.db, exact.copy(maxSyncWorkspaceEntries = exact.maxSyncWorkspaceEntries - 1))
                        .checkCapturedReadBase("bootstrap-exact-fit", base, extra)
                }
                val bytes = assertFailsWith<SyncWorkspaceLimitException> {
                    OversqliteWorkspace(fixture.db, exact.copy(maxSyncWorkspaceBytes = exact.maxSyncWorkspaceBytes - 1))
                        .checkCapturedReadBase("bootstrap-exact-fit", base, extra)
                }
                assertTrue(entries.message.orEmpty().contains("entries"))
                assertTrue(bytes.message.orEmpty().contains("bytes"))
                assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
            } } finally { row.close() }
        }
    }

    @Test fun lowerLimitsRejectSealedReopenWithoutChangingOwners() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("bootstrap-lower-limit")
        try { withBootstrapFixture(dbPath = path) { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            useBootstrapTransfer(fixture, transfer) { stage, _ -> stage.receive(transfer.rows); stage.seal() }
            val operation = OversqliteOperationStateStore(fixture.db).loadState()
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val db = newFileBackedDb(path)
            try {
                val tables = listOf("_sync_operation_state", "_sync_bootstrap_state", "_sync_bootstrap_tables", "_sync_bootstrap_wire", "_sync_bootstrap_pages",
                    "_sync_bootstrap_values_t0", "_sync_bootstrap_values_t1", "bootstrap_parents", "bootstrap_children", "local_memos")
                val before = baselineQualificationVector(db, tables)
                for (config in listOf(fixture.config.copy(maxSyncWorkspaceEntries = 1), fixture.config.copy(maxSyncWorkspaceBytes = 1),
                    fixture.config.copy(snapshotApplyBatchBytes = 1))) {
                    assertFails { inspectRetainedRuntime(db, config) }
                    assertEquals(before, baselineQualificationVector(db, tables))
                    assertEquals(operation, OversqliteOperationStateStore(db).loadState())
                }
                assertEquals("CURRENT", inspectRetainedRuntime(db, fixture.config))
            } finally { db.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun cancellationAfterCommittedPageResumesWithoutDuplicatingRows() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            val committed = CompletableDeferred<Unit>()
            var abandoned: TypedSnapshotStage? = null
            coroutineScope {
                val job = launch {
                    useBootstrapTransfer(fixture, transfer) { stage, _ ->
                        abandoned = stage
                        stage.receive(transfer.rows.take(1))
                        committed.complete(Unit)
                        awaitCancellation()
                    }
                }
                committed.await()
                job.cancelAndJoin()
            }
            assertEquals(1L, OversqliteOperationStateStore(fixture.db).loadState().snapshotStagedRowCount)
            val expired = assertFails { checkNotNull(abandoned).receive(transfer.rows) }
            assertTrue(expired.message.orEmpty().contains("expired operation"))
            useBootstrapTransfer(fixture, transfer, resume = true) { stage, _ ->
                stage.receive(transfer.rows.drop(1)); stage.seal()
            }
            assertEquals(2L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
            assertEquals(2L, fixture.db.scalarLong("SELECT bootstrap_page_count FROM _sync_operation_state"))
            assertEquals(6L, fixture.db.scalarLong("SELECT snapshot_stage_entries FROM _sync_operation_state"))
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
        }
    }
    @Test fun rolledBackPageCannotReuseAdvancedDigests() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("bootstrap-page-rollback")
        try { withBootstrapFixture(dbPath = path) { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            val before = useBootstrapTransfer(fixture, transfer) { stage, _ ->
                val before = OversqliteOperationStateStore(fixture.db).loadState()
                // A real stage unique-index violation follows digest mutation.
                // Some drivers quarantine the failed connection during cleanup;
                // persistence is therefore checked after a physical reopen.
                val failure = assertFails { stage.receive(listOf(transfer.rows[0], transfer.rows[0])) }
                assertTrue(generateSequence(failure) { it.cause }.any { it.message.orEmpty().contains("constraint", ignoreCase = true) })
                val poisoned = assertFails { stage.receive(transfer.rows) }
                assertTrue(poisoned.message.orEmpty().contains("verify the committed prefix"))
                before
            }
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val reopened = newFileBackedDb(path)
            val client = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables, schema = fixture.config.serviceId)
            try {
                client.open().getOrThrow()
                assertEquals(before, OversqliteOperationStateStore(reopened).loadState())
                assertEquals(0L, reopened.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                assertEquals(0L, reopened.scalarLong("SELECT count(*) FROM _sync_bootstrap_pages"))
                assertEquals(0L, reopened.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
                val resumedFixture = fixture.copy(db = reopened, client = client)
                val resumed = freshBootstrapTransfer(resumedFixture, transfer)
                useBootstrapTransfer(resumedFixture, resumed, resume = true) { stage, _ ->
                    stage.receive(resumed.rows); stage.seal()
                    reopened.transaction(dev.goquick.sqlitenow.core.TransactionMode.DEFERRED) {
                        reopened.withStatementCache(cleanupTimeoutMillis = 5_000) { stage.verifyForApply(it) }
                    }
                }
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
            } finally { client.closeFromCoordinatedOwner(); reopened.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun zeroRowReceiptSealsVerifiesAndCleansWithoutRowAllocation() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture, empty = true)
            useBootstrapTransfer(fixture, transfer) { stage, _ ->
                stage.seal()
                assertEquals(0L, fixture.db.scalarLong("SELECT bootstrap_page_count FROM _sync_operation_state"))
                assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
                fixture.db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                    fixture.db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                        assertEquals(listOf(0L, 0L), stage.verifyForApply(cache).tables.map { it.rows })
                        stage.clearAfterInstallation(cache)
                    }
                }
            }
            assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
        }
    }
    @Test fun receivedPagesCommitCountersAndSealIndependentEvidence() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val descriptor = bindFixtureDescriptor(fixture)
            val (metadata, admission) = withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
                metadata to assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-test", cache))
            }
            val rows = listOf(
                BootstrapWireRow(0, JsonArray(listOf(JsonPrimitive("parent"))), 1,
                    buildJsonObject { put("id", "parent"); put("label", "Parent") }),
                BootstrapWireRow(1, JsonArray(listOf(JsonPrimitive("child"))), 1,
                    buildJsonObject { put("id", "child"); put("parent_id", "parent"); put("tag", JsonNull)
                        put("amount", "9223372036854775807"); put("data", JsonNull) }),
            )
            val source = "fixture-source"
            val hash = SnapshotHashWriter(source, fixture.generation, "receipt", 1, null)
            var logical = 0L
            for (row in rows) {
                hash.add(row.tableIndex, row.key, row.version, row.payload)
                logical += utf8Size(canonicalizeJsonElement(JsonArray(listOf(JsonPrimitive("i"),
                    JsonPrimitive(row.tableIndex), row.key, JsonPrimitive(row.version), row.payload))))
            }
            val session = SnapshotSession("receipt", 1, 2, logical, "2099-01-01T00:00:00Z", hash.finish(2, logical))
            val transfer = Json.encodeToString(RetainedSnapshotSeal.serializer(), RetainedSnapshotSeal(session, source, fixture.config.serviceId))
            val operations = OversqliteClientOperationController(fixture.config,
                OversqliteClientResourceOwner(fixture.db, fixture.http), SnapshotDiagnosticsState()) {}
            val diagnosticLines = mutableListOf<String>()
            val previousLogger = sqliteNowLogger
            sqliteNowLogger = object : SqliteNowLogger {
                private fun capture(message: () -> String) { message().let { if (it.startsWith("SQLITENOW_DIAG ")) diagnosticLines += it } }
                override fun e(throwable: Throwable?, message: () -> String) = capture(message)
                override fun w(throwable: Throwable?, message: () -> String) = capture(message)
                override fun i(throwable: Throwable?, message: () -> String) = capture(message)
                override fun d(throwable: Throwable?, message: () -> String) = capture(message)
            }
            try { SqliteNowDiagnostics(true).operation("typed-http-receipt") {
            operations.runExclusiveOperation {
                TypedSnapshotStage.begin(fixture.db, fixture.config, admission, metadata, session, fixture.generation,
                    source, "a".repeat(64), transfer, operations) { stage ->
                    suspend fun deliver(index: Int, malformed: String? = null) {
                        val row = rows[index]
                        val bytes = utf8Size(canonicalizeJsonElement(JsonArray(listOf(JsonPrimitive("i"),
                            JsonPrimitive(row.tableIndex), row.key, JsonPrimitive(row.version), row.payload))))
                        var body = buildJsonObject {
                            put("rows", buildJsonArray { add(buildJsonArray { add(row.tableIndex); add(index + 1)
                                add(buildJsonArray { add(buildJsonArray { add("i"); add(row.key); add(row.version); add(row.payload) }) }) }) })
                            put("next_row_ordinal", index + 1); put("has_more", index == 0); put("logical_byte_count", bytes)
                        }.toString()
                        if (malformed == "duplicate") body = body.replace("\"id\":\"parent\"", "\"id\":\"parent\",\"id\":\"parent\"")
                        if (malformed == "unknown") body = body.dropLast(1) + ",\"unexpected\":true}"
                        if (malformed == "ordinal") body = body.replace("[[0,1,", "[[0,2,")
                        val http = HttpClient(MockEngine { respond(body, HttpStatusCode.OK, headersOf(HttpHeaders.ContentType, "application/json")) })
                        try {
                            val remote = OversqliteRemoteApi(http, Json, generation = { fixture.generation }, log = {})
                            BootstrapTransportScope.use(OversqliteWorkspace(fixture.db, fixture.config)) { transport ->
                                remote.fetchAndStageBootstrapChunk(stage, index.toLong(), 1, 65_536, 65_536, 65_536, transport)
                            }
                        } finally { http.close() }
                    }
                    for (malformed in listOf("duplicate", "unknown", "ordinal")) {
                        assertFails { deliver(0, malformed) }
                        assertEquals(0L, OversqliteOperationStateStore(fixture.db).loadState().snapshotStagedRowCount)
                        assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                        assertNull(currentCoroutineContext()[BootstrapTransportScope])
                    }
                    deliver(0)
                    assertEquals(1L, fixture.db.scalarLong("SELECT bootstrap_page_count FROM _sync_operation_state"))
                    assertEquals(3L, fixture.db.scalarLong("SELECT snapshot_stage_entries FROM _sync_operation_state"))
                    assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
                    deliver(1)
                    stage.seal()
                    assertTrue(OversqliteOperationStateStore(fixture.db).loadState().snapshotStageComplete)
                    fixture.db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                        fixture.db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                            val manifest = stage.verifyForApply(cache)
                            assertEquals(listOf(1L, 1L), manifest.tables.map { it.rows })
                            assertEquals(2L, measureBootstrapStage(fixture.db, descriptor, OversqliteOperationStateStore(fixture.db).loadState(), cache).rows)
                            assertEquals(measureBootstrapMetadataCharge(fixture.db, cache), bootstrapMetadataCharge(descriptor,
                                session.snapshotId, manifest.chain, manifest.canonical(stage.identity,
                                    OversqliteOperationStateStore(fixture.db).loadState()), manifest.tables))
                        }
                    }
                }
            }.getOrThrow()
            } } finally { sqliteNowLogger = previousLogger }
            val diagnosticRecords = diagnosticLines.map { Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject }
            val rootSpan = diagnosticRecords.single { it["kind"]?.jsonPrimitive?.content == "span_start" && it["name"]?.jsonPrimitive?.content == "typed-http-receipt" }.getValue("span")
            fun calls(name: String): Long {
                val families = diagnosticRecords.filter { it["kind"]?.jsonPrimitive?.content == "sql_family" && it["name"]?.jsonPrimitive?.content == name }.map { it.getValue("family") }.toSet()
                assertTrue(families.isNotEmpty())
                return diagnosticRecords.filter { it["kind"]?.jsonPrimitive?.content == "work_metric" && it["family"] in families && it["span"] == rootSpan }
                    .sumOf { it.getValue("counts").jsonObject.getValue("calls").jsonPrimitive.long }
            }
            assertEquals(7L, calls("row_processing.typed_scalar_decode"))
            assertEquals(2L, calls("snapshot.bootstrap.received_rows"))
            assertEquals(2L, calls("snapshot.bootstrap.received_canonicalizations"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_snapshot_stage"))
        }
    }
    private val columns = listOf(
        ColumnInfo("data", "BLOB", false, false, null, wireType = "bytes"),
        ColumnInfo("id", "TEXT", true, true, null, wireType = "text"),
        ColumnInfo("label", "TEXT", false, false, null, wireType = "text"),
        ColumnInfo("n", "INTEGER", false, true, null, wireType = "int64"),
        ColumnInfo("r", "REAL", false, false, null, wireType = "float64"),
    )
    private val table = GenerationTable("main", "items", listOf("id"), columns.map {
        GenerationColumn(it.name, checkNotNull(it.wireType), !it.notNull && !it.isPrimaryKey)
    }, emptyList())
    private val generation = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
    private val schema = BootstrapTableSchema(0, table, TableInfo("items", columns), columns,
        listOf(BootstrapUniqueKey(listOf(1 to "BINARY")), BootstrapUniqueKey(listOf(2 to "NOCASE"))),
        emptyList(), "a".repeat(64))

    @Test fun countedDescriptorAdmissionAndParameterBoundary() = runOwnedWebDatabaseTest {
        var admissions = 0
        var charged = WorkspaceCharge()
        val descriptor = SnapshotDescriptor.create(generation, listOf(schema), 10) {
            admissions++; charged = it
        }
        assertEquals(1, admissions)
        assertEquals(charged, descriptor.charge)
        assertEquals(bootstrapDescriptorCharge(descriptor.descriptorBytes, descriptor.sqlBytes, 1, 5, 0, 2), charged)
        assertEquals(descriptor.canonical.encodeToByteArray().size.toLong(), descriptor.descriptorBytes)
        val binding = descriptor.tables.single()
        assertEquals(1, binding.keyIndex)
        assertEquals("_sync_bootstrap_values_t0", binding.stageName)
        assertEquals(0, binding.tableIndex)
        assertEquals(sha256Hex(descriptor.canonical.encodeToByteArray()), descriptor.signature)
        val repeated = SnapshotDescriptor.create(generation, listOf(schema), 10) {}
        assertEquals(descriptor.signature, repeated.signature)
        assertEquals(descriptor.sqlBytes, repeated.sqlBytes)
        assertFails { SnapshotDescriptor.create(generation, listOf(schema), 9) { error("must refuse before admission") } }
        val failure = IllegalStateException("descriptor quota denied")
        assertSame(failure, assertFails { SnapshotDescriptor.create(generation, listOf(schema), 10) { throw failure } })
        assertEquals(WorkspaceCharge(16, 4096), bootstrapDescriptorCharge(0, 0, 0, 0, 0, 0))
        assertFails { bootstrapDescriptorCharge(Long.MAX_VALUE, 0, 0, 0, 0, 0) }
        assertFails { bootstrapDescriptorCharge(0, 0, Long.MAX_VALUE, 0, 0, 0) }
        assertFails { bootstrapDescriptorCharge(-1, 0, 0, 0, 0, 0) }
    }

    @Test fun descriptorBindsMappingSchemaTriggersAndIndexCollations() = runOwnedWebDatabaseTest {
        suspend fun signature(value: BootstrapTableSchema) = SnapshotDescriptor.create(generation, listOf(value), 999) {}.signature
        val original = signature(schema)
        assertNotEquals(original, signature(schema.copy(triggerSignature = "b".repeat(64))))
        val trimmed = schema.copy(uniqueKeys = listOf(schema.uniqueKeys.first(), BootstrapUniqueKey(listOf(2 to "RTRIM"))))
        assertNotEquals(original, signature(trimmed))
        assertNotEquals(original, signature(schema.copy(foreignKeys = listOf(BootstrapForeignKey(2, 0, 1, "BINARY")))))
        assertFails { signature(schema.copy(columns = columns.reversed())) }
        val custom = schema.copy(uniqueKeys = listOf(BootstrapUniqueKey(listOf(2 to "CUSTOM"))))
        assertFails { signature(custom) }
        val invalidParent = schema.copy(foreignKeys = listOf(BootstrapForeignKey(2, 1, 1, "BINARY")))
        assertFails { signature(invalidParent) }
        val secondTable = table.copy(table = "z_items")
        val two = generation.copy(tables = listOf(secondTable, table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val first = schema.copy(generationIndex = 1)
        val second = schema.copy(generationIndex = 0, generationTable = secondTable, physical = schema.physical.copy(table = "z_items"))
        val descriptor = SnapshotDescriptor.create(two, listOf(first, second), 999) {}
        assertEquals(listOf(1, 0), descriptor.tables.map { it.tableIndex })
        assertEquals(listOf(0, 1), descriptor.tables.map { it.slot })
        assertFails { SnapshotDescriptor.create(two, listOf(second, first), 999) {} }
    }

    @Test fun typedSqlPreservesStorageInt64RealBlobAndNullValues() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val binding = createStage(db)
            val rows = fixtureRows()
            rows.forEachIndexed { index, row -> insert(db, binding, index + 1L, row) }
            var observed = 0
            db.withPreparedStatement(binding.scanSql) { statement ->
                bindRange(statement)
                while (statement.step()) {
                    val row = rows[observed]
                    assertEquals(observed + 1L, statement.getLong(0))
                    assertEquals("{\"id\":\"${(row[1] as TypedValue.Text).value}\"}", statement.getText(1))
                    assertEquals(1L, statement.getLong(2))
                    val actual = SnapshotTypedValues.readProjection(
                        TableInfo("stage", listOf(
                            ColumnInfo("ordinal", "INTEGER", false, true, null),
                            ColumnInfo("key", "TEXT", false, true, null),
                            ColumnInfo("version", "INTEGER", false, true, null),
                            ColumnInfo("charge", "INTEGER", false, true, null),
                        ) + columns), statement).drop(4)
                    for (index in row.indices) {
                        val input = row[index]
                        // Section 5.1/5.2 normalizes both signs of floating zero.
                        val expected = if (input is TypedValue.Real && input.canonicalText.toDouble() == 0.0)
                            TypedValue.Real("0") else input
                        assertTrue(typedValuesEqual(expected, actual[index]),
                            "row=$observed column=${columns[index].name} expected=$expected actual=${actual[index]}")
                    }
                    observed++
                }
            }
            assertEquals(3, observed)
            db.withPreparedStatement(binding.measureSql) { statement ->
                bindRange(statement)
                assertTrue(statement.step()); assertEquals(3L, statement.getLong(0))
                val expectedBodies = rows.sumOf { bootstrapProjectionRowBytes("{\"id\":\"${(it[1] as TypedValue.Text).value}\"}", it) }
                assertEquals(expectedBodies, statement.getLong(2))
                assertEquals(rows.sumOf { rowCharge(it) }, statement.getLong(1))
                assertFalse(statement.step())
            }
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM items"))
            executeRange(db, binding.businessSql)
            assertEquals(3L, scalarLong(db, "SELECT count(*) FROM items"))
            assertEquals(Long.MIN_VALUE, scalarLong(db, "SELECT n FROM items WHERE id='one'"))
            assertEquals(Long.MAX_VALUE, scalarLong(db, "SELECT n FROM items WHERE id='two'"))
            assertEquals(9_007_199_254_740_993L, scalarLong(db, "SELECT n FROM items WHERE id='three'"))
            assertEquals(0L, verifyBusiness(db, binding))
            db.execSQL("UPDATE items SET label='NAME' WHERE id='one'")
            assertEquals(1L, verifyBusiness(db, binding), "payload proof uses BINARY equality despite NOCASE uniqueness")
        } finally { db.close() }
    }

    @Test fun activeStageUniqueNullAndStorageChecksRejectBeforeBusinessMutation() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val binding = createStage(db)
            fixtureRows().forEachIndexed { index, row -> insert(db, binding, index + 1L, row) }
            val duplicate = fixtureRows()[0].toMutableList().also {
                it[1] = TypedValue.Text("four"); it[2] = TypedValue.Text("name")
            }
            assertFails { insert(db, binding, 4, duplicate) }
            val invalid = fixtureRows()[0].toMutableList().also {
                it[1] = TypedValue.Text("four"); it[2] = TypedValue.Null; it[3] = TypedValue.Text("invalid integer")
            }
            assertFails { insert(db, binding, 4, invalid) }
            val missingKey = fixtureRows()[0].toMutableList().also { it[1] = TypedValue.Null }
            assertFails { insert(db, binding, 4, missingKey) }
            assertEquals(3L, scalarLong(db, "SELECT count(*) FROM _sync_bootstrap_values_t0"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM items"))
        } finally { db.close() }
    }

    @Test fun stageForeignKeyIndexesAndUuidStorageAreExplicit() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val descriptor = SnapshotDescriptor.create(generation,
                listOf(schema.copy(foreignKeys = listOf(BootstrapForeignKey(2, 0, 1, "BINARY")))), 999) {}
            val binding = descriptor.tables.single()
            db.execSQL(binding.createSql); binding.indexSql.forEach { db.execSQL(it) }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM pragma_index_list('_sync_bootstrap_values_t0') WHERE name='_sync_bootstrap_values_t0_foreign_0'"))
            // Stage download deliberately has indexes without cross-table FK constraints.
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_list('_sync_bootstrap_values_t0')"))
            val uuidColumn = ColumnInfo("id", "BLOB", true, true, null, wireType = "uuid")
            val uuidTable = GenerationTable("main", "uuid_items", listOf("id"), listOf(GenerationColumn("id", "uuid", false)), emptyList())
            val uuidGeneration = generation.copy(tables = listOf(uuidTable)).let { it.copy(fingerprint = it.computeFingerprint()) }
            val uuidSchema = BootstrapTableSchema(0, uuidTable, TableInfo("uuid_items", listOf(uuidColumn)), listOf(uuidColumn),
                listOf(BootstrapUniqueKey(listOf(0 to "BINARY"))), emptyList(), "a".repeat(64))
            val uuid = SnapshotDescriptor.create(uuidGeneration, listOf(uuidSchema), 999) {}.tables.single()
            db.execSQL("DROP TABLE _sync_bootstrap_values_t0")
            db.execSQL(uuid.createSql); uuid.indexSql.forEach { db.execSQL(it) }
            suspend fun add(bytes: ByteArray) = db.withPreparedStatement(uuid.insertSql) { statement ->
                statement.bindText(1, "s"); statement.bindLong(2, 1); statement.bindText(3, "{\"id\":\"00\"}")
                statement.bindLong(4, 1); statement.bindLong(5, 1); statement.bindBlob(6, bytes); statement.step()
            }
            assertFails { add(ByteArray(15)) }
            add(ByteArray(16))
            assertEquals(16L, scalarLong(db, "SELECT length(v0) FROM _sync_bootstrap_values_t0"))
        } finally { db.close() }
    }

    @Test fun remoteSchemaDoesNotNameTheLocalSqliteDatabase() = runOwnedWebDatabaseTest {
        val remote = table.copy(schema = "business")
        val selected = generation.copy(tables = listOf(remote)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val binding = SnapshotDescriptor.create(selected, listOf(schema.copy(generationTable = remote)), 999) {}.tables.single()
        assertEquals("business", binding.schema)
        assertEquals("main", binding.localSchema)
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE main.items(data BLOB,id TEXT PRIMARY KEY NOT NULL,label TEXT,n INTEGER NOT NULL,r REAL)")
            db.execSQL("CREATE TEMP TABLE items(id TEXT)")
            db.execSQL("INSERT INTO temp.items VALUES('local-sentinel')")
            db.execSQL(binding.createSql); binding.indexSql.forEach { db.execSQL(it) }
            fixtureRows().forEachIndexed { index, row -> insert(db, binding, index + 1L, row) }
            executeRange(db, binding.businessSql)
            assertEquals(3L, scalarLong(db, "SELECT count(*) FROM main.items"))
            assertEquals("local-sentinel", scalarText(db, "SELECT id FROM temp.items"))
            assertEquals(0L, verifyBusiness(db, binding))
        } finally { db.close() }
    }

    @Test fun receivedRowDecodesScalarsOnceAndKeepsWireAndBlobKeysIndependent() = runOwnedWebDatabaseTest {
        val wireColumns = listOf(
            "flag" to "boolean", "id" to "uuid", "integer" to "int64", "json" to "json",
            "money" to "decimal", "nothing" to "text", "octets" to "bytes", "real" to "float64", "text" to "text",
        )
        val physical = wireColumns.map { (name, type) -> ColumnInfo(name,
            when (type) { "boolean", "int64" -> "INTEGER"; "float64" -> "REAL"; "uuid", "bytes" -> "BLOB"; else -> "TEXT" },
            name == "id", name != "nothing", null, wireType = type) }
        val remote = GenerationTable("business", "receipt_values", listOf("id"), physical.map {
            GenerationColumn(it.name, checkNotNull(it.wireType), !it.notNull)
        }, emptyList())
        val selected = SyncGeneration("1", "f".repeat(64), listOf(remote))
        val binding = SnapshotDescriptor.create(selected, listOf(BootstrapTableSchema(0, remote,
            TableInfo(remote.table, physical), physical, listOf(BootstrapUniqueKey(listOf(1 to "BINARY"))),
            emptyList(), "a".repeat(64))), 999) {}.tables.single()
        val payload = Json.parseToJsonElement("""{"flag":true,"id":"123e4567-e89b-42d3-a456-426614174000","integer":"9223372036854775807","json":"{\"x\":\"é\"}","money":"123.45","nothing":null,"octets":"AP8=","real":"5e-324","text":"Ž🧩\n"}""").jsonObject
        val key = JsonArray(listOf(payload.getValue("id")))
        val owner = Any(); var admissions = 0; var admitted = WorkspaceCharge()
        val records = mutableListOf<String>(); val previous = sqliteNowLogger
        sqliteNowLogger = object : SqliteNowLogger {
            private fun capture(message: () -> String) { message().let { if (it.startsWith("SQLITENOW_DIAG ")) records += it } }
            override fun e(throwable: Throwable?, message: () -> String) = capture(message)
            override fun w(throwable: Throwable?, message: () -> String) = capture(message)
            override fun i(throwable: Throwable?, message: () -> String) = capture(message)
            override fun d(throwable: Throwable?, message: () -> String) = capture(message)
        }
        try {
            SqliteNowDiagnostics(true).operation("receipt-codec") {
                val row = BootstrapReceivedRow.decode(owner, binding, "snapshot", 1, Long.MAX_VALUE, Long.MAX_VALUE,
                    key, payload, SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span) {
                    admissions++; admitted = it
                }
                try {
                    assertEquals(1, admissions); assertEquals(admitted, row.hostCharge)
                    assertEquals("{\"id\":\"123e4567e89b42d3a456426614174000\"}", row.keyJson)
                    val values = row.values(owner)
                    assertEquals(TypedValue.Integer("1"), values[0])
                    assertContentEquals("123e4567e89b42d3a456426614174000".chunked(2).map { it.toInt(16).toByte() }.toByteArray(),
                        (values[1] as TypedValue.Blob).bytes)
                    assertEquals(TypedValue.Integer(Long.MAX_VALUE.toString()), values[2])
                    assertEquals(TypedValue.Text("{\"x\":\"é\"}"), values[3])
                    assertEquals(TypedValue.Text("123.45"), values[4]); assertEquals(TypedValue.Null, values[5])
                    assertContentEquals(byteArrayOf(0, -1), (values[6] as TypedValue.Blob).bytes)
                    assertEquals(Double.MIN_VALUE.toBits(), (values[7] as TypedValue.Real).canonicalText.toDouble().toBits())
                    assertEquals(TypedValue.Text("Ž🧩\n"), values[8])
                    val frame = row.frame(owner); requireSnapshotFrameLength(frame)
                    val ordinary = SnapshotHashWriter("source", selected, "snapshot", Long.MAX_VALUE, null)
                    ordinary.add(0, key, Long.MAX_VALUE, payload)
                    val saved = SnapshotHashWriter("source", selected, "snapshot", Long.MAX_VALUE, null)
                    saved.addPersistedFrame(frame, row.logicalBytes)
                    assertEquals(ordinary.finish(1, row.logicalBytes), saved.finish(1, row.logicalBytes))
                    assertEquals(64 + 6 * (utf8Size("snapshot") + utf8Size("business") + utf8Size(remote.table) + utf8Size(row.keyJson)) + frame.size, row.wireCharge.bytes)
                    assertEquals(96 + 6 * utf8Size(row.keyJson) + 2 * bootstrapProjectionRowBytes(row.keyJson, values), row.typedCharge.bytes)
                    assertFails { row.requireOwner(Any()) }
                    assertFails { row.requireOwner(owner, SnapshotDescriptor.create(selected, listOf(binding.metadata), 999) {}.tables.single()) }
                } finally { row.close() }
                assertFails { row.values(owner) }; assertFails { row.frame(owner) }
            }
        } finally { sqliteNowLogger = previous }
        val decoded = records.map { Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject }
        fun calls(name: String): Long {
            val family = decoded.single { it["kind"]?.jsonPrimitive?.content == "sql_family" && it["name"]?.jsonPrimitive?.content == name }.getValue("family")
            return decoded.single { it["kind"]?.jsonPrimitive?.content == "work_metric" && it["family"] == family }.getValue("counts").jsonObject.getValue("calls").jsonPrimitive.long
        }
        assertEquals(physical.size.toLong(), calls("row_processing.typed_scalar_decode"))
        assertEquals(1L, calls("snapshot.bootstrap.received_rows"))
        assertEquals(1L, calls("snapshot.bootstrap.received_canonicalizations"))
    }

    @Test fun receiptAdmissionAndStrictValidationPrecedeDurableEffects() = runOwnedWebDatabaseTest {
        val binding = SnapshotDescriptor.create(generation, listOf(schema), 999) {}.tables.single()
        val owner = Any()
        val payload = Json.parseToJsonElement("""{"data":null,"id":"one","label":null,"n":"9007199254740993","r":"0"}""").jsonObject
        val key = JsonArray(listOf(JsonPrimitive("one")))
        suspend fun receive(value: JsonObject = payload, keys: JsonArray = key, version: Long = 1,
            admit: suspend (WorkspaceCharge) -> Unit = {}) = BootstrapReceivedRow.decode(owner, binding,
            "snapshot", 1, version, 10, keys, value, admit = admit)
        val failure = IllegalStateException("quota denied")
        assertSame(failure, assertFails { receive(admit = { throw failure }) })
        val accepted = receive(); accepted.close()
        assertFails { receive(JsonObject(payload - "data")) }
        assertFails { receive(JsonObject(payload + ("extra" to JsonNull))) }
        assertFails { receive(keys = JsonArray(listOf(JsonPrimitive("other")))) }
        assertFails { receive(version = 0) }; assertFails { receive(version = 11) }
        for (invalid in listOf(JsonPrimitive("01"), JsonPrimitive("9223372036854775808"), JsonPrimitive(1), JsonNull))
            assertFails { receive(JsonObject(payload + ("n" to invalid))) }
        for (invalid in listOf("NaN", "Infinity", "-0", "1.0"))
            assertFails { receive(JsonObject(payload + ("r" to JsonPrimitive(invalid)))) }
    }

    @Test fun persistedWireFramesPreserveNumericEnvelopeAndRejectCorruptPrefixes() = runOwnedWebDatabaseTest {
        val binding = SnapshotDescriptor.create(generation, listOf(schema), 999) {}.tables.single()
        val payload = Json.parseToJsonElement("""{"data":"","id":"one","label":"Ž🧩\n","n":"-9223372036854775808","r":"1.5"}""").jsonObject
        val key = JsonArray(listOf(JsonPrimitive("one"))); val owner = Any()
        // Python json UTF-8 + struct.pack('>Q') + hashlib; independent of Kotlin framing.
        val frameVectors = mapOf(
            1L to (108 to "5ffbbf31f8b47eafc757fb3f8971f90d5b74a5083642643a5dfcf71f62375477"),
            9_999_999_999_999_999L to (123 to "276c348af68f30fc8df3396359d3860e5dec9ed862f43aca04ac6e2a78b19fb5"),
            Long.MAX_VALUE to (126 to "7cef4147d1ef881450560cf895ef4eb974a53de2d542c158071fd897a8d873b5"),
        )
        for (version in listOf(1L, 9_999_999_999_999_999L, Long.MAX_VALUE)) {
            val row = BootstrapReceivedRow.decode(owner, binding, "snapshot", 1, version, Long.MAX_VALUE, key, payload) {}
            try {
                assertEquals(frameVectors.getValue(version).first, row.frame(owner).size)
                assertEquals(frameVectors.getValue(version).second, sha256Hex(row.frame(owner)))
                val expectedLogical = OversqliteCompactCodec.snapshotLogicalByteCount(generation,
                    listOf(BundleRow("main", "items", mapOf("id" to "one"), "INSERT", version, payload)))
                assertEquals(expectedLogical, row.logicalBytes)
                val original = SnapshotHashWriter("source", generation, "snapshot", Long.MAX_VALUE, null)
                original.add(0, key, version, payload)
                val writer = SnapshotHashWriter("source", generation, "snapshot", Long.MAX_VALUE, null)
                writer.addPersistedFrame(row.frame(owner), row.logicalBytes)
                assertEquals(original.finish(1, expectedLogical), writer.finish(1, expectedLogical))
                val corruptions = listOf(ByteArray(7), row.frame(owner).copyOf().also { it[0] = 1 },
                    row.frame(owner).copyOf().also { it[7] = (it[7].toInt() xor 1).toByte() }, row.frame(owner).copyOf(row.frame(owner).size - 1))
                for (bad in corruptions) {
                    val invalid = SnapshotHashWriter("source", generation, "snapshot", Long.MAX_VALUE, null)
                    assertFails { invalid.addPersistedFrame(bad, row.logicalBytes) }
                    assertFails { invalid.addPersistedFrame(row.frame(owner), row.logicalBytes) }
                }
            } finally { row.close() }
        }
    }

    private suspend fun createStage(db: SafeSQLiteConnection): SnapshotTableBinding {
        db.execSQL("CREATE TABLE items(data BLOB,id TEXT PRIMARY KEY NOT NULL,label TEXT,n INTEGER NOT NULL,r REAL)")
        db.execSQL("CREATE UNIQUE INDEX items_label ON items(label COLLATE NOCASE)")
        return SnapshotDescriptor.create(generation, listOf(schema), 999) {}.tables.single().also {
            db.execSQL(it.createSql); it.indexSql.forEach { sql -> db.execSQL(sql) }
        }
    }

    private fun fixtureRows(): List<List<TypedValue>> = listOf(
        listOf(TypedValue.Blob(byteArrayOf(0, -1)), TypedValue.Text("one"), TypedValue.Text("Name"), TypedValue.Integer(Long.MIN_VALUE.toString()), TypedValue.Real("5e-324")),
        listOf(TypedValue.Blob(byteArrayOf()), TypedValue.Text("two"), TypedValue.Null, TypedValue.Integer(Long.MAX_VALUE.toString()), TypedValue.Real("-0.0")),
        listOf(TypedValue.Null, TypedValue.Text("three"), TypedValue.Null, TypedValue.Integer("9007199254740993"), TypedValue.Null),
    )

    private fun rowCharge(row: List<TypedValue>): Long {
        val key = "{\"id\":\"${(row[1] as TypedValue.Text).value}\"}"
        return 96 + 6 * utf8Size(key) + 2 * bootstrapProjectionRowBytes(key, row)
    }

    private suspend fun insert(db: SafeSQLiteConnection, binding: SnapshotTableBinding, ordinal: Long, row: List<TypedValue>) {
        val key = (row[1] as? TypedValue.Text)?.value ?: "missing"
        db.withPreparedStatement(binding.insertSql) { statement ->
            statement.bindText(1, "snapshot"); statement.bindLong(2, ordinal)
            statement.bindText(3, "{\"id\":\"$key\"}"); statement.bindLong(4, 1)
            statement.bindLong(5, if (row[1] is TypedValue.Text) rowCharge(row) else 1)
            row.forEachIndexed { index, value -> OversqliteValueCodec.bindTypedValue(statement, index + 6, columns[index], value) }
            statement.step()
        }
    }

    private fun bindRange(statement: androidx.sqlite.SQLiteStatement) {
        statement.bindText(1, "snapshot"); statement.bindLong(2, 0); statement.bindLong(3, 3)
    }
    private suspend fun executeRange(db: SafeSQLiteConnection, sql: String) =
        db.withPreparedStatement(sql) { bindRange(it); it.step() }
    private suspend fun verifyBusiness(db: SafeSQLiteConnection, binding: SnapshotTableBinding): Long =
        db.withPreparedStatement(binding.businessVerificationSql) {
            it.bindText(1, "snapshot"); it.bindText(2, "snapshot")
            assertTrue(it.step()); it.getLong(0)
        }
}
