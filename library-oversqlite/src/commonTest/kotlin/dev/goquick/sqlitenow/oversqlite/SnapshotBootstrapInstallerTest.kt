package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import kotlinx.serialization.json.*
import kotlin.test.*
import androidx.sqlite.SQLiteStatement
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.seconds

internal class SnapshotBootstrapInstallerTest : SnapshotBootstrapTestSupport() {
    private class StatementBoundaryFailure(val point: String) : RuntimeException("owned final $point failure")
    @Test fun cyclicSelfAndNullableForeignKeysUseSqliteMembershipAndUniqueness() = runOwnedWebDatabaseTest {
        val table = GenerationTable("business", "bootstrap_nodes", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("parent_id", "text", true),
            GenerationColumn("tag", "text", true)), listOf(GenerationForeignKey(listOf("parent_id"),
            "business", "bootstrap_nodes", listOf("id"), "restrict", "no_action")))
        val generation = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        for (scenario in listOf("cycle", "missing", "duplicate")) {
            val path = createTempSqliteNowTestDbPath("bootstrap-graph")
            val db = newFileBackedDb(path)
            val server = MockSyncServer().also { it.generationOverride = generation }
            val http = server.newHttpClient()
            val tables = listOf(SyncTable("bootstrap_nodes", "id"))
            val config = OversqliteConfig(1, tables, "fixture", "bootstrap")
            val client = newClient(db, http, syncTables = tables, schema = config.serviceId)
            try {
                db.execSQL("""CREATE TABLE bootstrap_nodes(id TEXT PRIMARY KEY NOT NULL,
                    parent_id TEXT REFERENCES bootstrap_nodes(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
                    tag TEXT,UNIQUE(parent_id,tag))""")
                db.execSQL("CREATE INDEX bootstrap_nodes_parent ON bootstrap_nodes(parent_id)")
                client.openAndConnect("fixture-owner").getOrThrow()
                val fixture = BootstrapFixture(db, server, http, client, config, generation)
                val empty = prepareBootstrapTransfer(fixture, empty = true)
                fun row(id: String, parent: String?, tag: String?) = BootstrapWireRow(0,
                    JsonArray(listOf(JsonPrimitive(id))), 1, buildJsonObject {
                        put("id", id); put("parent_id", parent?.let(::JsonPrimitive) ?: JsonNull)
                        put("tag", tag?.let(::JsonPrimitive) ?: JsonNull)
                    })
                val rows = listOf(row("a", if (scenario == "missing") "absent" else "b", "edge-a"),
                    row("b", "a", "edge-b"), row("n1", "a", null),
                    row("n2", "a", if (scenario == "duplicate") "edge-b" else null),
                    row("self", "self", "own"), row("root", null, null))
                val hash = SnapshotHashWriter(empty.source, generation, empty.session.snapshotId, 2, null)
                var bytes = 0L
                for (row in rows) {
                    hash.add(row.tableIndex, row.key, row.version, row.payload)
                    bytes += utf8Size(canonicalizeJsonElement(JsonArray(listOf(JsonPrimitive("i"),
                        JsonPrimitive(row.tableIndex), row.key, JsonPrimitive(row.version), row.payload))))
                }
                val session = empty.session.copy(rowCount = rows.size.toLong(), logicalByteCount = bytes,
                    snapshotHash = hash.finish(rows.size.toLong(), bytes))
                val transfer = empty.copy(session = session, rows = rows, transfer = Json.encodeToString(
                    RetainedSnapshotSeal.serializer(), RetainedSnapshotSeal(session, empty.source, config.serviceId)))
                val install: suspend () -> Unit = {
                    useBootstrapTransfer(fixture, transfer) { stage, operations ->
                        stage.receive(rows); stage.seal()
                        val state = RuntimeState(transfer.metadata, checkNotNull(OversqliteContractStore(db).readAdmission(1)),
                            "fixture-owner", transfer.source)
                        OversqliteApplyExecutor(db, OversqliteApplyStateStore(db), operations).inApplyModeTransaction(state) { cache ->
                            val context = OversqliteProvenanceContextStore(db).target(state, transfer.source)
                            SnapshotBootstrapInstaller(db, config, operations).install(stage, transfer.admission,
                                transfer.metadata, context, cache)
                            val local = OversqliteLocalStore(db, Json) { transfer.metadata }
                            for (id in listOf("a", "b", "n1", "n2", "self", "root"))
                                local.validateStoredServerProjection("business", "bootstrap_nodes", "{\"id\":\"$id\"}", cache)
                            OversqliteAttachmentStateStore(db).persistAttachedState(transfer.source, "fixture-owner",
                                unconfirmedLocalSchema, session.snapshotBundleSeq, false, "", cache)
                            stage.clearAfterInstallation(cache)
                        }
                    }
                }
                if (scenario == "cycle") {
                    install()
                    assertEquals(6L, db.scalarLong("SELECT count(*) FROM bootstrap_nodes"))
                    assertEquals(6L, db.scalarLong("SELECT count(*) FROM _sync_row_state"))
                    assertEquals(5L, db.scalarLong("SELECT count(*) FROM _sync_relationships"))
                    assertEquals(0L, db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(0L, db.scalarLong("""SELECT count(*) FROM _sync_relationships
                        WHERE parent_key COLLATE BINARY NOT IN ('{"id":"a"}','{"id":"b"}','{"id":"self"}')"""))
                    assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(db).loadState())
                } else {
                    val failure = assertFails { install() }
                    if (scenario == "missing") assertTrue(generateSequence(failure) { it.cause }
                        .any { it.message.orEmpty().contains("missing foreign-key principal") })
                    else assertTrue(generateSequence(failure) { it.cause }
                        .any { it.message.orEmpty().contains("UNIQUE constraint failed", ignoreCase = true) })
                }
                client.closeFromCoordinatedOwner(); db.close()
                val reopened = newFileBackedDb(path)
                try {
                    assertEquals("CURRENT", inspectRetainedRuntime(reopened, config))
                    assertEquals(if (scenario == "cycle") 6L else 0L, reopened.scalarLong("SELECT count(*) FROM bootstrap_nodes"))
                    if (scenario != "cycle") assertEquals(0L, reopened.scalarLong("SELECT (SELECT count(*) FROM bootstrap_nodes)+(SELECT count(*) FROM _sync_row_state)+(SELECT count(*) FROM _sync_relationships)"))
                    assertEquals(0L, reopened.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
                } finally { reopened.close() }
            } finally {
                client.closeFromCoordinatedOwner(); http.close(); db.close()
                deleteTempSqliteNowTestDbArtifacts(path)
            }
        }
    }

    @Test fun finalVerificationDiagnosticsCloseOnFailureAndCancellation() = runOwnedWebDatabaseTest {
        val previous = dev.goquick.sqlitenow.common.sqliteNowLogger
        val capture = BootstrapDiagnosticCapture()
        val diagnostics = dev.goquick.sqlitenow.core.SqliteNowDiagnostics(true)
        dev.goquick.sqlitenow.common.sqliteNowLogger = capture
        try {
            for ((fault, outcome) in listOf("prepare" to "failure", "cancel" to "cancelled")) {
                capture.clear()
                diagnostics.operation("snapshot.bootstrap.fault_fixture") { exerciseFinalFault("final", fault) }
                val records = capture.records()
                val starts = records.filter { it["kind"]?.jsonPrimitive?.content == "span_start" }
                val ends = records.filter { it["kind"]?.jsonPrimitive?.content == "span_end" }
                assertTrue(starts.isNotEmpty())
                assertEquals(starts.map { it["capture"] to it["span"] }.toSet(),
                    ends.map { it["capture"] to it["span"] }.toSet())
                assertEquals(starts.size, ends.size)
                for (name in listOf("snapshot.bootstrap.final_verification", "final_validation")) {
                    assertEquals(1, ends.count { it["name"]?.jsonPrimitive?.content == name &&
                        it["outcome"]?.jsonPrimitive?.content == outcome }, "$name/$outcome")
                    assertTrue(ends.any { it["name"]?.jsonPrimitive?.content == name &&
                        it["outcome"]?.jsonPrimitive?.content == "success" }, "retry closes $name")
                }
                assertTrue(records.all { it.getValue("lost_spans").jsonPrimitive.long == 0L &&
                    it.getValue("family_overflow_calls").jsonPrimitive.long == 0L })
            }
        } finally {
            diagnostics.setEnabled(false)
            dev.goquick.sqlitenow.common.sqliteNowLogger = previous
        }
    }

    @Test fun cancellationAtFinalSqlBoundariesReopensAndRetriesSealedStage() = runOwnedWebDatabaseTest {
        for (target in listOf("mode", "business", "baseline", "relationship", "final")) exerciseFinalFault(target, "cancel")
    }

    @Test fun finalStatementFaultsPreserveFailureAndRetryableStage() = runOwnedWebDatabaseTest {
        for (fault in listOf("prepare", "reset", "clear", "close", "deadline", "reset_close")) exerciseFinalFault("final", fault)
    }

    @Test fun independentFinalChecksRejectObservedStateMutations() = runOwnedWebDatabaseTest {
        for (target in listOf("business_check", "baseline_check", "relationship_check")) exerciseFinalFault(target, "mutation")
    }

    private suspend fun exerciseFinalFault(target: String, fault: String) {
        val path = createTempSqliteNowTestDbPath("bootstrap-final-fault")
        try { withBootstrapFixture(dbPath = path) { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            val descriptor = transfer.admission.descriptor
            val childTable = descriptor.tables.single { it.table == "bootstrap_children" }
            val selected = when (target) {
                "mode" -> null
                "business" -> childTable.businessSql
                "baseline" -> childTable.baselineSql
                "relationship" -> childTable.foreignSql.single().insert
                "business_check" -> childTable.businessVerificationSql
                "relationship_check" -> childTable.foreignSql.single().verification
                else -> childTable.authoritativeVerificationSql
            }
            var reached = false; var secondaryReached = false
            var watched: SQLiteStatement? = null
            var observed: Throwable? = null
            val primary = StatementBoundaryFailure(fault)
            val secondary = StatementBoundaryFailure("secondary close")
            coroutineScope {
                val worker = launch {
                    try { useBootstrapTransfer(fixture, transfer) { stage, operations ->
                        stage.receive(transfer.rows); stage.seal()
                        val job = currentCoroutineContext().job
                        val hooks = StatementCacheOperations(prepare = { connection, sql ->
                            val matches = if (target == "mode") "SET baseline_mode='bootstrap'" in sql else sql == selected
                            if (matches && fault == "prepare") { reached = true; throw primary }
                            if (matches && fault == "mutation") {
                                reached = true
                                when (target) {
                                    "business_check" -> connection.execSQL("UPDATE bootstrap_children SET amount=7")
                                    "baseline_check" -> {
                                        // Permit the deliberate observed-side corruption through
                                        // the unchanged ordinary baseline writer in this fixture.
                                        connection.execSQL("UPDATE _sync_apply_state SET baseline_mode='row',bootstrap_operation='',bootstrap_target_context=0,bootstrap_revision_low=0,bootstrap_revision_high=0")
                                        connection.execSQL("UPDATE _sync_row_state SET server_payload=json_set(server_payload,'$.amount.value','7') WHERE table_name='bootstrap_children'")
                                    }
                                    "relationship_check" -> connection.execSQL("DELETE FROM _sync_relationships")
                                }
                            }
                            connection.prepare(sql).also { if (matches) watched = it }
                        }, reset = { statement ->
                            statement.reset()
                            if (statement === watched && !reached && fault in setOf("reset", "reset_close", "cancel")) {
                                reached = true
                                if (fault == "cancel") job.cancel(CancellationException("owned final boundary cancellation"))
                                else throw primary
                            }
                        }, clearBindings = { statement ->
                            statement.clearBindings()
                            if (statement === watched && !reached && fault == "clear") { reached = true; throw primary }
                        }, close = { statement ->
                            statement.close()
                            if (statement === watched) {
                                if (!reached && fault in setOf("close", "deadline")) {
                                    reached = true
                                    if (fault == "deadline") awaitCancellation() else throw primary
                                }
                                if (fault == "reset_close" && !secondaryReached) { secondaryReached = true; throw secondary }
                            }
                        })
                        val executor = OversqliteApplyExecutor(fixture.db, OversqliteApplyStateStore(fixture.db), operations)
                        executor.faultInjector = SnapshotApplyFaultInjector(statementCacheOperations = hooks,
                            cleanupTimeoutMillis = if (fault == "deadline") 250 else 5_000)
                        val state = RuntimeState(transfer.metadata, checkNotNull(OversqliteContractStore(fixture.db).readAdmission(1)),
                            "fixture-owner", transfer.source)
                        executor.inApplyModeTransaction(state) { cache ->
                            val context = OversqliteProvenanceContextStore(fixture.db).target(state, transfer.source)
                            SnapshotBootstrapInstaller(fixture.db, fixture.config, operations).install(stage, transfer.admission,
                                transfer.metadata, context, cache)
                        }
                    } } catch (failure: Throwable) {
                        observed = failure
                        if (failure is CancellationException) throw failure
                    }
                }
                worker.join()
                assertTrue(reached, "$target/$fault must reach the actual final SQL boundary")
                if (fault == "cancel") assertTrue(worker.isCancelled)
            }
            val failure = assertNotNull(observed, "$target/$fault must fail")
            val causes = generateSequence(failure) { it.cause }.toList()
            val failures = mutableListOf<Throwable>()
            fun include(error: Throwable) {
                if (failures.any { it === error }) return
                failures += error
                error.cause?.let(::include)
                error.suppressedExceptions.forEach(::include)
            }
            include(failure)
            when (fault) {
                "cancel" -> assertTrue(failure is CancellationException)
                "deadline" -> assertTrue(causes.any { it is TimeoutCancellationException })
                "mutation" -> assertTrue(causes.any { it.message.orEmpty().contains(when (target) {
                    "business_check" -> "business values differ"
                    "baseline_check" -> "authoritative values differ"
                    else -> "relationship projection differs"
                }) }, failure.toString())
                else -> assertTrue(causes.any { it === primary }, failure.toString())
            }
            if (fault == "reset_close") {
                assertTrue(secondaryReached, "the real close injector must run")
                assertTrue(failures.any { it === secondary }, "the secondary failure must retain its identity through cleanup")
            }
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val reopened = newFileBackedDb(path)
            val client = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables, schema = fixture.config.serviceId)
            try {
                client.open().getOrThrow()
                assertEquals(0L, reopened.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)+(SELECT count(*) FROM _sync_row_state)+(SELECT count(*) FROM _sync_relationships)"))
                assertEquals(0L, reopened.scalarLong("SELECT baseline_revision FROM _sync_conflict_state"))
                assertEquals(1L, reopened.scalarLong("SELECT count(*) FROM _sync_apply_state WHERE apply_mode=0 AND baseline_mode='row' AND bootstrap_revision_low=0 AND bootstrap_revision_high=0 AND bootstrap_target_context=0"))
                assertEquals(2L, reopened.scalarLong("SELECT snapshot_staged_row_count FROM _sync_operation_state WHERE snapshot_stage_complete=1"))
                assertEquals(2L, reopened.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                val current = fixture.copy(db = reopened, client = client)
                val fresh = freshBootstrapTransfer(current, transfer)
                useBootstrapTransfer(current, fresh, resume = true) { stage, operations ->
                    val state = RuntimeState(fresh.metadata, checkNotNull(OversqliteContractStore(reopened).readAdmission(1)), "fixture-owner", fresh.source)
                    OversqliteApplyExecutor(reopened, OversqliteApplyStateStore(reopened), operations).inApplyModeTransaction(state) { cache ->
                        val context = OversqliteProvenanceContextStore(reopened).target(state, fresh.source)
                        SnapshotBootstrapInstaller(reopened, fixture.config, operations).install(stage, fresh.admission, fresh.metadata, context, cache)
                        OversqliteAttachmentStateStore(reopened).persistAttachedState(fresh.source, "fixture-owner", unconfirmedLocalSchema,
                            fresh.session.snapshotBundleSeq, false, "", cache)
                        stage.clearAfterInstallation(cache)
                    }
                }
                assertEquals(2L, reopened.scalarLong("SELECT count(*) FROM _sync_row_state"))
                assertEquals(1L, reopened.scalarLong("SELECT count(*) FROM _sync_relationships"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(reopened).loadState())
                assertEquals(0L, reopened.scalarLong("SELECT (SELECT count(*) FROM _sync_bootstrap_wire)+(SELECT count(*) FROM _sync_bootstrap_pages)"))
                assertEquals(0L, reopened.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                assertEquals("local-only-sentinel", scalarText(reopened, "SELECT value FROM local_memos"))
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
            } finally { client.closeFromCoordinatedOwner(); reopened.close() }
            println("BOOTSTRAP_FINAL_FAULT $target/$fault rollback_reopen_retry=true")
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun installationWorkUsesEveryUniqueComponentAndTypedBodyByte() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val descriptor = bindFixtureDescriptor(fixture)
            val n = 3L
            val rows = listOf(1L, 2L)
            val measured = BootstrapStageMeasure(n, 100, 201, 113, 101, 20, 57, WorkspaceCharge(), rows)
            val tables = descriptor.tables.size.toLong()
            val foreign = descriptor.tables.sumOf { it.metadata.foreignKeys.size.toLong() }
            val unique = descriptor.tables.sumOf { it.metadata.uniqueKeys.sumOf { key -> key.columns.size.toLong() } }
            assertTrue(unique > descriptor.tables.sumOf { it.metadata.uniqueKeys.size.toLong() }, "fixture has a composite unique key")
            val cells = descriptor.tables.sumOf { rows[it.slot] * it.columns.size }
            val edges = descriptor.tables.sumOf { rows[it.slot] * it.metadata.foreignKeys.size }
            val j = descriptor.tables.maxOf { it.metadata.foreignKeys.size.toLong() +
                maxOf(it.metadata.indexWorkComponents, it.metadata.uniqueKeys.sumOf { key -> key.columns.size.toLong() }) }
            val expected = 512 + 64 * (tables + foreign + unique) + 256 * n + 64 * cells + 32 * edges +
                2 * measured.frames + 8 * measured.bodies + 32 * n * 3 * (1 + j)
            assertEquals(expected, bootstrapInstallationWork(descriptor, measured))
            assertEquals(8L, bootstrapInstallationWork(descriptor, measured.copy(bodies = measured.bodies + 1)) - expected)
            assertEquals(Long.MAX_VALUE, bootstrapInstallationWork(descriptor, measured.copy(frames = Long.MAX_VALUE)))
        }
    }

    @Test fun bulkSqlInstallsIndependentBaselinesRelationshipsAndTargetContext() = runOwnedWebDatabaseTest {
        exerciseKernel(rollback = false)
    }

    @Test fun finalTransactionFailurePreservesSealedStageAndEmptyBusinessState() = runOwnedWebDatabaseTest {
        exerciseKernel(rollback = true)
    }

    @Test fun canonicalRtrimParentIdentityRemainsReadableByOrdinarySync() = runOwnedWebDatabaseTest {
        exerciseKernel(rollback = false, collation = "RTRIM")
    }

    private suspend fun exerciseKernel(rollback: Boolean, collation: String = "NOCASE") = withBootstrapFixture(parentCollation = collation) { fixture ->
        bindFixtureDescriptor(fixture)
        val (metadata, admission) = withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
            metadata to assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-kernel-test", cache))
        }
        val rows = listOf(
            BootstrapWireRow(1, JsonArray(listOf(JsonPrimitive("child"))), 1,
                buildJsonObject { put("id", "child"); put("parent_id", if (collation == "RTRIM") "parent " else "PARENT"); put("tag", JsonNull)
                    put("amount", "9223372036854775807"); put("data", "AAEC") }),
            BootstrapWireRow(0, JsonArray(listOf(JsonPrimitive("parent"))), 1,
                buildJsonObject { put("id", "parent"); put("label", "Parent") }),
        )
        val source = "00000000-0000-4000-8000-000000000009"
        val hash = SnapshotHashWriter(source, fixture.generation, "kernel", 1, null)
        var logical = 0L
        for (row in rows) {
            hash.add(row.tableIndex, row.key, row.version, row.payload)
            logical += utf8Size(canonicalizeJsonElement(JsonArray(listOf(JsonPrimitive("i"), JsonPrimitive(row.tableIndex),
                row.key, JsonPrimitive(row.version), row.payload))))
        }
        val session = SnapshotSession("kernel", 1, 2, logical, "2099-01-01T00:00:00Z", hash.finish(2, logical))
        val transfer = Json.encodeToString(RetainedSnapshotSeal.serializer(), RetainedSnapshotSeal(session, source, fixture.config.serviceId))
        val activeSource = fixture.db.withPreparedStatement("SELECT current_source_id FROM _sync_attachment_state") { require(it.step()); it.getText(0) }
        val state = RuntimeState(metadata, checkNotNull(OversqliteContractStore(fixture.db).readAdmission(1)), "fixture-owner", activeSource)
        val activeContext = OversqliteProvenanceContextStore(fixture.db).activeId()
        val operations = OversqliteClientOperationController(fixture.config,
            OversqliteClientResourceOwner(fixture.db, fixture.http), SnapshotDiagnosticsState()) {}
        val executor = OversqliteApplyExecutor(fixture.db, OversqliteApplyStateStore(fixture.db), operations)
        val failure = IllegalStateException("owned kernel commit fault")
        var commitFaultReached = false
        if (rollback) executor.faultInjector = SnapshotApplyFaultInjector(beforeCommit = { commitFaultReached = true; throw failure })
        operations.runExclusiveOperation {
            TypedSnapshotStage.begin(fixture.db, fixture.config, admission, metadata, session, fixture.generation,
                source, "a".repeat(64), transfer, operations) { stage ->
                stage.receive(rows); stage.seal()
                val apply: suspend () -> Unit = {
                    executor.inApplyModeTransaction(state) { cache ->
                        val target = OversqliteProvenanceContextStore(fixture.db).target(state, source)
                        assertNotEquals(activeContext, target)
                        assertEquals(setOf("bootstrap_children", "bootstrap_parents"), SnapshotBootstrapInstaller(fixture.db,
                            fixture.config, operations).install(stage, admission, metadata, target, cache))
                        OversqliteLocalStore(fixture.db, Json) { metadata }.validateStoredServerProjection(
                            "business", "bootstrap_children", "{\"id\":\"child\"}", cache)
                        if (!rollback) {
                            val local = OversqliteLocalStore(fixture.db, Json) { metadata }
                            for (invalid in listOf("{\"id\":\"ParEnt\"}", "{\"id\":\"unowned\"}", "{\"id\":\"parent\",\"extra\":1}")) {
                                fixture.db.withPreparedStatement("UPDATE _sync_relationships SET parent_key=? WHERE child_table='bootstrap_children'", cache) {
                                    it.bindText(1, invalid); it.step()
                                }
                                assertFails { local.validateStoredServerProjection("business", "bootstrap_children", "{\"id\":\"child\"}", cache) }
                            }
                            fixture.db.execSQL("UPDATE _sync_relationships SET parent_key='{\"id\":\"parent\"}' WHERE child_table='bootstrap_children'")
                            local.validateStoredServerProjection("business", "bootstrap_children", "{\"id\":\"child\"}", cache)
                        }
                        assertEquals(2L, fixture.db.scalarLong("SELECT baseline_revision FROM _sync_conflict_state"))
                        assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM _sync_row_state WHERE row_version=1 AND server_kind='live' AND server_context_id=$target AND server_revision=1 AND table_name='bootstrap_children'"))
                        assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM _sync_relationships WHERE child_table='bootstrap_children' AND parent_table='bootstrap_parents' AND owner_revision=1 AND parent_key='{" + "\"id\":\"parent\"}'"))
                        assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children WHERE amount=9223372036854775807 AND hex(data)='000102'"))
                        assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM local_memos WHERE value='local-only-sentinel'"))
                        assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM _sync_apply_state WHERE baseline_mode='row' AND bootstrap_target_context=0"))
                    }
                }
                if (rollback) {
                    val thrown = assertFails { apply() }
                    assertTrue(commitFaultReached)
                    assertTrue(generateSequence(thrown) { it.cause }.any { it.message == failure.message })
                    assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
                    assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
                    assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_row_state"))
                    assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_relationships"))
                    assertTrue(OversqliteOperationStateStore(fixture.db).loadState().snapshotStageComplete)
                    fixture.db.transaction(dev.goquick.sqlitenow.core.TransactionMode.DEFERRED) {
                        fixture.db.withStatementCache(cleanupTimeoutMillis = 5_000) { stage.verifyForApply(it) }
                    }
                } else apply()
            }
        }.getOrThrow()
        assertEquals(0L, fixture.db.scalarLong("SELECT apply_mode FROM _sync_apply_state"))
    }

    @Test fun chunkRowsAreBoundedByTheAvailableEntryQuota() = runOwnedWebDatabaseTest {
        // 900 rows fit one 1000-row chunk by row target, but the transport admission of such a
        // chunk (32 entries per JSON container, about 86k entries) does not fit 40,000 entries.
        // The installer requests smaller chunks and installs every row instead of refusing.
        val server = MockSyncServer()
        val columns = listOf(GenerationColumn("id", "text", false), GenerationColumn("name", "text", false))
        server.generationOverride = SyncGeneration("1", "", listOf(GenerationTable("main", "quota_rows", listOf("id"), columns, emptyList())))
            .let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb(); val http = server.newHttpClient()
        val tables = listOf(SyncTable("quota_rows", syncKeyColumnName = "id"))
        val client = newClient(db, http, syncTables = tables, workspaceEntries = 40_000, workUnits = 100_000_000)
        try {
            db.execSQL("CREATE TABLE quota_rows(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            client.openAndConnect("user-1").getOrThrow()
            server.addRemoteBundleForTest((0 until 900).map { row ->
                BundleRow("main", "quota_rows", mapOf("id" to "row-$row"), "INSERT", 1, buildJsonObject { put("id", "row-$row"); put("name", "name-$row") })
            })
            client.rebuild().getOrThrow()
            assertEquals(900L, db.scalarLong("SELECT count(*) FROM quota_rows"))
            assertEquals(900L, db.scalarLong("SELECT count(*) FROM _sync_row_state"))
            assertEquals("name-899", scalarText(db, "SELECT name FROM quota_rows WHERE id='row-899'"))
            assertTrue(server.snapshotChunkRequests.size > 1, "chunks shrink to the quota: ${server.snapshotChunkRequests.size} requests")
            assertEquals(0L, db.scalarLong("SELECT reserved_entries+reserved_bytes+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    @Test fun chunkSizingLeavesRoomForReceiptAndStaging() {
        // Each row costs its measured transport share plus 3 durable entries (typed row, wire frame,
        // page record); one 2-column receipt page and the shape constant take 2*(32+8)+256 entries.
        assertEquals(393, bootstrapChunkRowsForQuota(39_709, 97, 2, 1000))
        assertEquals(1000, bootstrapChunkRowsForQuota(1_000_000, 97, 2, 1000))
        assertEquals(1, bootstrapChunkRowsForQuota(300, 97, 2, 1000))
        assertEquals(1, bootstrapChunkRowsForQuota(-5, 97, 2, 1000))
        assertEquals(4, bootstrapCeilDiv(10, 3))
        assertEquals(3, bootstrapCeilDiv(9, 3))
    }

    @Test fun restoreBeyondOneChunkOfHeadroomCompletesAtDefaultQuota() = runOwnedWebDatabaseTest(timeout = 300.seconds) {
        // At the default 131,072 entries, 20,000 staged rows plus one 1000-row chunk being parsed
        // exceed the quota part-way through the restore. Later chunks are sized from fresh usage, so
        // the restore completes instead of refusing to stage a received chunk.
        val server = MockSyncServer()
        val columns = listOf(GenerationColumn("id", "text", false), GenerationColumn("name", "text", false))
        server.generationOverride = SyncGeneration("1", "", listOf(GenerationTable("main", "quota_rows", listOf("id"), columns, emptyList())))
            .let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb(); val http = server.newHttpClient()
        val tables = listOf(SyncTable("quota_rows", syncKeyColumnName = "id"))
        val client = newClient(db, http, syncTables = tables, workspaceEntries = OversqliteConfig(1, tables, "fixture", "main").maxSyncWorkspaceEntries,
            workUnits = 1_000_000_000)
        try {
            db.execSQL("CREATE TABLE quota_rows(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            client.openAndConnect("user-1").getOrThrow()
            server.addRemoteBundleForTest((0 until 20_000).map { row ->
                BundleRow("main", "quota_rows", mapOf("id" to "row-$row"), "INSERT", 1, buildJsonObject { put("id", "row-$row"); put("name", "name-$row") })
            })
            client.rebuild().getOrThrow()
            assertEquals(20_000L, db.scalarLong("SELECT count(*) FROM quota_rows"))
            assertEquals(20_000L, db.scalarLong("SELECT count(*) FROM _sync_row_state"))
            assertEquals(0L, db.scalarLong("SELECT reserved_entries+reserved_bytes+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }
}
