package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*
import kotlinx.coroutines.launch
import kotlinx.serialization.json.*
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import dev.goquick.sqlitenow.core.SqlTracePolicy
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.common.NoopSqliteNowLogger
import dev.goquick.sqlitenow.common.SqliteNowLogger

internal class SnapshotBootstrapLifecycleTest : SnapshotBootstrapTestSupport() {
    @Test fun bootstrapBaselinesSupportFrozenRejectionAndConflictResolution() = runOwnedWebDatabaseTest {
        withBootstrapFixture(remoteSchema = "main") { fixture ->
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            withAutomaticBootstrapWorkflow(fixture) { workflow, runtime ->
                assertNotNull(workflow.tryBootstrapSnapshot(runtime, SnapshotRebuildOutboxMode.CLEAN))
            }
            fixture.client.closeFromCoordinatedOwner()
            val client = newClient(fixture.db, fixture.http, resolver = DeferConflictResolver,
                syncTables = fixture.config.syncTables, schema = fixture.config.serviceId)
            try {
                client.openAndConnect("fixture-owner").getOrThrow()
                fixture.db.execSQL("UPDATE bootstrap_parents SET label='Frozen local' WHERE id='parent'")
                fixture.server.applicationRejectionCode = "application_rejected"
                fixture.server.beforePushResponse = { throw kotlinx.coroutines.CancellationException("owned rejected response stop") }
                assertFailsWith<kotlinx.coroutines.CancellationException> { client.pushPending().getOrThrow() }
                val base = scalarText(fixture.db, "SELECT base_payload FROM _sync_outbox_rows WHERE table_name='bootstrap_parents'")
                val context = fixture.db.scalarLong("SELECT base_context_id FROM _sync_outbox_rows WHERE table_name='bootstrap_parents'")
                assertEquals(1L, fixture.db.scalarLong("SELECT base_row_version FROM _sync_outbox_rows WHERE table_name='bootstrap_parents' AND base_kind='present'"))
                assertTrue(base.contains("Parent")); assertFalse(base.contains("Frozen local"))
                assertTrue(context > 0)
                fixture.db.execSQL("UPDATE bootstrap_parents SET label='Later intent' WHERE id='parent'")
                assertIs<OversqliteRemoteContractException>(client.pushPending().exceptionOrNull())
                assertEquals("rejected", scalarText(fixture.db, "SELECT state FROM _sync_outbox_bundle"))
                assertEquals(base, scalarText(fixture.db, "SELECT base_payload FROM _sync_outbox_rows WHERE table_name='bootstrap_parents'"))
                assertEquals(context, fixture.db.scalarLong("SELECT base_context_id FROM _sync_outbox_rows WHERE table_name='bootstrap_parents'"))
                assertEquals("Later intent", scalarText(fixture.db, "SELECT label FROM bootstrap_parents WHERE id='parent'"))
                assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_upload_journal"))
                fixture.server.applicationRejectionCode = null
                client.pushPending().getOrThrow()
                assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"))
                val version = fixture.db.scalarLong("SELECT row_version FROM _sync_row_state WHERE table_name='bootstrap_parents'") + 1
                fixture.server.addRemoteBundleForTest(listOf(BundleRow("main", "bootstrap_parents", mapOf("id" to "parent"), "INSERT", version,
                    buildJsonObject { put("id", "parent"); put("label", "Remote conflict") })))
                fixture.db.execSQL("UPDATE bootstrap_parents SET label='Local conflict' WHERE id='parent'")
                assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                client.resolveConflict(assertNotNull(client.pendingConflict().getOrThrow()), MergeResult.AcceptServer).getOrThrow()
                client.pullToStable().getOrThrow()
                assertEquals("Remote conflict", scalarText(fixture.db, "SELECT label FROM bootstrap_parents WHERE id='parent'"))
                assertEquals(0L, fixture.db.scalarLong("SELECT (SELECT count(*) FROM _sync_dirty_rows)+(SELECT count(*) FROM _sync_outbox_rows)+(SELECT count(*) FROM _sync_upload_journal)"))
                assertTrue(fixture.server.ackRequests.size >= 3)
                assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
                assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun bootstrapDiagnosticsHaveFixedPhasesExactStructuralCountsAndOffParity() = runOwnedWebDatabaseTest {
        data class Effects(val business: List<String>, val baselines: List<String>, val relationships: List<String>,
            val requests: List<Long>, val sqlStatements: Int, val generalPlanWrites: Int)
        suspend fun scenario(enabled: Boolean): Pair<Effects, List<JsonObject>> {
            val path = createTempSqliteNowTestDbPath("bootstrap-diagnostic-parity")
            val previous = sqliteNowLogger
            val capture = BootstrapDiagnosticCapture(printRecords = enabled)
            val diagnostics = SqliteNowDiagnostics(enabled)
            var statements = 0; var planWrites = 0
            val trace = SqlTracePolicy({ sql ->
                statements++
                if ("_sync_apply_" in sql && sql.trimStart().startsWith("INSERT", ignoreCase = true)) planWrites++
            }, maxEvents = 100_000, durationMillis = 120_000)
            sqliteNowLogger = capture
            var effects: Effects? = null
            var records = emptyList<JsonObject>()
            try { withBootstrapFixture(dbPath = path, remoteSchema = "main", sqlTrace = trace) { fixture ->
                fixture.server.addRemoteBundleForTest(bootstrapFixtureBundles(64))
                fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
                // Coordinated opening owns this cold descriptor work separately.
                diagnostics.operation("snapshot.bootstrap.cold_descriptor") { bindFixtureDescriptor(fixture) }
                capture.clear(); statements = 0; planWrites = 0
                diagnostics.operation("snapshot.bootstrap.fixture") {
                    withAutomaticBootstrapWorkflow(fixture.copy(config = fixture.config.copy(diagnostics = diagnostics))) { workflow, runtime ->
                        assertEquals(64L, assertNotNull(workflow.tryBootstrapSnapshot(runtime, SnapshotRebuildOutboxMode.CLEAN)).restore?.rowCount)
                    }
                }
                val operationStatements = statements
                records = capture.records()
                effects = Effects(
                    fixture.db.queryList("SELECT id||':'||label FROM bootstrap_parents ORDER BY id") { it.getText(0) } +
                        fixture.db.queryList("SELECT id||':'||parent_id||':'||amount||':'||hex(data) FROM bootstrap_children ORDER BY id") { it.getText(0) },
                    fixture.db.queryList("SELECT table_name||':'||key_json||':'||server_payload||':'||server_revision FROM _sync_row_state ORDER BY table_name,key_json") { it.getText(0) },
                    fixture.db.queryList("SELECT child_key||':'||parent_key||':'||owner_revision FROM _sync_relationships ORDER BY child_key") { it.getText(0) },
                    fixture.server.snapshotChunkRequests.map { it.second }, operationStatements, planWrites)
                assertEquals(0L, fixture.db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
                if (!enabled) assertTrue(records.isEmpty())
            } } finally { diagnostics.setEnabled(false); sqliteNowLogger = previous; deleteTempSqliteNowTestDbArtifacts(path) }
            return assertNotNull(effects) to records
        }
        val (off, none) = scenario(false)
        val (on, records) = scenario(true)
        assertEquals(off, on, "diagnostics preserve actual SQL, transport and effects")
        assertEquals(0, on.generalPlanWrites)
        assertTrue(none.isEmpty())
        val counts = bootstrapDiagnosticCounts(records, "snapshot.bootstrap.fixture")
        println("BOOTSTRAP_STRUCTURAL_COUNTS " + JsonObject(counts.mapValues { JsonPrimitive(it.value) }))
        assertEquals(64L, counts.getValue("received_rows")); assertEquals(64L, counts.getValue("canonicalizations"))
        assertEquals(224L, counts.getValue("scalar_decodes"))
        assertEquals(192L, counts.getValue("wire_rows")); assertEquals(128L, counts.getValue("projection_rows"))
        assertEquals(64L, counts.getValue("installation_rows")); assertEquals(64L, counts.getValue("cleanup_rows"))
        assertEquals(2L, counts.getValue("table_checks")); assertEquals(2L, counts.getValue("foreign_key_checks"))
        val pages = counts.getValue("stage_pages") + counts.getValue("install_pages")
        assertTrue(counts.getValue("quota_reads") <= 4 * pages + 12 * 2 + 4 + 32, counts.toString())
        assertTrue(counts.getValue("driver_steps") <= 8 * 64 + 20 * 2 + 8 + 20 * pages + 128, counts.toString())
        val starts = records.filter { it["kind"]?.jsonPrimitive?.content == "span_start" }
        val ends = records.filter { it["kind"]?.jsonPrimitive?.content == "span_end" }
        for (name in listOf("snapshot.bootstrap.admission", "snapshot.bootstrap.staging", "snapshot.bootstrap.projection_verification",
            "snapshot.bootstrap.foreign_keys", "snapshot.bootstrap.business_installation", "snapshot.bootstrap.baseline_installation",
            "snapshot.bootstrap.relationship_installation", "snapshot.bootstrap.final_verification", "snapshot.bootstrap.publication"))
            assertTrue(ends.any { it["name"]?.jsonPrimitive?.content == name && it["outcome"]?.jsonPrimitive?.content == "success" }, name)
        for (name in listOf("verification_last_chunk", "verification_seal", "verification_final_apply"))
            assertEquals(1, ends.count { it["name"]?.jsonPrimitive?.content == name && it["outcome"]?.jsonPrimitive?.content == "success" })
        for (start in starts.filter { it["name"]?.jsonPrimitive?.content in setOf("snapshot.bootstrap.business_installation",
            "snapshot.bootstrap.baseline_installation", "snapshot.bootstrap.relationship_installation", "snapshot.bootstrap.final_verification") }) {
            val parent = starts.single { it["span"] == start["parent"] }
            assertEquals(if (start["name"]?.jsonPrimitive?.content == "snapshot.bootstrap.final_verification") "final_validation" else "row_installation",
                parent["name"]?.jsonPrimitive?.content)
        }
        assertEquals(1, records.count { it["name"]?.jsonPrimitive?.content == "snapshot.engine" })
        assertEquals(1, records.count { it["name"]?.jsonPrimitive?.content == "snapshot.committed" && it["outcome"]?.jsonPrimitive?.content == "confirmed" })
        val peaks = records.filter { it["name"]?.jsonPrimitive?.content == "snapshot.bootstrap.workspace" }.map { it.getValue("counts").jsonObject }
        assertTrue(peaks.isNotEmpty())
        for (peak in peaks) {
            assertTrue(peak.getValue("admitted_peak_entries").jsonPrimitive.long <= peak.getValue("entry_limit").jsonPrimitive.long)
            assertTrue(peak.getValue("admitted_peak_bytes").jsonPrimitive.long <= peak.getValue("byte_limit").jsonPrimitive.long)
        }
        assertTrue(records.all { it.getValue("lost_spans").jsonPrimitive.long == 0L && it.getValue("family_overflow_calls").jsonPrimitive.long == 0L })
        for (name in listOf("snapshot.final.restoration", "snapshot.final.restoration_pass", "snapshot.unique_state_installation"))
            assertFalse(starts.any { it["name"]?.jsonPrimitive?.content == name })
    }

    @Test fun retainedGeneralApplyPlanPreventsBootstrapSelection() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            fixture.db.execSQL("""INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,
                table_cursor,fk_cursor,row_cursor,entries,bytes) VALUES(1,'retained-owner','${"a".repeat(64)}','prepared',0,0,0,0,NULL,0,0)""")
            val before = baselineQualificationVector(fixture.db, listOf("_sync_apply_plan", "_sync_operation_state", "_sync_attachment_state"))
            withAutomaticBootstrapWorkflow(fixture) { workflow, runtime ->
                assertNull(workflow.tryBootstrapSnapshot(runtime, SnapshotRebuildOutboxMode.CLEAN))
            }
            assertEquals(0, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(0L, bootstrapFamilyObjectCount(fixture.db))
            assertEquals(before, baselineQualificationVector(fixture.db, listOf("_sync_apply_plan", "_sync_operation_state", "_sync_attachment_state")))
        }
    }

    @Test fun boundedFixturesRespectSmallAndDefaultWorkTargets() = runOwnedWebDatabaseTest {
        for (count in listOf(16, 64)) for (small in listOf(false, true)) {
            val path = createTempSqliteNowTestDbPath("bootstrap-bounded-fixture")
            try { withBootstrapFixture(dbPath = path, remoteSchema = "main") { fixture ->
                fixture.server.addRemoteBundleForTest(bootstrapFixtureBundles(count))
                fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
                val config = fixture.config.copy(maxSyncWorkUnitsPerInvocation = if (small) 1 else 1_000_000,
                    snapshotApplyBatchRows = if (small) 1 else 256)
                withAutomaticBootstrapWorkflow(fixture.copy(config = config)) { workflow, runtime ->
                    assertEquals(count.toLong(), assertNotNull(workflow.tryBootstrapSnapshot(runtime, SnapshotRebuildOutboxMode.CLEAN)).restore?.rowCount)
                }
                assertEquals(count.toLong(), fixture.db.scalarLong("SELECT count(*) FROM _sync_row_state"))
                assertEquals(count.toLong(), fixture.db.scalarLong("SELECT baseline_revision FROM _sync_conflict_state"))
                assertEquals((count / 2).toLong(), fixture.db.scalarLong("SELECT count(*) FROM _sync_relationships"))
                assertEquals((count / 2).toLong(), fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children WHERE amount=9223372036854775807 AND hex(data)='000102'"))
                assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
                fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
                val reopened = newFileBackedDb(path)
                val client = newClient(reopened, fixture.http, syncTables = config.syncTables, schema = config.serviceId)
                try {
                    client.open().getOrThrow()
                    assertEquals("CURRENT", inspectRetainedRuntime(reopened, config))
                    assertEquals("local-only-sentinel", scalarText(reopened, "SELECT value FROM local_memos"))
                    assertEquals(count.toLong(), reopened.scalarLong("SELECT count(*) FROM _sync_row_state"))
                    assertEquals(0L, reopened.scalarLong("SELECT (SELECT count(*) FROM _sync_bootstrap_wire)+(SELECT count(*) FROM _sync_bootstrap_pages)"))
                    assertEquals(0L, reopened.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                } finally { client.closeFromCoordinatedOwner(); reopened.close() }
            } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun coordinatedOpeningRebindsBootstrapFamilyAfterPhysicalReopen() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("bootstrap-coordinated-rebind")
        try { withBootstrapFixture(dbPath = path, remoteSchema = "main") { fixture ->
            val previous = bindFixtureDescriptor(fixture)
            val definitions = bootstrapDefinitions(fixture.db)
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val next = fixture.generation.copy(id = "2", fingerprint = "").let { it.copy(fingerprint = it.computeFingerprint()) }
            fixture.server.generationDiscoveryOverrides["1"] = fixture.generation
            fixture.server.generationDiscoveryOverrides["2"] = next
            fixture.server.generationOverride = next
            val database = SqliteNowDatabase(path, object : DatabaseMigrations {
                override val targetVersion = 0
                override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int) = 0
            }, syncManaged = true)
            val config = fixture.config.copy(generationId = 2)
            val opening = database.openForSync(config, fixture.http,
                upgradePlan = OversqliteUpgradePlan("bootstrap-shared-rebind", 1, 2, 0, 0, setOf("local_memos")))
            try {
                assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
                val db = database.connection()
                assertEquals(definitions, bootstrapDefinitions(db))
                assertTrue(previous.signature != scalarText(db, "SELECT descriptor_signature FROM _sync_bootstrap_state"))
                assertEquals("2", scalarText(db, "SELECT json_extract(descriptor_json,'$[1]') FROM _sync_bootstrap_state"))
                assertEquals("local-only-sentinel", scalarText(db, "SELECT value FROM local_memos"))
                assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_bootstrap_pages"))
                assertEquals(0L, db.scalarLong("SELECT (SELECT count(*) FROM _sync_bootstrap_values_t0)+(SELECT count(*) FROM _sync_bootstrap_values_t1)"))
                assertEquals(0L, db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                assertEquals("CURRENT", inspectRetainedRuntime(db, config))
                assertNotNull(opening.readyOrNull()).client.attach("fixture-owner").getOrThrow()
                assertEquals(definitions, bootstrapDefinitions(db))
            } finally { opening.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @OptIn(dev.goquick.sqlitenow.core.SqliteNowInternalApi::class)
    @Test fun coordinatedMigrationProtectsBootstrapFamilyOnSharedRuntimes() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("bootstrap-coordinated-protection")
        try { withBootstrapFixture(dbPath = path, remoteSchema = "main") { fixture ->
            val previous = bindFixtureDescriptor(fixture)
            val definitions = bootstrapDefinitions(fixture.db)
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val database = SqliteNowDatabase(path, object : DatabaseMigrations {
                override val targetVersion = 1
                override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                    conn.execSQL("DROP TABLE _sync_bootstrap_values_t0")
                    return 1
                }
            }, syncManaged = true)
            val opening = database.openForSync(fixture.config, fixture.http,
                upgradePlan = OversqliteUpgradePlan("bootstrap-shared-protection", 1, 1, 0, 1, setOf("local_memos")))
            try {
                val failure = assertFails { opening.advance() }
                assertTrue(generateSequence(failure) { it.cause }.any {
                    it is dev.goquick.sqlitenow.core.SqliteNowCoordinatedMigrationException &&
                        it.diagnosticCode == "coordinated_migration_protected_object_forbidden"
                }, failure.toString())
            } finally { opening.close() }
            val reopened = newFileBackedDb(path)
            try {
                assertEquals(0L, reopened.scalarLong("PRAGMA user_version"))
                assertEquals(1L, reopened.scalarLong("SELECT generation_id FROM _sync_contract_identity"))
                assertEquals(previous.signature, scalarText(reopened, "SELECT descriptor_signature FROM _sync_bootstrap_state"))
                assertEquals(definitions, bootstrapDefinitions(reopened))
                assertEquals("local-only-sentinel", scalarText(reopened, "SELECT value FROM local_memos"))
                assertEquals(0L, reopened.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
            } finally { reopened.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    private suspend fun bootstrapDefinitions(db: SafeSQLiteConnection): List<String> =
        db.queryList("SELECT name||':'||type||':'||COALESCE(sql,'') FROM sqlite_schema WHERE $bootstrapFamilyPredicate ORDER BY name") { it.getText(0) }

    @Test fun initialAttachSelectsBootstrapOrGeneralFromSchemaCapability() = runOwnedWebDatabaseTest {
        for (capability in listOf("indexed", "missing_index", "extra_trigger", "failing_logger")) withBootstrapWarnings(fail = capability == "failing_logger") { warnings ->
            withBootstrapFixture(remoteSchema = "main") { fixture ->
                val indexed = capability == "indexed"
                assertFalse(fixture.config.verboseLogs)
                assertTrue(fixture.config.diagnostics?.isEnabled != true)
                fixture.client.detach().getOrThrow()
                if (capability == "extra_trigger") fixture.db.execSQL("CREATE TRIGGER extra_parent AFTER INSERT ON bootstrap_parents BEGIN SELECT 1; END")
                else if (!indexed) fixture.db.execSQL("DROP INDEX bootstrap_child_parent")
                assertEquals(0L, bootstrapFamilyObjectCount(fixture.db))
                fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                    fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
                val attached = fixture.client.attach("fixture-owner")
                val blocked = capability == "extra_trigger"
                if (blocked) assertIs<FinalStateApplyBlockedException>(attached.exceptionOrNull())
                else attached.getOrThrow()
                assertEquals(indexed, bootstrapFamilyObjectCount(fixture.db) > 0L)
                assertEquals(if (blocked) 0L else 1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
                assertEquals(if (blocked) 0L else 1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
                assertEquals(if (blocked) 0L else 2L, fixture.db.scalarLong("SELECT count(*) FROM _sync_row_state"))
                assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
                val operation = OversqliteOperationStateStore(fixture.db).loadState()
                if (blocked) assertEquals(generalSnapshotEngine, operation.snapshotEngine)
                else assertEquals(OversqliteOperationState(), operation)
                assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
                assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
                val expected = when (capability) {
                    "indexed" -> emptyList()
                    "extra_trigger" -> listOf("Oversqlite snapshot: typed bootstrap unavailable (additional_trigger); using the general snapshot installer.")
                    else -> listOf("Oversqlite snapshot: typed bootstrap unavailable (missing_child_index); using the general snapshot installer." +
                        " Required leading child index: table \"bootstrap_children\", column \"parent_id\", collation NOCASE." +
                        " Add a matching index to the application schema.")
                }
                assertEquals(expected, warnings)
            }
        }
    }

    private suspend fun <T> withBootstrapWarnings(fail: Boolean = false, block: suspend (List<String>) -> T): T {
        val previous = sqliteNowLogger
        val warnings = mutableListOf<String>()
        sqliteNowLogger = object : SqliteNowLogger by NoopSqliteNowLogger {
            override fun w(throwable: Throwable?, message: () -> String) {
                warnings += message()
                if (fail) error("fixture warning sink failure")
            }
        }
        return try { block(warnings) } finally { sqliteNowLogger = previous }
    }

    @Test fun rebuildRetainsPinnedGeneralSnapshot() = runOwnedWebDatabaseTest {
        withBootstrapFixture(remoteSchema = "main") { fixture ->
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            val store = OversqliteOperationStateStore(fixture.db)
            store.persistState(store.loadState().copy(reason = "explicit_rebuild"))
            withAutomaticBootstrapWorkflow(fixture) { workflow, runtime ->
                workflow.ensureOperationSnapshotStaged(runtime, SnapshotRebuildOutboxMode.CLEAN)
                assertEquals(generalSnapshotEngine, store.loadState().snapshotEngine)
                assertEquals(2L, workflow.rebuildFromSnapshot(runtime, null, null).restore?.rowCount)
            }
            assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(0L, bootstrapFamilyObjectCount(fixture.db))
            assertEquals(2L, fixture.db.scalarLong("SELECT count(*) FROM _sync_row_state"))
            assertEquals(OversqliteOperationState(), store.loadState())
            assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
        }
    }

    @Test fun automaticBootstrapPublishesThenSupportsOrdinaryPullPushAndRebuild() = runOwnedWebDatabaseTest {
        withBootstrapFixture(remoteSchema = "main") { fixture ->
            assertEquals(0L, bootstrapFamilyObjectCount(fixture.db))
            fun bundle(row: BootstrapWireRow, version: Long = row.version) = BundleRow(fixture.generation.tables[row.tableIndex].schema,
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", version, row.payload)
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { bundle(it) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            val result = withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                workflow.rebuildFromSnapshot(state, null, null)
            }
            assertTrue(bootstrapFamilyObjectCount(fixture.db) > 0L)
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, result.outcome)
            assertEquals(2L, result.restore?.rowCount)
            assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
            assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
            assertEquals(2L, fixture.db.scalarLong("SELECT count(*) FROM _sync_row_state"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_apply_rows"))
            assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            val changed = bootstrapFixtureRows()[1].copy(payload = buildJsonObject {
                put("id", "child"); put("parent_id", "parent"); put("tag", JsonNull); put("amount", "12"); put("data", JsonNull)
            })
            fixture.server.addRemoteBundleForTest(listOf(bundle(changed, 2)))
            fixture.client.pullToStable().getOrThrow()
            assertEquals(12L, fixture.db.scalarLong("SELECT amount FROM bootstrap_children"))
            fixture.server.addRemoteBundleForTest(listOf(BundleRow("main", "bootstrap_children", mapOf("id" to changed.key.single().jsonPrimitive.content), "DELETE", 3, null)))
            fixture.client.pullToStable().getOrThrow()
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
            fixture.db.execSQL("UPDATE bootstrap_parents SET label='Local edit' WHERE id='parent'")
            fixture.client.pushPending().getOrThrow()
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
            assertTrue(fixture.server.ackRequests.isNotEmpty())
            assertTrue(fixture.server.remoteRowsForBaselineTest().values.any { "Local edit" in it })
            val rowid = fixture.db.scalarLong("SELECT rowid FROM bootstrap_parents WHERE id='parent'")
            fixture.client.rebuild().getOrThrow()
            assertEquals(rowid, fixture.db.scalarLong("SELECT rowid FROM bootstrap_parents WHERE id='parent'"))
            assertEquals("Local edit", scalarText(fixture.db, "SELECT label FROM bootstrap_parents"))
            assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
        }
    }

    @Test fun automaticBootstrapRechecksNetworkChangesWithoutFallback() = runOwnedWebDatabaseTest {
        for (change in listOf("write", "schema", "epoch", "account", "source")) withBootstrapFixture(remoteSchema = "main") { fixture ->
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow(fixture.generation.tables[row.tableIndex].schema,
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            var changed = false
            fixture.server.beforeSnapshotChunkResponse = {
                changed = true
                when (change) {
                    "write" -> fixture.db.execSQL("INSERT INTO bootstrap_parents VALUES('local','Concurrent local')")
                    "schema" -> fixture.db.execSQL("CREATE INDEX concurrent_label ON bootstrap_parents(label)")
                    "epoch" -> fixture.db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1")
                    "account" -> fixture.db.execSQL("UPDATE _sync_attachment_state SET attached_user_id='another-owner'")
                    "source" -> {
                        val other = "00000000-0000-4000-8000-000000000099"
                        OversqliteSourceStateStore(fixture.db).ensureSource(other)
                        fixture.db.execSQL("UPDATE _sync_attachment_state SET current_source_id='$other'")
                    }
                }
            }
            assertFails { withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN)
            } }
            assertTrue(changed, "actual HTTP response mutation must execute")
            assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(typedSnapshotEngine, OversqliteOperationStateStore(fixture.db).loadState().snapshotEngine)
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_snapshot_stage"))
            assertEquals(if (change == "write") 1L else 0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
            if (change == "write") assertEquals("Concurrent local", scalarText(fixture.db, "SELECT label FROM bootstrap_parents"))
            assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
        }
    }

    @Test fun typedOneRowCapacityProgressRepeatsLifecycleAdmission() = runOwnedWebDatabaseTest {
        for (invalidate in listOf(false, true)) withBootstrapFixture(remoteSchema = "main", chunkBytes = 1024) { fixture ->
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            fixture.server.snapshotChunkTooSmallOnceForTest = true
            if (invalidate) fixture.server.beforeSnapshotChunkRequest = {
                fixture.db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1")
            }
            if (invalidate) {
                assertFails { withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                    workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN)
                } }
                assertEquals(1, fixture.server.snapshotChunkRequests.size)
                assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                assertEquals(typedSnapshotEngine, OversqliteOperationStateStore(fixture.db).loadState().snapshotEngine)
                assertEquals(0L, fixture.db.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
            } else {
                withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                    assertNotNull(workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN))
                }
                assertEquals(listOf(0L, 0L, 1L), fixture.server.snapshotChunkRequests.map { it.second })
                assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
                assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
                assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            }
            assertFalse(fixture.server.snapshotChunkTooSmallOnceForTest)
            assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
        }
    }

    @Test fun automaticEmptySnapshotCompletesTransportAndPublication() = runOwnedWebDatabaseTest {
        withBootstrapFixture(remoteSchema = "main") { fixture ->
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                val result = assertNotNull(workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN))
                assertEquals(0L, result.restore?.rowCount)
            }
            assertEquals(listOf(0L), fixture.server.snapshotChunkRequests.map { it.second })
            assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
            assertEquals(0L, fixture.db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
        }
    }

    @Test fun initialCreateLifecycleChangesRejectBeforeTypedPublication() = runOwnedWebDatabaseTest { kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) {
        for (change in listOf("epoch", "account", "source")) withBootstrapFixture(remoteSchema = "main") { fixture ->
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            var reached = false
            fixture.server.beforeSnapshotCreateResponse = {
                reached = true
                when (change) {
                    "epoch" -> fixture.db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1")
                    "account" -> fixture.db.execSQL("UPDATE _sync_attachment_state SET attached_user_id='another-owner'")
                    "source" -> {
                        val other = "00000000-0000-4000-8000-000000000099"
                        OversqliteSourceStateStore(fixture.db).ensureSource(other)
                        fixture.db.execSQL("UPDATE _sync_attachment_state SET current_source_id='$other'")
                    }
                }
                if (change != "epoch") bindLocalBaselineContext(fixture.db)
            }
            val failure = assertFails { withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN)
            } }
            assertTrue(reached)
            assertTrue(fixture.server.snapshotSessionIdsForTest().isEmpty(), "$change: ${fixture.server.requestTrace}; ${failure.message}; suppressed=${failure.suppressedExceptions.map { it.message }}")
            assertTrue(fixture.server.snapshotChunkRequests.isEmpty())
            assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_pages"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_state WHERE phase!='inactive' OR snapshot_id!=''"))
            assertEquals(0L, fixture.db.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
            assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
        }
    } }

    @Test fun automaticBootstrapRefusesNonemptyStateBeforeTransfer() = runOwnedWebDatabaseTest {
        withBootstrapWarnings { warnings ->
            withBootstrapFixture { fixture ->
                fixture.db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                try { fixture.db.execSQL("INSERT INTO bootstrap_parents VALUES('existing','Existing')") }
                finally { fixture.db.execSQL("UPDATE _sync_apply_state SET apply_mode=0") }
                fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
                val before = state(fixture)
                withAutomaticBootstrapWorkflow(fixture) { workflow, runtime ->
                    assertNull(workflow.tryBootstrapSnapshot(runtime, SnapshotRebuildOutboxMode.CLEAN))
                }
                assertEquals(0, fixture.server.snapshotSessionCreateRequestCount)
                assertEquals(before, state(fixture))
                assertEquals(0L, bootstrapFamilyObjectCount(fixture.db))
            }
            assertTrue(warnings.isEmpty())
        }
    }

    @Test fun automaticPartialAndSealedTransfersResumeAfterPhysicalReopen() = runOwnedWebDatabaseTest {
        for (complete in listOf(false, true)) {
            val path = createTempSqliteNowTestDbPath("bootstrap-automatic-resume")
            try { withBootstrapFixture(dbPath = path, remoteSchema = "main", chunkRows = 1) { fixture ->
                fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                    fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
                fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
                if (!complete) fixture.server.beforeSnapshotChunkResponse = {
                    fixture.server.beforeSnapshotChunkRequest = { throw kotlinx.coroutines.CancellationException("owned partial receipt stop") }
                }
                assertFailsWith<kotlinx.coroutines.CancellationException> {
                    withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                        workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN, onPhaseChanged = {
                            if (complete && it == OversqlitePhase.APPLYING_REMOTE_STATE)
                                throw kotlinx.coroutines.CancellationException("owned sealed stop")
                        })
                    }
                }
                val prefix = OversqliteOperationStateStore(fixture.db).loadState()
                assertEquals(typedSnapshotEngine, prefix.snapshotEngine)
                assertEquals(if (complete) 2L else 1L, prefix.snapshotStagedRowCount)
                assertEquals(complete, prefix.snapshotStageComplete)
                assertEquals(0L, fixture.db.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
                val beforeRequests = fixture.server.snapshotChunkRequests.size
                if (complete) fixture.server.expireSnapshotSessionForTest(prefix.stagedSnapshotId)
                fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
                val reopened = newFileBackedDb(path)
                val client = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables, schema = fixture.config.serviceId)
                try {
                    client.open().getOrThrow()
                    assertEquals(prefix, OversqliteOperationStateStore(reopened).loadState())
                    withAutomaticBootstrapWorkflow(fixture.copy(db = reopened, client = client)) { workflow, state ->
                        assertNotNull(workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN))
                    }
                    assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
                    assertEquals(beforeRequests + if (complete) 0 else 1, fixture.server.snapshotChunkRequests.size)
                    assertEquals(1L, reopened.scalarLong("SELECT count(*) FROM bootstrap_parents"))
                    assertEquals(1L, reopened.scalarLong("SELECT count(*) FROM bootstrap_children"))
                    assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(reopened).loadState())
                    assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                    assertEquals(0L, reopened.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                } finally { client.closeFromCoordinatedOwner(); reopened.close() }
            } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun expiredOrStalePartialTransferRetiresBeforeNextAutomaticAttempt() = runOwnedWebDatabaseTest {
        for (cause in listOf("expired", "epoch", "source")) withBootstrapFixture(remoteSchema = "main", chunkRows = 1) { fixture ->
            fixture.server.addRemoteBundleForTest(bootstrapFixtureRows().map { row -> BundleRow("main",
                fixture.generation.tables[row.tableIndex].table, mapOf("id" to row.key.single().jsonPrimitive.content), "INSERT", row.version, row.payload) })
            fixture.db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            fixture.server.beforeSnapshotChunkResponse = {
                fixture.server.beforeSnapshotChunkRequest = { throw kotlinx.coroutines.CancellationException("owned partial stop") }
            }
            assertFailsWith<kotlinx.coroutines.CancellationException> {
                withAutomaticBootstrapWorkflow(fixture) { workflow, state -> workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN) }
            }
            val prefix = OversqliteOperationStateStore(fixture.db).loadState()
            assertEquals(1L, prefix.snapshotStagedRowCount)
            if (cause == "expired") fixture.server.expireSnapshotSessionForTest(prefix.stagedSnapshotId)
            else if (cause == "epoch") fixture.db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1")
            else {
                val other = "00000000-0000-4000-8000-000000000099"
                OversqliteSourceStateStore(fixture.db).ensureSource(other)
                fixture.db.execSQL("UPDATE _sync_attachment_state SET current_source_id='$other'")
                bindLocalBaselineContext(fixture.db)
            }
            val failure = assertFails {
                withAutomaticBootstrapWorkflow(fixture) { workflow, state -> workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN) }
            }
            if (cause == "expired") assertIs<SnapshotHttpException>(failure) else assertIs<RebuildRequiredException>(failure)
            assertEquals(1, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(fixture.db).loadState())
            assertEquals(0L, fixture.db.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            withAutomaticBootstrapWorkflow(fixture) { workflow, state ->
                assertNotNull(workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN))
            }
            assertEquals(2, fixture.server.snapshotSessionCreateRequestCount)
            assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_parents"))
            assertEquals(1L, fixture.db.scalarLong("SELECT count(*) FROM bootstrap_children"))
            assertEquals(0L, fixture.db.scalarLong("SELECT count(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, fixture.db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
        }
    }

    @Test fun finalProofAndCleanupRejectAnotherConnectionOrUnverifiedWriter() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            useBootstrapTransfer(fixture, transfer) { stage, _ ->
                stage.receive(transfer.rows); stage.seal()
                val other = newDb()
                try {
                    fixture.db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                        other.withStatementCache { cache ->
                            val failure = assertFails { stage.verifyForApply(cache) }
                            assertTrue(failure.message.orEmpty().contains("different or expired connection"))
                        }
                        fixture.db.withStatementCache { cache ->
                            assertFails { stage.clearAfterInstallation(cache) }
                            val operation = OversqliteOperationStateStore(fixture.db).loadState()
                            val audit = BootstrapStageAudit.read(fixture.db, transfer.admission.descriptor, operation, cache)
                            assertFails { audit.requireCurrent(fixture.db, cache, transfer.admission.descriptor,
                                operation.copy(snapshotRowCount = 99)) }
                            fixture.db.withStatementCache { another ->
                                assertFails { audit.requireCurrent(fixture.db, another, transfer.admission.descriptor, operation) }
                            }
                        }
                    }
                } finally { other.close() }
                assertEquals(2L, fixture.db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                assertEquals(0L, fixture.db.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
            }
        }
    }

    @Test fun retirementAfterCommittedPageReopensWithoutAdoption() = runOwnedWebDatabaseTest {
        for (initiallySealed in listOf(false, true)) {
            val path = createTempSqliteNowTestDbPath("bootstrap-retirement-reopen")
            try { withBootstrapFixture(dbPath = path) { fixture ->
                val transfer = prepareBootstrapTransfer(fixture)
                useBootstrapTransfer(fixture, transfer) { stage, _ ->
                    stage.receive(transfer.rows.take(1)); stage.receive(transfer.rows.drop(1))
                    if (initiallySealed) stage.seal()
                }
                val committed = kotlinx.coroutines.CompletableDeferred<Unit>()
                var expired: BootstrapSnapshotRetirement? = null
                kotlinx.coroutines.coroutineScope {
                    val worker = launch {
                        val operations = OversqliteClientOperationController(fixture.config,
                            OversqliteClientResourceOwner(fixture.db, fixture.http), SnapshotDiagnosticsState()) {}
                        operations.runExclusiveOperation {
                            BootstrapSnapshotRetirement.use(fixture.db, fixture.config, transfer.admission.descriptor,
                                fixture.generation, operations) { retirement ->
                                expired = retirement
                                assertFalse(retirement.step())
                                committed.complete(Unit)
                                kotlinx.coroutines.awaitCancellation()
                            }
                        }.getOrThrow()
                    }
                    committed.await(); worker.cancel(); worker.join()
                }
                assertFails { checkNotNull(expired).step() }
                val prefix = OversqliteOperationStateStore(fixture.db).loadState()
                assertEquals(1L, prefix.bootstrapCleanupOrdinal)
                assertEquals(1L, prefix.bootstrapPageCount)
                assertEquals(1L, prefix.snapshotStagedRowCount)
                assertFalse(prefix.snapshotStageComplete)
                assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
                assertFails { useBootstrapTransfer(fixture, transfer, resume = true) { _, _ -> fail("retiring stage adopted") } }
                fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
                val reopened = newFileBackedDb(path)
                val client = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables, schema = fixture.config.serviceId)
                try {
                    client.open().getOrThrow()
                    assertEquals(prefix, OversqliteOperationStateStore(reopened).loadState())
                    assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                    val rollback = IllegalStateException("owned retirement mutation")
                    assertFails {
                        reopened.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                            reopened.execSQL("UPDATE _sync_operation_state SET bootstrap_cleanup_chain='${"c".repeat(64)}'")
                            assertFails { inspectRetainedRuntime(reopened, fixture.config) }
                            throw rollback
                        }
                    }
                    assertEquals(prefix, OversqliteOperationStateStore(reopened).loadState())
                    val operations = OversqliteClientOperationController(fixture.config,
                        OversqliteClientResourceOwner(reopened, fixture.http), SnapshotDiagnosticsState()) {}
                    operations.runExclusiveOperation {
                        BootstrapSnapshotRetirement.use(reopened, fixture.config, transfer.admission.descriptor,
                            fixture.generation, operations) { assertTrue(it.step()) }
                    }.getOrThrow()
                    assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(reopened).loadState())
                    assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                    assertEquals(0L, reopened.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                    assertEquals(0L, reopened.scalarLong("SELECT count(*) FROM _sync_bootstrap_pages"))
                    assertEquals(0L, reopened.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                    assertEquals(0L, reopened.scalarLong("SELECT (SELECT count(*) FROM bootstrap_parents)+(SELECT count(*) FROM bootstrap_children)"))
                    assertEquals("local-only-sentinel", scalarText(reopened, "SELECT value FROM local_memos"))
                } finally { client.closeFromCoordinatedOwner(); reopened.close() }
            } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun zeroAndPartialRetirementReleaseEverySnapshotOwner() = runOwnedWebDatabaseTest {
        for (rows in listOf(0, 1)) withBootstrapFixture { fixture ->
            val pending = if (rows == 0) OversqliteOperationState() else OversqliteOperationState(
                kind = operationKindRemoteReplace, targetUserId = "fixture-owner")
            OversqliteOperationStateStore(fixture.db).persistState(pending)
            val transfer = prepareBootstrapTransfer(fixture, empty = rows == 0)
            useBootstrapTransfer(fixture, transfer) { stage, _ -> stage.receive(transfer.rows.take(rows)) }
            val operations = OversqliteClientOperationController(fixture.config,
                OversqliteClientResourceOwner(fixture.db, fixture.http), SnapshotDiagnosticsState()) {}
            operations.runExclusiveOperation {
                BootstrapSnapshotRetirement.use(fixture.db, fixture.config, transfer.admission.descriptor,
                    fixture.generation, operations) { assertTrue(it.step()) }
            }.getOrThrow()
            assertEquals(pending, OversqliteOperationStateStore(fixture.db).loadState())
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            assertEquals(0L, fixture.db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
            assertEquals(transfer.admission.descriptor.tables.sumOf { 1L + it.indexSql.size }, bootstrapFamilyObjectCount(fixture.db))
        }
    }

    @Test fun partialReceiptReopensFromVerifiedPageChain() = runOwnedWebDatabaseTest {
        for (initiallySealed in listOf(false, true)) {
        val path = createTempSqliteNowTestDbPath("bootstrap-prefix-reopen")
        try { withBootstrapFixture(dbPath = path) { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            useBootstrapTransfer(fixture, transfer) { stage, _ ->
                stage.receive(if (initiallySealed) transfer.rows else transfer.rows.take(1))
                if (initiallySealed) stage.seal()
            }
            val prefix = OversqliteOperationStateStore(fixture.db).loadState()
            assertEquals(if (initiallySealed) 2L else 1L, prefix.snapshotStagedRowCount)
            assertEquals(initiallySealed, prefix.snapshotStageComplete)
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val reopened = newFileBackedDb(path)
            val client = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables, schema = fixture.config.serviceId)
            try {
                client.open().getOrThrow()
                assertEquals(prefix, OversqliteOperationStateStore(reopened).loadState())
                val nextFixture = fixture.copy(db = reopened, client = client)
                val resumed = freshBootstrapTransfer(nextFixture, transfer)
                useBootstrapTransfer(nextFixture, resumed, resume = true) { stage, _ ->
                    if (!initiallySealed) { stage.receive(resumed.rows.drop(1)); stage.seal() }
                    else assertFails { stage.receive(emptyList()) }
                }
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                useBootstrapTransfer(nextFixture, resumed, resume = true) { stage, operations ->
                    assertFails { stage.receive(emptyList()) }
                    val state = RuntimeState(resumed.metadata, checkNotNull(OversqliteContractStore(reopened).readAdmission(1)), "fixture-owner", resumed.source)
                    OversqliteApplyExecutor(reopened, OversqliteApplyStateStore(reopened), operations).inApplyModeTransaction(state) { cache ->
                        val target = OversqliteProvenanceContextStore(reopened).target(state, resumed.source)
                        SnapshotBootstrapInstaller(reopened, fixture.config, operations).install(stage, resumed.admission, resumed.metadata, target, cache)
                        OversqliteAttachmentStateStore(reopened).persistAttachedState(resumed.source, "fixture-owner", unconfirmedLocalSchema,
                            resumed.session.snapshotBundleSeq, false, "", cache)
                        stage.clearAfterInstallation(cache)
                    }
                }
                assertEquals(1L, reopened.scalarLong("SELECT count(*) FROM bootstrap_parents"))
                assertEquals(1L, reopened.scalarLong("SELECT count(*) FROM bootstrap_children"))
                assertEquals(2L, reopened.scalarLong("SELECT count(*) FROM _sync_row_state"))
                assertEquals(0L, reopened.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(reopened).loadState())
            } finally { client.closeFromCoordinatedOwner(); reopened.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun sealedWireAndTypedMutationsFailBeforeAdoption() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val transfer = prepareBootstrapTransfer(fixture)
            useBootstrapTransfer(fixture, transfer) { stage, _ -> stage.receive(transfer.rows); stage.seal() }
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            val slot = transfer.admission.descriptor.tables.single { it.tableIndex == 1 }
            val mutations = listOf(
                "UPDATE _sync_bootstrap_wire SET frame=CAST(substr(frame,1,length(frame)-1)||x'00' AS BLOB) WHERE row_ordinal=1",
                "UPDATE ${quoteIdent(slot.stageName)} SET v0=9 WHERE row_ordinal=2",
                "UPDATE ${quoteIdent(slot.stageName)} SET key_json='{\"id\":\"other\"}' WHERE row_ordinal=2",
                "UPDATE ${quoteIdent(slot.stageName)} SET row_version=2 WHERE row_ordinal=2",
                "UPDATE _sync_bootstrap_wire SET table_index=1 WHERE row_ordinal=1",
                "UPDATE _sync_bootstrap_pages SET wire_hash='${"c".repeat(64)}' WHERE page_ordinal=1",
                "UPDATE _sync_bootstrap_state SET expected_manifest=json_set(expected_manifest,'\$[8]','${"d".repeat(64)}')",
                "UPDATE _sync_operation_state SET bootstrap_reference='${"e".repeat(64)}'",
                "UPDATE ${quoteIdent(slot.stageName)} SET charge=charge+0.5 WHERE row_ordinal=2",
                "UPDATE _sync_operation_state SET snapshot_stage_entries=snapshot_stage_entries+0.5",
            )
            val tables = listOf("_sync_bootstrap_state", "_sync_bootstrap_tables", "_sync_bootstrap_wire", "_sync_bootstrap_pages",
                "_sync_operation_state", "bootstrap_parents", "bootstrap_children", "_sync_row_state", "_sync_relationships", "local_memos") +
                transfer.admission.descriptor.tables.map { it.stageName }
            val before = baselineQualificationVector(fixture.db, tables)
            for (sql in mutations) {
                val rollback = IllegalStateException("owned mutation rollback")
                val thrown = assertFails {
                    fixture.db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                        fixture.db.execSQL("PRAGMA ignore_check_constraints=ON")
                        try {
                            fixture.db.execSQL(sql)
                            assertFails { inspectRetainedRuntime(fixture.db, fixture.config) }
                            var adopted = false
                            assertFails { useBootstrapTransfer(fixture, transfer, resume = true) { _, _ -> adopted = true } }
                            assertFalse(adopted)
                        } finally { fixture.db.execSQL("PRAGMA ignore_check_constraints=OFF") }
                        throw rollback
                    }
                }
                assertTrue(generateSequence(thrown) { it.cause }.any { it.message == rollback.message })
                assertEquals(before, baselineQualificationVector(fixture.db, tables))
            }
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
        }
    }
    @Test fun descriptorLayoutReopensThroughOrdinaryClient() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("bootstrap-layout-reopen")
        try { withBootstrapFixture(dbPath = path) { fixture ->
            val descriptor = bindFixtureDescriptor(fixture)
            assertEquals("CURRENT", inspectRetainedRuntime(fixture.db, fixture.config))
            assertFails { inspectRetainedRuntime(fixture.db) }
            val objects = bootstrapFamilyObjectCount(fixture.db)
            assertEquals(descriptor.tables.sumOf { 1L + it.indexSql.size }, objects)
            assertEquals(1L + descriptor.tables.size, measureBootstrapMetadataCharge(fixture.db).entries)
            val state = descriptorState(fixture, descriptor)
            fixture.client.closeFromCoordinatedOwner(); fixture.db.close()
            val reopened = newFileBackedDb(path)
            val limited = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables,
                schema = fixture.config.serviceId, workspaceEntries = 1)
            try {
                assertIs<SyncWorkspaceLimitException>(limited.open().exceptionOrNull())
            } finally { limited.closeFromCoordinatedOwner() }
            val resumed = newClient(reopened, fixture.http, syncTables = fixture.config.syncTables,
                schema = fixture.config.serviceId, workspaceEntries = fixture.config.maxSyncWorkspaceEntries)
            try {
                resumed.openAndConnect("fixture-owner").getOrThrow()
                assertEquals("CURRENT", inspectRetainedRuntime(reopened, fixture.config))
                assertEquals(objects, bootstrapFamilyObjectCount(reopened))
                assertEquals(descriptor.signature, scalarText(reopened, "SELECT descriptor_signature FROM _sync_bootstrap_state"))
                assertEquals(state, descriptorState(fixture.copy(db = reopened), descriptor))
                assertEquals("local-only-sentinel", scalarText(reopened, "SELECT value FROM local_memos"))
                assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { resumed.closeFromCoordinatedOwner(); reopened.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun retainedDescriptorAndFamilyTamperingFailWithoutRepair() = runOwnedWebDatabaseTest {
        val mutations = listOf(
            listOf("UPDATE _sync_bootstrap_state SET descriptor_json=replace(descriptor_json,'main','pain')"),
            listOf("UPDATE _sync_bootstrap_tables SET table_index=99 WHERE slot=0"),
            listOf("UPDATE _sync_bootstrap_tables SET layout_signature=printf('%064d',1) WHERE slot=0"),
            listOf("DROP INDEX _sync_bootstrap_values_t1_unique_0", "CREATE UNIQUE INDEX _sync_bootstrap_values_t1_unique_0 ON _sync_bootstrap_values_t1(snapshot_id,v1 COLLATE BINARY)"),
            listOf("CREATE TABLE _sync_bootstrap_values_t99(value TEXT)"),
            listOf("CREATE TEMP TRIGGER stage_extra AFTER INSERT ON main._sync_bootstrap_values_t1 BEGIN SELECT 1; END"),
        )
        for (statements in mutations) withBootstrapFixture { fixture ->
            val descriptor = bindFixtureDescriptor(fixture)
            for (sql in statements) fixture.db.execSQL(sql)
            val before = descriptorState(fixture, descriptor)
            val definitions = familyDefinitions(fixture)
            assertFails { inspectRetainedRuntime(fixture.db, fixture.config) }
            assertEquals(before, descriptorState(fixture, descriptor))
            assertEquals(definitions, familyDefinitions(fixture))
            assertEquals(0L, scalarLong(fixture.db, "SELECT count(*) FROM bootstrap_parents"))
            assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
        }
    }

    @Test fun inactiveDescriptorCannotOwnSnapshotRows() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val descriptor = bindFixtureDescriptor(fixture)
            fixture.db.execSQL("""INSERT INTO _sync_bootstrap_values_t1(snapshot_id,row_ordinal,key_json,row_version,charge,v0,v1)
                VALUES('unowned',1,'{"id":"p"}',1,1,'p','parent')""")
            val before = descriptorState(fixture, descriptor)
            assertFails { inspectRetainedRuntime(fixture.db, fixture.config) }
            assertEquals(before, descriptorState(fixture, descriptor))
        }
        withBootstrapFixture { fixture ->
            fixture.db.execSQL("CREATE TABLE _sync_bootstrap_values_t0(value TEXT)")
            val before = state(fixture)
            assertFails { bindFixtureDescriptor(fixture) }
            assertFails { inspectRetainedRuntime(fixture.db, fixture.config) }
            assertEquals(before, state(fixture))
            assertEquals(1L, scalarLong(fixture.db, "SELECT count(*) FROM pragma_table_xinfo('_sync_bootstrap_values_t0')"))
        }
    }

    @Test fun schemaAdmissionBindsPhysicalIndexesCollationsAndCanonicalSlots() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
                val result = assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(
                    fixture.db, metadata, version, workspace, "bootstrap-schema", cache))
                result.pin.requireCurrent(fixture.db, metadata)
                val descriptor = result.descriptor
                assertEquals(listOf(1, 0), descriptor.tables.map { it.tableIndex })
                assertEquals(listOf(0, 1), descriptor.tables.map { it.slot })
                val child = descriptor.tables[0]; val parent = descriptor.tables[1]
                assertEquals("business", child.schema); assertEquals("main", child.localSchema)
                assertEquals(BootstrapForeignKey(3, 1, 0, "NOCASE"), child.metadata.foreignKeys.single())
                assertTrue(parent.metadata.uniqueKeys.any { it.columns == listOf(0 to "NOCASE") })
                assertTrue(parent.metadata.uniqueKeys.any { it.columns == listOf(1 to "RTRIM") })
                assertTrue(child.metadata.uniqueKeys.any { it.columns == listOf(3 to "BINARY", 4 to "BINARY") })
                assertTrue(descriptor.tables.all { isCanonicalSha256(it.metadata.triggerSignature) && isCanonicalSha256(it.metadata.indexSignature) })
                assertTrue(bootstrapManagedStateEmpty(fixture.db, metadata, cache))
                assertEquals("local-only-sentinel", scalarText(fixture.db, "SELECT value FROM local_memos"))
                assertEquals(0L, scalarLong(fixture.db, "SELECT count(*) FROM sqlite_schema WHERE name GLOB '_sync_bootstrap_values_t*'"))
                val other = newDb()
                try { assertFails { result.pin.requireCurrent(other, metadata) } } finally { other.close() }
            }
        }
    }

    @Test fun emptyUnmanagedIncomingForeignKeysRefuseBeforeStaging() = runOwnedWebDatabaseTest {
        for (temporary in listOf(false, true)) withBootstrapFixture { fixture ->
            fixture.db.execSQL("CREATE ${if (temporary) "TEMP " else ""}TABLE local_links(parent_id TEXT REFERENCES bootstrap_parents(id))")
            val before = state(fixture)
            assertEquals(BootstrapRefusal.UNMANAGED_FOREIGN_KEY, assertIs<BootstrapSchemaAdmission.Unavailable>(admitFixture(fixture)).reason)
            assertEquals(before, state(fixture))
            assertEquals(0L, scalarLong(fixture.db, "SELECT count(*) FROM local_links"))
        }
    }

    @Test fun additionalPersistentTemporaryAndInstallerTriggersRefuse() = runOwnedWebDatabaseTest {
        val statements = listOf(
            "CREATE TRIGGER extra_parent AFTER INSERT ON bootstrap_parents BEGIN SELECT 1; END",
            "CREATE TEMP TRIGGER extra_parent AFTER INSERT ON main.bootstrap_parents BEGIN SELECT 1; END",
            "CREATE TRIGGER extra_wire AFTER INSERT ON _sync_bootstrap_wire BEGIN SELECT 1; END",
        )
        for (sql in statements) withBootstrapFixture { fixture ->
            fixture.db.execSQL(sql)
            val before = state(fixture)
            assertEquals(BootstrapRefusal.ADDITIONAL_TRIGGER, assertIs<BootstrapSchemaAdmission.Unavailable>(admitFixture(fixture)).reason)
            assertEquals(before, state(fixture))
        }
    }

    @Test fun modifiedOwnedTriggerBodyIsNotAcceptedByItsName() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            fixture.db.execSQL("DROP TRIGGER trg_bootstrap_parents_bi_guard")
            fixture.db.execSQL("CREATE TRIGGER trg_bootstrap_parents_bi_guard BEFORE INSERT ON bootstrap_parents BEGIN SELECT 1; END")
            val before = state(fixture)
            assertEquals(BootstrapRefusal.TRIGGER_DEFINITION, assertIs<BootstrapSchemaAdmission.Unavailable>(admitFixture(fixture)).reason)
            assertEquals(before, state(fixture))
        }
    }

    @Test fun expressionPartialAndCustomCollationMetadataRefuse() = runOwnedWebDatabaseTest {
        for (sql in listOf(
            "CREATE UNIQUE INDEX unsupported_unique ON bootstrap_parents(lower(label))",
            "CREATE UNIQUE INDEX unsupported_unique ON bootstrap_parents(label) WHERE label!=''",
        )) withBootstrapFixture { fixture ->
            fixture.db.execSQL(sql)
            assertEquals(BootstrapRefusal.UNSUPPORTED_INDEX, assertIs<BootstrapSchemaAdmission.Unavailable>(admitFixture(fixture)).reason)
        }
        withBootstrapFixture { fixture ->
            // Test-driver metadata adapter: the public driver API does not register
            // custom collations. All SQL and storage still use the real connection.
            val operations = customCollationMetadataOperations()
            withBootstrapMetadata(fixture, operations) { metadata, version, workspace, cache ->
                assertEquals(BootstrapRefusal.UNSUPPORTED_INDEX, assertIs<BootstrapSchemaAdmission.Unavailable>(
                    admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-schema", cache)).reason)
            }
        }
    }

    @Test fun capabilityRefusalCannotHideStatementCleanupFailure() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            val cleanup = IllegalStateException("metadata reset failure")
            val before = state(fixture)
            val failure = assertFails {
                withBootstrapMetadata(fixture, customCollationMetadataOperations(cleanup)) { metadata, version, workspace, cache ->
                    admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-schema", cache)
                }
            }
            // Coroutine stack recovery may copy a standard exception and keep
            // the original as its cause. Preserve its exact identity once in
            // the failure graph, matching the existing cleanup contract tests.
            val visited = mutableListOf<Throwable>()
            fun identities(current: Throwable): Int {
                if (visited.any { it === current }) return 0
                visited += current
                return (if (current === cleanup) 1 else 0) +
                    (current.cause?.let(::identities) ?: 0) + current.suppressedExceptions.sumOf(::identities)
            }
            assertEquals(1, identities(failure), failure.stackTraceToString())
            assertTrue(failure.suppressedExceptions.isNotEmpty(), failure.stackTraceToString())
            assertEquals(before, state(fixture))
        }
    }

    @Test fun childIndexMustMatchParentComparisonAndAdditionalIndexesChangeBinding() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            fixture.db.execSQL("DROP INDEX bootstrap_child_parent")
            // Composite uniqueness starts with the child column but uses BINARY;
            // the referenced parent's PRIMARY KEY compares with NOCASE.
            val refused = assertIs<BootstrapSchemaAdmission.Unavailable>(admitFixture(fixture))
            assertEquals(BootstrapRefusal.MISSING_CHILD_INDEX, refused.reason)
            assertEquals(BootstrapChildIndexRequirement("bootstrap_children", "parent_id", "NOCASE"), refused.missingChildIndex)
            fixture.db.execSQL("CREATE INDEX bootstrap_child_parent ON bootstrap_children(parent_id COLLATE NOCASE)")
            val before = assertIs<BootstrapSchemaAdmission.Eligible>(admitFixture(fixture)).descriptor
            fixture.db.execSQL("CREATE INDEX additional_parent_label ON bootstrap_parents(label)")
            val after = assertIs<BootstrapSchemaAdmission.Eligible>(admitFixture(fixture)).descriptor
            assertNotEquals(before.signature, after.signature)
            assertEquals(before.tables[1].metadata.indexWorkComponents + 1, after.tables[1].metadata.indexWorkComponents)
        }
    }

    @Test fun schemaPinsDoNotSurviveDdlOrMetadataOwnerChanges() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
                fixture.db.execSQL("CREATE INDEX changed_during_metadata ON bootstrap_parents(label)")
                assertFails { admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-schema", cache) }
            }
            withBootstrapMetadata(fixture) { metadata, version, workspace, cache ->
                val admitted = assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(fixture.db, metadata, version, workspace, "bootstrap-schema", cache))
                assertFails { admitted.pin.requireCurrent(fixture.db, metadata.copy()) }
                fixture.db.execSQL("CREATE TEMP TABLE changed_after_pin(value TEXT)")
                assertFails { admitted.pin.requireCurrent(fixture.db, metadata) }
            }
        }
    }

    @Test fun emptyProofIncludesBusinessAndAuthoritativeAbsenceOwners() = runOwnedWebDatabaseTest {
        withBootstrapFixture { fixture ->
            withBootstrapMetadata(fixture) { metadata, _, _, cache ->
                assertTrue(bootstrapManagedStateEmpty(fixture.db, metadata, cache))
                fixture.db.execSQL("INSERT INTO bootstrap_parents VALUES('p','parent')")
                assertFalse(bootstrapManagedStateEmpty(fixture.db, metadata, cache))
            }
        }
        for (view in listOf("server", "desired")) withBootstrapFixture { fixture ->
            withBootstrapMetadata(fixture) { metadata, _, _, cache ->
                fixture.db.execSQL("""INSERT INTO _sync_relationships(view,child_schema,child_table,child_key,fk_column,
                    parent_schema,parent_table,parent_key,owner_revision) VALUES('$view','business','bootstrap_children',
                    '{"id":"c"}','parent_id','business','bootstrap_parents','{"id":"p"}',0)""")
                assertEquals(0L, scalarLong(fixture.db, "SELECT count(*) FROM bootstrap_children"))
                assertEquals(0L, scalarLong(fixture.db, "SELECT count(*) FROM _sync_row_state"))
                assertFalse(bootstrapManagedStateEmpty(fixture.db, metadata, cache))
            }
        }
        withBootstrapFixture { fixture ->
            withBootstrapMetadata(fixture) { metadata, _, _, cache ->
                val local = OversqliteLocalStore(fixture.db, json) { metadata }
                OversqliteSyncStateStore(fixture.db) { local }.installAuthoritativeBaseline("business", "bootstrap_parents",
                    "{\"id\":\"absent\"}", "absent", 0, null, "snapshot", 0, targetContext = OversqliteProvenanceContextStore(fixture.db).activeId())
                assertEquals(0L, scalarLong(fixture.db, "SELECT count(*) FROM bootstrap_parents"))
                assertFalse(bootstrapManagedStateEmpty(fixture.db, metadata, cache))
            }
        }
    }

    private suspend fun state(fixture: BootstrapFixture): BaselineQualificationVector = baselineQualificationVector(fixture.db,
        listOf("bootstrap_parents", "bootstrap_children", "local_memos", "_sync_row_state", "_sync_relationships", "_sync_operation_state"))

    private suspend fun descriptorState(fixture: BootstrapFixture, descriptor: SnapshotDescriptor): BaselineQualificationVector =
        baselineQualificationVector(fixture.db, listOf("bootstrap_parents", "bootstrap_children", "local_memos",
            "_sync_bootstrap_state", "_sync_bootstrap_tables", "_sync_bootstrap_wire", "_sync_bootstrap_pages") + descriptor.tables.map { it.stageName })

    private suspend fun familyDefinitions(fixture: BootstrapFixture): List<String> = fixture.db.queryList(
        "SELECT name||':'||type||':'||tbl_name||':'||COALESCE(sql,'') FROM (SELECT name,type,tbl_name,sql FROM sqlite_schema UNION ALL SELECT name,type,tbl_name,sql FROM sqlite_temp_schema) WHERE $bootstrapFamilyPredicate ORDER BY name,type",
    ) { it.getText(0) }
}
