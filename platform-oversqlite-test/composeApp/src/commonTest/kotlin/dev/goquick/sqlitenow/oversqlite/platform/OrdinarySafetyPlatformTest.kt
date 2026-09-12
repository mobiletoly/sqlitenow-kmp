package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.time.TimeSource
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

internal class OrdinarySafetyPlatformTest : PlatformCrossTargetTestSupport() {
    private fun scenarioEnabled(method: String): Boolean {
        val identity = "dev.goquick.sqlitenow.oversqlite.platform.OrdinarySafetyPlatformTest.$method"
        if (!platformSuiteEnabled()) {
            println("ORDINARY_NOT_EXECUTED ${webRuntimeKind()} $identity opt_in_disabled")
            return false
        }
        println("ORDINARY_ENTER ${webRuntimeKind()} $identity")
        return true
    }

    private fun config(work: Long = 100_000_000) = OversqliteConfig(
        generationId = 1, databaseId = "platform-sync", serviceId = "platform-mock",
        syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"), SyncTable("posts", syncKeyColumnName = "id")),
        maxSyncWorkUnitsPerInvocation = work,
    )

    private suspend fun sameLifetimeRetainedRefusal(
        openingConfig: OversqliteConfig,
        seed: suspend (dev.goquick.sqlitenow.core.SafeSQLiteConnection, MockSyncServer, io.ktor.client.HttpClient) -> Unit,
    ): Throwable {
        val server = MockSyncServer()
        val http = server.newHttpClient()
        var requestsBefore: Map<String, Int>? = null
        val provider = RetainedOpenerFixtureProvider(oversqliteTestConnectionProvider()) { connection ->
            seed(connection, server, http)
            requestsBefore = server.requestCounts.toMap()
        }
        val db = newCoordinatedDb(provider)
        var migrationCalls = 0
        db.migrationFailureForTest = { migrationCalls++ }
        try {
            val failure = assertFails {
                db.openClient(openingConfig, http, ServerWinsResolver,
                    onRefusalBeforeClose = { provider.observeBeforeClose() })
            }
            assertNotNull(provider.before, "fixture input reached the public coordinator")
            assertEquals(provider.before, assertNotNull(provider.after), "complete retained census before same-handle close")
            assertSame(provider.preparedConnection, provider.observedConnection, "no close/reopen or replacement handle in the preservation oracle")
            assertEquals(1, provider.seedCalls)
            assertEquals(1, provider.closeCalls, "real default-provider close joined")
            assertEquals(0, provider.publications, "refused state was never published")
            assertEquals(0, migrationCalls, "retained refusal preceded migration callback")
            assertEquals(assertNotNull(requestsBefore), server.requestCounts, "refusal made no HTTP callback/request after fixture preparation")
            return failure
        } finally {
            http.close(); db.close(); cleanupOwnedWorkerStorage()
        }
    }

    private suspend fun seedSameLifetimeReadyClient(
        connection: dev.goquick.sqlitenow.core.SafeSQLiteConnection,
        http: io.ktor.client.HttpClient,
        block: suspend (DefaultOversqliteClient) -> Unit,
    ) {
        createUsersAndPostsTables(connection)
        connection.execSQL("PRAGMA user_version=1")
        val client = DefaultOversqliteClient.forTesting(connection, config(), http, ServerWinsResolver)
        try {
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            block(client)
        } finally { client.closeFromCoordinatedOwner() }
    }

    private suspend fun workspaceRuntimeState(db: PlatformTestDatabase, cfg: OversqliteConfig): RuntimeState {
        val connection = db.connection()
        val generation = OversqliteContractStore(connection).readGeneration(1)
        val validated = SyncRuntimeInitializer(cfg, TableInfoCache()).validateConfig(connection).withGeneration(generation)
        val limits = testWireLimits()
        return RuntimeState(validated, AdmittedOversqliteContract("v1", generation,
            OversqliteWireLimits(limits.maxPushRows, limits.maxPushDecodedBytes, limits.maxPullBundles,
                limits.maxPullDecodedBytes, limits.maxSnapshotRowsPerChunk, limits.maxSnapshotDecodedBytesPerChunk,
                limits.maxSnapshotRows, limits.maxSnapshotLogicalBytes, limits.maxRowDecodedBytes), false),
            "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
    }

    private fun nodeEffects() = listOf(
        BundleRow("main", "nodes", mapOf("id" to "P"), "DELETE", 2, null),
        BundleRow("main", "nodes", mapOf("id" to "C"), "UPDATE", 2,
            Json.parseToJsonElement("""{"id":"C","name":"child","parent_id":"Q"}""")),
    )

    private fun remoteNode(id: String, parent: String?) = BundleRow(
        "main", "nodes", mapOf("id" to id), "UPDATE", 2,
        buildJsonObject {
            put("id", id)
            put("name", "S-$id")
            put("parent_id", parent?.let(::JsonPrimitive) ?: JsonNull)
        },
    )

    @Test fun admissionAndParents() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("admissionAndParents")) return@runTest
        val topologies = linkedMapOf(
            "new-chain" to listOf(remoteNode("X", "Y"), remoteNode("Y", null)),
            "new-self" to listOf(remoteNode("X", "X")),
            "new-cycle" to listOf(remoteNode("X", "Y"), remoteNode("Y", "X")),
            "existing" to listOf(remoteNode("C", "Q"), remoteNode("Q", null), remoteNode("D", "C")),
            "new-parent" to listOf(remoteNode("C", "X"), remoteNode("X", null)),
            "self" to listOf(remoteNode("C", "C")),
            "cycle" to listOf(remoteNode("C", "X"), remoteNode("X", "C")),
            "null" to listOf(remoteNode("C", null)),
        )
        try {
            for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) for ((topology, rows) in topologies) {
                val reason = "${action.replace(' ', '-')}/$topology"
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration(action)))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    val ddl = "CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)"
                    val nodeConfig = config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id")), snapshotApplyBatchRows = 2)
                    db.execSQL(ddl)
                    val client = db.openClient(nodeConfig, http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val seedProvenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")).jsonObject
                    val expectedValues = linkedMapOf("P" to ("parent" to null), "Q" to ("other" to null),
                        "C" to ("child" to "P"), "D" to ("grandchild" to "C"))
                    for (row in rows) {
                        val value = requireNotNull(row.payload).jsonObject
                        expectedValues[row.key.getValue("id")] = value.getValue("name").jsonPrimitive.content to value.getValue("parent_id").jsonPrimitive.contentOrNull
                    }
                    val changedRows = rows.map { it.key.getValue("id") }.toSet()
                    assertEquals(2L, server.addRemoteBundleForTest(rows))
                    val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                    val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(attachmentBefore.currentSourceId))
                    val before = databaseStateVector(db.connection(), listOf("nodes"))
                    val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
                    fun projection(columns: List<String>) = columns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
                    fun typed(value: JsonElement, storage: String) = buildJsonObject { put("storage", storage); put("value", value) }
                    val conflictQuery = "SELECT json_object(${projection(conflictColumns)}) FROM _sync_conflict_state"
                    val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
                    val revisionBefore = conflictBefore.getValue("baseline_revision").jsonObject.getValue("value").jsonPrimitive.long
                    val requestStart = server.requestSourceIds.size
                    val pushesBefore = server.pushRequestCount
                    val chunksBefore = server.snapshotChunkRequestCount
                    val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    val report = kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { client.rebuild().getOrThrow() }
                    val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, report.outcome)
                    assertEquals(RestoreSummary(bundleSeq = 2, rowCount = expectedValues.size.toLong()), report.restore)
                    assertEquals(SyncStatus(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, PendingSyncStatus(false, 0, false), 2), report.status)
                    fun verifyRestoreCounters(owner: DefaultOversqliteClient) {
                        val counters = owner.snapshotRestoreDiagnosticsForTest()
                        assertEquals(1L, counters.sessionCount); assertEquals(1L, counters.fetchedChunks)
                        assertEquals(expectedValues.size, counters.maxValidatedChunkRows)
                        assertEquals(2, counters.maxLiveApplyPageRows); assertEquals((expectedValues.size.toLong() + 1) / 2, counters.applyPages)
                        assertEquals(expectedValues.size.toLong(), counters.finalStagedRows); assertEquals(expectedValues.size.toLong(), counters.finalAppliedRows)
                        assertTrue(counters.maxDeclaredChunkBytes > 0 && counters.maxCompletelyDecodedChunkBodyBytes > 0)
                        assertTrue(counters.maxLiveApplyPageStagedTextBytes > 0 && counters.maxLiveApplyPageStagedTextBytes <= nodeConfig.snapshotApplyBatchBytes)
                        assertEquals(0L, counters.capacityResponses); assertEquals(0L, counters.capacityRetries); assertEquals(0L, counters.capacityWaitMillis)
                        assertTrue(counters.restoreDurationMillis >= 0)
                    }
                    verifyRestoreCounters(client)
                    val requests = server.requestSourceIds.drop(requestStart)
                    assertEquals(3, requests.size, "$reason one snapshot creation, one chunk and actual session cleanup")
                    assertEquals("/sync/snapshot-sessions", requests[0].first)
                    assertTrue(requests[1].first.startsWith("/sync/snapshot-sessions/"))
                    assertEquals(requests[1].first, requests[2].first)
                    assertEquals(List(3) { attachmentBefore.currentSourceId }, requests.map { it.second }, "$reason explicit rebuild has no source retarget")
                    assertEquals(chunksBefore + 1, server.snapshotChunkRequestCount)
                    assertTrue(server.requestedSnapshotSourceReplacements.isEmpty())
                    assertEquals(pushesBefore, server.pushRequestCount, "recovery never sends outbound intent")
                    fun payload(id: String, name: String, parent: String?) = buildJsonObject {
                        put("id", typed(JsonPrimitive(id), "text")); put("name", typed(JsonPrimitive(name), "text"))
                        put("parent_id", typed(parent?.let(::JsonPrimitive) ?: JsonNull, if (parent == null) "null" else "text"))
                    }
                    val expected = expectedValues.toList().sortedBy { it.first }.associate { (id, value) ->
                        id to payload(id, value.first, value.second)
                    }
                    suspend fun verifySnapshot(target: PlatformTestDatabase, source: String, firstRevision: Long, start: String, end: String) {
                        assertEquals(JsonArray(expected.values.toList()), Json.parseToJsonElement(scalarText(target,
                            "SELECT json_group_array(json_object(${projection(listOf("id", "name", "parent_id"))})) FROM (SELECT * FROM nodes ORDER BY id)")), "$reason every typed business field including clean grandchild")
                        assertEquals(expectedValues.size.toLong(), scalarLong(target, "SELECT count(*) FROM _sync_row_state"))
                        for ((index, entry) in expected.entries.withIndex()) {
                            val (id, value) = entry
                            val predicate = "schema_name='main' AND table_name='nodes' AND key_json='{\"id\":\"$id\"}'"
                            val version = if (id in changedRows) 2L else 1L
                            assertEquals(value, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE $predicate")))
                            assertEquals(JsonObject(seedProvenance + mapOf("origin" to JsonPrimitive("snapshot"), "origin_source_id" to JsonPrimitive(source),
                                "bundle_sequence" to JsonPrimitive(2), "authoritative_version" to JsonPrimitive(version))),
                                Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "$reason complete provenance $id")
                            assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                            assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                            assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                            assertEquals(firstRevision + index, scalarLong(target, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                            val updated = scalarText(target, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                            assertTrue(updated >= start && updated <= end)
                            for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                                assertEquals("text", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                            for (column in listOf("row_version", "relationships_known", "server_revision"))
                                assertEquals("integer", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                        }
                        val edgeColumns = listOf("view", "child_schema", "child_table", "child_key", "fk_column", "parent_schema", "parent_table", "parent_key", "owner_revision")
                        val edges = expectedValues.toList().sortedBy { it.first }.mapIndexedNotNull { index, (child, value) ->
                            value.second?.let { parent -> JsonObject(mapOf(
                                "view" to JsonPrimitive("server"), "child_schema" to JsonPrimitive("main"), "child_table" to JsonPrimitive("nodes"),
                                "child_key" to JsonPrimitive("{\"id\":\"$child\"}"), "fk_column" to JsonPrimitive("parent_id"), "parent_schema" to JsonPrimitive("main"),
                                "parent_table" to JsonPrimitive("nodes"), "parent_key" to JsonPrimitive("{\"id\":\"$parent\"}"),
                                "owner_revision" to JsonPrimitive(firstRevision + index)
                            ).mapValues { (column, field) -> typed(field, if (column == "owner_revision") "integer" else "text") }) }
                        }
                        assertEquals(JsonArray(edges), Json.parseToJsonElement(scalarText(target,
                            "SELECT json_group_array(json_object(${projection(edgeColumns)})) FROM (SELECT * FROM _sync_relationships ORDER BY child_key,view)")), "$reason exact typed authority edges, no dirty/desired edges")
                        val vector = databaseStateVector(target.connection(), listOf("nodes"))
                        for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                            assertTrue(vector.getValue(table).isEmpty(), "$reason retired transient owner $table")
                        assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(target.connection()).loadState())
                        assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(target.connection()).loadBundleState())
                        assertEquals(WorkspaceCharge(1, 536), OversqliteWorkspace(target.connection(), nodeConfig).usage(applyUnit("author-snapshot-parents", 0, "")))
                        assertEquals(0L, scalarLong(target, "SELECT count(*) FROM pragma_foreign_key_check"))
                    }
                    verifySnapshot(db, attachmentBefore.currentSourceId, revisionBefore + 5, publicationStart, publicationEnd)
                    assertEquals(sourceBefore, OversqliteSourceStateStore(db.connection()).loadState(attachmentBefore.currentSourceId))
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state")); assertEquals(2L, sourceBefore.nextSourceBundleId)
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 2, rebuildRequired = false, pendingInitializationId = ""),
                        OversqliteAttachmentStateStore(db.connection()).loadState())
                    assertEquals(JsonObject(conflictBefore + ("baseline_revision" to typed(JsonPrimitive(revisionBefore + 4 + expected.size), "integer"))),
                        Json.parseToJsonElement(scalarText(db, conflictQuery)), "$reason only four A removals and complete snapshot installations advance conflict controls")
                    val recovered = databaseStateVector(db.connection(), listOf("nodes"))
                    val changed = setOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_attachment_state", "_sync_conflict_state")
                    assertEquals(before.filterKeys { it !in changed }, recovered.filterKeys { it !in changed }, "$reason every other full primary/recovery/stage identity preserved")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} admissionAndParents $reason/full-snapshot")
                    val peer = newCoordinatedDb()
                    val peerHttp = server.newHttpClient()
                    try {
                        peer.execSQL(ddl)
                        val peerStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        val observer = peer.openClient(nodeConfig, peerHttp, ServerWinsResolver)
                        val peerAttachment = OversqliteAttachmentStateStore(peer.connection()).loadState()
                        val peerSource = requireNotNull(OversqliteSourceStateStore(peer.connection()).loadState(peerAttachment.currentSourceId))
                        val peerOpening = requireNotNull(OversqliteOpeningStore(peer.connection()).readOrNull())
                        val openingQuery = "SELECT json_object(${projection(oversqliteOpeningColumnNames)}) FROM _sync_opening_state"
                        val openingFields = Json.parseToJsonElement(scalarText(peer, openingQuery)).jsonObject
                        val peerConflict = Json.parseToJsonElement(scalarText(peer, conflictQuery)).jsonObject
                        val peerBefore = databaseStateVector(peer.connection(), listOf("nodes"))
                        assertFalse(peerOpening.contractConfirmed); assertEquals("", peerOpening.targetFingerprint)
                        val connected = assertIs<AttachResult.Connected>(kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { observer.attach("user-1").getOrThrow() })
                        assertEquals(AttachOutcome.USED_REMOTE_STATE, connected.outcome)
                        verifyRestoreCounters(observer)
                        observer.pullToStable().getOrThrow()
                        val peerEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        assertNotEquals(attachmentBefore.currentSourceId, peerAttachment.currentSourceId)
                        verifySnapshot(peer, peerAttachment.currentSourceId, 1, peerStart, peerEnd)
                        assertEquals(peerSource, OversqliteSourceStateStore(peer.connection()).loadState(peerAttachment.currentSourceId))
                        assertEquals(1L, peerSource.nextSourceBundleId); assertEquals("", peerSource.replacedBySourceId)
                        assertTrue(peerSource.createdAt >= peerStart && peerSource.createdAt <= peerEnd)
                        assertEquals(1L, scalarLong(peer, "SELECT count(*) FROM _sync_source_state"))
                        assertEquals(peerAttachment.copy(attachmentEpoch = peerAttachment.attachmentEpoch + 1, bindingState = attachmentBindingAttached,
                            attachedUserId = "user-1", schemaName = attachmentBefore.schemaName, lastBundleSeqSeen = 2, rebuildRequired = false, pendingInitializationId = ""),
                            OversqliteAttachmentStateStore(peer.connection()).loadState())
                        assertEquals(JsonObject(peerConflict + ("baseline_revision" to typed(JsonPrimitive(expected.size), "integer"))),
                            Json.parseToJsonElement(scalarText(peer, conflictQuery)), "$reason complete independent conflict controls")
                        val fingerprint = seedProvenance.getValue("generation_fingerprint").jsonPrimitive.content
                        val confirmed = peerOpening.copy(revision = peerOpening.revision + 1, targetFingerprint = fingerprint, contractConfirmed = true)
                        assertEquals(confirmed, OversqliteOpeningStore(peer.connection()).readOrNull())
                        assertEquals(JsonObject(openingFields + mapOf("revision" to typed(JsonPrimitive(confirmed.revision), "integer"),
                            "target_fingerprint" to typed(JsonPrimitive(fingerprint), "text"), "contract_confirmed" to typed(JsonPrimitive(1), "integer"),
                            "integrity_hash" to typed(JsonPrimitive(confirmed.integrityHash()), "text"))), Json.parseToJsonElement(scalarText(peer, openingQuery)),
                            "$reason full independent opening permits exact initial confirmation only")
                        val peerAfter = databaseStateVector(peer.connection(), listOf("nodes"))
                        for (table in listOf("_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                            assertEquals(recovered.getValue(table), peerAfter.getValue(table), "$reason full independent admitted fixture/contract identity $table")
                        // Initial attach publishes these admission owners; their complete
                        // vectors above must match the established client's exact contract.
                        val peerChanged = changed + setOf("_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")
                        assertEquals(peerBefore.filterKeys { it !in peerChanged }, peerAfter.filterKeys { it !in peerChanged },
                            "$reason every other independent primary/recovery/stage owner preserved")
                        assertEquals(pushesBefore, server.pushRequestCount)
                        assertEquals(recovered, databaseStateVector(db.connection(), listOf("nodes")), "$reason independent authority verification preserves original full state")
                        println("ORDINARY_SCENARIO ${webRuntimeKind()} admissionAndParents $reason/independent-authority-readback")
                    } finally { peerHttp.close(); peer.close() }
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun publicPushAndPullParentTransitionsPreserveCompleteVectors() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("publicPushAndPullParentTransitionsPreserveCompleteVectors")) return@runTest
        val cases = linkedMapOf(
            "new-chain" to listOf(remoteNode("X", "Y"), remoteNode("Y", null)),
            "new-self" to listOf(remoteNode("X", "X")),
            "new-cycle" to listOf(remoteNode("X", "Y"), remoteNode("Y", "X")),
            "existing" to listOf(remoteNode("C", "Q")),
            "new" to listOf(remoteNode("C", "X"), remoteNode("X", null)),
            "self" to listOf(remoteNode("C", "C")),
            "cycle" to listOf(remoteNode("C", "X"), remoteNode("X", "C")),
            "null" to listOf(remoteNode("C", null)),
        )
        try {
            for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) for (operation in listOf("push", "pull")) for ((case, rows) in cases) {
                val tag = "${action.replace(' ', '-')}/$operation/$case"
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration(action)))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
                    // Anonymous data already exists when the public attach admits it.
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.attach("user-1").getOrThrow()
                    assertEquals(0, server.pushRequestCount, "$tag attach does not imply a push")
                    assertEquals(4L, client.syncStatus().getOrThrow().pending.pendingRowCount)
                    client.pushPending().getOrThrow()
                    val before = databaseStateVector(db.connection(), listOf("nodes"))
                    val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                    val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
                    val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                    val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
                    val baselineBefore = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
                    val dirtyBefore = scalarLong(db, "SELECT dirty_revision FROM _sync_conflict_state")
                    fun typedProjection(columns: List<String>) = columns.joinToString(",") {
                        "'$it',json_object('storage',typeof($it),'value',$it)"
                    }
                    fun typed(payload: JsonObject) = buildJsonObject {
                        for ((column, field) in payload) putJsonObject(column) {
                            put("storage", if (field == JsonNull) "null" else "text"); put("value", field)
                        }
                    }
                    val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
                    val conflictQuery = "SELECT json_object(${typedProjection(conflictColumns)}) FROM _sync_conflict_state"
                    val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
                    val changed = rows.map { it.key.getValue("id") }.toSet()
                    val unchangedPredicate = "key_json NOT IN (${changed.joinToString(",") { "'{\"id\":\"$it\"}'" }})"
                    val unchangedA = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_row_state" to unchangedPredicate)).getValue("_sync_row_state")
                    val seedProvenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")).jsonObject
                    val expected = linkedMapOf("P" to ("parent" to null), "Q" to ("other" to null), "C" to ("child" to "P"), "D" to ("grandchild" to "C"))
                    val newKeys = changed - expected.keys
                    val capturedBaselineCount = if (operation == "push") newKeys.size else 0
                    for (row in rows) {
                        val value = requireNotNull(row.payload).jsonObject
                        expected[row.key.getValue("id")] = value.getValue("name").jsonPrimitive.content to value.getValue("parent_id").jsonPrimitive.contentOrNull
                    }
                    val requestsBefore = server.requestCounts.toMap()
                    val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    if (operation == "push") {
                        db.connection().transaction(TransactionMode.IMMEDIATE) {
                            for (row in rows) {
                                val id = row.key.getValue("id")
                                val (name, parent) = expected.getValue(id)
                                db.execSQL("INSERT INTO nodes VALUES('$id','$name',${parent?.let { "'$it'" } ?: "NULL"}) ON CONFLICT(id) DO UPDATE SET name=excluded.name,parent_id=excluded.parent_id")
                            }
                        }
                        assertEquals(baselineBefore + newKeys.size, scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state"), "$tag only new unpublished owners advance A during local capture")
                        for (id in newKeys) {
                            val predicate = "key_json='{\"id\":\"$id\"}'"
                            assertEquals("unpublished", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                            assertEquals(0L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                            assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                            assertEquals(1L, scalarLong(db, "SELECT server_payload IS NULL FROM _sync_row_state WHERE $predicate"))
                            val unpublishedProvenance = JsonObject(seedProvenance + mapOf("origin" to JsonPrimitive("never_published"), "bundle_sequence" to JsonNull, "authoritative_version" to JsonPrimitive(0)))
                            assertEquals(unpublishedProvenance, Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "$tag new owner is never-published, not unknown or confirmed authority")
                            assertEquals("absent", scalarText(db, "SELECT base_kind FROM _sync_dirty_rows WHERE $predicate"))
                            assertEquals(0L, scalarLong(db, "SELECT base_row_version FROM _sync_dirty_rows WHERE $predicate"))
                            assertEquals(1L, scalarLong(db, "SELECT base_payload IS NULL FROM _sync_dirty_rows WHERE $predicate"))
                            assertEquals(unpublishedProvenance, Json.parseToJsonElement(scalarText(db, "SELECT base_provenance FROM _sync_dirty_rows WHERE $predicate")))
                        }
                        client.pushPending().getOrThrow()
                    } else {
                        server.addRemoteBundleForTest(rows)
                        client.pullToStable().getOrThrow()
                    }
                    val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    val requestDelta = server.requestCounts.mapValues { (path, count) -> count - requestsBefore.getOrElse(path) { 0 } }.filterValues { it != 0 }
                    assertEquals(mapOf((if (operation == "push") "/sync/push" else "/sync/pull") to 1), requestDelta, "$tag exact one public data-plane request; no repair/evasion")
                    val expectedBusiness = buildJsonArray { for ((id, value) in expected.toList().sortedBy { it.first }) add(buildJsonObject {
                        put("id", id); put("name", value.first); put("parent_id", value.second?.let(::JsonPrimitive) ?: JsonNull)
                    }) }
                    assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "$tag complete business payload")
                    val typedBusinessQuery = "SELECT json_group_array(json_object(${typedProjection(listOf("id", "name", "parent_id"))})) FROM (SELECT * FROM nodes ORDER BY id)"
                    assertEquals(JsonArray(expectedBusiness.map { typed(it.jsonObject) }), Json.parseToJsonElement(scalarText(db, typedBusinessQuery)), "$tag complete business storage types and values")
                    assertEquals(expected.size.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_row_state"), "$tag no fabricated or missing A owner")
                    for ((id, value) in expected) {
                        val payload = buildJsonObject { put("id", id); put("name", value.first); put("parent_id", value.second?.let(::JsonPrimitive) ?: JsonNull) }
                        val typed = buildJsonObject { for ((column, field) in payload) putJsonObject(column) { put("storage", if (field == JsonNull) "null" else "text"); put("value", field) } }
                        assertEquals(typed, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$tag complete typed A $id")
                        val version = if (id in changed) 2L else 1L
                        val provenance = if (id in changed) JsonObject(seedProvenance + mapOf("origin" to JsonPrimitive(if (operation == "push") "settlement" else "pull"), "bundle_sequence" to JsonPrimitive(2), "authoritative_version" to JsonPrimitive(2))) else seedProvenance
                        assertEquals(provenance, Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$tag every provenance field $id")
                        assertEquals(version, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                        assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                        val predicate = "key_json='{\"id\":\"$id\"}'"
                        assertEquals("main", scalarText(db, "SELECT schema_name FROM _sync_row_state WHERE $predicate"))
                        assertEquals("nodes", scalarText(db, "SELECT table_name FROM _sync_row_state WHERE $predicate"))
                        assertEquals("live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                        for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                            assertEquals("text", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                        for (column in listOf("row_version", "relationships_known", "server_revision"))
                            assertEquals("integer", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                        if (id in changed) {
                            val at = scalarText(db, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                            assertTrue(at >= publicationStart && at <= publicationEnd, "$tag fresh A timestamp $id")
                        }
                    }
                    val changedKeys = changed.joinToString(",") { "'{\"id\":\"$it\"}'" }
                    assertEquals((baselineBefore + capturedBaselineCount + 1..baselineBefore + capturedBaselineCount + rows.size).toSet(), db.connection().queryList("SELECT server_revision FROM _sync_row_state WHERE key_json IN ($changedKeys)") { it.getLong(0) }.toSet(), "$tag exact fresh owner revision set after source-backed unpublished capture")
                    assertEquals(unchangedA, databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_row_state" to unchangedPredicate)).getValue("_sync_row_state"), "$tag every unaffected A field, revision and timestamp")
                    val edges = buildJsonArray { for ((id, value) in expected.toList().sortedBy { it.first }) value.second?.let { parent -> add(buildJsonObject { put("view", "server"); put("child", id); put("fk", "parent_id"); put("parent", parent) }) } }
                    assertEquals(edges, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('view',view,'child',json_extract(child_key,'$.id'),'fk',fk_column,'parent',json_extract(parent_key,'$.id'))) FROM (SELECT * FROM _sync_relationships ORDER BY child_key)")), "$tag every edge")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships e JOIN _sync_row_state a ON a.schema_name=e.child_schema AND a.table_name=e.child_table AND a.key_json=e.child_key WHERE e.owner_revision<>a.server_revision"))
                    val edgeColumns = listOf("view", "child_schema", "child_table", "child_key", "fk_column", "parent_schema", "parent_table", "parent_key", "owner_revision")
                    val expectedFullEdges = expected.toList().sortedBy { it.first }.mapNotNull { (id, value) -> value.second?.let { parent ->
                        val ownerRevision = scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")
                        buildJsonObject {
                            for ((column, field) in linkedMapOf("view" to "server", "child_schema" to "main", "child_table" to "nodes", "child_key" to "{\"id\":\"$id\"}", "fk_column" to "parent_id", "parent_schema" to "main", "parent_table" to "nodes", "parent_key" to "{\"id\":\"$parent\"}"))
                                putJsonObject(column) { put("storage", "text"); put("value", field) }
                            putJsonObject("owner_revision") { put("storage", "integer"); put("value", ownerRevision) }
                        }
                    } }
                    assertEquals(JsonArray(expectedFullEdges), Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object(${typedProjection(edgeColumns)})) FROM (SELECT * FROM _sync_relationships ORDER BY child_key)")), "$tag exact full nine-column typed edge inventory")
                    val after = databaseStateVector(db.connection(), listOf("nodes"))
                    for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(after.getValue(table).isEmpty(), "$tag cleanup $table")
                    for (table in listOf("_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) assertEquals(before.getValue(table), after.getValue(table), "$tag complete identity $table")
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    assertEquals(sourceBefore.copy(nextSourceBundleId = sourceBefore.nextSourceBundleId + if (operation == "push") 1 else 0), OversqliteSourceStateStore(db.connection()).loadState(source), "$tag full source owner permits only confirmed allocation")
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 2), OversqliteAttachmentStateStore(db.connection()).loadState(), "$tag full attachment/checkpoint owner")
                    assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState(), "$tag complete operation/recovery/transient owner restored")
                    fun integer(value: Long) = buildJsonObject { put("storage", "integer"); put("value", value) }
                    assertEquals(JsonObject(conflictBefore + mapOf("baseline_revision" to integer(baselineBefore + capturedBaselineCount + rows.size), "dirty_revision" to integer(dirtyBefore + if (operation == "push") 2L * rows.size else 0))), Json.parseToJsonElement(scalarText(db, conflictQuery)), "$tag exact complete conflict/revision owner")
                    assertEquals(source, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                    assertEquals(if (operation == "push") 3L else 2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(if (operation == "push") 2 else 1, server.pushRequestCount)
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0, "$tag no snapshot substitution")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} publicPushAndPullParentTransitionsPreserveCompleteVectors $tag")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun actionsAndSurvivors() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("actionsAndSurvivors")) return@runTest
        for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) {
            val server = MockSyncServer(
                registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                generationResponses = mapOf(1L to nodeGeneration(action)),
            )
            val db = newCoordinatedDb()
            val http = server.newHttpClient()
            try {
                db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                val client = db.openClient(
                    config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                    http, ServerWinsResolver,
                )
                client.attach("user-1").getOrThrow()
                db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                client.pushPending().getOrThrow()
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"), "$action seed clean")
                db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                val before = databaseStateVector(db.connection(), listOf("nodes"))
                server.normalizePushEffects = { nodeEffects() }
                client.pushPending().getOrThrow()
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='P'"), action)
                assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"), action)
                assertEquals("child", scalarText(db, "SELECT name FROM nodes WHERE id='C'"), action)
                assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"), action)
                assertEquals("grandchild", scalarText(db, "SELECT name FROM nodes WHERE id='D'"), action)
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"), action)
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"), action)
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"), action)
                assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"), action)
                assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), action)
                assertNotEquals(before, databaseStateVector(db.connection(), listOf("nodes")), "$action settled full vector")
            } finally { http.close(); db.close() }
        }
        cleanupOwnedWorkerStorage()
    }

    @Test fun reachedDeleteConflictRetainsDirtyDependentBasesAcrossAdmittedActions() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("reachedDeleteConflictRetainsDirtyDependentBasesAcrossAdmittedActions")) return@runTest
        try {
            for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration(action)))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, DeferConflictResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val stable = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_row_state" to "key_json<>'{\"id\":\"P\"}'"))
                    val provenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")).jsonObject
                    db.execSQL("UPDATE nodes SET name='frozen parent' WHERE id='P'")
                    db.execSQL("UPDATE nodes SET parent_id='Q' WHERE id='C'")
                    db.execSQL("UPDATE nodes SET name='dirty full grandchild' WHERE id='D'")
                    val dirtyFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
                    val dirtyQuery = "SELECT json_group_array($dirtyFields) FROM (SELECT * FROM _sync_dirty_rows WHERE key_json<>'{\"id\":\"P\"}' ORDER BY key_json)"
                    val expectedDirty = Json.parseToJsonElement(scalarText(db, dirtyQuery))
                    val expectedBusiness = Json.parseToJsonElement("""[{"id":"C","name":"child","parent_id":"Q"},{"id":"D","name":"dirty full grandchild","parent_id":"C"},{"id":"Q","name":"other","parent_id":null}]""")
                    // The known desired reparent makes the delete admissible.
                    // The application trigger then damages a dirty descendant;
                    // reached conflict must restore complete L without rewriting B.
                    db.execSQL("CREATE TRIGGER conflict_damage_d AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='D'; END")
                    server.conflictOverride = { row, _, _ -> if (row.key == mapOf("id" to "P")) PushConflictDetails(row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, true, null) else null }
                    assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                    val selected = requireNotNull(client.pendingConflict().getOrThrow())
                    assertEquals(3L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    val frozenWire = server.requestBodies.last { it.first == "/sync/push" }.second
                    val rejectedStages = databaseStateVector(db.connection(), listOf("nodes"))
                    client.resolveConflict(selected, MergeResult.AcceptServer).getOrThrow()
                    assertNull(client.pendingConflict().getOrThrow())
                    assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "$action full dirty/clean business survivors")
                    assertEquals(expectedDirty, Json.parseToJsonElement(scalarText(db, dirtyQuery)), "$action exact dirty identity/op/version/historical B/provenance/L")
                    val after = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_row_state" to "key_json<>'{\"id\":\"P\"}'"))
                    assertEquals(stable.getValue("_sync_row_state"), after.getValue("_sync_row_state"), "$action every untouched A field")
                    val tombstoneProvenance = JsonObject(provenance + mapOf("origin" to JsonPrimitive("explicit_conflict"), "bundle_sequence" to JsonNull, "authoritative_version" to JsonPrimitive(2)))
                    assertEquals(tombstoneProvenance, Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")))
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}' AND server_kind='deleted' AND row_version=2 AND server_payload IS NULL AND relationships_known=1"))
                    val edges = Json.parseToJsonElement("""[{"view":"desired","child":"C","parent":"Q"},{"view":"desired","child":"D","parent":"C"},{"view":"server","child":"C","parent":"P"},{"view":"server","child":"D","parent":"C"}]""")
                    assertEquals(edges, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('view',view,'child',json_extract(child_key,'$.id'),'parent',json_extract(parent_key,'$.id'))) FROM (SELECT * FROM _sync_relationships ORDER BY view,child_key)")), "$action exact historical server and current desired edge identities")
                    val serverOnly = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_relationships" to "view='server'"))
                    assertEquals(stable.getValue("_sync_relationships"), serverOnly.getValue("_sync_relationships"), "$action complete historical edge schemas, keys, FK columns and revisions")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships WHERE child_schema<>'main' OR child_table<>'nodes' OR parent_schema<>'main' OR parent_table<>'nodes' OR fk_column<>'parent_id' OR owner_revision<=0 OR owner_revision>(SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1) AND view='desired'"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships e JOIN _sync_row_state a ON a.schema_name=e.child_schema AND a.table_name=e.child_table AND a.key_json=e.child_key WHERE e.view='server' AND e.owner_revision<>a.server_revision"))
                    assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")) assertTrue(after.getValue(table).isEmpty(), "$action apply cleanup $table")
                    // Rejected outbound planning remains a charged durable
                    // owner until the next guarded push clears its stale plan.
                    for (table in listOf("_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertEquals(rejectedStages.getValue(table), after.getValue(table), "$action exact retained old plan $table")
                    val retained = OversqliteWorkspace(db.connection(), config()).usage(applyUnit("author-retained", 2, action))
                    assertTrue(retained.entries <= config().maxSyncWorkspaceEntries)
                    assertTrue(retained.bytes <= config().maxSyncWorkspaceBytes)
                    for (table in listOf("_sync_source_state", "_sync_attachment_state", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) assertEquals(stable.getValue(table), after.getValue(table), "$action source/checkpoint/recovery identity does not publish a rejected push")
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(frozenWire, server.requestBodies.last { it.first == "/sync/push" }.second)
                    assertEquals(2, server.pushRequestCount, "$action explicit resolution performs no push")
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} reachedDeleteConflictRetainsDirtyDependentBasesAcrossAdmittedActions $action/resolved")
                    client.pushPending().getOrThrow()
                    assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "$action one later public push retains complete L")
                    val final = databaseStateVector(db.connection(), listOf("nodes"))
                    for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(final.getValue(table).isEmpty(), "$action later guarded push reclaims $table")
                    for (id in listOf("C", "D")) {
                        assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                        val originalDirty = expectedDirty.jsonArray.single { it.jsonObject.getValue("key").jsonPrimitive.content == "{\"id\":\"$id\"}" }.jsonObject
                        assertEquals(Json.parseToJsonElement(originalDirty.getValue("L").jsonPrimitive.content), Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$action full published survivor A $id")
                    }
                    assertEquals(3, server.pushRequestCount)
                    assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} reachedDeleteConflictRetainsDirtyDependentBasesAcrossAdmittedActions $action/reclaimed-and-pushed")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun pullAndSnapshotActionSurvivors() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("pullAndSnapshotActionSurvivors")) return@runTest
        try {
            for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) {
                for (operation in listOf("pull", "snapshot")) {
                    val tag = "$action/$operation"
                    val server = MockSyncServer(
                        registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                        generationResponses = mapOf(1L to nodeGeneration(action)),
                    )
                    val db = newCoordinatedDb()
                    val http = server.newHttpClient()
                    try {
                        db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                        val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
                        client.attach("user-1").getOrThrow()
                        db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                        client.pushPending().getOrThrow()
                        val before = databaseStateVector(db.connection(), listOf("nodes"))
                        // Reached application SQL may damage a clean descendant; final
                        // installation must restore its complete payload and relationship.
                        db.execSQL("CREATE TRIGGER damage_d AFTER INSERT ON nodes WHEN NEW.id='Q' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='D'; END")
                        val rows = listOf(remoteNode("D", "C"), remoteNode("C", "Q"), remoteNode("Q", null)) +
                            BundleRow("main", "nodes", mapOf("id" to "P"), "DELETE", 2, null)
                        server.addRemoteBundleForTest(rows)
                        if (operation == "pull") client.pullToStable().getOrThrow() else client.rebuild().getOrThrow()
                        assertEquals(Json.parseToJsonElement("""[{"id":"C","name":"S-C","parent_id":"Q"},{"id":"D","name":"S-D","parent_id":"C"},{"id":"Q","name":"S-Q","parent_id":null}]"""),
                            Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "$tag complete business payload")
                        for (row in rows.filter { it.op != "DELETE" }) {
                            val id = row.key.getValue("id")
                            val expectedTyped = buildJsonObject {
                                for ((column, value) in requireNotNull(row.payload).jsonObject) {
                                    putJsonObject(column) {
                                        put("storage", if (value == JsonNull) "null" else "text")
                                        put("value", value)
                                    }
                                }
                            }
                            assertEquals(expectedTyped, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$tag exact typed A $id")
                            assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"), tag)
                            assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"), tag)
                            val provenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")).jsonObject
                            assertEquals(2, provenance.getValue("authoritative_version").jsonPrimitive.int, tag)
                            assertEquals(2, provenance.getValue("bundle_sequence").jsonPrimitive.int, tag)
                            assertEquals("user-1", provenance.getValue("account_scope").jsonObject.getValue("account_id").jsonPrimitive.content, tag)
                        }
                        assertEquals(Json.parseToJsonElement("""[{"view":"server","child":"C","fk":"parent_id","parent":"Q"},{"view":"server","child":"D","fk":"parent_id","parent":"C"}]"""),
                            Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('view',view,'child',json_extract(child_key,'$.id'),'fk',fk_column,'parent',json_extract(parent_key,'$.id'))) FROM (SELECT * FROM _sync_relationships ORDER BY child_key)")), "$tag exact relationship inventory")
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships e JOIN _sync_row_state a ON a.schema_name=e.child_schema AND a.table_name=e.child_table AND a.key_json=e.child_key WHERE e.owner_revision<>a.server_revision"), "$tag edge owner revision")
                        val after = databaseStateVector(db.connection(), listOf("nodes"))
                        for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_managed_tables")) {
                            assertEquals(before.getValue(table), after.getValue(table), "$tag complete preserved $table")
                        }
                        for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) {
                            assertTrue(after.getValue(table).isEmpty(), "$tag final cleanup $table")
                        }
                        assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), tag)
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"), tag)
                        assertEquals(1, server.pushRequestCount, "$tag remote install performs no extra push")
                        println("ORDINARY_SCENARIO ${webRuntimeKind()} pullAndSnapshotActionSurvivors $tag")
                    } finally { http.close(); db.close() }
                }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun versionSkippedSurvivorRetainsCompleteAuthoritativeState() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("versionSkippedSurvivorRetainsCompleteAuthoritativeState")) return@runTest
        for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) {
            val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"), generationResponses = mapOf(1L to nodeGeneration(action)))
            val db = newCoordinatedDb()
            val http = server.newHttpClient()
            try {
                db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
                client.attach("user-1").getOrThrow()
                db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                client.pushPending().getOrThrow()
                // Seed an already authoritative newer D without generating local intent.
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                db.execSQL("UPDATE nodes SET name='newer grandchild' WHERE id='D'")
                db.execSQL("UPDATE _sync_row_state SET row_version=3,server_payload=json_set(server_payload,'$.name.value','newer grandchild'),server_provenance=json_set(server_provenance,'$.authoritative_version',3) WHERE key_json='{\"id\":\"D\"}'")
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                db.execSQL("CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END")
                val projection = mapOf("nodes" to "id IN ('D','Q')", "_sync_row_state" to "key_json IN ('{\"id\":\"D\"}','{\"id\":\"Q\"}')", "_sync_relationships" to "child_key IN ('{\"id\":\"D\"}','{\"id\":\"Q\"}')")
                val before = databaseStateVector(db.connection(), listOf("nodes"), projection)
                val attachment = OversqliteAttachmentStateStore(db.connection()).loadState()
                val source = OversqliteSourceStateStore(db.connection()).loadState(attachment.currentSourceId)
                val operation = OversqliteOperationStateStore(db.connection()).loadState()
                val pushes = server.pushRequestCount
                val pulls = server.pullRequestCount
                val baselineRevision = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
                val descendantRevision = scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"D\"}'")
                val provenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"Q\"}'")).jsonObject
                val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                server.addRemoteBundleForTest(nodeEffects() + remoteNode("D", "C"))
                client.pullToStable().getOrThrow()
                val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                val after = databaseStateVector(db.connection(), listOf("nodes"), projection)
                for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_managed_tables", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts")) {
                    assertEquals(before.getValue(table), after.getValue(table), "version-skipped complete typed $table")
                }
                assertEquals(Json.parseToJsonElement("""[{"id":"C","name":"child","parent_id":"Q"},{"id":"D","name":"newer grandchild","parent_id":"C"},{"id":"Q","name":"other","parent_id":null}]"""),
                    Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "$action complete final business payloads")
                assertEquals(attachment.copy(lastBundleSeqSeen = 2), OversqliteAttachmentStateStore(db.connection()).loadState())
                assertEquals(source, OversqliteSourceStateStore(db.connection()).loadState(attachment.currentSourceId))
                assertEquals(operation, OversqliteOperationStateStore(db.connection()).loadState())
                assertEquals(pushes, server.pushRequestCount); assertEquals(pulls + 1, server.pullRequestCount)
                assertEquals(baselineRevision + 2, scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state"), "only P and C replace authority; older D is skipped")
                for ((id, parent, revision) in listOf(Triple("C", "Q", baselineRevision + 2), Triple("D", "C", descendantRevision))) {
                    assertEquals(revision, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                    assertEquals(parent, scalarText(db, "SELECT parent_id FROM nodes WHERE id='$id'"))
                }
                for (id in listOf("P", "C")) {
                    val predicate = "key_json='{\"id\":\"$id\"}'"
                    assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                    assertEquals(if (id == "P") "deleted" else "live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                    assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                    assertEquals("main", scalarText(db, "SELECT schema_name FROM _sync_row_state WHERE $predicate"))
                    assertEquals("nodes", scalarText(db, "SELECT table_name FROM _sync_row_state WHERE $predicate"))
                    assertEquals(JsonObject(provenance + mapOf("origin" to JsonPrimitive("pull"), "bundle_sequence" to JsonPrimitive(2), "authoritative_version" to JsonPrimitive(2))),
                        Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "$action every changed-authority provenance field $id")
                    if (id == "P") {
                        assertEquals(1L, scalarLong(db, "SELECT server_payload IS NULL FROM _sync_row_state WHERE $predicate"))
                        assertEquals(baselineRevision + 1, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                    } else assertEquals(Json.parseToJsonElement("""{"id":{"storage":"text","value":"C"},"name":{"storage":"text","value":"child"},"parent_id":{"storage":"text","value":"Q"}}"""),
                        Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE $predicate")))
                    val publishedAt = scalarText(db, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                    assertTrue(publishedAt >= publicationStart && publishedAt <= publicationEnd)
                    for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_provenance", "updated_at"))
                        assertEquals("text", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    for (column in listOf("row_version", "relationships_known", "server_revision"))
                        assertEquals("integer", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    assertEquals(if (id == "P") "null" else "text", scalarText(db, "SELECT typeof(server_payload) FROM _sync_row_state WHERE $predicate"))
                }
                val expectedEdges = JsonArray(listOf(Triple("C", "Q", baselineRevision + 2), Triple("D", "C", descendantRevision)).map { (child, parent, revision) ->
                    buildJsonObject {
                        put("view", "server"); put("child_schema", "main"); put("child_table", "nodes"); put("child_key", "{\"id\":\"$child\"}"); put("fk_column", "parent_id")
                        put("parent_schema", "main"); put("parent_table", "nodes"); put("parent_key", "{\"id\":\"$parent\"}"); put("owner_revision", revision)
                    }
                })
                assertEquals(expectedEdges, Json.parseToJsonElement(scalarText(db,
                    "SELECT json_group_array(json(edge)) FROM (SELECT json_object('view',view,'child_schema',child_schema,'child_table',child_table,'child_key',child_key,'fk_column',fk_column,'parent_schema',parent_schema,'parent_table',parent_table,'parent_key',parent_key,'owner_revision',owner_revision) AS edge FROM _sync_relationships ORDER BY child_key,view)")), "$action complete edge inventory; no desired or stale-parent edge")
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='P'"))
                assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
                assertEquals("child", scalarText(db, "SELECT name FROM nodes WHERE id='C'"))
                assertEquals("other", scalarText(db, "SELECT name FROM nodes WHERE id='Q'"))
                assertEquals(3L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                for (table in listOf("_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_snapshot_stage")) assertTrue(after.getValue(table).isEmpty(), "version-skipped cleanup $table")
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                println("ORDINARY_SCENARIO ${webRuntimeKind()} versionSkippedSurvivorRetainsCompleteAuthoritativeState ${action.replace(' ', '_')}/pull")
            } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
        }
    }

    @Test fun chainedRekeyPreservesDependentAndFrozenHistoricalBase() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("chainedRekeyPreservesDependentAndFrozenHistoricalBase")) return@runTest
        for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) {
            val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"), generationResponses = mapOf(1L to nodeGeneration(action)))
            val db = newCoordinatedDb()
            val http = server.newHttpClient()
            try {
                db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
                client.attach("user-1").getOrThrow()
                db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                client.pushPending().getOrThrow()
                val projection = mapOf("nodes" to "id IN ('D','Q')", "_sync_row_state" to "key_json IN ('{\"id\":\"D\"}','{\"id\":\"Q\"}')", "_sync_relationships" to "child_key IN ('{\"id\":\"D\"}','{\"id\":\"Q\"}')")
                val descendantBefore = databaseStateVector(db.connection(), listOf("nodes"), projection)
                val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
                val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
                db.connection().transaction(TransactionMode.IMMEDIATE) {
                    db.execSQL("UPDATE nodes SET id='R' WHERE id='P'")
                    db.execSQL("UPDATE nodes SET parent_id='R' WHERE id='C'")
                }
                client.pushPending().getOrThrow()
                val intermediatePayload = scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"R\"}'")
                val intermediateProvenance = scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"R\"}'")
                assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"R\"}'"))
                db.connection().transaction(TransactionMode.IMMEDIATE) {
                    db.execSQL("UPDATE nodes SET id='S' WHERE id='R'")
                    db.execSQL("UPDATE nodes SET parent_id='S' WHERE id='C'")
                }
                assertEquals("DELETE", scalarText(db, "SELECT op FROM _sync_dirty_rows WHERE key_json='{\"id\":\"R\"}'"))
                assertEquals(intermediatePayload, scalarText(db, "SELECT base_payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"R\"}'"), "published intermediate deletion keeps exact typed B")
                assertEquals(intermediateProvenance, scalarText(db, "SELECT base_provenance FROM _sync_dirty_rows WHERE key_json='{\"id\":\"R\"}'"), "published intermediate deletion keeps exact provenance")
                assertEquals("absent", scalarText(db, "SELECT base_kind FROM _sync_dirty_rows WHERE key_json='{\"id\":\"S\"}'"))
                server.beforePushCommit = { throw CancellationException("hold chained rekey before remote mutation") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val frozen = databaseStateVector(db.connection(), listOf("nodes"), projection)
                val frozenBody = server.requestBodies.last { it.first == "/sync/push" }.second
                val frozenHash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                val frozenNext = scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state")
                assertEquals(frozenNext, Json.parseToJsonElement(frozenBody).jsonObject.getValue("source_bundle_id").jsonPrimitive.long, "frozen request owns the current source sequence")
                assertEquals(3L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"), "R deletion, S insertion and C reparent frozen together")
                assertEquals(3, server.pushRequestCount, "seed, published intermediate, and refused precommit request")
                val frozenUsage = OversqliteWorkspace(db.connection(), config()).usage(applyUnit("author-rekey-retained", 0, ""))
                assertTrue(frozenUsage.entries > 1 && frozenUsage.bytes > 0, "frozen intent retains its actual charged plan")
                assertTrue(frozenUsage.entries <= config().maxSyncWorkspaceEntries && frozenUsage.bytes <= config().maxSyncWorkspaceBytes)
                val frozenRows = OversqliteOutboxStateStore(db.connection()).loadRows()
                val baselineRevision = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
                val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
                val conflictQuery = "SELECT json_object(${conflictColumns.joinToString(",") { "'$it',$it" }}) FROM _sync_conflict_state"
                val frozenConflict = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
                val frozenP = databaseStateVector(db.connection(), listOf("nodes"), mapOf("_sync_row_state" to "key_json='{\"id\":\"P\"}'")).getValue("_sync_row_state")
                val descendantRevision = scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"D\"}'")
                val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                server.beforePushCommit = null
                client.pushPending().getOrThrow()
                val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                assertEquals(frozenBody, server.requestBodies.last { it.first == "/sync/push" }.second, "same-lifetime replay uses exact frozen wire bytes")
                assertEquals(4, server.pushRequestCount, "one replay request follows the failed precommit request")
                assertEquals(frozenNext + 1, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"), "confirmation advances the frozen source sequence exactly once")
                assertTrue(frozenHash.matches(Regex("[0-9a-f]{64}")))
                assertEquals(source, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                assertEquals(sourceBefore.copy(nextSourceBundleId = sourceBefore.nextSourceBundleId + 2), OversqliteSourceStateStore(db.connection()).loadState(source))
                assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 3), OversqliteAttachmentStateStore(db.connection()).loadState())
                assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
                assertEquals(Json.parseToJsonElement("""[{"id":"C","name":"child","parent_id":"S"},{"id":"D","name":"grandchild","parent_id":"C"},{"id":"Q","name":"other","parent_id":null},{"id":"S","name":"parent","parent_id":null}]"""),
                    Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "complete settled rekey payload")
                val after = databaseStateVector(db.connection(), listOf("nodes"), projection)
                for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_managed_tables", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts")) {
                    assertEquals(descendantBefore.getValue(table), frozen.getValue(table), "frozen rekey preserves D $table")
                    assertEquals(descendantBefore.getValue(table), after.getValue(table), "settled rekey preserves D $table")
                }
                assertEquals("deleted", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"R\"}'"))
                assertEquals("live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"S\"}'"))
                assertEquals(frozenP, databaseStateVector(db.connection(), listOf("nodes"), mapOf("_sync_row_state" to "key_json='{\"id\":\"P\"}'")).getValue("_sync_row_state"), "first published deletion remains byte-identical authority")
                assertEquals(baselineRevision + 3, scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state"), "three frozen effects install exactly once")
                assertEquals(JsonObject(frozenConflict + ("baseline_revision" to JsonPrimitive(baselineRevision + 3))),
                    Json.parseToJsonElement(scalarText(db, conflictQuery)), "all conflict controls preserve the frozen identity; only three authority installs advance its revision")
                assertEquals(6L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"), "P/R tombstones and C/D/Q/S authority are the complete key inventory")
                val provenance = Json.parseToJsonElement(intermediateProvenance).jsonObject
                for (id in listOf("R", "S", "C")) {
                    val predicate = "key_json='{\"id\":\"$id\"}'"
                    val row = frozenRows.single { it.keyJson == "{\"id\":\"$id\"}" }
                    assertEquals(baselineRevision + row.rowOrdinal + 1, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE $predicate"), "committed effect follows the exact frozen ordinal")
                    assertEquals("main", scalarText(db, "SELECT schema_name FROM _sync_row_state WHERE $predicate"))
                    assertEquals("nodes", scalarText(db, "SELECT table_name FROM _sync_row_state WHERE $predicate"))
                    assertEquals(3L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                    assertEquals(if (id == "R") "deleted" else "live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                    assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                    assertEquals(JsonObject(provenance + mapOf("origin" to JsonPrimitive("settlement"), "bundle_sequence" to JsonPrimitive(3), "authoritative_version" to JsonPrimitive(3))),
                        Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "$action every rekey authority provenance field $id")
                    if (id == "R") assertEquals(1L, scalarLong(db, "SELECT server_payload IS NULL FROM _sync_row_state WHERE $predicate"))
                    else {
                        fun typed(value: JsonElement) = buildJsonObject { put("storage", if (value == JsonNull) "null" else "text"); put("value", value) }
                        val payload = buildJsonObject {
                            put("id", typed(JsonPrimitive(id)))
                            put("name", typed(JsonPrimitive(if (id == "C") "child" else "parent")))
                            put("parent_id", typed(if (id == "C") JsonPrimitive("S") else JsonNull))
                        }
                        assertEquals(payload, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE $predicate")), "$action complete rekey A $id")
                    }
                    val publishedAt = scalarText(db, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                    assertTrue(publishedAt >= publicationStart && publishedAt <= publicationEnd)
                    for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_provenance", "updated_at"))
                        assertEquals("text", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    for (column in listOf("row_version", "relationships_known", "server_revision"))
                        assertEquals("integer", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    assertEquals(if (id == "R") "null" else "text", scalarText(db, "SELECT typeof(server_payload) FROM _sync_row_state WHERE $predicate"))
                }
                val childRevision = baselineRevision + frozenRows.single { it.keyJson == "{\"id\":\"C\"}" }.rowOrdinal + 1
                val expectedEdges = JsonArray(listOf(Triple("C", "S", childRevision), Triple("D", "C", descendantRevision)).map { (child, parent, revision) ->
                    buildJsonObject {
                        put("view", "server"); put("child_schema", "main"); put("child_table", "nodes"); put("child_key", "{\"id\":\"$child\"}"); put("fk_column", "parent_id")
                        put("parent_schema", "main"); put("parent_table", "nodes"); put("parent_key", "{\"id\":\"$parent\"}"); put("owner_revision", revision)
                    }
                })
                assertEquals(expectedEdges, Json.parseToJsonElement(scalarText(db,
                    "SELECT json_group_array(json(edge)) FROM (SELECT json_object('view',view,'child_schema',child_schema,'child_table',child_table,'child_key',child_key,'fk_column',fk_column,'parent_schema',parent_schema,'parent_table',parent_table,'parent_key',parent_key,'owner_revision',owner_revision) AS edge FROM _sync_relationships ORDER BY child_key,view)")), "$action every edge field; no deleted-intermediate or desired edge survives")
                assertEquals(3L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState(), "rekey clears every frozen bundle field while retaining its singleton")
                for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(after.getValue(table).isEmpty(), "rekey cleanup $table")
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                println("ORDINARY_SCENARIO ${webRuntimeKind()} chainedRekeyPreservesDependentAndFrozenHistoricalBase ${action.replace(' ', '_')}/push-replay")
            } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
        }
    }

    @Test fun setDefaultGenerationIsRefusedAtPublicAttach() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("setDefaultGenerationIsRefusedAtPublicAttach")) return@runTest
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("nodes"),
            generationResponses = mapOf(1L to nodeGeneration("SET DEFAULT")),
        )
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT DEFAULT 'Q' REFERENCES nodes(id) ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED)")
            val client = db.openClient(
                config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                http, ServerWinsResolver,
            )
            val before = databaseStateVector(db.connection(), listOf("nodes"))
            val failure = assertIs<SnapshotResponseDecodeException>(client.attach("user-1").exceptionOrNull())
            assertEquals("generation discovery", failure.operation)
            assertEquals(1, server.requestCounts["/sync/generations/1"], "invalid descriptor was fetched once")
            assertEquals(0, server.requestCounts["/sync/connect"] ?: 0, "invalid descriptor blocked connect")
            assertEquals(before, databaseStateVector(db.connection(), listOf("nodes")),
                "unsupported action preserves complete durable state")
            val transient = webRuntimeKind() == "js-node"
            if (!transient) db.close()
            val inspection = if (transient) db.connection() else db.reopenForInspection()
            try {
                assertEquals(0L, inspection.queryList("SELECT count(*) FROM nodes") { it.getLong(0) }.single())
                assertEquals(0L, inspection.queryList("SELECT count(*) FROM pragma_foreign_key_check") { it.getLong(0) }.single())
            } finally { if (!transient) inspection.close() }
            println("ORDINARY_RUNTIME_DISPOSITION ${webRuntimeKind()} setDefaultGenerationIsRefusedAtPublicAttach inspection=" +
                if (transient) "existing-transient-owner" else "durable-reopen-after-close")
            assertEquals(0, server.pushRequestCount, "unsupported action never reaches a push")
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun secondConnectionAtResponseBarrier() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("secondConnectionAtResponseBarrier")) return@runTest
        val variants = listOf(
            "edit" to "UPDATE nodes SET name='new independent L' WHERE id='D'",
            "new-dependent" to "INSERT INTO nodes VALUES('E','new child','D')",
            "reparent" to "UPDATE nodes SET parent_id='Q' WHERE id='D'",
            "parent-write" to "UPDATE nodes SET name='new independent parent' WHERE id='Q'",
        )
        try {
            for ((case, sql) in variants) {
                val server = MockSyncServer(
                    registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
                )
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(
                        config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                        http, ServerWinsResolver,
                    )
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                    server.normalizePushEffects = { nodeEffects() }
                    var barrierReached = false
                    var writerVector: Map<String, List<String>>? = null
                    val unchangedPredicates = mapOf(
                        "nodes" to "id NOT IN ('P','C')",
                        "_sync_row_state" to "key_json NOT IN ('{\"id\":\"P\"}','{\"id\":\"C\"}')",
                        "_sync_relationships" to "child_key NOT IN ('{\"id\":\"P\"}','{\"id\":\"C\"}')",
                    )
                    server.beforePushResponse = {
                        server.beforePushResponse = null
                        val runtime = webRuntimeKind()
                        val browser = runtime in setOf("js-browser", "wasm-browser")
                        if (browser) {
                            val beforeRefusal = databaseStateVector(db.connection(), listOf("nodes"))
                            assertFailsWith<dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException> {
                                db.reopenForInspection().close()
                            }
                            assertEquals(beforeRefusal, databaseStateVector(db.connection(), listOf("nodes")),
                                "refused second worker preserves every retained and business field")
                            println("ORDINARY_RUNTIME_DISPOSITION $runtime secondConnectionAtResponseBarrier $case independent-handle=REFUSED existing-owner-write=covered")
                        }
                        val writer = if (runtime == "js-node" || browser) db.connection() else db.reopenForInspection()
                        if (runtime !in setOf("js-node", "js-browser", "wasm-browser")) assertNotSame(db.connection(), writer)
                        try {
                            writer.execSQL(sql)
                            barrierReached = true
                        } finally {
                            if (writer !== db.connection()) writer.close()
                        }
                        writerVector = databaseStateVector(db.connection(), listOf("nodes"), predicates = unchangedPredicates)
                    }
                    client.pushPending().getOrThrow()
                    assertTrue(barrierReached, "$case independent DML completed while HTTP response was held")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='P'"), case)
                    assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"), case)
                    when (case) {
                        "edit" -> assertEquals("new independent L", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
                        "new-dependent" -> assertEquals("D", scalarText(db, "SELECT parent_id FROM nodes WHERE id='E'"))
                        "reparent" -> assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
                        "parent-write" -> assertEquals("new independent parent", scalarText(db, "SELECT name FROM nodes WHERE id='Q'"))
                    }
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"), "$case independent intent")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"), case)
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"), case)
                    assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"), case)
                    assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), case)
                    val after = databaseStateVector(db.connection(), listOf("nodes"), predicates = unchangedPredicates)
                    val capturedWriter = requireNotNull(writerVector)
                    for (table in listOf("nodes", "_sync_row_state", "_sync_dirty_rows", "_sync_relationships", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts")) {
                        assertEquals(capturedWriter.getValue(table), after.getValue(table), "$case complete independent L/A/B/provenance/edge/identity preserved")
                    }
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} secondConnectionAtResponseBarrier $case")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun secondConnectionReadyGuard() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("secondConnectionReadyGuard")) return@runTest
        val variants = listOf(
            "edit" to "UPDATE nodes SET name='new independent L' WHERE id='D'",
            "new-dependent" to "INSERT INTO nodes VALUES('E','new child','D')",
            "reparent" to "UPDATE nodes SET parent_id='Q' WHERE id='D'",
            "parent-write" to "UPDATE nodes SET name='new independent parent' WHERE id='Q'",
        )
        try {
            for ((case, sql) in variants) {
                val server = MockSyncServer(
                    registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
                )
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val config = config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id")))
                    val client = db.openClient(config, http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val connection = db.connection()
                    val generation = OversqliteContractStore(connection).readGeneration(1)
                    val validated = SyncRuntimeInitializer(config, TableInfoCache())
                        .validateConfig(connection).withGeneration(generation)
                    val state = RuntimeState(
                        validated, AdmittedOversqliteContract("v1", generation, testWireLimits().let {
                            OversqliteWireLimits(
                                it.maxPushRows, it.maxPushDecodedBytes, it.maxPullBundles, it.maxPullDecodedBytes,
                                it.maxSnapshotRowsPerChunk, it.maxSnapshotDecodedBytesPerChunk,
                                it.maxSnapshotRows, it.maxSnapshotLogicalBytes, it.maxRowDecodedBytes,
                            )
                        }, false),
                        "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"),
                    )
                    val resources = OversqliteClientResourceOwner(connection, http)
                    val local = OversqliteLocalStore(connection, json) { validated }
                    val unit = applyUnit("snapshot", 2, "ready-independent-writer")
                    suspend fun prepareGuard(): String {
                        val operations = OversqliteClientOperationController(config, resources, SnapshotDiagnosticsState()) {}
                        val owner = OversqliteApplySurvivors(connection, local, config, operations, json)
                        connection.transaction(TransactionMode.IMMEDIATE) {
                            owner.prepare(state, unit, 0, "snapshot", 2) { error("empty snapshot has no roots") }
                        }
                        assertEquals("ready", scalarText(db, "SELECT phase FROM _sync_apply_plan"))
                        return scalarText(db, "SELECT guard FROM _sync_apply_plan")
                    }
                    val capturedGuard = prepareGuard()
                    val runtime = webRuntimeKind()
                    val browser = runtime in setOf("js-browser", "wasm-browser")
                    if (browser) {
                        val beforeRefusal = databaseStateVector(db.connection(), listOf("nodes"))
                        assertFailsWith<dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException> {
                            db.reopenForInspection().close()
                        }
                        assertEquals(beforeRefusal, databaseStateVector(db.connection(), listOf("nodes")),
                            "refused second worker preserves every retained and business field")
                        println("ORDINARY_RUNTIME_DISPOSITION $runtime secondConnectionReadyGuard $case independent-handle=REFUSED existing-owner-write=covered")
                    }
                    val writer = if (runtime == "js-node" || browser) connection else db.reopenForInspection()
                    if (runtime !in setOf("js-node", "js-browser", "wasm-browser")) assertNotSame(connection, writer)
                    try { writer.execSQL(sql) }
                    finally { if (writer !== connection) writer.close() }
                    val before = databaseStateVector(connection, listOf("nodes"), ignoreDerivedCharges = true,
                        includeDurableStages = false)
                    assertNotEquals(capturedGuard, prepareGuard(), "$case invalidated captured ready guard")
                    assertEquals(before, databaseStateVector(connection, listOf("nodes"), ignoreDerivedCharges = true,
                        includeDurableStages = false),
                        "$case revalidation preserves complete primary state")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} secondConnectionReadyGuard $case")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun publicStaleConflictPrecedesLiveWorkspaceCapacity() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("publicStaleConflictPrecedesLiveWorkspaceCapacity")) return@runTest
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val cfg = config().copy(maxSyncWorkspaceBytes = 64L * 1024 * 1024)
            val client = db.openClient(cfg, http, DeferConflictResolver)
            client.attach("user-1").getOrThrow()
            insertUser(db, "U", "seed")
            client.pushPending().getOrThrow()
            updateUserName(db, "U", "frozen local")
            val remote = buildJsonObject { put("id", "U"); put("name", "remote user") }
            server.conflictOverride = { row, _, _ -> PushConflictDetails(row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, false, remote) }
            assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
            val current = requireNotNull(client.pendingConflict().getOrThrow())
            val connection = db.connection()
            val state = workspaceRuntimeState(db, cfg)
            val workspace = OversqliteWorkspace(connection, cfg)
            val unit = applyUnit("author-live-buffer", 2, "stale-before-capacity")
            val before = databaseStateVector(connection, listOf("users", "posts"))
            val wire = server.requestBodies.last { it.first == "/sync/push" }.second
            val initial = workspace.usage(unit)
            val requested = WorkspaceCharge(bytes = cfg.maxSyncWorkspaceBytes - initial.bytes - 64 * 1024)
            assertTrue(requested.bytes > 0)
            val token = workspace.reserve(state, unit, requested)
            try {
                assertEquals(requested, workspace.reserved(token))
                val heldUsage = workspace.usage(unit)
                assertTrue(heldUsage.bytes <= cfg.maxSyncWorkspaceBytes)
                assertTrue(cfg.maxSyncWorkspaceBytes - heldUsage.bytes < 256 * 1024)
                val held = databaseStateVector(connection, listOf("users", "posts"))
                val stale = linkedMapOf(
                    "revision" to current.copy(revision = current.revision - 1),
                    "baseline" to current.copy(baselineRevision = current.baselineRevision - 1),
                    "source" to current.copy(sourceId = "other"),
                    "bundle" to current.copy(sourceBundleId = current.sourceBundleId + 1),
                    "hash" to current.copy(canonicalRequestHash = "0".repeat(64)),
                    "generation" to current.copy(generationId = current.generationId + 1),
                    "fingerprint" to current.copy(generationFingerprint = "0".repeat(64)),
                    "schema" to current.copy(conflict = current.conflict.copy(schema = "other")),
                    "table" to current.copy(conflict = current.conflict.copy(table = "other")),
                    "key" to current.copy(conflict = current.conflict.copy(key = mapOf("id" to "other"))),
                    "empty-key" to current.copy(conflict = current.conflict.copy(key = emptyMap())),
                    "wrong-field" to current.copy(conflict = current.conflict.copy(key = mapOf("other" to "U"))),
                    "extra-field" to current.copy(conflict = current.conflict.copy(key = mapOf("id" to "U", "extra" to "x"))),
                    "op" to current.copy(conflict = current.conflict.copy(localOp = "DELETE")),
                    "base" to current.copy(conflict = current.conflict.copy(baseRowVersion = current.conflict.baseRowVersion + 1)),
                    "server-version" to current.copy(conflict = current.conflict.copy(serverRowVersion = current.conflict.serverRowVersion + 1)),
                    "deleted" to current.copy(conflict = current.conflict.copy(serverRowDeleted = true)),
                )
                for ((tag, handle) in stale) {
                    assertIs<StaleConflictResolutionException>(client.resolveConflict(handle, MergeResult.AcceptServer).exceptionOrNull(), tag)
                    assertEquals(held, databaseStateVector(connection, listOf("users", "posts")), "$tag complete primary/stage/live-reservation identity")
                    assertEquals(current, client.pendingConflict().getOrThrow())
                    assertEquals(requested, workspace.reserved(token), "$tag operation gate does not reclaim a live owner")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} publicStaleConflictPrecedesLiveWorkspaceCapacity stale/$tag")
                }
                val failure = assertIs<SyncWorkspaceLimitException>(client.resolveConflict(current, MergeResult.AcceptServer).exceptionOrNull())
                assertEquals("bytes", failure.dimension)
                assertEquals(cfg.maxSyncWorkspaceBytes, failure.limit)
                assertEquals(heldUsage.bytes, failure.used)
                assertEquals(256L * 1024 + 4096, failure.reserved)
                assertFalse(failure.resumable)
                assertEquals(SyncProgress(outboundRows = 1), failure.progress)
                assertTrue(Regex("[0-9a-f]{64}").matches(failure.unit))
                assertEquals(held, databaseStateVector(connection, listOf("users", "posts")))
                assertEquals(current, client.pendingConflict().getOrThrow())
                assertEquals(requested, workspace.reserved(token))
                assertEquals(wire, server.requestBodies.last { it.first == "/sync/push" }.second)
                assertEquals(2, server.pushRequestCount)
                assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                println("ORDINARY_SCENARIO ${webRuntimeKind()} publicStaleConflictPrecedesLiveWorkspaceCapacity current/capacity")
            } finally { workspace.release(token) }
            assertEquals(before, databaseStateVector(connection, listOf("users", "posts")), "release removes only the held reservation")
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            val bothRequested = WorkspaceCharge(
                entries = cfg.maxSyncWorkspaceEntries - initial.entries - 64,
                bytes = requested.bytes,
            )
            val bothToken = workspace.reserve(state, unit, bothRequested)
            try {
                val bothUsed = workspace.usage(unit)
                val bothHeld = databaseStateVector(connection, listOf("users", "posts"))
                assertTrue(65 > cfg.maxSyncWorkspaceEntries - bothUsed.entries)
                assertTrue(256 * 1024 + 4096 > cfg.maxSyncWorkspaceBytes - bothUsed.bytes,
                    "both entry and byte dimensions would refuse this current decision")
                val failure = assertIs<SyncWorkspaceLimitException>(client.resolveConflict(current, MergeResult.AcceptServer).exceptionOrNull())
                assertEquals("entries", failure.dimension, "entries must win when both dimensions fail")
                assertEquals(cfg.maxSyncWorkspaceEntries, failure.limit)
                assertEquals(bothUsed.entries, failure.used)
                assertEquals(65L, failure.reserved, "64 buffer entries plus one reservation descriptor")
                assertFalse(failure.resumable)
                assertEquals(SyncProgress(outboundRows = 1), failure.progress)
                assertEquals(current, client.pendingConflict().getOrThrow())
                assertEquals(bothRequested, workspace.reserved(bothToken))
                assertEquals(bothHeld, databaseStateVector(connection, listOf("users", "posts")))
                assertEquals(wire, server.requestBodies.last { it.first == "/sync/push" }.second)
                assertEquals(2, server.pushRequestCount)
                println("ORDINARY_SCENARIO ${webRuntimeKind()} publicStaleConflictPrecedesLiveWorkspaceCapacity current/entries-first")
            } finally { workspace.release(bothToken) }
            assertEquals(before, databaseStateVector(connection, listOf("users", "posts")))
            client.resolveConflict(current, MergeResult.AcceptServer).getOrThrow()
            assertNull(client.pendingConflict().getOrThrow())
            assertEquals("remote user", scalarText(db, "SELECT name FROM users WHERE id='U'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(2, server.pushRequestCount)
            println("ORDINARY_SCENARIO ${webRuntimeKind()} publicStaleConflictPrecedesLiveWorkspaceCapacity released/resolved")
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun publicPullCapacityPrecedesPendingDiscoveryAndDetach() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("publicPullCapacityPrecedesPendingDiscoveryAndDetach")) return@runTest
        try {
            for (detail in listOf("dependency_unavailable", "relationship_unknown", "historical_repair_required")) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val cfg = config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id")), maxSyncWorkspaceBytes = 64L * 1024 * 1024)
                    val client = db.openClient(cfg, http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val parent = if (detail == "dependency_unavailable") "missing" else "Q"
                    if (detail == "relationship_unknown") db.execSQL("UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{\"id\":\"Q\"}'")
                    if (detail == "historical_repair_required") {
                        db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                        db.execSQL("DELETE FROM nodes WHERE id='Q'")
                        db.execSQL("UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL,row_version=5,server_provenance=json_set(server_provenance,'$.authoritative_version',5,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{\"id\":\"Q\"}'")
                        db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                    }
                    val selectedRow = remoteNode("R", parent).copy(op = "INSERT")
                    server.addRemoteBundleForTest(listOf(selectedRow))
                    val connection = db.connection()
                    val workspace = OversqliteWorkspace(connection, cfg)
                    val unit = applyUnit("author-live-buffer", 2, detail)
                    val before = databaseStateVector(connection, listOf("nodes"))
                    val initial = workspace.usage(unit)
                    val requested = WorkspaceCharge(bytes = cfg.maxSyncWorkspaceBytes - initial.bytes - 64 * 1024)
                    val capacityUnits = mutableListOf<String>()
                    val token = workspace.reserve(workspaceRuntimeState(db, cfg), unit, requested)
                    try {
                        val held = databaseStateVector(connection, listOf("nodes"))
                        val heldUsage = workspace.usage(unit)
                        for (operation in listOf("pull", "sync", "detach")) {
                            val pulls = server.requestCounts["/sync/pull"] ?: 0
                            val failure = assertIs<SyncWorkspaceLimitException>(when (operation) {
                                "pull" -> client.pullToStable().exceptionOrNull()
                                "sync" -> client.sync().exceptionOrNull()
                                else -> client.syncThenDetach().exceptionOrNull()
                            }, "$detail/$operation must not guess a completed blocker with no sample capacity")
                            assertEquals("bytes", failure.dimension)
                            assertEquals(cfg.maxSyncWorkspaceBytes, failure.limit)
                            assertEquals(16L * 4096, failure.reserved, "first blocking reference retention charge")
                            // One descriptor (512+6*two SHA identities), one
                            // intended R row, and the two current C/D FK edges
                            // remain live with the held buffer during admission.
                            val descriptor = 512L + 6 * (64 + 64)
                            val rowCharge = 64L + 6 * (5 + 1) + requireNotNull(selectedRow.payload).toString().encodeToByteArray().size
                            val edgeCharge = 2L * (64 + 6 * (5 + 1 + 5 + 1))
                            assertEquals(heldUsage.bytes + descriptor + rowCharge + edgeCharge, failure.used,
                                "$detail/$operation simultaneous owner byte accounting")
                            capacityUnits += failure.unit
                            assertTrue(failure.used <= failure.limit)
                            assertTrue(failure.reserved > failure.limit - failure.used)
                            assertFalse(failure.resumable)
                            assertEquals(SyncProgress(), failure.progress)
                            assertTrue(Regex("[0-9a-f]{64}").matches(failure.unit))
                            assertEquals(held, databaseStateVector(connection, listOf("nodes")), "$detail/$operation full primary/stage/reservation rollback")
                            assertEquals(requested, workspace.reserved(token), "operation cannot reclaim a live buffer")
                            assertEquals(pulls + 1, server.requestCounts["/sync/pull"], "one pull, no busy retry")
                            assertEquals(1, server.pushRequestCount, "no new intent or hidden push")
                            assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                            println("ORDINARY_SCENARIO ${webRuntimeKind()} publicPullCapacityPrecedesPendingDiscoveryAndDetach $detail/$operation/capacity")
                        }
                    } finally { workspace.release(token) }
                    assertEquals(before, databaseStateVector(connection, listOf("nodes")), "only the held live charge is released")
                    var pendingUnit: String? = null
                    for (operation in listOf("pull", "sync", "detach")) {
                        val pulls = server.requestCounts["/sync/pull"] ?: 0
                        val pending = assertIs<FKReconciliationRequiredException>(when (operation) {
                            "pull" -> client.pullToStable().exceptionOrNull()
                            "sync" -> client.sync().exceptionOrNull()
                            else -> client.syncThenDetach().exceptionOrNull()
                        })
                        assertEquals(detail, pending.detail)
                        assertEquals(listOf(PushDependencyRecordRef("main", "nodes", mapOf("id" to "R"))), pending.sample)
                        assertFalse(pending.hasMore)
                        assertEquals(SyncProgress(), pending.progress)
                        if (pendingUnit == null) pendingUnit = pending.unit else assertEquals(pendingUnit, pending.unit)
                        assertTrue(capacityUnits.all { it == pending.unit }, "capacity and pending bind the same complete input/context")
                        assertEquals(pulls + 1, server.requestCounts["/sync/pull"])
                        assertEquals(1, server.pushRequestCount)
                        assertEquals(before, databaseStateVector(connection, listOf("nodes")), "$detail/$operation full pending preservation after admission")
                        assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                        assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                        println("ORDINARY_SCENARIO ${webRuntimeKind()} publicPullCapacityPrecedesPendingDiscoveryAndDetach $detail/$operation/pending")
                    }
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun successfulExplicitResolutionRetainsAuthoritativeAndDesiredParentViews() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("successfulExplicitResolutionRetainsAuthoritativeAndDesiredParentViews")) return@runTest
        val parents = linkedMapOf("existing" to "Q", "self" to "C", "null" to null, "new" to "X", "cycle" to "X")
        try {
            for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) for ((topology, parent) in parents) for (decision in listOf("accept", "local", "merged")) {
                val tag = "${action.replace(' ', '-')}/$topology/$decision"
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration(action)))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, DeferConflictResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    if (topology in setOf("new", "cycle")) {
                        db.execSQL("INSERT INTO nodes VALUES('X','new local parent',${if (topology == "cycle") "'C'" else "NULL"})")
                        // A locally present parent becomes authoritative only
                        // through a confirmed ordinary publication.
                        client.pushPending().getOrThrow()
                    }
                    db.execSQL("UPDATE nodes SET name='frozen child' WHERE id='C'")
                    db.execSQL("UPDATE nodes SET name='other frozen intent' WHERE id='D'")
                    val before = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_row_state" to "key_json<>'{\"id\":\"C\"}'"))
                    val seedProvenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'")).jsonObject
                    val dirtyFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
                    val otherDirtyQuery = "SELECT json_group_array($dirtyFields) FROM (SELECT * FROM _sync_dirty_rows WHERE key_json<>'{\"id\":\"C\"}' ORDER BY key_json)"
                    val otherDirty = Json.parseToJsonElement(scalarText(db, otherDirtyQuery))
                    val remote = buildJsonObject { put("id", "C"); put("name", "selected remote child"); put("parent_id", parent?.let(::JsonPrimitive) ?: JsonNull) }
                    val merged = buildJsonObject { put("id", "C"); put("name", "chosen merged child"); put("parent_id", JsonNull) }
                    server.conflictOverride = { row, _, _ -> if (row.key == mapOf("id" to "C")) PushConflictDetails(row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, false, remote) else null }
                    assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                    val selected = requireNotNull(client.pendingConflict().getOrThrow())
                    val frozenWire = server.requestBodies.last { it.first == "/sync/push" }.second
                    client.resolveConflict(selected, when (decision) {
                        "accept" -> MergeResult.AcceptServer
                        "local" -> MergeResult.KeepLocal
                        else -> MergeResult.KeepMerged(merged)
                    }).getOrThrow()
                    assertNull(client.pendingConflict().getOrThrow())
                    fun typed(payload: JsonObject) = buildJsonObject { for ((column, field) in payload) putJsonObject(column) { put("storage", if (field == JsonNull) "null" else "text"); put("value", field) } }
                    val desired = when (decision) {
                        "accept" -> remote
                        "local" -> buildJsonObject { put("id", "C"); put("name", "frozen child"); put("parent_id", "P") }
                        else -> merged
                    }
                    val expectedBusiness = linkedMapOf(
                        "P" to buildJsonObject { put("id", "P"); put("name", "parent"); put("parent_id", JsonNull) },
                        "Q" to buildJsonObject { put("id", "Q"); put("name", "other"); put("parent_id", JsonNull) },
                        "C" to desired,
                        "D" to buildJsonObject { put("id", "D"); put("name", "other frozen intent"); put("parent_id", "C") },
                    )
                    if (topology in setOf("new", "cycle")) expectedBusiness["X"] = buildJsonObject { put("id", "X"); put("name", "new local parent"); put("parent_id", if (topology == "cycle") JsonPrimitive("C") else JsonNull) }
                    assertEquals(JsonArray(expectedBusiness.toList().sortedBy { it.first }.map { it.second }), Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT * FROM nodes ORDER BY id)")), "$tag every business value")
                    assertEquals(typed(remote), Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'")), "$tag authoritative A differs from chosen L when required")
                    val expectedProvenance = JsonObject(seedProvenance + mapOf("origin" to JsonPrimitive("explicit_conflict"), "bundle_sequence" to JsonNull, "authoritative_version" to JsonPrimitive(2)))
                    assertEquals(expectedProvenance, Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'")), "$tag every new authoritative provenance field")
                    assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'"))
                    assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'"))
                    assertEquals(otherDirty, Json.parseToJsonElement(scalarText(db, otherDirtyQuery)), "$tag unrelated frozen historical B and L requeued exactly")
                    if (decision == "accept") assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{\"id\":\"C\"}'"))
                    else {
                        assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{\"id\":\"C\"}' AND op='UPDATE' AND base_kind='present' AND base_row_version=2"))
                        assertEquals(typed(remote), Json.parseToJsonElement(scalarText(db, "SELECT base_payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"C\"}'")), "$tag B rebases from validated A, never current L")
                        assertEquals(expectedProvenance, Json.parseToJsonElement(scalarText(db, "SELECT base_provenance FROM _sync_dirty_rows WHERE key_json='{\"id\":\"C\"}'")))
                        assertEquals(typed(desired), Json.parseToJsonElement(scalarText(db, "SELECT payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"C\"}'")), "$tag exact chosen dirty L")
                    }
                    val expectedEdges = mutableListOf<Triple<String, String, String>>()
                    expectedEdges += Triple("server", "D", "C")
                    parent?.let { expectedEdges += Triple("server", "C", it) }
                    expectedEdges += Triple("desired", "D", "C")
                    if (decision == "local") expectedEdges += Triple("desired", "C", "P")
                    if (topology == "cycle") expectedEdges += Triple("server", "X", "C")
                    val edges = buildJsonArray { for ((view, child, edgeParent) in expectedEdges.sortedWith(compareBy({ it.first }, { it.second }))) add(buildJsonObject { put("view", view); put("child", child); put("fk", "parent_id"); put("parent", edgeParent) }) }
                    assertEquals(edges, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('view',view,'child',json_extract(child_key,'$.id'),'fk',fk_column,'parent',json_extract(parent_key,'$.id'))) FROM (SELECT * FROM _sync_relationships ORDER BY view,child_key)")), "$tag complete server/desired edge inventory")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships e JOIN _sync_row_state a ON a.schema_name=e.child_schema AND a.table_name=e.child_table AND a.key_json=e.child_key WHERE e.view='server' AND e.owner_revision<>a.server_revision"))
                    val after = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("_sync_row_state" to "key_json<>'{\"id\":\"C\"}'"))
                    assertEquals(before.getValue("_sync_row_state"), after.getValue("_sync_row_state"), "$tag every unaffected A field")
                    for (table in listOf("_sync_source_state", "_sync_attachment_state", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) assertEquals(before.getValue(table), after.getValue(table), "$tag source/checkpoint/recovery identity unchanged")
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    assertTrue(after.getValue("_sync_outbox_rows").isEmpty())
                    for (table in listOf("_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")) assertTrue(after.getValue(table).isEmpty(), "$tag apply cleanup")
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(frozenWire, server.requestBodies.last { it.first == "/sync/push" }.second)
                    assertEquals(if (topology in setOf("new", "cycle")) 3 else 2, server.pushRequestCount, "$tag explicit choice issues no push")
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} successfulExplicitResolutionRetainsAuthoritativeAndDesiredParentViews $tag")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun unpublishedParentAtReachedConflictDistinguishesIsolatedAndCycle() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("unpublishedParentAtReachedConflictDistinguishesIsolatedAndCycle")) return@runTest
        try {
            for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) for (topology in listOf("new", "cycle")) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration(action)))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, DeferConflictResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('X','unpublished parent',${if (topology == "cycle") "'C'" else "NULL"})")
                    db.execSQL("UPDATE nodes SET name='frozen child' WHERE id='C'")
                    db.execSQL("UPDATE nodes SET name='other frozen intent' WHERE id='D'")
                    val dirtyFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
                    val otherDirtyQuery = "SELECT json_group_array($dirtyFields) FROM (SELECT * FROM _sync_dirty_rows WHERE key_json<>'{\"id\":\"C\"}' ORDER BY key_json)"
                    val otherDirty = Json.parseToJsonElement(scalarText(db, otherDirtyQuery))
                    val seedProvenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'")).jsonObject
                    val unaffected = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("nodes" to "id<>'C'", "_sync_row_state" to "key_json<>'{\"id\":\"C\"}'"))
                    val remote = buildJsonObject { put("id", "C"); put("name", "selected remote child"); put("parent_id", "X") }
                    server.conflictOverride = { row, _, _ -> if (row.key == mapOf("id" to "C")) PushConflictDetails(row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, false, remote) else null }
                    assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                    val selected = requireNotNull(client.pendingConflict().getOrThrow())
                    val frozen = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
                    val wire = server.requestBodies.last { it.first == "/sync/push" }.second
                    assertEquals(3L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    if (topology == "new") {
                        val pending = assertIs<FKReconciliationRequiredException>(client.resolveConflict(selected, MergeResult.AcceptServer).exceptionOrNull())
                        assertEquals("dependency_unavailable", pending.detail)
                        assertTrue(Regex("[0-9a-f]{64}").matches(pending.unit))
                        assertEquals(listOf(mapOf("id" to "C")), pending.sample.map { it.key })
                        assertFalse(pending.hasMore)
                        assertEquals(SyncProgress(outboundRows = 3), pending.progress)
                        assertEquals(selected, client.pendingConflict().getOrThrow())
                        assertEquals(frozen, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), "$topology physical local parent does not become authoritative A")
                        val repeated = assertIs<FKReconciliationRequiredException>(client.resolveConflict(selected, MergeResult.AcceptServer).exceptionOrNull())
                        assertEquals(pending.unit, repeated.unit)
                        assertEquals(pending.sample, repeated.sample)
                        assertEquals(pending.progress, repeated.progress)
                        assertIs<StaleConflictResolutionException>(client.resolveConflict(selected.copy(canonicalRequestHash = "0".repeat(64)), MergeResult.AcceptServer).exceptionOrNull())
                        assertEquals(frozen, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true))
                    } else {
                        client.resolveConflict(selected, MergeResult.AcceptServer).getOrThrow()
                        assertNull(client.pendingConflict().getOrThrow())
                        assertEquals("selected remote child", scalarText(db, "SELECT name FROM nodes WHERE id='C'"))
                        assertEquals("X", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
                        assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='X'"))
                        assertEquals("unpublished parent", scalarText(db, "SELECT name FROM nodes WHERE id='X'"))
                        assertEquals("other frozen intent", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
                        assertEquals(5L, scalarLong(db, "SELECT count(*) FROM nodes"))
                        val expectedA = buildJsonObject { for ((column, field) in remote) putJsonObject(column) { put("storage", if (field == JsonNull) "null" else "text"); put("value", field) } }
                        assertEquals(expectedA, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'")))
                        assertEquals(JsonObject(seedProvenance + mapOf("origin" to JsonPrimitive("explicit_conflict"), "bundle_sequence" to JsonNull, "authoritative_version" to JsonPrimitive(2))), Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"C\"}'")))
                        assertEquals(otherDirty, Json.parseToJsonElement(scalarText(db, otherDirtyQuery)), "cycle retains complete unrelated historical B and unpublished X intent")
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{\"id\":\"C\"}'"))
                        val after = databaseStateVector(db.connection(), listOf("nodes"), predicates = mapOf("nodes" to "id<>'C'", "_sync_row_state" to "key_json<>'{\"id\":\"C\"}'"))
                        assertEquals(unaffected.getValue("nodes"), after.getValue("nodes"), "cycle preserves every unrelated business value")
                        assertEquals(unaffected.getValue("_sync_row_state"), after.getValue("_sync_row_state"), "cycle never promotes unpublished X to authoritative A")
                        for (table in listOf("_sync_source_state", "_sync_attachment_state", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) assertEquals(unaffected.getValue(table), after.getValue(table))
                        assertEquals(Json.parseToJsonElement("""[{"view":"desired","child":"D","parent":"C"},{"view":"desired","child":"X","parent":"C"},{"view":"server","child":"C","parent":"X"},{"view":"server","child":"D","parent":"C"}]"""), Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('view',view,'child',json_extract(child_key,'$.id'),'parent',json_extract(parent_key,'$.id'))) FROM (SELECT * FROM _sync_relationships ORDER BY view,child_key)")))
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships e JOIN _sync_row_state a ON a.schema_name=e.child_schema AND a.table_name=e.child_table AND a.key_json=e.child_key WHERE e.view='server' AND e.owner_revision<>a.server_revision"))
                        assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                        for (table in listOf("_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_outbox_rows")) assertTrue(after.getValue(table).isEmpty(), "cycle $table cleanup")
                    }
                    assertEquals(wire, server.requestBodies.last { it.first == "/sync/push" }.second)
                    assertEquals(2, server.pushRequestCount, "$topology no hidden parent publication or repair")
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} unpublishedParentAtReachedConflictDistinguishesIsolatedAndCycle ${action.replace(' ', '-')}/$topology/${if (topology == "new") "pending" else "resolved"}")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun reachedExplicitResolutionPreservesPendingDecisionAndGuards() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("reachedExplicitResolutionPreservesPendingDecisionAndGuards")) return@runTest
        try {
            for (detail in listOf("dependency_unavailable", "relationship_unknown", "historical_repair_required")) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, DeferConflictResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val parent = if (detail == "dependency_unavailable") "missing" else "Q"
                    if (detail == "relationship_unknown") db.execSQL("UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{\"id\":\"Q\"}'")
                    if (detail == "historical_repair_required") {
                        db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                        db.execSQL("DELETE FROM nodes WHERE id='Q'")
                        db.execSQL("UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL,row_version=5,server_provenance=json_set(server_provenance,'$.authoritative_version',5,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{\"id\":\"Q\"}'")
                        db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                    }
                    db.execSQL("UPDATE nodes SET name='frozen local parent' WHERE id='P'")
                    server.conflictOverride = { row, _, _ -> if (row.key == mapOf("id" to "P")) PushConflictDetails(
                        row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, false,
                        buildJsonObject { put("id", "P"); put("name", "selected remote parent"); put("parent_id", parent) },
                    ) else null }
                    val deferred = assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                    val selected = requireNotNull(client.pendingConflict().getOrThrow())
                    assertEquals(selected, deferred.pending)
                    val wire = server.requestBodies.last { it.first == "/sync/push" }.second
                    val frozen = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                    val pending = assertIs<FKReconciliationRequiredException>(client.resolveConflict(selected, MergeResult.AcceptServer).exceptionOrNull())
                    assertEquals(detail, pending.detail)
                    assertTrue(Regex("[0-9a-f]{64}").matches(pending.unit))
                    assertEquals(listOf(mapOf("id" to "P")), pending.sample.map { it.key })
                    assertFalse(pending.hasMore)
                    assertTrue(json.encodeToString(pending.sample).encodeToByteArray().size <= 4096)
                    assertEquals(SyncProgress(outboundRows = 1), pending.progress, "$detail direct resolution owns no prior push commit")
                    assertEquals(selected, client.pendingConflict().getOrThrow(), "$detail original decision remains pending")
                    assertEquals(frozen, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), "$detail full A/B/L/frozen/control identity retained")
                    val repeated = assertIs<FKReconciliationRequiredException>(client.resolveConflict(selected, MergeResult.AcceptServer).exceptionOrNull())
                    assertEquals(pending.unit, repeated.unit)
                    assertEquals(pending.sample, repeated.sample)
                    assertEquals(pending.progress, repeated.progress)
                    assertIs<StaleConflictResolutionException>(client.resolveConflict(selected.copy(canonicalRequestHash = "0".repeat(64)), MergeResult.AcceptServer).exceptionOrNull())
                    assertEquals(frozen, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), "$detail repeat and stale identity have no mutation")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} reachedExplicitResolutionPreservesPendingDecisionAndGuards $detail/stable")

                    // Keep the selected transition blocked while changing a
                    // relevant local guard through a dependent's newer intent.
                    db.execSQL("UPDATE nodes SET name='newer local grandchild' WHERE id='D'")
                    val changed = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
                    assertIs<StaleConflictResolutionException>(client.resolveConflict(selected, MergeResult.AcceptServer).exceptionOrNull())
                    assertEquals(changed, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), "$detail old revision loses before resolution")
                    val current = requireNotNull(client.pendingConflict().getOrThrow())
                    assertNotEquals(selected.revision, current.revision)
                    val changedPending = assertIs<FKReconciliationRequiredException>(client.resolveConflict(current, MergeResult.AcceptServer).exceptionOrNull())
                    assertEquals(detail, changedPending.detail)
                    assertNotEquals(pending.unit, changedPending.unit, "$detail relevant newer local guard changes unit")
                    assertEquals(SyncProgress(dirtyRows = 1, outboundRows = 1), changedPending.progress)
                    assertEquals(current, client.pendingConflict().getOrThrow())
                    assertEquals(changed, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), "$detail newer L plus original frozen B/decision unchanged")
                    assertEquals(wire, server.requestBodies.last { it.first == "/sync/push" }.second)
                    assertEquals(2, server.pushRequestCount, "$detail resolution performs no hidden HTTP retry")
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} reachedExplicitResolutionPreservesPendingDecisionAndGuards $detail/changed-guard")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun pendingPublicResults() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("pendingPublicResults")) return@runTest
        try {
            for (detail in listOf("dependency_unavailable", "relationship_unknown", "historical_repair_required")) {
                val server = MockSyncServer(
                    registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
                )
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(
                        config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                        http, ServerWinsResolver,
                    )
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    var parent = "missing"
                    if (detail == "relationship_unknown") {
                        parent = "Q"
                        db.execSQL("UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE json_extract(key_json,'$.id')='Q'")
                    }
                    if (detail == "historical_repair_required") {
                        parent = "Q"
                        db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                        db.execSQL("DELETE FROM nodes WHERE id='Q'")
                        db.execSQL("UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL,row_version=5,server_provenance=json_set(server_provenance,'$.authoritative_version',5,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE json_extract(key_json,'$.id')='Q'")
                        db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                    }
                    server.addRemoteBundleForTest(listOf(BundleRow(
                        "main", "nodes", mapOf("id" to "R"), "INSERT", 2,
                        buildJsonObject { put("id", "R"); put("name", "pending"); put("parent_id", parent) },
                    )))
                    val before = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
                    val pending = assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull())
                    assertEquals(detail, pending.detail)
                    assertEquals(64, pending.unit.length)
                    assertEquals(mapOf("id" to "R"), pending.sample.single().key)
                    assertTrue(pending.sample.size <= 16, "$detail bounded sample count")
                    assertTrue(json.encodeToString(pending.sample).encodeToByteArray().size <= 4096,
                        "$detail bounded serialized sample bytes")
                    assertFalse(pending.hasMore, "$detail one affected row")
                    assertEquals(SyncProgress(), pending.progress, "$detail per-call progress excludes seed")
                    assertEquals(before, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), detail)
                    assertEquals(pending.unit,
                        assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull()).unit,
                        "$detail stable unit")
                    val syncPending = assertIs<FKReconciliationRequiredException>(client.sync().exceptionOrNull())
                    assertEquals(detail, syncPending.detail)
                    assertEquals(pending.unit, syncPending.unit, "$detail sync retains pending unit")
                    assertEquals(SyncProgress(), syncPending.progress, "$detail sync has no nominal progress")
                    val detachPending = assertIs<FKReconciliationRequiredException>(client.syncThenDetach().exceptionOrNull())
                    assertEquals(detail, detachPending.detail)
                    assertEquals(pending.unit, detachPending.unit, "$detail detach retains pending unit")
                    assertEquals(SyncProgress(), detachPending.progress, "$detail blocked detach has no nominal progress")
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0, "$detail no snapshot evasion")
                    assertEquals(before, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true), detail)
                    assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"), detail)
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun pendingRetainsCommittedPrefixAndBoundedMultirowSample() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("pendingRetainsCommittedPrefixAndBoundedMultirowSample")) return@runTest
        try {
            for (affectedRows in listOf(15, 16, 17)) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"), generationResponses = mapOf(1L to nodeGeneration("CASCADE")))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val unchanged = mapOf("nodes" to "id<>'Q'", "_sync_row_state" to "key_json<>'{\"id\":\"Q\"}'")
                    val before = databaseStateVector(db.connection(), listOf("nodes"), unchanged)
                    server.addRemoteBundleForTest(listOf(remoteNode("Q", null)))
                    val blockedRows = (0 until affectedRows).map { remoteNode("R${it.toString().padStart(2, '0')}", "missing").copy(op = "INSERT", rowVersion = 3) }
                    server.addRemoteBundleForTest(blockedRows)
                    val failure = client.pullToStable().exceptionOrNull()
                    val pending = assertIs<FKReconciliationRequiredException>(failure, failure?.stackTraceToString())
                    assertEquals("dependency_unavailable", pending.detail)
                    // committedBundles counts confirmed push bundles only. The
                    // independently installed pull prefix lives in its checkpoint.
                    assertEquals(SyncProgress(), pending.progress, "$affectedRows pull prefix does not fabricate a push confirmation")
                    assertTrue(pending.sample.isNotEmpty() && pending.sample.size <= 16)
                    assertTrue(json.encodeToString(pending.sample).encodeToByteArray().size <= 4096)
                    assertTrue(pending.sample.all { ref -> ref.schema == "main" && ref.table == "nodes" && ref.key in blockedRows.map { it.key } })
                    // The current owner deliberately keeps the first blocking row,
                    // not a maximal sample. More affected rows must set hasMore.
                    assertTrue(pending.hasMore, "$affectedRows omitted blocking references are explicit")
                    assertEquals("S-Q", scalarText(db, "SELECT name FROM nodes WHERE id='Q'"))
                    assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"Q\"}'"))
                    assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id LIKE 'R%'"), "pending bundle installs no partial rows")
                    val after = databaseStateVector(db.connection(), listOf("nodes"), unchanged)
                    for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_managed_tables")) {
                        assertEquals(before.getValue(table), after.getValue(table), "$affectedRows complete state outside committed Q")
                    }
                    val repeated = assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull())
                    assertEquals(pending.unit, repeated.unit, "same blocked input and local guard after prefix")
                    assertEquals(pending.sample, repeated.sample)
                    assertEquals(pending.hasMore, repeated.hasMore)
                    assertEquals(SyncProgress(), repeated.progress, "repeat does not credit an already committed prefix")
                    assertEquals(after, databaseStateVector(db.connection(), listOf("nodes"), unchanged), "repeat has no nominal mutation")
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0, "pending never evades through snapshot")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} pendingRetainsCommittedPrefixAndBoundedMultirowSample affectedRows=$affectedRows")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun confirmedPushPendingReportsExactPrefixAndFrozenReplay() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("confirmedPushPendingReportsExactPrefixAndFrozenReplay")) return@runTest
        val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"), generationResponses = mapOf(1L to nodeGeneration("CASCADE")))
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE nodes SET name='local Q' WHERE id='Q'")
            val before = databaseStateVector(db.connection(), listOf("nodes"))
            val historicalPayload = scalarText(db, "SELECT base_payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"Q\"}'")
            val historicalProvenance = scalarText(db, "SELECT base_provenance FROM _sync_dirty_rows WHERE key_json='{\"id\":\"Q\"}'")
            server.normalizePushEffects = { proposed -> proposed + remoteNode("R", "missing").copy(op = "INSERT") }
            val failure = client.pushPending().exceptionOrNull()
            val pending = assertIs<FKReconciliationRequiredException>(failure, failure?.stackTraceToString())
            assertEquals("dependency_unavailable", pending.detail)
            assertEquals(SyncProgress(outboundRows = 1, committedBundles = 1), pending.progress,
                "confirmed remote push counts even while local settlement is pending")
            assertEquals(listOf(PushDependencyRecordRef("main", "nodes", mapOf("id" to "R"))), pending.sample)
            assertFalse(pending.hasMore)
            val frozen = databaseStateVector(db.connection(), listOf("nodes"))
            for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_source_state", "_sync_attachment_state", "_sync_managed_tables")) {
                assertEquals(before.getValue(table), frozen.getValue(table), "pending settlement preserves complete $table")
            }
            assertTrue(frozen.getValue("_sync_dirty_rows").isEmpty(), "captured dirty intent moved into the frozen outbox before HTTP")
            assertEquals(historicalPayload, scalarText(db, "SELECT base_payload FROM _sync_outbox_rows WHERE key_json='{\"id\":\"Q\"}'"), "capture preserves exact historical B")
            assertEquals(historicalProvenance, scalarText(db, "SELECT base_provenance FROM _sync_outbox_rows WHERE key_json='{\"id\":\"Q\"}'"), "capture preserves exact historical provenance")
            val request = server.requestBodies.last { it.first == "/sync/push" }.second
            val hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertEquals(2L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
            val repeated = assertIs<FKReconciliationRequiredException>(client.pushPending().exceptionOrNull())
            assertEquals(pending.unit, repeated.unit, "frozen response and local guard identify the same blocked unit")
            assertEquals(pending.sample, repeated.sample)
            assertEquals(pending.progress, repeated.progress, "one unique confirmed bundle in the new invocation; seed is excluded")
            assertEquals(request, server.requestBodies.last { it.first == "/sync/push" }.second, "pending replays exact frozen request bytes")
            assertEquals(hash, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
            assertEquals(frozen, databaseStateVector(db.connection(), listOf("nodes")), "replay preserves full primary, stage, reservation and recovery identity")
            assertEquals(3, server.pushRequestCount, "seed, confirmed pending, exact replay")
            assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0, "pending push never evades through snapshot")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun snapshotPendingKeepsPrimaryStateAndChangesGuardedContext() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("snapshotPendingKeepsPrimaryStateAndChangesGuardedContext")) return@runTest
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("nodes"),
            generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
        )
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val client = db.openClient(
                config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                http, ServerWinsResolver,
            )
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','complete parent',NULL),('C','complete child','P'),('D','complete grandchild','C')")
            client.pushPending().getOrThrow()
            server.addRemoteBundleForTest(listOf(remoteNode("R", "missing")))
            val pullPending = assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull())
            assertEquals("dependency_unavailable", pullPending.detail)
            val connection = db.connection()
            val attachmentBefore = OversqliteAttachmentStateStore(connection).loadState()
            val operationBefore = OversqliteOperationStateStore(connection).loadState()
            val primaryBefore = databaseStateVector(connection, listOf("nodes"),
                ignoreDerivedCharges = true, includeDurableStages = false)
                .filterKeys { it != "_sync_attachment_state" && it != "_sync_operation_state" }
            val pending = assertIs<FKReconciliationRequiredException>(client.rebuild().exceptionOrNull())
            assertEquals("dependency_unavailable", pending.detail)
            assertEquals(listOf(PushDependencyRecordRef("main", "nodes", mapOf("id" to "R"))), pending.sample)
            assertFalse(pending.hasMore)
            assertEquals(SyncProgress(), pending.progress)
            assertTrue(pending.unit.matches(Regex("[0-9a-f]{64}")))
            assertNotEquals(pullPending.unit, pending.unit, "snapshot input and operation context change guarded identity")
            assertEquals(primaryBefore, databaseStateVector(connection, listOf("nodes"),
                ignoreDerivedCharges = true, includeDurableStages = false)
                .filterKeys { it != "_sync_attachment_state" && it != "_sync_operation_state" },
                "all business/A/B/L/outbox/source/conflict rows remain exact")
            // Explicit rebuild marks recovery required and retains useful snapshot
            // staging in its own table. Compare every primary lifecycle field rather than
            // excluding source, checkpoint or recovery identity from the witness.
            assertEquals(attachmentBefore.copy(rebuildRequired = true),
                OversqliteAttachmentStateStore(connection).loadState())
            val operationAfter = OversqliteOperationStateStore(connection).loadState()
            assertEquals(operationBefore.copy(reason = "explicit_rebuild"), operationAfter)
            assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
            assertEquals(1L, scalarLong(db, "SELECT count(DISTINCT snapshot_id) FROM _sync_snapshot_stage"))
            assertTrue(scalarLong(db, "SELECT sum(charge) FROM _sync_snapshot_stage") > 0,
                "retained unresolved snapshot is charged")
            assertEquals("missing", scalarText(db, "SELECT json_extract(payload,'$.parent_id') FROM _sync_snapshot_stage WHERE key_json='{\"id\":\"R\"}'"))
            assertEquals(1, server.requestCounts["/sync/snapshot-sessions"], "one explicit rebuild attempt")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='R'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun pendingSampleByteBoundaryAndOversizeOmission() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("pendingSampleByteBoundaryAndOversizeOmission")) return@runTest
        val emptySampleBytes = json.encodeToString(listOf(
            PushDependencyRecordRef("main", "nodes", mapOf("id" to "")),
        )).encodeToByteArray().size
        try {
            for (sampleBytes in listOf(4095, 4096, 4097)) {
                val id = "R".repeat(sampleBytes - emptySampleBytes)
                val ref = PushDependencyRecordRef("main", "nodes", mapOf("id" to id))
                assertEquals(sampleBytes, json.encodeToString(listOf(ref)).encodeToByteArray().size)
                val server = MockSyncServer(
                    registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
                )
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(
                        config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                        http, ServerWinsResolver,
                    )
                    client.attach("user-1").getOrThrow()
                    server.addRemoteBundleForTest(listOf(BundleRow(
                        "main", "nodes", mapOf("id" to id), "INSERT", 1,
                        buildJsonObject { put("id", id); put("name", "pending"); put("parent_id", "missing") },
                    )))
                    val before = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
                    val pending = assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull())
                    assertEquals("dependency_unavailable", pending.detail)
                    assertTrue(pending.unit.matches(Regex("[0-9a-f]{64}")))
                    assertEquals(if (sampleBytes <= 4096) listOf(ref) else emptyList(), pending.sample,
                        "exact encoded sample boundary $sampleBytes")
                    assertEquals(sampleBytes > 4096, pending.hasMore)
                    assertTrue(json.encodeToString(pending.sample).encodeToByteArray().size <= 4096)
                    assertEquals(SyncProgress(), pending.progress)
                    assertEquals(pending.unit,
                        assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull()).unit)
                    assertEquals(before, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true))
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    db.execSQL("INSERT INTO nodes VALUES('Q','new independent local capture',NULL)")
                    val changedBefore = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
                    assertIs<DirtyStateRejectedException>(client.pullToStable().exceptionOrNull(),
                        "dirty-state precedence is retained ahead of constructing another pending result")
                    assertEquals(changedBefore, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true))
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun missingMandatoryParentMetadataIsIntegrityFailure() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("missingMandatoryParentMetadataIsIntegrityFailure")) return@runTest
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("nodes"),
            generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
        )
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val client = db.openClient(
                config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                http, ServerWinsResolver,
            )
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('Q','complete parent',NULL)")
            client.pushPending().getOrThrow()
            db.execSQL("DELETE FROM _sync_row_state WHERE key_json='{\"id\":\"Q\"}'")
            server.addRemoteBundleForTest(listOf(remoteNode("R", "Q")))
            val before = databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true)
            val failure = assertIs<IllegalStateException>(client.pullToStable().exceptionOrNull())
            assertContains(failure.message.orEmpty(), "mandatory parent authoritative metadata is missing")
            assertEquals(before, databaseStateVector(db.connection(), listOf("nodes"), ignoreDerivedCharges = true),
                "missing mandatory metadata is preserved for diagnosis; no partial child or checkpoint publication")
            assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun workspaceEntryByteAndWorkBoundaries() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("workspaceEntryByteAndWorkBoundaries")) return@runTest
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = db.openClient(config(), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('snapshot',1,'main','users','{\"id\":\"X\"}',1,'{}')")
            val workspace = OversqliteWorkspace(db.connection(), config())
            workspace.reconcileRetainedOwners()
            val unit = applyUnit("a3-platform-boundary", 0, "")
            val used = workspace.usage(unit)
            val before = databaseStateVector(db.connection(), listOf("users", "posts"))
            for (delta in listOf(-1L, 0L, 1L)) {
                val entries = OversqliteWorkspace(db.connection(),
                    config().copy(maxSyncWorkspaceEntries = used.entries + delta))
                if (delta < 0) assertFailsWith<SyncWorkspaceLimitException> { entries.check(unit) }
                else entries.check(unit)
                val bytes = OversqliteWorkspace(db.connection(),
                    config().copy(maxSyncWorkspaceBytes = used.bytes + delta))
                if (delta < 0) assertFailsWith<SyncWorkspaceLimitException> { bytes.check(unit) }
                else bytes.check(unit)
            }
            assertFailsWith<SyncWorkspaceLimitException> {
                workspace.add(unit, WorkspaceCharge(Long.MAX_VALUE), WorkspaceCharge(1))
            }
            for (work in listOf(16L, 17L, 18L)) {
                val owner = OversqliteClientOperationController(
                    config().copy(maxSyncWorkUnitsPerInvocation = 17),
                    OversqliteClientResourceOwner(db.connection(), http), SnapshotDiagnosticsState(),
                ) {}
                if (work > 17) {
                    val failure = assertFailsWith<SyncWorkspaceLimitException> { owner.reserveSyncWork(unit, work) }
                    assertEquals("atomic_apply_work", failure.dimension)
                    assertEquals(0L, owner.usedSyncWork())
                } else {
                    owner.reserveSyncWork(unit, work)
                    assertEquals(work, owner.usedSyncWork())
                }
            }
            assertEquals(before, databaseStateVector(db.connection(), listOf("users", "posts")),
                "entry/byte/work refusals preserve complete typed primary and retained stage vector")
            db.execSQL("UPDATE _sync_snapshot_stage SET charge=charge+1")
            assertFails { workspace.reconcileRetainedOwners() }
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation")) return@runTest
        for ((tag, lowered) in listOf(
            "entries-only" to config().copy(maxSyncWorkspaceEntries = 1),
            "bytes-only" to config().copy(maxSyncWorkspaceBytes = 1),
            "entries-and-bytes" to config().copy(maxSyncWorkspaceEntries = 1, maxSyncWorkspaceBytes = 1),
            "work" to config().copy(maxSyncWorkUnitsPerInvocation = 8),
        )) {
            var used: WorkspaceCharge? = null
            val failure = sameLifetimeRetainedRefusal(lowered) { connection, _, seedHttp ->
                seedSameLifetimeReadyClient(connection, seedHttp) {
                    connection.execSQL("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('snapshot',1,'main','users','{\"id\":\"X\"}',1,'{}')")
                    used = OversqliteWorkspace(connection, config()).usage(applyUnit("same-lifetime-lower-opening", 0, ""))
                    assertTrue(assertNotNull(used).entries > 1 && assertNotNull(used).bytes > 1)
                }
            }
            if (tag == "work") assertIs<SyncWorkLimitException>(failure)
            else {
                val limit = assertIs<SyncWorkspaceLimitException>(failure)
                val dimension = if (tag == "bytes-only") "bytes" else "entries"
                assertEquals(dimension, limit.dimension)
                assertEquals(1L, limit.limit)
            }
            println("ORDINARY_SCENARIO " + webRuntimeKind() + " lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation " + tag + "/same-lifetime-default-provider")
        }
        if (webRuntimeKind() == "js-node") {
            println("A3_RUNTIME_DISPOSITION js-node lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation durable-reopen=NOT_APPLICABLE same-lifetime=covered")
            return@runTest
        }
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = db.openClient(config(), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('snapshot',1,'main','users','{\"id\":\"X\"}',1,'{}')")
            val unit = applyUnit("a3-platform-lower-opening", 0, "")
            val used = OversqliteWorkspace(db.connection(), config()).usage(unit)
            assertTrue(used.entries > 1 && used.bytes > 1, "retained stage has a positive lower-limit boundary")
            db.close()

            suspend fun observed(): Map<String, List<String>> {
                val inspection = db.reopenForInspection()
                return try { retainedDatabaseVector(inspection) } finally { inspection.close() }
            }

            val before = observed()
            val requestsBefore = server.requestCounts.toMap()
            var migrationCalls = 0
            db.migrationFailureForTest = { migrationCalls++ }
            for ((dimension, tag, lowered) in listOf(
                Triple("entries", "entries-only", config().copy(maxSyncWorkspaceEntries = used.entries - 1)),
                Triple("bytes", "bytes-only", config().copy(maxSyncWorkspaceBytes = used.bytes - 1)),
                Triple("entries", "entries-and-bytes", config().copy(
                    maxSyncWorkspaceEntries = used.entries - 1,
                    maxSyncWorkspaceBytes = used.bytes - 1,
                )),
            )) {
                val failure = assertFailsWith<SyncWorkspaceLimitException> {
                    db.openClient(lowered, http, ServerWinsResolver)
                }
                assertEquals(dimension, failure.dimension)
                assertEquals(before, observed(), "$dimension refusal preserves complete retained census")
                assertEquals(requestsBefore, server.requestCounts, "$dimension refusal made no HTTP request")
                assertEquals(0, migrationCalls, "$dimension refusal preceded migration callback")
                println("ORDINARY_SCENARIO " + webRuntimeKind() + " lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation " + tag + "/full-retained-census")
            }
            assertFailsWith<SyncWorkLimitException> {
                db.openClient(config().copy(maxSyncWorkUnitsPerInvocation = 8), http, ServerWinsResolver)
            }
            assertEquals(before, observed(), "work refusal preserves complete retained census")
            assertEquals(requestsBefore, server.requestCounts, "work refusal made no HTTP request")
            assertEquals(0, migrationCalls, "work refusal preceded migration callback")
            println("ORDINARY_SCENARIO " + webRuntimeKind() + " lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation work/full-retained-census")
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun dependencyGroupAboveAdmittedRequestRowCapRefusesBeforeFreeze() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("dependencyGroupAboveAdmittedRequestRowCapRefusesBeforeFreeze")) return@runTest
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("nodes"),
            wireLimits = testWireLimits().copy(maxPushRows = 1),
            generationResponses = mapOf(1L to nodeGeneration("NO ACTION")),
        )
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED)")
            val client = db.openClient(
                config().copy(
                    syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id")),
                    transferTargets = OversqliteTransferTargets(maxPushRows = 1),
                ),
                http, ServerWinsResolver,
            )
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL)")
            client.pushPending().getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('C','child',NULL)")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE nodes SET parent_id='C' WHERE id='P'")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE nodes SET parent_id='P' WHERE id='C'")
            client.pushPending().getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("DELETE FROM nodes WHERE id='P'")
            db.execSQL("DELETE FROM nodes WHERE id='C'")
            db.execSQL("COMMIT")
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows WHERE op='DELETE'"))
            val before = databaseStateVector(db.connection(), listOf("nodes"))
            val failure = assertIs<PushDependencyGroupExceedsAdmittedLimitException>(
                client.pushPending().exceptionOrNull())
            assertEquals("rows", failure.dimension)
            assertEquals(2L, failure.actual)
            assertEquals(2L, failure.componentRows)
            val stagedPushOwners = setOf("_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
            val after = databaseStateVector(db.connection(), listOf("nodes"))
            assertEquals(before.filterKeys { it !in stagedPushOwners }, after.filterKeys { it !in stagedPushOwners },
                "row-cap refusal preserves primary, A/B/L, outbox, source/checkpoint and apply stages")
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_push_plan"), "guarded push plan retained")
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_push_nodes"), "both dependency nodes retained")
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_push_edges"), "cycle edges retained")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(4, server.pushRequestCount, "no request dispatched for oversized component")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun completeCyclicRequestByteBoundariesAndDiagnosticGuard() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("completeCyclicRequestByteBoundariesAndDiagnosticGuard")) return@runTest
        suspend fun exercise(label: String, byteLimit: Long?, hardRows: Int = 200): Long {
            val limits = testWireLimits().let { if (byteLimit == null) it else it.copy(maxPushDecodedBytes = byteLimit, maxPushRows = hardRows, maxRowDecodedBytes = 64) }
            val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"), wireLimits = limits,
                generationResponses = mapOf(1L to nodeGeneration("NO ACTION")))
            val db = newCoordinatedDb()
            val http = server.newHttpClient()
            try {
                db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED)")
                // Seed through an actual public snapshot. Its two-row cycle does
                // not rely on a push that would exceed the later request cap.
                server.addRemoteBundleForTest(listOf(remoteNode("P", "C").copy(rowVersion = 1), remoteNode("C", "P").copy(rowVersion = 1)))
                val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), http, ServerWinsResolver)
                client.attach("user-1").getOrThrow()
                client.rebuild().getOrThrow()
                assertEquals(2L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_relationships WHERE view='server'"))
                db.connection().transaction(TransactionMode.IMMEDIATE) { db.execSQL("DELETE FROM nodes") }
                val before = databaseStateVector(db.connection(), listOf("nodes"))
                server.beforePushCommit = { throw CancellationException("request admitted; hold before remote commit") }
                if (label in setOf("below", "row-first")) {
                    val failure = assertIs<PushDependencyGroupExceedsAdmittedLimitException>(client.pushPending().exceptionOrNull())
                    assertEquals(if (label == "row-first") "rows" else "decoded_bytes", failure.dimension)
                    assertEquals(if (label == "row-first") 1L else byteLimit, failure.limit)
                    assertEquals(2L, failure.componentRows)
                    assertEquals(SyncProgress(dirtyRows = 2), failure.progress)
                    assertEquals(0, server.pushRequestCount, "complete oversized SCC is refused before HTTP/freeze")
                    val stages = setOf("_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
                    val refused = databaseStateVector(db.connection(), listOf("nodes"))
                    assertEquals(before.filterKeys { it !in stages }, refused.filterKeys { it !in stages }, "$label complete protected vector")
                    val first = client.readPushDependencyDiagnostic(failure.diagnosticId, pageSize = 1).getOrThrow()
                    val second = client.readPushDependencyDiagnostic(failure.diagnosticId, requireNotNull(first.nextCursor), pageSize = 1).getOrThrow()
                    assertEquals(setOf(mapOf("id" to "P"), mapOf("id" to "C")), (first.identities + second.identities).map { it.key }.toSet())
                    assertEquals(1, first.identities.size); assertEquals(1, second.identities.size); assertNull(second.nextCursor)
                    val cursor = requireNotNull(first.nextCursor)
                    val tampered = cursor.dropLast(1) + if (cursor.last() == '0') "1" else "0"
                    assertIs<StalePushDependencyDiagnosticException>(client.readPushDependencyDiagnostic(failure.diagnosticId, tampered, 1).exceptionOrNull())
                    assertIs<StalePushDependencyDiagnosticException>(client.readPushDependencyDiagnostic("x".repeat(64), pageSize = 1).exceptionOrNull())
                    for (size in listOf(0, 129)) assertIs<IllegalArgumentException>(client.readPushDependencyDiagnostic(failure.diagnosticId, pageSize = size).exceptionOrNull())
                    assertEquals(refused, databaseStateVector(db.connection(), listOf("nodes")), "$label diagnostic reads/refusals retain complete state")
                    db.execSQL("INSERT INTO nodes VALUES('Q','new guarded intent',NULL)")
                    val changedGuard = databaseStateVector(db.connection(), listOf("nodes"))
                    assertIs<StalePushDependencyDiagnosticException>(client.readPushDependencyDiagnostic(failure.diagnosticId, cursor, 1).exceptionOrNull())
                    assertEquals(changedGuard, databaseStateVector(db.connection(), listOf("nodes")), "$label stale guard preserves newer capture")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} completeCyclicRequestByteBoundariesAndDiagnosticGuard $label")
                    return failure.componentDecodedBytes
                }
                // Stop before response decoding: this witness owns complete
                // request admission, not the independently bounded response.
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val request = server.requestBodies.last { it.first == "/sync/push" }.second
                val bytes = request.encodeToByteArray().size.toLong()
                assertEquals(1, server.pushRequestCount)
                assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_source_state", "_sync_attachment_state")) {
                    assertEquals(before.getValue(table), databaseStateVector(db.connection(), listOf("nodes")).getValue(table), "$label freeze preserves complete $table")
                }
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                println("ORDINARY_SCENARIO ${webRuntimeKind()} completeCyclicRequestByteBoundariesAndDiagnosticGuard $label")
                return bytes
            } finally { http.close(); db.close() }
        }
        try {
            val exact = exercise("golden", null)
            assertTrue(exact > 65, "bounded fixture admits its64-byte expanded row maximum below the complete envelope cap")
            assertEquals(exact, exercise("below", exact - 1), "diagnostic counts complete standalone envelope")
            assertEquals(exact, exercise("at", exact), "complete request fits at hard byte boundary")
            assertEquals(exact, exercise("above", exact + 1), "complete request fits below hard byte boundary")
            assertEquals(exact, exercise("row-first", exact - 1, 1), "simultaneous hard rows/bytes failure reports rows first")
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun expandedRowByteBoundariesRefuseBeforeFreeze() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("expandedRowByteBoundariesRefuseBeforeFreeze")) return@runTest
        val name = "é雪\\\"\n"
        // Independently count canonical key and payload JSON, including UTF-8,
        // quote/backslash/newline escapes. A row limit excludes request framing.
        val key = buildJsonObject { put("id", "U") }
        val payload = buildJsonObject { put("id", "U"); put("name", name) }
        val exact = key.toString().encodeToByteArray().size.toLong() + payload.toString().encodeToByteArray().size
        try {
            for ((label, limit) in listOf("below" to exact - 1, "at" to exact, "above" to exact + 1)) {
                val server = MockSyncServer(wireLimits = testWireLimits().copy(maxRowDecodedBytes = limit))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    createUsersAndPostsTables(db)
                    val client = db.openClient(config(), http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    insertUser(db, "U", name)
                    val before = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
                    val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
                    val conflictQuery = "SELECT json_object(${conflictColumns.joinToString(",") { "'$it',$it" }}) FROM _sync_conflict_state"
                    val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
                    server.beforePushCommit = { throw CancellationException("admitted row; hold before remote commit") }
                    val mutable = mutableSetOf("_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
                    if (label == "below") {
                        val failure = assertIs<PushRowExceedsAdmittedLimitException>(client.pushPending().exceptionOrNull())
                        assertEquals(exact, failure.actualBytes)
                        assertEquals(limit, failure.admittedLimit)
                        assertEquals(0, server.pushRequestCount)
                        assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    } else {
                        assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                        assertEquals(1, server.pushRequestCount)
                        val body = Json.parseToJsonElement(server.requestBodies.last { it.first == "/sync/push" }.second).jsonObject
                        val tuple = body.getValue("rows").jsonArray.single().jsonArray[2].jsonArray.single().jsonArray
                        assertEquals(JsonArray(listOf(JsonPrimitive("U"))), tuple[1])
                        assertEquals(payload, tuple[3], "$label full escaped payload")
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                        assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                        mutable += setOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_outbox_bundle")
                        // Moving exactly one dirty row to the frozen outbox
                        // advances its invalidation revision exactly once.
                        val expectedConflict = JsonObject(conflictBefore + ("dirty_revision" to JsonPrimitive(conflictBefore.getValue("dirty_revision").jsonPrimitive.long + 1)))
                        assertEquals(expectedConflict, Json.parseToJsonElement(scalarText(db, conflictQuery)), "$label every conflict control field, with one owned revision transition")
                        mutable += "_sync_conflict_state"
                    }
                    val after = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
                    assertEquals(before.filterKeys { it !in mutable }, after.filterKeys { it !in mutable }, "$label complete protected identity/payload vector")
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} expandedRowByteBoundariesRefuseBeforeFreeze $label")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun recoveryFirstDiscoveredInPull() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("recoveryFirstDiscoveredInPull")) return@runTest
        try {
            for (reason in listOf("history_pruned", "checkpoint_ahead")) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    generationResponses = mapOf(1L to nodeGeneration("CASCADE")))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    val ddl = "CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
                    val nodeConfig = config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id")), snapshotApplyBatchRows = 2)
                    db.execSQL(ddl)
                    val client = db.openClient(nodeConfig, http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
                    client.pushPending().getOrThrow()
                    val seedProvenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")).jsonObject
                    assertEquals(2L, server.addRemoteBundleForTest(listOf(BundleRow("main", "nodes", mapOf("id" to "C"), "UPDATE", 2,
                        Json.parseToJsonElement("""{"id":"C","name":"new remote child","parent_id":"Q"}""")))))
                    db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen=${if (reason == "history_pruned") 0 else 3}")
                    server.retainedBundleFloor = if (reason == "history_pruned") 1 else 0
                    val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                    assertFalse(attachmentBefore.rebuildRequired, "recovery must first be discovered by this public pull")
                    val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(attachmentBefore.currentSourceId))
                    val before = databaseStateVector(db.connection(), listOf("nodes"))
                    val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
                    fun projection(columns: List<String>) = columns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
                    fun typed(value: JsonElement, storage: String) = buildJsonObject { put("storage", storage); put("value", value) }
                    val conflictQuery = "SELECT json_object(${projection(conflictColumns)}) FROM _sync_conflict_state"
                    val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
                    val revisionBefore = conflictBefore.getValue("baseline_revision").jsonObject.getValue("value").jsonPrimitive.long
                    val requestStart = server.requestSourceIds.size
                    val pushesBefore = server.pushRequestCount
                    val chunksBefore = server.snapshotChunkRequestCount
                    val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    val report = kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { client.pullToStable().getOrThrow() }
                    val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, report.outcome)
                    assertEquals(RestoreSummary(bundleSeq = 2, rowCount = 4), report.restore)
                    assertEquals(SyncStatus(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, PendingSyncStatus(false, 0, false), 2), report.status)
                    fun verifyRestoreCounters(owner: DefaultOversqliteClient) {
                        val counters = owner.snapshotRestoreDiagnosticsForTest()
                        assertEquals(1L, counters.sessionCount); assertEquals(1L, counters.fetchedChunks)
                        assertEquals(4, counters.maxValidatedChunkRows)
                        assertEquals(2, counters.maxLiveApplyPageRows); assertEquals(2L, counters.applyPages)
                        assertEquals(4L, counters.finalStagedRows); assertEquals(4L, counters.finalAppliedRows)
                        assertTrue(counters.maxDeclaredChunkBytes > 0 && counters.maxCompletelyDecodedChunkBodyBytes > 0)
                        assertTrue(counters.maxLiveApplyPageStagedTextBytes > 0 && counters.maxLiveApplyPageStagedTextBytes <= nodeConfig.snapshotApplyBatchBytes)
                        assertEquals(0L, counters.capacityResponses); assertEquals(0L, counters.capacityRetries); assertEquals(0L, counters.capacityWaitMillis)
                        assertTrue(counters.restoreDurationMillis >= 0)
                    }
                    verifyRestoreCounters(client)
                    val requests = server.requestSourceIds.drop(requestStart)
                    assertEquals(4, requests.size, "$reason one rejected pull, one snapshot creation, one chunk and cleanup")
                    assertEquals("/sync/pull", requests[0].first); assertEquals("/sync/snapshot-sessions", requests[1].first)
                    assertTrue(requests[2].first.startsWith("/sync/snapshot-sessions/"))
                    assertEquals(requests[2].first, requests[3].first)
                    assertEquals(List(4) { attachmentBefore.currentSourceId }, requests.map { it.second }, "checkpoint recovery has no source retarget")
                    assertEquals(chunksBefore + 1, server.snapshotChunkRequestCount)
                    assertTrue(server.requestedSnapshotSourceReplacements.isEmpty())
                    assertEquals(pushesBefore, server.pushRequestCount, "recovery never sends outbound intent")
                    fun payload(id: String, name: String, parent: String?) = buildJsonObject {
                        put("id", typed(JsonPrimitive(id), "text")); put("name", typed(JsonPrimitive(name), "text"))
                        put("parent_id", typed(parent?.let(::JsonPrimitive) ?: JsonNull, if (parent == null) "null" else "text"))
                    }
                    val expected = linkedMapOf("C" to payload("C", "new remote child", "Q"), "D" to payload("D", "grandchild", "C"),
                        "P" to payload("P", "parent", null), "Q" to payload("Q", "other", null))
                    suspend fun verifySnapshot(target: PlatformTestDatabase, source: String, firstRevision: Long, start: String, end: String) {
                        assertEquals(JsonArray(expected.values.toList()), Json.parseToJsonElement(scalarText(target,
                            "SELECT json_group_array(json_object(${projection(listOf("id", "name", "parent_id"))})) FROM (SELECT * FROM nodes ORDER BY id)")), "$reason every typed business field including clean grandchild")
                        assertEquals(4L, scalarLong(target, "SELECT count(*) FROM _sync_row_state"))
                        for ((index, entry) in expected.entries.withIndex()) {
                            val (id, value) = entry
                            val predicate = "schema_name='main' AND table_name='nodes' AND key_json='{\"id\":\"$id\"}'"
                            val version = if (id == "C") 2L else 1L
                            assertEquals(value, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE $predicate")))
                            assertEquals(JsonObject(seedProvenance + mapOf("origin" to JsonPrimitive("snapshot"), "origin_source_id" to JsonPrimitive(source),
                                "bundle_sequence" to JsonPrimitive(2), "authoritative_version" to JsonPrimitive(version))),
                                Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "$reason complete provenance $id")
                            assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                            assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                            assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                            assertEquals(firstRevision + index, scalarLong(target, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                            val updated = scalarText(target, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                            assertTrue(updated >= start && updated <= end)
                            for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                                assertEquals("text", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                            for (column in listOf("row_version", "relationships_known", "server_revision"))
                                assertEquals("integer", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                        }
                        val edgeColumns = listOf("view", "child_schema", "child_table", "child_key", "fk_column", "parent_schema", "parent_table", "parent_key", "owner_revision")
                        val edges = listOf("C" to "Q", "D" to "C").map { (child, parent) -> JsonObject(mapOf(
                            "view" to JsonPrimitive("server"), "child_schema" to JsonPrimitive("main"), "child_table" to JsonPrimitive("nodes"),
                            "child_key" to JsonPrimitive("{\"id\":\"$child\"}"), "fk_column" to JsonPrimitive("parent_id"), "parent_schema" to JsonPrimitive("main"),
                            "parent_table" to JsonPrimitive("nodes"), "parent_key" to JsonPrimitive("{\"id\":\"$parent\"}"),
                            "owner_revision" to JsonPrimitive(firstRevision + if (child == "C") 0 else 1)
                        ).mapValues { (column, value) -> typed(value, if (column == "owner_revision") "integer" else "text") }) }
                        assertEquals(JsonArray(edges), Json.parseToJsonElement(scalarText(target,
                            "SELECT json_group_array(json_object(${projection(edgeColumns)})) FROM (SELECT * FROM _sync_relationships ORDER BY child_key,view)")), "$reason exact typed authority edges, no dirty/desired edges")
                        val vector = databaseStateVector(target.connection(), listOf("nodes"))
                        for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                            assertTrue(vector.getValue(table).isEmpty(), "$reason retired transient owner $table")
                        assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(target.connection()).loadState())
                        assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(target.connection()).loadBundleState())
                        assertEquals(WorkspaceCharge(1, 536), OversqliteWorkspace(target.connection(), nodeConfig).usage(applyUnit("author-pull-recovery", 0, "")))
                        assertEquals(0L, scalarLong(target, "SELECT count(*) FROM pragma_foreign_key_check"))
                    }
                    verifySnapshot(db, attachmentBefore.currentSourceId, revisionBefore + 5, publicationStart, publicationEnd)
                    assertEquals(sourceBefore, OversqliteSourceStateStore(db.connection()).loadState(attachmentBefore.currentSourceId))
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state")); assertEquals(2L, sourceBefore.nextSourceBundleId)
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 2, rebuildRequired = false, pendingInitializationId = ""),
                        OversqliteAttachmentStateStore(db.connection()).loadState())
                    assertEquals(JsonObject(conflictBefore + ("baseline_revision" to typed(JsonPrimitive(revisionBefore + 8), "integer"))),
                        Json.parseToJsonElement(scalarText(db, conflictQuery)), "$reason only four A removals and four installations advance conflict controls")
                    val recovered = databaseStateVector(db.connection(), listOf("nodes"))
                    val changed = setOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_attachment_state", "_sync_conflict_state")
                    assertEquals(before.filterKeys { it !in changed }, recovered.filterKeys { it !in changed }, "$reason every other full primary/recovery/stage identity preserved")
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} recoveryFirstDiscoveredInPull $reason/full-snapshot")
                    val peer = newCoordinatedDb()
                    val peerHttp = server.newHttpClient()
                    try {
                        peer.execSQL(ddl)
                        val peerStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        val observer = peer.openClient(nodeConfig, peerHttp, ServerWinsResolver)
                        val peerAttachment = OversqliteAttachmentStateStore(peer.connection()).loadState()
                        val peerSource = requireNotNull(OversqliteSourceStateStore(peer.connection()).loadState(peerAttachment.currentSourceId))
                        val peerOpening = requireNotNull(OversqliteOpeningStore(peer.connection()).readOrNull())
                        val openingQuery = "SELECT json_object(${projection(oversqliteOpeningColumnNames)}) FROM _sync_opening_state"
                        val openingFields = Json.parseToJsonElement(scalarText(peer, openingQuery)).jsonObject
                        val peerConflict = Json.parseToJsonElement(scalarText(peer, conflictQuery)).jsonObject
                        assertFalse(peerOpening.contractConfirmed); assertEquals("", peerOpening.targetFingerprint)
                        val connected = assertIs<AttachResult.Connected>(kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { observer.attach("user-1").getOrThrow() })
                        assertEquals(AttachOutcome.USED_REMOTE_STATE, connected.outcome)
                        verifyRestoreCounters(observer)
                        observer.pullToStable().getOrThrow()
                        val peerEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        assertNotEquals(attachmentBefore.currentSourceId, peerAttachment.currentSourceId)
                        verifySnapshot(peer, peerAttachment.currentSourceId, 1, peerStart, peerEnd)
                        assertEquals(peerSource, OversqliteSourceStateStore(peer.connection()).loadState(peerAttachment.currentSourceId))
                        assertEquals(1L, peerSource.nextSourceBundleId); assertEquals("", peerSource.replacedBySourceId)
                        assertTrue(peerSource.createdAt >= peerStart && peerSource.createdAt <= peerEnd)
                        assertEquals(1L, scalarLong(peer, "SELECT count(*) FROM _sync_source_state"))
                        assertEquals(peerAttachment.copy(attachmentEpoch = peerAttachment.attachmentEpoch + 1, bindingState = attachmentBindingAttached,
                            attachedUserId = "user-1", schemaName = attachmentBefore.schemaName, lastBundleSeqSeen = 2, rebuildRequired = false, pendingInitializationId = ""),
                            OversqliteAttachmentStateStore(peer.connection()).loadState())
                        assertEquals(JsonObject(peerConflict + ("baseline_revision" to typed(JsonPrimitive(4), "integer"))),
                            Json.parseToJsonElement(scalarText(peer, conflictQuery)), "$reason complete independent conflict controls")
                        val fingerprint = seedProvenance.getValue("generation_fingerprint").jsonPrimitive.content
                        val confirmed = peerOpening.copy(revision = peerOpening.revision + 1, targetFingerprint = fingerprint, contractConfirmed = true)
                        assertEquals(confirmed, OversqliteOpeningStore(peer.connection()).readOrNull())
                        assertEquals(JsonObject(openingFields + mapOf("revision" to typed(JsonPrimitive(confirmed.revision), "integer"),
                            "target_fingerprint" to typed(JsonPrimitive(fingerprint), "text"), "contract_confirmed" to typed(JsonPrimitive(1), "integer"),
                            "integrity_hash" to typed(JsonPrimitive(confirmed.integrityHash()), "text"))), Json.parseToJsonElement(scalarText(peer, openingQuery)),
                            "$reason full independent opening permits exact initial confirmation only")
                        val peerAfter = databaseStateVector(peer.connection(), listOf("nodes"))
                        for (table in listOf("_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                            assertEquals(recovered.getValue(table), peerAfter.getValue(table), "$reason full independent admitted fixture/contract identity $table")
                        assertEquals(pushesBefore, server.pushRequestCount)
                        assertEquals(recovered, databaseStateVector(db.connection(), listOf("nodes")), "$reason independent authority verification preserves original full state")
                        println("ORDINARY_SCENARIO ${webRuntimeKind()} recoveryFirstDiscoveredInPull $reason/independent-authority-readback")
                    } finally { peerHttp.close(); peer.close() }
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun preparedOutboxSurvivesSourceRecovery() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("preparedOutboxSurvivesSourceRecovery")) return@runTest
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs("nodes"),
            generationResponses = mapOf(1L to nodeGeneration("CASCADE")),
        )
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val client = db.openClient(
                config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))),
                http, ServerWinsResolver,
            )
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE nodes SET name='frozen L' WHERE id='P'")
            server.beforePushCommit = { throw CancellationException("freeze original before remote commit") }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            server.beforePushCommit = null
            val frozen = databaseStateVector(db.connection(), listOf("nodes"))
            assertTrue(frozen.getValue("_sync_push_plan").isNotEmpty(), "cancelled send retains its charged invalid derived plan")
            val workspace = OversqliteWorkspace(db.connection(), config())
            val workspaceUnit = applyUnit("author-prepared-recovery", 0, "")
            val frozenUsage = workspace.usage(workspaceUnit)
            assertTrue(frozenUsage.entries > 1 && frozenUsage.bytes > 536)
            assertTrue(frozenUsage.entries <= config().maxSyncWorkspaceEntries && frozenUsage.bytes <= config().maxSyncWorkspaceBytes)
            val originalRows = OversqliteOutboxStateStore(db.connection()).loadRows()
            val originalBundle = OversqliteOutboxStateStore(db.connection()).loadBundleState()
            val originalSource = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(originalSource))
            val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
            val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
            fun typedProjection(columns: List<String>) = columns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            val outboxColumns = listOf("source_bundle_id", "row_ordinal", "schema_name", "table_name", "key_json", "wire_key_json", "op", "base_row_version", "base_kind", "base_payload", "base_provenance", "local_payload", "wire_payload")
            val outboxQuery = "SELECT json_object(${typedProjection(outboxColumns)}) FROM _sync_outbox_rows"
            val frozenTypedRow = Json.parseToJsonElement(scalarText(db, outboxQuery)).jsonObject
            val bundleColumns = listOf("singleton_key", "canonical_json_contract", "state", "source_id", "source_bundle_id", "initialization_id", "canonical_request_hash", "row_count")
            val bundleQuery = "SELECT json_object(${typedProjection(bundleColumns)}) FROM _sync_outbox_bundle"
            val frozenTypedBundle = Json.parseToJsonElement(scalarText(db, bundleQuery)).jsonObject
            val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
            val conflictQuery = "SELECT json_object(${typedProjection(conflictColumns)}) FROM _sync_conflict_state"
            val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
            val revisionBefore = conflictBefore.getValue("baseline_revision").jsonObject.getValue("value").jsonPrimitive.long
            val provenanceBefore = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")).jsonObject
            // A newer snapshot changes C's complete authority/relationship while
            // P's frozen historical B/provenance and original wire stay owned.
            assertEquals(2L, server.addRemoteBundleForTest(listOf(BundleRow("main", "nodes", mapOf("id" to "C"), "UPDATE", 2,
                Json.parseToJsonElement("""{"id":"C","name":"snapshot child","parent_id":"Q"}""")))))
            val replacement = "00000000-0000-4000-8000-000000000099"
            OversqliteSourceStateStore(db.connection()).ensureSource(replacement)
            val replacementBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(replacement))
            OversqliteAttachmentStateStore(db.connection()).setRebuildRequired(true)
            OversqliteOperationStateStore(db.connection()).persistState(OversqliteOperationState(
                kind = operationKindSourceRecovery,
                reason = SourceRecoveryReason.HISTORY_PRUNED.toPersistedOperationReason(),
                replacementSourceId = replacement,
            ))
            val requestStart = server.requestSourceIds.size
            val snapshotCreatesBefore = server.requestCounts["/sync/snapshot-sessions"] ?: 0
            val snapshotChunksBefore = server.snapshotChunkRequestCount
            val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            // Keep the real MockEngine dispatcher and retirement deadline on the same clock.
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { client.rebuild().getOrThrow() }.outcome)
            val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            val recoveredRows = OversqliteOutboxStateStore(db.connection()).loadRows()
            fun typed(value: JsonElement, storage: String) = buildJsonObject { put("storage", storage); put("value", value) }
            assertEquals(originalRows.map { it.copy(sourceBundleId = 1) }, recoveredRows, "every historical B/wire/L/key/order field survives source rebind")
            assertEquals(JsonObject(frozenTypedRow + ("source_bundle_id" to typed(JsonPrimitive(1), "integer"))),
                Json.parseToJsonElement(scalarText(db, outboxQuery)), "full typed frozen row permits only source bundle rebind")
            assertEquals(originalBundle.copy(sourceId = replacement, sourceBundleId = 1), OversqliteOutboxStateStore(db.connection()).loadBundleState())
            assertEquals(JsonObject(frozenTypedBundle + mapOf("source_id" to typed(JsonPrimitive(replacement), "text"), "source_bundle_id" to typed(JsonPrimitive(1), "integer"))),
                Json.parseToJsonElement(scalarText(db, bundleQuery)), "full typed bundle permits only proven source/ID rebind")
            assertEquals(sourceBefore.copy(replacedBySourceId = replacement), OversqliteSourceStateStore(db.connection()).loadState(originalSource))
            assertEquals(replacementBefore.copy(nextSourceBundleId = 2), OversqliteSourceStateStore(db.connection()).loadState(replacement))
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
            assertEquals(attachmentBefore.copy(currentSourceId = replacement, attachmentEpoch = attachmentBefore.attachmentEpoch + 1, lastBundleSeqSeen = 2, rebuildRequired = false, pendingInitializationId = ""),
                OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
            assertEquals(JsonObject(conflictBefore + ("baseline_revision" to typed(JsonPrimitive(revisionBefore + 8), "integer"))),
                Json.parseToJsonElement(scalarText(db, conflictQuery)), "only four old A deletes and four snapshot A installs advance the control vector")
            assertEquals(snapshotCreatesBefore + 1, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
            assertEquals(snapshotChunksBefore + 1, server.snapshotChunkRequestCount)
            assertEquals(listOf(SnapshotSourceReplacement(originalSource, replacement, "history_pruned")), server.requestedSnapshotSourceReplacements)
            val snapshotRequests = server.requestSourceIds.drop(requestStart).filter { it.first.startsWith("/sync/snapshot-sessions") }
            assertEquals(3, snapshotRequests.size, "one authenticated creation, chunk and session cleanup")
            assertEquals("/sync/snapshot-sessions", snapshotRequests[0].first)
            assertTrue(snapshotRequests[1].first.startsWith("/sync/snapshot-sessions/"))
            assertEquals(snapshotRequests[1].first, snapshotRequests[2].first, "chunk and cleanup own the same snapshot session")
            assertEquals(listOf(originalSource, replacement, replacement), snapshotRequests.map { it.second }, "old source reserves replacement; new source owns chunk and cleanup")
            assertTrue(server.requestSourceIds.drop(requestStart).none { it.first == "/sync/push" }, "rebuild preserves prepared intent without sending it")
            fun payload(id: String, name: String, parent: String?) = buildJsonObject {
                putJsonObject("id") { put("storage", "text"); put("value", id) }
                putJsonObject("name") { put("storage", "text"); put("value", name) }
                putJsonObject("parent_id") { put("storage", if (parent == null) "null" else "text"); put("value", parent?.let(::JsonPrimitive) ?: JsonNull) }
            }
            val expectedA = linkedMapOf("C" to payload("C", "snapshot child", "Q"), "D" to payload("D", "grandchild", "C"), "P" to payload("P", "parent", null), "Q" to payload("Q", "other", null))
            assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
            for ((index, entry) in expectedA.entries.withIndex()) {
                val (id, expected) = entry
                val predicate = "schema_name='main' AND table_name='nodes' AND key_json='{\"id\":\"$id\"}'"
                val version = if (id == "C") 2L else 1L
                assertEquals(expected, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE $predicate")), "complete snapshot A $id")
                assertEquals(JsonObject(provenanceBefore + mapOf("origin" to JsonPrimitive("snapshot"), "bundle_sequence" to JsonPrimitive(2), "authoritative_version" to JsonPrimitive(version))),
                    Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "all snapshot provenance fields $id, including original hydration source")
                assertEquals(version, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                assertEquals("live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                assertEquals(revisionBefore + 5 + index, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                val publishedAt = scalarText(db, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                assertTrue(publishedAt >= publicationStart && publishedAt <= publicationEnd)
                for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                    assertEquals("text", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                for (column in listOf("row_version", "relationships_known", "server_revision"))
                    assertEquals("integer", scalarText(db, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
            }
            val businessProjection = listOf("id", "name", "parent_id").joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            assertEquals(JsonArray(expectedA.map { (id, authority) -> if (id == "P") payload("P", "frozen L", null) else authority }),
                Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object($businessProjection)) FROM (SELECT * FROM nodes ORDER BY id)")), "complete business L replays only the retained original intent")
            val edgeQuery = "SELECT json_group_array(json(edge)) FROM (SELECT json_object('view',view,'child_schema',child_schema,'child_table',child_table,'child_key',child_key,'fk_column',fk_column,'parent_schema',parent_schema,'parent_table',parent_table,'parent_key',parent_key,'owner_revision',owner_revision) AS edge FROM _sync_relationships ORDER BY child_key,view)"
            val expectedEdges = listOf("C" to "Q", "D" to "C").map { (child, parent) -> buildJsonObject {
                put("view", "server"); put("child_schema", "main"); put("child_table", "nodes"); put("child_key", "{\"id\":\"$child\"}")
                put("fk_column", "parent_id"); put("parent_schema", "main"); put("parent_table", "nodes"); put("parent_key", "{\"id\":\"$parent\"}")
                put("owner_revision", scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$child\"}'"))
            } }
            assertEquals(JsonArray(expectedEdges), Json.parseToJsonElement(scalarText(db, edgeQuery)), "exact server edges and no fabricated desired/dirty edges")
            val recovered = databaseStateVector(db.connection(), listOf("nodes"))
            val publishedOwners = setOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_source_state", "_sync_attachment_state", "_sync_conflict_state", "_sync_outbox_rows", "_sync_outbox_bundle")
            assertEquals(frozen.filterKeys { it !in publishedOwners }, recovered.filterKeys { it !in publishedOwners }, "all other full typed primary/recovery/stage identities remain exact")
            assertEquals(frozenUsage, workspace.usage(workspaceUnit), "snapshot recovery preserves the exact retained push-plan charge; transient hydration charges are released")
            for (table in listOf("_sync_dirty_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                assertTrue(recovered.getValue(table).isEmpty(), "retired recovery owner $table")
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            println("ORDINARY_SCENARIO ${webRuntimeKind()} preparedOutboxSurvivesSourceRecovery snapshot-retains-full-history")
            val unaffectedPredicate = mapOf("_sync_row_state" to "key_json!='{\"id\":\"P\"}'")
            val recoveredUnaffected = databaseStateVector(db.connection(), listOf("nodes"), unaffectedPredicate)
            val replayStart = server.requestSourceIds.size
            val replayBodiesBefore = server.requestBodies.count { it.first == "/sync/push" }
            val replayPublicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            val replayPublicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            val replayWire = Json.parseToJsonElement(server.requestBodies.last { it.first == "/sync/push" }.second).jsonObject
            val originalWire = Json.parseToJsonElement(server.requestBodies.filter { it.first == "/sync/push" }[replayBodiesBefore - 1].second).jsonObject
            assertEquals(JsonObject(originalWire + ("source_bundle_id" to JsonPrimitive(1))), replayWire, "complete canonical replay changes only the proven source bundle ID")
            assertEquals(listOf("/sync/push" to replacement), server.requestSourceIds.drop(replayStart))
            assertEquals(replacementBefore.copy(nextSourceBundleId = 2), OversqliteSourceStateStore(db.connection()).loadState(replacement))
            assertEquals(attachmentBefore.copy(currentSourceId = replacement, attachmentEpoch = attachmentBefore.attachmentEpoch + 1, lastBundleSeqSeen = 3, rebuildRequired = false, pendingInitializationId = ""),
                OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(JsonObject(conflictBefore + ("baseline_revision" to typed(JsonPrimitive(revisionBefore + 9), "integer"))),
                Json.parseToJsonElement(scalarText(db, conflictQuery)))
            assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
            assertEquals(payload("P", "frozen L", null), Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")))
            assertEquals(JsonObject(provenanceBefore + mapOf("origin_source_id" to JsonPrimitive(replacement), "origin" to JsonPrimitive("settlement"), "bundle_sequence" to JsonPrimitive(3), "authoritative_version" to JsonPrimitive(3))),
                Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")))
            assertEquals(3L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'"))
            assertEquals("live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'"))
            assertEquals(1L, scalarLong(db, "SELECT relationships_known FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'"))
            assertEquals(revisionBefore + 9, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'"))
            val replayPublishedAt = scalarText(db, "SELECT updated_at FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'")
            assertTrue(replayPublishedAt >= replayPublicationStart && replayPublishedAt <= replayPublicationEnd)
            val settled = databaseStateVector(db.connection(), listOf("nodes"), unaffectedPredicate)
            val replayOwners = setOf("_sync_outbox_rows", "_sync_outbox_bundle", "_sync_attachment_state", "_sync_conflict_state", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
            assertEquals(recoveredUnaffected.filterKeys { it !in replayOwners }, settled.filterKeys { it !in replayOwners }, "every complete non-P owner survives confirmation")
            for (table in listOf("_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(settled.getValue(table).isEmpty(), "confirmed replay completes charged stale-plan cleanup $table")
            assertEquals(WorkspaceCharge(1, 536), workspace.usage(workspaceUnit), "only the empty operation singleton remains charged")
            assertEquals(JsonArray(expectedEdges), Json.parseToJsonElement(scalarText(db, edgeQuery)))
            assertEquals(recovered.getValue("nodes"), settled.getValue("nodes"), "replay confirms the exact frozen business L")
            assertTrue(settled.getValue("_sync_outbox_rows").isEmpty()); assertTrue(settled.getValue("_sync_dirty_rows").isEmpty())
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            println("ORDINARY_SCENARIO ${webRuntimeKind()} preparedOutboxSurvivesSourceRecovery proven-source-replay")
            val beforeReadback = databaseStateVector(db.connection(), listOf("nodes"))
            val pushesBeforeReadback = server.pushRequestCount
            suspend fun verifyIndependentAuthority() {
                val peer = newCoordinatedDb()
                val peerHttp = server.newHttpClient()
                try {
                    peer.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL,parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val peerStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    val observer = peer.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))), peerHttp, ServerWinsResolver)
                    val peerAttachmentBefore = OversqliteAttachmentStateStore(peer.connection()).loadState()
                    val peerSourceBefore = requireNotNull(OversqliteSourceStateStore(peer.connection()).loadState(peerAttachmentBefore.currentSourceId))
                    val peerBefore = databaseStateVector(peer.connection(), listOf("nodes"))
                    val peerOpeningBefore = requireNotNull(OversqliteOpeningStore(peer.connection()).readOrNull())
                    assertEquals("", peerOpeningBefore.targetFingerprint); assertFalse(peerOpeningBefore.contractConfirmed)
                    val peerOpeningQuery = "SELECT json_object(${typedProjection(oversqliteOpeningColumnNames)}) FROM _sync_opening_state"
                    val peerOpeningFieldsBefore = Json.parseToJsonElement(scalarText(peer, peerOpeningQuery)).jsonObject
                    val peerIdentityQuery = "SELECT json_object(${typedProjection(listOf("singleton_key", "database_id", "service_id", "generation_id", "local_version", "physical_shape", "fingerprint"))}) FROM _sync_contract_identity"
                    val peerIdentityBefore = Json.parseToJsonElement(scalarText(peer, peerIdentityQuery)).jsonObject
                    assertEquals(typed(JsonPrimitive(""), "text"), peerIdentityBefore.getValue("fingerprint"))
                    assertTrue(peerBefore.getValue("_sync_admitted_contracts").isEmpty())
                    val connected = assertIs<AttachResult.Connected>(kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { observer.attach("user-1").getOrThrow() })
                    assertEquals(AttachOutcome.USED_REMOTE_STATE, connected.outcome)
                    observer.pullToStable().getOrThrow()
                    val peerEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    val peerSource = scalarText(peer, "SELECT current_source_id FROM _sync_attachment_state")
                    assertNotEquals(replacement, peerSource, "independent observer owns a separate source")
                    val expectedFinal = expectedA + ("P" to payload("P", "frozen L", null))
                    assertEquals(JsonArray(expectedFinal.values.toList()), Json.parseToJsonElement(scalarText(peer,
                        "SELECT json_group_array(json_object($businessProjection)) FROM (SELECT * FROM nodes ORDER BY id)")), "complete independent final authority business vector")
                    assertEquals(4L, scalarLong(peer, "SELECT count(*) FROM _sync_row_state"))
                    for ((index, entry) in expectedFinal.entries.withIndex()) {
                        val (id, expected) = entry
                        val predicate = "schema_name='main' AND table_name='nodes' AND key_json='{\"id\":\"$id\"}'"
                        val version = when (id) { "P" -> 3L; "C" -> 2L; else -> 1L }
                        assertEquals(expected, Json.parseToJsonElement(scalarText(peer, "SELECT server_payload FROM _sync_row_state WHERE $predicate")), "full independent A $id")
                        assertEquals(JsonObject(provenanceBefore + mapOf("origin_source_id" to JsonPrimitive(peerSource), "origin" to JsonPrimitive("snapshot"),
                            "bundle_sequence" to JsonPrimitive(3), "authoritative_version" to JsonPrimitive(version))),
                            Json.parseToJsonElement(scalarText(peer, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "full independent provenance $id")
                        assertEquals(version, scalarLong(peer, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                        assertEquals("live", scalarText(peer, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                        assertEquals(1L, scalarLong(peer, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                        assertEquals(index + 1L, scalarLong(peer, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                        val publishedAt = scalarText(peer, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                        assertTrue(publishedAt >= peerStart && publishedAt <= peerEnd)
                        for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                            assertEquals("text", scalarText(peer, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                        for (column in listOf("row_version", "relationships_known", "server_revision"))
                            assertEquals("integer", scalarText(peer, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    }
                    val peerEdges = expectedEdges.map { edge -> JsonObject(edge + ("owner_revision" to JsonPrimitive(scalarLong(peer,
                        "SELECT server_revision FROM _sync_row_state WHERE key_json='${edge.getValue("child_key").jsonPrimitive.content}'")))) }
                    assertEquals(JsonArray(peerEdges), Json.parseToJsonElement(scalarText(peer, edgeQuery)), "every independent authority edge field and no desired edges")
                    assertEquals(peerSourceBefore, OversqliteSourceStateStore(peer.connection()).loadState(peerSource))
                    assertEquals(1L, peerSourceBefore.nextSourceBundleId); assertEquals("", peerSourceBefore.replacedBySourceId)
                    assertTrue(peerSourceBefore.createdAt >= peerStart && peerSourceBefore.createdAt <= peerEnd)
                    assertEquals(1L, scalarLong(peer, "SELECT count(*) FROM _sync_source_state"))
                    assertEquals(peerAttachmentBefore.copy(attachmentEpoch = peerAttachmentBefore.attachmentEpoch + 1, bindingState = attachmentBindingAttached,
                        attachedUserId = "user-1", schemaName = attachmentBefore.schemaName, lastBundleSeqSeen = 3, rebuildRequired = false, pendingInitializationId = ""),
                        OversqliteAttachmentStateStore(peer.connection()).loadState())
                    val expectedConflict = JsonObject(conflictColumns.associateWith { column -> when (column) {
                        "singleton_key" -> typed(JsonPrimitive(1), "integer")
                        "baseline_revision" -> typed(JsonPrimitive(4), "integer")
                        "dirty_revision", "source_bundle_id", "generation_id" -> typed(JsonPrimitive(0), "integer")
                        "decision_state" -> typed(JsonPrimitive("none"), "text")
                        "local_payload_json" -> typed(JsonNull, "null")
                        else -> typed(JsonPrimitive(""), "text")
                    } })
                    assertEquals(expectedConflict, Json.parseToJsonElement(scalarText(peer, conflictQuery)), "full independent conflict controls")
                    assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(peer.connection()).loadState())
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(peer.connection()).loadBundleState())
                    val peerAfter = databaseStateVector(peer.connection(), listOf("nodes"))
                    for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                        assertTrue(peerAfter.getValue(table).isEmpty(), "independent readback retires $table")
                    val admittedFingerprint = provenanceBefore.getValue("generation_fingerprint").jsonPrimitive.content
                    val expectedOpening = peerOpeningBefore.copy(revision = peerOpeningBefore.revision + 1,
                        targetFingerprint = admittedFingerprint, contractConfirmed = true)
                    assertEquals(expectedOpening, OversqliteOpeningStore(peer.connection()).readOrNull(), "exact initial contract confirmation")
                    assertEquals(JsonObject(peerOpeningFieldsBefore + mapOf(
                        "revision" to typed(JsonPrimitive(expectedOpening.revision), "integer"),
                        "target_fingerprint" to typed(JsonPrimitive(admittedFingerprint), "text"),
                        "contract_confirmed" to typed(JsonPrimitive(1), "integer"),
                        "integrity_hash" to typed(JsonPrimitive(expectedOpening.integrityHash()), "text"))),
                        Json.parseToJsonElement(scalarText(peer, peerOpeningQuery)), "every opening column and storage type permits only initial confirmation")
                    assertEquals(JsonObject(peerIdentityBefore + ("fingerprint" to typed(JsonPrimitive(admittedFingerprint), "text"))),
                        Json.parseToJsonElement(scalarText(peer, peerIdentityQuery)), "full identity changes only admitted fingerprint")
                    assertEquals(admittedFingerprint, OversqliteContractStore(peer.connection()).readGeneration(1).fingerprint)
                    for (table in listOf("_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                        assertEquals(beforeReadback.getValue(table), peerAfter.getValue(table), "independent complete admitted contract identity $table")
                    assertEquals(1L, scalarLong(peer, "SELECT count(*) FROM _sync_admitted_contracts"))
                    assertEquals(JsonArray(listOf(JsonObject(mapOf("schema_name" to typed(JsonPrimitive("main"), "text"),
                        "table_name" to typed(JsonPrimitive("nodes"), "text"))))), Json.parseToJsonElement(scalarText(peer,
                        "SELECT json_group_array(json_object(${typedProjection(listOf("schema_name", "table_name"))})) FROM _sync_managed_tables")),
                        "exact registered fixture routing after admission")
                    assertEquals(WorkspaceCharge(1, 536), OversqliteWorkspace(peer.connection(), config()).usage(workspaceUnit))
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(pushesBeforeReadback, server.pushRequestCount, "verification never sends new intent")
                    assertEquals(beforeReadback, databaseStateVector(db.connection(), listOf("nodes")), "independent readback preserves every original primary/recovery/stage field")
                } finally { peerHttp.close(); peer.close() }
            }
            verifyIndependentAuthority()
            println("ORDINARY_SCENARIO ${webRuntimeKind()} preparedOutboxSurvivesSourceRecovery independent-authority-readback")
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun retainedLedgerTamperAndRollback() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("retainedLedgerTamperAndRollback")) return@runTest
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = db.openClient(config(), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            insertUser(db, "P", "parent")
            client.pushPending().getOrThrow()
            val unit = applyUnit("orphan", 0, "")
            val ledger = """[{"token":"$unit","unit":"$unit","context":"$unit","entries":4,"bytes":4196}]"""
            db.execSQL("UPDATE _sync_operation_state SET workspace_context='$ledger',reserved_entries=4,reserved_bytes=4196")
            try {
                db.transaction(TransactionMode.IMMEDIATE) {
                    OversqliteWorkspace(db.connection(), config()).reclaimAfterLifetimeAcquisition()
                    error("rollback witness")
                }
            } catch (_: IllegalStateException) { }
            assertEquals(ledger, scalarText(db, "SELECT workspace_context FROM _sync_operation_state"))
            db.execSQL("UPDATE _sync_operation_state SET reserved_entries=5")
            val beforeReject = databaseStateVector(db.connection(), listOf("users", "posts"))
            assertFails {
                db.transaction(TransactionMode.IMMEDIATE) {
                    OversqliteWorkspace(db.connection(), config()).reclaimAfterLifetimeAcquisition()
                }
            }
            assertEquals(beforeReject, databaseStateVector(db.connection(), listOf("users", "posts")),
                "tampered ledger rejected without primary or retained-stage mutation")
            assertEquals(ledger, scalarText(db, "SELECT workspace_context FROM _sync_operation_state"))
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun partialAndOldOpenersRefuseBeforeMigration() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("partialAndOldOpenersRefuseBeforeMigration")) return@runTest
        val oldFormatSql = createOversqliteOpeningTableSql
            .replace("CHECK(format_version=3)", "CHECK(format_version=2)")
            .replace(
                "    target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),",
                "    old_reads_available INTEGER NOT NULL CHECK(old_reads_available IN (0,1)),\n" +
                    "    target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),",
            )
        for ((case, retainedSql) in listOf(
            "partial" to "CREATE TABLE _sync_operation_state(singleton_key INTEGER PRIMARY KEY)",
            "old-opening" to oldFormatSql,
        )) {
            val failure = sameLifetimeRetainedRefusal(config()) { connection, _, _ ->
                createUsersAndPostsTables(connection)
                connection.execSQL(retainedSql)
                connection.execSQL("CREATE TABLE retained_business_sample(id INTEGER PRIMARY KEY NOT NULL, text_value TEXT NOT NULL, integer_value INTEGER NOT NULL, real_value REAL NOT NULL, blob_value BLOB NOT NULL, nullable_value TEXT)")
                connection.execSQL("CREATE INDEX retained_sample_text ON retained_business_sample(text_value)")
                connection.execSQL("INSERT INTO retained_business_sample VALUES(7,'complete retained payload',9223372036854775806,1.25,X'0001feff',NULL)")
                if (case == "partial") connection.execSQL("INSERT INTO _sync_operation_state VALUES(7)")
                assertEquals(1, retainedDatabaseVector(connection).getValue("table/retained_business_sample/rows").size)
            }
            assertIs<IllegalArgumentException>(failure)
            assertEquals(if (case == "partial") "incompatible partial oversqlite runtime; recreate this development database explicitly"
                else "unsupported coordinated opening journal format; a fresh database is required", failure.message)
            println("ORDINARY_SCENARIO " + webRuntimeKind() + " partialAndOldOpenersRefuseBeforeMigration " + case + "/same-lifetime-default-provider")
        }
        if (webRuntimeKind() == "js-node") {
            println("A3_RUNTIME_DISPOSITION js-node partialAndOldOpenersRefuseBeforeMigration durable-reopen=NOT_APPLICABLE same-lifetime=covered")
            return@runTest
        }
        try {
            for ((case, retainedSql) in listOf(
                "partial" to "CREATE TABLE _sync_operation_state(singleton_key INTEGER PRIMARY KEY)",
                "old-opening" to oldFormatSql,
            )) {
                val db = newCoordinatedDb()
                val server = MockSyncServer()
                val http = server.newHttpClient()
                try {
                    createUsersAndPostsTables(db)
                    val seeded = db.reopenForInspection()
                    try {
                        seeded.execSQL(retainedSql)
                        seeded.execSQL("CREATE TABLE retained_business_sample(id INTEGER PRIMARY KEY NOT NULL, text_value TEXT NOT NULL, integer_value INTEGER NOT NULL, real_value REAL NOT NULL, blob_value BLOB NOT NULL, nullable_value TEXT)")
                        seeded.execSQL("CREATE INDEX retained_sample_text ON retained_business_sample(text_value)")
                        seeded.execSQL("INSERT INTO retained_business_sample VALUES(7,'complete retained payload',9223372036854775806,1.25,X'0001feff',NULL)")
                        if (case == "partial") seeded.execSQL("INSERT INTO _sync_operation_state VALUES(7)")
                    } finally { seeded.close() }
                    var migrationCalls = 0
                    db.migrationFailureForTest = { migrationCalls++ }
                    val before = db.reopenForInspection()
                    val vectorBefore = try { retainedDatabaseVector(before) } finally { before.close() }
                    assertEquals(1, vectorBefore.getValue("table/retained_business_sample/rows").size, "$case mixed-storage retained row is present")
                    val requestsBefore = server.requestCounts.toMap()
                    assertFails { db.openClient(config(), http, ServerWinsResolver) }
                    assertEquals(0, migrationCalls, "$case refused before migration callback")
                    val after = db.reopenForInspection()
                    try {
                        assertEquals(vectorBefore, retainedDatabaseVector(after),
                            "$case complete retained schema/table/index/FK/typed rows and journal/schema/user versions preserved")
                        assertEquals(requestsBefore, server.requestCounts, "$case no HTTP callback/request")
                        println("ORDINARY_SCENARIO " + webRuntimeKind() + " partialAndOldOpenersRefuseBeforeMigration " + case + "/full-retained-census")
                    } finally { after.close() }
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun tamperedReadyOpenerRefusesWithoutRepair() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("tamperedReadyOpenerRefusesWithoutRepair")) return@runTest
        val refused = sameLifetimeRetainedRefusal(config()) { connection, seedServer, seedHttp ->
            seedSameLifetimeReadyClient(connection, seedHttp) { client ->
                connection.execSQL("INSERT INTO users VALUES('P','complete original parent')")
                connection.execSQL("INSERT INTO posts VALUES('C','P','complete retained clean child')")
                client.pushPending().getOrThrow()
                connection.execSQL("UPDATE users SET name='complete frozen local parent' WHERE id='P'")
                seedServer.beforePushCommit = { seedServer.beforePushCommit = null; throw CancellationException("retain real frozen same-lifetime outbox") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                assertEquals(1L, connection.queryList("SELECT count(*) FROM _sync_outbox_rows") { it.getLong(0) }.single())
                assertEquals(2L, connection.queryList("SELECT source_bundle_id FROM _sync_outbox_bundle") { it.getLong(0) }.single())
                assertEquals(1L, connection.queryList("SELECT last_bundle_seq_seen FROM _sync_attachment_state") { it.getLong(0) }.single())
            }
            connection.execSQL("UPDATE _sync_opening_state SET integrity_hash='tampered'")
        }
        assertIs<IllegalArgumentException>(refused)
        assertEquals("coordinated opening journal integrity mismatch", refused.message)
        println("ORDINARY_SCENARIO " + webRuntimeKind() + " tamperedReadyOpenerRefusesWithoutRepair same-lifetime-default-provider")
        if (webRuntimeKind() == "js-node") {
            println("A3_RUNTIME_DISPOSITION js-node tamperedReadyOpenerRefusesWithoutRepair durable-reopen=NOT_APPLICABLE same-lifetime=covered")
            return@runTest
        }
        val db = newCoordinatedDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = db.openClient(config(), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            insertUser(db, "P", "complete original parent")
            insertPost(db, "C", "P", "complete retained clean child")
            client.pushPending().getOrThrow()
            updateUserName(db, "P", "complete frozen local parent")
            server.beforePushCommit = { server.beforePushCommit = null; throw CancellationException("retain real frozen outbox before format rejection") }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"), "tampered fixture has a real frozen historical B/provenance/L")
            assertEquals(2L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            db.close()
            val seeded = db.reopenForInspection()
            try {
                seeded.execSQL("UPDATE _sync_opening_state SET integrity_hash='tampered'")
            } finally { seeded.close() }
            var migrationCalls = 0
            db.migrationFailureForTest = { migrationCalls++ }
            val before = db.reopenForInspection()
            val vectorBefore = try { retainedDatabaseVector(before) }
                finally { before.close() }
            val requestsBefore = server.requestCounts.toMap()
            assertFails { db.openClient(config(), http, ServerWinsResolver) }
            assertEquals(0, migrationCalls, "tampered ready opener refused before migration callback")
            assertEquals(requestsBefore, server.requestCounts, "tampered opener made no HTTP callback/request")
            val after = db.reopenForInspection()
            try {
                assertEquals(vectorBefore, retainedDatabaseVector(after),
                    "tampered opener preserved the complete retained schema/table/index/FK/typed rows and journal/schema/user versions")
                assertEquals("tampered", after.queryList("SELECT integrity_hash FROM _sync_opening_state") { it.getText(0) }.single())
                println("ORDINARY_SCENARIO " + webRuntimeKind() + " tamperedReadyOpenerRefusesWithoutRepair full-retained-census")
            } finally { after.close() }
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun settlementCancellationAndRecreation() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("settlementCancellationAndRecreation")) return@runTest
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            var client = db.openClient(config(), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            insertUser(db, "P", "parent")
            insertUser(db, "U", "historical")
            insertPost(db, "C", "P", "complete clean child")
            client.pushPending().getOrThrow()
            val historical = scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE table_name='users' AND key_json='{\"id\":\"U\"}'")
            updateUserName(db, "P", "changed parent")
            server.authoritativePayloadTransform = { row ->
                if (row.table == "users" && row.key["id"] == "P") JsonObject(row.payload!!.jsonObject + ("name" to JsonPrimitive("normalized parent"))) else row.payload
            }
            server.beforePushResponse = {
                server.beforePushResponse = null
                updateUserName(db, "U", "new independent L")
                throw CancellationException("committed response lost")
            }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            val frozen = databaseStateVector(db.connection(), listOf("users", "posts"))
            val unrelatedPredicates = mapOf("users" to "id='U'", "posts" to "0",
                "_sync_row_state" to "table_name='users' AND key_json='{\"id\":\"U\"}'",
                "_sync_dirty_rows" to "table_name='users' AND key_json='{\"id\":\"U\"}'",
                "_sync_relationships" to "child_key='{\"id\":\"U\"}'")
            val unrelated = databaseStateVector(db.connection(), listOf("users", "posts"), unrelatedPredicates).filterKeys { it in unrelatedPredicates }
            val request = server.requestBodies.last { it.first == "/sync/push" }.second
            assertEquals(2L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            if (webRuntimeKind() != "js-node") {
                val previousConnection = db.connection()
                val admittedName = db.databaseName
                db.close()
                client = db.openClient(config(), http, ServerWinsResolver)
                assertEquals(admittedName, db.databaseName)
                assertNotSame(previousConnection, db.connection(), "new native connection / default browser worker")
                assertEquals(frozen, databaseStateVector(db.connection(), listOf("users", "posts")))
                client.attach("user-1").getOrThrow()
                println("A3 recreated native connection/default worker; same admitted stored database=$admittedName")
            } else println("A3 Node transient replay; durable recreation NOT-APPLICABLE")
            client.pushPending().getOrThrow()
            assertEquals(unrelated, databaseStateVector(db.connection(), listOf("users", "posts"), unrelatedPredicates).filterKeys { it in unrelatedPredicates })
            assertEquals(request, server.requestBodies.last { it.first == "/sync/push" }.second)
            assertEquals("normalized parent", scalarText(db, "SELECT name FROM users WHERE id='P'"))
            assertEquals("complete clean child", scalarText(db, "SELECT title FROM posts WHERE id='C'"))
            assertEquals("P", scalarText(db, "SELECT user_id FROM posts WHERE id='C'"))
            assertEquals("new independent L", scalarText(db, "SELECT name FROM users WHERE id='U'"))
            assertEquals(historical, scalarText(db, "SELECT base_payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"U\"}'"))
            assertContains(scalarText(db, "SELECT payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"U\"}'"), "new independent L")
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            println("A3 final complete state=${databaseStateVector(db.connection(), listOf("users", "posts"))}")
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun admissionRefusalPreservesFrozenPrimaryState() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("admissionRefusalPreservesFrozenPrimaryState")) return@runTest
        if (webRuntimeKind() == "js-node") {
            println("ORDINARY_NOT_EXECUTED js-node admissionRefusalPreservesFrozenPrimaryState durable_reopen_unavailable")
            return@runTest
        }
        withFrozenIntent { db, http, server, _, request ->
            val before = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
            val limited = db.openClient(config(work = 1000), http, ServerWinsResolver)
            limited.attach("user-1").getOrThrow()
            val failure = assertFailsWith<SyncWorkspaceLimitException> { limited.pushPending().getOrThrow() }
            assertEquals("atomic_apply_work", failure.dimension)
            assertEquals(before, databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            val retry = db.openClient(config(), http, ServerWinsResolver)
            retry.attach("user-1").getOrThrow()
            assertFrozenReplay(db, server, retry, request)
            println("ORDINARY_SCENARIO ${webRuntimeKind()} admissionRefusalPreservesFrozenPrimaryState")
        }
    }

    @Test fun responseCancellationPreservesFrozenPrimaryState() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("responseCancellationPreservesFrozenPrimaryState")) return@runTest
        withFrozenIntent { db, _, server, client, request ->
            val before = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
            val barrier = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            server.beforePushResponse = { barrier.complete(Unit); release.await() }
            val operation = async { client.pushPending().getOrThrow() }
            try {
                cancelAtBarrier(barrier, release, operation, "response")
            } finally {
                server.beforePushResponse = null
            }
            assertEquals(before, databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            assertFrozenReplay(db, server, client, request)
            println("ORDINARY_SCENARIO ${webRuntimeKind()} responseCancellationPreservesFrozenPrimaryState")
        }
    }

    private suspend fun assertFrozenReplay(
        db: PlatformTestDatabase, server: MockSyncServer, client: DefaultOversqliteClient, request: String,
    ) {
        client.pushPending().getOrThrow()
        assertEquals(request, server.requestBodies.last { it.first == "/sync/push" }.second)
        assertEquals("new local intent", scalarText(db, "SELECT name FROM users WHERE id='P'"))
        assertEquals("complete child", scalarText(db, "SELECT title FROM posts WHERE id='C'"))
        assertEquals("P", scalarText(db, "SELECT user_id FROM posts WHERE id='C'"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
        assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
    }

    @Test fun rebuildCancellationAfterPage1() = checkApplyCancellation("rebuild", "page-1")
    @Test fun rebuildCancellationAfterPage2() = checkApplyCancellation("rebuild", "page-2")
    @Test fun rebuildCancellationAfterPage3() = checkApplyCancellation("rebuild", "page-3")
    @Test fun rebuildCancellationAfterRow1() = checkApplyCancellation("rebuild", "row-1")
    @Test fun rebuildCancellationAfterRow2() = checkApplyCancellation("rebuild", "row-2")
    @Test fun rebuildCancellationAfterRow3() = checkApplyCancellation("rebuild", "row-3")
    @Test fun rebuildCancellationAfterCacheClosed() = checkApplyCancellation("rebuild", "cache-closed")
    @Test fun rebuildCancellationBeforeCommit() = checkApplyCancellation("rebuild", "before-commit")
    @Test fun pushCancellationAfterCacheClosed() = checkApplyCancellation("push", "cache-closed")
    @Test fun pushCancellationBeforeCommit() = checkApplyCancellation("push", "before-commit")

    private fun applyCancellationName(operation: String, cut: String): String =
        operation + if (cut == "before-commit") "CancellationBeforeCommit"
        else "CancellationAfter" + cut.split('-').joinToString("") { it.replaceFirstChar(Char::uppercase) }

    private fun checkApplyCancellation(operationName: String, cut: String) = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled(applyCancellationName(operationName, cut))) return@runTest
        withFrozenIntent(forApply = true) { db, _, server, client, frozenRequest ->
            if (operationName == "rebuild") {
                // Existing fixture control selects admitted checkpoint recovery
                // with prepared intent. This is not a discovery-path witness.
                OversqliteAttachmentStateStore(db.connection()).setRebuildRequired(true)
            }
            val frozen = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
            val attachment = OversqliteAttachmentStateStore(db.connection()).loadState()
            val source = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(attachment.currentSourceId))
            val pushes = server.pushRequestCount
            val snapshots = server.requestCounts["/sync/snapshot-sessions"] ?: 0
            val marker = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            var pages = 0
            var rows = 0
            var reached = 0
            suspend fun hold() {
                reached++
                marker.complete(Unit)
                // Cache cleanup runs in NonCancellable. The parent releases
                // this observation only after cancelling the real caller.
                release.await()
            }
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                afterApplyPageLoaded = { pages++; if (cut == "page-$pages") hold() },
                afterAppliedRow = { rows++; if (cut == "row-$rows") hold() },
                afterStatementCacheClosed = { if (cut == "cache-closed") hold() },
                beforeCommit = { if (cut == "before-commit") hold() },
            ))
            val caller = async {
                if (operationName == "rebuild") client.rebuild().getOrThrow()
                else client.pushPending().getOrThrow()
            }
            try {
                cancelAtBarrier(marker, release, caller, "$operationName/$cut") {
                    assertEquals(1, reached, "$operationName/$cut reached exactly once")
                }
            } finally {
                client.setSnapshotApplyFaultInjectorForTest(null)
            }
            val rolledBack = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
            assertEquals(frozen.filterKeys { it != "_sync_snapshot_stage" },
                rolledBack.filterKeys { it != "_sync_snapshot_stage" }, "$operationName/$cut complete retained A/B/L, edges, source, checkpoint, conflict, identity, plans and outbox")
            assertEquals(attachment, OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(source, OversqliteSourceStateStore(db.connection()).loadState(attachment.currentSourceId))
            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            if (operationName == "rebuild") {
                assertEquals(pushes, server.pushRequestCount, "snapshot cancellation never sends frozen intent")
                assertEquals(snapshots + 1, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                assertEquals(3L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"), "completed durable snapshot input survives rollback")
                assertTrue(scalarLong(db, "SELECT snapshot_stage_entries FROM _sync_operation_state") > 0)
                if (cut.startsWith("page-")) assertEquals(cut.removePrefix("page-").toInt(), pages)
                if (cut.startsWith("row-")) assertEquals(cut.removePrefix("row-").toInt(), rows)
                if (cut in listOf("cache-closed", "before-commit")) { assertEquals(3, pages); assertEquals(3, rows) }
                client.rebuild().getOrThrow()
                assertEquals(frozen.getValue("users"), databaseStateVector(db.connection(), listOf("users", "posts")).getValue("users"), "snapshot replay preserves complete frozen business intent")
                assertEquals(frozen.getValue("posts"), databaseStateVector(db.connection(), listOf("users", "posts")).getValue("posts"))
                assertEquals(pushes, server.pushRequestCount, "public rebuild replays snapshot without hidden push")
            } else {
                assertEquals(frozen.getValue("_sync_snapshot_stage"), rolledBack.getValue("_sync_snapshot_stage"))
                assertEquals(0, pages); assertEquals(0, rows)
            }
            client.pushPending().getOrThrow()
            assertEquals(frozenRequest, server.requestBodies.last { it.first == "/sync/push" }.second, "same canonical prepared request and source-bundle identity")
            val settled = databaseStateVector(db.connection(), listOf("users", "posts"))
            assertEquals(frozen.getValue("users"), settled.getValue("users"))
            assertEquals(frozen.getValue("posts"), settled.getValue("posts"))
            assertEquals(attachment.copy(lastBundleSeqSeen = 2, rebuildRequired = false), OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(source.copy(nextSourceBundleId = source.nextSourceBundleId + 1),
                OversqliteSourceStateStore(db.connection()).loadState(attachment.currentSourceId), "only confirmed frozen publication advances the sequence")
            assertEquals("frozen local intent", scalarText(db, "SELECT name FROM users WHERE id='P'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                assertTrue(settled.getValue(table).isEmpty(), "$operationName/$cut final cleanup $table")
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context)+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
            println("ORDINARY_SCENARIO ${webRuntimeKind()} ${applyCancellationName(operationName, cut)} $operationName/$cut")
        }
    }

    private suspend fun withFrozenIntent(
        forApply: Boolean = false,
        block: suspend (PlatformTestDatabase, io.ktor.client.HttpClient, MockSyncServer, DefaultOversqliteClient, String) -> Unit,
    ) {
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        var primary: Throwable? = null
        try {
            createUsersAndPostsTables(db)
            val cfg = if (forApply) config().copy(snapshotApplyBatchRows = 1) else config()
            val client = db.openClient(cfg, http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            insertUser(db, "P", "parent")
            if (forApply) insertUser(db, "U", "unrelated complete row")
            insertPost(db, "C", "P", if (forApply) "complete clean child" else "complete child")
            client.pushPending().getOrThrow()
            updateUserName(db, "P", if (forApply) "frozen local intent" else "new local intent")
            server.beforePushCommit = {
                server.beforePushCommit = null
                throw CancellationException("retain prepared input before remote publication")
            }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            val request = server.requestBodies.last { it.first == "/sync/push" }.second
            block(db, http, server, client, request)
        } catch (failure: Throwable) {
            primary = failure
            throw failure
        } finally {
            withContext(NonCancellable) {
                val failures = listOfNotNull(
                    runCatching { db.close() }.exceptionOrNull(),
                    runCatching { http.close() }.exceptionOrNull(),
                    runCatching { cleanupOwnedWorkerStorage() }.exceptionOrNull(),
                )
                val first = primary ?: failures.firstOrNull()
                failures.filter { it !== first }.forEach { first?.addSuppressed(it) }
                if (primary == null && first != null) throw first
            }
        }
    }

    // Existing dispatcher deadlines observe arrival; release is unconditional before joining.
    // Joining an uninterruptible native call still depends on the outer runner's deadline.
    private suspend fun cancelAtBarrier(
        arrived: CompletableDeferred<Unit>, release: CompletableDeferred<Unit>, caller: kotlinx.coroutines.Deferred<*>,
        phase: String, timeout: kotlin.time.Duration = 15.seconds, onArrival: () -> Unit = {},
    ) {
        try {
            withContext(Dispatchers.Default) { withTimeout(timeout) { arrived.await() } }
            onArrival()
            println("ORDINARY_BARRIER ${webRuntimeKind()} $phase")
            caller.cancel()
            assertTrue(caller.isCancelled, "real caller cancelled before observation release: $phase")
            release.complete(Unit)
            caller.join()
            assertTrue(caller.isCompleted, "owned rollback and cleanup joined: $phase")
            assertFailsWith<CancellationException> { caller.await() }
        } finally {
            caller.cancel()
            release.complete(Unit)
            withContext(NonCancellable) { caller.join() }
        }
    }

    @Test fun missingCancellationBarrierReleasesAndJoinsCaller() = runTest(timeout = 15.seconds) {
        if (!scenarioEnabled("missingCancellationBarrierReleasesAndJoinsCaller")) return@runTest
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        var cleaned = false
        val caller = async(start = CoroutineStart.UNDISPATCHED) {
            try {
                withContext(NonCancellable) { started.complete(Unit); release.await() }
            } finally { cleaned = true }
        }
        started.await()
        val virtualStart = testScheduler.currentTime
        val elapsed = TimeSource.Monotonic.markNow()
        assertFailsWith<TimeoutCancellationException> {
            cancelAtBarrier(CompletableDeferred(), release, caller, "deliberately missing", 50.milliseconds)
        }
        assertTrue(elapsed.elapsedNow() < 5.seconds, "missing arrival must terminate promptly")
        assertEquals(virtualStart, testScheduler.currentTime, "arrival deadline must use real time")
        assertTrue(release.isCompleted)
        assertTrue(caller.isCancelled)
        assertTrue(caller.isCompleted)
        assertTrue(cleaned, "run-owned caller cleanup completed")
        println("ORDINARY_SCENARIO ${webRuntimeKind()} missingCancellationBarrierReleasesAndJoinsCaller")
    }

    @Test fun boundedGraphShapesAndSoftCapsDrainOnePublicInvocation() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("boundedGraphShapesAndSoftCapsDrainOnePublicInvocation")) return@runTest
        val shapes = linkedMapOf(
            "wide" to linkedMapOf("P" to null, "C" to "P", "D" to "P", "E" to "P"),
            "deep" to linkedMapOf("P" to null, "C" to "P", "D" to "C", "E" to "D"),
            "scc" to linkedMapOf("P" to "C", "C" to "P", "D" to "C", "E" to "D"),
        )
        try {
            for ((shape, parents) in shapes) {
                val server = MockSyncServer(registeredTableSpecs = testRegisteredTableSpecs("nodes"),
                    wireLimits = testWireLimits().copy(maxPushRows = 2),
                    generationResponses = mapOf(1L to nodeGeneration("NO ACTION")))
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED)")
                    val client = db.openClient(config().copy(syncTables = listOf(SyncTable("nodes", syncKeyColumnName = "id")),
                        transferTargets = OversqliteTransferTargets(maxPushRows = 1)), http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    db.connection().transaction(TransactionMode.IMMEDIATE) {
                        for ((id, parent) in parents) db.execSQL("INSERT INTO nodes VALUES('$id','complete $shape $id',${parent?.let { "'$it'" } ?: "NULL"})")
                    }
                    val before = databaseStateVector(db.connection(), listOf("nodes"))
                    val report = client.sync().getOrThrow() // The public invocation owns the entire drain.
                    val requests = server.requestBodies.filter { it.first == "/sync/push" }.map { Json.parseToJsonElement(it.second).jsonObject }
                    val batches = requests.map { request -> request.getValue("rows").jsonArray.flatMap { group ->
                        assertEquals(0, group.jsonArray[0].jsonPrimitive.int)
                        group.jsonArray[2].jsonArray.map { row -> row.jsonArray[1].jsonArray[0].jsonPrimitive.content }
                    } }
                    val count = if (shape == "scc") 3 else 4
                    assertEquals(count, server.pushRequestCount, shape)
                    assertEquals(count, requests.size, shape)
                    assertEquals(parents.keys, batches.flatten().toSet(), "$shape exact once-only member inventory")
                    assertEquals(4, batches.sumOf { it.size }, shape)
                    if (shape == "scc") {
                        assertEquals(setOf("P", "C"), batches.first().toSet(), "admitted SCC overruns soft row target as one group")
                        assertEquals(listOf(2, 1, 1), batches.map { it.size })
                    } else assertTrue(batches.all { it.size == 1 }, "$shape retains the soft target")
                    val batchOf = batches.flatMapIndexed { index, ids -> ids.map { it to index } }.toMap()
                    for ((id, parent) in parents) if (parent != null) assertTrue(batchOf.getValue(parent) <= batchOf.getValue(id), "$shape parent $parent precedes dependent $id")
                    assertEquals((1L..count.toLong()).toList(), requests.map { it.getValue("source_bundle_id").jsonPrimitive.long }, "$shape consecutive source bundle identities")
                    assertFalse(report.status.pending.hasPendingSyncData)
                    assertEquals(count.toLong(), report.status.lastBundleSeqSeen)
                    val after = databaseStateVector(db.connection(), listOf("nodes"))
                    assertEquals(before.getValue("nodes"), after.getValue("nodes"), "$shape complete business rows preserved")
                    for ((id, parent) in parents) {
                        val payload = buildJsonObject { put("id", id); put("name", "complete $shape $id"); put("parent_id", parent?.let(::JsonPrimitive) ?: JsonNull) }
                        val typed = buildJsonObject { for ((column, value) in payload) putJsonObject(column) { put("storage", if (value == JsonNull) "null" else "text"); put("value", value) } }
                        assertEquals(typed, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$shape full A $id")
                        val version = batchOf.getValue(id).toLong() + 1
                        assertEquals(version, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                        val provenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")).jsonObject
                        assertEquals(version, provenance.getValue("authoritative_version").jsonPrimitive.long)
                        assertEquals(version, provenance.getValue("bundle_sequence").jsonPrimitive.long)
                        assertEquals("user-1", provenance.getValue("account_scope").jsonObject.getValue("account_id").jsonPrimitive.content)
                    }
                    val expectedEdges = buildJsonArray { for ((id, parent) in parents.toList().sortedBy { it.first }) if (parent != null) add(buildJsonObject { put("view", "server"); put("child", id); put("fk", "parent_id"); put("parent", parent) }) }
                    assertEquals(expectedEdges, Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json_object('view',view,'child',json_extract(child_key,'$.id'),'fk',fk_column,'parent',json_extract(parent_key,'$.id'))) FROM (SELECT * FROM _sync_relationships ORDER BY child_key)")), "$shape exact edge inventory")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships e JOIN _sync_row_state a ON a.schema_name=e.child_schema AND a.table_name=e.child_table AND a.key_json=e.child_key WHERE e.owner_revision<>a.server_revision"))
                    for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(after.getValue(table).isEmpty(), "$shape cleans $table")
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    assertEquals(count.toLong() + 1, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertTrue(server.requestCounts.keys.none { "reconcile" in it })
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} boundedGraphShapesAndSoftCapsDrainOnePublicInvocation $shape")
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun ordinaryNoChangeAndOneRowHaveExactRequestsAndServerReadback() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("ordinaryNoChangeAndOneRowHaveExactRequestsAndServerReadback")) return@runTest
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = db.openClient(config(), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            fun counts() = server.requestCounts.toMap()
            fun delta(before: Map<String, Int>) = counts().mapValues { (path, count) -> count - before.getOrElse(path) { 0 } }.filterValues { it != 0 }
            val attached = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
            val beforeEmpty = counts()
            val empty = client.sync().getOrThrow()
            assertEquals(PushOutcome.NO_CHANGE, empty.pushOutcome)
            assertEquals(mapOf("/sync/pull" to 1), delta(beforeEmpty), "ordinary empty call issues only its data-plane pull")
            assertEquals(attached, databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true), "empty call preserves every owner")
            println("ORDINARY_SCENARIO ${webRuntimeKind()} ordinaryNoChangeAndOneRowHaveExactRequestsAndServerReadback empty")

            insertUser(db, "U", "complete one-row payload")
            val beforeOne = counts()
            val one = client.sync().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, one.pushOutcome)
            assertFalse(one.status.pending.hasPendingSyncData)
            assertEquals(mapOf("/sync/push" to 1, "/sync/pull" to 1), delta(beforeOne), "one row requires one direct push and one pull")
            val expectedBusiness = Json.parseToJsonElement("""[{"id":"U","name":"complete one-row payload"}]""")
            val businessQuery = "SELECT json_group_array(json_object('id',id,'name',name)) FROM (SELECT * FROM users ORDER BY id)"
            assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(db, businessQuery)))
            val typed = buildJsonObject { putJsonObject("id") { put("storage", "text"); put("value", "U") }; putJsonObject("name") { put("storage", "text"); put("value", "complete one-row payload") } }
            assertEquals(typed, Json.parseToJsonElement(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE table_name='users' AND key_json='{\"id\":\"U\"}'")))
            assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name='users' AND key_json='{\"id\":\"U\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships"))
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            val settled = databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true)
            val beforeSettled = counts()
            val unchanged = client.sync().getOrThrow()
            assertEquals(PushOutcome.NO_CHANGE, unchanged.pushOutcome)
            assertEquals(mapOf("/sync/pull" to 1), delta(beforeSettled))
            assertEquals(settled, databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true), "no-change call cannot advance source or rewrite A/B/L/identity")
            println("ORDINARY_SCENARIO ${webRuntimeKind()} ordinaryNoChangeAndOneRowHaveExactRequestsAndServerReadback one-row-and-settled")

            // Independent verification reads the same authority through a fresh
            // public client; its requests are outside the counted invocations.
            val viewer = newCoordinatedDb()
            val viewerHttp = server.newHttpClient()
            try {
                createUsersAndPostsTables(viewer)
                val readback = viewer.openClient(config(), viewerHttp, ServerWinsResolver)
                readback.attach("user-1").getOrThrow()
                readback.pullToStable().getOrThrow()
                assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(viewer, businessQuery)), "complete committed authority rows read through public pull")
                assertEquals(typed, Json.parseToJsonElement(scalarText(viewer, "SELECT server_payload FROM _sync_row_state WHERE table_name='users' AND key_json='{\"id\":\"U\"}'")))
                assertEquals(0L, scalarLong(viewer, "SELECT count(*) FROM posts"))
                assertEquals(1L, scalarLong(viewer, "SELECT count(*) FROM _sync_row_state"))
                assertEquals(0L, scalarLong(viewer, "SELECT count(*) FROM _sync_relationships"))
                assertEquals(1L, scalarLong(viewer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(1, server.pushRequestCount, "readback performs no authority mutation")
                assertEquals(settled, databaseStateVector(db.connection(), listOf("users", "posts"), ignoreDerivedCharges = true), "independent observation preserves original owners")
                println("ORDINARY_SCENARIO ${webRuntimeKind()} ordinaryNoChangeAndOneRowHaveExactRequestsAndServerReadback independent-readback")
            } finally { viewerHttp.close(); viewer.close() }
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun onePublicInvocationSharesWorkMeterAndRetainsUsefulFrozenProgress() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("onePublicInvocationSharesWorkMeterAndRetainsUsefulFrozenProgress")) return@runTest
        val referenceConfig = config().copy(transferTargets = OversqliteTransferTargets(maxPushRows = 2))
        val freshWork = mutableListOf<Long>()
        // Preparation only: establish that each identical two-row unit fits a
        // fresh invocation. This caller loop never substitutes for the tested sync.
        val referenceServer = MockSyncServer()
        val referenceDb = newCoordinatedDb()
        val referenceHttp = referenceServer.newHttpClient()
        try {
            createUsersAndPostsTables(referenceDb)
            val reference = referenceDb.openClient(referenceConfig, referenceHttp, ServerWinsResolver)
            reference.attach("user-1").getOrThrow()
            repeat(6) { insertUser(referenceDb, "U00$it", "complete payload $it") }
            repeat(3) {
                reference.pushPending().getOrThrow()
                freshWork += reference.workspaceDiagnostics().work
            }
            assertEquals(3, referenceServer.pushRequestCount)
            assertEquals(0L, scalarLong(referenceDb, "SELECT count(*) FROM _sync_dirty_rows"))
        } finally { referenceHttp.close(); referenceDb.close() }
        val budget = freshWork[0] + freshWork[1] - freshWork[2] / 4
        assertTrue(freshWork.all { it > 0 && it < budget })
        assertTrue(budget < freshWork.sum(), "complete drain cannot receive a fresh meter per internal push")
        assertTrue(budget >= freshWork[1] + freshWork[2], "the retained final two units can resume under the same configured limit")
        println("A3_CONTROL fresh-work=$freshWork shared-budget=$budget")
        try {
            for (operation in listOf("sync", "detach")) {
                val server = MockSyncServer()
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    createUsersAndPostsTables(db)
                    val cfg = referenceConfig.copy(maxSyncWorkUnitsPerInvocation = budget)
                    val client = db.openClient(cfg, http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    repeat(6) { insertUser(db, "U00$it", "complete payload $it") }
                    val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                    val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                    val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
                    val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
                    val before = databaseStateVector(db.connection(), listOf("users", "posts"))
                    val fields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
                    val originalIntents = Json.parseToJsonElement(scalarText(db, "SELECT json_group_array($fields) FROM (SELECT * FROM _sync_dirty_rows ORDER BY key_json)")).jsonArray
                    val pulls = server.requestCounts["/sync/pull"] ?: 0
                    val stopped = assertIs<SyncWorkLimitException>(when (operation) {
                        "sync" -> client.sync().exceptionOrNull()
                        else -> client.syncThenDetach().exceptionOrNull()
                    }, "$operation each unit fits fresh capacity; remaining invocation work is exhausted")
                    assertEquals(budget, stopped.limit)
                    assertEquals(client.workspaceDiagnostics().work, stopped.used)
                    assertTrue(stopped.used > freshWork[0] && stopped.used <= budget)
                    assertTrue(Regex("[0-9a-f]{64}").matches(stopped.unit))
                    assertEquals(SyncProgress(dirtyRows = 2, outboundRows = 2, committedBundles = 2), stopped.progress,
                        "confirmed second remote commit counts while its local settlement is still staged")
                    assertEquals(2, server.pushRequestCount)
                    assertEquals(pulls, server.requestCounts["/sync/pull"] ?: 0)
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 1, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
                    assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
                    assertEquals(sourceBefore.copy(nextSourceBundleId = 2), OversqliteSourceStateStore(db.connection()).loadState(source))
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
                    val state = requireNotNull(client.pendingConflict().getOrThrow().let { if (it == null) OversqliteOutboxStateStore(db.connection()).loadBundleState() else null })
                    assertEquals(source, state.sourceId)
                    assertEquals(2L, state.sourceBundleId)
                    val wires = server.requestBodies.filter { it.first == "/sync/push" }
                    val originalWire = wires.last().second
                    val secondWire = Json.parseToJsonElement(originalWire).jsonObject
                    assertEquals(state.canonicalRequestHash, secondWire.getValue("canonical_request_hash").jsonPrimitive.content)
                    assertEquals(2L, secondWire.getValue("source_bundle_id").jsonPrimitive.long)
                    assertEquals(List(2) { source }, server.requestSourceIds.filter { it.first == "/sync/push" }.map { it.second })
                    val remainingFields = fields.replace("'L',payload", "'L',local_payload")
                    val pendingIntents = Json.parseToJsonElement(scalarText(db, "SELECT json_group_array(json(intent)) FROM (SELECT key_json,$fields AS intent FROM _sync_dirty_rows UNION ALL SELECT key_json,$remainingFields AS intent FROM _sync_outbox_rows ORDER BY key_json)"))
                    assertEquals(JsonArray(originalIntents.drop(2)), pendingIntents, "$operation all remaining original historical B/provenance/L")
                    val retained = databaseStateVector(db.connection(), listOf("users", "posts"), predicates = mapOf("_sync_row_state" to "json_extract(key_json,'$.id')>='U002'"))
                    assertEquals(before.getValue("_sync_row_state").drop(2), retained.getValue("_sync_row_state"), "$operation complete unsettled A unchanged")
                    for (table in listOf("users", "posts", "_sync_relationships", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) assertEquals(before.getValue(table), retained.getValue(table), "$operation exact primary/recovery identity $table")
                    assertEquals("ready", scalarText(db, "SELECT phase FROM _sync_apply_plan"))
                    assertTrue(retained.getValue("_sync_apply_rows").isNotEmpty(), "$operation useful charged ready input retained")
                    val usage = OversqliteWorkspace(db.connection(), cfg).usage(stopped.unit)
                    assertTrue(usage.entries <= cfg.maxSyncWorkspaceEntries && usage.bytes <= cfg.maxSyncWorkspaceBytes)
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    val provenance = buildJsonObject {
                        put("database_id", cfg.databaseId); put("service_id", cfg.serviceId)
                        putJsonObject("account_scope") { put("binding", "attached"); put("account_id", "user-1") }
                        put("generation_id", cfg.generationId)
                        put("generation_fingerprint", scalarText(db, "SELECT fingerprint FROM _sync_contract_identity"))
                        put("admission", "admitted")
                        put("local_version", scalarLong(db, "SELECT local_version FROM _sync_contract_identity"))
                        put("physical_shape", scalarText(db, "SELECT physical_shape FROM _sync_contract_identity"))
                        put("origin_source_id", source); put("origin", "settlement")
                        put("bundle_sequence", 1); put("authoritative_version", 1)
                    }
                    suspend fun assertPublished(target: PlatformTestDatabase, count: Int, base: JsonObject = provenance, snapshot: Boolean = false) {
                        for (i in 0 until count) {
                            val typed = buildJsonObject { putJsonObject("id") { put("storage", "text"); put("value", "U00$i") }; putJsonObject("name") { put("storage", "text"); put("value", "complete payload $i") } }
                            val version = (i / 2 + 1).toLong()
                            assertEquals(typed, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"U00$i\"}'")))
                            assertEquals(JsonObject(base + mapOf("bundle_sequence" to JsonPrimitive(if (snapshot) 3L else version), "authoritative_version" to JsonPrimitive(version))), Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"U00$i\"}'")))
                            assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"U00$i\"}'"))
                            assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"U00$i\"}'"))
                            assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE key_json='{\"id\":\"U00$i\"}'"))
                        }
                    }
                    assertPublished(db, 2)
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} onePublicInvocationSharesWorkMeterAndRetainsUsefulFrozenProgress $operation/work-limit")
                    val resumed = client.sync().getOrThrow()
                    assertEquals(PushOutcome.COMMITTED, resumed.pushOutcome)
                    assertFalse(resumed.status.pending.hasPendingSyncData)
                    val finalWires = server.requestBodies.filter { it.first == "/sync/push" }
                    assertEquals(4, finalWires.size)
                    assertEquals(originalWire, finalWires[2].second, "exact frozen replay precedes new planning")
                    assertEquals(listOf(1L, 2L, 2L, 3L), finalWires.map { Json.parseToJsonElement(it.second).jsonObject.getValue("source_bundle_id").jsonPrimitive.long })
                    assertEquals(List(4) { source }, server.requestSourceIds.filter { it.first == "/sync/push" }.map { it.second })
                    assertEquals(pulls + 1, server.requestCounts["/sync/pull"])
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 3, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
                    assertEquals(sourceBefore.copy(nextSourceBundleId = 4), OversqliteSourceStateStore(db.connection()).loadState(source))
                    assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
                    assertPublished(db, 6)
                    val complete = databaseStateVector(db.connection(), listOf("users", "posts"))
                    for (table in listOf("users", "posts", "_sync_relationships", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) assertEquals(before.getValue(table), complete.getValue(table))
                    for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(complete.getValue(table).isEmpty(), "$operation final cleanup $table")
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} onePublicInvocationSharesWorkMeterAndRetainsUsefulFrozenProgress $operation/later-resume")
                    val viewer = newCoordinatedDb()
                    val viewerHttp = server.newHttpClient()
                    try {
                        createUsersAndPostsTables(viewer)
                        val readback = viewer.openClient(referenceConfig, viewerHttp, ServerWinsResolver)
                        readback.attach("user-1").getOrThrow()
                        readback.pullToStable().getOrThrow()
                        val view = databaseStateVector(viewer.connection(), listOf("users", "posts"))
                        assertEquals(before.getValue("users"), view.getValue("users"))
                        assertEquals(before.getValue("posts"), view.getValue("posts"))
                        val viewProvenance = JsonObject(provenance + mapOf("origin_source_id" to JsonPrimitive(scalarText(viewer, "SELECT current_source_id FROM _sync_attachment_state")), "origin" to JsonPrimitive("snapshot"), "bundle_sequence" to JsonPrimitive(3)))
                        assertPublished(viewer, 6, viewProvenance, snapshot = true)
                        assertEquals(3L, scalarLong(viewer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(4, server.pushRequestCount)
                        assertEquals(complete, databaseStateVector(db.connection(), listOf("users", "posts")))
                        println("ORDINARY_SCENARIO ${webRuntimeKind()} onePublicInvocationSharesWorkMeterAndRetainsUsefulFrozenProgress $operation/independent-readback")
                    } finally { viewerHttp.close(); viewer.close() }
                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun onePublicInvocationRetainsAttemptBudgetAndConfirmedPrefix() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("onePublicInvocationRetainsAttemptBudgetAndConfirmedPrefix")) return@runTest
        try {
            for (operation in listOf("sync", "detach")) {
                val server = MockSyncServer()
                val db = newCoordinatedDb()
                val http = server.newHttpClient()
                try {
                    createUsersAndPostsTables(db)
                    val cfg = config().copy(transferTargets = OversqliteTransferTargets(maxPushRows = 1))
                    val client = db.openClient(cfg, http, ServerWinsResolver)
                    client.attach("user-1").getOrThrow()
                    repeat(70) { insertUser(db, "U${it.toString().padStart(3, '0')}", "complete payload $it") }
                    val remaining = "json_extract(key_json,'$.id')>='U064'"
                    val before = databaseStateVector(db.connection(), listOf("users", "posts"), predicates = mapOf(
                        "_sync_dirty_rows" to remaining, "_sync_row_state" to remaining))
                    val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                    val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                    val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
                    val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
                    val pulls = server.requestCounts["/sync/pull"] ?: 0
                    if (operation == "sync") {
                        val stopped = assertIs<SyncIncompleteException>(client.sync().exceptionOrNull())
                        assertEquals(SyncIncompleteReason.PUSH_ATTEMPT_LIMIT, stopped.reason)
                        assertEquals(64, stopped.pushAttempts)
                        assertEquals(64, stopped.confirmedBundleCount, "one confirmed bundle per actual one-row request")
                        assertEquals(6L, stopped.dirtyCount)
                        assertEquals(0L, stopped.outboundCount)
                    } else {
                        val stopped = client.syncThenDetach().getOrThrow()
                        assertFalse(stopped.isSuccess())
                        assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, stopped.detach)
                        assertEquals(SyncThenDetachBlockedReason.PUSH_ATTEMPT_LIMIT, stopped.blockedReason)
                        assertEquals(1, stopped.syncRounds, "no fresh allowance hidden in detach rounds")
                        assertEquals(6L, stopped.remainingPendingRowCount)
                        assertNull(stopped.lastSync, "incomplete first round cannot claim a completed sync report")
                    }
                    assertEquals(64, server.pushRequestCount)
                    assertEquals(pulls, server.requestCounts["/sync/pull"] ?: 0, "unsent intent prevents unsafe pull")
                    assertEquals(0, server.requestCounts["/sync/snapshot-sessions"] ?: 0)
                    assertEquals(65L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(64L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 64, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
                    assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
                    assertEquals(sourceBefore.copy(nextSourceBundleId = 65), OversqliteSourceStateStore(db.connection()).loadState(source))
                    assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
                    assertEquals(source, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                    assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                    assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                    val stopped = databaseStateVector(db.connection(), listOf("users", "posts"), predicates = mapOf(
                        "_sync_dirty_rows" to remaining, "_sync_row_state" to remaining))
                    for (table in listOf("users", "posts", "_sync_dirty_rows", "_sync_row_state", "_sync_relationships",
                        "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")) {
                        assertEquals(before.getValue(table), stopped.getValue(table), "$operation full unconfirmed intent/business/identity $table")
                    }
                    for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")) assertTrue(stopped.getValue(table).isEmpty(), "$operation retained owner released $table")
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    val provenance = buildJsonObject {
                        put("database_id", cfg.databaseId); put("service_id", cfg.serviceId)
                        putJsonObject("account_scope") { put("binding", "attached"); put("account_id", "user-1") }
                        put("generation_id", cfg.generationId)
                        put("generation_fingerprint", scalarText(db, "SELECT fingerprint FROM _sync_contract_identity"))
                        put("admission", "admitted")
                        put("local_version", scalarLong(db, "SELECT local_version FROM _sync_contract_identity"))
                        put("physical_shape", scalarText(db, "SELECT physical_shape FROM _sync_contract_identity"))
                        put("origin_source_id", source); put("origin", "settlement")
                        put("bundle_sequence", 1); put("authoritative_version", 1)
                    }
                    assertEquals(provenance, Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"U000\"}'")), "$operation every initial confirmed provenance field")
                    suspend fun assertPublished(count: Int, target: PlatformTestDatabase = db, baseProvenance: JsonObject = provenance, snapshotSequence: Long? = null) {
                        for (i in 0 until count) {
                            val id = "U${i.toString().padStart(3, '0')}"
                            val typed = buildJsonObject { putJsonObject("id") { put("storage", "text"); put("value", id) }; putJsonObject("name") { put("storage", "text"); put("value", "complete payload $i") } }
                            assertEquals(typed, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$operation complete A $id")
                            assertEquals(JsonObject(baseProvenance + mapOf("bundle_sequence" to JsonPrimitive(snapshotSequence ?: (i + 1).toLong()), "authoritative_version" to JsonPrimitive(i + 1))), Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'")), "$operation full provenance $id")
                            assertEquals((i + 1).toLong(), scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                            assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                            assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'"))
                        }
                    }
                    assertPublished(64)
                    val requestWires = server.requestBodies.filter { it.first == "/sync/push" }.map { Json.parseToJsonElement(it.second).jsonObject }
                    assertEquals((1L..64L).toList(), requestWires.map { it.getValue("source_bundle_id").jsonPrimitive.long })
                    assertEquals(List(64) { source }, server.requestSourceIds.filter { it.first == "/sync/push" }.map { it.second })
                    assertEquals(64, requestWires.map { it.getValue("canonical_request_hash").jsonPrimitive.content }.distinct().size)
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} onePublicInvocationRetainsAttemptBudgetAndConfirmedPrefix $operation/stopped-prefix")

                    // A later public invocation has a fresh allowance; the first
                    // invocation itself neither loops in caller code nor resets it.
                    val resumed = client.sync().getOrThrow()
                    assertEquals(PushOutcome.COMMITTED, resumed.pushOutcome)
                    assertFalse(resumed.status.pending.hasPendingSyncData)
                    assertEquals(70, server.pushRequestCount)
                    assertEquals(pulls + 1, server.requestCounts["/sync/pull"])
                    assertEquals(71L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(70L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 70, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
                    assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
                    assertEquals(sourceBefore.copy(nextSourceBundleId = 71), OversqliteSourceStateStore(db.connection()).loadState(source))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertPublished(70)
                    val complete = databaseStateVector(db.connection(), listOf("users", "posts"))
                    assertEquals(before.getValue("users"), complete.getValue("users"))
                    assertEquals(before.getValue("posts"), complete.getValue("posts"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    println("ORDINARY_SCENARIO ${webRuntimeKind()} onePublicInvocationRetainsAttemptBudgetAndConfirmedPrefix $operation/later-drain")
                    // Independent server readback is outside both counted calls.
                    val viewer = newCoordinatedDb()
                    val viewerHttp = server.newHttpClient()
                    try {
                        createUsersAndPostsTables(viewer)
                        val readback = viewer.openClient(cfg, viewerHttp, ServerWinsResolver)
                        readback.attach("user-1").getOrThrow()
                        readback.pullToStable().getOrThrow()
                        val view = databaseStateVector(viewer.connection(), listOf("users", "posts"))
                        assertEquals(before.getValue("users"), view.getValue("users"), "$operation full final authority business vector")
                        assertEquals(before.getValue("posts"), view.getValue("posts"))
                        val viewerSource = scalarText(viewer, "SELECT current_source_id FROM _sync_attachment_state")
                        val viewProvenance = JsonObject(provenance + mapOf("origin_source_id" to JsonPrimitive(viewerSource),
                            "origin" to JsonPrimitive("snapshot"), "bundle_sequence" to JsonPrimitive(70)))
                        assertEquals(viewProvenance, Json.parseToJsonElement(scalarText(viewer, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"U000\"}'")), "$operation every snapshot provenance field")
                        assertPublished(70, viewer, viewProvenance, snapshotSequence = 70)
                        assertEquals(70L, scalarLong(viewer, "SELECT count(*) FROM _sync_row_state"))
                        assertEquals(0L, scalarLong(viewer, "SELECT count(*) FROM _sync_relationships"))
                        assertEquals(0L, scalarLong(viewer, "SELECT count(*) FROM _sync_dirty_rows"))
                        assertEquals(0L, scalarLong(viewer, "SELECT count(*) FROM _sync_outbox_rows"))
                        assertEquals(70L, scalarLong(viewer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(70, server.pushRequestCount)
                        assertEquals(complete, databaseStateVector(db.connection(), listOf("users", "posts")), "independent observation leaves original owners exact")
                        println("ORDINARY_SCENARIO ${webRuntimeKind()} onePublicInvocationRetainsAttemptBudgetAndConfirmedPrefix $operation/independent-readback")
                    } finally { viewerHttp.close(); viewer.close() }

                } finally { http.close(); db.close() }
            }
        } finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun onePublicSyncDrainsMultipleBatches() = runTest(timeout = 180.seconds) {
        if (!scenarioEnabled("onePublicSyncDrainsMultipleBatches")) return@runTest
        val server = MockSyncServer()
        val db = newCoordinatedDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = db.openClient(config().copy(transferTargets = OversqliteTransferTargets(maxPushRows = 2)), http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            repeat(6) { insertUser(db, "U$it", "complete payload $it") }
            val report = client.sync().getOrThrow()
            assertEquals(3, server.pushRequestCount)
            repeat(6) { assertEquals("complete payload $it", scalarText(db, "SELECT name FROM users WHERE id='U$it'")) }
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(4L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(3L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(6L, scalarLong(db, "SELECT count(*) FROM _sync_row_state WHERE server_kind='live' AND server_payload IS NOT NULL AND server_provenance IS NOT NULL"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships"))
            assertTrue(server.requestCounts.keys.none { "reconcile" in it })
            println("A3 one public sync report=$report final complete state=${databaseStateVector(db.connection(), listOf("users", "posts"))}")
        } finally { http.close(); db.close(); cleanupOwnedWorkerStorage() }
    }
}
