package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platform.*
import io.ktor.client.statement.HttpReceivePipeline
import io.ktor.client.request.HttpSendPipeline
import io.ktor.http.HttpHeaders
import io.ktor.http.content.OutgoingContent
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

internal class OrdinarySafetyLiveTest : RealServerHarnessSupport() {
    @Test fun pullDiscoveredRecoveryAndExplicitRebuildHydrateCompleteRemoteState() = runTest(timeout = 360.seconds) {
        val identity = "dev.goquick.sqlitenow.oversqlite.realserver.OrdinarySafetyLiveTest.pullDiscoveredRecoveryAndExplicitRebuildHydrateCompleteRemoteState"
        val authority = requireRealServerConfig() ?: run {
            println("A3_NOT_EXECUTED ${webRuntimeKind()} $identity opt_in_disabled")
            return@runTest
        }
        println("A3_ENTER ${webRuntimeKind()} $identity")
        val ddl = "CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
        val cfg = OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "business",
            syncTables = listOf(SyncTable("categories", syncKeyColumnName = "id")), snapshotApplyBatchRows = 2,
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1))
        val db = newCoordinatedDb()
        val writerDb = newCoordinatedDb()
        var http: io.ktor.client.HttpClient? = null
        var writerHttp: io.ktor.client.HttpClient? = null
        try {
            db.execSQL(ddl); writerDb.execSQL(ddl)
            val user = randomRealServerId("a3-live-recovery")
            val p = randomRealServerUuid(); val q = randomRealServerUuid(); val c = randomRealServerUuid(); val d = randomRealServerUuid()
            val requests = mutableListOf<String>()
            val routes = mutableListOf<Pair<String, String>>()
            val base = newRealServerHttpClient(authority.baseUrl,
                issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(db, authority.baseUrl)),
                beforeRequest = { path -> requests += path })
            val transport = base.config {
                install("A3LiveRecoverySourceObserver") {
                    sendPipeline.intercept(HttpSendPipeline.Monitoring) {
                        routes += context.url.build().encodedPath to context.headers["Oversync-Source-ID"].orEmpty()
                        proceed()
                    }
                }
            }
            base.close(); http = transport
            val client = db.openClient(cfg, transport, ServerWinsResolver)
            client.attach(user).getOrThrow()
            db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','other',NULL),('$c','child','$p'),('$d','grandchild','$c')")
            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            val provenance = Json.parseToJsonElement(scalarText(db, "SELECT server_provenance FROM _sync_row_state WHERE key_json='{\"id\":\"$p\"}'")).jsonObject
            writerHttp = newRealServerHttpClient(authority.baseUrl,
                issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(writerDb, authority.baseUrl)))
            val writer = writerDb.openClient(cfg, writerHttp, ServerWinsResolver)
            assertEquals(AttachOutcome.USED_REMOTE_STATE, assertIs<AttachResult.Connected>(writer.attach(user).getOrThrow()).outcome)
            writer.pullToStable().getOrThrow()
            assertNotEquals(scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"),
                scalarText(writerDb, "SELECT current_source_id FROM _sync_attachment_state"), "actual remote business writer owns an independent source")
            fun typed(value: JsonElement, storage: String) = buildJsonObject { put("storage", storage); put("value", value) }
            fun projection(columns: List<String>) = columns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            fun payload(id: String, name: String, parent: String?) = buildJsonObject {
                put("id", typed(JsonPrimitive(id), "text")); put("name", typed(JsonPrimitive(name), "text"))
                put("parent_id", typed(parent?.let(::JsonPrimitive) ?: JsonNull, if (parent == null) "null" else "text"))
            }
            val conflictQuery = "SELECT json_object(${projection(listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id", "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json"))}) FROM _sync_conflict_state"
            fun verifyRestoreCounters(owner: DefaultOversqliteClient) {
                val counters = owner.snapshotRestoreDiagnosticsForTest()
                assertEquals(1L, counters.sessionCount); assertEquals(1L, counters.fetchedChunks)
                assertEquals(4, counters.maxValidatedChunkRows); assertEquals(2L, counters.applyPages); assertEquals(2, counters.maxLiveApplyPageRows)
                assertEquals(4L, counters.finalStagedRows); assertEquals(4L, counters.finalAppliedRows)
                assertTrue(counters.maxDeclaredChunkBytes > 0 && counters.maxCompletelyDecodedChunkBodyBytes > 0)
                assertTrue(counters.maxLiveApplyPageStagedTextBytes > 0 && counters.maxLiveApplyPageStagedTextBytes <= cfg.snapshotApplyBatchBytes)
                assertEquals(0L, counters.capacityResponses); assertEquals(0L, counters.capacityRetries); assertEquals(0L, counters.capacityWaitMillis)
                assertTrue(counters.restoreDurationMillis >= 0)
            }
            suspend fun verifySnapshot(target: PlatformTestDatabase, expected: Map<String, JsonObject>, childVersion: Long,
                sequence: Long, firstRevision: Long, earliest: String, latest: String) {
                assertEquals(JsonArray(expected.values.toList()), Json.parseToJsonElement(scalarText(target,
                    "SELECT json_group_array(json_object(${projection(listOf("id", "name", "parent_id"))})) FROM (SELECT * FROM categories ORDER BY id)")), "complete typed business vector including clean grandchild")
                val source = scalarText(target, "SELECT current_source_id FROM _sync_attachment_state")
                assertEquals(4L, scalarLong(target, "SELECT count(*) FROM _sync_row_state"))
                for ((index, entry) in expected.entries.withIndex()) {
                    val (id, value) = entry
                    val predicate = "schema_name='business' AND table_name='categories' AND key_json='{\"id\":\"$id\"}'"
                    val version = if (id == c) childVersion else 1L
                    assertEquals(value, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE $predicate")))
                    assertEquals(JsonObject(provenance + mapOf("origin" to JsonPrimitive("snapshot"), "origin_source_id" to JsonPrimitive(source),
                        "bundle_sequence" to JsonPrimitive(sequence), "authoritative_version" to JsonPrimitive(version))),
                        Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")), "every provenance field $id")
                    assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                    assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                    assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                    assertEquals(firstRevision + index, scalarLong(target, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                    val updated = scalarText(target, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                    assertTrue(updated >= earliest && updated <= latest)
                    for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                        assertEquals("text", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    for (column in listOf("row_version", "relationships_known", "server_revision"))
                        assertEquals("integer", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                }
                val parent = expected.getValue(c).getValue("parent_id").jsonObject.getValue("value").jsonPrimitive.content
                val edges = listOf(c to parent, d to c).sortedBy { it.first }.map { (child, parentId) ->
                    JsonObject(mapOf("view" to JsonPrimitive("server"), "child_schema" to JsonPrimitive("business"), "child_table" to JsonPrimitive("categories"),
                        "child_key" to JsonPrimitive("{\"id\":\"$child\"}"), "fk_column" to JsonPrimitive("parent_id"), "parent_schema" to JsonPrimitive("business"),
                        "parent_table" to JsonPrimitive("categories"), "parent_key" to JsonPrimitive("{\"id\":\"$parentId\"}"),
                        "owner_revision" to JsonPrimitive(firstRevision + expected.keys.toList().indexOf(child)))
                        .mapValues { (column, value) -> typed(value, if (column == "owner_revision") "integer" else "text") })
                }
                val edgeColumns = listOf("view", "child_schema", "child_table", "child_key", "fk_column", "parent_schema", "parent_table", "parent_key", "owner_revision")
                assertEquals(JsonArray(edges), Json.parseToJsonElement(scalarText(target,
                    "SELECT json_group_array(json_object(${projection(edgeColumns)})) FROM (SELECT * FROM _sync_relationships ORDER BY child_key,view)")), "every typed server edge, no fabricated desired edges")
                val vector = databaseStateVector(target.connection(), listOf("categories"))
                for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                    assertTrue(vector.getValue(table).isEmpty(), "retired owner $table")
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(target.connection()).loadState())
                assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(target.connection()).loadBundleState())
                assertEquals(WorkspaceCharge(1, 536), OversqliteWorkspace(target.connection(), cfg).usage(applyUnit("author-live-recovery", 0, "")))
                assertEquals(0L, scalarLong(target, "SELECT count(*) FROM pragma_foreign_key_check"))
            }
            suspend fun exercise(mode: String, sequence: Long, childName: String, parent: String) {
                writerDb.execSQL("UPDATE categories SET name='$childName',parent_id='$parent' WHERE id='$c'")
                assertEquals(PushOutcome.COMMITTED, writer.pushPending().getOrThrow().outcome)
                assertEquals(sequence, writer.syncStatus().getOrThrow().lastBundleSeqSeen)
                if (mode == "checkpoint-ahead") db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen=99")
                val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                assertFalse(attachmentBefore.rebuildRequired, "public operation discovers/owns its recovery transition")
                val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(attachmentBefore.currentSourceId))
                val before = databaseStateVector(db.connection(), listOf("categories"))
                val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
                val revisionBefore = conflictBefore.getValue("baseline_revision").jsonObject.getValue("value").jsonPrimitive.long
                val requestStart = requests.size; val routeStart = routes.size
                val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                val report = if (mode == "checkpoint-ahead") client.pullToStable().getOrThrow() else client.rebuild().getOrThrow()
                val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, report.outcome)
                assertEquals(RestoreSummary(sequence, 4), report.restore)
                assertEquals(SyncStatus(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, PendingSyncStatus(false, 0, false), sequence), report.status)
                verifyRestoreCounters(client)
                val observed = requests.drop(requestStart)
                val offset = if (mode == "checkpoint-ahead") 1 else 0
                assertEquals(offset + 3, observed.size, "$mode one owned snapshot with chunk and DELETE; no retry/push/reconcile")
                if (offset == 1) assertEquals("/sync/pull", observed[0])
                assertEquals("/sync/snapshot-sessions", observed[offset])
                assertTrue(observed[offset + 1].startsWith("/sync/snapshot-sessions/"))
                assertEquals(observed[offset + 1], observed[offset + 2], "chunk and cleanup retire the same actual server session")
                assertEquals(observed, routes.drop(routeStart).map { it.first })
                assertEquals(List(observed.size) { attachmentBefore.currentSourceId }, routes.drop(routeStart).map { it.second }, "$mode every request has unchanged source ownership")
                val expected = listOf(payload(p, "parent", null), payload(q, "other", null), payload(c, childName, parent), payload(d, "grandchild", c))
                    .sortedBy { it.getValue("id").jsonObject.getValue("value").jsonPrimitive.content }
                    .associateBy { it.getValue("id").jsonObject.getValue("value").jsonPrimitive.content }
                verifySnapshot(db, expected, sequence, sequence, revisionBefore + 5, publicationStart, publicationEnd)
                assertEquals(sourceBefore, OversqliteSourceStateStore(db.connection()).loadState(attachmentBefore.currentSourceId))
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state")); assertEquals(2L, sourceBefore.nextSourceBundleId)
                assertEquals(attachmentBefore.copy(lastBundleSeqSeen = sequence, rebuildRequired = false, pendingInitializationId = ""),
                    OversqliteAttachmentStateStore(db.connection()).loadState())
                assertEquals(JsonObject(conflictBefore + ("baseline_revision" to typed(JsonPrimitive(revisionBefore + 8), "integer"))),
                    Json.parseToJsonElement(scalarText(db, conflictQuery)), "only four old A removals and four snapshot installations change conflict controls")
                val recovered = databaseStateVector(db.connection(), listOf("categories"))
                val changed = setOf("categories", "_sync_row_state", "_sync_relationships", "_sync_attachment_state", "_sync_conflict_state")
                assertEquals(before.filterKeys { it !in changed }, recovered.filterKeys { it !in changed }, "$mode full unowned primary/recovery/stage state preserved")
                println("A3_SCENARIO ${webRuntimeKind()} $identity $mode/full-snapshot")
                val counted = requests.toList(); val countedRoutes = routes.toList()
                suspend fun verifyIndependentAuthority() {
                    val peer = newCoordinatedDb()
                    var peerHttp: io.ktor.client.HttpClient? = null
                    try {
                        peer.execSQL(ddl)
                        val peerStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        peerHttp = newRealServerHttpClient(authority.baseUrl,
                            issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(peer, authority.baseUrl)))
                        val observer = peer.openClient(cfg, peerHttp, ServerWinsResolver)
                        val peerAttachment = OversqliteAttachmentStateStore(peer.connection()).loadState()
                        val peerSource = requireNotNull(OversqliteSourceStateStore(peer.connection()).loadState(peerAttachment.currentSourceId))
                        val peerBefore = databaseStateVector(peer.connection(), listOf("categories"))
                        val peerOpening = requireNotNull(OversqliteOpeningStore(peer.connection()).readOrNull())
                        val openingQuery = "SELECT json_object(${projection(oversqliteOpeningColumnNames)}) FROM _sync_opening_state"
                        val openingFields = Json.parseToJsonElement(scalarText(peer, openingQuery)).jsonObject
                        val peerConflict = Json.parseToJsonElement(scalarText(peer, conflictQuery)).jsonObject
                        assertFalse(peerOpening.contractConfirmed); assertEquals("", peerOpening.targetFingerprint)
                        assertEquals(AttachOutcome.USED_REMOTE_STATE, assertIs<AttachResult.Connected>(observer.attach(user).getOrThrow()).outcome)
                        verifyRestoreCounters(observer)
                        observer.pullToStable().getOrThrow()
                        val peerEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        assertNotEquals(attachmentBefore.currentSourceId, peerAttachment.currentSourceId)
                        verifySnapshot(peer, expected, sequence, sequence, 1, peerStart, peerEnd)
                        assertEquals(peerSource, OversqliteSourceStateStore(peer.connection()).loadState(peerAttachment.currentSourceId))
                        assertEquals(1L, peerSource.nextSourceBundleId); assertEquals("", peerSource.replacedBySourceId)
                        assertTrue(peerSource.createdAt >= peerStart && peerSource.createdAt <= peerEnd)
                        assertEquals(1L, scalarLong(peer, "SELECT count(*) FROM _sync_source_state"))
                        assertEquals(peerAttachment.copy(attachmentEpoch = peerAttachment.attachmentEpoch + 1, bindingState = attachmentBindingAttached,
                            attachedUserId = user, schemaName = attachmentBefore.schemaName, lastBundleSeqSeen = sequence, rebuildRequired = false, pendingInitializationId = ""),
                            OversqliteAttachmentStateStore(peer.connection()).loadState())
                        assertEquals(JsonObject(peerConflict + ("baseline_revision" to typed(JsonPrimitive(4), "integer"))), Json.parseToJsonElement(scalarText(peer, conflictQuery)))
                        val fingerprint = provenance.getValue("generation_fingerprint").jsonPrimitive.content
                        val confirmed = peerOpening.copy(revision = peerOpening.revision + 1, targetFingerprint = fingerprint, contractConfirmed = true)
                        assertEquals(confirmed, OversqliteOpeningStore(peer.connection()).readOrNull())
                        assertEquals(JsonObject(openingFields + mapOf("revision" to typed(JsonPrimitive(confirmed.revision), "integer"),
                            "target_fingerprint" to typed(JsonPrimitive(fingerprint), "text"), "contract_confirmed" to typed(JsonPrimitive(1), "integer"),
                            "integrity_hash" to typed(JsonPrimitive(confirmed.integrityHash()), "text"))), Json.parseToJsonElement(scalarText(peer, openingQuery)))
                        val peerAfter = databaseStateVector(peer.connection(), listOf("categories"))
                        for (table in listOf("_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                            assertEquals(recovered.getValue(table), peerAfter.getValue(table), "$mode full independent admitted fixture/contract identity $table")
                        val peerChanged = changed + setOf("_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables")
                        assertEquals(peerBefore.filterKeys { it !in peerChanged }, peerAfter.filterKeys { it !in peerChanged }, "$mode every other typed observer owner unchanged")
                        assertEquals(counted, requests); assertEquals(countedRoutes, routes, "independent readback excluded from counted public primitive")
                        assertEquals(recovered, databaseStateVector(db.connection(), listOf("categories")), "readback preserves every original full identity/control/capture field")
                    } finally { peerHttp?.close(); peer.close() }
                }
                verifyIndependentAuthority()
                println("A3_SCENARIO ${webRuntimeKind()} $identity $mode/independent-authority-readback")
            }
            exercise("checkpoint-ahead", 2, "new remote child", q)
            exercise("explicit-rebuild", 3, "newer remote child", p)
        } finally { writerHttp?.close(); http?.close(); writerDb.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun secondConnectionWritesAtRealResponseBarrierPreserveNewerCapture() = runTest(timeout = 360.seconds) {
        val identity = "dev.goquick.sqlitenow.oversqlite.realserver.OrdinarySafetyLiveTest.secondConnectionWritesAtRealResponseBarrierPreserveNewerCapture"
        val authority = requireRealServerConfig() ?: run {
            println("A3_NOT_EXECUTED ${webRuntimeKind()} $identity opt_in_disabled")
            return@runTest
        }
        println("A3_ENTER ${webRuntimeKind()} $identity")
        suspend fun exercise(case: String) {
            val ddl = "CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
            val db = newCoordinatedDb()
            var http: io.ktor.client.HttpClient? = null
            try {
                db.execSQL(ddl)
                val user = randomRealServerId("a3-response-writer")
                val token = issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(db, authority.baseUrl))
                val p = randomRealServerUuid(); val q = randomRealServerUuid(); val c = randomRealServerUuid(); val d = randomRealServerUuid(); val e = randomRealServerUuid()
                val predicates = mapOf("categories" to "id NOT IN ('$p','$c')",
                    "_sync_row_state" to "key_json NOT IN ('{\"id\":\"$p\"}','{\"id\":\"$c\"}')",
                    "_sync_relationships" to "child_key NOT IN ('{\"id\":\"$p\"}','{\"id\":\"$c\"}')")
                val requests = linkedMapOf<String, Int>()
                val wires = mutableListOf<String>(); val wireSources = mutableListOf<String>()
                var writerSql: String? = null
                var writerVector: Map<String, List<String>>? = null
                var writerBaselineRevision: Long? = null
                var frozenBundle: OversqliteOutboxBundleState? = null
                var frozenIntent: JsonElement? = null
                val intentFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
                val base = newRealServerHttpClient(authority.baseUrl, token,
                    beforeRequest = { path -> requests[path] = requests.getOrElse(path) { 0 } + 1 })
                val transport = base.config {
                    install("A3LiveWriterWireObserver") {
                        // ResponseObserver may run concurrently; this awaited barrier must finish before settlement.
                        receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                            if (response.call.request.url.encodedPath == "/sync/push") writerSql?.let { sql ->
                                assertTrue(response.status.value in 200..299)
                                writerSql = null
                                val runtime = webRuntimeKind()
                                val browser = runtime in setOf("js-browser", "wasm-browser")
                                if (browser) {
                                    val beforeRefusal = databaseStateVector(db.connection(), listOf("categories"))
                                    assertFailsWith<dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException> {
                                        db.reopenForInspection().close()
                                    }
                                    assertEquals(beforeRefusal, databaseStateVector(db.connection(), listOf("categories")),
                                        "refused second worker preserves every retained and business field")
                                    println("A3_RUNTIME_DISPOSITION $runtime $identity $case independent-handle=REFUSED existing-owner-write=covered")
                                }
                                val writer = if (runtime == "js-node" || browser) db.connection() else db.reopenForInspection()
                                try {
                                    if (runtime == "js-node") println("A3 live Node same-lifetime writer; file-independent writer NOT-APPLICABLE")
                                    else if (!browser) assertNotSame(db.connection(), writer, "default provider supplies a second database handle")
                                    writer.execSQL(sql)
                                } finally { if (writer !== db.connection()) writer.close() }
                                writerBaselineRevision = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
                                writerVector = databaseStateVector(db.connection(), listOf("categories"), predicates)
                                frozenBundle = OversqliteOutboxStateStore(db.connection()).loadBundleState()
                                frozenIntent = Json.parseToJsonElement(scalarText(db,
                                    "SELECT ${intentFields.replace("'L',payload", "'L',local_payload")} FROM _sync_outbox_rows"))
                            }
                        }
                        sendPipeline.intercept(HttpSendPipeline.Monitoring) {
                            if (context.url.build().encodedPath == "/sync/push") {
                                val bytes = (subject as OutgoingContent.ByteArrayContent).bytes()
                                wires += (if (context.headers[HttpHeaders.ContentEncoding] == "gzip")
                                    decodeOversqliteSingleMemberGzip(bytes, 1024 * 1024) else bytes).decodeToString()
                                wireSources += context.headers["Oversync-Source-ID"].orEmpty()
                            }
                            proceed()
                        }
                    }
                }
                base.close(); http = transport
                val cfg = OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "business",
                    syncTables = listOf(SyncTable("categories", syncKeyColumnName = "id")), maxSyncWorkUnitsPerInvocation = 100_000_000,
                    transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1))
                val client = db.openClient(cfg, transport, ServerWinsResolver)
                client.attach(user).getOrThrow()
                db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','a1-other',NULL),('$c','child','$p'),('$d','grandchild','$c')")
                client.pushPending().getOrThrow()
                db.execSQL("UPDATE categories SET name='a1-delete' WHERE id='$p'")
                val originalIntent = Json.parseToJsonElement(scalarText(db, "SELECT $intentFields FROM _sync_dirty_rows"))
                val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
                val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
                val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
                val requestStart = requests.toMap()
                writerSql = when (case) {
                    "edit" -> "UPDATE categories SET name='new independent L' WHERE id='$d'"
                    "reparent" -> "UPDATE categories SET parent_id='$q' WHERE id='$d'"
                    "new-dependent" -> "INSERT INTO categories VALUES('$e','new child','$d')"
                    else -> "UPDATE categories SET name='new independent parent' WHERE id='$q'"
                }
                val revisionBefore = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
                val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
                val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                val captured = requireNotNull(writerVector) { "writer completes while the actual push response is held" }
                assertNull(writerSql)
                assertEquals(originalIntent, frozenIntent, "newer writer never recaptures the original frozen historical B/provenance/L")
                val bundle = requireNotNull(frozenBundle)
                assertEquals(source, bundle.sourceId); assertEquals(2L, bundle.sourceBundleId)
                assertEquals(2, wires.size); assertEquals(List(2) { source }, wireSources)
                val wire = Json.parseToJsonElement(wires[1]).jsonObject
                assertEquals(bundle.canonicalRequestHash, wire.getValue("canonical_request_hash").jsonPrimitive.content)
                assertEquals(2L, wire.getValue("source_bundle_id").jsonPrimitive.long)
                assertEquals(mapOf("/sync/push" to 1), requests.mapValues { (path, count) -> count - requestStart.getOrElse(path) { 0 } }.filterValues { it != 0 })
                val after = databaseStateVector(db.connection(), listOf("categories"), predicates)
                // The confirmed parent-delete/child-reparent effects own P/C.
                // Every other full typed primary/capture/edge record stays exact.
                for (table in listOf("categories", "_sync_row_state", "_sync_dirty_rows", "_sync_relationships", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                    assertEquals(captured.getValue(table), after.getValue(table), "$case complete newer L/A/B/provenance/edge/recovery identity")
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(if (case == "new-dependent") 4L else 3L, scalarLong(db, "SELECT count(*) FROM categories"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM categories WHERE id='$p'"))
                assertEquals(q, scalarText(db, "SELECT parent_id FROM categories WHERE id='$c'"))
                assertEquals("child", scalarText(db, "SELECT name FROM categories WHERE id='$c'"))
                assertEquals(sourceBefore.copy(nextSourceBundleId = 3), OversqliteSourceStateStore(db.connection()).loadState(source))
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
                assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 2, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
                assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
                assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
                for (table in listOf("_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                    assertTrue(after.getValue(table).isEmpty(), "$case retained owner retired $table")
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                val provenance = buildJsonObject {
                    put("database_id", cfg.databaseId); put("service_id", cfg.serviceId)
                    putJsonObject("account_scope") { put("binding", "attached"); put("account_id", user) }
                    put("generation_id", cfg.generationId); put("generation_fingerprint", scalarText(db, "SELECT fingerprint FROM _sync_contract_identity"))
                    put("admission", "admitted"); put("local_version", scalarLong(db, "SELECT local_version FROM _sync_contract_identity"))
                    put("physical_shape", scalarText(db, "SELECT physical_shape FROM _sync_contract_identity"))
                }
                fun payload(id: String, name: String, parent: String?) = buildJsonObject {
                    putJsonObject("id") { put("storage", "text"); put("value", id) }
                    putJsonObject("name") { put("storage", "text"); put("value", name) }
                    putJsonObject("parent_id") { put("storage", if (parent == null) "null" else "text"); put("value", parent?.let(::JsonPrimitive) ?: JsonNull) }
                }
                suspend fun assertA(target: PlatformTestDatabase, id: String, expected: JsonElement, version: Long, origin: String, sequence: Long, earliest: String, latest: String) {
                    val predicate = "schema_name='business' AND table_name='categories' AND key_json='{\"id\":\"$id\"}'"
                    assertEquals(expected, Json.parseToJsonElement(scalarText(target, "SELECT coalesce(server_payload,'null') FROM _sync_row_state WHERE $predicate")))
                    assertEquals(JsonObject(provenance + mapOf("origin_source_id" to JsonPrimitive(scalarText(target, "SELECT current_source_id FROM _sync_attachment_state")),
                        "origin" to JsonPrimitive(origin), "bundle_sequence" to JsonPrimitive(sequence), "authoritative_version" to JsonPrimitive(version))),
                        Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")))
                    assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                    assertEquals(if (expected == JsonNull) "deleted" else "live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                    assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                    val publishedAt = scalarText(target, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                    assertTrue(publishedAt >= earliest && publishedAt <= latest, "$case publication timestamp belongs to this invocation")
                    for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_provenance", "updated_at"))
                        assertEquals("text", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    assertEquals(if (expected == JsonNull) "null" else "text", scalarText(target, "SELECT typeof(server_payload) FROM _sync_row_state WHERE $predicate"))
                    for (column in listOf("row_version", "relationships_known", "server_revision"))
                        assertEquals("integer", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                }
                assertA(db, p, JsonNull, 2, "settlement", 2, publicationStart, publicationEnd)
                assertA(db, c, payload(c, "child", q), 2, "settlement", 2, publicationStart, publicationEnd)
                // A newly inserted dependent first creates its own unpublished baseline.
                val revisionAfterWriter = revisionBefore + if (case == "new-dependent") 1 else 0
                assertEquals(revisionAfterWriter, writerBaselineRevision, "only a new key advances the writer baseline")
                assertEquals(setOf(revisionAfterWriter + 1, revisionAfterWriter + 2), setOf(
                    scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$p\"}'"),
                    scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$c\"}'")))
                assertEquals(revisionAfterWriter + 2, scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state"))
                val edgeQuery = "SELECT json_group_array(json(edge)) FROM (SELECT json_object('view',view,'child_schema',child_schema,'child_table',child_table,'child_key',child_key,'fk_column',fk_column,'parent_schema',parent_schema,'parent_table',parent_table,'parent_key',parent_key,'owner_revision',owner_revision) AS edge FROM _sync_relationships %s ORDER BY child_key,view)"
                suspend fun serverEdge(target: PlatformTestDatabase, child: String, parent: String) = buildJsonObject {
                    put("view", "server"); put("child_schema", "business"); put("child_table", "categories"); put("child_key", "{\"id\":\"$child\"}")
                    put("fk_column", "parent_id"); put("parent_schema", "business"); put("parent_table", "categories"); put("parent_key", "{\"id\":\"$parent\"}")
                    put("owner_revision", scalarLong(target, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$child\"}'"))
                }
                assertEquals(JsonArray(listOf(serverEdge(db, c, q))), Json.parseToJsonElement(scalarText(db,
                    edgeQuery.replace("%s", "WHERE child_key IN ('{\"id\":\"$p\"}','{\"id\":\"$c\"}')"))), "complete confirmed P/C edge replacement")
                println("A3_SCENARIO ${webRuntimeKind()} $identity $case/response-writer")
                val settled = databaseStateVector(db.connection(), listOf("categories"))
                val countedRequests = requests.toMap()
                suspend fun verifyIndependentAuthority() {
                    val peer = newCoordinatedDb()
                    var peerHttp: io.ktor.client.HttpClient? = null
                    try {
                        peer.execSQL(ddl)
                        peerHttp = newRealServerHttpClient(authority.baseUrl, issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(peer, authority.baseUrl)))
                        val observer = peer.openClient(cfg, peerHttp, ServerWinsResolver)
                        val snapshotStart = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        observer.attach(user).getOrThrow(); observer.pullToStable().getOrThrow()
                        val snapshotEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        val expectedBusiness = JsonArray(listOf(payload(q, "a1-other", null), payload(c, "child", q), payload(d, "grandchild", c)).sortedBy { it.getValue("id").jsonObject.getValue("value").jsonPrimitive.content })
                        val columns = listOf("id", "name", "parent_id").joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
                        assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(peer, "SELECT json_group_array(json_object($columns)) FROM (SELECT * FROM categories ORDER BY id)")))
                        assertA(peer, q, payload(q, "a1-other", null), 1, "snapshot", 2, snapshotStart, snapshotEnd)
                        assertA(peer, c, payload(c, "child", q), 2, "snapshot", 2, snapshotStart, snapshotEnd)
                        assertA(peer, d, payload(d, "grandchild", c), 1, "snapshot", 2, snapshotStart, snapshotEnd)
                        assertEquals(setOf(1L, 2L, 3L), listOf(q, c, d).map { id ->
                            scalarLong(peer, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$id\"}'") }.toSet())
                        assertEquals(JsonArray(listOf(c to q, d to c).sortedBy { it.first }.map { (child, parent) -> serverEdge(peer, child, parent) }),
                            Json.parseToJsonElement(scalarText(peer, edgeQuery.replace("%s", ""))), "complete independent authoritative edge set")
                        assertEquals(3L, scalarLong(peer, "SELECT count(*) FROM _sync_row_state"))
                        assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_dirty_rows"))
                        assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_outbox_rows"))
                        assertEquals(2L, scalarLong(peer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM pragma_foreign_key_check"))
                        assertEquals(countedRequests, requests, "independent readback is outside the counted public primitive")
                        assertEquals(settled, databaseStateVector(db.connection(), listOf("categories")), "readback preserves complete newer writer and original owners")
                    } finally { peerHttp?.close(); peer.close() }
                }
                verifyIndependentAuthority()
                println("A3_SCENARIO ${webRuntimeKind()} $identity $case/authority-readback")
            } finally { http?.close(); db.close() }
        }
        try { for (case in listOf("edit", "reparent", "new-dependent", "parent-write")) exercise(case) }
        finally { cleanupOwnedWorkerStorage() }
    }

    @Test fun ordinaryEmptyOneRowAndSettledHaveExactRequestsAndServerReadback() = runTest(timeout = 180.seconds) {
        val identity = "dev.goquick.sqlitenow.oversqlite.realserver.OrdinarySafetyLiveTest.ordinaryEmptyOneRowAndSettledHaveExactRequestsAndServerReadback"
        val authority = requireRealServerConfig() ?: run {
            println("A3_NOT_EXECUTED ${webRuntimeKind()} $identity opt_in_disabled")
            return@runTest
        }
        println("A3_ENTER ${webRuntimeKind()} $identity")
        val ddl = "CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
        val db = newCoordinatedDb()
        var http: io.ktor.client.HttpClient? = null
        try {
            db.execSQL(ddl)
            val user = randomRealServerId("a3-ordinary-counts")
            val token = issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(db, authority.baseUrl))
            val requests = linkedMapOf<String, Int>()
            val wires = mutableListOf<String>()
            val sources = mutableListOf<String>()
            val frozen = mutableListOf<JsonElement>()
            val originalFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
            val base = newRealServerHttpClient(authority.baseUrl, token, beforeRequest = { path ->
                requests[path] = requests.getOrElse(path) { 0 } + 1
            })
            val transport = base.config {
                install("A3OrdinaryCountWireObserver") {
                    sendPipeline.intercept(HttpSendPipeline.Monitoring) {
                        if (context.url.build().encodedPath == "/sync/push") {
                            val bytes = (subject as OutgoingContent.ByteArrayContent).bytes()
                            wires += (if (context.headers[HttpHeaders.ContentEncoding] == "gzip")
                                decodeOversqliteSingleMemberGzip(bytes, 1024 * 1024) else bytes).decodeToString()
                            sources += context.headers["Oversync-Source-ID"].orEmpty()
                            frozen += Json.parseToJsonElement(scalarText(db,
                                "SELECT ${originalFields.replace("'L',payload", "'L',local_payload")} FROM _sync_outbox_rows"))
                        }
                        proceed()
                    }
                }
            }
            base.close(); http = transport
            val cfg = OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "business",
                syncTables = listOf(SyncTable("categories", syncKeyColumnName = "id")),
                maxSyncWorkUnitsPerInvocation = 100_000_000,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1))
            val client = db.openClient(cfg, transport, ServerWinsResolver)
            client.attach(user).getOrThrow()
            fun delta(before: Map<String, Int>) = requests.mapValues { (path, count) -> count - before.getOrElse(path) { 0 } }.filterValues { it != 0 }
            val attached = databaseStateVector(db.connection(), listOf("categories"))
            val emptyStart = requests.toMap()
            val empty = client.sync().getOrThrow()
            assertEquals(PushOutcome.NO_CHANGE, empty.pushOutcome)
            assertFalse(empty.status.pending.hasPendingSyncData)
            assertEquals(mapOf("/sync/pull" to 1), delta(emptyStart))
            assertEquals(attached, databaseStateVector(db.connection(), listOf("categories")), "empty invocation preserves every complete owner")
            assertTrue(wires.isEmpty())
            println("A3_SCENARIO ${webRuntimeKind()} $identity empty")

            val id = randomRealServerUuid()
            db.execSQL("INSERT INTO categories VALUES('$id','complete one-row payload',NULL)")
            val originalIntent = Json.parseToJsonElement(scalarText(db, "SELECT $originalFields FROM _sync_dirty_rows"))
            val before = databaseStateVector(db.connection(), listOf("categories"))
            val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
            val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
            val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
            val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id",
                "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
            val conflictProjection = conflictColumns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            val conflictQuery = "SELECT json_object($conflictProjection) FROM _sync_conflict_state WHERE singleton_key=1"
            val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
            val oneStart = requests.toMap()
            val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            val one = client.sync().getOrThrow()
            val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            assertEquals(PushOutcome.COMMITTED, one.pushOutcome)
            assertFalse(one.status.pending.hasPendingSyncData)
            assertEquals(mapOf("/sync/push" to 1, "/sync/pull" to 1), delta(oneStart))
            assertEquals(listOf(source), sources)
            assertEquals(listOf(originalIntent), frozen)
            assertEquals(1, wires.size)
            val wire = Json.parseToJsonElement(wires.single()).jsonObject
            assertEquals(1L, wire.getValue("source_bundle_id").jsonPrimitive.long)
            assertTrue(Regex("[0-9a-f]{64}").matches(wire.getValue("canonical_request_hash").jsonPrimitive.content))
            assertEquals(1, wire.getValue("rows").jsonArray.sumOf { it.jsonArray[2].jsonArray.size })
            assertEquals(sourceBefore.copy(nextSourceBundleId = 2), OversqliteSourceStateStore(db.connection()).loadState(source))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
            assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 1, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
            assertEquals(JsonObject(conflictBefore + listOf("dirty_revision", "baseline_revision").associateWith { column ->
                buildJsonObject { put("storage", "integer"); put("value", conflictBefore.getValue(column).jsonObject.getValue("value").jsonPrimitive.long + 1) }
            }), Json.parseToJsonElement(scalarText(db, conflictQuery)), "only one freeze and publication advance their revisions")
            assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
            val settled = databaseStateVector(db.connection(), listOf("categories"))
            for (table in listOf("categories", "_sync_relationships", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                assertEquals(before.getValue(table), settled.getValue(table), "complete business/recovery identity $table")
            for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                assertTrue(settled.getValue(table).isEmpty(), "retired owner $table")
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            val payload = buildJsonObject {
                putJsonObject("id") { put("storage", "text"); put("value", id) }
                putJsonObject("name") { put("storage", "text"); put("value", "complete one-row payload") }
                putJsonObject("parent_id") { put("storage", "null"); put("value", JsonNull) }
            }
            val provenance = buildJsonObject {
                put("database_id", cfg.databaseId); put("service_id", cfg.serviceId)
                putJsonObject("account_scope") { put("binding", "attached"); put("account_id", user) }
                put("generation_id", cfg.generationId); put("generation_fingerprint", scalarText(db, "SELECT fingerprint FROM _sync_contract_identity"))
                put("admission", "admitted"); put("local_version", scalarLong(db, "SELECT local_version FROM _sync_contract_identity"))
                put("physical_shape", scalarText(db, "SELECT physical_shape FROM _sync_contract_identity"))
                put("bundle_sequence", 1); put("authoritative_version", 1)
            }
            suspend fun assertPublished(target: PlatformTestDatabase, origin: String, revision: Long, clockStart: String, clockEnd: String) {
                assertEquals(1L, scalarLong(target, "SELECT count(*) FROM _sync_row_state"))
                val predicate = "schema_name='business' AND table_name='categories' AND key_json='{\"id\":\"$id\"}'"
                assertEquals(payload, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE $predicate")))
                assertEquals(JsonObject(provenance + mapOf("origin" to JsonPrimitive(origin), "origin_source_id" to JsonPrimitive(scalarText(target, "SELECT current_source_id FROM _sync_attachment_state")))),
                    Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")))
                assertEquals(1L, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                assertEquals(revision, scalarLong(target, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                val publishedAt = scalarText(target, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                assertTrue(publishedAt >= clockStart && publishedAt <= clockEnd, "intended publication timestamp lies inside database-clock observations")
                for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                    assertEquals("text", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                for (column in listOf("row_version", "relationships_known", "server_revision"))
                    assertEquals("integer", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
            }
            assertPublished(db, "settlement", conflictBefore.getValue("baseline_revision").jsonObject.getValue("value").jsonPrimitive.long + 1, publicationStart, publicationEnd)
            println("A3_SCENARIO ${webRuntimeKind()} $identity one-row")
            val settledStart = requests.toMap()
            assertEquals(PushOutcome.NO_CHANGE, client.sync().getOrThrow().pushOutcome)
            assertEquals(mapOf("/sync/pull" to 1), delta(settledStart))
            assertEquals(settled, databaseStateVector(db.connection(), listOf("categories")), "settled invocation preserves every owner, A timestamp and source/checkpoint")
            assertEquals(1, wires.size)
            println("A3_SCENARIO ${webRuntimeKind()} $identity settled")

            suspend fun verifyIndependentReadback() {
                val peer = newCoordinatedDb()
                var peerHttp: io.ktor.client.HttpClient? = null
                try {
                    peer.execSQL(ddl)
                    peerHttp = newRealServerHttpClient(authority.baseUrl, issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(peer, authority.baseUrl)))
                    val observer = peer.openClient(cfg, peerHttp, ServerWinsResolver)
                    val observationStart = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    observer.attach(user).getOrThrow(); observer.pullToStable().getOrThrow()
                    val observationEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                    val viewed = databaseStateVector(peer.connection(), listOf("categories"))
                    assertEquals(settled.getValue("categories"), viewed.getValue("categories"), "complete final server typed business row")
                    assertPublished(peer, "snapshot", 1, observationStart, observationEnd)
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_relationships"))
                    assertEquals(1L, scalarLong(peer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(mapOf("/sync/pull" to 1), delta(settledStart), "peer verification remains outside counted invocations")
                    assertEquals(settled, databaseStateVector(db.connection(), listOf("categories")), "peer observation preserves every original owner")
                } finally { peerHttp?.close(); peer.close() }
            }
            verifyIndependentReadback()
            println("A3_SCENARIO ${webRuntimeKind()} $identity independent-readback")
        } finally { http?.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun onePublicSyncDrainsMultipleBatchesWithExactServerVector() = runTest(timeout = 180.seconds) {
        val identity = "dev.goquick.sqlitenow.oversqlite.realserver.OrdinarySafetyLiveTest.onePublicSyncDrainsMultipleBatchesWithExactServerVector"
        val authority = requireRealServerConfig() ?: run {
            println("A3_NOT_EXECUTED ${webRuntimeKind()} $identity opt_in_disabled")
            return@runTest
        }
        println("A3_ENTER ${webRuntimeKind()} $identity")
        val ddl = "CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
        val db = newCoordinatedDb()
        var http: io.ktor.client.HttpClient? = null
        try {
            db.execSQL(ddl)
            val user = randomRealServerId("a3-public-drain")
            val tables = listOf(SyncTable("categories", syncKeyColumnName = "id"))
            val bootstrapSource = bootstrapManagedSourceId(db, authority.baseUrl)
            val token = issueDummySigninToken(authority.baseUrl, user, bootstrapSource)
            val requests = linkedMapOf<String, Int>()
            val wires = mutableListOf<String>()
            val wireSources = mutableListOf<String>()
            val frozenIntents = mutableListOf<JsonElement>()
            // Mechanical transport observation uses the existing SDK content/gzip
            // owner. It observes real requests; it supplies no synthetic response.
            val base = newRealServerHttpClient(authority.baseUrl, token, beforeRequest = { path ->
                requests[path] = requests.getOrElse(path) { 0 } + 1
            })
            val transport = base.config {
                install("A3PublicDrainWireObserver") {
                    sendPipeline.intercept(HttpSendPipeline.Monitoring) {
                        if (context.url.build().encodedPath == "/sync/push") {
                            val bytes = (subject as OutgoingContent.ByteArrayContent).bytes()
                            val decoded = if (context.headers[HttpHeaders.ContentEncoding] == "gzip")
                                decodeOversqliteSingleMemberGzip(bytes, 1024 * 1024) else bytes
                            wires += decoded.decodeToString()
                            wireSources += context.headers["Oversync-Source-ID"].orEmpty()
                            val fields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',local_payload)"
                            frozenIntents += Json.parseToJsonElement(scalarText(db,
                                "SELECT json_group_array($fields) FROM (SELECT * FROM _sync_outbox_rows ORDER BY key_json)"))
                        }
                        proceed()
                    }
                }
            }
            base.close(); http = transport
            val cfg = OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "business",
                syncTables = tables, transferTargets = OversqliteTransferTargets(maxPushRows = 2),
                maxSyncWorkUnitsPerInvocation = 100_000_000,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1))
            val client = db.openClient(cfg, transport, ServerWinsResolver)
            client.attach(user).getOrThrow()
            // Sorted UUIDs make the independently expected batch/version map exact.
            val ids = List(6) { randomRealServerUuid() }.sorted()
            ids.forEachIndexed { i, id -> db.execSQL("INSERT INTO categories VALUES('$id','complete payload $i',NULL)") }
            val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
            val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
            val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
            val conflictColumns = listOf("singleton_key", "dirty_revision", "baseline_revision", "decision_state", "source_id", "source_bundle_id",
                "canonical_request_hash", "generation_id", "generation_fingerprint", "conflict_json", "local_payload_json")
            val conflictProjection = conflictColumns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            val conflictQuery = "SELECT json_object($conflictProjection) FROM _sync_conflict_state WHERE singleton_key=1"
            val conflictBefore = Json.parseToJsonElement(scalarText(db, conflictQuery)).jsonObject
            val before = databaseStateVector(db.connection(), listOf("categories"))
            val originalFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
            val originalIntents = Json.parseToJsonElement(scalarText(db,
                "SELECT json_group_array($originalFields) FROM (SELECT * FROM _sync_dirty_rows ORDER BY key_json)")).jsonArray
            val requestStart = requests.toMap()
            assertEquals(1L, sourceBefore.nextSourceBundleId)
            assertEquals(0L, attachmentBefore.lastBundleSeqSeen)
            assertEquals(6L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            val publicationStart = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            // Exactly one public invocation owns every push and the terminal pull.
            val result = client.sync().getOrThrow()
            val stats = client.workspaceDiagnostics()
            val publicationEnd = scalarText(db, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            assertEquals(PushOutcome.COMMITTED, result.pushOutcome)
            assertFalse(result.status.pending.hasPendingSyncData)
            assertEquals(3, wires.size)
            assertEquals(List(3) { source }, wireSources)
            val parsed = wires.map { Json.parseToJsonElement(it).jsonObject }
            assertEquals(listOf(1L, 2L, 3L), parsed.map { it.getValue("source_bundle_id").jsonPrimitive.long })
            val hashes = parsed.map { it.getValue("canonical_request_hash").jsonPrimitive.content }
            assertEquals(3, hashes.toSet().size)
            assertTrue(hashes.all { Regex("[0-9a-f]{64}").matches(it) })
            parsed.forEach { wire -> assertEquals(2, wire.getValue("rows").jsonArray.sumOf { it.jsonArray[2].jsonArray.size }) }
            assertEquals<List<JsonElement>>(List(3) { i -> JsonArray(originalIntents.drop(i * 2).take(2)) }, frozenIntents,
                "each real request freezes complete original B/provenance/L without later recapture")
            val deltas = requests.mapValues { (path, count) -> count - requestStart.getOrElse(path) { 0 } }.filterValues { it != 0 }
            assertEquals(mapOf("/sync/push" to 3, "/sync/pull" to 1), deltas,
                "counted invocation has no snapshot, connect, reconcile or hidden extra request")
            assertTrue(stats.work > 0 && stats.work <= cfg.maxSyncWorkUnitsPerInvocation)
            assertTrue(stats.savedCopies > 0 && stats.primaryReads > 0 && stats.serializedBytes > 0)
            assertEquals(sourceBefore.copy(nextSourceBundleId = 4), OversqliteSourceStateStore(db.connection()).loadState(source))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_source_state"))
            assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 3, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
            val expectedConflict = JsonObject(conflictBefore + listOf("dirty_revision", "baseline_revision").associateWith { column ->
                buildJsonObject { put("storage", "integer"); put("value", conflictBefore.getValue(column).jsonObject.getValue("value").jsonPrimitive.long + 6) }
            })
            assertEquals(expectedConflict, Json.parseToJsonElement(scalarText(db, conflictQuery)),
                "six frozen intent deletions and six authoritative publications advance only their exact revision counters")
            assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
            val settled = databaseStateVector(db.connection(), listOf("categories"))
            for (table in listOf("categories", "_sync_relationships", "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                assertEquals(before.getValue(table), settled.getValue(table), "complete business/recovery/identity $table")
            for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                assertTrue(settled.getValue(table).isEmpty(), "retired input/retained owner $table")
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            val provenance = buildJsonObject {
                put("database_id", cfg.databaseId); put("service_id", cfg.serviceId)
                putJsonObject("account_scope") { put("binding", "attached"); put("account_id", user) }
                put("generation_id", cfg.generationId)
                put("generation_fingerprint", scalarText(db, "SELECT fingerprint FROM _sync_contract_identity"))
                put("admission", "admitted"); put("local_version", scalarLong(db, "SELECT local_version FROM _sync_contract_identity"))
                put("physical_shape", scalarText(db, "SELECT physical_shape FROM _sync_contract_identity"))
                put("origin_source_id", source); put("origin", "settlement")
            }
            suspend fun assertAuthoritative(target: PlatformTestDatabase, revisionStart: Long, clockStart: String, clockEnd: String, snapshot: Boolean = false) {
                val originSource = scalarText(target, "SELECT current_source_id FROM _sync_attachment_state")
                assertEquals(6L, scalarLong(target, "SELECT count(*) FROM _sync_row_state"))
                ids.forEachIndexed { i, id ->
                    val version = (i / 2 + 1).toLong()
                    val payload = buildJsonObject {
                        putJsonObject("id") { put("storage", "text"); put("value", id) }
                        putJsonObject("name") { put("storage", "text"); put("value", "complete payload $i") }
                        putJsonObject("parent_id") { put("storage", "null"); put("value", JsonNull) }
                    }
                    val expectedProvenance = JsonObject(provenance + mapOf(
                        "origin_source_id" to JsonPrimitive(originSource), "origin" to JsonPrimitive(if (snapshot) "snapshot" else "settlement"),
                        "bundle_sequence" to JsonPrimitive(if (snapshot) 3L else version), "authoritative_version" to JsonPrimitive(version)))
                    val predicate = "schema_name='business' AND table_name='categories' AND key_json='{\"id\":\"$id\"}'"
                    assertEquals(payload, Json.parseToJsonElement(scalarText(target, "SELECT server_payload FROM _sync_row_state WHERE $predicate")))
                    assertEquals(expectedProvenance, Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")))
                    assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                    assertEquals("live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                    assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                    assertEquals(revisionStart + i + 1, scalarLong(target, "SELECT server_revision FROM _sync_row_state WHERE $predicate"))
                    // The publication timestamp is an intended mutation, bounded
                    // by database-clock observations outside the counted call.
                    val publishedAt = scalarText(target, "SELECT updated_at FROM _sync_row_state WHERE $predicate")
                    assertTrue(publishedAt >= clockStart && publishedAt <= clockEnd)
                    for (column in listOf("schema_name", "table_name", "key_json", "server_kind", "server_payload", "server_provenance", "updated_at"))
                        assertEquals("text", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                    for (column in listOf("row_version", "relationships_known", "server_revision"))
                        assertEquals("integer", scalarText(target, "SELECT typeof($column) FROM _sync_row_state WHERE $predicate"))
                }
            }
            assertAuthoritative(db, conflictBefore.getValue("baseline_revision").jsonObject.getValue("value").jsonPrimitive.long, publicationStart, publicationEnd)
            println("A3_SCENARIO ${webRuntimeKind()} $identity counted-drain")
            val countedRequests = requests.toMap()
            val peer = newCoordinatedDb()
            var peerHttp: io.ktor.client.HttpClient? = null
            try {
                peer.execSQL(ddl)
                val peerSource = bootstrapManagedSourceId(peer, authority.baseUrl)
                peerHttp = newRealServerHttpClient(authority.baseUrl, issueDummySigninToken(authority.baseUrl, user, peerSource))
                val observer = peer.openClient(cfg, peerHttp, ServerWinsResolver)
                val observationStart = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                observer.attach(user).getOrThrow(); observer.pullToStable().getOrThrow()
                val observationEnd = scalarText(peer, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                val viewed = databaseStateVector(peer.connection(), listOf("categories"))
                assertEquals(before.getValue("categories"), viewed.getValue("categories"), "complete authority business readback")
                assertAuthoritative(peer, 0, observationStart, observationEnd, snapshot = true)
                assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_relationships"))
                assertEquals(3L, scalarLong(peer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM pragma_foreign_key_check"))
                assertEquals(countedRequests, requests, "peer verification is outside counted invocation")
                assertEquals(settled, databaseStateVector(db.connection(), listOf("categories")), "peer observation preserves complete original state")
                println("A3_SCENARIO ${webRuntimeKind()} $identity independent-readback")
            } finally { peerHttp?.close(); peer.close() }
        } finally { http?.close(); db.close(); cleanupOwnedWorkerStorage() }
    }

    @Test fun committedReplayWith1000RowsWidth16() = runCommittedReplay(1000, 16)
    @Test fun committedReplayWith1000RowsWidth1024() = runCommittedReplay(1000, 1024)
    @Test fun committedReplayWith10000RowsWidth16() = runCommittedReplay(10000, 16)
    @Test fun committedReplayWith10000RowsWidth1024() = runCommittedReplay(10000, 1024)

    private fun runCommittedReplay(size: Int, width: Int) = runTest(timeout = 900.seconds) {
        val identity = "dev.goquick.sqlitenow.oversqlite.realserver.OrdinarySafetyLiveTest.committedReplayWith${size}RowsWidth$width"
        val authority = requireRealServerConfig() ?: run {
            println("A3_NOT_EXECUTED ${webRuntimeKind()} $identity opt_in_disabled")
            return@runTest
        }
        // JS browser owns all four configurations; other runtimes use the
        // 1k/16 case normally and all four in heavy mode.
        if ((size != 1000 || width != 16) && !authority.heavyMode && webRuntimeKind() != "js-browser") {
            println("A3_NOT_EXECUTED ${webRuntimeKind()} $identity heavy_mode_disabled")
            return@runTest
        }
        println("A3_ENTER ${webRuntimeKind()} $identity")
        val db = newCoordinatedDb()
        var http: io.ktor.client.HttpClient? = null
        var primaryFailure: Throwable? = null
        try {
            db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val user = randomRealServerId("a3-platform")
            val token = issueDummySigninToken(authority.baseUrl, user, bootstrapManagedSourceId(db, authority.baseUrl))
            var lose = false
            val requests = linkedMapOf<String, Int>()
            val pushBodies = mutableListOf<ByteArray>()
            val pushEncodings = mutableListOf<String?>()
            val pushWires = mutableListOf<String>()
            val pushSources = mutableListOf<String>()
            val original = newRealServerHttpClient(authority.baseUrl, token, beforeRequest = { path ->
                requests[path] = requests.getOrElse(path) { 0 } + 1
            })
            val transport = original.config {
                install("A3PlatformResponseLoss") {
                    sendPipeline.intercept(HttpSendPipeline.Monitoring) {
                        if (context.url.build().encodedPath == "/sync/push") {
                            val bytes = (subject as OutgoingContent.ByteArrayContent).bytes()
                            pushBodies += bytes.copyOf()
                            pushEncodings += context.headers[HttpHeaders.ContentEncoding]
                            val decoded = if (context.headers[HttpHeaders.ContentEncoding] == "gzip")
                                decodeOversqliteSingleMemberGzip(bytes, 1024 * 1024) else bytes
                            pushWires += decoded.decodeToString()
                            pushSources += context.headers["Oversync-Source-ID"].orEmpty()
                        }
                        proceed()
                    }
                    receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                        if (lose && response.call.request.url.encodedPath == "/sync/push") {
                            assertTrue(response.status.value in 200..299)
                            lose = false
                            throw CancellationException("committed response loss")
                        }
                    }
                }
            }
            original.close(); http = transport
            val config = OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "business",
                syncTables = listOf(SyncTable("categories", syncKeyColumnName = "id")),
                maxSyncWorkUnitsPerInvocation = 100_000_000,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1))
            var client = db.openClient(config, transport, ServerWinsResolver)
            client.attach(user).getOrThrow()
            val p = randomRealServerUuid(); val q = randomRealServerUuid()
            val c = randomRealServerUuid(); val d = randomRealServerUuid()
            db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','a1-other',NULL),('$c','child','$p'),('$d','grandchild','$c')")
            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state WHERE schema_name='business' AND table_name='categories'"), "seed uses the admitted server schema, not the SQLite namespace")
            val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val provenance = buildJsonObject {
                put("database_id", config.databaseId); put("service_id", config.serviceId)
                putJsonObject("account_scope") { put("binding", "attached"); put("account_id", user) }
                put("generation_id", config.generationId)
                put("generation_fingerprint", scalarText(db, "SELECT fingerprint FROM _sync_contract_identity"))
                put("admission", "admitted"); put("local_version", scalarLong(db, "SELECT local_version FROM _sync_contract_identity"))
                put("physical_shape", scalarText(db, "SELECT physical_shape FROM _sync_contract_identity"))
            }
            val authoritativeColumns = listOf("schema_name", "table_name", "key_json", "row_version", "server_kind",
                "server_payload", "server_provenance", "relationships_known", "server_revision", "updated_at")
            val authoritativeProjection = authoritativeColumns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            suspend fun completeA(target: PlatformTestDatabase, id: String) = Json.parseToJsonElement(scalarText(target,
                "SELECT json_object($authoritativeProjection) FROM _sync_row_state WHERE schema_name='business' AND table_name='categories' AND key_json='{\"id\":\"$id\"}'"))
            fun typedCategory(id: String, name: String, parent: String?) = buildJsonObject {
                putJsonObject("id") { put("storage", "text"); put("value", id) }
                putJsonObject("name") { put("storage", "text"); put("value", name) }
                putJsonObject("parent_id") { put("storage", if (parent == null) "null" else "text"); put("value", parent?.let(::JsonPrimitive) ?: JsonNull) }
            }
            suspend fun assertPublished(target: PlatformTestDatabase, id: String, name: String?, parent: String?, version: Long,
                origin: String, sequence: Long, originSource: String) {
                val predicate = "schema_name='business' AND table_name='categories' AND key_json='{\"id\":\"$id\"}'"
                val payload = if (name == null) JsonNull else typedCategory(id, name, parent)
                assertEquals(payload, Json.parseToJsonElement(scalarText(target, "SELECT coalesce(server_payload,'null') FROM _sync_row_state WHERE $predicate")))
                assertEquals(JsonObject(provenance + mapOf("origin_source_id" to JsonPrimitive(originSource), "origin" to JsonPrimitive(origin),
                    "bundle_sequence" to JsonPrimitive(sequence), "authoritative_version" to JsonPrimitive(version))),
                    Json.parseToJsonElement(scalarText(target, "SELECT server_provenance FROM _sync_row_state WHERE $predicate")))
                assertEquals(version, scalarLong(target, "SELECT row_version FROM _sync_row_state WHERE $predicate"))
                assertEquals(if (name == null) "deleted" else "live", scalarText(target, "SELECT server_kind FROM _sync_row_state WHERE $predicate"))
                assertEquals(1L, scalarLong(target, "SELECT relationships_known FROM _sync_row_state WHERE $predicate"))
                val row = completeA(target, id).jsonObject
                for (column in authoritativeColumns.filter { it !in listOf("row_version", "relationships_known", "server_revision", "server_payload") })
                    assertEquals("text", row.getValue(column).jsonObject.getValue("storage").jsonPrimitive.content)
                assertEquals(if (name == null) "null" else "text", row.getValue("server_payload").jsonObject.getValue("storage").jsonPrimitive.content)
                for (column in listOf("row_version", "relationships_known", "server_revision"))
                    assertEquals("integer", row.getValue(column).jsonObject.getValue("storage").jsonPrimitive.content)
            }
            assertPublished(db, q, "a1-other", null, 1, "settlement", 1, source)
            assertPublished(db, d, "grandchild", c, 1, "settlement", 1, source)
            val unchangedAuthoritative = mapOf(q to completeA(db, q), d to completeA(db, d))
            db.execSQL("UPDATE categories SET name='a1-delete' WHERE id='$p'")
            val intentFields = "json_object('schema',schema_name,'table',table_name,'key',key_json,'op',op,'version',base_row_version,'kind',base_kind,'B',base_payload,'provenance',base_provenance,'L',payload)"
            val originalIntent = Json.parseToJsonElement(scalarText(db, "SELECT $intentFields FROM _sync_dirty_rows WHERE key_json='{\"id\":\"$p\"}'")).jsonObject
            assertEquals("present", originalIntent.getValue("kind").jsonPrimitive.content)
            assertEquals(1L, originalIntent.getValue("version").jsonPrimitive.long)
            assertEquals(typedCategory(p, "parent", null), Json.parseToJsonElement(originalIntent.getValue("B").jsonPrimitive.content))
            assertEquals(typedCategory(p, "a1-delete", null), Json.parseToJsonElement(originalIntent.getValue("L").jsonPrimitive.content))
            assertEquals(JsonObject(provenance + mapOf("origin_source_id" to JsonPrimitive(source), "origin" to JsonPrimitive("settlement"),
                "bundle_sequence" to JsonPrimitive(1), "authoritative_version" to JsonPrimitive(1))),
                Json.parseToJsonElement(originalIntent.getValue("provenance").jsonPrimitive.content))
            lose = true
            val cancellation = assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            assertEquals("committed response loss", cancellation.message)
            assertFalse(lose)
            val hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertEquals(2L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
            assertEquals(2, pushWires.size)
            val frozenWire = pushWires[1]
            val frozenBundle = requireNotNull(OversqliteOutboxStateStore(db.connection()).loadBundleState())
            val frozenIntentFields = intentFields.replace("'L',payload", "'L',local_payload")
            assertEquals(originalIntent, Json.parseToJsonElement(scalarText(db, "SELECT $frozenIntentFields FROM _sync_outbox_rows")),
                "complete original historical B/provenance/L frozen before committed response loss")
            assertEquals(source, frozenBundle.sourceId)
            assertEquals(hash, Json.parseToJsonElement(frozenWire).jsonObject.getValue("canonical_request_hash").jsonPrimitive.content)
            assertEquals(2L, Json.parseToJsonElement(frozenWire).jsonObject.getValue("source_bundle_id").jsonPrimitive.long)
            val expectedBusiness = JsonArray(listOf(Triple(q, "a1-other", null), Triple(c, "child", q), Triple(d, "grandchild", c))
                .sortedBy { it.first }.map { (id, name, parent) -> typedCategory(id, name, parent) })
            val businessProjection = listOf("id", "name", "parent_id").joinToString(",") { "'$it',json_object('storage',typeof($it),'value',$it)" }
            val businessQuery = "SELECT json_group_array(json_object($businessProjection)) FROM (SELECT * FROM categories WHERE id IN ('$q','$c','$d') ORDER BY id)"
            suspend fun verifyIndependentReadback(localSettled: Boolean) {
                val originalState = databaseStateVector(db.connection(), listOf("categories"))
                val peer = newCoordinatedDb()
                var peerHttp: io.ktor.client.HttpClient? = null
                var peerFailure: Throwable? = null
                try {
                    val transport = newRealServerHttpClient(authority.baseUrl, token)
                    peerHttp = transport
                    peer.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                    val observer = peer.openClient(config, transport, ServerWinsResolver)
                    observer.attach(user).getOrThrow(); observer.rebuild().getOrThrow()
                    assertEquals(3L, scalarLong(peer, "SELECT count(*) FROM categories"))
                    assertEquals("grandchild", scalarText(peer, "SELECT name FROM categories WHERE id='$d'"))
                    assertEquals(c, scalarText(peer, "SELECT parent_id FROM categories WHERE id='$d'"))
                    assertEquals("child", scalarText(peer, "SELECT name FROM categories WHERE id='$c'"))
                    assertEquals(q, scalarText(peer, "SELECT parent_id FROM categories WHERE id='$c'"))
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(2L, scalarLong(peer, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(3L, scalarLong(peer, "SELECT count(*) FROM _sync_row_state WHERE server_kind='live' AND server_payload IS NOT NULL AND server_provenance IS NOT NULL"))
                    val peerSource = scalarText(peer, "SELECT current_source_id FROM _sync_attachment_state")
                    assertPublished(peer, q, "a1-other", null, 1, "snapshot", 2, peerSource)
                    assertPublished(peer, c, "child", q, 2, "snapshot", 2, peerSource)
                    assertPublished(peer, d, "grandchild", c, 1, "snapshot", 2, peerSource)
                    assertEquals(3L, scalarLong(peer, "SELECT count(*) FROM _sync_row_state"))
                    assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(peer, businessQuery)), "every final authority typed business field")
                    assertEquals(0L, scalarLong(peer, "SELECT count(*) FROM categories WHERE id='$p'"))
                    val edgeQuery = "SELECT json_group_array(json(edge)) FROM (SELECT json_object('view',view,'child_schema',child_schema,'child_table',child_table,'child_key',child_key,'fk_column',fk_column,'parent_schema',parent_schema,'parent_table',parent_table,'parent_key',parent_key,'owner_revision',owner_revision) AS edge FROM _sync_relationships ORDER BY child_key,view)"
                    val expectedEdges = listOf(c to q, d to c).sortedBy { it.first }.map { (child, parent) -> buildJsonObject {
                        put("view", "server"); put("child_schema", "business"); put("child_table", "categories"); put("child_key", "{\"id\":\"$child\"}")
                        put("fk_column", "parent_id"); put("parent_schema", "business"); put("parent_table", "categories"); put("parent_key", "{\"id\":\"$parent\"}")
                        put("owner_revision", scalarLong(peer, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"$child\"}'"))
                    } }
                    assertEquals(JsonArray(expectedEdges), Json.parseToJsonElement(scalarText(peer, edgeQuery)), "complete authoritative edge set")
                    if (localSettled) {
                        val localExpectedEdges = expectedEdges.map { edge -> JsonObject(edge + mapOf("owner_revision" to JsonPrimitive(scalarLong(db,
                            "SELECT server_revision FROM _sync_row_state WHERE key_json='${edge.getValue("child_key").jsonPrimitive.content}'")))) }
                        assertEquals(JsonArray(localExpectedEdges), Json.parseToJsonElement(scalarText(db, edgeQuery)), "complete local edge set, including no backlog desired edge")
                    }
                    assertEquals(originalState, databaseStateVector(db.connection(), listOf("categories")), "independent readback preserves the complete original state")
                } catch (failure: Throwable) {
                    peerFailure = failure
                    throw failure
                } finally {
                    withContext(NonCancellable) {
                        val failures = listOfNotNull(
                            runCatching { peer.close() }.exceptionOrNull(),
                            runCatching { peerHttp?.close() }.exceptionOrNull(),
                        )
                        val primary = peerFailure ?: failures.firstOrNull()
                        failures.filter { it !== primary }.forEach { primary?.addSuppressed(it) }
                        if (peerFailure == null && primary != null) throw primary
                    }
                }
            }
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), "lost response has not advanced the local checkpoint")
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"), "lost response has not advanced the local source sequence")
            verifyIndependentReadback(localSettled = false)
            assertEquals(2, pushBodies.size, "independent readback proves commitment before any replay")
            println("LIVE_REPLAY_COMMITTED ${webRuntimeKind()} $identity backlog=$size/width=$width")
            // Locally constructed published baseline is preparation, matching
            // the accepted host fixture. These unrelated rows are never sent.
            db.execSQL("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<$size) INSERT INTO categories SELECT printf('00000000-0000-4000-8000-%012x',i),'${"b".repeat(width)}',NULL FROM n")
            val predicate = "json_extract(key_json,'$.id') LIKE '00000000-0000-4000-8000-%'"
            db.execSQL("""UPDATE _sync_row_state SET row_version=1,server_kind='live',
                server_payload=(SELECT payload FROM _sync_dirty_rows d WHERE d.schema_name=_sync_row_state.schema_name AND d.table_name=_sync_row_state.table_name AND d.key_json=_sync_row_state.key_json),
                server_provenance=(SELECT server_provenance FROM _sync_row_state WHERE key_json='{"id":"$q"}'),relationships_known=1 WHERE $predicate""")
            db.execSQL("DELETE FROM _sync_dirty_rows WHERE $predicate")
            db.execSQL("UPDATE categories SET name='${"x".repeat(width)}' WHERE id LIKE '00000000-0000-4000-8000-%'")
            assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows WHERE base_kind='present' AND base_payload IS NOT NULL"))
            val predicates = mapOf("categories" to "id LIKE '00000000-0000-4000-8000-%'",
                "_sync_row_state" to predicate, "_sync_dirty_rows" to predicate,
                "_sync_relationships" to "json_extract(child_key,'$.id') LIKE '00000000-0000-4000-8000-%'")
            val before = databaseStateVector(db.connection(), listOf("categories"))
            val unrelated = databaseStateVector(db.connection(), listOf("categories"), predicates).filterKeys { it in predicates }
            if (webRuntimeKind() != "js-node") {
                val previous = db.connection()
                db.close()
                client = db.openClient(config, transport, ServerWinsResolver)
                assertNotSame(previous, db.connection())
                assertEquals(before, databaseStateVector(db.connection(), listOf("categories")))
                assertEquals(hash, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                client.attach(user).getOrThrow()
            } else println("A3 live Node transient; physical restart NOT-APPLICABLE")
            assertEquals(frozenBundle, OversqliteOutboxStateStore(db.connection()).loadBundleState(), "complete frozen identity survives preparation and recreation")
            val sourceBefore = requireNotNull(OversqliteSourceStateStore(db.connection()).loadState(source))
            val attachmentBefore = OversqliteAttachmentStateStore(db.connection()).loadState()
            val operationBefore = OversqliteOperationStateStore(db.connection()).loadState()
            val requestStart = requests.toMap()
            // One public primitive settles the original committed response;
            // it must not drain or recapture the unrelated dirty backlog.
            val replayObserver = if (suiteFlagEnabled("OVERSQLITE_R1_OBSERVATIONS")) {
                check(webRuntimeKind() in setOf("jvm", "native", "android")) { "R1 live observer requires a synchronous native driver" }
                R1LiveReplayObserver(db.connection())
            } else null
            if (replayObserver == null) client.pushPending().getOrThrow() else replayObserver.replay(client)
            replayObserver?.verify(size, width, client, identity)
            val stats = client.workspaceDiagnostics()
            assertTrue(stats.savedCopies <= 5, "saved copies $stats")
            assertTrue(stats.savedBytes < 4096, "saved bytes $stats")
            assertTrue(stats.peakPrimaryCopies <= 14, "peak primary copies $stats")
            assertTrue(stats.peakPrimaryBytes < 64L * (width + 1024), "peak primary bytes $stats")
            assertTrue(stats.primaryReads >= size, "backlog reads $stats")
            assertTrue(stats.primaryReads <= 5L * size + 10, "bounded reads $stats")
            assertTrue(stats.primaryBytes >= size.toLong() * width, "backlog bytes $stats")
            assertEquals(2L * stats.primaryReads, stats.indexedVisits, "indexed visits $stats")
            assertTrue(stats.duplicateChecks > 0, "duplicate checks $stats")
            assertTrue(stats.restorationPasses <= 3, "restoration passes $stats")
            assertTrue(stats.serializedBytes > 0, "serialized bytes $stats")
            assertTrue(stats.serializedBytes <= 4 * stats.primaryBytes + 4096, "bounded serialization $stats")
            assertTrue(stats.work >= 1024L * size, "charged work $stats")
            assertTrue(stats.work < 2048L * size + 10_000, "bounded work $stats")
            assertEquals(unrelated, databaseStateVector(db.connection(), listOf("categories"), predicates).filterKeys { it in predicates })
            assertEquals("grandchild", scalarText(db, "SELECT name FROM categories WHERE id='$d'"))
            assertEquals(c, scalarText(db, "SELECT parent_id FROM categories WHERE id='$d'"))
            assertEquals("child", scalarText(db, "SELECT name FROM categories WHERE id='$c'"))
            assertEquals(q, scalarText(db, "SELECT parent_id FROM categories WHERE id='$c'"))
            assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM categories WHERE id='$p'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(3, requests["/sync/push"], "seed + committed loss + exact replay")
            assertEquals(3, pushWires.size)
            assertEquals(3, pushBodies.size)
            assertContentEquals(pushBodies[1], pushBodies[2], "exact encoded HTTP body survives response loss and reopen")
            assertEquals(pushEncodings[1], pushEncodings[2], "replay content encoding")
            assertEquals(frozenWire, pushWires[2], "decoded replay request bytes equal the lost committed request")
            assertEquals(List(3) { source }, pushSources)
            assertEquals(mapOf("/sync/push" to 1), requests.mapValues { (path, count) -> count - requestStart.getOrElse(path) { 0 } }.filterValues { it != 0 },
                "counted replay does not drain unrelated intent or hide additional routes")
            assertEquals(sourceBefore.copy(nextSourceBundleId = 3), OversqliteSourceStateStore(db.connection()).loadState(source))
            assertEquals(attachmentBefore.copy(lastBundleSeqSeen = 2, pendingInitializationId = ""), OversqliteAttachmentStateStore(db.connection()).loadState())
            assertEquals(operationBefore, OversqliteOperationStateStore(db.connection()).loadState())
            assertEquals(OversqliteOutboxBundleState(), OversqliteOutboxStateStore(db.connection()).loadBundleState())
            unchangedAuthoritative.forEach { (id, row) -> assertEquals(row, completeA(db, id), "clean sibling/grandchild complete A unchanged") }
            assertPublished(db, c, "child", q, 2, "settlement", 2, source)
            assertPublished(db, p, null, null, 2, "settlement", 2, source)
            assertEquals(size + 4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
            assertEquals(expectedBusiness, Json.parseToJsonElement(scalarText(db, businessQuery)), "every local surviving typed business field")
            assertEquals(size + 3L, scalarLong(db, "SELECT count(*) FROM categories"))
            val settled = databaseStateVector(db.connection(), listOf("categories"))
            for (table in listOf("_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables"))
                assertEquals(before.getValue(table), settled.getValue(table), "primary/recovery identity $table")
            for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges"))
                assertTrue(settled.getValue(table).isEmpty(), "retained owner retired $table")
            assertTrue(requests.keys.none { "reconcile" in it })
            val queryPlan = db.connection().withExclusiveAccess {
                db.connection().prepare("EXPLAIN QUERY PLAN SELECT payload FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>('','','') ORDER BY schema_name,table_name,key_json LIMIT 1").use { statement ->
                    buildString { while (statement.step()) append(statement.getText(3)) }
                }
            }
            assertContains(queryPlan, "SEARCH"); assertContains(queryPlan, "INDEX"); assertFalse("TEMP" in queryPlan)
            verifyIndependentReadback(localSettled = true)
            println("A3 live backlog $size/$width; complete unrelated business/A/B/L/provenance/relationships unchanged; frozen=$hash; same admitted database recreation; setup and fingerprinting excluded from public call")
            println("A3_SCENARIO ${webRuntimeKind()} $identity backlog=$size/width=$width")
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            withContext(NonCancellable) {
                val failures = listOfNotNull(
                    runCatching { db.close() }.exceptionOrNull(),
                    runCatching { http?.close() }.exceptionOrNull(),
                    runCatching { cleanupOwnedWorkerStorage() }.exceptionOrNull(),
                )
                val primary = primaryFailure ?: failures.firstOrNull()
                failures.filter { it !== primary }.forEach { primary?.addSuppressed(it) }
                if (primaryFailure == null && primary != null) throw primary
            }
        }
        println("A3_COMPLETE ${webRuntimeKind()} $identity")
    }
}
