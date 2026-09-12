package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

internal class BaselineLifecycleTest : CrossTargetSyncTestSupport() {
    private suspend fun fixture(path: String? = null, block: suspend (SafeSQLiteConnection, MockSyncServer, HttpClient, DefaultOversqliteClient) -> Unit) {
        val db=if (path == null) newDb() else newFileBackedDb(path)
        val server=MockSyncServer(); val http=server.newHttpClient()
        val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db)
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db,"P","parent"); insertUser(db,"Q","other"); insertPost(db,"C","P","child")
            client.pushPending().getOrThrow()
            block(db,server,http,client)
        } finally { client.close(); http.close(); db.close() }
    }

    private suspend fun baselineStateStore(db: SafeSQLiteConnection): OversqliteSyncStateStore {
        val config = OversqliteConfig(generationId=1,databaseId="fixture",serviceId="main",
            syncTables=listOf(SyncTable("users",syncKeyColumnName="id"),SyncTable("posts",syncKeyColumnName="id")))
        val physical = SyncRuntimeInitializer(config,TableInfoCache()).validateConfig(db)
        val validated = physical.withGeneration(OversqliteContractStore(db).readGeneration(1))
        val local = OversqliteLocalStore(db,Json) { validated }
        return OversqliteSyncStateStore(db) { local }
    }

    @Test fun capturePreservesHistoricalBaseAndBothRelationshipViews() = runOwnedWebDatabaseTest {
        fixture { db,_,_,_ ->
            val original=scalarText(db,"SELECT server_payload FROM _sync_row_state WHERE table_name='posts'")
            db.execSQL("UPDATE posts SET user_id='Q',title='first' WHERE id='C'")
            db.execSQL("UPDATE posts SET title='second' WHERE id='C'")
            assertEquals(Json.parseToJsonElement(original),Json.parseToJsonElement(scalarText(db,"SELECT base_payload FROM _sync_dirty_rows WHERE table_name='posts'")))
            assertEquals(original,scalarText(db,"SELECT server_payload FROM _sync_row_state WHERE table_name='posts'"))
            assertEquals("P",scalarText(db,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server'"))
            assertEquals("Q",scalarText(db,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired'"))
            db.execSQL("DELETE FROM posts WHERE id='C'")
            assertEquals("P",scalarText(db,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server'"))
            assertEquals(0L,db.scalarLong("SELECT count(*) FROM _sync_relationships WHERE view='desired'"))
            assertEquals(1L,db.scalarLong("SELECT payload IS NULL FROM _sync_dirty_rows WHERE table_name='posts'"))
        }
    }

    @Test fun frozenBaseSurvivesLostResponseAndReopenThenSettlementRebasesNewerIntent() = runOwnedWebDatabaseTest {
        if (!supportsDurableTestDatabaseRestart()) {
            println("NOT-APPLICABLE: physical restart on supported transient Node; use transientOwnerReplaysExactFrozenRequest")
            return@runOwnedWebDatabaseTest
        }
        println("baseline-physical-replay: entered")
        val path = createTempSqliteNowTestDbPath("baseline-replay")
        try { fixture(path) { db,server,http,client ->
            val original=scalarText(db,"SELECT server_payload FROM _sync_row_state WHERE table_name='posts'")
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.beforePushResponse={ db.execSQL("UPDATE posts SET title='newer' WHERE id='C'"); throw CancellationException("lost response") }
            try { client.pushPending() } catch (_: CancellationException) { }
            val hash=scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertTrue(hash.isNotEmpty())
            assertEquals(original,scalarText(db,"SELECT base_payload FROM _sync_outbox_rows WHERE table_name='posts'"))
            val frozen = persistenceVector(db)
            println("baseline-physical-replay: frozen owner vector captured")
            val source = scalarText(db,"SELECT source_id FROM _sync_source_state")
            val nextId = db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state")
            val checkpoint = db.scalarLong("SELECT last_bundle_seq_seen FROM _sync_attachment_state")
            client.closeFromCoordinatedOwner()
            db.close()
            val reopenedDb = newFileBackedDb(path)
            val reopened=newClient(reopenedDb,http)
            try {
                assertEquals(frozen,persistenceVector(reopenedDb))
                println("baseline-physical-replay: physical reopen vector verified")
                reopened.openAndConnect("user-1").getOrThrow()
                assertEquals(hash,scalarText(reopenedDb,"SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                reopened.pushPending().getOrThrow()
                assertEquals("frozen",scalarText(reopenedDb,"SELECT json_extract(server_payload,'$.title.value') FROM _sync_row_state WHERE table_name='posts'"))
                assertEquals("frozen",scalarText(reopenedDb,"SELECT json_extract(base_payload,'$.title.value') FROM _sync_dirty_rows WHERE table_name='posts'"))
                assertEquals("newer",scalarText(reopenedDb,"SELECT title FROM posts WHERE id='C'"))
                assertEquals(source,scalarText(reopenedDb,"SELECT source_id FROM _sync_source_state"))
                assertEquals(nextId+1,reopenedDb.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state"))
                assertEquals(checkpoint+1,reopenedDb.scalarLong("SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L,reopenedDb.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals("P",scalarText(reopenedDb,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server'"))
                assertEquals("P",scalarText(reopenedDb,"SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired'"))
                println("baseline-physical-replay: settlement verified")
            } finally { reopened.close();reopenedDb.close() }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun transientOwnerReplaysExactFrozenRequest() = runOwnedWebDatabaseTest {
        fixture { db,server,_,client ->
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.beforePushResponse={ db.execSQL("UPDATE posts SET title='newer' WHERE id='C'"); throw CancellationException("lost committed response") }
            try { client.pushPending() } catch (_: CancellationException) { }
            val frozen = persistenceVector(db)
            val hash = scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertEquals(64,hash.length)
            client.pushPending().getOrThrow()
            assertEquals(server.pushBodies[server.pushBodies.lastIndex-1],server.pushBodies.last(),"exact request replay")
            assertEquals("newer",scalarText(db,"SELECT title FROM posts WHERE id='C'"))
            assertEquals("frozen",scalarText(db,"SELECT json_extract(base_payload,'$.title.value') FROM _sync_dirty_rows"))
            assertEquals(0L,db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
            assertTrue(frozen.tables.isNotEmpty())
            println("baseline-transient-replay: exact original body and newer intent preserved")
        }
    }

    private suspend fun persistenceVector(db: SafeSQLiteConnection) = baselineQualificationVector(db)

    @Test fun requeueKeepsFrozenBaseWhenNewerLocalIntentWins() = runOwnedWebDatabaseTest {
        fixture { db,server,_,client ->
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.beforePushResponse={ throw CancellationException("ambiguous") }
            try { client.pushPending() } catch (_: CancellationException) { }
            val outbox=OversqliteOutboxStateStore(db).loadRows().single()
            db.execSQL("UPDATE posts SET title='newer' WHERE id='C'")
            val row=DirtyRowCapture(outbox.schemaName,outbox.tableName,outbox.keyJson,"C",mapOf("id" to "C"),outbox.op,outbox.baseRowVersion,outbox.localPayload,outbox.rowOrdinal,outbox.base)
            // Exercise the reached proven-non-commit requeue owner directly; no remote resolution is claimed.
            db.transaction(TransactionMode.IMMEDIATE) {
                baselineStateStore(db).requeueSnapshotRows(PushOutboundSnapshot(outbox.sourceBundleId,listOf(row),listOf(outbox),"test"))
            }
            assertEquals(outbox.base.payload,scalarText(db,"SELECT base_payload FROM _sync_dirty_rows WHERE table_name='posts'"))
            assertEquals("newer",scalarText(db,"SELECT json_extract(payload,'$.title.value') FROM _sync_dirty_rows WHERE table_name='posts'"))
        }
    }

    @Test fun missingBaselineAndCounterOverflowRollbackBusinessWrites() = runOwnedWebDatabaseTest {
        fixture { db,_,_,_ ->
            db.execSQL("DELETE FROM _sync_row_state WHERE table_name='posts'")
            assertFailsWith<Exception> { db.execSQL("UPDATE posts SET title='unsafe' WHERE id='C'") }
            assertEquals("child",scalarText(db,"SELECT title FROM posts WHERE id='C'"))
            db.execSQL("UPDATE _sync_conflict_state SET dirty_revision=9223372036854775806")
            assertFailsWith<Exception> { db.execSQL("UPDATE users SET name='overflow' WHERE id='Q'") }
            assertEquals("other",scalarText(db,"SELECT name FROM users WHERE id='Q'"))
        }
    }

    @Test fun retainedConstraintAndCaptureTamperingRejectWithoutRepair() = runOwnedWebDatabaseTest {
        fixture { db,_,_,_ ->
            assertEquals("CURRENT",inspectRetainedRuntime(db))
            db.execSQL("DROP TRIGGER trg_posts_au")
            db.execSQL("CREATE TRIGGER trg_posts_au AFTER UPDATE ON posts BEGIN SELECT 1; END")
            assertFailsWith<IllegalArgumentException> { inspectRetainedRuntime(db) }
            assertTrue(scalarText(db,"SELECT sql FROM sqlite_schema WHERE name='trg_posts_au'").contains("SELECT 1"))
        }
    }
    @Test fun typedWireBaselineKeepsExactIntegersDecimalsBlobsJsonAndNulls() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
        val columns=listOf(GenerationColumn("id","text",false),GenerationColumn("n","int64",false),GenerationColumn("d","decimal",false),GenerationColumn("b","bytes",false),GenerationColumn("j","json",false),GenerationColumn("z","text",true))
        server.generationOverride=SyncGeneration("1","",listOf(GenerationTable("main","typed",listOf("id"),columns,emptyList()))).let { it.copy(fingerprint=it.computeFingerprint()) }
        val client=newClient(db,http,syncTables=listOf(SyncTable("typed",syncKeyColumnName="id")))
        try {
            db.execSQL("CREATE TABLE typed(id TEXT PRIMARY KEY NOT NULL,n INTEGER NOT NULL,d TEXT NOT NULL,b BLOB NOT NULL,j TEXT NOT NULL,z TEXT)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("""INSERT INTO typed VALUES('T',9223372036854775807,'1234567890.123456789',X'00ff','{"exact":"9007199254740993"}',NULL)""")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE typed SET n=1 WHERE id='T'")
            val base=Json.parseToJsonElement(scalarText(db,"SELECT base_payload FROM _sync_dirty_rows")) as kotlinx.serialization.json.JsonObject
            assertEquals(Json.parseToJsonElement("""{"storage":"integer","value":"9223372036854775807"}"""),base["n"])
            assertEquals(Json.parseToJsonElement("""{"storage":"text","value":"1234567890.123456789"}"""),base["d"])
            assertEquals(Json.parseToJsonElement("""{"storage":"blob","value":"00ff"}"""),base["b"])
            assertEquals(Json.parseToJsonElement("""{"storage":"null","value":null}"""),base["z"])
            assertEquals(6,base.size)
        } finally { client.close();http.close();db.close() }
    }

    @Test fun corruptProjectionBlocksFreezeAndPreservesIntent() = runOwnedWebDatabaseTest {
        fixture { db,_,_,client ->
            db.execSQL("UPDATE posts SET title='pending' WHERE id='C'")
            db.execSQL("DELETE FROM _sync_relationships WHERE view='server'")
            val result=client.pushPending()
            assertTrue(result.isFailure)
            val pending=result.exceptionOrNull()
            assertTrue(pending is FKReconciliationRequiredException)
            assertEquals("historical_repair_required",pending.detail)
            assertEquals(1L,db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L,db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
        }
    }


    @Test
    fun corruptServerProjectionBlocksApplyAndMissingHistoricalProvenanceBlocksFreeze() = runOwnedWebDatabaseTest {
        fixture { db, _, _, client ->
            db.execSQL("DELETE FROM _sync_relationships WHERE view='server'")
            assertFailsWith<IllegalStateException> {
                db.transaction(TransactionMode.IMMEDIATE) {
                    baselineStateStore(db).installAuthoritativeBaseline(
                        "main", "posts", "{\"id\":\"C\"}", "deleted", 2, null, "pull", 2,
                    )
                }
            }
            assertEquals("live", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE table_name='posts'"))
            db.execSQL("UPDATE users SET name='pending' WHERE id='Q'")
            db.execSQL("UPDATE _sync_dirty_rows SET base_provenance='{}'")
            val result = client.pushPending()
            assertTrue(result.isFailure)
            assertTrue(result.exceptionOrNull()?.message.orEmpty().contains("resolution_base_unavailable"))
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"))
        }
    }

    @Test
    fun independentRekeysRetainPerKeyBasesAndBaselineOverflowRollsBack() = runOwnedWebDatabaseTest {
        fixture { db, _, _, _ ->
            db.execSQL("UPDATE users SET id='R' WHERE id='Q'")
            db.execSQL("UPDATE users SET id='S' WHERE id='R'")
            assertEquals("present", scalarText(db, "SELECT base_kind FROM _sync_dirty_rows WHERE key_json='{\"id\":\"Q\"}'"))
            assertEquals("absent", scalarText(db, "SELECT base_kind FROM _sync_dirty_rows WHERE key_json='{\"id\":\"R\"}'"))
            assertEquals("absent", scalarText(db, "SELECT base_kind FROM _sync_dirty_rows WHERE key_json='{\"id\":\"S\"}'"))
            db.execSQL("UPDATE _sync_conflict_state SET baseline_revision=9223372036854775806")
            assertFailsWith<Exception> { insertUser(db, "overflow", "pending") }
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM users WHERE id='overflow'"))
        }
    }

    @Test
    fun tombstoneReinsertRetainsVersionAndUnpublishedNoopDoesNotConsumeSequence() = runOwnedWebDatabaseTest {
        fixture { db, _, _, client ->
            val sequence = db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state")
            insertUser(db, "temporary", "value")
            db.execSQL("DELETE FROM users WHERE id='temporary'")
            client.pushPending().getOrThrow()
            assertEquals(sequence, db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"))
            db.execSQL("DELETE FROM users WHERE id='Q'")
            client.pushPending().getOrThrow()
            assertEquals("deleted", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"Q\"}'"))
            val version = db.scalarLong("SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"Q\"}'")
            insertUser(db, "Q", "reinsert")
            assertEquals("deleted", scalarText(db, "SELECT base_kind FROM _sync_dirty_rows"))
            assertEquals(version, db.scalarLong("SELECT base_row_version FROM _sync_dirty_rows"))
            // This mock forgets remote tombstones. Assert the local capture boundary here;
            // live-server reinsertion remains a separate qualification obligation.
            assertEquals("deleted", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE key_json='{\"id\":\"Q\"}'"))
            assertEquals("reinsert", scalarText(db, "SELECT json_extract(payload,'$.name.value') FROM _sync_dirty_rows"))
        }
    }

    @Test
    fun corruptDesiredProjectionBlocksLoadingIntentForApply() = runOwnedWebDatabaseTest {
        fixture { db, _, _, _ ->
            db.execSQL("UPDATE posts SET user_id='Q' WHERE id='C'")
            db.execSQL("DELETE FROM _sync_relationships WHERE view='desired'")
            assertFailsWith<IllegalStateException> {
                baselineStateStore(db).loadDirtyUploadState("main", "posts", "{\"id\":\"C\"}")
            }
        }
    }

    @Test fun invalidProvenanceBlocksFreezeAndReplay() = runOwnedWebDatabaseTest {
        val accepted = mutableListOf<String>()
        for (owner in listOf("_sync_dirty_rows", "_sync_outbox_rows")) {
            for (mutation in listOf("'$.generation_id',NULL","'$.account_scope.account_id','other-account'","'$.generation_id','1'","'$.generation_id',-1","'$.generation_id',2","'$.database_id','other-db'","'$.service_id','other-service'","'$.generation_fingerprint','other-generation'","'$.admission','unknown'","'$.local_version','0'","'$.local_version',-1","'$.physical_shape',NULL","'$.account_scope.binding','anonymous'","'$.account_scope.account_id',1","'$.origin',NULL","'$.origin','unknown'","'$.origin','snapshot','$.bundle_sequence',NULL","'$.origin','never_published'","'$.origin_source_id',NULL","'$.origin_source_id',1","'$.origin_source_id',''","'$.bundle_sequence','1'","'$.bundle_sequence',-1","'$.bundle_sequence',1.5","'$.authoritative_version','1'","'$.authoritative_version',NULL")) {
                fixture { db, server, _, client ->
                    db.execSQL("UPDATE posts SET title='pending' WHERE id='C'")
                    if (owner == "_sync_outbox_rows") {
                        server.beforePushResponse = { throw CancellationException("lost response") }
                        try { client.pushPending() } catch (_: CancellationException) { }
                    }
                    db.execSQL("UPDATE $owner SET base_provenance=json_set(base_provenance,$mutation)")
                    val before = baselineIntentState(db)
                    val requests = server.pushRequestCount
                    val sequence = db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state")
                    val result = client.pushPending()
                    if (result.exceptionOrNull() !is ResolutionBaseUnavailableException) {
                        accepted += "$owner $mutation: ${result.exceptionOrNull()}"
                    } else {
                        assertEquals(before,baselineIntentState(db))
                        assertEquals(requests,server.pushRequestCount)
                        assertEquals(1L,db.scalarLong("SELECT count(*) FROM $owner"))
                        assertEquals(sequence,db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state"))
                        if (owner == "_sync_dirty_rows") assertEquals(0L,db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
                    }
                }
            }
        }
        assertEquals(emptyList(), accepted)
    }

    @Test fun incompleteOwnerRollsBackPull() = runOwnedWebDatabaseTest {
        for (corrupt in listOf("'{}'", "json_remove(server_payload,'$.title')", "json_set(server_payload,'$.user_id.storage','null')")) fixture { db, server, _, client ->
            db.execSQL("UPDATE _sync_row_state SET server_payload=$corrupt WHERE table_name='posts'")
            val before = scalarText(db,"SELECT server_payload FROM _sync_row_state WHERE table_name='posts'")
            val checkpoint = db.scalarLong("SELECT last_bundle_seq_seen FROM _sync_attachment_state")
            val revision = db.scalarLong("SELECT baseline_revision FROM _sync_conflict_state")
            server.addRemoteBundleForTest(listOf(BundleRow("main", "posts", mapOf("id" to "C"), "UPDATE", 2,
                Json.parseToJsonElement("""{"id":"C","user_id":"Q","title":"remote"}"""))))
            assertTrue(client.pullToStable().isFailure)
            assertEquals(before,scalarText(db,"SELECT server_payload FROM _sync_row_state WHERE table_name='posts'"))
            assertEquals(1L,db.scalarLong("SELECT row_version FROM _sync_row_state WHERE table_name='posts'"))
            assertEquals("child",scalarText(db,"SELECT title FROM posts WHERE id='C'"))
            assertEquals(checkpoint,db.scalarLong("SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(revision,db.scalarLong("SELECT baseline_revision FROM _sync_conflict_state"))
        }
    }

    private suspend fun baselineIntentState(db: SafeSQLiteConnection) = listOf(
        scalarText(db,"SELECT json_group_array(json_array(schema_name,table_name,key_json,op,base_row_version,payload,base_kind,base_payload,base_provenance,dirty_ordinal,updated_at)) FROM _sync_dirty_rows"),
        scalarText(db,"SELECT json_group_array(json_array(source_bundle_id,row_ordinal,schema_name,table_name,key_json,wire_key_json,op,base_row_version,wire_payload,local_payload,base_kind,base_payload,base_provenance)) FROM _sync_outbox_rows"),
        scalarText(db,"SELECT json_group_array(json_array(state,source_id,source_bundle_id,canonical_request_hash,row_count,initialization_id)) FROM _sync_outbox_bundle"),
    )

    @Test fun historicalSourceSurvivesFreezeAndReplay() = runOwnedWebDatabaseTest {
        for (unpublished in listOf(false,true)) fixture { db,server,http,client ->
            if (unpublished) insertUser(db,"N","new") else db.execSQL("UPDATE posts SET title='pending' WHERE id='C'")
            val source = if (unpublished) "" else "retired-source"
            db.execSQL("UPDATE _sync_dirty_rows SET base_provenance=json_set(base_provenance,'$.origin_source_id','$source')")
            val provenance = scalarText(db,"SELECT base_provenance FROM _sync_dirty_rows")
            server.beforePushResponse = { throw CancellationException("lost response") }
            try { client.pushPending() } catch (_: CancellationException) { }
            assertEquals(provenance,scalarText(db,"SELECT base_provenance FROM _sync_outbox_rows"))
            client.closeFromCoordinatedOwner()
            val reopened=newClient(db,http)
            try {
                reopened.openAndConnect("user-1").getOrThrow()
                assertEquals(provenance,scalarText(db,"SELECT base_provenance FROM _sync_outbox_rows"))
                reopened.pushPending().getOrThrow()
                assertEquals(0L,db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
            } finally { reopened.close() }
        }
    }
}
