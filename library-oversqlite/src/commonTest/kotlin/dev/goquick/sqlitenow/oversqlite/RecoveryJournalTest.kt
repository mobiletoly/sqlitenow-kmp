package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpRequestRetry
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*

internal suspend fun recoveryJournalVector(db: SafeSQLiteConnection): BaselineQualificationVector =
    baselineQualificationVector(db,listOf("users","posts","_sync_upload_journal","_sync_apply_plan","_sync_apply_rows","_sync_apply_edges","_sync_push_plan","_sync_push_nodes","_sync_push_edges","_sync_apply_state","_sync_snapshot_stage","_sync_contract_identity","_sync_admitted_contracts"))

internal fun recoveryJournalConfig(generationId:Long=1)=OversqliteConfig(generationId,listOf(SyncTable("users","id"),SyncTable("posts","id")),"fixture","main")

internal fun recoveryJournalDatabase(path:String,version:Int=1)=SqliteNowDatabase(path,object:DatabaseMigrations {
    override val targetVersion=version
    override suspend fun applyMigration(conn:SafeSQLiteConnection,currentVersion:Int):Int {
        if(currentVersion==-1) {
            conn.execSQL("CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            conn.execSQL("CREATE TABLE posts(id TEXT PRIMARY KEY NOT NULL,user_id TEXT NOT NULL REFERENCES users(id),title TEXT NOT NULL)")
            return version
        }
        check(currentVersion==version) {"C03 test must not enter migration"};return currentVersion
    }
},syncManaged=true)

internal class RecoveryJournalTest : CrossTargetSyncTestSupport() {
    private suspend fun attempt(block: suspend ()->Unit): Result<Unit> = try {block();Result.success(Unit)} catch(failure: Throwable) {Result.failure(failure)}
    private suspend fun fixture(unique: Boolean=false,resolver: Resolver=ServerWinsResolver,
        body: suspend(MockSyncServer,SafeSQLiteConnection,HttpClient,DefaultOversqliteClient)->Unit) {
        val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
        createUsersAndPostsTables(db)
        if(unique) db.execSQL("CREATE UNIQUE INDEX users_name ON users(name)")
        val client=newClient(db,http,resolver=resolver)
        try {
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id,name) VALUES('item','frozen')")
            body(server,db,http,client)
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }

    private suspend fun bodies(db: SafeSQLiteConnection)=Triple(
        scalarText(db,"SELECT hex(request_body) FROM _sync_upload_journal"),
        scalarText(db,"SELECT hex(outcome_body) FROM _sync_upload_journal"),
        scalarText(db,"SELECT phase FROM _sync_upload_journal"))

    private suspend fun settled(db: SafeSQLiteConnection) {
        assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
        assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
        assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
    }

    @Test fun outcomeCaptureCrashMatrix() = runOwnedWebDatabaseTest {
        for(failure in listOf("lost_response","partial_decode","outcome_store","partial_plan","local_apply")) {
            val path=if(supportsDurableTestDatabaseRestart()) createTempSqliteNowTestDbPath("journal-capture") else null
            if(path==null && failure in setOf("partial_plan","local_apply")) {
                println("NOT-APPLICABLE transient database SQL-fatal restart: $failure; actual JVM process proof remains required")
                continue
            }
            val server=MockSyncServer();val http=server.newHttpClient()
            var opening=path?.let {recoveryJournalDatabase(it).openForSync(recoveryJournalConfig(),http).also {owner -> owner.advance()}}
            var db=opening?.readyOrNull()?.database?.connection() ?: newDb()
            var client=opening?.readyOrNull()?.client ?: newClient(db,http)
            try {
                if(path==null)createUsersAndPostsTables(db)
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("INSERT INTO users(id,name) VALUES('item','frozen')")
                when(failure) {
                    "lost_response" -> server.beforePushResponse={throw CancellationException("synthetic lost response")}
                    "partial_decode" -> server.outcomeResponseOverride={"{\"source_id\":"}
                    "outcome_store" -> db.execSQL("CREATE TEMP TRIGGER fail_capture BEFORE UPDATE OF outcome_body ON _sync_upload_journal BEGIN SELECT RAISE(ABORT,'synthetic outcome write failure');END")
                    "partial_plan" -> db.execSQL("CREATE TEMP TRIGGER fail_plan BEFORE INSERT ON _sync_apply_rows BEGIN SELECT RAISE(ABORT,'synthetic partial plan');END")
                    "local_apply" -> db.execSQL("CREATE TEMP TRIGGER fail_apply BEFORE UPDATE ON users BEGIN SELECT RAISE(ABORT,'synthetic local apply');END")
                }
                val before=baselineQualificationVector(db)
                assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
                if(path!=null) {
                    opening!!.close();opening=null;db=newFileBackedDb(path)
                }
                val frozen=bodies(db)
                assertTrue(frozen.first.isNotEmpty());assertEquals("frozen",frozen.third)
                assertEquals(failure in setOf("partial_plan","local_apply"),frozen.second.isNotEmpty())
                val after=baselineQualificationVector(db)
                for(table in listOf("users","posts","_sync_row_state","_sync_source_state","_sync_attachment_state"))
                    assertEquals(before.rows(table),after.rows(table),"$failure: $table")
                assertEquals(0,server.ackRequests.size)
                for(trigger in listOf("fail_capture","fail_plan","fail_apply")) db.execSQL("DROP TRIGGER IF EXISTS temp.$trigger")
                server.pruneBundleHistoryForSnapshotTest()
                val discovery=server.discoverySourceIds.size;val connects=server.connectRequestCount
                server.rejectMutableAdmission=true
                if(path!=null) {
                    db.close();opening=recoveryJournalDatabase(path).openForSync(recoveryJournalConfig(),http).also {it.advance()}
                    db=opening!!.readyOrNull()!!.database.connection();client=opening!!.readyOrNull()!!.client
                } else {(client as DefaultOversqliteClient).closeFromCoordinatedOwner();client=newClient(db,http)}
                client.openAndConnect("user-1").getOrThrow()
                assertEquals(frozen,bodies(db));client.pushPending().getOrThrow();settled(db)
                assertEquals(discovery,server.discoverySourceIds.size);assertEquals(connects,server.connectRequestCount)
                assertEquals(1,server.pushRequestCount)
                assertEquals(if(frozen.second.isEmpty())1 else 0,server.resolveRequests.size)
                assertEquals(1,server.ackRequests.size)
                assertEquals(2L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
                println("RECOVERY_JOURNAL_COMPLETE outcome_capture $failure")
            } finally {
                if(opening!=null)opening!!.close() else {(client as? DefaultOversqliteClient)?.closeFromCoordinatedOwner();db.close()}
                http.close()
                if(path!=null)deleteTempSqliteNowTestDbArtifacts(path)
            }
        }
    }

    @Test fun ackCrashMatrix() = runOwnedWebDatabaseTest {
        for(failure in listOf("before_ack","lost_ack","after_ack_response","retired_binding_reopen")) {
            fixture {server,db,http,client ->
                when(failure) {
                    "before_ack","retired_binding_reopen" -> server.beforeAck={throw CancellationException("synthetic before ack")}
                    "lost_ack" -> server.afterAck={throw CancellationException("synthetic lost ack")}
                    "after_ack_response" -> db.execSQL("CREATE TEMP TRIGGER fail_cleanup BEFORE DELETE ON _sync_upload_journal BEGIN SELECT RAISE(ABORT,'synthetic ack cleanup');END")
                }
                assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
                assertEquals("ack_pending",bodies(db).third)
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(2L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
                val before=baselineQualificationVector(db)
                db.execSQL("DROP TRIGGER IF EXISTS temp.fail_cleanup")
                val discovery=server.discoverySourceIds.size;val connects=server.connectRequestCount
                client.closeFromCoordinatedOwner();server.rejectMutableAdmission=true
                val reopened=newClient(db,http)
                try {
                    reopened.openAndConnect("user-1").getOrThrow()
                    reopened.pushPending().getOrThrow()
                    settled(db)
                } finally {reopened.closeFromCoordinatedOwner()}
                val after=baselineQualificationVector(db)
                for(table in listOf("users","posts","_sync_row_state","_sync_dirty_rows","_sync_source_state","_sync_attachment_state"))
                    assertEquals(before.rows(table),after.rows(table),"ack must not replay settlement: $table")
                assertEquals(discovery,server.discoverySourceIds.size);assertEquals(connects,server.connectRequestCount)
                assertEquals(1,server.pushRequestCount);assertEquals(0,server.resolveRequests.size);assertEquals(2,server.ackRequests.size)
                println("RECOVERY_JOURNAL_COMPLETE ack $failure")
            }
        }
    }

    @Test fun cancelledRequestPreservesIntent() = runOwnedWebDatabaseTest {
        for(edit in listOf("later","same_value","delete_recreate")) fixture {server,db,_,client ->
            server.beforePushCommit={throw CancellationException("possibly dispatched")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            when(edit) {
                "later" -> db.execSQL("UPDATE users SET name='later' WHERE id='item'")
                "same_value" -> db.execSQL("UPDATE users SET name='frozen' WHERE id='item'")
                else -> {db.execSQL("DELETE FROM users WHERE id='item'");db.execSQL("INSERT INTO users(id,name) VALUES('item','recreated')")}
            }
            client.pushPending().getOrThrow()
            assertEquals(1,server.pushRequestCount);assertEquals(1,server.resolveRequests.size);assertEquals(1,server.ackRequests.size)
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            val intended=scalarText(db,"SELECT name FROM users WHERE id='item'")
            client.pushPending().getOrThrow();settled(db)
            assertEquals(intended,scalarText(db,"SELECT name FROM users WHERE id='item'"))
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            println("RECOVERY_JOURNAL_COMPLETE cancelled $edit")
        }
    }

    @Test fun rejectedRequestPreservesIntent() = runOwnedWebDatabaseTest {
        fixture {server,db,_,client ->
            server.applicationRejectionCode="application_rejected"
            val failure=client.pushPending().exceptionOrNull()
            assertIs<OversqliteRemoteContractException>(failure);assertEquals(200,failure.httpStatus)
            assertEquals("rejected",scalarText(db,"SELECT state FROM _sync_outbox_bundle"))
            assertEquals(2L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(1,server.ackRequests.size)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
            db.execSQL("UPDATE users SET name='later' WHERE id='item'")
            server.applicationRejectionCode=null
            client.pushPending().getOrThrow();settled(db)
            assertEquals("later",scalarText(db,"SELECT name FROM users WHERE id='item'"))
            val requests=server.pushBodies.map {Json.decodeFromString<CompletePushRequest>(it)}
            assertEquals(listOf(1L,2L),requests.map {it.sourceBundleId})
            assertEquals(2,server.ackRequests.size)
            println("RECOVERY_JOURNAL_COMPLETE rejected application")
        }
    }

    @Test fun sourceSequenceGaps() = runOwnedWebDatabaseTest {
        fixture {server,db,_,client ->
            server.beforePushCommit={throw CancellationException("not committed")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            client.pushPending().getOrThrow();client.pushPending().getOrThrow();settled(db)
            assertEquals(listOf("cancelled","committed"),server.terminalStatuses.values.toList())
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            println("RECOVERY_JOURNAL_COMPLETE source_sequence_gaps")
        }
    }

    @Test fun frozenBytesUnchanged() = runOwnedWebDatabaseTest {
        fixture {server,db,_,client ->
            server.beforePushCommit={throw CancellationException("uncertain")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            val before=bodies(db)
            db.execSQL("UPDATE users SET name='later' WHERE id='item'")
            server.beforeResolve={throw CancellationException("unavailable")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            assertEquals(before,bodies(db))
            db.execSQL("UPDATE _sync_upload_journal SET request_body=CAST('changed' AS BLOB)")
            val exchanges=server.requestTrace.size
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            assertEquals(exchanges,server.requestTrace.size)
            assertEquals(0,server.ackRequests.size)
            assertEquals("later",scalarText(db,"SELECT name FROM users WHERE id='item'"))
            println("RECOVERY_JOURNAL_COMPLETE frozen_bytes")
        }
    }

    @Test fun blockedSettlementRetainsOutcome() = runOwnedWebDatabaseTest {
        fixture(unique=true) {server,db,_,client ->
            server.normalizePushEffects={rows -> rows.map {it.copy(payload=buildJsonObject {put("id",JsonPrimitive("item"));put("name",JsonPrimitive("occupied"))})}}
            server.beforePushResponse={db.execSQL("INSERT INTO users(id,name) VALUES('later','occupied')")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            val certificate=bodies(db);assertTrue(certificate.second.isNotEmpty());assertEquals("frozen",certificate.third)
            val before=recoveryJournalVector(db)
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            assertEquals(certificate,bodies(db));assertEquals(before,recoveryJournalVector(db))
            assertEquals(0,server.ackRequests.size);assertEquals(0,server.resolveRequests.size);assertEquals(1,server.pushRequestCount)
            assertTrue(client.rebuild().isFailure)
            assertEquals(0,server.snapshotSessionCreateRequestCount)
            assertEquals(before,recoveryJournalVector(db))
            println("RECOVERY_JOURNAL_COMPLETE blocked_unique_settlement")
        }
    }

    @Test fun ackObligationBlocksDetach() = runOwnedWebDatabaseTest {
        fixture { server,db,_,client ->
            server.beforeAck={throw CancellationException("ack unavailable")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            assertEquals("ack_pending",bodies(db).third)
            val before=recoveryJournalVector(db)
            assertTrue(client.syncStatus().getOrThrow().pending.blocksDetach)
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA,client.detach().getOrThrow())
            assertEquals(before,recoveryJournalVector(db))
            assertEquals(1,server.ackRequests.size)
            client.pushPending().getOrThrow();settled(db)
            assertEquals(1,server.pushRequestCount)
            println("RECOVERY_JOURNAL_COMPLETE ack_blocks_detach")
        }
    }

    @Test fun initializationDispositionOwnsLeaseClearAndRetry() = runOwnedWebDatabaseTest {
        for(rejected in listOf(false,true)) {
            val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
            createUsersAndPostsTables(db)
            db.execSQL("INSERT INTO users(id,name) VALUES('item','seed')")
            val client=newClient(db,http)
            try {
                client.openAndConnect("user-1").getOrThrow()
                assertTrue(scalarText(db,"SELECT pending_initialization_id FROM _sync_attachment_state").isNotEmpty())
                if(rejected) server.applicationRejectionCode="application_rejected"
                val result=client.pushPending()
                if(!rejected) {
                    result.getOrThrow();settled(db)
                    assertEquals("",scalarText(db,"SELECT pending_initialization_id FROM _sync_attachment_state"))
                } else {
                    assertIs<OversqliteRemoteContractException>(result.exceptionOrNull())
                    assertEquals(1L,scalarLong(db,"SELECT initialization_reconnect_required FROM _sync_attachment_state"))
                    client.closeFromCoordinatedOwner()
                    val reopened=newClient(db,http)
                    try {
                        reopened.openAndConnect("user-1").getOrThrow()
                        assertIs<ConnectRequiredException>(reopened.pushPending().exceptionOrNull())
                        assertEquals("none",scalarText(db,"SELECT state FROM _sync_outbox_bundle"))
                        assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                        // Pull progress cannot release the initialization lease guard.
                        OversqliteAttachmentStateStore(db).markBundleSeen(0)
                        assertEquals(1L,scalarLong(db,"SELECT initialization_reconnect_required FROM _sync_attachment_state"))
                        server.applicationRejectionCode=null
                        reopened.attach("user-1").getOrThrow()
                        reopened.pushPending().getOrThrow();settled(db)
                        assertEquals("seed",scalarText(db,"SELECT name FROM users WHERE id='item'"))
                    } finally { reopened.closeFromCoordinatedOwner() }
                }
                println("RECOVERY_JOURNAL_COMPLETE initialization rejected=$rejected")
            } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
        }
    }

    @Test fun missingJournalOldFormatRejectsWithoutMutation() = runOwnedWebDatabaseTest {
        fixture { _,db,http,client ->
            client.closeFromCoordinatedOwner()
            db.execSQL("DROP TABLE _sync_upload_journal")
            val tables=db.queryList("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name") {it.getText(0)}
            val before=baselineQualificationVector(db,tables)
            val schema=db.queryList("SELECT sql FROM sqlite_schema ORDER BY name") {if(it.isNull(0)) "NULL" else it.getText(0)}
            val reopened=newClient(db,http)
            try {assertTrue(reopened.open().isFailure)} finally {reopened.closeFromCoordinatedOwner()}
            assertEquals(before,baselineQualificationVector(db,tables))
            assertEquals(schema,db.queryList("SELECT sql FROM sqlite_schema ORDER BY name") {if(it.isNull(0)) "NULL" else it.getText(0)})
            println("RECOVERY_JOURNAL_COMPLETE old_format_no_mutation")
        }
    }

    @Test fun actualResolveRetriesShareInvocationBudget() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer();val base=server.newHttpClient()
        val http=base.config {install(HttpRequestRetry) {maxRetries=100;retryOnServerErrors();delayMillis {0}}}
        val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id,name) VALUES('item','frozen')")
            server.beforePushCommit={throw CancellationException("possibly dispatched")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            val before=recoveryJournalVector(db)
            server.resolveCapacityUnavailable=true
            val failure=attempt {client.pushPending().getOrThrow()}.exceptionOrNull()
            assertIs<UploadRecoveryIncompleteException>(failure)
            assertEquals(64,server.resolveRequests.size)
            assertEquals(before,recoveryJournalVector(db));assertEquals(0,server.ackRequests.size)
            println("RECOVERY_JOURNAL_COMPLETE actual_resolve_retry_budget dispatches=${server.resolveRequests.size}")
        } finally {client.closeFromCoordinatedOwner();http.close();base.close();db.close()}
    }

    @Test fun initializationCancelledOperationNeedsAttachBeforeSuccessor() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer();val http=server.newHttpClient();val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);db.execSQL("INSERT INTO users(id,name) VALUES('item','seed')")
            client.openAndConnect("user-1").getOrThrow()
            val original=scalarText(db,"SELECT pending_initialization_id FROM _sync_attachment_state");assertTrue(original.isNotEmpty())
            server.beforePushCommit={throw CancellationException("original possibly dispatched")}
            assertTrue(attempt {client.pushPending().getOrThrow()}.isFailure)
            client.pushPending().getOrThrow()
            assertEquals("cancelled",server.terminalStatuses.values.single())
            assertEquals(original,scalarText(db,"SELECT pending_initialization_id FROM _sync_attachment_state"))
            assertEquals(1L,scalarLong(db,"SELECT initialization_reconnect_required FROM _sync_attachment_state"))
            assertIs<ConnectRequiredException>(client.pushPending().exceptionOrNull())
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            client.attach("user-1").getOrThrow();client.pushPending().getOrThrow();settled(db)
            assertEquals("",scalarText(db,"SELECT pending_initialization_id FROM _sync_attachment_state"))
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            println("RECOVERY_JOURNAL_COMPLETE initialization_cancelled explicit_attach_before_successor=true")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }
}
