package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
import kotlin.test.*

internal class AutomaticRecoveryTest : CrossTargetSyncTestSupport() {
    private fun row(id: String,name: String,version: Long=2) = BundleRow("main","users",mapOf("id" to id),"UPDATE",version,Json.parseToJsonElement("""{"id":"$id","name":"$name"}"""))
    private suspend fun fixture(work: Long=1000000,chunk: Int=1000,maxPushRows: Int=200,block: suspend(MockSyncServer,SafeSQLiteConnection,DefaultOversqliteClient)->Unit) {
        val server=MockSyncServer();val db=newDb();val http=server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client=newClient(db,http,workUnits=work,maxSnapshotChunkRows=chunk,maxPushRows=maxPushRows,resolver=DeferConflictResolver)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users VALUES('A','a'),('B','b')")
            client.pushPending().getOrThrow()
            if(maxPushRows==1)client.sync().getOrThrow()
            db.execSQL("CREATE UNIQUE INDEX users_name ON users(name)")
            block(server,db,client)
        } finally { http.close();db.close() }
    }
    private suspend fun vector(db: SafeSQLiteConnection): Map<String,List<kotlinx.serialization.json.JsonObject>> = baselineQualificationVector(db,listOf("users","posts","_sync_row_state","_sync_dirty_rows","_sync_outbox_rows","_sync_outbox_bundle","_sync_upload_journal","_sync_source_state")).tables.filterKeys {it !in setOf("_sync_operation_state","_sync_attachment_state","_sync_conflict_state","_sync_apply_plan","_sync_apply_rows","_sync_apply_edges","_sync_push_plan","_sync_push_nodes","_sync_push_edges","_sync_apply_state","_sync_snapshot_stage")}
    private fun snapshots(server: MockSyncServer)=server.requestTrace.count { "snapshot-sessions" in it }
    private fun historical(server: MockSyncServer,invalidSnapshot: Boolean=false) {
        server.addRemoteBundleForTest(listOf(row("A","collision"),row("B","collision")))
        if (!invalidSnapshot) server.addRemoteBundleForTest(listOf(row("A","b",3),row("B","a",3)))
    }
    @Test fun publicSyncHistoricalRecovery() = runOwnedWebDatabaseTest {
        for (work in listOf(1L,8L,17L,1000000L)) fixture(work) { server,db,client ->
            historical(server)
            val source=scalarText(db,"SELECT json_group_array(json_array(source_id,next_source_bundle_id)) FROM _sync_source_state")
            server.beforeSnapshotCreateResponse={ server.addRemoteBundleForTest(listOf(row("A","post-T",4))) }
            val report=client.sync().getOrThrow()
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT,report.remoteOutcome)
            assertEquals(4L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals("post-T",scalarText(db,"SELECT name FROM users WHERE id='A'"))
            assertEquals(source,scalarText(db,"SELECT json_group_array(json_array(source_id,next_source_bundle_id)) FROM _sync_source_state"))
            val status=client.recoveryStatus().getOrThrow();assertEquals("completed",status.phase);assertEquals(3L,status.observedBoundary)
            assertEquals(1,server.requestTrace.count {it=="POST /sync/snapshot-sessions"})
        }
    }
    @Test fun disabledMode() = runOwnedWebDatabaseTest { fixture { server,db,client ->
        historical(server);client.setRecoveryMode(RecoveryMode.DISABLED).getOrThrow();val before=vector(db)
        assertIs<FinalStateApplyBlockedException>(client.sync().exceptionOrNull());assertEquals(0,snapshots(server));assertEquals(before,vector(db))
        client.retryRecovery().getOrThrow();assertTrue(client.sync().isFailure);assertEquals(0,snapshots(server))
        client.setRecoveryMode(RecoveryMode.AUTOMATIC_CLEAN).getOrThrow();client.sync().getOrThrow();assertEquals(1,server.requestTrace.count{it=="POST /sync/snapshot-sessions"})
    } }
    @Test fun dirtyStateNeverDownloads() = runOwnedWebDatabaseTest {
        for (pending in listOf("dirty","prepared","outcome","rejected","ack_pending")) fixture { server,db,client ->
            db.execSQL("UPDATE users SET name='offline' WHERE id='A'")
            when(pending) {
                "dirty" -> client.pauseUploads()
                "prepared" -> { server.cancelNextPush=true;try{client.pushPending().getOrThrow()}catch(_:CancellationException){} }
                "outcome" -> {server.normalizePushEffects={ rows->rows.map {if(it.table=="users" && it.key["id"]=="A") row("A","server-normalized",it.rowVersion) else it} };client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit={throw CancellationException("stored C")}));try{client.pushPending().getOrThrow()}catch(_:CancellationException){};client.setSnapshotApplyFaultInjectorForTest(null);db.execSQL("UPDATE users SET name='server-normalized' WHERE id='B'")}
                "rejected" -> {server.addRemoteBundleForTest(listOf(row("A","remote",2)));assertTrue(client.pushPending().isFailure);assertNotNull(client.pendingConflict().getOrThrow())}
                "ack_pending" -> {server.beforeAck={throw CancellationException("ack cut")};try{client.pushPending().getOrThrow()}catch(_:CancellationException){}}
            }
            server.beforeResolve={throw CancellationException("retain F")};server.beforeAck={throw CancellationException("retain ack")}
            val before=baselineQualificationVector(db,listOf("users","posts","_sync_upload_journal"));val count=snapshots(server);val acks=server.ackRequests.size
            client.retryRecovery().getOrThrow()
            // Explicit Sync may upload dirty intent; PullToStable preserves it and
            // exercises the public download/recovery admission without disposition.
            try { assertTrue((if(pending=="dirty")client.pullToStable().isFailure else client.sync().isFailure)) }catch(_:CancellationException){}
            assertEquals(count,snapshots(server));assertEquals(acks+if(pending=="ack_pending")1 else 0,server.ackRequests.size,pending)
            assertEquals(before,baselineQualificationVector(db,listOf("users","posts","_sync_upload_journal")))
        }
    }
    @Test fun guardedPreparationRaces() = runOwnedWebDatabaseTest {
        for (change in listOf("dirty","main","temp","comparison","mode")) fixture {server,db,client ->
            historical(server);val before=vector(db)
            server.beforeSnapshotChunkResponse={when(change) {
                "dirty" -> db.execSQL("UPDATE users SET name='new offline' WHERE id='A'")
                "main" -> db.execSQL("CREATE INDEX changed_name ON users(name)")
                "temp" -> db.execSQL("CREATE TEMP TRIGGER changed_temp AFTER UPDATE ON users BEGIN SELECT 1;END")
                "comparison" -> {db.execSQL("DROP INDEX users_name");db.execSQL("CREATE UNIQUE INDEX users_name ON users(name COLLATE NOCASE)")}
                "mode" -> client.setRecoveryMode(RecoveryMode.DISABLED).getOrThrow()
            }}
            assertTrue(client.sync().isFailure)
            if(change=="dirty") assertEquals("new offline",scalarText(db,"SELECT name FROM users WHERE id='A'")) else assertEquals(before,vector(db))
            assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }
    @Test fun cancelAndReopen() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        historical(server);val before=vector(db)
        server.beforeSnapshotChunkRequest={throw CancellationException("page cut")}
        assertFailsWith<CancellationException>{client.sync().getOrThrow()};assertEquals(before,vector(db))
        server.beforeSnapshotChunkRequest=null
        client.sync().getOrThrow();assertEquals(1,server.requestTrace.count{it=="POST /sync/snapshot-sessions"})
        assertEquals("completed",client.recoveryStatus().getOrThrow().phase)
    } }
    @Test fun unchangedFailureParks() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        historical(server,invalidSnapshot=true);val before=vector(db)
        assertTrue(client.sync().isFailure);assertEquals(before,vector(db));assertEquals("blocked",client.recoveryStatus().getOrThrow().phase)
        val requests=server.requestTrace.toList();assertIs<RecoveryBlockedException>(client.sync().exceptionOrNull());assertEquals(requests,server.requestTrace)
        client.retryRecovery().getOrThrow();client.retryRecovery().getOrThrow();assertTrue(client.sync().isFailure)
        assertEquals(1,server.requestTrace.count{it=="POST /sync/snapshot-sessions"})
        db.execSQL("DROP INDEX users_name");client.sync().getOrThrow();assertEquals(2,server.requestTrace.count{it=="POST /sync/snapshot-sessions"})
    } }
    @Test fun exchangeBudget() = runOwnedWebDatabaseTest { fixture(chunk=1) {server,db,client ->
        server.addRemoteBundleForTest((1..70).map {row("remote-$it","remote-$it")});server.retainedBundleFloor=2
        val initial=snapshots(server)
        assertIs<UploadRecoveryIncompleteException>(client.sync().exceptionOrNull())
        assertEquals(64,snapshots(server)-initial);assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        client.sync().getOrThrow();assertEquals(1,server.requestTrace.count{it=="POST /sync/snapshot-sessions"})
    } }
    @Test fun aggregateBoundsAndCleanup() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        historical(server);client.sync().getOrThrow()
        for(table in listOf("_sync_snapshot_stage","_sync_apply_rows","_sync_apply_edges","_sync_upload_journal")) assertEquals(0L,scalarLong(db,"SELECT count(*) FROM $table"),table)
        assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
    } }
    @Test fun relevantLocalDependencyRearms() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        db.execSQL("CREATE TABLE notes(id INTEGER PRIMARY KEY,parent TEXT REFERENCES users(id) ON DELETE CASCADE,body BLOB)")
        db.execSQL("INSERT INTO notes VALUES(7,'A',x'0001ff')")
        historical(server);assertTrue(client.sync().isFailure);assertEquals("blocked",client.recoveryStatus().getOrThrow().phase)
        val trace=server.requestTrace.toList();assertIs<RecoveryBlockedException>(client.sync().exceptionOrNull());assertEquals(trace,server.requestTrace)
        db.execSQL("DELETE FROM notes WHERE id=7");client.sync().getOrThrow();assertEquals(2,server.requestTrace.count {it=="POST /sync/snapshot-sessions"})
    } }
    @Test fun aggregateMetadataBoundaries() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        historical(server);client.sync().getOrThrow()
        val config=OversqliteConfig(1,listOf(SyncTable("users","id"),SyncTable("posts","id")),"fixture","main")
        val usage=OversqliteWorkspace(db,config).usage("boundary")
        val extra=WorkspaceCharge(32,4096)
        for(dimension in listOf("entries","bytes")) {
            val exact=config.copy(maxSyncWorkspaceEntries=usage.entries+extra.entries,maxSyncWorkspaceBytes=usage.bytes+extra.bytes)
            OversqliteWorkspace(db,exact).check("boundary",extra)
            val below=if(dimension=="entries")exact.copy(maxSyncWorkspaceEntries=exact.maxSyncWorkspaceEntries-1)else exact.copy(maxSyncWorkspaceBytes=exact.maxSyncWorkspaceBytes-1)
            assertEquals(dimension,assertFailsWith<SyncWorkspaceLimitException>{OversqliteWorkspace(db,below).check("boundary",extra)}.dimension)
        }
    } }
    @Test fun staticFailuresNeverDownload() = runOwnedWebDatabaseTest {
        for(failure in listOf("index","corrupt"))fixture {server,db,client ->
            historical(server);val before=vector(db)
            if(failure=="index")db.execSQL("CREATE UNIQUE INDEX unsupported_index ON users(lower(name))")
            else db.execSQL("UPDATE _sync_operation_state SET recovery_state='{\"version\":1,\"version\":1}'")
            assertTrue(client.sync().isFailure);assertEquals(0,snapshots(server));assertEquals(before,vector(db))
        }
    }

    @Test fun uploadAndSnapshotShareDispatchBudget() = runOwnedWebDatabaseTest(
        timeout = kotlin.time.Duration.parse(
            if (dev.goquick.sqlitenow.common.platform() == dev.goquick.sqlitenow.common.PlatformType.JS) "120s" else "60s"
        )
    ) { fixture(maxPushRows=1) {server,db,client ->
        repeat(64) {db.execSQL("INSERT INTO users VALUES('budget-$it','budget-$it')")}
        var ackCount=0
        lateinit var ackHook:suspend()->Unit
        ackHook={if(++ackCount==64) {val target=server.addRemoteBundleForTest(listOf(row("A","future",67)));server.retainedBundleFloor=target}else server.beforeAck=ackHook}
        server.beforeAck=ackHook
        val priorAcks=server.ackRequests.size
        assertIs<UploadRecoveryIncompleteException>(client.sync().exceptionOrNull())
        assertEquals(64,server.ackRequests.size-priorAcks);assertEquals(0,server.snapshotSessionCreateRequestCount)
        assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
        val source=scalarText(db,"SELECT json_group_array(json_array(source_id,next_source_bundle_id)) FROM _sync_source_state")
        client.sync().getOrThrow();assertEquals(1,server.snapshotSessionCreateRequestCount);assertEquals(64,server.ackRequests.size-priorAcks)
        assertEquals(source,scalarText(db,"SELECT json_group_array(json_array(source_id,next_source_bundle_id)) FROM _sync_source_state"))
    } }

    @Test fun disabledOrdinaryCorrection() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        historical(server);client.setRecoveryMode(RecoveryMode.DISABLED).getOrThrow()
        assertIs<FinalStateApplyBlockedException>(client.sync().exceptionOrNull());assertEquals(0,snapshots(server))
        assertEquals(0L,scalarLong(db,"SELECT rebuild_required FROM _sync_attachment_state"))
        db.execSQL("DROP INDEX users_name");client.sync().getOrThrow();assertEquals(0,snapshots(server));assertEquals(3L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
    } }

    @Test fun boundedExactStatus() {
        val keys=(0..80).map { "exact-\"key-$it" }
        val status=boundedRecoveryDiagnostic(RecoveryStatus(reason="conflicting_desired_state",table="users",keys=keys))
        assertEquals(keys.subList(0,64),status.keys);assertTrue(status.truncated)
        val large=boundedRecoveryDiagnostic(RecoveryStatus(table="x".repeat(8193),keys=listOf(" intact ")))
        assertEquals("",large.table);assertEquals(listOf(" intact "),large.keys);assertTrue(large.truncated)
        assertEquals(8192L,recoveryDiagnosticByteBound(RecoveryStatus(keys=listOf("x".repeat(9000)))))
    }
    @Test fun observationFloorFailureParks() = runOwnedWebDatabaseTest { fixture {server,db,client ->
        historical(server)
        // Preserve an installed authoritative observation newer than both the
        // blocked unit and server S. Snapshot creation must retain that floor.
        // row_version owns the authoritative version; the compact provenance observation carries none.
        db.execSQL("UPDATE _sync_row_state SET row_version=17 WHERE table_name='users' AND key_json='{\"id\":\"B\"}'")
        assertTrue(client.sync().isFailure)
        assertEquals("recovery_floor_ahead",client.recoveryStatus().getOrThrow().reason)
        val before=vector(db);val requests=server.requestTrace.toList()
        assertIs<RecoveryBlockedException>(client.sync().exceptionOrNull());assertEquals(requests,server.requestTrace)
        client.retryRecovery().getOrThrow();assertTrue(client.sync().isFailure)
        assertEquals(before,vector(db));assertEquals(2,server.snapshotSessionCreateRequestCount)
    } }

    @Test fun corruptCoordinatorNeverDownloads()=runOwnedWebDatabaseTest {
        for(field in listOf("empty_object","version","mode","modeRevision","capturedModeRevision","sourceId","localGuard","status.recoveryId"))fixture {server,db,client ->
            historical(server);server.beforeSnapshotCreateRequest={throw CancellationException("captured")}
            assertFailsWith<CancellationException>{client.sync().getOrThrow()}
            val path="$."+field
            if(field=="empty_object")db.execSQL("UPDATE _sync_operation_state SET recovery_state='{}'")
            else db.execSQL("UPDATE _sync_operation_state SET recovery_state=json_remove(recovery_state,'$path')")
            val trace=server.requestTrace.toList();val before=vector(db)
            assertTrue(client.sync().isFailure);assertEquals(trace,server.requestTrace);assertEquals(before,vector(db))
        }
    }
    @Test fun intrinsicApplyCapacityNeverDownloads()=runOwnedWebDatabaseTest {fixture {server,db,client ->
        client.closeFromCoordinatedOwner()
        val http=server.newHttpClient()
        val limited=DefaultOversqliteClient.forOpening(db,OversqliteConfig(1,listOf(SyncTable("users","id"),SyncTable("posts","id")),"fixture","main",snapshotApplyBatchBytes=1),http,DeferConflictResolver,Any())
        try{limited.openAndConnect("user-1").getOrThrow();historical(server);assertIs<SnapshotTransferLimitException>(limited.sync().exceptionOrNull());assertEquals(0,snapshots(server))}
        finally{limited.closeFromCoordinatedOwner();http.close()}
    } }
    @Test fun responseLimitsParkUnchangedFailure()=runOwnedWebDatabaseTest {
        for(failure in listOf(SnapshotResponseBodyTooLargeException("snapshot_chunk_fetch",1),SnapshotUnsupportedContentEncodingException("snapshot_chunk_fetch","unsupported"),SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)))fixture {server,_,client ->
            historical(server);server.beforeSnapshotChunkRequest={throw failure}
            assertTrue(client.sync().isFailure);assertEquals("blocked",client.recoveryStatus().getOrThrow().phase)
            val requests=server.requestTrace.toList();assertIs<RecoveryBlockedException>(client.sync().exceptionOrNull());assertEquals(requests,server.requestTrace)
        }
    }

    @Test fun publicRelevantReplanLimit()=runOwnedWebDatabaseTest {fixture {server,db,client ->
        historical(server);var version=3L
        lateinit var next:suspend()->Unit
        next={server.addRemoteBundleForTest(listOf(row("A","collision",version+1),row("B","collision",version+1)));server.addRemoteBundleForTest(listOf(row("A","b",version+2),row("B","a",version+2)));version+=2;server.beforeSnapshotCreateResponse=next}
        server.beforeSnapshotCreateResponse=next
        assertEquals("replan_limit",assertIs<RecoveryProgressIncompleteException>(client.sync().exceptionOrNull()).reason)
        assertEquals(4,server.snapshotSessionCreateRequestCount);assertEquals(9L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
    } }

    @Test fun transientContextReplacementsShareReplanBudget()=runOwnedWebDatabaseTest {fixture {server,db,client ->
        client.closeFromCoordinatedOwner();val http=server.newHttpClient()
        val config=OversqliteConfig(1,listOf(SyncTable("users","id"),SyncTable("posts","id")),"fixture","main",transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=10,initialBackoffMillis=0,maxBackoffMillis=0,jitterRatio=0.0))
        val resumed=DefaultOversqliteClient.forTesting(db,config,http)
        try {
            resumed.openAndConnect("user-1").getOrThrow();historical(server);var attempt=0
            lateinit var fail:suspend()->Unit
            fail={db.execSQL("CREATE TEMP TABLE drift_${++attempt}(value TEXT)");server.beforeSnapshotChunkRequest=fail;throw kotlinx.io.IOException("synthetic transient page failure")}
            server.beforeSnapshotChunkRequest=fail
            assertEquals("replan_limit",assertIs<RecoveryProgressIncompleteException>(resumed.sync().exceptionOrNull()).reason)
            assertEquals(4,server.snapshotSessionCreateRequestCount);assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }finally{resumed.closeFromCoordinatedOwner();http.close()}
    } }

    @Test fun retainedCorruptStageParks()=runOwnedWebDatabaseTest {
        for(change in listOf("key_json='{}'","payload='{\"id\":\"A\",\"name\":\"a different valid payload\"}'","table_name='unknown'","row_version=999"))fixture(chunk=1) {server,db,client ->
            historical(server);var pages=0
            lateinit var cut:suspend()->Unit
            cut={if(++pages==2)throw CancellationException("incomplete S") else server.beforeSnapshotChunkRequest=cut}
            server.beforeSnapshotChunkRequest=cut
            assertFailsWith<CancellationException>{client.sync().getOrThrow()}
            db.execSQL("UPDATE _sync_snapshot_stage SET $change WHERE row_ordinal=1")
            assertTrue(client.sync().isFailure);assertEquals("blocked",client.recoveryStatus().getOrThrow().phase)
            val requests=server.requestTrace.toList();assertIs<RecoveryBlockedException>(client.sync().exceptionOrNull());assertEquals(requests,server.requestTrace)
            assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }
    @Test fun emptyDependencyMetadataWork()=runOwnedWebDatabaseTest {
        for(work in listOf(1L,8L,17L,1000000L))fixture(work) {server,db,client ->
            val columns=(0..63).joinToString(","){"c$it TEXT"}
            db.execSQL("CREATE TABLE empty_child(id INTEGER PRIMARY KEY,parent TEXT REFERENCES users(id),$columns)")
            repeat(32){db.execSQL("CREATE TABLE local_$it(value TEXT)")}
            historical(server);client.sync().getOrThrow();assertTrue(client.workspaceDiagnostics().work>=64*128);assertEquals(1,server.snapshotSessionCreateRequestCount)
        }
    }

}
