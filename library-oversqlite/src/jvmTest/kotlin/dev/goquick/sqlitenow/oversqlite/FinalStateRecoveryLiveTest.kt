package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*

/** Scoped synthetic upload, clean snapshot and public automatic recovery HTTP proofs. */
internal class FinalStateRecoveryLiveTest : RealServerSupport() {
    // The owning JVM lane has no authority. It reports these as native skips;
    // dedicated owned HTTP lanes must opt in and execute every selected body.
    private fun ownedHttpTest(method:String,block:suspend kotlinx.coroutines.CoroutineScope.()->Unit) {
        org.junit.Assume.assumeTrue("owned HTTP lane requires OVERSQLITE_REALSERVER_TESTS=true",realServerEnv("OVERSQLITE_REALSERVER_TESTS")=="true")
        runRealServerTest {
            block()
            println("RECOVERY_HTTP_BODY FinalStateRecoveryLiveTest.$method PASS")
        }
    }
    private val tables=listOf(SyncTable("categories","id"))
    private suspend fun attempt(block:suspend ()->Unit):Result<Unit> = try {block();Result.success(Unit)}catch(e:Throwable){Result.failure(e)}
    private suspend fun schema(db:SafeSQLiteConnection) = db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
    private suspend fun vector(db:SafeSQLiteConnection)=baselineQualificationVector(db,listOf("categories","_sync_upload_journal","_sync_admitted_contracts","_sync_apply_plan","_sync_apply_rows","_sync_apply_edges","_sync_push_plan","_sync_push_nodes","_sync_push_edges","_sync_apply_state","_sync_snapshot_stage"))
    private suspend fun controls(http:HttpClient,action:String,user:String,source:String="",key:String=""):JsonObject {
        val response=http.post("__recovery-fixture/$action") {contentType(ContentType.Application.Json);setBody(buildJsonObject {put("scope",user);put("source",source);put("key",key)}.toString())}
        assertEquals(HttpStatusCode.OK,response.status);return Json.parseToJsonElement(response.bodyAsText()).jsonObject
    }
    private fun activityFixtureUrl():String = assertNotNull(realServerEnv("OVERSQLITE_ACTIVITY_REAL_SERVER_SMOKE_BASE_URL")
        ?: realServerEnv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL"),"owned Activity HTTP fixture required")
    private fun HttpRequestBuilder.proof(source:String,generation:SyncGeneration) {
        header("Oversync-Protocol-Version","v1");header("Oversync-Source-ID",source)
        header("Oversync-Schema-Generation-ID",generation.id);header("Oversync-Schema-Generation-Fingerprint",generation.fingerprint)
        contentType(ContentType.Application.Json)
    }

    @Test fun lostResponsePrunedHistoryLostAckAndRetiredReopen() = ownedHttpTest("lostResponsePrunedHistoryLostAckAndRetiredReopen") {
        val cfg=assertNotNull(requireRealServerConfig(),"owned live fixture required; no skip credit")
        val path=createTempSqliteNowTestDbPath("c03-live");var db=newFileBackedDb(path)
        val user=randomRealServerId("c03-lost");var client:DefaultOversqliteClient?=null;var http:HttpClient?=null
        try {
            schema(db);val source=bootstrapManagedSourceId(db,cfg.baseUrl,tables)
            var losePush=true;var loseAck=true;val trace=mutableListOf<String>()
            val base=newRealServerHttpClient(cfg.baseUrl,issueDummySigninToken(cfg.baseUrl,user,source),beforeRequest={trace+=it})
            http=base.config {install("C03LostReplies") {receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                val route=response.call.request.url.encodedPath
                if((route=="/sync/push" && losePush)||(route=="/sync/push-outcomes/ack" && loseAck)) {
                    assertTrue(response.status.value==200 || response.status.value==204)
                    if(route=="/sync/push")losePush=false else loseAck=false
                    throw CancellationException("owned lost terminal reply: $route")
                }
            }}}
            base.close()
            client=newRealServerClient(db,http,tables);client.open().getOrThrow();client.attach(user).getOrThrow()
            val id=randomRealServerUuid();db.execSQL("INSERT INTO categories VALUES('$id','frozen',NULL)")
            assertTrue(attempt {client!!.pushPending().getOrThrow()}.isFailure)
            val frozen=scalarText(db,"SELECT hex(request_body) FROM _sync_upload_journal")
            val pruned=controls(http,"prune",user);assertEquals(0,pruned.getValue("history").jsonPrimitive.int)
            assertTrue(attempt {client!!.pushPending().getOrThrow()}.isFailure)
            assertEquals("ack_pending",scalarText(db,"SELECT phase FROM _sync_upload_journal"))
            val before=vector(db)
            val original=controls(http,"retire",user,source);assertEquals(0,original.getValue("outcomes").jsonPrimitive.int)
            client.closeFromCoordinatedOwner();db.close();db=newFileBackedDb(path)
            client=newRealServerClient(db,http,tables);trace.clear()
            client.open().getOrThrow();client.attach(user).getOrThrow()
            assertEquals(frozen,scalarText(db,"SELECT hex(request_body) FROM _sync_upload_journal"))
            client.pushPending().getOrThrow()
            assertEquals(listOf("/sync/push-outcomes/ack"),trace)
            val after=vector(db)
            for(table in listOf("categories","_sync_dirty_rows","_sync_outbox_rows","_sync_source_state","_sync_attachment_state","_sync_row_state"))assertEquals(before.rows(table),after.rows(table),table)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(2L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            println("C03_LIVE_COMPLETE lost_response_prune_lost_ack_retired_reopen source=$source frozen_unchanged=true no_reapply=true")
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close();deleteTempSqliteNowTestDbArtifacts(path)}
    }

    @Test fun resolveBeforeDelayedPushPermanentlyCancelsOriginal() = ownedHttpTest("resolveBeforeDelayedPushPermanentlyCancelsOriginal") {
        val cfg=assertNotNull(requireRealServerConfig(),"owned live fixture required")
        val db=newDb();val user=randomRealServerId("c03-cancel");var client:DefaultOversqliteClient?=null;var http:HttpClient?=null
        try {
            schema(db);val source=bootstrapManagedSourceId(db,cfg.baseUrl,tables)
            var freeze=false
            val base=newRealServerHttpClient(cfg.baseUrl,issueDummySigninToken(cfg.baseUrl,user,source))
            http=base.config {install("C03BeforeDispatch") {sendPipeline.intercept(HttpSendPipeline.Before) {
                if(freeze && context.url.build().encodedPath=="/sync/push") {freeze=false;throw CancellationException("before engine")}
            }}};base.close()
            client=newRealServerClient(db,http,tables);client.open().getOrThrow();client.attach(user).getOrThrow()
            val id=randomRealServerUuid();db.execSQL("INSERT INTO categories VALUES('$id','preserved',NULL)")
            freeze=true;assertTrue(attempt {client!!.pushPending().getOrThrow()}.isFailure)
            val raw=scalarText(db,"SELECT CAST(request_body AS TEXT) FROM _sync_upload_journal")
            val generation=OversqliteContractStore(db).readGeneration(1)
            client.pushPending().getOrThrow()
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            val delayed=http.post("sync/push") {proof(source,generation);setBody(raw)}
            assertEquals(HttpStatusCode.Gone,delayed.status);assertEquals("push_outcome_acknowledged",Json.parseToJsonElement(delayed.bodyAsText()).jsonObject.getValue("error").jsonPrimitive.content)
            assertEquals(0,controls(http,"state",user).getValue("categories").jsonPrimitive.int)
            client.pushPending().getOrThrow();assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals("preserved",scalarText(db,"SELECT name FROM categories WHERE id='$id'"))
            println("C03_LIVE_COMPLETE resolve_before_delayed_push source=$source delayed=410 successor=2")
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
    }

    @Test fun rejectedOutcomeUsesRealConflictAndPreservesLaterEdit() = ownedHttpTest("rejectedOutcomeUsesRealConflictAndPreservesLaterEdit") {
        val cfg=assertNotNull(requireRealServerConfig(),"owned live fixture required")
        val db=newDb();val user=randomRealServerId("c03-reject");var client:DefaultOversqliteClient?=null;var http:HttpClient?=null;var remote:HttpClient?=null
        try {
            schema(db);val source=bootstrapManagedSourceId(db,cfg.baseUrl,tables)
            http=newRealServerHttpClient(cfg.baseUrl,issueDummySigninToken(cfg.baseUrl,user,source))
            client=newRealServerClient(db,http,tables);client.open().getOrThrow();client.attach(user).getOrThrow()
            val id=randomRealServerUuid();db.execSQL("INSERT INTO categories VALUES('$id','seed',NULL)");client.pushPending().getOrThrow()
            val generation=OversqliteContractStore(db).readGeneration(1)
            val other=randomRealServerId("c03-other")
            remote=newRealServerHttpClient(cfg.baseUrl,issueDummySigninToken(cfg.baseUrl,user,other))
            val connect=remote.post("sync/connect") {proof(other,generation);setBody("""{"has_local_pending_rows":false}""")};assertEquals(HttpStatusCode.OK,connect.status);connect.bodyAsText()
            val rows=listOf(PushRequestRow("business","categories",mapOf("id" to id),"UPDATE",1,buildJsonObject {put("id",id);put("name","remote");put("parent_id",JsonNull)}))
            val hash=computePushRequestHashForTest(rows,generation)
            val body=canonicalizeJsonElement(buildJsonObject {put("source_bundle_id",1);put("canonical_request_hash",hash);put("rows",OversqliteCompactCodec.encodePushRows(generation,rows))})
            val committed=remote.post("sync/push") {proof(other,generation);setBody(body)};assertEquals(HttpStatusCode.OK,committed.status)
            val outcome=decodePushOutcome(committed.bodyAsText());assertEquals("committed",outcome.status)
            val ack=remote.post("sync/push-outcomes/ack") {proof(other,generation);setBody(buildJsonObject {put("source_bundle_id",1);put("canonical_request_hash",hash);put("outcome_hash",outcome.outcomeHash)}.toString())};assertEquals(HttpStatusCode.NoContent,ack.status)
            db.execSQL("UPDATE categories SET name='frozen' WHERE id='$id'")
            assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals("rejected",scalarText(db,"SELECT state FROM _sync_outbox_bundle"));assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
            val pending=assertNotNull(client.pendingConflict().getOrThrow())
            db.execSQL("UPDATE categories SET name='later' WHERE id='$id'")
            val beforeDecision=vector(db)
            assertIs<StaleConflictResolutionException>(client.retryConflict(pending).exceptionOrNull())
            assertEquals(beforeDecision,vector(db))
            client.retryConflict(assertNotNull(client.pendingConflict().getOrThrow())).getOrThrow()
            assertEquals("later",scalarText(db,"SELECT name FROM categories WHERE id='$id'"));assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            println("C03_LIVE_COMPLETE real_rejected_conflict source=$source original_seq=2 next_seq=3 later_edit_preserved=true")
        } finally {client?.closeFromCoordinatedOwner();http?.close();remote?.close();db.close()}
    }

    @Test fun unsafeCommittedOutcomeRetainsEverythingAndNeverSnapshots() = ownedHttpTest("unsafeCommittedOutcomeRetainsEverythingAndNeverSnapshots") {
        val cfg=assertNotNull(requireRealServerConfig(),"owned live fixture required")
        val db=newDb();val user=randomRealServerId("c03-unsafe");var client:DefaultOversqliteClient?=null;var http:HttpClient?=null
        try {
            schema(db);db.execSQL("CREATE UNIQUE INDEX local_parent ON categories(parent_id)")
            val source=bootstrapManagedSourceId(db,cfg.baseUrl,tables);var addLater=false;var acks=0;var snapshots=0
            val p=randomRealServerUuid();val q=randomRealServerUuid();val child=randomRealServerUuid();val later=randomRealServerUuid()
            val base=newRealServerHttpClient(cfg.baseUrl,issueDummySigninToken(cfg.baseUrl,user,source),beforeRequest={if(it=="/sync/push-outcomes/ack")acks++;if(it=="/sync/snapshot-sessions")snapshots++})
            http=base.config {install("C03LaterUniqueConflict") {receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                if(addLater && response.call.request.url.encodedPath=="/sync/push") {addLater=false;db.execSQL("INSERT INTO categories VALUES('$later','later','$q')")}
            }}};base.close()
            client=newRealServerClient(db,http,tables);client.open().getOrThrow();client.attach(user).getOrThrow()
            db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','a1-other',NULL),('$child','child','$p')");client.pushPending().getOrThrow();acks=0
            db.execSQL("UPDATE categories SET name='a1-delete' WHERE id='$p'");addLater=true
            assertTrue(client.pushPending().isFailure)
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen' AND length(outcome_body)>0"))
            val before=vector(db);assertTrue(client.pushPending().isFailure);assertEquals(before,vector(db))
            assertTrue(client.rebuild().isFailure);assertEquals(before,vector(db));assertEquals(0,acks);assertEquals(0,snapshots)
            println("C03_LIVE_COMPLETE unsafe_unique source=$source F_C_retained=true ack=0 snapshot=0")
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
    }

    @Test fun cleanLaterSnapshotPassesBadHistoryAndConsumesPostBoundaryWrite() = ownedHttpTest("cleanLaterSnapshotPassesBadHistoryAndConsumesPostBoundaryWrite") {
        val cfg=assertNotNull(requireRealServerConfig(),"owned category HTTP fixture required")
        val db=newDb();val user=randomRealServerId("recovery-history")
        var client:DefaultOversqliteClient?=null;var http:HttpClient?=null
        try {
            schema(db);val source=bootstrapManagedSourceId(db,cfg.baseUrl,tables)
            val trace=mutableListOf<String>();val left=randomRealServerUuid();val right=randomRealServerUuid()
            var postSnapshot=false;var afterSnapshot=0L
            val base=newRealServerHttpClient(cfg.baseUrl,issueDummySigninToken(cfg.baseUrl,user,source),beforeRequest={trace+=it})
            http=base.config {install("HistoricalSnapshotProof") {
                sendPipeline.intercept(HttpSendPipeline.Before) {
                    if(context.url.build().encodedPath.startsWith("/sync/")) {
                        assertEquals("v1",context.headers["Oversync-Protocol-Version"])
                        println("RECOVERY_HTTP_IDENTITY route=${context.url.encodedPath} source=${context.headers["Oversync-Source-ID"]} generation=${context.headers["Oversync-Schema-Generation-ID"]} fingerprint=${context.headers["Oversync-Schema-Generation-Fingerprint"]}")
                    }
                }
                receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                    if(postSnapshot && response.call.request.method==HttpMethod.Post && response.call.request.url.encodedPath=="/sync/snapshot-sessions") {
                        postSnapshot=false
                        afterSnapshot=controls(http!!,"post_snapshot_write",user,key=left).getValue("bundle_seq").jsonPrimitive.long
                    }
                }
            }};base.close()
            client=newRealServerClient(db,http,tables);client.open().getOrThrow();client.attach(user).getOrThrow()
            db.execSQL("INSERT INTO categories VALUES('$left','left',NULL),('$right','right',NULL)")
            client.pushPending().getOrThrow();db.execSQL("CREATE UNIQUE INDEX local_category_name ON categories(name)")
            val sequence=scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state")
            val bad=controls(http,"historical_bad",user).getValue("bundle_seq").jsonPrimitive.long
            val target=controls(http,"historical_good",user,key=left).getValue("bundle_seq").jsonPrimitive.long
            assertTrue(target>bad);trace.clear();postSnapshot=true
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT,client.sync().getOrThrow().remoteOutcome)
            val status=client.recoveryStatus().getOrThrow()
            assertEquals("completed",status.phase);assertEquals(target,status.observedBoundary)
            assertTrue(afterSnapshot>target);assertEquals(afterSnapshot,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals("post-T",scalarText(db,"SELECT name FROM categories WHERE id='$left'"))
            assertEquals("left",scalarText(db,"SELECT name FROM categories WHERE id='$right'"))
            assertEquals(sequence,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(1,trace.count {it=="/sync/snapshot-sessions"})
            assertEquals(0,trace.count {it=="/sync/push" || it=="/sync/push-outcomes/ack"})
            for(table in listOf("_sync_snapshot_stage","_sync_apply_plan","_sync_apply_rows","_sync_dirty_rows","_sync_outbox_rows","_sync_upload_journal"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM $table"),table)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            println("RECOVERY_HTTP_COMPLETE bad=$bad snapshot_T=$target post_T=$afterSnapshot vector=${vector(db)}")
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
    }
    @Test fun testActivityDT01() = ownedHttpTest("testActivityDT01") { activitySnapshot("DT01") }
    @Test fun testActivityDT02() = ownedHttpTest("testActivityDT02") { activitySnapshot("DT02") }
    @Test fun testActivityDT03() = ownedHttpTest("testActivityDT03") { activitySnapshot("DT03") }
    @Test fun testActivityDT04() = ownedHttpTest("testActivityDT04") { activitySnapshot("DT04") }
    @Test fun testActivityDT05() = ownedHttpTest("testActivityDT05") { activitySnapshot("DT05") }
    @Test fun testActivityDT06() = ownedHttpTest("testActivityDT06") { activitySnapshot("DT06") }


    private suspend fun activityBusinessVector(db: SafeSQLiteConnection,names: List<String>): Map<String,List<JsonObject>> {
        val result = linkedMapOf<String,List<JsonObject>>()
        for (name in names) {
            val columns = db.queryList("PRAGMA table_info(\"$name\")") { it.getText(1) }
            val fields = columns.joinToString(",") { column ->
                "'$column',json_object('storage',typeof(\"$column\"),'value',CASE WHEN typeof(\"$column\")='blob' THEN lower(hex(\"$column\")) WHEN \"$column\" IS NULL THEN NULL ELSE CAST(\"$column\" AS TEXT) END)"
            }
            result[name] = db.queryList("SELECT json_object($fields) FROM \"$name\" ORDER BY ${columns.joinToString(",") { "\"$it\"" }}") { Json.parseToJsonElement(it.getText(0)).jsonObject }
        }
        return result
    }

    private suspend fun activitySnapshot(scenario: String,automatic:Boolean=false) {
        assertEquals("true",realServerEnv("OVERSQLITE_REALSERVER_TESTS"),"owned HTTP lane required")
        val url = activityFixtureUrl()
        val db = newDb(); val expected = newDb(); var client: DefaultOversqliteClient? = null; var http: HttpClient? = null
        val scope = randomRealServerUuid(); val tables = ActivityRecoveryFixture.tables
        try {
            ActivityRecoveryFixture.create(db); expected.execSQL("PRAGMA foreign_keys=ON"); ActivityRecoveryFixture.create(expected); ActivityRecoveryFixture.seed(expected)
            val source = bootstrapManagedSourceId(db,url,tables)
            val requests = mutableListOf<String>()
            http = newRealServerHttpClient(url,issueDummySigninToken(url,scope,source),beforeRequest={ requests += it })
            client = newRealServerClient(db,http,tables,maxPushRows=256,maxSnapshotChunkRows=2)
            client.open().getOrThrow();client.attach(scope).getOrThrow()
            ActivityRecoveryFixture.seed(db);client.pushPending().getOrThrow()
            assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            val names = ActivityRecoveryFixture.generation().tables.map { it.table } + "app_settings"
            val settings = scalarText(db,"SELECT json_array(rowid,id,bootstrapped_date,most_recent_sync_date,bootstrap_version,wizard_completed) FROM app_settings")
            suspend fun change(case: String): JsonObject {
                val response = http.post("/__snapshot-fixture/change") {
                    contentType(ContentType.Application.Json)
                    setBody(buildJsonObject { put("scope",scope);put("scenario",case) }.toString())
                }
                assertEquals(HttpStatusCode.OK,response.status)
                return Json.parseToJsonElement(response.bodyAsText()).jsonObject
            }
            if (scenario == "DT06") {
                val before = baselineQualificationVector(db,names)
                assertEquals(true,change("DT06_invalid").getValue("rejected").jsonPrimitive.boolean)
                assertEquals(before,baselineQualificationVector(db,names))
            }
            val target = change(scenario).getValue("bundle_seq").jsonPrimitive.long
            val sql = when(scenario) {
                "DT01" -> "UPDATE value_registry SET numeric_value=74.5 WHERE key='body-weight';UPDATE profile SET essential_plan=0,unlock_codes='[\"retained text\"]' WHERE name='profile-name'"
                "DT02" -> "UPDATE profile SET body_weight=74 WHERE name='profile-name'"
                "DT03" -> "UPDATE activity SET sched_write_ref_start_at=NULL,sched_write_ref_repeat_after_days=NULL,sched_write_ref_days_duration=NULL WHERE id='A';UPDATE activity SET sched_write_ref_start_at='date-a',sched_write_ref_repeat_after_days='repeat-a',sched_write_ref_days_duration='duration-a' WHERE id='B';UPDATE program_item_input_goal SET input_key='weight-transferred' WHERE id='G'"
                "DT04" -> "DELETE FROM activity WHERE id='B'"
                "DT05" -> "UPDATE daily_log SET parent_daily_log_id=NULL WHERE id='L2';UPDATE daily_log SET parent_daily_log_id='L2',notes='child now' WHERE id='L1'"
                "DT06" -> "UPDATE program_item_input_goal_revision SET effective_day='2026-12-31',policy_json='{\"min\":7.25}' WHERE id='R'"
                else -> error("unknown maintained Activity case")
            }
            for (statement in sql.split(';')) expected.execSQL(statement)
            assertEquals(1L,scalarLong(expected,"PRAGMA foreign_keys"));assertEquals(1L,scalarLong(db,"PRAGMA foreign_keys"))
            val expectedRows = activityBusinessVector(expected,names)
            requests.clear()
            if(automatic) {
                assertEquals(target,change("history_prune").getValue("bundle_seq").jsonPrimitive.long)
                requests.clear();assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT,client.sync().getOrThrow().remoteOutcome)
                val status=client.recoveryStatus().getOrThrow();assertEquals("completed",status.phase);assertEquals(target,status.observedBoundary)
            } else client.rebuild().getOrThrow()
            assertEquals(expectedRows,baselineQualificationVector(db,names).tables.filterKeys { it in names },scenario)
            assertEquals(settings,scalarText(db,"SELECT json_array(rowid,id,bootstrapped_date,most_recent_sync_date,bootstrap_version,wizard_completed) FROM app_settings"))
            assertEquals(target,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(1,requests.count { it == "/sync/snapshot-sessions" })
            assertEquals(0,requests.count { it == "/sync/push" || it == "/sync/push-outcomes/ack" })
            for (table in listOf("_sync_snapshot_stage","_sync_apply_plan","_sync_apply_rows","_sync_dirty_rows","_sync_upload_journal"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM $table"),table)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            println("C04_ACTIVITY_HTTP_COMPLETE $scenario managed_tables=14 T=$target full_values=true settings_rowid=true source=$source")
        } finally { client?.closeFromCoordinatedOwner();http?.close();db.close();expected.close() }
    }

    @Test fun testActivityAutomaticRecovery()=ownedHttpTest("testActivityAutomaticRecovery") {activitySnapshot("DT02",automatic=true)}

    @Test fun testActivityDT07()=ownedHttpTest("testActivityDT07") {
        assertEquals("true",realServerEnv("OVERSQLITE_REALSERVER_TESTS"))
        val fixtureUrl=activityFixtureUrl()
        for(pending in listOf("dirty","prepared","unsafe_committed","ack_pending")) {
            val db=newDb();val scope=randomRealServerUuid();val tables=ActivityRecoveryFixture.tables
            var client:DefaultOversqliteClient?=null;var http:HttpClient?=null
            try {
                ActivityRecoveryFixture.create(db);val source=bootstrapManagedSourceId(db,fixtureUrl,tables)
                val trace=mutableListOf<String>();var retain=false
                val base=newRealServerHttpClient(fixtureUrl,issueDummySigninToken(fixtureUrl,scope,source),beforeRequest={trace+=it})
                http=base.config {install("AutomaticPendingCut") {sendPipeline.intercept(HttpSendPipeline.Before) {
                    val path=context.url.build().encodedPath
                    if(retain && (path=="/sync/push-outcomes/ack" || path=="/sync/push-outcomes/resolve" || pending=="prepared" && path=="/sync/push"))throw CancellationException("retain $pending")
                }}};base.close()
                client=newRealServerClient(db,http,tables,maxPushRows=256);client.open().getOrThrow();client.attach(scope).getOrThrow()
                ActivityRecoveryFixture.seed(db);client.pushPending().getOrThrow();trace.clear()
                when(pending) {
                    "dirty" -> db.execSQL("UPDATE daily_log SET notes='offline' WHERE id='L1'")
                    "prepared" -> {db.execSQL("UPDATE daily_log SET notes='offline' WHERE id='L1'");retain=true;assertTrue(attempt{client!!.pushPending().getOrThrow()}.isFailure)}
                    "unsafe_committed" -> {
                        db.execSQL("CREATE UNIQUE INDEX local_notes ON daily_log(notes)");db.execSQL("UPDATE daily_log SET notes='normalize' WHERE id='L1'")
                        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit={throw CancellationException("retain C")}))
                        assertTrue(attempt{client!!.pushPending().getOrThrow()}.isFailure);client.setSnapshotApplyFaultInjectorForTest(null)
                        assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal WHERE length(outcome_body)>0"))
                        db.execSQL("UPDATE daily_log SET notes='occupied' WHERE id='L2'")
                    }
                    "ack_pending" -> {db.execSQL("UPDATE daily_log SET notes='ack pending' WHERE id='L1'");retain=true;assertTrue(attempt{client!!.pushPending().getOrThrow()}.isFailure);assertEquals("ack_pending",scalarText(db,"SELECT phase FROM _sync_upload_journal"))}
                }
                for(scenario in listOf("DT02","history_prune")) {
                    val response=http.post("/__snapshot-fixture/change") {contentType(ContentType.Application.Json);setBody(buildJsonObject{put("scope",scope);put("scenario",scenario)}.toString())}
                    assertEquals(HttpStatusCode.OK,response.status);response.bodyAsText()
                }
                val names=ActivityRecoveryFixture.generation().tables.map{it.table}+"app_settings"
                val before=baselineQualificationVector(db,names);trace.clear();client.retryRecovery().getOrThrow()
                assertTrue(attempt{if(pending=="dirty")client!!.pullToStable().getOrThrow() else client!!.sync().getOrThrow()}.isFailure)
                assertEquals(before,baselineQualificationVector(db,names),pending);assertEquals(0,trace.count {"snapshot-sessions" in it})
                println("AUTOMATIC_ACTIVITY_HTTP DT07 $pending 14tables full_state=true snapshot=0 source=$source")
            } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
        }
    }

}
