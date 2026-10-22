package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.plugins.HttpRequestRetry
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.*
import kotlin.test.*

internal class FinalStateRecoveryTest : SurvivorTestSupport() {
    private fun swap() = listOf(
        BundleRow("main","nodes",mapOf("id" to "P"),"UPDATE",2,Json.parseToJsonElement("""{"id":"P","name":"other","parent_id":null}""")),
        BundleRow("main","nodes",mapOf("id" to "Q"),"UPDATE",2,Json.parseToJsonElement("""{"id":"Q","name":"parent","parent_id":null}""")),
    )
    private suspend fun vector(db: SafeSQLiteConnection) = BaselineQualificationVector(baselineQualificationVector(db,listOf("nodes",
        "_sync_row_state","_sync_dirty_rows","_sync_outbox_bundle","_sync_outbox_rows","_sync_upload_journal",
        "_sync_source_state","_sync_attachment_state")).tables.filterKeys { it != "_sync_operation_state" })

    @Test fun cleanAdmission() = runOwnedWebDatabaseTest {
        for (pending in listOf("dirty", "prepared", "stored_outcome", "rejected", "ack_pending")) withSurvivorFixture { server,db,_,client ->
            val acknowledgedBefore = server.ackRequests.size
            db.execSQL("UPDATE nodes SET name='pending' WHERE id='P'")
            if (pending == "prepared") {
                server.cancelNextPush = true
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            }
            if (pending == "stored_outcome") {
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = { throw CancellationException("stored outcome before settlement") }))
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                client.setSnapshotApplyFaultInjectorForTest(null)
                assertTrue(scalarLong(db,"SELECT length(outcome_body) FROM _sync_upload_journal") > 0)
                assertEquals(acknowledgedBefore,server.ackRequests.size)
            }
            if (pending == "rejected") {
                server.applicationRejectionCode = "application_rejected"
                assertTrue(client.pushPending().isFailure)
                assertEquals("rejected",scalarText(db,"SELECT state FROM _sync_outbox_bundle"))
            }
            if (pending == "ack_pending") {
                server.beforeAck = { throw CancellationException("owned ack cut") }
                try { client.pushPending().getOrThrow() } catch (_: CancellationException) { }
                assertEquals("ack_pending",scalarText(db,"SELECT phase FROM _sync_upload_journal"))
            }
            val before = vector(db)
            val requests = server.requestTrace.toList()
            assertTrue(client.rebuild().isFailure)
            assertEquals(requests,server.requestTrace,"entire scope admission precedes any network")
            assertEquals(before,vector(db))
        }
    }

    @Test fun snapshotPreservesLogicalAudit() = runOwnedWebDatabaseTest {
        for (kind in listOf("main","temp","sdk_spoof","unchanged")) withSurvivorFixture { server,db,_,client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            db.execSQL("CREATE TABLE audit(value TEXT)")
            val name = if (kind == "sdk_spoof") "trg_nodes_ad" else "application_delete"
            if (kind == "sdk_spoof") db.execSQL("DROP TRIGGER trg_nodes_ad")
            db.execSQL("CREATE ${if(kind == "temp") "TEMP " else ""}TRIGGER $name AFTER DELETE ON nodes BEGIN INSERT INTO audit VALUES(old.id); END")
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            if (kind != "unchanged") server.addRemoteBundleForTest(swap())
            val before = vector(db)
            val result = client.rebuild()
            if (kind == "unchanged") result.getOrThrow() else {
                assertEquals(name,assertIs<FinalStateApplyBlockedException>(result.exceptionOrNull()).trigger)
                assertEquals(before,vector(db))
            }
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM audit"))
        }
    }

    @Test fun snapshotPreservesLocalOnlyClosure() = runOwnedWebDatabaseTest {
        for (action in listOf("CASCADE","NO ACTION","empty")) withSurvivorFixture { server,db,_,client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            db.execSQL("CREATE TABLE notes(id INTEGER PRIMARY KEY,parent TEXT REFERENCES nodes(id) ON DELETE ${if(action=="empty")"CASCADE" else action} DEFERRABLE INITIALLY DEFERRED,body BLOB)")
            db.execSQL("CREATE TABLE app_settings(id INTEGER PRIMARY KEY,value TEXT)")
            db.execSQL("INSERT INTO app_settings VALUES(41,'retained settings')")
            if (action != "empty") db.execSQL("INSERT INTO notes VALUES(77,'P',x'00aaff')")
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            val before = vector(db)
            val local = baselineQualificationVector(db,listOf("notes","app_settings")).tables.filterKeys { it in setOf("notes","app_settings") }
            val ids = scalarText(db,"SELECT json_group_array(json_array(rowid,id)) FROM (SELECT rowid,id FROM nodes ORDER BY id)")
            server.addRemoteBundleForTest(swap())
            val result = client.rebuild()
            if (action == "CASCADE") {
                assertIs<FinalStateApplyBlockedException>(result.exceptionOrNull())
                assertEquals(before,vector(db))
            } else {
                result.getOrThrow()
                assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            }
            assertEquals(local,baselineQualificationVector(db,listOf("notes","app_settings")).tables.filterKeys { it in setOf("notes","app_settings") })
            assertEquals(ids,scalarText(db,"SELECT json_group_array(json_array(rowid,id)) FROM (SELECT rowid,id FROM nodes ORDER BY id)"))
            assertEquals("P",scalarText(db,"SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals("C",scalarText(db,"SELECT parent_id FROM nodes WHERE id='D'"))
        }
    }

    @Test fun dependencyMembership() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            db.execSQL("CREATE TABLE notes(id INTEGER PRIMARY KEY,parent TEXT REFERENCES nodes(id) ON DELETE CASCADE,body TEXT)")
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            val before = vector(db)
            server.addRemoteBundleForTest(swap())
            server.beforeSnapshotCreateResponse = { db.execSQL("INSERT INTO notes VALUES(91,'P','late child')") }
            assertIs<FinalStateApplyBlockedException>(client.rebuild().exceptionOrNull())
            assertEquals(before,vector(db))
            assertEquals("late child",scalarText(db,"SELECT body FROM notes WHERE id=91"))
        }
    }

    @Test fun snapshotDeltaRollbackAndVerifiedReuse() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
            val before = vector(db)
            server.addRemoteBundleForTest(swap())
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(afterAppliedRow = { throw CancellationException("during final delta") }))
            assertFailsWith<CancellationException> { client.rebuild().getOrThrow() }
            assertEquals(before,vector(db))
            assertEquals(1L,scalarLong(db,"SELECT snapshot_stage_complete FROM _sync_operation_state"))
            val created = server.snapshotSessionCreateRequestCount
            val pages = server.snapshotChunkRequests.size
            server.snapshotSessionIdsForTest().forEach { server.expireSnapshotSessionForTest(it) }
            client.setSnapshotApplyFaultInjectorForTest(null)
            client.rebuild().getOrThrow()
            assertEquals(created,server.snapshotSessionCreateRequestCount)
            assertEquals(pages,server.snapshotChunkRequests.size,"complete local seal survives remote expiry")
            assertEquals("other",scalarText(db,"SELECT name FROM nodes WHERE id='P'"))
            assertEquals("P",scalarText(db,"SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_snapshot_stage"))
        }
    }

    @Test fun snapshotTinySlices() = runOwnedWebDatabaseTest {
        withSurvivorFixture(workUnits = 1) { server,db,_,client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            server.addRemoteBundleForTest(swap())
            client.rebuild().getOrThrow()
            assertEquals("other",scalarText(db,"SELECT name FROM nodes WHERE id='P'"))
            assertEquals("C",scalarText(db,"SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_plan"))
        }
    }


    @Test fun writesBetweenPagesAndBeforeRefreshPreventDispatch() = runOwnedWebDatabaseTest {
        for (phase in listOf("page","refresh")) {
            val db = newDb(); val server = MockSyncServer(); val http = server.newHttpClient()
            val tables = listOf(SyncTable("users","id"),SyncTable("posts","id"))
            try {
                createUsersAndPostsTables(db)
                val client = newClient(db,http,syncTables=tables,maxSnapshotChunkRows=1)
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("INSERT INTO users VALUES('A','first'),('B','second')")
                client.pushPending().getOrThrow()
                if (phase == "page") {
                    db.execSQL("""CREATE TEMP TRIGGER late_snapshot_write AFTER UPDATE OF snapshot_staged_row_count
                        ON _sync_operation_state WHEN new.snapshot_staged_row_count=1 AND old.snapshot_staged_row_count=0
                        BEGIN UPDATE users SET name='later' WHERE id='A'; END""")
                } else {
                    server.beforeSnapshotChunkRequest = {
                        db.execSQL("UPDATE users SET name='later' WHERE id='A'")
                        server.snapshotSessionIdsForTest().forEach { server.expireSnapshotSessionForTest(it) }
                    }
                }
                assertTrue(client.rebuild().isFailure)
                assertEquals(1,server.snapshotSessionCreateRequestCount,phase)
                assertEquals(1,server.requestTrace.count { it == "GET /sync/snapshot-sessions/snapshot-1" },phase)
                assertEquals("later",scalarText(db,"SELECT name FROM users WHERE id='A'"))
                assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                client.closeFromCoordinatedOwner()
            } finally { http.close(); db.close() }
        }
    }


    @Test fun retriesRepeatSnapshotAdmission() = runOwnedWebDatabaseTest {
        for (phase in listOf("create", "page")) for (change in listOf("dirty", "main", "temp")) {
            val db = newDb(); val server = MockSyncServer(); val base = server.newHttpClient()
            var retryDecisions = 0
            val http = base.config { install(HttpRequestRetry) {
                maxRetries = 2
                retryOnExceptionIf { _, cause ->
                    (cause.message == "synthetic snapshot retry").also { if (it) retryDecisions++ }
                }
                constantDelay(1)
            } }
            try {
                createUsersAndPostsTables(db)
                val client = newClient(db, http, syncTables = listOf(SyncTable("users","id"),SyncTable("posts","id")))
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("INSERT INTO users VALUES('A','first')")
                client.pushPending().getOrThrow()
                val mutate: suspend () -> Unit = {
                    if (change == "dirty") db.execSQL("UPDATE users SET name='later' WHERE id='A'")
                    else db.execSQL("CREATE TABLE $change.changed_retry(id INTEGER)")
                    throw IllegalStateException("synthetic snapshot retry")
                }
                if (phase == "create") server.beforeSnapshotCreateRequest = mutate
                else server.beforeSnapshotChunkRequest = mutate
                assertTrue(client.rebuild().isFailure, "$phase/$change")
                assertEquals(1, retryDecisions, "the installed retry plugin must attempt another dispatch")
                assertEquals(1, server.requestTrace.count { it == "POST /sync/snapshot-sessions" }, "$phase/$change")
                assertEquals(if (phase == "page") 1 else 0,
                    server.requestTrace.count { it == "GET /sync/snapshot-sessions/snapshot-1" }, "$phase/$change")
                assertEquals(if (change == "dirty") "later" else "first", scalarText(db,"SELECT name FROM users WHERE id='A'"))
                assertEquals(if (change == "dirty") 1L else 0L, scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(1L, scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                client.closeFromCoordinatedOwner()
            } finally { http.close(); base.close(); db.close() }
        }
    }

    @Test fun oneRowFallbackRepeatsSnapshotAdmission() = runOwnedWebDatabaseTest {
        val db = newDb(); val server = MockSyncServer(); val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db,http,maxSnapshotChunkDecodedBytes = 1024)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users VALUES('A','first')")
            client.pushPending().getOrThrow()
            server.snapshotChunkTooSmallOnceForTest = true
            server.beforeSnapshotChunkRequest = { db.execSQL("UPDATE users SET name='later' WHERE id='A'") }
            assertTrue(client.rebuild().isFailure)
            assertFalse(server.snapshotChunkTooSmallOnceForTest, "the first GET must return the explicit one-row retry response")
            assertEquals(1,server.snapshotChunkRequests.size)
            assertEquals(1,server.requestTrace.count { it == "GET /sync/snapshot-sessions/snapshot-1" })
            assertEquals("later",scalarText(db,"SELECT name FROM users WHERE id='A'"))
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            client.closeFromCoordinatedOwner()
        } finally { http.close(); db.close() }
    }

    @Test fun ordinarySettlementPreservesIndependentReparenting() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects = { effects() }
            server.beforePushResponse = { db.execSQL("UPDATE nodes SET parent_id='Q' WHERE id='D'") }
            client.pushPending().getOrThrow()
            assertEquals("Q",scalarText(db,"SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals("grandchild",scalarText(db,"SELECT name FROM nodes WHERE id='D'"))
            assertEquals("Q",scalarText(db,"SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM nodes WHERE id='P'"))
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id')='D'"))
            assertEquals("Q",scalarText(db,"SELECT json_extract(payload,'$.parent_id.value') FROM _sync_dirty_rows"))
            assertEquals("C",scalarText(db,"SELECT json_extract(base_payload,'$.parent_id.value') FROM _sync_dirty_rows"))
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0,server.snapshotSessionCreateRequestCount)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
        }
    }

    @Test fun testActivityDT01() = runOwnedWebDatabaseTest { runActivity("DT01") }
    @Test fun testActivityDT02() = runOwnedWebDatabaseTest { runActivity("DT02") }
    @Test fun testActivityDT03() = runOwnedWebDatabaseTest { runActivity("DT03") }
    @Test fun testActivityDT04() = runOwnedWebDatabaseTest { runActivity("DT04") }
    @Test fun testActivityDT05() = runOwnedWebDatabaseTest { runActivity("DT05") }
    @Test fun testActivityDT06() = runOwnedWebDatabaseTest { runActivity("DT06") }

    @Test fun testActivityDT07() = runOwnedWebDatabaseTest {
        for(pending in listOf("dirty","prepared","unsafe_committed","ack_pending")) {
            val db=newDb();val server=MockSyncServer();val generation=ActivityRecoveryFixture.generation()
            server.generationOverride=generation;server.registeredTableSpecs=generation.tables.map { RegisteredTableSpec(it.schema,it.table,it.syncKeyColumns) }
            val http=server.newHttpClient()
            try {
                ActivityRecoveryFixture.create(db)
                val client=newClient(db,http,syncTables=ActivityRecoveryFixture.tables)
                client.openAndConnect("user-1").getOrThrow();ActivityRecoveryFixture.seed(db);client.pushPending().getOrThrow()
                db.execSQL("CREATE UNIQUE INDEX daily_log_notes_unique ON daily_log(notes)")
                db.execSQL("UPDATE daily_log SET notes='synthetic offline' WHERE id='L1'")
                when(pending) {
                    "dirty" -> client.pauseUploads()
                    "prepared" -> {server.cancelNextPush=true;try{client.pushPending().getOrThrow()}catch(_:CancellationException){}}
                    "unsafe_committed" -> {
                        server.normalizePushEffects={ rows->rows.map {if(it.table=="daily_log")it.copy(payload=JsonObject(it.payload!!.jsonObject.toMutableMap().also { p->p["notes"]=JsonPrimitive("server normalized") })) else it} }
                        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit={throw CancellationException("stored C")}))
                        try{client.pushPending().getOrThrow()}catch(_:CancellationException){}
                        client.setSnapshotApplyFaultInjectorForTest(null)
                        db.execSQL("UPDATE daily_log SET notes='server normalized' WHERE id='L2'")
                    }
                    "ack_pending" -> {server.beforeAck={throw CancellationException("ack cut")};try{client.pushPending().getOrThrow()}catch(_:CancellationException){}}
                }
                server.beforeResolve={throw CancellationException("retain F")};server.beforeAck={throw CancellationException("retain ack")}
                val names=generation.tables.map {it.table}+listOf("app_settings","_sync_upload_journal")
                val before=baselineQualificationVector(db,names);val snapshots=server.requestTrace.count {"snapshot-sessions" in it};val acks=server.ackRequests.size
                client.retryRecovery().getOrThrow()
                try{assertTrue(if(pending=="dirty")client.pullToStable().isFailure else client.sync().isFailure,pending)}catch(_:CancellationException){}
                assertEquals(snapshots,server.requestTrace.count {"snapshot-sessions" in it},pending)
                if(pending=="unsafe_committed")assertEquals(acks,server.ackRequests.size)
                assertEquals(before,baselineQualificationVector(db,names),pending)
            }finally{http.close();db.close()}
        }
    }

    private suspend fun runActivity(scenario: String) {
            val path = createTempSqliteNowTestDbPath("snapshot-activity-$scenario")
            var db = newFileBackedDb(path)
            val server = MockSyncServer()
            val generation = ActivityRecoveryFixture.generation()
            server.generationOverride = generation
            server.registeredTableSpecs = generation.tables.map { RegisteredTableSpec(it.schema,it.table,it.syncKeyColumns) }
            val http = server.newHttpClient()
            var client: DefaultOversqliteClient? = null
            try {
                ActivityRecoveryFixture.create(db)
                client = newClient(db,http,syncTables=ActivityRecoveryFixture.tables)
                client.openAndConnect("user-1").getOrThrow()
                ActivityRecoveryFixture.seed(db)
                client.pushPending().getOrThrow()
                db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
                val names = generation.tables.map { it.table } + "app_settings"
                val before = baselineQualificationVector(db,names).tables.filterKeys { it in names }
                val expected = before.toMutableMap()
                val effects = mutableListOf<BundleRow>()
                fun change(table: String, key: String, values: Map<String,Any?>) {
                    val descriptor = generation.tables.single { it.table == table }
                    val pk = descriptor.syncKeyColumns.single()
                    val old = Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("$table/$pk=$key")).jsonObject
                    val wire = old.toMutableMap()
                    for ((name,value) in values) wire[name] = when(value) { null -> JsonNull; is Boolean -> JsonPrimitive(value); else -> JsonPrimitive(value.toString()) }
                    effects += BundleRow("main",table,mapOf(pk to key),"UPDATE",2,JsonObject(wire))
                    expected[table] = expected.getValue(table).map { row ->
                        if (row.text(pk) != key) row else JsonObject(row.toMutableMap().also { fields ->
                            for ((name,value) in values) fields[name] = buildJsonObject {
                                put("storage",when(value) { null -> "null"; is Int, is Long, is Boolean -> "integer"; else -> "text" })
                                put("value",when(value) { null -> JsonNull; is Boolean -> JsonPrimitive(if(value) "1" else "0"); else -> JsonPrimitive(value.toString()) })
                            }
                        })
                    }
                }
                when(scenario) {
                    "DT01" -> {
                        assertEquals(14,generation.tables.size)
                        assertEquals(1L,scalarLong(db,"SELECT delete_when_expired FROM activity WHERE id='A'"))
                        assertEquals("real",scalarText(db,"SELECT typeof(numeric_value) FROM value_registry WHERE key='body-weight'"))
                    }
                    "DT02" -> change("profile","profile-name",mapOf("body_weight" to 74,"unlock_codes" to "[\"synthetic\"]"))
                    "DT03" -> {
                        change("activity","A",mapOf("sched_write_ref_start_at" to "date-b","sched_write_ref_repeat_after_days" to "repeat-b","sched_write_ref_days_duration" to "duration-b"))
                        change("activity","B",mapOf("sched_write_ref_start_at" to "date-a","sched_write_ref_repeat_after_days" to "repeat-a","sched_write_ref_days_duration" to "duration-a"))
                        change("program_item_input_goal","G",mapOf("input_key" to "reps"))
                        change("program_item_input_goal","H",mapOf("input_key" to "weight"))
                        change("program_item_input_goal_revision","R",mapOf("revision" to 2))
                        change("program_item_input_goal_revision","S",mapOf("revision" to 1))
                    }
                    "DT04" -> for ((table,key) in listOf("activity" to "B","program_item" to "Q","program_item_state" to "Q")) {
                        val pk = generation.tables.single { it.table == table }.syncKeyColumns.single()
                        effects += BundleRow("main",table,mapOf(pk to key),"DELETE",2,null)
                        expected[table] = expected.getValue(table).filter { it.text(pk) != key }
                    }
                    "DT05" -> {
                        change("daily_log","L1",mapOf("parent_daily_log_id" to "L2"))
                        change("daily_log","L2",mapOf("parent_daily_log_id" to null,"notes" to "new parent"))
                    }
                    "DT06" -> change("program_item_input_goal_revision","R",mapOf("effective_day" to "2026-1-01"))
                }
                if (effects.isNotEmpty()) server.addRemoteBundleForTest(effects)
                val localRowId = scalarLong(db,"SELECT rowid FROM app_settings WHERE id=1")
                val result = client.rebuild()
                if (scenario == "DT06") {
                    assertTrue(result.isFailure)
                    val failure = assertNotNull(result.exceptionOrNull())
                    println("C04_ACTIVITY_CHECK_FAILURE primary=$failure suppressed=${failure.suppressedExceptions.map { it.toString() }}")
                    client.closeFromCoordinatedOwner(); client = null; db.close()
                    if (!supportsDurableTestDatabaseRestart()) {
                        assertTrue(failure.message.orEmpty().contains("CHECK constraint failed: length(effective_day) = 10"),
                            "Node must reach the actual invalid final-state CHECK rejection")
                        println("NOT-APPLICABLE DT06 SQL-fatal closed-copy rollback: transient Node database; full durable14table rollback remains required on durable runtimes")
                        return
                    }
                    db = newFileBackedDb(path)
                }
                val actual = baselineQualificationVector(db,names).tables.filterKeys { it in names }
                if (scenario == "DT06") {
                    assertTrue(result.isFailure)
                    assertEquals(before,actual,"CHECK rejection rolls back complete business vector")
                    assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                } else {
                    result.getOrThrow()
                    assertEquals(expected,actual,"full values in all14managed and local-only tables")
                    assertEquals(if(effects.isEmpty())1L else 2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                }
                assertEquals(localRowId,scalarLong(db,"SELECT rowid FROM app_settings WHERE id=1"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                println("C04_ACTIVITY $scenario tables=14 full_values=true local_only_preserved=true")
                if (scenario == "DT03") assertActivityDuplicateRefusal(server, db, checkNotNull(client), names)
            } finally { client?.closeFromCoordinatedOwner(); http.close(); db.close(); deleteTempSqliteNowTestDbArtifacts(path) }
    }

    private suspend fun assertActivityDuplicateRefusal(
        server: MockSyncServer,
        db: SafeSQLiteConnection,
        client: DefaultOversqliteClient,
        businessNames: List<String>,
    ) {
        val duplicate = listOf("A", "B").map { key ->
            val original = Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("activity/id=$key")).jsonObject
            BundleRow("main", "activity", mapOf("id" to key), "UPDATE", 3,
                JsonObject(original + ("sched_write_ref_start_at" to JsonPrimitive("same"))))
        }
        server.addRemoteBundleForTest(duplicate)
        db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
        suspend fun protectedVector() = baselineQualificationVector(db, businessNames + "_sync_upload_journal")
            .tables.filterKeys { it != "_sync_operation_state" }
        val before = protectedVector()
        val localRowId = scalarLong(db, "SELECT rowid FROM app_settings WHERE id=1")
        val indexes = scalarText(db, "SELECT json_group_array(json_array(name,sql)) FROM (SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='activity' ORDER BY name)")
        val acks = server.ackRequests.size
        val pushes = server.requestTrace.count { it == "POST /sync/push" }
        val failure = assertIs<FinalStateApplyBlockedException>(client.rebuild().exceptionOrNull())
        assertEquals("conflicting_desired_state", failure.reason)
        assertEquals("activity", failure.table)
        assertEquals(before, protectedVector(), "duplicate target preserves all14 tables, local values and complete intent/source/checkpoint metadata")
        assertEquals(localRowId, scalarLong(db, "SELECT rowid FROM app_settings WHERE id=1"))
        assertEquals(indexes, scalarText(db, "SELECT json_group_array(json_array(name,sql)) FROM (SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='activity' ORDER BY name)"))
        assertEquals(acks, server.ackRequests.size)
        assertEquals(pushes, server.requestTrace.count { it == "POST /sync/push" })
        assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        println("C06_ACTIVITY_DT03_DUPLICATE_REFUSAL reason=${failure.reason} tables=14 protected_vector=true constraints=true source_checkpoint=true ack=0 push=0")
    }
}
