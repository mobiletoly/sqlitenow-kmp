package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.async.step
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

// Independent small-fixture oracles protect both full and selected observations.
internal class DatabaseStateVectorTest : PlatformCrossTargetTestSupport() {
    private val worker get() = webRuntimeKind() in setOf("js-node", "js-browser", "wasm-browser")
    private val projection = "json_object('v',json_object('storage',typeof(\"v\"),'value',CASE WHEN typeof(\"v\")='blob' THEN lower(hex(\"v\")) WHEN \"v\" IS NULL THEN NULL ELSE CAST(\"v\" AS TEXT) END))"

    private suspend fun fixture(name: String, body: suspend (SafeSQLiteConnection, DefaultOversqliteClient) -> Unit) {
        check(platformSuiteEnabled() || realServerSuiteEnabled()) { "F1 prerequisite requires platform or realserver opt-in" }
        println("F1_ENTER ${webRuntimeKind()} $name")
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            db.execSQL("CREATE TABLE probe(v)")
            val client = newClient(db, http)
            client.attach("user-1").getOrThrow()
            val connection = db.connection()
            connection.withExclusiveAccess {
                connection.prepare("PRAGMA encoding").use { statement ->
                    if (statement.step()) assertEquals("UTF-8", statement.getText(0))
                    else {
                        assertTrue(worker, "non-web BLOB selection requires PRAGMA encoding evidence")
                        assertEquals(1L, scalarLong(connection, "SELECT sqlite_compileoption_used('OMIT_UTF16')"),
                            "absent encoding pragma requires explicit UTF16-omitted worker build evidence")
                        println("F1_ENCODING worker OMIT_UTF16=1; PRAGMA encoding unavailable")
                    }
                }
            }
            body(connection, client)
        } finally {
            withContext(NonCancellable) {
                try { db.close() } finally { http.close(); cleanupOwnedWorkerStorage() }
            }
        }
        println("F1_PASS ${webRuntimeKind()} $name cleanup=complete encoding=UTF-8")
    }

    private suspend fun insert(db: SafeSQLiteConnection, value: Any?) = db.withExclusiveAccess {
        db.prepare("INSERT INTO probe(v) VALUES(?)").use { s ->
            when (value) {
                null -> s.bindNull(1)
                is String -> s.bindText(1, value)
                is Long -> s.bindLong(1, value)
                is Double -> s.bindDouble(1, value)
                is ByteArray -> s.bindBlob(1, value)
                else -> error("unsupported fixture value")
            }
            assertFalse(s.step())
        }
    }

    private suspend fun projected(db: SafeSQLiteConnection, blob: Boolean): List<ByteArray> = db.withExclusiveAccess {
        val result = if (blob) "CAST($projection AS BLOB)" else projection
        db.prepare("SELECT $result FROM probe ORDER BY v").use { s ->
            buildList { while (s.step()) add(if (blob) s.getBlob(0) else s.getText(0).encodeToByteArray()) }
        }
    }

    @Test fun typedBytesAndDigests() = runTest(timeout = 180.seconds) {
        fixture("typedBytesAndDigests") { db, _ ->
            val values = listOf(null, "", 1L, 1.0, "1", "é", "e\u0301", "😀", "a\u0000b",
                "quote\" slash\\ newline\n tab\t", "x".repeat(4096), byteArrayOf(), byteArrayOf(0, -1, 1), "duplicate", "duplicate")
            for (value in values) insert(db, value)
            val text = projected(db, false)
            val blob = projected(db, true)
            assertEquals(values.size, text.size)
            assertEquals(text.size, blob.size)
            text.zip(blob).forEach { (a,b) -> assertContentEquals(a,b) }
            assertEquals(text.map(::sha256Hex), blob.map(::sha256Hex))
            assertEquals(2, text.map(::sha256Hex).count { it == sha256Hex(text.first { row -> row.decodeToString().contains("duplicate") }) })
            for (storage in listOf("null", "integer", "real", "text", "blob")) {
                assertTrue(text.any { it.decodeToString().contains("\"storage\":\"$storage\"") })
            }
        }
    }

    @Test fun completeVectorsOptionsAndFailureCleanup() = runTest(timeout = 180.seconds) {
        fixture("completeVectorsOptionsAndFailureCleanup") { db, _ ->
            suspend fun compare(): Map<String,List<String>> {
                for (stages in listOf(false,true)) for (charges in listOf(false,true)) {
                    for (predicate in listOf(emptyMap(), mapOf("probe" to "v IS NOT NULL"))) {
                        if (!worker) assertEquals(textVectorOracle(db,listOf("users","posts","probe"),predicate,charges,stages),
                            blobVectorProbe(db,listOf("users","posts","probe"),predicate,charges,stages))
                        assertEquals(textVectorOracle(db,listOf("users","posts","probe"),predicate,charges,stages),
                            databaseStateVector(db,listOf("users","posts","probe"),predicate,charges,stages))
                    }
                }
                return databaseStateVector(db,listOf("users","posts","probe"))
            }
            val empty = compare()
            assertEquals(emptyList(),empty.getValue("probe"))
            insert(db,"z"); insert(db,null); insert(db,"a"); insert(db,"a")
            val populated = compare()
            assertNotEquals(empty.getValue("probe"),populated.getValue("probe"))
            assertEquals(4,populated.getValue("probe").size)
            insertUser(db,"local-user","before")
            val before = compare()
            updateUserName(db,"local-user","after")
            val after = compare()
            assertNotEquals(before.getValue("users"),after.getValue("users"))
            assertNotEquals(before.getValue("_sync_dirty_rows"),empty.getValue("_sync_dirty_rows"))
            assertFails { textVectorOracle(db,listOf("missing_fixture_table")) }
            assertFails { databaseStateVector(db,listOf("missing_fixture_table")) }
            assertFails { projectedInvalid(db) }
            assertEquals(after,compare(),"read failure leaves owner usable and values unchanged")
        }
    }

    @Test fun retainedStateAndOptionSensitivity() = runTest(timeout = 180.seconds) {
        fixture("retainedStateAndOptionSensitivity") { db, client ->
            val business = listOf("users", "posts")
            val empty = databaseStateVector(db,business)
            insertUser(db,"authoritative-user","baseline")
            client.pushPending().getOrThrow()
            val committed = databaseStateVector(db,business)
            assertNotEquals(empty.getValue("_sync_row_state"),committed.getValue("_sync_row_state"))
            insertUser(db,"pending-user","pending")
            val before = databaseStateVector(db,business)
            val rollback = VectorProbeRollback(1)
            val failure = assertFails {
                db.transaction {
                    // Only inspect representations here; never run replay on these temporary probes.
                    db.execSQL("""INSERT INTO _sync_outbox_rows(source_bundle_id,row_ordinal,schema_name,table_name,key_json,
                        wire_key_json,op,base_row_version,base_kind,base_payload,base_provenance,local_payload,wire_payload)
                        SELECT 1,1,schema_name,table_name,key_json,key_json,op,base_row_version,base_kind,base_payload,
                        base_provenance,payload,payload FROM _sync_dirty_rows WHERE table_name='users'""")
                    db.execSQL("""INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload,charge)
                        SELECT 'f1-representation',1,schema_name,table_name,key_json,row_version,server_payload,0
                        FROM _sync_row_state WHERE table_name='users' AND server_kind='live'""")
                    val populated = databaseStateVector(db,business)
                    for (table in listOf("_sync_outbox_rows","_sync_snapshot_stage")) {
                        assertEquals(1,populated.getValue(table).size)
                        assertNotEquals(before.getValue(table),populated.getValue(table))
                    }
                    assertFalse(databaseStateVector(db,business,includeDurableStages=false).containsKey("_sync_snapshot_stage"))
                    val ignoredBefore = databaseStateVector(db,business,ignoreDerivedCharges=true)
                    val subsetIgnoredBefore = selectedDatabaseStateVector(db,business,listOf("_sync_operation_state"),ignoreDerivedCharges=true)
                    db.execSQL("UPDATE _sync_operation_state SET reserved_bytes=reserved_bytes+1 WHERE singleton_key=1")
                    assertNotEquals(populated.getValue("_sync_operation_state"),databaseStateVector(db,business).getValue("_sync_operation_state"))
                    assertEquals(ignoredBefore,databaseStateVector(db,business,ignoreDerivedCharges=true))
                    assertEquals(subsetIgnoredBefore,selectedDatabaseStateVector(db,business,listOf("_sync_operation_state"),ignoreDerivedCharges=true))
                    assertNotEquals(populated.getValue("_sync_operation_state"),selectedDatabaseStateVector(db,business,listOf("_sync_operation_state")).getValue("_sync_operation_state"))
                    for (stages in listOf(false,true)) for (charges in listOf(false,true)) {
                        val expected = textVectorOracle(db,business,ignoreDerivedCharges=charges,includeDurableStages=stages)
                        assertEquals(expected,databaseStateVector(db,business,ignoreDerivedCharges=charges,includeDurableStages=stages))
                        assertEquals(expected,selectedDatabaseStateVector(db,business,expected.keys.toList(),ignoreDerivedCharges=charges,includeDurableStages=stages),
                            "subset options retain populated outbox/stage and exclude only declared derived charges")
                        if (!worker) assertEquals(expected,blobVectorProbe(db,business,ignoreDerivedCharges=charges,includeDurableStages=stages))
                    }
                    throw rollback
                }
            }
            assertSame(rollback,failure)
            assertEquals(before,databaseStateVector(db,business),"all probe state rolled back")
        }
    }

    @Test fun selectedVectorsMatchOracleAndRejectInvalidSelections() = runTest(timeout = 180.seconds) {
        fixture("selectedVectorsMatchOracleAndRejectInvalidSelections") { db, _ ->
            val business = listOf("users", "posts", "probe")
            for (value in listOf(null, "", 1L, 1.0, "é", byteArrayOf(0, -1), "duplicate", "duplicate")) insert(db, value)
            insertUser(db, "pending", "pending")
            for (stages in listOf(false, true)) for (charges in listOf(false, true)) {
                for (predicate in listOf(emptyMap(), mapOf("probe" to "v IS NOT NULL"))) {
                    val expected = textVectorOracle(db, business, predicate, charges, stages)
                    val selections = listOf(listOf("probe"), listOf("posts", "probe", "_sync_dirty_rows"),
                        listOf("_sync_operation_state"), expected.keys.toList().reversed())
                    for (selection in selections) {
                        val counts = DatabaseVectorScanDiagnostics()
                        val sql = mutableListOf<String>()
                        val actual = selectedDatabaseStateVector(db, business, selection, predicate, charges, stages, counts) {
                            sql += it
                            db.prepare(it)
                        }
                        assertEquals(selection, actual.keys.toList())
                        assertEquals(expected.filterKeys { it in selection }, actual)
                        assertEquals(selection.toSet(), counts.tables.keys)
                        assertEquals(selection.size * 2, sql.size, "one schema and one row query per selected owner")
                        selection.forEach { table ->
                            val scan = counts.tables.getValue(table)
                            assertEquals(1L, scan.metadataQueries)
                            assertEquals(1L, scan.rowQueries)
                            assertEquals(expected.getValue(table).size.toLong(), scan.rowsHashed)
                        }
                        assertEquals(emptyList(), actual["posts"] ?: emptyList(), "selected empty owners stay present")
                    }
                }
            }
            for (selection in listOf(emptyList(), listOf("probe", "probe"), listOf("unknown"), listOf(" probe"), listOf("probe "))) {
                assertFailsWith<IllegalArgumentException> {
                    selectedDatabaseStateVector(db, business, selection, prepare = { error("invalid selection reached SQL") })
                }
            }
            assertFailsWith<IllegalArgumentException> {
                selectedDatabaseStateVector(db, business, listOf("_sync_snapshot_stage"), includeDurableStages = false)
            }
            assertFails { selectedDatabaseStateVector(db, business + "missing_owner", listOf("missing_owner")) }
            // The omitted-owner failure remains at the retained complete boundary.
            val partial = selectedDatabaseStateVector(db, business + "missing_owner", listOf("probe"))
            assertEquals(textVectorOracle(db, business).getValue("probe"), partial.getValue("probe"))
            assertFails { databaseStateVector(db, business + "missing_owner") }
            val omittedFailure = mapOf("posts" to "missing_column > 0")
            selectedDatabaseStateVector(db, business, listOf("probe"), omittedFailure)
            assertFails { databaseStateVector(db, business, omittedFailure) }

            val counts = DatabaseVectorScanDiagnostics()
            val subset = selectedDatabaseStateVector(db, business, listOf("probe", "posts"), diagnostics = counts)
            val expectedBytes = projected(db, false).sumOf { it.size }.toLong()
            assertEquals(8L, counts.tables.getValue("probe").rowsHashed)
            assertEquals(expectedBytes, counts.tables.getValue("probe").bytesHashed)
            assertEquals(emptyList(), subset.getValue("posts"))
            println("SUBSET_SCAN ${webRuntimeKind()} selected=${counts.tables} fullRoster=${databaseStateVector(db, business).size} discardedQueries=0")
        }
    }

    @Test fun selectedVectorsDetectEveryTypedMutation() = runTest(timeout = 180.seconds) {
        fixture("selectedVectorsDetectEveryTypedMutation") { db, _ ->
            suspend fun observed() = selectedDatabaseStateVector(db, listOf("probe"), listOf("probe")).getValue("probe")
            fun expected(storage: String, value: String?): String = sha256Hex(buildJsonObject {
                put("v", buildJsonObject { put("storage", storage); put("value", value?.let(::JsonPrimitive) ?: JsonNull) })
            }.toString().encodeToByteArray())
            val samples = listOf(Triple(null, "null", null), Triple("", "text", ""), Triple(1L, "integer", "1"),
                Triple(1.0, "real", "1.0"), Triple("1", "text", "1"), Triple(byteArrayOf(), "blob", ""),
                Triple(byteArrayOf(0, -1, 1), "blob", "00ff01"), Triple("é", "text", "é"),
                Triple("e\u0301", "text", "e\u0301"), Triple("😀", "text", "😀"), Triple("a\u0000b", "text", "a\u0000b"))
            val seen = mutableSetOf<String>()
            for ((value, storage, text) in samples) {
                db.execSQL("DELETE FROM probe")
                insert(db, value)
                val actual = observed()
                assertEquals(listOf(expected(storage, text)), actual, "independent typed row expectation")
                assertTrue(seen.add(actual.single()), "storage/value change must be detected")
                assertEquals(textVectorOracle(db, listOf("probe")).getValue("probe"), actual)
            }
            db.execSQL("DELETE FROM probe")
            insert(db, "z"); insert(db, "a"); insert(db, "a")
            assertEquals(listOf(expected("text", "a"), expected("text", "a"), expected("text", "z")), observed(), "ordering and multiplicity")
            db.execSQL("DELETE FROM probe WHERE rowid=(SELECT min(rowid) FROM probe WHERE v='a')")
            assertEquals(listOf(expected("text", "a"), expected("text", "z")), observed(), "deletion is observable")
            db.execSQL("DELETE FROM probe")
            for (i in 0..63) insert(db, i.toString().padStart(3, '0'))
            val before = observed()
            db.execSQL("UPDATE probe SET v='changed beyond prefix' WHERE v='063'")
            val after = observed()
            assertEquals(64, after.size)
            assertNotEquals(before, after, "last row cannot be hidden by sampling or cached observation")
            assertEquals(expected("text", "changed beyond prefix"), after.last())
            assertEquals(before.dropLast(1), after.dropLast(1))
            assertEquals(textVectorOracle(db, listOf("probe")).getValue("probe"), after)
        }
    }

    @Test fun selectedVectorFailuresCloseAndRemainFresh() = runTest(timeout = 180.seconds) {
        fixture("selectedVectorFailuresCloseAndRemainFresh") { db, _ ->
            insert(db, "before")
            val business = listOf("probe")
            val selected = listOf("probe")
            val before = selectedDatabaseStateVector(db, business, selected)
            // Inject at both resource boundaries. Delegate stepping unchanged on
            // each target, including the worker's suspend statement interface.
            for (schema in listOf(true, false)) {
                var closes = 0
                val fault = VectorProbeRollback(2)
                val thrown = assertFails {
                    selectedDatabaseStateVector(db, business, selected, prepare = { sql ->
                        val raw = db.prepare(sql)
                        if (sql.startsWith("PRAGMA") != schema) raw else object : SQLiteStatement by raw {
                            override fun close() { closes++; raw.close(); throw fault }
                        }
                    })
                }
                assertSame(fault, thrown)
                assertEquals(1, closes)
                assertEquals(before, selectedDatabaseStateVector(db, business, selected))
            }
            var closes = 0
            val readFault = VectorProbeRollback(3)
            val closeFault = VectorProbeRollback(4)
            val thrown = assertFails {
                selectedDatabaseStateVector(db, business, selected, prepare = { sql ->
                    val raw = db.prepare(sql)
                    if (!sql.startsWith("SELECT")) raw else object : SQLiteStatement by raw {
                        override fun getText(index: Int): String = throw readFault
                        override fun getBlob(index: Int): ByteArray = throw readFault
                        override fun close() { closes++; raw.close(); throw closeFault }
                    }
                })
            }
            assertSame(readFault, thrown)
            assertTrue(thrown.suppressedExceptions.any { it === closeFault })
            assertEquals(1, closes)
            val cancellation = kotlinx.coroutines.CancellationException("subset cancelled after schema scan")
            assertFailsWith<kotlinx.coroutines.CancellationException> {
                selectedDatabaseStateVector(db, business, selected, prepare = { sql ->
                    if (sql.startsWith("SELECT")) throw cancellation else db.prepare(sql)
                })
            }
            var cancelledCloses = 0
            assertFailsWith<kotlinx.coroutines.CancellationException> {
                selectedDatabaseStateVector(db, business, selected, prepare = { sql ->
                    val raw = db.prepare(sql)
                    if (!sql.startsWith("SELECT")) raw else object : SQLiteStatement by raw {
                        override fun getText(index: Int): String = throw cancellation
                        override fun getBlob(index: Int): ByteArray = throw cancellation
                        override fun close() { cancelledCloses++; raw.close() }
                    }
                })
            }
            assertEquals(1, cancelledCloses, "cancellation while reading a row closes the active statement")
            assertFails { selectedDatabaseStateVector(db, business, selected, mapOf("probe" to "missing_column > 0")) }
            assertEquals(before, selectedDatabaseStateVector(db, business, selected), "failure/cancellation releases access")
            db.execSQL("UPDATE probe SET v='after'")
            val after = selectedDatabaseStateVector(db, business, selected)
            assertNotEquals(before, after, "fresh observation after failed scans and a write")
            assertEquals(textVectorOracle(db, business).getValue("probe"), after.getValue("probe"))
        }
    }

    @Test fun selectedReplayObservationsAvoidDiscardedScans() = runTest(timeout = 180.seconds) {
        fixture("selectedReplayObservationsAvoidDiscardedScans") { db, _ ->
            // Small structural witness only, separate from ordinary timing. Use
            // the exact replay selections/predicates against populated owners.
            db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY, name TEXT, parent_id TEXT)")
            for (i in 0..7) {
                val id = "00000000-0000-4000-8000-${i.toString().padStart(12, '0')}"
                db.execSQL("INSERT INTO categories VALUES('$id','payload-$i',NULL)")
                insertUser(db, id, "payload-$i")
            }
            val predicate = "json_extract(key_json,'$.id') LIKE '00000000-0000-4000-8000-%'"
            val predicates = mapOf("categories" to "id LIKE '00000000-0000-4000-8000-%'",
                "_sync_row_state" to predicate, "_sync_dirty_rows" to predicate,
                "_sync_relationships" to "json_extract(child_key,'$.id') LIKE '00000000-0000-4000-8000-%'")
            val settled = listOf("_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts", "_sync_managed_tables",
                "_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges",
                "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges")
            for ((site, selection) in listOf("initial-unrelated" to predicates.keys.toList(),
                "post-unrelated" to predicates.keys.toList(), "settled" to settled)) {
                val where = if (site == "settled") emptyMap() else predicates
                val full = textVectorOracle(db, listOf("categories"), where)
                val scans = DatabaseVectorScanDiagnostics()
                val queries = mutableListOf<String>()
                val subset = selectedDatabaseStateVector(db, listOf("categories"), selection, where, diagnostics = scans, prepare = { sql ->
                    queries += sql
                    db.prepare(sql)
                })
                assertEquals(full.filterKeys { it in selection }, subset)
                assertEquals(selection.toSet(), scans.tables.keys)
                assertEquals(selection.flatMap { table -> listOf("PRAGMA table_info(\"$table\")", table) },
                    queries.map { sql -> if (sql.startsWith("PRAGMA")) sql else selection.single { " FROM \"$it\"" in sql } })
                assertEquals(selection.sumOf { full.getValue(it).size }.toLong(), scans.tables.values.sumOf { it.rowsHashed })
                assertEquals(22, full.size)
                assertEquals(if (site == "settled") 11 else 4, subset.size)
                if (site == "settled") {
                    assertEquals(8, full.getValue("categories").size)
                    assertEquals(8, full.getValue("_sync_row_state").size)
                    assertEquals(8, full.getValue("_sync_dirty_rows").size)
                    assertTrue(listOf("categories", "_sync_row_state", "_sync_dirty_rows").none { it in scans.tables })
                }
                println("REPLAY_SUBSET_SCAN ${webRuntimeKind()} site=$site oldTables=${full.size} newTables=${subset.size} oldRows=${full.values.sumOf { it.size }} newRows=${scans.tables.values.sumOf { it.rowsHashed }} newBytes=${scans.tables.values.sumOf { it.bytesHashed }} discardedQueries=0")
            }
        }
    }

    @Test fun selectedVectorsObservePhysicalReopen() = runTest(timeout = 180.seconds) {
        if (webRuntimeKind() == "js-node") {
            println("F1_NOT_APPLICABLE js-node selectedVectorsObservePhysicalReopen transient_storage")
            return@runTest
        }
        check(platformSuiteEnabled() || realServerSuiteEnabled()) { "subset reopen requires suite opt-in" }
        println("F1_ENTER ${webRuntimeKind()} selectedVectorsObservePhysicalReopen")
        val db = newDb()
        val http = MockSyncServer().newHttpClient()
        var inspector: SafeSQLiteConnection? = null
        try {
            createUsersAndPostsTables(db)
            db.execSQL("CREATE TABLE probe(v)")
            newClient(db, http).attach("user-1").getOrThrow()
            insert(db.connection(), "before reopen")
            val before = selectedDatabaseStateVector(db.connection(), listOf("probe"), listOf("probe"))
            val original = db.connection()
            db.close()
            val reopened = db.reopenForInspection().also { inspector = it }
            assertNotSame(original, reopened)
            assertEquals(before, selectedDatabaseStateVector(reopened, listOf("probe"), listOf("probe")))
            insert(reopened, "after reopen")
            val after = selectedDatabaseStateVector(reopened, listOf("probe"), listOf("probe"))
            assertNotEquals(before, after)
            reopened.close(); inspector = null
            val next = db.reopenForInspection().also { inspector = it }
            assertNotSame(reopened, next)
            assertEquals(after, selectedDatabaseStateVector(next, listOf("probe"), listOf("probe")))
            assertEquals(textVectorOracle(next, listOf("probe")).getValue("probe"), after.getValue("probe"))
            assertEquals(2, after.getValue("probe").size)
        } finally {
            withContext(NonCancellable) {
                try { inspector?.close() } finally {
                    try { db.close() } finally { http.close(); cleanupOwnedWorkerStorage() }
                }
            }
        }
        println("F1_PASS ${webRuntimeKind()} selectedVectorsObservePhysicalReopen cleanup=complete")
    }

    // A field prevents coroutine stack-trace recovery from copying this identity witness.
    private class VectorProbeRollback(val marker: Int) : RuntimeException("test-owned representation probe rollback")

    private suspend fun projectedInvalid(db: SafeSQLiteConnection) = db.withExclusiveAccess {
        db.prepare("SELECT json('invalid json')").use { it.step() }
    }

    // Mirror the published worker row envelope, including JSON string escaping.
    private fun encodedBytes(bytes: ByteArray, blob: Boolean): Int {
        val cell = if (blob) buildJsonObject {
            put("type","blob"); put("blob",JsonArray(bytes.map { JsonPrimitive(it.toInt() and 255) }))
        } else buildJsonObject { put("type","text"); put("text",bytes.decodeToString()) }
        return JsonArray(listOf(cell)).toString().encodeToByteArray().size
    }

    @Test fun workerEncodedRowBoundaries() = runTest(timeout = 180.seconds) {
        if (!worker) {
            println("F1_NOT_APPLICABLE ${webRuntimeKind()} workerEncodedRowBoundaries non_worker")
            return@runTest
        }
        fixture("workerEncodedRowBoundaries") { db, _ ->
            val cap = 1_048_576
            suspend fun checkRow(value: String, blob: Boolean, size: Int? = null) {
                db.execSQL("DELETE FROM probe")
                insert(db,value)
                val expected = buildJsonObject {
                    put("v",buildJsonObject { put("storage","text"); put("value",value) })
                }.toString().encodeToByteArray()
                val encoded = encodedBytes(expected,blob)
                if (size != null) assertEquals(size,encoded)
                if (encoded <= cap) assertContentEquals(expected,projected(db,blob).single())
                else {
                    val failure = assertFails { projected(db,blob) }
                    val chain = generateSequence(failure) { it.cause }.joinToString { it.message.orEmpty() }
                    assertTrue(chain.contains("exceeds hard cap"),chain)
                }
                if (encodedBytes(expected,false) <= cap) {
                    assertContentEquals(expected,projected(db,false).single())
                    assertEquals(textVectorOracle(db,listOf("probe")),databaseStateVector(db,listOf("probe")))
                } else {
                    assertFails { textVectorOracle(db,listOf("probe")) }
                    assertFails { databaseStateVector(db,listOf("probe")) }
                }
                assertEquals(1L,scalarLong(db,"SELECT count(*) FROM probe"),"expected rejection cleanup")
                println("F1_ROW ${webRuntimeKind()} blob=$blob encoded=$encoded accepted=${encoded<=cap}")
            }
            checkRow("x".repeat(300_000),true)
            val empty = "{\"v\":{\"storage\":\"text\",\"value\":\"\"}}".encodeToByteArray()
            for (size in listOf(cap-1,cap,cap+1)) {
                checkRow("x".repeat(size-encodedBytes(empty,false)),false,size)
                val remaining = size-encodedBytes(empty,true)
                // x contributes four JSON-array bytes; space contributes three.
                val spaces = (0..3).first { (remaining-3*it)%4==0 }
                checkRow("x".repeat((remaining-3*spaces)/4)+" ".repeat(spaces),true,size)
            }
            checkRow("é😀\"\\\n".repeat(100),true)
        }
    }
}

// Immutable TEXT oracle copied from the reviewed helper before F1.
private suspend fun textVectorOracle(
    db: SafeSQLiteConnection,
    business: List<String>,
    predicates: Map<String, String> = emptyMap(),
    ignoreDerivedCharges: Boolean = false,
    includeDurableStages: Boolean = true,
): Map<String, List<String>> {
    val primary = business + listOf("_sync_row_state", "_sync_dirty_rows", "_sync_relationships",
        "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state",
        "_sync_attachment_state", "_sync_operation_state", "_sync_conflict_state",
        "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts")
    val durableStages = listOf("_sync_snapshot_stage", "_sync_apply_state", "_sync_apply_plan", "_sync_apply_rows",
        "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges",
        "_sync_managed_tables")
    val tables = primary + if (includeDurableStages) durableStages else emptyList()
    return tables.associateWith { table ->
        val columns = db.withExclusiveAccess { db.prepare("PRAGMA table_info(\"$table\")").use { statement ->
            buildList { while (statement.step()) add(statement.getText(1)) }
        }}
        assertTrue(columns.isNotEmpty(), "complete vector requires $table")
        val derived = setOf("workspace_context", "reserved_entries", "reserved_bytes", "snapshot_stage_entries",
            "snapshot_stage_bytes", "snapshot_cleanup_ordinal", "push_stage_entries", "push_stage_bytes", "push_cleanup_ordinal")
        val compared = if (ignoreDerivedCharges && table == "_sync_operation_state") columns.filter { it !in derived } else columns
        val projection = compared.joinToString(",") { column ->
            "'$column',json_object('storage',typeof(\"$column\"),'value',CASE WHEN typeof(\"$column\")='blob' THEN lower(hex(\"$column\")) WHEN \"$column\" IS NULL THEN NULL ELSE CAST(\"$column\" AS TEXT) END)"
        }
        val where = predicates[table]?.let { " WHERE $it" }.orEmpty()
        db.withExclusiveAccess { db.prepare("SELECT json_object($projection) FROM \"$table\"$where ORDER BY ${columns.joinToString(",") { "\"$it\"" }}").use { statement ->
            buildList { while (statement.step()) add(sha256Hex(statement.getText(0).encodeToByteArray())) }
        }}
    }
}

// Test-owned proposed query probe; never called by production fixture consumers.
private suspend fun blobVectorProbe(
    db: SafeSQLiteConnection,
    business: List<String>,
    predicates: Map<String, String> = emptyMap(),
    ignoreDerivedCharges: Boolean = false,
    includeDurableStages: Boolean = true,
): Map<String, List<String>> {
    val primary = business + listOf("_sync_row_state", "_sync_dirty_rows", "_sync_relationships",
        "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state",
        "_sync_attachment_state", "_sync_operation_state", "_sync_conflict_state",
        "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts")
    val durableStages = listOf("_sync_snapshot_stage", "_sync_apply_state", "_sync_apply_plan", "_sync_apply_rows",
        "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges",
        "_sync_managed_tables")
    val tables = primary + if (includeDurableStages) durableStages else emptyList()
    return tables.associateWith { table ->
        val columns = db.withExclusiveAccess { db.prepare("PRAGMA table_info(\"$table\")").use { statement ->
            buildList { while (statement.step()) add(statement.getText(1)) }
        }}
        assertTrue(columns.isNotEmpty(), "complete vector requires $table")
        val derived = setOf("workspace_context", "reserved_entries", "reserved_bytes", "snapshot_stage_entries",
            "snapshot_stage_bytes", "snapshot_cleanup_ordinal", "push_stage_entries", "push_stage_bytes", "push_cleanup_ordinal")
        val compared = if (ignoreDerivedCharges && table == "_sync_operation_state") columns.filter { it !in derived } else columns
        val projection = compared.joinToString(",") { column ->
            "'$column',json_object('storage',typeof(\"$column\"),'value',CASE WHEN typeof(\"$column\")='blob' THEN lower(hex(\"$column\")) WHEN \"$column\" IS NULL THEN NULL ELSE CAST(\"$column\" AS TEXT) END)"
        }
        val where = predicates[table]?.let { " WHERE $it" }.orEmpty()
        val sql = "SELECT json_object($projection) FROM \"$table\"$where ORDER BY ${columns.joinToString(",") { "\"$it\"" }}"
        db.withExclusiveAccess {
            val originalBytes = db.prepare(sql).use { statement ->
                buildList { while (statement.step()) add(statement.getText(0).encodeToByteArray()) }
            }
            val blobSql = "SELECT CAST(json_object($projection) AS BLOB) FROM \"$table\"$where ORDER BY ${columns.joinToString(",") { "\"$it\"" }}"
            db.prepare(blobSql).use { statement ->
                val digests = mutableListOf<String>()
                while (statement.step()) {
                    val bytes = statement.getBlob(0)
                    assertTrue(digests.size < originalBytes.size, "no additional BLOB row")
                    assertContentEquals(originalBytes[digests.size], bytes, "exact bytes for $table row ${digests.size}")
                    digests += sha256Hex(bytes)
                }
                assertEquals(originalBytes.size,digests.size)
                digests
            }
        }
    }
}
