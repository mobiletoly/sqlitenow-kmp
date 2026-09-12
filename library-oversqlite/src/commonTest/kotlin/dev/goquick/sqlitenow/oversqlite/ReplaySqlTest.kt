package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.common.platform
import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import kotlin.test.*

internal class ReplaySqlTest : CrossTargetSyncTestSupport() {
    @Test fun sqlMatchesIndependentFixturesAndPreservesBindingOrder() = runOwnedWebDatabaseTest {
        for (blob in listOf(false, true)) {
            val db = newDb()
            try {
                db.execSQL("CREATE TABLE parents_text(id TEXT PRIMARY KEY NOT NULL)")
                db.execSQL("CREATE TABLE parents_blob(id BLOB PRIMARY KEY NOT NULL)")
                db.execSQL("CREATE TABLE records(\"ID\" ${if (blob) "BLOB" else "TEXT"} PRIMARY KEY NOT NULL,i INTEGER,r REAL,t TEXT,b BLOB,n INTEGER,parent_text TEXT REFERENCES parents_text(id),parent_blob BLOB REFERENCES parents_blob(id))")
                db.execSQL("CREATE TABLE \"key-only\"(\"Key\" TEXT PRIMARY KEY NOT NULL)")
                val raw = TableInfoCache()
                val infos = listOf("parents_text", "parents_blob", "records", "key-only").associateWith { name ->
                    val info = raw.get(db, name)
                    info.copy(columns = info.columns.map { c -> c.copy(wireType = when(c.name) {
                        "ID", "parent_text", "parent_blob" -> "uuid"
                        "i", "n" -> "int64"
                        "r" -> "float64"
                        "b" -> "bytes"
                        else -> if (name.startsWith("parents")) "uuid" else "text"
                    }) })
                }
                val config = replayTestConfig(infos)
                val sql = config.replaySql.table("records")
                val key = if (blob) "lower(hex(\"ID\"))" else "CAST(\"ID\" AS TEXT)"
                assertEquals("SELECT $key FROM \"records\" ORDER BY \"ID\" LIMIT 1", sql.initialKey.value)
                assertEquals("SELECT length(CAST($key AS BLOB)) FROM \"records\" ORDER BY \"ID\" LIMIT 1", sql.initialKey.size)
                assertEquals("SELECT $key FROM \"records\" WHERE \"ID\">? ORDER BY \"ID\" LIMIT 1", sql.afterCursorKey.value)
                assertEquals("SELECT length(CAST($key AS BLOB)) FROM \"records\" WHERE \"ID\">? ORDER BY \"ID\" LIMIT 1", sql.afterCursorKey.size)
                val fkExpressions = listOf("CAST(\"parent_text\" AS TEXT)", "lower(hex(\"parent_blob\"))")
                infos.getValue("records").foreignKeys.forEachIndexed { index, fk ->
                    val expression = fkExpressions[if (fk.fromCol == "parent_text") 0 else 1]
                    val tail = " FROM \"records\" WHERE \"ID\"=? AND \"${fk.fromCol}\" IS NOT NULL"
                    assertEquals("SELECT $expression$tail", sql.foreignKey(index).value)
                    assertEquals("SELECT length(CAST($expression AS BLOB))$tail", sql.foreignKey(index).size)
                }
                assertEquals(goldenBound, sql.payloadByteBound)
                assertEquals("SELECT $goldenBound FROM \"records\" live_row WHERE \"ID\"=?", sql.captureSize)
                assertEquals("SELECT COALESCE((SELECT $goldenBound FROM \"records\" live_row WHERE \"ID\"=?),0)+COALESCE((SELECT length(CAST(payload AS BLOB)) FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?),0)", sql.decisionCopySize)
                assertEquals(goldenProjection, sql.liveProjection)
                assertEquals(goldenSelect, sql.liveSelect)
                assertEquals(goldenAuthoritative, sql.authoritativeSelect)
                assertEquals(goldenUpsert, sql.upsert)
                assertEquals("INSERT INTO \"key-only\" (\"Key\") VALUES (?) ON CONFLICT(\"Key\") DO NOTHING", config.replaySql.table("key-only").upsert)
                assertEquals(listOf("\"ID\"", "\"i\"", "\"r\"", "\"t\"", "\"b\"", "\"n\"", "\"parent_text\"", "\"parent_blob\""), sql.quotedColumns)
                assertEquals(8, sql.upsert.count { it == '?' }); assertEquals(8, sql.authoritativeSelect.count { it == '?' })
                val id = "00112233-4455-6677-8899-aabbccddeeff"
                db.execSQL("INSERT INTO parents_text VALUES('$id')")
                db.execSQL("INSERT INTO parents_blob VALUES(X'00112233445566778899aabbccddeeff')")
                val payload = Json.parseToJsonElement("""{"id":"$id","i":"9007199254740993","r":"1.25","t":"fresh 猫","b":"AP8=","n":null,"parent_text":"$id","parent_blob":"$id"}""").jsonObject
                var reads = 0
                val local = OversqliteLocalStore(db, Json) { reads++; config }
                local.upsertRow("records", payload, PayloadSource.AUTHORITATIVE_WIRE)
                assertEquals(1, reads, "one owner snapshot through normalization, SQL and binding")
                val pk = if (blob) "00112233445566778899aabbccddeeff" else id
                val captured = Json.parseToJsonElement(local.serializeExistingRow("records", pk)!!).jsonObject
                assertEquals(2, reads)
                assertEquals(Json.parseToJsonElement("""{"storage":"integer","value":"9007199254740993"}"""), captured["i"])
                assertEquals(Json.parseToJsonElement("""{"storage":"real","value":"1.25"}"""), captured["r"])
                assertEquals(Json.parseToJsonElement("""{"storage":"text","value":"fresh 猫"}"""), captured["t"])
                assertEquals(Json.parseToJsonElement("""{"storage":"blob","value":"00ff"}"""), captured["b"])
                assertEquals(Json.parseToJsonElement("""{"storage":"null","value":null}"""), captured["n"])
                assertEquals(captured, Json.parseToJsonElement(local.captureAuthoritativePayload(" RECORDS ", payload)))
                assertEquals(3, reads)
                val expectedBound = if (blob) 1260L else 1444L
                db.withPreparedStatement(sql.captureSize) { st ->
                    if (blob) st.bindBlob(1, decodeLocalUuidBytes(pk)) else st.bindText(1, pk)
                    assertTrue(st.step()); assertEquals(expectedBound, st.getLong(0))
                }
                db.execSQL("CREATE TABLE _sync_dirty_rows(schema_name TEXT,table_name TEXT,key_json TEXT,payload TEXT)")
                db.execSQL("INSERT INTO _sync_dirty_rows VALUES('main','records','synthetic','abc')")
                assertEquals(4, sql.decisionCopySize.count { it == '?' })
                for (key in listOf("synthetic", "absent")) db.withPreparedStatement(sql.decisionCopySize) { st ->
                    if (blob) st.bindBlob(1, decodeLocalUuidBytes(pk)) else st.bindText(1, pk)
                    st.bindText(2, "main"); st.bindText(3, "records"); st.bindText(4, key)
                    assertTrue(st.step()); assertEquals(expectedBound + if (key == "synthetic") 3 else 0, st.getLong(0))
                }
                repeat(2) { local.upsertRow("key-only", buildJsonObject { put("key", "only") }, PayloadSource.AUTHORITATIVE_WIRE) }
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM \"key-only\""))
                db.withStatementCache { cache ->
                    val plan = local.prepareSnapshotUpsertPlan(cache)
                    plan.upsertAuthoritativeRow("records", JsonObject(payload + ("t" to JsonPrimitive("snapshot"))))
                    plan.finish()
                    assertEquals("snapshot", scalarText(db, "SELECT t FROM records"))
                }
                for (shape in listOf(sql.initialKey, sql.afterCursorKey)) {
                    db.withPreparedStatement(shape.value) { st ->
                        if (shape === sql.afterCursorKey) {
                            if (blob) st.bindBlob(1, ByteArray(16)) else st.bindText(1, "")
                        }
                        assertTrue(st.step()); assertEquals(pk, st.getText(0))
                    }
                    db.withPreparedStatement(shape.size) { st ->
                        if (shape === sql.afterCursorKey) {
                            if (blob) st.bindBlob(1, ByteArray(16)) else st.bindText(1, "")
                        }
                        assertTrue(st.step()); assertEquals(pk.length.toLong(), st.getLong(0))
                    }
                }
                for (index in infos.getValue("records").foreignKeys.indices) {
                    val fk = infos.getValue("records").foreignKeys[index]
                    db.withPreparedStatement(sql.foreignKey(index).value) { st ->
                        if(blob) st.bindBlob(1, decodeLocalUuidBytes(pk)) else st.bindText(1, pk)
                        assertTrue(st.step()); assertEquals(if(fk.fromCol=="parent_blob") "00112233445566778899aabbccddeeff" else id, st.getText(0))
                    }
                    db.withPreparedStatement(sql.foreignKey(index).size) { st ->
                        if(blob) st.bindBlob(1, decodeLocalUuidBytes(pk)) else st.bindText(1, pk)
                        assertTrue(st.step()); assertEquals(if(fk.fromCol=="parent_blob") 32L else 36L, st.getLong(0))
                    }
                }
            } finally { db.close() }
        }
        val quoted = replayTestConfig(mapOf("table\"name" to TableInfo("table\"name", listOf(ColumnInfo("Key", "TEXT", true, true, null)))))
        assertEquals("SELECT CAST(\"Key\" AS TEXT) FROM \"table\"\"name\" ORDER BY \"Key\" LIMIT 1", quoted.replaySql.table("table\"name").initialKey.value)
        val quotedDb = newDb()
        try {
            quotedDb.execSQL("CREATE TABLE \"table\"\"name\"(\"Key\" TEXT PRIMARY KEY NOT NULL)")
            quotedDb.withPreparedStatement(quoted.replaySql.table("table\"name").upsert) { st -> st.bindText(1, "quoted"); st.step() }
            quotedDb.withPreparedStatement(quoted.replaySql.table("table\"name").initialKey.value) { st ->
                assertTrue(st.step()); assertEquals("quoted", st.getText(0))
            }
        } finally { quotedDb.close() }
    }

    @Test fun fixedShapesAreReusedAndBoundedBySchema() = runOwnedWebDatabaseTest {
        val infos = linkedMapOf(
            "first" to TableInfo("first", listOf(ColumnInfo("id", "TEXT", true, true, null), ColumnInfo("p", "TEXT", false, false, null)),
                listOf(ForeignKeyInfo(0,"second","p","id"),ForeignKeyInfo(1,"missing","p","id"))),
            "second" to TableInfo("second", listOf(ColumnInfo("id", "BLOB", true, true, null))),
            "unused" to TableInfo("unused", emptyList()),
        )
        val config = replayTestConfig(infos)
        val gate = CompletableDeferred<Unit>()
        val ready = List(8) { CompletableDeferred<Unit>() }
        coroutineScope {
            val calls = ready.map { signal -> async(Dispatchers.Default) { signal.complete(Unit); gate.await(); config.replaySql.table("first").afterCursorKey } }
            ready.forEach { it.await() }; gate.complete(Unit)
            val shapes = calls.awaitAll()
            shapes.forEach { assertSame(shapes.first(), it) }
        }
        val first = config.replaySql.table("first")
        assertNotSame(first.initialKey, first.afterCursorKey)
        assertNotSame(first, config.replaySql.table("second"))
        assertTrue(first.foreignKey(0).value.contains("lower(hex(\"p\"))"))
        repeat(2) { assertFailsWith<NoSuchElementException> { first.foreignKey(1) } }
        val unused = config.replaySql.table("unused")
        repeat(2) { assertFailsWith<IllegalStateException> { unused.initialKey } }
        assertEquals("json_object()", unused.liveProjection, "failed unused key shape does not poison other lazies")
        val stable = listOf(first.initialKey, first.afterCursorKey, first.foreignKey(0))
        repeat(1000) { assertSame(stable[0], first.initialKey); assertSame(stable[1], first.afterCursorKey); assertSame(stable[2], first.foreignKey(0)) }
        assertNotSame(first, config.copy().replaySql.table("first"))
        infos.clear()
        assertSame(first, config.replaySql.table("first"), "owner captured immutable schema membership")
        println("REPLAY_SQL_ORDER runtime=${platform()} callers=8 shapes=3 utf8Bytes=${stable.sumOf { it.size.encodeToByteArray().size + it.value.encodeToByteArray().size }}")
    }

    @Test fun fixedUnitsMatchOriginalHashes() = runOwnedWebDatabaseTest {
        val originals = listOf(
            Triple("apply", ReplayFixedUnits.apply, "c024999871ca1f1093ff0c8697d17248c97e127c3b5f82ca39a4aefc39f1a26c"),
            Triple("settlement-decisions", ReplayFixedUnits.settlementDecisions, "590d3b03e98d4c4d86468edad18e8bf5efb44e43d3e64e2643ac9271f03fb136"),
            Triple("reservation", ReplayFixedUnits.reservation, "2b50fc036e74b546151c1e58312fa06d6d93126dae2990fa337fbdfecc50a33e"),
            Triple("reservation-reconciliation", ReplayFixedUnits.reservationReconciliation, "77c370812d2f62dea349ed81798aaa8a3d3b63921bf4b22024554321da646038"),
            Triple("retained-reconciliation", ReplayFixedUnits.retainedReconciliation, "dcd1c9414d0ab3ae4456cd4f2a97587cd169541e1ee9b009f59f17bf1e92d4bc"),
        )
        val db = newDb()
        try {
            val workspace = OversqliteWorkspace(db, OversqliteConfig(1, emptyList(), "units", "main"))
            for ((kind, actual, expected) in originals) {
                assertEquals(expected, actual); assertEquals(expected, applyUnit(kind,0,""))
                assertNotEquals(actual, applyUnit(kind,1,"")); assertNotEquals(actual, applyUnit(kind,0,"hash"))
                val failure = assertFailsWith<SyncWorkspaceLimitException> { workspace.add(actual, WorkspaceCharge(Long.MAX_VALUE), WorkspaceCharge(1)) }
                assertEquals(expected, failure.unit); assertEquals("entries", failure.dimension)
                assertEquals(Long.MAX_VALUE, failure.used); assertEquals(1L, failure.reserved); assertFalse(failure.resumable)
            }
            assertNotEquals(ReplayFixedUnits.apply, applyUnit("bundle",0,""))
        } finally { db.close() }
    }

    private val goldenBound = """2+92+CASE typeof("live_row"."ID") WHEN 'blob' THEN 2*length("live_row"."ID")+2 WHEN 'text' THEN 6*length(CAST("live_row"."ID" AS BLOB))+2 ELSE 32 END+86+CASE typeof("live_row"."i") WHEN 'blob' THEN 2*length("live_row"."i")+2 WHEN 'text' THEN 6*length(CAST("live_row"."i" AS BLOB))+2 ELSE 32 END+86+CASE typeof("live_row"."r") WHEN 'blob' THEN 2*length("live_row"."r")+2 WHEN 'text' THEN 6*length(CAST("live_row"."r" AS BLOB))+2 ELSE 32 END+86+CASE typeof("live_row"."t") WHEN 'blob' THEN 2*length("live_row"."t")+2 WHEN 'text' THEN 6*length(CAST("live_row"."t" AS BLOB))+2 ELSE 32 END+86+CASE typeof("live_row"."b") WHEN 'blob' THEN 2*length("live_row"."b")+2 WHEN 'text' THEN 6*length(CAST("live_row"."b" AS BLOB))+2 ELSE 32 END+86+CASE typeof("live_row"."n") WHEN 'blob' THEN 2*length("live_row"."n")+2 WHEN 'text' THEN 6*length(CAST("live_row"."n" AS BLOB))+2 ELSE 32 END+146+CASE typeof("live_row"."parent_text") WHEN 'blob' THEN 2*length("live_row"."parent_text")+2 WHEN 'text' THEN 6*length(CAST("live_row"."parent_text" AS BLOB))+2 ELSE 32 END+146+CASE typeof("live_row"."parent_blob") WHEN 'blob' THEN 2*length("live_row"."parent_blob")+2 WHEN 'text' THEN 6*length(CAST("live_row"."parent_blob" AS BLOB))+2 ELSE 32 END"""
    private val goldenProjection = """json_object('ID', json_object('storage', typeof(live_row."ID"), 'value', CASE typeof(live_row."ID") WHEN 'blob' THEN lower(hex(live_row."ID")) WHEN 'real' THEN printf('%!.17g', live_row."ID") WHEN 'null' THEN NULL ELSE CAST(live_row."ID" AS TEXT) END), 'i', json_object('storage', typeof(live_row."i"), 'value', CASE typeof(live_row."i") WHEN 'blob' THEN lower(hex(live_row."i")) WHEN 'real' THEN printf('%!.17g', live_row."i") WHEN 'null' THEN NULL ELSE CAST(live_row."i" AS TEXT) END), 'r', json_object('storage', typeof(live_row."r"), 'value', CASE typeof(live_row."r") WHEN 'blob' THEN lower(hex(live_row."r")) WHEN 'real' THEN printf('%!.17g', live_row."r") WHEN 'null' THEN NULL ELSE CAST(live_row."r" AS TEXT) END), 't', json_object('storage', typeof(live_row."t"), 'value', CASE typeof(live_row."t") WHEN 'blob' THEN lower(hex(live_row."t")) WHEN 'real' THEN printf('%!.17g', live_row."t") WHEN 'null' THEN NULL ELSE CAST(live_row."t" AS TEXT) END), 'b', json_object('storage', typeof(live_row."b"), 'value', CASE typeof(live_row."b") WHEN 'blob' THEN lower(hex(live_row."b")) WHEN 'real' THEN printf('%!.17g', live_row."b") WHEN 'null' THEN NULL ELSE CAST(live_row."b" AS TEXT) END), 'n', json_object('storage', typeof(live_row."n"), 'value', CASE typeof(live_row."n") WHEN 'blob' THEN lower(hex(live_row."n")) WHEN 'real' THEN printf('%!.17g', live_row."n") WHEN 'null' THEN NULL ELSE CAST(live_row."n" AS TEXT) END), 'parent_text', json_object('storage', typeof(live_row."parent_text"), 'value', CASE typeof(live_row."parent_text") WHEN 'blob' THEN lower(hex(live_row."parent_text")) WHEN 'real' THEN printf('%!.17g', live_row."parent_text") WHEN 'null' THEN NULL ELSE CAST(live_row."parent_text" AS TEXT) END), 'parent_blob', json_object('storage', typeof(live_row."parent_blob"), 'value', CASE typeof(live_row."parent_blob") WHEN 'blob' THEN lower(hex(live_row."parent_blob")) WHEN 'real' THEN printf('%!.17g', live_row."parent_blob") WHEN 'null' THEN NULL ELSE CAST(live_row."parent_blob" AS TEXT) END))"""
    private val goldenSelect = """SELECT json_object('ID', json_object('storage', typeof(live_row."ID"), 'value', CASE typeof(live_row."ID") WHEN 'blob' THEN lower(hex(live_row."ID")) WHEN 'real' THEN printf('%!.17g', live_row."ID") WHEN 'null' THEN NULL ELSE CAST(live_row."ID" AS TEXT) END), 'i', json_object('storage', typeof(live_row."i"), 'value', CASE typeof(live_row."i") WHEN 'blob' THEN lower(hex(live_row."i")) WHEN 'real' THEN printf('%!.17g', live_row."i") WHEN 'null' THEN NULL ELSE CAST(live_row."i" AS TEXT) END), 'r', json_object('storage', typeof(live_row."r"), 'value', CASE typeof(live_row."r") WHEN 'blob' THEN lower(hex(live_row."r")) WHEN 'real' THEN printf('%!.17g', live_row."r") WHEN 'null' THEN NULL ELSE CAST(live_row."r" AS TEXT) END), 't', json_object('storage', typeof(live_row."t"), 'value', CASE typeof(live_row."t") WHEN 'blob' THEN lower(hex(live_row."t")) WHEN 'real' THEN printf('%!.17g', live_row."t") WHEN 'null' THEN NULL ELSE CAST(live_row."t" AS TEXT) END), 'b', json_object('storage', typeof(live_row."b"), 'value', CASE typeof(live_row."b") WHEN 'blob' THEN lower(hex(live_row."b")) WHEN 'real' THEN printf('%!.17g', live_row."b") WHEN 'null' THEN NULL ELSE CAST(live_row."b" AS TEXT) END), 'n', json_object('storage', typeof(live_row."n"), 'value', CASE typeof(live_row."n") WHEN 'blob' THEN lower(hex(live_row."n")) WHEN 'real' THEN printf('%!.17g', live_row."n") WHEN 'null' THEN NULL ELSE CAST(live_row."n" AS TEXT) END), 'parent_text', json_object('storage', typeof(live_row."parent_text"), 'value', CASE typeof(live_row."parent_text") WHEN 'blob' THEN lower(hex(live_row."parent_text")) WHEN 'real' THEN printf('%!.17g', live_row."parent_text") WHEN 'null' THEN NULL ELSE CAST(live_row."parent_text" AS TEXT) END), 'parent_blob', json_object('storage', typeof(live_row."parent_blob"), 'value', CASE typeof(live_row."parent_blob") WHEN 'blob' THEN lower(hex(live_row."parent_blob")) WHEN 'real' THEN printf('%!.17g', live_row."parent_blob") WHEN 'null' THEN NULL ELSE CAST(live_row."parent_blob" AS TEXT) END))
FROM "records" live_row
WHERE live_row."ID" = ?"""
    private val goldenAuthoritative = """SELECT json_object('ID', json_object('storage', typeof(observed."ID"), 'value', CASE typeof(observed."ID") WHEN 'blob' THEN lower(hex(observed."ID")) WHEN 'real' THEN printf('%!.17g', observed."ID") WHEN 'null' THEN NULL ELSE CAST(observed."ID" AS TEXT) END), 'i', json_object('storage', typeof(observed."i"), 'value', CASE typeof(observed."i") WHEN 'blob' THEN lower(hex(observed."i")) WHEN 'real' THEN printf('%!.17g', observed."i") WHEN 'null' THEN NULL ELSE CAST(observed."i" AS TEXT) END), 'r', json_object('storage', typeof(observed."r"), 'value', CASE typeof(observed."r") WHEN 'blob' THEN lower(hex(observed."r")) WHEN 'real' THEN printf('%!.17g', observed."r") WHEN 'null' THEN NULL ELSE CAST(observed."r" AS TEXT) END), 't', json_object('storage', typeof(observed."t"), 'value', CASE typeof(observed."t") WHEN 'blob' THEN lower(hex(observed."t")) WHEN 'real' THEN printf('%!.17g', observed."t") WHEN 'null' THEN NULL ELSE CAST(observed."t" AS TEXT) END), 'b', json_object('storage', typeof(observed."b"), 'value', CASE typeof(observed."b") WHEN 'blob' THEN lower(hex(observed."b")) WHEN 'real' THEN printf('%!.17g', observed."b") WHEN 'null' THEN NULL ELSE CAST(observed."b" AS TEXT) END), 'n', json_object('storage', typeof(observed."n"), 'value', CASE typeof(observed."n") WHEN 'blob' THEN lower(hex(observed."n")) WHEN 'real' THEN printf('%!.17g', observed."n") WHEN 'null' THEN NULL ELSE CAST(observed."n" AS TEXT) END), 'parent_text', json_object('storage', typeof(observed."parent_text"), 'value', CASE typeof(observed."parent_text") WHEN 'blob' THEN lower(hex(observed."parent_text")) WHEN 'real' THEN printf('%!.17g', observed."parent_text") WHEN 'null' THEN NULL ELSE CAST(observed."parent_text" AS TEXT) END), 'parent_blob', json_object('storage', typeof(observed."parent_blob"), 'value', CASE typeof(observed."parent_blob") WHEN 'blob' THEN lower(hex(observed."parent_blob")) WHEN 'real' THEN printf('%!.17g', observed."parent_blob") WHEN 'null' THEN NULL ELSE CAST(observed."parent_blob" AS TEXT) END)) FROM (SELECT ? AS "ID",CAST(? AS INTEGER) AS "i",CAST(? AS REAL) AS "r",? AS "t",? AS "b",CAST(? AS INTEGER) AS "n",? AS "parent_text",? AS "parent_blob") AS observed"""
    private val goldenUpsert = """INSERT INTO "records" ("ID", "i", "r", "t", "b", "n", "parent_text", "parent_blob") VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT("ID") DO UPDATE SET "i" = excluded."i", "r" = excluded."r", "t" = excluded."t", "b" = excluded."b", "n" = excluded."n", "parent_text" = excluded."parent_text", "parent_blob" = excluded."parent_blob""""
}

internal fun replayTestConfig(infos: Map<String, TableInfo>): ValidatedConfig = ValidatedConfig(
    null, infos.keys.map { ValidatedSyncTable(it, infos.getValue(it).primaryKey?.name ?: "id") },
    infos.mapValues { it.value.primaryKey?.name ?: "id" }, infos.mapValues { listOf(it.value.primaryKey?.name ?: "id") },
    infos.keys.mapIndexed { index, name -> name to index }.toMap(), infos,
)
