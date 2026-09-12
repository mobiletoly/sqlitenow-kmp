package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.Collections
import java.util.IdentityHashMap
import kotlin.test.*

internal class ReplaySqlLifecycleTest : SurvivorTestSupport() {
    @Test fun generationCopyRebuildsDescriptions() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("replay-owner")
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val db = newFileBackedDb(path)
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            db.execSQL("INSERT INTO users VALUES('u1', X'00ff')")
            client.open().getOrThrow()
            val unconfirmed = replayValidatedOwner(client)
            assertNull(unconfirmed.generation)
            val original = unconfirmed.replaySql.table("users")
            val shape = original.liveSelect
            assertSame(original, unconfirmed.withGeneration(null).replaySql.table("users"))
            val copied = unconfirmed.copy()
            assertNotSame(unconfirmed.replaySql, copied.replaySql)
            assertNotSame(original, copied.replaySql.table("users"))
            threadedFirstAccess(copied)
            assertOnlySchemaDescriptions(unconfirmed.replaySql)
            assertSame(unconfirmed, unconfirmed.withGeneration(null))
            assertNotSame(unconfirmed, unconfirmed.copy())
            val before = baselineQualificationVector(db)
            assertTrue(client.attach("user-1").isFailure)
            assertSame(unconfirmed, replayValidatedOwner(client), "rejected candidate is not published")
            assertSame(original, replayValidatedOwner(client).replaySql.table("users"))
            assertSame(shape, original.liveSelect)
            assertEquals(before, baselineQualificationVector(db))
            db.execSQL("UPDATE users SET name='valid' WHERE id='u1'")
            client.attach("user-1").getOrThrow()
            val admitted = replayValidatedOwner(client)
            assertNotSame(unconfirmed, admitted)
            assertNotSame(original, admitted.replaySql.table("users"))
            assertEquals(shape, admitted.replaySql.table("users").liveSelect)
            assertNotSame(admitted.replaySql, admitted.withGeneration(admitted.generation).replaySql)
            assertEquals("text", admitted.tableInfoByName.getValue("users").column("name").wireType)
            assertNull(unconfirmed.tableInfoByName.getValue("users").column("name").wireType)
            assertSame(admitted, replayValidatedOwner(client))
            val durable = baselineQualificationVector(db)
            client.closeFromCoordinatedOwner()
            db.close()
            val reopenedDb = newFileBackedDb(path)
            val reopened = newClient(reopenedDb, http)
            try {
                assertEquals(durable, baselineQualificationVector(reopenedDb))
                reopened.openAndConnect("user-1").getOrThrow()
                val next = replayValidatedOwner(reopened)
                assertNotSame(admitted, next)
                assertEquals(admitted, next, "equal schemas still have distinct owners")
                assertEquals(durable, baselineQualificationVector(reopenedDb))
            } finally { reopened.closeFromCoordinatedOwner(); reopenedDb.close() }
        } finally {
            withContext(NonCancellable) { client.closeFromCoordinatedOwner(); db.close(); http.close(); deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun schemaReplacementChangesColumnsAndKeyRepresentation() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("replay-schema")
        var previous: ValidatedConfig? = null
        try {
            for (blob in listOf(false, true)) {
                val server = MockSyncServer()
                val parentColumns = if (blob) listOf(GenerationColumn("label", "text", false), GenerationColumn("id", "uuid", false))
                    else listOf(GenerationColumn("id", "uuid", false), GenerationColumn("label", "text", false))
                val columns = listOf(GenerationColumn("id", "uuid", false), GenerationColumn("parent_id", "uuid", true))
                server.generationOverride = SyncGeneration("1", "", listOf(
                    GenerationTable("main", "parents", listOf("id"), parentColumns, emptyList()),
                    GenerationTable("main", "children", listOf("id"), columns,
                        listOf(GenerationForeignKey(listOf("parent_id"), "main", "parents", listOf("id"), "no_action", "no_action"))),
                )).let { it.copy(fingerprint = it.computeFingerprint()) }
                val http = server.newHttpClient()
                val db = newFileBackedDb(path)
                val client = newClient(db, http, syncTables = listOf(SyncTable("parents", "id"), SyncTable("children", "id")))
                try {
                    val keyType = if (blob) "BLOB" else "TEXT"
                    db.execSQL(if (blob) "CREATE TABLE parents(label TEXT NOT NULL,id BLOB PRIMARY KEY NOT NULL)"
                        else "CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL)")
                    db.execSQL("CREATE TABLE children(id $keyType PRIMARY KEY NOT NULL,parent_id $keyType REFERENCES parents(id))")
                    client.openAndConnect("user-1").getOrThrow()
                    val current = replayValidatedOwner(client)
                    val description = current.replaySql.table("parents")
                    val foreign = current.replaySql.table("children").foreignKey(0)
                    assertEquals(if(blob) "lower(hex(\"id\"))" else "CAST(\"id\" AS TEXT)", description.keyExpression)
                    assertEquals(if(blob) "SELECT lower(hex(\"parent_id\")) FROM \"children\" WHERE \"id\"=? AND \"parent_id\" IS NOT NULL"
                        else "SELECT CAST(\"parent_id\" AS TEXT) FROM \"children\" WHERE \"id\"=? AND \"parent_id\" IS NOT NULL", foreign.value)
                    assertEquals(blob, current.tableInfoByName.getValue("parents").primaryKeyIsBlob)
                    assertEquals(blob, current.tableInfoByName.getValue("children").primaryKeyIsBlob)
                    assertEquals(parentColumns.map { it.name }, current.tableInfoByName.getValue("parents").columns.map { it.name })
                    assertEquals("parents", current.tableInfoByName.getValue("children").foreignKeys.single().refTable)
                    previous?.let { old ->
                        assertNotSame(old, current)
                        assertNotSame(old.replaySql, current.replaySql)
                        assertNotEquals(old.replaySql.table("parents").upsert, description.upsert)
                        assertNotEquals(old.replaySql.table("children").foreignKey(0).value, foreign.value)
                        assertFalse(old.tableInfoByName.getValue("parents").primaryKeyIsBlob)
                        assertEquals(listOf("id", "label"), old.tableInfoByName.getValue("parents").columns.map { it.name })
                    }
                    previous = current
                    client.closeFromCoordinatedOwner()
                    // Negative control: arbitrary DDL cannot masquerade as a supported reopen.
                    db.execSQL("ALTER TABLE parents ADD COLUMN extra TEXT")
                    val before = baselineQualificationVector(db, listOf("parents", "children"))
                    val rejected = newClient(db, http, syncTables = listOf(SyncTable("parents", "id"), SyncTable("children", "id")))
                    try {
                        assertTrue(rejected.open().isFailure)
                        assertEquals(before, baselineQualificationVector(db, listOf("parents", "children")))
                    } finally { rejected.closeFromCoordinatedOwner() }
                } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
                // Explicitly recreate only this owned fixture after all owners have closed.
                deleteTempSqliteNowTestDbArtifacts(path)
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun reopenAndIndependentClientsDoNotShareDescriptions() = runOwnedWebDatabaseTest {
        val paths = List(2) { createTempSqliteNowTestDbPath("replay-independent-$it") }
        val owners = mutableListOf<ValidatedConfig>()
        try {
            for (path in paths) {
                val server = MockSyncServer()
                val http = server.newHttpClient()
                var db = newFileBackedDb(path)
                var client = newClient(db, http)
                try {
                    createUsersAndPostsTables(db)
                    db.execSQL("INSERT INTO users VALUES('u1','durable')")
                    client.open().getOrThrow()
                    val owner = replayValidatedOwner(client)
                    val description = owner.replaySql.table("users")
                    assertSame(description, owner.replaySql.table("users"))
                    description.liveSelect
                    assertOnlySchemaDescriptions(owner.replaySql)
                    owners.forEach { other ->
                        assertEquals(other, owner)
                        assertNotSame(other.replaySql, owner.replaySql)
                        assertNotSame(other.replaySql.table("users"), description)
                    }
                    owners += owner
                    val durable = baselineQualificationVector(db)
                    client.closeFromCoordinatedOwner(); db.close()
                    db = newFileBackedDb(path); client = newClient(db, http)
                    assertEquals(durable, baselineQualificationVector(db))
                    client.open().getOrThrow()
                    val reopened = replayValidatedOwner(client)
                    assertEquals(owner, reopened); assertNotSame(owner.replaySql, reopened.replaySql)
                    assertNotSame(description, reopened.replaySql.table("users"))
                    assertEquals(description.liveSelect, reopened.replaySql.table("users").liveSelect)
                    assertEquals(durable, baselineQualificationVector(db))
                } finally { client.closeFromCoordinatedOwner(); db.close(); http.close() }
            }
        } finally { paths.forEach { deleteTempSqliteNowTestDbArtifacts(it) } }
    }

    @Test fun sameSchemaReadsObserveRowAndMetadataChanges() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL,i INTEGER,r REAL,t TEXT,b BLOB,n INTEGER)")
            db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_operation_state")); db.execSQL(applyPlanDdl); db.execSQL(pushPlanDdl)
            db.execSQL("INSERT INTO _sync_operation_state(singleton_key) VALUES(1)")
            db.execSQL("INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes,edge_entries,edge_bytes) VALUES(1,'u','g','ready',0,0,0,0,2,10,3,20)")
            db.execSQL("INSERT INTO _sync_push_plan(singleton_key,unit,guard,phase,state,entries,bytes) VALUES(1,'u','g','ready','{}',4,40)")
            val physical = TableInfoCache().get(db,"items")
            val owner = replayTestConfig(mapOf("items" to physical.copy(columns = physical.columns.map { c -> c.copy(wireType=when(c.name) {
                "i", "n" -> "int64"; "r" -> "float64"; "b" -> "bytes"; else -> "text"
            }) })))
            val description = owner.replaySql.table("items")
            val local = OversqliteLocalStore(db, Json) { owner }
            val workspace = OversqliteWorkspace(db, OversqliteConfig(1, emptyList(), "reads", "main"))
            db.withStatementCache { cache ->
                suspend fun read(expected: String?) {
                    assertEquals(expected, local.serializeExistingRow("items", "a"))
                    assertEquals(expected, local.serializeExistingRow("items", "a", cache))
                    assertSame(description, owner.replaySql.table("items"))
                }
                read(null)
                db.execSQL("INSERT INTO items VALUES('a',9007199254740993,1.25,'fresh',X'00ff',NULL)")
                read("""{"id":{"storage":"text","value":"a"},"i":{"storage":"integer","value":"9007199254740993"},"r":{"storage":"real","value":"1.25"},"t":{"storage":"text","value":"fresh"},"b":{"storage":"blob","value":"00ff"},"n":{"storage":"null","value":null}}""")
                assertEquals(WorkspaceCharge(12,3202), workspace.usage("a", statementCache=cache))
                assertEquals(workspace.usage("a"), workspace.usage("a",statementCache=cache))
                db.execSQL("UPDATE items SET i=-7,r=2.5,t='changed 猫',b=X'fe',n=42")
                read("""{"id":{"storage":"text","value":"a"},"i":{"storage":"integer","value":"-7"},"r":{"storage":"real","value":"2.5"},"t":{"storage":"text","value":"changed 猫"},"b":{"storage":"blob","value":"fe"},"n":{"storage":"integer","value":"42"}}""")
                db.execSQL("UPDATE _sync_apply_plan SET unit='é',guard='猫',preserve_schema='🙂',preserve_table='t',preserve_key='k'")
                db.execSQL("UPDATE _sync_push_plan SET unit='é',guard='猫',state='🙂'")
                assertEquals(WorkspaceCharge(12,3286), workspace.usage("b",statementCache=cache))
                assertEquals(workspace.usage("b"), workspace.usage("b",statementCache=cache))
                db.execSQL("UPDATE _sync_apply_plan SET row_cursor='é'")
                assertEquals(WorkspaceCharge(12,3298), workspace.usage("c",statementCache=cache))
                db.execSQL("DELETE FROM items"); read(null)
                db.execSQL("DELETE FROM _sync_apply_plan"); db.execSQL("DELETE FROM _sync_push_plan")
                assertEquals(WorkspaceCharge(1,536), workspace.usage("d",statementCache=cache))
                assertEquals(workspace.usage("d"),workspace.usage("d",statementCache=cache))
            }
        } finally { db.close() }
    }

    private fun threadedFirstAccess(owner: ValidatedConfig) {
        val threads = Collections.synchronizedSet(mutableSetOf<Long>())
        val barrier = CyclicBarrier(8)
        val pool = Executors.newFixedThreadPool(8)
        try {
            val results = List(8) { pool.submit<ReplayKeySql> {
                threads += Thread.currentThread().id
                barrier.await(10, TimeUnit.SECONDS)
                owner.replaySql.table("users").afterCursorKey
            } }.map { it.get(15, TimeUnit.SECONDS) }
            assertEquals(8, threads.size)
            results.forEach { assertSame(results.first(), it) }
            println("REPLAY_SQL_THREADS callers=8 threads=8 published=1")
        } finally { pool.shutdownNow(); assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS)) }
    }
}

/** Inspect initialized lazies only: this never constructs an unused description. */
internal fun replayRetainedDescriptions(root: ReplaySchemaSql): Pair<Int, Long> {
    val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
    val strings = Collections.newSetFromMap(IdentityHashMap<String, Boolean>())
    var shapes = 0
    fun visit(value: Any?) {
        if (value == null || !visited.add(value)) return
        when(value) {
            is String -> { strings.add(value); return }
            is Lazy<*> -> { if(value.isInitialized()) { shapes++; visit(value.value) }; return }
            is Map<*,*> -> { value.values.forEach(::visit); return }
            is Iterable<*> -> { value.forEach(::visit); return }
        }
        if(value is ReplaySchemaSql || value is ReplayTableSql || value is ReplayKeySql) {
            value.javaClass.declaredFields.filterNot { java.lang.reflect.Modifier.isStatic(it.modifiers) }.forEach { field ->
                field.isAccessible=true; visit(field.get(value))
            }
        }
    }
    visit(root)
    return shapes to strings.filter { it.startsWith("SELECT") || it.startsWith("INSERT") || it.startsWith("json_object") || it.startsWith("2+") || it.startsWith("CAST") || it.startsWith("lower(hex") }.sumOf { it.encodeToByteArray().size.toLong() }
}

private fun assertOnlySchemaDescriptions(root: ReplaySchemaSql) {
    val forbidden = listOf(androidx.sqlite.SQLiteConnection::class.java, androidx.sqlite.SQLiteStatement::class.java,
        dev.goquick.sqlitenow.core.SafeSQLiteConnection::class.java, RuntimeState::class.java)
    for(type in listOf(ReplaySchemaSql::class.java, ReplayTableSql::class.java, ReplayKeySql::class.java)) {
        assertTrue(type.declaredFields.none { field -> forbidden.any { it.isAssignableFrom(field.type) } })
    }
    assertTrue(replayRetainedDescriptions(root).second > 0)
}
