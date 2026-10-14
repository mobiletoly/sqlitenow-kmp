package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Retains synthetic databases; the isolated runner owns authority cleanup. */
internal class DatabaseCleanupAuditJvmTest : RealServerSupport() {
    @Test fun defaultTarget() = audit(1_000_000)
    @Test fun targetOne() = audit(1)

    private fun audit(target: Long): Unit = runBlocking {
        val rootEnv = System.getenv("OVERSQLITE_CLEANUP_AUDIT_ROOT")
        if (rootEnv == null) { println("BODY_NOT_EXECUTED: retained-file live audit requires isolated runner"); return@runBlocking }
        withTimeout(570_000) {
            val root = Path.of(rootEnv); Files.createDirectories(root)
            val authority = assertNotNull(requireRealServerConfig())
            val tables = listOf(SyncTable("categories",syncKeyColumnName="id"))
            val user = randomRealServerId("cleanup-kmp")
            val ids = (0 until 125).map { "00000000-0000-4000-8000-%012x".format(it) }
            class Owner(val path: Path) {
                var db: SafeSQLiteConnection? = null
                var client: DefaultOversqliteClient? = null
                var http: HttpClient? = null
            }
            val owners = (0..1).map { Owner(root.resolve("$it.sqlite")) }
            suspend fun open(o: Owner) {
                val fresh = !Files.exists(o.path)
                val db = BundledSqliteConnectionProvider.openConnection(o.path.toString(),debug=false); o.db=db
                if (fresh) db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                db.execSQL("PRAGMA journal_mode=WAL")
                val source = bootstrapManagedSourceId(db,authority.baseUrl,syncTables=tables)
                val http = newRealServerHttpClient(authority.baseUrl,issueDummySigninToken(authority.baseUrl,user,source));o.http=http
                val client = DefaultOversqliteClient.forTesting(db,OversqliteConfig(generationId=1,databaseId="fixture",serviceId="business",syncTables=tables,
                    maxSyncWorkUnitsPerInvocation=target,transferTargets=OversqliteTransferTargets(maxPushRows=40)),http)
                o.client=client;client.open().getOrThrow();client.attach(user).getOrThrow()
            }
            suspend fun close(o: Owner) { o.client?.closeFromCoordinatedOwner();o.client=null;o.http?.close();o.http=null;o.db?.close();o.db=null }
            suspend fun census(label: String) {
                owners.forEachIndexed { i,o ->
                    val db=checkNotNull(o.db)
                    val names=db.queryList("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_sync_*' ORDER BY name") {it.getText(0)}
                    val tablesJson=buildJsonObject { for(name in names) put(name,scalarLong(db,"SELECT count(*) FROM \"$name\"")) }
                    val report=buildJsonObject {
                        put("label",label);put("target",target);put("owner",i);put("tables",tablesJson)
                        for(p in listOf("page_count","freelist_count","page_size")) put(p,scalarLong(db,"PRAGMA $p"))
                        put("business_rows",scalarLong(db,"SELECT count(*) FROM categories"))
                        put("foreign_key_errors",scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
                        put("reservations",scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                        for(suffix in listOf("","-wal","-shm")) { val path=Path.of(o.path.toString()+suffix);put("bytes$suffix",if(Files.exists(path)) Files.size(path) else 0) }
                    }

                    assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(0L,scalarLong(db,"SELECT apply_mode FROM _sync_apply_state"))
                    if(label != "cancelled") for(table in listOf("_sync_dirty_rows","_sync_outbox_rows","_sync_apply_plan","_sync_apply_rows","_sync_apply_edges","_sync_push_plan","_sync_push_nodes","_sync_push_edges","_sync_snapshot_stage"))
                        assertEquals(0L,scalarLong(db,"SELECT count(*) FROM $table"),"$table after $label")
                    Files.writeString(root.resolve("$label-$i-census.json"),report.toString())
                }
            }
            suspend fun save(label: String) {
                census(label)
                owners.forEachIndexed { i,o -> close(o);for(suffix in listOf("","-wal","-shm")) {val path=Path.of(o.path.toString()+suffix);if(Files.exists(path)) Files.copy(path,root.resolve("$label-$i.sqlite$suffix"))} }
            }
            suspend fun resume() {owners.forEach {open(it)} }
            suspend fun syncPair() {checkNotNull(owners[0].client).sync().getOrThrow();checkNotNull(owners[1].client).pullToStable().getOrThrow()}
            try {
                resume()
                ids.forEachIndexed { i,id -> checkNotNull(owners[0].db).execSQL("INSERT INTO categories VALUES('$id','initial-%03d',%s)".format(i,if(i==0) "NULL" else "'${ids[0]}'")) }
                syncPair();save("initial");resume()
                for(cycle in 1..10) {
                    val db=checkNotNull(owners[0].db)
                    for(i in 1..10) db.execSQL("UPDATE categories SET name='cycle-%02d-update-%03d' WHERE id='${ids[i]}'".format(cycle,i))
                    for(i in 115..124) db.execSQL("DELETE FROM categories WHERE id='${ids[i]}'")
                    syncPair()
                    for(i in 115..124) db.execSQL("INSERT INTO categories VALUES('${ids[i]}','cycle-%02d-reinsert-%03d','${ids[0]}')".format(cycle,i))
                    syncPair();if(cycle==5) checkNotNull(owners[1].client).rebuild().getOrThrow()
                    for(o in owners) { assertEquals(125L,scalarLong(checkNotNull(o.db),"SELECT count(*) FROM categories"));assertEquals(0L,scalarLong(checkNotNull(o.db),"SELECT count(*) FROM _sync_dirty_rows")) }

                    for(o in owners) ids.forEachIndexed { i,id ->
                        val expected = when(i) { in 1..10 -> "cycle-%02d-update-%03d".format(cycle,i); in 115..124 -> "cycle-%02d-reinsert-%03d".format(cycle,i); else -> "initial-%03d".format(i) }
                        assertEquals(expected,scalarText(checkNotNull(o.db),"SELECT name FROM categories WHERE id='$id'"))
                    }
                    save("cycle-%02d".format(cycle));resume()
                }
                val writer=checkNotNull(owners[0].client);val db=checkNotNull(owners[0].db)
                db.execSQL("UPDATE categories SET name='recovered' WHERE id<>'${ids[0]}'")
                var commits=0
                writer.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit={
                    if(++commits == if(target==1L) 2 else 1) throw CancellationException("audit cancellation after durable input/preparation")
                }))
                assertFailsWith<CancellationException> {writer.pushPending().getOrThrow()}
                val hash=scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                assertTrue(hash.isNotEmpty());save("cancelled");resume()
                assertEquals(hash,scalarText(checkNotNull(owners[0].db),"SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                syncPair();for(o in owners) assertEquals(124L,scalarLong(checkNotNull(o.db),"SELECT count(*) FROM categories WHERE name='recovered'"))
                save("recovered")
                Files.writeString(root.resolve("manifest.json"),buildJsonObject {put("user",user);put("target",target);put("cancelled_hash",hash)}.toString())
            } finally {owners.forEach {close(it)} }
        }
    }
}
