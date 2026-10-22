package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READONLY
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.*

internal class AutomaticRecoveryJvmTest : CrossTargetSyncTestSupport() {
    private fun row(id: String,name: String,version: Long) = BundleRow("main","users",mapOf("id" to id),"UPDATE",version,buildJsonObject {put("id",id);put("name",name)})
    @Test fun closedFileAndWalCleanup() = runOwnedWebDatabaseTest {
        val path=createTempSqliteNowTestDbPath("automatic-cleanup")
        val server=MockSyncServer();val http=server.newHttpClient()
        var db=newFileBackedDb(path);var client:DefaultOversqliteClient?=null
        try {
            db.execSQL("PRAGMA journal_mode=WAL");db.execSQL("PRAGMA synchronous=FULL")
            createUsersAndPostsTables(db);client=newClient(db,http)
            client.openAndConnect("user-1").getOrThrow();db.execSQL("PRAGMA synchronous=FULL")
            db.execSQL("INSERT INTO users VALUES('A','a'),('B','b')");client.pushPending().getOrThrow()
            db.execSQL("CREATE UNIQUE INDEX users_name ON users(name)")
            var warmedPages=0L;var peakWal=0L
            repeat(10) { cycle->
                val version=cycle+2L
                val rows=listOf(row("A","cycle-$cycle",version),if(cycle%3==1)BundleRow("main","users",mapOf("id" to "B"),"DELETE",version,null) else row("B","b",version))
                assertEquals(version,server.addRemoteBundleForTest(rows));server.retainedBundleFloor=version
                if(cycle==4) {
                    client!!.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit={throw CancellationException("owned preparation cut")}))
                    assertFailsWith<CancellationException>{client!!.sync().getOrThrow()}
                    val creates=server.snapshotSessionCreateRequestCount
                    client!!.setSnapshotApplyFaultInjectorForTest(null);client!!.closeFromCoordinatedOwner();db.close()
                    db=newFileBackedDb(path);client=newClient(db,http);client!!.openAndConnect("user-1").getOrThrow();db.execSQL("PRAGMA synchronous=FULL")
                    client!!.sync().getOrThrow();assertEquals(creates,server.snapshotSessionCreateRequestCount)
                } else client!!.sync().getOrThrow()
                for(table in listOf("_sync_snapshot_stage","_sync_apply_plan","_sync_apply_rows","_sync_apply_edges","_sync_upload_journal","_sync_dirty_rows","_sync_outbox_rows"))assertEquals(0L,scalarLong(db,"SELECT count(*) FROM $table"),table)
                val pages=scalarLong(db,"PRAGMA page_count");if(cycle==2)warmedPages=pages;if(cycle>2)assertTrue(pages<=warmedPages+8,"owned pages bounded")
                val wal=Path.of(path+"-wal");if(Files.exists(wal))peakWal=maxOf(peakWal,Files.size(wal))
                assertEquals(version,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            }
            assertEquals("wal",scalarText(db,"PRAGMA journal_mode"));assertEquals(2L,scalarLong(db,"PRAGMA synchronous"));assertTrue(peakWal<=8L*1024*1024)
            val census=linkedMapOf<String,Long>()
            for(table in db.queryList("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_sync_*' ORDER BY name"){it.getText(0)})census[table]=scalarLong(db,"SELECT count(*) FROM ${quoteIdent(table)}")
            client!!.closeFromCoordinatedOwner();db.close()
            val connection=BundledSQLiteDriver().open(path,SQLITE_OPEN_READONLY)
            try {
                val integrity=connection.prepare("PRAGMA integrity_check");try{assertTrue(integrity.step());assertEquals("ok",integrity.getText(0))}finally{integrity.close()}
                val fk=connection.prepare("PRAGMA foreign_key_check");try{assertFalse(fk.step())}finally{fk.close()}
                val readonly=connection.prepare("PRAGMA query_only=ON");try{readonly.step()}finally{readonly.close()}
            }finally{connection.close()}
            println("AUTOMATIC_RESOURCE_COMPLETE "+buildJsonObject {put("cycles",10);put("closed_sqlite_bytes",Files.size(Path.of(path)));put("peak_wal_bytes",peakWal);put("warm_pages",warmedPages);put("integrity","ok");put("read_only_audit",true);put("census",buildJsonObject {census.forEach { (name,count)->put(name,count)}})})
        }finally{client?.closeFromCoordinatedOwner();http.close();db.close();deleteTempSqliteNowTestDbArtifacts(path)}
    }
}
