package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.*
import dev.goquick.sqlitenow.core.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.*
import java.io.File
import kotlin.test.*

internal class SnapshotRestorePerformanceTest {
    @Test fun smallRichFixtures() = runBlocking { for(rows in listOf(16,64)) compare(RichSnapshotRestoreFixture(rows), listOf(false)) }
    @Test fun richSchemaDiagnosticsOffOnOff() = runBlocking { withTimeout(180_000) { compare(RichSnapshotRestoreFixture(),listOf(false,true,false)) } }
    private suspend fun compare(fixture: RichSnapshotRestoreFixture, policies: List<Boolean>) {
        initializePlatformTestContext()
        assertEquals(14,fixture.names.size);assertEquals(15,fixture.generation.tables.sumOf { it.foreignKeys.size })
        assertEquals(44,fixture.generation.tables.maxOf { it.columns.size })
        assertEquals(fixture.generation.fingerprint,fixture.generation.computeFingerprint())
        val diagnostics=SqliteNowDiagnostics(false);val sink=RichCapture();val previous=sqliteNowLogger;sqliteNowLogger=sink
        val results=mutableListOf<JsonObject>();var requests:List<String>?=null
        try { for(enabled in policies) {
            diagnostics.setEnabled(enabled)
            // JVM reserves an empty temporary file; Android returns an unused owned path.
            val path=createTempSqliteNowTestDbPath("rich-restore");assertTrue(!File(path).exists() || File(path).length()==0L)
            val config=OversqliteConfig(1,fixture.names.map { SyncTable(it,"id") },"fixture-service","main",diagnostics=diagnostics,
                maxSyncWorkUnitsPerInvocation=1_000_000,transferTargets=OversqliteTransferTargets(maxSnapshotChunkRows=1000))
            fun database()=SqliteNowDatabase(path,object:DatabaseMigrations {
                override val targetVersion=1
                override suspend fun applyMigration(conn:SafeSQLiteConnection,currentVersion:Int):Int { if(currentVersion<=0)for(ddl in fixture.ddl)conn.execSQL(ddl);return 1 }
            },debug=false,syncManaged=true).also { it.connectionConfig=SqliteConnectionConfig(diagnostics=diagnostics) }
            val http=fixture.client();var opening:OversqliteOpening<SqliteNowDatabase>?=null
            try {
                opening=database().openForSync(config,http,ServerWinsResolver);assertEquals(OversqliteOpeningOutcome.READY,opening.advance().outcome)
                val ready=checkNotNull(opening.readyOrNull());val db=ready.database.connection()
                db.execSQL("CREATE TABLE fixture_settings(id INTEGER PRIMARY KEY,value TEXT)");db.execSQL("INSERT INTO fixture_settings VALUES(1,'preserved')")
                val source=db.withPreparedStatement("SELECT current_source_id FROM _sync_attachment_state") { check(it.step());it.getText(0) }
                fixture.bindSource(source);sink.clear();fixture.requests.clear()
                val start=System.nanoTime();ready.client.attach("fixture-user").getOrThrow();val elapsed=System.nanoTime()-start
                val captured=sink.lines.toList();diagnostics.setEnabled(false)
                fixture.assertData(db)
                if(requests==null)requests=fixture.requests.toList() else assertEquals(requests,fixture.requests.toList())
                if(enabled) {
                    assertTrue(captured.isNotEmpty())
                    val records=captured.map { Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject }
                    assertTrue(records.any { it["name"]?.jsonPrimitive?.content=="snapshot.committed" && it["outcome"]?.jsonPrimitive?.content=="confirmed" })
                    for(n in listOf("verification_last_chunk","verification_seal","verification_final_apply","final_validation"))
                        assertEquals(1,records.count { it["kind"]?.jsonPrimitive?.content=="span_end" && it["name"]?.jsonPrimitive?.content==n && it["outcome"]?.jsonPrimitive?.content=="success" })
                } else assertTrue(captured.isEmpty())
                opening.close();opening=null
                val reopened=database().openForSync(config,http,ServerWinsResolver)
                try { assertEquals(OversqliteOpeningOutcome.READY,reopened.advance().outcome);fixture.assertData(checkNotNull(reopened.readyOrNull()).database.connection()) } finally { reopened.close() }
                results+=buildJsonObject { put("enabled",enabled);put("elapsed_ns",elapsed);put("records",captured.size) }
            } finally { opening?.close();http.close();deleteTempSqliteNowTestDbArtifacts(path);assertFalse(File(path).exists()) }
        }
        println("RICH_RESTORE_RESULT "+buildJsonObject { put("schema_sha256",fixture.schemaHash);put("data_sha256",fixture.dataHash);put("rows",fixture.rows.size);put("logical_bytes",fixture.logicalBytes);put("runs",JsonArray(results));put("reopen_and_cleanup",true) })
        } finally { diagnostics.setEnabled(false);sqliteNowLogger=previous }
    }
    private class RichCapture:SqliteNowLogger {
        val lines=mutableListOf<String>()
        fun clear()=lines.clear()
        private fun record(message:()->String) { val s=message();if(s.startsWith("SQLITENOW_DIAG ")) { check(lines.size<20_000);lines+=s;println(s) } }
        override fun e(throwable:Throwable?,message:()->String)=record(message)
        override fun w(throwable:Throwable?,message:()->String)=record(message)
        override fun i(throwable:Throwable?,message:()->String)=record(message)
        override fun d(throwable:Throwable?,message:()->String)=record(message)
    }
}
