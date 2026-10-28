package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.test.*

class SnapshotHashProcessingTest {
    @Test fun segmentedDigestMatchesIndependentOneShotAlgorithm() {
        for(size in listOf(0,1,55,56,63,64,65,129,65539)) {
            val bytes=ByteArray(size) { (it*37).toByte() }
            for(chunk in listOf(1,7,64,257)) {
                val accumulator=Sha256Accumulator()
                var n=0;while(n<bytes.size) { val end=minOf(n+chunk,bytes.size);accumulator.append(bytes.copyOfRange(n,end));n=end }
                assertEquals(sha256Hex(bytes),accumulator.finishHex(),"size=$size chunk=$chunk")
            }
        }
    }
    @Test fun framesPreserveNumericQuotedUnicodeAndLargePayloadBytes() = runTest {
        val messages=mutableListOf<String>();val old=sqliteNowLogger
        sqliteNowLogger=object:SqliteNowLogger {
            override fun e(throwable:Throwable?,message:()->String) { messages+=message() }
            override fun w(throwable:Throwable?,message:()->String) { messages+=message() }
            override fun i(throwable:Throwable?,message:()->String) { messages+=message() }
            override fun d(throwable:Throwable?,message:()->String) { messages+=message() }
        }
        try {
            val generation=SyncGeneration("1","f".repeat(64),(0..11).map { GenerationTable("main","entity_$it",listOf("id"),listOf(GenerationColumn("id","text",false),GenerationColumn("value","text",true)),emptyList()) })
            val payloads=listOf(buildJsonObject { put("value",JsonNull);put("id","k\"\\Ž🧩") },buildJsonObject { put("id","large");put("value","text\n"+"x".repeat(65539)) })
            val version=Long.MAX_VALUE;val source="source\"Ž";val id="snapshot\\🧩"
            val logical=payloads.sumOf { canonicalizeJsonElement(buildJsonArray { add("i");add(11);add(JsonArray(listOf(it.getValue("id"))));add(version);add(it) }).encodeToByteArray().size.toLong() }
            val frames=mutableListOf<ByteArray>()
            fun frame(e:JsonElement) { val b=canonicalizeJsonElement(e).encodeToByteArray();frames+=ByteArray(8){(b.size.toLong() ushr ((7-it)*8)).toByte()};frames+=b }
            frame(buildJsonArray { add("oversqlite.snapshot.v1");add(source);add("1");add(generation.fingerprint);add(id);add(version.toString());add(JsonNull) })
            for(p in payloads)frame(buildJsonArray { add("i");add("11");add(JsonArray(listOf(p.getValue("id"))));add(version.toString());add(p) })
            frame(buildJsonArray { add("end");add("2");add(logical.toString()) })
            val bytes=ByteArray(frames.sumOf { it.size });var offset=0;for(f in frames){f.copyInto(bytes,offset);offset+=f.size}
            SqliteNowDiagnostics(true).operation("hash-proof") {
                val writer=SnapshotHashWriter(source,generation,id,version,null,SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span)
                for(p in payloads)writer.add(11,JsonArray(listOf(p.getValue("id"))),version,p)
                assertEquals(sha256Hex(bytes),writer.finish(2,logical))
            }
            val records=messages.filter { it.startsWith("SQLITENOW_DIAG ") }.map { Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject }
            val family=records.single { it["kind"]?.jsonPrimitive?.content=="sql_family" && it["name"]?.jsonPrimitive?.content=="snapshot.hash.payload_canonicalization" }["family"]
            assertEquals(2L,records.single { it["kind"]?.jsonPrimitive?.content=="work_metric" && it["family"]==family }["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.long)
            val invalid=SnapshotHashWriter(source,generation,id,version,null)
            assertFails { invalid.add(11,JsonArray(listOf(JsonPrimitive("a"))),1,buildJsonObject { put("id","a");put("value",42) }) }
            assertFails { invalid.add(11,JsonArray(listOf(JsonPrimitive("a"))),1,buildJsonObject { put("id","a");put("value","\uD800") }) }
        } finally { sqliteNowLogger=old }
    }
}
