package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import java.security.MessageDigest
import kotlin.test.*

/** Independently authored neutral schema and data. No application schema or payload is imported. */
internal class RichSnapshotRestoreFixture(val smallRows: Int? = null) {
    private val populations = listOf(84,84,10,10,9,26,1344,1,228,228,3,3,1,3)
    private val widths = listOf(44,16,13,3,3,8,13,12,22,4,4,6,2,5)
    private val textCounts = listOf(28,8,12,2,3,8,8,9,13,2,3,5,2,4)
    private val fkCounts = listOf(2,2,1,1,0,2,3,0,1,1,1,1,0,0)
    private val json = Json { encodeDefaults = true }
    val names = (1..14).map { "entity_" + it.toString().padStart(2,'0') }
    private val counts = if (smallRows == null) populations else MutableList(14) { 1 }.also { it[6] += smallRows-14 }
    private fun target(table: Int, fk: Int) = if (table==6 && fk==0) 6 else if (fk==1) 4 else 12
    private fun field(n: Int) = "field_" + n.toString().padStart(2,'0')
    val generation = SyncGeneration("1", "", names.mapIndexed { i, name ->
        GenerationTable("main",name,listOf("id"), (0 until widths[i]).map { n ->
            GenerationColumn(if(n==0) "id" else field(n), when {
                n<textCounts[i] -> "text"
                i==13 -> "float64"
                n%3==0 -> "boolean"
                else -> "int64"
            }, n in 1..fkCounts[i] || n>fkCounts[i] && n%5==0)
        }, (0 until fkCounts[i]).map { fk -> GenerationForeignKey(listOf(field(fk+1)),"main",names[target(i,fk)],listOf("id"),"no_action","no_action") })
    }).let { it.copy(fingerprint=independentGenerationFingerprint(it)) }
    val ddl = generation.tables.flatMapIndexed { i,t ->
        listOf("CREATE TABLE ${t.table}(" + t.columns.mapIndexed { n,c ->
            c.name+" "+when(c.wireType){"text"->"TEXT";"float64"->"REAL";else->"INTEGER"}+
                (if(n==0) " PRIMARY KEY NOT NULL" else if(!c.nullable) " NOT NULL" else "")+
                (if(c.wireType=="boolean") " CHECK(${c.name} IN (0,1))" else "")
        }.joinToString(",") + t.foreignKeys.joinToString("") { ",FOREIGN KEY(${it.columns.single()}) REFERENCES ${it.referencedTable}(id)" } + ") WITHOUT ROWID") +
            (1..fkCounts[i]).map { "CREATE INDEX ${t.table}_reference_$it ON ${t.table}(${field(it)})" } +
            listOf("CREATE INDEX ${t.table}_value_idx ON ${t.table}(${field(widths[i]-1)})")
    }
    data class Row(val table: Int, val payload: JsonObject)
    private val initial = generation.tables.flatMapIndexed { i,t -> (0 until counts[i]).map { n ->
        Row(i,buildJsonObject { t.columns.forEachIndexed { c,col -> put(col.name,when {
            c==0 -> JsonPrimitive("key-$i-$n")
            c<=fkCounts[i] -> if(n%7==0 || i==6 && c==1 && n==0) JsonNull else JsonPrimitive("key-${target(i,c-1)}-${if(i==6 && c==1) n-1 else 0}")
            col.nullable && n%11==0 -> JsonNull
            col.wireType=="text" -> JsonPrimitive(if(c==fkCounts[i]+1) "{\"n\":$n,\"text\":\"Ž🧩\"}" else "value-$c-${n%19}")
            col.wireType=="boolean" -> JsonPrimitive(n%2==0)
            col.wireType=="float64" -> JsonPrimitive("0.5")
            else -> JsonPrimitive((n.toLong()*17-c).toString())
        }) } })
    } }
    private fun logical(row: Row) = independentCanonical(buildJsonArray { add("i");add(row.table);add(JsonArray(listOf(row.payload.getValue("id"))));add(1);add(row.payload) }).toByteArray().size.toLong()
    private val baseBytes = initial.sumOf(::logical)
    private val targetBytes = maxOf(baseBytes, 1_723_170L*initial.size/2034)
    private val paddedCount = initial.count { textCounts[it.table]>fkCounts[it.table]+1 }
    private var paddedOrdinal = 0
    val rows = initial.map { row ->
        val col=field(fkCounts[row.table]+1)
        if(generation.tables[row.table].columns.first { it.name==col }.wireType=="text") {
            val extra=(targetBytes-baseBytes)/paddedCount + if(paddedOrdinal++<(targetBytes-baseBytes)%paddedCount) 1 else 0
            row.copy(payload=JsonObject(row.payload+(col to JsonPrimitive(row.payload.getValue(col).jsonPrimitive.content+"x".repeat(extra.toInt())))))
        } else row
    }
    val logicalBytes = rows.sumOf(::logical)
    val schemaHash = independentSha(ddl.joinToString("\n").toByteArray())
    val dataHash = independentSha(independentCanonical(JsonArray(rows.map { buildJsonArray { add(it.table);add(it.payload) } })).toByteArray())
    val requests = mutableListOf<String>()
    private val discovery = json.encodeToString(GenerationDiscovery("v1",generation,testWireLimits(),false))
    private val chunks = (0 until rows.size step 1000).associateWith { after ->
        val selected=rows.drop(after).take(1000);var ordinal=after+1
        buildJsonObject {
            put("rows",JsonArray(selected.groupBy { it.table }.map { (table,rs) ->
                buildJsonArray { add(table);add(ordinal);add(JsonArray(rs.map { row -> buildJsonArray { add("i");add(JsonArray(listOf(row.payload.getValue("id"))));add(1);add(row.payload) } })) }.also { ordinal+=rs.size }
            }));put("next_row_ordinal",after+selected.size);put("has_more",after+selected.size<rows.size);put("logical_byte_count",selected.sumOf(::logical))
        }.toString()
    }
    private var sessionBody: String? = null
    fun bindSource(source: String) {
        val digest=MessageDigest.getInstance("SHA-256")
        fun frame(value: JsonElement) { val b=independentCanonical(value).toByteArray();digest.update(ByteArray(8) { (b.size.toLong() ushr ((7-it)*8)).toByte() });digest.update(b) }
        frame(buildJsonArray { add("oversqlite.snapshot.v1");add(source);add("1");add(generation.fingerprint);add("rich-snapshot");add("1");add(JsonNull) })
        for(row in rows) frame(buildJsonArray { add("i");add(row.table.toString());add(JsonArray(listOf(row.payload.getValue("id"))));add("1");add(row.payload) })
        frame(buildJsonArray { add("end");add(rows.size.toString());add(logicalBytes.toString()) })
        sessionBody=buildJsonObject { put("snapshot_id","rich-snapshot");put("snapshot_bundle_seq",1);put("row_count",rows.size);put("logical_byte_count",logicalBytes);put("expires_at","2099-01-01T00:00:00Z");put("snapshot_hash",digest.digest().joinToString("") { "%02x".format(it) }) }.toString()
    }
    fun client() = HttpClient(MockEngine { request ->
        val path=request.url.encodedPath;requests+="${request.method.value} $path"
        val body=when {
            path=="/sync/generations/1" -> discovery
            path=="/sync/connect" -> "{\"resolution\":\"remote_authoritative\"}"
            path=="/sync/snapshot-sessions" -> checkNotNull(sessionBody)
            request.method==HttpMethod.Delete && path=="/sync/snapshot-sessions/rich-snapshot" -> "{}"
            path=="/sync/snapshot-sessions/rich-snapshot" -> chunks.getValue(request.url.parameters["after_row_ordinal"]!!.toInt())
            else -> error("unexpected fixture request")
        }
        respond(body,HttpStatusCode.OK,headersOf(HttpHeaders.ContentType,"application/json"))
    })
    suspend fun assertData(db: SafeSQLiteConnection) {
        for((i,table) in generation.tables.withIndex()) {
            val expected=rows.filter { it.table==i }.associateBy { it.payload.getValue("id").jsonPrimitive.content }
            var count=0
            db.withPreparedStatement("SELECT ${table.columns.joinToString(",") { it.name }} FROM ${table.table}") { st ->
                while(st.step()) {
                    val row=expected.getValue(st.getText(0)).payload
                    table.columns.forEachIndexed { n,col ->
                        val wanted=row.getValue(col.name)
                        if(wanted==JsonNull) assertTrue(st.isNull(n)) else when(col.wireType) {
                            "boolean" -> assertEquals(if(wanted.jsonPrimitive.boolean)1L else 0L,st.getLong(n))
                            "int64" -> assertEquals(wanted.jsonPrimitive.long,st.getLong(n))
                            "float64" -> assertEquals(wanted.jsonPrimitive.double,st.getDouble(n))
                            else -> assertEquals(wanted.jsonPrimitive.content,st.getText(n))
                        }
                    };count++
                }
            };assertEquals(expected.size,count)
        }
        assertEquals(0L,db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
        assertEquals(rows.size.toLong(),db.scalarLong("SELECT count(*) FROM _sync_row_state"))
        assertEquals(1L,db.scalarLong("SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        assertEquals(1L,db.scalarLong("SELECT count(*) FROM fixture_settings WHERE id=1 AND value='preserved'"))
        for(t in listOf("_sync_apply_rows","_sync_snapshot_stage","_sync_dirty_rows","_sync_outbox_rows"))assertEquals(0L,db.scalarLong("SELECT count(*) FROM $t"))
        db.withPreparedStatement("PRAGMA integrity_check") { check(it.step());assertEquals("ok",it.getText(0)) }
    }
}

internal fun independentSha(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
internal fun independentCanonical(value: JsonElement): String = when(value) {
    is JsonObject -> value.entries.sortedBy { it.key }.joinToString(",","{","}") { JsonPrimitive(it.key).toString()+":"+independentCanonical(it.value) }
    is JsonArray -> value.joinToString(",","[","]",transform=::independentCanonical)
    else -> value.toString()
}
private fun independentGenerationFingerprint(g: SyncGeneration): String = independentSha(independentCanonical(buildJsonArray {
    add("oversync.schema-generation.v1");add(g.id);add(JsonArray(g.tables.sortedBy { it.table }.map { t -> buildJsonArray {
        add(t.schema);add(t.table);add(JsonArray(t.syncKeyColumns.map(::JsonPrimitive)))
        add(JsonArray(t.columns.sortedBy { it.name }.map { c -> buildJsonArray { add(c.name);add(c.wireType);add(c.nullable) } }))
        add(JsonArray(t.foreignKeys.sortedBy { it.columns.single() }.map { f -> buildJsonArray { add(JsonArray(f.columns.map(::JsonPrimitive)));add(f.referencedSchema);add(f.referencedTable);add(JsonArray(f.referencedColumns.map(::JsonPrimitive)));add(f.onDelete);add(f.onUpdate) } }))
    } }))
}).toByteArray())
