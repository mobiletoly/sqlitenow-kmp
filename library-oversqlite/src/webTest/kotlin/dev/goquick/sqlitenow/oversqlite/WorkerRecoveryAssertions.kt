package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.*
import kotlin.test.*

/** Lost committed responses are resolved once; resumable apply uses the retained certificate. */
internal fun assertWorkerResolveIdentity(original: String, resolves: List<PushOutcomeResolveRequest>) {
    val request = Json.decodeFromString<CompletePushRequest>(original)
    assertEquals(listOf(PushOutcomeResolveRequest(request.sourceBundleId, request.canonicalRequestHash,
        request.initializationId)), resolves, "resolve binds the exact original operation without resending it")
}

internal suspend fun assertWorkerUploadDisposition(
    db: SafeSQLiteConnection,
    original: String,
    generation: SyncGeneration,
    settled: Boolean,
) {
    val journal = loadUploadJournal(db)
    if (settled) {
        assertNull(journal, "successful settlement and ack release the upload certificate")
        return
    }
    val retained = assertNotNull(journal)
    assertEquals("frozen", retained.phase)
    assertEquals(sha256Hex(original.encodeToByteArray()), retained.requestDigest)
    val bodies = db.queryList("SELECT CAST(request_body AS TEXT),CAST(outcome_body AS TEXT) FROM _sync_upload_journal") {
        it.getText(0) to it.getText(1)
    }.single()
    assertEquals(original, bodies.first, "original F bytes survive failed or pending settlement")
    val request = Json.decodeFromString<CompletePushRequest>(original)
    val outcome = decodePushOutcome(bodies.second)
    val operation = retained.binding.operation
    assertEquals("committed", outcome.status)
    assertEquals(operation.sourceId, outcome.sourceId)
    assertEquals(generation.id, outcome.generationId)
    assertEquals(generation.fingerprint, outcome.generationFingerprint)
    assertEquals(request.sourceBundleId, outcome.sourceBundleId)
    assertEquals(request.canonicalRequestHash, outcome.canonicalRequestHash)
    assertEquals(request.initializationId, outcome.initializationId)
    assertEquals(outcome.outcomeHash, retained.outcomeHash)
    assertEquals(outcome.outcomeHash, hashPushOutcome(outcome, generation), "complete retained C hash and typed effects")
}

internal fun assertWorkerAckDisposition(original: String, acknowledgements: List<PushOutcomeAckRequest>, settled: Boolean) {
    assertEquals(if (settled) 2 else 1, acknowledgements.size,
        "one seed ack; original operation must not be acked until local settlement commits")
    if (settled) {
        val request = Json.decodeFromString<CompletePushRequest>(original)
        val ack = acknowledgements.last()
        assertEquals(request.sourceBundleId, ack.sourceBundleId)
        assertEquals(request.canonicalRequestHash, ack.canonicalRequestHash)
        assertTrue(isCanonicalSha256(ack.outcomeHash)) // Mock authority also verifies equality with its retained C.
    }
}


private const val receiptSqlReference = "__c06_sql_dictionary_index"

/** Lossless log encoding only. All execution/assertions keep the original values. */
internal fun encodeWorkerQualificationReceipt(receipt: JsonObject): String {
    val dictionary = mutableListOf<String>()
    val compact = transformWorkerReceipt(receipt, dictionary, decode = false)
    assertEquals(receipt, transformWorkerReceipt(compact, dictionary, decode = true),
        "qualification log must reconstruct the entire original receipt")
    return buildJsonObject {
        put("type", "oversqlite.worker-receipt.sql-dictionary.v1")
        put("original_canonical_sha256", sha256Hex(canonicalizeJsonElement(receipt).encodeToByteArray()))
        put("sql_dictionary", JsonArray(dictionary.map(::JsonPrimitive)))
        put("receipt", compact)
    }.toString()
}

private fun transformWorkerReceipt(value: JsonElement, dictionary: MutableList<String>, decode: Boolean): JsonElement = when (value) {
    is JsonArray -> JsonArray(value.map { transformWorkerReceipt(it, dictionary, decode) })
    is JsonObject -> {
        if (decode && value.keys == setOf(receiptSqlReference)) {
            val index = value.getValue(receiptSqlReference).jsonPrimitive.int
            assertTrue(index in dictionary.indices, "qualification SQL dictionary reference is valid")
            JsonPrimitive(dictionary[index])
        } else {
            assertTrue(decode || receiptSqlReference !in value, "original receipt has no reserved encoding key")
            JsonObject(value.mapValues { (key, child) ->
                if (!decode && key == "sql" && child is JsonPrimitive && child.isString) {
                    val sql = child.content
                    var index = dictionary.indexOf(sql)
                    if (index < 0) { index = dictionary.size; dictionary.add(sql) }
                    buildJsonObject { put(receiptSqlReference, index) }
                } else transformWorkerReceipt(child, dictionary, decode)
            })
        }
    }
    else -> value
}
