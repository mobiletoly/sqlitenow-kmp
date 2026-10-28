package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.*

@Serializable
internal data class TerminalPushOutcome(
    @SerialName("source_id") val sourceId: String,
    @SerialName("schema_generation_id") val generationId: String,
    @SerialName("schema_generation_fingerprint") val generationFingerprint: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("canonical_request_hash") val canonicalRequestHash: String,
    @SerialName("initialization_id") val initializationId: String?,
    val status: String,
    @SerialName("outcome_hash") val outcomeHash: String,
    val committed: CommittedPushEffect? = null,
    val rejection: PushOutcomeRejection? = null,
)
@Serializable
internal data class CommittedPushEffect(
    @SerialName("bundle_seq") val bundleSeq: Long,
    @SerialName("bundle_hash") val bundleHash: String,
    @SerialName("row_count") val rowCount: Long,
    val rows: JsonElement,
)
@Serializable
internal data class PushOutcomeRejection(val code: String, val conflict: PushConflictDetails? = null)
@Serializable
internal data class PushOutcomeResolveRequest(
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("canonical_request_hash") val canonicalRequestHash: String,
    @SerialName("initialization_id") val initializationId: String?,
)
@Serializable
internal data class PushOutcomeAckRequest(
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("canonical_request_hash") val canonicalRequestHash: String,
    @SerialName("outcome_hash") val outcomeHash: String,
)
@Serializable
data class SnapshotRecovery(@SerialName("minimum_bundle_seq") val minimumBundleSeq: Long)

private val recoveryJson = Json { ignoreUnknownKeys = false; explicitNulls = true; encodeDefaults = true }
private fun recoveryObject(raw: String): JsonObject {
    requireUniqueSnapshotJsonObjectMembers(raw)
    return recoveryJson.parseToJsonElement(raw).jsonObject
}
private fun JsonObject.closed(required: Set<String>, optional: Set<String> = emptySet(), nullable: String = "") {
    require(keys.containsAll(required) && keys.all { it in required || it in optional }) { "invalid closed object" }
    for ((name, value) in this) require(name == nullable || value != JsonNull) { "null member $name" }
}
private fun JsonObject.integer(name: String, minimum: Long = 0): Long {
    val token = getValue(name).jsonPrimitive
    require(!token.isString && token.content.matches(Regex("0|[1-9][0-9]*"))) { "invalid integer $name" }
    return token.content.toLong().also { require(it >= minimum) }
}
private fun JsonObject.string(name: String): String = getValue(name).jsonPrimitive.let {
    require(it.isString); it.content
}
private fun JsonObject.operationTuple(nullableInitialization: Boolean) {
    integer("source_bundle_id", 1)
    require(isCanonicalSha256(string("canonical_request_hash")))
    if (nullableInitialization && getValue("initialization_id") != JsonNull) {
        val token = string("initialization_id")
        require(token.matches(Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")))
    }
}
internal fun decodeOutcomeResolve(raw: String): PushOutcomeResolveRequest {
    val root = recoveryObject(raw)
    root.closed(setOf("source_bundle_id", "canonical_request_hash", "initialization_id"), nullable = "initialization_id")
    root.operationTuple(true)
    return recoveryJson.decodeFromString(raw)
}
internal fun decodeOutcomeAck(raw: String): PushOutcomeAckRequest {
    val root = recoveryObject(raw)
    root.closed(setOf("source_bundle_id", "canonical_request_hash", "outcome_hash"))
    root.operationTuple(false); require(isCanonicalSha256(root.string("outcome_hash")))
    return recoveryJson.decodeFromString(raw)
}
internal fun decodeRecoverySnapshotRequest(raw: String): SnapshotSessionCreateRequest {
    val root = recoveryObject(raw)
    root.closed(emptySet(), setOf("source_replacement", "recovery"))
    require(!(root.containsKey("recovery") && root.containsKey("source_replacement")))
    root["recovery"]?.jsonObject?.let { it.closed(setOf("minimum_bundle_seq")); it.integer("minimum_bundle_seq") }
    return recoveryJson.decodeFromString(raw)
}
internal fun decodePushOutcome(raw: String): TerminalPushOutcome {
    val root = recoveryObject(raw)
    root.closed(setOf("source_id", "schema_generation_id", "schema_generation_fingerprint", "source_bundle_id",
        "canonical_request_hash", "initialization_id", "status", "outcome_hash"), setOf("committed", "rejection"), "initialization_id")
    root.operationTuple(true)
    requireValidOversqliteSourceId(root.string("source_id"))
    require(root.string("schema_generation_id").matches(Regex("[1-9][0-9]*")))
    root.string("schema_generation_id").toLong()
    require(isCanonicalSha256(root.string("schema_generation_fingerprint")) && isCanonicalSha256(root.string("outcome_hash")))
    when (root.string("status")) {
        "committed" -> {
            require(root.containsKey("committed") && !root.containsKey("rejection"))
            val effect = root.getValue("committed").jsonObject
            effect.closed(setOf("bundle_seq", "bundle_hash", "row_count", "rows"))
            effect.integer("bundle_seq", 1); effect.integer("row_count", 1)
            require(isCanonicalSha256(effect.string("bundle_hash")))
        }
        "rejected" -> { require(root.containsKey("rejection") && !root.containsKey("committed")); validateOutcomeRejection(root.getValue("rejection").jsonObject) }
        "cancelled" -> require(!root.containsKey("committed") && !root.containsKey("rejection"))
        else -> error("invalid outcome status")
    }
    return recoveryJson.decodeFromString(raw)
}
private fun validateOutcomeRejection(rejection: JsonObject) {
    rejection.closed(setOf("code"), setOf("conflict"))
    val code = rejection.string("code")
    require(code.matches(Regex("[a-z][a-z0-9_]{0,63}")))
    require((code == "push_conflict") == rejection.containsKey("conflict"))
    rejection["conflict"]?.jsonObject?.let { conflict ->
        conflict.closed(setOf("schema", "table", "key", "op", "base_row_version", "server_row_version", "server_row_deleted", "server_row"), nullable = "server_row")
        conflict.integer("base_row_version"); conflict.integer("server_row_version")
        require(conflict.string("schema").isNotEmpty() && conflict.string("table").isNotEmpty())
        require(conflict.getValue("key").jsonObject.isNotEmpty())
        require(conflict.string("op") in setOf("i", "u", "d"))
        require(conflict.getValue("server_row_deleted").jsonPrimitive.let { !it.isString && it.booleanOrNull != null })
        require(conflict.getValue("server_row") == JsonNull || conflict.getValue("server_row") is JsonObject)
    }
}
internal fun hashPushOutcome(outcome: TerminalPushOutcome, generation: SyncGeneration): String {
    require(outcome.generationId == generation.id && outcome.generationFingerprint == generation.fingerprint)
    val committed = outcome.committed?.let { effect ->
        val rows = OversqliteCompactCodec.decodeBundleRows(generation, effect.rows)
        require(rows.size.toLong() == effect.rowCount && rows.all { it.rowVersion == effect.bundleSeq })
        require(canonicalCommittedBundleHash(rows) == effect.bundleHash)
        val logical = rows.map { row ->
            validateOutcomeWireRow(generation.tables.single { it.schema == row.schema && it.table == row.table },row.key,row.payload)
            JsonArray(listOf(JsonPrimitive(row.schema), JsonPrimitive(row.table), JsonObject(row.key.mapValues { JsonPrimitive(it.value) }),
                JsonPrimitive(mapOf("INSERT" to "I", "UPDATE" to "U", "DELETE" to "D").getValue(row.op)), JsonPrimitive(row.rowVersion.toString()), row.payload ?: JsonNull))
        }
        JsonArray(listOf(JsonPrimitive(effect.bundleSeq.toString()), JsonPrimitive(effect.bundleHash), JsonPrimitive(effect.rowCount.toString()), JsonArray(logical)))
    } ?: JsonNull
    val rejection = outcome.rejection?.let { rejection ->
        val conflict = rejection.conflict?.let { c ->
            val obj = recoveryJson.encodeToJsonElement(c).jsonObject.toMutableMap()
            obj["base_row_version"] = JsonPrimitive(c.baseRowVersion.toString())
            obj["server_row_version"] = JsonPrimitive(c.serverRowVersion.toString())
            JsonObject(obj)
        } ?: JsonNull
        JsonArray(listOf(JsonPrimitive(rejection.code), conflict))
    } ?: JsonNull
    val logical = JsonArray(listOf(JsonPrimitive("oversqlite.push-outcome.v1"), JsonPrimitive(outcome.sourceId), JsonPrimitive(outcome.generationId),
        JsonPrimitive(outcome.generationFingerprint), JsonPrimitive(outcome.sourceBundleId.toString()), JsonPrimitive(outcome.canonicalRequestHash),
        outcome.initializationId?.let(::JsonPrimitive) ?: JsonNull, JsonPrimitive(outcome.status), committed, rejection))
    return sha256Hex(canonicalizeJsonElement(logical).encodeToByteArray())
}

/** Streams sealed frames; network chunk boundaries do not enter the hash. */
internal class SnapshotHashWriter(source: String, private val generation: SyncGeneration, snapshot: String, boundary: Long, recovery: SnapshotRecovery?,
    private val diagnosticSpan: dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan? = null) {
    private val digest = newSnapshotDigest()
    private var finished = false
    private var rows = 0L
    private var logicalBytes = 0L
    init {
        require(source.isNotEmpty() && snapshot.isNotEmpty() && boundary >= 0 && (recovery == null || recovery.minimumBundleSeq >= 0 && boundary >= recovery.minimumBundleSeq))
        val echo = recovery?.let { buildJsonObject { put("minimum_bundle_seq", JsonPrimitive(it.minimumBundleSeq.toString())) } } ?: JsonNull
        frame(JsonArray(listOf(JsonPrimitive("oversqlite.snapshot.v1"), JsonPrimitive(source), JsonPrimitive(generation.id), JsonPrimitive(generation.fingerprint), JsonPrimitive(snapshot), JsonPrimitive(boundary.toString()), echo)))
    }
    private fun frame(value: JsonElement) {
        check(!finished)
        val bytes = canonicalizeJsonElement(value).encodeToByteArray()
        val size = bytes.size.toLong()
        digest.append(ByteArray(8) { index -> (size ushr ((7-index)*8)).toByte() })
        digest.append(bytes)
    }
    fun add(tableIndex: Int, key: JsonArray, version: Long, payload: JsonObject) {
        require(tableIndex in generation.tables.indices && version > 0)
        val table = generation.tables[tableIndex]
        require(key.size == table.syncKeyColumns.size)
        val wireKey = table.syncKeyColumns.zip(key).associate { (name,value) -> name to value.jsonPrimitive.let { require(it.isString); it.content } }
        measured("snapshot.hash.wire_validation") { validateOutcomeWireRow(table,wireKey,payload) }
        val canonicalPayload = measured("snapshot.hash.payload_canonicalization") { canonicalizeJsonElement(payload) }
        val payloadBytes = measured("snapshot.hash.byte_encoding") { canonicalPayload.encodeToByteArray() }
        val canonicalKey = canonicalizeJsonElement(key)
        fun prefix(index: JsonPrimitive, versionValue: JsonPrimitive): ByteArray =
            ("[\"i\"," + canonicalizeJsonElement(index) + "," + canonicalKey + "," + canonicalizeJsonElement(versionValue) + ",").encodeToByteArray()
        val numericPrefix = prefix(JsonPrimitive(tableIndex), JsonPrimitive(version))
        val wirePrefix = prefix(JsonPrimitive(tableIndex.toString()), JsonPrimitive(version.toString()))
        val logical = numericPrefix.size.toLong() + payloadBytes.size + 1
        require(rows < Long.MAX_VALUE && logicalBytes <= Long.MAX_VALUE - logical)
        check(!finished)
        measured("snapshot.hash.digest_update") {
            val size = wirePrefix.size.toLong() + payloadBytes.size + 1
            digest.append(ByteArray(8) { index -> (size ushr ((7-index)*8)).toByte() })
            digest.append(wirePrefix); digest.append(payloadBytes); digest.appendAscii(']')
        }
        rows++; logicalBytes += logical
    }
    private inline fun <T> measured(name: String, block: () -> T): T {
        val span = diagnosticSpan ?: return block()
        val began = span.nowNanos(); var failed = false
        try { return block() } catch (failure: Throwable) { failed = true; throw failure }
        finally { span.aggregateWork(name, span.nowNanos() - began, failed) }
    }
    fun finish(rowCount: Long, logicalByteCount: Long): String {
        require(rowCount == rows && logicalByteCount == logicalBytes && rowCount >= 0 && logicalByteCount >= 0)
        frame(JsonArray(listOf(JsonPrimitive("end"), JsonPrimitive(rowCount.toString()), JsonPrimitive(logicalByteCount.toString()))))
        finished = true
        return digest.finishHex()
    }
}

internal fun decodeRecoverySnapshotMetadata(raw: String, requested: SnapshotRecovery?): SnapshotSession {
    val root = recoveryObject(raw)
    root.closed(setOf("snapshot_id", "snapshot_bundle_seq", "row_count", "logical_byte_count", "expires_at", "snapshot_hash"), setOf("recovery"))
    require(root.string("snapshot_id").isNotEmpty() && isCanonicalSha256(root.string("snapshot_hash")))
    val boundary = root.integer("snapshot_bundle_seq")
    root.integer("row_count"); root.integer("logical_byte_count")
    root["recovery"]?.jsonObject?.let { it.closed(setOf("minimum_bundle_seq")); it.integer("minimum_bundle_seq") }
    require(root.containsKey("recovery") == (requested != null))
    if (requested != null) require(root.getValue("recovery").jsonObject.integer("minimum_bundle_seq") == requested.minimumBundleSeq && boundary >= requested.minimumBundleSeq)
    return recoveryJson.decodeFromString(raw)
}

/** The caller separately guards authenticated scope, authority and attachment. */
@Serializable
internal data class PushOperationBinding(
    @SerialName("source_id") val sourceId: String,
    @SerialName("schema_generation_id") val generationId: String,
    @SerialName("schema_generation_fingerprint") val generationFingerprint: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("canonical_request_hash") val canonicalRequestHash: String,
    @SerialName("initialization_id") val initializationId: String?,
)
internal fun validatePushOutcomeBinding(outcome: TerminalPushOutcome, binding: PushOperationBinding, generation: SyncGeneration) {
    require(outcome.sourceId == binding.sourceId && outcome.generationId == binding.generationId && outcome.generationFingerprint == binding.generationFingerprint &&
        outcome.sourceBundleId == binding.sourceBundleId && outcome.canonicalRequestHash == binding.canonicalRequestHash && outcome.initializationId == binding.initializationId)
    require(outcome.outcomeHash == hashPushOutcome(outcome, generation))
}

private fun validateOutcomeWireRow(table: GenerationTable,key: SyncKey,payload: JsonElement?) {
    for(name in table.syncKeyColumns) {
        val column=table.columns.single { it.name==name }
        validateOutcomeWireValue(column,JsonPrimitive(key.getValue(name)),true)
    }
    if(payload==null||payload==JsonNull)return
    val objectPayload=payload.jsonObject
    require(objectPayload.size==table.columns.size && table.columns.all { it.name in objectPayload })
    for(column in table.columns)validateOutcomeWireValue(column,objectPayload.getValue(column.name),false)
    require(table.syncKeyColumns.all { objectPayload[it]==JsonPrimitive(key.getValue(it)) })
}
private fun validateOutcomeWireValue(column: GenerationColumn,value: JsonElement,key: Boolean) {
    // The existing value codec owns canonical wire parsing. Physical storage is not
    // chosen here; TEXT selects its storage-independent canonical wire checks.
    OversqliteValueCodec.decodeGenerationWireValue(column,value,key)
}
