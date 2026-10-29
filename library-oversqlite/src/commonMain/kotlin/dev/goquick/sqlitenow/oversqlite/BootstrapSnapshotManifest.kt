package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive

/** Expected evidence has one durable owner, the bootstrap singleton. */
internal data class BootstrapTableExpectation(val rows: Long = 0, val projection: String = "") {
    init { require(rows >= 0 && (if (rows == 0L) projection.isEmpty() else isCanonicalSha256(projection))) }

    fun append(identity: BootstrapProjectionIdentity, binding: SnapshotTableBinding,
               page: Long, count: Long, receivedPageProjection: String): BootstrapTableExpectation {
        require(page > 0 && count > 0 && isCanonicalSha256(receivedPageProjection))
        return BootstrapTableExpectation(bootstrapCheckedAdd(rows, count), bootstrapFramedHash(
            bootstrapCanonicalStrings(listOf("oversqlite.bootstrap.table.pages.v1", identity.snapshotId,
                identity.descriptorSignature, binding.slot.toString(), binding.tableIndex.toString(),
                projection, page.toString(), count.toString(), receivedPageProjection)).encodeToByteArray()))
    }
}

internal data class BootstrapSnapshotManifest(
    val projection: String,
    val chain: String,
    val tables: List<BootstrapTableExpectation>,
    val pageCount: Long, val receivedRows: Long, val receivedBytes: Long,
) {
    fun canonical(identity: BootstrapProjectionIdentity, operation: OversqliteOperationState): String {
        require(operation.snapshotEngine == typedSnapshotEngine && operation.stagedSnapshotId == identity.snapshotId)
        require((isCanonicalSha256(projection) || projection.isEmpty() && operation.bootstrapCleanupChain.isNotEmpty()) && isCanonicalSha256(chain))
        require(receivedRows in 0..operation.snapshotRowCount && receivedBytes in 0..operation.snapshotByteCount &&
            pageCount in 0..receivedRows && (receivedRows == 0L || pageCount > 0))
        require(tables.fold(0L) { n, table -> bootstrapCheckedAdd(n, table.rows) } == receivedRows)
        if (operation.bootstrapCleanupChain.isEmpty()) require(pageCount == operation.bootstrapPageCount &&
            receivedRows == operation.snapshotStagedRowCount && receivedBytes == operation.snapshotStagedByteCount &&
            receivedRows == operation.snapshotRowCount && receivedBytes == operation.snapshotByteCount)
        else require(operation.bootstrapCleanupOrdinal == receivedRows - operation.snapshotStagedRowCount &&
            operation.bootstrapPageCount <= pageCount && operation.snapshotStagedByteCount <= receivedBytes)
        return canonicalizeJsonElement(JsonArray(listOf(
            JsonPrimitive("oversqlite.bootstrap.seal.v1"), JsonPrimitive(bootstrapRetainedFormat),
            JsonPrimitive(identity.header("oversqlite.bootstrap.identity.v1")),
            JsonPrimitive(operation.snapshotStageGuard), JsonPrimitive(sha256Hex(operation.snapshotTransfer.encodeToByteArray())),
            JsonPrimitive(operation.snapshotRowCount.toString()), JsonPrimitive(operation.snapshotByteCount.toString()),
            JsonPrimitive(pageCount.toString()), JsonPrimitive(projection), JsonPrimitive(chain),
            JsonArray(tables.map { JsonArray(listOf(JsonPrimitive(it.rows.toString()), JsonPrimitive(it.projection))) }),
            JsonPrimitive(receivedRows.toString()), JsonPrimitive(receivedBytes.toString()),
        )))
    }

    companion object {
        /** Parsing never establishes expected values: exact reconstruction must match the bound seal. */
        fun read(value: String, identity: BootstrapProjectionIdentity, operation: OversqliteOperationState,
                 tableCount: Int): BootstrapSnapshotManifest {
            val array = Json.parseToJsonElement(value) as? JsonArray ?: error("invalid bootstrap manifest")
            require(array.size == 13)
            fun text(index: Int): String = (array[index] as? JsonPrimitive)?.let {
                require(it.isString); it.content
            } ?: error("invalid bootstrap manifest member")
            fun number(index: Int): Long = text(index).let { it.toLong().also { n -> require(n.toString() == it) } }
            val tableArray = array[10] as? JsonArray ?: error("invalid bootstrap table manifest")
            require(tableArray.size == tableCount)
            val manifest = BootstrapSnapshotManifest(text(8), text(9), tableArray.map { item ->
                val pair = item as? JsonArray ?: error("invalid bootstrap table expectation")
                require(pair.size == 2)
                val row = pair[0] as? JsonPrimitive ?: error("invalid bootstrap table count")
                val proof = pair[1] as? JsonPrimitive ?: error("invalid bootstrap table proof")
                require(row.isString && proof.isString)
                val count = row.content.toLong(); require(count.toString() == row.content)
                BootstrapTableExpectation(count, proof.content)
            }, number(7), number(11), number(12))
            require(manifest.canonical(identity, operation) == value) { "bootstrap manifest binding differs" }
            return manifest
        }
    }
}

internal fun bootstrapOperationReference(
    identity: BootstrapProjectionIdentity, operation: OversqliteOperationState, phase: String,
    chain: String, manifest: String, tables: List<BootstrapTableExpectation>,
): String = bootstrapFramedHash(bootstrapCanonicalStrings(listOf(
    "oversqlite.bootstrap.operation.v1", bootstrapRetainedFormat, typedSnapshotEngine,
    identity.header("oversqlite.bootstrap.identity.v1"), phase, operation.kind, operation.targetUserId,
    operation.reason, operation.replacementSourceId, operation.snapshotStageGuard,
    sha256Hex(operation.snapshotTransfer.encodeToByteArray()), operation.snapshotRowCount.toString(),
    operation.snapshotByteCount.toString(), operation.snapshotStagedRowCount.toString(),
    operation.snapshotStagedByteCount.toString(), operation.bootstrapPageCount.toString(),
    operation.bootstrapCleanupOrdinal.toString(), operation.bootstrapCleanupChain,
    if (operation.snapshotStageComplete) "complete" else "partial", chain,
    sha256Hex(manifest.encodeToByteArray()), bootstrapCanonicalStrings(tables.flatMap { listOf(it.rows.toString(), it.projection) }),
)).encodeToByteArray())
