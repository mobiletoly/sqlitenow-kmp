/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SnapshotPhase3ProtocolTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun snapshotConfigDefaultsAndPositiveValidationAreLocked() {
        val config = config()
        assertEquals(1000, config.snapshotChunkRows)
        assertEquals(4L * 1024L * 1024L, config.snapshotChunkBytes)
        assertEquals(256, config.snapshotApplyBatchRows)
        assertEquals(4L * 1024L * 1024L, config.snapshotApplyBatchBytes)

        assertFailsWith<IllegalArgumentException> { config(snapshotChunkRows = 0) }
        assertFailsWith<IllegalArgumentException> { config(snapshotChunkBytes = 0) }
        assertFailsWith<IllegalArgumentException> { config(snapshotApplyBatchRows = 0) }
        assertFailsWith<IllegalArgumentException> { config(snapshotApplyBatchBytes = 0) }
    }

    @Test
    fun capabilitiesRequireClientLimitsButAllowOmittedServerMaterializationHints() {
        val valid = json.encodeToString(
            CapabilitiesResponse.serializer(),
            CapabilitiesResponse(
                protocolVersion = "v1",
                schemaVersion = 1,
                features = emptyMap(),
                bundleLimits = testBundleCapabilitiesLimits(),
            ),
        )
        val decoded = json.decodeFromString(CapabilitiesResponse.serializer(), valid)
        assertEquals(8, decoded.bundleLimits.maxConcurrentSnapshotChunkRequests)

        val aliasOnly = valid
            .replace(
                "\"max_concurrent_snapshot_chunk_requests\":8",
                "\"max_concurrent_snapshot_chunks\":8",
            )
        assertFailsWith<SerializationException> {
            json.decodeFromString(CapabilitiesResponse.serializer(), aliasOnly)
        }

        val missingBuilds = valid.replace("\"max_concurrent_snapshot_builds\":2,", "")
        assertFailsWith<SerializationException> {
            json.decodeFromString(CapabilitiesResponse.serializer(), missingBuilds)
        }

        val withoutServerMaterializationHints = valid
            .replace("\"snapshot_materialization_batch_rows\":1000,", "")
            .replace("\"snapshot_materialization_batch_bytes\":4194304,", "")
        val compatible = json.decodeFromString(
            CapabilitiesResponse.serializer(),
            withoutServerMaterializationHints,
        )
        assertEquals(0, compatible.bundleLimits.snapshotMaterializationBatchRows)
        assertEquals(0, compatible.bundleLimits.snapshotMaterializationBatchBytes)
        negotiateSnapshotLimits(compatible, config())

        listOf("snapshot_materialization_batch_rows", "snapshot_materialization_batch_bytes").forEach { field ->
            val root = json.parseToJsonElement(valid).jsonObject
            val limits = root.getValue("bundle_limits").jsonObject
            val withNull = JsonObject(
                root + ("bundle_limits" to JsonObject(limits + (field to kotlinx.serialization.json.JsonNull))),
            )
            assertFailsWith<SerializationException> {
                json.decodeFromString(CapabilitiesResponse.serializer(), withNull.toString())
            }
        }
    }

    @Test
    fun capabilitiesRejectEveryNormativelyRequiredOmission() {
        val valid = json.encodeToString(
            CapabilitiesResponse.serializer(),
            CapabilitiesResponse(
                protocolVersion = "v1",
                schemaVersion = 1,
                features = emptyMap(),
                bundleLimits = testBundleCapabilitiesLimits(),
            ),
        )

        listOf(
            "protocol_version",
            "schema_version",
            "features",
            "bundle_limits",
            "default_rows_per_snapshot_chunk",
            "max_rows_per_snapshot_chunk",
            "default_bytes_per_snapshot_chunk",
            "max_bytes_per_snapshot_chunk",
            "max_bytes_per_snapshot_row",
            "max_concurrent_snapshot_builds",
            "max_concurrent_snapshot_chunk_requests",
        ).forEach { field ->
            val root = json.parseToJsonElement(valid).jsonObject
            val withoutField = if (field in setOf("protocol_version", "schema_version", "features", "bundle_limits")) {
                JsonObject(root.filterKeys { it != field })
            } else {
                JsonObject(
                    root + ("bundle_limits" to JsonObject(root.getValue("bundle_limits").jsonObject.filterKeys { it != field })),
                )
            }
            assertFailsWith<SerializationException> {
                json.decodeFromString(CapabilitiesResponse.serializer(), withoutField.toString())
            }
        }
    }

    @Test
    fun sourceRetiredDecoderRejectsPresentNullAndPresentEmpty() {
        val absent = decodeSourceRecoveryRequiredExceptionOrNull(
            io.ktor.http.HttpStatusCode.Conflict,
            """{"error":"source_retired","message":"retired","source_id":"source-a"}""",
            "source-a",
        )
        assertTrue(absent is SourceRecoveryRequiredHttpException)

        listOf(
            """{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":null}""",
            """{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":""}""",
        ).forEach { body ->
            assertFails {
                decodeSourceRecoveryRequiredExceptionOrNull(io.ktor.http.HttpStatusCode.Conflict, body, "source-a")
            }
        }

        assertFails {
            decodeSourceRecoveryRequiredExceptionOrNull(
                io.ktor.http.HttpStatusCode.Conflict,
                """{"error":"source_retired","message":"retired","source_id":"source-b"}""",
                "source-a",
            )
        }
    }

    @Test
    fun negotiationRejectsEveryInvalidRelationshipAndAcceptsExactBoundary() {
        val base = testBundleCapabilitiesLimits()
        val client = config(snapshotChunkBytes = base.maxBytesPerSnapshotRow)
        assertEquals(
            base.maxBytesPerSnapshotRow,
            negotiateSnapshotLimits(capabilities(base), client).maxBytes,
        )

        listOf(
            base.copy(defaultRowsPerSnapshotChunk = 0),
            base.copy(defaultRowsPerSnapshotChunk = base.maxRowsPerSnapshotChunk + 1),
            base.copy(defaultBytesPerSnapshotChunk = 0),
            base.copy(defaultBytesPerSnapshotChunk = base.maxBytesPerSnapshotChunk + 1),
            base.copy(maxBytesPerSnapshotRow = base.maxBytesPerSnapshotChunk + 1),
            base.copy(maxConcurrentSnapshotBuilds = 0),
            base.copy(maxConcurrentSnapshotChunkRequests = 0),
        ).forEach { limits ->
            assertFailsWith<SnapshotCapabilitiesException> {
                negotiateSnapshotLimits(capabilities(limits), config())
            }
        }
        negotiateSnapshotLimits(capabilities(base.copy(snapshotMaterializationBatchRows = 0)), config())
        negotiateSnapshotLimits(capabilities(base.copy(snapshotMaterializationBatchBytes = 0)), config())

        assertFailsWith<SnapshotCapabilitiesException> {
            negotiateSnapshotLimits(
                capabilities(base),
                config(snapshotChunkBytes = base.maxBytesPerSnapshotRow - 1),
            )
        }
        assertFailsWith<SnapshotCapabilitiesException> {
            negotiateSnapshotLimits(
                capabilities(base),
                config(snapshotApplyBatchBytes = base.maxBytesPerSnapshotRow - 1),
            )
        }
    }

    @Test
    fun requiredByteCountPreservesMissingVersusZeroAndExactEmptyShape() {
        val emptySession = SnapshotSession("empty", 0, 0, 0, "2099-01-01T00:00:00Z")
        validateSnapshotSession(emptySession)
        assertFailsWith<IllegalArgumentException> {
            validateSnapshotSession(emptySession.copy(byteCount = 1))
        }
        assertFailsWith<IllegalArgumentException> {
            validateSnapshotSession(emptySession.copy(rowCount = 1, snapshotBundleSeq = 1, byteCount = 0))
        }

        assertFailsWith<SerializationException> {
            json.decodeFromString(
                SnapshotSession.serializer(),
                """{"snapshot_id":"missing","snapshot_bundle_seq":0,"row_count":0,"expires_at":"2099-01-01T00:00:00Z"}""",
            )
        }
        assertFailsWith<SerializationException> {
            json.decodeFromString(
                SnapshotChunkResponse.serializer(),
                """{"snapshot_id":"missing","snapshot_bundle_seq":0,"rows":[],"next_row_ordinal":0,"has_more":false}""",
            )
        }
    }

    @Test
    fun chunkValidationLocksIdentityBudgetsOrdinalsAndEmptyShape() {
        val row = SnapshotRow(
            schema = "main",
            table = "users",
            key = mapOf("id" to "opaque"),
            rowVersion = 1,
            payload = buildJsonObject { put("id", "opaque") },
        )
        val chunk = SnapshotChunkResponse("snapshot", 7, listOf(row), 1, false, 128)
        validateSnapshotChunkResponse(chunk, "snapshot", 7, 0, 1, 128)

        assertFailsWith<IllegalArgumentException> {
            validateSnapshotChunkResponse(chunk.copy(snapshotId = "other"), "snapshot", 7, 0, 1, 128)
        }
        assertFailsWith<IllegalArgumentException> {
            validateSnapshotChunkResponse(chunk.copy(byteCount = 129), "snapshot", 7, 0, 1, 128)
        }
        assertFailsWith<IllegalArgumentException> {
            validateSnapshotChunkResponse(chunk.copy(nextRowOrdinal = 2), "snapshot", 7, 0, 1, 128)
        }
        assertFailsWith<IllegalArgumentException> {
            validateSnapshotChunkResponse(
                SnapshotChunkResponse("snapshot", 7, emptyList(), 0, true, 0),
                "snapshot",
                7,
                0,
                1,
                128,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            validateSnapshotChunkResponse(chunk, "snapshot", 7, Long.MAX_VALUE, 1, 128)
        }
    }

    @Test
    fun snapshotErrorsRedactTextBinaryAndUnsafeIdentifiers() {
        val textKey = "customer-secret-key"
        val binaryKey = "601c32ed-8299-c541-c389-749094a88cf2"
        val payload = "customer-secret-payload"
        val error = SnapshotRowApplyException(
            rowOrdinal = 9,
            schemaName = "main\n$textKey",
            tableName = "users;$binaryKey;$payload",
        )
        val rendered = error.toString()
        assertFalse(rendered.contains(textKey))
        assertFalse(rendered.contains(binaryKey))
        assertFalse(rendered.contains(payload))
        assertTrue(rendered.contains("schema=<redacted>"))
        assertTrue(rendered.contains("table=<redacted>"))
        assertEquals("users", safeSnapshotDiagnosticIdentifier("users"))
    }

    @Test
    fun sourceIdsUseExactVisibleAsciiGrammarWithoutTrimming() {
        listOf("!", "source-1", "source.id", "~").forEach { sourceId ->
            assertEquals(sourceId, requireValidOversqliteSourceId(sourceId))
        }

        listOf("", " ", " source-1", "source-1 ", "source\tid", "source\nid", "\u007f", "é")
            .forEach { sourceId ->
                assertFailsWith<InvalidOversqliteSourceIdException> {
                    requireValidOversqliteSourceId(sourceId)
                }
            }
    }

    @Test
    fun sourceResponseBoundariesValidateBeforeComparisonAndRedactMismatches() {
        val expectedSourceId = "customer-secret-expected-source"
        val remoteSourceId = "customer-secret-remote-source"
        val invalidRemoteSourceId = "$remoteSourceId\ninjected"
        val canonicalRequestHash = "0".repeat(64)
        val createResponse = PushSessionCreateResponse(
            status = "already_committed",
            bundleSeq = 7,
            sourceId = expectedSourceId,
            sourceBundleId = 3,
            rowCount = 0,
            bundleHash = "bundle-hash",
            canonicalRequestHash = canonicalRequestHash,
        )

        val createMismatch = assertFailsWith<IllegalArgumentException> {
            committedPushBundleFromCreateResponse(
                createResponse.copy(sourceId = remoteSourceId),
                expectedSourceId,
                3,
            )
        }
        assertEquals(
            "push session already_committed response source_id does not match expected source",
            createMismatch.message,
        )
        assertFalse(createMismatch.toString().contains(expectedSourceId))
        assertFalse(createMismatch.toString().contains(remoteSourceId))
        assertFailsWith<InvalidOversqliteSourceIdException> {
            committedPushBundleFromCreateResponse(
                createResponse.copy(sourceId = invalidRemoteSourceId),
                expectedSourceId,
                3,
            )
        }
        assertFailsWith<InvalidOversqliteSourceIdException> {
            validatePushSessionCreateResponse(
                response = PushSessionCreateResponse(
                    pushId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    status = "staging",
                    plannedRowCount = 0,
                    sourceId = invalidRemoteSourceId,
                    canonicalRequestHash = canonicalRequestHash,
                ),
                sourceBundleId = 3,
                plannedRowCount = 0,
                sourceId = expectedSourceId,
                canonicalRequestHash = canonicalRequestHash,
            )
        }

        val commitResponse = PushSessionCommitResponse(
            bundleSeq = 7,
            sourceId = remoteSourceId,
            sourceBundleId = 3,
            rowCount = 0,
            bundleHash = "bundle-hash",
            canonicalRequestHash = canonicalRequestHash,
        )
        val commitMismatch = assertFailsWith<IllegalArgumentException> {
            committedPushBundleFromCommitResponse(commitResponse, expectedSourceId, 3)
        }
        assertEquals("push commit response source_id does not match expected source", commitMismatch.message)
        assertFalse(commitMismatch.toString().contains(expectedSourceId))
        assertFalse(commitMismatch.toString().contains(remoteSourceId))
        assertFailsWith<InvalidOversqliteSourceIdException> {
            committedPushBundleFromCommitResponse(
                commitResponse.copy(sourceId = invalidRemoteSourceId),
                expectedSourceId,
                3,
            )
        }

        val committed = CommittedPushBundle(
            bundleSeq = 7,
            sourceId = expectedSourceId,
            sourceBundleId = 3,
            rowCount = 0,
            bundleHash = "bundle-hash",
            canonicalRequestHash = canonicalRequestHash,
        )
        val rowsResponse = CommittedBundleRowsResponse(
            bundleSeq = 7,
            sourceId = remoteSourceId,
            sourceBundleId = 3,
            rowCount = 0,
            bundleHash = "bundle-hash",
            canonicalRequestHash = canonicalRequestHash,
            rows = emptyList(),
            nextRowOrdinal = -1,
            hasMore = false,
        )
        val rowsMismatch = assertFailsWith<IllegalArgumentException> {
            validateCommittedBundleRowsResponse(rowsResponse, committed, null)
        }
        assertEquals(
            "committed bundle chunk response source_id does not match expected source",
            rowsMismatch.message,
        )
        assertFalse(rowsMismatch.toString().contains(expectedSourceId))
        assertFalse(rowsMismatch.toString().contains(remoteSourceId))
        assertFailsWith<InvalidOversqliteSourceIdException> {
            validateCommittedBundleRowsResponse(
                rowsResponse.copy(sourceId = invalidRemoteSourceId),
                committed,
                null,
            )
        }
    }

    @Test
    fun sourceIngressAndPushDiagnosticsRejectOrRedactSensitiveIdentifiers() {
        val userId = "customer-secret-user"
        val sourceId = "customer-secret-source"
        val otherSourceId = "customer-secret-other-source"
        val invalidSourceId = "$sourceId\ninjected"
        val runtimeState = RuntimeState(
            validated = ValidatedConfig(
                schema = "main",
                tables = emptyList(),
                pkByTable = emptyMap(),
                keyByTable = emptyMap(),
                tableOrder = emptyMap(),
                tableInfoByName = emptyMap(),
            ),
            userId = userId,
            sourceId = sourceId,
        )

        val diagnostic = pushPendingStartDiagnostic(runtimeState, pendingDirtyCount = 2, pendingOutboundCount = 3)
        assertEquals("oversqlite pushPending start pendingDirty=2 pendingOutbound=3", diagnostic)
        assertFalse(diagnostic.contains(userId))
        assertFalse(diagnostic.contains(sourceId))

        val outboxMismatch = assertFailsWith<IllegalArgumentException> {
            requireMatchingOutboxSourceId(sourceId, otherSourceId)
        }
        assertEquals("persisted outbox source does not match current source", outboxMismatch.message)
        assertFalse(outboxMismatch.toString().contains(sourceId))
        assertFalse(outboxMismatch.toString().contains(otherSourceId))
        assertFailsWith<InvalidOversqliteSourceIdException> {
            requireMatchingOutboxSourceId(invalidSourceId, sourceId)
        }

        val watchError = assertFailsWith<InvalidOversqliteSourceIdException> {
            parseBundleChangeEventLines(
                listOf(
                    "event: bundle",
                    """data: {"bundle_seq":1,"source_id":"customer secret","source_bundle_id":1}""",
                    "",
                ),
                json,
            )
        }
        assertFalse(watchError.toString().contains("customer secret"))

        assertEquals(
            otherSourceId,
            validateSourceRetiredResponse(
                SourceRetiredResponse(
                    error = "source_retired",
                    message = "retired",
                    sourceId = sourceId,
                    replacedBySourceId = otherSourceId,
                ),
            ),
        )
        assertFailsWith<InvalidOversqliteSourceIdException> {
            validateSourceRetiredResponse(
                SourceRetiredResponse(
                    error = "source_retired",
                    message = "retired",
                    sourceId = invalidSourceId,
                    replacedBySourceId = otherSourceId,
                ),
            )
        }
        assertFailsWith<InvalidOversqliteSourceIdException> {
            validateSourceRetiredResponse(
                SourceRetiredResponse(
                    error = "source_retired",
                    message = "retired",
                    sourceId = sourceId,
                    replacedBySourceId = "",
                ),
            )
        }
    }

    @Test
    fun snapshotChunkDecoderRejectsDecodedNamesBeforeOverwrite() {
        val exactWire = assertFailsWith<SnapshotSemanticException> {
            decodeSnapshotChunkResponse(
                """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"main","table":"users","key":{"id":"1","id":"2"},"row_version":1,"payload":{"id":"1"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
            )
        }
        assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, exactWire.failure)

        val escapedWire = assertFailsWith<SnapshotSemanticException> {
            decodeSnapshotChunkResponse(
                """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"main","table":"users","key":{"id":"1"},"row_version":1,"payload":{"id":"1","\u0069d":"2"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
            )
        }
        assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, escapedWire.failure)
        assertEquals(null, escapedWire.cause)

        val exact = assertFailsWith<SnapshotSemanticException> {
            requireUniqueSnapshotJsonObjectMembers("""{"id":1,"id":2}""")
        }
        assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, exact.failure)

        val escaped = assertFailsWith<SnapshotSemanticException> {
            requireUniqueSnapshotJsonObjectMembers("""{"payload":{"id":1,"\u0069d":2}}""")
        }
        assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, escaped.failure)
        assertEquals(null, escaped.cause)

        requireUniqueSnapshotJsonObjectMembers("""{"payload":{"id":1,"ID":2}}""")
    }

    private fun capabilities(limits: BundleCapabilitiesLimits) = CapabilitiesResponse(
        protocolVersion = "v1",
        schemaVersion = 1,
        features = emptyMap(),
        bundleLimits = limits,
    )

    private fun config(
        snapshotChunkRows: Int = 1000,
        snapshotChunkBytes: Long = 4L * 1024L * 1024L,
        snapshotApplyBatchRows: Int = 256,
        snapshotApplyBatchBytes: Long = 4L * 1024L * 1024L,
    ) = OversqliteConfig(
        schema = "main",
        syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
        snapshotChunkRows = snapshotChunkRows,
        snapshotChunkBytes = snapshotChunkBytes,
        snapshotApplyBatchRows = snapshotApplyBatchRows,
        snapshotApplyBatchBytes = snapshotApplyBatchBytes,
    )
}
