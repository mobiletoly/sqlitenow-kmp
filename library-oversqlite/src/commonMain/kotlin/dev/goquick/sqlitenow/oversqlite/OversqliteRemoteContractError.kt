/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonObject

private data class RemoteContractErrorShape(
    val status: Int,
    val remediation: String,
    val retryable: Boolean = false,
)

private val remoteContractErrorShapes = mapOf(
    "missing_schema_generation" to RemoteContractErrorShape(400, "provide_schema_generation"),
    "invalid_schema_generation_id" to RemoteContractErrorShape(400, "correct_schema_generation_id"),
    "missing_schema_generation_fingerprint" to RemoteContractErrorShape(400, "provide_schema_generation_fingerprint"),
    "malformed_schema_generation_fingerprint" to RemoteContractErrorShape(400, "correct_schema_generation_fingerprint"),
    "unknown_schema_generation" to RemoteContractErrorShape(409, "upgrade_or_reconfigure_client"),
    "schema_generation_fingerprint_mismatch" to RemoteContractErrorShape(409, "upgrade_or_reconfigure_client"),
    "unsupported_schema_generation" to RemoteContractErrorShape(410, "upgrade_or_reconfigure_client"),
    "retired_schema_generation" to RemoteContractErrorShape(426, "upgrade_application"),
    "source_generation_unbound" to RemoteContractErrorShape(409, "connect_source"),
    "source_generation_conflict" to RemoteContractErrorShape(409, "rotate_source"),
    "schema_generation_downgrade_forbidden" to RemoteContractErrorShape(409, "keep_or_upgrade_generation"),
    "corrupt_source_generation_state" to RemoteContractErrorShape(500, "contact_operator"),
    "source_binding_limit_exceeded" to RemoteContractErrorShape(409, "contact_operator"),
    "session_generation_mismatch" to RemoteContractErrorShape(409, "restart_session_with_matching_generation"),
    "corrupt_session_generation_state" to RemoteContractErrorShape(500, "contact_operator"),
    "corrupt_committed_push_state" to RemoteContractErrorShape(500, "contact_operator"),
    "generation_history_unavailable" to RemoteContractErrorShape(409, "rebuild_from_snapshot"),
    "generation_projection_integrity_error" to RemoteContractErrorShape(500, "contact_operator"),
    "bundle_limit_exceeded" to RemoteContractErrorShape(413, "reduce_request"),
    "history_projection_capacity_exceeded" to RemoteContractErrorShape(503, "retry", retryable = true),
    "push_effect_coverage_incomplete" to RemoteContractErrorShape(422, "correct_server_write_logic"),
)

private val discoveryRemoteContractErrorShapes = mapOf(
    "invalid_generation_discovery_request" to RemoteContractErrorShape(400, "correct_request"),
    "unknown_schema_generation" to remoteContractErrorShapes.getValue("unknown_schema_generation"),
    "retired_schema_generation" to remoteContractErrorShapes.getValue("retired_schema_generation"),
)

private data class RemoteOperationErrorShape(
    val status: Int,
    val retryable: Boolean = false,
)

private val remoteOperationErrorShapes = mapOf(
    "invalid_request" to RemoteOperationErrorShape(400),
    "push_invalid" to RemoteOperationErrorShape(400),
    "push_replay_mismatch" to RemoteOperationErrorShape(409),
    "bundle_projection_too_large" to RemoteOperationErrorShape(422),
    "committed_bundle_unrepresentable" to RemoteOperationErrorShape(500),
    "invalid_gzip" to RemoteOperationErrorShape(400),
    "unsupported_media_type" to RemoteOperationErrorShape(415),
    "unsupported_content_encoding" to RemoteOperationErrorShape(415),
    "encoded_body_too_large" to RemoteOperationErrorShape(413),
    "decoded_body_too_large" to RemoteOperationErrorShape(413),
    "commit_outcome_unknown" to RemoteOperationErrorShape(503, retryable = true),
    "service_not_ready" to RemoteOperationErrorShape(503, retryable = true),
    "push_failed" to RemoteOperationErrorShape(500, retryable = true),
    "connect_invalid" to RemoteOperationErrorShape(400),
    "connect_failed" to RemoteOperationErrorShape(500, retryable = true),
    "pull_failed" to RemoteOperationErrorShape(500, retryable = true),
    "projection_response_encoding_failed" to RemoteOperationErrorShape(500),
    "response_encoding_failed" to RemoteOperationErrorShape(500),
)

private val applicationRejectionCode = Regex("^[a-z][a-z0-9_]{0,63}$")

internal fun decodeRemoteContractErrorOrNull(
    status: Int,
    raw: String,
    operation: String,
    expectedGenerationId: Long? = null,
): Throwable? {
    if (!raw.trimStart().startsWith("{")) return null
    requireUniqueSnapshotJsonObjectMembers(raw)
    val root = runCatching { Json.parseToJsonElement(raw).jsonObject }.getOrNull() ?: return null
    val code = root.stringValueOrNull("error") ?: return null
    val shapes = if (expectedGenerationId == null) remoteContractErrorShapes else discoveryRemoteContractErrorShapes
    val expected = shapes[code] ?: return null
    val allowedKeys = buildSet {
        add("error")
        add("message")
        add("retryable")
        add("remediation")
        if (expectedGenerationId != null) add("schema_generation_id")
    }
    if (root.keys != allowedKeys) throw RemoteResponseSemanticException(operation)
    val message = root.stringValueOrNull("message")?.takeIf(String::isNotEmpty)
        ?: throw RemoteResponseSemanticException(operation)
    val remediation = root.stringValueOrNull("remediation")
        ?: throw RemoteResponseSemanticException(operation)
    val retryable = root.booleanValueOrNull("retryable")
        ?: throw RemoteResponseSemanticException(operation)
    if (status != expected.status || remediation != expected.remediation || retryable != expected.retryable) {
        throw RemoteResponseSemanticException(operation)
    }
    if (expectedGenerationId != null) {
        val generationId = root.stringValueOrNull("schema_generation_id")
        if (generationId != expectedGenerationId.toString()) throw RemoteResponseSemanticException(operation)
    }
    return OversqliteRemoteContractException(
        code = code,
        httpStatus = status,
        operation = operation,
        remediation = remediation,
        retryable = retryable,
        generationId = expectedGenerationId,
        message = message,
    )
}

internal fun decodeRemoteOperationErrorOrNull(
    status: Int,
    raw: String,
    operation: String,
): Throwable? {
    if (!raw.trimStart().startsWith("{")) return null
    requireUniqueSnapshotJsonObjectMembers(raw)
    val root = runCatching { Json.parseToJsonElement(raw).jsonObject }.getOrNull() ?: return null
    val code = root.stringValueOrNull("error") ?: return null
    val message = root.stringValueOrNull("message")?.takeIf(String::isNotEmpty)
        ?: throw RemoteResponseSemanticException(operation)
    val shape = remoteOperationErrorShapes[code]
    if (shape == null && (status != 422 || !applicationRejectionCode.matches(code))) return null
    if (shape != null && status != shape.status) throw RemoteResponseSemanticException(operation)
    return OversqliteRemoteContractException(
        code = code,
        httpStatus = status,
        operation = operation,
        remediation = null,
        retryable = shape?.retryable ?: false,
        generationId = null,
        message = message,
    )
}

private fun JsonObject.stringValueOrNull(name: String): String? =
    (this[name] as? JsonPrimitive)?.takeIf(JsonPrimitive::isString)?.content

private fun JsonObject.booleanValueOrNull(name: String): Boolean? =
    (this[name] as? JsonPrimitive)?.takeUnless(JsonPrimitive::isString)?.booleanOrNull
