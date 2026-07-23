/*
 * Copyright 2026 Toly Pochkin
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
package dev.goquick.sqlitenow.core.worker

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive

internal const val SQLITE_WORKER_PROTOCOL = "sqlitenow-sqlite-worker-v1"
internal const val SQLITE_WORKER_DEFAULT_PAGE_ROWS = 64
internal const val SQLITE_WORKER_MAX_PAGE_ROWS = 1024
internal const val SQLITE_WORKER_MIN_PAGE_BYTES = 2
internal const val SQLITE_WORKER_DEFAULT_PAGE_BYTES = 64 * 1024
internal const val SQLITE_WORKER_MAX_PAGE_BYTES = 1024 * 1024
internal const val SQLITE_WORKER_HARD_ROW_BYTES = 1024 * 1024
internal const val SQLITE_WORKER_MAX_ENCODED_PAGE_BYTES = SQLITE_WORKER_HARD_ROW_BYTES + 2
internal const val SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS = 5_000

internal val sqliteWorkerJson = Json {
    encodeDefaults = false
    explicitNulls = false
    ignoreUnknownKeys = false
}

internal val sqliteWorkerCommands = setOf(
    "open",
    "completeOpen",
    "prepare",
    "page",
    "reset",
    "clearBindings",
    "closeStatement",
    "closeDatabase",
    "metrics",
    "shutdown",
)

@Serializable
internal data class SqliteWorkerConfig(
    val pageRows: Int = SQLITE_WORKER_DEFAULT_PAGE_ROWS,
    val pageBytes: Int = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
) {
    fun validate() {
        require(pageRows in 1..SQLITE_WORKER_MAX_PAGE_ROWS) {
            "Worker pageRows must be in 1..$SQLITE_WORKER_MAX_PAGE_ROWS."
        }
        require(pageBytes in SQLITE_WORKER_MIN_PAGE_BYTES..SQLITE_WORKER_MAX_PAGE_BYTES) {
            "Worker pageBytes must be in " +
                "$SQLITE_WORKER_MIN_PAGE_BYTES..$SQLITE_WORKER_MAX_PAGE_BYTES."
        }
    }
}

@Serializable(with = SqliteWorkerValueSerializer::class)
internal data class SqliteWorkerValue(
    val type: String,
    val integer: String? = null,
    val real: Double? = null,
    val text: String? = null,
    val blob: List<Int>? = null,
) {
    internal var realWireLiteral: String? = null

    fun validate() {
        when (type) {
            "null" -> requirePayload(integer == null && real == null && text == null && blob == null)
            "integer" -> {
                requirePayload(integer != null && real == null && text == null && blob == null)
                require(integer!!.isCanonicalSignedLong()) {
                    "SQLite INTEGER must be canonical signed decimal text in the Long range."
                }
            }
            "real" -> {
                requirePayload(integer == null && real?.isFinite() == true && text == null && blob == null)
            }
            "text" -> requirePayload(integer == null && real == null && text != null && blob == null)
            "blob" -> {
                requirePayload(integer == null && real == null && text == null && blob != null)
                require(blob!!.all { it in 0..255 }) {
                    "SQLite BLOB octets must be in 0..255."
                }
            }
            else -> error("Unsupported SQLite worker value tag: $type")
        }
    }

    private fun requirePayload(valid: Boolean) {
        require(valid) { "SQLite worker value payload does not match its $type tag." }
    }

    companion object {
        fun nullValue() = SqliteWorkerValue(type = "null")

        fun integer(value: Long) = SqliteWorkerValue(type = "integer", integer = value.toString())

        fun real(value: Double): SqliteWorkerValue {
            require(value.isFinite()) { "SQLite REAL must be finite." }
            return SqliteWorkerValue(type = "real", real = value)
        }

        fun text(value: String) = SqliteWorkerValue(type = "text", text = value)

        fun blob(value: ByteArray) =
            SqliteWorkerValue(type = "blob", blob = value.map { it.toInt() and 0xff })
    }
}

internal object SqliteWorkerValueSerializer : KSerializer<SqliteWorkerValue> {
    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("dev.goquick.sqlitenow.core.worker.SqliteWorkerValue")

    override fun serialize(encoder: Encoder, value: SqliteWorkerValue) {
        val jsonEncoder = encoder as? JsonEncoder
            ?: throw SerializationException("SQLite worker values require JSON encoding.")
        value.validate()
        val fields = linkedMapOf<String, JsonElement>("type" to JsonPrimitive(value.type))
        when (value.type) {
            "null" -> Unit
            "integer" -> fields["integer"] = JsonPrimitive(checkNotNull(value.integer))
            "real" -> fields["real"] = JsonPrimitive(checkNotNull(value.real))
            "text" -> fields["text"] = JsonPrimitive(checkNotNull(value.text))
            "blob" -> fields["blob"] = JsonArray(checkNotNull(value.blob).map(::JsonPrimitive))
        }
        jsonEncoder.encodeJsonElement(JsonObject(fields))
    }

    override fun deserialize(decoder: Decoder): SqliteWorkerValue {
        val jsonDecoder = decoder as? JsonDecoder
            ?: throw SerializationException("SQLite worker values require JSON decoding.")
        val value = jsonDecoder.decodeJsonElement() as? JsonObject
            ?: throw SerializationException("SQLite worker value must be a tagged object.")
        val type = value.requiredString("type")
        val payload = when (type) {
            "null" -> null
            "integer" -> "integer"
            "real" -> "real"
            "text" -> "text"
            "blob" -> "blob"
            else -> throw SerializationException("Unsupported SQLite worker value tag: $type")
        }
        val expectedKeys = if (payload == null) setOf("type") else setOf("type", payload)
        if (value.keys != expectedKeys) {
            throw SerializationException(
                "SQLite worker $type value must contain exactly ${expectedKeys.sorted()}.",
            )
        }
        payload?.let { field ->
            if (value[field] == JsonNull) {
                throw SerializationException("SQLite worker $type payload must not be null.")
            }
        }
        return try {
            when (type) {
                "null" -> SqliteWorkerValue.nullValue()
                "integer" -> SqliteWorkerValue(
                    type = type,
                    integer = value.requiredString("integer"),
                )
                "real" -> value.requiredNumber("real").let { primitive ->
                    SqliteWorkerValue(
                        type = type,
                        real = primitive.double,
                    ).also {
                        it.realWireLiteral = primitive.content
                    }
                }
                "text" -> SqliteWorkerValue(
                    type = type,
                    text = value.requiredString("text"),
                )
                "blob" -> SqliteWorkerValue(
                    type = type,
                    blob = value["blob"]!!.jsonArray.mapIndexed { index, element ->
                        val octet = element.jsonPrimitive
                        if (octet.isString) {
                            throw SerializationException(
                                "SQLite worker blob octet $index must be a number.",
                            )
                        }
                        octet.int
                    },
                )
                else -> error("Validated above.")
            }.also(SqliteWorkerValue::validate)
        } catch (failure: SerializationException) {
            throw failure
        } catch (failure: Throwable) {
            throw SerializationException("Invalid SQLite worker $type payload.", failure)
        }
    }

    private fun JsonObject.requiredPrimitive(name: String): JsonPrimitive =
        this[name] as? JsonPrimitive
            ?: throw SerializationException("SQLite worker value field $name must be primitive.")

    private fun JsonObject.requiredString(name: String): String =
        requiredPrimitive(name).also { primitive ->
            if (!primitive.isString) {
                throw SerializationException("SQLite worker value field $name must be a string.")
            }
        }.content

    private fun JsonObject.requiredNumber(name: String): JsonPrimitive =
        requiredPrimitive(name).also { primitive ->
            if (primitive.isString) {
                throw SerializationException("SQLite worker value field $name must be a number.")
            }
        }
}

@Serializable
internal data class SqliteWorkerRequest(
    val protocol: String,
    val command: String,
    val databaseId: Int? = null,
    val statementId: Int? = null,
    val fileName: String? = null,
    val sql: String? = null,
    val bindings: Map<Int, SqliteWorkerValue> = emptyMap(),
    val pageRows: Int? = null,
    val pageBytes: Int? = null,
    val legacySourceMode: String? = null,
    val openId: Int? = null,
    val legacySourceStatus: String? = null,
) {
    fun validate() {
        require(protocol == SQLITE_WORKER_PROTOCOL) {
            "Unsupported SQLite worker protocol."
        }
        require(command in sqliteWorkerCommands) {
            "Unsupported SQLite worker command: $command"
        }
        bindings.forEach { (index, value) ->
            require(index >= 1) { "SQLite bind indices are one-based." }
            value.validate()
        }
        when (command) {
            "open" -> {
                require(!fileName.isNullOrBlank()) {
                    "SQLite worker database name must be non-empty."
                }
                require(databaseId == null && statementId == null && sql == null && bindings.isEmpty())
                require(pageRows == null && pageBytes == null)
                require(legacySourceMode in setOf("built-in", "custom", "none")) {
                    "Worker open requires a supported legacySourceMode."
                }
                require(openId == null && legacySourceStatus == null)
            }
            "completeOpen" -> {
                require(openId != null && openId > 0) {
                    "Worker completeOpen requires a positive openId."
                }
                require(legacySourceStatus in setOf("present", "absent")) {
                    "Worker completeOpen requires a supported legacySourceStatus."
                }
                require(databaseId == null && statementId == null && fileName == null && sql == null)
                require(bindings.isEmpty() && pageRows == null && pageBytes == null)
                require(legacySourceMode == null)
            }
            "prepare" -> {
                requireDatabase()
                require(statementId == null && !sql.isNullOrBlank() && bindings.isEmpty())
                require(pageRows == null && pageBytes == null)
                require(legacySourceMode == null && openId == null && legacySourceStatus == null)
            }
            "page" -> {
                requireStatement()
                require(!sql.isNullOrBlank())
                SqliteWorkerConfig(
                    pageRows = requireNotNull(pageRows),
                    pageBytes = requireNotNull(pageBytes),
                ).validate()
                require(legacySourceMode == null && openId == null && legacySourceStatus == null)
            }
            "reset", "clearBindings", "closeStatement" -> {
                requireStatement()
                require(bindings.isEmpty() && pageRows == null && pageBytes == null)
                require(legacySourceMode == null && openId == null && legacySourceStatus == null)
            }
            "closeDatabase" -> {
                requireDatabase()
                require(statementId == null && sql == null && bindings.isEmpty())
                require(pageRows == null && pageBytes == null)
                require(legacySourceMode == null && openId == null && legacySourceStatus == null)
            }
            "metrics", "shutdown" -> {
                require(databaseId == null && statementId == null && fileName == null && sql == null)
                require(bindings.isEmpty() && pageRows == null && pageBytes == null)
                require(legacySourceMode == null && openId == null && legacySourceStatus == null)
            }
        }
    }

    private fun requireDatabase() {
        require(databaseId != null && databaseId > 0) { "A positive databaseId is required." }
        require(fileName == null)
    }

    private fun requireStatement() {
        requireDatabase()
        require(statementId != null && statementId > 0) { "A positive statementId is required." }
    }
}

@Serializable(with = SqliteWorkerErrorSerializer::class)
internal data class SqliteWorkerError(
    val operation: String,
    val message: String,
    val sql: String? = null,
    val sqliteCode: Int? = null,
    val cancelled: Boolean = false,
    val suppressed: List<String> = emptyList(),
    val inTransaction: Boolean? = null,
)

internal object SqliteWorkerErrorSerializer : KSerializer<SqliteWorkerError> {
    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("dev.goquick.sqlitenow.core.worker.SqliteWorkerError")

    override fun serialize(encoder: Encoder, value: SqliteWorkerError) {
        val jsonEncoder = encoder as? JsonEncoder
            ?: throw SerializationException("SQLite worker errors require JSON encoding.")
        val fields = linkedMapOf<String, JsonElement>(
            "operation" to JsonPrimitive(value.operation),
            "message" to JsonPrimitive(value.message),
        )
        value.sql?.let { fields["sql"] = JsonPrimitive(it) }
        value.sqliteCode?.let { fields["sqliteCode"] = JsonPrimitive(it) }
        fields["cancelled"] = JsonPrimitive(value.cancelled)
        fields["suppressed"] = JsonArray(value.suppressed.map(::JsonPrimitive))
        value.inTransaction?.let { fields["inTransaction"] = JsonPrimitive(it) }
        jsonEncoder.encodeJsonElement(JsonObject(fields))
    }

    override fun deserialize(decoder: Decoder): SqliteWorkerError {
        val jsonDecoder = decoder as? JsonDecoder
            ?: throw SerializationException("SQLite worker errors require JSON decoding.")
        val value = jsonDecoder.decodeJsonElement() as? JsonObject
            ?: throw SerializationException("SQLite worker error must be an object.")
        val allowedKeys = setOf(
            "operation",
            "message",
            "sql",
            "sqliteCode",
            "cancelled",
            "suppressed",
            "inTransaction",
        )
        if (!allowedKeys.containsAll(value.keys)) {
            throw SerializationException(
                "Unknown SQLite worker error fields: ${(value.keys - allowedKeys).sorted()}.",
            )
        }
        val suppressed = value["suppressed"] as? JsonArray
            ?: throw SerializationException("SQLite worker error suppressed must be an array.")
        return SqliteWorkerError(
            operation = value.requiredErrorString("operation"),
            message = value.requiredErrorString("message"),
            sql = value.optionalErrorString("sql"),
            sqliteCode = value.optionalErrorInt("sqliteCode"),
            cancelled = value.requiredErrorBoolean("cancelled"),
            suppressed = suppressed.mapIndexed { index, element ->
                val primitive = element as? JsonPrimitive
                    ?: throw SerializationException(
                        "SQLite worker suppressed failure $index must be a string.",
                    )
                if (!primitive.isString) {
                    throw SerializationException(
                        "SQLite worker suppressed failure $index must be a string.",
                    )
                }
                primitive.content
            },
            inTransaction = value.optionalErrorBoolean("inTransaction"),
        )
    }

    private fun JsonObject.requiredErrorString(name: String): String {
        val primitive = this[name] as? JsonPrimitive
            ?: throw SerializationException("SQLite worker error $name must be a string.")
        if (!primitive.isString) {
            throw SerializationException("SQLite worker error $name must be a string.")
        }
        return primitive.content
    }

    private fun JsonObject.optionalErrorString(name: String): String? {
        if (!containsKey(name)) return null
        return requiredErrorString(name)
    }

    private fun JsonObject.optionalErrorInt(name: String): Int? {
        if (!containsKey(name)) return null
        val primitive = this[name] as? JsonPrimitive
            ?: throw SerializationException("SQLite worker error $name must be an integer.")
        if (primitive.isString) {
            throw SerializationException("SQLite worker error $name must be an integer.")
        }
        return primitive.intOrNull
            ?: throw SerializationException("SQLite worker error $name must be an integer.")
    }

    private fun JsonObject.requiredErrorBoolean(name: String): Boolean {
        val primitive = this[name] as? JsonPrimitive
            ?: throw SerializationException("SQLite worker error $name must be a boolean.")
        if (primitive.isString) {
            throw SerializationException("SQLite worker error $name must be a boolean.")
        }
        return primitive.booleanOrNull
            ?: throw SerializationException("SQLite worker error $name must be a boolean.")
    }

    private fun JsonObject.optionalErrorBoolean(name: String): Boolean? {
        if (!containsKey(name)) return null
        return requiredErrorBoolean(name)
    }
}

@Serializable
internal data class SqliteWorkerFlushBatch(
    @Required val envelopes: List<SqliteWorkerEnvelope> = emptyList(),
    @Required val barrierFailures: List<String> = emptyList(),
)

@Serializable
internal data class SqliteWorkerMetrics(
    @Required val runtimeKind: String = "",
    @Required val sqliteVersion: String = "",
    @Required val storageMode: String = "",
    @Required val requestsStarted: Long = 0,
    @Required val requestsCompleted: Long = 0,
    @Required val requestsCancelled: Long = 0,
    @Required val pendingRequests: Int = 0,
    @Required val liveDatabases: Int = 0,
    @Required val liveStatements: Int = 0,
    @Required val transactionsRolledBackOnCancel: Long = 0,
    @Required val integerBindingsAsStrings: Long = 0,
    @Required val integerResultsAsStrings: Long = 0,
    @Required val integerNumberViolations: Long = 0,
    @Required val pageRequests: Long = 0,
    @Required val steppedRows: Long = 0,
    @Required val encodedRows: Long = 0,
    @Required val transferredRows: Long = 0,
    @Required val transferredBytes: Long = 0,
    @Required val maxPageRows: Int = 0,
    @Required val maxPageBytes: Int = 0,
    @Required val oversizedRows: Long = 0,
    @Required val snapshotExports: Long = 0,
    @Required val migrationSourceKind: String = "",
    @Required val migrationSourceBytes: Long = 0,
    @Required val migrationDurationMillis: Long = 0,
    @Required val migrationPeakOwnedBytes: Long = 0,
    @Required val migrationTargetFileName: String = "",
    @Required val migrationSourceSha256: String = "",
    @Required val migrationIntegrityCheck: String = "",
    @Required val migrationImportedUserVersion: Int = 0,
    @Required val migrationSourceRetained: Boolean = false,
    @Required val migrationHeapAvailable: Boolean = false,
    @Required val migrationHeapStartBytes: Long = 0,
    @Required val migrationHeapPeakBytes: Long = 0,
    @Required val migrationHeapEndBytes: Long = 0,
    @Required val workerStarts: Long = 0,
    @Required val workerStops: Long = 0,
)

@Serializable
internal data class SqliteWorkerResponse(
    val protocol: String,
    val databaseId: Int? = null,
    val openState: String? = null,
    val openId: Int? = null,
    val statementId: Int? = null,
    val columnNames: List<String>? = null,
    val rows: List<List<SqliteWorkerValue>>? = null,
    @Required val done: Boolean = false,
    @Required val oversizedRow: Boolean = false,
    @Required val pageRows: Int = 0,
    @Required val pageBytes: Int = 0,
    @Required val inTransaction: Boolean = false,
    val metrics: SqliteWorkerMetrics? = null,
    val runtimeKind: String? = null,
    val sqliteVersion: String? = null,
) {
    fun validate() {
        require(protocol == SQLITE_WORKER_PROTOCOL) { "Unsupported SQLite worker protocol." }
        rows?.flatten()?.forEach(SqliteWorkerValue::validate)
        require(pageRows in 0..SQLITE_WORKER_MAX_PAGE_ROWS)
        require(pageBytes in 0..SQLITE_WORKER_MAX_ENCODED_PAGE_BYTES)
        rows?.let {
            require(pageRows == it.size) { "pageRows must match the encoded row count." }
            require(!oversizedRow || it.size == 1) {
                "An oversized soft-limit page must contain one row."
            }
        }
    }

    fun validateFor(request: SqliteWorkerRequest) {
        validate()
        when (request.command) {
            "open" -> {
                require(statementId == null)
                require(runtimeKind?.isNotBlank() == true)
                require(sqliteVersion?.isNotBlank() == true)
                require(metrics == null && columnNames == null)
                requireEmptyPagePayload()
                require(!inTransaction)
                when (openState) {
                    "opened" -> {
                        requirePositive(databaseId, "databaseId")
                        require(openId == null)
                    }
                    "legacy-source-required" -> {
                        require(request.legacySourceMode == "custom")
                        require(databaseId == null)
                        requirePositive(openId, "openId")
                    }
                    else -> error("Worker open response requires a supported openState.")
                }
            }
            "completeOpen" -> {
                require(openState == "opened")
                requirePositive(databaseId, "databaseId")
                require(openId == null && statementId == null)
                require(runtimeKind?.isNotBlank() == true)
                require(sqliteVersion?.isNotBlank() == true)
                require(metrics == null && columnNames == null)
                requireEmptyPagePayload()
                require(!inTransaction)
            }
            "prepare" -> {
                require(openState == null && openId == null)
                require(databaseId == null)
                requirePositive(statementId, "statementId")
                requireNotNull(columnNames) {
                    "Worker prepare response requires columnNames."
                }
                require(runtimeKind == null && sqliteVersion == null && metrics == null)
                requireEmptyPagePayload()
            }
            "page" -> {
                require(openState == null && openId == null)
                require(databaseId == null && statementId == null && columnNames == null)
                require(runtimeKind == null && sqliteVersion == null && metrics == null)
                val page = requireNotNull(rows) { "Worker page response requires rows." }
                val exactBytes = page.exactWireJson().encodeToByteArray().size
                require(pageBytes == exactBytes) {
                    "pageBytes=$pageBytes must match the exact UTF-8 " +
                        "rows-array representation ($exactBytes bytes)."
                }
                require(pageBytes in SQLITE_WORKER_MIN_PAGE_BYTES..SQLITE_WORKER_MAX_ENCODED_PAGE_BYTES)
            }
            "reset", "clearBindings", "closeStatement" -> {
                require(openState == null && openId == null)
                require(databaseId == null && statementId == null && columnNames == null)
                require(runtimeKind == null && sqliteVersion == null && metrics == null)
                requireEmptyPagePayload()
            }
            "closeDatabase" -> {
                require(openState == null && openId == null)
                require(databaseId == null && statementId == null && columnNames == null)
                require(runtimeKind == null && sqliteVersion == null && metrics == null)
                requireEmptyPagePayload()
                require(!inTransaction)
            }
            "metrics", "shutdown" -> {
                require(openState == null && openId == null)
                require(databaseId == null && statementId == null && columnNames == null)
                require(runtimeKind == null && sqliteVersion == null)
                requireNotNull(metrics) { "Worker ${request.command} response requires metrics." }
                require(metrics.runtimeKind.isNotBlank())
                require(metrics.sqliteVersion.isNotBlank())
                require(metrics.storageMode.isNotBlank())
                requireEmptyPagePayload()
                require(!inTransaction)
            }
        }
    }

    private fun requireEmptyPagePayload() {
        require(rows == null && !done && !oversizedRow && pageRows == 0 && pageBytes == 0)
    }

    private fun requirePositive(value: Int?, label: String) {
        require(value != null && value > 0) { "Worker response requires a positive $label." }
    }
}

private fun List<List<SqliteWorkerValue>>.exactWireJson(): String = buildString {
    append('[')
    this@exactWireJson.forEachIndexed { rowIndex, row ->
        if (rowIndex > 0) append(',')
        append('[')
        row.forEachIndexed { valueIndex, value ->
            if (valueIndex > 0) append(',')
            append("{\"type\":")
            append(JsonPrimitive(value.type))
            when (value.type) {
                "null" -> Unit
                "integer" -> {
                    append(",\"integer\":")
                    append(JsonPrimitive(checkNotNull(value.integer)))
                }
                "real" -> {
                    append(",\"real\":")
                    append(
                        value.realWireLiteral
                            ?: JsonPrimitive(checkNotNull(value.real)).toString(),
                    )
                }
                "text" -> {
                    append(",\"text\":")
                    append(JsonPrimitive(checkNotNull(value.text)))
                }
                "blob" -> {
                    append(",\"blob\":[")
                    checkNotNull(value.blob).forEachIndexed { octetIndex, octet ->
                        if (octetIndex > 0) append(',')
                        append(octet)
                    }
                    append(']')
                }
                else -> error("Unsupported SQLite worker value tag: ${value.type}")
            }
            append('}')
        }
        append(']')
    }
    append(']')
}

@Serializable
internal data class SqliteWorkerEnvelope(
    val id: Int,
    val data: SqliteWorkerResponse? = null,
    val error: SqliteWorkerError? = null,
) {
    fun validate() {
        require(id != 0) { "Worker envelope id must be non-zero." }
        require((data == null) != (error == null)) {
            "Worker envelope must contain exactly one of data or error."
        }
        data?.validate()
    }

    fun validateFor(expectedId: Int, request: SqliteWorkerRequest) {
        require(expectedId > 0) { "Awaited SQLite worker request IDs must be positive." }
        validateForRequest(expectedId, request)
    }

    fun validateOneWayFor(expectedId: Int, request: SqliteWorkerRequest) {
        require(expectedId < 0) { "One-way SQLite worker request IDs must be negative." }
        validateForRequest(expectedId, request)
    }

    private fun validateForRequest(expectedId: Int, request: SqliteWorkerRequest) {
        validate()
        require(id == expectedId) {
            "SQLite worker response ID $id does not match request ID $expectedId."
        }
        data?.validateFor(request)
        error?.let {
            require(it.operation == request.command) {
                "SQLite worker error operation ${it.operation} does not match ${request.command}."
            }
        }
    }
}

private fun String.isCanonicalSignedLong(): Boolean {
    if (isEmpty()) return false
    val negative = first() == '-'
    val digits = if (negative) substring(1) else this
    if (digits.isEmpty() || digits.any { it !in '0'..'9' }) return false
    if (digits.length > 1 && digits.first() == '0') return false
    if (negative && digits == "0") return false
    return toLongOrNull()?.toString() == this
}
