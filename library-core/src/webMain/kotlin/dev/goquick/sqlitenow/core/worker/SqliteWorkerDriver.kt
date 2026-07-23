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

import androidx.sqlite.SQLITE_DATA_BLOB
import androidx.sqlite.SQLITE_DATA_FLOAT
import androidx.sqlite.SQLITE_DATA_INTEGER
import androidx.sqlite.SQLITE_DATA_NULL
import androidx.sqlite.SQLITE_DATA_TEXT
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteDriver
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.SuspendSQLiteConnectionCleanup
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.toSqliteExceptionPreservingSuppressed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

internal class SqliteWorkerSQLiteDriver private constructor(
    private val config: SqliteWorkerConfig,
    private val transport: SqliteWorkerTransport,
    internal val cleanupTimeoutMillis: Int,
) : SQLiteDriver {
    override val hasConnectionPool: Boolean = false

    private var nextRequestId = 1
    private val queuedOneWays = linkedMapOf<Int, SqliteWorkerRequest>()
    private var shutdown = false
    private var terminalFailure: Throwable? = null
    private var shutdownMetrics: SqliteWorkerMetrics? = null
    private val connections = mutableSetOf<SqliteWorkerSQLiteConnection>()

    override suspend fun open(fileName: String): SQLiteConnection =
        open(
            fileName = fileName,
            legacySourceMode = "built-in",
            customPersistence = null,
        )

    internal suspend fun open(
        fileName: String,
        legacySourceMode: String,
        customPersistence: SqlitePersistence?,
    ): SQLiteConnection {
        require(fileName.isNotBlank()) { "SQLite worker database name must be non-empty." }
        require(legacySourceMode in setOf("built-in", "custom", "none"))
        require((legacySourceMode == "custom") == (customPersistence != null)) {
            "Custom legacy mode requires exactly one custom persistence source."
        }
        val initial = request(
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "open",
                fileName = fileName,
                legacySourceMode = legacySourceMode,
            ),
        ) { it }
        val opened = when (initial.openState) {
            "opened" -> initial
            "legacy-source-required" -> {
                check(legacySourceMode == "custom") {
                    "Worker requested custom legacy bytes for non-custom mode."
                }
                val bytes = checkNotNull(customPersistence).load(fileName)
                val completion = SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "completeOpen",
                    openId = checkNotNull(initial.openId),
                    legacySourceStatus = if (bytes == null) "absent" else "present",
                )
                request(completion, legacyBytes = bytes) { it }
            }
            else -> error("Unsupported SQLite worker open state: ${initial.openState}")
        }
        return SqliteWorkerSQLiteConnection(
            driver = this,
            databaseId = checkNotNull(opened.databaseId),
            config = config,
        ).also(connections::add)
    }

    suspend fun metrics(): SqliteWorkerMetrics =
        request(
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "metrics",
            ),
        ) { response -> checkNotNull(response.metrics) }

    internal suspend fun cleanupMigrationStateForTest(databaseName: String) {
        checkUsable()
        transport.cleanupMigrationStateForTest(databaseName)
    }

    internal fun setMigrationInterruptionForTest(databaseName: String, stage: String) {
        checkUsable()
        transport.setMigrationInterruptionForTest(databaseName, stage)
    }

    internal fun holdMigrationCancellationForTest(databaseName: String, stage: String): Int {
        checkUsable()
        require(databaseName.isNotBlank()) {
            "Migration cancellation hold database name must be non-empty."
        }
        require(stage in setOf("before-intent-write", "after-integrity", "after-health")) {
            "Unsupported migration cancellation hold stage: $stage."
        }
        return transport.holdMigrationCancellationForTest(databaseName, stage)
    }

    internal fun holdNextCompleteOpenCancellationForTest(): Int {
        checkUsable()
        return transport.holdNextCompleteOpenCancellationForTest()
    }

    internal suspend fun awaitCancellationHoldForTest(controlId: Int): Int {
        checkUsable()
        require(controlId > 0) { "Cancellation hold control ID must be positive." }
        return transport.awaitCancellationHoldForTest(controlId)
    }

    internal suspend fun pendingOpenCountForTest(): Int {
        checkUsable()
        return transport.pendingOpenCountForTest()
    }

    internal fun setMigrationHeapSamplesForTest(samples: List<Long>) {
        checkUsable()
        require(samples.size in 3..4) {
            "Migration heap test samples must contain three or four values."
        }
        require(samples.all { it in 0..9_007_199_254_740_991L }) {
            "Migration heap test samples must be non-negative JavaScript-safe integers."
        }
        transport.setMigrationHeapSamplesForTest(
            samples.joinToString(prefix = "[", postfix = "]"),
        )
    }

    internal fun seedMigrationMarkerForTest(databaseName: String, mode: String) {
        checkUsable()
        transport.seedMigrationMarkerForTest(databaseName, mode)
    }

    fun runtimeKind(): String = transport.runtimeKind()

    internal fun failCancellationCleanupForNextRequestForTest() {
        transport.setCancellationCleanupFailuresForTest(
            """{"requestId":$nextRequestId,"finalize":"statement finalization failed","rollback":"rollback failed"}""",
        )
    }

    internal fun failCancellationReconciliationForNextRequestForTest() {
        transport.setNegativeReconciliationForTest()
    }

    internal fun failShutdownCleanupForTest(
        finalize: String,
        rollback: String,
        close: String,
    ) {
        transport.setShutdownFailuresForTest(
            sqliteWorkerJson.encodeToString(
                mapOf(
                    "finalize" to finalize,
                    "rollback" to rollback,
                    "close" to close,
                ),
            ),
        )
    }

    internal fun holdNextActivePageForTest() {
        transport.holdActivePageForTest()
    }

    internal suspend fun awaitActivePageForTest() {
        transport.awaitActivePageForTest()
    }

    internal fun holdNextResponseForTest(command: String) {
        transport.setResponseModeForTest(
            sqliteWorkerJson.encodeToString(mapOf("command" to command, "mode" to "hold")),
        )
    }

    internal fun dropNextResponseForTest(command: String) {
        transport.setResponseModeForTest(
            sqliteWorkerJson.encodeToString(mapOf("command" to command, "mode" to "drop")),
        )
    }

    internal fun malformNextResponseForTest(command: String) {
        transport.setResponseModeForTest(
            sqliteWorkerJson.encodeToString(mapOf("command" to command, "mode" to "malformed")),
        )
    }

    internal fun omitNextResponseFieldForTest(
        command: String,
        field: String,
        error: Boolean = false,
    ) {
        val control = linkedMapOf(
            "command" to command,
            "mode" to "omit-field",
            "field" to field,
        )
        if (error) control["payload"] = "error"
        transport.setResponseModeForTest(
            sqliteWorkerJson.encodeToString(control),
        )
    }

    internal fun setAcknowledgementModeForTest(mode: String) {
        transport.setAcknowledgementModeForTest(mode)
    }

    internal fun setTerminationModeForTest(mode: String) {
        transport.setTerminationModeForTest(mode)
    }

    internal suspend fun forceTerminateForTest() {
        transport.forceTerminate()
    }

    internal fun failWorkerForTest(message: String) {
        transport.failWorkerForTest(message)
    }

    internal fun setNextRequestIdForTest(requestId: Int) {
        require(requestId > 0)
        nextRequestId = requestId
    }

    internal fun setNextOneWayIdForTest(requestId: Int) {
        transport.setNextOneWayIdForTest(requestId)
    }

    internal fun diagnosticsForTest(): String = transport.diagnosticsForTest()

    suspend fun shutdown(): SqliteWorkerMetrics {
        if (shutdown) {
            flushQueued()
            terminalFailure?.let { throw it }
            return checkNotNull(shutdownMetrics) {
                "SQLite worker shutdown completed without metrics."
            }
        }
        shutdown = true
        connections.toList().forEach(SqliteWorkerSQLiteConnection::close)

        var primary: Throwable? = captureFailure(null) { flushQueued() }
        val requestId = allocateShutdownRequestId()
        val request = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "shutdown",
        )
        var metrics: SqliteWorkerMetrics? = null
        try {
            val envelope = decodeEnvelope(
                runCleanupBarrier("shutdown") {
                    transport.shutdown(requestId, sqliteWorkerJson.encodeToString(request))
                },
            )
            envelope.validateFor(requestId, request)
            envelope.error?.let { throw it.toSqliteException() }
            metrics = checkNotNull(checkNotNull(envelope.data).metrics)
        } catch (failure: Throwable) {
            primary = appendFailure(primary, failure.normalizeWorkerFailure())
            primary = forceTerminateAfterFailure(checkNotNull(primary))
        }
        primary?.let { throw it }
        return checkNotNull(metrics).also { shutdownMetrics = it }
    }

    internal suspend fun <T> request(
        request: SqliteWorkerRequest,
        legacyBytes: ByteArray? = null,
        onDispatched: (() -> Unit)? = null,
        accept: (SqliteWorkerResponse) -> T,
    ): T {
        request.validate()
        require(
            legacyBytes == null ||
                (request.command == "completeOpen" && request.legacySourceStatus == "present"),
        ) {
            "Legacy bytes are valid only for completeOpen with a present source."
        }
        require(
            request.command != "completeOpen" ||
                request.legacySourceStatus != "present" ||
                legacyBytes != null,
        ) {
            "A present completeOpen source requires legacy bytes."
        }
        flushQueued()
        checkUsable()
        val requestId = allocateRequestId()
        return try {
            onDispatched?.invoke()
            val encoded = try {
                val requestJson = sqliteWorkerJson.encodeToString(request)
                if (legacyBytes == null) {
                    transport.request(requestId, requestJson)
                } else {
                    transport.requestWithLegacyBytes(requestId, requestJson, legacyBytes)
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (transportFailure: Throwable) {
                throw forceTerminateAfterFailure(transportFailure.normalizeWorkerFailure())
            }
            val envelope = try {
                decodeEnvelope(encoded).also { it.validateFor(requestId, request) }
            } catch (failure: Throwable) {
                throw forceTerminateAfterFailure(failure.normalizeWorkerFailure())
            }
            currentCoroutineContext().ensureActive()
            try {
                transport.acknowledge(requestId)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (acknowledgementFailure: Throwable) {
                throw forceTerminateAfterFailure(
                    acknowledgementFailure.normalizeWorkerFailure(),
                )
            }
            currentCoroutineContext().ensureActive()
            val workerFailure = envelope.error?.also { error ->
                updateTransactionStateFromError(request, error)
            }?.toSqliteException()
            val accepted = envelope.data?.let { response ->
                try {
                    accept(response)
                } catch (acceptanceFailure: Throwable) {
                    throw forceTerminateAfterFailure(
                        acceptanceFailure.normalizeWorkerFailure(),
                    )
                }
            }
            try {
                transport.release(requestId)
            } catch (releaseFailure: Throwable) {
                throw forceTerminateAfterFailure(releaseFailure.normalizeWorkerFailure())
            }
            workerFailure?.let { throw it }
            @Suppress("UNCHECKED_CAST")
            accepted as T
        } catch (cancelled: CancellationException) {
            reconcileCancelledRequest(requestId, request, cancelled)
            throw cancelled
        } catch (failure: Throwable) {
            throw failure.normalizeWorkerFailure()
        }
    }

    internal fun sendOneWay(request: SqliteWorkerRequest): Int {
        if (shutdown) return 0
        request.validate()
        val requestId = transport.sendOneWay(sqliteWorkerJson.encodeToString(request))
        require(requestId < 0) { "One-way SQLite worker request IDs must be negative." }
        check(queuedOneWays.put(requestId, request) == null) {
            "One-way SQLite worker request ID $requestId is already queued."
        }
        return requestId
    }

    internal fun onConnectionClosed(connection: SqliteWorkerSQLiteConnection) {
        connections -= connection
    }

    internal suspend fun forceCleanup() {
        shutdown = true
        markConnectionsForceClosed()
        var primary: Throwable? = null
        try {
            transport.forceTerminate()
        } catch (terminationFailure: Throwable) {
            primary = appendFailure(primary, terminationFailure.normalizeWorkerFailure())
        }
        try {
            flushQueued()
        } catch (queuedFailure: Throwable) {
            primary = appendFailure(primary, queuedFailure.normalizeWorkerFailure())
        }
        primary?.let { throw it }
    }

    internal suspend fun awaitCleanupDeadline() {
        transport.awaitCleanupDeadline()
    }

    private suspend fun flushQueued() {
        if (queuedOneWays.isEmpty()) return
        val callerContext = currentCoroutineContext()
        val expected = queuedOneWays.toMap()
        var queuedFailure: Throwable? = null
        var terminalCause: Throwable? = null
        try {
            val encoded = runCleanupBarrier("one-way flush") { transport.flush() }
            val batch = sqliteWorkerJson.decodeFromString<SqliteWorkerFlushBatch>(encoded)
            val seen = mutableSetOf<Int>()
            for (envelope in batch.envelopes) {
                val request = expected[envelope.id]
                    ?: throw SqliteException(
                        "Unexpected one-way SQLite worker response ID ${envelope.id}.",
                    )
                require(seen.add(envelope.id)) {
                    "Duplicate one-way SQLite worker response ID ${envelope.id}."
                }
                envelope.validateOneWayFor(envelope.id, request)
                envelope.error?.let { error ->
                    queuedFailure = appendFailure(queuedFailure, error.toSqliteException())
                }
            }
            for (message in batch.barrierFailures) {
                terminalCause = appendFailure(terminalCause, SqliteException(message))
            }
            if (batch.barrierFailures.isEmpty()) {
                val missing = expected.keys - seen
                require(missing.isEmpty()) {
                    "Missing one-way SQLite worker responses: ${missing.joinToString()}."
                }
            }
        } catch (failure: Throwable) {
            terminalCause = appendFailure(
                terminalCause,
                failure.normalizeWorkerFailure(),
            )
        } finally {
            expected.keys.forEach(queuedOneWays::remove)
        }
        if (terminalCause != null) {
            val terminal = forceTerminateAfterFailure(terminalCause)
            queuedFailure = appendFailure(queuedFailure, terminal)
        }
        try {
            callerContext.ensureActive()
        } catch (cancelled: CancellationException) {
            queuedFailure?.let(cancelled::addSuppressedIfDistinct)
            throw cancelled
        }
        queuedFailure?.let { throw it }
    }

    private suspend fun reconcileCancelledRequest(
        requestId: Int,
        request: SqliteWorkerRequest,
        cancelled: CancellationException,
    ) {
        val envelope = try {
            runCleanupBarrier("cancellation reconciliation") {
                decodeEnvelope(transport.cancel(requestId))
            }
        } catch (cleanupFailure: Throwable) {
            val terminal = forceTerminateAfterFailure(
                cleanupFailure.normalizeWorkerFailure(),
            )
            cancelled.addSuppressedIfDistinct(terminal)
            return
        }
        val error = try {
            envelope.validateFor(requestId, request)
            checkNotNull(envelope.error) {
                "Cancellation reconciliation must return a worker error."
            }
        } catch (protocolFailure: Throwable) {
            val terminal = forceTerminateAfterFailure(
                protocolFailure.normalizeWorkerFailure(),
            )
            cancelled.addSuppressedIfDistinct(terminal)
            return
        }
        val reconciliationFailure = runCatching {
            require(error.cancelled) {
                "SQLite worker cancellation reconciliation was not authoritative."
            }
            if (request.databaseId != null) {
                requireNotNull(error.inTransaction) {
                    "SQLite worker cancellation reconciliation requires transaction state."
                }
            } else {
                require(error.inTransaction == null) {
                    "SQLite worker cancellation reconciliation returned unexpected transaction state."
                }
            }
        }.exceptionOrNull()
        if (reconciliationFailure != null) {
            val normalized = if (!error.cancelled) {
                error.toSqliteException()
            } else {
                reconciliationFailure.normalizeWorkerFailure()
            }
            val terminal = forceTerminateAfterFailure(normalized)
            cancelled.addSuppressedIfDistinct(terminal)
            return
        }
        error.inTransaction?.let { inTransaction ->
            request.databaseId?.let { databaseId ->
                connections.firstOrNull { it.databaseId == databaseId }
                    ?.updateTransactionState(inTransaction)
            }
        }
        if (error.suppressed.isEmpty()) {
            return
        }
        val protocolFailure = SqliteWorkerException(error)
        error.suppressed.forEach { message ->
            cancelled.addSuppressedIfDistinct(SqliteException(message, protocolFailure))
        }
    }

    private fun allocateRequestId(): Int {
        if (nextRequestId >= Int.MAX_VALUE) {
            throw SqliteException(
                "SQLite worker request IDs are exhausted; close the connection to start a new worker.",
            )
        }
        return nextRequestId++
    }

    private fun allocateShutdownRequestId(): Int {
        if (nextRequestId !in 1..Int.MAX_VALUE) {
            throw SqliteException("SQLite worker terminal request ID is unavailable.")
        }
        return nextRequestId
    }

    private suspend fun <T> runCleanupBarrier(
        operation: String,
        block: suspend () -> T,
    ): T {
        try {
            return withContext(NonCancellable) { block() }
        } catch (failure: Throwable) {
            if (!failure.message.orEmpty().contains("cleanup deadline")) throw failure
            throw forceTerminateAfterFailure(
                SqliteException(
                    "SQLite worker $operation exceeded the " +
                        "${cleanupTimeoutMillis}ms cleanup deadline.",
                    failure,
                ),
            )
        }
    }

    private suspend fun forceTerminateAfterFailure(primary: Throwable): Throwable {
        val terminal = terminalFailure ?: primary.also { terminalFailure = it }
        if (terminal !== primary) terminal.addSuppressedIfDistinct(primary)
        shutdown = true
        markConnectionsForceClosed()
        try {
            withContext(NonCancellable) { transport.forceTerminate() }
        } catch (terminationFailure: Throwable) {
            terminal.addSuppressedIfDistinct(terminationFailure.normalizeWorkerFailure())
        }
        return terminal
    }

    private fun checkUsable() {
        terminalFailure?.let { throw it }
        check(!shutdown) { "SQLite worker driver is shut down." }
    }

    private fun markConnectionsForceClosed() {
        connections.toList().forEach(SqliteWorkerSQLiteConnection::markForceClosed)
        connections.clear()
    }

    private fun updateTransactionStateFromError(
        request: SqliteWorkerRequest,
        error: SqliteWorkerError,
    ) {
        val inTransaction = error.inTransaction ?: return
        val databaseId = request.databaseId ?: return
        connections.firstOrNull { it.databaseId == databaseId }
            ?.updateTransactionState(inTransaction)
    }

    private fun decodeEnvelope(encoded: String): SqliteWorkerEnvelope =
        sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(encoded).also { it.validate() }

    companion object {
        suspend fun create(
            config: SqliteWorkerConfig = SqliteWorkerConfig(),
            workerModuleUrl: String? = null,
            startupModeForTest: String = "normal",
            cleanupTimeoutMillis: Int = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
        ): SqliteWorkerSQLiteDriver {
            config.validate()
            require(cleanupTimeoutMillis > 0) { "Worker cleanup timeout must be positive." }
            return SqliteWorkerSQLiteDriver(
                config = config,
                transport = SqliteWorkerTransport.create(
                    configJson = sqliteWorkerJson.encodeToString(config),
                    workerModuleUrl = workerModuleUrl,
                    startupModeForTest = startupModeForTest,
                    cleanupTimeoutMillis = cleanupTimeoutMillis,
                ),
                cleanupTimeoutMillis = cleanupTimeoutMillis,
            )
        }
    }
}

@Suppress("NotCloseable")
internal class SqliteWorkerSQLiteConnection(
    private val driver: SqliteWorkerSQLiteDriver,
    internal val databaseId: Int,
    private val config: SqliteWorkerConfig,
) : SQLiteConnection, SuspendSQLiteConnectionCleanup {
    private var closed = false
    private var transactionActive = false
    private val statements = mutableSetOf<SqliteWorkerSQLiteStatement>()

    override fun inTransaction(): Boolean {
        checkOpen()
        return transactionActive
    }

    override suspend fun prepare(sql: String): SQLiteStatement {
        checkOpen()
        return driver.request(
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "prepare",
                databaseId = databaseId,
                sql = sql,
            ),
        ) { response ->
            SqliteWorkerSQLiteStatement(
                connection = this,
                driver = driver,
                statementId = checkNotNull(response.statementId),
                sql = sql,
                columnNames = checkNotNull(response.columnNames),
                config = config,
            ).also {
                statements += it
                transactionActive = response.inTransaction
            }
        }
    }

    override fun close() {
        if (closed) return
        closed = true
        statements.toList().forEach(SqliteWorkerSQLiteStatement::close)
        driver.sendOneWay(
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "closeDatabase",
                databaseId = databaseId,
            ),
        )
        driver.onConnectionClosed(this)
    }

    override suspend fun awaitCleanup() {
        driver.shutdown()
    }

    override val cleanupTimeoutMillis: Int
        get() = driver.cleanupTimeoutMillis

    override suspend fun awaitCleanupDeadline() {
        driver.awaitCleanupDeadline()
    }

    override suspend fun forceCleanup() {
        driver.forceCleanup()
    }

    internal fun updateTransactionState(active: Boolean) {
        transactionActive = active
    }

    internal fun holdNextResponseForTest(command: String) {
        driver.holdNextResponseForTest(command)
    }

    internal fun setAcknowledgementModeForTest(mode: String) {
        driver.setAcknowledgementModeForTest(mode)
    }

    internal fun holdNextActivePageForTest() = driver.holdNextActivePageForTest()

    internal suspend fun awaitActivePageForTest() {
        driver.awaitActivePageForTest()
    }

    internal fun diagnosticsForTest(): String = driver.diagnosticsForTest()

    internal suspend fun metricsForTest(): SqliteWorkerMetrics = driver.metrics()

    internal fun onStatementClosed(statement: SqliteWorkerSQLiteStatement) {
        statements -= statement
    }

    internal fun markForceClosed() {
        closed = true
        transactionActive = false
        statements.toList().forEach(SqliteWorkerSQLiteStatement::markForceClosed)
        statements.clear()
    }

    private fun checkOpen() {
        check(!closed) { "SQLite worker connection is closed." }
    }
}

@Suppress("NotCloseable")
internal class SqliteWorkerSQLiteStatement(
    private val connection: SqliteWorkerSQLiteConnection,
    private val driver: SqliteWorkerSQLiteDriver,
    private val statementId: Int,
    private val sql: String,
    private val columnNames: List<String>,
    private val config: SqliteWorkerConfig,
) : SQLiteStatement {
    private val bindings = mutableMapOf<Int, SqliteWorkerValue>()
    private var closed = false
    private var done = false
    private var rows: List<List<SqliteWorkerValue>> = emptyList()
    private var rowIndex = -1

    override fun bindBlob(index: Int, value: ByteArray) {
        bind(index, SqliteWorkerValue.blob(value))
    }

    override fun bindDouble(index: Int, value: Double) {
        bind(index, SqliteWorkerValue.real(value))
    }

    override fun bindLong(index: Int, value: Long) {
        bind(index, SqliteWorkerValue.integer(value))
    }

    override fun bindText(index: Int, value: String) {
        bind(index, SqliteWorkerValue.text(value))
    }

    override fun bindNull(index: Int) {
        bind(index, SqliteWorkerValue.nullValue())
    }

    override fun getBlob(index: Int): ByteArray {
        val value = currentValue(index)
        if (value.type != "blob") {
            throw SqliteException("Column $index is not a blob (type=${value.type})")
        }
        return value.blob.orEmpty().map(Int::toByte).toByteArray()
    }

    override fun getDouble(index: Int): Double =
        convertValue(index, "Double") { value ->
            when (value.type) {
                "integer" -> checkNotNull(value.integer).toDouble()
                "real" -> checkNotNull(value.real)
                "text" -> checkNotNull(value.text).toDouble()
                else -> throw SqliteException(
                    "Column $index cannot convert to Double (type=${value.type})",
                )
            }
        }

    override fun getLong(index: Int): Long =
        convertValue(index, "Long") { value ->
            when (value.type) {
                "integer" -> checkNotNull(value.integer).toLong()
                "real" -> checkNotNull(value.real).toLong()
                "text" -> checkNotNull(value.text).toLong()
                else -> throw SqliteException(
                    "Column $index cannot convert to Long (type=${value.type})",
                )
            }
        }

    override fun getText(index: Int): String =
        convertValue(index, "Text") { value ->
            when (value.type) {
                "integer" -> checkNotNull(value.integer)
                "real" -> checkNotNull(value.real).toString()
                "text" -> checkNotNull(value.text)
                else -> throw SqliteException(
                    "Column $index cannot convert to Text (type=${value.type})",
                )
            }
        }

    override fun isNull(index: Int): Boolean = currentValue(index).type == "null"

    override fun getColumnCount(): Int = columnNames.size

    override fun getColumnName(index: Int): String = columnNames[index]

    override fun getColumnType(index: Int): Int =
        when (currentValue(index).type) {
            "integer" -> SQLITE_DATA_INTEGER
            "real" -> SQLITE_DATA_FLOAT
            "text" -> SQLITE_DATA_TEXT
            "blob" -> SQLITE_DATA_BLOB
            "null" -> SQLITE_DATA_NULL
            else -> error("Unknown SQLite worker storage class.")
        }

    override suspend fun step(): Boolean {
        checkOpen()
        if (rowIndex + 1 < rows.size) {
            rowIndex++
            return true
        }
        if (done) return false

        var dispatched = false
        return try {
            driver.request(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "page",
                    databaseId = connection.databaseId,
                    statementId = statementId,
                    sql = sql,
                    bindings = bindings,
                    pageRows = config.pageRows,
                    pageBytes = config.pageBytes,
                ),
                onDispatched = { dispatched = true },
            ) { response ->
                rows = checkNotNull(response.rows)
                rowIndex = if (rows.isEmpty()) -1 else 0
                done = response.done
                connection.updateTransactionState(response.inTransaction)
                rows.isNotEmpty()
            }
        } catch (cancelled: CancellationException) {
            if (dispatched) discardAfterRemoteFailure()
            throw cancelled
        } catch (failure: Throwable) {
            if (dispatched) discardAfterRemoteFailure()
            throw failure
        }
    }

    override fun reset() {
        checkOpen()
        discardCachedRows()
        driver.sendOneWay(statementRequest("reset"))
    }

    override fun clearBindings() {
        checkOpen()
        bindings.clear()
        discardCachedRows()
        driver.sendOneWay(statementRequest("clearBindings"))
    }

    override fun close() {
        if (closed) return
        closed = true
        discardCachedRows()
        driver.sendOneWay(statementRequest("closeStatement"))
        connection.onStatementClosed(this)
    }

    private fun bind(index: Int, value: SqliteWorkerValue) {
        checkOpen()
        require(index >= 1) { "SQLite bind indices are one-based." }
        bindings[index] = value
    }

    private fun statementRequest(command: String) =
        SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = command,
            databaseId = connection.databaseId,
            statementId = statementId,
            sql = sql,
        )

    private fun discardCachedRows() {
        rows = emptyList()
        rowIndex = -1
        done = false
    }

    private fun discardAfterRemoteFailure() {
        if (closed) return
        closed = true
        discardCachedRows()
        connection.onStatementClosed(this)
    }

    internal fun markForceClosed() {
        closed = true
        discardCachedRows()
    }

    private fun currentValue(index: Int): SqliteWorkerValue {
        checkOpen()
        check(rowIndex in rows.indices) { "No current SQLite row." }
        return rows[rowIndex][index]
    }

    private inline fun <T> convertValue(
        index: Int,
        targetType: String,
        conversion: (SqliteWorkerValue) -> T,
    ): T {
        val value = currentValue(index)
        if (value.type == "null") throw SqliteException("Column $index is NULL")
        return try {
            conversion(value)
        } catch (failure: Throwable) {
            if (failure is SqliteException) throw failure
            throw SqliteException(
                "Column $index cannot convert to $targetType (type=${value.type})",
                failure,
            )
        }
    }

    private fun checkOpen() {
        check(!closed) { "SQLite worker statement is closed." }
    }
}

internal class SqliteWorkerException(
    val protocolError: SqliteWorkerError,
) : RuntimeException(
    buildString {
        append("SQLite worker ")
        append(protocolError.operation)
        protocolError.sql?.let { sql ->
            append(" [")
            append(sql)
            append(']')
        }
        append(": ")
        append(protocolError.message)
    },
) {
    init {
        protocolError.suppressed.forEach { message ->
            addSuppressed(SqliteException(message, this))
        }
    }
}

private fun SqliteWorkerError.toSqliteException(): SqliteException =
    SqliteWorkerException(this).toSqliteExceptionPreservingSuppressed()

internal fun Throwable.normalizeWorkerFailure(): Throwable =
    when (this) {
        is CancellationException, is SqliteException -> this
        else -> toSqliteExceptionPreservingSuppressed()
    }

private fun Throwable.addSuppressedIfDistinct(additional: Throwable) {
    if (additional !== this && suppressedExceptions.none { it === additional }) {
        addSuppressed(additional)
    }
}

private suspend fun captureFailure(
    primary: Throwable?,
    block: suspend () -> Unit,
): Throwable? =
    try {
        block()
        primary
    } catch (failure: Throwable) {
        appendFailure(primary, failure)
    }

private fun appendFailure(primary: Throwable?, additional: Throwable): Throwable {
    if (primary == null) return additional
    if (primary !== additional) primary.addSuppressed(additional)
    return primary
}
