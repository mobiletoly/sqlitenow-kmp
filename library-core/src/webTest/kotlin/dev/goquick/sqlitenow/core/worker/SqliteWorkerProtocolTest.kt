package dev.goquick.sqlitenow.core.worker

import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

class SqliteWorkerProtocolTest {
    @Test
    fun phase5bOpenAndCompleteOpenVariantsAreStrict() {
        val builtInOpen = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "open",
            fileName = "phase5b-built-in",
            legacySourceMode = "built-in",
        )
        val customOpen = builtInOpen.copy(
            fileName = "phase5b-custom",
            legacySourceMode = "custom",
        )
        val completePresent = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "completeOpen",
            openId = 17,
            legacySourceStatus = "present",
        )
        val completeAbsent = completePresent.copy(legacySourceStatus = "absent")

        listOf(builtInOpen, customOpen, completePresent, completeAbsent).forEach {
            it.validate()
        }
        assertEquals(
            """{"protocol":"$SQLITE_WORKER_PROTOCOL","command":"open","fileName":"phase5b-custom","legacySourceMode":"custom"}""",
            sqliteWorkerJson.encodeToString(customOpen),
        )
        assertEquals(
            """{"protocol":"$SQLITE_WORKER_PROTOCOL","command":"completeOpen","openId":17,"legacySourceStatus":"present"}""",
            sqliteWorkerJson.encodeToString(completePresent),
        )

        listOf(
            builtInOpen.copy(legacySourceMode = null),
            builtInOpen.copy(legacySourceMode = "snapshot"),
            completePresent.copy(openId = null),
            completePresent.copy(openId = 0),
            completePresent.copy(legacySourceStatus = null),
            completePresent.copy(legacySourceStatus = "empty"),
            completePresent.copy(fileName = "cross-database"),
        ).forEach { request ->
            assertFailsWith<IllegalArgumentException>(request.toString()) {
                request.validate()
            }
        }

        SqliteWorkerEnvelope(
            id = 1,
            data = SqliteWorkerResponse(
                protocol = SQLITE_WORKER_PROTOCOL,
                openState = "legacy-source-required",
                openId = 17,
                runtimeKind = "browser-worker",
                sqliteVersion = "3.53.0",
            ),
        ).validateFor(1, customOpen)

        listOf(
            SqliteWorkerResponse(
                protocol = SQLITE_WORKER_PROTOCOL,
                openState = "legacy-source-required",
                runtimeKind = "browser-worker",
                sqliteVersion = "3.53.0",
            ),
            SqliteWorkerResponse(
                protocol = SQLITE_WORKER_PROTOCOL,
                openState = "opened",
                databaseId = 1,
                openId = 17,
                runtimeKind = "browser-worker",
                sqliteVersion = "3.53.0",
            ),
        ).forEach { response ->
            assertFailsWith<IllegalArgumentException>(response.toString()) {
                SqliteWorkerEnvelope(id = 1, data = response).validateFor(1, customOpen)
            }
        }
    }

    @Test
    fun productionPeerRejectsProtocolAndCommandDriftBeforeAllocatingHandles() = runTest {
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        var requestId = 1
        try {
            val hostileRequests = listOf(
                """{"command":"open","fileName":":memory:"}""",
                """{"protocol":"sqlitenow-sqlite-worker-v0","command":"open","fileName":":memory:"}""",
                """{"protocol":"sqlitenow-sqlite-worker-v2","command":"open","fileName":":memory:"}""",
                """{"protocol":"<script>sqlitenow-sqlite-worker-v1</script>","command":"open","fileName":":memory:"}""",
                """{"protocol":"sqlitenow-sqlite-worker-v1","command":"migrateLegacy","fileName":":memory:"}""",
                """{"protocol":"sqlitenow-sqlite-worker-v1","command":"open","fileName":":memory:","bindings":{"1":{"type":"integer","integer":"1"}}}""",
                """{"protocol":"sqlitenow-sqlite-worker-v1","command":"metrics","fileName":":memory:"}""",
                """{"protocol":"sqlitenow-sqlite-worker-v1","command":"page","databaseId":1,"statementId":1,"sql":"SELECT ?1","bindings":{"1":{"type":"null","unexpected":1}},"pageRows":1,"pageBytes":1024}""",
                """{"protocol":"sqlitenow-sqlite-worker-v1","command":"page","databaseId":1,"statementId":1,"sql":"SELECT ?1","bindings":{"01":{"type":"integer","integer":"1"}},"pageRows":1,"pageBytes":1024}""",
                """{"protocol":"sqlitenow-sqlite-worker-v1","command":"page","databaseId":1,"statementId":1,"sql":"SELECT ?1","bindings":{"1e0":{"type":"integer","integer":"1"}},"pageRows":1,"pageBytes":1024}""",
            )
            hostileRequests.forEach { request ->
                val currentRequestId = requestId++
                val envelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                    transport.request(currentRequestId, request),
                )
                assertTrue(envelope.error != null, request)
                transport.acknowledge(currentRequestId)
                transport.release(currentRequestId)
            }

            val metricsRequest = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "metrics",
            )
            val metricsRequestId = requestId++
            val metricsEnvelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                transport.request(metricsRequestId, sqliteWorkerJson.encodeToString(metricsRequest)),
            )
            transport.acknowledge(metricsRequestId)
            transport.release(metricsRequestId)
            val metrics = checkNotNull(checkNotNull(metricsEnvelope.data).metrics)
            assertEquals(0, metrics.liveDatabases)
            assertEquals(0, metrics.liveStatements)
            assertEquals(1, metrics.requestsStarted)
        } finally {
            val shutdown = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "shutdown",
            )
            transport.shutdown(requestId, sqliteWorkerJson.encodeToString(shutdown))
        }
    }

    @Test
    fun explicitNullBindingsAreRejectedWithoutDispatchAfterReset() = runTest {
        val databaseName = nextWorkerProtocolDatabaseName("explicit-null")
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        var requestId = 1
        suspend fun request(request: SqliteWorkerRequest): SqliteWorkerResponse {
            val id = requestId++
            val envelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                transport.request(id, sqliteWorkerJson.encodeToString(request)),
            )
            transport.acknowledge(id)
            transport.release(id)
            envelope.error?.let { workerError -> error(workerError.message) }
            return checkNotNull(envelope.data)
        }
        try {
            val databaseId = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "open",
                        fileName = databaseName,
                        legacySourceMode = transport.rawOpenLegacySourceMode(),
                    ),
                ).databaseId,
            )
            val sql = "SELECT ?1"
            val statementId = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "prepare",
                        databaseId = databaseId,
                        sql = sql,
                    ),
                ).statementId,
            )
            request(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "page",
                    databaseId = databaseId,
                    statementId = statementId,
                    sql = sql,
                    bindings = mapOf(1 to SqliteWorkerValue.integer(47)),
                    pageRows = 1,
                    pageBytes = 1024,
                ),
            )
            transport.sendOneWay(
                sqliteWorkerJson.encodeToString(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "reset",
                        databaseId = databaseId,
                        statementId = statementId,
                        sql = sql,
                    ),
                ),
            )
            transport.flush()
            val pageRequestsBefore = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "metrics",
                    ),
                ).metrics,
            ).pageRequests

            val nullBindingRequestId = requestId++
            val failure = assertFailsWith<Throwable> {
                transport.request(
                    nullBindingRequestId,
                    """{"protocol":"$SQLITE_WORKER_PROTOCOL","command":"page","databaseId":$databaseId,"statementId":$statementId,"sql":"SELECT ?1","bindings":null,"pageRows":1,"pageBytes":1024}""",
                )
            }
            assertTrue(failure.message.orEmpty().contains("bindings"))

            val pageRequestsAfter = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "metrics",
                    ),
                ).metrics,
            ).pageRequests
            assertEquals(pageRequestsBefore, pageRequestsAfter)
        } finally {
            val shutdown = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "shutdown",
            )
            transport.shutdown(requestId, sqliteWorkerJson.encodeToString(shutdown))
            cleanupWorkerProtocolDatabase(databaseName)
        }
    }

    @Test
    fun requestResponseErrorMetricAndEnvelopeJsonAreLocked() {
        val request = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "page",
            databaseId = 1,
            statementId = 7,
            sql = "SELECT ?, ?, ?, ?, ?",
            bindings = mapOf(
                1 to SqliteWorkerValue.integer(Long.MAX_VALUE),
                2 to SqliteWorkerValue.real(12.5),
                3 to SqliteWorkerValue.text("worker-boundary"),
                4 to SqliteWorkerValue.blob(byteArrayOf(0, 1, 127, -1)),
                5 to SqliteWorkerValue.nullValue(),
            ),
            pageRows = 64,
            pageBytes = 65536,
        )
        request.validate()
        assertEquals(
            """{"protocol":"sqlitenow-sqlite-worker-v1","command":"page","databaseId":1,"statementId":7,"sql":"SELECT ?, ?, ?, ?, ?","bindings":{"1":{"type":"integer","integer":"9223372036854775807"},"2":{"type":"real","real":12.5},"3":{"type":"text","text":"worker-boundary"},"4":{"type":"blob","blob":[0,1,127,255]},"5":{"type":"null"}},"pageRows":64,"pageBytes":65536}""",
            sqliteWorkerJson.encodeToString(request),
        )

        val metrics = SqliteWorkerMetrics(
            runtimeKind = "js-node-worker",
            sqliteVersion = "3.53.0",
            storageMode = "memory",
            requestsStarted = 4,
            requestsCompleted = 3,
            pendingRequests = 1,
            liveDatabases = 1,
            liveStatements = 1,
            transferredRows = 1,
            transferredBytes = 56,
            maxPageRows = 1,
            maxPageBytes = 56,
            migrationImportedUserVersion = -1,
            workerStarts = 1,
        )
        val response = SqliteWorkerResponse(
            protocol = SQLITE_WORKER_PROTOCOL,
            rows = listOf(listOf(SqliteWorkerValue.integer(Long.MIN_VALUE))),
            done = true,
            pageRows = 1,
            pageBytes = 56,
            metrics = metrics,
        )
        val envelope = SqliteWorkerEnvelope(id = 41, data = response)
        envelope.validate()
        assertEquals(
            """{"id":41,"data":{"protocol":"sqlitenow-sqlite-worker-v1","rows":[[{"type":"integer","integer":"-9223372036854775808"}]],"done":true,"oversizedRow":false,"pageRows":1,"pageBytes":56,"inTransaction":false,"metrics":{"runtimeKind":"js-node-worker","sqliteVersion":"3.53.0","storageMode":"memory","requestsStarted":4,"requestsCompleted":3,"requestsCancelled":0,"pendingRequests":1,"liveDatabases":1,"liveStatements":1,"transactionsRolledBackOnCancel":0,"integerBindingsAsStrings":0,"integerResultsAsStrings":0,"integerNumberViolations":0,"pageRequests":0,"steppedRows":0,"encodedRows":0,"transferredRows":1,"transferredBytes":56,"maxPageRows":1,"maxPageBytes":56,"oversizedRows":0,"snapshotExports":0,"migrationSourceKind":"","migrationSourceBytes":0,"migrationDurationMillis":0,"migrationPeakOwnedBytes":0,"migrationTargetFileName":"","migrationSourceSha256":"","migrationIntegrityCheck":"","migrationImportedUserVersion":-1,"migrationSourceRetained":false,"migrationHeapAvailable":false,"migrationHeapStartBytes":0,"migrationHeapPeakBytes":0,"migrationHeapEndBytes":0,"workerStarts":1,"workerStops":0}}}""",
            sqliteWorkerJson.encodeToString(envelope),
        )

        val error = SqliteWorkerEnvelope(
            id = 42,
            error = SqliteWorkerError(
                operation = "page",
                message = "interrupted",
                sql = "SELECT 1",
                sqliteCode = 9,
                cancelled = true,
                suppressed = listOf("rollback failed", "statement close failed"),
            ),
        )
        error.validate()
        assertEquals(
            """{"id":42,"error":{"operation":"page","message":"interrupted","sql":"SELECT 1","sqliteCode":9,"cancelled":true,"suppressed":["rollback failed","statement close failed"]}}""",
            sqliteWorkerJson.encodeToString(error),
        )
    }

    @Test
    fun integralRealWireLiteralKeepsExactWorkerPageByteAccounting() {
        val request = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "page",
            databaseId = 1,
            statementId = 1,
            sql = "SELECT CAST(4 AS REAL)",
            pageRows = 1,
            pageBytes = 1024,
        )
        val encoded =
            """{"id":1,"data":{"protocol":"$SQLITE_WORKER_PROTOCOL","rows":[[{"type":"real","real":4}]],"done":true,"oversizedRow":false,"pageRows":1,"pageBytes":28,"inTransaction":false}}"""
        val envelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(encoded)

        envelope.validateFor(1, request)
        assertEquals(4.0, envelope.data?.rows?.single()?.single()?.real)
    }

    @Test
    fun awaitedResponsesAreValidatedAgainstTheirCommandAndRequestId() {
        val open = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "open",
            fileName = ":memory:",
            legacySourceMode = "none",
        )
        val prepare = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "prepare",
            databaseId = 1,
            sql = "SELECT 1",
        )
        val page = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "page",
            databaseId = 1,
            statementId = 1,
            sql = "SELECT 1",
            pageRows = 1,
            pageBytes = 1024,
        )
        val metrics = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "metrics",
        )
        listOf(
            ResponseValidationScenario(
                name = "mismatched request ID",
                request = open,
                envelope = SqliteWorkerEnvelope(
                    id = 2,
                    data = SqliteWorkerResponse(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        databaseId = 1,
                        openState = "opened",
                        runtimeKind = "worker",
                        sqliteVersion = "3.53.0",
                    ),
                ),
            ),
            ResponseValidationScenario(
                name = "non-positive open handle",
                request = open,
                envelope = SqliteWorkerEnvelope(
                    id = 1,
                    data = SqliteWorkerResponse(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        databaseId = 0,
                        openState = "opened",
                        runtimeKind = "worker",
                        sqliteVersion = "3.53.0",
                    ),
                ),
            ),
            ResponseValidationScenario(
                name = "missing prepare handle",
                request = prepare,
                envelope = SqliteWorkerEnvelope(
                    id = 1,
                    data = SqliteWorkerResponse(protocol = SQLITE_WORKER_PROTOCOL),
                ),
            ),
            ResponseValidationScenario(
                name = "inexact page byte count",
                request = page,
                envelope = SqliteWorkerEnvelope(
                    id = 1,
                    data = SqliteWorkerResponse(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        done = true,
                        pageBytes = 1,
                    ),
                ),
            ),
            ResponseValidationScenario(
                name = "missing metrics",
                request = metrics,
                envelope = SqliteWorkerEnvelope(
                    id = 1,
                    data = SqliteWorkerResponse(protocol = SQLITE_WORKER_PROTOCOL),
                ),
            ),
            ResponseValidationScenario(
                name = "wrong error operation",
                request = prepare,
                envelope = SqliteWorkerEnvelope(
                    id = 1,
                    error = SqliteWorkerError(
                        operation = "page",
                        message = "wrong operation",
                    ),
                ),
            ),
        ).forEach { scenario ->
            assertFailsWith<IllegalArgumentException>(scenario.name) {
                scenario.envelope.validateFor(1, scenario.request)
            }
        }

        SqliteWorkerEnvelope(
            id = 1,
            data = SqliteWorkerResponse(
                protocol = SQLITE_WORKER_PROTOCOL,
                rows = emptyList(),
                done = true,
                pageBytes = SQLITE_WORKER_MIN_PAGE_BYTES,
            ),
        ).validateFor(1, page)
    }

    @Test
    fun mandatoryResponseMetricAndErrorFieldsRejectAbsenceButAcceptEmptyValues() {
        val completeResponse = sqliteWorkerJson.parseToJsonElement(
            """{"protocol":"$SQLITE_WORKER_PROTOCOL","done":false,"oversizedRow":false,"pageRows":0,"pageBytes":0,"inTransaction":false}""",
        ).jsonObject
        listOf("done", "oversizedRow", "pageRows", "pageBytes", "inTransaction").forEach { field ->
            assertFailsWith<SerializationException>(field) {
                sqliteWorkerJson.decodeFromString<SqliteWorkerResponse>(
                    JsonObject(completeResponse - field).toString(),
                )
            }
        }

        val completeMetrics = sqliteWorkerJson.parseToJsonElement(
            sqliteWorkerJson.encodeToString(
                SqliteWorkerMetrics(
                    runtimeKind = "worker",
                    sqliteVersion = "3.53.0",
                    storageMode = "memory",
                ),
            ),
        ).jsonObject
        completeMetrics.keys.forEach { field ->
            assertFailsWith<SerializationException>("metrics.$field") {
                sqliteWorkerJson.decodeFromString<SqliteWorkerMetrics>(
                    JsonObject(completeMetrics - field).toString(),
                )
            }
        }

        listOf("operation", "message", "cancelled", "suppressed").forEach { field ->
            val completeError = sqliteWorkerJson.parseToJsonElement(
                """{"operation":"page","message":"","cancelled":false,"suppressed":[]}""",
            ).jsonObject
            assertFailsWith<SerializationException>("error.$field") {
                sqliteWorkerJson.decodeFromString<SqliteWorkerError>(
                    JsonObject(completeError - field).toString(),
                )
            }
        }
        listOf("sql", "sqliteCode", "inTransaction").forEach { field ->
            assertFailsWith<SerializationException>("explicit null $field") {
                sqliteWorkerJson.decodeFromString<SqliteWorkerError>(
                    """{"operation":"page","message":"","cancelled":false,"suppressed":[],"$field":null}""",
                )
            }
        }

        val prepareRequest = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "prepare",
            databaseId = 1,
            sql = "SELECT 1",
        )
        assertFailsWith<IllegalArgumentException>("missing prepare columnNames") {
            SqliteWorkerEnvelope(
                id = 1,
                data = sqliteWorkerJson.decodeFromString(
                    """{"protocol":"$SQLITE_WORKER_PROTOCOL","statementId":1,"done":false,"oversizedRow":false,"pageRows":0,"pageBytes":0,"inTransaction":false}""",
                ),
            ).validateFor(1, prepareRequest)
        }

        val pageRequest = SqliteWorkerRequest(
            protocol = SQLITE_WORKER_PROTOCOL,
            command = "page",
            databaseId = 1,
            statementId = 1,
            sql = "SELECT 1",
            pageRows = 1,
            pageBytes = 1024,
        )
        assertFailsWith<IllegalArgumentException>("missing page rows") {
            SqliteWorkerEnvelope(
                id = 1,
                data = sqliteWorkerJson.decodeFromString(
                    """{"protocol":"$SQLITE_WORKER_PROTOCOL","done":true,"oversizedRow":false,"pageRows":0,"pageBytes":2,"inTransaction":false}""",
                ),
            ).validateFor(1, pageRequest)
        }
        SqliteWorkerEnvelope(
            id = 1,
            data = sqliteWorkerJson.decodeFromString(
                """{"protocol":"$SQLITE_WORKER_PROTOCOL","rows":[],"done":true,"oversizedRow":false,"pageRows":0,"pageBytes":2,"inTransaction":false}""",
            ),
        ).validateFor(1, pageRequest)
        sqliteWorkerJson.decodeFromString<SqliteWorkerError>(
            """{"operation":"page","message":"","cancelled":false,"suppressed":[]}""",
        )
    }

    @Test
    fun absentOlderNewerMalformedAndHostileProtocolIdsAreRejected() {
        assertFailsWith<SerializationException> {
            sqliteWorkerJson.decodeFromString<SqliteWorkerRequest>(
                """{"command":"metrics"}""",
            )
        }
        listOf(
            "sqlitenow-sqlite-worker-v0",
            "sqlitenow-sqlite-worker-v2",
            "",
            "v1",
            "sqlitenow-sqlite-worker-v1\u0000ignored",
            "<script>sqlitenow-sqlite-worker-v1</script>",
            "sqlitenow-sqlite-worker-v1".repeat(256),
        ).forEach { protocol ->
            assertFailsWith<IllegalArgumentException>(protocol) {
                SqliteWorkerRequest(protocol = protocol, command = "metrics").validate()
            }
        }
    }

    @Test
    fun commandAllowListRejectsProofAndLaterPhaseCommands() {
        sqliteWorkerCommands.forEach { command ->
            assertTrue(command.isNotBlank())
        }
        listOf(
            "migrateLegacy",
            "seedLegacySource",
            "cleanupProofStorage",
            "delay",
            "injectCloseFailures",
            "directVfs",
            "healthMarker",
            "importSnapshot",
        ).forEach { command ->
            assertFailsWith<IllegalArgumentException>(command) {
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = command,
                ).validate()
            }
        }
        assertFailsWith<IllegalArgumentException> {
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "open",
                fileName = ":memory:",
                bindings = mapOf(1 to SqliteWorkerValue.integer(1)),
                legacySourceMode = "none",
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "metrics",
                fileName = ":memory:",
            ).validate()
        }
    }

    @Test
    fun signedLongScenariosUseCanonicalDecimalText() {
        sqliteWorkerIntegerScenarios.forEach { scenario ->
            val encoded = SqliteWorkerValue.integer(scenario.value)
            encoded.validate()
            assertEquals(scenario.value.toString(), encoded.integer, scenario.name)
            assertEquals(scenario.value, encoded.integer!!.toLong(), scenario.name)
        }
        listOf("", "+1", "-0", "00", "01", "-01", "9223372036854775808", "-9223372036854775809")
            .forEach { invalid ->
                assertFailsWith<IllegalArgumentException>(invalid) {
                    SqliteWorkerValue(type = "integer", integer = invalid).validate()
                }
            }
    }

    @Test
    fun taggedValuesAndPageLimitsAreStrict() {
        listOf(
            SqliteWorkerValue.nullValue(),
            SqliteWorkerValue.integer(1),
            SqliteWorkerValue.real(1.25),
            SqliteWorkerValue.text("text"),
            SqliteWorkerValue.blob(byteArrayOf(0, -1)),
        ).forEach(SqliteWorkerValue::validate)

        assertFailsWith<IllegalArgumentException> {
            SqliteWorkerValue(type = "real", real = Double.NaN).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            SqliteWorkerValue(type = "blob", blob = listOf(256)).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            SqliteWorkerValue(type = "null", text = "unexpected").validate()
        }
        listOf(
            """{"type":"integer","integer":null}""",
            """{"type":"integer","integer":1}""",
            """{"type":"real","real":"1.0"}""",
            """{"type":"text","text":1}""",
            """{"type":"blob","blob":["1"]}""",
            """{"type":"text","text":null}""",
            """{"type":"blob","blob":null}""",
            """{"type":"null","text":null}""",
            """{"type":"integer","integer":"1","text":"conflict"}""",
            """{"type":"text","text":"value","nested":{"unknown":true}}""",
        ).forEach { encoded ->
            assertFailsWith<SerializationException>(encoded) {
                sqliteWorkerJson.decodeFromString<SqliteWorkerValue>(encoded)
            }
        }

        SqliteWorkerConfig().validate()
        SqliteWorkerConfig(pageBytes = SQLITE_WORKER_MIN_PAGE_BYTES).validate()
        SqliteWorkerConfig(pageRows = 1024, pageBytes = 1024 * 1024).validate()
        listOf(
            SqliteWorkerConfig(pageRows = 0),
            SqliteWorkerConfig(pageRows = 1025),
            SqliteWorkerConfig(pageBytes = 0),
            SqliteWorkerConfig(pageBytes = SQLITE_WORKER_MIN_PAGE_BYTES - 1),
            SqliteWorkerConfig(pageBytes = 1024 * 1024 + 1),
        ).forEach { config ->
            assertFailsWith<IllegalArgumentException> { config.validate() }
        }

        SqliteWorkerResponse(
            protocol = SQLITE_WORKER_PROTOCOL,
            rows = listOf(listOf(SqliteWorkerValue.text("oversized"))),
            oversizedRow = true,
            pageRows = 1,
            pageBytes = SQLITE_WORKER_DEFAULT_PAGE_BYTES + 1,
        ).validate()
        assertFailsWith<IllegalArgumentException> {
            SqliteWorkerResponse(
                protocol = SQLITE_WORKER_PROTOCOL,
                rows = listOf(
                    listOf(SqliteWorkerValue.text("first")),
                    listOf(SqliteWorkerValue.text("second")),
                ),
                oversizedRow = true,
                pageRows = 2,
                pageBytes = SQLITE_WORKER_DEFAULT_PAGE_BYTES + 1,
            ).validate()
        }
    }

    @Test
    fun pageBytesMeasureTheActualUtf8EncodedRowsArray() = runTest {
        val databaseName = nextWorkerProtocolDatabaseName("page-bytes")
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        var requestId = 1
        try {
            suspend fun request(request: SqliteWorkerRequest): SqliteWorkerResponse {
                val currentRequestId = requestId++
                val envelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                    transport.request(currentRequestId, sqliteWorkerJson.encodeToString(request)),
                )
                transport.acknowledge(currentRequestId)
                transport.release(currentRequestId)
                envelope.validate()
                return checkNotNull(envelope.data)
            }

            val databaseId = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "open",
                        fileName = databaseName,
                        legacySourceMode = transport.rawOpenLegacySourceMode(),
                    ),
                ).databaseId,
            )
            val statementId = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "prepare",
                        databaseId = databaseId,
                        sql = "SELECT '🙂' UNION ALL SELECT 'măsură'",
                    ),
                ).statementId,
            )
            val page = request(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "page",
                    databaseId = databaseId,
                    statementId = statementId,
                    sql = "SELECT '🙂' UNION ALL SELECT 'măsură'",
                    pageRows = 64,
                    pageBytes = 64 * 1024,
                ),
            )
            val pageRows = checkNotNull(page.rows)
            val encodedRows = sqliteWorkerJson.encodeToString(
                ListSerializer(ListSerializer(SqliteWorkerValue.serializer())),
                pageRows,
            )
            assertEquals(encodedRows.encodeToByteArray().size, page.pageBytes)
            assertTrue(page.pageBytes > pageRows.sumOf { row ->
                sqliteWorkerJson.encodeToString(
                    ListSerializer(SqliteWorkerValue.serializer()),
                    row,
                ).encodeToByteArray().size
            })

            val emptyStatementId = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "prepare",
                        databaseId = databaseId,
                        sql = "SELECT 1 WHERE 0",
                    ),
                ).statementId,
            )
            val emptyPage = request(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "page",
                    databaseId = databaseId,
                    statementId = emptyStatementId,
                    sql = "SELECT 1 WHERE 0",
                    pageRows = 1,
                    pageBytes = SQLITE_WORKER_MIN_PAGE_BYTES,
                ),
            )
            val emptyRows = checkNotNull(emptyPage.rows)
            val independentlyEncodedEmptyRows = sqliteWorkerJson.encodeToString(
                ListSerializer(ListSerializer(SqliteWorkerValue.serializer())),
                emptyRows,
            ).encodeToByteArray()
            assertEquals("[]", independentlyEncodedEmptyRows.decodeToString())
            assertEquals(SQLITE_WORKER_MIN_PAGE_BYTES, independentlyEncodedEmptyRows.size)
            assertEquals(independentlyEncodedEmptyRows.size, emptyPage.pageBytes)
        } finally {
            val shutdown = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "shutdown",
            )
            transport.shutdown(requestId, sqliteWorkerJson.encodeToString(shutdown))
            cleanupWorkerProtocolDatabase(databaseName)
        }
    }

    @Test
    fun duplicateIdsAndStaleResponsesCannotReplacePendingRequests() = runTest {
        val databaseName = nextWorkerProtocolDatabaseName("duplicate-ids")
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        val open = sqliteWorkerJson.encodeToString(
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "open",
                fileName = databaseName,
                legacySourceMode = transport.rawOpenLegacySourceMode(),
            ),
        )
        try {
            val first = async(start = CoroutineStart.UNDISPATCHED) {
                transport.request(1, open)
            }
            val activeDuplicate = assertFailsWith<Throwable> {
                transport.request(
                    1,
                    sqliteWorkerJson.encodeToString(
                        SqliteWorkerRequest(
                            protocol = SQLITE_WORKER_PROTOCOL,
                            command = "metrics",
                        ),
                    ),
                )
            }
            assertTrue(activeDuplicate.message.orEmpty().contains("already used"))
            val firstEnvelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                first.await(),
            )
            assertEquals(1, firstEnvelope.id)
            transport.acknowledge(1)
            transport.release(1)

            val beforeIgnored = sqliteWorkerClientDiagnostic(
                transport.diagnosticsForTest(),
                "ignoredResponses",
            )
            transport.injectResponseForTest(
                """{"id":1,"data":{"protocol":"sqlitenow-sqlite-worker-v1"}}""",
            )
            assertEquals(
                beforeIgnored + 1,
                sqliteWorkerClientDiagnostic(transport.diagnosticsForTest(), "ignoredResponses"),
            )

            val metricsEnvelope = sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                transport.request(
                    2,
                    sqliteWorkerJson.encodeToString(
                        SqliteWorkerRequest(
                            protocol = SQLITE_WORKER_PROTOCOL,
                            command = "metrics",
                        ),
                    ),
                ),
            )
            assertEquals(2, metricsEnvelope.id)
            transport.acknowledge(2)
            transport.release(2)
            val completedReuse = assertFailsWith<Throwable> {
                transport.request(1, open)
            }
            assertTrue(completedReuse.message.orEmpty().contains("already used"))
        } finally {
            val shutdown = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "shutdown",
            )
            transport.shutdown(3, sqliteWorkerJson.encodeToString(shutdown))
            cleanupWorkerProtocolDatabase(databaseName)
        }
    }

    @Test
    fun malformedReconciliationSidebandBecomesStickyTerminalFailure() = runTest {
        val beforeActive = sqliteWorkerJson.parseToJsonElement(
            SqliteWorkerTransport.globalDiagnosticsForTest(),
        ).jsonObject.getValue("activeWorkers").jsonPrimitive.content.toInt()
        val malformedMessages = listOf(
            "mismatched envelope ID" to
                """{"kind":"cancellation-reconciled","protocol":"$SQLITE_WORKER_PROTOCOL","id":1,"envelope":{"id":2,"error":{"operation":"cancel","message":"mismatched"}}}""",
            "negative reconciliation ID" to
                """{"kind":"cancellation-reconciled","protocol":"$SQLITE_WORKER_PROTOCOL","id":-1,"envelope":{"id":-1,"error":{"operation":"cancel","message":"negative"}}}""",
            "unknown nested error field" to
                """{"kind":"cancellation-reconciled","protocol":"$SQLITE_WORKER_PROTOCOL","id":1,"envelope":{"id":1,"error":{"operation":"cancel","message":"unknown","laterPhase":true}}}""",
            "invalid nested error type" to
                """{"kind":"cancellation-reconciled","protocol":"$SQLITE_WORKER_PROTOCOL","id":1,"envelope":{"id":1,"error":{"operation":"cancel","message":"invalid","cancelled":"yes"}}}""",
            "unexpected acknowledgement" to
                """{"kind":"acknowledged","protocol":"$SQLITE_WORKER_PROTOCOL","id":1}""",
            "unexpected release" to
                """{"kind":"released","protocol":"$SQLITE_WORKER_PROTOCOL","id":1}""",
        )
        malformedMessages.forEach { (name, malformedMessage) ->
            val transport = SqliteWorkerTransport.create(
                sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
            )
            try {
                transport.injectResponseForTest(malformedMessage)
                val request = SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "metrics",
                )
                val failure = assertFailsWith<Throwable>(name) {
                    transport.request(1, sqliteWorkerJson.encodeToString(request))
                }
                assertTrue(
                    failure.message.orEmpty().contains("reconciliation") ||
                        failure.message.orEmpty().contains("acknowledgement") ||
                        failure.message.orEmpty().contains("release") ||
                        failure.message.orEmpty().contains("Unknown"),
                    name,
                )
                assertEquals(
                    0,
                    sqliteWorkerClientDiagnostic(
                        transport.diagnosticsForTest(),
                        "reconciliationRequests",
                    ),
                    name,
                )
            } finally {
                runCatching { transport.forceTerminate() }
            }
        }
        val afterActive = sqliteWorkerJson.parseToJsonElement(
            SqliteWorkerTransport.globalDiagnosticsForTest(),
        ).jsonObject.getValue("activeWorkers").jsonPrimitive.content.toInt()
        assertEquals(beforeActive, afterActive)
    }

    @Test
    fun closeStatementCannotFinalizeAStatementOwnedByAnotherDatabase() = runTest {
        val firstDatabaseName = nextWorkerProtocolDatabaseName("owner-first")
        val secondDatabaseName = nextWorkerProtocolDatabaseName("owner-second")
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        var requestId = 1
        suspend fun request(request: SqliteWorkerRequest): SqliteWorkerEnvelope {
            val currentRequestId = requestId++
            return sqliteWorkerJson.decodeFromString<SqliteWorkerEnvelope>(
                transport.request(currentRequestId, sqliteWorkerJson.encodeToString(request)),
            ).also {
                transport.acknowledge(currentRequestId)
                transport.release(currentRequestId)
            }
        }
        try {
            val firstDatabase = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "open",
                        fileName = firstDatabaseName,
                        legacySourceMode = transport.rawOpenLegacySourceMode(),
                    ),
                ).data?.databaseId,
            )
            val secondDatabase = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "open",
                        fileName = secondDatabaseName,
                        legacySourceMode = transport.rawOpenLegacySourceMode(),
                    ),
                ).data?.databaseId,
            )
            val statement = checkNotNull(
                request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "prepare",
                        databaseId = firstDatabase,
                        sql = "SELECT 47",
                    ),
                ).data?.statementId,
            )

            val wrongOwner = request(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "closeStatement",
                    databaseId = secondDatabase,
                    statementId = statement,
                    sql = "SELECT 47",
                ),
            )
            assertTrue(
                wrongOwner.error?.message.orEmpty().contains("does not belong"),
            )

            val stillOwned = request(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "page",
                    databaseId = firstDatabase,
                    statementId = statement,
                    sql = "SELECT 47",
                    pageRows = 1,
                    pageBytes = 1024,
                ),
            )
            assertEquals("47", stillOwned.data?.rows?.single()?.single()?.integer)
        } finally {
            val shutdown = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "shutdown",
            )
            transport.shutdown(requestId, sqliteWorkerJson.encodeToString(shutdown))
            cleanupWorkerProtocolDatabase(firstDatabaseName)
            cleanupWorkerProtocolDatabase(secondDatabaseName)
        }
    }

    @Test
    fun requestIdHighWatermarksStayBoundedAndRejectExhaustedOneWayIds() = runTest {
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        val invalidClose = sqliteWorkerJson.encodeToString(
            SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "closeStatement",
                databaseId = Int.MAX_VALUE,
                statementId = Int.MAX_VALUE,
            ),
        )
        try {
            transport.setNextOneWayIdForTest(Int.MIN_VALUE)
            transport.sendOneWay(invalidClose)
            val exhausted = assertFailsWith<Throwable> {
                transport.sendOneWay(invalidClose)
            }
            assertTrue(exhausted.message.orEmpty().contains("exhausted"))

            val flushed = sqliteWorkerJson.decodeFromString<SqliteWorkerFlushBatch>(
                transport.flush(),
            )
            assertEquals(1, flushed.envelopes.size)
            assertTrue(flushed.envelopes.single().error != null)
            assertTrue(flushed.barrierFailures.isEmpty())
            assertEquals(
                Int.MIN_VALUE,
                sqliteWorkerClientDiagnostic(
                    transport.diagnosticsForTest(),
                    "lowestNegativeRequestId",
                ),
            )
            repeat(10_000) {
                if (
                    sqliteWorkerClientDiagnostic(
                        transport.diagnosticsForTest(),
                        "completedResponses",
                    ) == 0
                ) {
                    return@repeat
                }
                yield()
            }
            assertEquals(
                0,
                sqliteWorkerClientDiagnostic(transport.diagnosticsForTest(), "completedResponses"),
            )
        } finally {
            val shutdown = SqliteWorkerRequest(
                protocol = SQLITE_WORKER_PROTOCOL,
                command = "shutdown",
            )
            transport.shutdown(1, sqliteWorkerJson.encodeToString(shutdown))
        }
    }

    @Test
    fun cancellationHoldTestControlsAndSidebandsAreStrictAndOneShot() = runTest {
        val transport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        try {
            assertFailsWith<Throwable> {
                transport.holdMigrationCancellationForTest("", "after-integrity")
            }
            assertFailsWith<Throwable> {
                transport.holdMigrationCancellationForTest(
                    "phase5b-control",
                    "before-intent",
                )
            }

            val controlId = transport.holdMigrationCancellationForTest(
                "phase5b-control",
                "after-integrity",
            )
            transport.injectResponseForTest(
                """{"kind":"test-cancellation-hold","protocol":"$SQLITE_WORKER_PROTOCOL","id":$controlId,"command":"migration","databaseName":"phase5b-control","stage":"after-integrity","pendingOpenCount":0}""",
            )
            assertEquals(0, transport.awaitCancellationHoldForTest(controlId))
            val reused = assertFailsWith<Throwable> {
                transport.awaitCancellationHoldForTest(controlId)
            }
            assertTrue(reused.message.orEmpty().contains("reused"))
        } finally {
            transport.forceTerminate()
        }

        val duplicateWaiterTransport = SqliteWorkerTransport.create(
            sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
        )
        try {
            val controlId = duplicateWaiterTransport
                .holdNextCompleteOpenCancellationForTest()
            val firstWaiter = async(start = CoroutineStart.UNDISPATCHED) {
                duplicateWaiterTransport.awaitCancellationHoldForTest(controlId)
            }
            val duplicate = assertFailsWith<Throwable> {
                duplicateWaiterTransport.awaitCancellationHoldForTest(controlId)
            }
            assertTrue(duplicate.message.orEmpty().contains("already has a waiter"))
            duplicateWaiterTransport.injectResponseForTest(
                """{"kind":"test-cancellation-hold","protocol":"$SQLITE_WORKER_PROTOCOL","id":$controlId,"command":"completeOpen","stage":"before-dispatch","pendingOpenCount":1}""",
            )
            assertEquals(1, firstWaiter.await())
        } finally {
            runCatching { duplicateWaiterTransport.forceTerminate() }
        }

        listOf(
            "unknown command" to
                """{"kind":"test-cancellation-hold","protocol":"$SQLITE_WORKER_PROTOCOL","id":1,"command":"unknown","stage":"before-dispatch","pendingOpenCount":0}""",
            "wrong pending-open count type" to
                """{"kind":"test-pending-open-count","protocol":"$SQLITE_WORKER_PROTOCOL","id":1,"count":"0"}""",
        ).forEach { (name, sideband) ->
            val malformedTransport = SqliteWorkerTransport.create(
                sqliteWorkerJson.encodeToString(SqliteWorkerConfig()),
            )
            try {
                malformedTransport.injectResponseForTest(sideband)
                val failure = assertFailsWith<Throwable>(name) {
                    malformedTransport.pendingOpenCountForTest()
                }
                assertTrue(
                    failure.message.orEmpty().contains("test", ignoreCase = true) ||
                        failure.message.orEmpty().contains("unknown", ignoreCase = true),
                    "$name: ${failure.message}",
                )
            } finally {
                runCatching { malformedTransport.forceTerminate() }
            }
        }
    }

}

private var workerProtocolDatabaseSequence = 0

private fun nextWorkerProtocolDatabaseName(scenario: String): String =
    "__sqlitenow_worker_protocol_${scenario}_${workerProtocolDatabaseSequence++}"

private fun SqliteWorkerTransport.rawOpenLegacySourceMode(): String =
    if (runtimeKind() == "browser-worker") "built-in" else "none"

private suspend fun cleanupWorkerProtocolDatabase(databaseName: String) {
    val driver = SqliteWorkerSQLiteDriver.create()
    try {
        driver.cleanupMigrationStateForTest(databaseName)
    } finally {
        driver.shutdown()
    }
}

private data class ResponseValidationScenario(
    val name: String,
    val request: SqliteWorkerRequest,
    val envelope: SqliteWorkerEnvelope,
)

internal fun sqliteWorkerClientDiagnostic(diagnostics: String, name: String): Int =
    sqliteWorkerJson.parseToJsonElement(diagnostics)
        .jsonObject
        .getValue(name)
        .jsonPrimitive
        .content
        .toInt()

internal data class SqliteWorkerIntegerScenario(
    val name: String,
    val value: Long,
)

internal val sqliteWorkerIntegerScenarios = listOf(
    SqliteWorkerIntegerScenario("Long.MIN_VALUE", Long.MIN_VALUE),
    SqliteWorkerIntegerScenario("below negative JS-safe boundary", -9_007_199_254_740_993L),
    SqliteWorkerIntegerScenario("negative JS-safe boundary minus one", -9_007_199_254_740_992L),
    SqliteWorkerIntegerScenario("negative JS-safe boundary", -9_007_199_254_740_991L),
    SqliteWorkerIntegerScenario("ordinary negative", -1L),
    SqliteWorkerIntegerScenario("zero", 0L),
    SqliteWorkerIntegerScenario("ordinary positive", 1L),
    SqliteWorkerIntegerScenario("positive JS-safe boundary", 9_007_199_254_740_991L),
    SqliteWorkerIntegerScenario("positive JS-safe boundary plus one", 9_007_199_254_740_992L),
    SqliteWorkerIntegerScenario("above positive JS-safe boundary", 9_007_199_254_740_993L),
    SqliteWorkerIntegerScenario("Long.MAX_VALUE", Long.MAX_VALUE),
)
