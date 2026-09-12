package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AuthorityStatus
import dev.goquick.sqlitenow.oversqlite.OversqliteOpening
import dev.goquick.sqlitenow.oversqlite.OversqliteTransferTargets
import dev.goquick.sqlitenow.oversqlite.OwnedWorkerStorage
import dev.goquick.sqlitenow.oversqlite.PushOutcome
import dev.goquick.sqlitenow.oversqlite.RemoteSyncOutcome
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.TypedRowQuery
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.oversqlite.platformsupport.assertConnectedOutcome
import io.ktor.client.HttpClient
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class RealServerGeneratedRichNumericTest : RealServerHarnessSupport() {
    @Test
    fun generatedDatabaseFixturesAreIndependent() = runTest {
        val storage = OwnedWorkerStorage()
        val firstDb = newRealServerGeneratedDb(storage.newDatabaseName())
        val secondDb = newRealServerGeneratedDb(storage.newDatabaseName())
        val server = MockSyncServer()
        val firstHttp = server.newHttpClient()
        val secondHttp = server.newHttpClient()
        var firstOpening: OversqliteOpening<RealServerGeneratedDatabase>? = null
        var secondOpening: OversqliteOpening<RealServerGeneratedDatabase>? = null
        try {
            firstOpening = openRealServerGeneratedDatabase(firstDb, firstHttp)
            secondOpening = openRealServerGeneratedDatabase(secondDb, secondHttp)
            firstDb.connection().execSQL(
                "CREATE TABLE IF NOT EXISTS regular_fixture_isolation_probe(value INTEGER NOT NULL)",
            )
            secondDb.connection().execSQL(
                "CREATE TABLE IF NOT EXISTS regular_fixture_isolation_probe(value INTEGER NOT NULL)",
            )
            firstDb.connection().execSQL(
                "INSERT INTO regular_fixture_isolation_probe(value) VALUES(1)",
            )

            assertEquals(
                0L,
                scalarLong(
                    secondDb.connection(),
                    "SELECT COUNT(*) FROM regular_fixture_isolation_probe",
                ),
            )
        } finally {
            firstOpening?.close() ?: firstDb.close()
            secondOpening?.close() ?: secondDb.close()
            firstHttp.close()
            secondHttp.close()
            storage.cleanup()
        }
    }

    @Test
    fun generatedRichNumericProductionPath_runsAcrossHeavyRealServerTargets() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        if (!realServerHeavyModeEnabled(config)) {
            println("Skipping generated rich numeric production path; enable OVERSQLITE_REALSERVER_HEAVY.")
            return@runTest
        }
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("generated-rich-numeric-user")
        val storage = OwnedWorkerStorage()
        val seedDb = newRealServerGeneratedDb(storage.newDatabaseName())
        val pullDb = newRealServerGeneratedDb(storage.newDatabaseName())
        val rebuildDb = newRealServerGeneratedDb(storage.newDatabaseName())
        var seedHttp: HttpClient? = null
        var pullHttp: HttpClient? = null
        var rebuildHttp: HttpClient? = null
        var seedOpening: OversqliteOpening<RealServerGeneratedDatabase>? = null
        var pullOpening: OversqliteOpening<RealServerGeneratedDatabase>? = null
        var rebuildOpening: OversqliteOpening<RealServerGeneratedDatabase>? = null
        var seedToken: String? = null
        var pullToken: String? = null
        var rebuildToken: String? = null
        var frozenLargeIntegerPayload: String? = null
        try {
            seedHttp = newRealServerHttpClient(
                baseUrl = config.baseUrl,
                tokenProvider = { seedToken },
                beforeRequest = { path ->
                    if (path == "/sync/push" && frozenLargeIntegerPayload == null) {
                        frozenLargeIntegerPayload = scalarText(
                            seedDb.connection(),
                            """
                            SELECT wire_payload
                            FROM _sync_outbox_rows
                            WHERE table_name = 'typed_rows'
                              AND local_payload LIKE '%above-javascript-safe-range%'
                            """.trimIndent(),
                        )
                    }
                },
            )
            pullHttp = newRealServerHttpClient(
                baseUrl = config.baseUrl,
                tokenProvider = { pullToken },
            )
            rebuildHttp = newRealServerHttpClient(
                baseUrl = config.baseUrl,
                tokenProvider = { rebuildToken },
            )
            seedOpening = openRealServerGeneratedDatabase(seedDb, seedHttp)
            pullOpening = openRealServerGeneratedDatabase(pullDb, pullHttp)
            rebuildOpening = openRealServerGeneratedDatabase(rebuildDb, rebuildHttp)
            val seed = checkNotNull(seedOpening.readyOrNull()).client
            val pull = checkNotNull(pullOpening.readyOrNull()).client
            val rebuild = checkNotNull(rebuildOpening.readyOrNull()).client
            seedToken = issueDummySigninToken(
                config.baseUrl,
                userId,
                seed.sourceInfo().getOrThrow().currentSourceId,
            )
            pullToken = issueDummySigninToken(
                config.baseUrl,
                userId,
                pull.sourceInfo().getOrThrow().currentSourceId,
            )
            rebuildToken = issueDummySigninToken(
                config.baseUrl,
                userId,
                rebuild.sourceInfo().getOrThrow().currentSourceId,
            )
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = seed.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = pull.attach(userId).getOrThrow(),
            )
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = rebuild.attach(userId).getOrThrow(),
            )

            val fixtures = insertNumericScenarios(seedDb.connection())
            val largeFixture = fixtures.single { it.scenario.name == "above-javascript-safe-range" }
            val dirtyLargeIntegerPayload = scalarText(
                seedDb.connection(),
                "SELECT payload FROM _sync_dirty_rows WHERE table_name = 'typed_rows' AND key_json LIKE '%${largeFixture.id}%'",
            )
            val captured = json.parseToJsonElement(dirtyLargeIntegerPayload).jsonObject.getValue("count_value").jsonObject
            assertEquals("integer", captured.getValue("storage").jsonPrimitive.content)
            assertEquals("9007199254740993", captured.getValue("value").jsonPrimitive.content)

            assertEquals(PushOutcome.COMMITTED, seed.pushPending().getOrThrow().outcome)
            assertTrue(seed.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertEquals(PushOutcome.COMMITTED, seed.pushPending().getOrThrow().outcome)
            assertFalse(seed.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertLargeIntegerIsJsonString(assertNotNull(frozenLargeIntegerPayload))
            assertEquals(RemoteSyncOutcome.APPLIED_INCREMENTAL, pull.pullToStable().getOrThrow().outcome)
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, rebuild.rebuild().getOrThrow().outcome)

            assertNumericScenarios(seedDb, fixtures)
            assertNumericScenarios(pullDb, fixtures)
            assertNumericScenarios(rebuildDb, fixtures)
            assertFalse(seed.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertFalse(pull.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertFalse(rebuild.syncStatus().getOrThrow().pending.hasPendingSyncData)
        } finally {
            seedOpening?.close() ?: seedDb.close()
            pullOpening?.close() ?: pullDb.close()
            rebuildOpening?.close() ?: rebuildDb.close()
            seedHttp?.close()
            pullHttp?.close()
            rebuildHttp?.close()
            storage.cleanup()
        }
    }

    private suspend fun insertNumericScenarios(db: SafeSQLiteConnection): List<RichNumericFixture> =
        richNumericScenarios.map { scenario ->
            val id = randomRealServerUuid()
            fun local(name: String): String = scenario.local[name] ?: "NULL"
            val enabled = scenario.local["enabled_flag"] ?: "1"
            db.execSQL(
                """
                INSERT INTO typed_rows(
                    id, name, count_value, small_count, medium_count, exact_amount,
                    enabled_flag, rating, float4_value
                ) VALUES(
                    '$id', '${scenario.name}', ${local("count_value")}, ${local("small_count")},
                    ${local("medium_count")}, ${scenario.local["exact_amount"]?.let { "'$it'" } ?: "NULL"},
                    $enabled, ${local("rating")}, ${local("float4_value")}
                )
                """.trimIndent(),
            )
            RichNumericFixture(id, scenario)
        }

    private suspend fun assertNumericScenarios(
        db: RealServerGeneratedDatabase,
        fixtures: List<RichNumericFixture>,
    ) {
        fixtures.forEach { fixture ->
            val row = db.typedRow.selectById(
                TypedRowQuery.SelectById.Params(id = fixture.id),
            ).asOne()
            fixture.scenario.committed["count_value"]?.let {
                assertEquals(it.toLong(), row.countValue)
            }
            fixture.scenario.committed["small_count"]?.let {
                assertEquals(it.toLong(), row.smallCount)
            }
            fixture.scenario.committed["medium_count"]?.let {
                assertEquals(it.toLong(), row.mediumCount)
            }
            fixture.scenario.committed["exact_amount"]?.let {
                assertEquals(it, row.exactAmount)
            }
            fixture.scenario.committed["rating"]?.let {
                assertEquals(it.toDouble(), row.rating)
            }
            fixture.scenario.committed["float4_value"]?.let {
                assertEquals(it.toDouble(), row.float4Value)
            }
            fixture.scenario.committed["enabled_flag"]?.let {
                assertEquals(if (it == "true") 1L else 0L, row.enabledFlag)
            }
        }
    }

    private fun assertLargeIntegerIsJsonString(payload: String) {
        val value = json.parseToJsonElement(payload).jsonObject.getValue("count_value").jsonPrimitive
        assertTrue(value.isString, "count_value must remain a JSON string: $payload")
        assertEquals("9007199254740993", value.content)
    }

}

internal fun newRealServerGeneratedDb(dbName: String): RealServerGeneratedDatabase =
    RealServerGeneratedDatabase(
        dbName = dbName,
        migration = VersionBasedDatabaseMigrations(),
        debug = true,
    )

internal suspend fun openRealServerGeneratedDatabase(
    db: RealServerGeneratedDatabase,
    http: HttpClient,
): OversqliteOpening<RealServerGeneratedDatabase> {
    val opening = db.openForSync(
        config = db.buildOversqliteConfig(
            generationId = 1L,
            databaseId = "platform-sync",
            serviceId = "nethttp-server-example",
            transferTargets = OversqliteTransferTargets(
                maxPushRows = 8,
                maxPullBundles = 8,
            ),
        ),
        httpClient = http,
    )
    opening.advance()
    checkNotNull(opening.readyOrNull()) { "fresh generated coordinated opening did not publish ready" }
    return opening
}

internal data class RichNumericScenario(
    val name: String,
    val local: Map<String, String>,
    val committed: Map<String, String>,
)

private data class RichNumericFixture(
    val id: String,
    val scenario: RichNumericScenario,
)

internal val richNumericScenarios = listOf(
    RichNumericScenario(
        name = "signed-64-min",
        local = mapOf(
            "count_value" to "-9223372036854775808",
            "small_count" to "-32768",
            "medium_count" to "-2147483648",
            "exact_amount" to "-1234567890.123456789",
        ),
        committed = mapOf(
            "count_value" to "-9223372036854775808",
            "small_count" to "-32768",
            "medium_count" to "-2147483648",
            "exact_amount" to "-1234567890.123456789",
        ),
    ),
    RichNumericScenario(
        name = "signed-64-max",
        local = mapOf(
            "count_value" to "9223372036854775807",
            "small_count" to "32767",
            "medium_count" to "2147483647",
            "exact_amount" to "1234567890.123456789",
        ),
        committed = mapOf(
            "count_value" to "9223372036854775807",
            "small_count" to "32767",
            "medium_count" to "2147483647",
            "exact_amount" to "1234567890.123456789",
        ),
    ),
    RichNumericScenario(
        name = "above-javascript-safe-range",
        local = mapOf("count_value" to "9007199254740993"),
        committed = mapOf("count_value" to "9007199254740993"),
    ),
    RichNumericScenario(
        name = "binary64-negative-zero",
        local = mapOf("rating" to "-0.0"),
        committed = mapOf("rating" to "0"),
    ),
    RichNumericScenario(
        name = "binary64-subnormal",
        local = mapOf("rating" to "5e-324"),
        committed = mapOf("rating" to "5e-324"),
    ),
    RichNumericScenario(
        name = "binary64-ordinary",
        local = mapOf("rating" to "6.57111473696007"),
        committed = mapOf("rating" to "6.57111473696007"),
    ),
    RichNumericScenario(
        name = "binary64-maximum-finite",
        local = mapOf("rating" to "1.7976931348623157e+308"),
        committed = mapOf("rating" to "1.7976931348623157e+308"),
    ),
    RichNumericScenario(
        name = "postgres-float4-authoritative-spelling",
        local = mapOf("float4_value" to "1.2345678901234567"),
        committed = mapOf("float4_value" to "1.2345678806304932"),
    ),
    RichNumericScenario(
        name = "boolean-false",
        local = mapOf("enabled_flag" to "0"),
        committed = mapOf("enabled_flag" to "false"),
    ),
    RichNumericScenario(
        name = "boolean-true",
        local = mapOf("enabled_flag" to "1"),
        committed = mapOf("enabled_flag" to "true"),
    ),
)
