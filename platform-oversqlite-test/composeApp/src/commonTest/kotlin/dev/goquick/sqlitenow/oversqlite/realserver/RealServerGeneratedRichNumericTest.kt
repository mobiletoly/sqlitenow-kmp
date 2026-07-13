package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AuthorityStatus
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
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
    fun generatedRichNumericProductionPath_runsAcrossHeavyRealServerTargets() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        if (!realServerHeavyModeEnabled(config)) {
            println("Skipping generated rich numeric production path; enable OVERSQLITE_REALSERVER_HEAVY.")
            return@runTest
        }
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("generated-rich-numeric-user")
        val seedDb = newRealServerGeneratedDb()
        val pullDb = newRealServerGeneratedDb()
        val rebuildDb = newRealServerGeneratedDb()
        var seedHttp: HttpClient? = null
        var pullHttp: HttpClient? = null
        var rebuildHttp: HttpClient? = null
        var frozenLargeIntegerPayload: String? = null
        try {
            seedDb.open()
            pullDb.open()
            rebuildDb.open()

            val seedSource = bootstrapSourceId(config.baseUrl) { http -> newRealServerGeneratedClient(seedDb, http) }
            val pullSource = bootstrapSourceId(config.baseUrl) { http -> newRealServerGeneratedClient(pullDb, http) }
            val rebuildSource = bootstrapSourceId(config.baseUrl) { http -> newRealServerGeneratedClient(rebuildDb, http) }
            seedHttp = newRealServerHttpClient(
                baseUrl = config.baseUrl,
                token = issueDummySigninToken(config.baseUrl, userId, seedSource),
                beforeRequest = { path ->
                    if (path == "/sync/push-sessions" && frozenLargeIntegerPayload == null) {
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
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, pullSource),
            )
            rebuildHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, rebuildSource),
            )

            val seed = newRealServerGeneratedClient(seedDb, seedHttp)
            val pull = newRealServerGeneratedClient(pullDb, pullHttp)
            val rebuild = newRealServerGeneratedClient(rebuildDb, rebuildHttp)

            seed.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = seed.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            pull.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = pull.attach(userId).getOrThrow(),
            )
            rebuild.open().getOrThrow()
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
            assertLargeIntegerIsJsonString(dirtyLargeIntegerPayload)

            assertEquals(PushOutcome.COMMITTED, seed.pushPending().getOrThrow().outcome)
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
            seedHttp?.close()
            pullHttp?.close()
            rebuildHttp?.close()
            seedDb.close()
            pullDb.close()
            rebuildDb.close()
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

internal fun newRealServerGeneratedDb(): RealServerGeneratedDatabase =
    RealServerGeneratedDatabase(
        dbName = ":memory:",
        migration = VersionBasedDatabaseMigrations(),
        debug = true,
    )

internal fun newRealServerGeneratedClient(
    db: RealServerGeneratedDatabase,
    http: HttpClient,
): OversqliteClient =
    db.newOversqliteClient(
        schema = "business",
        httpClient = http,
        uploadLimit = 8,
        downloadLimit = 8,
        syncTables = RealServerGeneratedDatabase.syncTables,
    )

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
            "exact_amount" to "-1234567890.1234567890",
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
            "exact_amount" to "1234567890.1234567890",
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
