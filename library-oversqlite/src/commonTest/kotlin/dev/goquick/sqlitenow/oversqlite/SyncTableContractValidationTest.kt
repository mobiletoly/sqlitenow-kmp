package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

internal class SyncTableContractValidationTest {
    @Test
    fun exactContractMatchesRegardlessOfResponseOrder() {
        val validated = validated("users" to "ID", "posts" to "post_id")
        capabilities(
            RegisteredTableSpec("business", "posts", listOf("post_id")),
            RegisteredTableSpec("business", "users", listOf("id")),
        ).requireMatchingSyncTableContract(validated)
    }

    @Test
    fun mismatchDiagnosticsAreCompleteAndDeterministic() {
        val mismatch = assertFailsWith<SyncTableContractMismatchException> {
            capabilities(
                RegisteredTableSpec("business", "z_server", listOf("id")),
                RegisteredTableSpec("business", "users", listOf("server_id")),
                RegisteredTableSpec("business", "a_server", listOf("id")),
            ).requireMatchingSyncTableContract(
                validated("z_client" to "id", "users" to "id", "a_client" to "id"),
            )
        }

        assertEquals(
            listOf("business.a_server", "business.z_server"),
            mismatch.serverOnlyTables,
        )
        assertEquals(
            listOf("business.a_client", "business.z_client"),
            mismatch.clientOnlyTables,
        )
        assertEquals(
            listOf(
                SyncKeyContractMismatch(
                    qualifiedTable = "business.users",
                    clientSyncKeyColumns = listOf("id"),
                    serverSyncKeyColumns = listOf("server_id"),
                ),
            ),
            mismatch.syncKeyMismatches,
        )
    }

    @Test
    fun dayTempoHealthMeshMismatchReportsOnlyMonitoringFocusAsServerOnly() {
        val clientTables = listOf(
            "activity_sessions",
            "daily_goals",
            "focus_sessions",
            "projects",
            "tasks",
        )
        val serverTables = clientTables + "monitoring_focus"

        val mismatch = assertFailsWith<SyncTableContractMismatchException> {
            capabilities(
                *serverTables.reversed().map { table ->
                    RegisteredTableSpec("business", table, listOf("id"))
                }.toTypedArray(),
            ).requireMatchingSyncTableContract(
                validated(*clientTables.map { it to "id" }.toTypedArray()),
            )
        }

        assertEquals(listOf("business.monitoring_focus"), mismatch.serverOnlyTables)
        assertEquals(emptyList(), mismatch.clientOnlyTables)
        assertEquals(emptyList(), mismatch.syncKeyMismatches)
    }

    @Test
    fun schemaAndCaseAreComparedExactlyWithoutNormalizingServerValues() {
        val mismatch = assertFailsWith<SyncTableContractMismatchException> {
            capabilities(
                RegisteredTableSpec("Business", "Users", listOf("ID")),
            ).requireMatchingSyncTableContract(validated("users" to "id"))
        }

        assertEquals(listOf("Business.Users"), mismatch.serverOnlyTables)
        assertEquals(listOf("business.users"), mismatch.clientOnlyTables)
    }

    @Test
    fun malformedSpecsFailAsInvalidCapabilitiesInsteadOfMismatch() {
        val malformed = listOf(
            listOf(RegisteredTableSpec("", "users", listOf("id"))),
            listOf(RegisteredTableSpec("business", " ", listOf("id"))),
            listOf(RegisteredTableSpec("business", "users", emptyList())),
            listOf(RegisteredTableSpec("business", "users", listOf("id", "id"))),
            listOf(RegisteredTableSpec("business", "users", listOf(" "))),
            listOf(
                RegisteredTableSpec("business", "users", listOf("id")),
                RegisteredTableSpec("business", "users", listOf("id")),
            ),
        )

        malformed.forEach { specs ->
            assertFailsWith<RemoteResponseSemanticException> {
                capabilities(*specs.toTypedArray())
                    .requireMatchingSyncTableContract(validated("users" to "id"))
            }
        }
    }

    @Test
    fun exceptionMessageSanitizesAdvertisedIdentifiersAndKeys() {
        val secret = "hostile\nsecret"
        val mismatch = assertFailsWith<SyncTableContractMismatchException> {
            capabilities(
                RegisteredTableSpec(secret, "monitoring_focus", listOf("id")),
                RegisteredTableSpec("business", "users", listOf(secret)),
            ).requireMatchingSyncTableContract(validated("users" to "id"))
        }

        assertFalse(mismatch.message.orEmpty().contains(secret))
        assertEquals(OversqliteErrorCategory.PROTOCOL, mismatch.category)
    }

    private fun validated(vararg tables: Pair<String, String>) = ValidatedConfig(
        schema = "business",
        tables = tables.map { (table, key) -> ValidatedSyncTable(table, key) },
        pkByTable = emptyMap(),
        keyByTable = emptyMap(),
        tableOrder = emptyMap(),
        tableInfoByName = emptyMap(),
    )

    private fun capabilities(vararg specs: RegisteredTableSpec) = CapabilitiesResponse(
        protocolVersion = "v1",
        schemaVersion = 1,
        registeredTableSpecs = specs.toList(),
        features = emptyMap(),
        bundleLimits = testBundleCapabilitiesLimits(),
    )
}
