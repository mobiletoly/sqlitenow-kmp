package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

internal class SyncTableContractValidationTest {
    @Test
    fun exactContractMatchesRegardlessOfResponseOrder() {
        val validated = validated("users" to "ID", "posts" to "post_id")
        contract(
            RegisteredTableSpec("business", "posts", listOf("post_id")),
            RegisteredTableSpec("business", "users", listOf("id")),
        ).requireMatchingSyncTableContract(validated)
    }

    @Test
    fun mismatchDiagnosticsAreCompleteAndDeterministic() {
        val mismatch = assertFailsWith<SyncTableContractMismatchException> {
            contract(
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
            contract(
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
            contract(
                RegisteredTableSpec("Business", "Users", listOf("ID")),
            ).requireMatchingSyncTableContract(validated("users" to "id"))
        }

        assertEquals(listOf("Business.Users"), mismatch.serverOnlyTables)
        assertEquals(listOf("business.users"), mismatch.clientOnlyTables)
    }

    @Test
    fun malformedSpecsFailAsInvalidDiscoveryInsteadOfMismatch() {
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
                contract(*specs.toTypedArray())
                    .requireMatchingSyncTableContract(validated("users" to "id"))
            }
        }
    }

    @Test
    fun exceptionMessageSanitizesAdvertisedIdentifiersAndKeys() {
        val secret = "hostile\nsecret"
        val mismatch = assertFailsWith<SyncTableContractMismatchException> {
            contract(
                RegisteredTableSpec(secret, "monitoring_focus", listOf("id")),
                RegisteredTableSpec("business", "users", listOf(secret)),
            ).requireMatchingSyncTableContract(validated("users" to "id"))
        }

        assertFalse(mismatch.message.orEmpty().contains(secret))
        assertEquals(OversqliteErrorCategory.PROTOCOL, mismatch.category)
    }

    private fun validated(vararg tables: Pair<String, String>) = ValidatedConfig(
        generation = testGeneration(tables.map { (table, key) -> RegisteredTableSpec("business", table, listOf(key)) }),
        tables = tables.map { (table, key) -> ValidatedSyncTable(table, key) },
        pkByTable = emptyMap(),
        keyByTable = emptyMap(),
        tableOrder = emptyMap(),
        tableInfoByName = emptyMap(),
    )

    private fun contract(vararg specs: RegisteredTableSpec) = AdmittedOversqliteContract(
        protocolVersion = "v1",
        generation = testGeneration(specs.toList()),
        limits = testWireLimits(),
        watchEnabled = false,
    )
}
