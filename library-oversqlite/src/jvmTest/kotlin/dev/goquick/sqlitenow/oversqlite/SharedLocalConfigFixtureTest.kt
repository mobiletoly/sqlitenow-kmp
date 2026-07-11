package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedLocalConfigFixtureTest : BundleClientContractTestSupport() {
    private val contractJson = Json { ignoreUnknownKeys = true }
    private val fixtureFile = oversqliteContractFixture("local-schema/config-validation.json")

    @Test
    fun kmpSharedLocalConfigValidationFixtureMatchesRuntime() = runBlocking {
        val spec = contractJson.decodeFromString<LocalConfigValidationSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            runCase(case)
        }
    }

    private suspend fun runCase(case: LocalConfigValidationCase) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            executeStatements(db, case.schemaSql)
            fun newClient() = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = case.config.schema,
                    syncTables = case.config.syncTables.map {
                        SyncTable(
                            tableName = it.tableName,
                            syncKeyColumnName = it.syncKeyColumnName,
                            syncKeyColumns = it.syncKeyColumns,
                        )
                    },
                ),
                http = http,
                resolver = ServerWinsResolver,
            )

            if (case.initializeBeforeValidation) {
                val initializedClient = newClient()
                assertNull(initializedClient.open().exceptionOrNull(), "${case.name}: setup initialization must succeed")
                initializedClient.close()
                executeStatements(db, case.beforeValidationSql)
            }

            val client = newClient()
            val error = client.open().exceptionOrNull()
            assertExpectedError(case, error)
            if (error == null) {
                executeStatements(db, case.afterOpenSql)
            }
            for (query in case.expectedQueries) {
                assertQueryValue(case.name, db, query)
            }
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun assertExpectedError(case: LocalConfigValidationCase, error: Throwable?) {
        val expected = case.expectedError
        if (expected == null) {
            assertNull(error, "${case.name}: expected success")
            return
        }
        assertNotNull(error, "${case.name}: expected ${expected.kind} error")
        for (fragment in expected.messageContains) {
            assertTrue(
                error.message?.contains(fragment) == true,
                "${case.name}: expected message to contain '$fragment', got '${error.message}'",
            )
        }
    }

    private suspend fun executeStatements(db: SafeSQLiteConnection, statements: List<String>) {
        for (statement in statements) {
            db.execSQL(statement)
        }
    }

    private suspend fun assertQueryValue(caseName: String, db: SafeSQLiteConnection, query: ExpectedQuery) {
        db.withExclusiveAccess {
            db.prepare(query.sql).use { st ->
                assertTrue(st.step(), "$caseName/${query.name}: expected one row")
                val expectedLong = query.value.jsonPrimitive.longOrNull
                if (expectedLong != null) {
                    assertEquals(expectedLong, st.getLong(0), "$caseName/${query.name}")
                } else {
                    assertEquals(query.value.jsonPrimitive.content, st.getText(0), "$caseName/${query.name}")
                }
            }
        }
    }

    @Serializable
    private data class LocalConfigValidationSpec(
        val formatVersion: Int,
        val cases: List<LocalConfigValidationCase>,
    )

    @Serializable
    private data class LocalConfigValidationCase(
        val name: String,
        val description: String,
        val schemaSql: List<String>,
        val config: FixtureConfig,
        val initializeBeforeValidation: Boolean = false,
        val beforeValidationSql: List<String> = emptyList(),
        val afterOpenSql: List<String> = emptyList(),
        val expectedError: ExpectedError? = null,
        val expectedQueries: List<ExpectedQuery> = emptyList(),
    )

    @Serializable
    private data class FixtureConfig(
        val schema: String,
        val syncTables: List<FixtureSyncTable>,
    )

    @Serializable
    private data class FixtureSyncTable(
        val tableName: String,
        val syncKeyColumnName: String? = null,
        val syncKeyColumns: List<String> = emptyList(),
    )

    @Serializable
    private data class ExpectedError(
        val kind: String,
        val messageContains: List<String>,
    )

    @Serializable
    private data class ExpectedQuery(
        val name: String,
        val sql: String,
        val value: JsonElement,
    )
}
