package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertNotNull

class SharedRuntimeStateSchemaFixtureTest : SharedRuntimeStateFixtureSupport() {
    private val fixtureFile = oversqliteContractFixture("runtime-state/schema/v1.json")

    @Test
    fun kmpSharedRuntimeStateSchemaFixtureMatchesRuntime() = runBlocking {
        withRuntimeStateUsersDatabase {
            client.open().getOrThrow()
            assertOrUpdateExpected(fixtureFile, dumpRuntimeSchema(db))
        }
    }

    @Test
    fun legacyOutboxSchemaRequiresDatabaseRecreation() = runBlocking {
        withRuntimeStateUsersDatabase {
            db.execSQL(
                """
                CREATE TABLE _sync_outbox_bundle (
                  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
                  state TEXT NOT NULL DEFAULT 'none',
                  source_id TEXT NOT NULL DEFAULT '',
                  source_bundle_id INTEGER NOT NULL DEFAULT 0,
                  initialization_id TEXT NOT NULL DEFAULT '',
                  canonical_request_hash TEXT NOT NULL DEFAULT '',
                  row_count INTEGER NOT NULL DEFAULT 0,
                  remote_bundle_hash TEXT NOT NULL DEFAULT '',
                  remote_bundle_seq INTEGER NOT NULL DEFAULT 0
                )
                """.trimIndent(),
            )

            val error = assertNotNull(client.open().exceptionOrNull())
            assertContains(error.message.orEmpty(), "canonical_json_contract")
        }
    }
}
