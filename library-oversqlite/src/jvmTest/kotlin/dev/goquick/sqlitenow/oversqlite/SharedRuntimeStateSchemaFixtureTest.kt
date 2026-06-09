package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class SharedRuntimeStateSchemaFixtureTest : SharedRuntimeStateFixtureSupport() {
    private val fixtureFile = oversqliteContractFixture("runtime-state/schema/v0.json")

    @Test
    fun kmpSharedRuntimeStateSchemaFixtureMatchesRuntime() = runBlocking {
        withRuntimeStateUsersDatabase {
            client.open().getOrThrow()
            assertOrUpdateExpected(fixtureFile, dumpRuntimeSchema(db))
        }
    }
}
