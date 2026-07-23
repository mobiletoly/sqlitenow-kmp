package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlinx.coroutines.test.runTest

internal class Phase6OwnedStorageCleanupTest {
    @Test
    fun cleanupUsesExactOwnershipWithoutCatalogEnumeration() = runTest {
        if (webRuntimeKind() !in setOf("js-browser", "wasm-browser")) return@runTest

        val directName = oversqliteTestDatabaseName()
        val legacyOpfsName = oversqliteTestDatabaseName()
        val legacyIndexedDbName = oversqliteTestDatabaseName()
        val sentinelName = "${oversqliteTestDatabaseName()}-unrelated-sentinel"
        val ownedStorage = Phase6OwnedStorage()
        preparePhase6CleanupRegressionArtifacts(
            directDbName = directName,
            legacyOpfsDbName = legacyOpfsName,
            legacyIndexedDbName = legacyIndexedDbName,
            sentinelName = sentinelName,
        )
        ownedStorage.recordDirectWorkerDatabase(directName)
        ownedStorage.recordLegacyOpfsDatabase(legacyOpfsName)
        ownedStorage.recordLegacyIndexedDbDatabase(legacyIndexedDbName)
        ownedStorage.cleanup()
        verifyAndClearPhase6CleanupRegressionArtifacts(
            directDbName = directName,
            legacyOpfsDbName = legacyOpfsName,
            legacyIndexedDbName = legacyIndexedDbName,
            sentinelName = sentinelName,
        )
    }
}
