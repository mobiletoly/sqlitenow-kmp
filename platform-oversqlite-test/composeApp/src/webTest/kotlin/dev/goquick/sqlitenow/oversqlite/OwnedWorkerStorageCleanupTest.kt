package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlinx.coroutines.test.runTest

internal class OwnedWorkerStorageCleanupTest {
    @Test
    fun cleanupUsesExactOwnershipWithoutCatalogEnumeration() = runTest {
        if (webRuntimeKind() !in setOf("js-browser", "wasm-browser")) return@runTest

        val directName = oversqliteTestDatabaseName()
        val legacyOpfsName = oversqliteTestDatabaseName()
        val legacyIndexedDbName = oversqliteTestDatabaseName()
        val sentinelName = "${oversqliteTestDatabaseName()}-unrelated-sentinel"
        val ownedStorage = OwnedWorkerStorage()
        prepareStatementReuseleanupRegressionArtifacts(
            directDbName = directName,
            legacyOpfsDbName = legacyOpfsName,
            legacyIndexedDbName = legacyIndexedDbName,
            sentinelName = sentinelName,
        )
        ownedStorage.recordDirectWorkerDatabase(directName)
        ownedStorage.recordLegacyOpfsDatabase(legacyOpfsName)
        ownedStorage.recordLegacyIndexedDbDatabase(legacyIndexedDbName)
        ownedStorage.cleanup()
        verifyAndClearStatementReuseleanupRegressionArtifacts(
            directDbName = directName,
            legacyOpfsDbName = legacyOpfsName,
            legacyIndexedDbName = legacyIndexedDbName,
            sentinelName = sentinelName,
        )
    }
}
