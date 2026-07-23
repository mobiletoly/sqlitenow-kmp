package dev.goquick.sqlitenow.oversqlite

internal expect suspend fun preparePhase6CleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
)

internal expect suspend fun verifyAndClearPhase6CleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
)

internal expect suspend fun phase6WorkerStorageEvidence(dbName: String): String
