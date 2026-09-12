package dev.goquick.sqlitenow.oversqlite

internal expect suspend fun prepareStatementReuseleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
)

internal expect suspend fun verifyAndClearStatementReuseleanupRegressionArtifacts(
    directDbName: String,
    legacyOpfsDbName: String,
    legacyIndexedDbName: String,
    sentinelName: String,
)

internal expect suspend fun workerWorkerStorageEvidence(dbName: String): String
