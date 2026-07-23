package dev.goquick.sqlitenow.core.test

internal actual suspend fun cleanupPlatformHarnessDatabase(dbName: String) {
    cleanupGeneratedWorkerMigrationState(dbName)
}
