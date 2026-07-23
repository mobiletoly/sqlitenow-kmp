package dev.goquick.sqlitenow.core

internal expect fun legacySnapshotPersistenceForTest(
    dbName: String,
    forceOpfs: Boolean,
): SqlitePersistence
