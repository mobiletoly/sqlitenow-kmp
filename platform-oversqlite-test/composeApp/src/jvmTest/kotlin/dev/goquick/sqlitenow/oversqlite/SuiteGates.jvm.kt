package dev.goquick.sqlitenow.oversqlite

internal actual fun suiteEnv(name: String): String? = System.getenv(name)

internal actual fun webRuntimeKind(): String = "jvm"

internal actual suspend fun cleanupPhase6DirectWorkerDatabase(dbName: String) = Unit

internal actual suspend fun cleanupPhase6LegacyOpfsDatabase(dbName: String) = Unit

internal actual suspend fun cleanupPhase6LegacyIndexedDbDatabase(dbName: String) = Unit
