package dev.goquick.sqlitenow.oversqlite

import androidx.test.platform.app.InstrumentationRegistry

internal actual fun suiteEnv(name: String): String? =
    InstrumentationRegistry.getArguments().getString(name) ?: System.getenv(name)

internal actual fun webRuntimeKind(): String = "android"

internal actual suspend fun cleanupPhase6DirectWorkerDatabase(dbName: String) = Unit

internal actual suspend fun cleanupPhase6LegacyOpfsDatabase(dbName: String) = Unit

internal actual suspend fun cleanupPhase6LegacyIndexedDbDatabase(dbName: String) = Unit
