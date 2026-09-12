package dev.goquick.sqlitenow.oversqlite

import androidx.test.platform.app.InstrumentationRegistry

internal actual fun suiteEnv(name: String): String? =
    InstrumentationRegistry.getArguments().getString(name) ?: System.getenv(name)

internal actual fun webRuntimeKind(): String = "android"

internal actual suspend fun cleanupWorkerDirectWorkerDatabase(dbName: String) = Unit

internal actual suspend fun cleanupWorkerLegacyOpfsDatabase(dbName: String) = Unit

internal actual suspend fun cleanupWorkerLegacyIndexedDbDatabase(dbName: String) = Unit

internal actual suspend fun sameOriginIframeCanAcquireLifecycleLock(dbName: String): Boolean = false

internal actual suspend fun directWorkerStorageCensus(dbName: String): String = ""
