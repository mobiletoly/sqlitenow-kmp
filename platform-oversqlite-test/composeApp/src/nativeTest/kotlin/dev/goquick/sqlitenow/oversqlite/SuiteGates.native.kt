@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.goquick.sqlitenow.oversqlite

import kotlinx.cinterop.toKString
import platform.posix.getenv

internal actual fun suiteEnv(name: String): String? =
    getenv(name)?.toKString()

internal actual fun webRuntimeKind(): String = "native"

internal actual suspend fun cleanupWorkerDirectWorkerDatabase(dbName: String) = Unit

internal actual suspend fun cleanupWorkerLegacyOpfsDatabase(dbName: String) = Unit

internal actual suspend fun cleanupWorkerLegacyIndexedDbDatabase(dbName: String) = Unit

internal actual suspend fun sameOriginIframeCanAcquireLifecycleLock(dbName: String): Boolean = false

internal actual suspend fun directWorkerStorageCensus(dbName: String): String = ""
