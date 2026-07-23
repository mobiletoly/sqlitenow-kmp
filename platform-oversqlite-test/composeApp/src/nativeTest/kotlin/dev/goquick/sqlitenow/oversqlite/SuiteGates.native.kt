@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.goquick.sqlitenow.oversqlite

import kotlinx.cinterop.toKString
import platform.posix.getenv

internal actual fun suiteEnv(name: String): String? =
    getenv(name)?.toKString()

internal actual fun webRuntimeKind(): String = "native"

internal actual suspend fun cleanupPhase6DirectWorkerDatabase(dbName: String) = Unit

internal actual suspend fun cleanupPhase6LegacyOpfsDatabase(dbName: String) = Unit

internal actual suspend fun cleanupPhase6LegacyIndexedDbDatabase(dbName: String) = Unit
