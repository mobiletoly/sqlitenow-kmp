@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import platform.Foundation.NSFileManager
import platform.Foundation.NSTemporaryDirectory
import platform.Foundation.NSUUID

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    "${NSTemporaryDirectory()}$prefix-${NSUUID().UUIDString}.db"

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig()

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    deleteIfExists(path)
    deleteIfExists("$path-wal")
    deleteIfExists("$path-shm")
    deleteIfExists("$path-journal")
}

private fun deleteIfExists(path: String) {
    NSFileManager.defaultManager.removeItemAtPath(path, error = null)
}
