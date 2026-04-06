@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import kotlinx.cinterop.toKString
import platform.posix.getenv
import platform.posix.remove

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String {
    val tempDir =
        getenv("TMPDIR")?.toKString()?.trimEnd('/')?.takeIf { it.isNotEmpty() }
            ?: "/tmp"
    val uniqueSuffix = "${kotlin.random.Random.nextLong()}-${kotlin.random.Random.nextLong()}"
    return "$tempDir/$prefix-$uniqueSuffix.db"
}

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig()

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    deleteIfExists(path)
    deleteIfExists("$path-wal")
    deleteIfExists("$path-shm")
    deleteIfExists("$path-journal")
}

private fun deleteIfExists(path: String) {
    remove(path)
}
