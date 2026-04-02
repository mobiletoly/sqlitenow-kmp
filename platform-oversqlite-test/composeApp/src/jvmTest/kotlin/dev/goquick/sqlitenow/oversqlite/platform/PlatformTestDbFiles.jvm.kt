package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import java.nio.file.Files
import java.nio.file.Path

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    Files.createTempFile(prefix, ".db").toAbsolutePath().toString()

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig()

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    deleteIfExists(path)
    deleteIfExists("$path-wal")
    deleteIfExists("$path-shm")
    deleteIfExists("$path-journal")
}

private fun deleteIfExists(path: String) {
    Files.deleteIfExists(Path.of(path))
}
