package dev.goquick.sqlitenow.oversqlite.platform

import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import java.io.File
import java.util.UUID

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String {
    val context = InstrumentationRegistry.getInstrumentation().targetContext
    return File(context.cacheDir, "$prefix-${UUID.randomUUID()}.db").absolutePath
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
    File(path).delete()
}
