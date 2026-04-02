package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.sqliteDefaultPersistence
import kotlin.random.Random

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    "$prefix-${Random.nextInt()}-${Random.nextInt()}.db"

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig(persistence = sqliteDefaultPersistence(path))

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    sqliteDefaultPersistence(path)?.clear(path)
}
