package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.random.Random

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    "$prefix-${Random.nextInt()}-${Random.nextInt()}.db"

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig(persistence = IndexedDbSqlitePersistence())

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    IndexedDbSqlitePersistence().clear(path)
}
