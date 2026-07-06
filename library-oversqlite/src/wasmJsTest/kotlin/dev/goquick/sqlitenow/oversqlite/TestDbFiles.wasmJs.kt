package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.random.Random

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    "$prefix-${Random.nextInt()}-${Random.nextInt()}.db"

private val testPersistence = IndexedDbSqlitePersistence(
    storageName = "SqliteNowOversqliteTest",
    storeName = "sqlite-tests",
)

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig(persistence = testPersistence)

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    testPersistence.clear(path)
}
