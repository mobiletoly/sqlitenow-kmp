package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig

internal expect fun createTempSqliteNowTestDbPath(prefix: String): String

internal expect fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig

internal expect suspend fun deleteTempSqliteNowTestDbArtifacts(path: String)
