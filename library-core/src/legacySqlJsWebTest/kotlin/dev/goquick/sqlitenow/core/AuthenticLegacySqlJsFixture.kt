package dev.goquick.sqlitenow.core

internal const val AUTHENTIC_LEGACY_SQLJS_VERSION = "1.13.0"

internal class AuthenticLegacySqlJsFixture(
    val connection: SafeSQLiteConnection,
    private val exportAction: suspend () -> ByteArray,
) {
    suspend fun exportBytes(): ByteArray = exportAction()

    suspend fun close() = connection.close()
}

internal expect suspend fun createAuthenticLegacySqlJsFixture(
    dbName: String,
): AuthenticLegacySqlJsFixture
