package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteConnectionProvider
import kotlin.random.Random

internal expect fun suiteEnv(name: String): String?

internal fun suiteFlagEnabled(name: String): Boolean =
    when (suiteEnv(name)?.trim()?.lowercase()) {
        "1", "true", "yes", "on" -> true
        else -> false
    }

internal fun platformSuiteEnabled(): Boolean =
    suiteFlagEnabled("OVERSQLITE_PLATFORM_TESTS")

internal fun realServerSuiteEnabled(): Boolean =
    suiteFlagEnabled("OVERSQLITE_REALSERVER_TESTS")

internal fun realServerHeavyModeEnabled(): Boolean =
    suiteFlagEnabled("OVERSQLITE_REALSERVER_HEAVY")

internal expect fun webRuntimeKind(): String

internal expect suspend fun cleanupPhase6DirectWorkerDatabase(dbName: String)

internal expect suspend fun cleanupPhase6LegacyOpfsDatabase(dbName: String)

internal expect suspend fun cleanupPhase6LegacyIndexedDbDatabase(dbName: String)

internal fun oversqliteTestConnectionProvider(): SqliteConnectionProvider =
    BundledSqliteConnectionProvider

internal fun oversqliteTestDatabaseName(): String =
    if (webRuntimeKind() in setOf("js-node", "js-browser", "wasm-browser")) {
        val random = Random.nextLong().toString().removePrefix("-")
        "sqlitenow-phase7-oversqlite-$random"
    } else {
        ":memory:"
    }

internal class Phase6OwnedStorage {
    private val directWorkerDatabases = linkedSetOf<String>()
    private val legacyOpfsDatabases = linkedSetOf<String>()
    private val legacyIndexedDbDatabases = linkedSetOf<String>()

    fun newDatabaseName(): String =
        oversqliteTestDatabaseName().also(::recordDirectWorkerDatabase)

    fun recordDirectWorkerDatabase(dbName: String) {
        if (webRuntimeKind() in setOf("js-browser", "wasm-browser")) {
            directWorkerDatabases += dbName
        }
    }

    fun recordLegacyOpfsDatabase(dbName: String) {
        legacyOpfsDatabases += dbName
    }

    fun recordLegacyIndexedDbDatabase(dbName: String) {
        legacyIndexedDbDatabases += dbName
    }

    suspend fun cleanup() {
        directWorkerDatabases.forEach { cleanupPhase6DirectWorkerDatabase(it) }
        legacyOpfsDatabases.forEach { cleanupPhase6LegacyOpfsDatabase(it) }
        legacyIndexedDbDatabases.forEach { cleanupPhase6LegacyIndexedDbDatabase(it) }
        directWorkerDatabases.clear()
        legacyOpfsDatabases.clear()
        legacyIndexedDbDatabases.clear()
    }
}
