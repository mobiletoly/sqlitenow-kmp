package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqlJsDatabase
import dev.goquick.sqlitenow.core.sqlite.SqlJsSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.loadSqlJsModule
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

internal actual suspend fun createAuthenticLegacySqlJsFixture(
    dbName: String,
): AuthenticLegacySqlJsFixture {
    val executionContext = createSqliteConnectionExecutionContext("legacy-sqljs-fixture-$dbName")
    try {
        val raw = withContext(executionContext.dispatcher) {
            val module = loadSqlJsModule()
            @Suppress("UNCHECKED_CAST_TO_EXTERNAL_INTERFACE")
            val database = js("new module.Database()") as SqlJsDatabase
            SqlJsSQLiteConnection(database)
        }
        val safe = SafeSQLiteConnection(
            ref = raw,
            debug = false,
            persistenceController = NoopPersistenceController(),
            executionContext = executionContext,
        )
        return AuthenticLegacySqlJsFixture(
            connection = safe,
            exportAction = {
                safe.withDispatcherContext {
                    raw.exportToByteArray()
                }
            },
        )
    } catch (failure: Throwable) {
        withContext(NonCancellable) {
            executionContext.close()
        }
        throw failure
    }
}
