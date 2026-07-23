@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqlJsDatabaseHandle
import dev.goquick.sqlitenow.core.sqlite.SqlJsSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.dbCreate
import dev.goquick.sqlitenow.core.sqlite.ensureSqlJsLoaded
import dev.goquick.sqlitenow.core.sqlite.wrapSqlite
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

internal actual suspend fun createAuthenticLegacySqlJsFixture(
    dbName: String,
): AuthenticLegacySqlJsFixture {
    val executionContext = createSqliteConnectionExecutionContext("legacy-sqljs-fixture-$dbName")
    try {
        val raw = withContext(executionContext.dispatcher) {
            ensureSqlJsLoaded()
            SqlJsSQLiteConnection(SqlJsDatabaseHandle(wrapSqlite { dbCreate() }))
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
